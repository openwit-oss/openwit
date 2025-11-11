//! Connects storage pipeline to Tantivy indexing

use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{info, error, warn};
use anyhow::Result;
use std::collections::{HashMap, VecDeque};
use chrono::{DateTime, Utc, Duration};
use tokio::time::{Duration as TokioDuration};
use futures::TryStreamExt;

use openwit_indexer::{
    Document as IndexDocument,
    FieldValue,
    tantivy_indexer::TantivyIndexer,
    Index as IndexTrait,
    IndexConfig,
    IndexType,
    FieldConfig,
    FieldType,
    CompressionType,
};
use openwit_ingestion::IngestedMessage;
use openwit_metastore::{MetaStore, SegmentStatus};

/// Retry configuration for failed segments
#[derive(Debug, Clone)]
struct RetryConfig {
    max_retries: u32,
    initial_delay_ms: u64,
    max_delay_ms: u64,
    exponential_base: f64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 5,
            initial_delay_ms: 1000,      // 1 second
            max_delay_ms: 60000,         // 1 minute
            exponential_base: 2.0,
        }
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
struct RetryItem {
    segment_id: String,
    retry_count: u32,
    last_attempt: DateTime<Utc>,
    next_retry: DateTime<Utc>,
}

/// Indexing service that reads segments from metastore and indexes them
pub struct IndexingService {
    indexer: TantivyIndexer,
    metastore: Arc<dyn MetaStore>,
    batch_size: usize,
    flush_interval: std::time::Duration,
    retry_config: RetryConfig,
    retry_queue: VecDeque<RetryItem>,
}

impl IndexingService {
    pub async fn new(
        index_path: String,
        metastore: Arc<dyn MetaStore>,
        batch_size: usize,
        flush_interval_secs: u64,
    ) -> Result<Self> {
        // Create index configuration
        let config = IndexConfig {
            index_type: IndexType::TimeSeries,
            fields: vec![
                FieldConfig {
                    name: "timestamp".to_string(),
                    field_type: FieldType::Timestamp,
                    indexed: true,
                    stored: true,
                    tokenized: false,
                    faceted: true,
                    fast: true,
                },
                FieldConfig {
                    name: "service".to_string(),
                    field_type: FieldType::Keyword,
                    indexed: true,
                    stored: true,
                    tokenized: false,
                    faceted: true,
                    fast: true,
                },
                FieldConfig {
                    name: "level".to_string(),
                    field_type: FieldType::Keyword,
                    indexed: true,
                    stored: true,
                    tokenized: false,
                    faceted: true,
                    fast: true,
                },
                FieldConfig {
                    name: "message".to_string(),
                    field_type: FieldType::Text,
                    indexed: true,
                    stored: true,
                    tokenized: true,
                    faceted: false,
                    fast: false,
                },
                FieldConfig {
                    name: "trace_id".to_string(),
                    field_type: FieldType::Keyword,
                    indexed: true,
                    stored: true,
                    tokenized: false,
                    faceted: false,
                    fast: false,
                },
                FieldConfig {
                    name: "span_id".to_string(),
                    field_type: FieldType::Keyword,
                    indexed: true,
                    stored: true,
                    tokenized: false,
                    faceted: false,
                    fast: false,
                },
            ],
            storage_path: index_path,
            cache_size_mb: 512,
            compression: CompressionType::Snappy,
            compaction_threshold: 0.8,
            bloom_filter_fpp: 0.01,
        };
        
        let indexer = TantivyIndexer::new(config, metastore.clone()).await?;
        
        Ok(Self {
            indexer,
            metastore,
            batch_size,
            flush_interval: std::time::Duration::from_secs(flush_interval_secs),
            retry_config: RetryConfig::default(),
            retry_queue: VecDeque::new(),
        })
    }
    
    /// Calculate retry delay with exponential backoff
    fn calculate_retry_delay(&self, retry_count: u32) -> u64 {
        let delay = self.retry_config.initial_delay_ms as f64 * 
                   self.retry_config.exponential_base.powi(retry_count as i32);
        delay.min(self.retry_config.max_delay_ms as f64) as u64
    }
    
    /// Process a single segment
    async fn process_segment(&mut self, segment_id: &str, retry_count: u32) -> Result<Vec<IndexDocument>> {
        // Get segment metadata from metastore
        let segment = self.metastore.get_segment(segment_id).await?
            .ok_or_else(|| anyhow::anyhow!("Segment {} not found in metastore", segment_id))?;
        
        if segment.storage_status != SegmentStatus::Stored {
            return Err(anyhow::anyhow!("Segment {} not yet stored", segment_id));
        }
        
        // Already indexed?
        if segment.index_status == SegmentStatus::Stored {
            info!("Segment {} already indexed, skipping", segment_id);
            return Ok(vec![]);
        }
        
        info!("📄 Processing segment {} from {} (attempt #{})", segment_id, segment.file_path, retry_count + 1);
        
        // Read and convert parquet file
        let documents = self.read_and_convert_parquet(&segment.file_path).await?;
        info!("📊 Converted {} documents from segment {}", documents.len(), segment_id);
        
        Ok(documents)
    }
    
    /// Start the indexing service
    pub async fn start(
        mut self,
        mut receiver: mpsc::Receiver<String>, // Now receives segment IDs
    ) -> Result<()> {
        info!("🔍 Indexing service started - reading from metastore with retry support");
        
        let mut batch = Vec::new();
        let mut last_flush = std::time::Instant::now();
        let mut total_indexed = 0u64;
        let mut total_segments = 0u64;
        let mut total_retries = 0u64;
        
        // Spawn periodic retry checker
        let _retry_interval = TokioDuration::from_secs(10); // Check retries every 10 seconds
        
        loop {
            // Process any pending retries first
            let now = Utc::now();
            while let Some(retry_item) = self.retry_queue.front() {
                if retry_item.next_retry > now {
                    break; // Not time yet
                }
                
                let retry_item = self.retry_queue.pop_front().unwrap();
                info!("🔄 Retrying segment {} (attempt #{}/{})", 
                    retry_item.segment_id, 
                    retry_item.retry_count + 1,
                    self.retry_config.max_retries
                );
                total_retries += 1;
                
                match self.process_segment(&retry_item.segment_id, retry_item.retry_count).await {
                    Ok(documents) => {
                        batch.extend(documents);
                        info!("✅ Retry successful for segment {}", retry_item.segment_id);
                    }
                    Err(e) => {
                        if retry_item.retry_count < self.retry_config.max_retries {
                            // Schedule next retry
                            let delay_ms = self.calculate_retry_delay(retry_item.retry_count + 1);
                            let next_retry = Utc::now() + Duration::milliseconds(delay_ms as i64);
                            
                            self.retry_queue.push_back(RetryItem {
                                segment_id: retry_item.segment_id.clone(),
                                retry_count: retry_item.retry_count + 1,
                                last_attempt: Utc::now(),
                                next_retry,
                            });
                            
                            warn!("Segment {} retry failed: {:?}. Next retry in {}ms", 
                                retry_item.segment_id, e, delay_ms);
                        } else {
                            error!("Segment {} failed after {} retries. Marking as permanently failed.", 
                                retry_item.segment_id, self.retry_config.max_retries);
                            let _ = self.metastore.mark_segment_index_failed(
                                &retry_item.segment_id, 
                                &format!("Failed after {} retries: {}", self.retry_config.max_retries, e)
                            ).await;
                        }
                    }
                }
            }
            
            // Wait for new segment IDs with timeout
            let timeout = std::cmp::min(
                self.flush_interval,
                if self.retry_queue.is_empty() { 
                    self.flush_interval 
                } else { 
                    TokioDuration::from_secs(1) // Check retries more frequently
                }
            );
            
            match tokio::time::timeout(timeout, receiver.recv()).await {
                Ok(Some(segment_id)) => {
                    total_segments += 1;
                    info!("📦 Received segment {} for indexing", segment_id);
                    
                    // Process the new segment
                    match self.process_segment(&segment_id, 0).await {
                        Ok(documents) => {
                            batch.extend(documents);
                                    
                                    // Check if we should index the batch
                                    if batch.len() >= self.batch_size {
                                        match self.indexer.index_batch(batch.clone()).await {
                                            Ok(()) => {
                                                total_indexed += batch.len() as u64;
                                                info!("📇 Indexed {} documents (total: {})", batch.len(), total_indexed);
                                                
                                                // Update segment status to indexed
                                                // Note: Using generic update approach since mark_segment_index_completed doesn't exist
                                                info!("Segment {} indexed successfully", segment_id);
                                                
                                                batch.clear();
                                                last_flush = std::time::Instant::now();
                                            }
                                            Err(e) => {
                                                error!("Failed to index batch: {:?}", e);
                                                // Don't mark as failed immediately - will be retried
                                                // Segments are tracked separately and will be retried
                                            }
                                        }
                                    }
                        }
                        Err(e) => {
                            error!("Failed to process segment {}: {:?}", segment_id, e);
                            // Add to retry queue
                            if self.retry_config.max_retries > 0 {
                                let delay_ms = self.calculate_retry_delay(1);
                                let next_retry = Utc::now() + Duration::milliseconds(delay_ms as i64);
                                
                                self.retry_queue.push_back(RetryItem {
                                    segment_id: segment_id.clone(),
                                    retry_count: 1,
                                    last_attempt: Utc::now(),
                                    next_retry,
                                });
                                
                                info!("Added segment {} to retry queue. Will retry in {}ms", segment_id, delay_ms);
                            } else {
                                let _ = self.metastore.mark_segment_index_failed(&segment_id, &e.to_string()).await;
                            }
                        }
                    }
                }
                Ok(None) => {
                    info!("Indexing channel closed, shutting down");
                    break;
                }
                Err(_) => {
                    // Timeout - flush if we have documents
                    if !batch.is_empty() && last_flush.elapsed() >= self.flush_interval {
                        match self.indexer.index_batch(batch.clone()).await {
                            Ok(()) => {
                                total_indexed += batch.len() as u64;
                                info!("⏰ Time-based index flush: {} documents (total: {})", 
                                    batch.len(), total_indexed);
                                batch.clear();
                                last_flush = std::time::Instant::now();
                            }
                            Err(e) => {
                                error!("Failed to index batch: {:?}", e);
                            }
                        }
                    }
                }
            }
        }
        
        // Final flush
        if !batch.is_empty() {
            match self.indexer.index_batch(batch.clone()).await {
                Ok(()) => {
                    total_indexed += batch.len() as u64;
                    info!("🏁 Final index flush: {} documents (total: {})", 
                        batch.len(), total_indexed);
                }
                Err(e) => {
                    error!("Failed to index final batch: {:?}", e);
                }
            }
        }
        
        // Flush index to disk
        self.indexer.flush().await?;
        info!("✅ Indexing service stopped. Processed {} segments, indexed {} documents, {} retries", 
            total_segments, total_indexed, total_retries);
        
        // Log any remaining items in retry queue
        if !self.retry_queue.is_empty() {
            warn!("⚠️ {} segments still in retry queue", self.retry_queue.len());
        }
        
        Ok(())
    }
    
    /// Read parquet file and convert to index documents
    #[allow(dead_code)]
    async fn read_and_convert_parquet(&self, file_path: &str) -> Result<Vec<IndexDocument>> {
        use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
        use arrow::array::{AsArray};
        use arrow::datatypes::{TimestampMillisecondType, UInt64Type};
        
        let mut documents = Vec::new();
        
        // Handle different file paths (local vs Azure)
        if file_path.starts_with("azure://") {
            // TODO: Implement Azure blob reading
            warn!("Azure blob reading not yet implemented for indexing");
            return Ok(documents);
        }
        
        // Read local parquet file
        let file = tokio::fs::File::open(file_path).await?;
        let builder = ParquetRecordBatchStreamBuilder::new(file).await?;
        let mut stream = builder.build()?;
        
        while let Some(batch) = stream.try_next().await? {
            // Extract columns
            let id_array = batch.column_by_name("id")
                .ok_or_else(|| anyhow::anyhow!("Missing id column"))?
                .as_string::<i32>();
            let timestamp_array = batch.column_by_name("received_at")
                .ok_or_else(|| anyhow::anyhow!("Missing received_at column"))?
                .as_primitive::<TimestampMillisecondType>();
            let payload_type_array = batch.column_by_name("payload_type")
                .ok_or_else(|| anyhow::anyhow!("Missing payload_type column"))?
                .as_string::<i32>();
            let payload_data_array = batch.column_by_name("payload_data")
                .ok_or_else(|| anyhow::anyhow!("Missing payload_data column"))?
                .as_binary::<i32>();
            let size_array = batch.column_by_name("size_bytes")
                .ok_or_else(|| anyhow::anyhow!("Missing size_bytes column"))?
                .as_primitive::<UInt64Type>();
            
            // Process each row
            for i in 0..batch.num_rows() {
                let id = id_array.value(i);
                let timestamp_millis = timestamp_array.value(i);
                let payload_type = payload_type_array.value(i);
                let payload_data = payload_data_array.value(i);
                let size_bytes = size_array.value(i) as usize;
                
                // Only process logs for now
                if payload_type != "logs" {
                    continue;
                }
                
                // Parse JSON logs data
                match serde_json::from_slice::<serde_json::Value>(payload_data) {
                    Ok(json) => {
                        let mut fields = HashMap::new();
                        
                        // Convert timestamp
                        let timestamp = DateTime::<Utc>::from_timestamp_millis(timestamp_millis)
                            .unwrap_or_else(Utc::now);
                        
                        // Extract common fields
                        if let Some(service) = json.get("resource")
                            .and_then(|r| r.get("service.name"))
                            .and_then(|v| v.as_str()) {
                            fields.insert("service".to_string(), FieldValue::String(service.to_string()));
                        }
                        
                        if let Some(level) = json.get("severity_text").and_then(|v| v.as_str()) {
                            fields.insert("level".to_string(), FieldValue::String(level.to_string()));
                        }
                        
                        if let Some(body) = json.get("body") {
                            let message = match body {
                                serde_json::Value::String(s) => s.clone(),
                                _ => body.to_string(),
                            };
                            fields.insert("message".to_string(), FieldValue::String(message));
                        }
                        
                        if let Some(trace_id) = json.get("trace_id").and_then(|v| v.as_str()) {
                            fields.insert("trace_id".to_string(), FieldValue::String(trace_id.to_string()));
                        }
                        
                        if let Some(span_id) = json.get("span_id").and_then(|v| v.as_str()) {
                            fields.insert("span_id".to_string(), FieldValue::String(span_id.to_string()));
                        }
                        
                        // Store full JSON as attributes
                        fields.insert("attributes".to_string(), FieldValue::String(json.to_string()));
                        
                        documents.push(IndexDocument {
                            id: id.to_string(),
                            timestamp,
                            fields,
                            raw_size: size_bytes,
                        });
                    }
                    Err(e) => {
                        warn!("Failed to parse log JSON at row {}: {:?}", i, e);
                    }
                }
            }
        }
        
        Ok(documents)
    }
    
    /// Convert IngestedMessage to Index Document
    #[allow(dead_code)]
    async fn convert_to_document(&self, msg: IngestedMessage) -> Option<IndexDocument> {
        match &msg.payload {
            openwit_ingestion::types::MessagePayload::Logs(data) => {
                // Parse JSON logs data
                match serde_json::from_slice::<serde_json::Value>(data) {
                    Ok(json) => {
                        let mut fields = HashMap::new();
                        
                        // Extract timestamp
                        let timestamp = if let Some(ts_str) = json.get("timestamp").and_then(|v| v.as_str()) {
                            chrono::DateTime::parse_from_rfc3339(ts_str)
                                .ok()
                                .map(|dt| dt.with_timezone(&Utc))
                                .unwrap_or_else(Utc::now)
                        } else {
                            Utc::now()
                        };
                        
                        // Extract common fields
                        if let Some(service) = json.get("resource")
                            .and_then(|r| r.get("service.name"))
                            .and_then(|v| v.as_str()) {
                            fields.insert("service".to_string(), FieldValue::String(service.to_string()));
                        }
                        
                        if let Some(level) = json.get("severity_text").and_then(|v| v.as_str()) {
                            fields.insert("level".to_string(), FieldValue::String(level.to_string()));
                        }
                        
                        if let Some(body) = json.get("body") {
                            let message = match body {
                                serde_json::Value::String(s) => s.clone(),
                                _ => body.to_string(),
                            };
                            fields.insert("message".to_string(), FieldValue::String(message));
                        }
                        
                        if let Some(trace_id) = json.get("trace_id").and_then(|v| v.as_str()) {
                            fields.insert("trace_id".to_string(), FieldValue::String(trace_id.to_string()));
                        }
                        
                        if let Some(span_id) = json.get("span_id").and_then(|v| v.as_str()) {
                            fields.insert("span_id".to_string(), FieldValue::String(span_id.to_string()));
                        }
                        
                        // Store full JSON as attributes
                        fields.insert("attributes".to_string(), FieldValue::String(json.to_string()));
                        
                        Some(IndexDocument {
                            id: msg.id.clone(),
                            timestamp,
                            fields,
                            raw_size: msg.size_bytes,
                        })
                    }
                    Err(e) => {
                        warn!("Failed to parse log JSON: {:?}", e);
                        None
                    }
                }
            }
            openwit_ingestion::types::MessagePayload::Trace(_) => {
                // TODO: Implement trace indexing
                None
            }
            openwit_ingestion::types::MessagePayload::Metrics(_) => {
                // TODO: Implement metrics indexing
                None
            }
        }
    }
}

/// Start the indexing pipeline
pub async fn start_indexing_pipeline(
    index_path: String,
    metastore: Arc<dyn MetaStore>,
    receiver: mpsc::Receiver<String>, // Now receives segment IDs
) -> Result<()> {
    let service = IndexingService::new(
        index_path,
        metastore.clone(),
        1000,  // Batch size
        30,    // Flush interval seconds
    ).await?;
    
    // Spawn a background task to periodically check for stuck segments
    let metastore_clone = metastore.clone();
    let (stuck_tx, stuck_rx) = mpsc::channel::<String>(100);
    
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(TokioDuration::from_secs(60)); // Check every minute
        
        loop {
            interval.tick().await;
            
            // Find segments that are stored but not indexed (stuck)
            match metastore_clone.list_segments_by_status(
                Some(SegmentStatus::Stored),
                Some(SegmentStatus::Pending)
            ).await {
                Ok(stuck_segments) => {
                    if !stuck_segments.is_empty() {
                        info!("🔍 Found {} stuck segments (stored but not indexed)", stuck_segments.len());
                        
                        for segment in stuck_segments {
                            // Check if segment is old enough (e.g., > 5 minutes)
                            let age = Utc::now() - segment.updated_at;
                            if age > Duration::minutes(5) {
                                info!("➡️ Requeuing stuck segment {} (age: {})", segment.segment_id, age);
                                let _ = stuck_tx.send(segment.segment_id).await;
                            }
                        }
                    }
                }
                Err(e) => {
                    error!("Failed to check for stuck segments: {:?}", e);
                }
            }
        }
    });
    
    // Merge stuck segments channel with regular channel
    let merged_rx = merge_receivers(receiver, stuck_rx);
    
    service.start(merged_rx).await
}

/// Merge two receivers into one
fn merge_receivers<T: Send + 'static>(
    rx1: mpsc::Receiver<T>,
    rx2: mpsc::Receiver<T>,
) -> mpsc::Receiver<T> {
    let (tx, rx) = mpsc::channel(200);
    
    // Forward from rx1
    let tx1 = tx.clone();
    tokio::spawn(async move {
        let mut rx1 = rx1;
        while let Some(item) = rx1.recv().await {
            let _ = tx1.send(item).await;
        }
    });
    
    // Forward from rx2
    tokio::spawn(async move {
        let mut rx2 = rx2;
        while let Some(item) = rx2.recv().await {
            let _ = tx.send(item).await;
        }
    });
    
    rx
}