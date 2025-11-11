use tokio::sync::mpsc;
use tokio::sync::RwLock;
use anyhow::{Result, Context};
use tracing::{info, debug, error, warn};
use serde_json::Value;
use base64::{engine::general_purpose, Engine as _};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::record_batch::RecordBatch;
use openwit_storage::otlp_ingestion::{process_otlp_json_batch, create_complete_otlp_schema};
use openwit_inter_node::ArrowFlightBatch;

/// Configuration for batching
#[derive(Clone, Debug)]
pub struct BatchConfig {
    pub max_spans_per_batch: usize,
    pub max_batch_bytes: usize,
    pub flush_interval: Duration,
    pub max_messages_per_batch: usize,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            max_spans_per_batch: 10_000,
            max_batch_bytes: 50_000_000,
            flush_interval: Duration::from_secs(5),
            max_messages_per_batch: 1000,
        }
    }
}

/// A batch accumulator for a specific client
struct ClientBatch {
    client_name: String,
    messages: Vec<(String, Value, HashMap<String, String>)>, // (message_id, otlp_json, headers)
    total_spans: usize,
    total_bytes: usize,
    created_at: Instant,
}

impl ClientBatch {
    fn new(client_name: String) -> Self {
        Self {
            client_name,
            messages: Vec::new(),
            total_spans: 0,
            total_bytes: 0,
            created_at: Instant::now(),
        }
    }
    
    fn add_message(&mut self, message_id: String, otlp_json: Value, headers: HashMap<String, String>, size_estimate: usize) -> Result<usize> {
        // Count spans in this message
        let mut span_count = 0;
        if let Some(resource_spans) = otlp_json["resourceSpans"].as_array() {
            for rs in resource_spans {
                if let Some(scope_spans) = rs["scopeSpans"].as_array() {
                    for ss in scope_spans {
                        if let Some(spans) = ss["spans"].as_array() {
                            span_count += spans.len();
                        }
                    }
                }
            }
        }
        
        self.messages.push((message_id, otlp_json, headers));
        self.total_spans += span_count;
        self.total_bytes += size_estimate;
        
        Ok(span_count)
    }
    
    fn should_flush(&self, config: &BatchConfig) -> bool {
        self.total_spans >= config.max_spans_per_batch ||
        self.total_bytes >= config.max_batch_bytes ||
        self.messages.len() >= config.max_messages_per_batch ||
        self.created_at.elapsed() >= config.flush_interval
    }
}

/// Batched OTLP receiver that accumulates messages before sending
pub struct BatchedOtlpReceiver {
    node_id: String,
    batch_sender: mpsc::Sender<ArrowFlightBatch>,
    config: BatchConfig,
    /// Batches per client
    client_batches: Arc<RwLock<HashMap<String, ClientBatch>>>,
}

impl BatchedOtlpReceiver {
    pub fn new(
        node_id: String,
        batch_sender: mpsc::Sender<ArrowFlightBatch>,
        config: Option<BatchConfig>,
    ) -> Self {
        let config = config.unwrap_or_default();
        let receiver = Self {
            node_id,
            batch_sender,
            config: config.clone(),
            client_batches: Arc::new(RwLock::new(HashMap::new())),
        };
        
        // Start background flush task
        let batches = receiver.client_batches.clone();
        let sender = receiver.batch_sender.clone();
        let node_id = receiver.node_id.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            loop {
                interval.tick().await;
                if let Err(e) = flush_old_batches(batches.clone(), sender.clone(), &node_id, &config).await {
                    error!("Error flushing old batches: {}", e);
                }
            }
        });
        
        receiver
    }
    
    /// Process incoming Kafka message with OTLP data
    pub async fn process_message(
        &self,
        message_id: &str,
        payload: &[u8],
        headers: HashMap<String, String>,
    ) -> Result<()> {
        // Extract client name
        let client_name = headers.get("client_name")
            .or_else(|| headers.get("index_name"))
            .map(|s| s.to_string())
            .or_else(|| headers.get("topic").map(|t| extract_client_from_topic(t)))
            .unwrap_or_else(|| "default".to_string());
        
        // Parse OTLP data
        let otlp_json = parse_otlp_payload(payload)?;
        
        // Validate OTLP structure
        if otlp_json["resourceSpans"].as_array().is_none() {
            return Err(anyhow::anyhow!("Invalid OTLP structure: missing resourceSpans"));
        }
        
        // Add to batch
        let mut batches = self.client_batches.write().await;
        let batch = batches.entry(client_name.clone())
            .or_insert_with(|| ClientBatch::new(client_name.clone()));
        
        let size_estimate = payload.len();
        let span_count = batch.add_message(message_id.to_string(), otlp_json, headers, size_estimate)?;
        
        // Log every 100th message or first few messages
        let msg_count = batch.messages.len();
        if msg_count <= 5 || msg_count % 100 == 0 {
            info!("BATCHED_OTLP: Added message {} to {} batch", message_id, client_name);
            info!("BATCHED_OTLP:   - New spans: {}", span_count);
            info!("BATCHED_OTLP:   - Total messages in batch: {}", msg_count);
            info!("BATCHED_OTLP:   - Total spans in batch: {}", batch.total_spans);
            info!("BATCHED_OTLP:   - Total bytes in batch: {} ({:.2} MB)", 
                batch.total_bytes, batch.total_bytes as f64 / 1_048_576.0);
            info!("BATCHED_OTLP:   - Batch age: {:?}", batch.created_at.elapsed());
        }
        
        // Check if batch should be flushed
        if batch.should_flush(&self.config) {
            info!("BATCHED_OTLP: ========== FLUSHING BATCH ==========");
            info!("BATCHED_OTLP: Client: {}", client_name);
            info!("BATCHED_OTLP: Messages: {}", batch.messages.len());
            info!("BATCHED_OTLP: Total spans: {}", batch.total_spans);
            info!("BATCHED_OTLP: Total size: {} bytes ({:.2} MB)", 
                batch.total_bytes, batch.total_bytes as f64 / 1_048_576.0);
            info!("BATCHED_OTLP: Batch age: {:?}", batch.created_at.elapsed());
            info!("BATCHED_OTLP: Flush reason: {}", 
                if batch.total_spans >= self.config.max_spans_per_batch { "max_spans" }
                else if batch.total_bytes >= self.config.max_batch_bytes { "max_bytes" }
                else if batch.messages.len() >= self.config.max_messages_per_batch { "max_messages" }
                else { "time_based" }
            );
            info!("BATCHED_OTLP: ====================================");
            
            let ready_batch = batches.remove(&client_name).unwrap();
            drop(batches); // Release lock before processing
            
            // Process and send batch
            if let Err(e) = self.process_and_send_batch(ready_batch).await {
                error!("Failed to process batch for client {}: {}", client_name, e);
                return Err(e);
            }
        }
        
        Ok(())
    }
    
    /// Process accumulated messages and send as Arrow batches
    async fn process_and_send_batch(&self, batch: ClientBatch) -> Result<()> {
        let start = Instant::now();
        let client_name = &batch.client_name;
        let message_count = batch.messages.len();
        let span_count = batch.total_spans;
        
        // Merge all OTLP data into a single structure
        let merged_otlp = merge_otlp_messages(&batch.messages)?;
        
        // Generate a batch ID
        let batch_id = format!("{}_merged_{}", 
            uuid::Uuid::new_v4().to_string(),
            batch.messages.len()
        );
        
        // Use the first message's headers as base, adding batch metadata
        let mut metadata = if let Some((_, _, headers)) = batch.messages.first() {
            headers.clone()
        } else {
            HashMap::new()
        };
        
        metadata.insert("batch_id".to_string(), batch_id.clone());
        metadata.insert("merged_messages".to_string(), message_count.to_string());
        metadata.insert("total_spans".to_string(), span_count.to_string());
        metadata.insert("client_name".to_string(), client_name.to_string());
        metadata.insert("otlp_format".to_string(), "true".to_string());
        
        // Process merged OTLP to Arrow batches
        let arrow_batches = process_otlp_json_batch(
            &merged_otlp,
            &batch_id,
            &self.node_id,
            client_name,
            metadata.get("account_key").map(|s| s.as_str()),
        )?;
        
        // Send Arrow batches
        let arrow_batch_count = arrow_batches.len();
        for (idx, arrow_batch) in arrow_batches.into_iter().enumerate() {
            let sub_batch_id = format!("{}_{}", batch_id, idx);
            let row_count = arrow_batch.num_rows();
            
            if row_count == 0 {
                continue;
            }
            
            let mut batch_metadata = metadata.clone();
            batch_metadata.insert("sub_batch_id".to_string(), sub_batch_id.clone());
            batch_metadata.insert("arrow_batch_index".to_string(), idx.to_string());
            
            let flight_batch = ArrowFlightBatch {
                batch_id: sub_batch_id,
                source_node: self.node_id.clone(),
                target_node: "storage".to_string(),
                record_batch: arrow_batch,
                metadata: batch_metadata,
            };
            
            self.batch_sender.send(flight_batch).await
                .context("Failed to send batch to storage")?;
            
            debug!("Sent Arrow batch {} with {} rows", idx, row_count);
        }
        
        info!("BATCHED_OTLP: Batch processing complete for {}", client_name);
        info!("BATCHED_OTLP:   - Processing time: {:?}", start.elapsed());
        info!("BATCHED_OTLP:   - Input: {} messages, {} spans", message_count, span_count);
        info!("BATCHED_OTLP:   - Output: {} Arrow batches", arrow_batch_count);
        info!("BATCHED_OTLP:   - Processing rate: {:.2} spans/sec", 
            span_count as f64 / start.elapsed().as_secs_f64());
        
        Ok(())
    }
    
    /// Force flush all pending batches
    pub async fn flush_all(&self) -> Result<()> {
        let mut batches = self.client_batches.write().await;
        let all_batches: Vec<_> = batches.drain().map(|(_, batch)| batch).collect();
        drop(batches);
        
        for batch in all_batches {
            let client = batch.client_name.clone();
            if let Err(e) = self.process_and_send_batch(batch).await {
                error!("Failed to flush batch for client {}: {}", client, e);
            }
        }
        
        Ok(())
    }
}

/// Parse OTLP payload from various formats
fn parse_otlp_payload(payload: &[u8]) -> Result<Value> {
    // Try direct JSON parse
    if let Ok(json) = serde_json::from_slice::<Value>(payload) {
        return Ok(json);
    }
    
    // Try protobuf
    if let Ok(json) = try_parse_otlp_protobuf(payload) {
        return Ok(json);
    }
    
    // Try base64 decode then protobuf
    if let Ok(decoded) = general_purpose::STANDARD.decode(payload) {
        if let Ok(json) = try_parse_otlp_protobuf(&decoded) {
            return Ok(json);
        }
        // Try base64 decode then JSON
        if let Ok(json) = serde_json::from_slice::<Value>(&decoded) {
            return Ok(json);
        }
    }
    
    Err(anyhow::anyhow!("Failed to parse payload as JSON, protobuf, or base64"))
}

/// Merge multiple OTLP messages into a single structure
fn merge_otlp_messages(messages: &[(String, Value, HashMap<String, String>)]) -> Result<Value> {
    let mut merged = serde_json::json!({
        "resourceSpans": []
    });
    
    let merged_rs = merged["resourceSpans"].as_array_mut().unwrap();
    
    for (message_id, otlp_json, _) in messages {
        if let Some(resource_spans) = otlp_json["resourceSpans"].as_array() {
            for rs in resource_spans {
                // Clone and add message ID to each resourceSpan for traceability
                let mut rs_clone = rs.clone();
                if let Some(obj) = rs_clone.as_object_mut() {
                    obj.insert("_source_message_id".to_string(), Value::String(message_id.clone()));
                }
                merged_rs.push(rs_clone);
            }
        }
    }
    
    Ok(merged)
}

/// Background task to flush old batches
async fn flush_old_batches(
    batches: Arc<RwLock<HashMap<String, ClientBatch>>>,
    sender: mpsc::Sender<ArrowFlightBatch>,
    node_id: &str,
    config: &BatchConfig,
) -> Result<()> {
    let mut batches = batches.write().await;
    let mut to_flush = Vec::new();
    
    // Find batches that need flushing due to age
    for (client, batch) in batches.iter() {
        if batch.created_at.elapsed() >= config.flush_interval {
            to_flush.push(client.clone());
        }
    }
    
    // Remove old batches
    let old_batches: Vec<(String, ClientBatch)> = to_flush.into_iter()
        .filter_map(|client| {
            batches.remove(&client).map(|batch| (client, batch))
        })
        .collect();
    
    // Release lock before processing
    drop(batches);
    
    // Process old batches
    for (client, batch) in old_batches {
        info!("Flushing aged batch for client {} with {} messages, {} spans",
            client, batch.messages.len(), batch.total_spans);
        
        // Create a temporary receiver to process the batch
        let temp_receiver = BatchedOtlpReceiver {
            node_id: node_id.to_string(),
            batch_sender: sender.clone(),
            config: config.clone(),
            client_batches: Arc::new(RwLock::new(HashMap::new())),
        };
        
        if let Err(e) = temp_receiver.process_and_send_batch(batch).await {
            error!("Failed to flush aged batch for client {}: {}", client, e);
        }
    }
    
    Ok(())
}

/// Extract client name from topic
fn extract_client_from_topic(topic: &str) -> String {
    // Format: v6.qtw.traces.{client}.{partition}
    let parts: Vec<&str> = topic.split('.').collect();
    if parts.len() >= 4 {
        parts[3].to_string()
    } else {
        "unknown".to_string()
    }
}

/// Try to parse OTLP protobuf data
fn try_parse_otlp_protobuf(data: &[u8]) -> Result<Value> {
    use opentelemetry_proto::tonic::trace::v1::TracesData;
    use prost::Message;
    
    let traces_data = TracesData::decode(data)
        .context("Failed to decode OTLP protobuf")?;
    
    // Convert to JSON using serde
    let json_str = serde_json::to_string(&traces_data)?;
    let json: Value = serde_json::from_str(&json_str)?;
    
    Ok(json)
}