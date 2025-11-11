use std::sync::Arc;
use std::time::{Duration, Instant};
use anyhow::Result;
use tokio::sync::mpsc;
use tokio::time::interval;
use tracing::{info, error, debug};
use chrono::{DateTime, Utc};
use uuid::Uuid;

use openwit_ingestion::types::IngestedMessage;
use openwit_storage::{
    config::StorageConfig,
};

// Compatibility stubs for ParquetWriter
#[derive(Debug)]
pub struct ParquetMetadata {
    pub num_rows: u64,
}

#[derive(Debug)]
pub struct ParquetWriter {
    pub current_batch: Vec<Vec<u8>>,
    pub current_size_bytes: usize,
}

impl ParquetWriter {
    pub fn new(_config: openwit_storage::config::ParquetConfig) -> Self {
        Self {
            current_batch: Vec::new(),
            current_size_bytes: 0,
        }
    }
    
    pub fn add_messages<T>(&mut self, messages: Vec<T>) {
        self.current_size_bytes += messages.len() * 1000;
        self.current_batch.clear();
    }
    
    pub fn should_flush(&self) -> bool {
        self.current_size_bytes > 200 * 1024 * 1024
    }
    
    pub async fn write_to_file(&mut self, _path: &std::path::Path) -> anyhow::Result<ParquetMetadata> {
        self.current_batch.clear();
        self.current_size_bytes = 0;
        Ok(ParquetMetadata { num_rows: 100 })
    }
}
use opendal::{Operator};
use openwit_metastore::{MetaStore, SegmentMetadata, SegmentStatus};

/// Messages that the StorageActor can receive
#[derive(Debug)]
pub enum StorageMessage {
    /// Store a batch of messages
    StoreBatch(Vec<IngestedMessage>),
    /// Flush current buffer to storage
    Flush,
    /// Get storage statistics
    GetStats(mpsc::Sender<StorageStats>),
    /// Shutdown the actor
    Shutdown,
}

/// Storage statistics
#[derive(Debug, Default, Clone)]
pub struct StorageStats {
    pub messages_processed: u64,
    pub bytes_processed: u64,
    pub files_written: u64,
    pub files_uploaded: u64,
    pub last_flush: Option<DateTime<Utc>>,
    pub current_buffer_size: usize,
    pub current_buffer_bytes: usize,
}

/// Storage actor that handles writing data to Parquet files
pub struct StorageActor {
    /// Actor configuration
    config: StorageConfig,
    /// Parquet writer for columnar storage
    parquet_writer: ParquetWriter,
    /// Optional cloud storage operator
    cloud_operator: Option<Operator>,
    /// Metastore for segment registration
    metastore: Arc<dyn MetaStore>,
    /// Channel to send segment IDs to indexer
    segment_tx: mpsc::Sender<String>,
    /// Storage statistics
    stats: StorageStats,
    /// Last flush time
    last_flush: Instant,
    /// Storage data directory
    storage_path: std::path::PathBuf,
}

impl StorageActor {
    /// Create a new storage actor
    pub fn new(
        config: StorageConfig,
        metastore: Arc<dyn MetaStore>,
        segment_tx: mpsc::Sender<String>,
    ) -> Result<Self> {
        let parquet_config = config.parquet.clone();

        let storage_path = std::path::PathBuf::from(&config.local_storage_path);
        
        Ok(Self {
            config,
            parquet_writer: ParquetWriter::new(parquet_config),
            cloud_operator: None,
            metastore,
            segment_tx,
            stats: Default::default(),
            last_flush: Instant::now(),
            storage_path,
        })
    }

    /// Set the cloud storage operator
    pub fn with_cloud_operator(mut self, cloud_operator: Operator) -> Self {
        self.cloud_operator = Some(cloud_operator);
        self
    }

    /// Run the storage actor
    pub async fn run(
        mut self,
        mut rx: mpsc::Receiver<StorageMessage>,
    ) -> Result<()> {
        info!("🗄️ Storage actor starting");
        info!("  Data directory: {}", self.storage_path.display());
        info!("  Flush interval: 30 seconds"); // Fixed flush interval
        info!("  Target file size: {} MB", 
            self.config.parquet.target_file_size_bytes / 1_048_576);

        // Ensure storage directory exists
        tokio::fs::create_dir_all(&self.storage_path).await?;

        // Spawn flush timer
        let flush_interval = Duration::from_secs(30); // Fixed flush interval
        let (flush_tx, mut flush_rx) = mpsc::channel(1);
        
        tokio::spawn(async move {
            let mut ticker = interval(flush_interval);
            loop {
                ticker.tick().await;
                if flush_tx.send(()).await.is_err() {
                    break;
                }
            }
        });

        loop {
            tokio::select! {
                Some(msg) = rx.recv() => {
                    match msg {
                        StorageMessage::StoreBatch(messages) => {
                            self.handle_store_batch(messages).await?;
                        }
                        StorageMessage::Flush => {
                            self.handle_flush().await?;
                        }
                        StorageMessage::GetStats(tx) => {
                            let _ = tx.send(self.stats.clone()).await;
                        }
                        StorageMessage::Shutdown => {
                            info!("Storage actor shutting down");
                            // Flush any remaining data
                            if !self.parquet_writer.current_batch.is_empty() {
                                self.handle_flush().await?;
                            }
                            break;
                        }
                    }
                }
                Some(_) = flush_rx.recv() => {
                    // Time-based flush
                    if !self.parquet_writer.current_batch.is_empty() 
                        && self.last_flush.elapsed() >= flush_interval {
                        info!("⏰ Time-based flush triggered");
                        self.handle_flush().await?;
                    }
                }
            }
        }

        info!("Storage actor stopped");
        Ok(())
    }

    /// Handle storing a batch of messages
    async fn handle_store_batch(&mut self, messages: Vec<IngestedMessage>) -> Result<()> {
        let batch_size = messages.len();
        let batch_bytes: usize = messages.iter().map(|m| m.size_bytes).sum();
        
        debug!("Received batch: {} messages, {} bytes", batch_size, batch_bytes);
        
        // Update stats
        self.stats.messages_processed += batch_size as u64;
        self.stats.bytes_processed += batch_bytes as u64;
        
        // Add to parquet writer
        self.parquet_writer.add_messages(messages);
        
        // Update buffer stats
        self.stats.current_buffer_size = self.parquet_writer.current_batch.len();
        self.stats.current_buffer_bytes = self.parquet_writer.current_size_bytes;
        
        // Check if we should flush
        if self.parquet_writer.should_flush() {
            info!("Buffer full, triggering flush");
            self.handle_flush().await?;
        }
        
        Ok(())
    }

    /// Handle flushing current buffer to storage
    async fn handle_flush(&mut self) -> Result<()> {
        if self.parquet_writer.current_batch.is_empty() {
            return Ok(());
        }

        let start = Instant::now();
        let message_count = self.parquet_writer.current_batch.len();
        let size_bytes = self.parquet_writer.current_size_bytes;
        
        info!("💾 Flushing {} messages ({} MB) to storage", 
            message_count, size_bytes / 1_048_576);

        // Generate filename
        let timestamp = Utc::now();
        let filename = format!(
            "data_{}_{}.parquet",
            timestamp.format("%Y%m%d_%H%M%S"),
            Uuid::new_v4()
        );
        let local_path = self.storage_path.join(&filename);

        // Write parquet file
        match self.parquet_writer.write_to_file(&local_path).await {
            Ok(metadata) => {
                info!("✅ Parquet file written: {} ({} rows)", 
                    local_path.display(), metadata.num_rows);
                
                // Update stats
                self.stats.files_written += 1;
                self.stats.last_flush = Some(timestamp);
                
                // Calculate segment metadata
                let segment_id = Uuid::new_v4().to_string();
                let partition_id = timestamp.format("%Y/%m/%d/%H").to_string();
                
                let file_size = tokio::fs::metadata(&local_path).await?.len();
                
                // Determine timestamps from batch (approximation)
                let min_timestamp = timestamp - chrono::Duration::seconds(30);
                let max_timestamp = timestamp;
                
                // Create segment metadata
                let mut segment_metadata = SegmentMetadata {
                    segment_id: segment_id.clone(),
                    partition_id,
                    file_path: local_path.to_string_lossy().to_string(),
                    index_path: None,
                    row_count: metadata.num_rows as u64,
                    compressed_bytes: file_size,
                    uncompressed_bytes: size_bytes as u64,
                    min_timestamp,
                    max_timestamp,
                    storage_status: SegmentStatus::Pending,
                    index_status: SegmentStatus::Pending,
                    created_at: timestamp,
                    updated_at: timestamp,
                    schema_version: 1,
                    is_cold: false,
                    is_archived: false,
                    storage_class: None,
                    transitioned_at: None,
                    archived_at: None,
                    restore_status: None,
                };

                // Upload to cloud storage if configured
                if let Some(cloud_operator) = &self.cloud_operator {
                    match self.upload_to_cloud_storage(
                        cloud_operator, 
                        &local_path, 
                        &filename
                    ).await {
                        Ok(object_path) => {
                            segment_metadata.file_path = object_path;
                            segment_metadata.storage_class = Some("cloud".to_string());
                            self.stats.files_uploaded += 1;
                            
                            // Delete local file after successful upload
                            tokio::fs::remove_file(&local_path).await.ok();
                        }
                        Err(e) => {
                            error!("Failed to upload to cloud storage: {:?}", e);
                            // Continue with local storage
                        }
                    }
                }

                // Mark storage as successful
                segment_metadata.storage_status = SegmentStatus::Stored;
                segment_metadata.updated_at = Utc::now();

                // Register in metastore
                match self.metastore.create_segment(segment_metadata).await {
                    Ok(_) => {
                        info!("📝 Registered segment {} in metastore", segment_id);
                        
                        // Send to indexer
                        if let Err(e) = self.segment_tx.send(segment_id.clone()).await {
                            error!("Failed to send segment to indexer: {:?}", e);
                        }
                    }
                    Err(e) => {
                        error!("Failed to register segment in metastore: {:?}", e);
                        // Mark as failed
                        let _ = self.metastore.mark_segment_storage_failed(
                            &segment_id, 
                            &e.to_string()
                        ).await;
                    }
                }

                let elapsed = start.elapsed();
                info!("✅ Flush completed in {:?}", elapsed);

                // Reset flush timer
                self.last_flush = Instant::now();
                
                // Clear buffer stats
                self.stats.current_buffer_size = 0;
                self.stats.current_buffer_bytes = 0;

                Ok(())
            }
            Err(e) => {
                error!("Failed to write parquet file: {:?}", e);
                // Clear batch to recover
                self.parquet_writer.current_batch.clear();
                self.parquet_writer.current_size_bytes = 0;
                Err(e)
            }
        }
    }

    /// Upload file to cloud storage
    async fn upload_to_cloud_storage(
        &self,
        cloud_operator: &Operator,
        local_path: &std::path::Path,
        filename: &str,
    ) -> Result<String> {
        info!("☁️ Uploading to cloud storage: {}", filename);
        
        let data = tokio::fs::read(local_path).await?;
        cloud_operator.write(filename, data).await?;
        
        // Return cloud storage path
        Ok(format!("cloud://{}", filename))
    }
}

/// Spawn a storage actor
pub fn spawn_storage_actor(
    config: StorageConfig,
    metastore: Arc<dyn MetaStore>,
    segment_tx: mpsc::Sender<String>,
    cloud_operator: Option<Operator>,
) -> mpsc::Sender<StorageMessage> {
    let (tx, rx) = mpsc::channel(1000);
    
    let mut actor = StorageActor::new(config, metastore, segment_tx)
        .expect("Failed to create storage actor");
    
    if let Some(operator) = cloud_operator {
        actor = actor.with_cloud_operator(operator);
    }
    
    tokio::spawn(async move {
        if let Err(e) = actor.run(rx).await {
            error!("Storage actor failed: {:?}", e);
        }
    });
    
    tx
}