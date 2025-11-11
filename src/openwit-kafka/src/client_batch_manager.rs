use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use anyhow::{Result, Context};
use tokio::sync::{RwLock, mpsc};
use tokio::time::interval;
use uuid::Uuid;
use tracing::{info, error, debug};

use crate::batch_tracker::BatchTracker;
use crate::types::KafkaMessage;
use crate::client_extractor::ClientExtractor;
use openwit_config::unified::ingestion::BatchConfig;

/// Manages client-specific message batching
pub struct ClientBatchManager {
    /// HashMap of client buffers indexed by client_id
    client_buffers: Arc<RwLock<HashMap<String, ClientBuffer>>>,
    /// Batch configuration
    batch_config: BatchConfig,
    /// Topic index configuration for dynamic client extraction
    topic_index_config: Option<crate::types::TopicIndexConfig>,
    /// Batch tracker for PostgreSQL
    batch_tracker: Arc<BatchTracker>,
    /// Channel to send completed batches
    batch_sender: mpsc::Sender<CompletedBatch>,
    /// Channel to receive completed batches
    batch_receiver: Arc<RwLock<mpsc::Receiver<CompletedBatch>>>,
}

/// Individual client buffer
#[derive(Debug)]
pub struct ClientBuffer {
    pub client_id: String,
    pub messages: Vec<KafkaMessage>,
    pub total_bytes: usize,
    pub created_at: Instant,
    pub last_updated: Instant,
    pub first_offset: Option<i64>,
    pub last_offset: Option<i64>,
    pub topics: Vec<String>,
}

/// Represents a completed batch ready for processing
#[derive(Debug, Clone)]
pub struct CompletedBatch {
    pub batch_id: Uuid,
    pub client_id: String,
    pub messages: Vec<KafkaMessage>,
    pub total_bytes: usize,
    pub message_count: usize,
    pub created_at: Instant,
    pub first_offset: Option<i64>,
    pub last_offset: Option<i64>,
    pub topics: Vec<String>,
}

impl ClientBatchManager {
    /// Create a new client batch manager
    pub async fn new(
        batch_config: BatchConfig,
        topic_index_config: Option<crate::types::TopicIndexConfig>,
        batch_tracker: Arc<BatchTracker>,
    ) -> Result<Self> {
        let (batch_sender, batch_receiver) = mpsc::channel(100);
        
        let manager = Self {
            client_buffers: Arc::new(RwLock::new(HashMap::new())),
            batch_config,
            topic_index_config,
            batch_tracker,
            batch_sender,
            batch_receiver: Arc::new(RwLock::new(batch_receiver)),
        };
        
        Ok(manager)
    }
    
    /// Start the background monitoring task
    pub async fn start_monitoring(&self) {
        let buffers = self.client_buffers.clone();
        let config = self.batch_config.clone();
        let sender = self.batch_sender.clone();
        let tracker = self.batch_tracker.clone();
        
        tokio::spawn(async move {
            let mut check_interval = interval(Duration::from_secs(1));
            
            loop {
                check_interval.tick().await;
                
                // Check for batches that exceeded time limit
                let mut buffers_to_flush = Vec::new();
                {
                    let buffers_read = buffers.read().await;
                    let now = Instant::now();
                    
                    for (client_id, buffer) in buffers_read.iter() {
                        if now.duration_since(buffer.created_at) >= Duration::from_secs(config.max_age_seconds) {
                            buffers_to_flush.push(client_id.clone());
                        }
                    }
                }
                
                // Flush aged batches
                for client_id in buffers_to_flush {
                    if let Err(e) = Self::flush_client_internal(
                        &buffers, 
                        &client_id, 
                        &sender,
                        &tracker
                    ).await {
                        error!("Failed to flush aged batch for client {}: {}", client_id, e);
                    }
                }
            }
        });
        
        info!("Started client batch monitoring task");
    }
    
    /// Add a message to the appropriate client buffer
    pub async fn add_message(&self, message: KafkaMessage) -> Result<Option<Uuid>> {
        // Extract client_id from message headers
        let client_id = message.headers.get("client_name")
            .ok_or_else(|| anyhow::anyhow!("Message missing client_name header"))?
            .clone();
        
        // Check if this topic should auto-create client based on configuration
        let topic_config = self.topic_index_config.as_ref();
        if ClientExtractor::should_auto_create_client(&message.topic, topic_config) {
            debug!("Auto-creating client buffer for topic: {}", message.topic);
            ClientExtractor::log_client_discovery(&message.topic, &client_id, topic_config);
        }
        
        let mut buffers = self.client_buffers.write().await;
        
        // Get or create client buffer
        let buffer = buffers.entry(client_id.clone()).or_insert_with(|| {
            debug!("Creating new buffer for client: {}", client_id);
            ClientBuffer {
                client_id: client_id.clone(),
                messages: Vec::with_capacity(1000),
                total_bytes: 0,
                created_at: Instant::now(),
                last_updated: Instant::now(),
                first_offset: None,
                last_offset: None,
                topics: Vec::new(),
            }
        });
        
        // Update buffer metadata
        buffer.total_bytes += message.size_bytes;
        buffer.last_updated = Instant::now();
        
        // Track offsets
        if buffer.first_offset.is_none() {
            buffer.first_offset = Some(message.offset);
        }
        buffer.last_offset = Some(message.offset);
        
        // Track unique topics
        if !buffer.topics.contains(&message.topic) {
            buffer.topics.push(message.topic.clone());
        }
        
        buffer.messages.push(message);
        
        // Check if batch is ready (size or message count)
        if buffer.total_bytes >= self.batch_config.max_size_bytes || 
           buffer.messages.len() >= self.batch_config.max_messages {
            
            debug!(
                "Batch ready for client {}: {} messages, {} bytes",
                client_id, buffer.messages.len(), buffer.total_bytes
            );
            
            // Extract the buffer to flush it
            if let Some(ready_buffer) = buffers.remove(&client_id) {
                drop(buffers); // Release write lock before async operations
                
                // Create and send completed batch
                let batch_id = self.create_and_send_batch(ready_buffer).await?;
                return Ok(Some(batch_id));
            }
        }
        
        Ok(None)
    }
    
    /// Create a completed batch and send it
    async fn create_and_send_batch(&self, buffer: ClientBuffer) -> Result<Uuid> {
        let client_id = buffer.client_id.clone();
        let message_count = buffer.messages.len();
        
        // Create batch record in PostgreSQL
        let batch_id = self.batch_tracker.create_batch(
            client_id.clone(),
            buffer.total_bytes as i32,
            message_count as i32,
            buffer.first_offset,
            buffer.last_offset,
            buffer.topics.clone(),
        ).await?;
        
        let completed_batch = CompletedBatch {
            batch_id,
            client_id: buffer.client_id,
            messages: buffer.messages,
            total_bytes: buffer.total_bytes,
            message_count,
            created_at: buffer.created_at,
            first_offset: buffer.first_offset,
            last_offset: buffer.last_offset,
            topics: buffer.topics,
        };
        
        // Send to processing channel
        self.batch_sender.send(completed_batch)
            .await
            .context("Failed to send completed batch")?;
        
        info!("Created batch {} for client {}", batch_id, client_id);
        Ok(batch_id)
    }
    
    /// Flush a specific client's buffer
    pub async fn flush_client(&self, client_id: &str) -> Result<Option<Uuid>> {
        Self::flush_client_internal(
            &self.client_buffers,
            client_id,
            &self.batch_sender,
            &self.batch_tracker,
        ).await
    }
    
    /// Internal flush implementation
    async fn flush_client_internal(
        buffers: &Arc<RwLock<HashMap<String, ClientBuffer>>>,
        client_id: &str,
        sender: &mpsc::Sender<CompletedBatch>,
        tracker: &Arc<BatchTracker>,
    ) -> Result<Option<Uuid>> {
        let mut buffers_write = buffers.write().await;
        
        if let Some(buffer) = buffers_write.remove(client_id) {
            if buffer.messages.is_empty() {
                return Ok(None);
            }
            
            debug!(
                "Flushing batch for client {}: {} messages, {} bytes",
                client_id, buffer.messages.len(), buffer.total_bytes
            );
            
            // Create batch record
            let batch_id = tracker.create_batch(
                buffer.client_id.clone(),
                buffer.total_bytes as i32,
                buffer.messages.len() as i32,
                buffer.first_offset,
                buffer.last_offset,
                buffer.topics.clone(),
            ).await?;
            
            let completed_batch = CompletedBatch {
                batch_id,
                client_id: buffer.client_id.clone(),
                message_count: buffer.messages.len(),
                total_bytes: buffer.total_bytes,
                created_at: buffer.created_at,
                first_offset: buffer.first_offset,
                last_offset: buffer.last_offset,
                topics: buffer.topics,
                messages: buffer.messages,
            };
            
            // Send to processing channel
            sender.send(completed_batch)
                .await
                .context("Failed to send flushed batch")?;
            
            info!("Flushed batch {} for client {}", batch_id, client_id);
            Ok(Some(batch_id))
        } else {
            Ok(None)
        }
    }
    
    /// Flush all client buffers
    pub async fn flush_all(&self) -> Result<Vec<Uuid>> {
        let client_ids: Vec<String> = {
            let buffers = self.client_buffers.read().await;
            buffers.keys().cloned().collect()
        };
        
        let mut batch_ids = Vec::new();
        
        for client_id in client_ids {
            if let Some(batch_id) = self.flush_client(&client_id).await? {
                batch_ids.push(batch_id);
            }
        }
        
        info!("Flushed {} batches", batch_ids.len());
        Ok(batch_ids)
    }
    
    /// Get the batch receiver channel
    pub fn get_batch_receiver(&self) -> Arc<RwLock<mpsc::Receiver<CompletedBatch>>> {
        self.batch_receiver.clone()
    }
    
    /// Get current buffer statistics
    pub async fn get_stats(&self) -> HashMap<String, BufferStats> {
        let buffers = self.client_buffers.read().await;
        let now = Instant::now();
        
        buffers.iter().map(|(client_id, buffer)| {
            let stats = BufferStats {
                message_count: buffer.messages.len(),
                total_bytes: buffer.total_bytes,
                age_seconds: now.duration_since(buffer.created_at).as_secs(),
                last_updated_seconds: now.duration_since(buffer.last_updated).as_secs(),
            };
            (client_id.clone(), stats)
        }).collect()
    }
    
    pub async fn has_client(&self, client_id: &str) -> bool {
        let buffers = self.client_buffers.read().await;
        buffers.contains_key(client_id)
    }
    
    /// Get or create a client buffer dynamically
    pub async fn ensure_client_exists(&self, client_id: &str) {
        let mut buffers = self.client_buffers.write().await;
        buffers.entry(client_id.to_string()).or_insert_with(|| {
            info!("Dynamically creating buffer for new client: {}", client_id);
            ClientBuffer {
                client_id: client_id.to_string(),
                messages: Vec::with_capacity(1000),
                total_bytes: 0,
                created_at: Instant::now(),
                last_updated: Instant::now(),
                first_offset: None,
                last_offset: None,
                topics: Vec::new(),
            }
        });
    }
    
    /// Shutdown the manager
    pub async fn shutdown(self) -> Result<()> {
        info!("Shutting down client batch manager");
        
        // Flush all remaining batches
        self.flush_all().await?;
        
        Ok(())
    }
}

/// Statistics for a client buffer
#[derive(Debug, Clone)]
pub struct BufferStats {
    pub message_count: usize,
    pub total_bytes: usize,
    pub age_seconds: u64,
    pub last_updated_seconds: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_client_batch_manager() {
        let config = BatchConfig {
            max_size_bytes: 1024,
            max_messages: 10,
            max_age_seconds: 30,
        };

    }
}