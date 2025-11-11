use std::sync::Arc;
use tokio::sync::{RwLock, mpsc};
use anyhow::Result;
use bytes::Bytes;
use tracing::{info, debug};
use serde::{Serialize, Deserialize};

/// Lightweight buffer for proxy node
/// Only holds data temporarily while routing to ingestor nodes
#[allow(dead_code)]
pub struct ProxyBuffer {
    node_id: String,
    /// Temporary buffer for batching small messages
    buffer: Arc<RwLock<Vec<ProxyMessage>>>,
    /// Max messages before forcing flush
    max_batch_size: usize,
    /// Channel to notify router
    flush_tx: mpsc::Sender<Vec<ProxyMessage>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProxyMessage {
    pub id: String,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub source: MessageSource,
    pub data: Bytes,
    pub metadata: std::collections::HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MessageSource {
    Grpc { endpoint: String },
    Http { endpoint: String, method: String },
    Kafka { topic: String, partition: i32, offset: i64 },
}

impl ProxyBuffer {
    pub fn new(
        node_id: String,
        max_batch_size: usize,
        flush_tx: mpsc::Sender<Vec<ProxyMessage>>,
    ) -> Self {
        Self {
            node_id,
            buffer: Arc::new(RwLock::new(Vec::with_capacity(max_batch_size))),
            max_batch_size,
            flush_tx,
        }
    }
    
    /// Add message to buffer
    pub async fn add_message(&self, message: ProxyMessage) -> Result<()> {
        let mut buffer = self.buffer.write().await;
        buffer.push(message);
        
        // Check if we should flush
        if buffer.len() >= self.max_batch_size {
            let batch = std::mem::take(&mut *buffer);
            drop(buffer); // Release lock before sending
            
            debug!("Proxy buffer full, flushing {} messages", batch.len());
            self.flush_tx.send(batch).await?;
        }
        
        Ok(())
    }
    
    /// Add multiple messages
    pub async fn add_messages(&self, messages: Vec<ProxyMessage>) -> Result<()> {
        let mut buffer = self.buffer.write().await;
        let initial_len = buffer.len();
        buffer.extend(messages);
        
        // Check if we should flush
        if buffer.len() >= self.max_batch_size {
            let batch = std::mem::take(&mut *buffer);
            drop(buffer); // Release lock before sending
            
            debug!("Proxy buffer full after bulk add, flushing {} messages", batch.len());
            self.flush_tx.send(batch).await?;
        } else {
            debug!("Added {} messages to proxy buffer (now {} total)", 
                   buffer.len() - initial_len, buffer.len());
        }
        
        Ok(())
    }
    
    /// Force flush current buffer
    pub async fn flush(&self) -> Result<Vec<ProxyMessage>> {
        let mut buffer = self.buffer.write().await;
        let batch = std::mem::take(&mut *buffer);
        
        if !batch.is_empty() {
            info!("Force flushing {} messages from proxy buffer", batch.len());
            self.flush_tx.send(batch.clone()).await?;
        }
        
        Ok(batch)
    }
    
    /// Get current buffer size
    pub async fn size(&self) -> usize {
        self.buffer.read().await.len()
    }
    
    /// Create a periodic flusher
    pub fn start_periodic_flush(self: Arc<Self>, interval_ms: u64) {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(
                std::time::Duration::from_millis(interval_ms)
            );
            
            loop {
                interval.tick().await;
                
                let size = self.size().await;
                if size > 0 {
                    debug!("Periodic flush triggered, {} messages in buffer", size);
                    if let Err(e) = self.flush().await {
                        tracing::error!("Periodic flush failed: {}", e);
                    }
                }
            }
        });
    }
}

/// Metrics for proxy buffer
pub struct ProxyBufferMetrics {
    pub messages_received: u64,
    pub batches_sent: u64,
    pub bytes_processed: u64,
    pub avg_batch_size: f64,
}

impl ProxyBuffer {
    /// Get buffer metrics
    pub async fn get_metrics(&self) -> ProxyBufferMetrics {
        // In production, would track these properly
        ProxyBufferMetrics {
            messages_received: 0,
            batches_sent: 0,
            bytes_processed: 0,
            avg_batch_size: 0.0,
        }
    }
}