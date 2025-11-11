use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, RwLock};
use tokio::time::{interval};
use tracing::{info, warn, error, debug};
use anyhow::{Result};

use openwit_config::UnifiedConfig;
use openwit_ingestion::{
    IngestedMessage, IngestionConfig,
    BatchingBuffer,
};
// IngestionMetrics removed - monitoring not required at this stage

/// Messages that the ingestion actor can receive
#[derive(Debug, Clone)]
pub enum IngestionActorMessage {
    /// New messages from proxy
    IngestBatch(Vec<IngestedMessage>),
    
    /// Update configuration
    UpdateConfig(IngestionConfig),
    
    /// Force flush buffers
    Flush,
    
    /// Get current stats
    GetStats,
    
    /// Pause ingestion
    Pause,
    
    /// Resume ingestion
    Resume,
    
    /// Shutdown actor
    Shutdown,
}

/// Response from ingestion actor
#[derive(Debug, Clone)]
pub enum IngestionActorResponse {
    /// Acknowledgment with batch ID
    BatchAck { batch_id: String, success: bool },
    
    /// Current statistics
    Stats(IngestionStats),
    
    /// Operation acknowledged
    Ok,
}

#[derive(Debug, Clone)]
pub struct IngestionStats {
    pub messages_received: u64,
    pub messages_processed: u64,
    pub batches_written: u64,
    // WAL stats removed
    pub current_buffer_size: usize,
    pub memory_usage_bytes: usize,
    pub is_paused: bool,
}

/// Actor for handling ingestion within a node
pub struct IngestionActor {
    node_id: String,
    config: Arc<RwLock<IngestionConfig>>,
    
    // Channels
    receiver: mpsc::Receiver<IngestionActorMessage>,
    response_tx: mpsc::Sender<IngestionActorResponse>,
    storage_tx: mpsc::Sender<Vec<IngestedMessage>>,
    
    // Processing components
    buffer: Arc<BatchingBuffer>,
    // WAL removed - messages flow directly to storage
    
    // Control plane integration
    
    // metrics removed - monitoring not required at this stage
    
    // State
    is_paused: Arc<RwLock<bool>>,
    stats: Arc<RwLock<IngestionStats>>,
}

impl IngestionActor {
    pub async fn new(
        node_id: String,
        config: UnifiedConfig,
        receiver: mpsc::Receiver<IngestionActorMessage>,
        response_tx: mpsc::Sender<IngestionActorResponse>,
        storage_tx: mpsc::Sender<Vec<IngestedMessage>>,
        ) -> Result<Self> {
        let ingestion_config = IngestionConfig::from_unified(&config);
        let ingestion_config_arc = Arc::new(RwLock::new(ingestion_config.clone()));
        
        // WAL removed - messages flow directly to storage
        
        // Create buffer
        let (batch_tx, _batch_rx) = mpsc::channel(100);
        let buffer = Arc::new(BatchingBuffer::new(
            node_id.clone(),
            ingestion_config_arc.clone(),
            batch_tx,
        ));
        
        // metrics removed - monitoring not required at this stage
        
        // TODO: Update to use new pipeline architecture
        // let (pipeline, pipeline_tx) = MultiThreadedPipeline::new(
        //     node_id.clone(),
        //     ingestion_config.clone(),
        //     storage_tx.clone(),
        //     Some(4), // Number of worker threads
        // ).await?;
        // let pipeline = Arc::new(pipeline);
        
        let stats = Arc::new(RwLock::new(IngestionStats {
            messages_received: 0,
            messages_processed: 0,
            batches_written: 0,
            // WAL stats removed
            current_buffer_size: 0,
            memory_usage_bytes: 0,
            is_paused: false,
        }));
        
        Ok(Self {
            node_id,
            config: ingestion_config_arc,
            receiver,
            response_tx,
            storage_tx,
            buffer,
            // pipeline removed - using unified buffer approach
            // metrics removed - monitoring not required at this stage
            is_paused: Arc::new(RwLock::new(false)),
            stats,
        })
    }
    
    /// Run the actor
    pub async fn run(mut self) {
        info!("Starting ingestion actor for node: {}", self.node_id);
        
        // Start background tasks
        self.start_flush_timer();
        self.start_metrics_reporter();
        
        while let Some(msg) = self.receiver.recv().await {
            match msg {
                IngestionActorMessage::IngestBatch(messages) => {
                    if *self.is_paused.read().await {
                        warn!("Ingestion paused, dropping {} messages", messages.len());
                        continue;
                    }
                    
                    let batch_id = uuid::Uuid::new_v4().to_string();
                    let success = self.handle_batch(batch_id.clone(), messages).await;
                    
                    let _ = self.response_tx.send(IngestionActorResponse::BatchAck {
                        batch_id,
                        success,
                    }).await;
                }
                
                IngestionActorMessage::UpdateConfig(new_config) => {
                    info!("Updating ingestion configuration");
                    *self.config.write().await = new_config;
                    let _ = self.response_tx.send(IngestionActorResponse::Ok).await;
                }
                
                IngestionActorMessage::Flush => {
                    debug!("Force flush requested");
                    let _ = self.flush_buffers().await;
                    let _ = self.response_tx.send(IngestionActorResponse::Ok).await;
                }
                
                IngestionActorMessage::GetStats => {
                    let stats = self.stats.read().await.clone();
                    let _ = self.response_tx.send(IngestionActorResponse::Stats(stats)).await;
                }
                
                IngestionActorMessage::Pause => {
                    info!("Pausing ingestion");
                    *self.is_paused.write().await = true;
                    self.stats.write().await.is_paused = true;
                    let _ = self.response_tx.send(IngestionActorResponse::Ok).await;
                }
                
                IngestionActorMessage::Resume => {
                    info!("Resuming ingestion");
                    *self.is_paused.write().await = false;
                    self.stats.write().await.is_paused = false;
                    let _ = self.response_tx.send(IngestionActorResponse::Ok).await;
                }
                
                IngestionActorMessage::Shutdown => {
                    info!("Shutting down ingestion actor");
                    let _ = self.shutdown().await;
                    break;
                }
            }
        }
        
        info!("Ingestion actor stopped");
    }
    
    /// Handle a batch of messages
    async fn handle_batch(&self, batch_id: String, messages: Vec<IngestedMessage>) -> bool {
        let _start = Instant::now();
        let message_count = messages.len();
        let total_bytes: usize = messages.iter().map(|m| m.size_bytes).sum();
        
        debug!("Processing batch {} with {} messages ({} bytes)", 
               batch_id, message_count, total_bytes);
        
        // Metrics removed - monitoring not required at this stage
        
        // Update stats
        {
            let mut stats = self.stats.write().await;
            stats.messages_received += message_count as u64;
            stats.memory_usage_bytes += total_bytes;
        }
        
        // Add to buffer
        for message in messages {
            if let Err(e) = self.buffer.add_message(message).await {
                error!("Failed to add message to buffer: {}", e);
                return false;
            }
        }
        
        // Check if we should flush based on buffer stats
        let stats = self.buffer.get_stats().await;
        let config = self.config.read().await;
        if stats.pending_messages >= config.batch_size || 
           stats.used_memory >= config.max_buffer_memory {
            if let Err(e) = self.flush_buffers().await {
                error!("Failed to flush buffers: {}", e);
                return false;
            }
        }
        
        
        // Record batch processing time - metrics doesn't have this field yet
        true
    }
    
    /// Flush buffers to WAL and storage
    async fn flush_buffers(&self) -> Result<()> {
        let batch = match self.buffer.flush_batch().await {
            Some(b) => b,
            None => return Ok(()), // No batch to flush
        };
        if batch.messages.is_empty() {
            return Ok(());
        }
        
        let batch_id = batch.id.clone();
        let message_count = batch.messages.len();
        
        // Send directly to storage (no WAL)
        if let Err(e) = self.storage_tx.send(batch.messages).await {
            error!("Failed to send batch to storage: {}", e);
            return Err(anyhow::anyhow!("Storage channel closed"));
        }
        
        // Update stats
        {
            let mut stats = self.stats.write().await;
            stats.messages_processed += message_count as u64;
            stats.batches_written += 1;
            let buffer_stats = self.buffer.get_stats().await;
            stats.current_buffer_size = buffer_stats.pending_messages;
        }
        
        
        // WAL metrics removed
        info!("Flushed batch {} with {} messages", batch_id, message_count);
        
        Ok(())
    }
    
    /// Start periodic flush timer
    fn start_flush_timer(&self) {
        let _buffer = self.buffer.clone();
        let flush_interval = Duration::from_secs(5); // Default 5 seconds
        
        tokio::spawn(async move {
            let mut timer = interval(flush_interval);
            
            loop {
                timer.tick().await;
                
                // TODO: Implement should_flush check
                // if buffer.should_flush().await {
                //     debug!("Periodic flush triggered");
                //     // The actual flush will be handled by the main loop
                // }
            }
        });
    }
    
    /// Start metrics reporter
    fn start_metrics_reporter(&self) {
        let stats = self.stats.clone();
        let node_id = self.node_id.clone();
        
        tokio::spawn(async move {
            let mut timer = interval(Duration::from_secs(30));
            
            loop {
                timer.tick().await;
                
                let current_stats = stats.read().await;
                info!(
                    "Ingestion stats for {}: received={}, processed={}, batches={}, buffer_size={}",
                    node_id,
                    current_stats.messages_received,
                    current_stats.messages_processed,
                    current_stats.batches_written,
                    current_stats.current_buffer_size,
                );
            }
        });
    }
    
    /// Graceful shutdown
    async fn shutdown(&self) -> Result<()> {
        info!("Shutting down ingestion actor gracefully");
        
        // Flush any remaining messages
        self.flush_buffers().await?;
        
        // No WAL to flush - messages go directly to storage
        
        Ok(())
    }
}

/// Spawn an ingestion actor
pub fn spawn_ingestion_actor(
    node_id: String,
    config: UnifiedConfig,
    storage_tx: mpsc::Sender<Vec<IngestedMessage>>,
) -> (mpsc::Sender<IngestionActorMessage>, mpsc::Receiver<IngestionActorResponse>) {
    let (tx, rx) = mpsc::channel(1000);
    let (response_tx, response_rx) = mpsc::channel(100);
    
    tokio::spawn(async move {
        match IngestionActor::new(
            node_id,
            config,
            rx,
            response_tx,
            storage_tx,
        ).await {
            Ok(actor) => actor.run().await,
            Err(e) => error!("Failed to create ingestion actor: {}", e),
        }
    });
    
    (tx, response_rx)
}