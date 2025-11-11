use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::info;
use anyhow::Result;

use openwit_ingestion::{
    IngestionPipeline,
    IngestionConfig,
    IngestedMessage,
    // start_kafka_source, // Removed: Only proxy consumes from Kafka
    ControlCommand,
};
use openwit_config::UnifiedConfig;

/// Simple ingestion actor that uses the openwit_ingestion crate
pub struct SimpleIngestActor {
    node_id: String,
    config: Arc<UnifiedConfig>,
    pipeline: Option<IngestionPipeline>,
    control_rx: Receiver<ControlCommand>,
    storage_tx: Sender<Vec<IngestedMessage>>,
}

impl SimpleIngestActor {
    pub fn new(
        node_id: String,
        config: Arc<UnifiedConfig>,
        control_rx: Receiver<ControlCommand>,
        storage_tx: Sender<Vec<IngestedMessage>>,
    ) -> Self {
        Self {
            node_id,
            config,
            pipeline: None,
            control_rx,
            storage_tx,
        }
    }

    pub async fn run(mut self) -> Result<()> {
        info!("Starting SimpleIngestActor for node {}", self.node_id);
        
        // Create ingestion config from OpenWitConfig
        let ingestion_config = self.create_ingestion_config();
        
        // Create ingestion pipeline
        let (pipeline, _ingest_tx) = IngestionPipeline::new(
            self.node_id.clone(),
            ingestion_config,
            self.storage_tx.clone(),
        ).await?;
        
        // Removed: Only proxy consumes from Kafka
        // if self.config.ingestion.kafka.brokers.is_some() {
        //     start_kafka_source(Arc::new(self.config.ingestion.kafka.clone()), ingest_tx.clone()).await?;
        //     info!("Started Kafka ingestion");
        // }
        
        // Store pipeline reference
        self.pipeline = Some(pipeline);
        
        // Take ownership of control_rx for the spawned task
        let _control_rx = self.control_rx;
        
        // Run the pipeline (this blocks until shutdown)
        if let Some(pipeline) = self.pipeline {
            pipeline.start().await?;
        }
        
        Ok(())
    }
    
    fn create_ingestion_config(&self) -> IngestionConfig {
        let _mem_cfg: Option<&()> = None; // TODO: Get from unified config
        let buffer_cfg = self.config.processing.as_ref().map(|p| &p.buffer);

        IngestionConfig {
            max_buffer_memory: buffer_cfg.map(|b| b.max_size_bytes as usize).unwrap_or(512 * 1024 * 1024), // 512MB default
            batch_size: buffer_cfg.map(|b| b.flush_size_messages as usize).unwrap_or(10000), // Use config value or 10K default
            batch_timeout_ms: buffer_cfg.map(|b| b.batch_timeout_ms).unwrap_or(30000), // 30 seconds default
            // WAL configuration removed - no longer needed
        }
    }
}
