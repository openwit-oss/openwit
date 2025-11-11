use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{info, error};
use anyhow::Result;

use openwit_inter_node::{ArrowFlightService, ArrowFlightBatch, NodeAddressCache};

/// Handles Arrow Flight data reception for the storage node
pub struct StorageArrowFlightReceiver {
    node_id: String,
    bind_address: String,
    service: ArrowFlightService,
    _batch_sender: mpsc::Sender<ArrowFlightBatch>,
}

impl StorageArrowFlightReceiver {
    pub fn new(
        node_id: String,
        bind_address: String,
        batch_sender: mpsc::Sender<ArrowFlightBatch>,
    ) -> Self {
        // Create address cache for this storage node
        let address_cache = Arc::new(NodeAddressCache::new(
            node_id.clone(),
            "storage".to_string(),
        ));
        
        // Create Arrow Flight service
        let (service, service_receiver) = ArrowFlightService::new(
            node_id.clone(),
            "storage".to_string(),
            address_cache,
        );
        
        // Forward batches from service to storage processor
        let batch_sender_clone = batch_sender.clone();
        tokio::spawn(async move {
            let mut receiver = service_receiver;
            while let Some(batch) = receiver.recv().await {
                if let Err(e) = batch_sender_clone.send(batch).await {
                    error!("Failed to forward batch to storage processor: {}", e);
                }
            }
        });
        
        Self {
            node_id,
            bind_address,
            service,
            _batch_sender: batch_sender,
        }
    }
    
    /// Start the Arrow Flight server
    pub async fn start(self) -> Result<()> {
        info!("Starting Arrow Flight receiver for {} on {}", self.node_id, self.bind_address);
        
        // Extract port from bind address (format: "0.0.0.0:9401")
        let port = self.bind_address
            .rsplit(':')
            .next()
            .and_then(|p| p.parse::<u16>().ok())
            .ok_or_else(|| anyhow::anyhow!("Invalid bind address format: {}", self.bind_address))?;
        
        self.service
            .start(port)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to start Arrow Flight server: {}", e))?;
            
        Ok(())
    }
}