use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{info, warn, error};
use anyhow::Result;
use std::collections::HashMap;

use openwit_inter_node::{ArrowFlightService, ArrowFlightBatch, NodeAddressCache};
use openwit_config::UnifiedConfig;

/// Handles Arrow Flight data reception for the indexer node
#[allow(unused)]
pub struct IndexerArrowFlightReceiver {
    node_id: String,
    config: UnifiedConfig,
    service: ArrowFlightService,
    batch_receiver: mpsc::Receiver<ArrowFlightBatch>,
}

impl IndexerArrowFlightReceiver {
    pub async fn new(
        node_id: String,
        config: UnifiedConfig,
    ) -> Result<(Self, mpsc::Sender<ArrowFlightBatch>)> {
        // Create address cache for this indexer node
        let address_cache = Arc::new(NodeAddressCache::new(
            node_id.clone(),
            "indexer".to_string(),
        ));
        
        // Create Arrow Flight service for indexer (port 8090)
        let (service, batch_receiver) = ArrowFlightService::new(
            node_id.clone(),
            "indexer".to_string(),
            address_cache,
        );
        
        // Create a sender to return
        let (batch_sender, _receiver) = mpsc::channel(1000);
        
        Ok((
            Self {
                node_id,
                config,
                service,
                batch_receiver,
            },
            batch_sender,
        ))
    }
    
    /// Start the Arrow Flight server and batch processing
    #[allow(unused_mut)]
    pub async fn start(mut self) -> Result<()> {
        let port = 8090u16; // Indexer port
        
        // Split self to get ownership of service
        let Self { service, mut batch_receiver, .. } = self;
        
        // Start Arrow Flight server
        tokio::spawn(async move {
            if let Err(e) = service.start(port).await {
                error!("Arrow Flight server failed: {}", e);
            }
        });
        
        info!("✅ Indexer Arrow Flight receiver started on port {}", port);
        
        // Process received batches
        while let Some(batch) = batch_receiver.recv().await {
            // For now, just log the batch - proper processing needs redesign
            info!(
                "📥 Indexer received batch {} from {} ({} records)",
                batch.batch_id, batch.source_node, batch.record_batch.num_rows()
            );
            // TODO: Implement proper batch processing
        }
        
        Ok(())
    }
    
    /// Process a received batch
    #[allow(dead_code)]
    async fn process_batch(&self, batch: ArrowFlightBatch) -> Result<()> {
        info!(
            "📥 Indexer received batch {} from {} ({} records)",
            batch.batch_id, batch.source_node, batch.record_batch.num_rows()
        );
        
        // Extract metadata
        let _message_count = batch.metadata.get("message_count")
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(0);
            
        let _timestamp = batch.metadata.get("timestamp")
            .cloned()
            .unwrap_or_else(|| chrono::Utc::now().to_rfc3339());
            
        // Convert Arrow batch back to indexable format
        let schema = batch.record_batch.schema();
        let columns = batch.record_batch.columns();
        
        // Get column indices
        let mut column_map = HashMap::new();
        for (idx, field) in schema.fields().iter().enumerate() {
            column_map.insert(field.name().as_str(), idx);
        }
        
        // Process each record
        let num_rows = batch.record_batch.num_rows();
        for row_idx in 0..num_rows {
            if let Err(e) = self.index_record(&batch.batch_id, row_idx, &columns, &column_map).await {
                warn!("Failed to index record {} in batch {}: {}", row_idx, batch.batch_id, e);
            }
        }
        
        info!(
            "✅ Indexed batch {} with {} records",
            batch.batch_id, num_rows
        );
        
        // Report completion to control plane
        self.report_batch_indexed(&batch.batch_id, num_rows).await?;
        
        Ok(())
    }
    
    /// Index a single record
    #[allow(dead_code)]
    async fn index_record(
        &self,
        batch_id: &str,
        row_idx: usize,
        columns: &[Arc<dyn arrow::array::Array>],
        column_map: &HashMap<&str, usize>,
    ) -> Result<()> {
        use arrow::array::{StringArray};
        
        // Extract fields
        let message_id = if let Some(&idx) = column_map.get("message_id") {
            columns[idx]
                .as_any()
                .downcast_ref::<StringArray>()
                .and_then(|arr| arr.value(row_idx).parse::<String>().ok())
                .unwrap_or_else(|| format!("unknown-{}", row_idx))
        } else {
            format!("{}-{}", batch_id, row_idx)
        };
        
        let payload_type = if let Some(&idx) = column_map.get("payload_type") {
            columns[idx]
                .as_any()
                .downcast_ref::<StringArray>()
                .map(|arr| arr.value(row_idx))
                .unwrap_or("unknown")
        } else {
            "unknown"
        };
        
        let payload_data = if let Some(&idx) = column_map.get("payload_data") {
            columns[idx]
                .as_any()
                .downcast_ref::<StringArray>()
                .and_then(|arr| {
                    // Decode base64 payload
                    use base64::Engine;
                    base64::engine::general_purpose::STANDARD.decode(arr.value(row_idx)).ok()
                })
                .unwrap_or_default()
        } else {
            vec![]
        };
        
        // TODO: Integrate with actual indexing engine
        // For now, just log
        info!(
            "Would index: message_id={}, type={}, size={} bytes",
            message_id, payload_type, payload_data.len()
        );
        
        Ok(())
    }
    
    /// Report batch indexing completion to control plane
    #[allow(dead_code)]
    async fn report_batch_indexed(&self, batch_id: &str, record_count: usize) -> Result<()> {
        // TODO: Implement actual gRPC call to control plane
        info!(
            "Would report to control: batch {} indexed with {} records",
            batch_id, record_count
        );
        Ok(())
    }
}