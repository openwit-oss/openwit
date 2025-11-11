use std::sync::Arc;
use tokio::sync::mpsc;
use anyhow::Result;
use tracing::{info, warn, error, debug};

use openwit_inter_node::ArrowFlightBatch;

use crate::{
    parquet_file_manager::ParquetFileManager,
    arrow_flight_receiver::StorageArrowFlightReceiver,
};

/// Storage processor that handles OTLP data storage and queries
pub struct StorageProcessor {
    node_id: String,
    data_dir: String,
    /// Parquet file manager with client-based organization
    parquet_file_manager: Arc<ParquetFileManager>,
    /// Batch channel
    batch_receiver: Arc<tokio::sync::Mutex<mpsc::Receiver<ArrowFlightBatch>>>,
}

impl StorageProcessor {
    pub async fn new(
        node_id: String,
        data_dir: String,
        arrow_flight_port: u16,
    ) -> Result<(Self, mpsc::Sender<ArrowFlightBatch>)> {
        Self::new_with_config(node_id, data_dir, arrow_flight_port, None).await
    }
    
    pub async fn new_with_config(
        node_id: String,
        data_dir: String,
        arrow_flight_port: u16,
        config: Option<&openwit_config::UnifiedConfig>,
    ) -> Result<(Self, mpsc::Sender<ArrowFlightBatch>)> {
        info!("Initializing OTLP Storage Processor for node {}", node_id);
        
        // Create cloud storage configs if available
        let azure_config = config.and_then(|c| crate::cloud_upload::create_azure_config(c));
        let s3_config = config.and_then(|c| crate::cloud_upload::create_s3_config(c));
        let delete_after_upload = config.map(|c| c.storage.local_retention.delete_after_upload).unwrap_or(false);
        
        // Log cloud storage configuration
        match (&azure_config, &s3_config) {
            (Some(_), Some(_)) => info!("[CLOUD] Both Azure and S3 storage configured"),
            (Some(_), None) => info!("[CLOUD] Azure storage configured"),
            (None, Some(_)) => info!("[CLOUD] S3 storage configured"), 
            (None, None) => info!("[CLOUD] No cloud storage configured - local only"),
        }
        
        
        // Get file rotation config from unified config or use defaults
        let file_size_bytes = config
            .map(|c| c.storage.file_rotation.file_size_mb as u64 * 1024 * 1024)
            .unwrap_or(200 * 1024 * 1024); // Default 200MB
            
        let rotation_duration_secs = config
            .map(|c| c.storage.file_rotation.file_duration_minutes as u64 * 60)
            .unwrap_or(3600); // Default 1 hour
            
        let upload_delay_secs = config
            .map(|c| c.storage.file_rotation.upload_delay_minutes as u64 * 60)
            .unwrap_or(120); // Default 2 minutes
        
        info!("File rotation config: size={}MB, duration={}min, upload_delay={}min", 
              file_size_bytes / (1024 * 1024), 
              rotation_duration_secs / 60,
              upload_delay_secs / 60);
        
        // Create parquet file manager
        let parquet_file_manager = Arc::new(ParquetFileManager::new(
            data_dir.clone(),
            file_size_bytes,
            rotation_duration_secs,
            azure_config,
            s3_config,
            delete_after_upload,
            upload_delay_secs,
        ).await?);
        
        // Create batch channel
        let (batch_tx, batch_rx) = mpsc::channel::<ArrowFlightBatch>(100000);
        
        // Start Arrow Flight receiver in background
        let flight_receiver = StorageArrowFlightReceiver::new(
            node_id.clone(),
            format!("0.0.0.0:{}", arrow_flight_port),
            batch_tx.clone(),
        );
        
        tokio::spawn(async move {
            if let Err(e) = flight_receiver.start().await {
                error!("Arrow Flight server failed: {}", e);
            }
        });
        
        info!("OTLP Arrow Flight server started on port {}", arrow_flight_port);
        
        let processor = Self {
            node_id,
            data_dir,
            parquet_file_manager,
            batch_receiver: Arc::new(tokio::sync::Mutex::new(batch_rx)),
        };
        
        Ok((processor, batch_tx))
    }
    
    /// Run the OTLP storage processor
    pub async fn run(self) -> Result<()> {
        info!("Starting OTLP storage processor for node {}", self.node_id);
        
        let self_arc = Arc::new(self);
        
        
        // Start simple gRPC query service in background
        let grpc_data_path = self_arc.data_dir.clone();
        let grpc_query_port = std::env::var("STORAGE_GRPC_QUERY_PORT").unwrap_or_else(|_| "8083".to_string());
        let grpc_bind_addr = format!("0.0.0.0:{}", grpc_query_port);

        info!("[STORAGE] Starting simple gRPC query service on {}", grpc_bind_addr);

        tokio::spawn(async move {
            if let Err(e) = crate::simple_grpc_service::start_simple_grpc_query_service(grpc_data_path, grpc_bind_addr.clone()).await {
                error!("[STORAGE] Simple gRPC query service failed: {}", e);
            }
        });

        // Give gRPC service time to start
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
        info!("[STORAGE] Simple gRPC service should now be running on port {}", grpc_query_port);
        
        // Process incoming batches
        let self_clone = self_arc.clone();
        let mut batch_count = 0u64;
        while let Some(batch) = self_clone.batch_receiver.lock().await.recv().await {
            batch_count += 1;
            if batch_count % 100 == 0 {
                info!("[OTLP STORAGE] Processed {} batches so far", batch_count);
            }
            if let Err(e) = self_arc.process_batch(batch).await {
                error!("Failed to process batch: {}", e);
            }
        }
        
        Ok(())
    }
    
    /// Process an incoming batch
    async fn process_batch(&self, batch: ArrowFlightBatch) -> Result<()> {
        let batch_id = &batch.batch_id;
        let metadata = &batch.metadata;
        let record_batch = &batch.record_batch;
        
        debug!("Processing OTLP batch {} with {} rows", batch_id, record_batch.num_rows());
        
        // Extract client name from metadata
        let client_name = extract_client_name(metadata);
        
        // Store in parquet file manager
        let row_count = record_batch.num_rows();
        self.parquet_file_manager.write_batch(&client_name, record_batch.clone()).await?;
        
        info!("OTLP batch {} processed for client '{}' ({} rows stored)", batch_id, client_name, row_count);
        
        Ok(())
    }
    
    /// Force flush all pending data
    pub async fn flush_all(&self) -> Result<()> {
        info!("Flushing parquet file manager...");
        self.parquet_file_manager.flush_all().await?;
        Ok(())
    }
    
    /// Graceful shutdown
    pub async fn shutdown(self) -> Result<()> {
        info!("Shutting down storage processor");
        
        // Flush all pending data first
        self.flush_all().await?;
        
        // Shutdown parquet file manager to flush all active files
        if let Ok(manager) = Arc::try_unwrap(self.parquet_file_manager) {
            manager.shutdown().await?;
        } else {
            warn!("Could not get exclusive access to parquet file manager for shutdown");
        }
        
        info!("Storage processor shutdown complete");
        Ok(())
    }
}

/// Extract client name from topic
/// Extract client name from metadata
fn extract_client_name(metadata: &std::collections::HashMap<String, String>) -> String {
    metadata.get("header.client_name")
        .or_else(|| metadata.get("client_name"))
        .or_else(|| metadata.get("header.topic"))
        .map(|s| {
            if s.contains('.') {
                extract_client_from_topic(s).to_string()
            } else {
                s.to_string()
            }
        })
        .unwrap_or_else(|| "default".to_string())
}

/// Extract client from topic name
fn extract_client_from_topic(topic: &str) -> &str {
    // Format: v6.qtw.traces.CLIENT.partition
    let parts: Vec<&str> = topic.split('.').collect();
    if parts.len() > 3 {
        parts[3]
    } else {
        "default"
    }
}