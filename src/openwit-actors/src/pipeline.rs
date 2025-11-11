use std::sync::Arc;
use tokio::sync::mpsc;
use anyhow::Result;
use tracing::{info, warn, error};
use opendal::{Operator, services};

use openwit_config::UnifiedConfig;
use openwit_control_plane::ControlPlaneClient;

use crate::{
    ingestion::{spawn_ingestion_actor, IngestionActorMessage, IngestionActorResponse},
    storage::{spawn_storage_actor, StorageMessage},
    indexer::{spawn_indexer_actor, IndexerMessage},
    janitor::{spawn_janitor_actor, JanitorMessage, JanitorConfig},
};

/// Coordinates all actors in the pipeline
#[allow(unused)]
#[derive(Clone)]
pub struct PipelineCoordinator {
    node_id: String,
    config: UnifiedConfig,
    
    // Actor channels
    ingestion_tx: mpsc::Sender<IngestionActorMessage>,
    // ingestion_rx: mpsc::Receiver<IngestionActorResponse>, // Moved to start_monitoring
    storage_tx: mpsc::Sender<StorageMessage>,
    indexer_tx: mpsc::Sender<IndexerMessage>,
    janitor_tx: mpsc::Sender<JanitorMessage>,
    
    // Control plane integration
    control_client: Option<Arc<ControlPlaneClient>>,
}

impl PipelineCoordinator {
    /// Create a new pipeline coordinator
    pub async fn new(
        node_id: String,
        config: UnifiedConfig,
        control_plane_addr: Option<String>,
    ) -> Result<Self> {
        info!("Initializing pipeline coordinator for node: {}", node_id);
        
        // Create control plane integration if configured
        let control_client = if let Some(_addr) = control_plane_addr {
            Some(Arc::new(ControlPlaneClient::new(&node_id, &config).await?))
        } else {
            None
        };
        
        // Create metastore based on config
        let metastore = create_metastore(&config).await?;
        
        // Channel for storage to indexer communication
        let (segment_tx, mut segment_rx) = mpsc::channel(100);
        
        // Spawn storage actor
        // TODO: Create proper storage config from unified config
        let storage_config = openwit_storage::config::StorageConfig::default();
        let cloud_operator = create_cloud_operator(&config).await?;
        let storage_tx = spawn_storage_actor(
            storage_config,
            metastore.clone(),
            segment_tx,
            cloud_operator,
        );
        
        // Spawn indexer actor
        let index_path = format!("{}/index", config.storage.data_dir);
        let indexer_tx = spawn_indexer_actor(metastore.clone(), index_path);
        
        // Forward segments from storage to indexer
        let indexer_tx_clone = indexer_tx.clone();
        tokio::spawn(async move {
            while let Some(segment_id) = segment_rx.recv().await {
                if let Err(e) = indexer_tx_clone.send(IndexerMessage::IndexSegment(segment_id)).await {
                    error!("Failed to send segment to indexer: {}", e);
                }
            }
        });
        
        // Spawn janitor actor
        // TODO: Create proper janitor config from unified config
        let janitor_config = JanitorConfig::default();
        let janitor_tx = spawn_janitor_actor(janitor_config, metastore.clone());
        
        // Create channel for ingestion to storage
        let (ingestion_storage_tx, mut ingestion_storage_rx) = mpsc::channel(1000);
        
        // Spawn ingestion actor
        let (ingestion_tx, ingestion_rx) = spawn_ingestion_actor(
            node_id.clone(),
            config.clone(),
            ingestion_storage_tx,
        );
        
        // Forward messages from ingestion to storage
        let storage_tx_clone = storage_tx.clone();
        tokio::spawn(async move {
            while let Some(messages) = ingestion_storage_rx.recv().await {
                if let Err(e) = storage_tx_clone.send(StorageMessage::StoreBatch(messages)).await {
                    error!("Failed to send batch to storage: {}", e);
                }
            }
        });
        
        // Create coordinator
        let coordinator = PipelineCoordinator {
            node_id: node_id.clone(),
            config: config.clone(),
            ingestion_tx: ingestion_tx.clone(),
            storage_tx: storage_tx.clone(),
            indexer_tx: indexer_tx.clone(),
            janitor_tx,
            control_client,
        };
        
        // Start monitoring ingestion responses
        let coordinator_arc = Arc::new(coordinator.clone());
        Self::monitor_ingestion_responses(coordinator_arc, ingestion_rx);
        
        Ok(coordinator)
    }
    
    /// Get the ingestion actor channel for proxy integration
    pub fn ingestion_channel(&self) -> mpsc::Sender<IngestionActorMessage> {
        self.ingestion_tx.clone()
    }
    
    /// Monitor ingestion responses
    fn monitor_ingestion_responses(coordinator: Arc<Self>, mut rx: mpsc::Receiver<IngestionActorResponse>) {
        tokio::spawn(async move {
            while let Some(response) = rx.recv().await {
                match response {
                    IngestionActorResponse::BatchAck { batch_id, success } => {
                        if success {
                            info!("Batch {} acknowledged", batch_id);
                        } else {
                            warn!("Batch {} failed", batch_id);
                        }
                    }
                    IngestionActorResponse::Stats(stats) => {
                        info!("Ingestion stats: {:?}", stats);
                        // Report to control plane if available
                        if let Some(_client) = &coordinator.control_client {
                            // Convert and report stats
                        }
                    }
                    _ => {}
                }
            }
        });
    }
    
    /// Start monitoring actors
    pub async fn start_monitoring(self: Arc<Self>) {
        // Periodic health checks
        let coordinator = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));
            
            loop {
                interval.tick().await;
                
                // Check actor health
                if let Err(e) = coordinator.health_check().await {
                    error!("Pipeline health check failed: {}", e);
                }
                
                // Report to control plane
                if let Some(client) = &coordinator.control_client {
                    if let Err(e) = coordinator.report_pipeline_status(client).await {
                        warn!("Failed to report pipeline status: {}", e);
                    }
                }
            }
        });
    }
    
    /// Perform health check on all actors
    async fn health_check(&self) -> Result<()> {
        // Check ingestion actor
        self.ingestion_tx.send(IngestionActorMessage::GetStats).await?;
        
        // Check storage actor
        let (tx, _rx) = mpsc::channel(1);
        self.storage_tx.send(StorageMessage::GetStats(tx)).await?;
        
        // Check indexer actor
        let (tx, _rx) = mpsc::channel(1);
        self.indexer_tx.send(IndexerMessage::GetStats(tx)).await?;
        
        Ok(())
    }
    
    /// Report pipeline status to control plane
    async fn report_pipeline_status(&self, _client: &ControlPlaneClient) -> Result<()> {
        // Gather stats from all actors
        // This is a simplified version - in production, we'd collect actual metrics
        
        let _status = PipelineStatus {
            node_id: self.node_id.clone(),
            ingestion_active: true,
            storage_active: true,
            indexer_active: true,
            janitor_active: true,
            timestamp: chrono::Utc::now(),
        };
        
        // Report to control plane
        // client.report_pipeline_status(status).await?;
        
        Ok(())
    }
    
    /// Graceful shutdown
    pub async fn shutdown(self) -> Result<()> {
        info!("Shutting down pipeline coordinator");
        
        // Shutdown actors in order
        let _ = self.ingestion_tx.send(IngestionActorMessage::Shutdown).await;
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        
        let _ = self.storage_tx.send(StorageMessage::Shutdown).await;
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        
        let _ = self.indexer_tx.send(IndexerMessage::Shutdown).await;
        let _ = self.janitor_tx.send(JanitorMessage::Shutdown).await;
        
        info!("Pipeline coordinator shutdown complete");
        Ok(())
    }
}

/// Pipeline status for reporting
#[allow(unused)]
#[derive(Debug, Clone)]
struct PipelineStatus {
    node_id: String,
    ingestion_active: bool,
    storage_active: bool,
    indexer_active: bool,
    janitor_active: bool,
    timestamp: chrono::DateTime<chrono::Utc>,
}

/// Create metastore based on configuration
async fn create_metastore(config: &UnifiedConfig) -> Result<Arc<dyn openwit_metastore::MetaStore>> {
    if config.metastore.backend == "postgres" {
        let postgres_config = &config.metastore.postgres;
        
        let connection_string = postgres_config.connection_string.clone();
        
        let metastore_config = openwit_metastore::postgres_metastore::PostgresMetastoreConfig {
            connection_string,
            max_connections: 10,
            schema_name: "metastore".to_string(),
        };
        
        let metastore = openwit_metastore::postgres_metastore::PostgresMetaStore::new(metastore_config).await?;
        
        Ok(Arc::new(metastore))
    } else {
        // Default to sled
        let sled_path = config.metastore.sled.path.clone();
        let metastore_config = openwit_metastore::sled_metastore::SledMetastoreConfig {
            path: sled_path,
            cache_size_mb: 128,
        };
        
        let metastore = openwit_metastore::sled_metastore::SledMetaStore::new(metastore_config).await?;
        Ok(Arc::new(metastore))
    }
}

/// Create cloud storage operator if configured
pub async fn create_cloud_operator(config: &UnifiedConfig) -> Result<Option<Operator>> {
    // Check if cloud storage is configured based on backend
    match config.storage.backend.as_str() {
        "azure" => {
            let azure_config = &config.storage.azure;
            
            // Build Azure blob storage
            let mut builder = services::Azblob::default()
                .account_name(&azure_config.account_name)
                .container(&azure_config.container_name);
            
            if !azure_config.access_key.is_empty() {
                builder = builder.account_key(&azure_config.access_key);
            }
            
            let operator = Operator::new(builder)?
                .finish();
            
            Ok(Some(operator))
        }
        "s3" => {
            let s3_config = &config.storage.s3;
            
            // Build S3 storage
            let mut builder = services::S3::default()
                .bucket(&s3_config.bucket)
                .region(&s3_config.region);
            
            if !s3_config.access_key_id.is_empty() {
                builder = builder.access_key_id(&s3_config.access_key_id);
            }
            if !s3_config.secret_access_key.is_empty() {
                builder = builder.secret_access_key(&s3_config.secret_access_key);
            }
            if !s3_config.endpoint.is_empty() {
                builder = builder.endpoint(&s3_config.endpoint);
            }
            
            let operator = Operator::new(builder)?
                .finish();
            
            Ok(Some(operator))
        }
        "gcs" => {
            let gcs_config = &config.storage.gcs;
            
            // Build GCS storage
            let builder = services::Gcs::default()
                .bucket(&gcs_config.bucket);
            
            // Note: service_account_key contains the JSON key content, not a path
            // OpenDAL expects the path to the service account file
            // For now, we'll skip this configuration
            // TODO: Handle service account key content properly
            
            let operator = Operator::new(builder)?
                .finish();
            
            Ok(Some(operator))
        }
        _ => Ok(None),
    }
}