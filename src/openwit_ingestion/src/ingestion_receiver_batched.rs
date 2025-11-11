use std::sync::Arc;
use std::pin::Pin;
use anyhow::Result;
use tokio::sync::{mpsc, broadcast};
use tokio_stream::{Stream, wrappers::ReceiverStream};
use tonic::{Request, Response, Status, Streaming};
use tracing::{info, warn, error, debug};
use base64::Engine;
use prost::Message;

use openwit_proto::ingestion::{
    telemetry_ingestion_service_server::{TelemetryIngestionService, TelemetryIngestionServiceServer},
    TelemetryIngestRequest,
    TelemetryIngestResponse,
    TelemetryData,
    TelemetryIngestStatus,
};

use openwit_control_plane::ControlPlaneClient;
use openwit_config::UnifiedConfig;

// Import both OTLP receivers
use crate::otlp_preserving_receiver::OtlpPreservingReceiver;
use crate::batched_otlp_receiver::{BatchedOtlpReceiver, BatchConfig};
use openwit_inter_node::ArrowFlightBatch;

// Storage endpoint cache for service discovery
use std::time::{Instant, Duration};
use tokio::sync::Mutex;
use sqlx::PgPool;

/// Simple batch tracker for PostgreSQL updates
pub struct SimpleBatchTracker {
    db_pool: PgPool,
}

impl SimpleBatchTracker {
    pub fn new(db_pool: PgPool) -> Self {
        Self { db_pool }
    }
    
    pub async fn update_ingestion_completed(&self, batch_id: uuid::Uuid) -> anyhow::Result<()> {
        let query = r#"
        UPDATE batch_tracker 
        SET ingestion_pip = true, 
            ingestion_completed_at = NOW(),
            wal_duration_ms = CASE 
                WHEN wal_written_at IS NOT NULL 
                THEN EXTRACT(EPOCH FROM (NOW() - wal_written_at)) * 1000 
                ELSE NULL 
            END
        WHERE batch_id = $1
        "#;
        
        sqlx::query(query)
            .bind(batch_id)
            .execute(&self.db_pool)
            .await?;
            
        Ok(())
    }
}

/// Storage endpoint cache
struct StorageEndpointCache {
    endpoint: Option<String>,
    last_updated: Instant,
    ttl: Duration,
}

impl StorageEndpointCache {
    fn new() -> Self {
        Self {
            endpoint: None,
            last_updated: Instant::now() - Duration::from_secs(3600), // Force initial lookup
            ttl: Duration::from_secs(30), // Cache for 30 seconds
        }
    }

    fn is_expired(&self) -> bool {
        self.last_updated.elapsed() > self.ttl
    }

    fn update(&mut self, endpoint: String) {
        self.endpoint = Some(endpoint);
        self.last_updated = Instant::now();
    }
}

/// Enhanced gRPC service with batched OTLP support
pub struct BatchedTelemetryIngestionGrpcService {
    node_id: String,
    /// Service discovery cache for storage endpoints
    storage_cache: Arc<Mutex<StorageEndpointCache>>,
    /// Control plane client for service discovery
    control_client: Option<ControlPlaneClient>,
    /// Configuration
    config: UnifiedConfig,
    /// Batch tracker for PostgreSQL updates
    batch_tracker: Option<Arc<SimpleBatchTracker>>,
    /// OTLP receiver - either batched or preserving
    otlp_receiver: OtlpReceiverType,
    /// Sender for Arrow Flight batches
    batch_sender: Option<mpsc::Sender<ArrowFlightBatch>>,
}

enum OtlpReceiverType {
    Preserving(Arc<OtlpPreservingReceiver>),
    Batched(Arc<BatchedOtlpReceiver>),
}

impl BatchedTelemetryIngestionGrpcService {
    pub fn new(node_id: String, config: UnifiedConfig, enable_batching: bool) -> Self {
        info!("Creating BatchedTelemetryIngestionGrpcService for node: {} (batching: {})", 
            node_id, enable_batching);
        
        // Create a channel for OTLP batches to be sent to storage
        let (batch_tx, batch_rx) = mpsc::channel::<ArrowFlightBatch>(1000);
        
        // Create appropriate receiver based on configuration
        let otlp_receiver = if enable_batching {
            let batch_config = BatchConfig {
                max_spans_per_batch: std::env::var("OTLP_BATCH_MAX_SPANS")
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(10_000),
                max_batch_bytes: std::env::var("OTLP_BATCH_MAX_BYTES")
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(50_000_000),
                flush_interval: Duration::from_secs(
                    std::env::var("OTLP_BATCH_FLUSH_INTERVAL_SECS")
                        .ok()
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(5)
                ),
                max_messages_per_batch: std::env::var("OTLP_BATCH_MAX_MESSAGES")
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(1000),
            };
            
            info!("Using BatchedOtlpReceiver with config: {:?}", batch_config);
            OtlpReceiverType::Batched(Arc::new(BatchedOtlpReceiver::new(
                node_id.clone(),
                batch_tx.clone(),
                Some(batch_config),
            )))
        } else {
            info!("Using OtlpPreservingReceiver (non-batched)");
            OtlpReceiverType::Preserving(Arc::new(OtlpPreservingReceiver::new(
                node_id.clone(),
                batch_tx.clone(),
            )))
        };
        
        let storage_cache = Arc::new(Mutex::new(StorageEndpointCache::new()));
        
        // Spawn a task to forward batches to storage
        Self::spawn_batch_forwarder(
            node_id.clone(),
            batch_rx,
            storage_cache.clone(),
            None,
            config.clone(),
            None,
        );
        
        Self {
            node_id,
            storage_cache,
            control_client: None,
            config,
            batch_tracker: None,
            otlp_receiver,
            batch_sender: Some(batch_tx),
        }
    }
    
    /// Process telemetry message based on receiver type
    async fn process_telemetry_message(
        &self,
        message_id: &str,
        payload: &[u8],
        headers: std::collections::HashMap<String, String>,
    ) -> Result<(), Status> {
        match &self.otlp_receiver {
            OtlpReceiverType::Preserving(receiver) => {
                receiver.process_message(message_id, payload, headers).await
                    .map_err(|e| Status::internal(format!("Failed to process message: {}", e)))
            }
            OtlpReceiverType::Batched(receiver) => {
                receiver.process_message(message_id, payload, headers).await
                    .map_err(|e| Status::internal(format!("Failed to process batched message: {}", e)))
            }
        }
    }
    
    /// Spawn background task to forward batches to storage
    fn spawn_batch_forwarder(
        node_id: String,
        mut batch_rx: mpsc::Receiver<ArrowFlightBatch>,
        storage_cache: Arc<Mutex<StorageEndpointCache>>,
        control_client: Option<ControlPlaneClient>,
        config: UnifiedConfig,
        batch_tracker: Option<Arc<SimpleBatchTracker>>,
    ) {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(10));
            
            loop {
                tokio::select! {
                    Some(batch) = batch_rx.recv() => {
                        let batch_id = batch.batch_id.clone();
                        let row_count = batch.record_batch.num_rows();
                        
                        debug!("Forwarding batch {} with {} rows to storage", batch_id, row_count);
                        
                        // Get storage endpoint
                        let endpoint = match Self::get_storage_endpoint_static(
                            &storage_cache,
                            control_client.as_ref(),
                            &config,
                        ).await {
                            Ok(ep) => ep,
                            Err(e) => {
                                error!("Failed to get storage endpoint: {}", e);
                                continue;
                            }
                        };
                        
                        // Send batch via Arrow Flight
                        match openwit_inter_node::send_arrow_flight_batch(
                            &node_id,
                            &endpoint,
                            batch,
                        ).await {
                            Ok(_) => {
                                info!("Successfully sent batch {} to storage at {}", batch_id, endpoint);
                                
                                // Update batch tracker if available
                                if let Some(tracker) = &batch_tracker {
                                    if let Ok(batch_uuid) = uuid::Uuid::parse_str(&batch_id) {
                                        if let Err(e) = tracker.update_ingestion_completed(batch_uuid).await {
                                            warn!("Failed to update batch tracker for {}: {}", batch_id, e);
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                error!("Failed to send batch {} to storage: {}", batch_id, e);
                            }
                        }
                    }
                    _ = interval.tick() => {
                        debug!("Batch forwarder heartbeat");
                    }
                }
            }
        });
    }
    
    /// Get storage endpoint with caching and fallback
    async fn get_storage_endpoint_static(
        cache: &Arc<Mutex<StorageEndpointCache>>,
        control_client: Option<&ControlPlaneClient>,
        config: &UnifiedConfig,
    ) -> Result<String, String> {
        let mut cache_guard = cache.lock().await;
        
        // Check cache
        if !cache_guard.is_expired() {
            if let Some(endpoint) = &cache_guard.endpoint {
                return Ok(endpoint.clone());
            }
        }
        
        // Try control plane
        if let Some(client) = control_client {
            match Self::get_storage_from_control_plane_static(client).await {
                Ok(endpoint) => {
                    info!("Got storage endpoint from control plane: {}", endpoint);
                    cache_guard.update(endpoint.clone());
                    return Ok(endpoint);
                }
                Err(e) => {
                    warn!("Failed to get storage from control plane: {}", e);
                }
            }
        }
        
        // Use fallback
        let endpoint = format!("grpc://{}:{}",
            config.storage.arrow_flight.bind_address,
            config.storage.arrow_flight.port
        );
        
        info!("Using fallback storage endpoint: {}", endpoint);
        cache_guard.update(endpoint.clone());
        Ok(endpoint)
    }
    
    async fn get_storage_from_control_plane_static(client: &ControlPlaneClient) -> Result<String, String> {
        // Query control plane for storage nodes
        Err("Control plane integration not implemented".to_string())
    }
}

#[tonic::async_trait]
impl TelemetryIngestionService for BatchedTelemetryIngestionGrpcService {
    type IngestTelemetryBatchStream = Pin<Box<dyn Stream<Item = Result<TelemetryIngestResponse, Status>> + Send + 'static>>;
    
    async fn ingest_telemetry_batch(
        &self,
        request: Request<TelemetryIngestRequest>,
    ) -> Result<Response<Self::IngestTelemetryBatchStream>, Status> {
        let ingestion_request = request.into_inner();
        let batch_id = ingestion_request.batch_id.clone();
        
        info!("[INGESTION] Received batch {} with {} messages", 
            batch_id, ingestion_request.messages.len());
        
        let (tx, rx) = mpsc::channel(100);
        let node_id = self.node_id.clone();
        
        // Process messages
        let mut accepted = 0;
        let mut rejected = 0;
        
        for (idx, message) in ingestion_request.messages.into_iter().enumerate() {
            let message_id = message.message_id.clone();
            
            // Convert headers
            let mut headers = std::collections::HashMap::new();
            for (k, v) in &message.headers {
                headers.insert(k.clone(), v.clone());
            }
            
            // Process based on format
            let result = if message.is_json_format {
                // Legacy JSON format - not supported in batched mode
                warn!("JSON format not supported in batched ingestion for message {}", message_id);
                Err(Status::unimplemented("JSON format not supported in batched mode"))
            } else {
                // Binary/protobuf format
                self.process_telemetry_message(&message_id, &message.payload, headers).await
            };
            
            match result {
                Ok(_) => {
                    accepted += 1;
                    debug!("Processed message {} from batch {}", message_id, batch_id);
                }
                Err(e) => {
                    rejected += 1;
                    error!("Failed to process message {} from batch {}: {}", 
                        message_id, batch_id, e);
                }
            }
            
            // Send periodic updates
            if idx % 1000 == 0 && idx > 0 {
                let status_response = TelemetryIngestResponse {
                    batch_id: batch_id.clone(),
                    status: TelemetryIngestStatus::Processing as i32,
                    accepted_count: accepted,
                    rejected_count: rejected,
                    error_message: String::new(),
                    processing_time_ms: 0,
                };
                
                if tx.send(Ok(status_response)).await.is_err() {
                    break;
                }
            }
        }
        
        // Send final response
        let final_response = TelemetryIngestResponse {
            batch_id: batch_id.clone(),
            status: if rejected == 0 {
                TelemetryIngestStatus::Success as i32
            } else {
                TelemetryIngestStatus::PartialSuccess as i32
            },
            accepted_count: accepted,
            rejected_count: rejected,
            error_message: if rejected > 0 {
                format!("{} messages failed processing", rejected)
            } else {
                String::new()
            },
            processing_time_ms: 0, // TODO: Track actual time
        };
        
        let _ = tx.send(Ok(final_response)).await;
        
        info!("[INGESTION] Completed batch {} - accepted: {}, rejected: {}", 
            batch_id, accepted, rejected);
        
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
    
    async fn health_check(
        &self,
        _request: Request<()>,
    ) -> Result<Response<()>, Status> {
        Ok(Response::new(()))
    }
}