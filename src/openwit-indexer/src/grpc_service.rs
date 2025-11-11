use anyhow::{Context, Result};
use std::sync::Arc;
use tonic::{Request, Response, Status};
use tracing::{debug, error, info, warn};

use crate::{
    config::IndexerConfig,
    db::{DbClient, CoverageManifest},
    metrics::HealthStatus,
};

#[cfg(feature = "events")]
use crate::generated::openwit::indexer::v1::{
    index_service_server::{IndexService, IndexServiceServer},
    CoverageRequest, CoverageReply, Manifest, Delta,
    PruneRequest, PruneReply,
    HealthRequest, HealthReply,
    DataFileNotification, DataFileNotificationReply,
};

/// Implementation of the IndexService gRPC API
#[allow(dead_code)]
pub struct IndexServiceImpl {
    node_id: String,
    config: Arc<IndexerConfig>,
    db_client: Arc<DbClient>,
    start_time: std::time::Instant,
    event_sender: Option<tokio::sync::mpsc::Sender<crate::events::ControlEvent>>,
}

impl IndexServiceImpl {
    pub fn new(
        node_id: String,
        config: Arc<IndexerConfig>,
        db_client: Arc<DbClient>,
    ) -> Self {
        Self {
            node_id,
            config,
            db_client,
            start_time: std::time::Instant::now(),
            event_sender: None,
        }
    }

    pub fn with_event_sender(
        node_id: String,
        config: Arc<IndexerConfig>,
        db_client: Arc<DbClient>,
        event_sender: tokio::sync::mpsc::Sender<crate::events::ControlEvent>,
    ) -> Self {
        Self {
            node_id,
            config,
            db_client,
            start_time: std::time::Instant::now(),
            event_sender: Some(event_sender),
        }
    }

    /// Convert database coverage manifest to gRPC format
    fn convert_manifest(&self, cm: CoverageManifest) -> Manifest {
        let deltas = cm.deltas.into_iter()
            .map(|d| Delta {
                file_ulid: d.file_ulid,
                partition_key: d.partition_key,
                zone_map_url: d.zone_map_url.unwrap_or_default(),
                bloom_url: d.bloom_url.unwrap_or_default(),
                bitmap_url: d.bitmap_url.unwrap_or_default(),
                tantivy_segments: d.tantivy_segments,
            })
            .collect();

        Manifest {
            partition_key: cm.partition_key,
            level: cm.level,
            manifest_url: cm.manifest_url.unwrap_or_default(),
            deltas,
        }
    }
}

#[cfg(feature = "events")]
#[tonic::async_trait]
impl IndexService for IndexServiceImpl {
    /// Get coverage information for a time range
    async fn get_coverage(
        &self,
        request: Request<CoverageRequest>,
    ) -> Result<Response<CoverageReply>, Status> {
        let req = request.into_inner();
        
        debug!(
            tenant = %req.tenant,
            signal = %req.signal,
            from_ms = req.from_unix_ms,
            to_ms = req.to_unix_ms,
            "GetCoverage request"
        );

        // First try to get combined indexes if preferred
        let mut manifests = Vec::new();
        
        if req.prefer_combined_only {
            // Query for combined indexes
            match self.db_client.get_published_snapshots(
                &req.tenant,
                &req.signal,
                req.from_unix_ms,
                req.to_unix_ms,
                true, // prefer_combined
            ).await {
                Ok(snapshots) => {
                    for snapshot in snapshots {
                        if let Some(manifest_url) = snapshot.metadata.get("manifest_url")
                            .and_then(|v| v.as_str()) {
                            manifests.push(Manifest {
                                partition_key: snapshot.partition_key,
                                level: snapshot.level,
                                manifest_url: manifest_url.to_string(),
                                deltas: Vec::new(),
                            });
                        }
                    }
                }
                Err(e) => {
                    error!(error = %e, "Failed to query combined indexes");
                }
            }
        }

        // Fall back to delta indexes if no combined found or not preferred
        if manifests.is_empty() || !req.prefer_combined_only {
            match self.db_client.list_visible_deltas(
                &req.tenant,
                &req.signal,
                req.from_unix_ms,
                req.to_unix_ms,
            ).await {
                Ok(coverage_manifests) => {
                    for cm in coverage_manifests {
                        manifests.push(self.convert_manifest(cm));
                    }
                }
                Err(e) => {
                    error!(error = %e, "Failed to query delta indexes");
                    return Err(Status::internal("Failed to retrieve coverage"));
                }
            }
        }

        info!(
            tenant = %req.tenant,
            signal = %req.signal,
            manifest_count = manifests.len(),
            "GetCoverage response"
        );

        Ok(Response::new(CoverageReply { manifests }))
    }

    /// Server-side pruning (optional)
    async fn prune(
        &self,
        request: Request<PruneRequest>,
    ) -> Result<Response<PruneReply>, Status> {
        let req = request.into_inner();
        
        debug!(
            tenant = %req.tenant,
            signal = %req.signal,
            predicate_count = req.predicates.len(),
            "Prune request"
        );

        // This is a simplified implementation
        // In production, you would:
        // 1. Parse predicates and evaluate against zone maps/bloom filters
        // 2. Determine which row groups can be skipped
        // 3. Return only the splits that might contain matching data

        let splits = Vec::new(); // Placeholder
        let projection = req.columns_needed;

        warn!("Prune operation not fully implemented");

        Ok(Response::new(PruneReply {
            splits,
            projection,
        }))
    }

    /// Direct notification from storage nodes about new data files
    async fn notify_data_file_published(
        &self,
        request: Request<DataFileNotification>,
    ) -> Result<Response<DataFileNotificationReply>, Status> {
        let notification = request.into_inner();
        
        info!(
            file_ulid = %notification.file_ulid,
            tenant = %notification.tenant,
            signal = %notification.signal,
            partition_key = %notification.partition_key,
            storage_node = %notification.storage_node_id,
            "Received direct data file notification from storage"
        );

        // Convert to internal event type
        let event = crate::events::ControlEvent::DataFilePublished {
            file_ulid: notification.file_ulid,
            tenant: notification.tenant,
            signal: notification.signal,
            partition_key: notification.partition_key,
            parquet_url: notification.parquet_url,
            size_bytes: notification.size_bytes as u64,
            row_count: notification.row_count as u64,
            min_timestamp: chrono::DateTime::from_timestamp(notification.min_timestamp, 0)
                .unwrap_or(chrono::Utc::now()),
            max_timestamp: chrono::DateTime::from_timestamp(notification.max_timestamp, 0)
                .unwrap_or(chrono::Utc::now()),
        };

        // Send to processing queue
        if let Some(sender) = &self.event_sender {
            match sender.send(event).await {
                Ok(_) => {
                    crate::metrics::record_event_consumed("DataFilePublished", "direct_storage");
                    Ok(Response::new(DataFileNotificationReply {
                        success: true,
                        message: "Notification received and queued for processing".to_string(),
                    }))
                }
                Err(e) => {
                    error!(error = %e, "Failed to queue event for processing");
                    Err(Status::internal("Failed to queue event"))
                }
            }
        } else {
            error!("Event sender not configured");
            Err(Status::internal("Event processing not available"))
        }
    }

    /// Health check endpoint
    async fn healthz(
        &self,
        _request: Request<HealthRequest>,
    ) -> Result<Response<HealthReply>, Status> {
        debug!("Health check request");

        // Check database connectivity
        let db_healthy = match self.db_client.health_check().await {
            Ok(_) => true,
            Err(e) => {
                error!(error = %e, "Database health check failed");
                false
            }
        };

        // Get current metrics
        let build_queue_depth = crate::metrics::BUILD_QUEUE_DEPTH
            .with_label_values(&["normal"])
            .get() as usize;
        
        let combine_queue_depth = crate::metrics::COMBINE_QUEUE_DEPTH
            .with_label_values(&["hour"])
            .get() as usize
            + crate::metrics::COMBINE_QUEUE_DEPTH
                .with_label_values(&["day"])
                .get() as usize;

        let accepting = crate::metrics::ACCEPTING
            .with_label_values(&["backpressure"])
            .get() > 0.0;

        let health_status = if db_healthy {
            HealthStatus::healthy(
                accepting,
                build_queue_depth,
                combine_queue_depth,
                self.start_time.elapsed(),
            )
        } else {
            HealthStatus::unhealthy("Database unhealthy".to_string())
        };

        let mut metrics = std::collections::HashMap::new();
        metrics.insert("node_id".to_string(), self.node_id.clone());
        metrics.insert("version".to_string(), health_status.version);
        metrics.insert("uptime_seconds".to_string(), health_status.uptime_seconds.to_string());
        metrics.insert("memory_usage_mb".to_string(), health_status.memory_usage_mb.to_string());
        metrics.insert("cpu_usage_percent".to_string(), health_status.cpu_usage_percent.to_string());
        metrics.insert("build_queue_depth".to_string(), build_queue_depth.to_string());
        metrics.insert("combine_queue_depth".to_string(), combine_queue_depth.to_string());
        metrics.insert("accepting".to_string(), accepting.to_string());

        Ok(Response::new(HealthReply {
            healthy: health_status.status == "healthy",
            status: health_status.status,
            metrics,
        }))
    }
}

/// Start the gRPC server
pub async fn start_grpc_server(
    config: Arc<IndexerConfig>,
    db_client: Arc<DbClient>,
    addr: std::net::SocketAddr,
    event_sender: Option<tokio::sync::mpsc::Sender<crate::events::ControlEvent>>,
) -> Result<()> {
    let node_id = config.indexer.general.node_id.clone();
    
    let service = if let Some(sender) = event_sender {
        IndexServiceImpl::with_event_sender(
            node_id.clone(),
            config,
            db_client,
            sender,
        )
    } else {
        IndexServiceImpl::new(
            node_id.clone(),
            config,
            db_client,
        )
    };

    let index_service = IndexServiceServer::new(service);

    info!(
        node_id = %node_id,
        addr = %addr,
        "Starting IndexService gRPC server"
    );

    tonic::transport::Server::builder()
        .add_service(index_service)
        .serve(addr)
        .await
        .context("Failed to start gRPC server")?;

    Ok(())
}