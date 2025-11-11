/// Batch monitoring API for the searcher node
/// Provides centralized access to batch status across all ingestion nodes
use std::sync::Arc;
use std::collections::HashMap;
use axum::{
    routing::{get, post},
    extract::{State, Query, Path, Json},
    response::{IntoResponse, Response},
    http::StatusCode,
    Router,
};
use serde::{Deserialize, Serialize};
use tracing::{info, warn, error, debug};
use tokio::sync::RwLock;
use chrono::{DateTime, Utc};
use reqwest;

#[derive(Clone)]
pub struct BatchMonitoringState {
    /// Control plane client to discover ingestion nodes
    control_plane_url: String,
    
    /// Cache of batch status from ingestion nodes
    status_cache: Arc<RwLock<BatchStatusCache>>,
    
    /// HTTP client for fetching status
    http_client: reqwest::Client,
}

#[derive(Default)]
struct BatchStatusCache {
    /// Map of ingestion_node_id -> latest status
    node_status: HashMap<String, CachedNodeStatus>,
    
    /// Last update time
    last_update: Option<DateTime<Utc>>,
}

#[derive(Clone)]
struct CachedNodeStatus {
    node_id: String,
    node_url: String,
    status: BatchStatusSummary,
    fetched_at: DateTime<Utc>,
}

/// Batch status summary (matches ingestion node format)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchStatusSummary {
    pub timestamp: DateTime<Utc>,
    pub total_batches: usize,
    pub status_breakdown: StatusBreakdown,
    pub pending_wal_cleanup: PendingWalInfo,
    pub recent_activity: RecentActivity,
    pub detailed_batches: Vec<BatchSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusBreakdown {
    pub ingested_to_wal: usize,
    pub sent_to_storage: usize,
    pub storage_confirmed: usize,
    pub upload_started: usize,
    pub upload_completed: usize,
    pub completed: usize,
    pub failed: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingWalInfo {
    pub total_pending_batches: usize,
    pub total_wal_files_pending: usize,
    pub total_bytes_pending: usize,
    pub oldest_pending_batch: Option<BatchAge>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchAge {
    pub batch_id: String,
    pub age_minutes: u64,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecentActivity {
    pub last_hour_ingested: usize,
    pub last_hour_uploaded: usize,
    pub last_hour_completed: usize,
    pub upload_rate_per_minute: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchSummary {
    pub batch_id: String,
    pub status: String,
    pub index_name: String,
    pub message_count: usize,
    pub total_bytes: usize,
    pub wal_files: usize,
    pub parquet_files: usize,
    pub cloud_uploads: usize,
    pub age_minutes: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// API response types
#[derive(Serialize)]
pub struct AggregatedBatchStatus {
    pub timestamp: DateTime<Utc>,
    pub total_nodes: usize,
    pub nodes_reporting: usize,
    pub global_stats: GlobalBatchStats,
    pub node_summaries: Vec<NodeBatchSummary>,
    pub all_pending_batches: Vec<PendingBatchInfo>,
    pub alerts: Vec<BatchAlert>,
}

#[derive(Serialize)]
pub struct GlobalBatchStats {
    pub total_active_batches: usize,
    pub total_pending_wal_cleanup: usize,
    pub total_pending_bytes_mb: f64,
    pub global_status_breakdown: StatusBreakdown,
    pub global_recent_activity: RecentActivity,
}

#[derive(Serialize)]
pub struct NodeBatchSummary {
    pub node_id: String,
    pub node_url: String,
    pub last_update: DateTime<Utc>,
    pub total_batches: usize,
    pub pending_cleanup: usize,
    pub status: String,
}

#[derive(Serialize)]
pub struct PendingBatchInfo {
    pub batch_id: String,
    pub node_id: String,
    pub status: String,
    pub index_name: String,
    pub age_minutes: u64,
    pub size_mb: f64,
}

#[derive(Serialize)]
pub struct BatchAlert {
    pub severity: String,
    pub node_id: String,
    pub message: String,
    pub batch_id: Option<String>,
}

/// Query parameters for filtering
#[derive(Deserialize)]
pub struct BatchQueryParams {
    pub status: Option<String>,
    pub index_name: Option<String>,
    pub node_id: Option<String>,
    pub min_age_minutes: Option<u64>,
    pub limit: Option<usize>,
}

/// Create the batch monitoring API router
pub fn create_batch_monitoring_router(control_plane_url: String) -> Router {
    let state = BatchMonitoringState {
        control_plane_url,
        status_cache: Arc::new(RwLock::new(BatchStatusCache::default())),
        http_client: reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .unwrap(),
    };
    
    Router::new()
        // Global status endpoints
        .route("/api/batch-monitor/status", get(get_global_status))
        .route("/api/batch-monitor/refresh", post(refresh_status))
        
        // Node-specific endpoints
        .route("/api/batch-monitor/nodes", get(list_nodes))
        .route("/api/batch-monitor/nodes/:node_id", get(get_node_status))
        
        // Batch search and details
        .route("/api/batch-monitor/batches", get(search_batches))
        .route("/api/batch-monitor/batches/:batch_id", get(get_batch_details))
        
        // Alerts and health
        .route("/api/batch-monitor/alerts", get(get_alerts))
        .route("/api/batch-monitor/health", get(health_check))
        
        // Export endpoints
        .route("/api/batch-monitor/export/csv", get(export_csv))
        .route("/api/batch-monitor/export/json", get(export_json))
        
        .with_state(state)
}

/// Get global aggregated status
async fn get_global_status(
    State(state): State<BatchMonitoringState>,
) -> Result<Json<AggregatedBatchStatus>, StatusCode> {
    // Refresh cache if stale (older than 1 minute)
    let should_refresh = {
        let cache = state.status_cache.read().await;
        cache.last_update.map_or(true, |t| (Utc::now() - t).num_seconds() > 60)
    };
    
    if should_refresh {
        refresh_cache(&state).await;
    }
    
    // Build aggregated response
    let cache = state.status_cache.read().await;
    let aggregated = build_aggregated_status(&cache);
    
    Ok(Json(aggregated))
}

/// Manually refresh status from all nodes
async fn refresh_status(
    State(state): State<BatchMonitoringState>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    refresh_cache(&state).await;
    
    Ok(Json(serde_json::json!({
        "success": true,
        "message": "Status refreshed",
        "timestamp": Utc::now()
    })))
}

/// List all ingestion nodes
async fn list_nodes(
    State(state): State<BatchMonitoringState>,
) -> Result<Json<Vec<NodeBatchSummary>>, StatusCode> {
    let cache = state.status_cache.read().await;
    
    let nodes: Vec<NodeBatchSummary> = cache.node_status.values()
        .map(|node| NodeBatchSummary {
            node_id: node.node_id.clone(),
            node_url: node.node_url.clone(),
            last_update: node.fetched_at,
            total_batches: node.status.total_batches,
            pending_cleanup: node.status.pending_wal_cleanup.total_pending_batches,
            status: "active".to_string(),
        })
        .collect();
    
    Ok(Json(nodes))
}

/// Get status for specific node
async fn get_node_status(
    State(state): State<BatchMonitoringState>,
    Path(node_id): Path<String>,
) -> Result<Json<BatchStatusSummary>, StatusCode> {
    let cache = state.status_cache.read().await;
    
    match cache.node_status.get(&node_id) {
        Some(node) => Ok(Json(node.status.clone())),
        None => Err(StatusCode::NOT_FOUND),
    }
}

/// Search batches with filters
async fn search_batches(
    State(state): State<BatchMonitoringState>,
    Query(params): Query<BatchQueryParams>,
) -> Result<Json<Vec<PendingBatchInfo>>, StatusCode> {
    let cache = state.status_cache.read().await;
    let mut results = Vec::new();
    
    for (node_id, node_status) in &cache.node_status {
        for batch in &node_status.status.detailed_batches {
            // Apply filters
            if let Some(ref status) = params.status {
                if &batch.status != status {
                    continue;
                }
            }
            
            if let Some(ref index) = params.index_name {
                if !batch.index_name.contains(index) {
                    continue;
                }
            }
            
            if let Some(min_age) = params.min_age_minutes {
                if batch.age_minutes < min_age {
                    continue;
                }
            }
            
            results.push(PendingBatchInfo {
                batch_id: batch.batch_id.clone(),
                node_id: node_id.clone(),
                status: batch.status.clone(),
                index_name: batch.index_name.clone(),
                age_minutes: batch.age_minutes,
                size_mb: batch.total_bytes as f64 / 1_048_576.0,
            });
        }
    }
    
    // Apply limit
    if let Some(limit) = params.limit {
        results.truncate(limit);
    }
    
    Ok(Json(results))
}

/// Get details for specific batch
async fn get_batch_details(
    State(state): State<BatchMonitoringState>,
    Path(batch_id): Path<String>,
) -> Result<Response, StatusCode> {
    let cache = state.status_cache.read().await;
    
    // Search for batch across all nodes
    for (node_id, node_status) in &cache.node_status {
        if let Some(batch) = node_status.status.detailed_batches.iter()
            .find(|b| b.batch_id == batch_id) {
            
            // Try to get full details from the node
            let url = format!("{}/api/batch/{}/status", node_status.node_url, batch_id);
            
            match state.http_client.get(&url).send().await {
                Ok(response) => {
                    let body = response.bytes().await.unwrap_or_default();
                    return Ok(Response::builder()
                        .status(StatusCode::OK)
                        .header("Content-Type", "application/json")
                        .body(body.into())
                        .unwrap());
                }
                Err(e) => {
                    warn!("Failed to fetch batch details from {}: {}", node_id, e);
                    // Return cached summary
                    return Ok(Json(batch.clone()).into_response());
                }
            }
        }
    }
    
    Err(StatusCode::NOT_FOUND)
}

/// Get alerts for problematic batches
async fn get_alerts(
    State(state): State<BatchMonitoringState>,
) -> Result<Json<Vec<BatchAlert>>, StatusCode> {
    let cache = state.status_cache.read().await;
    let mut alerts = Vec::new();
    
    for (node_id, node_status) in &cache.node_status {
        // Check for old batches
        if let Some(oldest) = &node_status.status.pending_wal_cleanup.oldest_pending_batch {
            if oldest.age_minutes > 60 {
                alerts.push(BatchAlert {
                    severity: "warning".to_string(),
                    node_id: node_id.clone(),
                    message: format!("Batch {} is {} minutes old", oldest.batch_id, oldest.age_minutes),
                    batch_id: Some(oldest.batch_id.clone()),
                });
            }
        }
        
        // Check for high pending count
        let pending = node_status.status.pending_wal_cleanup.total_pending_batches;
        if pending > 100 {
            alerts.push(BatchAlert {
                severity: "warning".to_string(),
                node_id: node_id.clone(),
                message: format!("High pending batch count: {}", pending),
                batch_id: None,
            });
        }
        
        // Check for failed batches
        if node_status.status.status_breakdown.failed > 0 {
            alerts.push(BatchAlert {
                severity: "error".to_string(),
                node_id: node_id.clone(),
                message: format!("{} failed batches", node_status.status.status_breakdown.failed),
                batch_id: None,
            });
        }
    }
    
    Ok(Json(alerts))
}

/// Health check endpoint
async fn health_check(
    State(state): State<BatchMonitoringState>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let cache = state.status_cache.read().await;
    
    Ok(Json(serde_json::json!({
        "status": "healthy",
        "nodes_monitored": cache.node_status.len(),
        "last_update": cache.last_update,
    })))
}

/// Export status as CSV
async fn export_csv(
    State(state): State<BatchMonitoringState>,
) -> Result<String, StatusCode> {
    let cache = state.status_cache.read().await;
    let mut csv = String::from("batch_id,node_id,status,index_name,age_minutes,size_mb,created_at\n");
    
    for (node_id, node_status) in &cache.node_status {
        for batch in &node_status.status.detailed_batches {
            csv.push_str(&format!(
                "{},{},{},{},{},{:.2},{}\n",
                batch.batch_id,
                node_id,
                batch.status,
                batch.index_name,
                batch.age_minutes,
                batch.total_bytes as f64 / 1_048_576.0,
                batch.created_at.to_rfc3339()
            ));
        }
    }
    
    Ok(csv)
}

/// Export status as JSON
async fn export_json(
    State(state): State<BatchMonitoringState>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let cache = state.status_cache.read().await;
    let aggregated = build_aggregated_status(&cache);
    
    Ok(Json(serde_json::to_value(aggregated).unwrap()))
}

/// Helper function to refresh cache from all ingestion nodes
async fn refresh_cache(state: &BatchMonitoringState) {
    info!("Refreshing batch status from all ingestion nodes");
    
    // Get list of ingestion nodes from control plane
    match get_ingestion_nodes(&state.control_plane_url, &state.http_client).await {
        Ok(nodes) => {
            let mut new_cache = HashMap::new();
            
            // Fetch status from each node in parallel
            let futures: Vec<_> = nodes.into_iter()
                .map(|(node_id, node_url)| {
                    let client = state.http_client.clone();
                    async move {
                        fetch_node_status(node_id, node_url, client).await
                    }
                })
                .collect();
            
            let results = futures::future::join_all(futures).await;
            
            for result in results {
                if let Some(node_status) = result {
                    new_cache.insert(node_status.node_id.clone(), node_status);
                }
            }
            
            // Update cache
            let mut cache = state.status_cache.write().await;
            cache.node_status = new_cache;
            cache.last_update = Some(Utc::now());
            
            info!("Cache refreshed with {} nodes", cache.node_status.len());
        }
        Err(e) => {
            error!("Failed to get ingestion nodes from control plane: {}", e);
        }
    }
}

/// Get ingestion nodes from control plane
async fn get_ingestion_nodes(
    _control_plane_url: &str,
    _client: &reqwest::Client,
) -> Result<Vec<(String, String)>, anyhow::Error> {
    // TODO: Replace with actual control plane API call
    // For now, return mock data
    Ok(vec![
        ("ingestion-0".to_string(), "http://ingestion-0:8081".to_string()),
        ("ingestion-1".to_string(), "http://ingestion-1:8081".to_string()),
    ])
}

/// Fetch status from a single node
async fn fetch_node_status(
    node_id: String,
    node_url: String,
    client: reqwest::Client,
) -> Option<CachedNodeStatus> {
    let status_url = format!("{}/latest_status.json", node_url);
    
    match client.get(&status_url).send().await {
        Ok(response) => {
            match response.json::<BatchStatusSummary>().await {
                Ok(status) => {
                    debug!("Fetched status from node {}: {} batches", node_id, status.total_batches);
                    Some(CachedNodeStatus {
                        node_id,
                        node_url,
                        status,
                        fetched_at: Utc::now(),
                    })
                }
                Err(e) => {
                    warn!("Failed to parse status from node {}: {}", node_id, e);
                    None
                }
            }
        }
        Err(e) => {
            warn!("Failed to fetch status from node {}: {}", node_id, e);
            None
        }
    }
}

/// Build aggregated status from cache
fn build_aggregated_status(cache: &BatchStatusCache) -> AggregatedBatchStatus {
    let mut global_stats = GlobalBatchStats {
        total_active_batches: 0,
        total_pending_wal_cleanup: 0,
        total_pending_bytes_mb: 0.0,
        global_status_breakdown: StatusBreakdown {
            ingested_to_wal: 0,
            sent_to_storage: 0,
            storage_confirmed: 0,
            upload_started: 0,
            upload_completed: 0,
            completed: 0,
            failed: 0,
        },
        global_recent_activity: RecentActivity {
            last_hour_ingested: 0,
            last_hour_uploaded: 0,
            last_hour_completed: 0,
            upload_rate_per_minute: 0.0,
        },
    };
    
    let mut node_summaries = Vec::new();
    let mut all_pending_batches = Vec::new();
    let mut alerts = Vec::new();
    
    // Aggregate from all nodes
    for (node_id, node_status) in &cache.node_status {
        // Update global stats
        global_stats.total_active_batches += node_status.status.total_batches;
        global_stats.total_pending_wal_cleanup += node_status.status.pending_wal_cleanup.total_pending_batches;
        global_stats.total_pending_bytes_mb += node_status.status.pending_wal_cleanup.total_bytes_pending as f64 / 1_048_576.0;
        
        // Aggregate status breakdown
        global_stats.global_status_breakdown.ingested_to_wal += node_status.status.status_breakdown.ingested_to_wal;
        global_stats.global_status_breakdown.sent_to_storage += node_status.status.status_breakdown.sent_to_storage;
        global_stats.global_status_breakdown.storage_confirmed += node_status.status.status_breakdown.storage_confirmed;
        global_stats.global_status_breakdown.upload_started += node_status.status.status_breakdown.upload_started;
        global_stats.global_status_breakdown.upload_completed += node_status.status.status_breakdown.upload_completed;
        global_stats.global_status_breakdown.completed += node_status.status.status_breakdown.completed;
        global_stats.global_status_breakdown.failed += node_status.status.status_breakdown.failed;
        
        // Aggregate recent activity
        global_stats.global_recent_activity.last_hour_ingested += node_status.status.recent_activity.last_hour_ingested;
        global_stats.global_recent_activity.last_hour_uploaded += node_status.status.recent_activity.last_hour_uploaded;
        global_stats.global_recent_activity.last_hour_completed += node_status.status.recent_activity.last_hour_completed;
        global_stats.global_recent_activity.upload_rate_per_minute += node_status.status.recent_activity.upload_rate_per_minute;
        
        // Add node summary
        node_summaries.push(NodeBatchSummary {
            node_id: node_id.clone(),
            node_url: node_status.node_url.clone(),
            last_update: node_status.fetched_at,
            total_batches: node_status.status.total_batches,
            pending_cleanup: node_status.status.pending_wal_cleanup.total_pending_batches,
            status: "active".to_string(),
        });
        
        // Collect pending batches
        for batch in &node_status.status.detailed_batches {
            if batch.status != "Completed" && batch.status != "Failed" {
                all_pending_batches.push(PendingBatchInfo {
                    batch_id: batch.batch_id.clone(),
                    node_id: node_id.clone(),
                    status: batch.status.clone(),
                    index_name: batch.index_name.clone(),
                    age_minutes: batch.age_minutes,
                    size_mb: batch.total_bytes as f64 / 1_048_576.0,
                });
            }
        }
    }
    
    // Sort pending batches by age (oldest first)
    all_pending_batches.sort_by(|a, b| b.age_minutes.cmp(&a.age_minutes));
    
    AggregatedBatchStatus {
        timestamp: Utc::now(),
        total_nodes: cache.node_status.len(),
        nodes_reporting: cache.node_status.len(),
        global_stats,
        node_summaries,
        all_pending_batches,
        alerts,
    }
}