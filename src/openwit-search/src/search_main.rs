use anyhow::{Result, Context};
use tracing::{info, error, warn};
use tokio::time::{interval, Duration};
use std::sync::Arc;

use openwit_config::UnifiedConfig;
use openwit_control_plane::client::ControlPlaneClient;
use crate::query_engine::{QueryEngineV2, QueryEngineConfig};
use axum::{
    extract::{Json, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Router,
};
use tower_http::trace::TraceLayer;
use tower_http::cors::CorsLayer;

/// Start the search service with control plane integration (V2)
pub async fn run_search_service_v2(
    config: UnifiedConfig,
    node_id: String,
    port: u16,
) -> Result<()> {
    info!("🚀 Starting Search Service V2 with control plane integration");
    info!("   Node ID: {}", node_id);
    info!("   HTTP Port: {}", port);
    
    // Create control plane client first
    let control_plane_client = create_control_plane_client(&node_id, &config).await?;
    
    // Start health reporting
    let health_reporter = control_plane_client.clone();
    let reporter_node_id = node_id.clone();
    let search_endpoint = config.networking.service_endpoints.search.clone()
        .unwrap_or_else(|| format!("http://localhost:{}", port));
    
    tokio::spawn(async move {
        start_health_reporter(health_reporter, reporter_node_id, search_endpoint).await;
    });
    
    // Create query engine with control plane integration
    let query_engine_config = QueryEngineConfig {
        query_timeout_secs: 300,
        node_id: node_id.clone(),
    };
    
    let query_engine = QueryEngineV2::new(
        query_engine_config,
        None, // Will create from control plane client
        Some(config.clone()),
    ).await?;
    
    // Create a simple HTTP server with the V2 query engine
    let bind_addr = format!("0.0.0.0:{}", port);
    
    // Start HTTP server with V2 query engine
    info!("✅ Search Service V2 initialized successfully");
    start_http_server_v2(bind_addr, Arc::new(query_engine)).await
}

/// Create control plane client
async fn create_control_plane_client(
    node_id: &str,
    config: &UnifiedConfig,
) -> Result<ControlPlaneClient> {
    // Get control plane endpoint
    let control_plane_endpoint = config.networking.service_endpoints.control_plane.clone()
        .unwrap_or_else(|| "http://localhost:7019".to_string());
    
    info!("Connecting to control plane at: {}", control_plane_endpoint);
    
    // Create control plane client
    let control_client = ControlPlaneClient::new(node_id, config).await
        .context("Failed to create control plane client")?;
    
    info!("✅ Connected to control plane successfully");
    
    Ok(control_client)
}

/// Start health reporting to control plane
async fn start_health_reporter(
    mut control_client: ControlPlaneClient,
    node_id: String,
    service_endpoint: String,
) {
    let mut ticker = interval(Duration::from_secs(30));
    
    loop {
        ticker.tick().await;
        
        // Health reporting disabled - control plane only provides service discovery now
        /*
        // Report health to control plane with endpoint metadata
        let mut metadata = std::collections::HashMap::new();
        metadata.insert("service_endpoint".to_string(), service_endpoint.clone());
        metadata.insert("status".to_string(), "accepting".to_string());
        
        // Create NodeHealthReport
        let health_report = openwit_proto::control::NodeHealthReport {
            node_id: node_id.clone(),
            node_role: "search".to_string(),
            cpu_percent: 0.0,
            memory_percent: 0.0,
            disk_percent: 0.0,
            is_healthy: true,
            timestamp: None,
            metadata,
        };
        
        match control_client.report_node_health(health_report).await {
            Ok(_) => {
                info!("Successfully reported search node health to control plane");
            }
            Err(e) => {
                warn!("Failed to report health to control plane: {}", e);
            }
        }
        */
    }
}

/// Main entry point for search service V2
pub async fn main_v2() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt::init();
    
    info!("Starting OpenWit Search Service V2");
    
    // Load configuration from file
    let config_path = std::env::var("CONFIG_PATH")
        .unwrap_or_else(|_| "/openwit/config/default.yaml".to_string());
    let config = UnifiedConfig::from_file(&config_path)?;
    
    // Get node ID
    let node_id = std::env::var("NODE_ID")
        .unwrap_or_else(|_| {
            let hostname = gethostname::gethostname();
            format!("search-{}", hostname.to_string_lossy())
        });
    
    // Get port from configuration first, then environment, then default
    let port = config.service_ports.search.service;
    let port = std::env::var("SEARCH_PORT")
        .ok()
        .and_then(|p| p.parse::<u16>().ok())
        .unwrap_or(port);
    
    // Run the service
    run_search_service_v2(config, node_id, port).await
}

/// Start HTTP server with V2 query engine
async fn start_http_server_v2(bind_addr: String, query_engine: Arc<QueryEngineV2>) -> Result<()> {
    
    // Get control plane URL from config (we'll need to pass this down)
    let control_plane_url = "http://localhost:7019"; // TODO: Get from config
    
    // Create batch monitoring router
    let batch_monitor_router = crate::batch_monitoring_api::create_batch_monitoring_router(
        control_plane_url.to_string()
    );
    
    // Create main router
    let app = Router::new()
        .route("/health", get(handle_health))
        .route("/search", post(handle_search_v2))
        .route("/search/azure", post(handle_azure_search))
        .layer(TraceLayer::new_for_http())
        .layer(CorsLayer::permissive())
        .with_state(query_engine);
    
    // Combine routers - batch monitoring doesn't need state
    let app = Router::new()
        .nest("/api", app)
        .nest("/monitor", batch_monitor_router);
    
    // Start server
    let listener = tokio::net::TcpListener::bind(&bind_addr)
        .await
        .with_context(|| format!("Failed to bind to {}", bind_addr))?;
    
    info!("Search HTTP server V2 listening on {}", bind_addr);
    
    axum::serve(listener, app)
        .await
        .context("Server error")?;
    
    Ok(())
}

/// Health check handler
async fn handle_health() -> impl IntoResponse {
    Json(serde_json::json!({
        "status": "healthy",
        "service": "openwit-search-v2"
    }))
}

/// Search handler for V2
async fn handle_search_v2(
    State(query_engine): State<Arc<QueryEngineV2>>,
    Json(query): Json<crate::types::SearchQuery>,
) -> Result<impl IntoResponse, AppError> {
    info!("[SEARCH V2] Received search query: {}", query.request_id);
    
    let response = query_engine
        .search(query)
        .await
        .map_err(|e| {
            error!("[SEARCH V2] Search query failed: {}", e);
            AppError::QueryError(e.to_string())
        })?;
    
    Ok(Json(response))
}

/// Azure search handler
async fn handle_azure_search(
    Json(query): Json<crate::types::SearchQuery>,
) -> Result<impl IntoResponse, AppError> {
    use crate::azure_query::{AzureQueryExecutor, AzureQueryConfig};
    
    info!("[AZURE SEARCH] Received search query: {}", query.request_id);
    info!("[AZURE SEARCH] SQL: {}", query.query);
    
    // Get Azure credentials from environment
    let account_name = std::env::var("AZURE_STORAGE_ACCOUNT")
        .map_err(|_| AppError::QueryError("AZURE_STORAGE_ACCOUNT not set".to_string()))?;
    let account_key = std::env::var("AZURE_STORAGE_KEY")
        .map_err(|_| AppError::QueryError("AZURE_STORAGE_KEY not set".to_string()))?;
    let container_name = std::env::var("AZURE_CONTAINER_NAME")
        .unwrap_or_else(|_| "openwit-data".to_string());
    let prefix = std::env::var("AZURE_PREFIX").ok();
    
    // Create Azure query executor
    let config = AzureQueryConfig {
        account_name,
        account_key,
        container_name,
        prefix,
    };
    
    let executor = AzureQueryExecutor::new(config).await
        .map_err(|e| AppError::QueryError(format!("Failed to connect to Azure: {}", e)))?;
    
    // Execute query
    let start = std::time::Instant::now();
    let results = executor
        .execute_query(&query.query, query.client_id.as_deref())
        .await
        .map_err(|e| AppError::QueryError(format!("Query failed: {}", e)))?;
    
    let execution_time = start.elapsed().as_millis() as i64;
    
    // Build response
    let search_result = crate::types::SearchResult {
        schema: None,
        schema_json: None,
        rows: results,
        total_count: None,
        has_more: false,
        record_batches: vec![],
    };
    
    let response = crate::types::SearchResponse {
        request_id: query.request_id,
        status: crate::types::QueryStatus::Success,
        result: Some(search_result),
        error: None,
        execution_time_ms: execution_time as u64,
        rows_scanned: 0,
        bytes_scanned: 0,
        partition_info: None,
    };
    
    info!("[AZURE SEARCH] Query completed: {} rows in {}ms", 
          response.result.as_ref().map(|r| r.rows.len()).unwrap_or(0), execution_time);
    
    Ok(Json(response))
}

/// Application error type
#[derive(Debug)]
enum AppError {
    QueryError(String),
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, error_message) = match self {
            AppError::QueryError(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg),
        };

        let body = Json(serde_json::json!({
            "error": error_message,
        }));

        (status, body).into_response()
    }
}