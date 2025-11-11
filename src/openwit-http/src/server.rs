use std::net::SocketAddr;
use std::sync::Arc;
use axum::{
    routing::{get, post},
    Router,
    response::IntoResponse,
    http::StatusCode,
    Json,
};
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;
use tokio::net::TcpListener;
use tokio::sync::mpsc::Sender;
use tracing::{info, error};

use crate::otlp_handlers::{handle_traces, handle_logs, handle_metrics, HandlerState};
use crate::error::Result;
use crate::ingestion_client::{create_ingestion_client};
use openwit_config::UnifiedConfig;

pub struct HttpServer {
    config: Arc<UnifiedConfig>,
    bind_addr: SocketAddr,
    ingest_tx: Option<Sender<openwit_ingestion::IngestedMessage>>,
}

impl HttpServer {
    pub fn new(
        config: UnifiedConfig,
        ingest_tx: Option<Sender<openwit_ingestion::IngestedMessage>>,
    ) -> Result<Self> {
        let bind_addr = config.ingestion.http.bind.parse()
            .map_err(|e| anyhow::anyhow!("Invalid HTTP bind address: {}", e))?;
        
        Ok(Self {
            config: Arc::new(config),
            bind_addr,
            ingest_tx,
        })
    }
    
    pub fn bind_address(&self) -> &SocketAddr {
        &self.bind_addr
    }
    
    pub fn config(&self) -> &UnifiedConfig {
        &self.config
    }
    
    pub async fn start(self) -> Result<()> {
        info!("Starting OpenWit HTTP ingestion server on {}", self.bind_addr);
        
        // Create ingestion client for forwarding to ingestion nodes
        let node_id = format!("http-node-{}", self.bind_addr.port());
        let ingestion_client = create_ingestion_client(&self.config, node_id).await?;
        
        if ingestion_client.is_some() {
            info!("HTTP node will forward data to ingestion nodes via gRPC");
        } else {
            info!("HTTP node will process data locally");
        }
        
        let state = HandlerState::new(self.ingest_tx, ingestion_client);
        
        let app = Router::new()
            // OTLP endpoints
            .route("/v1/traces", post(handle_traces))
            .route("/v1/logs", post(handle_logs))
            .route("/v1/metrics", post(handle_metrics))
            // Health check
            .route("/health", get(health_check))
            .route("/healthz", get(health_check))
            // Metrics endpoint
            .route("/metrics", get(metrics_handler))
            // Add state
            .with_state(state)
            // Add middleware
            .layer(CorsLayer::permissive())
            .layer(TraceLayer::new_for_http());
        
        let listener = TcpListener::bind(&self.bind_addr).await
            .map_err(|e| anyhow::anyhow!("Failed to bind to {}: {}", self.bind_addr, e))?;
        
        info!("OpenWit HTTP server listening on {}", self.bind_addr);
        
        axum::serve(listener, app)
            .await
            .map_err(|e| anyhow::anyhow!("HTTP server error: {}", e))?;
        
        Ok(())
    }
}

async fn health_check() -> impl IntoResponse {
    Json(serde_json::json!({
        "status": "healthy",
        "service": "openwit-http"
    }))
}

async fn metrics_handler() -> impl IntoResponse {
    use prometheus::{TextEncoder, Encoder};
    
    let encoder = TextEncoder::new();
    let metric_families = prometheus::gather();
    let mut buffer = vec![];
    
    if let Err(e) = encoder.encode(&metric_families, &mut buffer) {
        error!("Failed to encode metrics: {}", e);
        return (StatusCode::INTERNAL_SERVER_ERROR, "Failed to encode metrics").into_response();
    }
    
    let output = String::from_utf8(buffer).unwrap_or_else(|_| String::from("Failed to convert metrics to string"));
    
    (StatusCode::OK, output).into_response()
}