use anyhow::Result;
use axum::{
    routing::get,
    Router,
    response::{IntoResponse, Response},
    http::StatusCode,
    Json,
};
use tracing::info;

/// Handler for /metrics endpoint
async fn metrics_handler() -> impl IntoResponse {
    match crate::metrics::gather_metrics() {
        Ok(metrics) => Response::builder()
            .status(StatusCode::OK)
            .body(metrics)
            .unwrap(),
        Err(e) => Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body(format!("Error gathering metrics: {}", e))
            .unwrap(),
    }
}

/// Handler for /health endpoint
async fn health_handler() -> impl IntoResponse {
    let health = crate::metrics::HealthStatus::healthy(
        true, // accepting
        0,    // build_queue_depth
        0,    // combine_queue_depth
        std::time::Duration::from_secs(0),
    );
    Json(health)
}

/// Handler for /ready endpoint
async fn ready_handler() -> impl IntoResponse {
    (StatusCode::OK, "ready")
}

/// Start HTTP server for metrics and health endpoints
pub async fn start_metrics_server(addr: String) -> Result<()> {
    info!(addr = %addr, "Starting metrics HTTP server");

    let app = Router::new()
        .route("/metrics", get(metrics_handler))
        .route("/health", get(health_handler))
        .route("/ready", get(ready_handler));

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}