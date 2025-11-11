//! Axum router that exposes Prometheus metrics at `/metrics`.

use axum::{response::IntoResponse, routing::get, Router};
use axum::http::{header::CONTENT_TYPE, HeaderValue, StatusCode};
use prometheus::{Encoder, TextEncoder};

use super::REGISTRY;

/// Build a router that serves `GET /metrics` in Prometheus text format.
///
/// # Example
///
/// ```ignore
/// let app = openwit_metrics::exporter::router();
/// axum::Server::bind(&"0.0.0.0:9000".parse().unwrap())
///     .serve(app.into_make_service())
///     .await?;
/// ```
pub fn router() -> Router {
    Router::new().route("/metrics", get(metrics_handler))
}

async fn metrics_handler() -> impl IntoResponse {
    let encoder = TextEncoder::new();
    let families = REGISTRY.gather();

    let mut buf = Vec::with_capacity(8 * 1024);
    encoder.encode(&families, &mut buf).unwrap();

    (
        StatusCode::OK,
        [(CONTENT_TYPE, HeaderValue::from_static("text/plain; version=0.0.4; charset=utf-8"))],
        buf,
    )
}