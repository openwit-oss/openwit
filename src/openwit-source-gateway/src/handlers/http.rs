use crate::gateway::SourceGateway;
use crate::errors::GatewayError;
use axum::{
    extract::{State, Path},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde_json::Value;
use std::sync::Arc;
use tower_http::cors::CorsLayer;
use tracing::info;

/// Application state
#[derive(Clone)]
pub struct AppState {
    pub gateway: Arc<SourceGateway>,
}

/// Create HTTP routes for the source gateway
pub fn create_http_routes(gateway: Arc<SourceGateway>) -> Router {
    let state = AppState { gateway };
    
    Router::new()
        // OTLP endpoints
        .route("/v1/logs", post(handle_logs))
        .route("/v1/traces", post(handle_traces))
        .route("/v1/metrics", post(handle_metrics))
        
        // Health and admin endpoints
        .route("/health", get(health_check))
        .route("/ready", get(ready_check))
        .route("/schemas", get(list_schemas))
        .route("/schemas/:type", get(get_schema))
        .route("/admin/reload", post(reload_schemas))
        
        // Add state and middleware
        .with_state(state)
        .layer(CorsLayer::permissive())
}

/// Handle logs endpoint
async fn handle_logs(
    State(state): State<AppState>,
    Json(payload): Json<Value>,
) -> Result<impl IntoResponse, GatewayError> {
    info!("Received logs request");
    let result = state.gateway.process_logs(payload).await?;
    Ok(Json(result))
}

/// Handle traces endpoint
async fn handle_traces(
    State(state): State<AppState>,
    Json(payload): Json<Value>,
) -> Result<impl IntoResponse, GatewayError> {
    info!("Received traces request");
    let result = state.gateway.process_traces(payload).await?;
    Ok(Json(result))
}

/// Handle metrics endpoint
async fn handle_metrics(
    State(state): State<AppState>,
    Json(payload): Json<Value>,
) -> Result<impl IntoResponse, GatewayError> {
    info!("Received metrics request");
    let result = state.gateway.process_metrics(payload).await?;
    Ok(Json(result))
}

/// Health check endpoint
async fn health_check(
    State(state): State<AppState>,
) -> Result<impl IntoResponse, GatewayError> {
    let health = state.gateway.health_check().await?;
    Ok(Json(health))
}

/// Ready check endpoint
async fn ready_check(
    State(state): State<AppState>,
) -> Result<impl IntoResponse, GatewayError> {
    // Check if gateway is ready by attempting to get schema types
    let schema_types = state.gateway.get_schema_types().await;
    
    if schema_types.is_empty() {
        return Err(GatewayError::ServiceUnavailable);
    }
    
    Ok(Json(serde_json::json!({
        "status": "ready",
        "schemas_loaded": schema_types.len()
    })))
}

/// List available schemas
async fn list_schemas(
    State(state): State<AppState>,
) -> Result<impl IntoResponse, GatewayError> {
    let schema_types = state.gateway.get_schema_types().await;
    
    Ok(Json(serde_json::json!({
        "schemas": schema_types,
        "endpoints": {
            "logs": "/v1/logs",
            "traces": "/v1/traces",
            "metrics": "/v1/metrics"
        }
    })))
}

/// Get schema details (placeholder for now)
async fn get_schema(
    Path(schema_type): Path<String>,
    State(_state): State<AppState>,
) -> Result<impl IntoResponse, GatewayError> {
    // TODO: Implement schema retrieval
    Ok(Json(serde_json::json!({
        "type": schema_type,
        "message": "Schema details endpoint not yet implemented"
    })))
}

/// Reload schemas
async fn reload_schemas(
    State(state): State<AppState>,
) -> Result<impl IntoResponse, GatewayError> {
    info!("Schema reload requested");
    state.gateway.reload_schemas().await?;
    
    Ok(Json(serde_json::json!({
        "status": "success",
        "message": "Schemas reloaded successfully"
    })))
}

/// Example validation response with detailed errors
pub fn create_validation_error_response(errors: Vec<String>) -> impl IntoResponse {
    (
        StatusCode::BAD_REQUEST,
        Json(serde_json::json!({
            "status": "validation_failed",
            "errors": errors,
            "suggestion": "Please check your data against the schema requirements"
        }))
    )
}