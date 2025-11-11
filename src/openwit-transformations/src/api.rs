//! REST API for SQL transformation

use axum::{
    http::StatusCode,
    response::Json,
    routing::post,
    Router,
};
use serde::{Deserialize, Serialize};
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;

use crate::{transform_sql, logical_plan::LogicalQuery, error::QueryError, tantivy_converter};

/// API request structure
#[derive(Debug, Deserialize, Serialize)]
pub struct TransformRequest {
    /// SQL query to transform
    pub sql: String,
    
    /// Optional: return only validation result
    pub validate_only: Option<bool>,
    
    /// Optional: include index hints
    pub include_hints: Option<bool>,
}

/// API response structure
#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum TransformResponse {
    /// Successful transformation
    Success {
        #[serde(skip_serializing_if = "Option::is_none")]
        sql: Option<String>,
        logical_query: LogicalQuery,
        tantivy_query: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        validation: Option<ValidationResult>,
    },
    
    /// Error response
    Error {
        error: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        details: Option<ErrorDetails>,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ValidationResult {
    pub valid: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub warnings: Option<Vec<String>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ErrorDetails {
    pub error_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub position: Option<usize>,
}

/// Create the API router
pub fn create_router() -> Router {
    Router::new()
        .route("/transform", post(transform_handler))
        .route("/health", axum::routing::get(health_check))
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
}

/// Transform SQL endpoint handler
async fn transform_handler(
    Json(request): Json<TransformRequest>,
) -> Result<Json<TransformResponse>, StatusCode> {
    // Log the incoming request
    tracing::info!("Transforming SQL: {}", request.sql);
    
    match transform_sql(&request.sql).await {
        Ok(logical_query) => {
            // Convert to Tantivy query
            let tantivy_query = tantivy_converter::to_tantivy_query(&logical_query)
                .unwrap_or_else(|e| format!("Error converting to Tantivy: {}", e));
            
            let response = TransformResponse::Success {
                sql: Some(request.sql.clone()),
                logical_query,
                tantivy_query,
                validation: Some(ValidationResult {
                    valid: true,
                    warnings: None,
                }),
            };
            Ok(Json(response))
        }
        Err(err) => {
            let (error_type, _) = categorize_error(&err);
            
            let response = TransformResponse::Error {
                error: err.to_string(),
                details: Some(ErrorDetails {
                    error_type,
                    field: extract_field_from_error(&err),
                    position: None,
                }),
            };
            
            tracing::warn!("Transform error: {:?}", err);
            Ok(Json(response))
        }
    }
}

/// Health check endpoint
async fn health_check() -> &'static str {
    "OK"
}

/// Categorize errors for better client handling
fn categorize_error(err: &QueryError) -> (String, Option<String>) {
    match err {
        QueryError::ParseError(_) => ("parse_error".to_string(), None),
        QueryError::UnknownField(field) => ("unknown_field".to_string(), Some(field.clone())),
        QueryError::UnknownTable(table) => ("unknown_table".to_string(), Some(table.clone())),
        QueryError::InvalidFunctionUsage { function, .. } => ("invalid_function".to_string(), Some(function.clone())),
        QueryError::TypeMismatch { .. } => ("type_mismatch".to_string(), None),
        QueryError::GroupingError(_) => ("grouping_error".to_string(), None),
        QueryError::SubqueriesNotSupported => ("unsupported_feature".to_string(), Some("subquery".to_string())),
        QueryError::JoinsNotSupported => ("unsupported_feature".to_string(), Some("join".to_string())),
        _ => ("unknown_error".to_string(), None),
    }
}

/// Extract field name from error if applicable
fn extract_field_from_error(err: &QueryError) -> Option<String> {
    match err {
        QueryError::UnknownField(field) => Some(field.clone()),
        QueryError::InvalidFunctionUsage { function, .. } => Some(function.clone()),
        _ => None,
    }
}

/// Run the API server
pub async fn run_server(port: u16) -> Result<(), Box<dyn std::error::Error>> {
    let app = create_router();
    
    let addr = format!("0.0.0.0:{}", port);
    tracing::info!("SQL Transformation API listening on {}", addr);
    
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;
    
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    #[tokio::test]
    async fn test_transform_endpoint() {
        let app = create_router();
        
        let request = TransformRequest {
            sql: "SELECT level FROM logs WHERE service = 'auth'".to_string(),
            validate_only: None,
            include_hints: Some(true),
        };
        
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/transform")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&request).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_invalid_sql() {
        let app = create_router();
        
        let request = TransformRequest {
            sql: "SELECT FROM WHERE".to_string(),
            validate_only: None,
            include_hints: None,
        };
        
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/transform")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&request).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        
        assert_eq!(response.status(), StatusCode::OK); // We return errors in JSON
        
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let response: TransformResponse = serde_json::from_slice(&body).unwrap();
        
        match response {
            TransformResponse::Error { error, .. } => {
                assert!(error.contains("Parse error"));
            }
            _ => panic!("Expected error response"),
        }
    }
}