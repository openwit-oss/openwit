use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum HttpError {
    #[error("Invalid request: {0}")]
    InvalidRequest(String),
    
    #[error("Internal server error: {0}")]
    InternalError(String),
    
    #[error("Service unavailable")]
    ServiceUnavailable,
    
    #[error("Ingestion error: {0}")]
    IngestionError(String),
    
    #[error(transparent)]
    Other(#[from] anyhow::Error),
    
    #[error("JSON error: {0}")]
    JsonError(#[from] serde_json::Error),
}

pub type Result<T> = std::result::Result<T, HttpError>;

impl IntoResponse for HttpError {
    fn into_response(self) -> Response {
        let (status, error_message) = match self {
            HttpError::InvalidRequest(msg) => (StatusCode::BAD_REQUEST, msg),
            HttpError::InternalError(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg),
            HttpError::ServiceUnavailable => (StatusCode::SERVICE_UNAVAILABLE, "Service unavailable".to_string()),
            HttpError::IngestionError(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg),
            HttpError::Other(err) => (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
            HttpError::JsonError(err) => (StatusCode::BAD_REQUEST, err.to_string()),
        };

        let body = Json(json!({
            "status": "error",
            "message": error_message,
        }));

        (status, body).into_response()
    }
}