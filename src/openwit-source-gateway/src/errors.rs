use thiserror::Error;
use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Serialize;

#[derive(Error, Debug)]
pub enum ValidationError {
    #[error("Missing required field: {field}")]
    MissingRequiredField { field: String },
    
    #[error("Invalid field type for {field}: expected {expected}, got {actual}")]
    InvalidFieldType {
        field: String,
        expected: String,
        actual: String,
    },
    
    #[error("Field {field} exceeds maximum length: {max_length}")]
    FieldTooLong { field: String, max_length: usize },
    
    #[error("Field {field} does not match pattern: {pattern}")]
    PatternMismatch { field: String, pattern: String },
    
    #[error("Invalid enum value for {field}: {value}")]
    InvalidEnumValue { field: String, value: String },
    
    #[error("Timestamp {field} is invalid: {reason}")]
    InvalidTimestamp { field: String, reason: String },
    
    #[error("Attribute count exceeds maximum: {max}")]
    TooManyAttributes { max: usize },
    
    #[error("Batch size exceeds maximum: {max_size} bytes")]
    BatchTooLarge { max_size: usize },
    
    #[error("Rate limit exceeded: {limit} per second")]
    RateLimitExceeded { limit: String },
    
    #[error("Invalid trace ID format")]
    InvalidTraceId,
    
    #[error("Invalid span ID format")]
    InvalidSpanId,
    
    #[error("Schema validation failed: {0}")]
    SchemaValidation(String),
    
    #[error("Multiple validation errors: {0:?}")]
    Multiple(Vec<ValidationError>),
}

#[derive(Error, Debug)]
pub enum GatewayError {
    #[error("Validation failed: {0}")]
    Validation(#[from] ValidationError),
    
    #[error("Schema not found: {schema_type}")]
    SchemaNotFound { schema_type: String },
    
    #[error("Failed to load schema: {0}")]
    SchemaLoadError(String),
    
    #[error("Failed to parse request: {0}")]
    ParseError(String),
    
    #[error("Failed to forward to ingestion: {0}")]
    IngestionError(String),
    
    #[error("Internal error: {0}")]
    Internal(String),
    
    #[error("Unsupported content type: {0}")]
    UnsupportedContentType(String),
    
    #[error("Service unavailable")]
    ServiceUnavailable,
}

#[derive(Serialize)]
struct ErrorResponse {
    error: String,
    details: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    field: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    suggestion: Option<String>,
}

impl IntoResponse for GatewayError {
    fn into_response(self) -> Response {
        let (status, error_response) = match self {
            GatewayError::Validation(ref validation_error) => {
                let (details, field, suggestion) = match validation_error {
                    ValidationError::MissingRequiredField { field } => {
                        (None, Some(field.clone()), Some("Add the required field to your request".to_string()))
                    }
                    ValidationError::InvalidFieldType { field, expected, .. } => {
                        (None, Some(field.clone()), Some(format!("Change field type to {}", expected)))
                    }
                    ValidationError::FieldTooLong { field, max_length } => {
                        (None, Some(field.clone()), Some(format!("Reduce field length to {} characters or less", max_length)))
                    }
                    ValidationError::Multiple(errors) => {
                        let details = errors.iter().map(|e| e.to_string()).collect();
                        (Some(details), None, None)
                    }
                    _ => (None, None, None),
                };
                
                (
                    StatusCode::BAD_REQUEST,
                    ErrorResponse {
                        error: self.to_string(),
                        details,
                        field,
                        suggestion,
                    },
                )
            }
            GatewayError::SchemaNotFound { .. } => (
                StatusCode::NOT_FOUND,
                ErrorResponse {
                    error: self.to_string(),
                    details: None,
                    field: None,
                    suggestion: Some("Check available schema types: logs, traces, metrics".to_string()),
                },
            ),
            GatewayError::ParseError(_) => (
                StatusCode::BAD_REQUEST,
                ErrorResponse {
                    error: self.to_string(),
                    details: None,
                    field: None,
                    suggestion: Some("Ensure your request body is valid JSON".to_string()),
                },
            ),
            GatewayError::UnsupportedContentType(_) => (
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                ErrorResponse {
                    error: self.to_string(),
                    details: None,
                    field: None,
                    suggestion: Some("Use Content-Type: application/json".to_string()),
                },
            ),
            GatewayError::ServiceUnavailable => (
                StatusCode::SERVICE_UNAVAILABLE,
                ErrorResponse {
                    error: self.to_string(),
                    details: None,
                    field: None,
                    suggestion: Some("Ingestion service is temporarily unavailable. Please try again later.".to_string()),
                },
            ),
            _ => (
                StatusCode::INTERNAL_SERVER_ERROR,
                ErrorResponse {
                    error: "Internal server error".to_string(),
                    details: None,
                    field: None,
                    suggestion: None,
                },
            ),
        };
        
        (status, Json(error_response)).into_response()
    }
}