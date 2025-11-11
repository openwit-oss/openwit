pub mod otlp_handlers;
pub mod server;
pub mod metrics;
pub mod error;
pub mod control_plane_integration;
pub mod ingestion_client;

pub use server::HttpServer;
pub use error::{HttpError, Result};

// Re-export commonly used types
pub use otlp_handlers::{
    OtlpTracesRequest,
    OtlpLogsRequest,
    OtlpMetricsRequest,
    OtlpResponse,
};