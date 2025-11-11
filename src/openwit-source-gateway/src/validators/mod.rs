pub mod logs;
pub mod traces;
pub mod metrics;
pub mod common;

pub use logs::LogsValidator;
pub use traces::TracesValidator;
pub use metrics::MetricsValidator;
pub use common::CommonValidator;

use crate::errors::ValidationError;
use serde_json::Value;
use async_trait::async_trait;

/// Trait for all schema validators
#[async_trait]
pub trait SchemaValidator: Send + Sync {
    /// Validate the input data against the schema
    async fn validate(&self, data: &Value) -> Result<(), ValidationError>;
    
    /// Get the schema type this validator handles
    fn schema_type(&self) -> &str;
    
    /// Pre-process data before validation (optional)
    async fn preprocess(&self, data: &mut Value) -> Result<(), ValidationError> {
        Ok(())
    }
    
    /// Post-process data after validation (optional)
    async fn postprocess(&self, data: &mut Value) -> Result<(), ValidationError> {
        Ok(())
    }
}