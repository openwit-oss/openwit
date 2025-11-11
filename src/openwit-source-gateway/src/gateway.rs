use crate::errors::{GatewayError, ValidationError};
use crate::schemas::SchemaLoader;
use crate::validators::{SchemaValidator, LogsValidator, TracesValidator, MetricsValidator};
use openwit_config::UnifiedConfig;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, warn, error};
use serde_json::Value;

/// Client for forwarding validated data to ingestion
struct ForwardingClient {
    endpoint: String,
}

impl ForwardingClient {
    fn new(endpoint: String) -> Self {
        Self { endpoint }
    }
    
    async fn forward_data(&self, data_type: &str, data: Value) -> Result<(), String> {
        // For now, just log the forwarding attempt
        // TODO: Implement actual forwarding using HTTP client
        info!("Would forward {} data to {}", data_type, self.endpoint);
        Ok(())
    }
}

/// Main source gateway that validates and routes telemetry data
pub struct SourceGateway {
    config: Arc<UnifiedConfig>,
    schema_loader: Arc<RwLock<SchemaLoader>>,
    validators: Arc<RwLock<HashMap<String, Box<dyn SchemaValidator>>>>,
    forwarding_client: Option<ForwardingClient>,
}

impl SourceGateway {
    /// Create a new source gateway
    pub async fn new(config: Arc<UnifiedConfig>) -> Result<Self, GatewayError> {
        // Initialize schema loader
        let schema_dir = std::path::PathBuf::from("config/schemas");
        let mut schema_loader = SchemaLoader::new(schema_dir);
        schema_loader.load_schemas().await?;
        schema_loader.validate_required_schemas()?;
        
        let schema_loader = Arc::new(RwLock::new(schema_loader));
        
        // Initialize validators
        let validators = Arc::new(RwLock::new(HashMap::new()));
        
        // Create forwarding client if configured
        let forwarding_client = if let Ok(endpoint) = std::env::var("INGESTION_ENDPOINT") {
            info!("Created forwarding client to endpoint: {}", endpoint);
            Some(ForwardingClient::new(endpoint))
        } else {
            warn!("No INGESTION_ENDPOINT configured. Will validate only without forwarding.");
            None
        };
        
        let gateway = Self {
            config,
            schema_loader: schema_loader.clone(),
            validators: validators.clone(),
            forwarding_client,
        };
        
        // Initialize validators
        gateway.initialize_validators().await?;
        
        Ok(gateway)
    }
    
    /// Initialize validators from loaded schemas
    async fn initialize_validators(&self) -> Result<(), GatewayError> {
        let schema_loader = self.schema_loader.read().await;
        let mut validators = self.validators.write().await;
        
        // Create logs validator
        if let Some(schema) = schema_loader.get_schema("logs") {
            let validator = LogsValidator::new(schema.clone());
            validators.insert("logs".to_string(), Box::new(validator));
            info!("Initialized logs validator");
        }
        
        // Create traces validator
        if let Some(schema) = schema_loader.get_schema("traces") {
            let validator = TracesValidator::new(schema.clone());
            validators.insert("traces".to_string(), Box::new(validator));
            info!("Initialized traces validator");
        }
        
        // Create metrics validator
        if let Some(schema) = schema_loader.get_schema("metrics") {
            let validator = MetricsValidator::new(schema.clone());
            validators.insert("metrics".to_string(), Box::new(validator));
            info!("Initialized metrics validator");
        }
        
        if validators.is_empty() {
            return Err(GatewayError::SchemaLoadError(
                "No validators could be initialized".to_string()
            ));
        }
        
        info!("Initialized {} validators", validators.len());
        Ok(())
    }
    
    /// Validate and forward logs data
    pub async fn process_logs(&self, mut data: Value) -> Result<Value, GatewayError> {
        // Get validator
        let validators = self.validators.read().await;
        let validator = validators.get("logs")
            .ok_or_else(|| GatewayError::SchemaNotFound {
                schema_type: "logs".to_string()
            })?;
        
        // Preprocess
        validator.preprocess(&mut data).await?;
        
        // Validate
        validator.validate(&data).await?;
        
        // Postprocess
        validator.postprocess(&mut data).await?;
        
        // Forward to ingestion if client is available
        if let Some(client) = &self.forwarding_client {
            if let Err(e) = client.forward_data("logs", data.clone()).await {
                error!("Failed to forward logs to ingestion: {}", e);
                return Err(GatewayError::IngestionError(e));
            }
            info!("Successfully forwarded validated logs to ingestion");
        }
        
        Ok(serde_json::json!({
            "status": "success",
            "message": "Logs validated and forwarded successfully"
        }))
    }
    
    /// Validate and forward traces data
    pub async fn process_traces(&self, mut data: Value) -> Result<Value, GatewayError> {
        // Get validator
        let validators = self.validators.read().await;
        let validator = validators.get("traces")
            .ok_or_else(|| GatewayError::SchemaNotFound {
                schema_type: "traces".to_string()
            })?;
        
        // Preprocess
        validator.preprocess(&mut data).await?;
        
        // Validate
        validator.validate(&data).await?;
        
        // Postprocess
        validator.postprocess(&mut data).await?;
        
        // Forward to ingestion if client is available
        if let Some(client) = &self.forwarding_client {
            if let Err(e) = client.forward_data("traces", data.clone()).await {
                error!("Failed to forward traces to ingestion: {}", e);
                return Err(GatewayError::IngestionError(e));
            }
            info!("Successfully forwarded validated traces to ingestion");
        }
        
        Ok(serde_json::json!({
            "status": "success",
            "message": "Traces validated and forwarded successfully"
        }))
    }
    
    /// Validate and forward metrics data
    pub async fn process_metrics(&self, mut data: Value) -> Result<Value, GatewayError> {
        // Get validator
        let validators = self.validators.read().await;
        let validator = validators.get("metrics")
            .ok_or_else(|| GatewayError::SchemaNotFound {
                schema_type: "metrics".to_string()
            })?;
        
        // Preprocess
        validator.preprocess(&mut data).await?;
        
        // Validate
        validator.validate(&data).await?;
        
        // Postprocess  
        validator.postprocess(&mut data).await?;
        
        // Forward to ingestion if client is available
        if let Some(client) = &self.forwarding_client {
            if let Err(e) = client.forward_data("metrics", data.clone()).await {
                error!("Failed to forward metrics to ingestion: {}", e);
                return Err(GatewayError::IngestionError(e));
            }
            info!("Successfully forwarded validated metrics to ingestion");
        }
        
        Ok(serde_json::json!({
            "status": "success",
            "message": "Metrics validated and forwarded successfully"
        }))
    }
    
    /// Reload schemas (for hot-reloading)
    pub async fn reload_schemas(&self) -> Result<(), GatewayError> {
        info!("Reloading schemas...");
        
        // Reload schemas
        let mut schema_loader = self.schema_loader.write().await;
        schema_loader.reload_schemas().await?;
        
        // Reinitialize validators
        drop(schema_loader);
        self.initialize_validators().await?;
        
        info!("Schemas reloaded successfully");
        Ok(())
    }
    
    /// Get available schema types
    pub async fn get_schema_types(&self) -> Vec<String> {
        let schema_loader = self.schema_loader.read().await;
        schema_loader.get_schema_types()
    }
    
    /// Health check
    pub async fn health_check(&self) -> Result<serde_json::Value, GatewayError> {
        let validators = self.validators.read().await;
        let schema_loader = self.schema_loader.read().await;
        
        Ok(serde_json::json!({
            "status": "healthy",
            "validators_loaded": validators.len(),
            "schemas_loaded": schema_loader.get_schema_types(),
            "ingestion_connected": self.forwarding_client.is_some(),
        }))
    }
}