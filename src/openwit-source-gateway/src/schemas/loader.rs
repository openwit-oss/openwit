use super::{SchemaDefinition};
use crate::errors::GatewayError;
use std::path::{Path, PathBuf};
use std::collections::HashMap;
use tokio::fs;
use tracing::{info, warn, error};

pub struct SchemaLoader {
    schema_dir: PathBuf,
    schemas: HashMap<String, SchemaDefinition>,
}

impl SchemaLoader {
    pub fn new(schema_dir: impl AsRef<Path>) -> Self {
        Self {
            schema_dir: schema_dir.as_ref().to_path_buf(),
            schemas: HashMap::new(),
        }
    }
    
    /// Load all schema definitions from the schema directory
    pub async fn load_schemas(&mut self) -> Result<(), GatewayError> {
        info!("Loading schemas from: {:?}", self.schema_dir);
        
        // Ensure schema directory exists
        if !self.schema_dir.exists() {
            return Err(GatewayError::SchemaLoadError(
                format!("Schema directory not found: {:?}", self.schema_dir)
            ));
        }
        
        // Load each schema file
        let schema_files = vec!["logs.yaml", "traces.yaml", "metrics.yaml"];
        
        for file_name in schema_files {
            let file_path = self.schema_dir.join(file_name);
            
            if file_path.exists() {
                match self.load_schema_file(&file_path).await {
                    Ok(schema) => {
                        let schema_type = schema.schema_type.clone();
                        self.schemas.insert(schema_type.clone(), schema);
                        info!("Loaded {} schema from {:?}", schema_type, file_path);
                    }
                    Err(e) => {
                        error!("Failed to load schema from {:?}: {}", file_path, e);
                        return Err(e);
                    }
                }
            } else {
                warn!("Schema file not found: {:?}", file_path);
            }
        }
        
        if self.schemas.is_empty() {
            return Err(GatewayError::SchemaLoadError(
                "No valid schemas found".to_string()
            ));
        }
        
        info!("Loaded {} schemas successfully", self.schemas.len());
        Ok(())
    }
    
    /// Load a single schema file
    async fn load_schema_file(&self, path: &Path) -> Result<SchemaDefinition, GatewayError> {
        let content = fs::read_to_string(path)
            .await
            .map_err(|e| GatewayError::SchemaLoadError(
                format!("Failed to read schema file: {}", e)
            ))?;
            
        let schema: SchemaDefinition = serde_yaml::from_str(&content)
            .map_err(|e| GatewayError::SchemaLoadError(
                format!("Failed to parse schema YAML: {}", e)
            ))?;
            
        Ok(schema)
    }
    
    /// Get a schema by type
    pub fn get_schema(&self, schema_type: &str) -> Option<&SchemaDefinition> {
        self.schemas.get(schema_type)
    }
    
    /// Reload schemas (useful for hot-reloading)
    pub async fn reload_schemas(&mut self) -> Result<(), GatewayError> {
        self.schemas.clear();
        self.load_schemas().await
    }
    
    /// Get all loaded schema types
    pub fn get_schema_types(&self) -> Vec<String> {
        self.schemas.keys().cloned().collect()
    }
    
    /// Validate that all required schemas are loaded
    pub fn validate_required_schemas(&self) -> Result<(), GatewayError> {
        let required = vec!["logs", "traces", "metrics"];
        let missing: Vec<_> = required
            .into_iter()
            .filter(|&schema_type| !self.schemas.contains_key(schema_type))
            .collect();
            
        if !missing.is_empty() {
            return Err(GatewayError::SchemaLoadError(
                format!("Missing required schemas: {:?}", missing)
            ));
        }
        
        Ok(())
    }
}