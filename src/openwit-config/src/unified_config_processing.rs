use crate::unified_config::*;
use crate::unified::validation::Validatable;
use crate::unified::config_validator::ConfigValidator;
use std::fs;
use std::path::Path;
use thiserror::Error;
use tracing::{info, warn};

#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("Failed to read config file: {0}")]
    FileReadError(#[from] std::io::Error),
    
    #[error("Failed to parse YAML: {0}")]
    YamlParseError(#[from] serde_yaml::Error),
    
    #[error("Config validation error: {0}")]
    ValidationError(String),
    
    #[error("Environment variable not found: {0}")]
    EnvVarError(String),
}

pub type Result<T> = std::result::Result<T, ConfigError>;

impl UnifiedConfig {
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let content = fs::read_to_string(path)?;
        Self::from_yaml(&content)
    }
    
    pub fn from_file_safe_mode<P: AsRef<Path>>(path: P) -> Result<Self> {
        info!("Loading configuration in SAFE MODE - applying conservative defaults");
        let content = fs::read_to_string(path)?;
        Self::from_yaml_safe_mode(&content)
    }
    
    pub fn from_yaml(yaml_content: &str) -> Result<Self> {
        let expanded_content = expand_env_vars(yaml_content)?;
        let mut config: UnifiedConfig = serde_yaml::from_str(&expanded_content)?;
        
        // Basic validation
        config.validate()?;
        
        // Limit validation
        config.validate_and_log_limits();
        
        // Comprehensive validation with conflict detection
        let mut validator = ConfigValidator::new();
        if !validator.validate_comprehensive(&config) {
            return Err(ConfigError::ValidationError(
                "Configuration validation failed. Please check the logs for details.".to_string()
            ));
        }
        
        Ok(config)
    }
    
    pub fn from_yaml_safe_mode(yaml_content: &str) -> Result<Self> {
        let expanded_content = expand_env_vars(yaml_content)?;
        let mut config: UnifiedConfig = serde_yaml::from_str(&expanded_content)?;
        config.apply_safe_mode();
        
        // Basic validation
        config.validate()?;
        
        // Comprehensive validation with conflict detection
        let mut validator = ConfigValidator::new();
        if !validator.validate_comprehensive(&config) {
            return Err(ConfigError::ValidationError(
                "Configuration validation failed. Please check the logs for details.".to_string()
            ));
        }
        
        Ok(config)
    }
    
    pub fn validate(&self) -> Result<()> {
        // No longer validating mandatory nodes - they are optional
        
        // Validate storage backend
        match self.storage.backend.as_str() {
            "azure" => {
                if !self.storage.azure.enabled {
                    return Err(ConfigError::ValidationError(
                        "Azure storage backend selected but not enabled".to_string()
                    ));
                }
            }
            "s3" => {
                if !self.storage.s3.enabled {
                    return Err(ConfigError::ValidationError(
                        "S3 storage backend selected but not enabled".to_string()
                    ));
                }
            }
            "gcs" => {
                if !self.storage.gcs.enabled {
                    return Err(ConfigError::ValidationError(
                        "GCS storage backend selected but not enabled".to_string()
                    ));
                }
            }
            "local" => {
                if !self.storage.local.enabled {
                    return Err(ConfigError::ValidationError(
                        "Local storage backend selected but not enabled".to_string()
                    ));
                }
            }
            _ => {
                return Err(ConfigError::ValidationError(
                    format!("Invalid storage backend: {}", self.storage.backend)
                ));
            }
        }
        
        // Validate ingestion sources - at least one should be enabled
        if !self.ingestion.sources.kafka.enabled &&
           !self.ingestion.sources.grpc.enabled &&
           !self.ingestion.sources.http.enabled {
            return Err(ConfigError::ValidationError(
                "At least one ingestion source must be enabled".to_string()
            ));
        }
        
        Ok(())
    }
    
    pub fn to_yaml(&self) -> Result<String> {
        Ok(serde_yaml::to_string(self)?)
    }
    
    /// Load from YAML without any validation - for CLI use when overrides will be applied
    pub fn from_yaml_no_validation(yaml_content: &str) -> Result<Self> {
        let expanded_content = expand_env_vars(yaml_content)?;
        let config: UnifiedConfig = serde_yaml::from_str(&expanded_content)?;
        Ok(config)
    }
    
    /// Validate all configuration limits and log warnings/errors
    pub fn validate_and_log_limits(&mut self) {
        info!("Validating configuration limits...");
        
        // Validate ingestion config
        if self.ingestion.kafka.brokers.is_some() {
            let validation_results = self.ingestion.kafka.validate();
            for result in validation_results {
                result.log();
            }
        }
        
        // Validate processing config if present
        if let Some(ref processing) = self.processing {
            let buffer_results = processing.buffer.validate();
            for result in buffer_results {
                result.log();
            }

            let lsm_results = processing.lsm_engine.validate();
            for result in lsm_results {
                result.log();
            }
        }
        
        // Validate control plane config
        let control_results = self.control_plane.validate();
        for result in control_results {
            result.log();
        }
        
        // Validate memory config
        let memory_results = self.memory.validate();
        for result in memory_results {
            result.log();
        }
        
        // Validate storage config
        let storage_results = self.storage.validate();
        for result in storage_results {
            result.log();
        }
        
        info!("Configuration validation complete");
    }
    
    /// Apply safe mode defaults to all configuration sections
    pub fn apply_safe_mode(&mut self) {
        warn!("SAFE MODE ENABLED - Applying conservative configuration defaults");
        
        // Apply safe defaults to ingestion
        self.ingestion.kafka.apply_safe_defaults();
        
        // Apply safe defaults to processing if present
        if let Some(ref mut processing) = self.processing {
            processing.buffer.apply_safe_defaults();
            processing.lsm_engine.apply_safe_defaults();
            processing.pipeline.apply_safe_defaults();
        }
        
        // Apply safe defaults to control plane
        self.control_plane.apply_safe_defaults();
        
        // Apply safe defaults to memory
        self.memory.apply_safe_defaults();
        
        // Apply safe defaults to storage
        self.storage.apply_safe_defaults();
        
        info!("Safe mode configuration applied successfully");
    }
}

fn expand_env_vars(content: &str) -> Result<String> {
    let mut result = content.to_string();
    let env_var_pattern = regex::Regex::new(r"\$\{([^}]+)\}").unwrap();
    
    for cap in env_var_pattern.captures_iter(content) {
        let var_name = &cap[1];
        let placeholder = format!("${{{}}}", var_name);
        
        match std::env::var(var_name) {
            Ok(env_value) => {
                result = result.replace(&placeholder, &env_value);
            }
            Err(_) => {
                // For missing environment variables, replace with empty string
                // This allows the configuration to load with defaults
                warn!("Environment variable '{}' not found, using empty string", var_name);
                result = result.replace(&placeholder, "");
            }
        }
    }
    
    Ok(result)
}