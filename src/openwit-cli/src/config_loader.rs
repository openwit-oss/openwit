use std::path::PathBuf;
use anyhow::{Context, Result};
use openwit_config::UnifiedConfig;
use tracing::{info, debug, warn};

/// Configuration loader for OpenWit CLI
/// Handles loading unified configuration from default or custom locations
pub struct ConfigLoader {
    /// Default config path relative to the project root
    default_config_path: PathBuf,
}

impl ConfigLoader {
    /// Create a new ConfigLoader with default settings
    pub fn new() -> Self {
        Self {
            // Default path to the unified control config
            default_config_path: PathBuf::from("config/openwit-unified-control.yaml"),
        }
    }

    /// Load configuration from either the specified path or the default location
    /// 
    /// # Arguments
    /// * `custom_path` - Optional custom path to the configuration file
    /// 
    /// # Returns
    /// * `Result<UnifiedConfig>` - The loaded configuration or an error
    pub async fn load(&self, custom_path: Option<&str>) -> Result<UnifiedConfig> {
        let config_path = self.resolve_config_path(custom_path)?;
        
        info!("Loading configuration from: {}", config_path.display());
        
        // Load the configuration using UnifiedConfig's from_file method
        let config = UnifiedConfig::from_file(&config_path)
            .context("Failed to load unified configuration")?;
        
        // Validate the configuration
        self.validate_config(&config)?;
        
        info!("Congratulations! Configuration found and loaded successfully");
        debug!("Environment: {}", config.environment);
        debug!("Deployment mode: {:?}", config.deployment.mode);
        
        Ok(config)
    }
    
    /// Load configuration without validation - useful when CLI overrides will be applied
    /// 
    /// # Arguments
    /// * `custom_path` - Optional custom path to the configuration file
    /// 
    /// # Returns
    /// * `Result<UnifiedConfig>` - The loaded configuration or an error
    pub async fn load_without_validation(&self, custom_path: Option<&str>) -> Result<UnifiedConfig> {
        let config_path = self.resolve_config_path(custom_path)?;
        
        info!("Loading configuration from: {}", config_path.display());
        
        // Load the configuration using UnifiedConfig's from_yaml method
        // which handles parsing but we'll skip validation
        let content = std::fs::read_to_string(&config_path)
            .context("Failed to read configuration file")?;
        
        // Use the from_yaml_no_validation method to skip validation
        // since CLI will apply overrides and then validate
        let config = UnifiedConfig::from_yaml_no_validation(&content)
            .context("Failed to parse YAML configuration")?;
        
        info!("Configuration loaded successfully (validation deferred)");
        debug!("Environment: {}", config.environment);
        debug!("Deployment mode: {:?}", config.deployment.mode);
        
        Ok(config)
    }

    /// Resolve the configuration path
    /// Checks custom path first, then default path
    fn resolve_config_path(&self, custom_path: Option<&str>) -> Result<PathBuf> {
        if let Some(path) = custom_path {
            let custom = PathBuf::from(path);
            if custom.exists() {
                info!("Using custom configuration path: {}", custom.display());
                return Ok(custom);
            } else {
                return Err(anyhow::anyhow!(
                    "Custom configuration file not found: {}",
                    custom.display()
                ));
            }
        }

        // Try default path
        if self.default_config_path.exists() {
            info!("Using default configuration path: {}", self.default_config_path.display());
            return Ok(self.default_config_path.clone());
        }

        // Try from current directory
        let current_dir_config = PathBuf::from("openwit-unified-control.yaml");
        if current_dir_config.exists() {
            info!("Using configuration from current directory: {}", current_dir_config.display());
            return Ok(current_dir_config);
        }

        // Try from parent directories (useful when running from subdirectories)
        let mut current = std::env::current_dir()?;
        for _ in 0..5 {  // Check up to 5 parent directories
            let possible_path = current.join(&self.default_config_path);
            if possible_path.exists() {
                info!("Found configuration at: {}", possible_path.display());
                return Ok(possible_path);
            }
            if !current.pop() {
                break;
            }
        }

        Err(anyhow::anyhow!(
            "Configuration file not found. Searched:\n\
             - Custom path (if provided)\n\
             - Default: {}\n\
             - Current directory: openwit-unified-control.yaml\n\
             - Parent directories up to project root",
            self.default_config_path.display()
        ))
    }

    /// Validate the loaded configuration
    fn validate_config(&self, config: &UnifiedConfig) -> Result<()> {
        // Basic validation
        if config.environment.is_empty() {
            return Err(anyhow::anyhow!("Environment must be specified"));
        }

        // Validate deployment mode specific requirements
        match config.deployment.mode.as_str() {
            "monolith" => {
                debug!("Validating monolith configuration");
                // Monolith mode validation
            }
            "distributed" => {
                debug!("Validating distributed configuration");
                // Distributed mode requires networking configuration
                // Gossip functionality deprecated - distributed mode now uses simplified networking
                warn!("Distributed mode running with simplified networking (gossip deprecated)");
            }
            mode => {
                return Err(anyhow::anyhow!("Unknown deployment mode: {}", mode));
            }
        }

        // Validate ingestion configuration
        if config.ingestion.sources.grpc.enabled || config.ingestion.sources.kafka.enabled || config.ingestion.sources.http.enabled {
            debug!("At least one ingestion source is enabled");
        } else {
            warn!("No ingestion sources are enabled");
        }

        Ok(())
    }

    /// Get configuration with environment variable overrides without validation
    /// Allows overriding specific configuration values via environment variables
    pub async fn load_with_env_overrides_no_validation(&self, custom_path: Option<&str>) -> Result<UnifiedConfig> {
        let mut config = self.load_without_validation(custom_path).await?;
        
        // Apply environment variable overrides
        // Format: OPENWIT_<SECTION>_<KEY>=value
        
        // Example: OPENWIT_ENVIRONMENT=production
        if let Ok(env) = std::env::var("OPENWIT_ENVIRONMENT") {
            info!("Overriding environment from env var: {}", env);
            config.environment = env;
        }

        // Example: OPENWIT_DEPLOYMENT_MODE=distributed
        if let Ok(mode) = std::env::var("OPENWIT_DEPLOYMENT_MODE") {
            info!("Overriding deployment mode from env var: {}", mode);
            config.deployment.mode = mode;
        }

        // Example: OPENWIT_INGESTION_GRPC_ENABLED=true
        if let Ok(enabled) = std::env::var("OPENWIT_INGESTION_GRPC_ENABLED") {
            if let Ok(val) = enabled.parse::<bool>() {
                info!("Overriding gRPC ingestion enabled from env var: {}", val);
                config.ingestion.sources.grpc.enabled = val;
            }
        }

        // Example: OPENWIT_STORAGE_BACKEND=s3
        if let Ok(backend) = std::env::var("OPENWIT_STORAGE_BACKEND") {
            info!("Overriding storage backend from env var: {}", backend);
            config.storage.backend = backend;
        }

        Ok(config)
    }
    
    /// Get configuration with environment variable overrides
    /// Allows overriding specific configuration values via environment variables
    pub async fn load_with_env_overrides(&self, custom_path: Option<&str>) -> Result<UnifiedConfig> {
        let mut config = self.load(custom_path).await?;
        
        // Apply environment variable overrides
        // Format: OPENWIT_<SECTION>_<KEY>=value
        
        // Example: OPENWIT_ENVIRONMENT=production
        if let Ok(env) = std::env::var("OPENWIT_ENVIRONMENT") {
            info!("Overriding environment from env var: {}", env);
            config.environment = env;
        }

        // Example: OPENWIT_DEPLOYMENT_MODE=distributed
        if let Ok(mode) = std::env::var("OPENWIT_DEPLOYMENT_MODE") {
            info!("Overriding deployment mode from env var: {}", mode);
            config.deployment.mode = mode;
        }

        // Example: OPENWIT_INGESTION_GRPC_ENABLED=true
        if let Ok(enabled) = std::env::var("OPENWIT_INGESTION_GRPC_ENABLED") {
            if let Ok(val) = enabled.parse::<bool>() {
                info!("Overriding gRPC ingestion enabled from env var: {}", val);
                config.ingestion.sources.grpc.enabled = val;
            }
        }

        // Example: OPENWIT_STORAGE_BACKEND=s3
        if let Ok(backend) = std::env::var("OPENWIT_STORAGE_BACKEND") {
            info!("Overriding storage backend from env var: {}", backend);
            config.storage.backend = backend;
        }

        Ok(config)
    }

    /// Create a sample configuration file at the specified path
    pub fn create_sample_config(path: &str) -> Result<()> {
        use std::fs;
        
        let sample_config = r#"# OpenWit Unified Configuration
# This is a sample configuration file for OpenWit

environment: development

deployment:
  mode: monolith  # Options: monolith, distributed
  kubernetes:
    enabled: false
    namespace: openwit
    headless_service: openwit-headless

networking:
  gossip:
    enabled: false
    listen_addr: "0.0.0.0:7946"
    seeds: []

ingestion:
  grpc:
    enabled: true
    bind: "0.0.0.0:4317"
    max_message_size: 4194304  # 4MB
  
  kafka:
    enabled: false
    brokers: "localhost:9092"
    topics:
      - logs
      - traces
      - metrics
    consumer_group: openwit-ingestion
  
  http:
    enabled: false
    bind: "0.0.0.0:8080"

storage:
  backend: local  # Options: local, s3, azure
  local:
    path: ./data/storage
  
  s3:
    bucket: openwit-data
    region: us-east-1
    endpoint: null  # Use AWS default
  
  azure:
    container: openwit-data
    account_name: ""
    account_key: ""

indexing:
  enabled: true
  backend: tantivy  # Options: tantivy, bleve
  path: ./data/index
  
  settings:
    max_merge_threads: 2
    commit_interval_seconds: 10

search:
  enabled: true
  bind: "0.0.0.0:9001"
  
  cache:
    enabled: true
    size_mb: 256
    ttl_seconds: 300

metastore:
  backend: sled  # Options: sled, postgres
  sled:
    path: ./data/metastore
  
  postgres:
    connection_string: "postgresql://user:password@localhost/openwit"
    max_connections: 10

observability:
  prometheus:
    enabled: true
    bind: "0.0.0.0:9090"
  
  tracing:
    enabled: false
    endpoint: "http://localhost:4318"
    service_name: openwit

performance:
  buffer_size_mb: 256
  batch_size: 1000
  batch_timeout_ms: 100
  wal_enabled: true
  wal_path: ./data/wal
"#;

        fs::write(path, sample_config)
            .context("Failed to write sample configuration file")?;
        
        info!("Sample configuration file created at: {}", path);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_config_loader_creation() {
        let loader = ConfigLoader::new();
        assert_eq!(
            loader.default_config_path,
            PathBuf::from("config/openwit-unified-control.yaml")
        );
    }

    #[test]
    fn test_create_sample_config() {
        use tempfile::NamedTempFile;
        
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();
        
        ConfigLoader::create_sample_config(path).unwrap();
        
        // Verify file was created and contains expected content
        let content = std::fs::read_to_string(path).unwrap();
        assert!(content.contains("environment: development"));
        assert!(content.contains("deployment:"));
        assert!(content.contains("ingestion:"));
    }
}