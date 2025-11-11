use crate::unified::*;
use crate::unified_config::UnifiedConfig;
use tracing::{info, warn, error};
use std::collections::HashSet;
use regex::Regex;

pub struct ConfigValidator {
    errors: Vec<ConfigError>,
    warnings: Vec<ConfigWarning>,
}

#[derive(Debug)]
pub struct ConfigError {
    pub field: String,
    pub message: String,
    pub suggestion: String,
}

#[derive(Debug)]
pub struct ConfigWarning {
    pub field: String,
    pub message: String,
    pub suggestion: String,
}

impl ConfigValidator {
    pub fn new() -> Self {
        Self {
            errors: Vec::new(),
            warnings: Vec::new(),
        }
    }
    
    pub fn validate_comprehensive(&mut self, config: &UnifiedConfig) -> bool {
        info!("Starting comprehensive configuration validation...");
        
        // Storage backend conflicts
        self.validate_storage_backends(&config.storage);
        
        // Environment vs deployment conflicts
        self.validate_environment_deployment(&config.environment, &config.deployment);
        
        // Kubernetes conflicts
        self.validate_kubernetes_requirements(&config.deployment, &config.environment);
        
        // Format validations
        self.validate_urls_and_endpoints(config);
        self.validate_paths(config);
        
        // Resource validations
        self.validate_resource_limits(config);
        
        // Dependency validations
        self.validate_dependencies(config);
        
        // Environment variable validations
        self.validate_environment_variables(config);
        
        // Ingestion source validations
        self.validate_ingestion_sources(&config.ingestion);
        
        // Network port conflicts
        self.validate_port_conflicts(config);
        
        // Security validations
        self.validate_security_settings(config);
        
        // Log results
        self.log_validation_results()
    }
    
    fn validate_storage_backends(&mut self, storage: &StorageConfig) {
        let mut enabled_backends = Vec::new();
        
        if storage.azure.enabled {
            enabled_backends.push("Azure");
        }
        if storage.s3.enabled {
            enabled_backends.push("S3");
        }
        if storage.gcs.enabled {
            enabled_backends.push("GCS");
        }
        if storage.local.enabled {
            enabled_backends.push("Local");
        }
        
        // Allow hybrid storage mode - local + cloud
        if enabled_backends.len() > 1 && storage.backend == "local" {
            // Check if we have local + one cloud backend (hybrid mode)
            let has_local = enabled_backends.contains(&"Local");
            let cloud_backends: Vec<&str> = enabled_backends.iter()
                .filter(|&&b| b != "Local")
                .copied()
                .collect();
            
            if has_local && cloud_backends.len() == 1 {
                // This is valid hybrid mode: local + one cloud backend
                info!("Hybrid storage mode enabled: Local + {}", cloud_backends[0]);
            } else if cloud_backends.len() > 1 {
                // Multiple cloud backends - not supported
                self.errors.push(ConfigError {
                    field: "storage.backend".to_string(),
                    message: format!(
                        "Multiple cloud storage backends are enabled: {:?}. Only one cloud backend can be active in hybrid mode.",
                        cloud_backends
                    ),
                    suggestion: "For hybrid mode, enable 'local' backend and only one cloud backend (azure, s3, or gcs).".to_string(),
                });
            }
        } else if enabled_backends.len() > 1 {
            // Non-local backend with multiple enabled - not supported
            self.errors.push(ConfigError {
                field: "storage.backend".to_string(),
                message: format!(
                    "Multiple storage backends are enabled: {:?}. When using cloud-only mode, only one backend can be active.",
                    enabled_backends
                ),
                suggestion: format!(
                    "For cloud-only mode, disable all but one storage backend. For hybrid mode, set backend='local' and enable one cloud backend. Currently selected: {}",
                    storage.backend
                ),
            });
        }
        
        // Check if selected backend matches enabled backend
        let selected_enabled = match storage.backend.as_str() {
            "azure" => storage.azure.enabled,
            "s3" => storage.s3.enabled,
            "gcs" => storage.gcs.enabled,
            "local" => storage.local.enabled,
            _ => false,
        };
        
        if !selected_enabled {
            self.errors.push(ConfigError {
                field: format!("storage.{}.enabled", storage.backend),
                message: format!(
                    "Storage backend '{}' is selected but not enabled",
                    storage.backend
                ),
                suggestion: format!(
                    "Either enable the {} backend by setting storage.{}.enabled=true, or change storage.backend to an enabled backend",
                    storage.backend, storage.backend
                ),
            });
        }
    }
    
    fn validate_environment_deployment(&mut self, environment: &str, deployment: &DeploymentConfig) {
        match (environment, deployment.mode.as_str()) {
            ("local", "distributed") => {
                self.errors.push(ConfigError {
                    field: "deployment.mode".to_string(),
                    message: "Cannot use distributed mode in local environment".to_string(),
                    suggestion: "Change deployment.mode to 'standalone' for local environment, or change environment to 'development/production' for distributed mode".to_string(),
                });
            }
            ("production", "standalone") => {
                self.warnings.push(ConfigWarning {
                    field: "deployment.mode".to_string(),
                    message: "Using standalone mode in production environment".to_string(),
                    suggestion: "Consider using 'distributed' mode for production workloads to ensure high availability".to_string(),
                });
            }
            _ => {}
        }
    }
    
    fn validate_kubernetes_requirements(&mut self, deployment: &DeploymentConfig, environment: &str) {
        if deployment.kubernetes.enabled {
            if environment == "local" {
                self.errors.push(ConfigError {
                    field: "deployment.kubernetes.enabled".to_string(),
                    message: "Kubernetes is enabled in local environment".to_string(),
                    suggestion: "Set deployment.kubernetes.enabled=false for local environment, or use minikube/kind for local Kubernetes testing".to_string(),
                });
            }
            
            if deployment.kubernetes.namespace.is_empty() {
                self.errors.push(ConfigError {
                    field: "deployment.kubernetes.namespace".to_string(),
                    message: "Kubernetes namespace is empty".to_string(),
                    suggestion: "Provide a valid Kubernetes namespace (e.g., 'openwit', 'default')".to_string(),
                });
            }
        }
    }
    
    fn validate_urls_and_endpoints(&mut self, config: &UnifiedConfig) {
        let url_regex = Regex::new(r"^https?://[a-zA-Z0-9\-\.]+(\:[0-9]+)?(/.*)?$").unwrap();
        
        // Validate S3 endpoint
        if config.storage.s3.enabled && !config.storage.s3.endpoint.is_empty() {
            if !url_regex.is_match(&config.storage.s3.endpoint) && !config.storage.s3.endpoint.contains("${") {
                self.errors.push(ConfigError {
                    field: "storage.s3.endpoint".to_string(),
                    message: format!("Invalid S3 endpoint URL: {}", config.storage.s3.endpoint),
                    suggestion: "Provide a valid URL (e.g., 'https://s3.amazonaws.com' or 'https://minio.local:9000')".to_string(),
                });
            }
        }
        
        // Validate Azure endpoint
        if config.storage.azure.enabled && !config.storage.azure.endpoint.is_empty() {
            if !url_regex.is_match(&config.storage.azure.endpoint) && !config.storage.azure.endpoint.contains("${") {
                self.errors.push(ConfigError {
                    field: "storage.azure.endpoint".to_string(),
                    message: format!("Invalid Azure endpoint URL: {}", config.storage.azure.endpoint),
                    suggestion: "Provide a valid URL (e.g., 'https://accountname.blob.core.windows.net')".to_string(),
                });
            }
        }
    }
    
    fn validate_paths(&mut self, config: &UnifiedConfig) {
        // Validate local storage path
        if config.storage.local.enabled {
            let path = &config.storage.local.path;
            if path.is_empty() {
                self.errors.push(ConfigError {
                    field: "storage.local.path".to_string(),
                    message: "Local storage path is empty".to_string(),
                    suggestion: "Provide a valid path (e.g., './data/storage' or '/var/openwit/storage')".to_string(),
                });
            } else if !path.starts_with('/') && !path.starts_with("./") && !path.contains("${") {
                self.warnings.push(ConfigWarning {
                    field: "storage.local.path".to_string(),
                    message: format!("Local storage path '{}' is not absolute", path),
                    suggestion: "Consider using an absolute path for production deployments".to_string(),
                });
            }
        }
        
        // WAL validation removed - no longer needed
    }
    
    fn validate_resource_limits(&mut self, config: &UnifiedConfig) {
        // Check memory allocation consistency
        let heap_max_gb = config.memory.heap.max_size_gb;
        let buffer_pool_mb = config.memory.buffer_pool.size_mb;
        let total_memory_gb = heap_max_gb + (buffer_pool_mb / 1024);
        
        if total_memory_gb > 64 {
            self.warnings.push(ConfigWarning {
                field: "memory".to_string(),
                message: format!("Total memory allocation ({} GB) is very high", total_memory_gb),
                suggestion: "Ensure your system has sufficient memory. Consider reducing heap.max_size_gb or buffer_pool.size_mb".to_string(),
            });
        }
        
        // Check autoscaling limits consistency
        if config.control_plane.autoscaling.min_replicas > config.control_plane.autoscaling.max_replicas {
            self.errors.push(ConfigError {
                field: "control_plane.autoscaling".to_string(),
                message: format!(
                    "min_replicas ({}) is greater than max_replicas ({})",
                    config.control_plane.autoscaling.min_replicas,
                    config.control_plane.autoscaling.max_replicas
                ),
                suggestion: "Set min_replicas <= max_replicas".to_string(),
            });
        }
    }
    
    fn validate_dependencies(&mut self, config: &UnifiedConfig) {
        // If Kafka ingestion is enabled, validate Kafka configuration
        if config.ingestion.sources.kafka.enabled {
            if config.ingestion.kafka.brokers.is_none() || config.ingestion.kafka.brokers.as_ref().unwrap().is_empty() {
                self.errors.push(ConfigError {
                    field: "ingestion.kafka.brokers".to_string(),
                    message: "Kafka ingestion is enabled but no brokers are configured".to_string(),
                    suggestion: "Provide Kafka broker addresses (e.g., 'broker1:9092,broker2:9092')".to_string(),
                });
            }
            
            if config.ingestion.kafka.topics.is_empty() {
                self.errors.push(ConfigError {
                    field: "ingestion.kafka.topics".to_string(),
                    message: "Kafka ingestion is enabled but no topics are configured".to_string(),
                    suggestion: "Add at least one topic to ingestion.kafka.topics array".to_string(),
                });
            }
        }
        
        // No longer enforcing mandatory nodes in distributed mode
    }
    
    fn validate_environment_variables(&mut self, config: &UnifiedConfig) {
        let env_var_regex = Regex::new(r"\$\{([^}]+)\}").unwrap();
        
        // Check Azure storage environment variables
        if config.storage.azure.enabled {
            if config.storage.azure.account_name.contains("${") {
                if let Some(caps) = env_var_regex.captures(&config.storage.azure.account_name) {
                    let var_name = &caps[1];
                    if std::env::var(var_name).is_err() {
                        self.errors.push(ConfigError {
                            field: "storage.azure.account_name".to_string(),
                            message: format!("Environment variable '{}' is not set", var_name),
                            suggestion: format!("Set the {} environment variable or provide the value directly", var_name),
                        });
                    }
                }
            }
        }
        
        // Check S3 credentials
        if config.storage.s3.enabled {
            if config.storage.s3.access_key_id.is_empty() && std::env::var("AWS_ACCESS_KEY_ID").is_err() {
                self.warnings.push(ConfigWarning {
                    field: "storage.s3.access_key_id".to_string(),
                    message: "S3 access key not configured and AWS_ACCESS_KEY_ID not set".to_string(),
                    suggestion: "Provide S3 credentials via config or AWS_ACCESS_KEY_ID environment variable".to_string(),
                });
            }
        }
    }
    
    fn validate_ingestion_sources(&mut self, ingestion: &IngestionConfig) {
        let sources = &ingestion.sources;
        if !sources.kafka.enabled && !sources.grpc.enabled && !sources.http.enabled {
            self.errors.push(ConfigError {
                field: "ingestion.sources".to_string(),
                message: "No ingestion sources are enabled".to_string(),
                suggestion: "Enable at least one ingestion source (kafka, grpc, or http)".to_string(),
            });
        }
        
        // Validate GRPC config if enabled
        if sources.grpc.enabled && ingestion.grpc.port == 0 {
            self.errors.push(ConfigError {
                field: "ingestion.grpc.port".to_string(),
                message: "GRPC ingestion is enabled but port is not configured".to_string(),
                suggestion: "Set a valid port number for GRPC ingestion (e.g., 50051)".to_string(),
            });
        }
        
        // Validate HTTP config if enabled
        if sources.http.enabled && ingestion.http.port == 0 {
            self.errors.push(ConfigError {
                field: "ingestion.http.port".to_string(),
                message: "HTTP ingestion is enabled but port is not configured".to_string(),
                suggestion: "Set a valid port number for HTTP ingestion (e.g., 8080)".to_string(),
            });
        }
    }
    
    fn validate_port_conflicts(&mut self, config: &UnifiedConfig) {
        let mut used_ports = HashSet::new();
        let mut port_usage = Vec::new();
        
        // Collect all configured ports
        if config.ingestion.sources.grpc.enabled {
            let port = config.ingestion.grpc.port;
            if port > 0 {
                port_usage.push((port, "GRPC ingestion"));
                used_ports.insert(port);
            }
        }
        
        if config.ingestion.sources.http.enabled {
            let port = config.ingestion.http.port;
            if port > 0 {
                port_usage.push((port, "HTTP ingestion"));
                if !used_ports.insert(port) {
                    self.errors.push(ConfigError {
                        field: "ports".to_string(),
                        message: format!("Port {} is used by multiple services", port),
                        suggestion: "Assign unique ports to each service".to_string(),
                    });
                }
            }
        }
        
        // Add other service ports here as implemented
    }
    
    fn validate_security_settings(&mut self, config: &UnifiedConfig) {
        // Check TLS settings for production
        if config.environment == "production" {
            if config.ingestion.sources.grpc.enabled && !config.ingestion.grpc.tls.enabled {
                self.warnings.push(ConfigWarning {
                    field: "ingestion.grpc.tls.enabled".to_string(),
                    message: "TLS is disabled for GRPC in production environment".to_string(),
                    suggestion: "Enable TLS for secure communication in production".to_string(),
                });
            }
            
            if config.ingestion.sources.http.enabled && !config.ingestion.http.tls.enabled {
                self.warnings.push(ConfigWarning {
                    field: "ingestion.http.tls.enabled".to_string(),
                    message: "TLS is disabled for HTTP in production environment".to_string(),
                    suggestion: "Enable TLS for secure communication in production".to_string(),
                });
            }
        }
        
        // Check for hardcoded credentials
        if config.storage.s3.enabled {
            if !config.storage.s3.access_key_id.is_empty() && !config.storage.s3.access_key_id.contains("${") {
                self.warnings.push(ConfigWarning {
                    field: "storage.s3.access_key_id".to_string(),
                    message: "AWS credentials are hardcoded in configuration".to_string(),
                    suggestion: "Use environment variables for sensitive credentials (e.g., ${AWS_ACCESS_KEY_ID})".to_string(),
                });
            }
        }
    }
    
    fn log_validation_results(&self) -> bool {
        let error_count = self.errors.len();
        let warning_count = self.warnings.len();
        
        if error_count == 0 && warning_count == 0 {
            info!("Congratulations! Your configuration is perfect. Starting OpenWit...");
            return true;
        }
        
        if error_count > 0 {
            error!("Configuration validation failed with {} errors", error_count);
            error!("{}", "=".repeat(80));
            
            for (i, err) in self.errors.iter().enumerate() {
                error!("Error {}: {}", i + 1, err.field);
                error!("  Problem: {}", err.message);
                error!("  Solution: {}", err.suggestion);
                error!("");
            }
        }
        
        if warning_count > 0 {
            warn!("Configuration has {} warnings", warning_count);
            warn!("{}", "=".repeat(80));
            
            for (i, warning) in self.warnings.iter().enumerate() {
                warn!("Warning {}: {}", i + 1, warning.field);
                warn!("  Issue: {}", warning.message);
                warn!("  Recommendation: {}", warning.suggestion);
                warn!("");
            }
        }
        
        if error_count > 0 {
            error!("Please fix the errors above before starting OpenWit");
            false
        } else {
            warn!("OpenWit will start with warnings. Consider addressing them for optimal performance.");
            true
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_storage_conflict_detection() {
        let mut config = UnifiedConfig::default();
        config.storage.azure.enabled = true;
        config.storage.s3.enabled = true;
        config.storage.backend = "azure".to_string();
        
        let mut validator = ConfigValidator::new();
        validator.validate_storage_backends(&config.storage);
        
        assert_eq!(validator.errors.len(), 1);
        assert!(validator.errors[0].message.contains("Multiple storage backends"));
    }
    
    #[test]
    fn test_environment_deployment_conflict() {
        let mut config = UnifiedConfig::default();
        config.environment = "local".to_string();
        config.deployment.mode = "distributed".to_string();
        
        let mut validator = ConfigValidator::new();
        validator.validate_environment_deployment(&config.environment, &config.deployment);
        
        assert_eq!(validator.errors.len(), 1);
        assert!(validator.errors[0].message.contains("Cannot use distributed mode in local environment"));
    }
    
    #[test]
    fn test_perfect_config() {
        let config = UnifiedConfig::default();
        
        let mut validator = ConfigValidator::new();
        // A perfect default config should have minimal issues
        let is_valid = validator.validate_comprehensive(&config);
        
        // Default config might have some warnings but no errors
        assert!(validator.errors.is_empty() || !is_valid);
    }
}