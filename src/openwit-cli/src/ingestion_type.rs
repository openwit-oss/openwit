use anyhow::{Result, anyhow};
use clap::ValueEnum;
use openwit_config::UnifiedConfig;
use tracing::{info, debug};

/// Default ports for ingestion services
pub const DEFAULT_GRPC_PORT: u16 = 4321;
pub const DEFAULT_HTTP_PORT: u16 = 8801;
pub const DEFAULT_GRPC_BIND: &str = "0.0.0.0";
pub const DEFAULT_HTTP_BIND: &str = "0.0.0.0";

/// Supported ingestion types
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum IngestionType {
    /// Enable only Kafka ingestion
    Kafka,
    /// Enable only HTTP ingestion
    Http,
    /// Enable only gRPC ingestion
    Grpc,
    /// Enable all ingestion sources (default)
    All,
}

impl Default for IngestionType {
    fn default() -> Self {
        IngestionType::All
    }
}

impl IngestionType {
    /// Apply the ingestion type to the configuration
    pub fn apply_to_config(&self, config: &mut UnifiedConfig) -> Result<()> {
        info!("Configuring ingestion type: {:?}", self);
        
        // First, disable all sources
        config.ingestion.sources.kafka.enabled = false;
        config.ingestion.sources.grpc.enabled = false;
        config.ingestion.sources.http.enabled = false;
        
        // Then enable based on the selected type
        match self {
            IngestionType::Kafka => {
                config.ingestion.sources.kafka.enabled = true;
                debug!("Enabled Kafka ingestion only");
            }
            IngestionType::Http => {
                config.ingestion.sources.http.enabled = true;
                self.apply_http_defaults(config);
                debug!("Enabled HTTP ingestion only");
            }
            IngestionType::Grpc => {
                config.ingestion.sources.grpc.enabled = true;
                self.apply_grpc_defaults(config);
                debug!("Enabled gRPC ingestion only");
            }
            IngestionType::All => {
                config.ingestion.sources.kafka.enabled = true;
                config.ingestion.sources.grpc.enabled = true;
                config.ingestion.sources.http.enabled = true;
                self.apply_grpc_defaults(config);
                self.apply_http_defaults(config);
                debug!("Enabled all ingestion sources");
            }
        }
        
        Ok(())
    }
    
    /// Apply default gRPC configuration if not set
    fn apply_grpc_defaults(&self, config: &mut UnifiedConfig) {
        // Set default port if it's 0 or not set
        if config.ingestion.grpc.port == 0 {
            config.ingestion.grpc.port = DEFAULT_GRPC_PORT;
            debug!("Applied default gRPC port: {}", DEFAULT_GRPC_PORT);
        }
        
        // Set default bind address if empty
        if config.ingestion.grpc.bind.is_empty() {
            config.ingestion.grpc.bind = format!("{}:{}", DEFAULT_GRPC_BIND, config.ingestion.grpc.port);
            debug!("Applied default gRPC bind address: {}", config.ingestion.grpc.bind);
        }
    }
    
    /// Apply default HTTP configuration if not set
    fn apply_http_defaults(&self, config: &mut UnifiedConfig) {
        // Set default port if it's 0 or not set
        if config.ingestion.http.port == 0 {
            config.ingestion.http.port = DEFAULT_HTTP_PORT;
            debug!("Applied default HTTP port: {}", DEFAULT_HTTP_PORT);
        }
        
        // Set default bind address if empty
        if config.ingestion.http.bind.is_empty() {
            config.ingestion.http.bind = format!("{}:{}", DEFAULT_HTTP_BIND, config.ingestion.http.port);
            debug!("Applied default HTTP bind address: {}", config.ingestion.http.bind);
        }
    }
    
    /// Validate the configuration based on the selected ingestion type
    pub fn validate_config(&self, config: &UnifiedConfig) -> Result<()> {
        match self {
            IngestionType::Kafka => {
                self.validate_kafka_config(config)?;
            }
            IngestionType::Http => {
                self.validate_http_config(config)?;
            }
            IngestionType::Grpc => {
                self.validate_grpc_config(config)?;
            }
            IngestionType::All => {
                // Validate all enabled sources
                if config.ingestion.sources.kafka.enabled {
                    self.validate_kafka_config(config)?;
                }
                if config.ingestion.sources.http.enabled {
                    self.validate_http_config(config)?;
                }
                if config.ingestion.sources.grpc.enabled {
                    self.validate_grpc_config(config)?;
                }
            }
        }
        
        Ok(())
    }
    
    /// Validate Kafka configuration
    fn validate_kafka_config(&self, config: &UnifiedConfig) -> Result<()> {
        if config.ingestion.sources.kafka.enabled {
            // Check if brokers are configured
            if config.ingestion.kafka.brokers.as_ref().map(|s| s.is_empty()).unwrap_or(true) {
                return Err(anyhow!(
                    "Kafka ingestion is enabled but no brokers are configured. \
                    Please set kafka.brokers in your config file or use --kafka-brokers"
                ));
            }
            
            // Check if topics are configured
            if config.ingestion.kafka.topics.is_empty() {
                return Err(anyhow!(
                    "Kafka ingestion is enabled but no topics are configured. \
                    Please set kafka.topics in your config file"
                ));
            }
            
            // Check if group_id is configured
            if config.ingestion.kafka.group_id.as_ref().map(|s| s.is_empty()).unwrap_or(true) {
                return Err(anyhow!(
                    "Kafka ingestion is enabled but no group_id is configured. \
                    Please set kafka.group_id in your config file"
                ));
            }
            
            info!("Kafka configuration validated successfully");
        }
        
        Ok(())
    }
    
    /// Validate HTTP configuration
    fn validate_http_config(&self, config: &UnifiedConfig) -> Result<()> {
        if config.ingestion.sources.http.enabled {
            // Check if bind address is valid
            if config.ingestion.http.bind.is_empty() {
                return Err(anyhow!(
                    "HTTP ingestion is enabled but no bind address is configured. \
                    Please set http.bind in your config file"
                ));
            }
            
            // Validate port
            let port = config.ingestion.http.port;
            if port == 0 {
                return Err(anyhow!(
                    "HTTP ingestion is enabled but port is set to 0. \
                    Please set a valid port in http.port"
                ));
            }
            
            info!("HTTP configuration validated successfully (bind: {}:{})", 
                  config.ingestion.http.bind, port);
        }
        
        Ok(())
    }
    
    /// Validate gRPC configuration
    fn validate_grpc_config(&self, config: &UnifiedConfig) -> Result<()> {
        if config.ingestion.sources.grpc.enabled {
            // Check if bind address is valid
            if config.ingestion.grpc.bind.is_empty() {
                return Err(anyhow!(
                    "gRPC ingestion is enabled but no bind address is configured. \
                    Please set grpc.bind in your config file"
                ));
            }
            
            // Validate port
            let port = config.ingestion.grpc.port;
            if port == 0 {
                return Err(anyhow!(
                    "gRPC ingestion is enabled but port is set to 0. \
                    Please set a valid port in grpc.port"
                ));
            }
            
            info!("gRPC configuration validated successfully (bind: {}:{})", 
                  config.ingestion.grpc.bind, port);
        }
        
        Ok(())
    }
    
    /// Get a description of what this ingestion type enables
    pub fn description(&self) -> &'static str {
        match self {
            IngestionType::Kafka => "Kafka message queue ingestion only",
            IngestionType::Http => "HTTP REST API ingestion only",
            IngestionType::Grpc => "gRPC/OTLP ingestion only",
            IngestionType::All => "All ingestion sources (Kafka, HTTP, and gRPC)",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_default_ingestion_type() {
        assert_eq!(IngestionType::default(), IngestionType::All);
    }
    
    #[test]
    fn test_apply_to_config() {
        let mut config = UnifiedConfig::default();
        
        // Test Kafka only
        IngestionType::Kafka.apply_to_config(&mut config).unwrap();
        assert!(config.ingestion.sources.kafka.enabled);
        assert!(!config.ingestion.sources.http.enabled);
        assert!(!config.ingestion.sources.grpc.enabled);
        
        // Test HTTP only
        IngestionType::Http.apply_to_config(&mut config).unwrap();
        assert!(!config.ingestion.sources.kafka.enabled);
        assert!(config.ingestion.sources.http.enabled);
        assert!(!config.ingestion.sources.grpc.enabled);
        
        // Test All
        IngestionType::All.apply_to_config(&mut config).unwrap();
        assert!(config.ingestion.sources.kafka.enabled);
        assert!(config.ingestion.sources.http.enabled);
        assert!(config.ingestion.sources.grpc.enabled);
    }
}