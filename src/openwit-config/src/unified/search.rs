use serde::{Deserialize, Serialize};
use crate::unified::validation::{Validatable, ValidationResult};
use tracing::info;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    
    #[serde(default)]
    pub grpc: SearchGrpcConfig,
    
    #[serde(default)]
    pub http: SearchHttpConfig,
    
    #[serde(default)]
    pub query_engine: QueryEngineConfig,
}

impl Default for SearchConfig {
    fn default() -> Self {
        Self {
            enabled: default_true(),
            grpc: SearchGrpcConfig::default(),
            http: SearchHttpConfig::default(),
            query_engine: QueryEngineConfig::default(),
        }
    }
}

impl Validatable for SearchConfig {
    fn validate(&self) -> Vec<ValidationResult> {
        Vec::new() // Add specific validations as needed
    }
    
    fn apply_safe_defaults(&mut self) {
        info!("Applying safe defaults to Search configuration");
        self.query_engine.apply_safe_defaults();
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchGrpcConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    
    #[serde(default = "default_search_grpc_port")]
    pub port: u16,
    
    #[serde(default = "default_search_grpc_max_concurrent")]
    pub max_concurrent_requests: u32,
}

impl Default for SearchGrpcConfig {
    fn default() -> Self {
        Self {
            enabled: default_true(),
            port: default_search_grpc_port(),
            max_concurrent_requests: default_search_grpc_max_concurrent(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchHttpConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    
    #[serde(default = "default_search_http_port")]
    pub port: u16,
    
    #[serde(default = "default_search_http_max_concurrent")]
    pub max_concurrent_requests: u32,
}

impl Default for SearchHttpConfig {
    fn default() -> Self {
        Self {
            enabled: default_true(),
            port: default_search_http_port(),
            max_concurrent_requests: default_search_http_max_concurrent(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryEngineConfig {
    #[serde(default = "default_query_engine_type", rename = "type")]
    pub engine_type: String,
    
    #[serde(default = "default_query_timeout_seconds")]
    pub timeout_seconds: u32,
    
    #[serde(default = "default_max_results")]
    pub max_results: u32,
    
    #[serde(default = "default_true")]
    pub enable_caching: bool,
    
    #[serde(default = "default_cache_size_mb")]
    pub cache_size_mb: u32,
    
    #[serde(default = "default_cache_ttl_seconds")]
    pub cache_ttl_seconds: u32,
}

impl Default for QueryEngineConfig {
    fn default() -> Self {
        Self {
            engine_type: default_query_engine_type(),
            timeout_seconds: default_query_timeout_seconds(),
            max_results: default_max_results(),
            enable_caching: default_true(),
            cache_size_mb: default_cache_size_mb(),
            cache_ttl_seconds: default_cache_ttl_seconds(),
        }
    }
}

impl Validatable for QueryEngineConfig {
    fn validate(&self) -> Vec<ValidationResult> {
        Vec::new() // Add specific validations as needed
    }
    
    fn apply_safe_defaults(&mut self) {
        self.max_results = 1000; // Conservative result limit
        self.cache_size_mb = 256; // Conservative cache size
    }
}

// Default functions
fn default_true() -> bool { true }
fn default_search_grpc_port() -> u16 { 50059 }
fn default_search_grpc_max_concurrent() -> u32 { 1000 }
fn default_search_http_port() -> u16 { 9001 }
fn default_search_http_max_concurrent() -> u32 { 500 }
fn default_query_engine_type() -> String { "tantivy".to_string() }
fn default_query_timeout_seconds() -> u32 { 30 }
fn default_max_results() -> u32 { 100000 }
fn default_cache_size_mb() -> u32 { 1024 }
fn default_cache_ttl_seconds() -> u32 { 300 }
