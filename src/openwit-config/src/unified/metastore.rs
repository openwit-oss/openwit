use serde::{Deserialize, Serialize};
use crate::unified::validation::{Validatable, ValidationResult};
use tracing::info;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetastoreConfig {
    #[serde(default = "default_metastore_backend")]
    pub backend: String,
    
    #[serde(default)]
    pub postgres: PostgresConfig,
    
    #[serde(default)]
    pub sled: SledConfig,
    
    #[serde(default)]
    pub pool: ConnectionPoolConfig,
}

impl Default for MetastoreConfig {
    fn default() -> Self {
        Self {
            backend: default_metastore_backend(),
            postgres: PostgresConfig::default(),
            sled: SledConfig::default(),
            pool: ConnectionPoolConfig::default(),
        }
    }
}

impl Validatable for MetastoreConfig {
    fn validate(&self) -> Vec<ValidationResult> {
        Vec::new() // Add specific validations as needed
    }
    
    fn apply_safe_defaults(&mut self) {
        info!("Applying safe defaults to Metastore configuration");
        self.pool.apply_safe_defaults();
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostgresConfig {
    #[serde(default = "default_postgres_connection_string")]
    pub connection_string: String,
    
    #[serde(default = "default_max_connections")]
    pub max_connections: u32,
    
    #[serde(default = "default_min_connections")]
    pub min_connections: u32,
    
    #[serde(default = "default_connection_timeout_seconds")]
    pub connection_timeout_seconds: u32,
    
    #[serde(default = "default_idle_timeout_seconds")]
    pub idle_timeout_seconds: u32,
    
    #[serde(default = "default_schema_name")]
    pub schema_name: String,
}

impl Default for PostgresConfig {
    fn default() -> Self {
        Self {
            connection_string: default_postgres_connection_string(),
            max_connections: default_max_connections(),
            min_connections: default_min_connections(),
            connection_timeout_seconds: default_connection_timeout_seconds(),
            idle_timeout_seconds: default_idle_timeout_seconds(),
            schema_name: default_schema_name(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SledConfig {
    #[serde(default = "default_sled_path")]
    pub path: String,
    
    #[serde(default = "default_sled_cache_size_mb")]
    pub cache_size_mb: u32,
}

impl Default for SledConfig {
    fn default() -> Self {
        Self {
            path: default_sled_path(),
            cache_size_mb: default_sled_cache_size_mb(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionPoolConfig {
    #[serde(default = "default_pool_max_size")]
    pub max_size: u32,
    
    #[serde(default = "default_pool_min_idle")]
    pub min_idle: u32,
    
    #[serde(default = "default_true")]
    pub test_on_borrow: bool,
}

impl Default for ConnectionPoolConfig {
    fn default() -> Self {
        Self {
            max_size: default_pool_max_size(),
            min_idle: default_pool_min_idle(),
            test_on_borrow: default_true(),
        }
    }
}

impl ConnectionPoolConfig {
    fn apply_safe_defaults(&mut self) {
        self.max_size = 10; // Conservative pool size
        self.min_idle = 2; // Fewer idle connections
    }
}

// Default functions
fn default_true() -> bool { true }
fn default_metastore_backend() -> String { "postgres".to_string() }
fn default_postgres_connection_string() -> String { "".to_string() }
fn default_max_connections() -> u32 { 20 }
fn default_min_connections() -> u32 { 5 }
fn default_connection_timeout_seconds() -> u32 { 30 }
fn default_idle_timeout_seconds() -> u32 { 600 }
fn default_schema_name() -> String { "openwit_metastore".to_string() }
fn default_sled_path() -> String { "./data/metastore".to_string() }
fn default_sled_cache_size_mb() -> u32 { 512 }
fn default_pool_max_size() -> u32 { 20 }
fn default_pool_min_idle() -> u32 { 5 }
