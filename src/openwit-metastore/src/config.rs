//! Configuration helpers for metastore

use crate::MetastoreConfig;

/// Creates metastore config from environment variables
pub fn from_env() -> MetastoreConfig {
    let backend = std::env::var("METASTORE_BACKEND").unwrap_or_else(|_| "postgres".to_string());
    
    match backend.as_str() {
        "postgres" | "postgresql" => MetastoreConfig {
            backend,
            connection_string: std::env::var("METASTORE_CONNECTION_STRING").ok(),
            path: None,
            max_connections: std::env::var("METASTORE_MAX_CONNECTIONS")
                .ok()
                .and_then(|s| s.parse().ok()),
            schema_name: std::env::var("METASTORE_SCHEMA_NAME").ok(),
            cache_size_mb: None,
        },
        "sled" => MetastoreConfig {
            backend,
            connection_string: None,
            path: std::env::var("METASTORE_PATH").ok(),
            max_connections: None,
            schema_name: None,
            cache_size_mb: std::env::var("METASTORE_CACHE_SIZE_MB")
                .ok()
                .and_then(|s| s.parse().ok()),
        },
        _ => panic!("Unknown metastore backend: {}", backend),
    }
}

/// Validates metastore configuration
pub fn validate_config(config: &MetastoreConfig) -> Result<(), String> {
    match config.backend.as_str() {
        "postgres" | "postgresql" => {
            if config.connection_string.is_none() {
                return Err("PostgreSQL requires connection_string".to_string());
            }
        }
        "sled" => {
            // Sled uses default path if not specified
        }
        _ => {
            return Err(format!("Unknown backend: {}", config.backend));
        }
    }
    Ok(())
}