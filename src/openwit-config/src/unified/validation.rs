use tracing::{warn, error};

/// Configuration limits for validation
#[derive(Debug, Clone)]
pub struct ConfigLimits {
    pub min: i64,
    pub max: i64,
    pub field_name: String,
    pub recommended: i64,
}

impl ConfigLimits {
    pub fn new(field_name: &str, min: i64, max: i64, recommended: i64) -> Self {
        Self {
            min,
            max,
            field_name: field_name.to_string(),
            recommended,
        }
    }
    
    pub fn validate(&self, value: i64) -> ValidationResult {
        if value < self.min {
            ValidationResult::BelowMin {
                field: self.field_name.clone(),
                value,
                min: self.min,
                recommended: self.recommended,
            }
        } else if value > self.max {
            ValidationResult::AboveMax {
                field: self.field_name.clone(),
                value,
                max: self.max,
                recommended: self.recommended,
            }
        } else if value > self.recommended * 2 {
            ValidationResult::Warning {
                field: self.field_name.clone(),
                value,
                recommended: self.recommended,
                message: "Value is significantly higher than recommended".to_string(),
            }
        } else {
            ValidationResult::Valid
        }
    }
}

#[derive(Debug, Clone)]
pub enum ValidationResult {
    Valid,
    Warning {
        field: String,
        value: i64,
        recommended: i64,
        message: String,
    },
    BelowMin {
        field: String,
        value: i64,
        min: i64,
        recommended: i64,
    },
    AboveMax {
        field: String,
        value: i64,
        max: i64,
        recommended: i64,
    },
}

impl ValidationResult {
    pub fn log(&self) {
        match self {
            ValidationResult::Valid => {},
            ValidationResult::Warning { field, value, recommended, message } => {
                warn!(
                    "Configuration warning for '{}': {} (current: {}, recommended: {})",
                    field, message, value, recommended
                );
            },
            ValidationResult::BelowMin { field, value, min, recommended } => {
                error!(
                    "Configuration error for '{}': Value {} is below minimum threshold {}. Recommended value: {}. Using safe default.",
                    field, value, min, recommended
                );
            },
            ValidationResult::AboveMax { field, value, max, recommended } => {
                error!(
                    "Configuration error for '{}': Value {} exceeds maximum threshold {}. Recommended value: {}. Using safe default.",
                    field, value, max, recommended
                );
            },
        }
    }
    
    pub fn is_valid(&self) -> bool {
        matches!(self, ValidationResult::Valid | ValidationResult::Warning { .. })
    }
    
    pub fn error(field: &str, value: i64, message: &str) -> Self {
        ValidationResult::Warning {
            field: field.to_string(),
            value,
            recommended: 0,
            message: message.to_string(),
        }
    }
}

/// Trait for validatable configuration values
pub trait Validatable {
    fn validate(&self) -> Vec<ValidationResult>;
    fn apply_safe_defaults(&mut self);
}

/// Safe mode defaults - conservative values for stable operation
pub mod safe_defaults {
    // Kafka defaults (safe mode)
    pub const KAFKA_POOL_SIZE: u32 = 4;  // Reduced from 16
    pub const KAFKA_BATCH_SIZE: u32 = 1000;  // Reduced from 50000
    pub const KAFKA_CHUNK_SIZE: u32 = 100;  // Reduced from 1000
    pub const KAFKA_MAX_POLL_INTERVAL_MS: u64 = 300000;  // 5 minutes instead of 15
    
    // Buffer defaults (safe mode)
    pub const BUFFER_MAX_SIZE_MESSAGES: u64 = 100000;  // Reduced from 1M
    pub const BUFFER_MAX_SIZE_BYTES: u64 = 1073741824;  // 1GB instead of 10GB
    pub const BUFFER_WORKER_THREADS: u32 = 4;  // Reduced from 16
    pub const BUFFER_IO_THREADS: u32 = 2;  // Reduced from 8
    
    // Processing defaults (safe mode)
    pub const PIPELINE_BATCH_SIZE: u32 = 1000;  // Reduced from 10000
    pub const PIPELINE_QUEUE_SIZE: u32 = 10000;  // Reduced from 100000
    
    // Memory defaults (safe mode)
    pub const MEMTABLE_SIZE_LIMIT_MB: u32 = 128;  // Reduced from 512
    pub const HEAP_SIZE_MB: u32 = 4096;  // 4GB instead of 16GB
    pub const BUFFER_POOL_SIZE_MB: u32 = 2048;  // 2GB instead of 8GB
    pub const HEAP_INITIAL_SIZE_GB: u32 = 2;  // 2GB initial heap
    pub const HEAP_MAX_SIZE_GB: u32 = 8;  // 8GB max heap (safe mode)
    pub const CACHE_MAX_ENTRIES: u64 = 100000;  // Reduced from 1M
    
    // Networking defaults (safe mode)
    pub const MAX_CONCURRENT_REQUESTS: u32 = 1000;  // Reduced from 10000
    pub const CONNECTION_POOL_SIZE: u32 = 10;  // Reduced from 20
    
    // Control plane defaults (safe mode)
    pub const MAX_CONCURRENT_ACTIONS: u32 = 2;  // Reduced from 5
    pub const MIN_REPLICAS: u32 = 1;
    pub const MAX_REPLICAS: u32 = 5;  // Reduced from 20
}

/// Configuration limits for various fields
pub mod limits {
    use super::ConfigLimits;
    
    // Kafka limits
    pub fn kafka_pool_size() -> ConfigLimits {
        ConfigLimits::new("kafka.pool_size", 1, 100, 16)
    }
    
    pub fn kafka_batch_size() -> ConfigLimits {
        ConfigLimits::new("kafka.batching.batch_size", 100, 100000, 50000)
    }
    
    pub fn kafka_session_timeout_ms() -> ConfigLimits {
        ConfigLimits::new("kafka.timeouts.session_timeout_ms", 6000, 3600000, 180000)
    }
    
    // Buffer limits
    pub fn buffer_worker_threads() -> ConfigLimits {
        ConfigLimits::new("processing.buffer.worker_threads", 1, 128, 16)
    }
    
    pub fn buffer_max_size_bytes() -> ConfigLimits {
        ConfigLimits::new("processing.buffer.max_size_bytes", 1048576, 107374182400, 10737418240) // 1MB to 100GB
    }
    
    // Memory limits
    pub fn heap_size_mb() -> ConfigLimits {
        ConfigLimits::new("memory.heap_size_mb", 512, 65536, 16384) // 512MB to 64GB
    }
    
    pub fn memtable_size_limit_mb() -> ConfigLimits {
        ConfigLimits::new("memory.memtable.size_limit_mb", 64, 4096, 512)
    }
    
    // Control plane limits
    pub fn autoscaling_min_replicas() -> ConfigLimits {
        ConfigLimits::new("control_plane.autoscaling.min_replicas", 1, 10, 1)
    }
    
    pub fn autoscaling_max_replicas() -> ConfigLimits {
        ConfigLimits::new("control_plane.autoscaling.max_replicas", 1, 100, 20)
    }
    
    // Networking limits
    pub fn max_concurrent_requests() -> ConfigLimits {
        ConfigLimits::new("grpc.max_concurrent_requests", 10, 50000, 10000)
    }
    
    pub fn connection_timeout_ms() -> ConfigLimits {
        ConfigLimits::new("networking.inter_node.connection_timeout_ms", 100, 60000, 5000)
    }
    
    // Storage limits
    pub fn parquet_target_file_size_mb() -> ConfigLimits {
        ConfigLimits::new("storage.parquet.target_file_size_mb", 10, 5120, 200) // 10MB to 5GB
    }
    
    pub fn compaction_max_files() -> ConfigLimits {
        ConfigLimits::new("storage.compaction.max_files_per_compaction", 2, 1000, 20)
    }
    
    pub fn heap_max_size_gb() -> ConfigLimits {
        ConfigLimits::new("memory.heap.max_size_gb", 1, 512, 32)
    }
    
    pub fn buffer_pool_size_mb() -> ConfigLimits {
        ConfigLimits::new("memory.buffer_pool.size_mb", 64, 16384, 2048)
    }
    
    pub fn cache_max_entries() -> ConfigLimits {
        ConfigLimits::new("memory.cache.max_entries", 1000, 100000000, 1000000)
    }
    
    // Worker thread limits
    pub fn worker_threads() -> ConfigLimits {
        ConfigLimits::new("worker_threads", 1, 128, 16)
    }
    
    // Indexing limits
    pub fn datafusion_batch_size() -> ConfigLimits {
        ConfigLimits::new("indexing.datafusion.batch_size", 1024, 1048576, 32768) // 1K to 1M
    }
    
    pub fn tantivy_query_limit() -> ConfigLimits {
        ConfigLimits::new("indexing.tantivy.query_limit_default", 10, 100000, 1000)
    }
    
    // Actor system limits
    pub fn simple_ingest_memory_buffer_mb() -> ConfigLimits {
        ConfigLimits::new("actors.simple_ingest.memory_buffer_mb", 64, 8192, 512)
    }
    
    // Janitor limits
    pub fn janitor_retention_days() -> ConfigLimits {
        ConfigLimits::new("janitor.cleanup.retention_period_days", 1, 3650, 30) // 1 day to 10 years
    }
    
    // Search limits
    pub fn search_max_results() -> ConfigLimits {
        ConfigLimits::new("search.query_engine.max_results", 10, 1000000, 100000)
    }
}

/// Validate and potentially correct a numeric configuration value
pub fn validate_and_correct<T: Into<i64> + From<i64> + Copy>(
    value: T,
    limits: &ConfigLimits,
    use_safe_default: bool,
) -> T {
    let val = value.into();
    let result = limits.validate(val);
    result.log();
    
    match result {
        ValidationResult::Valid | ValidationResult::Warning { .. } => value,
        ValidationResult::BelowMin { .. } | ValidationResult::AboveMax { .. } => {
            if use_safe_default {
                T::from(limits.recommended)
            } else {
                T::from(val.clamp(limits.min, limits.max))
            }
        }
    }
}

/// Helper macro to validate multiple fields
#[macro_export]
macro_rules! validate_config_field {
    ($value:expr, $limits:expr, $safe_mode:expr) => {
        validate_and_correct($value, &$limits, $safe_mode)
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_validation_limits() {
        let limits = ConfigLimits::new("test_field", 10, 100, 50);
        
        // Test below minimum
        assert!(matches!(limits.validate(5), ValidationResult::BelowMin { .. }));
        
        // Test above maximum
        assert!(matches!(limits.validate(150), ValidationResult::AboveMax { .. }));
        
        // Test valid
        assert!(matches!(limits.validate(50), ValidationResult::Valid));
        
        // Test warning (more than 2x recommended)
        assert!(matches!(limits.validate(101), ValidationResult::AboveMax { .. }));
    }
    
}