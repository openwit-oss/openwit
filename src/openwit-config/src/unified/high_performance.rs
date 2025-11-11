use serde::{Deserialize, Serialize};

/// High-performance configuration optimized for 1M+ messages/sec
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HighPerformanceConfig {
    pub enabled: bool,
    pub kafka: HighPerfKafkaSettings,
    pub ingestion: HighPerfIngestionSettings,
    pub grpc: HighPerfGrpcSettings,
    pub memory: HighPerfMemorySettings,
}

impl Default for HighPerformanceConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            kafka: HighPerfKafkaSettings::default(),
            ingestion: HighPerfIngestionSettings::default(),
            grpc: HighPerfGrpcSettings::default(),
            memory: HighPerfMemorySettings::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HighPerfKafkaSettings {
    // Consumer settings
    pub num_consumers: usize,              // Number of parallel Kafka consumers
    pub consumer_threads_per_consumer: usize,  // Threads per consumer
    pub batch_size: usize,                 // Messages per batch
    pub batch_timeout_ms: u64,             // Max time to wait for batch
    
    // Kafka performance tuning
    pub fetch_min_bytes: usize,            // Min bytes per fetch (1MB)
    pub fetch_max_bytes: usize,            // Max bytes per fetch (50MB)
    pub fetch_max_wait_ms: u64,            // Max wait for fetch (5ms)
    pub max_partition_fetch_bytes: usize,  // Max bytes per partition (10MB)
    pub socket_buffer_bytes: usize,        // Socket buffer size (10MB)
    
    // Processing
    pub processing_threads: usize,         // Message processing threads
    pub channel_buffer_size: usize,        // Internal channel size
    
    // Memory optimization
    pub enable_zero_copy: bool,            // Use zero-copy for large messages
    pub preallocate_buffers: bool,         // Preallocate message buffers
    pub buffer_pool_size: usize,           // Size of buffer pool
}

impl Default for HighPerfKafkaSettings {
    fn default() -> Self {
        Self {
            // Optimized for 1M msgs/sec across cluster
            num_consumers: 8,                      // 8 parallel consumers
            consumer_threads_per_consumer: 2,      // 2 threads each = 16 total
            batch_size: 10000,                     // 10K messages per batch
            batch_timeout_ms: 10,                  // 10ms max wait
            
            // Kafka tuning
            fetch_min_bytes: 1024 * 1024,         // 1MB
            fetch_max_bytes: 50 * 1024 * 1024,    // 50MB
            fetch_max_wait_ms: 5,                  // 5ms
            max_partition_fetch_bytes: 10 * 1024 * 1024, // 10MB
            socket_buffer_bytes: 10 * 1024 * 1024,        // 10MB
            
            // Processing
            processing_threads: 16,
            channel_buffer_size: 100000,
            
            // Memory
            enable_zero_copy: true,
            preallocate_buffers: true,
            buffer_pool_size: 1000,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HighPerfIngestionSettings {
    // Batching
    pub arrow_batch_size: usize,           // Arrow batch size
    pub arrow_batch_timeout_ms: u64,       // Arrow batch timeout
    
    // Parallelism
    pub ingestion_threads: usize,          // Parallel ingestion threads
    pub router_threads: usize,             // Batch routing threads
    pub flight_client_pool_size: usize,    // Arrow Flight client pool
    
    // Buffers
    pub memtable_size_mb: usize,           // Memtable size in MB
    pub buffer_pool_size: usize,           // Buffer pool for reuse
    pub queue_depth: usize,                // Processing queue depth
    
    // Optimization
    pub enable_compression: bool,          // Enable compression
    pub compression_level: i32,            // Compression level (1-9)
    pub enable_deduplication: bool,        // Enable dedup
    pub dedup_window_seconds: u64,         // Dedup window
}

impl Default for HighPerfIngestionSettings {
    fn default() -> Self {
        Self {
            // Batching
            arrow_batch_size: 50000,           // 50K rows per Arrow batch
            arrow_batch_timeout_ms: 100,       // 100ms timeout
            
            // Parallelism
            ingestion_threads: 16,
            router_threads: 8,
            flight_client_pool_size: 8,
            
            // Buffers
            memtable_size_mb: 1024,            // 1GB memtables
            buffer_pool_size: 100,
            queue_depth: 10000,
            
            // Optimization
            enable_compression: true,
            compression_level: 1,              // Fast compression
            enable_deduplication: false,       // Disable for performance
            dedup_window_seconds: 0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HighPerfGrpcSettings {
    // Connection pooling
    pub connection_pool_size: usize,       // Per endpoint
    pub max_concurrent_streams: usize,     // Per connection
    
    // Message settings
    pub max_message_size: usize,           // Max gRPC message size
    pub max_batch_size: usize,             // Max messages per RPC
    
    // Performance
    pub keepalive_interval_ms: u64,        // Keepalive interval
    pub keepalive_timeout_ms: u64,         // Keepalive timeout
    pub initial_window_size: usize,        // HTTP2 window
    pub initial_connection_window: usize,  // HTTP2 connection window
    
    // Timeouts
    pub connect_timeout_ms: u64,
    pub request_timeout_ms: u64,
}

impl Default for HighPerfGrpcSettings {
    fn default() -> Self {
        Self {
            // Connection pooling
            connection_pool_size: 4,
            max_concurrent_streams: 1000,
            
            // Messages
            max_message_size: 100 * 1024 * 1024,      // 100MB
            max_batch_size: 10000,
            
            // Performance
            keepalive_interval_ms: 10000,              // 10s
            keepalive_timeout_ms: 5000,                // 5s
            initial_window_size: 10 * 1024 * 1024,     // 10MB
            initial_connection_window: 100 * 1024 * 1024, // 100MB
            
            // Timeouts
            connect_timeout_ms: 5000,
            request_timeout_ms: 30000,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HighPerfMemorySettings {
    // JEMalloc tuning
    pub enable_jemalloc: bool,
    pub jemalloc_background_threads: usize,
    
    // Memory limits
    pub max_memory_gb: usize,              // Max memory usage
    pub buffer_memory_gb: usize,           // Buffer pool memory
    pub cache_memory_gb: usize,            // Cache memory
    
    // GC and cleanup
    pub gc_interval_seconds: u64,
    pub memory_cleanup_threshold: f64,     // Cleanup at X% usage
    
    // Arena settings
    pub arena_count: usize,                // Number of memory arenas
    pub tcache_max: usize,                 // Thread cache max
}

impl Default for HighPerfMemorySettings {
    fn default() -> Self {
        Self {
            // JEMalloc
            enable_jemalloc: true,
            jemalloc_background_threads: 4,
            
            // Memory (assuming 32GB+ system)
            max_memory_gb: 16,
            buffer_memory_gb: 8,
            cache_memory_gb: 4,
            
            // GC
            gc_interval_seconds: 60,
            memory_cleanup_threshold: 0.8,
            
            // Arena
            arena_count: 16,                   // Match CPU cores
            tcache_max: 32768,
        }
    }
}

impl HighPerformanceConfig {
    /// Create config optimized for specific message rate
    pub fn for_message_rate(messages_per_second: usize) -> Self {
        let mut config = Self::default();
        config.enabled = true;
        
        // Scale settings based on target rate
        if messages_per_second >= 1_000_000 {
            // 1M+ msgs/sec settings
            config.kafka.num_consumers = 16;
            config.kafka.batch_size = 20000;
            config.kafka.processing_threads = 32;
            config.ingestion.arrow_batch_size = 100000;
            config.ingestion.ingestion_threads = 32;
            config.grpc.connection_pool_size = 8;
            config.memory.max_memory_gb = 32;
        } else if messages_per_second >= 500_000 {
            // 500K msgs/sec settings
            config.kafka.num_consumers = 12;
            config.kafka.batch_size = 15000;
            config.kafka.processing_threads = 24;
            config.ingestion.arrow_batch_size = 75000;
            config.ingestion.ingestion_threads = 24;
        } else if messages_per_second >= 100_000 {
            // 100K msgs/sec settings
            config.kafka.num_consumers = 8;
            config.kafka.batch_size = 10000;
            config.kafka.processing_threads = 16;
            config.ingestion.arrow_batch_size = 50000;
            config.ingestion.ingestion_threads = 16;
        }
        
        config
    }
    
    /// Validate settings for feasibility
    pub fn validate(&self) -> Result<(), String> {
        // Check memory requirements
        let total_memory = self.memory.max_memory_gb + self.memory.buffer_memory_gb + self.memory.cache_memory_gb;
        if total_memory > 64 {
            return Err(format!("Total memory requirement {}GB exceeds typical system capacity", total_memory));
        }
        
        // Check thread counts
        let total_threads = self.kafka.num_consumers * self.kafka.consumer_threads_per_consumer
            + self.kafka.processing_threads
            + self.ingestion.ingestion_threads
            + self.ingestion.router_threads;
        if total_threads > 128 {
            return Err(format!("Total thread count {} may exceed system capacity", total_threads));
        }
        
        // Check batch sizes (assuming ~5KB per message average)
        if self.kafka.batch_size * 5000 > self.grpc.max_message_size {
            return Err("Kafka batch size may exceed gRPC message size limit".to_string());
        }
        
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_high_perf_config_defaults() {
        let config = HighPerformanceConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.kafka.num_consumers, 8);
        assert_eq!(config.kafka.batch_size, 10000);
    }
    
    #[test]
    fn test_config_for_1m_messages() {
        let config = HighPerformanceConfig::for_message_rate(1_000_000);
        assert!(config.enabled);
        assert_eq!(config.kafka.num_consumers, 16);
        assert_eq!(config.kafka.batch_size, 20000);
        assert_eq!(config.ingestion.arrow_batch_size, 100000);
    }
    
    #[test]
    fn test_validation() {
        let mut config = HighPerformanceConfig::default();
        config.memory.max_memory_gb = 100; // Excessive
        assert!(config.validate().is_err());
    }
}