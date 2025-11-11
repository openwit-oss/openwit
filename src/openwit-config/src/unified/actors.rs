use serde::{Deserialize, Serialize};
use crate::unified::validation::{Validatable, ValidationResult, limits};
use tracing::info;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActorSystemConfig {
    #[serde(default)]
    pub simple_ingest: SimpleIngestActorConfig,
    
    #[serde(default)]
    pub memtable: MemtableActorConfig,
    
    #[serde(default)]
    pub flusher: FlusherActorConfig,
    
    #[serde(default)]
    pub monitor: MonitorActorConfig,
}

impl Default for ActorSystemConfig {
    fn default() -> Self {
        Self {
            simple_ingest: SimpleIngestActorConfig::default(),
            memtable: MemtableActorConfig::default(),
            flusher: FlusherActorConfig::default(),
            monitor: MonitorActorConfig::default(),
        }
    }
}

impl Validatable for ActorSystemConfig {
    fn validate(&self) -> Vec<ValidationResult> {
        let mut results = Vec::new();
        results.extend(self.simple_ingest.validate());
        results.extend(self.memtable.validate());
        results.extend(self.flusher.validate());
        results
    }
    
    fn apply_safe_defaults(&mut self) {
        info!("Applying safe defaults to Actor System configuration");
        self.simple_ingest.apply_safe_defaults();
        self.memtable.apply_safe_defaults();
        self.flusher.apply_safe_defaults();
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimpleIngestActorConfig {
    #[serde(default = "default_simple_ingest_memory_buffer_mb")]
    pub memory_buffer_mb: u32,
    
    #[serde(default = "default_simple_ingest_batch_timeout_seconds")]
    pub batch_timeout_seconds: u32,
    
    #[serde(default = "default_simple_ingest_max_batch_size")]
    pub max_batch_size: u32,
}

impl Default for SimpleIngestActorConfig {
    fn default() -> Self {
        Self {
            memory_buffer_mb: default_simple_ingest_memory_buffer_mb(),
            batch_timeout_seconds: default_simple_ingest_batch_timeout_seconds(),
            max_batch_size: default_simple_ingest_max_batch_size(),
        }
    }
}

impl Validatable for SimpleIngestActorConfig {
    fn validate(&self) -> Vec<ValidationResult> {
        let mut results = Vec::new();
        results.push(limits::simple_ingest_memory_buffer_mb().validate(self.memory_buffer_mb as i64));
        results
    }
    
    fn apply_safe_defaults(&mut self) {
        self.memory_buffer_mb = 256; // Conservative memory buffer
        self.batch_timeout_seconds = 60; // Longer timeout for safety
        self.max_batch_size = 5000; // Smaller batch size
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemtableActorConfig {
    #[serde(default = "default_memtable_initial_shard_count")]
    pub initial_shard_count: u32,
    
    #[serde(default = "default_memtable_max_shard_count")]
    pub max_shard_count: u32,
    
    #[serde(default = "default_memtable_shard_channel_capacity")]
    pub shard_channel_capacity: u32,
    
    #[serde(default = "default_memtable_batch_size")]
    pub batch_size: u32,
    
    #[serde(default = "default_memtable_max_memory_mb")]
    pub max_memory_mb: u32,
    
    #[serde(default = "default_memtable_write_buffer_size_mb")]
    pub write_buffer_size_mb: u32,
    
    #[serde(default = "default_memtable_compression")]
    pub compression: String,
}

impl Default for MemtableActorConfig {
    fn default() -> Self {
        Self {
            initial_shard_count: default_memtable_initial_shard_count(),
            max_shard_count: default_memtable_max_shard_count(),
            shard_channel_capacity: default_memtable_shard_channel_capacity(),
            batch_size: default_memtable_batch_size(),
            max_memory_mb: default_memtable_max_memory_mb(),
            write_buffer_size_mb: default_memtable_write_buffer_size_mb(),
            compression: default_memtable_compression(),
        }
    }
}

impl Validatable for MemtableActorConfig {
    fn validate(&self) -> Vec<ValidationResult> {
        let mut results = Vec::new();
        if self.initial_shard_count > self.max_shard_count {
            results.push(ValidationResult::error(
                "actors.memtable.initial_shard_count",
                self.initial_shard_count as i64,
                "Initial shard count cannot exceed max shard count"
            ));
        }
        results
    }
    
    fn apply_safe_defaults(&mut self) {
        self.initial_shard_count = 4; // Conservative shard count
        self.max_shard_count = 8; // Limited max shards
        self.shard_channel_capacity = 1000; // Smaller channel capacity
        self.max_memory_mb = 2048; // Conservative memory limit
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlusherActorConfig {
    #[serde(default = "default_flusher_memory_threshold_mb")]
    pub flush_memory_threshold_mb: u32,
    
    #[serde(default = "default_flusher_flush_seconds")]
    pub flush_seconds: u32,
    
    #[serde(default = "default_flusher_batch_size")]
    pub batch_size: u32,
    
    #[serde(default = "default_flusher_output_dir")]
    pub output_dir: String,
    
    #[serde(default = "default_flusher_max_concurrent_flushes")]
    pub max_concurrent_flushes: u32,
}

impl Default for FlusherActorConfig {
    fn default() -> Self {
        Self {
            flush_memory_threshold_mb: default_flusher_memory_threshold_mb(),
            flush_seconds: default_flusher_flush_seconds(),
            batch_size: default_flusher_batch_size(),
            output_dir: default_flusher_output_dir(),
            max_concurrent_flushes: default_flusher_max_concurrent_flushes(),
        }
    }
}

impl Validatable for FlusherActorConfig {
    fn validate(&self) -> Vec<ValidationResult> {
        Vec::new() // Add specific validations as needed
    }
    
    fn apply_safe_defaults(&mut self) {
        self.flush_memory_threshold_mb = 256; // Lower threshold for safety
        self.batch_size = 10000; // Smaller batches
        self.max_concurrent_flushes = 2; // Limited concurrent flushes
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MonitorActorConfig {
    #[serde(default = "default_monitor_check_interval_seconds")]
    pub check_interval_seconds: u32,
    
    #[serde(default = "default_monitor_cpu_threshold_percent")]
    pub cpu_threshold_percent: u32,
    
    #[serde(default = "default_monitor_memory_threshold_percent")]
    pub memory_threshold_percent: u32,
    
    #[serde(default = "default_monitor_disk_threshold_percent")]
    pub disk_threshold_percent: u32,
}

impl Default for MonitorActorConfig {
    fn default() -> Self {
        Self {
            check_interval_seconds: default_monitor_check_interval_seconds(),
            cpu_threshold_percent: default_monitor_cpu_threshold_percent(),
            memory_threshold_percent: default_monitor_memory_threshold_percent(),
            disk_threshold_percent: default_monitor_disk_threshold_percent(),
        }
    }
}

// Default functions
fn default_simple_ingest_memory_buffer_mb() -> u32 { 512 }
fn default_simple_ingest_batch_timeout_seconds() -> u32 { 30 }
fn default_simple_ingest_max_batch_size() -> u32 { 10000 }
fn default_memtable_initial_shard_count() -> u32 { 8 }
fn default_memtable_max_shard_count() -> u32 { 32 }
fn default_memtable_shard_channel_capacity() -> u32 { 10000 }
fn default_memtable_batch_size() -> u32 { 10000 }
fn default_memtable_max_memory_mb() -> u32 { 8192 }
fn default_memtable_write_buffer_size_mb() -> u32 { 256 }
fn default_memtable_compression() -> String { "zstd".to_string() }
fn default_flusher_memory_threshold_mb() -> u32 { 512 }
fn default_flusher_flush_seconds() -> u32 { 10 }
fn default_flusher_batch_size() -> u32 { 50000 }
fn default_flusher_output_dir() -> String { "./data/wal".to_string() }
fn default_flusher_max_concurrent_flushes() -> u32 { 4 }
fn default_monitor_check_interval_seconds() -> u32 { 30 }
fn default_monitor_cpu_threshold_percent() -> u32 { 80 }
fn default_monitor_memory_threshold_percent() -> u32 { 85 }
fn default_monitor_disk_threshold_percent() -> u32 { 90 }
