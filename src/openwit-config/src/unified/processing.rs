use serde::{Deserialize, Serialize};
use crate::unified::validation::{Validatable, ValidationResult, limits, safe_defaults};
use tracing::info;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ProcessingConfig {
    pub buffer: BufferConfig,
    pub lsm_engine: LsmEngineConfig,
    pub system_metrics: SystemMetricsConfig,
    // WAL removed - no longer needed
    pub pipeline: PipelineConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BufferConfig {
    #[serde(default = "default_max_size_messages")]
    pub max_size_messages: u64,
    
    #[serde(default = "default_max_size_bytes")]
    pub max_size_bytes: u64,
    
    #[serde(default = "default_queue_capacity")]
    pub queue_capacity: u64,
    
    #[serde(default = "default_flush_interval_seconds")]
    pub flush_interval_seconds: u64,
    
    #[serde(default = "default_flush_size_messages")]
    pub flush_size_messages: u64,
    
    #[serde(default = "default_flush_size_bytes")]
    pub flush_size_bytes: u64,
    
    #[serde(default = "default_buffer_batch_timeout_ms")]
    pub batch_timeout_ms: u64,
    
    #[serde(default = "default_worker_threads")]
    pub worker_threads: u32,
    
    #[serde(default = "default_io_threads")]
    pub io_threads: u32,
    
    #[serde(default = "default_compression")]
    pub compression: String,
    
    #[serde(default = "default_true")]
    pub dedup_enabled: bool,
    
    #[serde(default = "default_dedup_window_seconds")]
    pub dedup_window_seconds: u64,
}

impl Default for BufferConfig {
    fn default() -> Self {
        Self {
            max_size_messages: default_max_size_messages(),
            max_size_bytes: default_max_size_bytes(),
            queue_capacity: default_queue_capacity(),
            flush_interval_seconds: default_flush_interval_seconds(),
            flush_size_messages: default_flush_size_messages(),
            flush_size_bytes: default_flush_size_bytes(),
            batch_timeout_ms: default_buffer_batch_timeout_ms(),
            worker_threads: default_worker_threads(),
            io_threads: default_io_threads(),
            compression: default_compression(),
            dedup_enabled: default_true(),
            dedup_window_seconds: default_dedup_window_seconds(),
        }
    }
}

fn default_max_size_messages() -> u64 { safe_defaults::BUFFER_MAX_SIZE_MESSAGES }
fn default_max_size_bytes() -> u64 { safe_defaults::BUFFER_MAX_SIZE_BYTES }
fn default_queue_capacity() -> u64 { safe_defaults::BUFFER_MAX_SIZE_MESSAGES }
fn default_flush_interval_seconds() -> u64 { 30 }
fn default_flush_size_messages() -> u64 { 10000 } // Safe default
fn default_flush_size_bytes() -> u64 { 104857600 } // 100MB safe default
fn default_buffer_batch_timeout_ms() -> u64 { 1000 }
fn default_worker_threads() -> u32 { safe_defaults::BUFFER_WORKER_THREADS }
fn default_io_threads() -> u32 { safe_defaults::BUFFER_IO_THREADS }
fn default_compression() -> String { "snappy".to_string() }
fn default_true() -> bool { true }
fn default_dedup_window_seconds() -> u64 { 60 }

impl Validatable for BufferConfig {
    fn validate(&self) -> Vec<ValidationResult> {
        let mut results = Vec::new();
        results.push(limits::buffer_worker_threads().validate(self.worker_threads as i64));
        results.push(limits::buffer_max_size_bytes().validate(self.max_size_bytes as i64));
        results
    }
    
    fn apply_safe_defaults(&mut self) {
        info!("Applying safe defaults to Buffer configuration");
        self.worker_threads = safe_defaults::BUFFER_WORKER_THREADS;
        self.max_size_bytes = safe_defaults::BUFFER_MAX_SIZE_BYTES;
        self.max_size_messages = safe_defaults::BUFFER_MAX_SIZE_MESSAGES;
        self.io_threads = safe_defaults::BUFFER_IO_THREADS;
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LsmEngineConfig {
    #[serde(default = "default_memtable_size_limit_mb")]
    pub memtable_size_limit_mb: u32,
    
    #[serde(default = "default_bloom_filter_bits")]
    pub bloom_filter_bits: u32,
    
    #[serde(default = "default_bloom_false_positive_rate")]
    pub bloom_false_positive_rate: f64,
    
    #[serde(default = "default_write_buffer_size_mb")]
    pub write_buffer_size_mb: u32,
    
    #[serde(default = "default_max_write_buffer_number")]
    pub max_write_buffer_number: u32,
    
    #[serde(default = "default_level0_file_num_compaction_trigger")]
    pub level0_file_num_compaction_trigger: u32,
}

fn default_memtable_size_limit_mb() -> u32 { safe_defaults::MEMTABLE_SIZE_LIMIT_MB }
fn default_bloom_filter_bits() -> u32 { 10 }
fn default_bloom_false_positive_rate() -> f64 { 0.01 }
fn default_write_buffer_size_mb() -> u32 { 128 } // Safe default
fn default_max_write_buffer_number() -> u32 { 2 } // Safe default
fn default_level0_file_num_compaction_trigger() -> u32 { 4 }

impl Validatable for LsmEngineConfig {
    fn validate(&self) -> Vec<ValidationResult> {
        let mut results = Vec::new();
        results.push(limits::memtable_size_limit_mb().validate(self.memtable_size_limit_mb as i64));
        results
    }
    
    fn apply_safe_defaults(&mut self) {
        self.memtable_size_limit_mb = safe_defaults::MEMTABLE_SIZE_LIMIT_MB;
        self.write_buffer_size_mb = 128;
        self.max_write_buffer_number = 2;
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SystemMetricsConfig {
    #[serde(default = "default_collection_interval_seconds")]
    pub collection_interval_seconds: u64,
    
    #[serde(default = "default_true")]
    pub enable_cpu_metrics: bool,
    
    #[serde(default = "default_true")]
    pub enable_memory_metrics: bool,
    
    #[serde(default = "default_true")]
    pub enable_disk_metrics: bool,
    
    #[serde(default = "default_true")]
    pub enable_network_metrics: bool,
}

fn default_collection_interval_seconds() -> u64 { 5 }

// WAL configuration removed - no longer needed

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PipelineConfig {
    #[serde(default = "default_pipeline_batch_size")]
    pub batch_size: u32,
    
    #[serde(default = "default_pipeline_batch_timeout_seconds")]
    pub batch_timeout_seconds: u64,
    
    #[serde(default = "default_worker_threads")]
    pub worker_threads: u32,
    
    #[serde(default = "default_pipeline_queue_size")]
    pub queue_size: u32,
    
    #[serde(default)]
    pub backpressure: BackpressureConfig,
}

fn default_pipeline_batch_size() -> u32 { safe_defaults::PIPELINE_BATCH_SIZE }
fn default_pipeline_batch_timeout_seconds() -> u64 { 5 }
fn default_pipeline_queue_size() -> u32 { safe_defaults::PIPELINE_QUEUE_SIZE }

impl Validatable for PipelineConfig {
    fn validate(&self) -> Vec<ValidationResult> {
        Vec::new() // Add specific validations as needed
    }
    
    fn apply_safe_defaults(&mut self) {
        self.batch_size = safe_defaults::PIPELINE_BATCH_SIZE;
        self.queue_size = safe_defaults::PIPELINE_QUEUE_SIZE;
        self.worker_threads = safe_defaults::BUFFER_WORKER_THREADS;
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackpressureConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    
    #[serde(default = "default_initial_queue_size")]
    pub initial_queue_size: u32,
    
    #[serde(default = "default_min_queue_size")]
    pub min_queue_size: u32,
    
    #[serde(default = "default_max_queue_size")]
    pub max_queue_size: u32,
    
    #[serde(default = "default_target_utilization_percent")]
    pub target_utilization_percent: f64,
    
    #[serde(default = "default_growth_factor")]
    pub growth_factor: f64,
    
    #[serde(default = "default_shrink_factor")]
    pub shrink_factor: f64,
    
    #[serde(default = "default_check_interval_seconds")]
    pub check_interval_seconds: u64,
    
    #[serde(default = "default_drop_policy")]
    pub drop_policy: String,
}

impl Default for BackpressureConfig {
    fn default() -> Self {
        Self {
            enabled: default_true(),
            initial_queue_size: default_initial_queue_size(),
            min_queue_size: default_min_queue_size(),
            max_queue_size: default_max_queue_size(),
            target_utilization_percent: default_target_utilization_percent(),
            growth_factor: default_growth_factor(),
            shrink_factor: default_shrink_factor(),
            check_interval_seconds: default_check_interval_seconds(),
            drop_policy: default_drop_policy(),
        }
    }
}

fn default_initial_queue_size() -> u32 { 1000 }
fn default_min_queue_size() -> u32 { 500 }
fn default_max_queue_size() -> u32 { 100000 }
fn default_target_utilization_percent() -> f64 { 70.0 }
fn default_growth_factor() -> f64 { 1.5 }
fn default_shrink_factor() -> f64 { 0.8 }
fn default_check_interval_seconds() -> u64 { 10 }
fn default_drop_policy() -> String { "drop_oldest".to_string() }