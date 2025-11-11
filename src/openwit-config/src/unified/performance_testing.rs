use serde::{Deserialize, Serialize};
use crate::unified::validation::{Validatable, ValidationResult};
use tracing::info;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceTestingConfig {
    #[serde(default)]
    pub load_generator: LoadGeneratorConfig,
    
    #[serde(default)]
    pub presets: PerformancePresets,
}

impl Default for PerformanceTestingConfig {
    fn default() -> Self {
        Self {
            load_generator: LoadGeneratorConfig::default(),
            presets: PerformancePresets::default(),
        }
    }
}

impl Validatable for PerformanceTestingConfig {
    fn validate(&self) -> Vec<ValidationResult> {
        Vec::new() // Add specific validations as needed
    }
    
    fn apply_safe_defaults(&mut self) {
        info!("Applying safe defaults to Performance Testing configuration");
        self.load_generator.apply_safe_defaults();
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoadGeneratorConfig {
    #[serde(default = "default_total_messages")]
    pub total_messages: u64,
    
    #[serde(default = "default_batch_size")]
    pub batch_size: u32,
    
    #[serde(default = "default_concurrent_connections")]
    pub concurrent_connections: u32,
    
    #[serde(default = "default_message_size_bytes")]
    pub message_size_bytes: u32,
}

impl Default for LoadGeneratorConfig {
    fn default() -> Self {
        Self {
            total_messages: default_total_messages(),
            batch_size: default_batch_size(),
            concurrent_connections: default_concurrent_connections(),
            message_size_bytes: default_message_size_bytes(),
        }
    }
}

impl LoadGeneratorConfig {
    fn apply_safe_defaults(&mut self) {
        self.total_messages = 10000; // Much smaller test in safe mode
        self.batch_size = 100;
        self.concurrent_connections = 5;
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformancePresets {
    #[serde(default)]
    pub billion_logs: BillionLogsPreset,
    
    #[serde(default)]
    pub high_throughput: HighThroughputPreset,
    
    #[serde(default)]
    pub low_latency: LowLatencyPreset,
    
    #[serde(default)]
    pub memory_stress: MemoryStressPreset,
    
    #[serde(default)]
    pub bandwidth_test: BandwidthTestPreset,
}

impl Default for PerformancePresets {
    fn default() -> Self {
        Self {
            billion_logs: BillionLogsPreset::default(),
            high_throughput: HighThroughputPreset::default(),
            low_latency: LowLatencyPreset::default(),
            memory_stress: MemoryStressPreset::default(),
            bandwidth_test: BandwidthTestPreset::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BillionLogsPreset {
    #[serde(default)]
    pub enabled: bool,
    
    #[serde(default = "default_billion_total_messages")]
    pub total_messages: u64,
    
    #[serde(default = "default_billion_batch_size")]
    pub batch_size: u32,
    
    #[serde(default = "default_billion_concurrent_connections")]
    pub concurrent_connections: u32,
    
    #[serde(default = "default_billion_target_throughput")]
    pub target_throughput_msg_sec: u32,
}

impl Default for BillionLogsPreset {
    fn default() -> Self {
        Self {
            enabled: false,
            total_messages: default_billion_total_messages(),
            batch_size: default_billion_batch_size(),
            concurrent_connections: default_billion_concurrent_connections(),
            target_throughput_msg_sec: default_billion_target_throughput(),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct HighThroughputPreset {
    #[serde(default = "default_high_throughput_total_messages")]
    pub total_messages: u64,
    
    #[serde(default = "default_high_throughput_batch_size")]
    pub batch_size: u32,
    
    #[serde(default = "default_high_throughput_concurrent_connections")]
    pub concurrent_connections: u32,
    
    #[serde(default = "default_high_throughput_message_size")]
    pub message_size_bytes: u32,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LowLatencyPreset {
    #[serde(default = "default_low_latency_total_messages")]
    pub total_messages: u64,
    
    #[serde(default = "default_low_latency_batch_size")]
    pub batch_size: u32,
    
    #[serde(default = "default_low_latency_concurrent_connections")]
    pub concurrent_connections: u32,
    
    #[serde(default = "default_low_latency_message_size")]
    pub message_size_bytes: u32,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MemoryStressPreset {
    #[serde(default = "default_memory_stress_total_messages")]
    pub total_messages: u64,
    
    #[serde(default = "default_memory_stress_batch_size")]
    pub batch_size: u32,
    
    #[serde(default = "default_memory_stress_concurrent_connections")]
    pub concurrent_connections: u32,
    
    #[serde(default = "default_memory_stress_limit_gb")]
    pub memory_limit_gb: u32,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BandwidthTestPreset {
    #[serde(default = "default_bandwidth_total_messages")]
    pub total_messages: u64,
    
    #[serde(default = "default_bandwidth_batch_size")]
    pub batch_size: u32,
    
    #[serde(default = "default_bandwidth_concurrent_connections")]
    pub concurrent_connections: u32,
    
    #[serde(default = "default_bandwidth_message_size")]
    pub message_size_bytes: u32,
}

// Default functions
fn default_total_messages() -> u64 { 1000000 }
fn default_batch_size() -> u32 { 1000 }
fn default_concurrent_connections() -> u32 { 50 }
fn default_message_size_bytes() -> u32 { 1024 }
fn default_billion_total_messages() -> u64 { 1000000000 }
fn default_billion_batch_size() -> u32 { 10000 }
fn default_billion_concurrent_connections() -> u32 { 100 }
fn default_billion_target_throughput() -> u32 { 700000 }
fn default_high_throughput_total_messages() -> u64 { 100000000 }
fn default_high_throughput_batch_size() -> u32 { 5000 }
fn default_high_throughput_concurrent_connections() -> u32 { 50 }
fn default_high_throughput_message_size() -> u32 { 512 }
fn default_low_latency_total_messages() -> u64 { 10000000 }
fn default_low_latency_batch_size() -> u32 { 100 }
fn default_low_latency_concurrent_connections() -> u32 { 10 }
fn default_low_latency_message_size() -> u32 { 256 }
fn default_memory_stress_total_messages() -> u64 { 500000000 }
fn default_memory_stress_batch_size() -> u32 { 20000 }
fn default_memory_stress_concurrent_connections() -> u32 { 25 }
fn default_memory_stress_limit_gb() -> u32 { 16 }
fn default_bandwidth_total_messages() -> u64 { 50000000 }
fn default_bandwidth_batch_size() -> u32 { 1000 }
fn default_bandwidth_concurrent_connections() -> u32 { 100 }
fn default_bandwidth_message_size() -> u32 { 10240 }
