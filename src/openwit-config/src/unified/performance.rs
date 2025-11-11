use serde::{Deserialize, Serialize};
use crate::unified::validation::{Validatable, ValidationResult};
use tracing::info;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceConfig {
    #[serde(default)]
    pub threads: ThreadConfig,
    
    #[serde(default)]
    pub memory: MemoryPerformanceConfig,
    
    #[serde(default)]
    pub io: IoConfig,
    
    #[serde(default)]
    pub network: NetworkPerformanceConfig,
    
    #[serde(default)]
    pub cpu: CpuConfig,
}

impl Default for PerformanceConfig {
    fn default() -> Self {
        Self {
            threads: ThreadConfig::default(),
            memory: MemoryPerformanceConfig::default(),
            io: IoConfig::default(),
            network: NetworkPerformanceConfig::default(),
            cpu: CpuConfig::default(),
        }
    }
}

impl Validatable for PerformanceConfig {
    fn validate(&self) -> Vec<ValidationResult> {
        Vec::new() // Add specific validations as needed
    }
    
    fn apply_safe_defaults(&mut self) {
        info!("Applying safe defaults to Performance configuration");
        self.threads.apply_safe_defaults();
        self.io.apply_safe_defaults();
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThreadConfig {
    #[serde(default = "default_worker_threads")]
    pub worker_threads: u32,
    
    #[serde(default = "default_blocking_threads")]
    pub blocking_threads: u32,
    
    #[serde(default = "default_io_threads")]
    pub io_threads: u32,
}

impl Default for ThreadConfig {
    fn default() -> Self {
        Self {
            worker_threads: default_worker_threads(),
            blocking_threads: default_blocking_threads(),
            io_threads: default_io_threads(),
        }
    }
}

impl ThreadConfig {
    fn apply_safe_defaults(&mut self) {
        self.worker_threads = 8; // Conservative thread count
        self.blocking_threads = 16;
        self.io_threads = 4;
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryPerformanceConfig {
    #[serde(default = "default_allocator")]
    pub allocator: String,
    
    #[serde(default = "default_memory_limit_gb")]
    pub memory_limit_gb: u32,
    
    #[serde(default)]
    pub enable_memory_profiling: bool,
}

impl Default for MemoryPerformanceConfig {
    fn default() -> Self {
        Self {
            allocator: default_allocator(),
            memory_limit_gb: default_memory_limit_gb(),
            enable_memory_profiling: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IoConfig {
    #[serde(default = "default_true")]
    pub use_io_uring: bool,
    
    #[serde(default = "default_true")]
    pub direct_io: bool,
    
    #[serde(default)]
    pub sync_io: bool,
}

impl Default for IoConfig {
    fn default() -> Self {
        Self {
            use_io_uring: default_true(),
            direct_io: default_true(),
            sync_io: false,
        }
    }
}

impl IoConfig {
    fn apply_safe_defaults(&mut self) {
        self.use_io_uring = false; // Disable advanced features in safe mode
        self.direct_io = false;
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkPerformanceConfig {
    #[serde(default = "default_true")]
    pub tcp_nodelay: bool,
    
    #[serde(default = "default_tcp_keepalive_seconds")]
    pub tcp_keepalive_seconds: u32,
    
    #[serde(default = "default_socket_buffer_size_kb")]
    pub socket_buffer_size_kb: u32,
}

impl Default for NetworkPerformanceConfig {
    fn default() -> Self {
        Self {
            tcp_nodelay: default_true(),
            tcp_keepalive_seconds: default_tcp_keepalive_seconds(),
            socket_buffer_size_kb: default_socket_buffer_size_kb(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CpuConfig {
    #[serde(default = "default_true")]
    pub enable_simd: bool,
    
    #[serde(default)]
    pub cpu_affinity: Vec<u32>,
}

impl Default for CpuConfig {
    fn default() -> Self {
        Self {
            enable_simd: default_true(),
            cpu_affinity: Vec::new(),
        }
    }
}

// Default functions
fn default_true() -> bool { true }
fn default_worker_threads() -> u32 { 16 }
fn default_blocking_threads() -> u32 { 32 }
fn default_io_threads() -> u32 { 8 }
fn default_allocator() -> String { "jemalloc".to_string() }
fn default_memory_limit_gb() -> u32 { 32 }
fn default_tcp_keepalive_seconds() -> u32 { 60 }
fn default_socket_buffer_size_kb() -> u32 { 8192 }
