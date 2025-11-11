use serde::{Deserialize, Serialize};
use crate::unified::validation::{Validatable, ValidationResult, limits, safe_defaults};
use tracing::info;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryConfig {
    #[serde(default)]
    pub heap: HeapConfig,
    
    #[serde(default)]
    pub buffer_pool: BufferPoolConfig,
    
    #[serde(default)]
    pub cache: CacheConfig,
    
    #[serde(default)]
    pub mmap: MmapConfig,
    
    #[serde(default)]
    pub allocator: AllocatorConfig,
    
    #[serde(default)]
    pub profiling: ProfilingConfig,
}

impl Default for MemoryConfig {
    fn default() -> Self {
        Self {
            heap: HeapConfig::default(),
            buffer_pool: BufferPoolConfig::default(),
            cache: CacheConfig::default(),
            mmap: MmapConfig::default(),
            allocator: AllocatorConfig::default(),
            profiling: ProfilingConfig::default(),
        }
    }
}

impl Validatable for MemoryConfig {
    fn validate(&self) -> Vec<ValidationResult> {
        let mut results = Vec::new();
        results.extend(self.heap.validate());
        results.extend(self.buffer_pool.validate());
        results.extend(self.cache.validate());
        results
    }
    
    fn apply_safe_defaults(&mut self) {
        info!("Applying safe defaults to Memory configuration");
        self.heap.apply_safe_defaults();
        self.buffer_pool.apply_safe_defaults();
        self.cache.apply_safe_defaults();
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeapConfig {
    #[serde(default = "default_initial_size_gb")]
    pub initial_size_gb: u32,
    
    #[serde(default = "default_max_size_gb")]
    pub max_size_gb: u32,
    
    #[serde(default = "default_young_gen_size_mb")]
    pub young_gen_size_mb: u32,
    
    #[serde(default = "default_gc_threshold_percent")]
    pub gc_threshold_percent: u32,
}

impl Default for HeapConfig {
    fn default() -> Self {
        Self {
            initial_size_gb: default_initial_size_gb(),
            max_size_gb: default_max_size_gb(),
            young_gen_size_mb: default_young_gen_size_mb(),
            gc_threshold_percent: default_gc_threshold_percent(),
        }
    }
}

impl Validatable for HeapConfig {
    fn validate(&self) -> Vec<ValidationResult> {
        let mut results = Vec::new();
        results.push(limits::heap_max_size_gb().validate(self.max_size_gb as i64));
        
        // Validate initial <= max
        if self.initial_size_gb > self.max_size_gb {
            results.push(ValidationResult::Warning {
                field: "heap.initial_size_gb".to_string(),
                value: self.initial_size_gb as i64,
                recommended: self.max_size_gb as i64,
                message: "initial_size_gb is greater than max_size_gb".to_string(),
            });
        }
        
        results
    }
    
    fn apply_safe_defaults(&mut self) {
        self.initial_size_gb = safe_defaults::HEAP_INITIAL_SIZE_GB;
        self.max_size_gb = safe_defaults::HEAP_MAX_SIZE_GB;
        self.young_gen_size_mb = 512; // Conservative young gen size
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BufferPoolConfig {
    #[serde(default = "default_size_mb")]
    pub size_mb: u32,
    
    #[serde(default = "default_page_size_kb")]
    pub page_size_kb: u32,
    
    #[serde(default = "default_direct_io_enabled")]
    pub direct_io_enabled: bool,
    
    #[serde(default = "default_preallocate")]
    pub preallocate: bool,
}

impl Default for BufferPoolConfig {
    fn default() -> Self {
        Self {
            size_mb: default_size_mb(),
            page_size_kb: default_page_size_kb(),
            direct_io_enabled: default_direct_io_enabled(),
            preallocate: default_preallocate(),
        }
    }
}

impl Validatable for BufferPoolConfig {
    fn validate(&self) -> Vec<ValidationResult> {
        let mut results = Vec::new();
        results.push(limits::buffer_pool_size_mb().validate(self.size_mb as i64));
        results
    }
    
    fn apply_safe_defaults(&mut self) {
        self.size_mb = safe_defaults::BUFFER_POOL_SIZE_MB;
        self.preallocate = false; // Don't preallocate in safe mode
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    
    #[serde(default = "default_max_entries")]
    pub max_entries: u64,
    
    #[serde(default = "default_ttl_seconds")]
    pub ttl_seconds: u64,
    
    #[serde(default = "default_eviction_policy")]
    pub eviction_policy: String,
    
    #[serde(default = "default_true")]
    pub compression_enabled: bool,
    
    #[serde(default)]
    pub l1_cache: L1CacheConfig,
    
    #[serde(default)]
    pub l2_cache: L2CacheConfig,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            enabled: default_true(),
            max_entries: default_max_entries(),
            ttl_seconds: default_ttl_seconds(),
            eviction_policy: default_eviction_policy(),
            compression_enabled: default_true(),
            l1_cache: L1CacheConfig::default(),
            l2_cache: L2CacheConfig::default(),
        }
    }
}

impl Validatable for CacheConfig {
    fn validate(&self) -> Vec<ValidationResult> {
        let mut results = Vec::new();
        results.push(limits::cache_max_entries().validate(self.max_entries as i64));
        results
    }
    
    fn apply_safe_defaults(&mut self) {
        self.max_entries = safe_defaults::CACHE_MAX_ENTRIES;
        self.l1_cache.size_mb = 256; // Conservative L1 cache
        self.l2_cache.size_mb = 512; // Conservative L2 cache
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct L1CacheConfig {
    #[serde(default = "default_l1_size_mb")]
    pub size_mb: u32,
    
    #[serde(default = "default_true")]
    pub enabled: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct L2CacheConfig {
    #[serde(default = "default_l2_size_mb")]
    pub size_mb: u32,
    
    #[serde(default = "default_true")]
    pub enabled: bool,
    
    #[serde(default = "default_l2_tier")]
    pub tier: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MmapConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    
    #[serde(default = "default_max_files")]
    pub max_files: u32,
    
    #[serde(default = "default_max_size_gb")]
    pub max_size_gb: u32,
    
    #[serde(default = "default_true")]
    pub hugepages_enabled: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AllocatorConfig {
    #[serde(default = "default_allocator_type")]
    pub allocator_type: String,
    
    #[serde(default = "default_jemalloc")]
    pub jemalloc: JemallocConfig,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct JemallocConfig {
    #[serde(default = "default_true")]
    pub background_thread: bool,
    
    #[serde(default = "default_dirty_decay_ms")]
    pub dirty_decay_ms: u64,
    
    #[serde(default = "default_muzzy_decay_ms")]
    pub muzzy_decay_ms: u64,
    
    #[serde(default = "default_narenas")]
    pub narenas: u32,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ProfilingConfig {
    #[serde(default)]
    pub enabled: bool,
    
    #[serde(default = "default_profile_interval_seconds")]
    pub interval_seconds: u64,
    
    #[serde(default = "default_profile_output_dir")]
    pub output_dir: String,
    
    #[serde(default)]
    pub heap_profiling: bool,
    
    #[serde(default)]
    pub allocation_tracking: bool,
}

// Default functions
fn default_true() -> bool { true }
fn default_initial_size_gb() -> u32 { safe_defaults::HEAP_INITIAL_SIZE_GB }
fn default_max_size_gb() -> u32 { safe_defaults::HEAP_MAX_SIZE_GB }
fn default_young_gen_size_mb() -> u32 { 1024 }
fn default_gc_threshold_percent() -> u32 { 75 }
fn default_size_mb() -> u32 { safe_defaults::BUFFER_POOL_SIZE_MB }
fn default_page_size_kb() -> u32 { 4 }
fn default_direct_io_enabled() -> bool { false }
fn default_preallocate() -> bool { true }
fn default_max_entries() -> u64 { safe_defaults::CACHE_MAX_ENTRIES }
fn default_ttl_seconds() -> u64 { 3600 }
fn default_eviction_policy() -> String { "lru".to_string() }
fn default_l1_size_mb() -> u32 { 512 }
fn default_l2_size_mb() -> u32 { 2048 }
fn default_l2_tier() -> String { "ssd".to_string() }
fn default_max_files() -> u32 { 100 }
fn default_allocator_type() -> String { "jemalloc".to_string() }
fn default_dirty_decay_ms() -> u64 { 10000 }
fn default_muzzy_decay_ms() -> u64 { 0 }
fn default_narenas() -> u32 { 8 }
fn default_profile_interval_seconds() -> u64 { 60 }
fn default_profile_output_dir() -> String { "./profiling".to_string() }
fn default_jemalloc() -> JemallocConfig { JemallocConfig::default() }