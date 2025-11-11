use serde::{Deserialize, Serialize};
use crate::unified::validation::{Validatable, ValidationResult, limits};
use tracing::info;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexingConfig {
    #[serde(default = "default_worker_threads")]
    pub worker_threads: u32,
    
    #[serde(default = "default_index_writer_threads")]
    pub index_writer_threads: u32,
    
    #[serde(default)]
    pub index: IndexConfig,
    
    #[serde(default)]
    pub datafusion: DataFusionConfig,
    
    #[serde(default)]
    pub ballista: BallistaConfig,
    
    #[serde(default)]
    pub tantivy: TantivyConfig,
    
    #[serde(default)]
    pub partitioning: PartitioningConfig,
}

impl Default for IndexingConfig {
    fn default() -> Self {
        Self {
            worker_threads: default_worker_threads(),
            index_writer_threads: default_index_writer_threads(),
            index: IndexConfig::default(),
            datafusion: DataFusionConfig::default(),
            ballista: BallistaConfig::default(),
            tantivy: TantivyConfig::default(),
            partitioning: PartitioningConfig::default(),
        }
    }
}

impl Validatable for IndexingConfig {
    fn validate(&self) -> Vec<ValidationResult> {
        let mut results = Vec::new();
        results.push(limits::worker_threads().validate(self.worker_threads as i64));
        results.extend(self.datafusion.validate());
        results.extend(self.tantivy.validate());
        results
    }
    
    fn apply_safe_defaults(&mut self) {
        info!("Applying safe defaults to Indexing configuration");
        self.worker_threads = 4; // Conservative worker threads
        self.index_writer_threads = 2; // Conservative writer threads
        self.datafusion.apply_safe_defaults();
        self.tantivy.apply_safe_defaults();
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexConfig {
    #[serde(default = "default_index_memory_mb")]
    pub max_memory_mb: u32,
    
    #[serde(default = "default_schema_version")]
    pub schema_version: u32,
}

impl Default for IndexConfig {
    fn default() -> Self {
        Self {
            max_memory_mb: default_index_memory_mb(),
            schema_version: default_schema_version(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataFusionConfig {
    #[serde(default = "default_datafusion_batch_size")]
    pub batch_size: u32,
    
    #[serde(default = "default_datafusion_max_threads")]
    pub max_threads: u32,
    
    #[serde(default = "default_datafusion_memory_limit_gb")]
    pub memory_limit_gb: u32,
    
    #[serde(default = "default_true")]
    pub enable_optimizer: bool,
}

impl Default for DataFusionConfig {
    fn default() -> Self {
        Self {
            batch_size: default_datafusion_batch_size(),
            max_threads: default_datafusion_max_threads(),
            memory_limit_gb: default_datafusion_memory_limit_gb(),
            enable_optimizer: default_true(),
        }
    }
}

impl Validatable for DataFusionConfig {
    fn validate(&self) -> Vec<ValidationResult> {
        let mut results = Vec::new();
        results.push(limits::datafusion_batch_size().validate(self.batch_size as i64));
        results
    }
    
    fn apply_safe_defaults(&mut self) {
        self.batch_size = 8192; // Conservative batch size
        self.max_threads = 4; // Conservative thread count
        self.memory_limit_gb = 2; // Conservative memory limit
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BallistaConfig {
    #[serde(default)]
    pub enabled: bool,
    
    #[serde(default = "default_ballista_scheduler_host")]
    pub scheduler_host: String,
    
    #[serde(default = "default_ballista_scheduler_port")]
    pub scheduler_port: u16,
    
    #[serde(default = "default_ballista_partitions")]
    pub partitions: u32,
    
    #[serde(default = "default_ballista_memory_limit_gb")]
    pub memory_limit_gb: u32,
    
    #[serde(default = "default_ballista_max_threads")]
    pub max_threads: u32,
}

impl Default for BallistaConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            scheduler_host: default_ballista_scheduler_host(),
            scheduler_port: default_ballista_scheduler_port(),
            partitions: default_ballista_partitions(),
            memory_limit_gb: default_ballista_memory_limit_gb(),
            max_threads: default_ballista_max_threads(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TantivyConfig {
    #[serde(default = "default_tantivy_writer_heap_size_mb")]
    pub writer_heap_size_mb: u32,
    
    #[serde(default = "default_tantivy_max_docs_before_commit")]
    pub max_docs_before_commit: u32,
    
    #[serde(default = "default_tantivy_commit_interval_seconds")]
    pub commit_interval_seconds: u32,
    
    #[serde(default = "default_tantivy_query_limit")]
    pub query_limit_default: u32,
}

impl Default for TantivyConfig {
    fn default() -> Self {
        Self {
            writer_heap_size_mb: default_tantivy_writer_heap_size_mb(),
            max_docs_before_commit: default_tantivy_max_docs_before_commit(),
            commit_interval_seconds: default_tantivy_commit_interval_seconds(),
            query_limit_default: default_tantivy_query_limit(),
        }
    }
}

impl Validatable for TantivyConfig {
    fn validate(&self) -> Vec<ValidationResult> {
        let mut results = Vec::new();
        results.push(limits::tantivy_query_limit().validate(self.query_limit_default as i64));
        results
    }
    
    fn apply_safe_defaults(&mut self) {
        self.writer_heap_size_mb = 256; // Conservative heap size
        self.max_docs_before_commit = 10000; // More frequent commits
        self.query_limit_default = 100; // Conservative query limit
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitioningConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    
    #[serde(default = "default_partition_interval")]
    pub interval: String,
    
    #[serde(default = "default_partition_format")]
    pub format: String,
}

impl Default for PartitioningConfig {
    fn default() -> Self {
        Self {
            enabled: default_true(),
            interval: default_partition_interval(),
            format: default_partition_format(),
        }
    }
}

// Default functions
fn default_true() -> bool { true }
fn default_worker_threads() -> u32 { 8 }
fn default_index_writer_threads() -> u32 { 4 }
fn default_index_memory_mb() -> u32 { 1024 }
fn default_schema_version() -> u32 { 1 }
fn default_datafusion_batch_size() -> u32 { 32768 }
fn default_datafusion_max_threads() -> u32 { 16 }
fn default_datafusion_memory_limit_gb() -> u32 { 4 }
fn default_ballista_scheduler_host() -> String { "localhost".to_string() }
fn default_ballista_scheduler_port() -> u16 { 50050 }
fn default_ballista_partitions() -> u32 { 16 }
fn default_ballista_memory_limit_gb() -> u32 { 8 }
fn default_ballista_max_threads() -> u32 { 16 }
fn default_tantivy_writer_heap_size_mb() -> u32 { 512 }
fn default_tantivy_max_docs_before_commit() -> u32 { 100000 }
fn default_tantivy_commit_interval_seconds() -> u32 { 30 }
fn default_tantivy_query_limit() -> u32 { 1000 }
fn default_partition_interval() -> String { "1h".to_string() }
fn default_partition_format() -> String { "YYYY/MM/DD/HH".to_string() }
