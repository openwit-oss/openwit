//! Storage Node Configuration Module
//! 
//! This module defines the comprehensive configuration for storage nodes
//! implementing the dual-phase publish model with LSM-tree storage.

use serde::{Deserialize, Serialize};
use crate::unified::validation::{Validatable, ValidationResult};
use tracing::info;

/// Main storage node configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageNodeConfig {
    #[serde(default)]
    pub general: GeneralNodeConfig,
    
    #[serde(default)]
    pub postgres: PostgresNodeConfig,
    
    #[serde(default)]
    pub cluster: ClusterNodeConfig,
    
    #[serde(default)]
    pub flight: FlightNodeConfig,
    
    #[serde(default)]
    pub lsm: LSMNodeConfig,
    
    #[serde(default)]
    pub parquet: ParquetNodeConfig,
    
    #[serde(default)]
    pub opendal: OpenDALNodeConfig,
    
    #[serde(default)]
    pub publish: PublishNodeConfig,
    
    #[serde(default)]
    pub gossip: GossipNodeConfig,
    
    #[serde(default)]
    pub wal: WALNodeConfig,
    
    #[serde(default)]
    pub compaction: CompactionNodeConfig,
}

impl Default for StorageNodeConfig {
    fn default() -> Self {
        Self {
            general: GeneralNodeConfig::default(),
            postgres: PostgresNodeConfig::default(),
            cluster: ClusterNodeConfig::default(),
            flight: FlightNodeConfig::default(),
            lsm: LSMNodeConfig::default(),
            parquet: ParquetNodeConfig::default(),
            opendal: OpenDALNodeConfig::default(),
            publish: PublishNodeConfig::default(),
            gossip: GossipNodeConfig::default(),
            wal: WALNodeConfig::default(),
            compaction: CompactionNodeConfig::default(),
        }
    }
}

impl Validatable for StorageNodeConfig {
    fn validate(&self) -> Vec<ValidationResult> {
        let mut results = Vec::new();
        results.extend(self.lsm.validate());
        results.extend(self.parquet.validate());
        results.extend(self.compaction.validate());
        results
    }
    
    fn apply_safe_defaults(&mut self) {
        info!("Applying safe defaults to Storage Node configuration");
        self.lsm.apply_safe_defaults();
        self.parquet.apply_safe_defaults();
        self.compaction.apply_safe_defaults();
    }
}

/// General storage node settings
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeneralNodeConfig {
    #[serde(default = "default_node_id")]
    pub node_id: String,
    
    #[serde(default = "default_storage_mode")]
    pub mode: String,
    
    #[serde(default = "default_work_dir")]
    pub work_dir: String,
    
    #[serde(default)]
    pub tenant_filter: Vec<String>,
}

impl Default for GeneralNodeConfig {
    fn default() -> Self {
        Self {
            node_id: default_node_id(),
            mode: default_storage_mode(),
            work_dir: default_work_dir(),
            tenant_filter: Vec::new(),
        }
    }
}

/// Postgres configuration for storage node
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostgresNodeConfig {
    #[serde(default = "default_postgres_dsn")]
    pub dsn: String,
    
    #[serde(default = "default_pool_size")]
    pub pool_size: u32,
}

impl Default for PostgresNodeConfig {
    fn default() -> Self {
        Self {
            dsn: default_postgres_dsn(),
            pool_size: default_pool_size(),
        }
    }
}

/// Cluster communication configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterNodeConfig {
    #[serde(default = "default_gossip_bind")]
    pub gossip_bind: String,
    
    #[serde(default = "default_health_push_interval_ms")]
    pub health_push_interval_ms: u64,
    
    #[serde(default = "default_control_request_timeout_ms")]
    pub control_request_timeout_ms: u64,
    
    #[serde(default = "default_weighted_rr_weights")]
    pub weighted_rr_weights: WeightedRRConfig,
}

impl Default for ClusterNodeConfig {
    fn default() -> Self {
        Self {
            gossip_bind: default_gossip_bind(),
            health_push_interval_ms: default_health_push_interval_ms(),
            control_request_timeout_ms: default_control_request_timeout_ms(),
            weighted_rr_weights: WeightedRRConfig::default(),
        }
    }
}

/// Weighted round-robin configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WeightedRRConfig {
    #[serde(default = "default_cpu_weight")]
    pub cpu_weight: f64,
    
    #[serde(default = "default_mem_weight")]
    pub mem_weight: f64,
    
    #[serde(default = "default_queue_weight")]
    pub queue_weight: f64,
    
    #[serde(default = "default_failure_weight")]
    pub failure_weight: f64,
    
    #[serde(default = "default_failure_cooloff_ms")]
    pub failure_cooloff_ms: u64,
}

impl Default for WeightedRRConfig {
    fn default() -> Self {
        Self {
            cpu_weight: default_cpu_weight(),
            mem_weight: default_mem_weight(),
            queue_weight: default_queue_weight(),
            failure_weight: default_failure_weight(),
            failure_cooloff_ms: default_failure_cooloff_ms(),
        }
    }
}

/// Arrow Flight server configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlightNodeConfig {
    #[serde(default = "default_flight_bind")]
    pub bind_addr: String,
    
    #[serde(default = "default_max_in_flight_batches")]
    pub max_in_flight_batches: usize,
    
    #[serde(default = "default_max_batch_bytes")]
    pub max_batch_bytes: usize,
    
    #[serde(default = "default_backpressure_threshold")]
    pub backpressure_threshold: f64,
}

impl Default for FlightNodeConfig {
    fn default() -> Self {
        Self {
            bind_addr: default_flight_bind(),
            max_in_flight_batches: default_max_in_flight_batches(),
            max_batch_bytes: default_max_batch_bytes(),
            backpressure_threshold: default_backpressure_threshold(),
        }
    }
}

/// LSM-tree configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LSMNodeConfig {
    #[serde(default = "default_memtable_max_bytes")]
    pub memtable_max_bytes: usize,
    
    #[serde(default = "default_memtable_max_ms")]
    pub memtable_max_ms: u64,
    
    #[serde(default = "default_row_group_target_mb")]
    pub row_group_target_mb: u32,
    
    #[serde(default = "default_file_target_mb")]
    pub file_target_mb: u32,
    
    #[serde(default = "default_sort_key")]
    pub sort_key: String,
    
    #[serde(default = "default_ram_retention_ms")]
    pub ram_retention_ms: u64,
    
    #[serde(default = "default_disk_retention_ms")]
    pub disk_retention_ms: u64,
}

impl Default for LSMNodeConfig {
    fn default() -> Self {
        Self {
            memtable_max_bytes: default_memtable_max_bytes(),
            memtable_max_ms: default_memtable_max_ms(),
            row_group_target_mb: default_row_group_target_mb(),
            file_target_mb: default_file_target_mb(),
            sort_key: default_sort_key(),
            ram_retention_ms: default_ram_retention_ms(),
            disk_retention_ms: default_disk_retention_ms(),
        }
    }
}

impl Validatable for LSMNodeConfig {
    fn validate(&self) -> Vec<ValidationResult> {
        let mut results = Vec::new();
        
        if self.memtable_max_bytes == 0 {
            results.push(ValidationResult::error("memtable_max_bytes", self.memtable_max_bytes as i64, "must be > 0"));
        }
        
        if self.row_group_target_mb == 0 {
            results.push(ValidationResult::error("row_group_target_mb", self.row_group_target_mb as i64, "must be > 0"));
        }
        
        if self.file_target_mb < self.row_group_target_mb {
            results.push(ValidationResult::Warning {
                field: "file_target_mb".to_string(),
                value: self.file_target_mb as i64,
                recommended: self.row_group_target_mb as i64,
                message: "should be >= row_group_target_mb".to_string(),
            });
        }
        
        results
    }
    
    fn apply_safe_defaults(&mut self) {
        self.memtable_max_bytes = 128_000_000; // 128MB
        self.memtable_max_ms = 60_000; // 1 minute
        self.row_group_target_mb = 64; // Conservative row group size
        self.file_target_mb = 256; // Conservative file size
    }
}

/// Parquet writer configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParquetNodeConfig {
    #[serde(default = "default_compression")]
    pub compression: String,
    
    #[serde(default = "default_zstd_level")]
    pub zstd_level: i32,
    
    #[serde(default = "default_enable_page_index")]
    pub enable_page_index: bool,
    
    #[serde(default = "default_enable_bloom")]
    pub enable_bloom: bool,
    
    #[serde(default)]
    pub bloom_columns: Vec<String>,
    
    #[serde(default = "default_enable_statistics")]
    pub enable_statistics: bool,
    
    #[serde(default = "default_dictionary_encoding")]
    pub enable_dictionary: bool,
    
    #[serde(default = "default_page_size_kb")]
    pub page_size_kb: u32,
}

impl Default for ParquetNodeConfig {
    fn default() -> Self {
        Self {
            compression: default_compression(),
            zstd_level: default_zstd_level(),
            enable_page_index: default_enable_page_index(),
            enable_bloom: default_enable_bloom(),
            bloom_columns: vec!["trace_id".to_string(), "span_id".to_string()],
            enable_statistics: default_enable_statistics(),
            enable_dictionary: default_dictionary_encoding(),
            page_size_kb: default_page_size_kb(),
        }
    }
}

impl Validatable for ParquetNodeConfig {
    fn validate(&self) -> Vec<ValidationResult> {
        let mut results = Vec::new();
        
        match self.compression.as_str() {
            "none" | "snappy" | "gzip" | "lz4" | "brotli" | "zstd" | "lz4_raw" => {},
            _ => results.push(ValidationResult::error(
                "compression",
                0,
                &format!("Invalid compression codec: {}", self.compression)
            )),
        }
        
        if self.compression == "zstd" && (self.zstd_level < 1 || self.zstd_level > 22) {
            results.push(ValidationResult::error(
                "zstd_level",
                self.zstd_level as i64,
                "must be between 1 and 22"
            ));
        }
        
        results
    }
    
    fn apply_safe_defaults(&mut self) {
        self.compression = "snappy".to_string(); // Fast compression
        self.enable_bloom = false; // Disable bloom filters in safe mode
    }
}

/// OpenDAL configuration for cloud storage
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenDALNodeConfig {
    #[serde(default = "default_artifact_uri")]
    pub artifact_uri: String,
    
    #[serde(default)]
    pub region: Option<String>,
    
    #[serde(default = "default_multipart_threshold_mb")]
    pub multipart_threshold_mb: u32,
    
    #[serde(default = "default_max_concurrency")]
    pub max_concurrency: usize,
    
    #[serde(default = "default_upload_policy")]
    pub upload_policy: String,
}

impl Default for OpenDALNodeConfig {
    fn default() -> Self {
        Self {
            artifact_uri: default_artifact_uri(),
            region: Some("us-east-1".to_string()),
            multipart_threshold_mb: default_multipart_threshold_mb(),
            max_concurrency: default_max_concurrency(),
            upload_policy: default_upload_policy(),
        }
    }
}

/// Publishing configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublishNodeConfig {
    #[serde(default = "default_emit_event_bus")]
    pub emit_event_bus: bool,
    
    #[serde(default = "default_publish_batch_size")]
    pub publish_batch_size: usize,
    
    #[serde(default = "default_dual_phase_enabled")]
    pub dual_phase_enabled: bool,
}

impl Default for PublishNodeConfig {
    fn default() -> Self {
        Self {
            emit_event_bus: default_emit_event_bus(),
            publish_batch_size: default_publish_batch_size(),
            dual_phase_enabled: default_dual_phase_enabled(),
        }
    }
}

/// Gossip protocol configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GossipNodeConfig {
    #[serde(default = "default_gossip_interval_ms")]
    pub interval_ms: u64,
    
    #[serde(default = "default_gossip_fanout")]
    pub fanout: usize,
    
    #[serde(default = "default_gossip_max_packet_size")]
    pub max_packet_size: usize,
    
    #[serde(default = "default_gossip_suspected_timeout_ms")]
    pub suspected_timeout_ms: u64,
}

impl Default for GossipNodeConfig {
    fn default() -> Self {
        Self {
            interval_ms: default_gossip_interval_ms(),
            fanout: default_gossip_fanout(),
            max_packet_size: default_gossip_max_packet_size(),
            suspected_timeout_ms: default_gossip_suspected_timeout_ms(),
        }
    }
}

/// WAL configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WALNodeConfig {
    #[serde(default = "default_wal_max_file_size_mb")]
    pub max_file_size_mb: u32,
    
    #[serde(default = "default_wal_rotation_check_interval_ms")]
    pub rotation_check_interval_ms: u64,
    
    #[serde(default = "default_wal_keep_files")]
    pub keep_files: usize,
    
    #[serde(default = "default_wal_fsync_on_write")]
    pub fsync_on_write: bool,
}

impl Default for WALNodeConfig {
    fn default() -> Self {
        Self {
            max_file_size_mb: default_wal_max_file_size_mb(),
            rotation_check_interval_ms: default_wal_rotation_check_interval_ms(),
            keep_files: default_wal_keep_files(),
            fsync_on_write: default_wal_fsync_on_write(),
        }
    }
}

/// Compaction configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactionNodeConfig {
    #[serde(default = "default_compaction_enabled")]
    pub enabled: bool,
    
    #[serde(default = "default_compaction_trigger_files")]
    pub trigger_files: usize,
    
    #[serde(default = "default_compaction_target_level")]
    pub target_level: u8,
    
    #[serde(default = "default_compaction_max_concurrent")]
    pub max_concurrent: usize,
    
    #[serde(default = "default_compaction_grace_period_ms")]
    pub grace_period_ms: u64,
}

impl Default for CompactionNodeConfig {
    fn default() -> Self {
        Self {
            enabled: default_compaction_enabled(),
            trigger_files: default_compaction_trigger_files(),
            target_level: default_compaction_target_level(),
            max_concurrent: default_compaction_max_concurrent(),
            grace_period_ms: default_compaction_grace_period_ms(),
        }
    }
}

impl Validatable for CompactionNodeConfig {
    fn validate(&self) -> Vec<ValidationResult> {
        let mut results = Vec::new();
        
        if self.trigger_files < 2 {
            results.push(ValidationResult::error(
                "trigger_files",
                self.trigger_files as i64,
                "must be at least 2"
            ));
        }
        
        if self.target_level > 7 {
            results.push(ValidationResult::Warning {
                field: "target_level".to_string(),
                value: self.target_level as i64,
                recommended: 7,
                message: "values > 7 may cause deep LSM trees".to_string(),
            });
        }
        
        results
    }
    
    fn apply_safe_defaults(&mut self) {
        self.enabled = false; // Disable compaction in safe mode
        self.max_concurrent = 1;
    }
}

// Default value functions
fn default_node_id() -> String { 
    format!("storage-{}", ulid::Ulid::new().to_string())
}
fn default_storage_mode() -> String { "local".to_string() }
fn default_work_dir() -> String { "/var/lib/openwit/storage".to_string() }
fn default_postgres_dsn() -> String { "postgres://localhost/openwit".to_string() }
fn default_pool_size() -> u32 { 10 }
fn default_gossip_bind() -> String { "0.0.0.0:7878".to_string() }
fn default_health_push_interval_ms() -> u64 { 3000 }
fn default_control_request_timeout_ms() -> u64 { 1500 }
fn default_weighted_rr_weights() -> WeightedRRConfig { WeightedRRConfig::default() }
fn default_cpu_weight() -> f64 { 0.3 }
fn default_mem_weight() -> f64 { 0.3 }
fn default_queue_weight() -> f64 { 0.3 }
fn default_failure_weight() -> f64 { 0.1 }
fn default_failure_cooloff_ms() -> u64 { 30000 }
fn default_flight_bind() -> String { "0.0.0.0:9401".to_string() }
fn default_max_in_flight_batches() -> usize { 4096 }
fn default_max_batch_bytes() -> usize { 32_000_000 }
fn default_backpressure_threshold() -> f64 { 0.8 }
fn default_memtable_max_bytes() -> usize { 256_000_000 }
fn default_memtable_max_ms() -> u64 { 30000 }
fn default_row_group_target_mb() -> u32 { 128 }
fn default_file_target_mb() -> u32 { 512 }
fn default_sort_key() -> String { "ts,service.name,severity,trace_id".to_string() }
fn default_ram_retention_ms() -> u64 { 3600000 } // 1 hour
fn default_disk_retention_ms() -> u64 { 86400000 } // 24 hours
fn default_compression() -> String { "zstd".to_string() }
fn default_zstd_level() -> i32 { 4 }
fn default_enable_page_index() -> bool { true }
fn default_enable_bloom() -> bool { true }
fn default_enable_statistics() -> bool { true }
fn default_dictionary_encoding() -> bool { true }
fn default_page_size_kb() -> u32 { 1024 }
fn default_artifact_uri() -> String { "fs:///var/lib/openwit/storage/artifacts".to_string() }
fn default_multipart_threshold_mb() -> u32 { 64 }
fn default_max_concurrency() -> usize { 8 }
fn default_upload_policy() -> String { "immediate".to_string() }
fn default_emit_event_bus() -> bool { true }
fn default_publish_batch_size() -> usize { 100 }
fn default_dual_phase_enabled() -> bool { true }
fn default_gossip_interval_ms() -> u64 { 200 }
fn default_gossip_fanout() -> usize { 3 }
fn default_gossip_max_packet_size() -> usize { 1400 }
fn default_gossip_suspected_timeout_ms() -> u64 { 5000 }
fn default_wal_max_file_size_mb() -> u32 { 100 }
fn default_wal_rotation_check_interval_ms() -> u64 { 1000 }
fn default_wal_keep_files() -> usize { 10 }
fn default_wal_fsync_on_write() -> bool { true }
fn default_compaction_enabled() -> bool { true }
fn default_compaction_trigger_files() -> usize { 5 }
fn default_compaction_target_level() -> u8 { 1 }
fn default_compaction_max_concurrent() -> usize { 2 }
fn default_compaction_grace_period_ms() -> u64 { 300000 } // 5 minutes
