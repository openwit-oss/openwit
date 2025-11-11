use serde::{Deserialize, Serialize};
use crate::unified::validation::{Validatable, ValidationResult};
use tracing::info;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JanitorConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    
    #[serde(default)]
    pub intervals: JanitorIntervals,
    
    #[serde(default)]
    pub cleanup: CleanupConfig,
    
    #[serde(default)]
    pub compaction: JanitorCompactionConfig,
    
    #[serde(default)]
    pub metastore_cache: MetastoreCacheConfig,
}

impl Default for JanitorConfig {
    fn default() -> Self {
        Self {
            enabled: default_true(),
            intervals: JanitorIntervals::default(),
            cleanup: CleanupConfig::default(),
            compaction: JanitorCompactionConfig::default(),
            metastore_cache: MetastoreCacheConfig::default(),
        }
    }
}

impl Validatable for JanitorConfig {
    fn validate(&self) -> Vec<ValidationResult> {
        Vec::new() // Add specific validations as needed
    }
    
    fn apply_safe_defaults(&mut self) {
        info!("Applying safe defaults to Janitor configuration");
        self.intervals.apply_safe_defaults();
        self.cleanup.apply_safe_defaults();
        self.compaction.apply_safe_defaults();
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JanitorIntervals {
    #[serde(default = "default_run_interval_seconds")]
    pub run_interval_seconds: u32,
    
    #[serde(default = "default_compaction_interval_seconds")]
    pub compaction_interval_seconds: u32,
    
    #[serde(default = "default_retention_check_interval_seconds")]
    pub retention_check_interval_seconds: u32,
    
    #[serde(default = "default_wal_cleanup_interval_seconds")]
    pub wal_cleanup_interval_seconds: u32,
    
    #[serde(default = "default_cleanup_interval_seconds")]
    pub cleanup_interval_seconds: u32,
}

impl Default for JanitorIntervals {
    fn default() -> Self {
        Self {
            run_interval_seconds: default_run_interval_seconds(),
            compaction_interval_seconds: default_compaction_interval_seconds(),
            retention_check_interval_seconds: default_retention_check_interval_seconds(),
            wal_cleanup_interval_seconds: default_wal_cleanup_interval_seconds(),
            cleanup_interval_seconds: default_cleanup_interval_seconds(),
        }
    }
}

impl JanitorIntervals {
    fn apply_safe_defaults(&mut self) {
        self.run_interval_seconds = 600; // 10 minutes
        self.compaction_interval_seconds = 7200; // 2 hours
        self.retention_check_interval_seconds = 86400; // 24 hours
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CleanupConfig {
    #[serde(default = "default_max_segments_per_partition")]
    pub max_segments_per_partition: u32,
    
    #[serde(default = "default_retention_period_days")]
    pub retention_period_days: u32,
    
    #[serde(default = "default_wal_retention_hours")]
    pub wal_retention_hours: u32,
    
    #[serde(default = "default_max_wal_age_hours")]
    pub max_wal_age_hours: u32,
    
    #[serde(default = "default_orphaned_file_check_hours")]
    pub orphaned_file_check_hours: u32,
    
    #[serde(default = "default_temp_file_retention_hours")]
    pub temp_file_retention_hours: u32,
}

impl Default for CleanupConfig {
    fn default() -> Self {
        Self {
            max_segments_per_partition: default_max_segments_per_partition(),
            retention_period_days: default_retention_period_days(),
            wal_retention_hours: default_wal_retention_hours(),
            max_wal_age_hours: default_max_wal_age_hours(),
            orphaned_file_check_hours: default_orphaned_file_check_hours(),
            temp_file_retention_hours: default_temp_file_retention_hours(),
        }
    }
}

impl CleanupConfig {
    fn apply_safe_defaults(&mut self) {
        self.retention_period_days = 7; // Conservative retention
        self.wal_retention_hours = 48; // Keep WAL longer for safety
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JanitorCompactionConfig {
    #[serde(default = "default_compaction_threshold_mb")]
    pub compaction_threshold_mb: u32,
    
    #[serde(default = "default_min_files_to_compact")]
    pub min_files_to_compact: u32,
    
    #[serde(default = "default_max_files_per_compaction")]
    pub max_files_per_compaction: u32,
    
    #[serde(default = "default_target_file_size_mb")]
    pub target_file_size_mb: u32,
}

impl Default for JanitorCompactionConfig {
    fn default() -> Self {
        Self {
            compaction_threshold_mb: default_compaction_threshold_mb(),
            min_files_to_compact: default_min_files_to_compact(),
            max_files_per_compaction: default_max_files_per_compaction(),
            target_file_size_mb: default_target_file_size_mb(),
        }
    }
}

impl JanitorCompactionConfig {
    fn apply_safe_defaults(&mut self) {
        self.min_files_to_compact = 10; // More files before compaction
        self.max_files_per_compaction = 10; // Smaller compaction batches
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetastoreCacheConfig {
    #[serde(default = "default_metastore_cache_size_mb")]
    pub size_mb: u32,
}

impl Default for MetastoreCacheConfig {
    fn default() -> Self {
        Self {
            size_mb: default_metastore_cache_size_mb(),
        }
    }
}

// Default functions
fn default_true() -> bool { true }
fn default_run_interval_seconds() -> u32 { 300 }
fn default_compaction_interval_seconds() -> u32 { 3600 }
fn default_retention_check_interval_seconds() -> u32 { 86400 }
fn default_wal_cleanup_interval_seconds() -> u32 { 1800 }
fn default_cleanup_interval_seconds() -> u32 { 3600 }
fn default_max_segments_per_partition() -> u32 { 10 }
fn default_retention_period_days() -> u32 { 30 }
fn default_wal_retention_hours() -> u32 { 24 }
fn default_max_wal_age_hours() -> u32 { 24 }
fn default_orphaned_file_check_hours() -> u32 { 12 }
fn default_temp_file_retention_hours() -> u32 { 6 }
fn default_compaction_threshold_mb() -> u32 { 100 }
fn default_min_files_to_compact() -> u32 { 5 }
fn default_max_files_per_compaction() -> u32 { 20 }
fn default_target_file_size_mb() -> u32 { 1024 }
fn default_metastore_cache_size_mb() -> u32 { 64 }
