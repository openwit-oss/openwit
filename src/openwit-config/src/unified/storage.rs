use serde::{Deserialize, Serialize};
use crate::unified::validation::{Validatable, ValidationResult, limits};
use tracing::info;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    #[serde(default = "default_storage_backend")]
    pub backend: String,
    
    #[serde(default)]
    pub azure: AzureStorageConfig,
    
    #[serde(default)]
    pub s3: S3StorageConfig,
    
    #[serde(default)]
    pub gcs: GcsStorageConfig,
    
    #[serde(default)]
    pub local: LocalStorageConfig,
    
    #[serde(default)]
    pub upload: UploadConfig,
    
    #[serde(default)]
    pub parquet: ParquetConfig,
    
    #[serde(default)]
    pub compaction: CompactionConfig,
    
    #[serde(default)]
    pub lifecycle: LifecycleConfig,
    
    #[serde(default)]
    pub file_rotation: FileRotationConfig,
    
    #[serde(default)]
    pub local_retention: LocalRetentionConfig,
    
    
    #[serde(default = "default_disk_time_threshold_seconds")]
    pub disk_time_threshold_seconds: u64,
    
    #[serde(default = "default_parquet_split_size_mb")]
    pub parquet_split_size_mb: u32,
    
    #[serde(default = "default_data_dir")]
    pub data_dir: String,
    
    #[serde(default)]
    pub enable_csv_backup: bool,
    
    #[serde(default)]
    pub tier_management: TierManagementConfig,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            backend: default_storage_backend(),
            azure: AzureStorageConfig::default(),
            s3: S3StorageConfig::default(),
            gcs: GcsStorageConfig::default(),
            local: LocalStorageConfig::default(),
            upload: UploadConfig::default(),
            parquet: ParquetConfig::default(),
            compaction: CompactionConfig::default(),
            lifecycle: LifecycleConfig::default(),
            file_rotation: FileRotationConfig::default(),
            local_retention: LocalRetentionConfig::default(),
            disk_time_threshold_seconds: default_disk_time_threshold_seconds(),
            parquet_split_size_mb: default_parquet_split_size_mb(),
            data_dir: default_data_dir(),
            enable_csv_backup: false,
            tier_management: TierManagementConfig::default(),
        }
    }
}

impl Validatable for StorageConfig {
    fn validate(&self) -> Vec<ValidationResult> {
        let mut results = Vec::new();
        results.extend(self.parquet.validate());
        results.extend(self.compaction.validate());
        results
    }
    
    fn apply_safe_defaults(&mut self) {
        info!("Applying safe defaults to Storage configuration");
        self.parquet.apply_safe_defaults();
        self.compaction.apply_safe_defaults();
        self.upload.apply_safe_defaults();
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AzureStorageConfig {
    #[serde(default)]
    pub enabled: bool,
    
    #[serde(default = "default_azure_account_name")]
    pub account_name: String,
    
    #[serde(default = "default_azure_container_name")]
    pub container_name: String,
    
    #[serde(default = "default_auth_method")]
    pub auth_method: String,
    
    #[serde(default = "default_azure_endpoint")]
    pub endpoint: String,
    
    #[serde(default)]
    pub sas_token: String,
    
    #[serde(default)]
    pub connection_string: String,
    
    #[serde(default)]
    pub access_key: String,
    
    #[serde(default)]
    pub retry_policy: RetryPolicy,
    
    #[serde(default = "default_max_connections")]
    pub max_connections: u32,
}

impl Default for AzureStorageConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            account_name: default_azure_account_name(),
            container_name: default_azure_container_name(),
            auth_method: default_auth_method(),
            endpoint: default_azure_endpoint(),
            sas_token: String::new(),
            connection_string: String::new(),
            access_key: String::new(),
            retry_policy: RetryPolicy::default(),
            max_connections: default_max_connections(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3StorageConfig {
    #[serde(default)]
    pub enabled: bool,
    
    #[serde(default = "default_s3_bucket")]
    pub bucket: String,
    
    #[serde(default = "default_s3_region")]
    pub region: String,
    
    #[serde(default = "default_s3_endpoint")]
    pub endpoint: String,
    
    #[serde(default)]
    pub access_key_id: String,
    
    #[serde(default)]
    pub secret_access_key: String,
    
    #[serde(default)]
    pub use_path_style: bool,
    
    #[serde(default)]
    pub retry_policy: RetryPolicy,
    
    #[serde(default = "default_multipart_threshold_mb")]
    pub multipart_threshold_mb: u32,
    
    #[serde(default = "default_multipart_chunk_size_mb")]
    pub multipart_chunk_size_mb: u32,
}

impl Default for S3StorageConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            bucket: default_s3_bucket(),
            region: default_s3_region(),
            endpoint: default_s3_endpoint(),
            access_key_id: String::new(),
            secret_access_key: String::new(),
            use_path_style: false,
            retry_policy: RetryPolicy::default(),
            multipart_threshold_mb: default_multipart_threshold_mb(),
            multipart_chunk_size_mb: default_multipart_chunk_size_mb(),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct GcsStorageConfig {
    #[serde(default)]
    pub enabled: bool,
    
    #[serde(default = "default_gcs_bucket")]
    pub bucket: String,
    
    #[serde(default = "default_gcs_project_id")]
    pub project_id: String,
    
    #[serde(default)]
    pub service_account_key: String,
    
    #[serde(default)]
    pub retry_policy: RetryPolicy,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalStorageConfig {
    #[serde(default)]
    pub enabled: bool,
    
    #[serde(default = "default_local_path")]
    pub path: String,
    
    #[serde(default = "default_max_disk_usage_percent")]
    pub max_disk_usage_percent: u32,
    
    #[serde(default = "default_cleanup_interval_seconds")]
    pub cleanup_interval_seconds: u64,
}

impl Default for LocalStorageConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            path: default_local_path(),
            max_disk_usage_percent: default_max_disk_usage_percent(),
            cleanup_interval_seconds: default_cleanup_interval_seconds(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadConfig {
    #[serde(default = "default_max_concurrent_uploads")]
    pub max_concurrent_uploads: u32,
    
    #[serde(default = "default_upload_batch_size")]
    pub batch_size: u32,
    
    #[serde(default = "default_upload_timeout_seconds")]
    pub timeout_seconds: u64,
    
    #[serde(default = "default_upload_buffer_size_mb")]
    pub buffer_size_mb: u32,
    
    #[serde(default = "default_true")]
    pub compress_before_upload: bool,
}

impl Default for UploadConfig {
    fn default() -> Self {
        Self {
            max_concurrent_uploads: default_max_concurrent_uploads(),
            batch_size: default_upload_batch_size(),
            timeout_seconds: default_upload_timeout_seconds(),
            buffer_size_mb: default_upload_buffer_size_mb(),
            compress_before_upload: default_true(),
        }
    }
}

impl Validatable for UploadConfig {
    fn validate(&self) -> Vec<ValidationResult> {
        Vec::new() // Add specific validations as needed
    }
    
    fn apply_safe_defaults(&mut self) {
        self.max_concurrent_uploads = 2; // Conservative concurrent uploads
        self.batch_size = 10; // Smaller batch size
        self.buffer_size_mb = 64; // Smaller buffer
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParquetConfig {
    #[serde(default = "default_row_group_size")]
    pub row_group_size: u32,
    
    #[serde(default = "default_target_file_size_mb")]
    pub target_file_size_mb: u32,
    
    #[serde(default = "default_compression_codec")]
    pub compression_codec: String,
    
    #[serde(default = "default_compression_level")]
    pub compression_level: u32,
    
    #[serde(default = "default_true")]
    pub enable_statistics: bool,
    
    #[serde(default = "default_true")]
    pub enable_bloom_filter: bool,
    
    #[serde(default = "default_data_page_size_kb")]
    pub data_page_size_kb: u32,
}

impl Default for ParquetConfig {
    fn default() -> Self {
        Self {
            row_group_size: default_row_group_size(),
            target_file_size_mb: default_target_file_size_mb(),
            compression_codec: default_compression_codec(),
            compression_level: default_compression_level(),
            enable_statistics: default_true(),
            enable_bloom_filter: default_true(),
            data_page_size_kb: default_data_page_size_kb(),
        }
    }
}

impl Validatable for ParquetConfig {
    fn validate(&self) -> Vec<ValidationResult> {
        let mut results = Vec::new();
        results.push(limits::parquet_target_file_size_mb().validate(self.target_file_size_mb as i64));
        results
    }
    
    fn apply_safe_defaults(&mut self) {
        self.target_file_size_mb = 100; // Conservative file size
        self.row_group_size = 10000; // Smaller row groups
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactionConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    
    #[serde(default = "default_compaction_interval_seconds")]
    pub interval_seconds: u64,
    
    #[serde(default = "default_min_files_to_compact")]
    pub min_files_to_compact: u32,
    
    #[serde(default = "default_max_files_per_compaction")]
    pub max_files_per_compaction: u32,
    
    #[serde(default = "default_target_file_size_mb")]
    pub target_file_size_mb: u32,
    
    #[serde(default = "default_max_concurrent_compactions")]
    pub max_concurrent_compactions: u32,
    
    #[serde(default)]
    pub small_file_threshold_mb: u32,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            enabled: default_true(),
            interval_seconds: default_compaction_interval_seconds(),
            min_files_to_compact: default_min_files_to_compact(),
            max_files_per_compaction: default_max_files_per_compaction(),
            target_file_size_mb: default_target_file_size_mb(),
            max_concurrent_compactions: default_max_concurrent_compactions(),
            small_file_threshold_mb: 50,
        }
    }
}

impl Validatable for CompactionConfig {
    fn validate(&self) -> Vec<ValidationResult> {
        let mut results = Vec::new();
        results.push(limits::compaction_max_files().validate(self.max_files_per_compaction as i64));
        results
    }
    
    fn apply_safe_defaults(&mut self) {
        self.max_files_per_compaction = 10; // Conservative compaction
        self.max_concurrent_compactions = 1; // Single compaction at a time
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LifecycleConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    
    #[serde(default = "default_retention_days")]
    pub retention_days: u32,
    
    #[serde(default = "default_archive_after_days")]
    pub archive_after_days: u32,
    
    #[serde(default = "default_delete_after_days")]
    pub delete_after_days: u32,
    
    #[serde(default = "default_lifecycle_check_interval_hours")]
    pub check_interval_hours: u32,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RetryPolicy {
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,
    
    #[serde(default = "default_initial_delay_ms")]
    pub initial_delay_ms: u64,
    
    #[serde(default = "default_max_delay_ms")]
    pub max_delay_ms: u64,
    
    #[serde(default = "default_retry_multiplier")]
    pub multiplier: f64,
}

// Default functions
fn default_true() -> bool { true }
fn default_storage_backend() -> String { "azure".to_string() }
fn default_azure_account_name() -> String { "".to_string() }
fn default_azure_container_name() -> String { "openwit".to_string() }
fn default_auth_method() -> String { "connection_string".to_string() }
fn default_azure_endpoint() -> String { "".to_string() }
fn default_max_connections() -> u32 { 20 }
fn default_s3_bucket() -> String { "openwit".to_string() }
fn default_s3_region() -> String { "us-east-1".to_string() }
fn default_s3_endpoint() -> String { "https://s3.amazonaws.com".to_string() }
fn default_multipart_threshold_mb() -> u32 { 64 }
fn default_multipart_chunk_size_mb() -> u32 { 16 }
fn default_gcs_bucket() -> String { "openwit".to_string() }
fn default_gcs_project_id() -> String { "".to_string() }
fn default_local_path() -> String { "./data/storage".to_string() }
fn default_max_disk_usage_percent() -> u32 { 80 }
fn default_cleanup_interval_seconds() -> u64 { 3600 }
fn default_max_concurrent_uploads() -> u32 { 5 }
fn default_upload_batch_size() -> u32 { 100 }
fn default_upload_timeout_seconds() -> u64 { 300 }
fn default_upload_buffer_size_mb() -> u32 { 128 }
fn default_row_group_size() -> u32 { 100000 }
fn default_target_file_size_mb() -> u32 { 200 }
fn default_compression_codec() -> String { "snappy".to_string() }
fn default_compression_level() -> u32 { 6 }
fn default_data_page_size_kb() -> u32 { 1024 }
fn default_compaction_interval_seconds() -> u64 { 3600 }
fn default_min_files_to_compact() -> u32 { 5 }
fn default_max_files_per_compaction() -> u32 { 20 }
fn default_max_concurrent_compactions() -> u32 { 2 }
fn default_retention_days() -> u32 { 30 }
fn default_archive_after_days() -> u32 { 7 }
fn default_delete_after_days() -> u32 { 90 }
fn default_lifecycle_check_interval_hours() -> u32 { 24 }
fn default_max_retries() -> u32 { 3 }
fn default_initial_delay_ms() -> u64 { 100 }
fn default_max_delay_ms() -> u64 { 10000 }
fn default_retry_multiplier() -> f64 { 2.0 }
fn default_disk_time_threshold_seconds() -> u64 { 604800 } // 7 days
fn default_parquet_split_size_mb() -> u32 { 100 }
fn default_data_dir() -> String { "./data/storage".to_string() }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TierManagementConfig {
    // Promotion settings
    #[serde(default = "default_promotion_min_access_count")]
    pub promotion_min_access_count: u64,
    
    #[serde(default = "default_promotion_access_window_hours")]
    pub promotion_access_window_hours: u32,
    
    #[serde(default = "default_promotion_heat_threshold")]
    pub promotion_heat_threshold: f64,
    
    // Demotion settings
    #[serde(default = "default_hot_max_age_days")]
    pub hot_max_age_days: u32,
    
    #[serde(default = "default_hot_max_size_gb")]
    pub hot_max_size_gb: usize,
    
    // LRU and prefetching
    #[serde(default = "default_true")]
    pub enable_lru_eviction: bool,
    
    #[serde(default = "default_true")]
    pub enable_prefetching: bool,
    
    #[serde(default = "default_prefetch_related_count")]
    pub prefetch_related_count: usize,
}

impl Default for TierManagementConfig {
    fn default() -> Self {
        Self {
            promotion_min_access_count: default_promotion_min_access_count(),
            promotion_access_window_hours: default_promotion_access_window_hours(),
            promotion_heat_threshold: default_promotion_heat_threshold(),
            hot_max_age_days: default_hot_max_age_days(),
            hot_max_size_gb: default_hot_max_size_gb(),
            enable_lru_eviction: default_true(),
            enable_prefetching: default_true(),
            prefetch_related_count: default_prefetch_related_count(),
        }
    }
}

// Tier management defaults
fn default_promotion_min_access_count() -> u64 { 3 }
fn default_promotion_access_window_hours() -> u32 { 1 }
fn default_promotion_heat_threshold() -> f64 { 0.5 }
fn default_hot_max_age_days() -> u32 { 7 }
fn default_hot_max_size_gb() -> usize { 100 }
fn default_prefetch_related_count() -> usize { 5 }

// File rotation configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileRotationConfig {
    #[serde(default = "default_file_duration_minutes")]
    pub file_duration_minutes: u32,  // 30, 60, 120, etc.
    
    #[serde(default = "default_file_size_mb")]
    pub file_size_mb: u32,  // Size threshold for rotation (e.g., 100MB)
    
    #[serde(default = "default_true")]
    pub enable_active_stable_files: bool,
    
    #[serde(default = "default_upload_delay_minutes")]
    pub upload_delay_minutes: u32,  // Delay after file rotation before upload
}

impl Default for FileRotationConfig {
    fn default() -> Self {
        Self {
            file_duration_minutes: default_file_duration_minutes(),
            file_size_mb: default_file_size_mb(),
            enable_active_stable_files: default_true(),
            upload_delay_minutes: default_upload_delay_minutes(),
        }
    }
}

// Local retention configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalRetentionConfig {
    #[serde(default = "default_local_retention_days")]
    pub retention_days: u32,  // 7, 10, 30, etc.
    
    #[serde(default = "default_cleanup_interval_hours")]
    pub cleanup_interval_hours: u32,
    
    #[serde(default = "default_true")]
    pub delete_after_upload: bool,  // Whether to delete immediately after successful upload
}

impl Default for LocalRetentionConfig {
    fn default() -> Self {
        Self {
            retention_days: default_local_retention_days(),
            cleanup_interval_hours: default_cleanup_interval_hours(),
            delete_after_upload: false,
        }
    }
}

fn default_file_duration_minutes() -> u32 { 60 }  // 1 hour default
fn default_file_size_mb() -> u32 { 100 }  // 100MB default
fn default_upload_delay_minutes() -> u32 { 5 }   // 5 minutes delay before upload
fn default_local_retention_days() -> u32 { 7 }    // 7 days default
fn default_cleanup_interval_hours() -> u32 { 1 }  // Check every hour