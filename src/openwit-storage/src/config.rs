// Stub module for compatibility
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    pub local_storage_path: String,
    pub parquet: ParquetConfig,
    pub object_store: Option<ObjectStoreConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParquetConfig {
    pub target_file_size_bytes: u64,
    pub row_group_size: usize,
    pub max_rows_per_file: usize,
    pub compression: CompressionType,
    pub enable_bloom_filter: bool,
    pub enable_statistics: bool,
    pub bloom_filter_fpp: f64,
    pub enable_dictionary: bool,
    pub page_size_bytes: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CompressionType {
    Snappy,
    Gzip,
    Lz4,
    Zstd,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectStoreConfig {
    // Stub for object store config
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            local_storage_path: "./data/storage".to_string(),
            parquet: ParquetConfig {
                target_file_size_bytes: 200 * 1024 * 1024, // 200MB
                row_group_size: 10000,
                max_rows_per_file: 100000,
                compression: CompressionType::Snappy,
                enable_bloom_filter: true,
                enable_statistics: true,
                bloom_filter_fpp: 0.01,
                enable_dictionary: true,
                page_size_bytes: 1024 * 1024,
            },
            object_store: None,
        }
    }
}