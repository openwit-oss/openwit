//! OpenWit Metastore - Central coordination point for storage, indexing, and control plane
//! 
//! This crate provides the single source of truth for all metadata in the OpenWit system.
//! It tracks partitions, segments, and their lifecycle states across storage and indexing.

pub mod postgres_metastore;
pub mod sled_metastore;
pub mod config;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use anyhow::Result;
use chrono::{DateTime, Utc};

/// Status of a segment in its lifecycle
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SegmentStatus {
    /// Segment is being written/created
    Pending,
    /// Segment has been successfully stored/indexed
    Stored,
    /// Segment operation failed
    Failed,
}

impl Default for SegmentStatus {
    fn default() -> Self {
        SegmentStatus::Pending
    }
}

/// Metadata about a time partition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionMetadata {
    pub partition_id: String,
    pub time_range: TimeRange,
    pub segment_count: usize,
    pub row_count: u64,
    pub compressed_bytes: u64,
    pub uncompressed_bytes: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Metadata about a segment within a partition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentMetadata {
    pub segment_id: String,
    pub partition_id: String,
    pub file_path: String,        // Always set after storage
    pub index_path: Option<String>, // Set after indexing
    pub row_count: u64,
    pub compressed_bytes: u64,
    pub uncompressed_bytes: u64,
    pub min_timestamp: DateTime<Utc>,
    pub max_timestamp: DateTime<Utc>,
    pub storage_status: SegmentStatus, // NEW: track storage state
    pub index_status: SegmentStatus,   // NEW: track index state
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub schema_version: u32,
    // Cold storage fields
    pub is_cold: bool,
    pub is_archived: bool,
    pub storage_class: Option<String>,
    pub transitioned_at: Option<DateTime<Utc>>,
    pub archived_at: Option<DateTime<Utc>>,
    pub restore_status: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeRange {
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
}

/// Table metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableMetadata {
    pub table_name: String,
    pub schema: Vec<FieldSchema>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Field schema
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FieldSchema {
    pub name: String,
    pub field_type: FieldType,
    pub nullable: bool,
}

/// Field types
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum FieldType {
    String,
    Integer,
    Float,
    Boolean,
    Timestamp,
    Json,
}

/// Lifecycle statistics for control plane monitoring
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LifecycleStats {
    pub total_segments: u64,
    pub pending_storage: u64,
    pub stored_segments: u64,
    pub failed_storage: u64,
    pub pending_index: u64,
    pub indexed_segments: u64,
    pub failed_index: u64,
    pub orphaned_indexes: u64, // indexed but not stored
    pub missing_indexes: u64,  // stored but not indexed
}

/// Trait for metastore implementations
#[async_trait]
pub trait MetaStore: Send + Sync {
    /// Create or update table schema
    async fn create_table(&self, table: TableMetadata) -> Result<()>;
    
    /// Get table metadata
    async fn get_table(&self, table_name: &str) -> Result<Option<TableMetadata>>;
    
    /// Register a new partition
    async fn create_partition(&self, metadata: PartitionMetadata) -> Result<()>;
    
    /// Update partition metadata
    async fn update_partition(&self, metadata: PartitionMetadata) -> Result<()> {
        self.create_partition(metadata).await
    }
    
    /// Get partition metadata
    async fn get_partition(&self, partition_id: &str) -> Result<Option<PartitionMetadata>>;
    
    /// Register a new segment
    async fn create_segment(&self, metadata: SegmentMetadata) -> Result<()>;
    
    /// Update segment metadata (e.g., after indexing completes)
    async fn update_segment(&self, segment_id: &str, metadata: SegmentMetadata) -> Result<()>;
    
    /// Get segment metadata
    async fn get_segment(&self, segment_id: &str) -> Result<Option<SegmentMetadata>>;
    
    /// List segments in a partition
    async fn list_segments(&self, partition_id: &str) -> Result<Vec<SegmentMetadata>>;
    
    /// Delete partition and all its segments
    async fn delete_partition(&self, partition_id: &str) -> Result<()>;
    
    /// Delete a segment
    async fn delete_segment(&self, segment_id: &str) -> Result<()>;
    
    /// Get total storage statistics
    async fn get_storage_stats(&self) -> Result<StorageStats>;
    
    /// List partitions overlapping with time range
    async fn list_partitions(&self, time_range: TimeRange) -> Result<Vec<PartitionMetadata>>;
    
    /// Get last indexed WAL sequence
    async fn get_last_indexed_sequence(&self) -> Result<Option<u64>>;
    
    /// Set last indexed WAL sequence
    async fn set_last_indexed_sequence(&self, sequence: u64) -> Result<()>;
    
    // NEW: Control plane monitoring queries
    
    /// List segments by storage and index status
    async fn list_segments_by_status(
        &self, 
        storage_status: Option<SegmentStatus>, 
        index_status: Option<SegmentStatus>
    ) -> Result<Vec<SegmentMetadata>>;
    
    /// Get lifecycle statistics for monitoring
    async fn get_segment_lifecycle_stats(&self) -> Result<LifecycleStats>;
    
    /// Get segments ready for indexing (stored but not indexed)
    async fn get_segments_ready_for_indexing(&self) -> Result<Vec<SegmentMetadata>> {
        self.list_segments_by_status(Some(SegmentStatus::Stored), Some(SegmentStatus::Pending)).await
    }
    
    /// Mark segment storage as failed
    async fn mark_segment_storage_failed(&self, segment_id: &str, error: &str) -> Result<()>;
    
    /// Mark segment indexing as failed
    async fn mark_segment_index_failed(&self, segment_id: &str, error: &str) -> Result<()>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageStats {
    pub total_partitions: u64,
    pub total_segments: u64,
    pub total_rows: u64,
    pub total_compressed_bytes: u64,
    pub total_uncompressed_bytes: u64,
}

/// Configuration for metastore backends
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetastoreConfig {
    pub backend: String,
    pub connection_string: Option<String>,
    pub path: Option<String>,
    pub max_connections: Option<u32>,
    pub schema_name: Option<String>,
    pub cache_size_mb: Option<usize>,
}

/// Factory function to create metastore based on config
pub async fn create_metastore_from_config(
    config: &MetastoreConfig,
    local_dev: bool,
) -> Result<Arc<dyn MetaStore>> {
    match config.backend.as_str() {
        "postgres" | "postgresql" => {
            let postgres_config = postgres_metastore::PostgresMetastoreConfig {
                connection_string: config.connection_string.clone()
                    .ok_or_else(|| anyhow::anyhow!("PostgreSQL connection_string is required"))?,
                max_connections: config.max_connections.unwrap_or(10),
                schema_name: config.schema_name.clone().unwrap_or_else(|| "metastore".to_string()),
            };
            Ok(Arc::new(postgres_metastore::PostgresMetaStore::new(postgres_config).await?))
        }
        "sled" => {
            if !local_dev {
                anyhow::bail!("Sled metastore is only available in local development mode (--local flag)");
            }
            let sled_config = sled_metastore::SledMetastoreConfig {
                path: config.path.clone().unwrap_or_else(|| "./data/metastore".to_string()),
                cache_size_mb: config.cache_size_mb.unwrap_or(128),
            };
            Ok(Arc::new(sled_metastore::SledMetaStore::new(sled_config).await?))
        }
        _ => anyhow::bail!("Unknown metastore backend: {}", config.backend),
    }
}

/// Helper to create default metastore config
pub fn default_metastore_config(local_dev: bool) -> MetastoreConfig {
    if local_dev {
        MetastoreConfig {
            backend: "sled".to_string(),
            connection_string: None,
            path: Some("./data/metastore".to_string()),
            max_connections: None,
            schema_name: None,
            cache_size_mb: Some(128),
        }
    } else {
        MetastoreConfig {
            backend: "postgres".to_string(),
            connection_string: Some("postgresql://openwit:openwit@localhost/openwit".to_string()),
            path: None,
            max_connections: Some(10),
            schema_name: Some("metastore".to_string()),
            cache_size_mb: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_segment_status_default() {
        assert_eq!(SegmentStatus::default(), SegmentStatus::Pending);
    }
}