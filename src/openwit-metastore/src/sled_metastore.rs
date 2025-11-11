//! Sled-based metastore implementation (for local development/testing)

use async_trait::async_trait;
use anyhow::Result;
use chrono::Utc;
use tracing::{info, warn};

use crate::{
    MetaStore, PartitionMetadata, SegmentMetadata, SegmentStatus, TableMetadata,
    TimeRange, StorageStats, LifecycleStats,
};

/// Sled metastore configuration
#[derive(Debug, Clone)]
pub struct SledMetastoreConfig {
    pub path: String,
    pub cache_size_mb: usize,
}

/// Sled-based metastore implementation
pub struct SledMetaStore {
    db: sled::Db,
    partitions_tree: sled::Tree,
    segments_tree: sled::Tree,
    meta_tree: sled::Tree,
    tables_tree: sled::Tree,
    errors_tree: sled::Tree, // Track error messages
}

impl SledMetaStore {
    pub async fn new(config: SledMetastoreConfig) -> Result<Self> {
        info!("Initializing Sled metastore at: {}", config.path);
        
        let db = sled::open(&config.path)?;
        let partitions_tree = db.open_tree("partitions")?;
        let segments_tree = db.open_tree("segments")?;
        let meta_tree = db.open_tree("metadata")?;
        let tables_tree = db.open_tree("tables")?;
        let errors_tree = db.open_tree("errors")?;
        
        Ok(Self {
            db,
            partitions_tree,
            segments_tree,
            meta_tree,
            tables_tree,
            errors_tree,
        })
    }
}

#[async_trait]
impl MetaStore for SledMetaStore {
    async fn create_table(&self, table: TableMetadata) -> Result<()> {
        let key = table.table_name.as_bytes();
        let value = bincode::serialize(&table)?;
        self.tables_tree.insert(key, value)?;
        self.tables_tree.flush_async().await?;
        Ok(())
    }
    
    async fn get_table(&self, table_name: &str) -> Result<Option<TableMetadata>> {
        let key = table_name.as_bytes();
        match self.tables_tree.get(key)? {
            Some(value) => Ok(Some(bincode::deserialize(&value)?)),
            None => Ok(None),
        }
    }
    
    async fn create_partition(&self, metadata: PartitionMetadata) -> Result<()> {
        let key = metadata.partition_id.as_bytes();
        let value = bincode::serialize(&metadata)?;
        self.partitions_tree.insert(key, value)?;
        self.partitions_tree.flush_async().await?;
        Ok(())
    }
    
    async fn get_partition(&self, partition_id: &str) -> Result<Option<PartitionMetadata>> {
        let key = partition_id.as_bytes();
        match self.partitions_tree.get(key)? {
            Some(value) => Ok(Some(bincode::deserialize(&value)?)),
            None => Ok(None),
        }
    }
    
    async fn list_partitions(&self, time_range: TimeRange) -> Result<Vec<PartitionMetadata>> {
        let mut partitions = Vec::new();
        
        for item in self.partitions_tree.iter() {
            let (_, value) = item?;
            let metadata: PartitionMetadata = bincode::deserialize(&value)?;
            
            // Check if partition overlaps with time range
            if metadata.time_range.start <= time_range.end && metadata.time_range.end >= time_range.start {
                partitions.push(metadata);
            }
        }
        
        // Sort by start time
        partitions.sort_by_key(|p| p.time_range.start);
        Ok(partitions)
    }
    
    async fn create_segment(&self, metadata: SegmentMetadata) -> Result<()> {
        let key = metadata.segment_id.as_bytes();
        let value = bincode::serialize(&metadata)?;
        self.segments_tree.insert(key, value)?;
        
        // Update partition segment count
        if let Some(mut partition) = self.get_partition(&metadata.partition_id).await? {
            partition.segment_count += 1;
            partition.row_count += metadata.row_count;
            partition.compressed_bytes += metadata.compressed_bytes;
            partition.uncompressed_bytes += metadata.uncompressed_bytes;
            partition.updated_at = Utc::now();
            self.create_partition(partition).await?;
        }
        
        self.segments_tree.flush_async().await?;
        Ok(())
    }
    
    async fn update_segment(&self, segment_id: &str, metadata: SegmentMetadata) -> Result<()> {
        let key = segment_id.as_bytes();
        let value = bincode::serialize(&metadata)?;
        self.segments_tree.insert(key, value)?;
        self.segments_tree.flush_async().await?;
        Ok(())
    }
    
    async fn get_segment(&self, segment_id: &str) -> Result<Option<SegmentMetadata>> {
        let key = segment_id.as_bytes();
        match self.segments_tree.get(key)? {
            Some(value) => Ok(Some(bincode::deserialize(&value)?)),
            None => Ok(None),
        }
    }
    
    async fn list_segments(&self, partition_id: &str) -> Result<Vec<SegmentMetadata>> {
        let mut segments = Vec::new();
        
        for item in self.segments_tree.iter() {
            let (_, value) = item?;
            let metadata: SegmentMetadata = bincode::deserialize(&value)?;
            if metadata.partition_id == partition_id {
                segments.push(metadata);
            }
        }
        
        // Sort by min timestamp
        segments.sort_by_key(|s| s.min_timestamp);
        Ok(segments)
    }
    
    async fn delete_segment(&self, segment_id: &str) -> Result<()> {
        // Get segment to update partition stats
        let key = segment_id.as_bytes();
        if let Some(value) = self.segments_tree.get(key)? {
            let segment: SegmentMetadata = bincode::deserialize(&value)?;
            
            if let Some(mut partition) = self.get_partition(&segment.partition_id).await? {
                partition.segment_count = partition.segment_count.saturating_sub(1);
                partition.row_count = partition.row_count.saturating_sub(segment.row_count);
                partition.compressed_bytes = partition.compressed_bytes.saturating_sub(segment.compressed_bytes);
                partition.uncompressed_bytes = partition.uncompressed_bytes.saturating_sub(segment.uncompressed_bytes);
                partition.updated_at = Utc::now();
                self.create_partition(partition).await?;
            }
        }
        
        self.segments_tree.remove(segment_id.as_bytes())?;
        self.segments_tree.flush_async().await?;
        Ok(())
    }
    
    async fn delete_partition(&self, partition_id: &str) -> Result<()> {
        // Delete all segments in the partition
        let segments = self.list_segments(partition_id).await?;
        for segment in segments {
            self.segments_tree.remove(segment.segment_id.as_bytes())?;
        }
        
        // Delete the partition
        self.partitions_tree.remove(partition_id.as_bytes())?;
        
        self.db.flush_async().await?;
        Ok(())
    }
    
    async fn get_storage_stats(&self) -> Result<StorageStats> {
        let mut stats = StorageStats {
            total_partitions: 0,
            total_segments: 0,
            total_rows: 0,
            total_compressed_bytes: 0,
            total_uncompressed_bytes: 0,
        };
        
        // Count partitions and aggregate stats
        for item in self.partitions_tree.iter() {
            let (_, value) = item?;
            let metadata: PartitionMetadata = bincode::deserialize(&value)?;
            stats.total_partitions += 1;
            stats.total_rows += metadata.row_count;
            stats.total_compressed_bytes += metadata.compressed_bytes;
            stats.total_uncompressed_bytes += metadata.uncompressed_bytes;
        }
        
        // Count segments
        stats.total_segments = self.segments_tree.len() as u64;
        
        Ok(stats)
    }
    
    async fn get_last_indexed_sequence(&self) -> Result<Option<u64>> {
        match self.meta_tree.get(b"last_indexed_sequence")? {
            Some(value) => Ok(Some(bincode::deserialize(&value)?)),
            None => Ok(None),
        }
    }
    
    async fn set_last_indexed_sequence(&self, sequence: u64) -> Result<()> {
        let value = bincode::serialize(&sequence)?;
        self.meta_tree.insert(b"last_indexed_sequence", value)?;
        self.meta_tree.flush_async().await?;
        Ok(())
    }
    
    // NEW: Control plane monitoring methods
    
    async fn list_segments_by_status(
        &self, 
        storage_status: Option<SegmentStatus>, 
        index_status: Option<SegmentStatus>
    ) -> Result<Vec<SegmentMetadata>> {
        let mut segments = Vec::new();
        
        for item in self.segments_tree.iter() {
            let (_, value) = item?;
            let metadata: SegmentMetadata = bincode::deserialize(&value)?;
            
            let storage_match = storage_status.map_or(true, |s| metadata.storage_status == s);
            let index_match = index_status.map_or(true, |s| metadata.index_status == s);
            
            if storage_match && index_match {
                segments.push(metadata);
            }
        }
        
        // Sort by creation time
        segments.sort_by_key(|s| s.created_at);
        Ok(segments)
    }
    
    async fn get_segment_lifecycle_stats(&self) -> Result<LifecycleStats> {
        let mut stats = LifecycleStats {
            total_segments: 0,
            pending_storage: 0,
            stored_segments: 0,
            failed_storage: 0,
            pending_index: 0,
            indexed_segments: 0,
            failed_index: 0,
            orphaned_indexes: 0,
            missing_indexes: 0,
        };
        
        for item in self.segments_tree.iter() {
            let (_, value) = item?;
            let metadata: SegmentMetadata = bincode::deserialize(&value)?;
            
            stats.total_segments += 1;
            
            // Count storage status
            match metadata.storage_status {
                SegmentStatus::Pending => stats.pending_storage += 1,
                SegmentStatus::Stored => stats.stored_segments += 1,
                SegmentStatus::Failed => stats.failed_storage += 1,
            }
            
            // Count index status
            match metadata.index_status {
                SegmentStatus::Pending => stats.pending_index += 1,
                SegmentStatus::Stored => stats.indexed_segments += 1,
                SegmentStatus::Failed => stats.failed_index += 1,
            }
            
            // Check for inconsistencies
            if metadata.index_status == SegmentStatus::Stored && metadata.storage_status != SegmentStatus::Stored {
                stats.orphaned_indexes += 1;
            }
            if metadata.storage_status == SegmentStatus::Stored && metadata.index_status == SegmentStatus::Failed {
                stats.missing_indexes += 1;
            }
        }
        
        Ok(stats)
    }
    
    async fn mark_segment_storage_failed(&self, segment_id: &str, error: &str) -> Result<()> {
        if let Some(mut segment) = self.get_segment(segment_id).await? {
            segment.storage_status = SegmentStatus::Failed;
            segment.updated_at = Utc::now();
            self.update_segment(segment_id, segment).await?;
            
            // Store error message
            let error_key = format!("storage_error:{}", segment_id);
            self.errors_tree.insert(error_key.as_bytes(), error.as_bytes())?;
            self.errors_tree.flush_async().await?;
            
            warn!("Marked segment {} storage as failed: {}", segment_id, error);
        }
        Ok(())
    }
    
    async fn mark_segment_index_failed(&self, segment_id: &str, error: &str) -> Result<()> {
        if let Some(mut segment) = self.get_segment(segment_id).await? {
            segment.index_status = SegmentStatus::Failed;
            segment.updated_at = Utc::now();
            self.update_segment(segment_id, segment).await?;
            
            // Store error message
            let error_key = format!("index_error:{}", segment_id);
            self.errors_tree.insert(error_key.as_bytes(), error.as_bytes())?;
            self.errors_tree.flush_async().await?;
            
            warn!("Marked segment {} index as failed: {}", segment_id, error);
        }
        Ok(())
    }
}