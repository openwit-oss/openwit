use std::path::PathBuf;
use std::sync::Arc;
use anyhow::{Result, Context};
use chrono::{DateTime, Utc};
use tokio::fs;
use tracing::{info, debug, warn};
use walkdir::WalkDir;

use openwit_metastore::{MetaStore, PartitionMetadata, TimeRange};
// ObjectStoreBackend has been removed in favor of OpenDAL

pub struct RetentionPolicy {
    retention_period_days: u64,
    storage_base_path: PathBuf,
    metastore: Arc<dyn MetaStore>,
}

impl RetentionPolicy {
    pub fn new(
        retention_period_days: u64,
        storage_base_path: PathBuf,
        metastore: Arc<dyn MetaStore>,
    ) -> Self {
        Self {
            retention_period_days,
            storage_base_path,
            metastore,
        }
    }

    pub async fn cleanup_old_data(&self, cutoff_time: DateTime<Utc>) -> Result<(usize, usize, u64)> {
        info!("Starting retention cleanup with cutoff time: {}", cutoff_time);
        
        let mut partitions_removed = 0;
        let mut files_removed = 0;
        let mut bytes_removed = 0;

        // List all partitions
        let time_range = TimeRange {
            start: DateTime::<Utc>::MIN_UTC,
            end: DateTime::<Utc>::MAX_UTC,
        };
        let partitions = self.metastore.list_partitions(time_range).await?;

        for partition in partitions {
            // Check if partition is older than retention period
            if partition.time_range.end < cutoff_time {
                match self.remove_partition(&partition).await {
                    Ok((files, bytes)) => {
                        partitions_removed += 1;
                        files_removed += files;
                        bytes_removed += bytes;
                        info!("Removed partition {} ({} files, {} bytes)",
                              partition.partition_id, files, bytes);
                    }
                    Err(e) => {
                        warn!("Failed to remove partition {}: {}",
                              partition.partition_id, e);
                    }
                }
            }
        }

        Ok((partitions_removed, files_removed, bytes_removed))
    }

    async fn remove_partition(&self, partition: &PartitionMetadata) -> Result<(usize, u64)> {
        let mut files_removed = 0;
        let mut bytes_removed = 0;

        // Get all segments for this partition
        let segments = self.metastore.list_segments(&partition.partition_id).await?;

        // Remove Parquet files
        for segment in &segments {
            let full_path = self.storage_base_path.join(&segment.file_path);
            if full_path.exists() {
                let metadata = fs::metadata(&full_path).await?;
                bytes_removed += metadata.len();
                fs::remove_file(&full_path).await
                    .context("Failed to remove Parquet file")?;
                files_removed += 1;
                debug!("Removed Parquet file: {:?}", full_path);
            }
        }

        // Remove index directory
        let index_path = self.storage_base_path
            .parent()
            .unwrap()
            .join("index")
            .join(&partition.partition_id);
        
        if index_path.exists() {
            let (idx_files, idx_bytes) = self.remove_directory(&index_path).await?;
            files_removed += idx_files;
            bytes_removed += idx_bytes;
            debug!("Removed index directory: {:?}", index_path);
        }

        // Remove segments from metastore
        for segment in segments {
            self.metastore.delete_segment(&segment.segment_id).await?;
        }

        // Remove partition from metastore
        self.metastore.delete_partition(&partition.partition_id).await?;

        Ok((files_removed, bytes_removed))
    }

    async fn remove_directory(&self, path: &PathBuf) -> Result<(usize, u64)> {
        let mut files_removed = 0;
        let mut bytes_removed = 0;

        // Walk directory and calculate size
        for entry in WalkDir::new(path) {
            let entry = entry?;
            if entry.file_type().is_file() {
                let metadata = entry.metadata()?;
                bytes_removed += metadata.len();
                files_removed += 1;
            }
        }

        // Remove the directory
        fs::remove_dir_all(path).await
            .context("Failed to remove directory")?;

        Ok((files_removed, bytes_removed))
    }

    pub async fn get_expired_partitions(&self, cutoff_time: DateTime<Utc>) -> Result<Vec<PartitionMetadata>> {
        let time_range = TimeRange {
            start: DateTime::<Utc>::MIN_UTC,
            end: DateTime::<Utc>::MAX_UTC,
        };
        let partitions = self.metastore.list_partitions(time_range).await?;
        
        Ok(partitions.into_iter()
            .filter(|p| p.time_range.end < cutoff_time)
            .collect())
    }

    pub async fn estimate_cleanup_impact(&self, cutoff_time: DateTime<Utc>) -> Result<CleanupEstimate> {
        let expired_partitions = self.get_expired_partitions(cutoff_time).await?;
        
        let mut total_files = 0;
        let mut total_bytes = 0;
        
        for partition in &expired_partitions {
            let segments = self.metastore.list_segments(&partition.partition_id).await?;
            total_files += segments.len();
            total_bytes += segments.iter()
                .map(|s| s.compressed_bytes)
                .sum::<u64>();
            
            // Estimate index size (typically 20% of data size)
            total_bytes += (total_bytes as f64 * 0.2) as u64;
        }

        Ok(CleanupEstimate {
            partitions_to_remove: expired_partitions.len(),
            files_to_remove: total_files,
            bytes_to_remove: total_bytes,
            oldest_partition_date: expired_partitions.first()
                .map(|p| p.time_range.start),
        })
    }

    pub fn is_partition_expired(&self, partition: &PartitionMetadata, now: DateTime<Utc>) -> bool {
        let retention_cutoff = now - chrono::Duration::days(self.retention_period_days as i64);
        partition.time_range.end < retention_cutoff
    }
}

#[derive(Debug, Clone)]
pub struct CleanupEstimate {
    pub partitions_to_remove: usize,
    pub files_to_remove: usize,
    pub bytes_to_remove: u64,
    pub oldest_partition_date: Option<DateTime<Utc>>,
}