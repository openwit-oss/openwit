use std::path::PathBuf;
use std::sync::Arc;
use std::fs::Metadata;
use anyhow::Result;
use chrono::{DateTime, Utc, Duration};
use tokio::fs;
use tracing::{info, debug, warn};

use openwit_metastore::MetaStore;

pub struct WalCleanupPolicy {
    retention_hours: u64,
    wal_base_path: PathBuf,
    metastore: Arc<dyn MetaStore>,
}

impl WalCleanupPolicy {
    pub fn new(
        retention_hours: u64,
        wal_base_path: PathBuf,
        metastore: Arc<dyn MetaStore>,
    ) -> Self {
        Self {
            retention_hours,
            wal_base_path,
            metastore,
        }
    }

    pub async fn cleanup(&self) -> Result<(usize, u64)> {
        info!("Starting WAL cleanup with retention period: {} hours", self.retention_hours);
        
        let mut files_removed = 0;
        let mut bytes_removed = 0;

        // Get the cutoff time
        let cutoff_time = Utc::now() - Duration::hours(self.retention_hours as i64);

        // List all WAL files
        let wal_files = self.list_wal_files().await?;
        
        for (file_path, metadata) in wal_files {
            // Check if we can safely remove this WAL file
            if self.can_remove_wal_file(&file_path, &metadata, cutoff_time).await? {
                let file_size = metadata.len();
                
                match fs::remove_file(&file_path).await {
                    Ok(_) => {
                        files_removed += 1;
                        bytes_removed += file_size;
                        debug!("Removed WAL file: {:?} ({} bytes)", file_path, file_size);
                    }
                    Err(e) => {
                        warn!("Failed to remove WAL file {:?}: {}", file_path, e);
                    }
                }
            }
        }

        info!("WAL cleanup completed: removed {} files, {} bytes",
              files_removed, bytes_removed);

        Ok((files_removed, bytes_removed))
    }

    async fn list_wal_files(&self) -> Result<Vec<(PathBuf, Metadata)>> {
        let mut wal_files = Vec::new();

        if !self.wal_base_path.exists() {
            debug!("WAL directory does not exist: {:?}", self.wal_base_path);
            return Ok(wal_files);
        }

        let mut entries = fs::read_dir(&self.wal_base_path).await?;
        
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            let metadata = entry.metadata().await?;
            
            if metadata.is_file() && path.extension().and_then(|s| s.to_str()) == Some("wal") {
                wal_files.push((path, metadata));
            }
        }

        // Sort by modification time (oldest first)
        wal_files.sort_by_key(|(_, metadata)| {
            metadata.modified()
                .unwrap_or(std::time::SystemTime::UNIX_EPOCH)
        });

        Ok(wal_files)
    }

    async fn can_remove_wal_file(
        &self,
        file_path: &PathBuf,
        metadata: &Metadata,
        cutoff_time: DateTime<Utc>,
    ) -> Result<bool> {
        // Extract sequence number from filename if possible
        let sequence = self.extract_sequence_from_path(file_path);
        
        if let Some(seq) = sequence {
            // Check if this sequence has been indexed
            let last_indexed_seq = self.metastore
                .get_last_indexed_sequence()
                .await?
                .unwrap_or(0);
            
            if seq > last_indexed_seq {
                // This WAL file hasn't been indexed yet, don't remove
                debug!("WAL file {:?} (seq {}) not yet indexed (last indexed: {})",
                       file_path, seq, last_indexed_seq);
                return Ok(false);
            }
        }

        // Check modification time
        let modified = metadata.modified()?;
        let modified_time = DateTime::<Utc>::from(modified);
        
        if modified_time > cutoff_time {
            // File is too recent, don't remove
            debug!("WAL file {:?} is too recent (modified: {})",
                   file_path, modified_time);
            return Ok(false);
        }

        Ok(true)
    }

    fn extract_sequence_from_path(&self, path: &PathBuf) -> Option<u64> {
        // Expected format: wal_000000001.wal
        path.file_stem()
            .and_then(|stem| stem.to_str())
            .and_then(|s| s.strip_prefix("wal_"))
            .and_then(|s| s.parse::<u64>().ok())
    }

    pub async fn get_wal_stats(&self) -> Result<WalStats> {
        let wal_files = self.list_wal_files().await?;
        
        let total_files = wal_files.len();
        let total_bytes: u64 = wal_files.iter()
            .map(|(_, metadata)| metadata.len())
            .sum();
        
        let oldest_file = wal_files.first()
            .and_then(|(path, metadata)| {
                metadata.modified().ok()
                    .map(|t| (path.clone(), DateTime::<Utc>::from(t)))
            });
        
        let newest_file = wal_files.last()
            .and_then(|(path, metadata)| {
                metadata.modified().ok()
                    .map(|t| (path.clone(), DateTime::<Utc>::from(t)))
            });

        Ok(WalStats {
            total_files,
            total_bytes,
            oldest_file,
            newest_file,
        })
    }

    pub async fn estimate_cleanup_impact(&self) -> Result<CleanupEstimate> {
        let cutoff_time = Utc::now() - Duration::hours(self.retention_hours as i64);
        let wal_files = self.list_wal_files().await?;
        
        let mut files_to_remove = 0;
        let mut bytes_to_remove = 0;
        
        for (file_path, metadata) in wal_files {
            if self.can_remove_wal_file(&file_path, &metadata, cutoff_time).await? {
                files_to_remove += 1;
                bytes_to_remove += metadata.len();
            }
        }

        Ok(CleanupEstimate {
            files_to_remove,
            bytes_to_remove,
        })
    }
}

#[derive(Debug, Clone)]
pub struct WalStats {
    pub total_files: usize,
    pub total_bytes: u64,
    pub oldest_file: Option<(PathBuf, DateTime<Utc>)>,
    pub newest_file: Option<(PathBuf, DateTime<Utc>)>,
}

#[derive(Debug, Clone)]
pub struct CleanupEstimate {
    pub files_to_remove: usize,
    pub bytes_to_remove: u64,
}