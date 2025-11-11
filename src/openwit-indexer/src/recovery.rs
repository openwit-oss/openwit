use std::path::{Path, PathBuf};
use std::sync::Arc;
use anyhow::{Result};
use tokio::sync::mpsc;
use tracing::{info, warn, error, debug};
use chrono::{DateTime, Utc};

use openwit_storage::metastore::{MetaStore, TimeRange};
use openwit_storage::wal_reader::WalReader;
use openwit_ingestion::{IngestedMessage};
use crate::tantivy_indexer::TantivyIndexer;
use crate::Index;

#[allow(unused)]
pub struct IndexRecoveryManager {
    metastore: Arc<dyn MetaStore>,
    wal_dir: PathBuf,
    index_dir: PathBuf,
    node_id: String,
}

#[derive(Debug)]
pub struct RecoveryStatus {
    pub last_indexed_sequence: u64,
    pub last_wal_sequence: u64,
    pub entries_to_recover: u64,
    pub corrupted_partitions: Vec<String>,
    pub missing_partitions: Vec<String>,
}

#[derive(Debug)]
pub struct RecoveryReport {
    pub started_at: DateTime<Utc>,
    pub completed_at: DateTime<Utc>,
    pub entries_recovered: u64,
    pub partitions_rebuilt: usize,
    pub errors: Vec<String>,
}

impl IndexRecoveryManager {
    pub fn new(
        metastore: Arc<dyn MetaStore>,
        wal_dir: PathBuf,
        index_dir: PathBuf,
        node_id: String,
    ) -> Self {
        Self {
            metastore,
            wal_dir,
            index_dir,
            node_id,
        }
    }

    /// Check recovery status and determine what needs to be recovered
    pub async fn check_recovery_status(&self) -> Result<RecoveryStatus> {
        info!("Checking index recovery status");
        
        // Get last indexed sequence from metastore
        let last_indexed_sequence = self.metastore
            .get_last_indexed_sequence()
            .await?
            .unwrap_or(0);
        
        // Get last WAL sequence
        // TODO: Implement get_last_sequence method in WalReader
        let last_wal_sequence = 0u64;
        
        // Check for corrupted or missing partitions
        let (corrupted, missing) = self.check_index_health().await?;
        
        let entries_to_recover = if last_wal_sequence > last_indexed_sequence {
            last_wal_sequence - last_indexed_sequence
        } else {
            0
        };
        
        Ok(RecoveryStatus {
            last_indexed_sequence,
            last_wal_sequence,
            entries_to_recover,
            corrupted_partitions: corrupted,
            missing_partitions: missing,
        })
    }

    /// Perform index recovery
    pub async fn recover(&self) -> Result<RecoveryReport> {
        let started_at = Utc::now();
        let mut report = RecoveryReport {
            started_at,
            completed_at: Utc::now(),
            entries_recovered: 0,
            partitions_rebuilt: 0,
            errors: Vec::new(),
        };
        
        // Check what needs recovery
        let status = self.check_recovery_status().await?;
        
        info!(
            "Starting recovery: {} entries to recover, {} corrupted partitions, {} missing partitions",
            status.entries_to_recover,
            status.corrupted_partitions.len(),
            status.missing_partitions.len()
        );
        
        // Rebuild corrupted partitions
        for partition_id in &status.corrupted_partitions {
            match self.rebuild_partition(partition_id).await {
                Ok(_) => {
                    report.partitions_rebuilt += 1;
                    info!("Rebuilt corrupted partition: {}", partition_id);
                }
                Err(e) => {
                    let error = format!("Failed to rebuild partition {}: {}", partition_id, e);
                    error!("{}", error);
                    report.errors.push(error);
                }
            }
        }
        
        // Recover from WAL
        if status.entries_to_recover > 0 {
            match self.recover_from_wal(status.last_indexed_sequence).await {
                Ok(entries) => {
                    report.entries_recovered = entries;
                    info!("Recovered {} entries from WAL", entries);
                }
                Err(e) => {
                    let error = format!("WAL recovery failed: {}", e);
                    error!("{}", error);
                    report.errors.push(error);
                }
            }
        }
        
        report.completed_at = Utc::now();
        
        info!(
            "Recovery completed in {:?}: {} entries recovered, {} partitions rebuilt, {} errors",
            report.completed_at - report.started_at,
            report.entries_recovered,
            report.partitions_rebuilt,
            report.errors.len()
        );
        
        Ok(report)
    }

    /// Check health of all index partitions
    async fn check_index_health(&self) -> Result<(Vec<String>, Vec<String>)> {
        let mut corrupted = Vec::new();
        let mut missing = Vec::new();
        
        // List all partitions from metastore (use a very wide time range to get all)
        let time_range = TimeRange {
            start: Utc::now() - chrono::Duration::days(365 * 10), // 10 years ago
            end: Utc::now() + chrono::Duration::days(365), // 1 year in future
        };
        let partitions = self.metastore.list_partitions(time_range).await?;
        
        for partition in partitions {
            let partition_path = self.index_dir.join(&partition.partition_id);
            
            if !partition_path.exists() {
                missing.push(partition.partition_id.clone());
                continue;
            }
            
            // Check if index can be opened
            match tantivy::Index::open_in_dir(&partition_path.join("tantivy")) {
                Ok(index) => {
                    // Verify index integrity
                    if let Err(e) = self.verify_index_integrity(&index).await {
                        warn!("Partition {} failed integrity check: {}", partition.partition_id, e);
                        corrupted.push(partition.partition_id);
                    }
                }
                Err(e) => {
                    warn!("Cannot open index for partition {}: {}", partition.partition_id, e);
                    corrupted.push(partition.partition_id);
                }
            }
        }
        
        Ok((corrupted, missing))
    }

    /// Verify index integrity
    async fn verify_index_integrity(&self, index: &tantivy::Index) -> Result<()> {
        let reader = index.reader()?;
        let searcher = reader.searcher();
        
        // Try to read segment metadata
        for segment_reader in searcher.segment_readers() {
            let num_docs = segment_reader.num_docs();
            if num_docs == 0 {
                return Err(anyhow::anyhow!("Empty segment detected"));
            }
        }
        
        // Try a simple count query
        let count_query = tantivy::query::AllQuery;
        let count = searcher.search(&count_query, &tantivy::collector::Count)?;
        
        debug!("Index has {} documents", count);
        Ok(())
    }

    /// Rebuild a corrupted partition from WAL
    pub async fn rebuild_partition(&self, partition_id: &str) -> Result<()> {
        info!("Rebuilding partition: {}", partition_id);
        
        // Delete corrupted index files
        let partition_path = self.index_dir.join(partition_id);
        if partition_path.exists() {
            tokio::fs::remove_dir_all(&partition_path).await?;
        }
        
        // Get partition metadata
        let _partition = self.metastore
            .get_partition(partition_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Partition {} not found in metastore", partition_id))?;
        
        // Find WAL entries for this partition's time range
        // TODO: Implement read_time_range method in WalReader
        let entries: Vec<IngestedMessage> = Vec::new();
        
        if entries.is_empty() {
            warn!("No WAL entries found for partition {}", partition_id);
            return Ok(());
        }
        
        // Create new indexer for this partition
        // TODO: Implement new_for_partition method in TantivyIndexer
        // For now, use the regular new method
        let mut config = crate::IndexConfig::default();
        config.storage_path = self.index_dir.to_string_lossy().to_string();
        let mut indexer = TantivyIndexer::new(
            config,
            self.metastore.clone(),
        ).await?;
        
        // Re-index all entries
        let mut indexed = 0;
        for _entry in entries {
            // TODO: Convert IngestedMessage to Document format
            // indexer.index_batch(vec![entry]).await?;
            indexed += 1;
        }
        
        indexer.flush().await?;
        
        info!("Rebuilt partition {} with {} entries", partition_id, indexed);
        Ok(())
    }

    /// Recover missing entries from WAL
    async fn recover_from_wal(&self, from_sequence: u64) -> Result<u64> {
        info!("Recovering from WAL starting at sequence {}", from_sequence);
        
        let _wal_reader = WalReader::new(self.node_id.clone());
        
        // WAL channel removed - no longer needed
        
        // Start WAL reader task
        // TODO: Implement stream_from_sequence method in WalReader
        let reader_task = tokio::spawn(async move {
            Ok::<(), anyhow::Error>(())
        });
        
        // Create indexer
        let mut config = crate::IndexConfig::default();
        config.storage_path = self.index_dir.to_string_lossy().to_string();
        let mut indexer = TantivyIndexer::new(
            config,
            self.metastore.clone(),
        ).await?;
        
        let recovered = 0;
        let last_sequence = from_sequence;
        
        // Process entries as they come
        // TODO: Implement proper WAL entry processing once WalReader supports streaming
        /*
        while let Some(entry) = rx.recv().await {
            last_sequence = entry.sequence;
            
            // Index the batch
            indexer.index_batch(entry.messages).await?;
            recovered += 1;
            
            // Commit periodically
            if recovered % 1000 == 0 {
                indexer.flush().await?;
                
                // Update last indexed sequence
                self.metastore.set_last_indexed_sequence(last_sequence).await?;
                
                debug!("Recovered {} entries, last sequence: {}", recovered, last_sequence);
            }
        }
        */
        
        // Final commit
        if recovered % 1000 != 0 {
            indexer.flush().await?;
            self.metastore.set_last_indexed_sequence(last_sequence).await?;
        }
        
        // Wait for reader to complete
        reader_task.await??;
        
        Ok(recovered)
    }

    /// Trigger a full reindex from a specific sequence
    pub async fn reindex_from_sequence(&self, from_sequence: u64) -> Result<RecoveryReport> {
        info!("Starting full reindex from sequence {}", from_sequence);
        
        // Set last indexed sequence to trigger full recovery
        self.metastore.set_last_indexed_sequence(from_sequence).await?;
        
        // Run recovery
        self.recover().await
    }

    /// Create a recovery checkpoint
    pub async fn create_checkpoint(&self) -> Result<CheckpointInfo> {
        let status = self.check_recovery_status().await?;
        
        let checkpoint = CheckpointInfo {
            created_at: Utc::now(),
            last_indexed_sequence: status.last_indexed_sequence,
            partition_count: {
                let time_range = TimeRange {
                    start: Utc::now() - chrono::Duration::days(365 * 10),
                    end: Utc::now() + chrono::Duration::days(365),
                };
                self.metastore.list_partitions(time_range).await?.len()
            },
            index_size_bytes: self.calculate_index_size().await?,
        };
        
        // Store checkpoint in metastore
        let _checkpoint_data = serde_json::to_string(&checkpoint)?;
        // This would store the checkpoint - implementation depends on metastore
        
        Ok(checkpoint)
    }

    async fn calculate_index_size(&self) -> Result<u64> {
        let mut total_size = 0;
        
        let mut entries = tokio::fs::read_dir(&self.index_dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            if entry.file_type().await?.is_dir() {
                let size = self.dir_size(&entry.path()).await?;
                total_size += size;
            }
        }
        
        Ok(total_size)
    }

    fn dir_size<'a>(&'a self, path: &'a Path) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<u64>> + Send + 'a>> {
        Box::pin(async move {
            let mut size = 0;
            let mut entries = tokio::fs::read_dir(path).await?;
            
            while let Some(entry) = entries.next_entry().await? {
                let metadata = entry.metadata().await?;
                if metadata.is_file() {
                    size += metadata.len();
                } else if metadata.is_dir() {
                    size += self.dir_size(&entry.path()).await?;
                }
            }
            
            Ok(size)
        })
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CheckpointInfo {
    pub created_at: DateTime<Utc>,
    pub last_indexed_sequence: u64,
    pub partition_count: usize,
    pub index_size_bytes: u64,
}