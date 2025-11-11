use std::path::PathBuf;
use std::sync::Arc;
use anyhow::{Result, Context};
use tantivy::{Index};
use tracing::{info, debug, warn};

use openwit_metastore::{MetaStore, PartitionMetadata, TimeRange};
use chrono::{DateTime, Utc};

pub struct CompactionPolicy {
    max_segments_per_partition: usize,
    index_base_path: PathBuf,
    metastore: Arc<dyn MetaStore>,
}

impl CompactionPolicy {
    pub fn new(
        max_segments_per_partition: usize,
        index_base_path: PathBuf,
        metastore: Arc<dyn MetaStore>,
    ) -> Self {
        Self {
            max_segments_per_partition,
            index_base_path,
            metastore,
        }
    }

    pub async fn compact_partition(&self, partition: &PartitionMetadata) -> Result<usize> {
        // Get segments for this partition
        let segments = self.metastore
            .list_segments(&partition.partition_id)
            .await?;

        // Check if compaction is needed
        if segments.len() <= self.max_segments_per_partition {
            debug!("Partition {} has {} segments, no compaction needed",
                  partition.partition_id, segments.len());
            return Ok(0);
        }

        info!("Compacting partition {} with {} segments",
              partition.partition_id, segments.len());

        // Get the index path for this partition
        let index_path = self.index_base_path.join(&partition.partition_id).join("tantivy");
        
        if !index_path.exists() {
            warn!("Index path {:?} does not exist for partition {}",
                  index_path, partition.partition_id);
            return Ok(0);
        }

        // Open the Tantivy index
        let index = Index::open_in_dir(&index_path)
            .context("Failed to open Tantivy index")?;

        // Force merge segments
        let mut index_writer: tantivy::IndexWriter<tantivy::TantivyDocument> = index.writer(100_000_000)?; // 100MB heap
        
        // Tantivy will automatically merge segments based on its merge policy
        // Commit to trigger potential merges
        index_writer.commit()?;
        
        // Wait for merges to complete
        index_writer.wait_merging_threads()?;

        // Count segments after merge
        let reader = index.reader()?;
        let searcher = reader.searcher();
        let segment_readers = searcher.segment_readers();
        let new_segment_count = segment_readers.len();

        let segments_removed = segments.len().saturating_sub(new_segment_count);
        
        if segments_removed > 0 {
            info!("Compacted {} segments in partition {}, {} segments remaining",
                  segments_removed, partition.partition_id, new_segment_count);
            
            // Update segment count in metastore
            let mut updated_partition = partition.clone();
            updated_partition.segment_count = new_segment_count;
            self.metastore.update_partition(updated_partition).await?;
        }

        Ok(segments_removed)
    }

    pub async fn should_compact(&self, partition: &PartitionMetadata) -> bool {
        partition.segment_count > self.max_segments_per_partition
    }

    pub async fn estimate_compaction_benefit(&self, partition: &PartitionMetadata) -> Result<CompactionEstimate> {
        let segments = self.metastore
            .list_segments(&partition.partition_id)
            .await?;

        let total_size: u64 = segments.iter()
            .map(|s| s.compressed_bytes)
            .sum();

        // Estimate size after compaction (typically 70-80% of original)
        let estimated_size_after = (total_size as f64 * 0.75) as u64;
        let space_saved = total_size.saturating_sub(estimated_size_after);

        Ok(CompactionEstimate {
            current_segments: segments.len(),
            estimated_segments_after: self.max_segments_per_partition.min(segments.len() / 2),
            current_size_bytes: total_size,
            estimated_size_after_bytes: estimated_size_after,
            space_saved_bytes: space_saved,
        })
    }

    pub async fn compact_all_partitions(&self) -> Result<CompactionSummary> {
        // Get all partitions (using a very wide time range)
        let time_range = TimeRange {
            start: DateTime::<Utc>::MIN_UTC,
            end: DateTime::<Utc>::MAX_UTC,
        };
        let partitions = self.metastore.list_partitions(time_range).await?;
        
        let mut total_segments_removed = 0;
        let mut partitions_compacted = 0;
        let mut failed_partitions = Vec::new();

        for partition in partitions {
            match self.compact_partition(&partition).await {
                Ok(segments_removed) => {
                    if segments_removed > 0 {
                        total_segments_removed += segments_removed;
                        partitions_compacted += 1;
                    }
                }
                Err(e) => {
                    warn!("Failed to compact partition {}: {}",
                          partition.partition_id, e);
                    failed_partitions.push(partition.partition_id);
                }
            }
        }

        Ok(CompactionSummary {
            partitions_compacted,
            total_segments_removed,
            failed_partitions,
        })
    }
}

#[derive(Debug, Clone)]
pub struct CompactionEstimate {
    pub current_segments: usize,
    pub estimated_segments_after: usize,
    pub current_size_bytes: u64,
    pub estimated_size_after_bytes: u64,
    pub space_saved_bytes: u64,
}

#[derive(Debug, Clone)]
pub struct CompactionSummary {
    pub partitions_compacted: usize,
    pub total_segments_removed: usize,
    pub failed_partitions: Vec<String>,
}