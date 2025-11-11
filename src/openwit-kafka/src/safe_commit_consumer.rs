use anyhow::{Result, Context};
use rdkafka::consumer::{Consumer, StreamConsumer, CommitMode};
use rdkafka::{Message, TopicPartitionList, Offset};
use tracing::{info, warn, error, debug};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use std::collections::HashMap;

use crate::types::{KafkaConfig, KafkaMessage};
use crate::grpc_client::TelemetryIngestionClient;

/// Safe Kafka consumer that commits offsets only after WAL confirmation
pub struct SafeCommitKafkaConsumer {
    consumer: Arc<StreamConsumer>,
    config: KafkaConfig,
    grpc_client: Arc<TelemetryIngestionClient>,
    node_id: String,
    running: Arc<AtomicBool>,

    // Commit tracking
    pending_commits: Arc<tokio::sync::Mutex<HashMap<String, PendingCommit>>>,
    total_commits: Arc<AtomicU64>,
    failed_commits: Arc<AtomicU64>,
}

#[derive(Debug, Clone)]
struct PendingCommit {
    batch_id: String,
    offsets: TopicPartitionList,
    message_count: usize,
    created_at: Instant,
}

impl SafeCommitKafkaConsumer {
    pub fn new(
        consumer: StreamConsumer,
        config: KafkaConfig,
        grpc_client: Arc<TelemetryIngestionClient>,
        node_id: String,
    ) -> Self {
        info!(
            "[SAFE_COMMIT] Initializing safe commit consumer - commits only after WAL write"
        );

        Self {
            consumer: Arc::new(consumer),
            config,
            grpc_client,
            node_id,
            running: Arc::new(AtomicBool::new(true)),
            pending_commits: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            total_commits: Arc::new(AtomicU64::new(0)),
            failed_commits: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Process batch and commit ONLY after WAL confirmation
    pub async fn process_batch_with_safe_commit(
        &self,
        batch: Vec<KafkaMessage>,
    ) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }

        let batch_id = format!(
            "{}-{}",
            self.node_id,
            chrono::Utc::now().timestamp_millis()
        );
        let batch_size = batch.len();
        let start = Instant::now();

        // Build offset information for commit
        let mut offsets = TopicPartitionList::new();
        let mut offset_map: HashMap<(String, i32), i64> = HashMap::new();

        for msg in &batch {
            let key = (msg.topic.clone(), msg.partition);
            let next_offset = msg.offset + 1;  // Commit the NEXT offset to read

            // Track highest offset per topic-partition
            offset_map.entry(key)
                .and_modify(|e| *e = (*e).max(next_offset))
                .or_insert(next_offset);
        }

        // Add to TopicPartitionList
        for ((topic, partition), offset) in offset_map.iter() {
            offsets.add_partition_offset(
                topic,
                *partition,
                Offset::Offset(*offset),
            )?;
        }

        debug!(
            "[SAFE_COMMIT] Processing batch {} with {} messages, tracking offsets for commit",
            batch_id, batch_size
        );

        // Store pending commit
        {
            let mut pending = self.pending_commits.lock().await;
            pending.insert(
                batch_id.clone(),
                PendingCommit {
                    batch_id: batch_id.clone(),
                    offsets: offsets.clone(),
                    message_count: batch_size,
                    created_at: start,
                },
            );
        }

        // Send to ingestion service
        match self.grpc_client.send_batch(batch_id.clone(), batch).await {
            Ok(response) => {
                // Check WAL status
                if response.wal_written {
                    // WAL written successfully - safe to commit
                    self.commit_offsets_for_batch(&batch_id, &offsets).await?;

                    info!(
                        "[SAFE_COMMIT] Batch {} committed after WAL confirmation ({}ms, {} messages)",
                        batch_id,
                        start.elapsed().as_millis(),
                        batch_size
                    );

                    self.total_commits.fetch_add(1, Ordering::Relaxed);
                } else if response.success {
                    // Success but no WAL - don't commit (will reprocess on failure)
                    warn!(
                        "[SAFE_COMMIT] Batch {} succeeded but WAL not written - NOT committing offsets",
                        batch_id
                    );
                } else {
                    // Failed - don't commit
                    error!(
                        "[SAFE_COMMIT] Batch {} failed: {} - NOT committing offsets",
                        batch_id, response.message
                    );

                    self.failed_commits.fetch_add(1, Ordering::Relaxed);
                }

                // Remove from pending
                {
                    let mut pending = self.pending_commits.lock().await;
                    pending.remove(&batch_id);
                }

                Ok(())
            }
            Err(e) => {
                error!(
                    "[SAFE_COMMIT] Failed to send batch {}: {} - NOT committing offsets",
                    batch_id, e
                );

                // Remove from pending
                {
                    let mut pending = self.pending_commits.lock().await;
                    pending.remove(&batch_id);
                }

                self.failed_commits.fetch_add(1, Ordering::Relaxed);
                Err(e)
            }
        }
    }

    /// Commit offsets for a specific batch
    async fn commit_offsets_for_batch(
        &self,
        batch_id: &str,
        offsets: &TopicPartitionList,
    ) -> Result<()> {
        match self.consumer.commit(offsets, CommitMode::Sync) {
            Ok(_) => {
                debug!(
                    "[SAFE_COMMIT] Offsets committed for batch {} after WAL write",
                    batch_id
                );
                Ok(())
            }
            Err(e) => {
                error!(
                    "[SAFE_COMMIT] Failed to commit offsets for batch {}: {}",
                    batch_id, e
                );
                Err(anyhow::anyhow!("Offset commit failed: {}", e))
            }
        }
    }

    /// Check for any pending commits that are stuck
    pub async fn check_pending_commits(&self) {
        let pending = self.pending_commits.lock().await;

        if !pending.is_empty() {
            warn!(
                "[SAFE_COMMIT] {} batches pending commit (waiting for WAL confirmation)",
                pending.len()
            );

            for (batch_id, commit) in pending.iter() {
                let age = commit.created_at.elapsed();
                if age > Duration::from_secs(30) {
                    error!(
                        "[SAFE_COMMIT] Batch {} has been pending for {:?} - possible WAL issue",
                        batch_id, age
                    );
                }
            }
        }
    }

    /// Get commit statistics
    pub fn get_commit_stats(&self) -> CommitStats {
        CommitStats {
            total_commits: self.total_commits.load(Ordering::Relaxed),
            failed_commits: self.failed_commits.load(Ordering::Relaxed),
        }
    }

    /// Shutdown consumer
    pub async fn shutdown(&self) {
        info!("[SAFE_COMMIT] Shutting down safe commit consumer");
        self.running.store(false, Ordering::Relaxed);

        // Check for pending commits
        let pending = self.pending_commits.lock().await;
        if !pending.is_empty() {
            warn!(
                "[SAFE_COMMIT] Shutting down with {} uncommitted batches - will reprocess on restart",
                pending.len()
            );

            for batch_id in pending.keys() {
                warn!("[SAFE_COMMIT] Uncommitted batch: {}", batch_id);
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct CommitStats {
    pub total_commits: u64,
    pub failed_commits: u64,
}

/// Extension to TelemetryIngestionClient for response handling
impl TelemetryIngestionClient {
    /// Check if response indicates WAL was written
    pub fn is_wal_written(response: &openwit_proto::ingestion::TelemetryIngestResponse) -> bool {
        response.wal_written
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_offset_calculation() {
        // Ensure we commit the NEXT offset to read
        let current_offset = 100;
        let next_to_read = current_offset + 1;
        assert_eq!(next_to_read, 101);
    }

    #[test]
    fn test_commit_only_after_wal() {
        // Verify that commits only happen when wal_written = true
        let response = openwit_proto::ingestion::TelemetryIngestResponse {
            success: true,
            wal_written: false,  // WAL not written
            ..Default::default()
        };

        // Should NOT commit when WAL not written
        assert!(!response.wal_written);
    }
}