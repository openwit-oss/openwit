use anyhow::{Result, Context};
use rdkafka::consumer::{Consumer, StreamConsumer, CommitMode};
use rdkafka::Message;
use rdkafka::TopicPartitionList;
use tracing::{info, warn, error, debug};
use std::time::Instant;

/// Safe Kafka consumer that only commits after successful WAL write
pub struct SafeKafkaConsumer {
    consumer: StreamConsumer,
    node_id: String,
    commit_mode: OffsetCommitMode,
}

#[derive(Debug, Clone)]
pub enum OffsetCommitMode {
    /// Commit immediately after sending (CURRENT - RISKY!)
    Immediate,

    /// Commit only after WAL write confirmation (SAFE)
    AfterWAL,

    /// Commit only after storage confirmation (SAFEST but slowest)
    AfterStorage,
}

impl SafeKafkaConsumer {
    pub fn new(consumer: StreamConsumer, node_id: String, commit_mode: OffsetCommitMode) -> Self {
        info!("Creating SafeKafkaConsumer with commit mode: {:?}", commit_mode);
        Self {
            consumer,
            node_id,
            commit_mode,
        }
    }

    /// Process batch and commit ONLY after confirmation
    pub async fn process_batch_with_safe_commit(
        &self,
        batch_id: String,
        messages: Vec<KafkaMessage>,
        grpc_client: &TelemetryIngestionClient,
    ) -> Result<()> {
        let start = Instant::now();
        let message_count = messages.len();

        // Store offset information BEFORE processing
        let mut offset_commits = TopicPartitionList::new();
        for msg in &messages {
            offset_commits.add_partition_offset(
                &msg.topic,
                msg.partition,
                rdkafka::Offset::Offset(msg.offset + 1), // Next offset to read
            )?;
        }

        debug!(
            "[SAFE_CONSUMER] Processing batch {} with {} messages",
            batch_id, message_count
        );

        // Send to ingestion service
        match grpc_client.send_batch(batch_id.clone(), messages).await {
            Ok(response) => {
                // Check if WAL write was successful
                if response.wal_written {
                    match self.commit_mode {
                        OffsetCommitMode::AfterWAL => {
                            // Commit now - WAL write confirmed
                            self.commit_offsets(&offset_commits, &batch_id).await?;
                            info!(
                                "[SAFE_CONSUMER] Batch {} committed after WAL write ({:?})",
                                batch_id,
                                start.elapsed()
                            );
                        }
                        OffsetCommitMode::AfterStorage => {
                            // Wait for storage confirmation
                            if response.storage_written {
                                self.commit_offsets(&offset_commits, &batch_id).await?;
                                info!(
                                    "[SAFE_CONSUMER] Batch {} committed after storage write ({:?})",
                                    batch_id,
                                    start.elapsed()
                                );
                            } else {
                                warn!(
                                    "[SAFE_CONSUMER] Batch {} WAL written but storage pending - not committing",
                                    batch_id
                                );
                            }
                        }
                        OffsetCommitMode::Immediate => {
                            // Old behavior - risky!
                            self.commit_offsets(&offset_commits, &batch_id).await?;
                            warn!(
                                "[SAFE_CONSUMER] Batch {} committed immediately (RISKY!)",
                                batch_id
                            );
                        }
                    }
                } else {
                    error!(
                        "[SAFE_CONSUMER] Batch {} failed WAL write - NOT committing offsets!",
                        batch_id
                    );
                    return Err(anyhow::anyhow!("WAL write failed"));
                }
            }
            Err(e) => {
                error!(
                    "[SAFE_CONSUMER] Batch {} failed to send - NOT committing offsets: {}",
                    batch_id, e
                );
                return Err(e);
            }
        }

        Ok(())
    }

    /// Commit specific offsets
    async fn commit_offsets(
        &self,
        offsets: &TopicPartitionList,
        batch_id: &str,
    ) -> Result<()> {
        match self.consumer.commit(offsets, CommitMode::Sync) {
            Ok(_) => {
                debug!("[SAFE_CONSUMER] Offsets committed for batch {}", batch_id);
                Ok(())
            }
            Err(e) => {
                error!(
                    "[SAFE_CONSUMER] Failed to commit offsets for batch {}: {}",
                    batch_id, e
                );
                Err(anyhow::anyhow!("Offset commit failed: {}", e))
            }
        }
    }

    /// Get current commit mode
    pub fn commit_mode(&self) -> &OffsetCommitMode {
        &self.commit_mode
    }
}

/// Response from ingestion service with WAL/storage status
pub struct IngestionResponse {
    pub success: bool,
    pub wal_written: bool,
    pub storage_written: bool,
    pub message: String,
}

// Mock types for compilation
pub struct KafkaMessage {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
}

pub struct TelemetryIngestionClient;

impl TelemetryIngestionClient {
    pub async fn send_batch(
        &self,
        _batch_id: String,
        _messages: Vec<KafkaMessage>,
    ) -> Result<IngestionResponse> {
        // Mock implementation
        Ok(IngestionResponse {
            success: true,
            wal_written: true,
            storage_written: false,
            message: "Batch written to WAL".to_string(),
        })
    }
}