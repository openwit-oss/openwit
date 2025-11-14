//! Flush worker pool for parallel batch processing
//!
//! This module provides a worker pool that processes batches in parallel.
//! Workers receive batches from a queue and forward them to the ingestion pipeline.
//!
//! Architecture:
//! ```text
//! Batcher → flush_batch() → Queue → Worker Pool → WAL Write → Ingestion Pipeline
//!                                    (5 workers)
//! ```

use anyhow::Result;
use openwit_ingestion::IngestedMessage;
use openwit_postgres::BatchTracker;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::wal::{WalManager, BatchStatus};

/// A batch ready to be processed by workers
#[derive(Debug)]
pub struct ReadyBatch {
    /// Unique batch identifier
    pub id: String,
    /// Messages in this batch
    pub messages: Vec<IngestedMessage>,
    /// Total size in bytes
    pub total_bytes: usize,
    /// When this batch was created
    pub created_at: Instant,
    /// Flush reason (size, timeout, shutdown)
    pub reason: String,
    /// Index ID for categorizing batches (optional)
    pub index_id: Option<String>,
}

/// Configuration for flush worker pool
#[derive(Debug, Clone)]
pub struct FlushPoolConfig {
    /// Number of parallel workers
    pub worker_count: usize,
    /// Queue capacity (max batches waiting)
    pub queue_capacity: usize,
}

impl Default for FlushPoolConfig {
    fn default() -> Self {
        Self {
            worker_count: 5,
            queue_capacity: 1000,
        }
    }
}

/// Flush worker pool - processes batches in parallel
pub struct FlushWorkerPool {
    /// Channels to send batches to individual workers (one per worker)
    worker_channels: Vec<mpsc::Sender<ReadyBatch>>,

    /// Worker task handles
    workers: Vec<JoinHandle<()>>,

    /// Round-robin counter for load balancing
    round_robin: Arc<AtomicUsize>,

    /// Configuration
    config: FlushPoolConfig,
}

impl FlushWorkerPool {
    /// Create new flush worker pool
    pub fn new(
        config: FlushPoolConfig,
        ingest_tx: mpsc::Sender<IngestedMessage>,
        wal_manager: Arc<WalManager>,
        batch_tracker: Option<Arc<BatchTracker>>,
    ) -> Arc<Self> {
        tracing::info!(
            "Creating flush worker pool: {} workers, queue capacity: {}, WAL: {:?}, Batch tracking: {}",
            config.worker_count,
            config.queue_capacity,
            wal_manager.wal_directory(),
            if batch_tracker.is_some() { "enabled" } else { "disabled" }
        );

        let mut workers = Vec::new();
        let mut worker_channels = Vec::new();

        // Create a separate channel for each worker
        for worker_id in 0..config.worker_count {
            let (batch_tx, batch_rx) = mpsc::channel::<ReadyBatch>(config.queue_capacity);
            worker_channels.push(batch_tx);

            let tx = ingest_tx.clone();
            let wal = wal_manager.clone();
            let tracker = batch_tracker.clone();

            let handle = tokio::spawn(async move {
                worker_task(worker_id, batch_rx, tx, wal, tracker).await;
            });

            workers.push(handle);
            tracing::debug!("Flush worker {} spawned", worker_id);
        }

        Arc::new(Self {
            worker_channels,
            workers,
            round_robin: Arc::new(AtomicUsize::new(0)),
            config,
        })
    }

    /// Submit batch for processing (non-blocking)
    pub async fn submit_batch(&self, batch: ReadyBatch) -> Result<()> {
        tracing::debug!(
            "Submitting batch {} ({} messages) to worker pool",
            batch.id,
            batch.messages.len()
        );

        // Round-robin load balancing across workers
        let worker_idx = self.round_robin.fetch_add(1, Ordering::Relaxed) % self.config.worker_count;

        self.worker_channels[worker_idx]
            .send(batch)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to queue batch: {}", e))
    }

    /// Get worker count
    pub fn worker_count(&self) -> usize {
        self.config.worker_count
    }

    /// Shutdown worker pool gracefully
    pub async fn shutdown(self) -> Result<()> {
        tracing::info!("Shutting down flush worker pool...");

        // Close all worker channels
        drop(self.worker_channels);

        // Wait for all workers to finish
        for (i, handle) in self.workers.into_iter().enumerate() {
            tracing::debug!("Waiting for worker {} to finish", i);
            handle.await?;
        }

        tracing::info!("All flush workers terminated");
        Ok(())
    }
}

/// Worker task - processes batches from queue
async fn worker_task(
    worker_id: usize,
    mut batch_rx: mpsc::Receiver<ReadyBatch>,
    ingest_tx: mpsc::Sender<IngestedMessage>,
    wal_manager: Arc<WalManager>,
    batch_tracker: Option<Arc<BatchTracker>>,
) {
    tracing::info!("Flush worker {} started", worker_id);

    let mut batches_processed = 0;

    while let Some(batch) = batch_rx.recv().await {
        let batch_start = Instant::now();
        let batch_id = batch.id.clone();
        let message_count = batch.messages.len();
        let total_bytes = batch.total_bytes;
        let flush_reason = batch.reason.clone();
        let index_id = batch.index_id.clone();

        if let Some(ref idx) = index_id {
            tracing::info!(
                "Worker {} processing batch {} for index_id={} - {} messages ({} bytes, reason: {})",
                worker_id,
                batch_id,
                idx,
                message_count,
                total_bytes,
                flush_reason
            );
        } else {
            tracing::info!(
                "Worker {} processing batch {} (default) - {} messages ({} bytes, reason: {})",
                worker_id,
                batch_id,
                message_count,
                total_bytes,
                flush_reason
            );
        }

        // Track batch creation in PostgreSQL (non-blocking background task)
        if let Some(ref tracker) = batch_tracker {
            let tracker_clone = tracker.clone();
            let batch_id_clone = batch_id.clone();
            let index_id_clone = index_id.clone();
            let message_count_clone = message_count;
            let total_bytes_clone = total_bytes;

            tokio::spawn(async move {
                // Extract peer address from first message if available
                let peer_address = None; // TODO: Extract from message source if needed
                let telemetry_type = Some("grpc".to_string());

                if let Err(e) = tracker_clone.create_batch(
                    batch_id_clone.clone(),
                    "grpc".to_string(),
                    index_id_clone.clone().unwrap_or_else(|| "default".to_string()),
                    total_bytes_clone as i32,
                    message_count_clone as i32,
                    None, // first_offset (Kafka only)
                    None, // last_offset (Kafka only)
                    vec![], // topics (Kafka only)
                    peer_address,
                    telemetry_type,
                ).await {
                    tracing::warn!("Failed to create batch record in tracker for {}: {}", batch_id_clone, e);
                }
            });
        }

        // STEP 1: Write batch to WAL before processing (with index_id for directory structure)
        match wal_manager.write_batch_with_index(&batch_id, batch.messages.clone(), total_bytes, &flush_reason, &index_id).await {
            Ok(_) => {
                if let Some(ref idx) = index_id {
                    tracing::debug!(
                        "Worker {} wrote batch {} to WAL for index_id={} ({} messages)",
                        worker_id,
                        batch_id,
                        idx,
                        message_count
                    );
                } else {
                    tracing::debug!(
                        "Worker {} wrote batch {} to WAL (default) ({} messages)",
                        worker_id,
                        batch_id,
                        message_count
                    );
                }

                // Update batch tracker - WAL pipeline completed (non-blocking background task)
                if let Some(ref tracker) = batch_tracker {
                    let tracker_clone = tracker.clone();
                    let batch_id_clone = batch_id.clone();
                    tokio::spawn(async move {
                        if let Err(e) = tracker_clone.update_wal_completed(&batch_id_clone).await {
                            tracing::warn!("Failed to update WAL status in batch tracker for {}: {}", batch_id_clone, e);
                        }
                    });
                }
            }
            Err(e) => {
                tracing::error!(
                    "Worker {} FAILED to write batch {} to WAL: {}",
                    worker_id,
                    batch_id,
                    e
                );
                // Skip processing if WAL write failed - data is not persisted
                continue;
            }
        }

        // STEP 2: Process all messages in batch
        let mut sent_count = 0;
        let mut processing_failed = false;

        for message in batch.messages {
            match ingest_tx.send(message).await {
                Ok(_) => {
                    sent_count += 1;
                }
                Err(e) => {
                    tracing::error!(
                        "Worker {} failed to send message from batch {}: {}",
                        worker_id,
                        batch_id,
                        e
                    );
                    processing_failed = true;
                    break;
                }
            }
        }

        let duration = batch_start.elapsed();

        // STEP 3: Update batch status (WAL cleanup handled by janitor)
        if !processing_failed && sent_count == message_count {
            // Success: Mark as completed
            match wal_manager.update_batch_status_with_index(&batch_id, BatchStatus::Completed, &index_id).await {
                Ok(_) => {
                    if let Some(ref idx) = index_id {
                        tracing::info!(
                            "Worker {} completed batch {} in {:?} ({} messages, {} total batches) - WAL marked completed for index_id={}",
                            worker_id,
                            batch_id,
                            duration,
                            sent_count,
                            batches_processed + 1,
                            idx
                        );
                    } else {
                        tracing::info!(
                            "Worker {} completed batch {} in {:?} ({} messages, {} total batches) - WAL marked completed",
                            worker_id,
                            batch_id,
                            duration,
                            sent_count,
                            batches_processed + 1
                        );
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        "Worker {} completed batch {} but failed to update WAL status: {}",
                        worker_id,
                        batch_id,
                        e
                    );
                }
            }
            batches_processed += 1;
        } else {
            // Failure: Mark as failed
            match wal_manager.update_batch_status_with_index(&batch_id, BatchStatus::Failed, &index_id).await {
                Ok(_) => {
                    if let Some(ref idx) = index_id {
                        tracing::error!(
                            "Worker {} FAILED batch {} for index_id={} - {}/{} messages sent - WAL marked failed",
                            worker_id,
                            batch_id,
                            idx,
                            sent_count,
                            message_count
                        );
                    } else {
                        tracing::error!(
                            "Worker {} FAILED batch {} - {}/{} messages sent - WAL marked failed at {:?}",
                            worker_id,
                            batch_id,
                            sent_count,
                            message_count,
                            wal_manager.wal_directory().join(format!("{}.wal", batch_id))
                        );
                    }
                }
                Err(e) => {
                    tracing::error!(
                        "Worker {} FAILED batch {} and failed to update WAL status: {}",
                        worker_id,
                        batch_id,
                        e
                    );
                }
            }
        }
    }

    tracing::info!(
        "Flush worker {} terminated (processed {} batches)",
        worker_id,
        batches_processed
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use openwit_ingestion::types::{MessagePayload, MessageSource};

    #[tokio::test]
    async fn test_flush_pool_creation() {
        let (ingest_tx, _ingest_rx) = mpsc::channel(100);

        let temp_dir = tempfile::TempDir::new().unwrap();
        let wal_manager = Arc::new(WalManager::new(temp_dir.path()).await.unwrap());

        let config = FlushPoolConfig {
            worker_count: 3,
            queue_capacity: 10,
        };

        let pool = FlushWorkerPool::new(config, ingest_tx, wal_manager, None);

        assert_eq!(pool.worker_count(), 3);
    }

    #[tokio::test]
    async fn test_batch_processing() {
        let (ingest_tx, mut ingest_rx) = mpsc::channel(100);

        let temp_dir = tempfile::TempDir::new().unwrap();
        let wal_manager = Arc::new(WalManager::new(temp_dir.path()).await.unwrap());

        let config = FlushPoolConfig {
            worker_count: 2,
            queue_capacity: 10,
        };

        let pool = FlushWorkerPool::new(config, ingest_tx, wal_manager, None);

        // Create test batch
        let batch = ReadyBatch {
            id: "test-batch-1".to_string(),
            messages: vec![
                IngestedMessage {
                    id: "msg-1".to_string(),
                    received_at: 12345,
                    size_bytes: 100,
                    source: MessageSource::Grpc {
                        peer_addr: "127.0.0.1:50051".to_string(),
                    },
                    payload: MessagePayload::Trace(vec![1, 2, 3]),
                    index_name: None,
                },
            ],
            total_bytes: 100,
            created_at: Instant::now(),
            reason: "test".to_string(),
            index_id: None,
        };

        // Submit batch
        pool.submit_batch(batch).await.unwrap();

        // Verify message received
        let msg = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            ingest_rx.recv()
        )
        .await
        .expect("Timeout waiting for message")
        .expect("Channel closed");

        assert_eq!(msg.id, "msg-1");
    }
}
