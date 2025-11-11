use anyhow::{Result, Context};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer, CommitMode};
use rdkafka::{Message, TopicPartitionList, Offset};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use std::collections::HashMap;
use tokio::sync::RwLock;
use tokio_stream::StreamExt;
use tracing::{info, warn, error, debug};

use crate::types::{KafkaConfig, KafkaMessage};
use crate::grpc_client::TelemetryIngestionClient;
use openwit_config::UnifiedConfig;
use openwit_control_plane::ControlPlaneClient;

/// Kafka consumer with safe offset commits only after WAL confirmation
pub struct KafkaConsumerSafe {
    consumer: Arc<StreamConsumer>,
    config: KafkaConfig,
    grpc_client: Arc<TelemetryIngestionClient>,
    node_id: String,
    running: Arc<AtomicBool>,

    // Commit tracking
    last_committed_offsets: Arc<RwLock<HashMap<(String, i32), i64>>>,
    pending_offsets: Arc<RwLock<HashMap<String, TopicPartitionList>>>,
}

impl KafkaConsumerSafe {
    pub async fn new(
        kafka_config: KafkaConfig,
        node_id: String,
        unified_config: UnifiedConfig,
        control_plane_client: ControlPlaneClient,
    ) -> Result<Self> {
        info!("[KAFKA_SAFE] Initializing safe Kafka consumer with WAL-based commits");

        // Create Kafka consumer with auto-commit DISABLED
        let consumer: StreamConsumer = ClientConfig::new()
            .set("group.id", &kafka_config.group_id)
            .set("bootstrap.servers", &kafka_config.brokers)
            .set("enable.auto.commit", "false")  // CRITICAL: Disable auto-commit
            .set("auto.offset.reset", "latest")
            .set("session.timeout.ms", &kafka_config.session_timeout_ms.to_string())
            .set("heartbeat.interval.ms", "3000")
            .set("max.poll.interval.ms", &kafka_config.max_poll_interval_ms.to_string())
            .set("enable.partition.eof", "false")
            .set("socket.keepalive.enable", "true")
            .set("api.version.request", "true")
            .set("partition.assignment.strategy", "roundrobin")
            .create()
            .context("Failed to create Kafka consumer")?;

        // Subscribe to topics
        let topics: Vec<&str> = kafka_config.topics.iter().map(|s| s.as_str()).collect();
        consumer
            .subscribe(&topics)
            .context("Failed to subscribe to topics")?;

        info!(
            "[KAFKA_SAFE] Consumer created for topics: {:?} with commit strategy: after_wal",
            topics
        );

        // Create gRPC client
        let grpc_client = Arc::new(
            TelemetryIngestionClient::new(node_id.clone(), control_plane_client, unified_config).await?
        );

        Ok(Self {
            consumer: Arc::new(consumer),
            config: kafka_config,
            grpc_client,
            node_id,
            running: Arc::new(AtomicBool::new(false)),
            last_committed_offsets: Arc::new(RwLock::new(HashMap::new())),
            pending_offsets: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// Start consuming with safe commits
    pub async fn start(&self) -> Result<()> {
        self.running.store(true, Ordering::Relaxed);
        info!("[KAFKA_SAFE] Starting consumer with safe commit strategy");

        let mut message_stream = self.consumer.stream();
        let mut batch = Vec::new();
        let mut batch_start = Instant::now();

        while self.running.load(Ordering::Relaxed) {
            // Use timeout to flush batch periodically
            match tokio::time::timeout(
                Duration::from_millis(self.config.batch_timeout_ms),
                message_stream.next()
            ).await {
                Ok(Some(msg_result)) => {
                    match msg_result {
                        Ok(msg) => {
                            // Process message and add to batch
                            if let Some(kafka_msg) = self.process_message(msg) {
                                batch.push(kafka_msg);

                                // Send batch if it reaches size limit
                                if batch.len() >= self.config.batch_size {
                                    self.send_batch_with_safe_commit(&mut batch).await?;
                                    batch_start = Instant::now();
                                }
                            }
                        }
                        Err(e) => {
                            error!("[KAFKA_SAFE] Error consuming message: {}", e);
                        }
                    }
                }
                Ok(None) => {
                    debug!("[KAFKA_SAFE] Stream ended");
                    break;
                }
                Err(_) => {
                    // Timeout - send current batch if not empty
                    if !batch.is_empty() && batch_start.elapsed() >= Duration::from_millis(self.config.batch_timeout_ms) {
                        self.send_batch_with_safe_commit(&mut batch).await?;
                        batch_start = Instant::now();
                    }
                }
            }
        }

        info!("[KAFKA_SAFE] Consumer stopped");
        Ok(())
    }

    /// Send batch and commit ONLY after WAL confirmation
    async fn send_batch_with_safe_commit(&self, batch: &mut Vec<KafkaMessage>) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }

        let batch_id = format!("{}-{}", self.node_id, chrono::Utc::now().timestamp_millis());
        let batch_size = batch.len();
        let start = Instant::now();

        // Build offset map for this batch
        let mut offset_list = TopicPartitionList::new();
        let mut max_offsets: HashMap<(String, i32), i64> = HashMap::new();

        for msg in batch.iter() {
            let key = (msg.topic.clone(), msg.partition);
            let next_offset = msg.offset + 1;  // Commit next offset to read

            max_offsets
                .entry(key)
                .and_modify(|e| *e = (*e).max(next_offset))
                .or_insert(next_offset);
        }

        // Add to TopicPartitionList for commit
        for ((topic, partition), offset) in &max_offsets {
            offset_list.add_partition_offset(
                topic,
                *partition,
                Offset::Offset(*offset),
            )?;
        }

        debug!(
            "[KAFKA_SAFE] Sending batch {} with {} messages, will commit offsets ONLY after WAL",
            batch_id, batch_size
        );

        // Send to ingestion service
        match self.grpc_client.send_batch(batch_id.clone(), batch.drain(..).collect()).await {
            Ok(response) => {
                // CRITICAL: Check WAL status before committing
                if response.wal_written {
                    // WAL confirmed - safe to commit
                    match self.consumer.commit(&offset_list, CommitMode::Sync) {
                        Ok(_) => {
                            // Update last committed offsets
                            let mut last_committed = self.last_committed_offsets.write().await;
                            for ((topic, partition), offset) in &max_offsets {
                                last_committed.insert((topic.clone(), *partition), *offset);
                            }

                            info!(
                                "[KAFKA_SAFE] ✅ Batch {} committed after WAL confirmation ({}ms, {} msgs)",
                                batch_id,
                                start.elapsed().as_millis(),
                                batch_size
                            );

                            if response.wal_write_time_ms > 0 {
                                debug!(
                                    "[KAFKA_SAFE] WAL write took {}ms, path: {}",
                                    response.wal_write_time_ms,
                                    response.wal_path
                                );
                            }
                        }
                        Err(e) => {
                            error!("[KAFKA_SAFE] Failed to commit offsets: {}", e);
                            return Err(anyhow::anyhow!("Offset commit failed: {}", e));
                        }
                    }
                } else {
                    // WAL NOT written - DO NOT commit
                    warn!(
                        "[KAFKA_SAFE] ⚠️ Batch {} NOT committed - WAL not written! Will reprocess on failure",
                        batch_id
                    );

                    // Store as pending
                    let mut pending = self.pending_offsets.write().await;
                    pending.insert(batch_id.clone(), offset_list);
                }
            }
            Err(e) => {
                error!(
                    "[KAFKA_SAFE] ❌ Batch {} failed: {} - NOT committing offsets",
                    batch_id, e
                );
                return Err(e);
            }
        }

        Ok(())
    }

    /// Process a single Kafka message
    fn process_message(&self, msg: impl Message) -> Option<KafkaMessage> {
        let topic = msg.topic();
        let partition = msg.partition();
        let offset = msg.offset();
        let timestamp = msg.timestamp().to_millis().unwrap_or(0);

        let payload = msg.payload()?;

        Some(KafkaMessage {
            id: format!("kafka:{}:{}:{}", topic, partition, offset),
            timestamp: chrono::DateTime::from_timestamp_millis(timestamp)
                .unwrap_or_else(|| chrono::Utc::now()),
            topic: topic.to_string(),
            partition,
            offset,
            key: msg.key().map(|k| k.to_vec()),
            payload: bytes::Bytes::from(payload.to_vec()),
            headers: HashMap::new(),
            payload_json: String::from_utf8_lossy(payload).to_string(),
            payload_type: "auto".to_string(),
            size_bytes: payload.len(),
            is_zero_copy: false,
        })
    }

    /// Get last committed offsets for monitoring
    pub async fn get_last_committed_offsets(&self) -> HashMap<(String, i32), i64> {
        self.last_committed_offsets.read().await.clone()
    }

    /// Check for pending offsets
    pub async fn get_pending_offsets_count(&self) -> usize {
        self.pending_offsets.read().await.len()
    }

    /// Stop the consumer
    pub fn stop(&self) {
        info!("[KAFKA_SAFE] Stopping consumer");
        self.running.store(false, Ordering::Relaxed);
    }
}

impl Drop for KafkaConsumerSafe {
    fn drop(&mut self) {
        self.stop();
    }
}