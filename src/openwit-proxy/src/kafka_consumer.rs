use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};
use std::time::Duration;
use anyhow::{Result, Context};
use bytes::Bytes;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer, CommitMode};
use rdkafka::consumer::DefaultConsumerContext;
use rdkafka::util::DefaultRuntime;
use rdkafka::Message;
use tokio::time::interval;
use tokio_stream::StreamExt;
use tracing::{info, warn, error, debug};

use openwit_config::unified::IngestionConfig;
use crate::proxy_buffer::{ProxyBuffer, ProxyMessage, MessageSource};
use crate::round_robin::RoundRobinRouter;

/// Kafka consumer for proxy node
#[allow(dead_code)]
pub struct ProxyKafkaConsumer {
    node_id: String,
    consumer: Arc<StreamConsumer<DefaultConsumerContext, DefaultRuntime>>,
    buffer: Arc<ProxyBuffer>,
    router: Arc<RoundRobinRouter>,
    config: KafkaConsumerConfig,
    metrics: Arc<ConsumerMetrics>,
    running: Arc<AtomicBool>,
}

#[derive(Clone)]
pub struct KafkaConsumerConfig {
    pub brokers: String,
    pub group_id: String,
    pub topics: Vec<String>,
    pub batch_size: usize,
    pub commit_interval_ms: u64,
}

impl From<&IngestionConfig> for KafkaConsumerConfig {
    fn from(config: &IngestionConfig) -> Self {
        Self {
            brokers: config.kafka.brokers.clone().unwrap_or_else(|| "localhost:9092".to_string()),
            group_id: config.kafka.group_id.clone().unwrap_or_else(|| "openwit-proxy".to_string()),
            topics: config.kafka.topics.clone(),
            batch_size: 1000, // Default batch size
            commit_interval_ms: 5000, // Default 5 seconds
        }
    }
}

pub struct ConsumerMetrics {
    pub messages_consumed: AtomicU64,
    pub bytes_consumed: AtomicU64,
    pub batches_sent: AtomicU64,
    pub commit_count: AtomicU64,
    pub error_count: AtomicU64,
}

impl ProxyKafkaConsumer {
    pub async fn new(
        node_id: String,
        config: KafkaConsumerConfig,
        buffer: Arc<ProxyBuffer>,
        router: Arc<RoundRobinRouter>,
    ) -> Result<Self> {
        info!("Creating Kafka consumer for proxy node: {}", node_id);
        info!("  Brokers: {}", config.brokers);
        info!("  Group ID: {}", config.group_id);
        info!("  Topics: {:?}", config.topics);
        
        let consumer: StreamConsumer<DefaultConsumerContext, DefaultRuntime> = ClientConfig::new()
            .set("group.id", &config.group_id)
            .set("bootstrap.servers", &config.brokers)
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "latest")
            .set("session.timeout.ms", "30000")
            .set("heartbeat.interval.ms", "3000")
            .set("max.poll.interval.ms", "300000")
            .set("enable.partition.eof", "false")
            .set("fetch.min.bytes", "1024")
            .set("fetch.wait.max.ms", "500")
            .create()
            .context("Failed to create Kafka consumer")?;
        
        // Subscribe to topics
        let topics: Vec<&str> = config.topics.iter().map(|s| s.as_str()).collect();
        consumer.subscribe(&topics)
            .context("Failed to subscribe to topics")?;
        
        Ok(Self {
            node_id,
            consumer: Arc::new(consumer),
            buffer,
            router,
            config,
            metrics: Arc::new(ConsumerMetrics {
                messages_consumed: AtomicU64::new(0),
                bytes_consumed: AtomicU64::new(0),
                batches_sent: AtomicU64::new(0),
                commit_count: AtomicU64::new(0),
                error_count: AtomicU64::new(0),
            }),
            running: Arc::new(AtomicBool::new(false)),
        })
    }
    
    /// Start consuming messages
    pub async fn start(self: Arc<Self>) -> Result<()> {
        if self.running.swap(true, Ordering::SeqCst) {
            warn!("Kafka consumer already running");
            return Ok(());
        }
        
        info!("Starting Kafka consumer for proxy");
        
        // Start consumer task
        let consumer_task = self.clone();
        tokio::spawn(async move {
            if let Err(e) = consumer_task.consume_loop().await {
                error!("Kafka consumer error: {}", e);
            }
        });
        
        // Start commit task
        let commit_task = self.clone();
        tokio::spawn(async move {
            commit_task.commit_loop().await;
        });
        
        Ok(())
    }
    
    /// Main consume loop
    async fn consume_loop(&self) -> Result<()> {
        let mut stream = self.consumer.stream();
        let mut batch = Vec::with_capacity(self.config.batch_size);
        
        while self.running.load(Ordering::Relaxed) {
            tokio::select! {
                Some(message) = stream.next() => {
                    match message {
                        Ok(msg) => {
                            if let Some(proxy_msg) = self.process_message(&msg).await {
                                batch.push(proxy_msg);
                                
                                // Check if batch is full
                                if batch.len() >= self.config.batch_size {
                                    self.send_batch(&mut batch).await;
                                }
                            }
                        }
                        Err(e) => {
                            error!("Kafka consume error: {}", e);
                            self.metrics.error_count.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
                _ = tokio::time::sleep(Duration::from_millis(100)) => {
                    // Timeout - send partial batch if any
                    if !batch.is_empty() {
                        self.send_batch(&mut batch).await;
                    }
                }
            }
        }
        
        // Send final batch
        if !batch.is_empty() {
            self.send_batch(&mut batch).await;
        }
        
        Ok(())
    }
    
    /// Process a single Kafka message
    async fn process_message(
        &self,
        msg: &rdkafka::message::BorrowedMessage<'_>,
    ) -> Option<ProxyMessage> {
        let topic = msg.topic();
        let partition = msg.partition();
        let offset = msg.offset();
        
        // Get message payload
        let payload = match msg.payload() {
            Some(data) => data,
            None => {
                warn!("Empty message from Kafka topic {} partition {}", topic, partition);
                return None;
            }
        };
        
        // Update metrics
        self.metrics.messages_consumed.fetch_add(1, Ordering::Relaxed);
        self.metrics.bytes_consumed.fetch_add(payload.len() as u64, Ordering::Relaxed);
        
        // Create proxy message
        let proxy_msg = ProxyMessage {
            id: uuid::Uuid::new_v4().to_string(),
            timestamp: msg.timestamp()
                .to_millis()
                .map(|ms| chrono::DateTime::from_timestamp_millis(ms))
                .flatten()
                .unwrap_or_else(chrono::Utc::now),
            source: MessageSource::Kafka {
                topic: topic.to_string(),
                partition,
                offset,
            },
            data: Bytes::copy_from_slice(payload),
            metadata: {
                let mut m = std::collections::HashMap::new();
                if let Some(key) = msg.key() {
                    if let Ok(key_str) = std::str::from_utf8(key) {
                        m.insert("kafka_key".to_string(), key_str.to_string());
                    }
                }
                m.insert("kafka_topic".to_string(), topic.to_string());
                m.insert("kafka_partition".to_string(), partition.to_string());
                m.insert("kafka_offset".to_string(), offset.to_string());
                m
            },
        };
        
        debug!("Processed Kafka message: topic={}, partition={}, offset={}, size={}", 
               topic, partition, offset, payload.len());
        
        Some(proxy_msg)
    }
    
    /// Send batch to router
    async fn send_batch(&self, batch: &mut Vec<ProxyMessage>) {
        if batch.is_empty() {
            return;
        }
        
        let batch_size = batch.len();
        let messages = std::mem::take(batch);
        
        debug!("Sending batch of {} messages to router", batch_size);
        
        // Add to buffer which will trigger routing
        if let Err(e) = self.buffer.add_messages(messages).await {
            error!("Failed to buffer Kafka messages: {}", e);
            self.metrics.error_count.fetch_add(1, Ordering::Relaxed);
        } else {
            self.metrics.batches_sent.fetch_add(1, Ordering::Relaxed);
        }
    }
    
    /// Periodic commit loop
    async fn commit_loop(&self) {
        let mut ticker = interval(Duration::from_millis(self.config.commit_interval_ms));
        
        while self.running.load(Ordering::Relaxed) {
            ticker.tick().await;
            
            if let Err(e) = self.consumer.commit_consumer_state(CommitMode::Async) {
                error!("Failed to commit Kafka offsets: {}", e);
                self.metrics.error_count.fetch_add(1, Ordering::Relaxed);
            } else {
                debug!("Committed Kafka offsets");
                self.metrics.commit_count.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
    
    /// Stop the consumer
    pub async fn stop(&self) {
        info!("Stopping Kafka consumer");
        self.running.store(false, Ordering::SeqCst);
        
        // Final commit
        if let Err(e) = self.consumer.commit_consumer_state(CommitMode::Sync) {
            error!("Failed to commit final offsets: {}", e);
        }
    }
    
    /// Get consumer metrics
    pub fn get_metrics(&self) -> ConsumerMetricsSnapshot {
        ConsumerMetricsSnapshot {
            messages_consumed: self.metrics.messages_consumed.load(Ordering::Relaxed),
            bytes_consumed: self.metrics.bytes_consumed.load(Ordering::Relaxed),
            batches_sent: self.metrics.batches_sent.load(Ordering::Relaxed),
            commit_count: self.metrics.commit_count.load(Ordering::Relaxed),
            error_count: self.metrics.error_count.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ConsumerMetricsSnapshot {
    pub messages_consumed: u64,
    pub bytes_consumed: u64,
    pub batches_sent: u64,
    pub commit_count: u64,
    pub error_count: u64,
}