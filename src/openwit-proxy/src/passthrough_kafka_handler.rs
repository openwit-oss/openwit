use std::sync::Arc;
use anyhow::{Result, Context};
use rdkafka::{
    consumer::{StreamConsumer, Consumer},
    producer::{FutureProducer, FutureRecord},
    Message,
    config::ClientConfig,
};
use tokio::sync::RwLock;
use tracing::{info, debug, warn, error};
use futures::StreamExt;

use crate::control_plane_client::{ProxyControlPlaneClient, KafkaEndpoint};

/// Pass-through Kafka handler that routes messages directly without buffering
pub struct PassthroughKafkaHandler {
    node_id: String,
    control_client: Arc<ProxyControlPlaneClient>,
    /// Kafka consumer for receiving messages
    consumer: Arc<StreamConsumer>,
    /// Producer pool for sending to healthy Kafka brokers
    producer_pool: Arc<RwLock<ProducerPool>>,
    /// Statistics
    stats: Arc<RwLock<HandlerStats>>,
    /// Configuration
    config: KafkaProxyConfig,
}

#[derive(Clone)]
pub struct KafkaProxyConfig {
    /// Consumer group ID
    pub group_id: String,
    /// Topics to consume from
    pub topics: Vec<String>,
    /// Output topic prefix (e.g., "processed-")
    pub output_topic_prefix: String,
    /// Whether to preserve original topic names
    pub preserve_topic_names: bool,
    /// Max message size
    pub max_message_bytes: usize,
}

struct ProducerPool {
    /// Producers keyed by broker endpoint
    producers: std::collections::HashMap<String, FutureProducer>,
    /// Round-robin counter
    counter: usize,
}

#[derive(Debug, Default)]
struct HandlerStats {
    messages_consumed: u64,
    messages_produced: u64,
    messages_failed: u64,
    bytes_processed: u64,
}

impl PassthroughKafkaHandler {
    pub async fn new(
        node_id: String,
        control_client: Arc<ProxyControlPlaneClient>,
        config: KafkaProxyConfig,
    ) -> Result<Self> {
        // Get Kafka brokers from control client config
        let kafka_brokers = control_client.get_kafka_brokers().await
            .unwrap_or_else(|_| "localhost:9092".to_string());
            
        info!("Creating Kafka consumer with brokers: {}", kafka_brokers);
        
        // Create consumer
        let consumer: StreamConsumer = ClientConfig::new()
            .set("group.id", &config.group_id)
            .set("bootstrap.servers", &kafka_brokers)
            .set("enable.auto.commit", "true")
            .set("auto.commit.interval.ms", "5000")
            .set("session.timeout.ms", "30000")
            .set("enable.partition.eof", "false")
            .create()?;
            
        // Subscribe to topics
        // For pattern matching, we need to fetch metadata and subscribe to matching topics
        if config.topics.iter().any(|t| t.contains('*')) {
            info!("Detected topic patterns, fetching metadata to find matching topics...");
            
            // Get metadata to list all topics
            let metadata = consumer.fetch_metadata(None, std::time::Duration::from_secs(5))
                .context("Failed to fetch Kafka metadata")?;
            
            let all_topics: Vec<String> = metadata.topics()
                .iter()
                .map(|t| t.name().to_string())
                .collect();
            
            info!("Found {} topics in cluster", all_topics.len());
            
            // Match topics against patterns
            let mut matching_topics = Vec::new();
            for pattern in &config.topics {
                if pattern.contains('*') {
                    // Convert glob pattern to regex
                    let regex_pattern = format!("^{}$", 
                        pattern
                            .replace(".", "\\.")
                            .replace("*", ".*"));
                    
                    let re = regex::Regex::new(&regex_pattern)
                        .context("Invalid topic pattern")?;
                    
                    for topic in &all_topics {
                        if re.is_match(topic) {
                            matching_topics.push(topic.clone());
                        }
                    }
                } else {
                    matching_topics.push(pattern.clone());
                }
            }
            
            // Remove duplicates
            matching_topics.sort();
            matching_topics.dedup();
            
            info!("Subscribing to {} topics matching patterns: {:?}", 
                matching_topics.len(), 
                matching_topics.iter().take(10).collect::<Vec<_>>());
            
            if matching_topics.is_empty() {
                warn!("No topics found matching patterns: {:?}", config.topics);
                // Subscribe to patterns directly as fallback
                let topics: Vec<&str> = config.topics.iter().map(|s| s.as_str()).collect();
                consumer.subscribe(&topics)?;
            } else {
                let topic_refs: Vec<&str> = matching_topics.iter().map(|s| s.as_str()).collect();
                consumer.subscribe(&topic_refs)?;
            }
        } else {
            // Use regular subscription for exact topic names
            let topics: Vec<&str> = config.topics.iter().map(|s| s.as_str()).collect();
            consumer.subscribe(&topics)?;
            info!("Kafka consumer subscribed to topics: {:?}", config.topics);
        }
        
        Ok(Self {
            node_id,
            control_client,
            consumer: Arc::new(consumer),
            producer_pool: Arc::new(RwLock::new(ProducerPool {
                producers: std::collections::HashMap::new(),
                counter: 0,
            })),
            stats: Arc::new(RwLock::new(HandlerStats::default())),
            config,
        })
    }
    
    /// Start consuming and proxying messages
    pub async fn start(self: Arc<Self>) -> Result<()> {
        info!("Starting Kafka pass-through proxy");
        
        // Start producer pool maintenance
        let self_clone = self.clone();
        tokio::spawn(async move {
            self_clone.maintain_producer_pool().await;
        });
        
        // Start consuming messages
        let mut message_stream = self.consumer.stream();
        
        while let Some(message_result) = message_stream.next().await {
            match message_result {
                Ok(message) => {
                    if let Err(e) = self.process_message(message).await {
                        error!("Failed to process message: {}", e);
                        
                        let mut stats = self.stats.write().await;
                        stats.messages_failed += 1;
                    }
                }
                Err(e) => {
                    error!("Kafka consumer error: {}", e);
                }
            }
        }
        
        Ok(())
    }
    
    /// Process a single Kafka message
    async fn process_message(&self, message: rdkafka::message::BorrowedMessage<'_>) -> Result<()> {
        // Update stats
        {
            let mut stats = self.stats.write().await;
            stats.messages_consumed += 1;
            if let Some(payload) = message.payload() {
                stats.bytes_processed += payload.len() as u64;
            }
        }
        
        // Get message details
        let topic = message.topic();
        let partition = message.partition();
        let offset = message.offset();
        let key = message.key();
        let payload = message.payload();
        
        debug!("Processing message from {}:{} @ offset {}", topic, partition, offset);
        
        // Determine output topic
        let output_topic = if self.config.preserve_topic_names {
            topic.to_string()
        } else {
            format!("{}{}", self.config.output_topic_prefix, topic)
        };
        
        // Get healthy Kafka brokers
        let brokers = self.control_client.get_healthy_kafka_brokers().await?;
        if brokers.is_empty() {
            return Err(anyhow::anyhow!("No healthy Kafka brokers available"));
        }
        
        // Select producer from pool
        let producer = self.get_producer(&brokers).await?;
        
        // Create future record
        let mut record = FutureRecord::to(&output_topic)
            .partition(partition);
            
        if let Some(key) = key {
            record = record.key(key);
        }
        
        if let Some(payload) = payload {
            if payload.len() > self.config.max_message_bytes {
                warn!("Message size {} exceeds limit {}, skipping", 
                    payload.len(), self.config.max_message_bytes);
                return Ok(());
            }
            record = record.payload(payload);
        }
        
        // Add headers for tracing
        record = record.headers(rdkafka::message::OwnedHeaders::new()
            .insert(rdkafka::message::Header {
                key: "x-proxy-node",
                value: Some(self.node_id.as_bytes()),
            })
            .insert(rdkafka::message::Header {
                key: "x-proxy-timestamp",
                value: Some(chrono::Utc::now().to_rfc3339().as_bytes()),
            }));
        
        // Send message
        match producer.send(record, std::time::Duration::from_secs(10)).await {
            Ok((partition, offset)) => {
                debug!("Message sent to {}:{} @ offset {}", output_topic, partition, offset);
                
                let mut stats = self.stats.write().await;
                stats.messages_produced += 1;
            }
            Err((e, _)) => {
                error!("Failed to send message: {}", e);
                
                let mut stats = self.stats.write().await;
                stats.messages_failed += 1;
                
                return Err(anyhow::anyhow!("Producer error: {}", e));
            }
        }
        
        Ok(())
    }
    
    /// Get or create a producer for healthy brokers
    async fn get_producer(&self, brokers: &[KafkaEndpoint]) -> Result<FutureProducer> {
        let mut pool = self.producer_pool.write().await;
        
        // Round-robin select broker
        let broker = &brokers[pool.counter % brokers.len()];
        pool.counter += 1;
        
        // Check if we have a producer for this broker
        if let Some(producer) = pool.producers.get(&broker.endpoint) {
            return Ok(producer.clone());
        }
        
        // Create new producer
        debug!("Creating producer for broker: {}", broker.endpoint);
        
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", &broker.endpoint)
            .set("message.timeout.ms", "30000")
            .set("queue.buffering.max.messages", "10000")
            .set("queue.buffering.max.kbytes", "1048576")
            .set("batch.size", "16384")
            .set("linger.ms", "10")
            .set("compression.type", "snappy")
            .create()?;
            
        pool.producers.insert(broker.endpoint.clone(), producer.clone());
        
        Ok(producer)
    }
    
    /// Maintain producer pool - remove producers for unhealthy brokers
    async fn maintain_producer_pool(&self) {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
        
        loop {
            interval.tick().await;
            
            match self.control_client.get_healthy_kafka_brokers().await {
                Ok(healthy_brokers) => {
                    let healthy_endpoints: std::collections::HashSet<_> = 
                        healthy_brokers.iter().map(|b| &b.endpoint).collect();
                    
                    let mut pool = self.producer_pool.write().await;
                    pool.producers.retain(|endpoint, _| {
                        let keep = healthy_endpoints.contains(endpoint);
                        if !keep {
                            info!("Removing producer for unhealthy broker: {}", endpoint);
                        }
                        keep
                    });
                    
                    debug!("Producer pool maintenance: {} producers active", pool.producers.len());
                }
                Err(e) => {
                    warn!("Failed to get healthy brokers for pool maintenance: {}", e);
                }
            }
        }
    }
    
    /// Get handler statistics
    pub async fn get_stats(&self) -> serde_json::Value {
        let stats = self.stats.read().await;
        let brokers = self.control_client.get_healthy_kafka_brokers().await
            .unwrap_or_default();
        let pool = self.producer_pool.read().await;
        
        serde_json::json!({
            "node_id": self.node_id,
            "proxy_type": "kafka_passthrough",
            "config": {
                "group_id": self.config.group_id,
                "topics": self.config.topics,
                "output_topic_prefix": self.config.output_topic_prefix,
            },
            "stats": {
                "messages_consumed": stats.messages_consumed,
                "messages_produced": stats.messages_produced,
                "messages_failed": stats.messages_failed,
                "bytes_processed": stats.bytes_processed,
                "success_rate": if stats.messages_consumed > 0 {
                    ((stats.messages_produced as f64) / (stats.messages_consumed as f64)) * 100.0
                } else {
                    0.0
                },
            },
            "brokers": {
                "healthy_count": brokers.len(),
                "producer_pool_size": pool.producers.len(),
            },
        })
    }
    
    /// Get the total number of messages consumed
    pub async fn get_messages_consumed(&self) -> u64 {
        self.stats.read().await.messages_consumed
    }
}

/// Builder for Kafka proxy configuration
impl KafkaProxyConfig {
    pub fn builder() -> KafkaProxyConfigBuilder {
        KafkaProxyConfigBuilder::default()
    }
}

#[derive(Default)]
pub struct KafkaProxyConfigBuilder {
    group_id: Option<String>,
    topics: Vec<String>,
    output_topic_prefix: String,
    preserve_topic_names: bool,
    max_message_bytes: usize,
}

impl KafkaProxyConfigBuilder {
    pub fn group_id(mut self, id: String) -> Self {
        self.group_id = Some(id);
        self
    }
    
    pub fn topics(mut self, topics: Vec<String>) -> Self {
        self.topics = topics;
        self
    }
    
    pub fn add_topic(mut self, topic: String) -> Self {
        self.topics.push(topic);
        self
    }
    
    pub fn output_topic_prefix(mut self, prefix: String) -> Self {
        self.output_topic_prefix = prefix;
        self
    }
    
    pub fn preserve_topic_names(mut self, preserve: bool) -> Self {
        self.preserve_topic_names = preserve;
        self
    }
    
    pub fn max_message_bytes(mut self, max_bytes: usize) -> Self {
        self.max_message_bytes = max_bytes;
        self
    }
    
    pub fn build(self) -> Result<KafkaProxyConfig> {
        Ok(KafkaProxyConfig {
            group_id: self.group_id.ok_or_else(|| anyhow::anyhow!("group_id is required"))?,
            topics: if self.topics.is_empty() {
                return Err(anyhow::anyhow!("At least one topic is required"));
            } else {
                self.topics
            },
            output_topic_prefix: self.output_topic_prefix,
            preserve_topic_names: self.preserve_topic_names,
            max_message_bytes: if self.max_message_bytes == 0 {
                1024 * 1024 * 10 // 10MB default
            } else {
                self.max_message_bytes
            },
        })
    }
}