use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use anyhow::{Result, Context};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer, CommitMode};
use rdkafka::consumer::DefaultConsumerContext;
use rdkafka::util::DefaultRuntime;
use rdkafka::Message;
use rdkafka::message::Headers;
use tokio::time::interval;
use tokio_stream::StreamExt;
use tracing::{info, warn, error, debug, trace};

use crate::types::{KafkaConfig, KafkaMessage, TopicIndexConfig};
use crate::grpc_client::TelemetryIngestionClient;
use openwit_metrics::{
    record_ingestion,
    INGESTION_MESSAGES_TOTAL,
    KAFKA_MESSAGES_CONSUMED,
};

/// Extract client name from topic based on configuration
fn extract_client_from_topic(topic: &str, config: &Option<TopicIndexConfig>) -> String {
    if let Some(index_config) = config {
        let client_config = &index_config.client_name_config;
        let parts: Vec<&str> = topic.split(&client_config.topic_separator).collect();
        
        if client_config.kafka_client_position < parts.len() {
            return parts[client_config.kafka_client_position].to_string();
        }
    }
    
    // Fallback: use first part of topic split by "."
    let parts: Vec<&str> = topic.split('.').collect();
    parts.get(0).unwrap_or(&"unknown_client").to_string()
}

/// Extract index name from topic based on configuration
fn extract_index_from_topic(topic: &str, config: &Option<TopicIndexConfig>) -> String {
    let parts: Vec<&str> = topic.split('.').collect();
    
    if let Some(index_config) = config {
        // Check if any pattern matches
        for pattern in &index_config.patterns {
            if topic_matches_pattern(topic, &pattern.pattern) {
                if let Some(index) = extract_by_position(&parts, pattern.index_position, pattern.index_prefix.as_deref()) {
                    return index;
                }
            }
        }
        
        // Use default position if no pattern matches
        if let Some(index) = extract_by_position(&parts, index_config.default_index_position, None) {
            return index;
        }
        
        // Auto-generate if enabled
        if index_config.auto_generate.enabled {
            return generate_index_name(topic, &index_config.auto_generate.prefix, &index_config.auto_generate.strategy);
        }
    }

    warn!("Unable to extract index from topic: {}, using 'default'", topic);
    "default".to_string()
}

/// Check if topic matches a glob-like pattern
fn topic_matches_pattern(topic: &str, pattern: &str) -> bool {
    // Convert glob pattern to regex
    let regex_pattern = pattern
        .replace(".", "\\.")
        .replace("*", ".*");
    
    match regex::Regex::new(&format!("^{}$", regex_pattern)) {
        Ok(re) => re.is_match(topic),
        Err(_) => false,
    }
}

/// Extract index from topic parts by position
fn extract_by_position(parts: &[&str], position: usize, prefix: Option<&str>) -> Option<String> {
    if position < parts.len() {
        let index = parts[position].to_string();
        if let Some(prefix) = prefix {
            Some(format!("{}{}", prefix, index))
        } else {
            Some(index)
        }
    } else {
        None
    }
}

/// Generate an index name based on strategy
fn generate_index_name(topic: &str, prefix: &str, strategy: &str) -> String {
    match strategy {
        "hash" => {
            let mut hasher = DefaultHasher::new();
            topic.hash(&mut hasher);
            format!("{}{:x}", prefix, hasher.finish())
        },
        "timestamp" => {
            format!("{}{}", prefix, chrono::Utc::now().timestamp_millis())
        },
        "sequential" => {
            // In production, this would need a counter service
            format!("{}seq_{}", prefix, chrono::Utc::now().timestamp())
        },
        _ => format!("{}{}", prefix, "unknown"),
    }
}

/// Kafka consumer for OpenWit ingestion
pub struct KafkaConsumer {
    node_id: String,
    consumer: Arc<StreamConsumer<DefaultConsumerContext, DefaultRuntime>>,
    config: KafkaConfig,
    grpc_client: Arc<TelemetryIngestionClient>,
    running: Arc<AtomicBool>,
}

impl KafkaConsumer {
    pub async fn new(
        node_id: String,
        config: KafkaConfig,
        grpc_client: Arc<TelemetryIngestionClient>,
    ) -> Result<Self> {
        info!("Creating Kafka consumer for node: {}", node_id);
        info!("  Brokers: {}", config.brokers);
        info!("  Group ID: {}", config.group_id);
        info!("  Topics: {:?}", config.topics);
        
        // With streaming, batch size is less critical but still affects memory usage
        if config.batch_size > 10000 {
            warn!("⚠️ Configured batch_size {} is very large. Consider reducing for better memory usage.", config.batch_size);
            info!("ℹ️ Using gRPC streaming - no message size limits!");
        }
        
        let consumer: StreamConsumer<DefaultConsumerContext, DefaultRuntime> = ClientConfig::new()
            .set("client.id", &node_id)
            .set("group.id", &config.group_id)
            .set("bootstrap.servers", &config.brokers)
            .set("enable.auto.commit", if config.enable_auto_commit { "true" } else { "false" })
            .set("auto.offset.reset", "latest")
            .set("session.timeout.ms", &config.session_timeout_ms.to_string())
            .set("partition.assignment.strategy", "roundrobin")
            .set("heartbeat.interval.ms", "3000")
            .set("max.poll.interval.ms", "300000")
            .set("heartbeat.interval.ms", "3000")
            .set("max.poll.interval.ms", &config.max_poll_interval_ms.to_string())
            .set("enable.partition.eof", "false")
            .set("fetch.min.bytes", "1024")
            .set("fetch.wait.max.ms", "500")
            .create()
            .context("Failed to create Kafka consumer")?;
        
        // Subscribe to topics
        // For pattern matching, we need to fetch metadata and subscribe to matching topics
        if config.topics.iter().any(|t| t.contains('*')) {
            info!("Detected topic patterns, fetching metadata to find matching topics...");
            
            // Get metadata to list all topics
            let metadata = consumer.fetch_metadata(None, Duration::from_secs(5))
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
                return Err(anyhow::anyhow!("No topics found matching patterns: {:?}", config.topics));
            }
            
            let topic_refs: Vec<&str> = matching_topics.iter().map(|s| s.as_str()).collect();
            consumer.subscribe(&topic_refs)
                .context("Failed to subscribe to topics")?;
        } else {
            // Use regular subscription for exact topic names
            let topics: Vec<&str> = config.topics.iter().map(|s| s.as_str()).collect();
            consumer.subscribe(&topics)
                .context("Failed to subscribe to topics")?;
        }
        
        Ok(Self {
            node_id,
            consumer: Arc::new(consumer),
            config,
            grpc_client,
            running: Arc::new(AtomicBool::new(false)),
        })
    }
    
    /// Wait for ingestion nodes to be available before starting
    async fn wait_for_ingestion_nodes(&self) -> Result<()> {
        info!("Waiting for healthy ingestion nodes before starting Kafka consumer...");
        
        let mut retry_count = 0;
        loop {
            if self.has_healthy_ingestion_nodes().await {
                info!("Found healthy ingestion nodes! Starting Kafka consumer.");
                return Ok(());
            }
            
            retry_count += 1;
            if retry_count == 1 {
                warn!("No healthy ingestion nodes available. Kafka consumer will wait until ingestion nodes are available.");
            } else if retry_count % 12 == 0 { // Log every minute
                warn!("Still waiting for healthy ingestion nodes ({}s elapsed). Kafka consumer not started.", 
                     retry_count * 5);
            }
            
            // Check if we should stop waiting
            if !self.running.load(Ordering::Relaxed) {
                return Err(anyhow::anyhow!("Consumer stopped while waiting for ingestion nodes"));
            }
            
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    }

    /// Start consuming messages
    pub async fn start(self: Arc<Self>) -> Result<()> {
        if self.running.swap(true, Ordering::SeqCst) {
            warn!("Kafka consumer already running");
            return Ok(());
        }
        
        // Wait for ingestion nodes before starting
        self.wait_for_ingestion_nodes().await?;
        
        info!("Starting Kafka consumer");
        info!("📋 Configuration: batch_size={}, commit_interval={}ms", 
            self.config.batch_size, 
            self.config.commit_interval_ms
        );
        
        // Start consumer task
        let consumer_task = self.clone();
        tokio::spawn(async move {
            if let Err(e) = consumer_task.consume_loop().await {
                error!("Kafka consumer error: {}", e);
            }
        });
        
        // Start commit task if auto-commit is disabled
        if !self.config.enable_auto_commit {
            let commit_task = self.clone();
            tokio::spawn(async move {
                commit_task.commit_loop().await;
            });
        }
        
        // Start metrics reporting task
        let metrics_task = self.clone();
        tokio::spawn(async move {
            metrics_task.report_metrics().await;
        });
        
        Ok(())
    }
    
    /// Stop the consumer
    pub async fn stop(&self) {
        info!("Stopping Kafka consumer");
        self.running.store(false, Ordering::SeqCst);
    }
    
    /// Check if ingestion nodes are available
    async fn has_healthy_ingestion_nodes(&self) -> bool {
        match self.grpc_client.get_healthy_ingestion_nodes().await {
            Ok(nodes) => !nodes.is_empty(),
            Err(_) => false,
        }
    }

    /// Main consume loop
    async fn consume_loop(&self) -> Result<()> {
        let mut stream = self.consumer.stream();
        let mut batch: Vec<KafkaMessage> = Vec::with_capacity(self.config.batch_size);
        let mut last_health_check = Instant::now();
        let health_check_interval = Duration::from_secs(30); // Check less frequently since we wait at startup
        
        while self.running.load(Ordering::Relaxed) {
            // Periodically verify ingestion nodes are still available
            if last_health_check.elapsed() >= health_check_interval {
                let has_nodes = self.has_healthy_ingestion_nodes().await;
                last_health_check = Instant::now();
                
                if !has_nodes {
                    // If nodes disappear, stop the consumer completely
                    error!("Lost all healthy ingestion nodes! Stopping consumer to prevent message loss.");
                    self.running.store(false, Ordering::SeqCst);
                    return Err(anyhow::anyhow!("No healthy ingestion nodes available"));
                }
            }
            
            tokio::select! {
                Some(message) = stream.next() => {
                    match message {
                        Ok(msg) => {
                            if let Some(ingested_msg) = self.process_message(&msg).await {
                                batch.push(ingested_msg);
                                
                                // Check if batch is full
                                if batch.len() >= self.config.batch_size {
                                    self.send_batch(&mut batch).await;
                                }
                            }
                        }
                        Err(e) => {
                            error!("Kafka consume error: {}", e);
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
    ) -> Option<KafkaMessage> {
        let topic = msg.topic();
        let partition = msg.partition();
        let offset = msg.offset();
        
        // Get message payload
        let payload = match msg.payload() {
            Some(data) => {
                debug!("[KAFKA CONSUMER] Received payload: topic={}, partition={}, offset={}, size={} bytes", 
                      topic, partition, offset, data.len());
                data
            },
            None => {
                warn!("Empty message from Kafka topic {} partition {}", topic, partition);
                return None;
            }
        };
        
        // Start timing for metrics
        let _start_time = Instant::now();
        
        // Update local and global metrics
        
        // Update global Kafka metrics
        KAFKA_MESSAGES_CONSUMED
            .with_label_values(&[topic, &self.node_id])
            .inc();
        
        // Extract headers
        let mut headers = std::collections::HashMap::new();
        if let Some(msg_headers) = msg.headers() {
            for i in 0..msg_headers.count() {
                let header = msg_headers.get(i);
                if let Some(value) = header.value {
                    if let Ok(value_str) = std::str::from_utf8(value) {
                        headers.insert(header.key.to_string(), value_str.to_string());
                    }
                }
            }
        }
        
        // Extract index name and client name from topic based on configuration
        let index_name = extract_index_from_topic(topic, &self.config.topic_index_config);
        let client_name = extract_client_from_topic(topic, &self.config.topic_index_config);
        
        headers.insert("index_name".to_string(), index_name.clone());
        headers.insert("client_name".to_string(), client_name.clone());
        
        // Only log extraction once per topic in debug mode
        debug!("Extracted from topic '{}': index='{}', client='{}'", topic, index_name, client_name);
        
        // Parse timestamp
        let timestamp = match msg.timestamp() {
            rdkafka::Timestamp::CreateTime(ts) => ts,
            rdkafka::Timestamp::LogAppendTime(ts) => ts,
            _ => chrono::Utc::now().timestamp_millis(),
        };
        
        // Determine payload type from headers or topic name FIRST
        let payload_type = headers.get("payload_type")
            .cloned()
            .or_else(|| {
                if topic.contains("trace") { Some("trace".to_string()) }
                else if topic.contains("log") { Some("log".to_string()) }
                else if topic.contains("metric") { Some("metric".to_string()) }
                else { Some("unknown".to_string()) }
            })
            .unwrap_or_else(|| "trace".to_string());
        
        // Convert payload to JSON string
        // First check if this is OTLP protobuf data
        let payload_json = if payload_type == "trace" || payload_type == "metric" || payload_type == "log" {
            // This is likely OTLP protobuf data, not JSON
            debug!("[KAFKA CONSUMER] Processing {} message: size={} bytes", payload_type, payload.len());
            
            // Always encode as base64 for transport
            use base64::Engine;
            base64::engine::general_purpose::STANDARD.encode(payload)
        } else {
            // Try to parse as UTF-8 string first, otherwise base64 encode
            match std::str::from_utf8(payload) {
                Ok(s) => {
                    debug!("[KAFKA CONSUMER] UTF-8 message: size={} bytes", s.len());
                    if s.is_empty() {
                        warn!("[KAFKA CONSUMER] Empty payload: topic={} partition={} offset={}", 
                              topic, partition, offset);
                    }
                    s.to_string()
                },
                Err(_) => {
                    debug!("[KAFKA CONSUMER] Binary message: size={} bytes, encoding as base64", payload.len());
                    use base64::Engine;
                    base64::engine::general_purpose::STANDARD.encode(payload)
                }
            }
        };
        
        // Create Kafka message with JSON payload
        let kafka_msg = KafkaMessage {
            id: format!("kafka:{}:{}:{}", topic, partition, offset),
            timestamp: chrono::DateTime::from_timestamp_millis(timestamp)
                .unwrap_or_else(|| chrono::Utc::now()),
            topic: topic.to_string(),
            partition,
            offset,
            key: msg.key().map(|k| k.to_vec()),
            payload: bytes::Bytes::from(payload.to_vec()),
            headers: headers.clone(),
            payload_json: payload_json.clone(),
            payload_type: payload_type.clone(),
            size_bytes: payload.len(),
            is_zero_copy: false, // Standard consumer doesn't use zero-copy
        };
        
        debug!("[KAFKA CONSUMER] Message processed: type={}, size={} bytes", payload_type, kafka_msg.size_bytes);
        
        Some(kafka_msg)
    }
    
    /// Send a batch of messages to the ingestion pipeline via streaming
    async fn send_batch(&self, batch: &mut Vec<KafkaMessage>) {
        if batch.is_empty() {
            return;
        }
        
        let batch_size = batch.len();
        let start_time = Instant::now();
        let total_bytes: usize = batch.iter().map(|m| m.size_bytes).sum();
        let batch_id = format!("{}-{}", self.node_id, chrono::Utc::now().timestamp_millis());
        
        debug!("Sending batch of {} messages ({:.2} MB) to ingestion node via gRPC streaming", 
               batch_size, total_bytes as f64 / 1_048_576.0);
        
        // Send batch to ingestion node via gRPC streaming (no size limits!)
        match self.grpc_client.send_batch(batch_id.clone(), batch.drain(..).collect()).await {
            Ok(_) => {
                // Record success metrics
                record_ingestion("kafka", total_bytes, true, start_time);
                INGESTION_MESSAGES_TOTAL
                    .with_label_values(&["kafka", "success"])
                    .inc_by(batch_size as u64);
                
                // Log batch success only in debug mode
                let duration = start_time.elapsed();
                debug!("Batch sent: {} messages, {:.2} MB, {:.1}ms (batch_id: {})", 
                    batch_size,
                    total_bytes as f64 / 1_048_576.0,
                    duration.as_secs_f64() * 1000.0,
                    batch_id
                );
            }
            Err(e) => {
                error!("Failed to send batch {} ({} messages, {:.2} MB): {}", 
                    batch_id,
                    batch_size,
                    total_bytes as f64 / 1_048_576.0,
                    e
                );
                INGESTION_MESSAGES_TOTAL
                    .with_label_values(&["kafka", "error"])
                    .inc_by(batch_size as u64);
                // Note: Messages are lost here. In production, you might want to implement
                // a retry queue or dead letter topic
            }
        }
    }
    
    /// Periodically commit offsets
    async fn commit_loop(&self) {
        let mut interval = interval(Duration::from_millis(self.config.commit_interval_ms));
        
        while self.running.load(Ordering::Relaxed) {
            interval.tick().await;
            
            // Only commit if we've processed messages since last commit
            if !self.consumer.assignment().unwrap_or_default().elements().is_empty() {
                match self.consumer.commit_consumer_state(CommitMode::Async) {
                    Ok(_) => {
                        // Only log in trace level to reduce noise
                        trace!("Committed Kafka offsets");
                    }
                    Err(e) => {
                        // Only log actual errors, not "no offset to commit" errors
                        if !e.to_string().contains("no offset to commit") {
                            error!("Failed to commit Kafka offsets: {}", e);
                        }
                    }
                }
            }
        }
    }
    
    /// Report metrics periodically
    async fn report_metrics(&self) {
        let mut interval = interval(Duration::from_secs(60));
        
        while self.running.load(Ordering::Relaxed) {
            interval.tick().await;
            
            // Metrics reporting removed
        }
    }
}