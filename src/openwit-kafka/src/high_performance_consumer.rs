use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use std::collections::HashMap;
use std::hash::{Hash, Hasher, DefaultHasher};
use anyhow::{Result, Context};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::Message;
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tracing::{info, warn, error, debug};
use uuid::Uuid;

use crate::types::{KafkaMessage, TopicIndexConfig};
use crate::grpc_client::TelemetryIngestionClient;
use crate::client_extractor::ClientExtractor;
use crate::client_batch_manager::{ClientBatchManager, CompletedBatch};
use crate::batch_tracker::BatchTracker;
use crate::wal_writer::WalWriter;
use openwit_metrics::{
    INGESTION_MESSAGES_TOTAL,
    KAFKA_MESSAGES_CONSUMED,
};

/// High-performance Kafka consumer optimized for 1M messages/sec
pub struct HighPerformanceKafkaConsumer {
    node_id: String,
    config: HighPerfKafkaConfig,
    grpc_clients: Vec<Arc<TelemetryIngestionClient>>,
    running: Arc<AtomicBool>,
    messages_processed: Arc<AtomicU64>,
    bytes_processed: Arc<AtomicU64>,
    pod_index: Option<usize>,  // Pod index for static partition assignment
    total_pods: Option<usize>,  // Total number of pods
    // Client batching components
    client_batch_manager: Option<Arc<ClientBatchManager>>,
    batch_tracker: Option<Arc<BatchTracker>>,
    wal_writer: Option<Arc<WalWriter>>,
}

/// Optimized configuration for high throughput
#[derive(Clone, Debug)]
pub struct HighPerfKafkaConfig {
    pub brokers: String,
    pub group_id: String,
    pub topics: Vec<String>,
    
    // Performance tuning
    pub num_consumers: usize,              // Number of parallel consumers (default: 8)
    pub batch_size: usize,                 // Messages per batch (default: 10000)
    pub batch_timeout_ms: u64,             // Max time to wait for batch (default: 10ms)
    pub num_grpc_clients: usize,           // Number of gRPC client connections (default: 4)
    pub processing_threads: usize,         // Number of message processing threads (default: 16)
    pub channel_buffer_size: usize,        // Size of internal channels (default: 100000)
    
    // Kafka tuning
    pub fetch_min_bytes: usize,            // Min bytes per fetch (default: 1MB)
    pub fetch_max_bytes: usize,            // Max bytes per fetch (default: 50MB)
    pub fetch_max_wait_ms: u64,            // Max wait for fetch (default: 5ms)
    pub max_partition_fetch_bytes: usize,  // Max bytes per partition (default: 10MB)
    
    // Memory optimization
    pub enable_zero_copy: bool,            // Use zero-copy for large messages
    pub zero_copy_threshold_bytes: usize,  // Threshold for zero-copy (default: 100KB)
    pub preallocate_buffers: bool,         // Preallocate message buffers
    pub buffer_pool_size: usize,           // Size of buffer pool
    
    // Performance limits
    pub max_batch_bytes: usize,            // Maximum batch size in bytes
    
    // Standard Kafka settings
    pub commit_interval_ms: u64,
    pub enable_auto_commit: bool,
    pub session_timeout_ms: u64,
    pub max_poll_interval_ms: u64,
    
    // Topic and index configuration
    pub topic_index_config: Option<TopicIndexConfig>,
    
    // Static partition assignment
    pub enable_static_assignment: bool,     // Enable static partition assignment
    pub pod_index: Option<usize>,           // This pod's index (0-based)
    pub total_pods: Option<usize>,          // Total number of pods
}

impl Default for HighPerfKafkaConfig {
    fn default() -> Self {
        Self {
            brokers: "localhost:9092".to_string(),
            group_id: "openwit-kafka-hp".to_string(),
            topics: vec![],
            
            // Optimized for 1M msgs/sec
            num_consumers: 8,
            batch_size: 10000,
            batch_timeout_ms: 10,
            num_grpc_clients: 4,
            processing_threads: 16,
            channel_buffer_size: 100000,
            
            // Kafka performance
            fetch_min_bytes: 1024 * 1024,         // 1MB
            fetch_max_bytes: 50 * 1024 * 1024,    // 50MB
            fetch_max_wait_ms: 5,
            max_partition_fetch_bytes: 10 * 1024 * 1024, // 10MB
            
            // Memory optimization
            enable_zero_copy: true,
            zero_copy_threshold_bytes: 100 * 1024,  // 100KB
            preallocate_buffers: true,
            buffer_pool_size: 1000,
            
            // Performance limits
            max_batch_bytes: 15_728_640,  // 15MB
            
            // Standard settings
            commit_interval_ms: 1000,
            enable_auto_commit: false,
            session_timeout_ms: 30000,
            max_poll_interval_ms: 300000,
            
            // Topic configuration
            topic_index_config: None,
            
            // Static partition assignment
            enable_static_assignment: false,
            pod_index: None,
            total_pods: None,
        }
    }
}

impl HighPerformanceKafkaConsumer {
    pub async fn new(
        node_id: String,
        config: HighPerfKafkaConfig,
        unified_config: &openwit_config::UnifiedConfig,
    ) -> Result<Self> {
        info!("Creating high-performance Kafka consumer for node: {}", node_id);
        info!("  Brokers: {}", config.brokers);
        info!("  Topics: {:?}", config.topics);
        info!("  Parallel consumers: {}", config.num_consumers);
        info!("  Batch size: {}", config.batch_size);
        info!("  Processing threads: {}", config.processing_threads);
        
        // Create control plane client
        let control_plane_client = openwit_control_plane::ControlPlaneClient::new(
            &node_id,
            unified_config
        ).await?;
        
        // Create multiple gRPC clients for load distribution
        let mut grpc_clients = Vec::new();
        for i in 0..config.num_grpc_clients {
            let client = TelemetryIngestionClient::new(
                format!("{}-grpc-{}", node_id, i),
                control_plane_client.clone(),
                unified_config.clone(),
            ).await?;
            grpc_clients.push(Arc::new(client));
        }
        
        // Always use static partition assignment to prevent rebalancing
        // Extract pod index more intelligently
        let pod_index = if config.pod_index.is_some() {
            config.pod_index
        } else {
            // Use the full node_id as-is for static member ID
            // Don't try to extract numbers - it causes confusion
            None
        };
        
        // Don't set total_pods - let Kafka handle distribution dynamically
        let total_pods = config.total_pods;
        
        info!("Node ID '{}' mapped to pod index: {:?}", node_id, pod_index);
        
        if pod_index.is_some() && total_pods.is_some() {
            info!("Static partition assignment enabled: pod {} of {} total pods", 
                 pod_index.unwrap(), total_pods.unwrap());
        };
        
        // Initialize client batching components if enabled
        let (client_batch_manager, batch_tracker, wal_writer) = 
            if unified_config.ingestion.batch_tracker.enabled {
                info!("Initializing client batching system");
                
                // Initialize batch tracker
                info!("Initializing batch tracker with PostgreSQL...");
                let postgres_url = &unified_config.ingestion.batch_tracker.postgres_url;
                
                // Check if we can use an environment variable override
                let final_postgres_url = match std::env::var("OPENWIT_POSTGRES_URL") {
                    Ok(env_url) => {
                        info!("Using PostgreSQL URL from OPENWIT_POSTGRES_URL environment variable");
                        env_url
                    }
                    Err(_) => {
                        info!("Using PostgreSQL URL from config file: {}", postgres_url);
                        postgres_url.clone()
                    }
                };
                
                let batch_tracker = Arc::new(
                    BatchTracker::new(&final_postgres_url)
                        .await
                        .context("Failed to create batch tracker")?
                );
                
                // Initialize WAL writer
                let wal_writer = Arc::new(
                    WalWriter::new(&unified_config.ingestion.batch_tracker.wal_directory)
                        .context("Failed to create WAL writer")?
                );
                
                // Initialize client batch manager
                let batch_config = unified_config.ingestion.batch_tracker.batch_config.clone();
                // Convert from config TopicIndexConfig to types TopicIndexConfig
                let topic_index_config = Some(crate::types::TopicIndexConfig::from(unified_config.ingestion.kafka.topic_index_config.clone()));
                let client_batch_manager = Arc::new(
                    ClientBatchManager::new(batch_config, topic_index_config, batch_tracker.clone())
                        .await
                        .context("Failed to create client batch manager")?
                );
                
                (Some(client_batch_manager), Some(batch_tracker), Some(wal_writer))
            } else {
                info!("Client batching disabled");
                (None, None, None)
            };
        
        Ok(Self {
            node_id,
            config,
            grpc_clients,
            running: Arc::new(AtomicBool::new(false)),
            messages_processed: Arc::new(AtomicU64::new(0)),
            bytes_processed: Arc::new(AtomicU64::new(0)),
            pod_index,
            total_pods,
            client_batch_manager,
            batch_tracker,
            wal_writer,
        })
    }
    
    /// Extract client name from topic based on configuration
    fn extract_client_from_topic(topic: &str, config: &Option<TopicIndexConfig>) -> String {
        let client = ClientExtractor::extract_client_from_topic(topic, config.as_ref());
        
        // Log client discovery with configuration
        ClientExtractor::log_client_discovery(topic, &client, config.as_ref());
        
        // Return normalized client ID
        ClientExtractor::normalize_client_id(&client)
    }
    
    /// Extract index name from topic based on configuration
    fn extract_index_from_topic(topic: &str, config: &Option<TopicIndexConfig>) -> String {
        let parts: Vec<&str> = topic.split('.').collect();
        
        if let Some(index_config) = config {
            // Check if any pattern matches
            for pattern in &index_config.patterns {
                if Self::topic_matches_pattern(topic, &pattern.pattern) {
                    if let Some(index) = Self::extract_by_position(&parts, pattern.index_position, pattern.index_prefix.as_deref()) {
                        return index;
                    }
                }
            }
            
            // Use default position if no pattern matches
            if let Some(index) = Self::extract_by_position(&parts, index_config.default_index_position, None) {
                return index;
            }
            
            // Auto-generate if enabled
            if index_config.auto_generate.enabled {
                return Self::generate_index_name(topic, &index_config.auto_generate.prefix, &index_config.auto_generate.strategy);
            }
        }
        
        // Fallback: use topic name directly or extract from it
        if parts.len() > 1 {
            parts[0].to_string()
        } else {
            topic.to_string()
        }
    }
    
    /// Check if topic matches a glob-like pattern
    fn topic_matches_pattern(topic: &str, pattern: &str) -> bool {
        // Simple glob matching without regex for performance
        if pattern.contains('*') {
            let prefix = pattern.split('*').next().unwrap_or("");
            let suffix = pattern.split('*').last().unwrap_or("");
            topic.starts_with(prefix) && topic.ends_with(suffix)
        } else {
            topic == pattern
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
            _ => format!("{}{}", prefix, topic),
        }
    }
    
    /// Wait for all required dependencies to be available before starting
    async fn wait_for_dependencies(&self) -> Result<()> {
        info!("Waiting for required dependencies before starting Kafka consumer...");
        
        let mut retry_count = 0;
        let mut logged_ingestion = false;
        let mut logged_postgres = false;
        
        loop {
            // Check ingestion nodes
            let has_ingestion = self.has_healthy_ingestion_nodes().await;
            if has_ingestion && !logged_ingestion {
                info!("✓ Found healthy ingestion nodes");
                logged_ingestion = true;
            }
            
            // Check PostgreSQL connectivity if batch tracking is enabled
            let mut postgres_ok = true;
            if let Some(ref batch_tracker) = self.batch_tracker {
                postgres_ok = self.check_postgres_connectivity(batch_tracker).await;
                if postgres_ok && !logged_postgres {
                    info!("✓ PostgreSQL connection verified");
                    logged_postgres = true;
                }
            }
            
            // If all dependencies are ready, we can start
            if has_ingestion && postgres_ok {
                info!("All dependencies available! Starting Kafka consumer.");
                return Ok(());
            }
            
            // Log status on first attempt and periodically
            retry_count += 1;
            if retry_count == 1 {
                let mut missing = Vec::new();
                if !has_ingestion {
                    missing.push("ingestion nodes");
                }
                if !postgres_ok {
                    missing.push("PostgreSQL database");
                }
                warn!("Waiting for: {}. Kafka consumer will not start until all dependencies are available.", 
                     missing.join(", "));
            } else if retry_count % 12 == 0 { // Log every minute
                let mut missing = Vec::new();
                if !has_ingestion {
                    missing.push("ingestion nodes");
                }
                if !postgres_ok {
                    missing.push("PostgreSQL database");
                }
                warn!("Still waiting for: {} ({}s elapsed). Kafka consumer not started.", 
                     missing.join(", "), retry_count * 5);
            }
            
            // Check if we should stop waiting
            if !self.running.load(Ordering::Relaxed) {
                return Err(anyhow::anyhow!("Consumer stopped while waiting for dependencies"));
            }
            
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    }
    
    /// Check PostgreSQL connectivity
    async fn check_postgres_connectivity(&self, batch_tracker: &Arc<BatchTracker>) -> bool {
        // Try to get batch status for a non-existent batch to test connectivity
        match batch_tracker.get_batch_status(Uuid::nil()).await {
            Ok(_) => true,  // Connection works (even if no batch found)
            Err(e) => {
                debug!("PostgreSQL connectivity check failed: {}", e);
                false
            }
        }
    }

    /// Start the high-performance consumer
    pub async fn start(self: Arc<Self>) -> Result<()> {
        if self.running.swap(true, Ordering::SeqCst) {
            warn!("High-performance Kafka consumer already running");
            return Ok(());
        }
        
        // Wait for all dependencies before starting
        self.wait_for_dependencies().await?;
        
        info!("Starting high-performance Kafka consumer");
        
        // Check if we're using client batching
        if let Some(ref batch_manager) = self.client_batch_manager {
            info!("Using client-based batching system");
            
            // Create channels for Kafka messages
            let (msg_tx, mut msg_rx) = mpsc::channel::<KafkaMessage>(self.config.channel_buffer_size);
            
            // Start consumer tasks
            let num_consumers = self.config.num_consumers;
            for i in 0..num_consumers {
                let consumer = Arc::new(self.create_consumer(i).await?);
                let msg_tx = msg_tx.clone();
                let self_clone = self.clone();
                let consumer_id = i;
                
                tokio::spawn(async move {
                    if let Err(e) = self_clone.consume_task(consumer_id, consumer, msg_tx).await {
                        error!("Consumer {} error: {}", consumer_id, e);
                    }
                });
            }
            
            // Start the batch monitoring task
            let batch_manager_clone = batch_manager.clone();
            tokio::spawn(async move {
                batch_manager_clone.start_monitoring().await;
            });
            
            // Start message router that sends to client batch manager
            let batch_manager_clone = batch_manager.clone();
            tokio::spawn(async move {
                while let Some(msg) = msg_rx.recv().await {
                    // Add message to appropriate client buffer
                    if let Err(e) = batch_manager_clone.add_message(msg).await {
                        error!("Failed to add message to batch: {}", e);
                    }
                }
                info!("Message routing task completed");
            });
            
            // Start batch processor for completed batches
            let batch_receiver = batch_manager.get_batch_receiver();
            let self_clone = self.clone();
            tokio::spawn(async move {
                info!("Starting batch processing task");
                let mut batch_rx = batch_receiver.write().await;
                while let Some(completed_batch) = batch_rx.recv().await {
                    if let Err(e) = self_clone.process_completed_batch(completed_batch).await {
                        error!("Failed to process completed batch: {}", e);
                    }
                }
                info!("Batch processing task completed");
            });
            
        } else {
            // Original non-batching mode
            info!("Using direct message processing (no client batching)");
            
            let (msg_tx, msg_rx) = mpsc::channel::<KafkaMessage>(1_000_000);
            let msg_rx = Arc::new(tokio::sync::Mutex::new(msg_rx));
            
            // Start multiple consumer tasks
            let num_consumers = 8;
            for i in 0..num_consumers {
                let consumer = Arc::new(self.create_consumer(i).await?);
                
                let msg_tx = msg_tx.clone();
                let self_clone = self.clone();
                let consumer_id = i;
                
                tokio::spawn(async move {
                    if let Err(e) = self_clone.consume_task(consumer_id, consumer, msg_tx).await {
                        error!("Consumer {} error: {}", consumer_id, e);
                    }
                });
            }
            
            // Start multiple batch processing tasks
            let num_processors = 4;
            for i in 0..num_processors {
                let msg_rx = msg_rx.clone();
                let self_clone = self.clone();
                let grpc_client = self.grpc_clients[i % self.grpc_clients.len()].clone();
                
                tokio::spawn(async move {
                    self_clone.batch_processor(i, msg_rx, grpc_client).await;
                });
            }
        }
        
        // Start metrics reporting
        let self_clone = self.clone();
        tokio::spawn(async move {
            self_clone.report_performance_metrics().await;
        });
        
        
        Ok(())
    }
    
    /// Process a completed batch from ClientBatchManager
    async fn process_completed_batch(&self, batch: CompletedBatch) -> Result<()> {
        let start_time = Instant::now();
        info!(
            "Processing completed batch: id={}, client={}, messages={}, size={} bytes",
            batch.batch_id, batch.client_id, batch.message_count, batch.total_bytes
        );
        
        // Step 1: Write to WAL
        if let Some(ref wal_writer) = self.wal_writer {
            match wal_writer.write_batch(&batch).await {
                Ok(wal_path) => {
                    info!("Batch {} written to WAL: {:?}", batch.batch_id, wal_path);
                    
                    // Step 2: Update PostgreSQL - WAL completed
                    if let Some(ref tracker) = self.batch_tracker {
                        if let Err(e) = tracker.update_wal_completed(batch.batch_id).await {
                            error!("Failed to update WAL status in PostgreSQL: {}", e);
                        }
                    }
                },
                Err(e) => {
                    error!("Failed to write batch {} to WAL: {}", batch.batch_id, e);
                    
                    // Record error in tracker
                    if let Some(ref tracker) = self.batch_tracker {
                        let _ = tracker.record_error(batch.batch_id, "wal", &e.to_string()).await;
                    }
                    
                    return Err(e);
                }
            }
        }
        
        // Step 3: Send to ingestion service
        // Select gRPC client based on batch ID for load distribution
        let grpc_client_idx = (batch.batch_id.as_u128() as usize) % self.grpc_clients.len();
        let grpc_client = &self.grpc_clients[grpc_client_idx];
        
        // Send batch to ingestion service
        match grpc_client.send_batch(batch.batch_id.to_string(), batch.messages.clone()).await {
            Ok(_) => {
                info!(
                    "Batch {} sent to ingestion successfully ({} messages in {:?})",
                    batch.batch_id, batch.message_count, start_time.elapsed()
                );
                
                // Step 4: Update PostgreSQL - Kafka completed (sent to ingestion)
                if let Some(ref tracker) = self.batch_tracker {
                    if let Err(e) = tracker.update_kafka_completed(batch.batch_id).await {
                        error!("Failed to update kafka status in PostgreSQL: {}", e);
                    }
                }
                
                // Update metrics
                self.messages_processed.fetch_add(batch.message_count as u64, Ordering::Relaxed);
                self.bytes_processed.fetch_add(batch.total_bytes as u64, Ordering::Relaxed);
                
                // Per-client metrics
                KAFKA_MESSAGES_CONSUMED
                    .with_label_values(&[&batch.client_id, &self.node_id])
                    .inc_by(batch.message_count as u64);
            },
            Err(e) => {
                error!("Failed to send batch {} to ingestion: {}", batch.batch_id, e);
                
                // Record error in tracker
                if let Some(ref tracker) = self.batch_tracker {
                    let _ = tracker.record_error(batch.batch_id, "ingestion", &e.to_string()).await;
                }
                
                return Err(e.into());
            }
        }
        
        Ok(())
    }
    
    /// Create a Kafka consumer with optimized settings
    async fn create_consumer(&self, consumer_id: usize) -> Result<StreamConsumer> {
        // Use the same group ID for all pods but with static membership
        let group_id = self.config.group_id.clone();
        
        // Create static member ID based on node_id
        // This ensures consistent IDs even with complex node names
        let static_member_id = format!("{}-consumer-{}", self.node_id, consumer_id);
        
        info!("Creating consumer {} with static member ID: {}", consumer_id, static_member_id);
        
        let consumer: StreamConsumer = ClientConfig::new()
            .set("client.id", &format!("{}-{}", self.node_id, consumer_id))
            .set("group.id", &group_id)
            .set("bootstrap.servers", &self.config.brokers)
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")  // Read from beginning if no committed offsets
            
            // CRITICAL: Set partition assignment strategy to round-robin
            .set("partition.assignment.strategy", "roundrobin")  // Ensures even distribution
            
            // Static membership configuration
            .set("group.instance.id", &static_member_id)  // Static member ID
            .set("session.timeout.ms", "45000")  // 45 seconds - balanced timeout
            
            // Performance optimizations
            .set("fetch.min.bytes", "1048576")              // 1MB minimum fetch
            .set("fetch.wait.max.ms", "5")                  // Low latency fetch
            .set("max.partition.fetch.bytes", "10485760")   // 10MB per partition
            .set("queued.min.messages", "100000")            // Large internal queue
            .set("queued.max.messages.kbytes", "1048576")   // 1GB queue
            .set("socket.receive.buffer.bytes", "10485760")  // 10MB socket buffer
            .set("socket.send.buffer.bytes", "10485760")     // 10MB socket buffer
            
            // Session management - using defaults from static membership above
            .set("heartbeat.interval.ms", "3000")  // 3 seconds heartbeat
            .set("max.poll.interval.ms", &self.config.max_poll_interval_ms.to_string())
            
            // Note: compression.type and batch.size are producer-only properties
            // Consumers automatically decompress based on message headers
            
            .create()
            .context("Failed to create high-performance Kafka consumer")?;
            
        
        // Subscribe to topics
        // Check if any topic contains wildcards
        if self.config.topics.iter().any(|t| t.contains('*')) {
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
            for pattern in &self.config.topics {
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
                return Err(anyhow::anyhow!("No topics found matching patterns: {:?}", self.config.topics));
            }
            
            let topic_refs: Vec<&str> = matching_topics.iter().map(|s| s.as_str()).collect();
            consumer.subscribe(&topic_refs)
                .context("Failed to subscribe to topics")?;
        } else {
            // Use direct subscription for exact topic names
            consumer.subscribe(&self.config.topics.iter().map(|s| s.as_str()).collect::<Vec<_>>())
                .context("Failed to subscribe to topics")?;
        }
        
        info!("Consumer {} subscribed successfully. Static member ID: {}", consumer_id, static_member_id);
        let topics_info = if self.config.topics.iter().any(|t| t.contains('*')) { 
            "pattern-based".to_string() 
        } else { 
            self.config.topics.len().to_string() 
        };
        info!("Consumer group: {}, Topics count: {}", group_id, topics_info);
            
        Ok(consumer)
    }
    
    /// Check if ingestion nodes are available
    async fn has_healthy_ingestion_nodes(&self) -> bool {
        // Check if any gRPC client can find healthy nodes
        if let Some(client) = self.grpc_clients.first() {
            // Use the internal method from grpc_client to check node availability
            match client.get_healthy_ingestion_nodes().await {
                Ok(nodes) => !nodes.is_empty(),
                Err(_) => false,
            }
        } else {
            false
        }
    }

    /// Consumer task that reads from Kafka and sends to processing channel
    async fn consume_task(
        &self,
        consumer_id: usize,
        consumer: Arc<StreamConsumer>,
        msg_tx: mpsc::Sender<KafkaMessage>,
    ) -> Result<()> {
        let mut stream = consumer.stream();
        let mut local_buffer = Vec::with_capacity(1000);
        let mut last_flush = Instant::now();
        let mut last_health_check = Instant::now();
        let health_check_interval = Duration::from_secs(30); // Check less frequently since we wait at startup
        // No more periodic commits - offsets will be committed after WAL confirmation
        
        while self.running.load(Ordering::Relaxed) {
            // Periodically verify ingestion nodes are still available
            if last_health_check.elapsed() >= health_check_interval {
                let has_nodes = self.has_healthy_ingestion_nodes().await;
                last_health_check = Instant::now();
                
                if !has_nodes {
                    // If nodes disappear, stop the consumer completely
                    error!("Lost all healthy ingestion nodes! Stopping consumer {} to prevent message loss.", consumer_id);
                    self.running.store(false, Ordering::SeqCst);
                    return Err(anyhow::anyhow!("No healthy ingestion nodes available"));
                }
            }
            
            // Try to read multiple messages at once
            let deadline = Instant::now() + Duration::from_millis(5);
            
            while Instant::now() < deadline && local_buffer.len() < 1000 {
                match stream.next().await {
                    Some(Ok(msg)) => {
                        if let Some(mut kafka_msg) = self.process_message_fast(&msg).await {
                            // Add consumer ID to headers for offset tracking
                            kafka_msg.headers.insert("consumer_id".to_string(), format!("consumer-{}", consumer_id));
                            local_buffer.push(kafka_msg);
                            // Do NOT store or commit offset here - wait for WAL confirmation
                        }
                    }
                    Some(Err(e)) => {
                        // Only log topic errors once, not repeatedly
                        match e {
                            rdkafka::error::KafkaError::MessageConsumption(
                                rdkafka::error::RDKafkaErrorCode::UnknownTopicOrPartition
                            ) => {
                                // Topic doesn't exist - log once per consumer
                                static LOGGED: std::sync::Once = std::sync::Once::new();
                                LOGGED.call_once(|| {
                                    error!("Topics not found in Kafka. Please create topics or check configuration.");
                                });
                            }
                            _ => {
                                error!("Kafka consume error: {}", e);
                            }
                        }
                    }
                    None => break, // No more messages available
                }
            }
            
            // Send buffered messages with better backpressure handling
            if !local_buffer.is_empty() && 
               (local_buffer.len() >= 100 || last_flush.elapsed() > Duration::from_millis(10)) {
                
                // Track send failures to estimate channel pressure
                let mut send_failures = 0u32;
                let mut _messages_sent = 0u32;
                
                // Send messages with proper error handling
                for msg in local_buffer.drain(..) {
                    match msg_tx.try_send(msg.clone()) {
                        Ok(_) => {
                            _messages_sent += 1;
                        },
                        Err(mpsc::error::TrySendError::Full(msg)) => {
                            send_failures += 1;
                            
                            // Apply backpressure based on failure rate
                            if send_failures == 1 {
                                // First failure, brief pause
                                warn!("Channel full, applying backpressure");
                                tokio::time::sleep(Duration::from_millis(1)).await;
                            } else if send_failures > 5 {
                                // Many failures, channel is very full
                                warn!("Channel consistently full ({}+ failures), using blocking send", send_failures);
                                tokio::time::sleep(Duration::from_millis(10)).await;
                            }
                            
                            // Try blocking send
                            if let Err(e) = msg_tx.send(msg).await {
                                error!("Failed to send message: {}", e);
                                break;
                            }
                            _messages_sent += 1;
                        },
                        Err(mpsc::error::TrySendError::Closed(_)) => {
                            error!("Channel closed, stopping consumer");
                            return Ok(());
                        }
                    }
                }
                
                last_flush = Instant::now();
            }
            
            // No periodic commits - offsets committed only after WAL confirmation
            
            // Prevent busy waiting
            if local_buffer.is_empty() {
                tokio::time::sleep(Duration::from_micros(100)).await;
            }
        }
        
        // No final commit - only commit after WAL confirmation
        
        Ok(())
    }
    
    /// Fast message processing with minimal allocations
    async fn process_message_fast(
        &self,
        msg: &rdkafka::message::BorrowedMessage<'_>,
    ) -> Option<KafkaMessage> {
        let payload = msg.payload()?;
        
        // Skip empty messages
        if payload.is_empty() {
            debug!("Skipping empty message from topic {} partition {} offset {}", 
                  msg.topic(), msg.partition(), msg.offset());
            return None;
        }
        
        let payload_size = payload.len();
        let use_zero_copy = self.config.enable_zero_copy && payload_size >= self.config.zero_copy_threshold_bytes;
        
        // Extract common fields
        let msg_id = format!("k:{}:{}:{}", msg.topic(), msg.partition(), msg.offset());
        let timestamp = chrono::Utc::now();
        let topic = msg.topic();
        
        // Create headers
        let mut headers = HashMap::new();
        let index_name = Self::extract_index_from_topic(topic, &self.config.topic_index_config);
        let client_name = Self::extract_client_from_topic(topic, &self.config.topic_index_config);
        headers.insert("index_name".to_string(), index_name);
        headers.insert("client_name".to_string(), client_name);
        headers.insert("topic".to_string(), topic.to_string());
        
        // Use zero-copy for large messages
        let payload_bytes = if use_zero_copy {
            debug!("Using zero-copy for large message: {} bytes", payload_size);
            // For zero-copy, we create Bytes without copying the data
            // This uses Arc internally for reference counting
            bytes::Bytes::from(payload.to_vec())
        } else {
            // For smaller messages, use the standard copy
            bytes::Bytes::copy_from_slice(payload)
        };
        
        let kafka_msg = KafkaMessage {
            id: msg_id,
            timestamp,
            topic: topic.to_string(),
            partition: msg.partition(),
            offset: msg.offset(),
            key: msg.key().map(|k| k.to_vec()),
            payload: payload_bytes,
            headers,
            is_zero_copy: use_zero_copy,
            payload_json: {
                // Determine payload type first
                let payload_type = if msg.topic().contains("trace") { "trace" } 
                                  else if msg.topic().contains("log") { "log" }
                                  else if msg.topic().contains("metric") { "metric" }
                                  else { "unknown" };
                
                // For OTLP data types, encode as base64
                if payload_type == "trace" || payload_type == "metric" || payload_type == "log" {
                    debug!("[HIGH PERF CONSUMER] Processing {} message: size={} bytes", 
                          payload_type, payload.len());
                    
                    use base64::Engine;
                    let encoded = base64::engine::general_purpose::STANDARD.encode(&payload);
                    
                    // Validate the base64 encoding
                    if encoded.is_empty() {
                        warn!("Base64 encoding resulted in empty string for {} message", payload_type);
                        return None;
                    }
                    encoded
                } else {
                    // For non-OTLP data, try UTF-8 first
                    match std::str::from_utf8(&payload) {
                        Ok(s) => {
                            if s.trim().is_empty() {
                                debug!("Skipping message with empty UTF-8 payload");
                                return None;
                            }
                            s.to_string()
                        },
                        Err(_) => {
                            use base64::Engine;
                            base64::engine::general_purpose::STANDARD.encode(&payload)
                        }
                    }
                }
            },
            payload_type: if msg.topic().contains("trace") { "trace" } 
                         else if msg.topic().contains("log") { "log" }
                         else if msg.topic().contains("metric") { "metric" }
                         else { "unknown" }.to_string(),
            size_bytes: payload_size,
        };
        
        self.messages_processed.fetch_add(1, Ordering::Relaxed);
        self.bytes_processed.fetch_add(payload_size as u64, Ordering::Relaxed);
        
        // Track per-topic metrics
        KAFKA_MESSAGES_CONSUMED
            .with_label_values(&[msg.topic(), &self.node_id])
            .inc();
        
        Some(kafka_msg)
    }
    
    /// Batch processor that collects messages and sends to ingestion
    async fn batch_processor(
        &self,
        processor_id: usize,
        msg_rx: Arc<tokio::sync::Mutex<mpsc::Receiver<KafkaMessage>>>,
        grpc_client: Arc<TelemetryIngestionClient>,
    ) {
        // Use larger initial batch capacity for efficiency
        let mut batch = Vec::with_capacity(self.config.batch_size * 2);
        let mut last_send = Instant::now();
        let batch_timeout = Duration::from_millis(self.config.batch_timeout_ms);
        
        // Use configurable batch size from performance config
        let max_batch_bytes = self.config.max_batch_bytes;
        let mut current_batch_bytes = 0usize;
        
        // Track processing performance
        let mut messages_processed = 0u64;
        let mut batches_sent = 0u64;
        
        loop {
            // Try to fill batch more efficiently
            let _collect_start = Instant::now();
            let collect_timeout = Duration::from_millis(50); // Shorter collection windows
            
            // Use blocking receive for the first message to avoid busy waiting
            if batch.is_empty() {
                let mut rx = msg_rx.lock().await;
                match rx.recv().await {
                    Some(msg) => {
                        current_batch_bytes += msg.size_bytes;
                        batch.push(msg);
                    },
                    None => {
                        info!("Processor {} channel closed, shutting down", processor_id);
                        break;
                    }
                }
                
                // Try to collect more messages while we have the lock
                let deadline = Instant::now() + collect_timeout;
                while batch.len() < self.config.batch_size && 
                      current_batch_bytes < max_batch_bytes && 
                      Instant::now() < deadline {
                    match rx.try_recv() {
                        Ok(msg) => {
                            // Debug log for large messages (not a problem, just informational)
                            if msg.size_bytes > 1_000_000 {
                                debug!(
                                    "Large message detected: {} bytes ({:.2} MB) from topic {}",
                                    msg.size_bytes, msg.size_bytes as f64 / 1_048_576.0, msg.topic
                                );
                            }
                            
                            // Check if adding this message would exceed the limit
                            if current_batch_bytes + msg.size_bytes > max_batch_bytes {
                                // If batch is empty and single message is too large, compress it
                                if batch.is_empty() {
                                    warn!(
                                        "Single message too large for batch: {} bytes ({:.2} MB), will compress, from topic {}",
                                        msg.size_bytes, msg.size_bytes as f64 / 1_048_576.0, msg.topic
                                    );
                                    // Message will be compressed when sent, add it anyway
                                    current_batch_bytes += msg.size_bytes;
                                    batch.push(msg);
                                    // Force send this single large message
                                    break;
                                }
                                // Otherwise, break to send current batch
                                break;
                            }
                            current_batch_bytes += msg.size_bytes;
                            batch.push(msg);
                        },
                        Err(_) => break,
                    }
                }
                drop(rx);
            } else {
                // We have messages, try to fill the batch
                let mut rx = msg_rx.lock().await;
                let deadline = Instant::now() + collect_timeout;
                
                while batch.len() < self.config.batch_size && 
                      current_batch_bytes < max_batch_bytes &&
                      Instant::now() < deadline {
                    match rx.try_recv() {
                        Ok(msg) => {
                            // Debug log for large messages (not a problem, just informational)
                            if msg.size_bytes > 1_000_000 {
                                debug!(
                                    "Large message detected: {} bytes ({:.2} MB) from topic {}",
                                    msg.size_bytes, msg.size_bytes as f64 / 1_048_576.0, msg.topic
                                );
                            }
                            
                            // Check if adding this message would exceed the limit
                            if current_batch_bytes + msg.size_bytes > max_batch_bytes {
                                // If batch is empty and single message is too large, compress it
                                if batch.is_empty() {
                                    warn!(
                                        "Single message too large for batch: {} bytes ({:.2} MB), will compress, from topic {}",
                                        msg.size_bytes, msg.size_bytes as f64 / 1_048_576.0, msg.topic
                                    );
                                    // Message will be compressed when sent, add it anyway
                                    current_batch_bytes += msg.size_bytes;
                                    batch.push(msg);
                                    // Force send this single large message
                                    break;
                                }
                                // Otherwise, break to send current batch
                                break;
                            }
                            current_batch_bytes += msg.size_bytes;
                            batch.push(msg);
                        },
                        Err(_) => break,
                    }
                }
                drop(rx);
            }
            
            // Send batch if ready
            let should_send = batch.len() >= self.config.batch_size || 
                             current_batch_bytes >= max_batch_bytes ||
                             (batch.len() > 0 && last_send.elapsed() > batch_timeout);
            
            if should_send {
                let batch_size = batch.len();
                let batch_id = format!("{}-p{}-{}", self.node_id, processor_id, chrono::Utc::now().timestamp_millis());
                
                debug!(
                    "Processor {} sending batch of {} messages ({:.2} MB)",
                    processor_id, batch_size, current_batch_bytes as f64 / 1_048_576.0
                );
                
                // Take ownership of the batch and prepare a new one
                let messages_to_send = std::mem::replace(&mut batch, Vec::with_capacity(self.config.batch_size * 2));
                current_batch_bytes = 0; // Reset byte counter
                
                
                match grpc_client.send_batch(batch_id.clone(), messages_to_send).await {
                    Ok(_) => {
                        // WAL write confirmed
                        debug!("Successfully sent batch {}", batch_id);
                        
                        messages_processed += batch_size as u64;
                        batches_sent += 1;
                        
                        if batches_sent % 100 == 0 {
                            info!(
                                "Processor {} sent {} batches ({} messages total, avg {:.1} msgs/batch)",
                                processor_id, batches_sent, messages_processed,
                                messages_processed as f64 / batches_sent as f64
                            );
                        }
                        
                        INGESTION_MESSAGES_TOTAL
                            .with_label_values(&["kafka", "success"])
                            .inc_by(batch_size as u64);
                    }
                    Err(e) => {
                        warn!("Processor {} failed to send batch of {} messages: {}", processor_id, batch_size, e);
                        
                        // On error, we might want to retry or handle the messages differently
                        // For now, we'll log and continue
                        INGESTION_MESSAGES_TOTAL
                            .with_label_values(&["kafka", "error"])
                            .inc_by(batch_size as u64);
                    }
                }
                
                last_send = Instant::now();
            }
            
            if !self.running.load(Ordering::Relaxed) {
                break;
            }
        }
        
        // Send final batch
        if !batch.is_empty() {
            let batch_id = format!("{}-p{}-final", self.node_id, processor_id);
            let batch_size = batch.len();
            
            
            match grpc_client.send_batch(batch_id.clone(), batch).await {
                Ok(_) => {
                    debug!("Sent final batch of {} messages", batch_size);
                }
                Err(e) => {
                    error!("Failed to send final batch: {}", e);
                }
            }
        }
    }
    
    /// Report performance metrics
    async fn report_performance_metrics(&self) {
        let mut last_messages = 0u64;
        let mut last_bytes = 0u64;
        let mut last_time = Instant::now();
        let mut tick_count = 0u64;
        
        loop {
            tokio::time::sleep(Duration::from_secs(10)).await;
            tick_count += 1;
            
            let current_messages = self.messages_processed.load(Ordering::Relaxed);
            let current_bytes = self.bytes_processed.load(Ordering::Relaxed);
            let elapsed = last_time.elapsed().as_secs_f64();
            
            let msg_rate = (current_messages - last_messages) as f64 / elapsed;
            let mb_rate = (current_bytes - last_bytes) as f64 / elapsed / 1_048_576.0;
            
            // Get channel metrics
            // Only log if there's activity or every 60 seconds
            if msg_rate > 0.0 || tick_count % 6 == 0 {
                info!(
                    "High-Perf Mode | {} consumers | {:.0} msgs/sec, {:.2} MB/sec | Total: {} messages ({:.1} GB)",
                    self.config.num_consumers,
                    msg_rate,
                    mb_rate,
                    current_messages,
                    current_bytes as f64 / 1_073_741_824.0  // GB
                );
            }
            
            // Note: KAFKA_MESSAGES_CONSUMED expects [topic, consumer_id] labels
            // Since we consume multiple topics, we'll skip this metric here
            // Individual topic metrics are tracked in process_message_fast()
            
            last_messages = current_messages;
            last_bytes = current_bytes;
            last_time = Instant::now();
            
            if !self.running.load(Ordering::Relaxed) {
                break;
            }
        }
    }
    
    /// Stop the consumer
    pub async fn stop(&self) {
        info!("Stopping high-performance Kafka consumer");
        self.running.store(false, Ordering::SeqCst);
    }
}

