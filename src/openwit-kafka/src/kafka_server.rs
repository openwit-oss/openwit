use std::sync::Arc;
use anyhow::{Result, Context};
use tracing::{info, warn, error, debug};

use openwit_config::UnifiedConfig;
use crate::kafka_consumer::KafkaConsumer;
use crate::high_performance_consumer::{HighPerformanceKafkaConsumer, HighPerfKafkaConfig};
use crate::types::KafkaConfig;
use crate::grpc_client::TelemetryIngestionClient;
use openwit_control_plane::ControlPlaneClient;

pub struct KafkaServer {
    config: Arc<UnifiedConfig>,
    consumer: Option<ConsumerType>,
}

enum ConsumerType {
    Standard(Arc<KafkaConsumer>),
    HighPerformance(Arc<HighPerformanceKafkaConsumer>),
}

impl KafkaServer {
    pub fn new(config: UnifiedConfig) -> Self {
        Self {
            config: Arc::new(config),
            consumer: None,
        }
    }
    
    pub async fn start(mut self) -> Result<()> {
        let kafka_cfg = &self.config.ingestion.kafka;
        
        // Check if Kafka is configured
        if kafka_cfg.brokers.is_none() || kafka_cfg.topics.is_empty() {
            info!("Kafka ingestion not configured - skipping");
            return Ok(());
        }
        
        info!("Starting Kafka ingestion server");

        let node_id = if let Ok(override_id) = std::env::var("OPENWIT_NODE_ID") {
            // Allow explicit override via environment variable
            override_id
        } else if let Ok(hostname) = std::env::var("HOSTNAME") {
            // Extract pod number from hostname like "kafka-hotcache-kafka-0" -> "openwit-kafka-0"
            if let Some(pod_num) = hostname.split('-').last().and_then(|s| s.parse::<u32>().ok()) {
                format!("openwit-kafka-{}", pod_num)
            } else {
                format!("openwit-kafka-0")
            }
        } else {
            format!("openwit-kafka-{}", uuid::Uuid::new_v4().to_string().split('-').next().unwrap())
        };
        
        info!("Kafka server node ID: {}", node_id);
        
        // Get control plane endpoint from environment or config
        let control_plane_endpoint = if let Ok(env_endpoint) = std::env::var("CONTROL_PLANE_ENDPOINT") {
            info!("Using control plane endpoint from environment: {}", env_endpoint);
            env_endpoint
        } else {
            info!("Using control plane endpoint from config: {}", self.config.control_plane.grpc_endpoint);
            self.config.control_plane.grpc_endpoint.clone()
        };
        
        info!("Connecting to control plane at: {}", control_plane_endpoint);
        
        // Create control plane client
        let control_plane_client = ControlPlaneClient::new(&node_id, &self.config)
            .await
            .context("Failed to create control plane client")?;
        
        // Register with control plane as kafka node
        let control_plane_for_registration = control_plane_client.clone();
        let node_id_for_health = node_id.clone();
        let kafka_endpoint = format!("http://{}:8080", 
            std::env::var("HOSTNAME").unwrap_or_else(|_| "localhost".to_string()));
        
        tokio::spawn(async move {
            // Initial registration
            if let Err(e) = register_with_control_plane(&control_plane_for_registration, &node_id_for_health, &kafka_endpoint).await {
                error!("Failed to register Kafka node with control plane: {}", e);
            } else {
                info!("Successfully registered Kafka node {} with control plane as kafka service", node_id_for_health);
            }
            
            // Periodic health reporting disabled - only register once
            // let mut interval = tokio::time::interval(Duration::from_secs(30));
            // loop {
            //     interval.tick().await;
            //     if let Err(e) = send_health_report(&control_plane_for_registration, &node_id_for_health).await {
            //         warn!("Failed to send health report: {}", e);
            //     }
            // }
        });
        
        // TODO: Create gRPC client for ingestion after control plane connection is verified
        // let grpc_client = Arc::new(
        //     TelemetryIngestionClient::new(
        //         node_id.clone(),
        //         control_plane_client,
        //         (*self.config).clone(),
        //     )
        //     .await
        //     .context("Failed to create Kafka ingestion gRPC client")?
        // );
        // 
        // // Start cleanup task for gRPC client
        // let cleanup_client = grpc_client.clone();
        // tokio::spawn(async move {
        //     cleanup_client.cleanup_stale_clients().await;
        // });
        
        // For now, create a dummy gRPC client to satisfy the compiler
        // TODO: Remove this once we properly handle ingestion node connection
        let grpc_client = Arc::new(
            TelemetryIngestionClient::new(
                node_id.clone(),
                control_plane_client,
                (*self.config).clone(),
            )
            .await
            .context("Failed to create Kafka ingestion gRPC client")?
        );
        
        // Create consumer based on configuration
        let consumer_type = kafka_cfg.consumer_type.as_str();
        info!("Creating {} consumer", consumer_type);
        
        match consumer_type {
            "high_performance" => {
                // Create high-performance configuration
                // Extract pod index from node_id to create unique consumer group
                let pod_index = node_id.split('-').last()
                    .and_then(|s| s.parse::<u32>().ok())
                    .unwrap_or(0);
                let unique_group_id = format!("openwit-kafka-hp-{}", pod_index);
                
                let hp_config = HighPerfKafkaConfig {
                    brokers: kafka_cfg.brokers.clone().unwrap_or_else(|| "localhost:9092".to_string()),
                    group_id: unique_group_id,
                    topics: kafka_cfg.topics.clone(),
                    
                    // Performance settings from config
                    num_consumers: kafka_cfg.high_performance.num_consumers,
                    batch_size: kafka_cfg.batching.batch_size as usize,
                    batch_timeout_ms: kafka_cfg.batching.fetch_wait_max_ms,
                    num_grpc_clients: kafka_cfg.high_performance.num_grpc_clients,
                    processing_threads: kafka_cfg.high_performance.processing_threads,
                    channel_buffer_size: kafka_cfg.high_performance.channel_buffer_size,
                    
                    // Kafka tuning
                    fetch_min_bytes: kafka_cfg.high_performance.fetch_min_bytes,
                    fetch_max_bytes: kafka_cfg.high_performance.fetch_max_bytes,
                    fetch_max_wait_ms: 5, // Hardcoded for low latency
                    max_partition_fetch_bytes: kafka_cfg.high_performance.max_partition_fetch_bytes,
                    
                    // Memory optimization
                    enable_zero_copy: kafka_cfg.high_performance.enable_zero_copy,
                    zero_copy_threshold_bytes: kafka_cfg.high_performance.zero_copy_threshold_bytes,
                    preallocate_buffers: kafka_cfg.high_performance.preallocate_buffers,
                    buffer_pool_size: kafka_cfg.high_performance.buffer_pool_size,
                    
                    // Performance limits from config
                    max_batch_bytes: kafka_cfg.performance.max_batch_bytes,
                    
                    // Standard settings
                    commit_interval_ms: kafka_cfg.timeouts.commit_interval_seconds * 1000,
                    enable_auto_commit: false,
                    session_timeout_ms: kafka_cfg.timeouts.session_timeout_ms,
                    max_poll_interval_ms: kafka_cfg.timeouts.max_poll_interval_ms,
                    
                    // Static partition assignment is always enabled
                    enable_static_assignment: true,
                    pod_index: None,  // Will be extracted from node_id
                    total_pods: None,  // Let Kafka handle distribution dynamically
                    
                    // Topic index configuration
                    topic_index_config: Some(crate::types::TopicIndexConfig {
                        default_index_position: kafka_cfg.topic_index_config.default_index_position,
                        patterns: kafka_cfg.topic_index_config.patterns.iter().map(|p| crate::types::TopicIndexPattern {
                            pattern: p.r#match.clone(),
                            index_position: p.index_position,
                            index_prefix: p.index_prefix.clone(),
                        }).collect(),
                        auto_generate: crate::types::AutoGenerateConfig {
                            enabled: kafka_cfg.topic_index_config.auto_generate.enabled,
                            prefix: kafka_cfg.topic_index_config.auto_generate.prefix.clone(),
                            strategy: kafka_cfg.topic_index_config.auto_generate.strategy.clone(),
                        },
                        client_name_config: crate::types::ClientNameConfig {
                            kafka_client_position: kafka_cfg.topic_index_config.client_name_config.kafka_client_position,
                            topic_separator: kafka_cfg.topic_index_config.client_name_config.topic_separator.clone(),
                        },
                    }),
                };
                
                let hp_consumer = Arc::new(
                    HighPerformanceKafkaConsumer::new(
                        node_id.clone(),
                        hp_config,
                        &self.config,
                    ).await?
                );
                
                self.consumer = Some(ConsumerType::HighPerformance(hp_consumer.clone()));
                hp_consumer.start().await?;
            },
            _ => {
                // Default to standard consumer with unique group ID
                let pod_index = node_id.split('-').last()
                    .and_then(|s| s.parse::<u32>().ok())
                    .unwrap_or(0);
                let mut kafka_config = KafkaConfig::from(&self.config.ingestion);
                kafka_config.group_id = format!("openwit-kafka-{}", pod_index);
                
                let consumer = Arc::new(
                    KafkaConsumer::new(
                        node_id.clone(),
                        kafka_config,
                        grpc_client,
                    ).await?
                );
                
                self.consumer = Some(ConsumerType::Standard(consumer.clone()));
                consumer.start().await?;
            }
        }
        
        info!("Kafka ingestion server started successfully");
        
        // Keep the server running
        tokio::signal::ctrl_c().await?;
        
        info!("Shutting down Kafka server...");
        if let Some(consumer) = &self.consumer {
            match consumer {
                ConsumerType::Standard(c) => c.stop().await,
                ConsumerType::HighPerformance(c) => c.stop().await,
            }
        }
        
        Ok(())
    }
    
    /// Create and start a Kafka server from configuration
    pub async fn from_config(config: UnifiedConfig) -> Result<()> {
        // Check if Kafka is enabled
        if !config.ingestion.sources.kafka.enabled {
            info!("Kafka ingestion disabled in configuration");
            return Ok(());
        }
        
        let server = KafkaServer::new(config);
        server.start().await
    }
}

/// Register Kafka node with control plane
async fn register_with_control_plane(
    control_plane_client: &ControlPlaneClient,
    node_id: &str,
    endpoint: &str,
) -> Result<()> {
    info!("Registering Kafka node {} with control plane", node_id);
    
    // Clone client for registration (client methods require &mut self)
    let mut client = control_plane_client.clone();
    
    // Create metadata for registration
    let mut metadata = std::collections::HashMap::new();
    metadata.insert("service_type".to_string(), "kafka".to_string());
    metadata.insert("endpoint".to_string(), endpoint.to_string());
    metadata.insert("version".to_string(), env!("CARGO_PKG_VERSION").to_string());
    
    // Register with control plane
    match client.register_node(node_id, "kafka", metadata).await {
        Ok(_) => {
            info!("Successfully registered Kafka node {} with control plane", node_id);
            Ok(())
        }
        Err(e) => {
            error!("Failed to register Kafka node {}: {}", node_id, e);
            Err(anyhow::anyhow!("Registration failed: {}", e))
        }
    }
}

// Health reporting disabled - control plane only provides service discovery now
/*
/// Send periodic health report to control plane
async fn send_health_report(
    control_plane_client: &ControlPlaneClient,
    node_id: &str,
) -> Result<()> {
    let mut client = control_plane_client.clone();
    
    let health_report = openwit_proto::control::NodeHealthReport {
        node_id: node_id.to_string(),
        node_role: "kafka".to_string(), // Use "kafka" role
        cpu_percent: 0.0,
        memory_percent: 0.0,
        disk_percent: 0.0,
        is_healthy: true,
        timestamp: Some(openwit_proto::prost_types::Timestamp::from(std::time::SystemTime::now())),
        metadata: {
            let mut metadata = std::collections::HashMap::new();
            metadata.insert("last_health_check".to_string(), chrono::Utc::now().to_rfc3339());
            metadata.insert("service_type".to_string(), "kafka".to_string());
            metadata
        },
    };
    
    match client.report_node_health(health_report).await {
        Ok(_) => {
            debug!("Sent health report for Kafka node {}", node_id);
            Ok(())
        }
        Err(e) => {
            warn!("Failed to send health report for Kafka node {}: {}", node_id, e);
            Err(anyhow::anyhow!("Health report failed: {}", e))
        }
    }
}
*/