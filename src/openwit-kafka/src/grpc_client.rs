use std::sync::Arc;
use std::time::{Duration, Instant};
use anyhow::Result;
use tokio::sync::RwLock;
use tonic::transport::Channel;
use tracing::{info, warn, error, debug};
use flate2::Compression;
use flate2::write::GzEncoder;

use openwit_control_plane::ControlPlaneClient;
use openwit_config::UnifiedConfig;
use openwit_proto::ingestion::{
    telemetry_ingestion_service_client::TelemetryIngestionServiceClient,
    TelemetryData as ProtoTelemetryData,
    TelemetryIngestRequest,
};
use crate::types::KafkaMessage;
use openwit_postgres::BatchTracker;

/// Client for sending Kafka messages to ingestion nodes via gRPC
#[allow(unused)]
#[derive(Clone)]
pub struct TelemetryIngestionClient {
    node_id: String,
    control_plane_client: Arc<RwLock<ControlPlaneClient>>,
    config: Arc<UnifiedConfig>,
    /// Cached gRPC clients for each ingestion node
    clients: Arc<RwLock<std::collections::HashMap<String, TelemetryIngestionServiceClient<Channel>>>>,
    /// Current ingestion node index for round-robin
    current_index: Arc<tokio::sync::Mutex<usize>>,
    /// Cached service discovery results with TTL
    service_discovery_cache: Arc<RwLock<Option<(Vec<IngestionNodeInfo>, Instant)>>>,
}

impl TelemetryIngestionClient {
    pub async fn new(
        node_id: String,
        control_plane_client: ControlPlaneClient,
        config: UnifiedConfig,
    ) -> Result<Self> {
        Ok(Self {
            node_id,
            control_plane_client: Arc::new(RwLock::new(control_plane_client)),
            config: Arc::new(config),
            clients: Arc::new(RwLock::new(std::collections::HashMap::new())),
            current_index: Arc::new(tokio::sync::Mutex::new(0)),
            service_discovery_cache: Arc::new(RwLock::new(None)),
        })
    }
    
    /// Send a batch of Kafka messages to an ingestion node using batch mode
    pub async fn send_batch(
        &self,
        batch_id: String,
        messages: Vec<KafkaMessage>,
    ) -> Result<openwit_proto::ingestion::TelemetryIngestResponse> {
        // Get available ingestion nodes with retry
        let max_retries = self.config.ingestion.kafka.performance.max_retries;
        let retry_delay_ms = self.config.ingestion.kafka.performance.retry_delay_ms;
        let mut retries = max_retries;
        loop {
            match self.send_messages_batch(batch_id.clone(), messages.clone()).await {
                Ok(response) => {
                    debug!("Successfully sent batch {} via batch mode to ingestion node", batch_id);
                    return Ok(response);
                }
                Err(e) => {
                    retries -= 1;
                    if retries == 0 {
                        error!("Failed to send batch {} after {} retries: {}", batch_id, max_retries, e);
                        return Err(e);
                    }
                    warn!("Failed to send batch {}, retrying... ({})", batch_id, e);
                    tokio::time::sleep(Duration::from_millis(retry_delay_ms)).await;
                }
            }
        }
    }
    
    /// Split oversized messages into chunks if needed
    fn split_oversized_messages(&self, messages: Vec<KafkaMessage>) -> Vec<KafkaMessage> {
        let max_message_size = self.config.ingestion.kafka.performance.max_message_bytes;
        let mut result = Vec::new();
        
        for msg in messages {
            // Check if message needs splitting (even after potential compression)
            if msg.size_bytes > max_message_size {
                info!("Splitting oversized message {} ({} bytes) into chunks", msg.id, msg.size_bytes);
                
                // Split the payload_json into chunks (use 80% of max size for overhead)
                let chunk_size = (max_message_size as f64 * 0.8) as usize;
                let payload_bytes = msg.payload_json.as_bytes();
                let total_chunks = (payload_bytes.len() + chunk_size - 1) / chunk_size;
                
                for (i, chunk) in payload_bytes.chunks(chunk_size).enumerate() {
                    let mut chunk_msg = msg.clone();
                    chunk_msg.id = format!("{}_chunk_{}_of_{}", msg.id, i + 1, total_chunks);
                    
                    // Convert chunk back to string (base64 if needed)
                    chunk_msg.payload_json = if msg.payload_type == "trace" || msg.payload_type == "metric" || msg.payload_type == "log" {
                        // Already base64 encoded, split the base64 string
                        String::from_utf8_lossy(chunk).to_string()
                    } else {
                        // Re-encode chunk as base64
                        use base64::Engine;
                        base64::engine::general_purpose::STANDARD.encode(chunk)
                    };
                    
                    chunk_msg.size_bytes = chunk.len();
                    chunk_msg.headers.insert("chunked".to_string(), "true".to_string());
                    chunk_msg.headers.insert("chunk_index".to_string(), i.to_string());
                    chunk_msg.headers.insert("total_chunks".to_string(), total_chunks.to_string());
                    chunk_msg.headers.insert("original_msg_id".to_string(), msg.id.clone());
                    
                    result.push(chunk_msg);
                }
            } else {
                result.push(msg);
            }
        }
        
        result
    }
    
    /// Send messages via batch mode to next available ingestion node
    async fn send_messages_batch(
        &self,
        batch_id: String,
        messages: Vec<KafkaMessage>,
    ) -> Result<openwit_proto::ingestion::TelemetryIngestResponse> {
        // Get configuration for retries
        let retry_delay_ms = self.config.ingestion.kafka.performance.retry_delay_ms;
        
        // Get healthy ingestion nodes from control plane with retry
        let mut retry_count = 0;
        let ingestion_nodes = loop {
            let nodes = self.get_healthy_ingestion_nodes().await?;
            if !nodes.is_empty() {
                break nodes;
            }
            
            // No nodes available - pause and retry
            retry_count += 1;
            if retry_count == 1 {
                warn!("No healthy ingestion nodes available for batch {}. Pausing operations and retrying every {}ms...", batch_id, retry_delay_ms);
            } else if retry_count % 12 == 0 { // Log every minute (approximately)
                warn!("Still waiting for healthy ingestion nodes for batch {} ({}ms elapsed)", 
                     batch_id, retry_count * retry_delay_ms);
            }
            
            tokio::time::sleep(Duration::from_millis(retry_delay_ms)).await;
        };
        
        if retry_count > 0 {
            info!("Found healthy ingestion nodes after {} retries for batch {}", retry_count, batch_id);
        }
        
        // Round-robin selection
        let mut index = self.current_index.lock().await;
        *index = (*index + 1) % ingestion_nodes.len();
        let selected_node = &ingestion_nodes[*index];
        
        debug!("Sending batch to ingestion node: {}", selected_node.node_id);
        
        // Get or create gRPC client for this node
        let mut client = self.get_or_create_client(&selected_node.node_id, &selected_node.grpc_endpoint).await?;
        
        // Split oversized messages if needed
        let messages_to_send = self.split_oversized_messages(messages.clone());
        let original_count = messages.len();
        
        // Split into smaller batches based on gRPC message size limit (4MB)
        const MAX_BATCH_SIZE_BYTES: usize = 3 * 1024 * 1024; // 3MB to leave room for overhead
        let mut sub_batches = Vec::new();
        let mut current_batch = Vec::new();
        let mut current_batch_size = 0;
        
        for msg in messages_to_send {
            // Calculate approximate size including metadata
            let msg_size = msg.payload_json.len() + msg.id.len() + 200; // Extra overhead for proto encoding
            
            // Start new batch if adding this message would exceed limit
            if current_batch_size + msg_size > MAX_BATCH_SIZE_BYTES && !current_batch.is_empty() {
                sub_batches.push(current_batch);
                current_batch = Vec::new();
                current_batch_size = 0;
            }
            
            current_batch_size += msg_size;
            current_batch.push(msg);
        }
        
        if !current_batch.is_empty() {
            sub_batches.push(current_batch);
        }
        
        let sub_batch_count = sub_batches.len();
        if sub_batch_count > 1 {
            debug!("[GRPC CLIENT] Split large batch {} into {} sub-batches (original: {} messages)", 
                  batch_id, sub_batch_count, original_count);
        }
        
        let mut total_accepted = 0;
        let mut total_rejected = 0;
        let mut any_wal_written = false;
        let mut any_storage_written = false;
        let mut total_wal_time_ms = 0i64;
        
        // Send each sub-batch
        for (idx, sub_batch) in sub_batches.into_iter().enumerate() {
            let sub_batch_id = if sub_batch_count > 1 {
                format!("{}_part_{}", batch_id, idx + 1)
            } else {
                batch_id.clone()
            };
            
            let sub_batch_count = sub_batch.len();
            let sub_batch_size: usize = sub_batch.iter().map(|m| m.payload_json.len()).sum();
            
            debug!("[GRPC CLIENT] Sending batch {} with {} messages (~{:.2}MB) to {} at {}", 
                  sub_batch_id, sub_batch_count, 
                  sub_batch_size as f64 / (1024.0 * 1024.0),
                  selected_node.node_id, selected_node.grpc_endpoint);
            
            // Log sample of first message
            if let Some(first_msg) = sub_batch.first() {
                debug!("[GRPC CLIENT] First message sample: id={}, size={} bytes, type={}", 
                      first_msg.id, first_msg.size_bytes, first_msg.payload_type);
            }
            
            // Convert messages to proto format
            let proto_messages: Vec<ProtoTelemetryData> = sub_batch
                .into_iter()
                .map(|msg| self.kafka_message_to_proto(msg))
                .collect();
            
            // Create batch request
            let request = TelemetryIngestRequest {
                source_node_id: self.node_id.clone(),
                batch_id: sub_batch_id.clone(),
                messages: proto_messages,
                client_id: format!("kafka-{}", self.node_id),  // Kafka consumer identifier
                offsets: vec![],  // TODO: Pass actual offsets for safe commits
            };
            
            // Send sub-batch with timeout
            let response = match tokio::time::timeout(
                Duration::from_secs(30),
                client.ingest_telemetry_batch(request)
            ).await {
                Ok(Ok(response)) => response.into_inner(),
            Ok(Err(e)) => {
                error!("gRPC batch call to {} failed with error: {:?}", selected_node.node_id, e);
                return Err(anyhow::anyhow!("gRPC batch call failed: {:?}", e));
            }
            Err(_) => {
                error!("gRPC batch call to {} timed out after 30 seconds", selected_node.node_id);
                return Err(anyhow::anyhow!("Batch call timeout after 30 seconds"));
            }
            };
            
            // Process sub-batch response
            let accepted = response.accepted_count;
            let rejected = response.rejected_count;

            total_accepted += accepted;
            total_rejected += rejected;

            // Track WAL and storage status
            if response.wal_written {
                any_wal_written = true;
            }
            if response.storage_written {
                any_storage_written = true;
            }
            total_wal_time_ms += response.wal_write_time_ms;

            if sub_batch_count > 1 {
                debug!("[GRPC CLIENT] Sub-batch {} completed: {} accepted, {} rejected, WAL: {}",
                      sub_batch_id, accepted, rejected, response.wal_written);
            }

            if rejected > 0 {
                warn!("Ingestion node rejected {} messages from sub-batch {}", rejected, sub_batch_id);
                if !response.rejected_ids.is_empty() {
                    debug!("Rejected message IDs: {:?}", response.rejected_ids);
                }
            }

            if !response.success {
                warn!("Sub-batch {} reported failure: {}", sub_batch_id, response.message);
            }
        }
        
        // Log final results
        // Only log summary for large batches or errors
        if original_count > 100 || total_rejected > 0 {
            info!("[GRPC CLIENT] Batch {} completed: {} accepted, {} rejected out of {} total",
                   batch_id, total_accepted, total_rejected, original_count);
        } else {
            debug!("[GRPC CLIENT] Batch {} completed: {} messages sent", batch_id, original_count);
        }

        // Return combined response with proper WAL tracking
        Ok(openwit_proto::ingestion::TelemetryIngestResponse {
            success: total_rejected == 0 && any_wal_written,
            message: format!("Batch {} processed: {} accepted, {} rejected, WAL: {}",
                           batch_id, total_accepted, total_rejected,
                           if any_wal_written { "written" } else { "not written" }),
            accepted_count: total_accepted,
            rejected_count: total_rejected,
            rejected_ids: vec![], // Could aggregate all rejected IDs if needed
            wal_written: any_wal_written,  // True if ANY sub-batch wrote to WAL
            storage_written: any_storage_written,
            wal_path: format!("wal/{}", batch_id),  // Placeholder path
            wal_write_time_ms: total_wal_time_ms,
        })
    }
    
    /// Get healthy ingestion nodes from control plane with 30-second caching
    pub async fn get_healthy_ingestion_nodes(&self) -> Result<Vec<IngestionNodeInfo>> {
        const CACHE_TTL_SECONDS: u64 = 30; // 30-second cache TTL as requested
        
        // Check cache first
        {
            let cache = self.service_discovery_cache.read().await;
            if let Some((cached_nodes, cached_at)) = cache.as_ref() {
                if cached_at.elapsed().as_secs() < CACHE_TTL_SECONDS {
                    debug!("Using cached service discovery results ({} nodes, cached {}s ago)", 
                           cached_nodes.len(), cached_at.elapsed().as_secs());
                    return Ok(cached_nodes.clone());
                }
            }
        }
        
        debug!("Service discovery cache expired or empty, fetching from control plane");
        
        // Cache miss or expired - fetch from control plane
        let mut control_client = self.control_plane_client.write().await;
        
        match control_client.get_healthy_nodes("ingest").await {
            Ok(nodes) => {
                let ingestion_nodes: Vec<IngestionNodeInfo> = nodes
                    .into_iter()
                    .filter(|n| n.health_score >= 0.0)  // Accept any registered node
                    .map(|n| IngestionNodeInfo {
                        node_id: n.node_id,
                        grpc_endpoint: n.grpc_endpoint,
                        health_score: n.health_score,
                    })
                    .collect();
                
                if !ingestion_nodes.is_empty() {
                    info!("Found {} healthy ingestion nodes from control plane (cached for {}s)", 
                          ingestion_nodes.len(), CACHE_TTL_SECONDS);
                    for node in &ingestion_nodes {
                        debug!("  - Node {} at endpoint {} (health: {:.2})", 
                              node.node_id, node.grpc_endpoint, node.health_score);
                    }
                    
                    // Update cache
                    {
                        let mut cache = self.service_discovery_cache.write().await;
                        *cache = Some((ingestion_nodes.clone(), Instant::now()));
                    }
                    
                    return Ok(ingestion_nodes);
                }
                
                // No nodes available - don't cache empty results
                debug!("No healthy ingestion nodes found, not caching empty result");
                Ok(Vec::new())
            }
            Err(e) => {
                warn!("Failed to get healthy nodes from control plane: {}", e);
                // Return empty list instead of error to allow retry
                Ok(Vec::new())
            }
        }
    }
    
    /// Get or create gRPC client for an ingestion node
    async fn get_or_create_client(
        &self,
        node_id: &str,
        endpoint: &str,
    ) -> Result<TelemetryIngestionServiceClient<Channel>> {
        let mut clients = self.clients.write().await;
        
        if let Some(client) = clients.get(node_id) {
            return Ok(client.clone());
        }
        
        // Create new client
        debug!("Creating gRPC client for ingestion node {} at {}", node_id, endpoint);
        let channel = match Channel::from_shared(endpoint.to_string()) {
            Ok(endpoint) => endpoint,
            Err(e) => {
                error!("Failed to create channel endpoint from '{}': {}", endpoint, e);
                return Err(anyhow::anyhow!("Invalid endpoint: {}", e));
            }
        };
        
        debug!("Connecting to gRPC endpoint: {}", endpoint);
        let channel = match channel
            .timeout(Duration::from_secs(30))
            .connect_timeout(Duration::from_secs(10))
            .connect()
            .await {
            Ok(chan) => {
                debug!("Successfully connected to {}", endpoint);
                chan
            }
            Err(e) => {
                warn!("Failed to connect to ingestion node at {}: {:?}", endpoint, e);
                return Err(anyhow::anyhow!("Connection failed: {:?}", e));
            }
        };
            
        let client = TelemetryIngestionServiceClient::new(channel)
            .max_decoding_message_size(32 * 1024 * 1024)  // 32MB to match server  
            .max_encoding_message_size(32 * 1024 * 1024); // 32MB to match server
        clients.insert(node_id.to_string(), client.clone());
        
        Ok(client)
    }
    
    /// Convert internal KafkaMessage to proto format with optional compression
    fn kafka_message_to_proto(&self, msg: KafkaMessage) -> ProtoTelemetryData {
        debug!("[GRPC CLIENT] Converting KafkaMessage to proto: id={}, topic={}, payload_type={}, size={} bytes", 
              msg.id, msg.topic, msg.payload_type, msg.payload_json.len());
        
        // Temporarily disable compression until ingestion service supports it
        // The messages are already base64 encoded and within size limits
        let (payload_json, headers) = (msg.payload_json, msg.headers);
        
        // Log large messages for monitoring
        if msg.size_bytes > 1_048_576 {
            debug!(
                "Processing large message: {} bytes ({:.2} MB) from topic {}",
                msg.size_bytes, msg.size_bytes as f64 / 1_048_576.0, msg.topic
            );
        }
        
        ProtoTelemetryData {
            id: msg.id,
            source: msg.topic,
            partition: msg.partition,
            sequence: msg.offset,
            timestamp: Some(openwit_proto::prost_types::Timestamp::from(std::time::SystemTime::now())),
            payload_json,
            payload_type: msg.payload_type,
            size_bytes: msg.size_bytes as i32,
            headers,
        }
    }
    
    /// Compress payload using gzip
    fn compress_payload(&self, payload: &str) -> Result<String> {
        use std::io::Write;
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(payload.as_bytes())?;
        let compressed = encoder.finish()?;
        
        // For already base64-encoded data (like OTLP traces), return compressed bytes directly as base64
        // The ingestion service will decompress and get the original base64 payload
        use base64::Engine;
        Ok(base64::engine::general_purpose::STANDARD.encode(&compressed))
    }
    
    /// Cleanup stale gRPC clients periodically
    pub async fn cleanup_stale_clients(&self) {
        let mut interval = tokio::time::interval(Duration::from_secs(300)); // 5 minutes
        
        loop {
            interval.tick().await;
            
            let mut clients = self.clients.write().await;
            let before_count = clients.len();
            
            // In production, would check connection health
            // For now, just clear all to force reconnection
            if before_count > 10 {
                clients.clear();
                info!("Cleaned up {} gRPC client connections", before_count);
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct IngestionNodeInfo {
    pub node_id: String,
    pub grpc_endpoint: String,
    pub health_score: f64,
}