use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use anyhow::Result;
use tokio::sync::{RwLock, mpsc};
use tracing::{info, warn, debug, error};
use bincode;

use openwit_network::ClusterHandle;
use openwit_inter_node::TransferManager;
use openwit_config::UnifiedConfig;
use crate::proxy_buffer::ProxyMessage;

use openwit_ingestion::types::{IngestedMessage, MessageSource, MessagePayload};

/// Round-robin router for distributing traffic to ingestor nodes
#[allow(dead_code)]
pub struct RoundRobinRouter {
    node_id: String,
    config: UnifiedConfig,
    cluster_handle: ClusterHandle,
    transfer_manager: Arc<TransferManager>,
    /// Current round-robin counter
    counter: AtomicUsize,
    /// Cached list of available ingestor nodes
    ingestor_nodes: Arc<RwLock<Vec<String>>>,
    /// Last update time for node list
    last_update: Arc<RwLock<std::time::Instant>>,
    // Actor channel for local mode - removed: ingestion actor removed
    // ingestion_actor_tx: Arc<RwLock<Option<mpsc::Sender<IngestionActorMessage>>>>,"
}

impl RoundRobinRouter {
    pub fn new(
        node_id: String,
        cluster_handle: ClusterHandle,
        transfer_manager: Arc<TransferManager>,
        config: UnifiedConfig,
    ) -> Self {
        Self {
            node_id,
            config,
            cluster_handle,
            transfer_manager,
            counter: AtomicUsize::new(0),
            ingestor_nodes: Arc::new(RwLock::new(Vec::new())),
            last_update: Arc::new(RwLock::new(std::time::Instant::now())),
            // ingestion_actor_tx: Arc::new(RwLock::new(None)), // Removed: ingestion actor removed
        }
    }
    
    // Set the ingestion actor channel for local mode - removed: ingestion actor removed
    // pub fn set_ingestion_actor(&self, tx: mpsc::Sender<IngestionActorMessage>) {
    //     tokio::spawn({
    //         let actor_tx = self.ingestion_actor_tx.clone();
    //         async move {
    //             *actor_tx.write().await = Some(tx);
    //         }
    //     });
    // }
    
    /// Start monitoring for ingestor nodes
    pub async fn start_monitoring(self: Arc<Self>) {
        let self_clone = self.clone();
        
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
            
            loop {
                interval.tick().await;
                if let Err(e) = self_clone.update_ingestor_list().await {
                    warn!("Failed to update ingestor list: {}", e);
                }
            }
        });
        
        // Initial update
        let _ = self.update_ingestor_list().await;
    }
    
    /// Update list of available ingestor nodes (only healthy ones)
    async fn update_ingestor_list(&self) -> Result<()> {
        // Get only healthy ingestor nodes
        let nodes = self.cluster_handle.view.get_healthy_nodes_with_role("ingest").await;
        
        let mut current_nodes = self.ingestor_nodes.write().await;
        
        if nodes != *current_nodes {
            info!("Healthy ingestor nodes updated: {:?} -> {:?}", *current_nodes, nodes);
            
            // Log removed nodes (might be unhealthy)
            for old_node in current_nodes.iter() {
                if !nodes.contains(old_node) {
                    warn!("Node {} removed from routing pool (likely unhealthy)", old_node);
                }
            }
            
            // Register new nodes with transfer manager
            for node in &nodes {
                if !current_nodes.contains(node) {
                    info!("Node {} added to routing pool (healthy)", node);
                    // Get node address from gossip or use convention
                    let addr = format!("{}:9002", node); // Inter-node port
                    if let Err(e) = self.transfer_manager.register_node(
                        node.clone(), 
                        addr
                    ).await {
                        warn!("Failed to register node {}: {}", node, e);
                    }
                }
            }
            
            *current_nodes = nodes;
            *self.last_update.write().await = std::time::Instant::now();
        }
        
        Ok(())
    }
    
    /// Get next ingestor node using round-robin
    pub async fn get_next_ingestor(&self) -> Result<String> {
        let nodes = self.ingestor_nodes.read().await;
        
        if nodes.is_empty() {
            return Err(anyhow::anyhow!("No ingestor nodes available"));
        }
        
        let index = self.counter.fetch_add(1, Ordering::Relaxed) % nodes.len();
        let selected_node = nodes[index].clone();
        
        debug!("Selected ingestor node: {} (index: {}/{})", 
               selected_node, index, nodes.len());
        
        Ok(selected_node)
    }
    
    /// Route a single message (will batch internally)
    pub async fn route_message(&self, message: ProxyMessage) -> Result<()> {
        // For single messages, we could batch them
        // For now, route immediately
        self.route_batch(vec![message]).await
    }
    pub async fn route_batch(&self, messages: Vec<ProxyMessage>) -> Result<()> {
        if messages.is_empty() {
            return Ok(());
        }
        
        if self.config.environment == "local" {
            // In local mode, send directly to local ingestion actor
            info!("Local mode: routing {} messages to local ingestion", messages.len());
            
            // Check if we have an actor channel
            // Ingestion actor removed
            let use_actor = false;
            if use_actor {
                // Convert proxy messages to ingested messages inline
                let ingested_messages: Vec<IngestedMessage> = messages.into_iter()
                    .map(|proxy_msg| {
                        let source = match proxy_msg.source {
                            crate::proxy_buffer::MessageSource::Grpc { endpoint } => {
                                MessageSource::Grpc { peer_addr: endpoint }
                            }
                            crate::proxy_buffer::MessageSource::Http { endpoint, .. } => {
                                MessageSource::Grpc { peer_addr: endpoint }
                            }
                            crate::proxy_buffer::MessageSource::Kafka { topic, partition, offset } => {
                                MessageSource::Kafka { topic, partition, offset }
                            }
                        };
                        
                        let payload_type = proxy_msg.metadata.get("content_type")
                            .map(|s| s.as_str())
                            .unwrap_or("trace");
                        
                        let payload = match payload_type {
                            "trace" | "traces" => MessagePayload::Trace(proxy_msg.data.to_vec()),
                            "log" | "logs" => MessagePayload::Logs(proxy_msg.data.to_vec()),
                            "metric" | "metrics" => MessagePayload::Metrics(proxy_msg.data.to_vec()),
                            _ => MessagePayload::Trace(proxy_msg.data.to_vec()),
                        };
                        
                        IngestedMessage {
                            id: proxy_msg.id,
                            received_at: proxy_msg.timestamp.timestamp_millis() as u64,
                            size_bytes: proxy_msg.data.len(),
                            source,
                            payload,
                            index_name: None,
                        }
                    })
                    .collect();
                
                // Send to actor
                // Actor send removed
                // if let Err(e) = tx.send(IngestionActorMessage::IngestBatch(ingested_messages)).await {
                if let Err(e) = async { Err::<(), String>("Actor removed".to_string()) }.await {
                    error!("Failed to send messages to ingestion actor: {}", e);
                    return Err(anyhow::anyhow!("Ingestion actor channel closed"));
                }
                
                debug!("Successfully routed batch to local ingestion actor");
            } else {
                // Fallback to inter-node transfer
                warn!("No ingestion actor channel available, using inter-node transfer");
                let target = "local-ingestor";
                
                let batch_data = bincode::serialize(&messages)?;
                let mut metadata = std::collections::HashMap::new();
                metadata.insert("batch_size".to_string(), messages.len().to_string());
                metadata.insert("mode".to_string(), "local".to_string());
                
                self.transfer_manager.initiate_data_transfer(
                    target,
                    openwit_inter_node::proto::TransferType::BufferedData,
                    metadata,
                    batch_data.len() as u64,
                ).await?;
            }
        } else {
            // Distributed mode is handled by DistributedRouter
            return Err(anyhow::anyhow!("Use DistributedRouter for non-local environments"));
        }
        
        Ok(())
    }
    
    /// Get current router statistics
    pub async fn get_stats(&self) -> RouterStats {
        let nodes = self.ingestor_nodes.read().await;
        let last_update = *self.last_update.read().await;
        
        RouterStats {
            available_nodes: nodes.len(),
            node_list: nodes.clone(),
            total_requests: self.counter.load(Ordering::Relaxed),
            last_update_ago: last_update.elapsed(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct RouterStats {
    pub available_nodes: usize,
    pub node_list: Vec<String>,
    pub total_requests: usize,
    pub last_update_ago: std::time::Duration,
}

/// Health check for routing
impl RoundRobinRouter {
    pub async fn health_check(&self) -> Result<()> {
        let nodes = self.ingestor_nodes.read().await;
        
        if nodes.is_empty() {
            return Err(anyhow::anyhow!("No ingestor nodes available"));
        }
        
        // Check if node list is stale
        let last_update = *self.last_update.read().await;
        if last_update.elapsed() > std::time::Duration::from_secs(30) {
            warn!("Ingestor node list is stale (last updated {} seconds ago)", 
                  last_update.elapsed().as_secs());
        }
        
        Ok(())
    }
}