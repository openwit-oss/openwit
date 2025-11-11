use std::sync::Arc;
use std::time::Duration;
use anyhow::Result;
use chrono::Utc;
use dashmap::DashMap;
use tokio::time::interval;
use tracing::{info, warn, error};

use crate::types::*;

pub struct NodeManager {
    nodes: Arc<DashMap<NodeId, NodeMetadata>>,
    heartbeat_timeout: Duration,
}

impl NodeManager {
    pub fn new(heartbeat_timeout_secs: u64) -> Self {
        Self {
            nodes: Arc::new(DashMap::new()),
            heartbeat_timeout: Duration::from_secs(heartbeat_timeout_secs),
        }
    }
    
    pub async fn register_node(&self, node: NodeMetadata) -> Result<()> {
        let node_id = node.id.clone();
        let service_type = node.metadata.get("service_type").cloned().unwrap_or_else(|| "unknown".to_string());

        // Check if node already exists (heartbeat update, not new registration)
        let is_new = !self.nodes.contains_key(&node_id);

        if is_new {
            // Only log for NEW nodes
            info!("Registering node: {} with role: {:?}, service_type: {}", node_id.0, node.role, service_type);

            self.nodes.insert(node_id.clone(), node);

            // Log current node count by service type
            let http_count = self.nodes.iter()
                .filter(|entry| entry.value().metadata.get("service_type").map(|st| st == "http").unwrap_or(false))
                .count();
            let grpc_count = self.nodes.iter()
                .filter(|entry| entry.value().metadata.get("service_type").map(|st| st == "grpc").unwrap_or(false))
                .count();
            let kafka_count = self.nodes.iter()
                .filter(|entry| entry.value().metadata.get("service_type").map(|st| st == "kafka").unwrap_or(false))
                .count();

            info!("Current node counts - HTTP: {}, gRPC: {}, Kafka: {}", http_count, grpc_count, kafka_count);

            // Notify other components about new node
            self.broadcast_node_change(&node_id, "registered").await;
        } else {
            // Silently update existing node (heartbeat refresh)
            self.nodes.insert(node_id.clone(), node);
        }

        Ok(())
    }
    
    pub async fn update_heartbeat(&self, node_id: &NodeId) -> Result<()> {
        if let Some(mut node) = self.nodes.get_mut(node_id) {
            node.last_heartbeat = Utc::now();
        } else {
            return Err(anyhow::anyhow!("Node {} not registered", node_id.0));
        }
        
        Ok(())
    }
    
    pub async fn update_node_state(&self, node_id: &NodeId, state: NodeState) -> Result<()> {
        if let Some(mut node) = self.nodes.get_mut(node_id) {
            let old_state = node.state.clone();
            node.state = state.clone();
            
            info!("Node {} state changed: {:?} -> {:?}", node_id.0, old_state, state);
            
            // Handle state transitions
            match (&old_state, &state) {
                (NodeState::Running, NodeState::Failed) => {
                    self.handle_node_failure(node_id).await?;
                }
                (_, NodeState::Stopped) => {
                    self.handle_node_shutdown(node_id).await?;
                }
                _ => {}
            }
        }
        
        Ok(())
    }
    
    pub async fn get_node(&self, node_id: &NodeId) -> Option<NodeMetadata> {
        self.nodes.get(node_id).map(|entry| entry.value().clone())
    }
    
    pub async fn get_nodes_by_role(&self, role: &NodeRole) -> Vec<NodeMetadata> {
        self.nodes.iter()
            .filter(|entry| match &entry.value().role {
                NodeRole::Hybrid(roles) => roles.contains(role),
                r => r == role,
            })
            .map(|entry| entry.value().clone())
            .collect()
    }
    
    pub async fn update_node_metadata(&self, node_id: &NodeId, key: String, value: String) -> Result<()> {
        if let Some(mut node) = self.nodes.get_mut(node_id) {
            node.metadata.insert(key, value);
            Ok(())
        } else {
            Err(anyhow::anyhow!("Node not found"))
        }
    }
    
    pub async fn get_healthy_nodes(&self) -> Vec<NodeMetadata> {
        let cutoff = Utc::now() - chrono::Duration::from_std(self.heartbeat_timeout).unwrap();
        
        self.nodes.iter()
            .filter(|entry| {
                entry.value().last_heartbeat > cutoff && 
                entry.value().state == NodeState::Running
            })
            .map(|entry| entry.value().clone())
            .collect()
    }
    
    pub async fn get_healthy_nodes_by_role(&self, role: NodeRole) -> Vec<NodeMetadata> {
        let cutoff = Utc::now() - chrono::Duration::from_std(self.heartbeat_timeout).unwrap();
        
        self.nodes.iter()
            .filter(|entry| {
                let node = entry.value();
                // Check if node is healthy
                node.last_heartbeat > cutoff && 
                node.state == NodeState::Running &&
                // Check if node has the requested role
                match &node.role {
                    NodeRole::Hybrid(roles) => roles.contains(&role),
                    r => r == &role,
                }
            })
            .map(|entry| entry.value().clone())
            .collect()
    }
    
    pub async fn start_health_monitor(&self) {
        let nodes = self.nodes.clone();
        let timeout = self.heartbeat_timeout;
        
        tokio::spawn(async move {
            let mut ticker = interval(Duration::from_secs(10));
            
            loop {
                ticker.tick().await;
                
                let cutoff = Utc::now() - chrono::Duration::from_std(timeout).unwrap();
                
                // Check for stale heartbeats
                for entry in nodes.iter() {
                    let node = entry.value();
                    if node.last_heartbeat < cutoff && node.state == NodeState::Running {
                        let node_id = entry.key().clone();
                        let mut updated_node = node.clone();
                        
                        warn!("Node {} missed heartbeat deadline", node_id.0);
                        updated_node.state = NodeState::Failed;
                        nodes.insert(node_id, updated_node);
                    }
                }
            }
        });
    }
    
    async fn handle_node_failure(&self, node_id: &NodeId) -> Result<()> {
        error!("Handling failure for node: {}", node_id.0);
        
        // Get node info
        if let Some(node) = self.nodes.get(node_id) {
            match &node.role {
                NodeRole::Ingest => {
                    // Redistribute ingestion load
                    let healthy_ingest = self.get_nodes_by_role(&NodeRole::Ingest).await
                        .into_iter()
                        .filter(|n| n.state == NodeState::Running)
                        .collect::<Vec<_>>();
                    
                    if healthy_ingest.is_empty() {
                        error!("No healthy ingest nodes available!");
                    } else {
                        info!("Redistributing load from {} to {} ingest nodes", 
                            node_id.0, healthy_ingest.len());
                    }
                }
                NodeRole::Search => {
                    // Leader election disabled - search nodes manage themselves
                    info!("Search node {} failed - nodes will coordinate themselves", node_id.0);
                }
                _ => {}
            }
        }
        
        Ok(())
    }
    
    async fn handle_node_shutdown(&self, node_id: &NodeId) -> Result<()> {
        info!("Node {} shutting down", node_id.0);
        
        // Remove from active nodes after grace period
        tokio::spawn({
            let nodes = self.nodes.clone();
            let node_id = node_id.clone();
            async move {
                tokio::time::sleep(Duration::from_secs(30)).await;
                nodes.remove(&node_id);
                info!("Node {} removed from registry", node_id.0);
            }
        });
        
        Ok(())
    }
    
    async fn broadcast_node_change(&self, node_id: &NodeId, event: &str) {
        info!("Broadcasting node {} event: {}", node_id.0, event);
        // This would integrate with the gossip protocol
    }
}