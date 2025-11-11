use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::info;

/// Address cache that stores node endpoints based on node role
/// Each node type only caches addresses for nodes it needs to communicate with
#[derive(Clone)]
pub struct NodeAddressCache {
    node_id: String,
    node_role: String,
    /// Control plane addresses (all nodes need these)
    control_addresses: Arc<RwLock<VecDeque<NodeAddress>>>,
    /// Role-specific addresses based on data flow
    target_addresses: Arc<RwLock<HashMap<String, VecDeque<NodeAddress>>>>,
    /// Round-robin counters
    counters: Arc<RwLock<HashMap<String, usize>>>,
}

#[derive(Debug, Clone)]
pub struct NodeAddress {
    pub node_id: String,
    pub grpc_endpoint: String,
    pub flight_endpoint: String,
    pub health_score: f32,
}

impl NodeAddressCache {
    pub fn new(node_id: String, node_role: String) -> Self {
        Self {
            node_id,
            node_role,
            control_addresses: Arc::new(RwLock::new(VecDeque::new())),
            target_addresses: Arc::new(RwLock::new(HashMap::new())),
            counters: Arc::new(RwLock::new(HashMap::new())),
        }
    }
    
    /// Update control plane addresses (all nodes need these)
    pub async fn update_control_addresses(&self, addresses: Vec<NodeAddress>) {
        let mut control = self.control_addresses.write().await;
        control.clear();
        control.extend(addresses);
        
        info!("Updated control addresses: {} nodes", control.len());
    }
    
    /// Update target addresses based on node role
    pub async fn update_target_addresses(&self, role: &str, addresses: Vec<NodeAddress>) {
        let mut targets = self.target_addresses.write().await;
        let queue = targets.entry(role.to_string()).or_insert_with(VecDeque::new);
        queue.clear();
        queue.extend(addresses);
        
        info!("Updated {} addresses for {}: {} nodes", role, self.node_role, queue.len());
    }
    
    /// Get next control node (round-robin)
    pub async fn get_next_control_node(&self) -> Option<NodeAddress> {
        let control = self.control_addresses.read().await;
        if control.is_empty() {
            return None;
        }
        
        let mut counters = self.counters.write().await;
        let counter = counters.entry("control".to_string()).or_insert(0);
        let index = *counter % control.len();
        *counter = (*counter + 1) % control.len();
        
        control.get(index).cloned()
    }
    
    /// Get next target node of specific role (round-robin)
    pub async fn get_next_target_node(&self, role: &str) -> Option<NodeAddress> {
        let targets = self.target_addresses.read().await;
        let nodes = targets.get(role)?;
        
        if nodes.is_empty() {
            return None;
        }
        
        let mut counters = self.counters.write().await;
        let counter = counters.entry(role.to_string()).or_insert(0);
        let index = *counter % nodes.len();
        *counter = (*counter + 1) % nodes.len();
        
        nodes.get(index).cloned()
    }
    
    /// Get all addresses of a specific role
    pub async fn get_all_addresses(&self, role: &str) -> Vec<NodeAddress> {
        if role == "control" {
            self.control_addresses.read().await.iter().cloned().collect()
        } else {
            self.target_addresses.read().await
                .get(role)
                .map(|nodes| nodes.iter().cloned().collect())
                .unwrap_or_default()
        }
    }
    
    /// Get addresses this node type should cache based on data flow
    pub fn get_required_roles(&self) -> Vec<&'static str> {
        match self.node_role.as_str() {
            "proxy" => vec!["control", "ingest"],
            "ingest" => vec!["control", "indexer", "storage"],
            "indexer" => vec!["control", "storage"],
            "storage" => vec!["control"],
            "search" => vec!["control", "storage", "indexer"],
            "janitor" => vec!["control", "storage"],
            "control" => vec![], // Control nodes don't need to cache others
            _ => vec!["control"],
        }
    }
    
    /// Cache statistics
    pub async fn get_cache_stats(&self) -> HashMap<String, usize> {
        let mut stats = HashMap::new();
        
        stats.insert("control".to_string(), self.control_addresses.read().await.len());
        
        let targets = self.target_addresses.read().await;
        for (role, addresses) in targets.iter() {
            stats.insert(role.clone(), addresses.len());
        }
        
        stats
    }
}

/// Helper to parse node endpoints from gossip or control plane
pub fn parse_node_endpoint(node_id: &str, metadata: &HashMap<String, String>) -> NodeAddress {
    // Extract ports from metadata or use defaults
    let grpc_port = metadata.get("grpc_port")
        .and_then(|p| p.parse::<u16>().ok())
        .unwrap_or(8081);
        
    // Arrow Flight port determined by role for data transfer only
    let role = metadata.get("role").map(|s| s.as_str()).unwrap_or("unknown");
    let flight_port = super::arrow_flight_service::get_flight_port(role);
        
    let health_score = metadata.get("health_score")
        .and_then(|s| s.parse::<f32>().ok())
        .unwrap_or(1.0);
    
    NodeAddress {
        node_id: node_id.to_string(),
        grpc_endpoint: format!("{}:{}", node_id, grpc_port),
        flight_endpoint: format!("{}:{}", node_id, flight_port),
        health_score,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_address_cache_round_robin() {
        let cache = NodeAddressCache::new("test-node".to_string(), "proxy".to_string());
        
        let control_nodes = vec![
            NodeAddress {
                node_id: "control-1".to_string(),
                grpc_endpoint: "control-1:8081".to_string(),
                flight_endpoint: "control-1:8093".to_string(),
                health_score: 1.0,
            },
            NodeAddress {
                node_id: "control-2".to_string(),
                grpc_endpoint: "control-2:8081".to_string(),
                flight_endpoint: "control-2:8093".to_string(),
                health_score: 1.0,
            },
        ];
        
        cache.update_control_addresses(control_nodes).await;
        
        // Test round-robin
        let first = cache.get_next_control_node().await.unwrap();
        assert_eq!(first.node_id, "control-1");
        
        let second = cache.get_next_control_node().await.unwrap();
        assert_eq!(second.node_id, "control-2");
        
        let third = cache.get_next_control_node().await.unwrap();
        assert_eq!(third.node_id, "control-1"); // Back to first
    }
    
    #[test]
    fn test_required_roles() {
        let proxy_cache = NodeAddressCache::new("proxy-1".to_string(), "proxy".to_string());
        assert_eq!(proxy_cache.get_required_roles(), vec!["control", "ingest"]);
        
        let ingest_cache = NodeAddressCache::new("ingest-1".to_string(), "ingest".to_string());
        assert_eq!(ingest_cache.get_required_roles(), vec!["control", "indexer", "storage"]);
    }
}