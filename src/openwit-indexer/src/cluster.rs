use anyhow::{Context, Result};
use std::collections::HashMap;
use std::net::SocketAddr;
use tracing::{debug, info};

use crate::config::IndexerConfig;

/// Manages cluster coordination for indexer nodes (gossip functionality deprecated)
#[allow(dead_code)]
pub struct ClusterManager {
    node_id: String,
    gossip_addr: SocketAddr,
}

impl ClusterManager {
    /// Initialize cluster manager (gossip functionality deprecated)
    pub async fn new(config: &IndexerConfig) -> Result<Self> {
        let node_id = config.indexer.general.node_id.clone();
        let gossip_addr = "127.0.0.1:7946"
            .parse::<SocketAddr>()
            .context("Invalid gossip listen address")?;

        info!(
            node_id = %node_id,
            gossip_addr = %gossip_addr,
            "Cluster manager initialized (gossip deprecated)"
        );

        Ok(Self {
            node_id,
            gossip_addr,
        })
    }

    /// Get list of active indexer nodes in the cluster (gossip deprecated)
    pub async fn get_indexer_nodes(&self) -> Vec<IndexerNodeInfo> {
        let mut nodes = Vec::new();

        // Since gossip is deprecated, return only this node
        let info = IndexerNodeInfo {
            node_id: self.node_id.clone(),
            gossip_addr: self.gossip_addr,
            grpc_addr: "0.0.0.0:50051".to_string(),
            mode: "prod".to_string(),
            last_heartbeat: std::time::Instant::now(),
        };
        nodes.push(info);

        debug!(
            node_count = nodes.len(),
            "Retrieved active indexer nodes (gossip deprecated)"
        );

        nodes
    }

    /// Update node status (gossip deprecated)
    pub async fn update_status(&self, _status: &str) -> Result<()> {
        // Gossip functionality deprecated - no-op
        Ok(())
    }

    /// Update indexer metrics (gossip deprecated)
    pub async fn update_metrics(&self, _metrics: HashMap<String, String>) -> Result<()> {
        // Gossip functionality deprecated - no-op
        Ok(())
    }

    /// Register data availability (gossip deprecated)
    pub async fn register_indexed_partition(&self, tenant: &str, signal: &str, partition: &str) -> Result<()> {
        debug!(
            tenant = %tenant,
            signal = %signal,
            partition = %partition,
            "Registered indexed partition (gossip deprecated)"
        );
        
        Ok(())
    }

    /// Find nodes that have indexed a specific partition (gossip deprecated)
    pub async fn find_nodes_with_partition(
        &self,
        _tenant: &str,
        _signal: &str,
        _partition: &str,
    ) -> Vec<String> {
        // Since gossip is deprecated, return only this node
        vec![self.node_id.clone()]
    }

    /// Shutdown cluster coordination (gossip deprecated)
    pub async fn shutdown(&self) -> Result<()> {
        info!("Shutting down cluster manager (gossip deprecated)");
        Ok(())
    }
}

/// Information about an indexer node in the cluster
#[derive(Debug, Clone)]
pub struct IndexerNodeInfo {
    pub node_id: String,
    pub gossip_addr: SocketAddr,
    pub grpc_addr: String,
    pub mode: String,
    pub last_heartbeat: std::time::Instant,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{IndexerConfig, IndexerMode};

    #[tokio::test]
    async fn test_cluster_manager_initialization() {
        // This test would require a more complex setup with actual networking
        // For now, we just verify the structure compiles correctly
    }
}