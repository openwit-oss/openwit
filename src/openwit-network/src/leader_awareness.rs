use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::{interval, Duration};
use tracing::info;
use serde::{Serialize, Deserialize};

use crate::ClusterHandle;

/// Leader status information propagated through gossip
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaderInfo {
    pub role_group: String,
    pub leader_node_id: String,
    pub term: u64,
    pub elected_at: String,
}

/// Tracks leader status for a node
#[derive(Clone)]
pub struct LeaderAwareness {
    node_id: String,
    role: String,
    cluster_handle: ClusterHandle,
    is_leader: Arc<RwLock<bool>>,
    leader_info: Arc<RwLock<Option<LeaderInfo>>>,
}

impl LeaderAwareness {
    pub fn new(node_id: String, role: String, cluster_handle: ClusterHandle) -> Self {
        Self {
            node_id,
            role,
            cluster_handle,
            is_leader: Arc::new(RwLock::new(false)),
            leader_info: Arc::new(RwLock::new(None)),
        }
    }
    
    /// Start monitoring leader status from gossip
    pub async fn start_monitoring(self: Arc<Self>) {
        info!("🔍 Starting leader awareness monitoring for {} node: {}", self.role, self.node_id);
        
        // Set initial leader status in gossip
        self.cluster_handle.set_self_kv("is_leader", "false").await;
        self.cluster_handle.set_self_kv("leader_term", "0").await;
        
        let self_clone = self.clone();
        tokio::spawn(async move {
            let mut ticker = interval(Duration::from_secs(2));
            
            loop {
                ticker.tick().await;
                self_clone.check_leader_status().await;
            }
        });
    }
    
    /// Check if this node is the leader based on gossip state
    async fn check_leader_status(&self) {
        let role_group = format!("{}_nodes", self.role);
        
        // Get all nodes with same role
        let _nodes = self.cluster_handle.view.get_nodes_with_role(&self.role).await;
        
        // Look for leader information in gossip
        let snapshot = self.cluster_handle.view.node_states.read().await;
        
        let mut current_leader: Option<(String, LeaderInfo)> = None;
        
        for node_state in &snapshot.node_states {
            let node_id = &node_state.chitchat_id().node_id;
            
            // Check if this node claims to be leader for our role group
            if let Some(leader_role_group) = node_state.get("leader_role_group") {
                if leader_role_group == role_group {
                    if let Some(leader_term) = node_state.get("leader_term") {
                        if let Ok(term) = leader_term.parse::<u64>() {
                            let info = LeaderInfo {
                                role_group: leader_role_group.to_string(),
                                leader_node_id: node_id.clone(),
                                term,
                                elected_at: node_state.get("leader_elected_at")
                                    .unwrap_or(&"unknown".to_string())
                                    .to_string(),
                            };
                            
                            // Keep the leader with highest term
                            if current_leader.is_none() || 
                               current_leader.as_ref().unwrap().1.term < term {
                                current_leader = Some((node_id.clone(), info));
                            }
                        }
                    }
                }
            }
        }
        
        // Update our leader status
        if let Some((leader_id, info)) = current_leader {
            let was_leader = *self.is_leader.read().await;
            let is_leader_now = leader_id == self.node_id;
            
            if is_leader_now != was_leader {
                *self.is_leader.write().await = is_leader_now;
                *self.leader_info.write().await = Some(info.clone());
                
                if is_leader_now {
                    info!("🎉 This node {} is now the LEADER for {} (term: {})", 
                          self.node_id, role_group, info.term);
                    self.on_became_leader().await;
                } else {
                    info!("📢 Node {} is now a FOLLOWER. Leader is {} (term: {})", 
                          self.node_id, leader_id, info.term);
                    self.on_became_follower().await;
                }
            }
        }
    }
    
    /// Called when this node becomes the leader
    async fn on_became_leader(&self) {
        // Update gossip state to reflect leader status
        self.cluster_handle.set_self_kv("is_leader", "true").await;
        
        // Trigger role-specific leader behaviors
        match self.role.as_str() {
            "ingest" => {
                info!("  → As ingest leader, will coordinate partition assignments");
            }
            "search" => {
                info!("  → As search leader, will coordinate query distribution");
            }
            "janitor" => {
                info!("  → As janitor leader, will coordinate cleanup tasks");
            }
            "indexer" => {
                info!("  → As indexer leader, will coordinate indexing jobs");
            }
            "storage" => {
                info!("  → As storage leader, will coordinate data placement");
            }
            _ => {}
        }
    }
    
    /// Called when this node becomes a follower
    async fn on_became_follower(&self) {
        // Update gossip state
        self.cluster_handle.set_self_kv("is_leader", "false").await;
        
        // Stop any leader-specific tasks
        match self.role.as_str() {
            "ingest" => {
                info!("  → As ingest follower, will accept partition assignments");
            }
            "search" => {
                info!("  → As search follower, will handle assigned queries");
            }
            "janitor" => {
                info!("  → As janitor follower, will execute assigned cleanup tasks");
            }
            "indexer" => {
                info!("  → As indexer follower, will process assigned indexing jobs");
            }
            "storage" => {
                info!("  → As storage follower, will store assigned data");
            }
            _ => {}
        }
    }
    
    /// Check if this node is currently the leader
    pub async fn is_leader(&self) -> bool {
        *self.is_leader.read().await
    }
    
    /// Get current leader information
    pub async fn get_leader_info(&self) -> Option<LeaderInfo> {
        self.leader_info.read().await.clone()
    }
    
    /// Handle leader announcement from control plane
    pub async fn handle_leader_announcement(&self, announcement: LeaderInfo) -> anyhow::Result<()> {
        let role_group = format!("{}_nodes", self.role);
        
        if announcement.role_group != role_group {
            return Ok(()); // Not for our role group
        }
        
        info!("📨 Received leader announcement: {} is leader for {} (term: {})",
              announcement.leader_node_id, announcement.role_group, announcement.term);
        
        // If we are announced as leader, update our gossip state
        if announcement.leader_node_id == self.node_id {
            self.cluster_handle.set_self_kv("leader_role_group", &announcement.role_group).await;
            self.cluster_handle.set_self_kv("leader_term", &announcement.term.to_string()).await;
            self.cluster_handle.set_self_kv("leader_elected_at", &announcement.elected_at).await;
            self.cluster_handle.set_self_kv("is_leader", "true").await;
            
            info!("✅ Updated gossip state as leader for {}", role_group);
        }
        
        Ok(())
    }
    
    /// Get all nodes in the same role group with their leader status
    pub async fn get_role_group_status(&self) -> Vec<(String, bool)> {
        let nodes = self.cluster_handle.view.get_nodes_with_role(&self.role).await;
        let snapshot = self.cluster_handle.view.node_states.read().await;
        
        let mut status = Vec::new();
        
        for node_id in nodes {
            let is_leader = snapshot.node_states.iter()
                .find(|n| n.chitchat_id().node_id == node_id)
                .and_then(|n| n.get("is_leader"))
                .map(|v| v == "true")
                .unwrap_or(false);
            
            status.push((node_id, is_leader));
        }
        
        status
    }
}

/// Extension trait for ClusterHandle to add leader awareness
pub trait ClusterHandleLeaderExt {
    /// Create and start leader awareness for this node
    fn with_leader_awareness(self, node_id: String, role: String) -> (Self, Arc<LeaderAwareness>)
    where
        Self: Sized;
}

impl ClusterHandleLeaderExt for ClusterHandle {
    fn with_leader_awareness(self, node_id: String, role: String) -> (Self, Arc<LeaderAwareness>) {
        let awareness = Arc::new(LeaderAwareness::new(
            node_id,
            role,
            self.clone(),
        ));
        
        // Start monitoring in background
        let awareness_clone = awareness.clone();
        tokio::spawn(async move {
            awareness_clone.start_monitoring().await;
        });
        
        (self, awareness)
    }
}