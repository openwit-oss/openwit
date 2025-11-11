use std::sync::Arc;
use std::time::Duration;
use anyhow::Result;
use tokio::sync::RwLock;
use tokio::time::interval;
use tracing::{info, warn};

use openwit_network::{ClusterHandle, LeaderAwareness};
use crate::cleanup::JanitorService;

/// Integrates janitor service with leader awareness
pub struct LeaderAwareJanitor {
    janitor_service: Arc<JanitorService>,
    leader_awareness: Arc<LeaderAwareness>,
    cluster_handle: ClusterHandle,
    leader_task_handle: Arc<RwLock<Option<tokio::task::JoinHandle<()>>>>,
}

impl LeaderAwareJanitor {
    pub fn new(
        janitor_service: Arc<JanitorService>,
        leader_awareness: Arc<LeaderAwareness>,
        cluster_handle: ClusterHandle,
    ) -> Self {
        Self {
            janitor_service,
            leader_awareness,
            cluster_handle,
            leader_task_handle: Arc::new(RwLock::new(None)),
        }
    }
    
    /// Start monitoring leader status and adjust behavior
    pub async fn start(self: Arc<Self>) -> Result<()> {
        info!("🧹 Starting leader-aware janitor service");
        
        let self_clone = self.clone();
        tokio::spawn(async move {
            let mut ticker = interval(Duration::from_secs(5));
            let mut was_leader = false;
            
            loop {
                ticker.tick().await;
                
                let is_leader = self_clone.leader_awareness.is_leader().await;
                
                if is_leader != was_leader {
                    if is_leader {
                        self_clone.on_became_leader().await;
                    } else {
                        self_clone.on_became_follower().await;
                    }
                    was_leader = is_leader;
                }
            }
        });
        
        Ok(())
    }
    
    /// Called when this janitor becomes the leader
    async fn on_became_leader(&self) {
        info!("🎉 Janitor became LEADER - starting coordination tasks");
        
        // Cancel any existing follower tasks
        if let Some(handle) = self.leader_task_handle.write().await.take() {
            handle.abort();
        }
        
        // Start leader-specific tasks
        let janitor = self.janitor_service.clone();
        let cluster = self.cluster_handle.clone();
        
        let handle = tokio::spawn(async move {
            info!("  → Scheduling cleanup tasks across janitor nodes");
            
            let mut ticker = interval(Duration::from_secs(60)); // Check every minute
            
            loop {
                ticker.tick().await;
                
                // Get all janitor nodes
                let janitor_nodes = cluster.view.get_nodes_with_role("janitor").await;
                info!("  → Found {} janitor nodes to coordinate", janitor_nodes.len());
                
                // Distribute cleanup tasks
                for (idx, node_id) in janitor_nodes.iter().enumerate() {
                    let task_type = match idx % 3 {
                        0 => "wal_cleanup",
                        1 => "buffer_cleanup", 
                        2 => "segment_compaction",
                        _ => unreachable!(),
                    };
                    
                    // Set task assignment in gossip
                    let key = format!("janitor_task:{}", node_id);
                    cluster.set_self_kv(&key, task_type).await;
                    
                    info!("  → Assigned {} to {}", task_type, node_id);
                }
                
                // Monitor disk usage across cluster
                let usage_key = "cluster_disk_usage_check";
                cluster.set_self_kv(usage_key, &chrono::Utc::now().to_rfc3339()).await;
                
                // Schedule index optimization
                if chrono::Utc::now().hour() == 2 { // 2 AM
                    info!("  → Scheduling nightly index optimization");
                    cluster.set_self_kv("index_optimization", "scheduled").await;
                }
            }
        });
        
        *self.leader_task_handle.write().await = Some(handle);
        
        // Update gossip state
        self.cluster_handle.set_self_kv("janitor_leader_active", "true").await;
        self.cluster_handle.set_self_kv("cleanup_schedule", "coordinating").await;
    }
    
    /// Called when this janitor becomes a follower
    async fn on_became_follower(&self) {
        info!("📍 Janitor became FOLLOWER - executing assigned tasks");
        
        // Cancel any leader tasks
        if let Some(handle) = self.leader_task_handle.write().await.take() {
            handle.abort();
        }
        
        // Start follower task executor
        let janitor = self.janitor_service.clone();
        let cluster = self.cluster_handle.clone();
        let node_id = cluster.self_id().to_string();
        
        let handle = tokio::spawn(async move {
            let mut ticker = interval(Duration::from_secs(30));
            
            loop {
                ticker.tick().await;
                
                // Check for assigned tasks in gossip
                let task_key = format!("janitor_task:{}", node_id);
                
                // In real implementation, would read from gossip
                // For now, simulate task execution
                info!("  → Checking for assigned cleanup tasks");
                
                // Execute assigned task type
                match janitor.clean_buffer_files().await {
                    Ok(cleaned) => {
                        info!("  → Cleaned {} buffer files", cleaned);
                        cluster.set_self_kv("last_cleanup", &chrono::Utc::now().to_rfc3339()).await;
                    }
                    Err(e) => {
                        warn!("  → Cleanup failed: {}", e);
                    }
                }
            }
        });
        
        *self.leader_task_handle.write().await = Some(handle);
        
        // Update gossip state
        self.cluster_handle.set_self_kv("janitor_leader_active", "false").await;
        self.cluster_handle.set_self_kv("cleanup_schedule", "following").await;
    }
    
    /// Get current leader node ID
    pub async fn get_leader_node_id(&self) -> Option<String> {
        self.leader_awareness.get_leader_info().await
            .map(|info| info.leader_node_id)
    }
    
    /// Check if ready to perform cleanup (leader decision)
    pub async fn should_perform_cleanup(&self) -> bool {
        let is_leader = self.leader_awareness.is_leader().await;
        
        if is_leader {
            // Leader decides based on cluster state
            true
        } else {
            // Followers check if they have assigned tasks
            // In real implementation, would check gossip for assignments
            true
        }
    }
}