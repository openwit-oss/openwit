use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use anyhow::{Result, anyhow};
use tokio::sync::RwLock;
use tokio::time::interval;
use tracing::{info, warn};

use openwit_network::ClusterHandle;
use openwit_config::unified::mandatory_nodes::MandatoryNodesConfig;

#[derive(Clone)]
pub struct MandatoryNodeChecker {
    config: MandatoryNodesConfig,
    cluster_handle: ClusterHandle,
    verified: Arc<RwLock<bool>>,
}

impl MandatoryNodeChecker {
    pub fn new(config: MandatoryNodesConfig, cluster_handle: ClusterHandle) -> Self {
        Self {
            config,
            cluster_handle,
            verified: Arc::new(RwLock::new(false)),
        }
    }
    
    /// Check if all mandatory nodes are present and healthy
    pub async fn verify_mandatory_nodes(&self) -> Result<()> {
        // Skip verification if no required types are specified
        if self.config.required_types.is_empty() && self.config.minimum_counts.is_empty() {
            info!("✅ No mandatory nodes configured - proceeding with startup");
            *self.verified.write().await = true;
            return Ok(());
        }
        
        info!("🔍 Verifying mandatory nodes before startup...");
        info!("   Required node types: {:?}", self.config.required_types);
        info!("   Minimum counts: {:?}", self.config.minimum_counts);
        
        let start_time = std::time::Instant::now();
        let timeout = self.config.health_check.timeout_seconds as u64;
        let check_interval = Duration::from_secs(self.config.health_check.interval_seconds as u64);
        
        let mut ticker = interval(check_interval);
        let mut last_missing_report = std::time::Instant::now();
        
        loop {
            ticker.tick().await;
            
            // Check timeout
            if start_time.elapsed() > Duration::from_secs(timeout) {
                if self.config.missing_node_policy.emergency_shutdown {
                    return Err(anyhow!("Mandatory node verification timeout after {}s - emergency shutdown", timeout));
                } else if self.config.missing_node_policy.allow_partial_operation {
                    warn!("⚠️  Mandatory node verification timeout - proceeding with partial operation");
                    break;
                } else {
                    // Continue waiting indefinitely
                    if last_missing_report.elapsed() > Duration::from_secs(60) {
                        warn!("⏳ Still waiting for mandatory nodes after {} seconds...", 
                              start_time.elapsed().as_secs());
                        last_missing_report = std::time::Instant::now();
                    }
                }
            }
            
            // Get current cluster state
            let roles = self.cluster_handle.view.get_roles().await;
            let node_counts = self.count_nodes_by_role(&roles);
            
            // Check if all requirements are met
            let (all_present, missing) = self.check_requirements(&node_counts);
            
            if all_present {
                *self.verified.write().await = true;
                info!("");
                info!("🎉 Congratulations! All nodes have successfully been found. Let us begin!");
                info!("════════════════════════════════════════════════════════════════════");
                self.log_node_summary(&roles).await;
                info!("════════════════════════════════════════════════════════════════════");
                info!("");
                return Ok(());
            }
            
            // Report missing nodes periodically (every 5 seconds)
            if last_missing_report.elapsed() > Duration::from_secs(5) {
                self.report_missing_nodes(&missing, &node_counts);
                last_missing_report = std::time::Instant::now();
            }
        }
        
        Ok(())
    }
    
    /// Count nodes by role
    fn count_nodes_by_role(&self, roles: &HashMap<String, String>) -> HashMap<String, u32> {
        let mut counts = HashMap::new();
        
        for (_, role) in roles {
            *counts.entry(role.clone()).or_insert(0) += 1;
        }
        
        counts
    }
    
    /// Check if all requirements are met
    fn check_requirements(&self, node_counts: &HashMap<String, u32>) -> (bool, Vec<(String, u32, u32)>) {
        let mut all_present = true;
        let mut missing = Vec::new();
        
        // Check required types
        for required_type in &self.config.required_types {
            if !node_counts.contains_key(required_type) {
                all_present = false;
                let required = self.config.minimum_counts.get(required_type).copied().unwrap_or(1);
                missing.push((required_type.clone(), 0, required));
            }
        }
        
        // Check minimum counts
        for (node_type, min_count) in &self.config.minimum_counts {
            let current_count = node_counts.get(node_type).copied().unwrap_or(0);
            if current_count < *min_count {
                all_present = false;
                missing.push((node_type.clone(), current_count, *min_count));
            }
        }
        
        (all_present, missing)
    }
    
    /// Report missing nodes
    fn report_missing_nodes(&self, missing: &[(String, u32, u32)], node_counts: &HashMap<String, u32>) {
        info!("Waiting for mandatory nodes to join the cluster...");
        
        for (node_type, current, required) in missing {
            if *current == 0 {
                info!("   Waiting for {} node(s) of type '{}' (0/{} found)", 
                      required, node_type, required);
            } else {
                info!("   Waiting for {} more {} node(s) ({}/{} found)", 
                      required - current, node_type, current, required);
            }
        }
        
        // Show what we have found
        if !node_counts.is_empty() {
            info!("   ✓ Currently discovered nodes:");
            for (role, count) in node_counts {
                info!("     - {}: {} node(s)", role, count);
            }
        }
        
        info!("   Checking again in {} seconds...", 
              self.config.health_check.interval_seconds);
    }
    
    /// Log final node summary
    async fn log_node_summary(&self, roles: &HashMap<String, String>) {
        info!("Cluster Node Summary:");
        
        // Group nodes by role
        let mut nodes_by_role: HashMap<String, Vec<String>> = HashMap::new();
        for (node_id, role) in roles {
            nodes_by_role.entry(role.clone()).or_insert_with(Vec::new).push(node_id.clone());
        }
        
        // Display each role group
        for (role, nodes) in nodes_by_role {
            info!("   {} nodes ({} total):", role.to_uppercase(), nodes.len());
            for node in nodes {
                info!("     - {}", node);
            }
        }
    }
    
    /// Check if verification is complete
    pub async fn is_verified(&self) -> bool {
        *self.verified.read().await
    }
    
    /// Block until verification is complete
    pub async fn wait_for_verification(&self) -> Result<()> {
        if self.is_verified().await {
            return Ok(());
        }
        
        self.verify_mandatory_nodes().await
    }
    
    /// Get verification status for health checks
    pub async fn get_status(&self) -> MandatoryNodeStatus {
        let roles = self.cluster_handle.view.get_roles().await;
        let node_counts = self.count_nodes_by_role(&roles);
        let (all_present, missing) = self.check_requirements(&node_counts);
        
        MandatoryNodeStatus {
            verified: *self.verified.read().await,
            all_requirements_met: all_present,
            node_counts,
            missing_nodes: missing,
        }
    }
}

#[derive(Debug, Clone)]
pub struct MandatoryNodeStatus {
    pub verified: bool,
    pub all_requirements_met: bool,
    pub node_counts: HashMap<String, u32>,
    pub missing_nodes: Vec<(String, u32, u32)>, // (type, current, required)
}

