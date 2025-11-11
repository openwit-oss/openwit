use std::sync::{Arc, Mutex};
use anyhow::Result;
use tracing::{info};

use openwit_network::{ClusterRuntime, ClusterHandle};
use openwit_config::UnifiedConfig;
use openwit_metrics::{HealthMonitor, NodeHealth};

/// Integration with control plane and network
#[allow(dead_code)]
pub struct ProxyControlIntegration {
    node_id: String,
    cluster_handle: ClusterHandle,
    config: UnifiedConfig,
    health_monitor: Arc<Mutex<HealthMonitor>>,
}

impl ProxyControlIntegration {
    pub async fn new(
        node_id: String,
        config: UnifiedConfig,
    ) -> Result<Self> {
        info!("Initializing proxy control plane integration");
        
        // Start network (gossip functionality deprecated)
        let listen_addr = "127.0.0.1:7946".to_string();
        let seeds = Vec::<String>::new();
        
        let runtime = if config.deployment.kubernetes.enabled {
            ClusterRuntime::new_with_k8s(
                &node_id,
                &listen_addr,
                seeds,
                Some(&config.deployment.kubernetes),
                "proxy", // Proxy role
            ).await?
        } else {
            ClusterRuntime::new(
                &node_id,
                &listen_addr,
                seeds,
            ).await?
        };
        
        // Create health monitor with default thresholds
        let health_monitor = Arc::new(Mutex::new(
            HealthMonitor::new(openwit_config::unified::NodeHealthThresholds::default())
        ));
        
        // Get cluster handle with health monitor
        let cluster_handle = runtime.handle()
            .with_health_monitor(health_monitor.clone());
        
        // Set proxy-specific metadata (gossip functionality deprecated)
        cluster_handle.set_self_kv("node_type", "proxy").await;
        cluster_handle.set_self_kv("proxy_strategy", "round_robin").await;
        cluster_handle.set_self_kv("proxy_version", env!("CARGO_PKG_VERSION")).await;
        
        info!("Proxy node {} joined cluster with health monitoring", node_id);
        
        Ok(Self {
            node_id,
            cluster_handle,
            config,
            health_monitor,
        })
    }
    
    /// Get cluster handle for other components
    pub fn cluster_handle(&self) -> ClusterHandle {
        self.cluster_handle.clone()
    }
    
    /// Report proxy health to control plane
    pub async fn report_health(&self, stats: ProxyHealthStats) -> Result<()> {
        // Get system health from monitor
        let node_health = if let Ok(mut monitor) = self.health_monitor.lock() {
            monitor.calculate_health()
        } else {
            NodeHealth {
                status: openwit_metrics::HealthStatus::Warning,
                cpu_percent: 0.0,
                memory_percent: 0.0,
                disk_percent: 0.0,
                network_percent: 0.0,
                timestamp: chrono::Utc::now().to_rfc3339(),
                details: "Failed to acquire health monitor lock".to_string(),
            }
        };
        
        let health_data = serde_json::json!({
            "buffer_size": stats.buffer_size,
            "ingestor_count": stats.available_ingestors,
            "requests_routed": stats.requests_routed,
            "kafka_messages": stats.kafka_messages_consumed,
            "errors": stats.error_count,
            "system_health": {
                "status": node_health.status,
                "cpu_percent": node_health.cpu_percent,
                "memory_percent": node_health.memory_percent,
                "disk_percent": node_health.disk_percent,
                "details": node_health.details,
            }
        });
        
        self.cluster_handle.set_self_kv("proxy_health", &health_data.to_string()).await;
        
        Ok(())
    }
    
    /// Get current health status
    pub fn get_health(&self) -> Option<NodeHealth> {
        self.health_monitor.lock().ok()
            .map(|mut monitor| monitor.calculate_health())
    }
    
    /// Update routing configuration from control plane
    pub async fn get_routing_config(&self) -> RoutingConfig {
        // In future, could get dynamic config from control plane
        // For now, use defaults
        RoutingConfig {
            strategy: RoutingStrategy::RoundRobin,
            health_check_interval_ms: 5000,
            max_retries: 3,
        }
    }
    
    /// Register with control plane as mandatory node
    pub async fn register_as_mandatory(&self) -> Result<()> {
        info!("Registering proxy as mandatory node with control plane");
        
        // Set mandatory node marker (gossip functionality deprecated)  
        self.cluster_handle.set_self_kv("mandatory_node", "true").await;
        self.cluster_handle.set_self_kv("mandatory_role", "proxy").await;
        
        // Control plane will detect this via monitoring
        
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct ProxyHealthStats {
    pub buffer_size: usize,
    pub available_ingestors: usize,
    pub requests_routed: u64,
    pub kafka_messages_consumed: u64,
    pub error_count: u64,
}

#[derive(Debug, Clone)]
pub struct RoutingConfig {
    pub strategy: RoutingStrategy,
    pub health_check_interval_ms: u64,
    pub max_retries: u32,
}

#[derive(Debug, Clone)]
pub enum RoutingStrategy {
    RoundRobin,
    LeastConnections,
    Random,
    WeightedRoundRobin(Vec<(String, u32)>), // (node_id, weight)
}