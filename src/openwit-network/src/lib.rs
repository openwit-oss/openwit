use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Duration};
use std::sync::Mutex;

use chitchat::{ChitchatConfig, ChitchatHandle, ChitchatId, FailureDetectorConfig, spawn_chitchat};
use chitchat::transport::UdpTransport;
use chitchat::ClusterStateSnapshot;

use tokio::sync::RwLock;
use tracing::{info, debug};

pub mod discovery;
pub mod k8s_discovery;
pub mod leader_awareness;

pub use discovery::{ServiceDiscovery, is_kubernetes_environment};
pub use leader_awareness::{LeaderAwareness, LeaderInfo, ClusterHandleLeaderExt};

#[derive(Clone)]
pub struct ClusterView {
    pub node_states: Arc<RwLock<ClusterStateSnapshot>>,
}

impl ClusterView {
    pub fn new() -> Self {
        Self {
            node_states: Arc::new(RwLock::new(ClusterStateSnapshot {
                node_states: vec![],
                seed_addrs: Default::default(),
            })),
        }
    }

    pub async fn update(&self, snapshot: ClusterStateSnapshot) {
        *self.node_states.write().await = snapshot;
    }

    pub async fn get_roles(&self) -> HashMap<String, String> {
        let snapshot = self.node_states.read().await;
        snapshot.node_states.iter().filter_map(|node| {
            let node_id = node.chitchat_id().node_id.clone();
            let role = node.key_values_including_deleted()
                .find(|(k, _)| *k == "role")
                .map(|(_, v)| v.value.clone());
            role.map(|r| (node_id, r))
        }).collect()
    }
    pub async fn get_nodes_with_role(&self, target_role: &str) -> Vec<String> {
        let snapshot = self.node_states.read().await;
        snapshot.node_states.iter().filter_map(|node| {
            let node_id = node.chitchat_id().node_id.clone();
            let role = node.key_values_including_deleted()
                .find(|(k, _)| *k == "role")
                .map(|(_, v)| v.value.clone());

            match role {
                Some(ref r) if r == target_role => Some(node_id),
                _ => None,
            }
        }).collect()
    }
    
    /// Get healthy nodes with a specific role
    pub async fn get_healthy_nodes_with_role(&self, target_role: &str) -> Vec<String> {
        let snapshot = self.node_states.read().await;
        snapshot.node_states.iter().filter_map(|node| {
            let node_id = node.chitchat_id().node_id.clone();
            
            // Check role
            let role = node.key_values_including_deleted()
                .find(|(k, _)| *k == "role")
                .map(|(_, v)| v.value.clone());
            
            if role.as_ref().map(|r| r == target_role).unwrap_or(false) {
                // Check health status
                if let Some((_, health_value)) = node.key_values_including_deleted()
                    .find(|(k, _)| *k == "health") {
                    // Parse health JSON
                    if let Ok(health_json) = serde_json::from_str::<serde_json::Value>(&health_value.value) {
                        if let Some(status) = health_json.get("status").and_then(|s| s.as_str()) {
                            // Only include healthy nodes
                            if status == "Healthy" || status == "Warning" {
                                return Some(node_id);
                            }
                        }
                    }
                } else {
                    // If no health data, assume healthy (for backward compatibility)
                    return Some(node_id);
                }
            }
            None
        }).collect()
    }

}

pub struct ClusterRuntime {
    pub self_id: String,
    pub handle: Arc<ChitchatHandle>,
    pub view: ClusterView,
}

impl ClusterRuntime {
    /// Create a new cluster runtime with automatic K8s discovery
    pub async fn new(
        self_node_name: &str,
        listen_addr: &str,
        static_seeds: Vec<String>,
    ) -> anyhow::Result<Self> {
        let listen_addr = listen_addr.parse::<SocketAddr>()?;
        let seed_addrs: Vec<SocketAddr> = static_seeds
            .iter()
            .filter_map(|s| s.parse().ok())
            .collect();
        
        // Default to monolith role
        Self::start(self_node_name, listen_addr, seed_addrs, "monolith").await
    }
    
    /// Create with K8s configuration
    pub async fn new_with_k8s(
        self_node_name: &str,
        listen_addr: &str,
        static_seeds: Vec<String>,
        k8s_config: Option<&openwit_config::unified::KubernetesConfig>,
        role: &str,
    ) -> anyhow::Result<Self> {
        if k8s_config.is_some() {
            info!("🌐 Kubernetes mode detected - using headless service discovery");
        }
        
        let listen_addr_parsed = listen_addr.parse::<SocketAddr>()?;
        
        // Combine static and K8s discovered seeds
        let all_seeds = k8s_discovery::create_seed_nodes(static_seeds, k8s_config).await?;
        
        let seed_addrs: Vec<SocketAddr> = all_seeds
            .iter()
            .filter_map(|s| s.parse().ok())
            .collect();
        
        Self::start(self_node_name, listen_addr_parsed, seed_addrs, role).await
    }
    
    /// Starts the chitchat runtime, sets the node role, and spawns a background snapshot poller.
    pub async fn start(
        self_node_name: &str,
        listen_addr: SocketAddr,
        seeds: Vec<SocketAddr>,
        role: &str,
    ) -> anyhow::Result<Self> {
        Self::start_with_config(self_node_name, listen_addr, seeds, role, None).await
    }
    
    /// Starts with UnifiedConfig for service endpoint discovery
    pub async fn start_with_config(
        self_node_name: &str,
        listen_addr: SocketAddr,
        seeds: Vec<SocketAddr>,
        role: &str,
        config: Option<&openwit_config::UnifiedConfig>,
    ) -> anyhow::Result<Self> {
        // Use the node name directly without role suffix
        let node_id_str = self_node_name.to_string();
        let node_id = ChitchatId::new(node_id_str.clone(), 0, listen_addr);

        debug!("Starting cluster node: {}", node_id_str);
        debug!("   Role: {}", role);
        debug!("   Listen address: {}", listen_addr);
        debug!("   Initial seeds: {:?}", seeds);
        
        // Setup service discovery
        // Check deployment mode from config or environment
        let deployment_mode = std::env::var("OPENWIT_DEPLOYMENT_MODE")
            .unwrap_or_else(|_| "auto".to_string());
        
        // Force local mode if explicitly set
        let is_k8s = if deployment_mode == "local" {
            debug!("Using local deployment mode (Kubernetes discovery disabled)");
            false
        } else if deployment_mode == "kubernetes" {
            debug!("Using Kubernetes deployment mode");
            true
        } else {
            // Auto-detect based on environment
            let detected = is_kubernetes_environment();
            debug!("Environment detected: {}", if detected { "Kubernetes" } else { "Local" });
            detected
        };
        
        // Use config-based discovery if available
        let discovery = if let Some(cfg) = config {
            Arc::new(ServiceDiscovery::new_with_endpoints(
                role.to_string(), 
                is_k8s, 
                Some(&cfg.networking.service_endpoints)
            ))
        } else {
            Arc::new(ServiceDiscovery::new(role.to_string(), is_k8s))
        };
        
        // Get initial peers
        let mut all_seeds = seeds;
        if let Ok(discovered) = discovery.discover_peers().await {
            debug!("Discovered {} additional peers via service discovery", discovered.len());
            all_seeds.extend(discovered);
        }

        // Remove duplicates and self
        all_seeds.sort();
        all_seeds.dedup();
        all_seeds.retain(|addr| addr != &listen_addr);

        debug!("Final seed list: {} peers", all_seeds.len());
        
        let seed_strings: Vec<String> = all_seeds.iter().map(|s| s.to_string()).collect();

        let config = ChitchatConfig {
            chitchat_id: node_id.clone(),
            cluster_id: "openwit-cluster".into(),
            gossip_interval: Duration::from_millis(500),
            listen_addr,
            seed_nodes: seed_strings,
            failure_detector_config: FailureDetectorConfig::default(),
            extra_liveness_predicate: None,
            marked_for_deletion_grace_period: Duration::from_secs(60),
            catchup_callback: None,
        };

        debug!("Chitchat configuration: gossip_interval=500ms, cluster_id=openwit-cluster");

        let _transport = UdpTransport;
        let raw_handle: ChitchatHandle =
        spawn_chitchat(config, Vec::new(), &UdpTransport).await?;
        let handle = Arc::new(raw_handle);
        
        // Set initial metadata in gossip state
        handle.with_chitchat(|c| {
            c.self_node_state().set("role", role);
            c.self_node_state().set("started_at", &chrono::Utc::now().to_rfc3339());
            c.self_node_state().set("version", env!("CARGO_PKG_VERSION"));
            debug!("Initial gossip metadata set");
        }).await;
        
        // Start periodic re-discovery
        discovery.clone().start_periodic_discovery(60).await;

        // Set up snapshot view poller
        let view = ClusterView::new();
        let h_clone = handle.clone();
        let v_clone = view.clone();

        Self::spawn_snapshot_poller(h_clone, v_clone);

        

        Ok(Self {
            self_id: node_id.node_id.clone(),
            handle,
            view,
        })
    }
    pub fn handle(&self) -> ClusterHandle {
        ClusterHandle::new(self.self_id.clone(), self.view.clone(),self.handle.clone())
    }
    pub fn spawn_snapshot_poller(handle: Arc<ChitchatHandle>, view: ClusterView) {
        tokio::spawn(async move {
            let mut last_node_count = 0;
            let mut last_heartbeat_log = std::time::Instant::now();
            
            loop {
                let snapshot: ClusterStateSnapshot = handle
                    .with_chitchat(|c| c.state_snapshot())
                    .await;
                
                let node_count = snapshot.node_states.len();
                
                // Log when cluster membership changes
                if node_count != last_node_count {
                    debug!("Cluster membership changed: {} nodes", node_count);
                    for node_state in &snapshot.node_states {
                        let node_id = &node_state.chitchat_id().node_id;
                        let role = node_state.get("role").unwrap_or("unknown");
                        let heartbeat = node_state.get("heartbeat").unwrap_or("none");
                        debug!("   {} (role: {}, last heartbeat: {})", node_id, role, heartbeat);
                    }
                    last_node_count = node_count;
                }
                
                // Periodically log heartbeat status (every 30 seconds)
                if last_heartbeat_log.elapsed() > Duration::from_secs(30) {
                    debug!("Heartbeat status for {} nodes:", node_count);
                    for node_state in &snapshot.node_states {
                        let node_id = &node_state.chitchat_id().node_id;
                        if let Some(heartbeat_data) = node_state.get("heartbeat") {
                            debug!("   - {}: {}", node_id, heartbeat_data);
                        }
                    }
                    last_heartbeat_log = std::time::Instant::now();
                }
                
                view.update(snapshot).await;
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        });
    }

}

#[derive(Clone)]
pub struct ClusterHandle {
    pub self_id: String,
    pub view: ClusterView,
    pub(crate) handle: Arc<ChitchatHandle>,
    health_monitor: Option<Arc<Mutex<openwit_metrics::HealthMonitor>>>,
}

impl ClusterHandle {
    pub fn new(self_id: String, view: ClusterView,handle:Arc<ChitchatHandle>) -> Self {
        let cluster = Self { 
            self_id, 
            view,
            handle,
            health_monitor: None,
        };
        
        // Start heartbeat sender
        cluster.start_heartbeat_sender();
        
        cluster
    }
    
    pub fn with_health_monitor(mut self, monitor: Arc<Mutex<openwit_metrics::HealthMonitor>>) -> Self {
        self.health_monitor = Some(monitor);
        self
    }
    
    pub async fn set_self_kv(&self, key: &str, value: &str) {
        self.handle.with_chitchat(|c| {
            c.self_node_state().set(key, value)
        }).await;
        debug!("📝 Set gossip KV: {} = {}", key, value);
    }
    
    pub fn chitchat_handle(&self) -> Arc<ChitchatHandle> {
        self.handle.clone()
    }

    pub fn self_id(&self) -> &str {
        &self.self_id
    }
    
    pub async fn self_node_id(&self) -> String {
        self.self_id.clone()
    }
    
    pub async fn dump_state(&self) -> anyhow::Result<HashMap<String, String>> {
        let snapshot = self.view.node_states.read().await;
        let mut state = HashMap::new();
        
        for node_state in &snapshot.node_states {
            let node_id = &node_state.chitchat_id().node_id;
            for (key, versioned_value) in node_state.key_values_including_deleted() {
                let full_key = format!("{}:{}", node_id, key);
                state.insert(full_key, versioned_value.value.clone());
            }
        }
        
        Ok(state)
    }

    pub fn view(&self) -> &ClusterView {
        &self.view
    }
    
    /// Send heartbeat with node status
    pub async fn send_heartbeat(&self, status: serde_json::Value) {
        let heartbeat = serde_json::json!({
            "timestamp": chrono::Utc::now().to_rfc3339(),
            "status": status
        });
        
        self.handle.with_chitchat(|c| {
            c.self_node_state().set("heartbeat", &heartbeat.to_string());
        }).await;
        
        debug!("💓 Sent heartbeat: {}", heartbeat);
    }
    
    /// Start automatic heartbeat sender
    fn start_heartbeat_sender(&self) {
        let handle = self.handle.clone();
        let self_id = self.self_id.clone();
        let health_monitor = self.health_monitor.clone();
        
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(5));
            
            loop {
                interval.tick().await;
                
                // Get health status if monitor is available
                let health_data = if let Some(ref monitor) = health_monitor {
                    if let Ok(mut guard) = monitor.lock() {
                        let health = guard.calculate_health();
                        Some(serde_json::json!({
                            "status": health.status,
                            "cpu_percent": health.cpu_percent,
                            "memory_percent": health.memory_percent,
                            "disk_percent": health.disk_percent,
                            "network_percent": health.network_percent,
                            "details": health.details,
                        }))
                    } else {
                        None
                    }
                } else {
                    None
                };
                
                let heartbeat = serde_json::json!({
                    "timestamp": chrono::Utc::now().to_rfc3339(),
                    "alive": true,
                    "uptime_secs": 0, // TODO: Track actual uptime
                    "health": health_data,
                });
                
                handle.with_chitchat(|c| {
                    c.self_node_state().set("heartbeat", &heartbeat.to_string());
                    if let Some(health) = &health_data {
                        c.self_node_state().set("health", &health.to_string());
                    }
                }).await;
                
                debug!("💓 {} auto-heartbeat sent with health status", self_id);
            }
        });
    }
    
    /// Check if we should communicate with a peer based on roles
    pub async fn should_gossip_with_peer(&self, peer_id: &str) -> bool {
        let snapshot = self.view.node_states.read().await;
        
        // Get our role
        let my_role = snapshot.node_states.iter()
            .find(|n| n.chitchat_id().node_id == self.self_id)
            .and_then(|n| n.get("role"))
            .unwrap_or("unknown");
        
        // Get peer role
        let peer_role = snapshot.node_states.iter()
            .find(|n| n.chitchat_id().node_id == *peer_id)
            .and_then(|n| n.get("role"))
            .unwrap_or("unknown");
        
        // Apply role-based filtering
        let should_gossip = match (my_role, peer_role) {
            // Control nodes can gossip with everyone
            ("control", _) | (_, "control") => true,
            // Monolith can gossip with everyone
            ("monolith", _) | (_, "monolith") => true,
            // Same role nodes can gossip
            (a, b) if a == b => true,
            // Different roles cannot gossip
            _ => false,
        };
        
        if !should_gossip {
            debug!("🚫 Blocking gossip between {} ({}) and {} ({})", 
                   self.self_id, my_role, peer_id, peer_role);
        }
        
        should_gossip
    }
}




