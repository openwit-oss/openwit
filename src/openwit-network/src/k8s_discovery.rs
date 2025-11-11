
use std::collections::HashSet;
use anyhow::{Result, Context};
use tracing::{info, warn, error};
use tokio::time::{interval, Duration};
use openwit_config::unified::KubernetesConfig;

/// Kubernetes service discovery for headless services
pub struct K8sServiceDiscovery {
    namespace: String,
    service_name: String,
    port: u16,
}

impl K8sServiceDiscovery {
    pub fn new(namespace: String, service_name: String, port: u16) -> Self {
        Self {
            namespace,
            service_name,
            port,
        }
    }
    
    /// Discover peer nodes via DNS lookup of headless service
    pub async fn discover_peers(&self) -> Result<Vec<String>> {
        let fqdn = format!("{}.{}.svc.cluster.local", self.service_name, self.namespace);
        info!("🔍 Discovering peers via headless service: {}", fqdn);
        
        // Perform DNS lookup
        let result = tokio::net::lookup_host(format!("{}:9000", fqdn)).await;
        match result {
            Ok(addrs) => {
                let peers: Vec<String> = addrs
                    .map(|addr| format!("{}:{}", addr.ip(), self.port))
                    .collect();
                
                info!("✅ Discovered {} peers: {:?}", peers.len(), peers);
                Ok(peers)
            }
            Err(e) => {
                warn!("❌ DNS lookup failed for {}: {}", fqdn, e);
                
                // Try individual pod DNS pattern
                self.discover_via_pod_dns().await
            }
        }
    }
    
    /// Alternative discovery using pod DNS pattern
    async fn discover_via_pod_dns(&self) -> Result<Vec<String>> {
        let mut peers = Vec::new();
        
        // Try common pod patterns (0-9)
        for i in 0..10 {
            let pod_fqdn = format!(
                "{}-{}.{}.{}.svc.cluster.local",
                self.service_name.trim_end_matches("-headless"),
                i,
                self.service_name,
                self.namespace
            );
            
            match tokio::net::lookup_host(&format!("{}:{}", pod_fqdn, self.port)).await {
                Ok(mut addrs) => {
                    if let Some(addr) = addrs.next() {
                        let peer = format!("{}:{}", addr.ip(), self.port);
                        info!("✅ Found pod {}: {}", i, peer);
                        peers.push(peer);
                    }
                }
                Err(_) => {
                    // Pod doesn't exist, stop searching
                    break;
                }
            }
        }
        
        if peers.is_empty() {
            Err(anyhow::anyhow!("No peers discovered via pod DNS"))
        } else {
            Ok(peers)
        }
    }
    
    /// Start continuous peer discovery
    pub async fn start_discovery_loop(&self, update_interval: Duration) -> Result<()> {
        let mut ticker = interval(update_interval);
        let mut known_peers = HashSet::new();
        
        loop {
            ticker.tick().await;
            
            match self.discover_peers().await {
                Ok(peers) => {
                    let new_peers: HashSet<String> = peers.into_iter().collect();
                    
                    // Check for changes
                    let added: Vec<_> = new_peers.difference(&known_peers).cloned().collect();
                    let removed: Vec<_> = known_peers.difference(&new_peers).cloned().collect();
                    
                    if !added.is_empty() {
                        info!("➕ New peers discovered: {:?}", added);
                    }
                    
                    if !removed.is_empty() {
                        info!("➖ Peers removed: {:?}", removed);
                    }
                    
                    known_peers = new_peers;
                }
                Err(e) => {
                    error!("Peer discovery failed: {}", e);
                }
            }
        }
    }
}

/// Get seed nodes from Kubernetes configuration
pub async fn get_k8s_seed_nodes(config: &KubernetesConfig) -> Result<Vec<String>> {
    if !config.enabled {
        return Ok(Vec::new());
    }
    
    let namespace = if !config.namespace.is_empty() {
        config.namespace.clone()
    } else {
        std::env::var("KUBERNETES_NAMESPACE").unwrap_or_else(|_| "default".to_string())
    };
    
    if config.headless_service.is_empty() {
        return Err(anyhow::anyhow!("Headless service name required for K8s discovery"));
    }
    let service = &config.headless_service;
    
    // Extract port from service name if formatted as "service:port"
    let (service_name, port) = if let Some(idx) = service.find(':') {
        let (name, port_str) = service.split_at(idx);
        let port = port_str[1..].parse::<u16>()
            .context("Invalid port in headless service")?;
        (name.to_string(), port)
    } else {
        (service.clone(), 9090) // Default gossip port
    };
    
    let discovery = K8sServiceDiscovery::new(namespace, service_name, port);
    discovery.discover_peers().await
}

/// Create gossip seed nodes combining static config and K8s discovery
pub async fn create_seed_nodes(
    static_seeds: Vec<String>,
    k8s_config: Option<&KubernetesConfig>,
) -> Result<Vec<String>> {
    let mut all_seeds = static_seeds;
    
    if let Some(k8s) = k8s_config {
        match get_k8s_seed_nodes(k8s).await {
            Ok(k8s_seeds) => {
                info!("🌐 Adding {} K8s discovered seeds", k8s_seeds.len());
                all_seeds.extend(k8s_seeds);
            }
            Err(e) => {
                warn!("K8s discovery failed, using only static seeds: {}", e);
            }
        }
    }
    
    // Remove duplicates
    let unique_seeds: HashSet<String> = all_seeds.into_iter().collect();
    Ok(unique_seeds.into_iter().collect())
}