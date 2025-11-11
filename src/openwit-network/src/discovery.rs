use std::net::SocketAddr;
use std::time::Duration;
use tokio::time::interval;
use tracing::{info, warn, error, debug};
use anyhow::Result;
use openwit_config::unified::ServiceEndpoints;

/// Service discovery modes
#[derive(Debug, Clone)]
pub enum DiscoveryMode {
    /// Kubernetes DNS-based discovery
    Kubernetes {
        namespace: String,
        services: Vec<String>,
    },
    /// Static seed list (for non-K8s deployments)
    Static {
        seeds: Vec<SocketAddr>,
    },
}

/// Service discovery manager
pub struct ServiceDiscovery {
    mode: DiscoveryMode,
    role: String,
    last_discovered: tokio::sync::RwLock<Vec<SocketAddr>>,
}

impl ServiceDiscovery {
    pub fn new(role: String, is_kubernetes: bool) -> Self {
        Self::new_with_endpoints(role, is_kubernetes, None)
    }
    
    pub fn new_with_endpoints(role: String, is_kubernetes: bool, service_endpoints: Option<&ServiceEndpoints>) -> Self {
        let mode = if is_kubernetes {
            let namespace = std::env::var("KUBERNETES_NAMESPACE")
                .unwrap_or_else(|_| "default".to_string());
            
            // Extract service names from configuration or use defaults
            let extract_service_name = |endpoint: &Option<String>| -> Option<String> {
                endpoint.as_ref().and_then(|e| {
                    // Extract hostname from URL like "http://hotcache-cp:50051"
                    if let Some(start) = e.find("://") {
                        let after_protocol = &e[start + 3..];
                        let end = after_protocol.find(':').or_else(|| after_protocol.find('/')).unwrap_or(after_protocol.len());
                        Some(after_protocol[..end].to_string())
                    } else {
                        // If no protocol, assume it's just hostname or hostname:port
                        let end = e.find(':').unwrap_or(e.len());
                        Some(e[..end].to_string())
                    }
                })
            };
            
            // Get service names from endpoints or use environment/defaults
            let (service_name, control_service) = if let Some(endpoints) = service_endpoints {
                let service_name = match role.as_str() {
                    "control" => extract_service_name(&endpoints.control_plane),
                    "ingest" => extract_service_name(&endpoints.ingestion),
                    "storage" => extract_service_name(&endpoints.storage),
                    "search" => extract_service_name(&endpoints.search),
                    "query" => extract_service_name(&endpoints.query),
                    _ => None,
                }.unwrap_or_else(|| format!("openwit-{}", role));
                
                let control_service = extract_service_name(&endpoints.control_plane)
                    .unwrap_or_else(|| "openwit-control-plane".to_string());
                
                (service_name, control_service)
            } else {
                // Fallback to environment or defaults
                let service_prefix = std::env::var("OPENWIT_SERVICE_PREFIX").unwrap_or_else(|_| "openwit".to_string());
                let service_name = match role.as_str() {
                    "control" => format!("{}-control-plane", service_prefix),
                    "ingest" => format!("{}-ingestion", service_prefix),
                    "storage" => format!("{}-storage", service_prefix),
                    "search" => format!("{}-search", service_prefix),
                    _ => format!("{}-{}", service_prefix, role),
                };
                let control_service = format!("{}-control-plane", service_prefix);
                (service_name, control_service)
            };
            
            let mut services = vec![service_name];
            
            // All nodes should discover control nodes
            if role != "control" {
                services.push(control_service);
            }
            
            // Monolith discovers all services
            if role == "monolith" {
                if let Some(endpoints) = service_endpoints {
                    services = vec![
                        extract_service_name(&endpoints.control_plane),
                        extract_service_name(&endpoints.ingestion),
                        extract_service_name(&endpoints.storage),
                        extract_service_name(&endpoints.search),
                        extract_service_name(&endpoints.query),
                    ].into_iter()
                    .filter_map(|s| s)
                    .collect();
                } else {
                    let prefix = std::env::var("OPENWIT_SERVICE_PREFIX").unwrap_or_else(|_| "openwit".to_string());
                    services = vec![
                        format!("{}-control-plane", prefix),
                        format!("{}-ingestion", prefix),
                        format!("{}-storage", prefix),
                        format!("{}-search", prefix),
                        format!("{}-indexer", prefix),
                    ];
                }
            }
            
            info!("🔍 Kubernetes discovery mode enabled for role '{}' in namespace '{}'", role, namespace);
            info!("   Services to discover: {:?}", services);
            
            DiscoveryMode::Kubernetes { namespace, services }
        } else {
            // Read seeds from environment or config
            let seeds_str = std::env::var("OPENWIT_SEEDS")
                .unwrap_or_else(|_| String::new());
            
            let seeds: Vec<SocketAddr> = seeds_str
                .split(',')
                .filter_map(|s| {
                    let trimmed = s.trim();
                    if trimmed.is_empty() {
                        None
                    } else {
                        match trimmed.parse::<SocketAddr>() {
                            Ok(addr) => Some(addr),
                            Err(e) => {
                                warn!("Failed to parse seed address '{}': {}", trimmed, e);
                                None
                            }
                        }
                    }
                })
                .collect();

            debug!("Static discovery mode enabled with {} seeds", seeds.len());

            DiscoveryMode::Static { seeds }
        };
        
        Self {
            mode,
            role,
            last_discovered: tokio::sync::RwLock::new(Vec::new()),
        }
    }
    
    /// Discover peers based on the configured mode
    pub async fn discover_peers(&self) -> Result<Vec<SocketAddr>> {
        match &self.mode {
            DiscoveryMode::Kubernetes { namespace, services } => {
                self.discover_kubernetes(namespace, services).await
            }
            DiscoveryMode::Static { seeds } => {
                Ok(seeds.clone())
            }
        }
    }
    
    /// Kubernetes DNS-based discovery
    async fn discover_kubernetes(&self, _namespace: &str, _services: &[String]) -> Result<Vec<SocketAddr>> {
        // Simplified discovery - just use environment seeds
        let seeds_str = std::env::var("OPENWIT_SEEDS").unwrap_or_default();
        let all_peers: Vec<SocketAddr> = seeds_str
            .split(',')
            .filter_map(|s| s.trim().parse::<SocketAddr>().ok())
            .collect();
        
        // Update last discovered
        *self.last_discovered.write().await = all_peers.clone();
        
        Ok(all_peers)
    }
    
    /// Start periodic re-discovery
    pub async fn start_periodic_discovery(self: std::sync::Arc<Self>, interval_secs: u64) {
        let mut ticker = interval(Duration::from_secs(interval_secs));
        
        tokio::spawn(async move {
            debug!("Starting periodic peer discovery every {}s", interval_secs);

            loop {
                ticker.tick().await;

                match self.discover_peers().await {
                    Ok(peers) => {
                        let peer_count = peers.len();
                        if peer_count > 0 {
                            debug!("Re-discovered {} peers", peer_count);
                            for peer in &peers {
                                debug!("   - {}", peer);
                            }
                        }
                    }
                    Err(e) => {
                        debug!("Periodic discovery failed: {}", e);
                    }
                }
            }
        });
    }
    
    /// Get last discovered peers
    pub async fn get_peers(&self) -> Vec<SocketAddr> {
        self.last_discovered.read().await.clone()
    }
    
    /// Check if we should communicate with a peer based on role filtering
    pub fn should_gossip_with(&self, peer_role: &str) -> bool {
        // Control nodes can gossip with everyone
        if self.role == "control" || peer_role == "control" {
            return true;
        }
        
        // Monolith can gossip with everyone
        if self.role == "monolith" || peer_role == "monolith" {
            return true;
        }
        
        // Same role nodes can gossip
        self.role == peer_role
    }
}

/// Helper to detect Kubernetes environment
pub fn is_kubernetes_environment() -> bool {
    // Check if explicitly disabled
    if std::env::var("OPENWIT_DEPLOYMENT_MODE").unwrap_or_default() == "local" {
        return false;
    }
    
    // Check for Kubernetes service account token
    if std::path::Path::new("/var/run/secrets/kubernetes.io/serviceaccount/token").exists() {
        return true;
    }
    
    // Check for Kubernetes environment variables
    if std::env::var("KUBERNETES_SERVICE_HOST").is_ok() {
        return true;
    }
    
    false
}