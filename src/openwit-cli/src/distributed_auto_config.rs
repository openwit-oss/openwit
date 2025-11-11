
use std::collections::{HashMap, HashSet};
use std::net::{TcpListener};
use std::path::PathBuf;
use anyhow::{Result, anyhow};
use tracing::{info, warn};
use openwit_config::UnifiedConfig;
use serde::{Serialize, Deserialize};
use tokio::fs;

/// Service registry file for distributed mode
const SERVICE_REGISTRY_FILE: &str = "./data/.openwit_services.json";

/// Service information for distributed discovery
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceInfo {
    pub node_id: String,
    pub node_type: String,
    pub service_port: u16,
    pub gossip_port: Option<u16>,
    pub endpoint: String,
    pub started_at: chrono::DateTime<chrono::Utc>,
    #[serde(default)]
    pub arrow_flight_port: u16,
}

/// Distributed service registry
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DistributedServiceRegistry {
    pub services: HashMap<String, Vec<ServiceInfo>>,
}

impl DistributedServiceRegistry {
    /// Load registry from file
    pub async fn load() -> Result<Self> {
        let path = PathBuf::from(SERVICE_REGISTRY_FILE);
        if path.exists() {
            let content = fs::read_to_string(&path).await?;
            let registry: DistributedServiceRegistry = serde_json::from_str(&content)?;
            Ok(registry)
        } else {
            Ok(Self::default())
        }
    }

    /// Save registry to file
    pub async fn save(&self) -> Result<()> {
        let path = PathBuf::from(SERVICE_REGISTRY_FILE);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }
        let content = serde_json::to_string_pretty(self)?;
        fs::write(&path, content).await?;
        Ok(())
    }

    /// Register a service
    pub async fn register_service(&mut self, service_type: &str, info: ServiceInfo) -> Result<()> {
        let services = self.services.entry(service_type.to_string()).or_insert_with(Vec::new);
        
        // Remove old entries for the same node_id
        services.retain(|s| s.node_id != info.node_id);
        
        // Add new entry
        services.push(info);
        
        // Save to file
        self.save().await?;
        
        Ok(())
    }

    /// Get services of a specific type
    pub fn get_services(&self, service_type: &str) -> Vec<ServiceInfo> {
        self.services.get(service_type).cloned().unwrap_or_default()
    }

    /// Get first available service of a type
    pub fn get_service(&self, service_type: &str) -> Option<ServiceInfo> {
        self.services.get(service_type)
            .and_then(|services| services.first())
            .cloned()
    }

    /// Clean up old entries (older than 5 minutes)
    pub async fn cleanup_old_entries(&mut self) -> Result<()> {
        let cutoff = chrono::Utc::now() - chrono::Duration::minutes(5);
        
        for services in self.services.values_mut() {
            services.retain(|s| s.started_at > cutoff);
        }
        
        self.save().await?;
        Ok(())
    }
}

/// Auto-configuration for distributed nodes
pub struct DistributedAutoConfig {
    node_type: String,
    node_id: String,
    preferred_ports: HashMap<String, u16>,
}

impl DistributedAutoConfig {
    pub fn new(node_type: &str, node_id: String) -> Self {
        let preferred_ports = HashMap::new();
        
        Self {
            node_type: node_type.to_string(),
            node_id,
            preferred_ports,
        }
    }
    
    pub fn with_config(node_type: &str, node_id: String, config: &UnifiedConfig) -> Self {
        let mut preferred_ports = HashMap::new();
        
        // Extract ports from actual configuration - prioritize service_ports section
        match node_type {
            "control" => {
                // Use service_ports config first, then fallback to existing config
                preferred_ports.insert("service".to_string(), config.service_ports.control_plane.service);
            }
            "ingest" => {
                // Use service_ports config for ingestion
                preferred_ports.insert("service".to_string(), config.service_ports.ingestion.grpc);
            }
            "storage" => {
                // Use service_ports config for storage
                preferred_ports.insert("service".to_string(), config.service_ports.storage.service);
                preferred_ports.insert("arrow_flight".to_string(), config.service_ports.storage.arrow_flight);
            }
            "http" => {
                // Use service_ports config for HTTP ingestion
                preferred_ports.insert("service".to_string(), config.service_ports.ingestion.http);
            }
            "proxy" => {
                // Use service_ports config for proxy
                preferred_ports.insert("service".to_string(), config.service_ports.proxy.service);
            }
            "search" => {
                // Use service_ports config for search
                preferred_ports.insert("service".to_string(), config.service_ports.search.service);
            }
            "indexer" => {
                // Use service_ports config for indexer
                preferred_ports.insert("service".to_string(), config.service_ports.indexer.service);
            }
            _ => {
                // Fallback defaults
                preferred_ports.insert("service".to_string(), 9000);
            }
        }
        
        Self {
            node_type: node_type.to_string(),
            node_id,
            preferred_ports,
        }
    }
    
    // Helper to extract port from endpoint like "http://localhost:50051"
    #[allow(dead_code)]
    fn extract_port_from_endpoint(endpoint: &str) -> Option<u16> {
        endpoint.split(':').last()?.parse().ok()
    }
    
    // Helper to extract port from address like "127.0.0.1:8000"
    #[allow(dead_code)]
    fn extract_port_from_address(address: &str) -> Option<u16> {
        address.split(':').last()?.parse().ok()
    }

    /// Find next available port starting from preferred
    async fn find_available_port(&self, port_type: &str) -> Result<u16> {
        // Check if running in Kubernetes - use fixed configured ports
        if std::env::var("KUBERNETES_SERVICE_HOST").is_ok() || 
           std::env::var("OPENWIT_DEPLOYMENT_MODE").unwrap_or_default() == "kubernetes" {
            let configured_port = if let Some(port) = self.preferred_ports.get(port_type).copied() {
                port
            } else {
                // Port not configured - return error for control plane, use defaults for others
                if self.node_type == "control" {
                    return Err(anyhow!(
                        "Control plane {} port is not configured. Please set 'service_ports.control_plane.{}' in your config.yaml",
                        port_type, port_type
                    ));
                }
                
                // Default ports for other services
                match (self.node_type.as_str(), port_type) {
                    ("ingest", "service") => 4317,
                    ("storage", "service") => 8081,
                    _ => 9000,
                }
            };
            info!("Kubernetes deployment detected - using fixed port: {}", configured_port);
            return Ok(configured_port);
        }
        
        // For control plane, use the configured port from preferred_ports
        if self.node_type == "control" && port_type == "service" {
            let control_port = self.preferred_ports.get("service")
                .copied()
                .ok_or_else(|| anyhow!("Control plane service port is not configured. Please set 'service_ports.control_plane.service' in your config.yaml"))?;
            // Try to bind to ensure it's available
            if TcpListener::bind(format!("127.0.0.1:{}", control_port)).is_ok() {
                info!("Control plane using configured port: {}", control_port);
                return Ok(control_port);
            } else {
                return Err(anyhow!("Control plane configured port {} is not available. Please free up this port.", control_port));
            }
        }
        
        // Gossip functionality deprecated - no longer need gossip port handling
        
        // Load current registry to check for used ports
        let registry = DistributedServiceRegistry::load().await?;
        
        let start_port = self.preferred_ports.get(port_type).copied().unwrap_or(9000);
        
        // Collect all ports in use from the registry
        let mut used_ports = HashSet::new();
        for (_service_type, services) in &registry.services {
            for service in services {
                if port_type == "service" {
                    used_ports.insert(service.service_port);
                }
                // Gossip functionality deprecated - no longer check gossip ports
            }
        }
        
        for port in start_port..=65535 {
            // Check if port is in use by another registered service
            if used_ports.contains(&port) {
                continue;
            }
            
            // Check if port is available on the system
            if TcpListener::bind(format!("127.0.0.1:{}", port)).is_ok() {
                info!("Found available {} port: {} for {}", port_type, port, self.node_type);
                return Ok(port);
            }
        }
        
        Err(anyhow!("No available {} ports found for {}", port_type, self.node_type))
    }

    /// Auto-configure node and register with distributed registry
    pub async fn configure_and_register(&self, config: &mut UnifiedConfig) -> Result<ServiceInfo> {
        info!("Auto-configuring {} node: {}", self.node_type, self.node_id);
        
        // Find available service port
        let service_port = self.find_available_port("service").await?;
        
        // Gossip functionality deprecated - no gossip port needed
        let gossip_port: Option<u16> = None;
        
        // Get service-specific advertise address from config
        let advertise_addr = config.networking.get_service_advertise_address(&self.node_type);
        
        // Apply configuration based on node type
        let endpoint = match self.node_type.as_str() {
            "control" => {
                config.control_plane.grpc_endpoint = format!("http://{}:{}", advertise_addr, service_port);
                format!("http://{}:{}", advertise_addr, service_port)
            }
            "http" => {
                config.ingestion.http.bind = format!("0.0.0.0:{}", service_port);
                config.ingestion.http.port = service_port;
                config.ingestion.sources.http.enabled = true;
                format!("http://{}:{}", advertise_addr, service_port)
            }
            "proxy" => {
                // Proxy configuration would be applied here if it existed
                format!("http://{}:{}", advertise_addr, service_port)
            }
            "ingest" => {
                config.ingestion.grpc.bind = "0.0.0.0".to_string();
                config.ingestion.grpc.port = service_port;
                config.ingestion.sources.grpc.enabled = true;
                format!("http://{}:{}", advertise_addr, service_port)
            }
            "storage" => {
                // Storage uses flight port
                format!("http://{}:{}", advertise_addr, service_port)
            }
            _ => format!("http://{}:{}", advertise_addr, service_port),
        };
        
        // Gossip configuration deprecated - no longer needed
        
        // Create service info
        let arrow_flight_port = match self.node_type.as_str() {
            "storage" => 8091,  // Storage nodes use 8091 for Arrow Flight
            "ingest" => 8089,   // Ingestion nodes use 8089 for Arrow Flight
            _ => 0,             // Other nodes don't use Arrow Flight
        };
        
        let service_info = ServiceInfo {
            node_id: self.node_id.clone(),
            node_type: self.node_type.clone(),
            service_port,
            gossip_port,
            endpoint: endpoint.clone(),
            started_at: chrono::Utc::now(),
            arrow_flight_port,
        };
        
        // Register with distributed registry
        let mut registry = DistributedServiceRegistry::load().await?;
        registry.register_service(&self.node_type, service_info.clone()).await?;
        
        info!("Registered {} node at {}", self.node_type, endpoint);
        if arrow_flight_port > 0 {
            info!("   Arrow Flight port: {}", arrow_flight_port);
        }
        
        Ok(service_info)
    }

    /// Discover and configure connections to other services
    pub async fn discover_services(&self, config: &mut UnifiedConfig) -> Result<()> {
        info!("Discovering other services...");
        
        let registry = DistributedServiceRegistry::load().await?;
        
        // Discover control plane
        if self.node_type != "control" {
            if let Some(control) = registry.get_service("control") {
                config.control_plane.grpc_endpoint = control.endpoint.clone();
                info!("   Found control plane: {}", control.endpoint);
                
                // Add control plane gossip seed if available
                if let Some(gp) = control.gossip_port {
                    let seed = format!("127.0.0.1:{}", gp);
                    // Gossip functionality deprecated - seed nodes no longer used
                    // if !config.networking.seed_nodes.contains(&seed) {
                    //     config.networking.seed_nodes.push(seed);
                    // }
                }
            } else {
                warn!("   Control plane not found - will retry on startup");
            }
        }
        
        // For HTTP nodes, discover ingestion nodes
        if self.node_type == "http" {
            if let Some(ingest) = registry.get_service("ingest") {
                std::env::set_var("OPENWIT_GRPC_INGESTION_ENDPOINT", &ingest.endpoint);
                info!("   Found ingestion service: {}", ingest.endpoint);
            } else {
                warn!("   Ingestion service not found - will use default");
            }
        }
        
        // For proxy nodes, discover all ingestion nodes
        if self.node_type == "proxy" {
            let http_nodes = registry.get_services("http");
            let ingest_nodes = registry.get_services("ingest");
            
            info!("   Found {} HTTP nodes", http_nodes.len());
            info!("   Found {} ingestion nodes", ingest_nodes.len());
            
            // Store discovered nodes in environment for proxy to use
            if !http_nodes.is_empty() {
                let http_endpoints: Vec<String> = http_nodes.iter()
                    .map(|n| n.endpoint.clone())
                    .collect();
                std::env::set_var("OPENWIT_HTTP_ENDPOINTS", http_endpoints.join(","));
            }
            
            if !ingest_nodes.is_empty() {
                let ingest_endpoints: Vec<String> = ingest_nodes.iter()
                    .map(|n| n.endpoint.clone())
                    .collect();
                std::env::set_var("OPENWIT_INGESTION_ENDPOINTS", ingest_endpoints.join(","));
            }
        }
        
        Ok(())
    }

    /// Get a summary of the distributed configuration
    pub async fn get_summary(&self) -> Result<String> {
        let registry = DistributedServiceRegistry::load().await?;
        
        let mut summary = String::from("\n🌐 OpenWit Distributed Mode - Service Registry\n");
        summary.push_str("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n");
        
        for (service_type, services) in &registry.services {
            if !services.is_empty() {
                summary.push_str(&format!("{} Nodes:\n", service_type.to_uppercase()));
                for service in services {
                    summary.push_str(&format!("   • {} at {}", service.node_id, service.endpoint));
                    if let Some(gp) = service.gossip_port {
                        summary.push_str(&format!(" (gossip: {})", gp));
                    }
                    summary.push_str("\n");
                }
                summary.push_str("\n");
            }
        }
        
        summary.push_str("💡 Quick Commands:\n");
        
        if let Some(http) = registry.get_service("http") {
            summary.push_str(&format!("   curl -X POST {}/v1/traces -H 'Content-Type: application/json' -d '{{}}'\n", http.endpoint));
        }
        
        if let Some(control) = registry.get_service("control") {
            summary.push_str(&format!("   Status: {}/status\n", control.endpoint));
        }
        
        summary.push_str("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");
        
        Ok(summary)
    }
}

/// Clean up service registry on shutdown
pub async fn cleanup_service_on_shutdown(node_id: &str) -> Result<()> {
    let mut registry = DistributedServiceRegistry::load().await?;
    
    // Remove this node from all service types
    for services in registry.services.values_mut() {
        services.retain(|s| s.node_id != node_id);
    }
    
    registry.save().await?;
    Ok(())
}