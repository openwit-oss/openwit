use std::collections::HashMap;
use std::net::{TcpListener};
use anyhow::{Result, anyhow};
use tracing::{info};
use openwit_config::UnifiedConfig;

#[derive(Debug, Clone)]
pub struct ServicePorts {
    pub control_plane: u16,
    pub http_ingestion: u16,
    pub grpc_ingestion: u16,
    pub kafka_ingestion: Option<u16>,
    pub storage_flight: u16,
    pub indexer: u16,
    pub search: u16,
    pub metrics: u16,
    pub gossip: u16,
}

impl Default for ServicePorts {
    fn default() -> Self {
        Self {
            control_plane: 7001,
            http_ingestion: 4318,
            grpc_ingestion: 4317,
            kafka_ingestion: None,
            storage_flight: 8081,
            indexer: 50060,
            search: 8082,
            metrics: 9090,
            gossip: 9092,
        }
    }
}

pub struct AutoConfig {
    ports: ServicePorts,
    allocated_ports: HashMap<String, u16>,
}

impl AutoConfig {
    pub fn new() -> Self {
        Self {
            ports: ServicePorts::default(),
            allocated_ports: HashMap::new(),
        }
    }

    /// Find an available port starting from the given port
    fn find_available_port(start_port: u16, name: &str) -> Result<u16> {
        for port in start_port..=65535 {
            if TcpListener::bind(format!("127.0.0.1:{}", port)).is_ok() {
                info!("Allocated port {} for {}", port, name);
                return Ok(port);
            }
        }
        Err(anyhow!("No available ports found for {}", name))
    }

    /// Auto-configure all service ports
    pub fn configure_ports(&mut self) -> Result<ServicePorts> {
        info!("Auto-configuring service ports for monolith mode...");

        // Control plane (try default first)
        self.ports.control_plane = Self::find_available_port(7001, "control-plane")?;
        self.allocated_ports.insert("control-plane".to_string(), self.ports.control_plane);

        // HTTP ingestion
        self.ports.http_ingestion = Self::find_available_port(4318, "http-ingestion")?;
        self.allocated_ports.insert("http-ingestion".to_string(), self.ports.http_ingestion);

        // gRPC ingestion
        self.ports.grpc_ingestion = Self::find_available_port(4317, "grpc-ingestion")?;
        self.allocated_ports.insert("grpc-ingestion".to_string(), self.ports.grpc_ingestion);

        // Storage flight
        self.ports.storage_flight = Self::find_available_port(8081, "storage-flight")?;
        self.allocated_ports.insert("storage-flight".to_string(), self.ports.storage_flight);

        // Indexer
        self.ports.indexer = Self::find_available_port(50060, "indexer")?;
        self.allocated_ports.insert("indexer".to_string(), self.ports.indexer);

        // Search
        self.ports.search = Self::find_available_port(8082, "search")?;
        self.allocated_ports.insert("search".to_string(), self.ports.search);

        // Metrics
        self.ports.metrics = Self::find_available_port(9090, "metrics")?;
        self.allocated_ports.insert("metrics".to_string(), self.ports.metrics);

        // Gossip (for local cluster)
        self.ports.gossip = Self::find_available_port(9092, "gossip")?;
        self.allocated_ports.insert("gossip".to_string(), self.ports.gossip);

        info!("Service port configuration complete:");
        info!("   Control Plane: {}", self.ports.control_plane);
        info!("   HTTP Ingestion: {}", self.ports.http_ingestion);
        info!("   gRPC Ingestion: {}", self.ports.grpc_ingestion);
        info!("   Storage Flight: {}", self.ports.storage_flight);
        info!("   Indexer: {}", self.ports.indexer);
        info!("   Search: {}", self.ports.search);
        info!("   Metrics: {}", self.ports.metrics);
        info!("   Gossip: {}", self.ports.gossip);

        Ok(self.ports.clone())
    }

    /// Apply auto-configuration to UnifiedConfig
    pub fn apply_to_config(&self, config: &mut UnifiedConfig) {
        info!("📝 Applying auto-configuration to UnifiedConfig...");

        // Update control plane endpoint
        config.control_plane.grpc_endpoint = format!("http://localhost:{}", self.ports.control_plane);
        
        // Update service ports configuration
        config.service_ports.control_plane.service = self.ports.control_plane;
        // Gossip port deprecated - no longer configured
        
        // Update ingestion endpoints
        config.ingestion.grpc.bind = format!("0.0.0.0:{}", self.ports.grpc_ingestion);
        config.ingestion.http.bind = format!("0.0.0.0:{}", self.ports.http_ingestion);
        
        // Update ingestion sources
        config.ingestion.sources.grpc.enabled = true;
        config.ingestion.sources.http.enabled = true;
        
        // Set ingestion endpoint for HTTP to forward to gRPC
        std::env::set_var("INGESTION_ENDPOINT", format!("http://localhost:{}", self.ports.grpc_ingestion));
        
        // Update search endpoint
        // Update search port only
        config.search.http.port = self.ports.search;
        
        // Metrics endpoint removed
        
        // Gossip configuration deprecated - no longer needed for local mode
        
        // Enable all services for monolith
        config.control_plane.enabled = true;
        config.ingestion.sources.grpc.enabled = true;
        config.ingestion.sources.http.enabled = true;
        config.storage.local.enabled = true;
        // Indexing configuration is managed via config file
        config.search.enabled = true;
        // Prometheus monitoring removed
        
        // Set pod role to monolith
        config.deployment.kubernetes.pod_role = "monolith".to_string();
        
        // Disable gossip for true monolith mode
        // Disable gossip for true monolith mode by disabling it via env var
        std::env::set_var("OPENWIT_DISABLE_GOSSIP", "1");
        
        info!("Auto-configuration applied successfully");
    }

    /// Create service registry for internal service discovery
    pub fn create_service_registry(&self) -> HashMap<String, String> {
        let mut registry = HashMap::new();
        
        registry.insert("control-plane".to_string(), 
            format!("http://localhost:{}", self.ports.control_plane));
        registry.insert("http-ingestion".to_string(), 
            format!("http://localhost:{}", self.ports.http_ingestion));
        registry.insert("grpc-ingestion".to_string(), 
            format!("http://localhost:{}", self.ports.grpc_ingestion));
        registry.insert("storage-flight".to_string(), 
            format!("http://localhost:{}", self.ports.storage_flight));
        registry.insert("indexer".to_string(), 
            format!("http://localhost:{}", self.ports.indexer));
        registry.insert("search".to_string(), 
            format!("http://localhost:{}", self.ports.search));
        registry.insert("metrics".to_string(), 
            format!("http://localhost:{}", self.ports.metrics));
        
        registry
    }

    /// Get a summary of the configuration
    pub fn get_summary(&self) -> String {
        format!(
            r#"
OpenWit Monolith Mode - Auto Configuration Summary
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Ingestion Endpoints:
   • HTTP/OTLP: http://localhost:{}
   • gRPC/OTLP: http://localhost:{}

🔧 Internal Services:
   • Control Plane: http://localhost:{}
   • Storage: http://localhost:{}
   • Search API: http://localhost:{}
   • Metrics: http://localhost:{}/metrics

📊 Status Dashboard: http://localhost:{}/status

💡 Quick Test:
   curl -X POST http://localhost:{}/v1/traces \
     -H 'Content-Type: application/json' \
     -d '{{"resourceSpans":[{{"resource":{{}},"scopeSpans":[{{}}]}}]}}'

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"#,
            self.ports.http_ingestion,
            self.ports.grpc_ingestion,
            self.ports.control_plane,
            self.ports.storage_flight,
            self.ports.search,
            self.ports.metrics,
            self.ports.control_plane,
            self.ports.http_ingestion
        )
    }
}

/// Environment-based service discovery
pub struct ServiceDiscovery {
    registry: HashMap<String, String>,
}

impl ServiceDiscovery {
    pub fn new(registry: HashMap<String, String>) -> Self {
        Self { registry }
    }

    pub fn get_endpoint(&self, service: &str) -> Option<String> {
        self.registry.get(service).cloned()
    }

    /// Set environment variables for service discovery
    pub fn set_env_vars(&self) {
        for (service, endpoint) in &self.registry {
            let env_key = format!("OPENWIT_{}_ENDPOINT", service.to_uppercase().replace("-", "_"));
            std::env::set_var(&env_key, endpoint);
            info!("Set {} = {}", env_key, endpoint);
        }
    }
}