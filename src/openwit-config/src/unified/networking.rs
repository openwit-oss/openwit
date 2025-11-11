use serde::{Deserialize, Serialize};
use crate::unified::validation::{Validatable, ValidationResult};
use tracing::info;

/// Service endpoints configuration for service discovery
/// These are base URLs without port numbers - ports are determined automatically
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceEndpoints {
    /// Control plane service endpoint (e.g., "http://openwit-control-plane")
    #[serde(default)]
    pub control_plane: Option<String>,
    
    /// Storage service endpoint (e.g., "http://openwit-storage")
    #[serde(default)]
    pub storage: Option<String>,
    
    /// Ingestion service endpoint (e.g., "http://openwit-ingestion")
    #[serde(default)]
    pub ingestion: Option<String>,
    
    /// Query service endpoint (e.g., "http://openwit-query")
    #[serde(default)]
    pub query: Option<String>,
    
    /// Search service endpoint (e.g., "http://openwit-search")
    #[serde(default)]
    pub search: Option<String>,
    
    /// Kafka service endpoint (e.g., "http://openwit-kafka")
    #[serde(default)]
    pub kafka: Option<String>,
}

impl Default for ServiceEndpoints {
    fn default() -> Self {
        Self {
            control_plane: None,
            storage: None,
            ingestion: None,
            query: None,
            search: None,
            kafka: None,
        }
    }
}

impl ServiceEndpoints {
    /// Get endpoint for a service type, with port
    pub fn get_endpoint(&self, service_type: &str, port: u16) -> String {
        let base_url = match service_type {
            "control" => self.control_plane.as_ref(),
            "storage" => self.storage.as_ref(),
            "ingest" | "ingestion" => self.ingestion.as_ref(),
            "query" => self.query.as_ref(),
            "search" => self.search.as_ref(),
            "kafka" => self.kafka.as_ref(),
            _ => None,
        };
        
        if let Some(base) = base_url {
            // Remove trailing slash if present
            let base = base.trim_end_matches('/');
            format!("{}:{}", base, port)
        } else {
            // Fall back to localhost
            format!("http://localhost:{}", port)
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkingConfig {
    /// External address that this node advertises to other nodes
    /// This is the address other nodes will use to connect to this node
    /// If not set, will be determined from environment or use localhost
    #[serde(default)]
    pub advertise_address: Option<String>,
    
    /// Service discovery endpoints - base URLs without ports
    /// Ports will be determined automatically by the services
    #[serde(default)]
    pub service_endpoints: ServiceEndpoints,
    
    #[serde(default)]
    pub kubernetes: KubernetesNetworkingConfig,
    
    #[serde(default)]
    pub seed_discovery: SeedDiscoveryConfig,
    
    #[serde(default)]
    pub failure_detector: FailureDetectorConfig,
    
    #[serde(default)]
    pub service_mesh: ServiceMeshConfig,
    
    #[serde(default)]
    pub inter_node: InterNodeConfig,
}

impl Default for NetworkingConfig {
    fn default() -> Self {
        Self {
            advertise_address: None,
            service_endpoints: ServiceEndpoints::default(),
            kubernetes: KubernetesNetworkingConfig::default(),
            seed_discovery: SeedDiscoveryConfig::default(),
            failure_detector: FailureDetectorConfig::default(),
            service_mesh: ServiceMeshConfig::default(),
            inter_node: InterNodeConfig::default(),
        }
    }
}

impl NetworkingConfig {
    /// Get the advertise address, falling back to environment or localhost
    pub fn get_advertise_address(&self) -> String {
        if let Some(ref addr) = self.advertise_address {
            return addr.clone();
        }
        
        // Try environment variable
        if let Ok(addr) = std::env::var("OPENWIT_ADVERTISE_ADDRESS") {
            return addr;
        }
        
        // Try to get from Kubernetes environment
        if let Ok(pod_ip) = std::env::var("OPENWIT_POD_IP") {
            return pod_ip;
        }
        
        // Default to localhost
        "localhost".to_string()
    }
    
    /// Get the advertise address for a specific service type
    /// Uses service_endpoints if configured, otherwise falls back to general advertise_address
    pub fn get_service_advertise_address(&self, service_type: &str) -> String {
        // Check if there's a specific endpoint configured for this service
        let service_endpoint = match service_type {
            "control" => self.service_endpoints.control_plane.as_ref(),
            "storage" => self.service_endpoints.storage.as_ref(),
            "ingest" | "ingestion" => self.service_endpoints.ingestion.as_ref(),
            "query" => self.service_endpoints.query.as_ref(),
            "search" => self.service_endpoints.search.as_ref(),
            "kafka" => self.service_endpoints.kafka.as_ref(),
            _ => None,
        };
        
        if let Some(endpoint) = service_endpoint {
            // Extract hostname from URL (remove protocol, port and path)
            let host = if let Some(h) = endpoint.strip_prefix("http://") {
                h
            } else if let Some(h) = endpoint.strip_prefix("https://") {
                h
            } else {
                endpoint
            };
            
            // Remove port and path
            let hostname = host
                .split(':').next()  // Remove port
                .unwrap_or(host)
                .split('/').next()  // Remove path
                .unwrap_or(host);
                
            return hostname.to_string();
        }
        
        // Fall back to general advertise address
        self.get_advertise_address()
    }
}

impl Validatable for NetworkingConfig {
    fn validate(&self) -> Vec<ValidationResult> {
        Vec::new() // Add specific validations as needed
    }
    
    fn apply_safe_defaults(&mut self) {
        info!("Applying safe defaults to Networking configuration");
        self.failure_detector.apply_safe_defaults();
        self.inter_node.apply_safe_defaults();
    }
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KubernetesNetworkingConfig {
    #[serde(default = "default_k8s_namespace")]
    pub namespace: String,
    
    #[serde(default = "default_k8s_headless_service")]
    pub headless_service: String,
}

impl Default for KubernetesNetworkingConfig {
    fn default() -> Self {
        Self {
            namespace: default_k8s_namespace(),
            headless_service: default_k8s_headless_service(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SeedDiscoveryConfig {
    #[serde(default = "default_openwit_seeds")]
    pub openwit_seeds: String,
    
    #[serde(default = "default_true")]
    pub auto_discovery: bool,
}

impl Default for SeedDiscoveryConfig {
    fn default() -> Self {
        Self {
            openwit_seeds: default_openwit_seeds(),
            auto_discovery: default_true(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailureDetectorConfig {
    #[serde(default = "default_phi_threshold")]
    pub phi_threshold: f64,
    
    #[serde(default = "default_min_std_deviation_ms")]
    pub min_std_deviation_ms: u32,
    
    #[serde(default = "default_acceptable_heartbeat_pause_ms")]
    pub acceptable_heartbeat_pause_ms: u32,
    
    #[serde(default = "default_first_heartbeat_estimate_ms")]
    pub first_heartbeat_estimate_ms: u32,
    
    #[serde(default = "default_max_sample_size")]
    pub max_sample_size: u32,
}

impl Default for FailureDetectorConfig {
    fn default() -> Self {
        Self {
            phi_threshold: default_phi_threshold(),
            min_std_deviation_ms: default_min_std_deviation_ms(),
            acceptable_heartbeat_pause_ms: default_acceptable_heartbeat_pause_ms(),
            first_heartbeat_estimate_ms: default_first_heartbeat_estimate_ms(),
            max_sample_size: default_max_sample_size(),
        }
    }
}

impl FailureDetectorConfig {
    fn apply_safe_defaults(&mut self) {
        self.phi_threshold = 10.0; // More conservative threshold
        self.acceptable_heartbeat_pause_ms = 5000; // More tolerance
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceMeshConfig {
    #[serde(default)]
    pub enabled: bool,
    
    #[serde(default = "default_service_mesh_provider")]
    pub provider: String,
}

impl Default for ServiceMeshConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            provider: default_service_mesh_provider(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InterNodeConfig {
    #[serde(default = "default_connection_timeout_ms")]
    pub connection_timeout_ms: u32,
    
    #[serde(default = "default_request_timeout_ms")]
    pub request_timeout_ms: u32,
    
    #[serde(default = "default_max_connections_per_node")]
    pub max_connections_per_node: u32,
    
    #[serde(default = "default_keepalive_interval_ms")]
    pub keepalive_interval_ms: u32,
}

impl Default for InterNodeConfig {
    fn default() -> Self {
        Self {
            connection_timeout_ms: default_connection_timeout_ms(),
            request_timeout_ms: default_request_timeout_ms(),
            max_connections_per_node: default_max_connections_per_node(),
            keepalive_interval_ms: default_keepalive_interval_ms(),
        }
    }
}

impl InterNodeConfig {
    fn apply_safe_defaults(&mut self) {
        self.connection_timeout_ms = 10000; // Longer timeout
        self.request_timeout_ms = 60000; // Longer request timeout
        self.max_connections_per_node = 5; // Fewer connections
    }
}

// Default functions
fn default_true() -> bool { true }
fn default_self_node_name() -> String { "".to_string() }
fn default_k8s_namespace() -> String { "".to_string() }
fn default_k8s_headless_service() -> String { "openwit-headless:9090".to_string() }
fn default_openwit_seeds() -> String { "".to_string() }
fn default_phi_threshold() -> f64 { 8.0 }
fn default_min_std_deviation_ms() -> u32 { 500 }
fn default_acceptable_heartbeat_pause_ms() -> u32 { 2000 }
fn default_first_heartbeat_estimate_ms() -> u32 { 100 }
fn default_max_sample_size() -> u32 { 200 }
fn default_service_mesh_provider() -> String { "istio".to_string() }
fn default_connection_timeout_ms() -> u32 { 5000 }
fn default_request_timeout_ms() -> u32 { 30000 }
fn default_max_connections_per_node() -> u32 { 10 }
fn default_keepalive_interval_ms() -> u32 { 10000 }
