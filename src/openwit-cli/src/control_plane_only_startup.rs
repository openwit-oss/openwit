use anyhow::Result;
use openwit_config::UnifiedConfig;
use tracing::info;

/// Start OpenWit in control plane only mode
pub async fn start_control_plane_only(
    config: UnifiedConfig,
    provider: String,
    endpoint: String,
    update_interval: u64,
) -> Result<()> {
    info!("Starting OpenWit in Control Plane Only Mode");
    info!("════════════════════════════════════════");
    info!("Provider: {}", provider);
    info!("Endpoint: {}", endpoint);
    info!("Update interval: {}s", update_interval);
    
    // Create control plane only config
    let cp_config = openwit_proxy::ControlPlaneOnlyConfig {
        provider: provider.clone(),
        endpoint: endpoint.clone(),
        update_interval_secs: update_interval,
        domain: config.control_plane_only.as_ref()
            .and_then(|c| c.domain.clone()),
        socket_path: config.control_plane_only.as_ref()
            .and_then(|c| c.socket_path.clone()),
        target_groups: config.control_plane_only.as_ref()
            .and_then(|c| c.target_groups.clone()),
    };
    
    // Create the provider
    let provider = openwit_proxy::create_provider(&cp_config)?;
    
    // Create control plane only instance
    let control_plane_only = openwit_proxy::ControlPlaneOnly::new(
        config.control_plane.endpoint.clone(),
        provider,
        tokio::time::Duration::from_secs(update_interval),
    );
    
    // Log configuration details
    match provider.as_str() {
        "coredns" => {
            info!("CoreDNS Configuration:");
            info!("  Domain: {}", cp_config.domain.as_deref().unwrap_or("openwit.local"));
            info!("  TTL: 30s");
            info!("");
            info!("Clients can now use DNS names:");
            info!("  http-service.{}", cp_config.domain.as_deref().unwrap_or("openwit.local"));
            info!("  grpc-service.{}", cp_config.domain.as_deref().unwrap_or("openwit.local"));
        },
        "haproxy" => {
            info!("HAProxy Configuration:");
            info!("  Socket: {}", cp_config.socket_path.as_deref().unwrap_or("/var/run/haproxy.sock"));
            info!("  Backends will be updated dynamically");
        },
        "consul" => {
            info!("Consul Configuration:");
            info!("  Services will be registered in Consul");
            info!("  Clients can use Consul DNS or API for discovery");
        },
        "aws-alb" => {
            info!("AWS ALB Configuration:");
            info!("  Target groups will be updated dynamically");
            info!("  Ensure IAM permissions for elasticloadbalancing:*");
        },
        _ => {},
    }
    
    info!("");
    info!("Control plane only mode active. Services will be discovered and");
    info!("registered with {} automatically.", provider);
    info!("");
    info!("To test direct access:");
    info!("  1. Start your HTTP/gRPC services");
    info!("  2. Wait for service registration (~10s)");
    info!("  3. Access services directly via DNS/LB");
    info!("");
    
    // Start the control plane only loop
    control_plane_only.start().await?;
    
    Ok(())
}

/// Example: How clients would discover services
pub mod client_examples {
    use anyhow::Result;
    
    /// DNS-based discovery
    pub async fn discover_via_dns(service_name: &str, domain: &str) -> Result<Vec<String>> {
        use trust_dns_resolver::TokioAsyncResolver;
        
        let resolver = TokioAsyncResolver::tokio_from_system_conf()?;
        let fqdn = format!("{}.{}", service_name, domain);
        
        let response = resolver.lookup_ip(fqdn).await?;
        let addresses: Vec<String> = response.iter()
            .map(|ip| ip.to_string())
            .collect();
            
        Ok(addresses)
    }
    
    /// Consul-based discovery
    pub async fn discover_via_consul(service_name: &str, consul_endpoint: &str) -> Result<Vec<String>> {
        let client = reqwest::Client::new();
        let url = format!("{}/v1/catalog/service/{}", consul_endpoint, service_name);
        
        #[derive(serde::Deserialize)]
        struct ConsulService {
            #[serde(rename = "ServiceAddress")]
            address: String,
            #[serde(rename = "ServicePort")]
            port: u16,
        }
        
        let services: Vec<ConsulService> = client.get(&url)
            .send()
            .await?
            .json()
            .await?;
            
        let addresses = services.iter()
            .map(|s| format!("{}:{}", s.address, s.port))
            .collect();
            
        Ok(addresses)
    }
    
    /// Static configuration (fallback)
    pub fn discover_via_config(service_name: &str, config: &ClientConfig) -> Result<Vec<String>> {
        config.endpoints.get(service_name)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("Service {} not found in config", service_name))
    }
    
    #[derive(serde::Deserialize)]
    pub struct ClientConfig {
        pub mode: String,
        pub service_discovery: String,
        pub dns_suffix: Option<String>,
        pub consul_endpoint: Option<String>,
        pub endpoints: std::collections::HashMap<String, Vec<String>>,
    }
}

/// Updated client that supports both proxy and direct modes
pub struct OpenWitClient {
    mode: ClientMode,
    http_client: reqwest::Client,
}

pub enum ClientMode {
    /// Traditional proxy mode
    Proxy { endpoint: String },
    
    /// Direct mode with DNS discovery
    DirectDns { domain: String },
    
    /// Direct mode with Consul discovery
    DirectConsul { endpoint: String },
    
    /// Direct mode with static endpoints
    DirectStatic { endpoints: std::collections::HashMap<String, Vec<String>> },
}

impl OpenWitClient {
    pub async fn send_traces(&self, traces: Vec<Trace>) -> Result<()> {
        let endpoint = match &self.mode {
            ClientMode::Proxy { endpoint } => {
                // Send to proxy
                format!("{}/v1/traces", endpoint)
            },
            ClientMode::DirectDns { domain } => {
                // Discover via DNS
                let addresses = client_examples::discover_via_dns("http-service", domain).await?;
                let addr = addresses.first()
                    .ok_or_else(|| anyhow::anyhow!("No healthy HTTP services found"))?;
                format!("http://{}/v1/traces", addr)
            },
            ClientMode::DirectConsul { endpoint } => {
                // Discover via Consul
                let addresses = client_examples::discover_via_consul("http-service", endpoint).await?;
                let addr = addresses.first()
                    .ok_or_else(|| anyhow::anyhow!("No healthy HTTP services found"))?;
                format!("http://{}/v1/traces", addr)
            },
            ClientMode::DirectStatic { endpoints } => {
                // Use static config
                let addresses = endpoints.get("http")
                    .ok_or_else(|| anyhow::anyhow!("No HTTP endpoints configured"))?;
                let addr = addresses.first()
                    .ok_or_else(|| anyhow::anyhow!("No HTTP endpoints configured"))?;
                format!("http://{}/v1/traces", addr)
            },
        };
        
        self.http_client.post(&endpoint)
            .json(&traces)
            .send()
            .await?;
            
        Ok(())
    }
}