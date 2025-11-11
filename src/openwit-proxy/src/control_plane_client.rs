use std::sync::Arc;
use std::collections::HashMap;
use std::path::PathBuf;
use anyhow::Result;
use tokio::sync::RwLock;
use tonic::transport::Channel;
use tracing::{info, warn, error, debug};
use serde::{Serialize, Deserialize};

use openwit_config::UnifiedConfig;
use openwit_proto::control::{
    control_plane_service_client::ControlPlaneServiceClient,
    GetHealthyNodesRequest,
};

/// Enhanced control plane client for proxy with service health monitoring
#[derive(Clone)]
pub struct ProxyControlPlaneClient {
    node_id: String,
    client: Arc<RwLock<Option<ControlPlaneServiceClient<Channel>>>>,
    config: UnifiedConfig,
    /// Cached healthy services by type
    healthy_services: Arc<RwLock<HealthyServicesCache>>,
    /// Last successful connection time
    last_connection: Arc<RwLock<Option<std::time::Instant>>>,
}

#[derive(Debug, Clone, Default)]
struct HealthyServicesCache {
    /// HTTP service endpoints
    http_services: Vec<ServiceEndpoint>,
    /// gRPC service endpoints  
    grpc_services: Vec<ServiceEndpoint>,
    /// Kafka brokers
    kafka_brokers: Vec<KafkaEndpoint>,
    /// Last update timestamp
    last_update: Option<std::time::Instant>,
}

#[derive(Debug, Clone)]
pub struct ServiceEndpoint {
    pub node_id: String,
    pub endpoint: String,
    pub health_score: f64,
    pub cpu_percent: f64,
    pub memory_percent: f64,
    pub active_connections: u32,
}

#[derive(Debug, Clone)]
pub struct KafkaEndpoint {
    pub broker_id: String,
    pub endpoint: String,
    pub topics: Vec<String>,
    pub health_score: f64,
}

impl ProxyControlPlaneClient {
    /// Create a new control plane client for the proxy
    pub async fn new(node_id: String, config: UnifiedConfig) -> Result<Self> {
        let client = Arc::new(RwLock::new(None));
        
        let proxy_client = Self {
            node_id,
            client,
            config,
            healthy_services: Arc::new(RwLock::new(HealthyServicesCache::default())),
            last_connection: Arc::new(RwLock::new(None)),
        };
        
        // Try initial connection, but don't fail if it doesn't work
        if let Err(e) = proxy_client.connect().await {
            warn!("Failed initial connection to control plane: {}, will use service registry", e);
        }
        
        // Load services from environment variables if available
        proxy_client.load_services_from_env().await;
        
        Ok(proxy_client)
    }
    
    /// Connect or reconnect to control plane
    async fn connect(&self) -> Result<()> {
        // Get control plane endpoint from config or discover via gossip
        let endpoint = self.get_control_plane_endpoint().await?;
        
        info!("Connecting to control plane at {}", endpoint);
        
        let channel = Channel::from_shared(endpoint.clone())?
            .connect_timeout(std::time::Duration::from_secs(5))
            .timeout(std::time::Duration::from_secs(30))
            .connect()
            .await?;
            
        let client = ControlPlaneServiceClient::new(channel);
        
        *self.client.write().await = Some(client);
        *self.last_connection.write().await = Some(std::time::Instant::now());
        
        info!("Successfully connected to control plane");
        
        Ok(())
    }
    
    /// Get control plane endpoint from config or gossip
    async fn get_control_plane_endpoint(&self) -> Result<String> {
        // Use endpoint from config if available
        Ok(self.config.control_plane.grpc_endpoint.clone())
    }
    
    /// Load services from environment variables set by distributed auto-config
    async fn load_services_from_env(&self) {
        let mut cache = self.healthy_services.write().await;
        
        // Load HTTP endpoints from environment
        if let Ok(http_endpoints) = std::env::var("OPENWIT_HTTP_ENDPOINTS") {
            let services: Vec<ServiceEndpoint> = http_endpoints
                .split(',')
                .filter(|s| !s.is_empty())
                .enumerate()
                .map(|(i, endpoint)| ServiceEndpoint {
                    node_id: format!("http-env-{}", i),
                    endpoint: endpoint.to_string(),
                    health_score: 1.0,
                    cpu_percent: 0.0,
                    memory_percent: 0.0,
                    active_connections: 0,
                })
                .collect();
            
            if !services.is_empty() {
                info!("Loaded {} HTTP endpoints from environment", services.len());
                cache.http_services = services;
            }
        }
        
        // Load ingestion endpoints from environment
        if let Ok(ingest_endpoints) = std::env::var("OPENWIT_INGESTION_ENDPOINTS") {
            let services: Vec<ServiceEndpoint> = ingest_endpoints
                .split(',')
                .filter(|s| !s.is_empty())
                .enumerate()
                .map(|(i, endpoint)| ServiceEndpoint {
                    node_id: format!("grpc-env-{}", i),
                    endpoint: endpoint.to_string(),
                    health_score: 1.0,
                    cpu_percent: 0.0,
                    memory_percent: 0.0,
                    active_connections: 0,
                })
                .collect();
            
            if !services.is_empty() {
                info!("Loaded {} gRPC endpoints from environment", services.len());
                cache.grpc_services = services;
            }
        }
        
        cache.last_update = Some(std::time::Instant::now());
    }
    
    /// Ensure we have a valid connection
    async fn ensure_connected(&self) -> Result<()> {
        let needs_reconnect = {
            let client = self.client.read().await;
            if client.is_none() {
                true
            } else {
                // Check if connection is stale (>60s)
                let last_conn = self.last_connection.read().await;
                last_conn.map_or(true, |t| t.elapsed() > std::time::Duration::from_secs(60))
            }
        };
        
        if needs_reconnect {
            match self.connect().await {
                Ok(_) => Ok(()),
                Err(e) => {
                    warn!("Failed to connect to control plane: {}, will use service registry fallback", e);
                    // Don't fail here, let the caller handle fallback
                    Err(e)
                }
            }
        } else {
            Ok(())
        }
    }
    
    /// Get healthy HTTP services
    pub async fn get_healthy_http_services(&self) -> Result<Vec<ServiceEndpoint>> {
        self.refresh_services_if_needed().await?;
        
        let cache = self.healthy_services.read().await;
        Ok(cache.http_services.clone())
    }
    
    /// Get healthy gRPC services
    pub async fn get_healthy_grpc_services(&self) -> Result<Vec<ServiceEndpoint>> {
        self.refresh_services_if_needed().await?;
        
        let cache = self.healthy_services.read().await;
        Ok(cache.grpc_services.clone())
    }
    
    /// Get healthy Kafka brokers
    pub async fn get_healthy_kafka_brokers(&self) -> Result<Vec<KafkaEndpoint>> {
        self.refresh_services_if_needed().await?;
        
        let cache = self.healthy_services.read().await;
        Ok(cache.kafka_brokers.clone())
    }
    
    /// Get Kafka brokers as a comma-separated string for consumer configuration
    pub async fn get_kafka_brokers(&self) -> Result<String> {
        // First try to get from config
        if let Some(brokers) = &self.config.ingestion.kafka.brokers {
            return Ok(brokers.clone());
        }
        
        // Fall back to healthy brokers
        let healthy_brokers = self.get_healthy_kafka_brokers().await?;
        if !healthy_brokers.is_empty() {
            let broker_list = healthy_brokers.iter()
                .map(|b| b.endpoint.clone())
                .collect::<Vec<String>>()
                .join(",");
            return Ok(broker_list);
        }
        
        // Last resort fallback
        Ok("localhost:9092".to_string())
    }
    
    /// Refresh services if cache is stale
    async fn refresh_services_if_needed(&self) -> Result<()> {
        let needs_refresh = {
            let cache = self.healthy_services.read().await;
            cache.last_update.map_or(true, |t| t.elapsed() > std::time::Duration::from_secs(10))
        };
        
        if needs_refresh {
            self.refresh_all_services().await?;
        }
        
        Ok(())
    }
    
    /// Refresh all service types from control plane
    async fn refresh_all_services(&self) -> Result<()> {
        // Use control plane only - file-based registry removed
        self.refresh_from_control_plane().await
    }
    
    /// Refresh services from control plane
    async fn refresh_from_control_plane(&self) -> Result<()> {
        self.ensure_connected().await?;
        
        // Fetch all service types in parallel
        let (http_services, grpc_services, kafka_brokers) = tokio::try_join!(
            self.fetch_healthy_services("http"),
            self.fetch_healthy_services("grpc"),
            self.fetch_kafka_brokers()
        )?;
        
        // Update cache
        let mut cache = self.healthy_services.write().await;
        cache.http_services = http_services;
        cache.grpc_services = grpc_services;
        cache.kafka_brokers = kafka_brokers;
        cache.last_update = Some(std::time::Instant::now());
        
        info!("Refreshed healthy services cache from control plane: {} HTTP, {} gRPC, {} Kafka",
            cache.http_services.len(),
            cache.grpc_services.len(),
            cache.kafka_brokers.len()
        );
        
        Ok(())
    }
    
    /// Fetch healthy services of a specific type
    async fn fetch_healthy_services(&self, service_type: &str) -> Result<Vec<ServiceEndpoint>> {
        let mut client_guard = self.client.write().await;
        let client = client_guard.as_mut()
            .ok_or_else(|| anyhow::anyhow!("Control plane client not connected"))?;
        
        let request = tonic::Request::new(GetHealthyNodesRequest {
            requester_id: self.node_id.clone(),
            role: service_type.to_string(),
            max_nodes: 0, // No limit
        });
        
        match client.get_healthy_nodes(request).await {
            Ok(response) => {
                let nodes: Vec<ServiceEndpoint> = response.into_inner().nodes
                    .into_iter()
                    .map(|n| {
                        // Use appropriate endpoint based on service type
                        let endpoint = match service_type {
                            "http" => n.metadata.get("http_endpoint")
                                .or_else(|| n.metadata.get("bind_address"))
                                .cloned()
                                .unwrap_or_else(|| n.grpc_endpoint.clone()),
                            "grpc" => n.grpc_endpoint.clone(),
                            _ => n.grpc_endpoint.clone(),
                        };
                        
                        // Extract CPU and memory from metadata if available
                        let cpu_percent = n.metadata.get("cpu_percent")
                            .and_then(|v| v.parse::<f64>().ok())
                            .unwrap_or(0.0);
                        
                        let memory_percent = n.metadata.get("memory_percent")
                            .and_then(|v| v.parse::<f64>().ok())
                            .unwrap_or(0.0);
                        
                        let active_connections = n.metadata.get("active_connections")
                            .and_then(|v| v.parse::<u32>().ok())
                            .unwrap_or(0);
                        
                        ServiceEndpoint {
                            node_id: n.node_id,
                            endpoint,
                            health_score: n.health_score,
                            cpu_percent,
                            memory_percent,
                            active_connections,
                        }
                    })
                    .filter(|e| e.health_score > 0.5) // Only include reasonably healthy nodes
                    .collect();
                    
                debug!("Fetched {} healthy {} services", nodes.len(), service_type);
                Ok(nodes)
            }
            Err(e) => {
                error!("Failed to get healthy {} services from control plane: {}", service_type, e);

                // Return cached data if available
                let cache = self.healthy_services.read().await;
                match service_type {
                    "http" => Ok(cache.http_services.clone()),
                    "grpc" => Ok(cache.grpc_services.clone()),
                    _ => Ok(vec![]),
                }
            }
        }
    }
    
    /// Fetch healthy Kafka brokers
    async fn fetch_kafka_brokers(&self) -> Result<Vec<KafkaEndpoint>> {
        // Get brokers from config
        let default_broker = "localhost:9092".to_string();
        let broker_string = self.config.ingestion.kafka.brokers
            .as_ref()
            .unwrap_or(&default_broker);
            
        let brokers: Vec<KafkaEndpoint> = broker_string
            .split(',')
            .enumerate()
            .map(|(i, broker)| KafkaEndpoint {
                broker_id: format!("broker-{}", i),
                endpoint: broker.to_string().trim().to_string(),
                topics: self.config.ingestion.kafka.topics.clone(),
                health_score: 0.0, // Will be updated by health check
            })
            .collect();
        
        // Perform health checks on brokers
        let mut healthy_brokers = Vec::new();
        for mut broker in brokers {
            let broker_endpoint = broker.endpoint.clone();
            match self.check_kafka_broker_health(&broker_endpoint).await {
                Ok(health_score) => {
                    broker.health_score = health_score;
                    if health_score > 0.5 {
                        debug!("Kafka broker {} is healthy (score: {})", broker_endpoint, health_score);
                        healthy_brokers.push(broker);
                    } else {
                        warn!("Kafka broker {} is unhealthy (score: {})", broker_endpoint, health_score);
                    }
                }
                Err(e) => {
                    warn!("Failed to check health of Kafka broker {}: {}", broker_endpoint, e);
                }
            }
        }
        
        if healthy_brokers.is_empty() {
            warn!("No healthy Kafka brokers found!");
        }
            
        Ok(healthy_brokers)
    }
    
    /// Check health of a single Kafka broker
    async fn check_kafka_broker_health(&self, broker_endpoint: &str) -> Result<f64> {
        // Create a simple Kafka admin client to check broker connectivity
        use rdkafka::admin::{AdminClient};
        use rdkafka::client::DefaultClientContext;
        use rdkafka::config::ClientConfig;
        use std::time::Duration;
        
        let admin_client: AdminClient<DefaultClientContext> = ClientConfig::new()
            .set("bootstrap.servers", broker_endpoint)
            .set("socket.timeout.ms", "5000")
            .set("request.timeout.ms", "5000")
            .create()
            .map_err(|e| anyhow::anyhow!("Failed to create admin client: {}", e))?;
        
        // Try to fetch cluster metadata as a health check
        let metadata = tokio::time::timeout(
            Duration::from_secs(5),
            tokio::task::spawn_blocking(move || {
                admin_client.inner().fetch_metadata(None, Duration::from_secs(5))
            })
        ).await;
        
        match metadata {
            Ok(Ok(Ok(metadata))) => {
                // Check if broker has topics and is reachable
                let broker_count = metadata.brokers().len();
                let topic_count = metadata.topics().len();
                
                if broker_count > 0 {
                    // Healthy: broker is reachable and part of cluster
                    debug!("Kafka broker {} is healthy: {} brokers, {} topics in cluster", 
                        broker_endpoint, broker_count, topic_count);
                    Ok(1.0)
                } else {
                    // Unhealthy: no brokers in metadata
                    Ok(0.0)
                }
            }
            Ok(Ok(Err(e))) => {
                // Failed to fetch metadata
                debug!("Failed to fetch metadata from Kafka broker {}: {}", broker_endpoint, e);
                Ok(0.0)
            }
            Ok(Err(e)) => {
                // Task panicked
                debug!("Task panicked while checking Kafka broker {}: {}", broker_endpoint, e);
                Ok(0.0)
            }
            Err(_) => {
                // Timeout
                debug!("Timeout checking Kafka broker {}", broker_endpoint);
                Ok(0.0)
            }
        }
    }
    
    /// Start background health refresh task
    pub async fn start_health_monitor(self: Arc<Self>) {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
            
            loop {
                interval.tick().await;
                
                if let Err(e) = self.refresh_all_services().await {
                    warn!("Failed to refresh healthy services: {}", e);
                }
            }
        });
    }
    
    /// Report proxy health to control plane
    /// NOTE: This method is disabled as control plane has been simplified to only service discovery
    pub async fn report_proxy_health(&self, _stats: ProxyHealthStats) -> Result<()> {
        // Health reporting disabled - control plane only provides service discovery now
        debug!("Proxy node {} is healthy", self.node_id);
        Ok(())
    }
    
    /// Get system health metrics
    async fn get_system_health(&self) -> Option<SystemHealth> {
        // Try to get health from system monitoring
        // For now, return placeholder values
        Some(SystemHealth {
            cpu_percent: 20.0,  // TODO: Get actual CPU usage
            memory_percent: 30.0,  // TODO: Get actual memory usage
        })
    }
    
    /// Get the best service endpoint based on load
    pub fn select_best_endpoint(endpoints: &[ServiceEndpoint]) -> Option<&ServiceEndpoint> {
        endpoints.iter()
            .filter(|e| e.health_score > 0.5)
            .min_by(|a, b| {
                // Prefer endpoint with lower resource usage
                let a_load = (a.cpu_percent + a.memory_percent) / 2.0;
                let b_load = (b.cpu_percent + b.memory_percent) / 2.0;
                a_load.partial_cmp(&b_load).unwrap_or(std::cmp::Ordering::Equal)
            })
    }
}

#[derive(Debug, Clone)]
pub struct ProxyHealthStats {
    pub active_connections: u32,
    pub requests_per_second: f64,
    pub bytes_per_second: f64,
    pub error_rate: f64,
    pub p99_latency_ms: f64,
}

#[derive(Debug, Clone)]
struct SystemHealth {
    pub cpu_percent: f64,
    pub memory_percent: f64,
}