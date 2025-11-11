use std::sync::Arc;
use std::collections::{HashMap};
use anyhow::Result;
use tokio::sync::RwLock;
use tracing::{info, warn, debug, error};

use openwit_network::ClusterHandle;
use openwit_config::UnifiedConfig;
use openwit_inter_node::{ArrowFlightClient, NodeAddressCache};
use crate::proxy_buffer::ProxyMessage;
use crate::control_plane_client::{ProxyControlPlaneClient, ServiceEndpoint};
use crate::health_aware_router::{HealthAwareRouter, RoutingStrategy};

/// Extended router for distributed deployments with multi-instance support
#[allow(dead_code)]
pub struct DistributedRouter {
    node_id: String,
    cluster_handle: ClusterHandle,
    config: UnifiedConfig,
    /// Control plane client for service discovery
    control_client: Arc<ProxyControlPlaneClient>,
    /// Health-aware router for intelligent routing
    health_router: Arc<HealthAwareRouter>,
    /// Service instance pools by type
    service_pools: Arc<RwLock<ServicePools>>,
    /// Arrow Flight client from inter-node
    flight_client: Arc<ArrowFlightClient>,
    /// Address cache for node endpoints
    address_cache: Arc<NodeAddressCache>,
    /// Statistics
    stats: Arc<RwLock<RouterStats>>,
}

#[derive(Default)]
struct ServicePools {
    /// HTTP service instances
    http_services: HashMap<String, ServiceInstance>,
    /// gRPC service instances
    grpc_services: HashMap<String, ServiceInstance>,
    /// Ingestor service instances (for Arrow Flight)
    ingestor_services: HashMap<String, ServiceInstance>,
    /// Last update time
    last_update: Option<std::time::Instant>,
}

#[derive(Clone)]
struct ServiceInstance {
    node_id: String,
    endpoint: String,
    service_type: String,
    health_score: f64,
    last_used: std::time::Instant,
    request_count: u64,
    error_count: u64,
}

#[derive(Default)]
struct RouterStats {
    total_requests: u64,
    successful_requests: u64,
    failed_requests: u64,
    service_discoveries: u64,
    routing_decisions: u64,
}

#[derive(Debug, Clone)]
pub struct IngestorEndpoint {
    pub node_id: String,
    pub flight_endpoint: String,
    pub health_score: f32,
}

impl DistributedRouter {
    pub async fn new(
        node_id: String,
        cluster_handle: ClusterHandle,
        config: UnifiedConfig,
    ) -> Result<Self> {
        // Create control plane client
        let control_client = Arc::new(
            ProxyControlPlaneClient::new(node_id.clone(), config.clone()).await?
        );
        
        // Create health-aware router
        let routing_strategy = RoutingStrategy::WeightedHealth; // Best for distributed
        let health_router = Arc::new(HealthAwareRouter::new(
            node_id.clone(),
            control_client.clone(),
            routing_strategy,
        ));
        
        // Create address cache for proxy node
        let address_cache = Arc::new(NodeAddressCache::new(
            node_id.clone(),
            "proxy".to_string(),
        ));
        
        Ok(Self {
            node_id: node_id.clone(),
            cluster_handle,
            config,
            control_client,
            health_router,
            service_pools: Arc::new(RwLock::new(ServicePools::default())),
            flight_client: Arc::new(ArrowFlightClient::new(node_id)),
            address_cache,
            stats: Arc::new(RwLock::new(RouterStats::default())),
        })
    }
    
    /// Start background tasks for distributed mode
    pub async fn start(self: Arc<Self>) {
        // Start health router maintenance
        self.health_router.clone().start_maintenance().await;
        
        // Start control client health monitor
        self.control_client.clone().start_health_monitor().await;
        
        let self_clone = self.clone();
        let self_clone2 = self.clone();
        
        // Task 1: Update service pools (every 10s)
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(10));
            
            loop {
                interval.tick().await;
                if let Err(e) = self_clone.update_service_pools().await {
                    warn!("Failed to update service pools: {}", e);
                }
            }
        });
        
        // Task 2: Clean stale service instances (every 60s)
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
            
            loop {
                interval.tick().await;
                self_clone2.cleanup_stale_instances().await;
            }
        });
        
        // Initial service discovery
        let _ = self.update_service_pools().await;
    }
    
    /// Update service pools from control plane
    async fn update_service_pools(&self) -> Result<()> {
        let mut stats = self.stats.write().await;
        stats.service_discoveries += 1;
        drop(stats);
        
        // Fetch all service types in parallel
        let (http_services, grpc_services, ingestor_services) = tokio::try_join!(
            self.control_client.get_healthy_http_services(),
            self.control_client.get_healthy_grpc_services(),
            self.fetch_healthy_ingestors()
        )?;
        
        let mut pools = self.service_pools.write().await;
        
        // Update HTTP services
        Self::update_service_map(&mut pools.http_services, http_services, "http");
        
        // Update gRPC services
        Self::update_service_map(&mut pools.grpc_services, grpc_services, "grpc");
        
        // Update ingestor services
        Self::update_service_map(&mut pools.ingestor_services, ingestor_services, "ingestor");
        
        pools.last_update = Some(std::time::Instant::now());
        
        info!("Service pools updated: {} HTTP, {} gRPC, {} ingestors",
            pools.http_services.len(),
            pools.grpc_services.len(),
            pools.ingestor_services.len()
        );
        
        Ok(())
    }
    
    /// Update a service map with new healthy services
    fn update_service_map(
        map: &mut HashMap<String, ServiceInstance>,
        services: Vec<ServiceEndpoint>,
        service_type: &str,
    ) {
        // Mark existing services as potentially stale
        let _existing_nodes: std::collections::HashSet<_> =
            map.keys().cloned().collect();
        
        // Update or add healthy services
        for service in &services {
            match map.get_mut(&service.node_id) {
                Some(instance) => {
                    // Update existing instance
                    instance.health_score = service.health_score;
                    instance.endpoint = service.endpoint.clone();
                }
                None => {
                    // Add new instance
                    let instance = ServiceInstance {
                        node_id: service.node_id.clone(),
                        endpoint: service.endpoint.clone(),
                        service_type: service_type.to_string(),
                        health_score: service.health_score,
                        last_used: std::time::Instant::now(),
                        request_count: 0,
                        error_count: 0,
                    };
                    map.insert(service.node_id.clone(), instance);
                }
            }
        }
        
        // Remove services that are no longer healthy
        let healthy_nodes: std::collections::HashSet<_> = 
            services.iter().map(|s| s.node_id.clone()).collect();
        
        map.retain(|node_id, _| healthy_nodes.contains(node_id));
    }
    
    /// Fetch healthy ingestor services
    async fn fetch_healthy_ingestors(&self) -> Result<Vec<ServiceEndpoint>> {
        // Get ingestor nodes from gossip and control plane
        let ingestor_nodes = self.cluster_handle.view
            .get_healthy_nodes_with_role("ingest").await;
        
        Ok(ingestor_nodes.into_iter().map(|node_id| {
            ServiceEndpoint {
                node_id: node_id.clone(),
                endpoint: format!("{}:8089", node_id), // Arrow Flight port
                health_score: 1.0,
                cpu_percent: 0.0,
                memory_percent: 0.0,
                active_connections: 0,
            }
        }).collect())
    }
    
    /// Clean up stale service instances
    async fn cleanup_stale_instances(&self) {
        let mut pools = self.service_pools.write().await;
        let now = std::time::Instant::now();
        let stale_threshold = std::time::Duration::from_secs(300); // 5 minutes
        
        // Clean up each service type
        Self::cleanup_service_map(&mut pools.http_services, now, stale_threshold);
        Self::cleanup_service_map(&mut pools.grpc_services, now, stale_threshold);
        Self::cleanup_service_map(&mut pools.ingestor_services, now, stale_threshold);
        
        debug!("Service pool cleanup complete");
    }
    
    /// Clean up a specific service map
    fn cleanup_service_map(
        map: &mut HashMap<String, ServiceInstance>,
        now: std::time::Instant,
        threshold: std::time::Duration,
    ) {
        map.retain(|node_id, instance| {
            let age = now.duration_since(instance.last_used);
            if age > threshold && instance.request_count == 0 {
                info!("Removing stale {} service: {}", instance.service_type, node_id);
                false
            } else {
                true
            }
        });
    }
    
    /// Route batch to best available ingestor using Arrow Flight
    pub async fn route_batch_flight(
        &self, 
        messages: Vec<ProxyMessage>,
        batch_id: &str,
    ) -> Result<()> {
        let mut stats = self.stats.write().await;
        stats.total_requests += 1;
        stats.routing_decisions += 1;
        drop(stats);
        
        // Select best ingestor
        let target = self.select_best_ingestor().await?;
        
        info!("Routing batch {} ({} messages) to ingestor {} via Arrow Flight", 
              batch_id, messages.len(), target.node_id);
        
        // Update instance usage
        {
            let mut pools = self.service_pools.write().await;
            if let Some(instance) = pools.ingestor_services.get_mut(&target.node_id) {
                instance.last_used = std::time::Instant::now();
                instance.request_count += 1;
            }
        }
        
        // Convert messages to Arrow RecordBatch
        let record_batch = crate::arrow_utils::convert_to_arrow_batch(&messages)?;
        
        // Add metadata
        let mut metadata = HashMap::new();
        metadata.insert("source_node".to_string(), self.node_id.clone());
        metadata.insert("batch_size".to_string(), messages.len().to_string());
        metadata.insert("timestamp".to_string(), chrono::Utc::now().to_rfc3339());
        
        // Send via Arrow Flight
        match self.flight_client.send_batch(
            &target.endpoint,
            batch_id,
            record_batch,
            metadata,
        ).await {
            Ok(_) => {
                let mut stats = self.stats.write().await;
                stats.successful_requests += 1;
                
                // Report success to health router
                self.health_router.report_success(&target.node_id).await;
                
                // Report batch completion
                self.report_batch_completion(batch_id, &target.node_id, messages.len()).await?;
                
                Ok(())
            }
            Err(e) => {
                error!("Failed to send batch to {}: {}", target.node_id, e);
                
                let mut stats = self.stats.write().await;
                stats.failed_requests += 1;
                
                // Update instance error count
                let mut pools = self.service_pools.write().await;
                if let Some(instance) = pools.ingestor_services.get_mut(&target.node_id) {
                    instance.error_count += 1;
                }
                
                // Report failure to health router
                self.health_router.report_failure(&target.node_id, "ingestor").await;
                
                Err(e)
            }
        }
    }
    
    /// Select the best ingestor based on load and health
    async fn select_best_ingestor(&self) -> Result<ServiceEndpoint> {
        let pools = self.service_pools.read().await;
        
        if pools.ingestor_services.is_empty() {
            return Err(anyhow::anyhow!("No healthy ingestor services available"));
        }
        
        // Convert to ServiceEndpoint format for health router
        let endpoints: Vec<ServiceEndpoint> = pools.ingestor_services
            .values()
            .map(|instance| ServiceEndpoint {
                node_id: instance.node_id.clone(),
                endpoint: instance.endpoint.clone(),
                health_score: instance.health_score,
                cpu_percent: 0.0, // Would need actual metrics
                memory_percent: 0.0,
                active_connections: instance.request_count as u32,
            })
            .collect();
        
        // Use health router to select best endpoint
        ProxyControlPlaneClient::select_best_endpoint(&endpoints)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("Failed to select ingestor"))
    }
    
    /// Report batch completion to control plane
    async fn report_batch_completion(
        &self,
        batch_id: &str,
        ingestor_id: &str,
        message_count: usize,
    ) -> Result<()> {
        debug!("Batch {} sent to {} ({} messages)", batch_id, ingestor_id, message_count);
        
        // TODO: Report to control plane via client when API is available
        // For now, just update local stats
        
        Ok(())
    }
    
    /// Get router statistics
    pub async fn get_stats(&self) -> serde_json::Value {
        let stats = self.stats.read().await;
        let pools = self.service_pools.read().await;
        
        // Calculate service-specific stats
        let service_stats: HashMap<String, serde_json::Value> = [
            ("http", &pools.http_services),
            ("grpc", &pools.grpc_services),
            ("ingestor", &pools.ingestor_services),
        ].iter()
        .map(|(name, services)| {
            let total_requests: u64 = services.values().map(|s| s.request_count).sum();
            let total_errors: u64 = services.values().map(|s| s.error_count).sum();
            let avg_health: f64 = if services.is_empty() {
                0.0
            } else {
                services.values().map(|s| s.health_score).sum::<f64>() / services.len() as f64
            };
            
            (name.to_string(), serde_json::json!({
                "instance_count": services.len(),
                "total_requests": total_requests,
                "total_errors": total_errors,
                "average_health": avg_health,
                "error_rate": if total_requests > 0 {
                    (total_errors as f64 / total_requests as f64) * 100.0
                } else {
                    0.0
                },
            }))
        })
        .collect();
        
        serde_json::json!({
            "node_id": self.node_id,
            "total_requests": stats.total_requests,
            "successful_requests": stats.successful_requests,
            "failed_requests": stats.failed_requests,
            "success_rate": if stats.total_requests > 0 {
                (stats.successful_requests as f64 / stats.total_requests as f64) * 100.0
            } else {
                0.0
            },
            "service_discoveries": stats.service_discoveries,
            "routing_decisions": stats.routing_decisions,
            "service_pools": service_stats,
            "last_update": pools.last_update.map(|t| t.elapsed().as_secs()),
        })
    }
    
    /// Route to specific service type with multi-instance support
    pub async fn route_to_service(
        &self,
        service_type: &str,
        request_id: &str,
    ) -> Result<ServiceEndpoint> {
        let pools = self.service_pools.read().await;
        
        let services = match service_type {
            "http" => &pools.http_services,
            "grpc" => &pools.grpc_services,
            "ingestor" => &pools.ingestor_services,
            _ => return Err(anyhow::anyhow!("Unknown service type: {}", service_type)),
        };
        
        if services.is_empty() {
            return Err(anyhow::anyhow!("No {} services available", service_type));
        }
        
        // Convert to endpoints for routing
        let endpoints: Vec<ServiceEndpoint> = services.values()
            .map(|s| ServiceEndpoint {
                node_id: s.node_id.clone(),
                endpoint: s.endpoint.clone(),
                health_score: s.health_score,
                cpu_percent: 0.0,
                memory_percent: 0.0,
                active_connections: s.request_count as u32,
            })
            .collect();
        
        // Use health-aware routing
        let hash = crate::health_aware_router::compute_hash(request_id);
        match service_type {
            "http" => self.health_router.select_http_service(Some(hash)).await,
            "grpc" => self.health_router.select_grpc_service(Some(hash)).await,
            _ => ProxyControlPlaneClient::select_best_endpoint(&endpoints)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("Failed to select service")),
        }
    }
}