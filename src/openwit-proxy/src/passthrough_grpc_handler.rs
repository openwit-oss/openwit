use std::sync::Arc;
use anyhow::Result;
use tonic::{Request, Response, Status};
use tonic::transport::{Channel, Endpoint};
use tracing::{debug, warn};
use tokio::sync::RwLock;
use futures::future::BoxFuture;

use crate::control_plane_client::{ProxyControlPlaneClient, ServiceEndpoint};

/// Pass-through gRPC handler that routes requests directly without buffering
pub struct PassthroughGrpcHandler {
    node_id: String,
    control_client: Arc<ProxyControlPlaneClient>,
    /// Cached gRPC channels to upstream services
    channel_cache: Arc<RwLock<std::collections::HashMap<String, Channel>>>,
    /// Statistics
    stats: Arc<RwLock<HandlerStats>>,
}

#[allow(dead_code)]
#[derive(Debug, Default)]
struct HandlerStats {
    total_requests: u64,
    successful_requests: u64,
    failed_requests: u64,
    bytes_proxied: u64,
}

impl PassthroughGrpcHandler {
    pub fn new(
        node_id: String,
        control_client: Arc<ProxyControlPlaneClient>,
    ) -> Self {
        Self {
            node_id,
            control_client,
            channel_cache: Arc::new(RwLock::new(std::collections::HashMap::new())),
            stats: Arc::new(RwLock::new(HandlerStats::default())),
        }
    }
    
    /// Get or create a gRPC channel to an upstream service
    async fn get_channel(&self, endpoint: &str) -> Result<Channel> {
        // Check cache first
        {
            let cache = self.channel_cache.read().await;
            if let Some(channel) = cache.get(endpoint) {
                return Ok(channel.clone());
            }
        }
        
        // Create new channel
        debug!("Creating new gRPC channel to {}", endpoint);
        
        let channel = Endpoint::from_shared(endpoint.to_string())?
            .connect_timeout(std::time::Duration::from_secs(5))
            .timeout(std::time::Duration::from_secs(30))
            .connect()
            .await?;
            
        // Cache the channel
        {
            let mut cache = self.channel_cache.write().await;
            cache.insert(endpoint.to_string(), channel.clone());
        }
        
        Ok(channel)
    }
    
    /// Select the best gRPC service endpoint
    async fn select_grpc_service(&self) -> Result<ServiceEndpoint, Status> {
        let services = self.control_client.get_healthy_grpc_services().await
            .map_err(|e| Status::unavailable(format!("Service discovery failed: {}", e)))?;
            
        if services.is_empty() {
            return Err(Status::unavailable("No healthy gRPC services available"));
        }
        
        ProxyControlPlaneClient::select_best_endpoint(&services)
            .cloned()
            .ok_or_else(|| Status::unavailable("No suitable gRPC service found"))
    }
    
    /// Proxy a unary gRPC call
    pub async fn proxy_unary<T, U>(
        &self,
        request: Request<T>,
        service_name: &str,
        method_name: &str,
    ) -> Result<Response<U>, Status>
    where
        T: prost::Message + Default + 'static,
        U: prost::Message + Default + 'static,
    {
        // Update stats
        {
            let mut stats = self.stats.write().await;
            stats.total_requests += 1;
        }
        
        // Select upstream service
        let target = self.select_grpc_service().await?;
        
        debug!("Proxying gRPC {}/{} to {}", service_name, method_name, target.node_id);
        
        // Get or create channel
        let channel = self.get_channel(&target.endpoint).await
            .map_err(|e| Status::unavailable(format!("Failed to connect to upstream: {}", e)))?;
        
        // Create a dynamic client
        let mut client = tonic::client::Grpc::new(channel);
        
        // Prepare the request
        let path = format!("/{}/{}", service_name, method_name);
        let codec = tonic::codec::ProstCodec::<T, U>::default();
        
        // Extract metadata and add proxy headers
        let mut request = request;
        request.metadata_mut().insert(
            "x-forwarded-for",
            self.node_id.parse().unwrap()
        );
        request.metadata_mut().insert(
            "x-proxy-node",
            self.node_id.parse().unwrap()
        );
        request.metadata_mut().insert(
            "x-upstream-node",
            target.node_id.parse().unwrap()
        );
        
        // Make the upstream call
        match client.unary(request, path.parse().unwrap(), codec).await {
            Ok(response) => {
                // Update success stats
                {
                    let mut stats = self.stats.write().await;
                    stats.successful_requests += 1;
                }
                
                Ok(response)
            }
            Err(status) => {
                // Update failure stats
                {
                    let mut stats = self.stats.write().await;
                    stats.failed_requests += 1;
                }
                
                warn!("Upstream gRPC call failed: {}", status);
                Err(status)
            }
        }
    }
    
    /// Proxy a server streaming gRPC call
    pub async fn proxy_server_streaming<T, U>(
        &self,
        request: Request<T>,
        service_name: &str,
        method_name: &str,
    ) -> Result<Response<tonic::codec::Streaming<U>>, Status>
    where
        T: prost::Message + Default + 'static,
        U: prost::Message + Default + 'static,
    {
        // Select upstream service
        let target = self.select_grpc_service().await?;
        
        debug!("Proxying streaming gRPC {}/{} to {}", service_name, method_name, target.node_id);
        
        // Get or create channel
        let channel = self.get_channel(&target.endpoint).await
            .map_err(|e| Status::unavailable(format!("Failed to connect to upstream: {}", e)))?;
        
        // Create a dynamic client
        let mut client = tonic::client::Grpc::new(channel);
        
        // Prepare the request
        let path = format!("/{}/{}", service_name, method_name);
        let codec = tonic::codec::ProstCodec::<T, U>::default();
        
        // Add proxy headers
        let mut request = request;
        request.metadata_mut().insert(
            "x-proxy-node",
            self.node_id.parse().unwrap()
        );
        request.metadata_mut().insert(
            "x-upstream-node",
            target.node_id.parse().unwrap()
        );
        
        // Make the upstream call
        client.server_streaming(request, path.parse().unwrap(), codec).await
    }
    
    /// Get handler statistics
    pub async fn get_stats(&self) -> serde_json::Value {
        let stats = self.stats.read().await;
        let services = self.control_client.get_healthy_grpc_services().await
            .unwrap_or_default();
        
        serde_json::json!({
            "node_id": self.node_id,
            "proxy_type": "grpc_passthrough",
            "stats": {
                "total_requests": stats.total_requests,
                "successful_requests": stats.successful_requests,
                "failed_requests": stats.failed_requests,
                "success_rate": if stats.total_requests > 0 {
                    (stats.successful_requests as f64 / stats.total_requests as f64) * 100.0
                } else {
                    0.0
                },
            },
            "upstream_services": services.len(),
            "channel_cache_size": self.channel_cache.read().await.len(),
        })
    }
    
    /// Clean up stale channels periodically
    pub async fn start_channel_cleanup(self: Arc<Self>) {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(300)); // 5 minutes
            
            loop {
                interval.tick().await;
                
                // Get current healthy services
                if let Ok(services) = self.control_client.get_healthy_grpc_services().await {
                    let healthy_endpoints: std::collections::HashSet<_> = 
                        services.iter().map(|s| &s.endpoint).collect();
                    
                    // Remove channels for unhealthy services
                    let mut cache = self.channel_cache.write().await;
                    cache.retain(|endpoint, _| healthy_endpoints.contains(endpoint));
                    
                    debug!("Channel cleanup: {} channels remaining", cache.len());
                }
            }
        });
    }
}

/// Generic gRPC proxy service implementation
/// This can be used as a base for implementing specific gRPC service proxies
#[derive(Clone)]
pub struct GenericGrpcProxy {
    handler: Arc<PassthroughGrpcHandler>,
    service_name: String,
}

impl GenericGrpcProxy {
    pub fn new(handler: Arc<PassthroughGrpcHandler>, service_name: String) -> Self {
        Self {
            handler,
            service_name,
        }
    }
    
    /// Helper to create a proxy method implementation
    pub fn proxy_method<T, U>(
        &self,
        method_name: &'static str,
    ) -> impl Fn(Request<T>) -> BoxFuture<'static, Result<Response<U>, Status>> + Clone
    where
        T: prost::Message + Default + 'static,
        U: prost::Message + Default + 'static,
    {
        let handler = self.handler.clone();
        let service_name = self.service_name.clone();
        
        move |request: Request<T>| {
            let handler = handler.clone();
            let service_name = service_name.clone();
            
            Box::pin(async move {
                handler.proxy_unary(request, &service_name, method_name).await
            })
        }
    }
}

/// Example implementation for a specific gRPC service
/// This shows how to use the generic proxy for actual services
pub mod example_proxy {
    use super::*;
    use tonic::{Request, Response, Status};
    
    // Example: Proxy for an ingestion service
    pub struct IngestServiceProxy {
        proxy: GenericGrpcProxy,
    }
    
    impl IngestServiceProxy {
        pub fn new(handler: Arc<PassthroughGrpcHandler>) -> Self {
            Self {
                proxy: GenericGrpcProxy::new(handler, "openwit.ingest.v1.IngestService".to_string()),
            }
        }
        
        // Example method proxy
        pub async fn ingest_batch<T, U>(&self, request: Request<T>) -> Result<Response<U>, Status>
        where
            T: prost::Message + Default + 'static,
            U: prost::Message + Default + 'static,
        {
            self.proxy.handler.proxy_unary(
                request,
                "openwit.ingest.v1.IngestService",
                "IngestBatch"
            ).await
        }
    }
}