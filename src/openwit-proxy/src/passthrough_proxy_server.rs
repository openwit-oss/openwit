use std::sync::Arc;
use std::net::SocketAddr;
use anyhow::Result;
use tokio::time::interval;
use tracing::{info, warn, error};

use openwit_config::UnifiedConfig;

use crate::{
    control_plane_client::{ProxyControlPlaneClient, ProxyHealthStats},
    passthrough_http_handler::PassthroughHttpHandler,
    passthrough_grpc_handler::PassthroughGrpcHandler,
    passthrough_kafka_handler::{PassthroughKafkaHandler, KafkaProxyConfig},
    health_aware_router::{HealthAwareRouter, RoutingStrategy},
    control_integration::ProxyControlIntegration,
};

/// Main pass-through proxy server that coordinates all components
#[allow(dead_code)]
pub struct PassthroughProxyServer {
    node_id: String,
    config: UnifiedConfig,
    control_client: Arc<ProxyControlPlaneClient>,
    health_router: Arc<HealthAwareRouter>,
    control_integration: Arc<ProxyControlIntegration>,
    http_handler: Option<Arc<PassthroughHttpHandler>>,
    grpc_handler: Option<Arc<PassthroughGrpcHandler>>,
    kafka_handler: Option<Arc<PassthroughKafkaHandler>>,
}

#[allow(dead_code)]
impl PassthroughProxyServer {
    pub async fn new(node_id: String, config: UnifiedConfig) -> Result<Self> {
        info!("Initializing OpenWit Pass-through Proxy Server: {}", node_id);
        
        // Initialize control plane integration for gossip
        let control_integration = Arc::new(
            ProxyControlIntegration::new(node_id.clone(), config.clone()).await?
        );
        
        // Register as mandatory node
        control_integration.register_as_mandatory().await?;
        
        // Create control plane client for service discovery
        let control_client = Arc::new(
            ProxyControlPlaneClient::new(node_id.clone(), config.clone()).await?
        );
        
        // Start health monitoring
        control_client.clone().start_health_monitor().await;
        
        // Use default routing strategy for now (can be made configurable later)
        let routing_strategy = RoutingStrategy::RoundRobin;
        
        // Create health-aware router
        let health_router = Arc::new(HealthAwareRouter::new(
            node_id.clone(),
            control_client.clone(),
            routing_strategy.clone(),
        ));
        
        // Start router maintenance
        health_router.clone().start_maintenance().await;
        
        // Create handlers based on enabled sources
        let http_handler = if config.ingestion.sources.http.enabled {
            let mut handler = PassthroughHttpHandler::new(
                node_id.clone(),
                control_client.clone(),
            );
            handler.set_health_router(health_router.clone());
            Some(Arc::new(handler))
        } else {
            None
        };
        
        let grpc_handler = if config.ingestion.sources.grpc.enabled {
            Some(Arc::new(PassthroughGrpcHandler::new(
                node_id.clone(),
                control_client.clone(),
            )))
        } else {
            None
        };
        
        // Create Kafka handler if enabled
        let kafka_handler = if config.ingestion.sources.kafka.enabled {
            let kafka_config = KafkaProxyConfig::builder()
                .group_id(format!("openwit-proxy-{}", node_id))
                .topics(config.ingestion.kafka.topics.clone())
                .output_topic_prefix("proxied-".to_string())
                .preserve_topic_names(false)
                .max_message_bytes(10 * 1024 * 1024) // 10MB
                .build()?;
                
            Some(Arc::new(PassthroughKafkaHandler::new(
                node_id.clone(),
                control_client.clone(),
                kafka_config,
            ).await?))
        } else {
            None
        };
        
        info!("Pass-through proxy initialized with:");
        info!("  - HTTP: {}", if http_handler.is_some() { "enabled" } else { "disabled" });
        info!("  - gRPC: {}", if grpc_handler.is_some() { "enabled" } else { "disabled" });
        info!("  - Kafka: {}", if kafka_handler.is_some() { "enabled" } else { "disabled" });
        info!("  - Routing: {:?}", routing_strategy);
        
        Ok(Self {
            node_id,
            config,
            control_client,
            health_router,
            control_integration,
            http_handler,
            grpc_handler,
            kafka_handler,
        })
    }
    
    /// Start all proxy services
    pub async fn start(self) -> Result<()> {
        info!("Starting pass-through proxy services...");
        
        let self_arc = Arc::new(self);
        
        // Start HTTP server if enabled
        if let Some(http_handler) = &self_arc.http_handler {
            let http_port = self_arc.config.ingestion.http.port;
            self_arc.clone().start_http_server(http_handler.clone(), http_port);
        }
        
        // Start gRPC server if enabled
        if let Some(grpc_handler) = &self_arc.grpc_handler {
            let grpc_port = self_arc.config.ingestion.grpc.port;
            self_arc.clone().start_grpc_server(grpc_handler.clone(), grpc_port);
        }
        
        // Start Kafka consumer if enabled
        if let Some(kafka_handler) = &self_arc.kafka_handler {
            kafka_handler.clone().start().await?;
            info!("Kafka pass-through consumer started");
        }
        
        // Start health reporting
        self_arc.clone().start_health_reporter();
        
        // Start statistics reporter
        self_arc.clone().start_stats_reporter();
        
        info!("All pass-through proxy services started successfully");
        
        // Keep running
        tokio::signal::ctrl_c().await?;
        info!("Shutting down pass-through proxy server...");
        
        Ok(())
    }
    
    /// Start HTTP server
    fn start_http_server(self: Arc<Self>, handler: Arc<PassthroughHttpHandler>, port: u16) {
        let addr: SocketAddr = format!("0.0.0.0:{}", port).parse().unwrap();
        
        let app = handler.create_router()
            .layer(tower_http::trace::TraceLayer::new_for_http())
            .layer(tower_http::cors::CorsLayer::permissive());
        
        tokio::spawn(async move {
            info!("Starting pass-through HTTP server on {}", addr);
            
            let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
            if let Err(e) = axum::serve(listener, app).await {
                error!("HTTP server error: {}", e);
            }
        });
    }
    
    /// Start gRPC server
    fn start_grpc_server(self: Arc<Self>, handler: Arc<PassthroughGrpcHandler>, port: u16) {
        let addr: SocketAddr = format!("0.0.0.0:{}", port).parse().unwrap();
        
        // Start channel cleanup
        let handler_clone = handler.clone();
        tokio::spawn(async move {
            handler_clone.start_channel_cleanup().await;
        });
        
        tokio::spawn(async move {
            info!("Starting pass-through gRPC server on {}", addr);
            
            // For a pass-through proxy, we don't need reflection service
            // The proxy just forwards raw gRPC traffic to backend services
            
            // TODO: Implement actual gRPC pass-through proxy logic
            // This would involve:
            // 1. Accept incoming gRPC connections
            // 2. Parse enough of the request to determine routing
            // 3. Forward the raw request to the selected backend
            // 4. Stream the response back to the client
            
            info!("gRPC pass-through proxy ready at {}", addr);
            
            // Keep the server running for now
            tokio::signal::ctrl_c().await.unwrap();
            error!("gRPC proxy server shutting down");
        });
    }
    
    /// Start health reporting to control plane
    fn start_health_reporter(self: Arc<Self>) {
        tokio::spawn(async move {
            let mut ticker = interval(std::time::Duration::from_secs(10));
            
            loop {
                ticker.tick().await;
                
                let stats = self.collect_health_stats().await;
                
                // Report to control plane client
                if let Err(e) = self.control_client.report_proxy_health(stats.clone()).await {
                    warn!("Failed to report health to control plane: {}", e);
                }
                
                // Get Kafka messages consumed if handler exists
                let kafka_messages = if let Some(kafka_handler) = &self.kafka_handler {
                    kafka_handler.get_messages_consumed().await
                } else {
                    0
                };
                
                // Also report via gossip integration
                let gossip_stats = crate::control_integration::ProxyHealthStats {
                    buffer_size: 0, // No buffer in pass-through mode
                    available_ingestors: stats.active_connections as usize,
                    requests_routed: (stats.requests_per_second * 10.0) as u64, // Approximate
                    kafka_messages_consumed: kafka_messages,
                    error_count: (stats.error_rate * stats.requests_per_second * 10.0) as u64,
                };
                
                if let Err(e) = self.control_integration.report_health(gossip_stats).await {
                    warn!("Failed to report health via gossip: {}", e);
                }
            }
        });
    }
    
    /// Start statistics reporting
    fn start_stats_reporter(self: Arc<Self>) {
        tokio::spawn(async move {
            let mut ticker = interval(std::time::Duration::from_secs(30));
            
            loop {
                ticker.tick().await;
                
                info!("=== Pass-through Proxy Statistics ===");
                
                // Router stats
                let router_stats = self.health_router.get_stats().await;
                info!("Router: {}", serde_json::to_string_pretty(&router_stats).unwrap());
                
                // HTTP stats
                if let Some(_http_handler) = &self.http_handler {
                    // For now, just log that HTTP handler is active
                    info!("HTTP handler is active");
                }
                
                // gRPC stats
                if let Some(_grpc_handler) = &self.grpc_handler {
                    // For now, just log that gRPC handler is active
                    info!("gRPC handler is active");
                }
                
                // Kafka stats
                if let Some(_kafka_handler) = &self.kafka_handler {
                    // For now, just log that Kafka handler is active
                    info!("Kafka handler is active");
                }
                
                info!("=====================================");
            }
        });
    }
    
    /// Collect health statistics
    async fn collect_health_stats(&self) -> ProxyHealthStats {
        let mut _total_requests = 0u64;
        let mut _successful_requests = 0u64;
        let mut failed_requests = 0u64;
        let mut bytes_proxied = 0u64;
        
        // Collect HTTP handler stats
        if let Some(http_handler) = &self.http_handler {
            let (total, success, failed, bytes) = http_handler.get_handler_stats().await;
            _total_requests += total;
            _successful_requests += success;
            failed_requests += failed;
            bytes_proxied += bytes;
        }
        
        // TODO: Collect gRPC handler stats when available
        // TODO: Collect Kafka handler stats (already have message count separately)
        
        // Calculate metrics - assuming we're collecting every 10 seconds
        let requests_per_second = (_total_requests as f64) / 10.0;
        let bytes_per_second = (bytes_proxied as f64) / 10.0;
        let error_rate = if _total_requests > 0 {
            (failed_requests as f64) / (_total_requests as f64)
        } else {
            0.0
        };
        
        // Get active connections from control plane client
        let active_connections = self.control_client.get_healthy_http_services().await
            .map(|services| services.len() as u32)
            .unwrap_or(0);
        
        ProxyHealthStats {
            active_connections,
            requests_per_second,
            bytes_per_second,
            error_rate,
            p99_latency_ms: 0.0, // TODO: Implement latency tracking
        }
    }
}

/// Proxy configuration extensions
pub mod proxy_config {
    #[derive(Debug, Clone)]
    pub struct ProxyConfig {
        pub routing_strategy: String,
    }
    
    impl Default for ProxyConfig {
        fn default() -> Self {
            Self {
                routing_strategy: "round_robin".to_string(),
            }
        }
    }
}