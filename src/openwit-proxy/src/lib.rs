// Passthrough proxy modules
pub mod passthrough_http_handler;
pub mod passthrough_grpc_handler;
pub mod passthrough_kafka_handler;
pub mod passthrough_proxy_server;

// Routing and health monitoring
pub mod health_aware_router;
pub mod distributed_router;
pub mod control_plane_client;

// Core utilities
pub mod kafka_consumer;
pub mod proxy_buffer;
pub mod round_robin;
pub mod control_integration;
pub mod arrow_utils;

// Re-exports
pub use passthrough_proxy_server::PassthroughProxyServer;
pub use proxy_buffer::ProxyBuffer;
pub use round_robin::RoundRobinRouter;

use anyhow::Result;
use tracing::info;

/// Main entry point for the proxy node
pub async fn start_proxy_node(
    node_id: String,
    config: openwit_config::UnifiedConfig,
) -> Result<()> {
    info!("Starting OpenWit Proxy Node: {}", node_id);
    info!("  Role: Pass-through traffic proxy");
    info!("  Architecture: True distributed without buffering");
    
    // Use pass-through proxy for true distributed architecture
    let proxy_server = PassthroughProxyServer::new(node_id, config).await?;
    proxy_server.start().await?;
    
    Ok(())
}