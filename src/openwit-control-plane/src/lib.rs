pub mod node_manager;
pub mod grpc_server;
pub mod surveillance;
pub mod types;
pub mod mandatory_node_checker;
pub mod client;

pub use surveillance::SurveillanceNode;
pub use client::ControlPlaneClient;
pub use types::*;
pub use grpc_server::ControlPlaneGrpcService;

use std::sync::Arc;
use std::net::SocketAddr;
use anyhow::Result;
use tonic::transport::Server;
use tracing::info;

// Re-export proto types
pub mod proto {
    tonic::include_proto!("openwit.control");
}

/// Start the control plane surveillance node
pub async fn start_control_plane(
    node_id: String,
    cluster_handle: openwit_network::ClusterHandle,
    grpc_addr: SocketAddr,
    config_path: Option<&str>,
) -> Result<()> {
    info!("Control Plane Initialization");
    info!("════════════════════════════════════════");
    info!("Node ID: {}", node_id);
    info!("gRPC Address: {}", grpc_addr);
    info!("════════════════════════════════════════");
    info!("");
    
    // Set control plane metadata in gossip
    cluster_handle.set_self_kv("node_type", "control").await;
    cluster_handle.set_self_kv("node_id", &node_id).await;
    cluster_handle.set_self_kv("grpc_addr", &grpc_addr.to_string()).await;
    
    // Create surveillance node
    info!("Creating surveillance node...");
    let mut surveillance = SurveillanceNode::new(node_id.clone(), cluster_handle.clone()).await?;
    
    // Load configuration if provided
    if let Some(path) = config_path {
        info!("Loading configuration from: {}", path);
        let config = openwit_config::UnifiedConfig::from_file(path)?;
        surveillance = surveillance.with_config(config);
        info!("   Configuration loaded with mandatory node requirements");
    }
    
    let surveillance = Arc::new(surveillance);
    info!("   Surveillance node created");
    
    // Start surveillance systems
    info!("Starting surveillance subsystems...");
    surveillance.start().await?;
    info!("   All subsystems started:");
    info!("      - Health Monitor: ACTIVE");
    
    // Create gRPC service
    info!("");
    info!("Starting control plane gRPC server...");
    let grpc_service = ControlPlaneGrpcService::new(surveillance.clone());
    
    // Start gRPC server
    info!("   Binding to: {}", grpc_addr);
    info!("   Services exposed:");
    info!("      - Service Discovery API (GetHealthyNodes)");
    info!("");
    info!("Control plane ready and serving requests");
    
    Server::builder()
        .add_service(grpc_service.into_service())
        .serve(grpc_addr)
        .await?;
    
    Ok(())
}