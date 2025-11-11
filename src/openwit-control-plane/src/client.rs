use anyhow::Result;
use tonic::transport::Channel;
use tracing::{info, error};

use openwit_config::UnifiedConfig;
use crate::proto::{
    control_plane_service_client::ControlPlaneServiceClient,
    GetHealthyNodesRequest, RegisterNodeRequest,
};

/// Client for interacting with the control plane
#[derive(Clone)]
pub struct ControlPlaneClient {
    node_id: String,
    client: ControlPlaneServiceClient<Channel>,
}

impl ControlPlaneClient {
    /// Create a new control plane client
    pub async fn new(node_id: &str, config: &UnifiedConfig) -> Result<Self> {
        // Get control plane endpoint from config or environment
        let endpoint = if let Ok(env_endpoint) = std::env::var("CONTROL_PLANE_ENDPOINT") {
            info!("Using control plane endpoint from environment: {}", env_endpoint);
            env_endpoint
        } else if let Some(service_endpoint) = &config.networking.service_endpoints.control_plane {
            // Use service endpoint if configured (for Kubernetes)
            let endpoint_with_port = if let Some(url) = service_endpoint.strip_prefix("http://").or_else(|| service_endpoint.strip_prefix("https://")) {
                // It's a URL with protocol - check if it already has a port
                if url.contains(':') {
                    // Already has a port
                    service_endpoint.clone()
                } else {
                    // Add port to the URL
                    let control_port = config.service_ports.control_plane.service;
                    format!("{}:{}", service_endpoint, control_port)
                }
            } else {
                // No protocol prefix - check if it has a port
                if service_endpoint.contains(':') {
                    // Already has a port
                    service_endpoint.clone()
                } else {
                    // Add port
                    let control_port = config.service_ports.control_plane.service;
                    format!("{}:{}", service_endpoint, control_port)
                }
            };
            info!("Using control plane endpoint from service_endpoints: {}", endpoint_with_port);
            endpoint_with_port
        } else {
            // Fall back to control plane grpc_endpoint
            config.control_plane.grpc_endpoint.clone()
        };
        
        let channel = Channel::from_shared(endpoint.clone())?
            .connect()
            .await?;
            
        let client = ControlPlaneServiceClient::new(channel);
        
        info!("Connected to control plane at {}", endpoint);
        
        Ok(Self {
            node_id: node_id.to_string(),
            client,
        })
    }
    
    
    /// Register a node with the control plane
    pub async fn register_node(&mut self, node_id: &str, role: &str, metadata: std::collections::HashMap<String, String>) -> Result<()> {
        let request = tonic::Request::new(RegisterNodeRequest {
            node_id: node_id.to_string(),
            role: role.to_string(),
            metadata,
        });
        
        match self.client.register_node(request).await {
            Ok(response) => {
                let resp = response.into_inner();
                if resp.success {
                    info!("Node {} registered successfully", node_id);
                    Ok(())
                } else {
                    Err(anyhow::anyhow!("Registration failed: {}", resp.message))
                }
            }
            Err(e) => {
                error!("Failed to register node: {}", e);
                Err(anyhow::anyhow!("gRPC error: {}", e))
            }
        }
    }
    
    /// Get healthy nodes for a role
    pub async fn get_healthy_nodes(&mut self, role: &str) -> Result<Vec<NodeInfo>> {
        let request = tonic::Request::new(GetHealthyNodesRequest {
            requester_id: self.node_id.clone(),
            role: role.to_string(),
            max_nodes: 0, // No limit
        });
        
        match self.client.get_healthy_nodes(request).await {
            Ok(response) => {
                let nodes = response.into_inner().nodes
                    .into_iter()
                    .map(|n| NodeInfo {
                        node_id: n.node_id,
                        grpc_endpoint: n.grpc_endpoint,
                        arrow_flight_endpoint: n.arrow_flight_endpoint,
                        health_score: n.health_score,
                    })
                    .collect();
                Ok(nodes)
            }
            Err(e) => {
                error!("Failed to get healthy nodes: {}", e);
                Err(anyhow::anyhow!("gRPC error: {}", e))
            }
        }
    }
    
}

#[derive(Debug, Clone)]
pub struct NodeInfo {
    pub node_id: String,
    pub grpc_endpoint: String,
    pub arrow_flight_endpoint: String,
    pub health_score: f64,
}