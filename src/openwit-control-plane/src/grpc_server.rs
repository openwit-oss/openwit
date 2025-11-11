use std::sync::Arc;
use tonic::{Request, Response, Status};
use tracing::{info, warn, error};

use crate::proto::{
    control_plane_service_server::{ControlPlaneService, ControlPlaneServiceServer},
    GetHealthyNodesRequest, GetHealthyNodesResponse, NodeInfo,
    RegisterNodeRequest, RegisterNodeResponse,
};
use crate::surveillance::SurveillanceNode;
use crate::types as internal_types;

pub struct ControlPlaneGrpcService {
    surveillance: Arc<SurveillanceNode>,
}

impl ControlPlaneGrpcService {
    pub fn new(surveillance: Arc<SurveillanceNode>) -> Self {
        Self { surveillance }
    }
    
    pub fn into_service(self) -> ControlPlaneServiceServer<Self> {
        ControlPlaneServiceServer::new(self)
    }
}

#[tonic::async_trait]
impl ControlPlaneService for ControlPlaneGrpcService {
    async fn register_node(
        &self,
        request: Request<RegisterNodeRequest>,
    ) -> Result<Response<RegisterNodeResponse>, Status> {
        let req = request.into_inner();
        info!("Node registration request: id={}, role={}", req.node_id, req.role);
        
        // Convert role string to internal role type
        let role = match req.role.as_str() {
            "control" => internal_types::NodeRole::Control,
            "ingest" => internal_types::NodeRole::Ingest,
            "search" => internal_types::NodeRole::Search,
            "storage" => internal_types::NodeRole::Storage,
            "query" => internal_types::NodeRole::Query,
            "kafka" => internal_types::NodeRole::Kafka,
            _ => {
                return Ok(Response::new(RegisterNodeResponse {
                    success: false,
                    message: format!("Unknown node role: {}", req.role),
                }));
            }
        };
        
        // Create node metadata
        let node_metadata = internal_types::NodeMetadata {
            id: internal_types::NodeId::new(req.node_id.clone()),
            role,
            state: internal_types::NodeState::Running,
            last_heartbeat: chrono::Utc::now(),
            metadata: req.metadata,
        };
        
        // Register with node manager
        match self.surveillance.node_manager.register_node(node_metadata).await {
            Ok(_) => {
                info!("Successfully registered node: {}", req.node_id);
                Ok(Response::new(RegisterNodeResponse {
                    success: true,
                    message: "Node registered successfully".to_string(),
                }))
            }
            Err(e) => {
                error!("Failed to register node {}: {}", req.node_id, e);
                Ok(Response::new(RegisterNodeResponse {
                    success: false,
                    message: format!("Registration failed: {}", e),
                }))
            }
        }
    }
    
    async fn get_healthy_nodes(
        &self,
        request: Request<GetHealthyNodesRequest>,
    ) -> Result<Response<GetHealthyNodesResponse>, Status> {
        let req = request.into_inner();
        info!("Service discovery request: role={}, requester={}", req.role, req.requester_id);
        
        // Convert role string to internal role type
        let role = match req.role.as_str() {
            "control" => internal_types::NodeRole::Control,
            "ingest" => internal_types::NodeRole::Ingest,
            "search" => internal_types::NodeRole::Search,
            "storage" => internal_types::NodeRole::Storage,
            "query" => internal_types::NodeRole::Query,
            "kafka" => internal_types::NodeRole::Kafka,
            _ => {
                warn!("Unknown node role requested: '{}'", req.role);
                return Ok(Response::new(GetHealthyNodesResponse { nodes: vec![] }));
            }
        };
        
        // Get healthy nodes from surveillance system
        let healthy_nodes = self.surveillance.node_manager.get_healthy_nodes_by_role(role).await;
        
        // Apply limit if specified
        let nodes_to_return = if req.max_nodes > 0 {
            healthy_nodes.into_iter().take(req.max_nodes as usize).collect()
        } else {
            healthy_nodes
        };
        
        // Convert to proto format with endpoint construction
        let nodes: Vec<NodeInfo> = nodes_to_return.iter()
            .map(|node| {
                // Build gRPC endpoint
                let grpc_endpoint = self.build_grpc_endpoint(node);
                
                // Build Arrow Flight endpoint
                let arrow_flight_endpoint = self.build_arrow_flight_endpoint(node);
                
                // Calculate health score (0-100) based on resource usage
                let health_score = self.calculate_health_score(node);
                
                NodeInfo {
                    node_id: node.id.0.clone(),
                    grpc_endpoint,
                    arrow_flight_endpoint,
                    health_score,
                    metadata: node.metadata.clone(),
                }
            })
            .collect();
        
        info!("Returning {} healthy {} nodes to {}", 
              nodes.len(), req.role, req.requester_id);
        
        Ok(Response::new(GetHealthyNodesResponse { nodes }))
    }
}

impl ControlPlaneGrpcService {
    /// Build gRPC endpoint for a node
    fn build_grpc_endpoint(&self, node: &internal_types::NodeMetadata) -> String {
        // Check if running in Kubernetes
        if std::env::var("KUBERNETES_SERVICE_HOST").is_ok() {
            // Use service name for stable endpoints in K8s
            if let Some(ref config) = self.surveillance.config {
                let service_type = match node.role {
                    internal_types::NodeRole::Ingest => "ingestion",
                    internal_types::NodeRole::Control => "control",
                    internal_types::NodeRole::Storage => "storage",
                    internal_types::NodeRole::Search => "search",
                    _ => "ingestion",
                };
                
                let service_name = config.networking.get_service_advertise_address(service_type);
                let grpc_port = node.metadata.get("grpc_port")
                    .and_then(|p| p.parse::<u16>().ok())
                    .unwrap_or_else(|| match node.role {
                        internal_types::NodeRole::Ingest => 50051,
                        _ => 4317,
                    });
                
                return format!("http://{}:{}", service_name, grpc_port);
            }
        }
        
        // Use reported endpoint if available
        if let Some(endpoint) = node.metadata.get("grpc_endpoint") {
            return endpoint.clone();
        }
        
        // Fallback to constructing from node ID
        let default_port = match node.role {
            internal_types::NodeRole::Ingest => 50051,
            internal_types::NodeRole::Storage => 9090,
            _ => 4317,
        };
        
        format!("http://localhost:{}", default_port)
    }
    
    /// Build Arrow Flight endpoint for a node
    fn build_arrow_flight_endpoint(&self, node: &internal_types::NodeMetadata) -> String {
        // Check for reported endpoint first
        if let Some(endpoint) = node.metadata.get("arrow_flight_endpoint") {
            return endpoint.clone();
        }
        
        // Get port from config or defaults
        let flight_port = if let Some(ref config) = self.surveillance.config {
            match node.role {
                internal_types::NodeRole::Storage => config.service_ports.storage.arrow_flight,
                internal_types::NodeRole::Ingest => config.service_ports.ingestion.arrow_flight,
                _ => 8093,
            }
        } else {
            match node.role {
                internal_types::NodeRole::Storage => 9401,
                internal_types::NodeRole::Ingest => 8089,
                _ => 8093,
            }
        };
        
        // Build endpoint
        if std::env::var("KUBERNETES_SERVICE_HOST").is_ok() {
            if let Some(ref config) = self.surveillance.config {
                let service_type = match node.role {
                    internal_types::NodeRole::Ingest => "ingestion",
                    internal_types::NodeRole::Storage => "storage",
                    _ => return format!("grpc://localhost:{}", flight_port),
                };
                
                let service_name = config.networking.get_service_advertise_address(service_type);
                return format!("grpc://{}:{}", service_name, flight_port);
            }
        }
        
        format!("grpc://localhost:{}", flight_port)
    }
    
    /// Calculate health score based on node state
    fn calculate_health_score(&self, node: &internal_types::NodeMetadata) -> f64 {
        match &node.state {
            internal_types::NodeState::Running => 100.0,
            internal_types::NodeState::Degraded => 50.0,
            internal_types::NodeState::Starting => 25.0,
            _ => 0.0,
        }
    }
}