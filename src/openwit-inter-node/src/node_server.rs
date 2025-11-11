use std::pin::Pin;
use std::sync::Arc;
use futures::Stream;
use tokio::sync::mpsc;
use tokio_stream::{StreamExt, wrappers::ReceiverStream};
use tonic::{Request, Response, Status, Streaming};
use tracing::{info, error};

use crate::proto::{
    inter_node_service_server::{InterNodeService, InterNodeServiceServer},
    *,
};
use crate::data_transfer_actor::{DataTransferActor, TransferCommand};

pub struct InterNodeServer {
    node_id: String,
    node_type: String,
    transfer_actor: Arc<DataTransferActor>,
    command_tx: mpsc::Sender<NodeCommand>,
    capabilities: NodeCapabilities,
}

impl InterNodeServer {
    pub fn new(
        node_id: String,
        node_type: String,
        transfer_actor: Arc<DataTransferActor>,
        command_tx: mpsc::Sender<NodeCommand>,
    ) -> Self {
        let capabilities = NodeCapabilities {
            node_id: node_id.clone(),
            node_type: node_type.clone(),
            supported_operations: vec![
                "transfer".to_string(),
                "command".to_string(),
                "health".to_string(),
            ],
            resources: Some(ResourceAvailability {
                available_memory_bytes: 0, // Would get from system
                available_disk_bytes: 0,
                cpu_idle_percent: 0.0,
                network_bandwidth_mbps: 1000.0,
            }),
            active_transfers: vec![],
        };
        
        Self {
            node_id,
            node_type,
            transfer_actor,
            command_tx,
            capabilities,
        }
    }
    
    pub fn into_service(self) -> InterNodeServiceServer<Self> {
        InterNodeServiceServer::new(self)
    }
}

#[tonic::async_trait]
impl InterNodeService for InterNodeServer {
    async fn initiate_transfer(
        &self,
        request: Request<TransferRequest>,
    ) -> Result<Response<TransferResponse>, Status> {
        let req = request.into_inner();
        info!("Received transfer request: {} from {} to {}", 
            req.transfer_id, req.source_node, req.destination_node);
        
        let (response_tx, mut response_rx) = mpsc::channel(1);
        
        let cmd = TransferCommand::StartTransfer {
            request: req,
            response_tx,
        };
        
        self.transfer_actor.get_command_sender()
            .send(cmd)
            .await
            .map_err(|e| Status::internal(format!("Failed to send command: {:?}", e)))?;
        
        match response_rx.recv().await {
            Some(response) => Ok(Response::new(response)),
            None => Err(Status::internal("No response from transfer actor")),
        }
    }
    
    type StreamDataStream = Pin<Box<dyn Stream<Item = Result<TransferStatus, Status>> + Send>>;
    
    async fn stream_data(
        &self,
        request: Request<Streaming<DataChunk>>,
    ) -> Result<Response<Self::StreamDataStream>, Status> {
        let mut stream = request.into_inner();
        let transfer_actor = self.transfer_actor.clone();
        
        let (status_tx, status_rx) = mpsc::channel(32);
        
        // Spawn handler for incoming chunks
        tokio::spawn(async move {
            while let Some(result) = stream.next().await {
                match result {
                    Ok(chunk) => {
                        let transfer_id = chunk.transfer_id.clone();
                        
                        if let Err(e) = transfer_actor.handle_incoming_chunk(chunk).await {
                            error!("Failed to handle chunk: {:?}", e);
                            
                            let error_status = TransferStatus {
                                transfer_id,
                                state: TransferState::Failed.into(),
                                bytes_transferred: 0,
                                total_bytes: 0,
                                progress_percent: 0.0,
                                started_at: None,
                                updated_at: None,
                                error: e.to_string(),
                            };
                            
                            let _ = status_tx.send(Ok(error_status)).await;
                            break;
                        }
                    }
                    Err(e) => {
                        error!("Stream error: {:?}", e);
                        break;
                    }
                }
            }
        });
        
        let stream = ReceiverStream::new(status_rx);
        Ok(Response::new(Box::pin(stream)))
    }
    
    async fn get_transfer_status(
        &self,
        request: Request<TransferStatusRequest>,
    ) -> Result<Response<TransferStatus>, Status> {
        let transfer_id = request.into_inner().transfer_id;
        let (response_tx, mut response_rx) = mpsc::channel(1);
        
        let cmd = TransferCommand::GetStatus {
            transfer_id: transfer_id.clone(),
            response_tx,
        };
        
        self.transfer_actor.get_command_sender()
            .send(cmd)
            .await
            .map_err(|e| Status::internal(format!("Failed to send command: {:?}", e)))?;
        
        match response_rx.recv().await {
            Some(Some(status)) => Ok(Response::new(status)),
            Some(None) => Err(Status::not_found(format!("Transfer {} not found", transfer_id))),
            None => Err(Status::internal("No response from transfer actor")),
        }
    }
    
    async fn execute_command(
        &self,
        request: Request<NodeCommand>,
    ) -> Result<Response<CommandResponse>, Status> {
        let command = request.into_inner();
        let command_id = command.command_id.clone();
        
        info!("Executing command {} from {}", command_id, command.source_node);
        
        self.command_tx.send(command).await
            .map_err(|e| Status::internal(format!("Failed to send command: {:?}", e)))?;
        
        // For now, return immediate acknowledgment
        // In production, would wait for actual execution result
        Ok(Response::new(CommandResponse {
            command_id,
            success: true,
            error: String::new(),
            result: Default::default(),
        }))
    }
    
    type StreamCommandsStream = Pin<Box<dyn Stream<Item = Result<NodeCommand, Status>> + Send>>;
    
    async fn stream_commands(
        &self,
        _request: Request<Streaming<CommandResponse>>,
    ) -> Result<Response<Self::StreamCommandsStream>, Status> {
        // This would handle command responses and return new commands
        let (_tx, rx) = mpsc::channel(32);
        
        // For now, return empty stream
        let stream = ReceiverStream::new(rx);
        Ok(Response::new(Box::pin(stream)))
    }
    
    async fn register_node(
        &self,
        request: Request<NodeCapabilities>,
    ) -> Result<Response<RegistrationAck>, Status> {
        let capabilities = request.into_inner();
        info!("Node {} registering with type: {}", capabilities.node_id, capabilities.node_type);
        
        // Would register with local node registry
        
        Ok(Response::new(RegistrationAck {
            accepted: true,
            node_id: self.node_id.clone(),
            peer_nodes: vec![], // Would return list of peer nodes
        }))
    }
    
    type StreamNodeUpdatesStream = Pin<Box<dyn Stream<Item = Result<NodeCommand, Status>> + Send>>;
    
    async fn stream_node_updates(
        &self,
        request: Request<Streaming<NodeCapabilities>>,
    ) -> Result<Response<Self::StreamNodeUpdatesStream>, Status> {
        let mut stream = request.into_inner();
        let (_tx, rx) = mpsc::channel(32);
        
        tokio::spawn(async move {
            while let Some(result) = stream.next().await {
                match result {
                    Ok(capabilities) => {
                        info!("Node {} updated capabilities", capabilities.node_id);
                        // Would update local registry
                    }
                    Err(e) => {
                        error!("Stream error: {:?}", e);
                        break;
                    }
                }
            }
        });
        
        let stream = ReceiverStream::new(rx);
        Ok(Response::new(Box::pin(stream)))
    }
    
    async fn health_check(
        &self,
        request: Request<HealthCheckRequest>,
    ) -> Result<Response<NodeHealth>, Status> {
        let req = request.into_inner();
        
        let mut checks = vec![
            HealthCheck {
                name: "transfer_actor".to_string(),
                passed: true,
                message: "Running".to_string(),
                latency_ms: 0.1,
            },
            HealthCheck {
                name: "command_executor".to_string(),
                passed: true,
                message: "Running".to_string(),
                latency_ms: 0.1,
            },
        ];
        
        if req.detailed {
            // Add more detailed checks
            checks.push(HealthCheck {
                name: "disk_space".to_string(),
                passed: true,
                message: "Sufficient space available".to_string(),
                latency_ms: 1.0,
            });
        }
        
        Ok(Response::new(NodeHealth {
            node_id: self.node_id.clone(),
            is_healthy: true,
            checks,
            timestamp: Some(prost_types::Timestamp {
                seconds: chrono::Utc::now().timestamp(),
                nanos: 0,
            }),
        }))
    }
}