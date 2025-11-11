use std::sync::Arc;
use anyhow::Result;
use tokio::sync::{mpsc, RwLock};
use tracing::{info, warn};
use uuid::Uuid;

use crate::proto::*;
use crate::data_transfer_actor::DataTransferActor;
use crate::node_client::InterNodeClient;

pub struct TransferManager {
    node_id: String,
    transfer_actor: Arc<DataTransferActor>,
    client: Arc<InterNodeClient>,
    node_registry: Arc<RwLock<std::collections::HashMap<String, String>>>, // node_id -> address
}

impl TransferManager {
    pub fn new(
        node_id: String,
        transfer_actor: Arc<DataTransferActor>,
        client: Arc<InterNodeClient>,
    ) -> Self {
        Self {
            node_id,
            transfer_actor,
            client,
            node_registry: Arc::new(RwLock::new(std::collections::HashMap::new())),
        }
    }
    
    pub async fn register_node(&self, node_id: String, address: String) -> Result<()> {
        self.node_registry.write().await.insert(node_id.clone(), address.clone());
        self.client.connect_to_node(&node_id, &address).await?;
        Ok(())
    }
    
    pub async fn initiate_data_transfer(
        &self,
        destination_node: &str,
        transfer_type: TransferType,
        metadata: std::collections::HashMap<String, String>,
        estimated_size: u64,
    ) -> Result<String> {
        let transfer_id = Uuid::new_v4().to_string();
        
        info!("Initiating transfer {} to node {} (type: {:?})", 
            transfer_id, destination_node, transfer_type);
        
        // Check if we're connected to destination
        let registry = self.node_registry.read().await;
        if !registry.contains_key(destination_node) {
            return Err(anyhow::anyhow!("Destination node {} not registered", destination_node));
        }
        
        // Create transfer request
        let request = TransferRequest {
            transfer_id: transfer_id.clone(),
            source_node: self.node_id.clone(),
            destination_node: destination_node.to_string(),
            transfer_type: transfer_type as i32,
            estimated_size,
            metadata,
        };
        
        // Send to destination node
        let response = self.client.initiate_transfer(destination_node, request.clone()).await?;
        
        if !response.accepted {
            return Err(anyhow::anyhow!("Transfer rejected: {}", response.error));
        }
        
        // Start transfer on our side
        let (tx, mut rx) = mpsc::channel(1);
        self.transfer_actor.get_command_sender().send(
            crate::data_transfer_actor::TransferCommand::StartTransfer {
                request,
                response_tx: tx,
            }
        ).await?;
        
        if let Some(response) = rx.recv().await {
            if response.accepted {
                Ok(transfer_id)
            } else {
                Err(anyhow::anyhow!("Local transfer start failed: {}", response.error))
            }
        } else {
            Err(anyhow::anyhow!("No response from transfer actor"))
        }
    }
    
    
    pub async fn get_active_transfers(&self) -> Vec<TransferStatus> {
        // Would query transfer actor for active transfers
        vec![]
    }
    
    pub async fn cancel_transfer(&self, transfer_id: &str) -> Result<()> {
        self.transfer_actor.get_command_sender().send(
            crate::data_transfer_actor::TransferCommand::CancelTransfer {
                transfer_id: transfer_id.to_string(),
            }
        ).await?;
        
        Ok(())
    }
}