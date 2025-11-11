use std::sync::Arc;
use std::time::Duration;
use anyhow::Result;
use tokio::sync::RwLock;
use tokio_stream::StreamExt;
use tonic::transport::Channel;
use tracing::{info, error};

use crate::proto::{
    inter_node_service_client::InterNodeServiceClient,
    *,
};

pub struct InterNodeClient {
    node_id: String,
    clients: Arc<RwLock<std::collections::HashMap<String, InterNodeServiceClient<Channel>>>>,
}

impl InterNodeClient {
    pub fn new(node_id: String) -> Self {
        Self {
            node_id,
            clients: Arc::new(RwLock::new(std::collections::HashMap::new())),
        }
    }
    
    pub async fn connect_to_node(&self, target_node: &str, address: &str) -> Result<()> {
        info!("Connecting to node {} at {}", target_node, address);
        
        let channel = Channel::from_shared(format!("http://{}", address))?
            .timeout(Duration::from_secs(30))
            .connect()
            .await?;
        
        let client = InterNodeServiceClient::new(channel);
        
        self.clients.write().await.insert(target_node.to_string(), client);
        
        info!("Connected to node: {}", target_node);
        Ok(())
    }
    
    pub async fn initiate_transfer(
        &self,
        target_node: &str,
        request: TransferRequest,
    ) -> Result<TransferResponse> {
        let clients = self.clients.read().await;
        let client = clients.get(target_node)
            .ok_or_else(|| anyhow::anyhow!("Not connected to node: {}", target_node))?;
        
        let mut client = client.clone();
        let response = client.initiate_transfer(request).await?;
        
        Ok(response.into_inner())
    }
    
    pub async fn stream_data_to_node(
        &self,
        target_node: &str,
        chunks: impl futures::Stream<Item = DataChunk> + Send + 'static,
    ) -> Result<Vec<TransferStatus>> {
        let clients = self.clients.read().await;
        let client = clients.get(target_node)
            .ok_or_else(|| anyhow::anyhow!("Not connected to node: {}", target_node))?;
        
        let mut client = client.clone();
        let response = client.stream_data(chunks).await?;
        let mut stream = response.into_inner();
        
        let mut statuses = Vec::new();
        while let Some(status) = stream.next().await {
            match status {
                Ok(s) => statuses.push(s),
                Err(e) => {
                    error!("Stream error: {:?}", e);
                    return Err(e.into());
                }
            }
        }
        
        Ok(statuses)
    }
    
    pub async fn send_command(
        &self,
        target_node: &str,
        command: NodeCommand,
    ) -> Result<CommandResponse> {
        let clients = self.clients.read().await;
        let client = clients.get(target_node)
            .ok_or_else(|| anyhow::anyhow!("Not connected to node: {}", target_node))?;
        
        let mut client = client.clone();
        let response = client.execute_command(command).await?;
        
        Ok(response.into_inner())
    }
    
    pub async fn check_health(&self, target_node: &str) -> Result<NodeHealth> {
        let clients = self.clients.read().await;
        let client = clients.get(target_node)
            .ok_or_else(|| anyhow::anyhow!("Not connected to node: {}", target_node))?;
        
        let mut client = client.clone();
        let request = HealthCheckRequest {
            node_id: target_node.to_string(),
            detailed: true,
        };
        
        let response = client.health_check(request).await?;
        Ok(response.into_inner())
    }
    
    pub async fn disconnect_from_node(&self, target_node: &str) {
        self.clients.write().await.remove(target_node);
        info!("Disconnected from node: {}", target_node);
    }
    
    pub async fn broadcast_command(&self, command: NodeCommand) -> Vec<(String, Result<CommandResponse>)> {
        let clients = self.clients.read().await;
        let mut results = Vec::new();
        
        for (node_id, client) in clients.iter() {
            let mut client = client.clone();
            let cmd = command.clone();
            let node = node_id.clone();
            
            let result = tokio::spawn(async move {
                (node, client.execute_command(cmd).await.map(|r| r.into_inner()).map_err(|e| anyhow::anyhow!("gRPC error: {}", e)))
            });
            
            results.push(result);
        }
        
        let mut responses = Vec::new();
        for result in results {
            match result.await {
                Ok(res) => responses.push(res),
                Err(e) => {
                    error!("Failed to get command result: {:?}", e);
                }
            }
        }
        
        responses
    }
}