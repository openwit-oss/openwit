use std::sync::Arc;
use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::mpsc;
use tracing::{info, warn, error};

use crate::proto::*;

#[async_trait]
pub trait NodeCommandHandler: Send + Sync {
    async fn handle_flush_buffer(&self, buffer_type: &str, force: bool) -> Result<()>;
    async fn handle_compact_data(&self, data_ids: Vec<String>) -> Result<()>;
    async fn handle_cleanup_disk(&self, target_free_bytes: u64, retention_days: u32) -> Result<()>;
    async fn handle_start_transfer(&self, target_node: &str, transfer_type: TransferType, params: std::collections::HashMap<String, String>) -> Result<()>;
}

pub struct CommandExecutor {
    node_id: String,
    node_type: String,
    handler: Option<Arc<dyn NodeCommandHandler>>,
    command_rx: Arc<tokio::sync::RwLock<Option<mpsc::Receiver<NodeCommand>>>>,
    response_tx: mpsc::Sender<CommandResponse>,
}

impl CommandExecutor {
    pub fn new(
        node_id: String,
        node_type: String,
        response_tx: mpsc::Sender<CommandResponse>,
    ) -> (Self, mpsc::Sender<NodeCommand>) {
        let (tx, rx) = mpsc::channel(100);
        
        let executor = Self {
            node_id,
            node_type,
            handler: None,
            command_rx: Arc::new(tokio::sync::RwLock::new(Some(rx))),
            response_tx,
        };
        
        (executor, tx)
    }
    
    pub fn with_handler(mut self, handler: Arc<dyn NodeCommandHandler>) -> Self {
        self.handler = Some(handler);
        self
    }
    
    pub async fn start(&self) -> Result<()> {
        let mut rx = self.command_rx.write().await.take().unwrap();
        let handler = self.handler.clone();
        let response_tx = self.response_tx.clone();
        let node_id = self.node_id.clone();
        let node_type = self.node_type.clone();
        
        tokio::spawn(async move {
            info!("Command executor started for {} node: {}", node_type, node_id);
            
            while let Some(command) = rx.recv().await {
                let command_id = command.command_id.clone();
                info!("Received command {} from {}", command_id, command.source_node);
                
                let result = match &handler {
                    Some(h) => Self::execute_command(h.clone(), command).await,
                    None => Err(anyhow::anyhow!("No command handler configured")),
                };
                
                let response = match result {
                    Ok(result_data) => CommandResponse {
                        command_id,
                        success: true,
                        error: String::new(),
                        result: result_data,
                    },
                    Err(e) => {
                        error!("Command execution failed: {:?}", e);
                        CommandResponse {
                            command_id,
                            success: false,
                            error: e.to_string(),
                            result: Default::default(),
                        }
                    }
                };
                
                if let Err(e) = response_tx.send(response).await {
                    error!("Failed to send command response: {:?}", e);
                }
            }
        });
        
        Ok(())
    }
    
    async fn execute_command(
        handler: Arc<dyn NodeCommandHandler>,
        command: NodeCommand,
    ) -> Result<std::collections::HashMap<String, String>> {
        let mut result = std::collections::HashMap::new();
        
        match command.command {
            Some(node_command::Command::StartTransfer(start)) => {
                info!("Starting transfer to node: {}", start.target_node);
                handler.handle_start_transfer(
                    &start.target_node,
                    TransferType::try_from(start.transfer_type).unwrap_or(TransferType::Unknown),
                    start.parameters,
                ).await?;
                result.insert("status".to_string(), "transfer_initiated".to_string());
            }
            
            Some(node_command::Command::FlushBuffer(flush)) => {
                info!("Flushing buffer: {}", flush.buffer_type);
                handler.handle_flush_buffer(&flush.buffer_type, flush.force).await?;
                result.insert("status".to_string(), "buffer_flushed".to_string());
            }
            
            Some(node_command::Command::CompactData(compact)) => {
                info!("Compacting {} data items", compact.data_ids.len());
                handler.handle_compact_data(compact.data_ids).await?;
                result.insert("status".to_string(), "compaction_started".to_string());
            }
            
            Some(node_command::Command::CleanupDisk(cleanup)) => {
                info!("Cleaning up disk to free {} bytes", cleanup.target_free_bytes);
                handler.handle_cleanup_disk(
                    cleanup.target_free_bytes,
                    cleanup.retention_days,
                ).await?;
                result.insert("status".to_string(), "cleanup_complete".to_string());
            }
            
            _ => {
                warn!("Unknown command type");
                return Err(anyhow::anyhow!("Unknown command type"));
            }
        }
        
        Ok(result)
    }
}