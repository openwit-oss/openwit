use std::sync::Arc;
use std::time::Duration;
use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::{mpsc, RwLock};
use tokio::time::interval;
use tracing::{info, warn, error, debug};

use crate::proto::*;

#[derive(Debug, Clone)]
pub struct Transfer {
    pub id: String,
    pub source_node: String,
    pub destination_node: String,
    pub transfer_type: TransferType,
    pub state: TransferState,
    pub total_bytes: u64,
    pub transferred_bytes: u64,
    pub started_at: chrono::DateTime<chrono::Utc>,
    pub chunks: Vec<DataChunk>,
}

#[async_trait]
pub trait DataSource: Send + Sync {
    async fn prepare_transfer(&self, transfer_type: TransferType, metadata: &std::collections::HashMap<String, String>) -> Result<(u64, Box<dyn tokio::io::AsyncRead + Send + Unpin>)>;
}

#[async_trait]
pub trait DataSink: Send + Sync {
    async fn receive_data(&self, transfer_id: &str, data: Bytes) -> Result<()>;
    async fn finalize_transfer(&self, transfer_id: &str) -> Result<()>;
}

use tokio::io::AsyncReadExt;

pub struct DataTransferActor {
    node_id: String,
    transfers: Arc<RwLock<std::collections::HashMap<String, Transfer>>>,
    command_rx: Arc<RwLock<Option<mpsc::Receiver<TransferCommand>>>>,
    command_tx: mpsc::Sender<TransferCommand>,
    status_tx: mpsc::Sender<TransferStatus>,
    data_source: Option<Arc<dyn DataSource>>,
    data_sink: Option<Arc<dyn DataSink>>,
}

#[derive(Debug, Clone)]
pub enum TransferCommand {
    StartTransfer {
        request: TransferRequest,
        response_tx: mpsc::Sender<TransferResponse>,
    },
    CancelTransfer {
        transfer_id: String,
    },
    GetStatus {
        transfer_id: String,
        response_tx: mpsc::Sender<Option<TransferStatus>>,
    },
}

impl DataTransferActor {
    pub fn new(
        node_id: String,
        status_tx: mpsc::Sender<TransferStatus>,
    ) -> Self {
        let (tx, rx) = mpsc::channel(100);
        
        Self {
            node_id,
            transfers: Arc::new(RwLock::new(std::collections::HashMap::new())),
            command_rx: Arc::new(RwLock::new(Some(rx))),
            command_tx: tx,
            status_tx,
            data_source: None,
            data_sink: None,
        }
    }
    
    pub fn with_source(mut self, source: Arc<dyn DataSource>) -> Self {
        self.data_source = Some(source);
        self
    }
    
    pub fn with_sink(mut self, sink: Arc<dyn DataSink>) -> Self {
        self.data_sink = Some(sink);
        self
    }
    
    pub async fn start(self: Arc<Self>) -> Result<()> {
        // Start command processor
        self.clone().start_command_processor();
        
        // Start transfer monitor
        self.clone().start_transfer_monitor();
        
        info!("Data transfer actor started for node: {}", self.node_id);
        Ok(())
    }
    
    fn start_command_processor(self: Arc<Self>) {
        let command_rx = self.command_rx.clone();
        tokio::spawn(async move {
            let mut rx = command_rx.write().await.take().unwrap();
            let transfers = self.transfers.clone();
            let node_id = self.node_id.clone();
            let data_source = self.data_source.clone();
            let _data_sink = self.data_sink.clone();
            let status_tx = self.status_tx.clone();
            while let Some(cmd) = rx.recv().await {
                match cmd {
                    TransferCommand::StartTransfer { request, response_tx } => {
                        let transfer_id = request.transfer_id.clone();
                        
                        // Validate we're the right node
                        if request.source_node != node_id && request.destination_node != node_id {
                            let _ = response_tx.send(TransferResponse {
                                transfer_id,
                                accepted: false,
                                error: "Wrong node for transfer".to_string(),
                                status: None,
                            }).await;
                            continue;
                        }
                        
                        // Create transfer record
                        let transfer = Transfer {
                            id: transfer_id.clone(),
                            source_node: request.source_node.clone(),
                            destination_node: request.destination_node.clone(),
                            transfer_type: request.transfer_type(),
                            state: TransferState::Pending,
                            total_bytes: request.estimated_size,
                            transferred_bytes: 0,
                            started_at: chrono::Utc::now(),
                            chunks: Vec::new(),
                        };
                        
                        transfers.write().await.insert(transfer_id.clone(), transfer.clone());
                        
                        // Start transfer based on role
                        let transfer_status = DataTransferActor::create_status(&transfer);
                        
                        if request.source_node == node_id {
                            // We're sending data
                            if let Some(source) = &data_source {
                                let source = source.clone();
                                let transfers = transfers.clone();
                                let status_tx = status_tx.clone();
                                
                                tokio::spawn(async move {
                                    if let Err(e) = Self::handle_send_transfer(
                                        transfer,
                                        source,
                                        transfers,
                                        status_tx,
                                        request.metadata,
                                    ).await {
                                        error!("Send transfer failed: {:?}", e);
                                    }
                                });
                            }
                        } else {
                            // We're receiving data
                            // Receiver logic handled by StreamData RPC
                        }
                        let _ = response_tx.send(TransferResponse {
                            transfer_id,
                            accepted: true,
                            error: String::new(),
                            status: Some(transfer_status),
                        }).await;
                    }
                    
                    TransferCommand::CancelTransfer { transfer_id } => {
                        if let Some(transfer) = transfers.write().await.get_mut(&transfer_id) {
                            transfer.state = TransferState::Cancelled;
                            info!("Cancelled transfer: {}", transfer_id);
                        }
                    }
                    
                    TransferCommand::GetStatus { transfer_id, response_tx } => {
                        let status = transfers.read().await.get(&transfer_id)
                            .map(|t| DataTransferActor::create_status(t));
                        let _ = response_tx.send(status).await;
                    }
                }
            }
        });
    }
    
    fn start_transfer_monitor(self: Arc<Self>) {
        let transfers = self.transfers.clone();
        let status_tx = self.status_tx.clone();
        
        tokio::spawn(async move {
            let mut ticker = interval(Duration::from_secs(1));
            
            loop {
                ticker.tick().await;
                
                let active_transfers: Vec<_> = transfers.read().await
                    .values()
                    .filter(|t| t.state == TransferState::InProgress)
                    .cloned()
                    .collect();
                
                for transfer in active_transfers {
                    let status = TransferStatus {
                        transfer_id: transfer.id.clone(),
                        state: transfer.state as i32,
                        bytes_transferred: transfer.transferred_bytes,
                        total_bytes: transfer.total_bytes,
                        progress_percent: if transfer.total_bytes > 0 {
                            (transfer.transferred_bytes as f64 / transfer.total_bytes as f64) * 100.0
                        } else {
                            0.0
                        },
                        started_at: Some(prost_types::Timestamp {
                            seconds: transfer.started_at.timestamp(),
                            nanos: 0,
                        }),
                        updated_at: Some(prost_types::Timestamp {
                            seconds: chrono::Utc::now().timestamp(),
                            nanos: 0,
                        }),
                        error: String::new(),
                    };
                    
                    let _ = status_tx.send(status).await;
                }
            }
        });
    }
    
    async fn handle_send_transfer(
        mut transfer: Transfer,
        source: Arc<dyn DataSource>,
        transfers: Arc<RwLock<std::collections::HashMap<String, Transfer>>>,
        _status_tx: mpsc::Sender<TransferStatus>,
        metadata: std::collections::HashMap<String, String>,
    ) -> Result<()> {
        info!("Starting send transfer: {} to {}", transfer.id, transfer.destination_node);
        
        // Prepare data source
        let (total_size, mut reader) = source.prepare_transfer(transfer.transfer_type, &metadata).await?;
        transfer.total_bytes = total_size;
        transfer.state = TransferState::InProgress;
        
        transfers.write().await.insert(transfer.id.clone(), transfer.clone());
        
        // TODO: Connect to destination node and stream data
        // This would use the InterNodeClient to connect to destination
        
        // For now, simulate transfer
        let chunk_size = 64 * 1024; // 64KB chunks
        let mut buffer = vec![0u8; chunk_size];
        let mut sequence = 0;
        
        loop {
            match reader.read(&mut buffer).await {
                Ok(0) => break, // EOF
                Ok(n) => {
                    transfer.transferred_bytes += n as u64;
                    sequence += 1;
                    
                    // Create chunk
                    let _chunk = DataChunk {
                        transfer_id: transfer.id.clone(),
                        sequence_number: sequence,
                        data: buffer[..n].to_vec(),
                        is_final: false,
                        total_size: total_size,
                        checksum: String::new(), // TODO: Calculate checksum
                    };
                    
                    // TODO: Send chunk to destination
                    debug!("Sent chunk {} ({} bytes)", sequence, n);
                    
                    // Update transfer state
                    transfers.write().await.insert(transfer.id.clone(), transfer.clone());
                }
                Err(e) => {
                    error!("Read error during transfer: {:?}", e);
                    transfer.state = TransferState::Failed;
                    transfers.write().await.insert(transfer.id.clone(), transfer.clone());
                    return Err(e.into());
                }
            }
        }
        
        // Send final chunk
        let _final_chunk = DataChunk {
            transfer_id: transfer.id.clone(),
            sequence_number: sequence + 1,
            data: vec![],
            is_final: true,
            total_size: total_size,
            checksum: String::new(),
        };
        
        // TODO: Send final chunk
        
        transfer.state = TransferState::Completed;
        transfers.write().await.insert(transfer.id.clone(), transfer.clone());
        
        info!("Transfer {} completed: {} bytes", transfer.id, transfer.transferred_bytes);
        
        Ok(())
    }
    
    fn create_status(transfer: &Transfer) -> TransferStatus {
        TransferStatus {
            transfer_id: transfer.id.clone(),
            state: transfer.state as i32,
            bytes_transferred: transfer.transferred_bytes,
            total_bytes: transfer.total_bytes,
            progress_percent: if transfer.total_bytes > 0 {
                (transfer.transferred_bytes as f64 / transfer.total_bytes as f64) * 100.0
            } else {
                0.0
            },
            started_at: Some(prost_types::Timestamp {
                seconds: transfer.started_at.timestamp(),
                nanos: 0,
            }),
            updated_at: Some(prost_types::Timestamp {
                seconds: chrono::Utc::now().timestamp(),
                nanos: 0,
            }),
            error: String::new(),
        }
    }
    
    pub fn get_command_sender(&self) -> mpsc::Sender<TransferCommand> {
        self.command_tx.clone()
    }
    
    pub async fn handle_incoming_chunk(&self, chunk: DataChunk) -> Result<()> {
        let mut transfers = self.transfers.write().await;
        
        if let Some(transfer) = transfers.get_mut(&chunk.transfer_id) {
            if chunk.is_final {
                transfer.state = TransferState::Completed;
                info!("Transfer {} completed", chunk.transfer_id);
                
                // Finalize with sink
                if let Some(sink) = &self.data_sink {
                    sink.finalize_transfer(&chunk.transfer_id).await?;
                }
            } else {
                // Process chunk
                let data_len = chunk.data.len();
                if let Some(sink) = &self.data_sink {
                    sink.receive_data(&chunk.transfer_id, chunk.data.into()).await?;
                }
                
                transfer.transferred_bytes += data_len as u64;
                // Note: chunk.data was moved, cannot push chunk anymore
            }
        } else {
            warn!("Received chunk for unknown transfer: {}", chunk.transfer_id);
        }
        
        Ok(())
    }
}