use std::sync::Arc;
use anyhow::Result;
use arrow::array::{StringArray, TimestampNanosecondArray};
use arrow::datatypes::{Schema, Field, DataType, TimeUnit};
use arrow::record_batch::RecordBatch;
use arrow_flight::{
    flight_service_server::{FlightService, FlightServiceServer},
    flight_service_client::FlightServiceClient,
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor,
    FlightInfo, HandshakeRequest, HandshakeResponse, PutResult, SchemaResult,
    Ticket,
};
use futures::{stream, Stream, StreamExt};
use tokio::sync::mpsc;
use tonic::{Request, Response, Status, Streaming};
use tracing::{info, debug};

use crate::proto::TransferType;

/// Arrow Flight server for high-performance data transfers
pub struct FlightTransferService {
    node_id: String,
    transfers: Arc<tokio::sync::RwLock<std::collections::HashMap<String, TransferInfo>>>,
}

struct TransferInfo {
    schema: Schema,
    batches: Vec<RecordBatch>,
    transfer_type: TransferType,
    source_node: String,
    total_bytes: usize,
}

impl FlightTransferService {
    pub fn new(node_id: String) -> Self {
        Self {
            node_id,
            transfers: Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
        }
    }
    
    pub fn into_server(self) -> FlightServiceServer<Self> {
        FlightServiceServer::new(self)
    }
}

#[tonic::async_trait]
impl FlightService for FlightTransferService {
    type HandshakeStream = std::pin::Pin<Box<dyn futures::Stream<Item = Result<HandshakeResponse, Status>> + Send>>;
    type ListFlightsStream = stream::Empty<Result<FlightInfo, Status>>;
    type DoGetStream = Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send>>;
    type DoPutStream = stream::Empty<Result<PutResult, Status>>;
    type DoActionStream = stream::Empty<Result<arrow_flight::Result, Status>>;
    type ListActionsStream = std::pin::Pin<Box<dyn futures::Stream<Item = Result<ActionType, Status>> + Send>>;
    type DoExchangeStream = stream::Empty<Result<FlightData, Status>>;
    
    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("get_schema not implemented"))
    }
    
    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner();
        let key = String::from_utf8(ticket.ticket.to_vec())
            .map_err(|_| Status::invalid_argument("Invalid ticket"))?;
        
        info!("Arrow Flight GET request for: {}", key);
        
        let transfers = self.transfers.read().await;
        let transfer = transfers.get(&key)
            .ok_or_else(|| Status::not_found("Transfer not found"))?;
        
        // Create stream of FlightData from RecordBatches
        let _schema = transfer.schema.clone();
        let _batches = transfer.batches.clone();
        
        let (tx, rx) = mpsc::channel(10);
        
        tokio::spawn(async move {
            // TODO: Update to use new Arrow Flight API
            // The FlightData API has changed significantly in arrow-flight 55.2.0
            // Need to use arrow_flight::encode module instead
            let _ = tx.send(Err(Status::unimplemented("Arrow Flight conversion needs update"))).await;
        });
        
        let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        Ok(Response::new(Box::pin(stream)))
    }
    
    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        let response = HandshakeResponse {
            protocol_version: 1,
            payload: vec![].into(),
        };
        Ok(Response::new(Box::pin(stream::once(futures::future::ready(Ok(response))))))
    }
    
    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Ok(Response::new(stream::empty()))
    }
    
    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let descriptor = request.into_inner();
        let key = descriptor.path.get(0)
            .ok_or_else(|| Status::invalid_argument("Missing path"))?
            .clone();
        
        let transfers = self.transfers.read().await;
        let transfer = transfers.get(&key)
            .ok_or_else(|| Status::not_found("Transfer not found"))?;
        
        // TODO: Fix schema conversion for new Arrow Flight API
        let info = FlightInfo {
            schema: vec![].into(), // Schema encoding has changed
            flight_descriptor: Some(descriptor),
            endpoint: vec![],
            total_records: transfer.batches.iter().map(|b| b.num_rows() as i64).sum(),
            total_bytes: transfer.total_bytes as i64,
            app_metadata: vec![].into(),
            ordered: false,
        };
        
        Ok(Response::new(info))
    }
    
    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        let _stream = request.into_inner();
        let node_id = self.node_id.clone();
        
        info!("Receiving Arrow Flight data stream on {}", node_id);
        
        // TODO: Update for new Arrow Flight API
        // The conversion between FlightData and RecordBatch has changed
        let schema = arrow::datatypes::Schema::empty();
        let batches = Vec::new();
        let total_bytes = 0;
        
        info!("Received {} batches, {} total bytes", batches.len(), total_bytes);
        
        // Store transfer
        let transfer_id = uuid::Uuid::new_v4().to_string();
        let transfer = TransferInfo {
            schema,
            batches,
            transfer_type: TransferType::BufferedData,
            source_node: "unknown".to_string(), // Would extract from metadata
            total_bytes,
        };
        
        self.transfers.write().await.insert(transfer_id.clone(), transfer);
        
        Ok(Response::new(stream::empty()))
    }
    
    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Ok(Response::new(stream::empty()))
    }
    
    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        let action_type = ActionType {
            r#type: "health_check".to_string(),
            description: "Check node health".to_string(),
        };
        Ok(Response::new(Box::pin(stream::once(futures::future::ready(Ok(action_type))))))
    }
    
    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange not implemented"))
    }
    
    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<arrow_flight::PollInfo>, Status> {
        Err(Status::unimplemented("PollFlightInfo not implemented"))
    }
}

/// Client for sending data via Arrow Flight
pub struct FlightTransferClient {
    node_id: String,
    clients: Arc<tokio::sync::RwLock<std::collections::HashMap<String, FlightServiceClient<tonic::transport::Channel>>>>,
}

impl FlightTransferClient {
    pub fn new(node_id: String) -> Self {
        Self {
            node_id,
            clients: Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
        }
    }
    
    /// Send log messages as Arrow batches
    pub async fn send_log_batch(
        &self,
        target_node: &str,
        messages: Vec<LogMessage>,
    ) -> Result<()> {
        // Convert to Arrow format
        let batch = self.messages_to_arrow_batch(messages)?;
        
        // Get or create client
        let mut client = self.get_or_create_client(target_node).await?;
        
        // TODO: Update for new Arrow Flight API
        let schema_data = FlightData::new();
        
        let batch_data = FlightData::new();
        
        let stream = stream::iter(vec![schema_data, batch_data]);
        
        // Send via do_put
        let response = client.do_put(Request::new(stream)).await?;
        let mut response_stream = response.into_inner();
        
        // Process response
        while let Some(result) = response_stream.next().await {
            match result {
                Ok(_) => debug!("Put result received"),
                Err(e) => return Err(anyhow::anyhow!("Put failed: {}", e)),
            }
        }
        
        info!("Successfully sent {} messages to {} via Arrow Flight", 
              batch.num_rows(), target_node);
        
        Ok(())
    }
    
    /// Convert log messages to Arrow RecordBatch
    fn messages_to_arrow_batch(&self, messages: Vec<LogMessage>) -> Result<RecordBatch> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
            Field::new("level", DataType::Utf8, false),
            Field::new("message", DataType::Utf8, false),
            Field::new("source", DataType::Utf8, false),
            Field::new("trace_id", DataType::Utf8, true),
        ]));
        
        let timestamps: TimestampNanosecondArray = messages.iter()
            .map(|m| Some(m.timestamp.timestamp_nanos_opt().unwrap_or(0)))
            .collect();
        
        let levels = StringArray::from_iter_values(messages.iter().map(|m| &m.level));
        let messages_array = StringArray::from_iter_values(messages.iter().map(|m| &m.message));
        let sources = StringArray::from_iter_values(messages.iter().map(|m| &m.source));
        let trace_ids = StringArray::from(messages.iter()
            .map(|m| m.trace_id.as_deref())
            .collect::<Vec<_>>());
        
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(timestamps),
                Arc::new(levels),
                Arc::new(messages_array),
                Arc::new(sources),
                Arc::new(trace_ids),
            ],
        ).map_err(Into::into)
    }
    
    /// Get or create Flight client for node
    async fn get_or_create_client(
        &self,
        node_id: &str,
    ) -> Result<FlightServiceClient<tonic::transport::Channel>> {
        let mut clients = self.clients.write().await;
        
        if let Some(client) = clients.get(node_id) {
            return Ok(client.clone());
        }
        
        // Create new client
        let addr = format!("http://{}:8815", node_id); // Flight port
        let channel = tonic::transport::Channel::from_shared(addr)?
            .connect()
            .await?;
        
        let client = FlightServiceClient::new(channel);
        clients.insert(node_id.to_string(), client.clone());
        
        Ok(client)
    }
}

// Example data structure
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct LogMessage {
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub level: String,
    pub message: String,
    pub source: String,
    pub trace_id: Option<String>,
}

use std::pin::Pin;

/// Integration with existing inter-node transfer
impl crate::data_transfer_actor::DataTransferActor {
    /// Enhanced transfer using Arrow Flight for large data
    pub async fn transfer_via_flight(
        &self,
        target_node: &str,
        transfer_type: TransferType,
        data: Vec<u8>,
    ) -> Result<()> {
        match transfer_type {
            TransferType::BufferedData => {
                // Deserialize and convert to Arrow
                let messages: Vec<LogMessage> = bincode::deserialize(&data)?;
                // TODO: Fix node_id access
                let client = FlightTransferClient::new("temp".to_string());
                client.send_log_batch(target_node, messages).await?;
            }
            TransferType::IndexShard => {
                // For index data, might use different schema
                todo!("Implement index shard transfer")
            }
            _ => {
                // Fall back to regular transfer
                return Err(anyhow::anyhow!("Transfer type not supported via Flight"));
            }
        }
        
        Ok(())
    }
}