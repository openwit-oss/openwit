use std::sync::Arc;
use std::collections::HashMap;
use anyhow::Result;
use arrow::record_batch::RecordBatch;
use arrow_flight::{
    flight_service_server::{FlightService, FlightServiceServer},
    flight_service_client::FlightServiceClient,
    FlightData, FlightDescriptor, PutResult,
};
use futures::{stream::BoxStream, StreamExt};
use tokio::sync::{RwLock, mpsc};
use tonic::{Request, Response, Status, Streaming, transport::Channel};
use tracing::{info, warn, error, debug};

use crate::address_cache::NodeAddressCache;

/// Batch data received via Arrow Flight
#[derive(Debug, Clone)]
pub struct ArrowFlightBatch {
    pub batch_id: String,
    pub source_node: String,
    pub target_node: String,
    pub record_batch: RecordBatch,
    pub metadata: HashMap<String, String>,
}

/// Arrow Flight service for all node types
pub struct ArrowFlightService {
    node_id: String,
    node_role: String,
    /// Channel to send received batches to node-specific handler
    batch_receiver: mpsc::Sender<ArrowFlightBatch>,
    /// Address cache for routing
    address_cache: Arc<NodeAddressCache>,
}

impl ArrowFlightService {
    pub fn new(
        node_id: String,
        node_role: String,
        address_cache: Arc<NodeAddressCache>,
    ) -> (Self, mpsc::Receiver<ArrowFlightBatch>) {
        let (tx, rx) = mpsc::channel(100);
        
        (Self {
            node_id,
            node_role,
            batch_receiver: tx,
            address_cache,
        }, rx)
    }
    
    /// Start Arrow Flight server on appropriate port based on node role
    pub async fn start(self, port: u16) -> Result<()> {
        let addr = format!("0.0.0.0:{}", port).parse()?;
        info!("Starting Arrow Flight server for {} node on {}", self.node_role, addr);
        
        let service = FlightServiceServer::new(self);
        
        tonic::transport::Server::builder()
            .add_service(service)
            .serve(addr)
            .await
            .map_err(|e| anyhow::anyhow!("Arrow Flight server error: {}", e))?;
            
        Ok(())
    }
}

#[tonic::async_trait]
impl FlightService for ArrowFlightService {
    type HandshakeStream = BoxStream<'static, Result<arrow_flight::HandshakeResponse, Status>>;
    type ListFlightsStream = BoxStream<'static, Result<arrow_flight::FlightInfo, Status>>;
    type DoGetStream = BoxStream<'static, Result<FlightData, Status>>;
    type DoPutStream = BoxStream<'static, Result<PutResult, Status>>;
    type DoActionStream = BoxStream<'static, Result<arrow_flight::Result, Status>>;
    type ListActionsStream = BoxStream<'static, Result<arrow_flight::ActionType, Status>>;
    type DoExchangeStream = BoxStream<'static, Result<FlightData, Status>>;
    
    async fn handshake(
        &self,
        _request: Request<Streaming<arrow_flight::HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("Handshake not implemented"))
    }
    
    async fn list_flights(
        &self,
        _request: Request<arrow_flight::Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        // Return empty stream for health checks
        let stream = futures::stream::empty();
        Ok(Response::new(Box::pin(stream)))
    }
    
    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<arrow_flight::FlightInfo>, Status> {
        Err(Status::unimplemented("GetFlightInfo not implemented"))
    }
    
    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<arrow_flight::SchemaResult>, Status> {
        Err(Status::unimplemented("GetSchema not implemented"))
    }
    
    async fn do_get(
        &self,
        _request: Request<arrow_flight::Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        Err(Status::unimplemented("DoGet not implemented - nodes only receive data"))
    }
    
    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        let mut stream = request.into_inner();
        let node_id = self.node_id.clone();
        let node_role = self.node_role.clone();
        let batch_receiver = self.batch_receiver.clone();
        
        // Spawn task to handle the stream
        let result_stream = async_stream::stream! {
            let mut descriptor: Option<FlightDescriptor> = None;
            let mut schema: Option<Arc<arrow::datatypes::Schema>> = None;
            let mut batches = Vec::new();
            let mut total_bytes = 0;
            let mut metadata = HashMap::new();
            
            // Process incoming flight data
            while let Some(data) = stream.next().await {
                match data {
                    Ok(flight_data) => {
                        // First message contains descriptor
                        if descriptor.is_none() && flight_data.flight_descriptor.is_some() {
                            descriptor = flight_data.flight_descriptor.clone();
                            
                            // Extract metadata from descriptor
                            if let Some(ref desc) = descriptor {
                                if let Ok(batch_id) = String::from_utf8(desc.cmd.to_vec()) {
                                    metadata.insert("batch_id".to_string(), batch_id);
                                }
                                
                                // Path might contain additional metadata
                                for (i, path_part) in desc.path.iter().enumerate() {
                                    metadata.insert(format!("path_{}", i), path_part.clone());
                                }
                            }
                            
                            // Extract metadata from app_metadata field if present
                            if !flight_data.app_metadata.is_empty() {
                                if let Ok(app_metadata_str) = std::str::from_utf8(&flight_data.app_metadata) {
                                    if let Ok(app_metadata_map) = serde_json::from_str::<HashMap<String, String>>(app_metadata_str) {
                                        debug!("Extracted app_metadata: {:?}", app_metadata_map);
                                        // Merge app_metadata into the metadata map
                                        for (key, value) in app_metadata_map {
                                            metadata.insert(key, value);
                                        }
                                    } else {
                                        debug!("Failed to parse app_metadata as JSON: {}", app_metadata_str);
                                    }
                                } else {
                                    debug!("app_metadata is not valid UTF-8");
                                }
                            }
                            
                            debug!("Received flight descriptor for node {} with metadata: {:?}", node_id, metadata);
                        }
                        
                        // Check if this FlightData contains schema information
                        if schema.is_none() && !flight_data.data_header.is_empty() {
                            // Try to parse schema using the Arrow IPC format
                            if let Ok(message) = arrow::ipc::root_as_message(&flight_data.data_header) {
                                // Check if this is a schema message
                                if message.header_type() == arrow::ipc::MessageHeader::Schema {
                                    if let Some(header) = message.header_as_schema() {
                                        let decoded_schema = arrow::ipc::convert::fb_to_schema(header);
                                        schema = Some(Arc::new(decoded_schema));
                                        debug!("Successfully decoded schema from flight data");
                                    }
                                }
                            }
                        }
                        
                        // Accumulate data
                        total_bytes += flight_data.data_body.len();
                        batches.push(flight_data);
                    }
                    Err(e) => {
                        error!("Error receiving flight data: {}", e);
                        yield Err(e);
                        return;
                    }
                }
            }
            
            // Process complete batch
            if let Some(ref batch_schema) = schema {
                debug!("Processing batch with schema: {:?}", batch_schema);
                let batch_id = metadata.get("batch_id")
                    .cloned()
                    .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
                    
                info!("{} node {} received batch {} ({} bytes) with metadata: {:?}", 
                      node_role, node_id, batch_id, total_bytes, metadata);
                
                // Convert FlightData to RecordBatch
                let mut record_batches = Vec::new();
                
                // Use the decode module for converting FlightData to RecordBatch
                use arrow_flight::decode::FlightRecordBatchStream;
                
                // Create a stream from our FlightData messages
                // Include all messages - the decoder will handle schema and data messages appropriately
                let data_stream = futures::stream::iter(
                    batches.into_iter()
                        .filter(|fd| !fd.data_header.is_empty() || !fd.data_body.is_empty())  // Filter out empty messages
                        .map(Ok::<_, arrow_flight::error::FlightError>)
                );
                
                // Create the decoder - it automatically detects schema from the stream
                let mut decoder = FlightRecordBatchStream::new_from_flight_data(data_stream);
                
                // Decode all batches
                while let Some(decoded_result) = decoder.next().await {
                    match decoded_result {
                        Ok(batch) => {
                            info!("Decoded record batch with {} rows", batch.num_rows());
                            record_batches.push(batch);
                        }
                        Err(e) => {
                            error!("Failed to decode flight data: {}", e);
                            yield Err(Status::internal(format!("Failed to decode batch: {}", e)));
                            return;
                        }
                    }
                }
                
                // Send all batches to handler
                for (i, batch) in record_batches.into_iter().enumerate() {
                    let arrow_batch = ArrowFlightBatch {
                        batch_id: format!("{}_{}", batch_id, i),
                        source_node: metadata.get("source_node")
                            .cloned()
                            .unwrap_or_else(|| "unknown".to_string()),
                        target_node: node_id.clone(),
                        record_batch: batch,
                        metadata: metadata.clone(),
                    };
                    
                    // Send to node-specific handler
                    if let Err(e) = batch_receiver.send(arrow_batch).await {
                        error!("Failed to send batch to handler: {}", e);
                        yield Err(Status::internal("Failed to process batch"));
                        return;
                    }
                }
                
                // Send success response
                yield Ok(PutResult {
                    app_metadata: format!("Batch {} received by {} node", batch_id, node_role).into_bytes().into(),
                });
                
                info!("Successfully processed batch {} on {} node {}", 
                      batch_id, node_role, node_id);
            } else {
                yield Err(Status::invalid_argument("Missing schema"));
            }
        };
        
        Ok(Response::new(Box::pin(result_stream)))
    }
    
    async fn do_action(
        &self,
        _request: Request<arrow_flight::Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("DoAction not implemented"))
    }
    
    async fn list_actions(
        &self,
        _request: Request<arrow_flight::Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("ListActions not implemented"))
    }
    
    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("DoExchange not implemented"))
    }
    
    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<arrow_flight::PollInfo>, Status> {
        Err(Status::unimplemented("PollFlightInfo not implemented"))
    }
}

/// Arrow Flight client for sending batches
pub struct ArrowFlightClient {
    node_id: String,
    clients: Arc<RwLock<HashMap<String, FlightServiceClient<Channel>>>>,
}

impl ArrowFlightClient {
    pub fn new(node_id: String) -> Self {
        Self {
            node_id,
            clients: Arc::new(RwLock::new(HashMap::new())),
        }
    }
    
    /// Get or create a Flight client for the given endpoint
    async fn get_client(&self, endpoint: &str) -> Result<FlightServiceClient<Channel>> {
        let mut clients = self.clients.write().await;
        
        if let Some(client) = clients.get(endpoint) {
            return Ok(client.clone());
        }
        
        info!("Creating new Arrow Flight connection to {}", endpoint);
        
        // Handle different endpoint formats
        let connection_url = if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
            endpoint.to_string()
        } else if endpoint.starts_with("grpc://") {
            // Convert grpc:// to http:// for tonic
            endpoint.replace("grpc://", "http://")
        } else {
            format!("http://{}", endpoint)
        };
        
        info!("Connecting to Arrow Flight URL: {} (converted from {})", connection_url, endpoint);
        
        // Add retry logic for connection attempts
        let mut retry_count = 0;
        let max_retries = 3;
        let mut last_error: Option<String> = None;
        
        while retry_count < max_retries {
            match FlightServiceClient::connect(connection_url.clone()).await {
                Ok(client) => {
                    info!("Successfully connected to Arrow Flight endpoint {}", endpoint);
                    clients.insert(endpoint.to_string(), client.clone());
                    return Ok(client);
                }
                Err(e) => {
                    retry_count += 1;
                    last_error = Some(format!("{:?}", e));
                    
                    if retry_count < max_retries {
                        let delay_ms = 100 * (2_u64.pow(retry_count - 1));
                        warn!(
                            "Failed to connect to Flight endpoint {} (converted URL: {}) - attempt {}/{}: {} - retrying in {}ms", 
                            endpoint, connection_url, retry_count, max_retries, last_error.as_ref().unwrap(), delay_ms
                        );
                        tokio::time::sleep(tokio::time::Duration::from_millis(delay_ms)).await;
                    }
                }
            }
        }
        
        Err(anyhow::anyhow!(
            "Failed to connect to Flight endpoint {} after {} attempts: {:?}", 
            endpoint, max_retries, last_error
        ))
    }
    
    /// Send a batch to target node
    pub async fn send_batch(
        &self,
        target_endpoint: &str,
        batch_id: &str,
        record_batch: RecordBatch,
        metadata: HashMap<String, String>,
    ) -> Result<()> {
        let mut client = self.get_client(target_endpoint).await?;
        
        debug!("Sending batch {} to {} via Arrow Flight ({} rows)", 
               batch_id, target_endpoint, record_batch.num_rows());
        
        // Create flight descriptor with batch metadata
        // Format: "batch_id=xxx;tenant=yyy;signal=zzz"
        let metadata_str = format!(
            "batch_id={};tenant={};signal={}", 
            batch_id,
            metadata.get("tenant").cloned().unwrap_or_else(|| "default".to_string()),
            metadata.get("signal").cloned().unwrap_or_else(|| "traces".to_string())
        );
        let mut descriptor = FlightDescriptor::new_cmd(metadata_str.as_bytes().to_vec());
        
        // Add metadata to path
        descriptor.path.push(self.node_id.clone()); // source node
        
        // Convert RecordBatch to FlightData stream
        let flight_data_stream = create_flight_data_stream(record_batch, descriptor, metadata).await?;
        
        // Send data
        let response = client.do_put(flight_data_stream)
            .await
            .map_err(|e| anyhow::anyhow!("Flight do_put failed: {}", e))?;
        
        // Process put results
        let mut response_stream = response.into_inner();
        while let Some(result) = response_stream.message().await? {
            debug!("Flight put result: {:?}", result);
        }
        
        info!("Successfully sent batch {} to {}", batch_id, target_endpoint);
        Ok(())
    }
}

/// Convert RecordBatch to FlightData stream
async fn create_flight_data_stream(
    batch: RecordBatch,
    descriptor: FlightDescriptor,
    metadata: HashMap<String, String>,
) -> Result<impl futures::Stream<Item = FlightData>> {
    use futures::stream;
    
    // Prepare options
    let options = arrow::ipc::writer::IpcWriteOptions::default();
    
    // Create initial message with just the descriptor and metadata
    let mut flight_data_vec = vec![
        FlightData {
            flight_descriptor: Some(descriptor),
            app_metadata: if !metadata.is_empty() {
                serde_json::to_vec(&metadata).ok().map(|v| v.into()).unwrap_or_default()
            } else {
                Default::default()
            },
            ..Default::default()
        },
    ];
    
    // Convert RecordBatch to FlightData using FlightDataEncoder
    use arrow_flight::encode::FlightDataEncoderBuilder;
    
    // Get the row count before moving the batch
    let num_rows = batch.num_rows();
    
    let encoder = FlightDataEncoderBuilder::new()
        .with_options(options)
        .build(futures::stream::once(async move { Ok(batch) }));
    
    // Collect all flight data messages
    let encoder_data: Vec<FlightData> = encoder
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| anyhow::anyhow!("Failed to encode batch: {}", e))?;
    
    // Add all messages from encoder (including schema)
    // We need to include the encoder's schema for proper decoding
    for data in encoder_data {
        flight_data_vec.push(data);
    }
    
    info!("Created flight data stream with {} messages for batch with {} rows", 
        flight_data_vec.len(), num_rows);
    
    Ok(stream::iter(flight_data_vec))
}

/// Get the Arrow Flight port for a given node role
pub fn get_flight_port(node_role: &str) -> u16 {
    match node_role {
        "proxy" => 8088,      // Proxy nodes don't receive, only send
        "ingest" => 8089,     // Ingestion nodes receive from proxy
        "indexer" => 8090,    // Indexer nodes receive from ingestion
        "storage" => 8091,    // Storage nodes receive from various sources
        "search" => 8092,     // Search nodes might receive index updates
        "control" => 8093,    // Control nodes might receive status updates
        "janitor" => 8094,    // Janitor nodes might receive cleanup tasks
        _ => 8095,            // Default for unknown roles
    }
}