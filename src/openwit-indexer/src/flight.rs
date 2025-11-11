use anyhow::{Context, Result};
use arrow::array::RecordBatch;
use arrow_flight::{
    flight_service_server::{FlightService, FlightServiceServer},
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor,
    FlightInfo, HandshakeRequest, HandshakeResponse, PutResult, PollInfo,
    SchemaResult, Ticket,
};
use bytes::Bytes;
use futures::{stream::BoxStream, StreamExt};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, info, error};
use uuid::Uuid;

use crate::{
    config::IndexerConfig,
    db::{DbClient},
    storage::StorageClient,
    artifacts::ArtifactBuilder,
    wal::WAL,
};

/// Arrow Flight service for receiving data from storage nodes
#[allow(dead_code)]
pub struct IndexerFlightService {
    node_id: String,
    config: Arc<IndexerConfig>,
    db_client: Arc<DbClient>,
    storage_client: Arc<StorageClient>,
    artifact_builder: Arc<ArtifactBuilder>,
    wal: Arc<WAL>,
    active_streams: Arc<RwLock<HashMap<String, StreamMetadata>>>,
    batch_sender: mpsc::Sender<IncomingBatch>,
}

/// Metadata for active streaming sessions
#[allow(dead_code)]
#[derive(Debug, Clone)]
struct StreamMetadata {
    stream_id: String,
    file_ulid: String,
    tenant: String,
    signal: String,
    partition_key: String,
    start_time: chrono::DateTime<chrono::Utc>,
    row_count: usize,
}

/// Incoming batch to be processed
pub struct IncomingBatch {
    pub stream_id: String,
    pub file_ulid: String,
    pub tenant: String,
    pub signal: String,
    pub partition_key: String,
    pub batch: RecordBatch,
}

impl IndexerFlightService {
    pub fn new(
        node_id: String,
        config: Arc<IndexerConfig>,
        db_client: Arc<DbClient>,
        storage_client: Arc<StorageClient>,
        artifact_builder: Arc<ArtifactBuilder>,
        wal: Arc<WAL>,
    ) -> (Self, mpsc::Receiver<IncomingBatch>) {
        let (batch_sender, batch_receiver) = mpsc::channel(1000);

        let service = Self {
            node_id,
            config,
            db_client,
            storage_client,
            artifact_builder,
            wal,
            active_streams: Arc::new(RwLock::new(HashMap::new())),
            batch_sender,
        };

        (service, batch_receiver)
    }

    /// Process incoming batches in the background
    pub async fn process_batches(
        mut batch_receiver: mpsc::Receiver<IncomingBatch>,
        artifact_builder: Arc<ArtifactBuilder>,
        storage_client: Arc<StorageClient>,
        db_client: Arc<DbClient>,
        wal: Arc<WAL>,
    ) {
        let mut accumulated_batches: HashMap<String, Vec<RecordBatch>> = HashMap::new();

        while let Some(incoming) = batch_receiver.recv().await {
            debug!(
                file_ulid = %incoming.file_ulid,
                rows = incoming.batch.num_rows(),
                "Received batch for processing"
            );

            // Accumulate batches by file
            accumulated_batches
                .entry(incoming.file_ulid.clone())
                .or_insert_with(Vec::new)
                .push(incoming.batch);

            // Process when we have enough data or on stream completion
            // This is simplified - in production you'd have more sophisticated logic
            if let Some(batches) = accumulated_batches.get(&incoming.file_ulid) {
                if batches.iter().map(|b| b.num_rows()).sum::<usize>() >= 100_000 {
                    if let Some(batches) = accumulated_batches.remove(&incoming.file_ulid) {
                        tokio::spawn(Self::create_artifacts(
                            incoming.file_ulid,
                            incoming.tenant,
                            incoming.signal,
                            incoming.partition_key,
                            batches,
                            artifact_builder.clone(),
                            storage_client.clone(),
                            db_client.clone(),
                            wal.clone(),
                        ));
                    }
                }
            }
        }
    }

    /// Create index artifacts from accumulated batches
    async fn create_artifacts(
        file_ulid: String,
        tenant: String,
        signal: String,
        partition_key: String,
        batches: Vec<RecordBatch>,
        artifact_builder: Arc<ArtifactBuilder>,
        storage_client: Arc<StorageClient>,
        db_client: Arc<DbClient>,
        wal: Arc<WAL>,
    ) -> Result<()> {
        let operation_id = format!("flight_index_{}", file_ulid);

        // Check if already completed
        if wal.is_operation_completed(&operation_id) {
            debug!(file_ulid = %file_ulid, "File already indexed via flight");
            return Ok(());
        }

        // Log operation start
        let operation = crate::wal::WALOperation::IndexDataFile {
            file_ulid: file_ulid.clone(),
            tenant: tenant.clone(),
            signal: signal.clone(),
            partition_key: partition_key.clone(),
            parquet_url: format!("flight://{}", file_ulid), // Placeholder URL
            size_bytes: batches.iter().map(|b| b.get_array_memory_size()).sum::<usize>() as u64,
        };

        wal.log_operation_start(operation_id.clone(), operation)?;

        info!(
            file_ulid = %file_ulid,
            batch_count = batches.len(),
            total_rows = batches.iter().map(|b| b.num_rows()).sum::<usize>(),
            "Creating index artifacts from flight data"
        );

        // Create artifacts
        let result = async {
            // Create bloom filter
            let bloom_filter = artifact_builder.build_bloom_filter(
                &file_ulid,
                &partition_key,
                &batches,
            )?;

            let bloom_data = bloom_filter.serialize()?;
            let bloom_metadata = crate::storage::ArtifactMetadata {
                file_ulid: file_ulid.clone(),
                tenant: tenant.clone(),
                signal: signal.clone(),
                partition_key: partition_key.clone(),
                artifact_type: crate::storage::ArtifactType::Bloom,
                size_bytes: bloom_data.len() as u64,
                content_hash: crate::storage::calculate_hash(&bloom_data),
                created_at: chrono::Utc::now(),
            };

            let bloom_url = storage_client.store_artifact(&bloom_metadata, bloom_data).await?;

            // Create bitmap index
            let bitmap_index = artifact_builder.build_bitmap_index(
                &file_ulid,
                &partition_key,
                &batches,
            )?;

            let bitmap_data = bitmap_index.serialize()?;
            let bitmap_metadata = crate::storage::ArtifactMetadata {
                file_ulid: file_ulid.clone(),
                tenant: tenant.clone(),
                signal: signal.clone(),
                partition_key: partition_key.clone(),
                artifact_type: crate::storage::ArtifactType::Bitmap,
                size_bytes: bitmap_data.len() as u64,
                content_hash: crate::storage::calculate_hash(&bitmap_data),
                created_at: chrono::Utc::now(),
            };

            let bitmap_url = storage_client.store_artifact(&bitmap_metadata, bitmap_data).await?;

            // Store artifact metadata in database
            let bloom_artifact = crate::db::IndexArtifact {
                artifact_id: Uuid::new_v4(),
                file_ulid: file_ulid.clone(),
                tenant: tenant.clone(),
                signal: signal.clone(),
                partition_key: partition_key.clone(),
                artifact_type: "bloom".to_string(),
                artifact_url: bloom_url,
                size_bytes: bloom_metadata.size_bytes as i64,
                created_at: chrono::Utc::now(),
                metadata: serde_json::json!({"source": "arrow_flight"}),
            };

            db_client.insert_index_artifact(&bloom_artifact).await?;

            let bitmap_artifact = crate::db::IndexArtifact {
                artifact_id: Uuid::new_v4(),
                file_ulid: file_ulid.clone(),
                tenant: tenant.clone(),
                signal: signal.clone(),
                partition_key: partition_key.clone(),
                artifact_type: "bitmap".to_string(),
                artifact_url: bitmap_url,
                size_bytes: bitmap_metadata.size_bytes as i64,
                created_at: chrono::Utc::now(),
                metadata: serde_json::json!({"source": "arrow_flight"}),
            };

            db_client.insert_index_artifact(&bitmap_artifact).await?;

            Ok::<(), anyhow::Error>(())
        }.await;

        match result {
            Ok(()) => {
                wal.log_operation_completed(operation_id)?;
                info!(file_ulid = %file_ulid, "Successfully created artifacts from flight data");
            }
            Err(e) => {
                wal.log_operation_failed(operation_id, e.to_string())?;
                error!(file_ulid = %file_ulid, error = %e, "Failed to create artifacts from flight data");
                return Err(e);
            }
        }

        Ok(())
    }
}

#[tonic::async_trait]
impl FlightService for IndexerFlightService {
    type HandshakeStream = BoxStream<'static, Result<HandshakeResponse, Status>>;
    type ListFlightsStream = BoxStream<'static, Result<FlightInfo, Status>>;
    type DoGetStream = BoxStream<'static, Result<FlightData, Status>>;
    type DoPutStream = BoxStream<'static, Result<PutResult, Status>>;
    type DoActionStream = BoxStream<'static, Result<arrow_flight::Result, Status>>;
    type ListActionsStream = BoxStream<'static, Result<ActionType, Status>>;
    type DoExchangeStream = BoxStream<'static, Result<FlightData, Status>>;

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("Handshake not implemented"))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("ListFlights not implemented"))
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("GetFlightInfo not implemented"))
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("GetSchema not implemented"))
    }

    async fn do_get(
        &self,
        _request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        Err(Status::unimplemented("DoGet not implemented - indexer only receives data"))
    }

    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        let stream_id = Uuid::new_v4().to_string();
        info!(stream_id = %stream_id, "Received DoPut request");

        let mut stream = request.into_inner();
        let _batch_sender = self.batch_sender.clone();
        let active_streams = self.active_streams.clone();

        // Process the stream
        let (tx, rx) = mpsc::channel(10);

        tokio::spawn(async move {
            let mut metadata: Option<StreamMetadata> = None;
            let mut schema = None;

            while let Some(flight_data) = stream.next().await {
                match flight_data {
                    Ok(data) => {
                        // First message should contain the descriptor
                        if metadata.is_none() && data.flight_descriptor.is_some() {
                            if let Some(ref desc) = data.flight_descriptor {
                                // Parse metadata from descriptor path
                                // Format: "tenant/signal/partition/file_ulid"
                                let parts: Vec<&str> = desc.path[0].split('/').collect();
                                if parts.len() >= 4 {
                                    let meta = StreamMetadata {
                                        stream_id: stream_id.clone(),
                                        tenant: parts[0].to_string(),
                                        signal: parts[1].to_string(),
                                        partition_key: parts[2].to_string(),
                                        file_ulid: parts[3].to_string(),
                                        start_time: chrono::Utc::now(),
                                        row_count: 0,
                                    };

                                    active_streams.write().await.insert(stream_id.clone(), meta.clone());
                                    metadata = Some(meta);
                                }
                            }
                        }

                        // Process schema and data
                        if !data.data_header.is_empty() {
                            // This is schema information
                            schema = Some(data);
                        } else if let (Some(_meta), Some(_schema)) = (&metadata, &schema) {
                            // This is actual data
                            // In a real implementation, you'd decode the RecordBatch here
                            // For now, we'll send a placeholder
                            
                            debug!(
                                stream_id = %stream_id,
                                "Processing flight data batch"
                            );

                            // Send result
                            let result = PutResult {
                                app_metadata: Bytes::from("OK"),
                            };
                            let _ = tx.send(Ok(result)).await;
                        }
                    }
                    Err(e) => {
                        error!(stream_id = %stream_id, error = %e, "Error in flight stream");
                        break;
                    }
                }
            }

            // Clean up
            active_streams.write().await.remove(&stream_id);
            info!(stream_id = %stream_id, "Flight stream completed");
        });

        Ok(Response::new(Box::pin(
            tokio_stream::wrappers::ReceiverStream::new(rx)
        )))
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("DoAction not implemented"))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
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
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("PollFlightInfo not implemented"))
    }
}

/// Create and start the Arrow Flight server
pub async fn start_flight_server(
    config: Arc<IndexerConfig>,
    db_client: Arc<DbClient>,
    storage_client: Arc<StorageClient>,
    artifact_builder: Arc<ArtifactBuilder>,
    wal: Arc<WAL>,
    addr: std::net::SocketAddr,
) -> Result<()> {
    let node_id = config.indexer.general.node_id.clone();
    
    let (service, batch_receiver) = IndexerFlightService::new(
        node_id.clone(),
        config,
        db_client.clone(),
        storage_client.clone(),
        artifact_builder.clone(),
        wal.clone(),
    );

    // Start batch processor
    tokio::spawn(IndexerFlightService::process_batches(
        batch_receiver,
        artifact_builder,
        storage_client,
        db_client,
        wal,
    ));

    let flight_service = FlightServiceServer::new(service);

    info!(
        node_id = %node_id,
        addr = %addr,
        "Starting Arrow Flight server for indexer"
    );

    tonic::transport::Server::builder()
        .add_service(flight_service)
        .serve(addr)
        .await
        .context("Failed to start Arrow Flight server")?;

    Ok(())
}