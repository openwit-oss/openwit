use anyhow::Result;
use tonic::{Request, Response, Status};
use tracing::{info, error, debug};
use std::sync::Arc;
use futures::Stream;
use std::pin::Pin;
use openwit_proto::storage::{
    storage_query_service_server::{StorageQueryService, StorageQueryServiceServer},
    QueryRequest as ProtoQueryRequest,
    QueryResponse as ProtoQueryResponse,
    QueryResult, Row, Value, ColumnSchema, QueryResultChunk,
    StorageStatsRequest, StorageStatsResponse,
    value::Value as ValueEnum,
};

use crate::simple_query_executor::{SimpleQueryExecutor, QueryResponse as InternalQueryResponse};

/// gRPC service implementation for storage queries
pub struct SimpleStorageQueryService {
    query_executor: Arc<SimpleQueryExecutor>,
}

impl SimpleStorageQueryService {
    pub fn new(data_path: String) -> Result<Self> {
        let query_executor = Arc::new(SimpleQueryExecutor::new(data_path)?);

        Ok(Self {
            query_executor,
        })
    }

    /// Convert internal query response to protobuf format
    fn convert_to_proto_response(&self,
        internal_response: InternalQueryResponse,
        request_id: String,
    ) -> Result<ProtoQueryResponse> {
        debug!("Converting response with {} rows", internal_response.row_count);

        let query_result = self.convert_results_to_proto(&internal_response)?;

        Ok(ProtoQueryResponse {
            request_id,
            success: true,
            error: String::new(),
            result: Some(query_result),
            execution_time_ms: 0,
        })
    }

    /// Convert JSON results to protobuf format
    fn convert_results_to_proto(&self,
        response: &InternalQueryResponse
    ) -> Result<QueryResult> {
        debug!("Converting {} JSON rows to proto format", response.results.len());

        let mut proto_rows = Vec::new();
        let mut columns = Vec::new();
        let mut columns_initialized = false;

        for json_row in &response.results {
            if let serde_json::Value::Object(obj) = json_row {
                // Initialize columns from first row
                if !columns_initialized {
                    for key in obj.keys() {
                        columns.push(ColumnSchema {
                            name: key.clone(),
                            data_type: "STRING".to_string(), // Simplified - all as strings for now
                            nullable: true,
                        });
                    }
                    columns_initialized = true;
                }

                let mut proto_values = Vec::new();

                for (_key, value) in obj {
                    let proto_value = match value {
                        serde_json::Value::String(s) => Value {
                            value: Some(ValueEnum::StringValue(s.clone())),
                        },
                        serde_json::Value::Number(n) => {
                            if let Some(i) = n.as_i64() {
                                Value {
                                    value: Some(ValueEnum::Int64Value(i)),
                                }
                            } else if let Some(f) = n.as_f64() {
                                Value {
                                    value: Some(ValueEnum::DoubleValue(f)),
                                }
                            } else {
                                Value {
                                    value: Some(ValueEnum::StringValue(n.to_string())),
                                }
                            }
                        },
                        serde_json::Value::Bool(b) => Value {
                            value: Some(ValueEnum::BoolValue(*b)),
                        },
                        serde_json::Value::Null => Value {
                            value: Some(ValueEnum::NullValue(0)),
                        },
                        _ => Value {
                            value: Some(ValueEnum::StringValue(value.to_string())),
                        },
                    };
                    proto_values.push(proto_value);
                }

                proto_rows.push(Row {
                    values: proto_values,
                });
            }
        }

        Ok(QueryResult {
            columns,
            rows: proto_rows,
            total_rows: response.row_count as u64,
            rows_scanned: 0, // Could track this in DataFusion
            bytes_scanned: 0, // Could track this in DataFusion
        })
    }
}

#[tonic::async_trait]
impl StorageQueryService for SimpleStorageQueryService {
    async fn execute_query(
        &self,
        request: Request<ProtoQueryRequest>,
    ) -> Result<Response<ProtoQueryResponse>, Status> {
        let proto_request = request.into_inner();

        info!("Received query request - Query: {}", proto_request.query);
        info!("Client ID: {}", proto_request.client_id);
        info!("Limit: {}", proto_request.limit);

        // Convert limit from u32 to Option<usize>
        let limit = if proto_request.limit > 0 {
            Some(proto_request.limit as usize)
        } else {
            None
        };

        // Execute the query
        let start = std::time::Instant::now();

        match self.query_executor.execute_query(
            proto_request.query.clone(),
            proto_request.client_id.clone(),
            limit,
        ).await {
            Ok(internal_response) => {
                let execution_time_ms = start.elapsed().as_millis() as u64;

                // Convert to proto response
                match self.convert_to_proto_response(internal_response, proto_request.request_id.clone()) {
                    Ok(mut proto_response) => {
                        proto_response.execution_time_ms = execution_time_ms;
                        info!("Query executed successfully in {}ms", execution_time_ms);
                        Ok(Response::new(proto_response))
                    }
                    Err(e) => {
                        error!("Failed to convert response: {}", e);
                        Ok(Response::new(ProtoQueryResponse {
                            request_id: proto_request.request_id,
                            success: false,
                            error: format!("Failed to convert response: {}", e),
                            result: None,
                            execution_time_ms,
                        }))
                    }
                }
            }
            Err(e) => {
                let execution_time_ms = start.elapsed().as_millis() as u64;
                error!("Query execution failed: {}", e);
                Ok(Response::new(ProtoQueryResponse {
                    request_id: proto_request.request_id,
                    success: false,
                    error: format!("Query execution failed: {}", e),
                    result: None,
                    execution_time_ms,
                }))
            }
        }
    }

    type ExecuteStreamingQueryStream = Pin<Box<dyn Stream<Item = Result<QueryResultChunk, Status>> + Send>>;

    async fn execute_streaming_query(
        &self,
        _request: Request<ProtoQueryRequest>,
    ) -> Result<Response<Self::ExecuteStreamingQueryStream>, Status> {
        // Not implemented for now - return an error
        Err(Status::unimplemented("Streaming queries not yet implemented"))
    }

    async fn get_storage_stats(
        &self,
        _request: Request<StorageStatsRequest>,
    ) -> Result<Response<StorageStatsResponse>, Status> {
        // Not implemented for now - return empty stats
        Ok(Response::new(StorageStatsResponse {
            total_files: 0,
            total_size_bytes: 0,
            clients: vec![],
            last_updated: None,
        }))
    }
}

/// Start the gRPC query service
pub async fn start_simple_grpc_query_service(
    data_path: String,
    bind_addr: String,
) -> Result<()> {
    info!("Starting simple gRPC query service");
    info!("Bind address: {}", bind_addr);
    info!("Data path: {}", data_path);

    // Verify data path exists
    if !std::path::Path::new(&data_path).exists() {
        error!("Data path does not exist: {}", data_path);
        std::fs::create_dir_all(&data_path)?;
        info!("Created data directory: {}", data_path);
    }

    // Create service
    let service = SimpleStorageQueryService::new(data_path)?;

    // Create gRPC server
    let addr = bind_addr.parse()
        .map_err(|e| anyhow::anyhow!("Invalid bind address: {}", e))?;

    info!("Starting gRPC server on {}", addr);

    tonic::transport::Server::builder()
        .add_service(StorageQueryServiceServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
}