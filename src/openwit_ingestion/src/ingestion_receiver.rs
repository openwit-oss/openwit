use std::sync::Arc;
use std::pin::Pin;
use anyhow::Result;
use tokio::sync::{mpsc, broadcast};
use tokio_stream::{Stream, wrappers::ReceiverStream};
use tonic::{Request, Response, Status, Streaming};
use tracing::{info, warn, error, debug};
use base64::Engine;
use prost::Message;
use arrow::array::{StringArray, RecordBatch};
use arrow::datatypes::{Schema, Field, DataType};
use std::sync::Arc as StdArc;

use openwit_proto::ingestion::{
    telemetry_ingestion_service_server::{TelemetryIngestionService, TelemetryIngestionServiceServer},
    TelemetryIngestRequest,
    TelemetryIngestResponse,
    TelemetryData,
    TelemetryIngestStatus,
};

use openwit_control_plane::ControlPlaneClient;
use openwit_config::UnifiedConfig;

// Import OTLP processing modules
use crate::otlp_preserving_receiver::OtlpPreservingReceiver;
use openwit_inter_node::ArrowFlightBatch;

// Storage endpoint cache for service discovery
use std::time::{Instant, Duration};
use tokio::sync::Mutex;
use sqlx::PgPool;

/// Simple batch tracker for PostgreSQL updates (to avoid circular dependency)
pub struct SimpleBatchTracker {
    db_pool: PgPool,
}

impl SimpleBatchTracker {
    pub fn new(db_pool: PgPool) -> Self {
        Self { db_pool }
    }
    
    /// Update ingestion pipeline status
    pub async fn update_ingestion_completed(&self, batch_id: uuid::Uuid) -> anyhow::Result<()> {
        let query = r#"
        UPDATE batch_tracker 
        SET ingestion_pip = true, 
            ingestion_completed_at = NOW(),
            wal_duration_ms = CASE 
                WHEN wal_completed_at IS NOT NULL 
                THEN EXTRACT(EPOCH FROM (NOW() - wal_completed_at)) * 1000
                ELSE NULL 
            END
        WHERE batch_id = $1
        "#;
        
        sqlx::query(query)
            .bind(batch_id)
            .execute(&self.db_pool)
            .await?;
        
        Ok(())
    }
}

struct StorageEndpointCache {
    endpoint: Option<String>,
    last_updated: Instant,
    ttl: Duration,
}

impl StorageEndpointCache {
    fn new() -> Self {
        Self {
            endpoint: None,
            last_updated: Instant::now() - Duration::from_secs(3600), // Force initial lookup
            ttl: Duration::from_secs(30), // Cache for 30 seconds
        }
    }

    fn is_expired(&self) -> bool {
        self.last_updated.elapsed() > self.ttl
    }

    fn update(&mut self, endpoint: String) {
        self.endpoint = Some(endpoint);
        self.last_updated = Instant::now();
    }
}

// Import OTLP protobuf types for decoding
use opentelemetry_proto::tonic::trace::v1::TracesData;

/// Decode base64-encoded OTLP protobuf data to JSON
fn decode_otlp_to_json(base64_payload: &str) -> Result<String, String> {
    // Decode base64
    let decoded_bytes = match base64::engine::general_purpose::STANDARD.decode(base64_payload) {
        Ok(bytes) => bytes,
        Err(e) => return Err(format!("Base64 decode error: {}", e)),
    };
    
    // Try to decode as OTLP TracesData protobuf
    match TracesData::decode(&decoded_bytes[..]) {
        Ok(traces_data) => {
            // Manually convert to JSON structure
            let mut json_obj = serde_json::Map::new();
            let mut resource_spans = Vec::new();
            
            for resource_span in traces_data.resource_spans {
                let mut rs_obj = serde_json::Map::new();
                
                // Add resource attributes
                if let Some(resource) = resource_span.resource {
                    let mut resource_obj = serde_json::Map::new();
                    let mut attributes = Vec::new();
                    
                    for attr in resource.attributes {
                        let mut attr_obj = serde_json::Map::new();
                        attr_obj.insert("key".to_string(), serde_json::Value::String(attr.key));
                        
                        if let Some(value) = attr.value {
                            let mut value_obj = serde_json::Map::new();
                            if let Some(v) = value.value {
                                match v {
                                    opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s) => {
                                        value_obj.insert("stringValue".to_string(), serde_json::Value::String(s));
                                    }
                                    opentelemetry_proto::tonic::common::v1::any_value::Value::BoolValue(b) => {
                                        value_obj.insert("boolValue".to_string(), serde_json::Value::Bool(b));
                                    }
                                    opentelemetry_proto::tonic::common::v1::any_value::Value::IntValue(i) => {
                                        value_obj.insert("intValue".to_string(), serde_json::Value::Number(serde_json::Number::from(i)));
                                    }
                                    opentelemetry_proto::tonic::common::v1::any_value::Value::DoubleValue(d) => {
                                        if let Some(num) = serde_json::Number::from_f64(d) {
                                            value_obj.insert("doubleValue".to_string(), serde_json::Value::Number(num));
                                        }
                                    }
                                    _ => {
                                        value_obj.insert("other".to_string(), serde_json::Value::String("unsupported_type".to_string()));
                                    }
                                }
                            }
                            attr_obj.insert("value".to_string(), serde_json::Value::Object(value_obj));
                        }
                        
                        attributes.push(serde_json::Value::Object(attr_obj));
                    }
                    
                    resource_obj.insert("attributes".to_string(), serde_json::Value::Array(attributes));
                    rs_obj.insert("resource".to_string(), serde_json::Value::Object(resource_obj));
                }
                
                // Add scope spans (simplified)
                let mut scope_spans = Vec::new();
                for scope_span in resource_span.scope_spans {
                    let mut ss_obj = serde_json::Map::new();
                    
                    if let Some(scope) = scope_span.scope {
                        let mut scope_obj = serde_json::Map::new();
                        scope_obj.insert("name".to_string(), serde_json::Value::String(scope.name));
                        scope_obj.insert("version".to_string(), serde_json::Value::String(scope.version));
                        ss_obj.insert("scope".to_string(), serde_json::Value::Object(scope_obj));
                    }
                    
                    ss_obj.insert("spans".to_string(), serde_json::Value::Array(vec![])); // Simplified - not parsing spans for now
                    scope_spans.push(serde_json::Value::Object(ss_obj));
                }
                
                rs_obj.insert("scopeSpans".to_string(), serde_json::Value::Array(scope_spans));
                resource_spans.push(serde_json::Value::Object(rs_obj));
            }
            
            json_obj.insert("resourceSpans".to_string(), serde_json::Value::Array(resource_spans));
            
            // Convert to pretty JSON string
            match serde_json::to_string_pretty(&serde_json::Value::Object(json_obj)) {
                Ok(json) => Ok(json),
                Err(e) => Err(format!("JSON serialization error: {}", e)),
            }
        }
        Err(e) => {
            // If protobuf decoding fails, return the first 500 chars as fallback
            let preview = if base64_payload.len() > 500 {
                format!("{}...", &base64_payload[..500])
            } else {
                base64_payload.to_string()
            };
            Err(format!("Protobuf decode error: {} - Raw data: {}", e, preview))
        }
    }
}

/// Convert decoded JSON to flattened key-value map for Arrow Flight
fn json_to_kv_map(json_str: &str) -> Result<std::collections::HashMap<String, String>, String> {
    let json_value: serde_json::Value = match serde_json::from_str(json_str) {
        Ok(v) => v,
        Err(e) => return Err(format!("JSON parse error: {}", e)),
    };
    
    let mut kv_map = std::collections::HashMap::new();
    
    // Flatten the JSON structure into key-value pairs
    flatten_json(&json_value, String::new(), &mut kv_map);
    
    Ok(kv_map)
}

/// Recursively flatten JSON into dot-notation key-value pairs with depth protection
fn flatten_json(value: &serde_json::Value, prefix: String, result: &mut std::collections::HashMap<String, String>) {
    flatten_json_with_depth(value, prefix, result, 0, 50); // Max depth of 50
}

fn flatten_json_with_depth(
    value: &serde_json::Value, 
    prefix: String, 
    result: &mut std::collections::HashMap<String, String>,
    depth: usize,
    max_depth: usize
) {
    // Prevent infinite recursion and stack overflow
    if depth > max_depth {
        result.insert(format!("{}_TRUNCATED_MAX_DEPTH", prefix), "exceeded_max_depth".to_string());
        return;
    }
    
    // Limit total field count to prevent memory issues
    if result.len() > 10000 {
        result.insert(format!("{}_TRUNCATED_MAX_FIELDS", prefix), "exceeded_max_fields".to_string());
        return;
    }

    match value {
        serde_json::Value::Object(obj) => {
            // Handle empty objects
            if obj.is_empty() {
                result.insert(prefix, "{}".to_string());
                return;
            }
            
            for (key, val) in obj {
                // Sanitize key names for storage compatibility
                let clean_key = sanitize_key(key);
                let new_key = if prefix.is_empty() {
                    clean_key
                } else {
                    format!("{}.{}", prefix, clean_key)
                };
                flatten_json_with_depth(val, new_key, result, depth + 1, max_depth);
            }
        }
        serde_json::Value::Array(arr) => {
            // Handle empty arrays
            if arr.is_empty() {
                result.insert(prefix, "[]".to_string());
                return;
            }
            
            // Add array length for querying
            result.insert(format!("{}_length", prefix), arr.len().to_string());
            
            // Limit array processing to prevent explosion
            let max_array_items = 1000;
            let items_to_process = arr.len().min(max_array_items);
            
            for (index, val) in arr.iter().enumerate().take(items_to_process) {
                let new_key = format!("{}[{}]", prefix, index);
                flatten_json_with_depth(val, new_key, result, depth + 1, max_depth);
            }
            
            if arr.len() > max_array_items {
                result.insert(format!("{}[TRUNCATED]", prefix), 
                    format!("array_truncated_at_{}_of_{}", max_array_items, arr.len()));
            }
        }
        serde_json::Value::String(s) => {
            // Handle very long strings
            if s.len() > 10000 {
                result.insert(prefix.clone(), format!("{}..._TRUNCATED", &s[..1000]));
                result.insert(format!("{}_full_length", prefix), s.len().to_string());
            } else {
                result.insert(prefix, s.clone());
            }
        }
        serde_json::Value::Number(n) => {
            result.insert(prefix, n.to_string());
        }
        serde_json::Value::Bool(b) => {
            result.insert(prefix, b.to_string());
        }
        serde_json::Value::Null => {
            result.insert(prefix, "null".to_string());
        }
    }
}

/// Sanitize key names for storage and query compatibility
fn sanitize_key(key: &str) -> String {
    key.chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '_' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

/// Extract key service information for indexing
fn extract_service_metadata(kv_map: &std::collections::HashMap<String, String>) -> std::collections::HashMap<String, String> {
    let mut metadata = std::collections::HashMap::new();
    
    // Extract common service identifiers
    for (key, value) in kv_map {
        if key.contains("service.name") {
            metadata.insert("service_name".to_string(), value.clone());
        } else if key.contains("app.version") {
            metadata.insert("app_version".to_string(), value.clone());
        } else if key.contains("project.name") {
            metadata.insert("project_name".to_string(), value.clone());
        } else if key.contains("telemetry.sdk.language") {
            metadata.insert("sdk_language".to_string(), value.clone());
        } else if key.contains("mw.account_key") || key.contains("account_key") || key.contains("tenant") || key.contains("customer") {
            metadata.insert("account_key".to_string(), value.clone());
            metadata.insert("tenant".to_string(), value.clone());
        } else if key.contains("mw.agent_id") {
            metadata.insert("agent_id".to_string(), value.clone());
        } else if key.contains("resource.fingerprint") {
            metadata.insert("resource_fingerprint".to_string(), value.clone());
        } else if key.contains("scope.name") {
            metadata.insert("scope_name".to_string(), value.clone());
        }
    }
    
    metadata
}

/// Create Arrow Flight ready record from KV map and message metadata
fn create_arrow_record(
    message_id: &str,
    kv_map: &std::collections::HashMap<String, String>,
    metadata: &std::collections::HashMap<String, String>,
    headers: &std::collections::HashMap<String, String>
) -> std::collections::HashMap<String, String> {
    let mut arrow_record = std::collections::HashMap::new();
    
    // Add message metadata
    arrow_record.insert("message_id".to_string(), message_id.to_string());
    arrow_record.insert("timestamp".to_string(), chrono::Utc::now().to_rfc3339());
    arrow_record.insert("ingestion_node".to_string(), std::env::var("HOSTNAME").unwrap_or_else(|_| "unknown".to_string()));
    
    // Add headers (from Kafka)
    for (key, value) in headers {
        arrow_record.insert(format!("header.{}", key), value.clone());
    }
    
    // Add service metadata (extracted key fields)
    for (key, value) in metadata {
        arrow_record.insert(key.clone(), value.clone());
    }
    
    // Add flattened telemetry data
    for (key, value) in kv_map {
        // Prefix telemetry fields to avoid conflicts
        arrow_record.insert(format!("telemetry.{}", key), value.clone());
    }
    
    arrow_record
}


/// Create Arrow RecordBatch from the KV map for Arrow Flight transfer
fn create_arrow_batch(arrow_record: &std::collections::HashMap<String, String>) -> Result<RecordBatch, String> {
    // Create dynamic schema based on the fields in the record
    let mut fields = Vec::new();
    let mut field_names = Vec::new();
    
    // Sort keys for consistent schema
    let mut sorted_keys: Vec<_> = arrow_record.keys().collect();
    sorted_keys.sort();
    
    for key in &sorted_keys {
        fields.push(Field::new(key.as_str(), DataType::Utf8, true));
        field_names.push(key.as_str());
    }
    
    let schema = Schema::new(fields);
    
    // Create arrays for each field
    let mut arrays: Vec<StdArc<dyn arrow::array::Array>> = Vec::new();
    
    for key in &sorted_keys {
        let default_value = String::new();
        let value = arrow_record.get(*key).unwrap_or(&default_value);
        let array = StringArray::from(vec![Some(value.as_str())]);
        arrays.push(StdArc::new(array));
    }
    
    // Create RecordBatch
    match RecordBatch::try_new(StdArc::new(schema), arrays) {
        Ok(batch) => Ok(batch),
        Err(e) => Err(format!("Failed to create Arrow batch: {}", e)),
    }
}

fn create_combined_arrow_batch(arrow_records: &[std::collections::HashMap<String, String>]) -> Result<RecordBatch, String> {
    if arrow_records.is_empty() {
        return Err("Cannot create batch from empty records".to_string());
    }
    
    // Collect all unique field names from all records
    let mut all_fields: std::collections::HashSet<String> = std::collections::HashSet::new();
    for record in arrow_records {
        for key in record.keys() {
            all_fields.insert(key.clone());
        }
    }
    
    // Sort field names for consistent schema
    let mut sorted_fields: Vec<_> = all_fields.into_iter().collect();
    sorted_fields.sort();
    
    // Create schema
    let fields: Vec<_> = sorted_fields.iter()
        .map(|name| Field::new(name.as_str(), DataType::Utf8, true))
        .collect();
    let schema = Schema::new(fields);
    
    // Create arrays for each field
    let mut arrays: Vec<StdArc<dyn arrow::array::Array>> = Vec::new();
    
    for field_name in &sorted_fields {
        let mut values = Vec::new();
        for record in arrow_records {
            let value = record.get(field_name).map(|s| s.as_str());
            values.push(value);
        }
        let array = StringArray::from(values);
        arrays.push(StdArc::new(array));
    }
    
    // Create RecordBatch
    match RecordBatch::try_new(StdArc::new(schema), arrays) {
        Ok(batch) => Ok(batch),
        Err(e) => Err(format!("Failed to create combined Arrow batch: {}", e)),
    }
}

/// Send Arrow Flight data to storage pod only
async fn send_batch_to_storage_via_arrow_flight(
    arrow_records: Vec<std::collections::HashMap<String, String>>,
    service_name: &str,
    account_key: &str,
    storage_endpoint: &str,
    node_id: &str,
    batch_id: &str,
    batch_metadata: std::collections::HashMap<String, String>,
) -> Result<(), String> {
    if arrow_records.is_empty() {
        return Ok(());
    }
    
    // Create combined Arrow RecordBatch from all records
    let batch = create_combined_arrow_batch(&arrow_records)?;
    
    // Create Arrow Flight client
    let arrow_client = openwit_inter_node::ArrowFlightClient::new(node_id.to_string());
    
    // Use original batch_id from Kafka (no timestamp suffix for true batching)
    let final_batch_id = format!("{};tenant={};signal=traces", batch_id, account_key);
    
    // Use the provided batch metadata
    let mut metadata = batch_metadata;
    metadata.insert("batch_id".to_string(), final_batch_id.clone());
    
    // Send to storage via Arrow Flight
    match arrow_client.send_batch(storage_endpoint, &final_batch_id, batch, metadata).await {
        Ok(_) => {
            info!("Successfully sent batch {} with {} records to storage at {}", final_batch_id, arrow_records.len(), storage_endpoint);
            Ok(())
        }
        Err(e) => {
            error!("Failed to send batch {} with {} records to storage: {}", final_batch_id, arrow_records.len(), e);
            Err(format!("Storage transfer failed: {}", e))
        }
    }
}

async fn send_to_storage_via_arrow_flight(
    arrow_record: &std::collections::HashMap<String, String>,
    service_name: &str,
    account_key: &str,
    storage_endpoint: &str,
    node_id: &str,
    batch_id: &str,
) -> Result<(), String> {
    // Create Arrow RecordBatch
    let batch = create_arrow_batch(arrow_record)?;
    
    // Create Arrow Flight client
    let arrow_client = openwit_inter_node::ArrowFlightClient::new(node_id.to_string());
    
    // Use batch_id from Kafka request (maintaining batch traceability)
    let message_batch_id = format!("{}-msg-{}", batch_id, chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0));
    
    // Create metadata for the batch
    let mut metadata = std::collections::HashMap::new();
    metadata.insert("service_name".to_string(), service_name.to_string());
    metadata.insert("account_key".to_string(), account_key.to_string());
    metadata.insert("source_node".to_string(), node_id.to_string());
    metadata.insert("tenant".to_string(), account_key.to_string());
    metadata.insert("signal".to_string(), "traces".to_string());
    metadata.insert("timestamp".to_string(), chrono::Utc::now().to_rfc3339());
    
    // Include all arrow_record fields in metadata (includes header.client_name from Kafka)
    for (key, value) in arrow_record {
        metadata.insert(key.clone(), value.clone());
    }
    
    // Send to storage via Arrow Flight
    match arrow_client.send_batch(storage_endpoint, &message_batch_id, batch, metadata).await {
        Ok(_) => {
            debug!("Successfully sent message batch {} (from Kafka batch {}) to storage at {}", message_batch_id, batch_id, storage_endpoint);
            Ok(())
        }
        Err(e) => {
            error!("Failed to send message batch {} (from Kafka batch {}) to storage: {}", message_batch_id, batch_id, e);
            Err(format!("Storage transfer failed: {}", e))
        }
    }
}


/// Send real-time processing updates to control plane for each message
async fn send_processing_update_to_control_plane(message_id: &str, field_count: usize) {
    // Create a simple processing status update
    let node_id = std::env::var("HOSTNAME").unwrap_or_else(|_| "unknown".to_string());
    
    // Note: In a real implementation, you would maintain a control plane client instance
    // For now, we'll send a status log that could be picked up by monitoring
    debug!("Processed: {}", message_id);
    
    // TODO: If you want actual gRPC calls to control plane, you would need to:
    // 1. Maintain a ControlPlaneClient instance in the service
    // 2. Create a new gRPC method for processing updates
    // 3. Call that method here with processing statistics
}

// Simplified imports - no buffer processing for now
// use crate::{
//     multi_threaded_buffer::MultiThreadedBuffer,
//     types::{IngestedMessage, MessageSource, MessagePayload},
// };

/// gRPC service implementation for receiving telemetry data from Kafka consumers
pub struct TelemetryIngestionGrpcService {
    node_id: String,
    /// Service discovery cache for storage endpoints
    storage_cache: Arc<Mutex<StorageEndpointCache>>,
    /// Control plane client for service discovery
    control_client: Option<ControlPlaneClient>,
    /// Configuration for fallback endpoints
    config: UnifiedConfig,
    /// Batch tracker for PostgreSQL updates
    batch_tracker: Option<Arc<SimpleBatchTracker>>,
    /// OTLP preserving receiver for processing messages
    otlp_receiver: Option<Arc<OtlpPreservingReceiver>>,
    /// Sender for Arrow Flight batches
    batch_sender: Option<mpsc::Sender<ArrowFlightBatch>>,
    // buffer: Arc<MultiThreadedBuffer>, // Removed for simplified testing
}

impl TelemetryIngestionGrpcService {
    pub fn new(node_id: String, config: UnifiedConfig) -> Self {
        info!("Creating simplified TelemetryIngestionGrpcService for node: {}", node_id);
        
        // Create a channel for OTLP batches to be sent to storage
        let (batch_tx, batch_rx) = mpsc::channel::<ArrowFlightBatch>(1000);
        let otlp_receiver = Arc::new(OtlpPreservingReceiver::new(node_id.clone(), batch_tx.clone()));
        
        let storage_cache = Arc::new(Mutex::new(StorageEndpointCache::new()));
        
        // Spawn a task to forward batches from OTLP receiver to storage
        Self::spawn_batch_forwarder(
            node_id.clone(),
            batch_rx,
            storage_cache.clone(),
            None, // No control client
            config.clone(),
        );
        
        Self {
            node_id,
            storage_cache,
            control_client: None,
            config,
            batch_tracker: None,
            otlp_receiver: Some(otlp_receiver),
            batch_sender: Some(batch_tx),
            // buffer, // Removed for simplified testing
        }
    }
    
    pub fn new_with_control_plane(node_id: String, control_client: ControlPlaneClient, config: UnifiedConfig) -> Self {
        info!("Creating TelemetryIngestionGrpcService with control plane for node: {}", node_id);
        
        // Create a channel for OTLP batches to be sent to storage
        let (batch_tx, batch_rx) = mpsc::channel::<ArrowFlightBatch>(1000);
        let otlp_receiver = Arc::new(OtlpPreservingReceiver::new(node_id.clone(), batch_tx.clone()));
        
        let storage_cache = Arc::new(Mutex::new(StorageEndpointCache::new()));
        
        // Spawn a task to forward batches from OTLP receiver to storage
        Self::spawn_batch_forwarder(
            node_id.clone(),
            batch_rx,
            storage_cache.clone(),
            Some(control_client.clone()),
            config.clone(),
        );
        
        Self {
            node_id,
            storage_cache,
            control_client: Some(control_client),
            config,
            batch_tracker: None,
            otlp_receiver: Some(otlp_receiver),
            batch_sender: Some(batch_tx),
            // buffer, // Removed for simplified testing
        }
    }
    
    /// Create with BatchTracker for PostgreSQL integration
    pub fn new_with_batch_tracker(
        node_id: String, 
        control_client: ControlPlaneClient, 
        config: UnifiedConfig,
        batch_tracker: Arc<SimpleBatchTracker>
    ) -> Self {
        info!("Creating TelemetryIngestionGrpcService with batch tracking for node: {}", node_id);
        
        // Create a channel for OTLP batches to be sent to storage
        let (batch_tx, batch_rx) = mpsc::channel::<ArrowFlightBatch>(1000);
        let otlp_receiver = Arc::new(OtlpPreservingReceiver::new(node_id.clone(), batch_tx.clone()));
        
        let storage_cache = Arc::new(Mutex::new(StorageEndpointCache::new()));
        
        // Spawn a task to forward batches from OTLP receiver to storage
        Self::spawn_batch_forwarder(
            node_id.clone(),
            batch_rx,
            storage_cache.clone(),
            Some(control_client.clone()),
            config.clone(),
        );
        
        Self {
            node_id,
            storage_cache,
            control_client: Some(control_client),
            config,
            batch_tracker: Some(batch_tracker),
            otlp_receiver: Some(otlp_receiver),
            batch_sender: Some(batch_tx),
            // buffer, // Removed for simplified testing
        }
    }
    
    /// Spawn a task to forward batches from OTLP receiver to storage
    fn spawn_batch_forwarder(
        node_id: String,
        mut batch_rx: mpsc::Receiver<ArrowFlightBatch>,
        storage_cache: Arc<Mutex<StorageEndpointCache>>,
        control_client: Option<ControlPlaneClient>,
        config: UnifiedConfig,
    ) {
        tokio::spawn(async move {
            let arrow_client = openwit_inter_node::ArrowFlightClient::new(node_id.clone());
            info!("Batch forwarder started for node {}", node_id);
            
            while let Some(batch) = batch_rx.recv().await {
                let batch_id = batch.batch_id.clone();
                
                // Get storage endpoint
                let storage_endpoint = match Self::get_storage_endpoint_static(
                    &storage_cache,
                    &control_client,
                    &config
                ).await {
                    Ok(endpoint) => endpoint,
                    Err(e) => {
                        // Silently discard data when no storage is available
                        if e.contains("No healthy storage nodes found") {
                            debug!("No storage nodes available, discarding batch {}", batch_id);
                        } else {
                            warn!("Failed to get storage endpoint for batch {}: {}", batch_id, e);
                        }
                        continue;
                    }
                };
                
                // Send batch to storage
                match arrow_client.send_batch(
                    &storage_endpoint,
                    &batch.batch_id,
                    batch.record_batch,
                    batch.metadata
                ).await {
                    Ok(_) => {
                        debug!("Successfully forwarded batch {} to storage at {}", batch_id, storage_endpoint);
                    }
                    Err(e) => {
                        error!("Failed to forward batch {} to storage at {}: {}", batch_id, storage_endpoint, e);
                    }
                }
            }
            warn!("Batch forwarder task ended");
        });
    }
    
    /// Static version of get_storage_endpoint for use in spawned tasks
    async fn get_storage_endpoint_static(
        storage_cache: &Arc<Mutex<StorageEndpointCache>>,
        control_client: &Option<ControlPlaneClient>,
        config: &UnifiedConfig,
    ) -> Result<String, String> {
        let mut cache = storage_cache.lock().await;
        
        // Check if cached endpoint is still valid
        if let Some(ref endpoint) = cache.endpoint {
            if !cache.is_expired() {
                return Ok(endpoint.clone());
            }
        }
        
        // Need to get fresh endpoint from control plane
        if let Some(ref client) = control_client {
            let mut client = client.clone();
            match client.get_healthy_nodes("storage").await {
                Ok(nodes) if !nodes.is_empty() => {
                    // Select node with best health score
                    let best_node = nodes.into_iter()
                        .max_by(|a, b| a.health_score.partial_cmp(&b.health_score).unwrap_or(std::cmp::Ordering::Equal))
                        .unwrap();
                    
                    let endpoint = best_node.arrow_flight_endpoint;
                    cache.update(endpoint.clone());
                    Ok(endpoint)
                }
                Ok(_) => Err("No healthy storage nodes found".to_string()),
                Err(e) => Err(format!("Failed to get storage nodes from control plane: {}", e)),
            }
        } else {
            // Fallback to configured storage endpoint
            let bind_addr = &config.storage_node.flight.bind_addr;
            let endpoint = if bind_addr.starts_with("http://") || bind_addr.starts_with("https://") {
                bind_addr.clone()
            } else if bind_addr.starts_with("0.0.0.0:") {
                // Replace 0.0.0.0 with localhost for client connections
                format!("http://localhost:{}", bind_addr.strip_prefix("0.0.0.0:").unwrap())
            } else {
                format!("http://{}", bind_addr)
            };
            
            cache.update(endpoint.clone());
            Ok(endpoint)
        }
    }
    
    /// Get storage endpoint from control plane with caching
    async fn get_storage_endpoint(&self) -> Result<String, String> {
        let mut cache = self.storage_cache.lock().await;
        
        // Check if cached endpoint is still valid
        if let Some(ref endpoint) = cache.endpoint {
            if !cache.is_expired() {
                return Ok(endpoint.clone());
            }
        }
        
        // Need to get fresh endpoint from control plane
        if let Some(ref client) = self.control_client {
            let mut client = client.clone();
            match client.get_healthy_nodes("storage").await {
                Ok(nodes) if !nodes.is_empty() => {
                    // Select node with best health score
                    let best_node = nodes.into_iter()
                        .max_by(|a, b| a.health_score.partial_cmp(&b.health_score).unwrap_or(std::cmp::Ordering::Equal))
                        .unwrap();
                    
                    let endpoint = best_node.arrow_flight_endpoint;
                    cache.update(endpoint.clone());
                    Ok(endpoint)
                }
                Ok(_) => Err("No healthy storage nodes found".to_string()),
                Err(e) => Err(format!("Failed to get storage nodes from control plane: {}", e)),
            }
        } else {
            // Fallback to configured storage endpoint
            let bind_addr = &self.config.storage_node.flight.bind_addr;
            let endpoint = if bind_addr.starts_with("http://") || bind_addr.starts_with("https://") {
                bind_addr.clone()
            } else if bind_addr.starts_with("0.0.0.0:") {
                // Replace 0.0.0.0 with localhost for client connections
                format!("http://localhost:{}", bind_addr.strip_prefix("0.0.0.0:").unwrap())
            } else {
                format!("http://{}", bind_addr)
            };
            
            info!("Control plane unavailable, using configured storage endpoint: {}", endpoint);
            cache.update(endpoint.clone());
            Ok(endpoint)
        }
    }
    
    /// Create a tonic server for this service
    pub fn into_server(self) -> TelemetryIngestionServiceServer<Self> {
        TelemetryIngestionServiceServer::new(self)
    }
}

#[tonic::async_trait]
impl TelemetryIngestionService for TelemetryIngestionGrpcService {
    /// Handle batch telemetry ingestion using OTLP format
    async fn ingest_telemetry_batch(
        &self,
        request: Request<TelemetryIngestRequest>,
    ) -> Result<Response<TelemetryIngestResponse>, Status> {
        let req = request.into_inner();
        
        debug!("[INGESTION] Processing batch {} from {} with {} messages", 
              req.batch_id, req.source_node_id, req.messages.len());
        
        let mut accepted_count = 0;
        let mut rejected_count = 0;
        let mut rejected_ids = Vec::new();
        let batch_start = std::time::Instant::now();
        
        // Get OTLP receiver
        let otlp_receiver = match &self.otlp_receiver {
            Some(receiver) => receiver.clone(),
            None => {
                return Err(Status::internal("OTLP receiver not initialized"));
            }
        };
        
        // Get batch sender for direct storage communication
        let batch_sender = match &self.batch_sender {
            Some(sender) => sender.clone(),
            None => {
                return Err(Status::internal("Batch sender not initialized"));
            }
        };
        
        // Create a receiver to collect processed batches
        let (result_tx, result_rx) = mpsc::channel::<Result<ArrowFlightBatch, String>>(100);
        
        // Process each message using OTLP preserving receiver
        let processing_start = std::time::Instant::now();
        
        for (msg_idx, telemetry_data) in req.messages.iter().enumerate() {
            let id = telemetry_data.id.clone();
            let payload = telemetry_data.payload_json.clone();
            let headers = telemetry_data.headers.clone();
            
            // The payload_json field contains the raw data from Kafka
            // Despite its name, it can contain:
            // 1. Raw protobuf binary data (most common for traces)
            // 2. JSON string
            // 3. Base64-encoded data (less common)
            
            // First check if it looks like protobuf (starts with 0x0A which is field 1, wire type 2)
            let decoded_payload = if payload.starts_with("\n") || payload.as_bytes().first() == Some(&0x0A) {
                // This is likely raw protobuf data
                debug!("Detected raw protobuf data for message {}", id);
                payload.as_bytes().to_vec()
            } else if let Ok(bytes) = base64::engine::general_purpose::STANDARD.decode(&payload) {
                // Successfully decoded as base64
                debug!("Successfully decoded base64 for message {}", id);
                bytes
            } else {
                // Assume it's raw data (JSON or binary)
                debug!("Using payload as-is for message {}", id);
                payload.as_bytes().to_vec()
            };
            
            // Only log first message or errors
            if msg_idx == 0 {
                debug!("[INGESTION] Processing first message: id={}, size={} bytes", id, decoded_payload.len());
            }
            
            // Process using OTLP preserving receiver
            match otlp_receiver.process_message(&id, &decoded_payload, headers).await {
                Ok(_) => {
                    accepted_count += 1;
                    debug!("Successfully processed OTLP message {}", id);
                }
                Err(e) => {
                    rejected_count += 1;
                    rejected_ids.push(id.clone());
                    error!("Failed to process OTLP message {}: {}", id, e);
                }
            }
        }
        
        let processing_duration = processing_start.elapsed();
        
        // Only log if processing took a long time or had rejections
        if processing_duration.as_secs() > 5 || rejected_count > 0 {
            info!("[INGESTION] Batch {} processed: {} accepted, {} rejected in {:?}",
                  req.batch_id, accepted_count, rejected_count, processing_duration);
        } else {
            debug!("[INGESTION] Batch {} processed: {} messages in {:?}",
                   req.batch_id, accepted_count, processing_duration);
        }
        
        // Now we need to forward the OTLP batches to storage
        // The OTLP receiver has already sent batches through the batch_sender channel
        // We need to set up a task to forward those batches to storage
        
        // Check if storage is available, but don't fail if it isn't
        match self.get_storage_endpoint().await {
            Ok(storage_endpoint) => {
                // The batches have already been sent to storage by the OTLP receiver
                // No need to forward them again - the OTLP receiver sends directly to storage
                debug!("[INGESTION] OTLP batches forwarded to storage at {}", storage_endpoint);
            }
            Err(e) => {
                if e.contains("No healthy storage nodes found") {
                    // Silently accept the data - it will be discarded
                    debug!("[INGESTION] No storage nodes available, data accepted but discarded for batch {}", req.batch_id);
                    // Still mark as successful - we processed the data even if we couldn't store it
                } else {
                    // For other errors, still log them but don't fail the request
                    warn!("[INGESTION] Failed to get storage endpoint: {}. Data accepted but may not be stored.", e);
                }
            }
        }
        
        let batch_duration = batch_start.elapsed();
        
        // Summary log only for large batches
        if req.messages.len() > 100 {
            info!("[INGESTION] Large batch {} completed: {} messages ({} accepted) in {:?}",
                  req.batch_id, req.messages.len(), accepted_count, batch_duration);
        }
        
        // Update PostgreSQL batch tracking if available
        if let Some(ref tracker) = self.batch_tracker {
            // Parse batch_id as UUID (it should be in UUID format from Kafka)
            if let Ok(batch_uuid) = uuid::Uuid::parse_str(&req.batch_id) {
                if let Err(e) = tracker.update_ingestion_completed(batch_uuid).await {
                    error!("Failed to update ingestion completion status in PostgreSQL for batch {}: {}", req.batch_id, e);
                } else {
                    debug!("[INGESTION] Updated PostgreSQL for batch {}", req.batch_id);
                }
            } else {
                warn!("Batch ID {} is not a valid UUID, skipping PostgreSQL update", req.batch_id);
            }
        }
        
        let response = TelemetryIngestResponse {
            success: rejected_count == 0,
            message: format!("Batch {} processed: {} accepted, {} rejected using OTLP format",
                           req.batch_id, accepted_count, rejected_count),
            accepted_count: accepted_count as i32,
            rejected_count: rejected_count as i32,
            rejected_ids,
            wal_written: false,  // TODO: Implement WAL writing
            storage_written: false,
            wal_path: String::new(),
            wal_write_time_ms: 0,
        };
        
        Ok(Response::new(response))
    }
    
    /// Handle streaming telemetry ingestion
    type IngestTelemetryStreamStream = Pin<Box<dyn Stream<Item = Result<TelemetryIngestStatus, Status>> + Send>>;
    
    async fn ingest_telemetry_stream(
        &self,
        request: Request<Streaming<TelemetryData>>,
    ) -> Result<Response<Self::IngestTelemetryStreamStream>, Status> {
        let mut stream = request.into_inner();
        let node_id = self.node_id.clone();
        
        info!("INGESTION: Starting stream ingestion for {}", node_id);
        
        let (tx, rx) = mpsc::channel::<Result<TelemetryIngestStatus, Status>>(100);
        
        // Get service references for stream processing
        let node_id_clone = node_id.clone();
        let storage_cache_clone = self.storage_cache.clone();
        let control_client_clone = self.control_client.clone();
        let config_clone = self.config.clone();
        
        // Spawn task to process incoming stream
        tokio::spawn(async move {
            let mut message_count = 0;
            let mut total_fields = 0;
            let stream_start = std::time::Instant::now();
            
            while let Some(result) = tokio_stream::StreamExt::next(&mut stream).await {
                match result {
                    Ok(telemetry_data) => {
                        message_count += 1;
                        let message_id = telemetry_data.id.clone();
                        let payload = telemetry_data.payload_json.clone();
                        let headers = telemetry_data.headers.clone();
                        
                        info!("INGESTION STREAM: Processing message {}", message_id);
                        
                        // Real processing with OTLP decoding and Arrow Flight transfer
                        let json_result = decode_otlp_to_json(&payload);
                        
                        let field_count = match json_result {
                            Ok(json_str) => {
                                // Convert to Arrow-compatible format
                                match json_to_kv_map(&json_str) {
                                    Ok(kv_map) => {
                                        let field_count = kv_map.len();
                                        
                                        // Extract service metadata
                                        let metadata = extract_service_metadata(&kv_map);
                                        
                                        // Debug: Log some sample fields to understand the data structure
                                        let sample_fields: Vec<String> = kv_map.keys().take(10).cloned().collect();
                                        debug!("INGESTION STREAM: Sample telemetry fields: {:?}", sample_fields);
                                        debug!("INGESTION STREAM: Extracted metadata: {:?}", metadata);
                                        
                                        let service_name = metadata.get("service_name").cloned().unwrap_or_else(|| "unknown".to_string());
                                        let account_key = metadata.get("account_key").cloned().unwrap_or_else(|| "default".to_string());
                                        
                                        // Create Arrow record
                                        let arrow_record = create_arrow_record(&message_id, &kv_map, &metadata, &headers);
                                        
                                        // Get storage endpoint and send via Arrow Flight
                                        let mut cache = storage_cache_clone.lock().await;
                                        
                                        // Check if cached endpoint is still valid
                                        let storage_endpoint = if let Some(ref endpoint) = cache.endpoint {
                                            if !cache.is_expired() {
                                                Ok(endpoint.clone())
                                            } else {
                                                // Need fresh endpoint
                                                if let Some(ref client) = control_client_clone {
                                                    let mut client = client.clone();
                                                    match client.get_healthy_nodes("storage").await {
                                                        Ok(nodes) if !nodes.is_empty() => {
                                                            let best_node = nodes.into_iter()
                                                                .max_by(|a, b| a.health_score.partial_cmp(&b.health_score).unwrap_or(std::cmp::Ordering::Equal))
                                                                .unwrap();
                                                            let endpoint = best_node.arrow_flight_endpoint;
                                                            cache.update(endpoint.clone());
                                                            Ok(endpoint)
                                                        }
                                                        Ok(_) => Err("No healthy storage nodes found".to_string()),
                                                        Err(e) => Err(format!("Failed to get storage nodes: {}", e)),
                                                    }
                                                } else {
                                                    let bind_addr = &config_clone.storage_node.flight.bind_addr;
                                                    let endpoint = if bind_addr.starts_with("0.0.0.0:") {
                                                        format!("http://localhost:{}", bind_addr.strip_prefix("0.0.0.0:").unwrap())
                                                    } else {
                                                        format!("http://{}", bind_addr)
                                                    };
                                                    cache.update(endpoint.clone());
                                                    Ok(endpoint)
                                                }
                                            }
                                        } else {
                                            // No cached endpoint
                                            if let Some(ref client) = control_client_clone {
                                                let mut client = client.clone();
                                                match client.get_healthy_nodes("storage").await {
                                                    Ok(nodes) if !nodes.is_empty() => {
                                                        let best_node = nodes.into_iter()
                                                            .max_by(|a, b| a.health_score.partial_cmp(&b.health_score).unwrap_or(std::cmp::Ordering::Equal))
                                                            .unwrap();
                                                        let endpoint = best_node.arrow_flight_endpoint;
                                                        cache.update(endpoint.clone());
                                                        Ok(endpoint)
                                                    }
                                                    Ok(_) => Err("No healthy storage nodes found".to_string()),
                                                    Err(e) => Err(format!("Failed to get storage nodes: {}", e)),
                                                }
                                            } else {
                                                let bind_addr = &config_clone.storage_node.flight.bind_addr;
                                                let endpoint = if bind_addr.starts_with("0.0.0.0:") {
                                                    format!("http://localhost:{}", bind_addr.strip_prefix("0.0.0.0:").unwrap())
                                                } else {
                                                    format!("http://{}", bind_addr)
                                                };
                                                cache.update(endpoint.clone());
                                                Ok(endpoint)
                                            }
                                        };
                                        
                                        drop(cache); // Release lock
                                        
                                        match storage_endpoint {
                                            Ok(endpoint) => {
                                                info!("INGESTION STREAM: Using storage endpoint {} for message {}", endpoint, message_id);
                                                // Generate batch_id for streaming mode (individual message)
                                                let stream_batch_id = format!("stream-{}", message_id);
                                                match send_to_storage_via_arrow_flight(
                                                    &arrow_record,
                                                    &service_name,
                                                    &account_key,
                                                    &endpoint,
                                                    &node_id_clone,
                                                    &stream_batch_id,
                                                ).await {
                                                    Ok(_) => {
                                                        info!("INGESTION STREAM: Successfully sent message {} to storage", message_id);
                                                    }
                                                    Err(e) => {
                                                        error!("INGESTION STREAM: Failed to send message {} to storage: {}", message_id, e);
                                                    }
                                                }
                                            }
                                            Err(e) => {
                                                // Silently discard when no storage is available
                                                if e.contains("No healthy storage nodes found") {
                                                    debug!("No storage nodes available, discarding message {}", message_id);
                                                } else {
                                                    warn!("Failed to get storage endpoint for message {}: {}", message_id, e);
                                                }
                                            }
                                        }
                                        
                                        field_count
                                    }
                                    Err(e) => {
                                        error!("INGESTION STREAM: Failed to convert JSON to KV map for message {}: {}", message_id, e);
                                        0
                                    }
                                }
                            }
                            Err(e) => {
                                error!("INGESTION STREAM: Failed to decode OTLP payload for message {}: {}", message_id, e);
                                0
                            }
                        };
                        
                        total_fields += field_count;
                        
                        // Send real-time processing update immediately
                        send_processing_update_to_control_plane(&message_id, field_count).await;
                        
                        // Simplified processing - just print and acknowledge
                        let status = TelemetryIngestStatus {
                            message_id: message_id.clone(),
                            accepted: true,
                            error: String::new(),
                        };
                        
                        // Send status back
                        if tx.send(Ok(status)).await.is_err() {
                            println!("Client disconnected from stream");
                            break;
                        }
                    }
                    Err(e) => {
                        println!("Stream error: {:?}", e);
                        let _ = tx.send(Err(Status::internal(format!("Stream error: {}", e)))).await;
                        break;
                    }
                }
            }
            
            let stream_duration = stream_start.elapsed();
            let avg_fields_per_msg = if message_count > 0 { total_fields / message_count } else { 0 };
            
            info!("Stream completed: {} msgs in {:?}", message_count, stream_duration);
        });
        
        let output_stream = ReceiverStream::new(rx);
        Ok(Response::new(Box::pin(output_stream) as Self::IngestTelemetryStreamStream))
    }
}

// Simplified implementation - all processing methods removed for testing