use anyhow::Result;
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tonic::transport::Channel;
use tracing::{debug, error, info, warn};

use openwit_config::UnifiedConfig;
use openwit_proto::ingestion::{
    telemetry_ingestion_service_client::TelemetryIngestionServiceClient,
    TelemetryData,
    TelemetryIngestRequest,
};

/// Client for sending data from HTTP nodes to Ingestion nodes via gRPC
#[allow(dead_code)]
#[derive(Clone)]
pub struct IngestionClient {
    client: Arc<RwLock<Option<TelemetryIngestionServiceClient<Channel>>>>,
    endpoint: String,
    node_id: String,
    use_control_plane: bool,
    control_plane_endpoint: Option<String>,
    config: Option<Arc<UnifiedConfig>>,
}

impl IngestionClient {
    /// Create a new ingestion client
    pub async fn new(node_id: String, endpoint: String) -> Result<Self> {
        info!("Creating ingestion client to endpoint: {}", endpoint);
        
        // Don't connect immediately - will connect on first use
        Ok(Self {
            client: Arc::new(RwLock::new(None)),
            endpoint,
            node_id,
            use_control_plane: false,
            control_plane_endpoint: None,
            config: None,
        })
    }
    
    /// Create a new ingestion client that uses control plane for discovery
    pub async fn new_with_control_plane(
        node_id: String, 
        control_plane_endpoint: String,
        config: Arc<UnifiedConfig>
    ) -> Result<Self> {
        info!("Creating ingestion client with control plane discovery: {}", control_plane_endpoint);
        
        Ok(Self {
            client: Arc::new(RwLock::new(None)),
            endpoint: String::new(), // Will be discovered
            node_id,
            use_control_plane: true,
            control_plane_endpoint: Some(control_plane_endpoint),
            config: Some(config),
        })
    }
    
    /// Connect to the ingestion service
    async fn connect(endpoint: &str) -> Result<TelemetryIngestionServiceClient<Channel>> {
        let channel = Channel::from_shared(endpoint.to_string())?
            .connect()
            .await?;
        
        Ok(TelemetryIngestionServiceClient::new(channel))
    }
    
    /// Ensure we have a connected client
    async fn ensure_connected(&self) -> Result<()> {
        let mut client_guard = self.client.write().await;
        
        if client_guard.is_some() {
            return Ok(());
        }
        
        // Get endpoint - either static or from control plane
        let endpoint = if self.use_control_plane {
            info!("Using control plane discovery for ingestion endpoint");
            match self.discover_ingestion_endpoint().await {
                Ok(ep) => {
                    info!("Discovered ingestion endpoint: {}", ep);
                    ep
                },
                Err(e) => {
                    error!("Failed to discover ingestion endpoint: {}", e);
                    return Err(anyhow::anyhow!("Failed to discover ingestion endpoint: {}", e));
                }
            }
        } else {
            info!("Using static ingestion endpoint: {}", self.endpoint);
            self.endpoint.clone()
        };
        
        info!("Connecting to ingestion service at {}", endpoint);
        match Self::connect(&endpoint).await {
            Ok(c) => {
                *client_guard = Some(c);
                info!("Successfully connected to ingestion service at {}", endpoint);
                Ok(())
            }
            Err(e) => {
                error!("Failed to connect to ingestion service at {}: {}", endpoint, e);
                Err(anyhow::anyhow!("Failed to connect to ingestion service at {}: {}", endpoint, e))
            }
        }
    }
    
    /// Discover ingestion endpoint from control plane
    async fn discover_ingestion_endpoint(&self) -> Result<String> {

        let config = self.config.as_ref()
            .ok_or_else(|| anyhow::anyhow!("Config not provided for control plane client"))?;
        
        // Try service registry first as it's more reliable
        match self.discover_from_service_registry().await {
            Ok(endpoint) => return Ok(endpoint),
            Err(e) => {
                warn!("Service registry discovery failed: {}, trying control plane", e);
            }
        }
        
        // Fall back to control plane
        self.try_control_plane_discovery(config).await
    }
    
    /// Try to discover from control plane
    async fn try_control_plane_discovery(&self, config: &UnifiedConfig) -> Result<String> {
        use openwit_control_plane::ControlPlaneClient;
        
        // Create a temporary control plane client
        let mut control_client = ControlPlaneClient::new(
            &self.node_id,
            config
        ).await?;
        
        // Get healthy ingestion nodes
        let nodes = control_client.get_healthy_nodes("ingest").await?;
        
        if nodes.is_empty() {
            return Err(anyhow::anyhow!("No healthy ingestion nodes found"));
        }
        
        // Pick the first healthy node
        // TODO: Implement better load balancing
        let node = &nodes[0];
        info!("Selected ingestion node: {} at {}", node.node_id, node.grpc_endpoint);
        
        Ok(node.grpc_endpoint.clone())
    }
    
    /// Discover ingestion endpoint from service registry
    async fn discover_from_service_registry(&self) -> Result<String> {
        use std::path::PathBuf;
        use serde::{Deserialize, Serialize};
        
        #[derive(Debug, Deserialize, Serialize)]
        struct ServiceInfo {
            node_id: String,
            service_port: u16,
            endpoint: String,
            #[serde(default)]
            arrow_flight_port: u16,
        }
        
        #[derive(Debug, Deserialize, Serialize)]
        struct ServiceRegistry {
            services: std::collections::HashMap<String, Vec<ServiceInfo>>,
        }
        
        let registry_path = PathBuf::from("./data/.openwit_services.json");
        
        if !registry_path.exists() {
            debug!("Service registry file not found at {:?}", registry_path);
            return Err(anyhow::anyhow!("Service registry file not found"));
        }
        
        let content = tokio::fs::read_to_string(&registry_path).await?;
        let registry: ServiceRegistry = serde_json::from_str(&content)?;
        
        info!("Loaded service registry with {} service types", registry.services.len());
        
        let ingest_services = registry.services.get("ingest")
            .ok_or_else(|| anyhow::anyhow!("No ingestion services in registry"))?;
        
        if ingest_services.is_empty() {
            return Err(anyhow::anyhow!("No ingestion services registered"));
        }
        
        info!("Found {} ingestion services in registry", ingest_services.len());
        
        // Try to find an active service by attempting to connect
        let mut last_error = None;
        
        for (_idx, service) in ingest_services.iter().enumerate() {
            // Use http:// format for gRPC (tonic requires it)
            let endpoint = format!("http://localhost:{}", service.service_port);
            
            // Quick connectivity check
            match tokio::time::timeout(
                Duration::from_secs(1),
                Self::connect(&endpoint)
            ).await {
                Ok(Ok(_)) => {
                    info!("Selected active ingestion service from registry: {} at {}", service.node_id, endpoint);
                    return Ok(endpoint);
                }
                Ok(Err(e)) => {
                    debug!("Ingestion service {} at {} failed to connect: {}", service.node_id, endpoint, e);
                    last_error = Some(e);
                }
                Err(_) => {
                    debug!("Ingestion service {} at {} timed out", service.node_id, endpoint);
                }
            }
        }
        
        // If no active services found, return error
        Err(anyhow::anyhow!(
            "No active ingestion services found in registry. Last error: {:?}",
            last_error
        ))
    }
    
    /// Send traces to ingestion node
    pub async fn send_traces(&self, traces: Value) -> Result<()> {
        self.ensure_connected().await?;
        
        // Convert OTLP traces to Telemetry data format
        let messages = self.convert_traces_to_messages(traces)?;
        
        if messages.is_empty() {
            return Ok(());
        }
        
        let batch_id = uuid::Uuid::new_v4().to_string();
        let request = TelemetryIngestRequest {
            source_node_id: self.node_id.clone(),
            batch_id: batch_id.clone(),
            messages,
            client_id: format!("http-{}", self.node_id),  // HTTP client identifier
            offsets: vec![],  // No offsets for HTTP ingestion
        };
        
        // Send to ingestion node
        let mut client_guard = self.client.write().await;
        if let Some(client) = client_guard.as_mut() {
            match client.ingest_telemetry_batch(request).await {
                Ok(response) => {
                    let resp = response.into_inner();
                    if resp.success {
                        debug!("Successfully sent {} traces to ingestion node", resp.accepted_count);
                    } else {
                        warn!("Ingestion node rejected some traces: {}", resp.message);
                    }
                }
                Err(e) => {
                    error!("Failed to send traces to ingestion node: {}", e);
                    // Mark client as disconnected so we reconnect next time
                    *client_guard = None;
                    return Err(anyhow::anyhow!("gRPC error: {}", e));
                }
            }
        }
        
        Ok(())
    }
    
    /// Send logs to ingestion node
    pub async fn send_logs(&self, logs: Value) -> Result<()> {
        // Try to connect with retry if needed
        let mut retry_count = 0;
        const MAX_RETRIES: u32 = 3;
        
        while retry_count < MAX_RETRIES {
            match self.ensure_connected().await {
                Ok(_) => break,
                Err(e) => {
                    retry_count += 1;
                    if retry_count >= MAX_RETRIES {
                        return Err(anyhow::anyhow!("Failed to connect to ingestion service after {} retries: {}", MAX_RETRIES, e));
                    }
                    warn!("Failed to connect to ingestion service: {} - Retrying... ({}/{})", e, retry_count, MAX_RETRIES);
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    // Clear the client to force reconnection
                    let mut client_guard = self.client.write().await;
                    *client_guard = None;
                    drop(client_guard);
                }
            }
        }
        
        // Convert OTLP logs to Telemetry data format
        let messages = self.convert_logs_to_messages(logs)?;
        
        if messages.is_empty() {
            return Ok(());
        }
        
        let batch_id = uuid::Uuid::new_v4().to_string();
        let request = TelemetryIngestRequest {
            source_node_id: self.node_id.clone(),
            batch_id: batch_id.clone(),
            messages,
            client_id: format!("http-{}", self.node_id),  // HTTP client identifier
            offsets: vec![],  // No offsets for HTTP ingestion
        };
        
        // Send to ingestion node
        let mut client_guard = self.client.write().await;
        if let Some(client) = client_guard.as_mut() {
            match client.ingest_telemetry_batch(request).await {
                Ok(response) => {
                    let resp = response.into_inner();
                    if resp.success {
                        debug!("Successfully sent {} logs to ingestion node", resp.accepted_count);
                    } else {
                        warn!("Ingestion node rejected some logs: {}", resp.message);
                    }
                }
                Err(e) => {
                    error!("Failed to send logs to ingestion node: {}", e);
                    *client_guard = None;
                    return Err(anyhow::anyhow!("gRPC error: {}", e));
                }
            }
        }
        
        Ok(())
    }
    
    /// Send metrics to ingestion node
    pub async fn send_metrics(&self, metrics: Value) -> Result<()> {
        self.ensure_connected().await?;
        
        // Convert OTLP metrics to Telemetry data format
        let messages = self.convert_metrics_to_messages(metrics)?;
        
        if messages.is_empty() {
            return Ok(());
        }
        
        let batch_id = uuid::Uuid::new_v4().to_string();
        let request = TelemetryIngestRequest {
            source_node_id: self.node_id.clone(),
            batch_id: batch_id.clone(),
            messages,
            client_id: format!("http-{}", self.node_id),  // HTTP client identifier
            offsets: vec![],  // No offsets for HTTP ingestion
        };
        
        // Send to ingestion node
        let mut client_guard = self.client.write().await;
        if let Some(client) = client_guard.as_mut() {
            match client.ingest_telemetry_batch(request).await {
                Ok(response) => {
                    let resp = response.into_inner();
                    if resp.success {
                        debug!("Successfully sent {} metrics to ingestion node", resp.accepted_count);
                    } else {
                        warn!("Ingestion node rejected some metrics: {}", resp.message);
                    }
                }
                Err(e) => {
                    error!("Failed to send metrics to ingestion node: {}", e);
                    *client_guard = None;
                    return Err(anyhow::anyhow!("gRPC error: {}", e));
                }
            }
        }
        
        Ok(())
    }
    
    /// Convert OTLP traces to Telemetry data format
    fn convert_traces_to_messages(&self, traces: Value) -> Result<Vec<TelemetryData>> {
        use opentelemetry_proto::tonic::trace::v1::TracesData;
        use base64::{engine::general_purpose, Engine as _};
        
        let mut messages = Vec::new();
        
        // Extract custom index_name if present at the top level
        let index_name = traces.get("index_name")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        
        // Convert JSON to protobuf TracesData
        TracesData {
            resource_spans: Vec::new(),
        };
        
        if let Some(resource_spans) = traces.get("resourceSpans").and_then(|rs| rs.as_array()) {
            for resource_span_json in resource_spans {
                // Extract index_name from resource attributes if not already set
                let resource_index_name = if index_name.is_none() {
                    resource_span_json.get("resource")
                        .and_then(|r| r.get("attributes"))
                        .and_then(|attrs| attrs.as_array())
                        .and_then(|attrs| {
                            attrs.iter().find(|attr| {
                                attr.get("key").and_then(|k| k.as_str()) == Some("index.name") ||
                                attr.get("key").and_then(|k| k.as_str()) == Some("service.namespace") ||
                                attr.get("key").and_then(|k| k.as_str()) == Some("customer.name")
                            })
                        })
                        .and_then(|attr| attr.get("value"))
                        .and_then(|v| v.get("stringValue"))
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string())
                } else {
                    None
                };
                
                // Use the found index_name or default to service name
                let final_index_name = index_name.clone()
                    .or(resource_index_name)
                    .or_else(|| {
                        // Try to use service name as index if no explicit index_name
                        resource_span_json.get("resource")
                            .and_then(|r| r.get("attributes"))
                            .and_then(|attrs| attrs.as_array())
                            .and_then(|attrs| {
                                attrs.iter().find(|attr| {
                                    attr.get("key").and_then(|k| k.as_str()) == Some("service.name")
                                })
                            })
                            .and_then(|attr| attr.get("value"))
                            .and_then(|v| v.get("stringValue"))
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string())
                    });
                
                // For now, we'll encode the entire TracesData as one message
                // In a real implementation, you'd properly convert JSON to protobuf structures
                let traces_json = serde_json::json!({
                    "resourceSpans": [resource_span_json]
                });
                
                // Create a temporary ExportTraceServiceRequest with the JSON data
                // Note: This is a simplified version - in production, you'd properly convert JSON to protobuf
                let proto_bytes = serde_json::to_vec(&traces_json)?;
                
                // Base64 encode the protobuf bytes
                let base64_payload = general_purpose::STANDARD.encode(&proto_bytes);
                let size_bytes = base64_payload.len() as i32;
                
                let mut headers = std::collections::HashMap::new();
                
                // Add index_name to headers if found
                if let Some(ref idx) = final_index_name {
                    headers.insert("index_name".to_string(), idx.clone());
                }
                
                let message = TelemetryData {
                    id: uuid::Uuid::new_v4().to_string(),
                    source: "http-traces".to_string(),
                    partition: 0,
                    sequence: 0,
                    timestamp: Some(prost_types::Timestamp::from(std::time::SystemTime::now())),
                    payload_json: base64_payload,
                    payload_type: "trace".to_string(),
                    size_bytes,
                    headers,
                };
                
                messages.push(message);
            }
        }
        
        Ok(messages)
    }
    
    /// Convert OTLP logs to Telemetry data format
    fn convert_logs_to_messages(&self, logs: Value) -> Result<Vec<TelemetryData>> {
        use base64::{engine::general_purpose, Engine as _};
        
        let mut messages = Vec::new();
        
        // Extract custom index_name if present
        let index_name = logs.get("index_name")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        
        if let Some(resource_logs) = logs.get("resourceLogs").and_then(|rl| rl.as_array()) {
            for resource_log_json in resource_logs {
                // For now, encode JSON directly as base64
                // In production, convert to proper protobuf format
                let logs_json = serde_json::json!({
                    "resourceLogs": [resource_log_json]
                });
                
                let proto_bytes = serde_json::to_vec(&logs_json)?;
                let base64_payload = general_purpose::STANDARD.encode(&proto_bytes);
                let size_bytes = base64_payload.len() as i32;
                
                let mut headers = std::collections::HashMap::new();
                
                // Add index_name to headers if provided
                if let Some(ref idx) = index_name {
                    headers.insert("index_name".to_string(), idx.clone());
                }
                
                let message = TelemetryData {
                    id: uuid::Uuid::new_v4().to_string(),
                    source: "http-logs".to_string(),
                    partition: 0,
                    sequence: 0,
                    timestamp: Some(prost_types::Timestamp::from(std::time::SystemTime::now())),
                    payload_json: base64_payload,
                    payload_type: "log".to_string(),
                    size_bytes,
                    headers,
                };
                
                messages.push(message);
            }
        }
        
        Ok(messages)
    }
    
    /// Convert OTLP metrics to Telemetry data format
    fn convert_metrics_to_messages(&self, metrics: Value) -> Result<Vec<TelemetryData>> {
        use base64::{engine::general_purpose, Engine as _};
        
        let mut messages = Vec::new();
        
        if let Some(resource_metrics) = metrics.get("resourceMetrics").and_then(|rm| rm.as_array()) {
            for resource_metric_json in resource_metrics {
                // For now, encode JSON directly as base64
                // In production, convert to proper protobuf format
                let metrics_json = serde_json::json!({
                    "resourceMetrics": [resource_metric_json]
                });
                
                let proto_bytes = serde_json::to_vec(&metrics_json)?;
                let base64_payload = general_purpose::STANDARD.encode(&proto_bytes);
                let size_bytes = base64_payload.len() as i32;
                
                let message = TelemetryData {
                    id: uuid::Uuid::new_v4().to_string(),
                    source: "http-metrics".to_string(),
                    partition: 0,
                    sequence: 0,
                    timestamp: Some(prost_types::Timestamp::from(std::time::SystemTime::now())),
                    payload_json: base64_payload,
                    payload_type: "metric".to_string(),
                    size_bytes,
                    headers: std::collections::HashMap::new(),
                };
                
                messages.push(message);
            }
        }
        
        Ok(messages)
    }
}

/// Create an ingestion client from configuration
pub async fn create_ingestion_client(config: &UnifiedConfig, node_id: String) -> Result<Option<IngestionClient>> {
    // Check if we should forward to ingestion
    if !config.ingestion.sources.http.enabled {
        return Ok(None);
    }
    
    // Check if control plane is configured
    let control_plane_endpoint = if !config.control_plane.grpc_endpoint.is_empty() {
        Some(config.control_plane.grpc_endpoint.clone())
    } else {
        None
    };
    
    // Create client based on configuration
    let client = if let Some(cp_endpoint) = control_plane_endpoint {
        // Use control plane for service discovery
        info!("Creating ingestion client with control plane discovery");
        match IngestionClient::new_with_control_plane(node_id.clone(), cp_endpoint, Arc::new(config.clone())).await {
            Ok(client) => client,
            Err(e) => {
                warn!("Failed to create control plane-based ingestion client: {}", e);
                // Fall back to environment variable or default
                // Don't use hardcoded default - require explicit configuration
                return Err(anyhow::anyhow!("Failed to create ingestion client: {}. Please ensure ingestion nodes are running.", e));
            }
        }
    } else if let Ok(endpoint) = std::env::var("OPENWIT_GRPC_INGESTION_ENDPOINT")
        .or_else(|_| std::env::var("INGESTION_ENDPOINT")) {
        // Use environment variable
        info!("Using ingestion endpoint from environment: {}", endpoint);
        IngestionClient::new(node_id.clone(), endpoint).await?
    } else {
        // Try to create client with control plane discovery even without explicit endpoint
        // This will use service registry as fallback
        info!("No explicit ingestion endpoint configured, attempting service discovery");
        IngestionClient::new_with_control_plane(node_id, String::new(), Arc::new(config.clone())).await?
    };
    
    Ok(Some(client))
}