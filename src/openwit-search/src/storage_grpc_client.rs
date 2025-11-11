use std::sync::Arc;
use std::time::{Duration, Instant};
use std::collections::HashMap;
use anyhow::{Result, Context};
use tokio::sync::RwLock;
use tonic::transport::Channel;
use tracing::{info, warn, error, debug};

use openwit_control_plane::client::ControlPlaneClient;
use openwit_proto::storage::{
    storage_query_service_client::StorageQueryServiceClient,
    QueryRequest as ProtoQueryRequest, QueryResponse as ProtoQueryResponse,
};

/// Information about a storage node
#[derive(Debug, Clone)]
struct StorageNodeInfo {
    node_id: String,
    grpc_endpoint: String,
    health_score: f64,
}

/// Cache for storage nodes
#[derive(Clone)]
struct StorageNodeCache {
    nodes: Vec<StorageNodeInfo>,
    last_update: Instant,
    ttl: Duration,
}

impl StorageNodeCache {
    fn new() -> Self {
        // Get TTL from environment variable or use default
        let ttl_secs = std::env::var("STORAGE_NODE_CACHE_TTL_SECS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(60); // Default to 1 minute
            
        info!("[STORAGE NODE CACHE] Initialized with TTL: {} seconds", ttl_secs);
        
        Self {
            nodes: Vec::new(),
            last_update: Instant::now(),
            ttl: Duration::from_secs(ttl_secs),
        }
    }
    
    fn is_valid(&self) -> bool {
        self.last_update.elapsed() < self.ttl && !self.nodes.is_empty()
    }
    
    fn update(&mut self, nodes: Vec<StorageNodeInfo>) {
        self.nodes = nodes;
        self.last_update = Instant::now();
    }
}

/// Enhanced gRPC client for querying storage nodes with control plane integration
#[derive(Clone)]
pub struct StorageGrpcClientV2 {
    _node_id: String,
    control_plane_client: Arc<RwLock<ControlPlaneClient>>,
    /// Cached gRPC clients for each storage node
    clients: Arc<RwLock<HashMap<String, StorageQueryServiceClient<Channel>>>>,
    /// Current storage node index for round-robin
    _current_index: Arc<tokio::sync::Mutex<usize>>,
    /// Query timeout
    query_timeout: Duration,
    /// Cached storage nodes
    node_cache: Arc<RwLock<StorageNodeCache>>,
}

impl StorageGrpcClientV2 {
    pub async fn new(
        node_id: String,
        control_plane_client: ControlPlaneClient,
    ) -> Result<Self> {
        // Start periodic cleanup of stale clients
        let clients_clone = Arc::new(RwLock::new(HashMap::new()));
        let cleanup_clients = clients_clone.clone();
        
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(300)); // 5 minutes
            loop {
                interval.tick().await;
                let mut clients = cleanup_clients.write().await;
                clients.clear(); // Simple cleanup - clear all cached connections
                debug!("Cleaned up cached storage gRPC clients");
            }
        });
        
        Ok(Self {
            _node_id: node_id,
            control_plane_client: Arc::new(RwLock::new(control_plane_client)),
            clients: clients_clone,
            _current_index: Arc::new(tokio::sync::Mutex::new(0)),
            query_timeout: Duration::from_secs(300), // 5 minutes
            node_cache: Arc::new(RwLock::new(StorageNodeCache::new())),
        })
    }
    
    /// Invalidate the node cache (useful when nodes change)
    pub async fn invalidate_cache(&self) {
        let mut cache = self.node_cache.write().await;
        cache.nodes.clear();
        cache.last_update = Instant::now().checked_sub(Duration::from_secs(3600)).unwrap(); // Set to 1 hour ago
        info!("[SEARCH->STORAGE] Invalidated node cache");
    }
    
    /// Execute query on storage nodes with automatic failover
    pub async fn execute_query(
        &self,
        sql: String,
        client_name: Option<String>,
        limit: Option<usize>,
        request_id: String,
    ) -> Result<StorageQueryResult> {
        info!("[SEARCH->STORAGE] Starting query execution");
        info!("[SEARCH->STORAGE] Request ID: {}", request_id);
        info!("[SEARCH->STORAGE] SQL: {}", sql);
        info!("[SEARCH->STORAGE] Client: {:?}", client_name);
        info!("[SEARCH->STORAGE] Limit: {:?}", limit);
        
        let mut retries = 3;
        let mut last_error = None;
        
        while retries > 0 {
            match self.query_with_failover(sql.clone(), client_name.clone(), limit, request_id.clone()).await {
                Ok(result) => {
                    debug!("Successfully executed query {} on storage node", request_id);
                    return Ok(result);
                }
                Err(e) => {
                    retries -= 1;
                    last_error = Some(e);
                    if retries > 0 {
                        warn!("Failed to execute query {}, retrying... ({} retries left)", request_id, retries);
                        tokio::time::sleep(Duration::from_millis(500)).await;
                    }
                }
            }
        }
        
        Err(last_error.unwrap_or_else(|| anyhow::anyhow!("Failed to execute query after 3 retries")))
    }
    
    /// Query with automatic failover to next healthy storage node
    async fn query_with_failover(
        &self,
        sql: String,
        client_name: Option<String>,
        limit: Option<usize>,
        request_id: String,
    ) -> Result<StorageQueryResult> {
        // Get healthy storage nodes from control plane
        let storage_nodes = self.get_healthy_storage_nodes().await?;
        if storage_nodes.is_empty() {
            return Err(anyhow::anyhow!("No healthy storage nodes available"));
        }
        
        // Try each storage node until one succeeds
        let mut errors = Vec::new();
        
        for node in &storage_nodes {
            info!("[STORAGE GRPC V2] Attempting query on storage node: {} at {}", 
                  node.node_id, node.grpc_endpoint);
            
            match self.query_single_node(
                node,
                sql.clone(),
                client_name.clone(),
                limit,
                request_id.clone()
            ).await {
                Ok(result) => {
                    info!("[STORAGE GRPC V2] Query successful on node: {}", node.node_id);
                    return Ok(result);
                }
                Err(e) => {
                    error!("[STORAGE GRPC V2] Query failed on node {}: {}", node.node_id, e);
                    errors.push((node.node_id.clone(), e));
                }
            }
        }
        
        // All nodes failed
        let error_summary = errors.iter()
            .map(|(node, err)| format!("{}: {}", node, err))
            .collect::<Vec<_>>()
            .join("; ");
        
        Err(anyhow::anyhow!("All storage nodes failed. Errors: {}", error_summary))
    }
    
    /// Query a single storage node
    async fn query_single_node(
        &self,
        node: &StorageNodeInfo,
        sql: String,
        client_name: Option<String>,
        limit: Option<usize>,
        request_id: String,
    ) -> Result<StorageQueryResult> {
        // Get or create gRPC client for this node
        let mut client = self.get_or_create_client(&node.node_id, &node.grpc_endpoint).await?;
        
        // Create request
        info!("[STORAGE GRPC V2] Creating query request - SQL: {}, Client: {:?}", sql, client_name);
        let request = ProtoQueryRequest {
            request_id: request_id.clone(),
            query: sql,  // Map 'sql' parameter to 'query' field
            client_id: client_name.unwrap_or_default(),  // Map 'client_name' to 'client_id'
            limit: limit.map(|l| l as u32).unwrap_or(0),
            offset: 0,
            metadata: HashMap::new(),
        };
        
        // Execute query with timeout
        let response = tokio::time::timeout(
            self.query_timeout,
            client.execute_query(request)
        )
        .await
        .context("Query timeout")?
        .context("Failed to execute query")?
        .into_inner();
        
        if !response.success {
            return Err(anyhow::anyhow!("Query failed: {}", response.error));
        }
        
        // Convert response to our format
        self.convert_proto_response(response)
    }
    
    /// Get healthy storage nodes from control plane with caching
    async fn get_healthy_storage_nodes(&self) -> Result<Vec<StorageNodeInfo>> {
        // Check cache first
        {
            let cache = self.node_cache.read().await;
            if cache.is_valid() {
                info!("[SEARCH->STORAGE] Using cached storage nodes ({} nodes, cached {} seconds ago)", 
                    cache.nodes.len(), 
                    cache.last_update.elapsed().as_secs());
                return Ok(cache.nodes.clone());
            }
        }
        
        info!("[SEARCH->STORAGE] Cache miss or expired, querying control plane for storage nodes...");
        
        // Try to get nodes from control plane with shorter timeout
        match tokio::time::timeout(
            Duration::from_secs(5),
            self.get_nodes_from_control_plane()
        ).await {
            Ok(Ok(nodes)) if !nodes.is_empty() => {
                info!("[SEARCH->STORAGE] Control plane returned {} nodes", nodes.len());
                
                // Update cache
                {
                    let mut cache = self.node_cache.write().await;
                    cache.update(nodes.clone());
                    info!("[SEARCH->STORAGE] Updated node cache with {} nodes", nodes.len());
                }
                
                return Ok(nodes);
            }
            Ok(Ok(_)) => {
                warn!("[SEARCH->STORAGE] Control plane returned no storage nodes");
            }
            Ok(Err(e)) => {
                warn!("[SEARCH->STORAGE] Control plane error: {}", e);
            }
            Err(_) => {
                warn!("[SEARCH->STORAGE] Control plane request timed out after 5 seconds");
            }
        }
        
        // Check if we have stale cache data we can use
        {
            let cache = self.node_cache.read().await;
            if !cache.nodes.is_empty() {
                warn!("[SEARCH->STORAGE] Using stale cache data ({} nodes, {} seconds old)", 
                    cache.nodes.len(),
                    cache.last_update.elapsed().as_secs());
                return Ok(cache.nodes.clone());
            }
        }
        
        // Fallback to default storage endpoints
        info!("[SEARCH->STORAGE] Using fallback storage endpoints");
        let fallback_nodes = self.get_fallback_storage_nodes();
        
        if fallback_nodes.is_empty() {
            return Err(anyhow::anyhow!("No storage nodes available (control plane timeout and no fallback configured)"));
        }
        
        // Cache the fallback nodes too
        {
            let mut cache = self.node_cache.write().await;
            cache.update(fallback_nodes.clone());
            info!("[SEARCH->STORAGE] Cached fallback nodes");
        }
        
        Ok(fallback_nodes)
    }
    
    /// Try to get nodes from control plane
    async fn get_nodes_from_control_plane(&self) -> Result<Vec<StorageNodeInfo>> {
        let mut control_client = self.control_plane_client.write().await;
        
        let nodes = control_client.get_healthy_nodes("storage").await?;
        
        let storage_nodes: Vec<StorageNodeInfo> = nodes
            .into_iter()
            .filter(|n| n.health_score > 0.5)  // Only use nodes with good health
            .map(|n| StorageNodeInfo {
                node_id: n.node_id,
                grpc_endpoint: n.grpc_endpoint,
                health_score: n.health_score,
            })
            .collect();
        
        info!("[STORAGE GRPC V2] Found {} healthy storage nodes from control plane", 
              storage_nodes.len());
        
        if storage_nodes.is_empty() {
            error!("[SEARCH->STORAGE] NO STORAGE NODES FOUND FROM CONTROL PLANE!");
            error!("[SEARCH->STORAGE] This explains why queries aren't reaching storage!");
        }
        
        // Log discovered nodes
        for node in &storage_nodes {
            info!("[STORAGE GRPC V2]   - Node {} at endpoint {} (health: {:.2})", 
                  node.node_id, node.grpc_endpoint, node.health_score);
        }
        
        Ok(storage_nodes)
    }
    
    /// Get fallback storage nodes from environment or configuration
    fn get_fallback_storage_nodes(&self) -> Vec<StorageNodeInfo> {
        let mut nodes = Vec::new();
        
        // Check environment variable first
        if let Ok(storage_endpoints) = std::env::var("STORAGE_GRPC_ENDPOINTS") {
            for endpoint in storage_endpoints.split(',') {
                let endpoint = endpoint.trim();
                if !endpoint.is_empty() {
                    nodes.push(StorageNodeInfo {
                        node_id: format!("storage-fallback-{}", nodes.len()),
                        grpc_endpoint: endpoint.to_string(),
                        health_score: 1.0,
                    });
                }
            }
        }
        
        // If no env var, use default localhost endpoint
        if nodes.is_empty() {
            // Default to localhost with standard storage gRPC port
            let default_port = std::env::var("STORAGE_GRPC_QUERY_PORT")
                .unwrap_or_else(|_| "8083".to_string());
            
            nodes.push(StorageNodeInfo {
                node_id: "storage-localhost".to_string(),
                grpc_endpoint: format!("http://127.0.0.1:{}", default_port),
                health_score: 1.0,
            });
            
            // Also try the known storage service port from config
            if default_port != "8081" {
                nodes.push(StorageNodeInfo {
                    node_id: "storage-service".to_string(),
                    grpc_endpoint: "http://127.0.0.1:8081".to_string(),
                    health_score: 1.0,
                });
            }
        }
        
        info!("[SEARCH->STORAGE] Configured {} fallback storage endpoints", nodes.len());
        for node in &nodes {
            info!("[SEARCH->STORAGE] - {} at {}", node.node_id, node.grpc_endpoint);
        }
        
        nodes
    }
    
    /// Get or create gRPC client for a storage node
    async fn get_or_create_client(
        &self,
        node_id: &str,
        endpoint: &str,
    ) -> Result<StorageQueryServiceClient<Channel>> {
        // Add timeout to client cache access to prevent hanging
        let mut clients = tokio::time::timeout(
            Duration::from_secs(10),
            self.clients.write()
        ).await
        .context("Timeout waiting for client cache lock after 10 seconds")?;
        
        if let Some(client) = clients.get(node_id) {
            debug!("[STORAGE GRPC V2] Using cached client for node {}", node_id);
            return Ok(client.clone());
        }
        
        // Create new client
        info!("[STORAGE GRPC V2] Creating new gRPC client for storage node {} at {}", 
              node_id, endpoint);
        
        // Ensure endpoint has a scheme
        let endpoint_with_scheme = if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
            endpoint.to_string()
        } else {
            format!("http://{}", endpoint)
        };
        
        let channel = Channel::from_shared(endpoint_with_scheme.clone())
            .map_err(|e| {
                error!("[STORAGE GRPC V2] Invalid endpoint '{}': {}", endpoint_with_scheme, e);
                anyhow::anyhow!("Invalid endpoint: {}", e)
            })?;
        
        // Add overall timeout to gRPC connection establishment
        let channel = tokio::time::timeout(
            Duration::from_secs(20),
            channel
                .timeout(Duration::from_secs(30))
                .connect_timeout(Duration::from_secs(10))
                .connect()
        ).await
        .context("gRPC connection establishment timed out after 20 seconds")?
        .map_err(|e| {
            error!("[STORAGE GRPC V2] Failed to connect to storage node at {}: {:?}", 
                   endpoint_with_scheme, e);
            anyhow::anyhow!("Connection failed: {:?}", e)
        })?;
        
        info!("[STORAGE GRPC V2] Successfully connected to storage node at {}", endpoint_with_scheme);
        
        let client = StorageQueryServiceClient::new(channel);
        clients.insert(node_id.to_string(), client.clone());
        
        Ok(client)
    }
    
    /// Convert protobuf response to internal format
    fn convert_proto_response(&self, response: ProtoQueryResponse) -> Result<StorageQueryResult> {
        let result = response.result
            .ok_or_else(|| anyhow::anyhow!("No result in response"))?;
        
        // Convert to JSON for compatibility with existing code
        let mut json_rows = Vec::new();
        
        for row in result.rows {
            let mut json_row = serde_json::Map::new();
            
            for (i, value) in row.values.into_iter().enumerate() {
                if let Some(column) = result.columns.get(i) {
                    json_row.insert(
                        column.name.clone(),
                        self.proto_value_to_json(&value),
                    );
                }
            }
            
            json_rows.push(serde_json::Value::Object(json_row));
        }
        
        // Create schema JSON from columns
        let schema_json: Vec<serde_json::Value> = result.columns.iter()
            .map(|col| serde_json::json!({
                "name": col.name,
                "data_type": col.data_type,
                "nullable": col.nullable
            }))
            .collect();
        let schema = serde_json::to_string(&schema_json).unwrap_or_else(|_| "[]".to_string());
        
        Ok(StorageQueryResult {
            request_id: response.request_id,
            success: response.success,
            error: if response.error.is_empty() { None } else { Some(response.error) },
            results: json_rows,
            schema: Some(schema),
            execution_time_ms: response.execution_time_ms,
            row_count: result.total_rows as usize,
            rows_scanned: result.rows_scanned,
            bytes_scanned: result.bytes_scanned,
        })
    }
    
    /// Convert protobuf value to JSON
    fn proto_value_to_json(&self, value: &openwit_proto::storage::Value) -> serde_json::Value {
        use openwit_proto::storage::value::Value as ProtoValue;
        
        match &value.value {
            None | Some(ProtoValue::NullValue(_)) => serde_json::Value::Null,
            Some(ProtoValue::BoolValue(b)) => serde_json::Value::Bool(*b),
            Some(ProtoValue::Int64Value(i)) => serde_json::Value::Number(
                serde_json::Number::from(*i)
            ),
            Some(ProtoValue::Uint64Value(u)) => serde_json::Value::Number(
                serde_json::Number::from(*u)
            ),
            Some(ProtoValue::DoubleValue(d)) => {
                serde_json::Number::from_f64(*d)
                    .map(serde_json::Value::Number)
                    .unwrap_or(serde_json::Value::Null)
            },
            Some(ProtoValue::StringValue(s)) => serde_json::Value::String(s.clone()),
            Some(ProtoValue::BytesValue(b)) => {
                // Convert bytes to base64 string
                use base64::{engine::general_purpose, Engine as _};
                serde_json::Value::String(general_purpose::STANDARD.encode(b))
            },
            Some(ProtoValue::TimestampValue(ts)) => {
                // Convert timestamp to ISO string
                use chrono::{TimeZone, Utc};
                let datetime = Utc.timestamp_opt(ts.seconds, ts.nanos as u32)
                    .single()
                    .map(|dt| dt.to_rfc3339())
                    .unwrap_or_else(|| "invalid timestamp".to_string());
                serde_json::Value::String(datetime)
            },
            Some(ProtoValue::ArrayValue(arr)) => {
                let values = arr.values.iter()
                    .map(|v| self.proto_value_to_json(v))
                    .collect();
                serde_json::Value::Array(values)
            },
            Some(ProtoValue::MapValue(map)) => {
                let mut json_map = serde_json::Map::new();
                for (k, v) in &map.entries {
                    json_map.insert(k.clone(), self.proto_value_to_json(v));
                }
                serde_json::Value::Object(json_map)
            },
        }
    }
}

/// Storage query result
#[derive(Debug, Clone)]
pub struct StorageQueryResult {
    pub request_id: String,
    pub success: bool,
    pub error: Option<String>,
    pub results: Vec<serde_json::Value>,
    pub schema: Option<String>,
    pub execution_time_ms: u64,
    pub row_count: usize,
    pub rows_scanned: u64,
    pub bytes_scanned: u64,
}

impl StorageQueryResult {
    /// Merge multiple query results
    pub fn merge(results: Vec<Self>) -> Self {
        if results.is_empty() {
            return Self {
                request_id: String::new(),
                success: true,
                error: None,
                results: vec![],
                schema: None,
                execution_time_ms: 0,
                row_count: 0,
                rows_scanned: 0,
                bytes_scanned: 0,
            };
        }
        
        let mut merged = Self {
            request_id: results[0].request_id.clone(),
            success: true,
            error: None,
            results: vec![],
            schema: results[0].schema.clone(),
            execution_time_ms: 0,
            row_count: 0,
            rows_scanned: 0,
            bytes_scanned: 0,
        };
        
        for result in results {
            merged.results.extend(result.results);
            merged.row_count += result.row_count;
            merged.rows_scanned += result.rows_scanned;
            merged.bytes_scanned += result.bytes_scanned;
            merged.execution_time_ms = merged.execution_time_ms.max(result.execution_time_ms);
        }
        
        merged
    }
}