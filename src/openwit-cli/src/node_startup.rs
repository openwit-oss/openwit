use anyhow::{Result, Context};
use openwit_config::UnifiedConfig;
use tracing::{info, error, warn, debug};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::io::Write;
use std::sync::Arc;
use tokio::sync::mpsc;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::collections::HashMap;
use chrono::Datelike;
use crate::control_plane_discovery::{register_control_plane, get_control_plane_seed, cleanup_control_plane_file};

/// Start the proxy node with unified config
pub async fn start_proxy_node(config: UnifiedConfig, node_id: String, port: u16) -> Result<()> {
    info!("Starting Proxy Node: {}", node_id);
    info!("════════════════════════════════════════");
    info!("Service port: {}", port);
    
    // Proxy node uses the port for HTTP ingestion
    // Update config to use the specified port
    let mut proxy_config = config;
    proxy_config.ingestion.http.port = port;
    proxy_config.ingestion.http.bind = format!("0.0.0.0:{}", port);
    
    // Using control plane for service discovery
    
    // Control plane discovery will be handled automatically
    
    // Start proxy node
    openwit_proxy::start_proxy_node(node_id, proxy_config).await?;
    
    Ok(())
}

/// Start the control plane node with unified config
pub async fn start_control_node(config: UnifiedConfig, node_id: String, port: u16) -> Result<()> {
    info!("Starting Control Plane Node");
    info!("════════════════════════════════════════");
    
    // Build bind address from port
    let addr: SocketAddr = format!("0.0.0.0:{}", port).parse()
        .context("Invalid control plane port")?;
    
    info!("Node ID: {}", node_id);
    info!("Binding to: {}", addr);
    
    // Gossip functionality deprecated
    let mut control_config = config;
    
    // Initialize cluster networking
    let cluster_handle = init_cluster_networking(&control_config).await?;
    
    // Register control plane for discovery (gossip deprecated)
    register_control_plane(0, port)?;
    
    // Setup cleanup on shutdown
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        cleanup_control_plane_file().ok();
    });
    
    // Start control plane with optional config path
    openwit_control_plane::start_control_plane(
        node_id,
        cluster_handle,
        addr,
        None, // Config is already loaded
    ).await?;
    
    Ok(())
}

/// Start the ingestion node with unified config
pub async fn start_ingest_node(config: UnifiedConfig, node_id: String, port: u16) -> Result<()> {
    info!("Starting Ingestion Node: {}", node_id);
    info!("════════════════════════════════════════");
    
    // Use gRPC configuration from YAML if available, otherwise use provided port
    let grpc_addr = if config.ingestion.sources.grpc.enabled {
        // Check if bind already contains port (e.g., "0.0.0.0:4317")
        if config.ingestion.grpc.bind.contains(':') {
            config.ingestion.grpc.bind.clone()
        } else {
            // Otherwise construct from bind host and port
            format!("{}:{}", config.ingestion.grpc.bind, config.ingestion.grpc.port)
        }
    } else {
        format!("0.0.0.0:{}", port)
    };
    
    info!("gRPC address to bind: {}", grpc_addr);
    let addr: SocketAddr = grpc_addr.parse()
        .context("Invalid gRPC ingestion address")?;
    
    info!("gRPC Configuration:");
    info!("  Bind address: {}", addr);
    info!("  Max message size: {} bytes", config.ingestion.grpc.max_message_size);
    info!("  Max concurrent requests: {}", config.ingestion.grpc.max_concurrent_requests);
    info!("  Request timeout: {} ms", config.ingestion.grpc.request_timeout_ms);
    info!("  Connection pool size: {}", config.ingestion.grpc.connection_pool_size);
    info!("  Keepalive time: {} ms", config.ingestion.grpc.keepalive_time_ms);
    
    if !config.ingestion.sources.grpc.enabled {
        return Err(anyhow::anyhow!("gRPC ingestion is disabled in configuration. Enable it by setting ingestion.sources.grpc.enabled: true"));
    }
    
    // Gossip functionality deprecated - networking disabled for simplicity
    let mut ingest_config = config;
    let _cluster_handle: Option<openwit_network::ClusterHandle> = None;
    info!("Cluster networking DISABLED (gossip deprecated)");
    
    // Update the gRPC port in config to match the provided port
    ingest_config.ingestion.grpc.port = port;
    ingest_config.ingestion.grpc.bind = format!("0.0.0.0:{}", port);
    
    // Log ingestion configuration details
    info!("Ingestion configuration:");
    info!("  gRPC port: {}", port);
    info!("  Node ID: {}", node_id);
    
    // Create control plane client and pass to ingestion service
    let control_client = match openwit_control_plane::client::ControlPlaneClient::new(&node_id, &ingest_config).await {
        Ok(client) => {
            info!("✅ Connected to control plane at {}", ingest_config.control_plane.grpc_endpoint);
            Some(client)
        }
        Err(e) => {
            warn!("⚠️ Failed to connect to control plane: {}. Ingestion will run without control plane integration.", e);
            None
        }
    };
    
    // Register ingestion node with control plane
    if let Some(client) = control_client {
        // Create new ingestion service with control plane
        let ingestion_service = openwit_ingestion::TelemetryIngestionGrpcService::new_with_control_plane(
            node_id.clone(),
            client.clone(),
            ingest_config.clone(),
        );
        
        // Spawn registration task
        let node_id_clone = node_id.clone();
        let config_clone = ingest_config.clone();
        let mut client_clone = client.clone();
        
        tokio::spawn(async move {
            // Initial registration
            register_ingestion_node_with_control_plane(&mut client_clone, &config_clone, &node_id_clone, port).await;
            
            // Start periodic health reporting
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));
            
            loop {
                interval.tick().await;
                register_ingestion_node_with_control_plane(&mut client_clone, &config_clone, &node_id_clone, port).await;
            }
        });
        
        // Start the gRPC server for ingestion
        info!("Starting ingestion gRPC server on {} (with control plane)", addr);
        
        let ingestion_server = ingestion_service.into_server();
        
        tonic::transport::Server::builder()
            .add_service(ingestion_server)
            .serve(addr)
            .await?;
    } else {
        // Start without control plane
        info!("Starting ingestion gRPC server on {} (without control plane)", addr);
        
        // Create ingestion service without control plane
        let ingestion_service = openwit_ingestion::TelemetryIngestionGrpcService::new(
            node_id.clone(),
            ingest_config.clone(),
        );
        
        let ingestion_server = ingestion_service.into_server();
        
        tonic::transport::Server::builder()
            .add_service(ingestion_server)
            .serve(addr)
            .await?;
    }
    
    Ok(())
}

/// Start the storage node with unified config
pub async fn start_storage_node(config: UnifiedConfig, node_id: String, port: u16) -> Result<()> {
    info!("Starting Storage Node: {}", node_id);
    info!("════════════════════════════════════════");
    
    // Gossip functionality deprecated
    let gossip_port = 0;
    
    // Get Arrow Flight port from config
    let arrow_flight_port = config.service_ports.storage.arrow_flight;
    
    // Create a temporary TOML config file for storage node
    let _storage_config_toml = create_storage_config_toml(&config, &node_id, arrow_flight_port, gossip_port)?;
    
    info!("Storage configuration:");
    info!("  Backend: {}", config.storage.backend);
    info!("  Data dir: {}", config.storage.local.path);
    info!("  Service port: {}", port);
    info!("  Arrow Flight port: {}", arrow_flight_port);
    
    match config.storage.backend.as_str() {
        "azure" => {
            info!("  Azure account: {}", config.storage.azure.account_name);
            info!("  Azure container: {}", config.storage.azure.container_name);
        },
        "s3" => {
            info!("  S3 bucket: {}", config.storage.s3.bucket);
            info!("  S3 region: {}", config.storage.s3.region);
        },
        _ => {
            info!("  Using local storage");
        },
    }
    
    // Gossip functionality deprecated - simplified storage setup
    let storage_config = config;
    
    // Create a minimal cluster handle for storage processor
    let cluster_handle = init_cluster_networking_with_node_id(&storage_config, "storage", &node_id).await?;
    
    // Start the Arrow Flight storage server with ParquetFileManager (NEW IMPLEMENTATION)
    info!("Starting Arrow Flight storage server with cloud upload support on port {}", arrow_flight_port);
    start_arrow_flight_storage_server(storage_config.clone(), node_id.clone(), arrow_flight_port, cluster_handle).await?;
    
    // Start gRPC query service on port 8083
    // Start simple gRPC query service on port 8083
    let grpc_query_port = 8083;
    let grpc_data_path = storage_config.storage.data_dir.clone();
    let grpc_bind_addr = format!("0.0.0.0:{}", grpc_query_port);

    tokio::spawn(async move {
        info!("Starting simple storage query gRPC service on {}", grpc_bind_addr);
        if let Err(e) = openwit_storage::start_simple_grpc_query_service(grpc_data_path, grpc_bind_addr).await {
            error!("Failed to start simple gRPC query service: {}", e);
        }
    });

    info!("Storage node {} is running", node_id);
    info!("  - Service on port {}", port);
    info!("  - Arrow Flight server on port {} (FULL MODE)", arrow_flight_port);
    info!("  - Simple gRPC query service on port {}", grpc_query_port);
    info!("  - Gossip on port {}", gossip_port);
    info!("  - Reporting health to control plane");
    info!("  - Ready to receive batches from ingestion nodes");
    info!("  - Ready to handle queries via gRPC");
    
    // Start control plane registration and health reporting
    start_storage_control_plane_registration(&storage_config, node_id.clone(), arrow_flight_port);
    
    // Wait for shutdown signal
    tokio::signal::ctrl_c().await?;
    
    Ok(())
}

/// Start a simplified Arrow Flight storage server
async fn start_simple_arrow_flight_storage_server(
    config: UnifiedConfig,
    node_id: String,
    port: u16,
) -> Result<()> {
    info!("Starting simplified Arrow Flight storage server for node {} on port {}", node_id, port);
    
    // Use the existing Arrow Flight service from openwit-inter-node
    let address_cache = std::sync::Arc::new(openwit_inter_node::NodeAddressCache::new(
        node_id.clone(),
        "storage".to_string(),
    ));
    let (flight_service, mut batch_receiver) = openwit_inter_node::ArrowFlightService::new(
        node_id.clone(),
        "storage".to_string(),
        address_cache,
    );
    
    // Start the Arrow Flight server
    let flight_service = flight_service;
    let bind_addr = format!("0.0.0.0:{}", port);
    info!("Arrow Flight server binding to: {}", bind_addr);
    
    tokio::spawn(async move {
        if let Err(e) = flight_service.start(port).await {
            error!("Arrow Flight server error: {}", e);
        }
    });
    
    // Handle incoming batches
    let data_path = config.storage.data_dir.clone();
    tokio::spawn(async move {
        info!("Starting Arrow Flight batch handler for storage node {}", node_id);
        
        while let Some(batch) = batch_receiver.recv().await {
            info!(
                "STORAGE: =========================================="
            );
            info!(
                "STORAGE: Received Arrow Flight batch {} from {} ({} rows, {} columns)",
                batch.batch_id,
                batch.source_node,
                batch.record_batch.num_rows(),
                batch.record_batch.num_columns()
            );
            
            // Log metadata for debugging
            if !batch.metadata.is_empty() {
                debug!("Batch metadata: {:?}", batch.metadata);
            }
            
            // Store the batch to local storage in Parquet format
            let client_name = extract_client_name_from_metadata(&batch.metadata);
            let current_date = chrono::Utc::now();
            let date_path = format!("{:04}/{:02}/{:02}", 
                current_date.year(), 
                current_date.month(), 
                current_date.day()
            );
            
            // Create directory structure: {data_path}/{client_name}/{year}/{month}/{day}/
            let storage_dir = format!("{}/{}/{}", data_path, client_name, date_path);
            if let Err(e) = tokio::fs::create_dir_all(&storage_dir).await {
                error!("STORAGE: Failed to create storage directory {}: {}", storage_dir, e);
            } else {
                // Generate unique filename with timestamp
                let timestamp = current_date.timestamp_millis();
                let clean_batch_id = batch.batch_id.replace("/", "_").replace(":", "_").replace("=", "_").replace(";", "_");
                let file_path = format!("{}/traces_{}_{}.parquet", storage_dir, timestamp, clean_batch_id);
                
                // Write batch as Parquet
                match write_batch_to_parquet(&batch.record_batch, &file_path).await {
                    Ok(_) => {
                        info!("STORAGE: Successfully wrote batch {} to {}", batch.batch_id, file_path);
                        info!("STORAGE: Stored for client '{}' on date {}", client_name, date_path);
                    }
                    Err(e) => {
                        error!("STORAGE: Failed to write batch {} to Parquet: {}", batch.batch_id, e);
                    }
                }
            }
            
            info!("STORAGE: Processed batch {} (client: {}, date: {})", batch.batch_id, client_name, date_path);
            info!("STORAGE: ==========================================");
        }
        
        warn!("Arrow Flight batch receiver closed for storage node {}", node_id);
    });
    
    // Wait a moment for the server to start
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    
    // Verify the server is listening
    match tokio::net::TcpStream::connect(format!("127.0.0.1:{}", port)).await {
        Ok(_) => {
            info!("Arrow Flight storage server verified on port {}", port);
        }
        Err(e) => {
            error!("Arrow Flight storage server not accessible on port {}: {}", port, e);
        }
    }
    
    Ok(())
}

/// Extract client name from batch metadata
fn extract_client_name_from_metadata(metadata: &HashMap<String, String>) -> String {
    // Debug: log all metadata to understand the available fields
    debug!("Extracting client name from metadata: {:?}", metadata);
    
    // First try to get client_name from header (from Kafka)
    if let Some(client_name) = metadata.get("header.client_name") {
        if !client_name.is_empty() {
            info!("Extracted client name '{}' from header.client_name", client_name);
            return client_name.clone();
        }
    }
    
    // Try to get client_name directly
    if let Some(client_name) = metadata.get("client_name") {
        if !client_name.is_empty() {
            info!("Extracted client name '{}' from client_name", client_name);
            return client_name.clone();
        }
    }
    
    // Try to extract from tenant if available and not default
    if let Some(tenant) = metadata.get("tenant") {
        if !tenant.is_empty() && tenant != "default" {
            return tenant.clone();
        }
    }
    
    // Try to extract from account_key if available and not default
    if let Some(account_key) = metadata.get("account_key") {
        if !account_key.is_empty() && account_key != "default" {
            return account_key.clone();
        }
    }
    
    // Try to extract from topic name if available (both direct and header fields)
    let topic_fields = vec!["topic", "kafka_topic", "header.topic", "header.kafka_topic"];
    for field in topic_fields {
        if let Some(topic) = metadata.get(field) {
            // Extract client from Kafka topic patterns like:
            // "v6.qtw.traces.client1.1" -> client1 (position 3)
            // For OpenWit pattern: v6.qtw.*.*.*
            let parts: Vec<&str> = topic.split('.').collect();
            if parts.len() >= 4 && parts[0] == "v6" && parts[1] == "qtw" {
                let client_name = parts[3].to_string();
                info!("Extracted client name '{}' from Kafka topic pattern '{}' at position 3", client_name, topic);
                return client_name;
            }
            
            // Extract client from standard OTEL topic patterns:
            // "opentelemetry-traces-<client>" -> client
            // "traces-<client>" -> client  
            // "<client>-traces" -> client
            if let Some(client) = topic.strip_prefix("opentelemetry-traces-") {
                info!("Extracted client name '{}' from topic pattern 'opentelemetry-traces-*'", client);
                return client.to_string();
            }
            if let Some(client) = topic.strip_prefix("traces-") {
                info!("Extracted client name '{}' from topic pattern 'traces-*'", client);
                return client.to_string();
            }
            if let Some(client) = topic.strip_suffix("-traces") {
                info!("Extracted client name '{}' from topic pattern '*-traces'", client);
                return client.to_string();
            }
            
            // Try to extract from generic dot-separated patterns (fallback)
            if topic.contains('.') {
                let parts: Vec<&str> = topic.split('.').collect();
                if parts.len() > 0 {
                    // Use first part as potential client name (if it's not a system prefix)
                    let first_part = parts[0];
                    if !["v6", "opentelemetry", "otel", "traces", "metrics", "logs"].contains(&first_part) {
                        info!("Extracted client name '{}' from topic first part: {}", first_part, topic);
                        return first_part.to_string();
                    }
                }
            }
        }
    }
    
    // Try to extract from index_name if available
    if let Some(index_name) = metadata.get("index_name") {
        return index_name.clone();
    }
    
    // Try to extract from source_node if available
    if let Some(source_node) = metadata.get("source_node") {
        // Extract client from node patterns like:
        // "ingest-<client>-ingest-<id>" -> client
        // "ingestion-<client>-<id>" -> client
        let parts: Vec<&str> = source_node.split('-').collect();
        
        // Pattern: ingest-<client>-ingest-<id>
        if parts.len() >= 4 && parts[0] == "ingest" && parts[2] == "ingest" {
            let client_name = parts[1].to_string();
            info!("Extracted client name '{}' from source_node pattern 'ingest-<client>-ingest-<id>': {}", client_name, source_node);
            return client_name;
        }
        
        // Pattern: ingestion-<client>-<id>
        if parts.len() >= 3 && parts[0] == "ingestion" {
            let client_name = parts[1].to_string();
            info!("Extracted client name '{}' from source_node pattern 'ingestion-<client>-<id>': {}", client_name, source_node);
            return client_name;
        }
        
        // Pattern: ingest-<client>-<id> (simpler pattern)
        if parts.len() >= 3 && parts[0] == "ingest" {
            let client_name = parts[1].to_string();
            info!("Extracted client name '{}' from source_node pattern 'ingest-<client>-<id>': {}", client_name, source_node);
            return client_name;
        }
        
        // Use the source node as client name (fallback)
        warn!("Using full source_node '{}' as client name (no pattern matched)", source_node);
        return source_node.clone();
    }
    
    // Default fallback
    warn!("No client name found in metadata, using 'default'");
    "default".to_string()
}

/// Write Arrow RecordBatch to Parquet file
async fn write_batch_to_parquet(
    record_batch: &arrow::record_batch::RecordBatch, 
    file_path: &str
) -> Result<()> {
    // Create the file
    let file = std::fs::File::create(file_path)
        .with_context(|| format!("Failed to create Parquet file: {}", file_path))?;
    
    // Configure Parquet writer properties
    let props = WriterProperties::builder()
        .set_compression(parquet::basic::Compression::SNAPPY)
        .set_dictionary_enabled(true)
        .build();
    
    // Create Arrow writer
    let mut writer = ArrowWriter::try_new(file, record_batch.schema(), Some(props))
        .with_context(|| format!("Failed to create Parquet writer for: {}", file_path))?;
    
    // Write the record batch
    writer.write(record_batch)
        .with_context(|| format!("Failed to write record batch to Parquet file: {}", file_path))?;
    
    // Close the writer
    writer.close()
        .with_context(|| format!("Failed to close Parquet writer for: {}", file_path))?;
    
    info!("Successfully wrote {} rows to Parquet file: {}", record_batch.num_rows(), file_path);
    Ok(())
}

/// Start control plane registration and health reporting for storage node
fn start_storage_control_plane_registration(
    config: &UnifiedConfig,
    node_id: String,
    arrow_flight_port: u16,
) {
    let config_clone = config.clone();
    
    tokio::spawn(async move {
        // Wait a bit for initialization
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        
        // Initial registration
        register_storage_node_with_control_plane(&config_clone, &node_id, arrow_flight_port).await;
        
        // Start periodic health reporting
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));
        
        loop {
            interval.tick().await;
            register_storage_node_with_control_plane(&config_clone, &node_id, arrow_flight_port).await;
        }
    });
}

/// Register storage node with control plane
async fn register_storage_node_with_control_plane(
    config: &UnifiedConfig,
    node_id: &str,
    arrow_flight_port: u16,
) {
    match openwit_control_plane::client::ControlPlaneClient::new("storage-node", config).await {
        Ok(mut client) => {
            // Determine the correct Arrow Flight endpoint based on environment
            let arrow_flight_endpoint = if std::env::var("KUBERNETES_SERVICE_HOST").is_ok() {
                // In Kubernetes, we need to determine the correct service name
                if let Ok(service_name) = std::env::var("STORAGE_SERVICE_NAME") {
                    info!("Using Kubernetes service name for Arrow Flight endpoint: {}", service_name);
                    format!("grpc://{}:{}", service_name, arrow_flight_port)
                } else if let Some(storage_endpoint) = &config.networking.service_endpoints.storage {
                    // Extract hostname from the storage service endpoint
                    if let Some(host) = storage_endpoint.strip_prefix("http://").or_else(|| storage_endpoint.strip_prefix("https://")) {
                        let service_name = host.split(':').next().unwrap_or(host).split('/').next().unwrap_or(host);
                        info!("Using storage service endpoint from config: {}", service_name);
                        format!("grpc://{}:{}", service_name, arrow_flight_port)
                    } else {
                        format!("grpc://{}:{}", storage_endpoint, arrow_flight_port)
                    }
                } else {
                    // Fallback to pod IP
                    let pod_ip = std::env::var("POD_IP").unwrap_or_else(|_| "localhost".to_string());
                    warn!("No storage service name configured, using pod IP: {}", pod_ip);
                    format!("grpc://{}:{}", pod_ip, arrow_flight_port)
                }
            } else {
                // Use the service-specific advertise address from networking config
                let advertise_addr = config.networking.get_service_advertise_address("storage");
                format!("grpc://{}:{}", advertise_addr, arrow_flight_port)
            };
            
            // Create registration metadata
            let mut metadata = std::collections::HashMap::new();
            metadata.insert("arrow_flight_endpoint".to_string(), arrow_flight_endpoint.clone());
            metadata.insert("status".to_string(), "accepting".to_string());
            metadata.insert("service_type".to_string(), "storage".to_string());
            
            // Add gRPC query service endpoint
            let grpc_query_port = 8083; // Storage query service port
            let grpc_endpoint = if std::env::var("KUBERNETES_SERVICE_HOST").is_ok() {
                // In Kubernetes, use service name
                if let Ok(service_name) = std::env::var("STORAGE_SERVICE_NAME") {
                    format!("{}:{}", service_name, grpc_query_port)
                } else if let Some(storage_endpoint) = &config.networking.service_endpoints.storage {
                    // Extract hostname from the storage service endpoint
                    if let Some(host) = storage_endpoint.strip_prefix("http://").or_else(|| storage_endpoint.strip_prefix("https://")) {
                        let service_name = host.split(':').next().unwrap_or(host).split('/').next().unwrap_or(host);
                        format!("{}:{}", service_name, grpc_query_port)
                    } else {
                        format!("{}:{}", storage_endpoint, grpc_query_port)
                    }
                } else {
                    // Fallback to pod IP
                    let pod_ip = std::env::var("POD_IP").unwrap_or_else(|_| "localhost".to_string());
                    format!("{}:{}", pod_ip, grpc_query_port)
                }
            } else {
                // Use the service-specific advertise address from networking config
                let advertise_addr = config.networking.get_service_advertise_address("storage");
                format!("{}:{}", advertise_addr, grpc_query_port)
            };
            metadata.insert("grpc_endpoint".to_string(), grpc_endpoint.clone());
            
            // Add pod information for Kubernetes
            if std::env::var("KUBERNETES_SERVICE_HOST").is_ok() {
                if let Ok(pod_ip) = std::env::var("POD_IP") {
                    metadata.insert("pod_ip".to_string(), pod_ip);
                }
                if let Ok(host_ip) = std::env::var("HOST_IP") {
                    metadata.insert("host_ip".to_string(), host_ip);
                }
                if let Ok(namespace) = std::env::var("POD_NAMESPACE") {
                    metadata.insert("namespace".to_string(), namespace);
                }
                if let Ok(pod_name) = std::env::var("HOSTNAME") {
                    metadata.insert("pod_name".to_string(), pod_name);
                }
            }
            
            // Register storage node with control plane
            match client.register_node(node_id, "storage", metadata).await {
                Ok(_) => {
                    info!("Successfully registered storage node {} with control plane", node_id);
                    info!("Arrow Flight endpoint: {}", arrow_flight_endpoint);
                    info!("gRPC query endpoint: {}", grpc_endpoint);
                }
                Err(e) => {
                    warn!("Failed to register storage node {} with control plane: {}", node_id, e);
                }
            }
        }
        Err(e) => {
            warn!("Failed to create control plane client for storage node {}: {}", node_id, e);
        }
    }
}

/// Start the indexer node with unified config
pub async fn start_indexer_node(config: UnifiedConfig, node_id: String, port: u16) -> Result<()> {
    info!("Starting Indexer Node: {}", node_id);
    info!("════════════════════════════════════════");
    
    // Build bind address from port
    let bind_address = format!("0.0.0.0:{}", port);
    info!("Binding to: {}", bind_address);
    
    // Gossip functionality deprecated - simplified indexer setup
    let indexer_config = config;
    let _cluster_handle: Option<openwit_network::ClusterHandle> = None;
    
    // Start indexer service
    info!("Indexer configuration:");
    info!("  - Node ID: {}", node_id);
    info!("  - Bind address: {}", bind_address);
    info!("  - Data path: {}/indexer", indexer_config.storage.local.path);
    info!("  - Max memory: {}GB", indexer_config.indexing.index.max_memory_mb as f64 / 1024.0);
    info!("  - Writer threads: {}", indexer_config.indexing.tantivy.writer_heap_size_mb as usize / 128);
    info!("  - Query threads: {}", indexer_config.indexing.worker_threads);
    
    // Convert unified config to indexer config
    let indexer_specific_config = openwit_indexer::config::IndexerConfig::from_unified(indexer_config.clone());
    
    // Start the indexer service
    openwit_indexer::indexer_main::start_indexer_service(
        indexer_specific_config,
        node_id,
        bind_address,
        "0.0.0.0:8080".to_string(), // HTTP metrics endpoint
    ).await?;
    
    Ok(())
}

/// Start the search node with unified config
pub async fn start_search_node(config: UnifiedConfig, node_id: String, port: u16) -> Result<()> {
    info!("Starting Search Node: {}", node_id);
    info!("════════════════════════════════════════");
    
    // Use port from config if not overridden
    let service_port = if port == 0 {
        // Use search service port 8083 to avoid conflict with storage on 8082
        8083
    } else {
        port
    };
    
    // Gossip functionality deprecated - simplified search setup
    let search_config = config;
    let _cluster_handle: Option<openwit_network::ClusterHandle> = None;
    
    // Start the search service
    info!("Search configuration:");
    info!("  HTTP port: {}", service_port);
    info!("  gRPC port: {}", search_config.search.grpc.port);
    info!("  Max concurrent requests: {}", search_config.search.http.max_concurrent_requests);
    info!("  Query timeout: {}s", search_config.search.query_engine.timeout_seconds);
    info!("  Caching: {}", if search_config.search.query_engine.enable_caching { "enabled" } else { "disabled" });
    
    if search_config.search.query_engine.enable_caching {
        info!("  Cache size: {}MB", search_config.search.query_engine.cache_size_mb);
        info!("  Cache TTL: {}s", search_config.search.query_engine.cache_ttl_seconds);
    }
    
    // Start the search service with the service port
    use openwit_search::search_main::run_search_service_v2;
    run_search_service_v2(search_config, node_id, service_port).await?;
    
    Ok(())
}

/// Start the janitor node with unified config
pub async fn start_janitor_node(config: UnifiedConfig, node_id: String, port: u16) -> Result<()> {
    info!("Starting Janitor Node: {}", node_id);
    info!("════════════════════════════════════════");
    info!("Service port: {} (janitor runs background tasks)", port);
    
    // Gossip functionality deprecated - simplified janitor setup
    let janitor_config = config;
    let _cluster_handle: Option<openwit_network::ClusterHandle> = None;
    
    // TODO: Create janitor when janitor module is fully implemented
    info!("Janitor configuration:");
    info!("  Run interval: {}s", janitor_config.janitor.intervals.run_interval_seconds);
    info!("  Compaction interval: {}s", janitor_config.janitor.intervals.compaction_interval_seconds);
    info!("  Retention check interval: {}s", janitor_config.janitor.intervals.retention_check_interval_seconds);
    info!("  WAL cleanup interval: {}s", janitor_config.janitor.intervals.wal_cleanup_interval_seconds);
    
    info!("Compaction policy:");
    info!("  Min files to compact: {}", janitor_config.janitor.compaction.min_files_to_compact);
    info!("  Max files per compaction: {}", janitor_config.janitor.compaction.max_files_per_compaction);
    info!("  Target file size: {}MB", janitor_config.janitor.compaction.target_file_size_mb);
    
    info!("Retention policy:");
    info!("  Retention period: {} days", janitor_config.janitor.cleanup.retention_period_days);
    info!("  WAL retention: {} hours", janitor_config.janitor.cleanup.wal_retention_hours);
    
    // Keep running
    tokio::signal::ctrl_c().await?;
    
    Ok(())
}

/// Start the Kafka consumer node with unified config
pub async fn start_kafka_node(config: UnifiedConfig, node_id: String) -> Result<()> {
    info!("Starting Kafka Consumer Node: {}", node_id);
    info!("════════════════════════════════════════");
    
    // Verify Kafka configuration
    if config.ingestion.kafka.brokers.is_none() {
        return Err(anyhow::anyhow!("Kafka brokers not configured"));
    }
    
    info!("Kafka configuration:");
    info!("  Brokers: {:?}", config.ingestion.kafka.brokers);
    info!("  Topics: {:?}", config.ingestion.kafka.topics);
    info!("  Group ID: {:?}", config.ingestion.kafka.group_id);
    info!("  Timeouts:");
    info!("    Session timeout: {}ms", config.ingestion.kafka.timeouts.session_timeout_ms);
    info!("    Heartbeat interval: {}ms", config.ingestion.kafka.timeouts.heartbeat_interval_ms);
    info!("    Poll timeout: {}ms", config.ingestion.kafka.timeouts.poll_timeout_ms);
    info!("    Batch timeout: {}ms", config.ingestion.kafka.timeouts.batch_timeout_ms);
    info!("  Batching:");
    info!("    Batch size: {}", config.ingestion.kafka.batching.batch_size);
    info!("    Min fetch bytes: {}", config.ingestion.kafka.batching.min_fetch_bytes);
    info!("    Max fetch bytes: {}", config.ingestion.kafka.batching.max_fetch_bytes);
    
    // Create and start Kafka server
    let kafka_server = openwit_kafka::KafkaServer::new(config);
    
    info!("Kafka consumer starting...");
    info!("Consumer will join group 'ingestion-group' and partitions will be rebalanced automatically");
    
    // Start server and wait
    kafka_server.start().await?;
    
    Ok(())
}

/// Initialize cluster networking from config (gossip deprecated)
async fn init_cluster_networking(_config: &UnifiedConfig) -> Result<openwit_network::ClusterHandle> {
    init_cluster_networking_with_role(_config, "control").await
}

/// Initialize cluster networking from config with specific role (gossip deprecated)
async fn init_cluster_networking_with_role(_config: &UnifiedConfig, role: &str) -> Result<openwit_network::ClusterHandle> {
    // Gossip functionality deprecated - return a dummy handle
    info!("Cluster networking disabled (gossip deprecated) for role: {}", role);
    
    // Create a minimal cluster runtime without gossip
    let node_name = format!("{}-{}", role, uuid::Uuid::new_v4().to_string().split('-').next().unwrap());
    let gossip_addr: SocketAddr = "127.0.0.1:7946".parse()?;
    let seed_nodes: Vec<SocketAddr> = Vec::new();
    
    let runtime = openwit_network::ClusterRuntime::start(
        &node_name,
        gossip_addr,
        seed_nodes,
        role,
    ).await?;
    
    Ok(runtime.handle())
}

/// Create storage node TOML config from unified config
fn create_storage_config_toml(
    config: &UnifiedConfig,
    node_id: &str,
    flight_port: u16,
    gossip_port: u16,
) -> Result<PathBuf> {
    use std::env;
    
    let temp_dir = env::temp_dir();
    let config_path = temp_dir.join(format!("storage-{}.toml", node_id));
    
    // Determine storage backend URL
    let artifact_uri = match config.storage.backend.as_str() {
        "s3" => format!("s3://{}", config.storage.s3.bucket),
        "azure" => format!("azblob://{}/{}", config.storage.azure.account_name, config.storage.azure.container_name),
        _ => format!("fs://{}", config.storage.local.path),
    };
    
    // Create postgres DSN - for local testing, we'll use a placeholder
    // In production, this should be properly configured
    let postgres_dsn = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| {
            info!("No DATABASE_URL found, using placeholder for local testing");
            // This will fail but allows us to see other initialization issues
            "postgres://localhost:5432/openwit_placeholder".to_string()
        });
    
    let toml_content = format!(r#"
[general]
node_id = "{}"
mode = "local"
work_dir = "{}/storage/{}"

[postgres]
dsn = "{}"

[cluster]
gossip_bind = "0.0.0.0:{}"
health_push_interval_ms = 3000
control_request_timeout_ms = 1500

[flight]
bind_addr = "0.0.0.0:{}"
external_endpoint = "{}"
max_in_flight_batches = 4096
max_batch_bytes = 32000000

[lsm]
memtable_max_bytes = 256000000
memtable_max_ms = 30000
row_group_target_mb = 128
file_target_mb = 512
sort_key = "ts,service.name,severity,trace_id"
ram_retention_ms = 3600000
disk_retention_ms = 86400000

[parquet]
compression = "zstd"
zstd_level = 4
enable_page_index = true
enable_bloom = true
bloom_columns = ["trace_id", "span_id"]

[opendal]
artifact_uri = "{}"
region = "auto"
multipart_threshold_mb = 64
max_concurrency = 8

[publish]
emit_event_bus = true
publish_batch_size = 100

[compactor]
enabled = true
compaction_interval_ms = 300000
min_compaction_size_mb = 100
max_compaction_size_mb = 1000

[gossip]
enabled = true
bind_addr = "0.0.0.0:{}"
advertise_address = "{}"
seed_nodes = {:?}
interval_ms = 5000
"#,
        node_id,
        config.storage.local.path,
        node_id,
        postgres_dsn,
        gossip_port,
        flight_port,
        // Determine external endpoint for Arrow Flight
        if let Some(storage_endpoint) = &config.networking.service_endpoints.storage {
            // Extract hostname and use with flight port
            if let Some(host) = storage_endpoint.strip_prefix("http://").or_else(|| storage_endpoint.strip_prefix("https://")) {
                let service_name = host.split(':').next().unwrap_or(host).split('/').next().unwrap_or(host);
                format!("grpc://{}:{}", service_name, flight_port)
            } else {
                format!("grpc://{}:{}", storage_endpoint, flight_port)
            }
        } else {
            // Fallback to advertise address
            format!("grpc://{}:{}", config.networking.get_service_advertise_address("storage"), flight_port)
        },
        artifact_uri,
        gossip_port,
        config.networking.get_service_advertise_address("storage"),
        Vec::<String>::new(), // Empty seed nodes since gossip is deprecated
    );
    
    let mut file = std::fs::File::create(&config_path)
        .context("Failed to create storage config file")?;
    file.write_all(toml_content.as_bytes())
        .context("Failed to write storage config")?;
    
    Ok(config_path)
}

/// Initialize cluster networking with a specific node ID (gossip deprecated)
async fn init_cluster_networking_with_node_id(_config: &UnifiedConfig, role: &str, node_id: &str) -> Result<openwit_network::ClusterHandle> {
    info!("Cluster networking disabled (gossip deprecated)");
    info!("  Node name: {}", node_id);
    info!("  Role: {}", role);
    
    // Create a minimal cluster runtime without gossip
    let gossip_addr: SocketAddr = "127.0.0.1:7946".parse()?;
    let seed_nodes: Vec<SocketAddr> = Vec::new();
    
    let runtime = openwit_network::ClusterRuntime::start(
        node_id,
        gossip_addr,
        seed_nodes,
        role,
    ).await?;
    
    Ok(runtime.handle())
}

/// Start the real Arrow Flight storage server
async fn start_arrow_flight_storage_server(
    config: UnifiedConfig,
    node_id: String,
    port: u16,
    cluster_handle: openwit_network::ClusterHandle,
) -> Result<()> {
    use arrow_flight::flight_service_server::FlightServiceServer;
    use tonic::transport::Server;
    
    info!("Starting Arrow Flight storage server on port {}", port);
    
    // Ensure storage directories exist before initialization
    let storage_dir = std::path::Path::new(&config.storage.data_dir);
    if !storage_dir.exists() {
        info!("Creating storage directory: {:?}", storage_dir);
        std::fs::create_dir_all(&storage_dir)
            .context("Failed to create storage directory")?;
    }
    
    // Also ensure the local path exists if it's different
    if config.storage.local.path != config.storage.data_dir {
        let local_dir = std::path::Path::new(&config.storage.local.path);
        if !local_dir.exists() {
            info!("Creating local storage directory: {:?}", local_dir);
            std::fs::create_dir_all(&local_dir)
                .context("Failed to create local storage directory")?;
        }
    }
    
    // Create storage service using proper StorageProcessor with better error handling
    let (storage_service, storage_processor) = match StorageFlightService::new(
        node_id.clone(),
        config.clone(),
        cluster_handle.clone(),
    ).await {
        Ok(result) => {
            info!("Successfully created StorageFlightService");
            result
        }
        Err(e) => {
            error!("Failed to create StorageFlightService: {}", e);
            error!("This is likely due to initialization issues. Check that:");
            error!("  - Storage data directory {} exists and is writable", config.storage.data_dir);
            error!("  - Local storage path {} exists and is writable", config.storage.local.path);
            error!("  - All required subdirectories can be created");
            error!("  - No port conflicts on {}", port);
            return Err(anyhow::anyhow!("Storage service initialization failed: {}", e));
        }
    };
    
    // Start the storage processor in the background
    let _processor_handle = tokio::spawn(async move {
        if let Err(e) = storage_processor.run().await {
            error!("Storage processor error: {}", e);
        }
    });
    
    // Create the Arrow Flight server
    let flight_server = FlightServiceServer::new(storage_service);
    
    // Spawn the server in the background
    let addr = format!("0.0.0.0:{}", port).parse()?;
    let server_ready = Arc::new(tokio::sync::Notify::new());
    let server_ready_clone = server_ready.clone();
    
    tokio::spawn(async move {
        info!("Starting Arrow Flight server on {}", addr);
        
        // Build the server
        let server = Server::builder().add_service(flight_server);
        
        // Notify that server is ready before starting to serve
        server_ready_clone.notify_one();
        
        info!("Arrow Flight server listening on {}", addr);
        if let Err(e) = server.serve(addr).await {
            error!("Arrow Flight server error: {}", e);
        }
    });
    
    // Wait for the server to be ready
    server_ready.notified().await;
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    
    // Verify the server is actually listening
    info!("Verifying Arrow Flight server is accessible on port {}", port);
    match tokio::net::TcpStream::connect(format!("127.0.0.1:{}", port)).await {
        Ok(_) => {
            info!("Arrow Flight server verified on port {}", port);
            info!("Storage node is ready to receive Arrow Flight connections");
        }
        Err(e) => {
            error!("Arrow Flight server not accessible on port {}: {}", port, e);
            error!("The server spawned but might not be listening correctly");
            // Don't fail here, as it might be a transient issue
        }
    }
    
    // Start health reporting
    start_storage_health_reporter(&config, node_id, port, cluster_handle);
    
    Ok(())
}

/// Start health reporting for storage node
fn start_storage_health_reporter(
    config: &UnifiedConfig,
    node_id: String,
    flight_port: u16,
    _cluster_handle: openwit_network::ClusterHandle,
) {
    let config_clone = config.clone();
    
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
        
        // Wait a bit for initialization
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        
        loop {
            interval.tick().await;
            
            // Create control plane client using unified config approach
            // This will use the control plane endpoint from the config rather than gossip discovery
            match openwit_control_plane::client::ControlPlaneClient::new("storage-health-reporter", &config_clone).await {
                Ok(mut client) => {
                    // Determine the correct endpoint based on environment
                    let arrow_flight_endpoint = if std::env::var("KUBERNETES_SERVICE_HOST").is_ok() {
                        // In Kubernetes, we need to determine the correct service name
                        // First try environment variable, then use service endpoints from config
                        if let Ok(service_name) = std::env::var("STORAGE_SERVICE_NAME") {
                            info!("Using Kubernetes service name for Arrow Flight endpoint: {}", service_name);
                            format!("grpc://{}:{}", service_name, flight_port)
                        } else if let Some(storage_endpoint) = &config_clone.networking.service_endpoints.storage {
                            // Extract hostname from the storage service endpoint
                            if let Some(host) = storage_endpoint.strip_prefix("http://").or_else(|| storage_endpoint.strip_prefix("https://")) {
                                let service_name = host.split(':').next().unwrap_or(host).split('/').next().unwrap_or(host);
                                info!("Using storage service endpoint from config: {}", service_name);
                                format!("grpc://{}:{}", service_name, flight_port)
                            } else {
                                format!("grpc://{}:{}", storage_endpoint, flight_port)
                            }
                        } else {
                            // Fallback to pod IP
                            let pod_ip = std::env::var("POD_IP").unwrap_or_else(|_| "localhost".to_string());
                            warn!("No storage service name configured, using pod IP: {}", pod_ip);
                            format!("grpc://{}:{}", pod_ip, flight_port)
                        }
                    } else {
                        // Use the service-specific advertise address from networking config
                        let advertise_addr = config_clone.networking.get_service_advertise_address("storage");
                        format!("grpc://{}:{}", advertise_addr, flight_port)
                    };
                    
                    // Create health report using the proto types with pod info
                    let mut metadata = std::collections::HashMap::new();
                    metadata.insert("arrow_flight_endpoint".to_string(), arrow_flight_endpoint.clone());
                    metadata.insert("status".to_string(), "accepting".to_string());
                    
                    // Add gRPC query service endpoint
                    let grpc_query_port = std::env::var("STORAGE_GRPC_QUERY_PORT")
                        .unwrap_or_else(|_| "8083".to_string())
                        .parse::<u16>()
                        .unwrap_or(8083); // Storage query service port
                    let grpc_endpoint = if std::env::var("KUBERNETES_SERVICE_HOST").is_ok() {
                        // In Kubernetes, use service name
                        if let Ok(service_name) = std::env::var("STORAGE_SERVICE_NAME") {
                            format!("{}:{}", service_name, grpc_query_port)
                        } else if let Some(storage_endpoint) = &config_clone.networking.service_endpoints.storage {
                            // Extract hostname from the storage service endpoint
                            if let Some(host) = storage_endpoint.strip_prefix("http://").or_else(|| storage_endpoint.strip_prefix("https://")) {
                                let service_name = host.split(':').next().unwrap_or(host).split('/').next().unwrap_or(host);
                                format!("{}:{}", service_name, grpc_query_port)
                            } else {
                                format!("{}:{}", storage_endpoint, grpc_query_port)
                            }
                        } else {
                            // Fallback to pod IP
                            let pod_ip = std::env::var("POD_IP").unwrap_or_else(|_| "localhost".to_string());
                            format!("{}:{}", pod_ip, grpc_query_port)
                        }
                    } else {
                        // Use the service-specific advertise address from networking config
                        let advertise_addr = config_clone.networking.get_service_advertise_address("storage");
                        format!("{}:{}", advertise_addr, grpc_query_port)
                    };
                    metadata.insert("grpc_endpoint".to_string(), grpc_endpoint.clone());
                    info!("Storage node registering with gRPC query endpoint: {}", grpc_endpoint);
                    
                    // Add pod IP information for Kubernetes
                    if std::env::var("KUBERNETES_SERVICE_HOST").is_ok() {
                        if let Ok(pod_ip) = std::env::var("POD_IP") {
                            metadata.insert("pod_ip".to_string(), pod_ip);
                        }
                        if let Ok(host_ip) = std::env::var("HOST_IP") {
                            metadata.insert("host_ip".to_string(), host_ip);
                        }
                        // Add namespace information
                        if let Ok(namespace) = std::env::var("POD_NAMESPACE") {
                            metadata.insert("namespace".to_string(), namespace);
                        }
                    }
                    
                    // Register storage node with control plane
                    match client.register_node(&node_id, "storage", metadata).await {
                        Ok(_) => {
                            info!("Successfully registered storage node with control plane at endpoint: {}", grpc_endpoint);
                        }
                        Err(e) => {
                            warn!("Failed to register storage node with control plane: {}", e);
                        }
                    }
                }
                Err(e) => {
                    warn!("Failed to create control plane client: {}", e);
                }
            }
        }
    });
}

/// Register ingestion node with control plane
async fn register_ingestion_node_with_control_plane(
    client: &mut openwit_control_plane::client::ControlPlaneClient,
    config: &UnifiedConfig,
    node_id: &str,
    grpc_port: u16,
) {
    // Determine the correct ingestion endpoint based on environment
    let ingestion_endpoint = if std::env::var("KUBERNETES_SERVICE_HOST").is_ok() {
        // In Kubernetes, use service name
        if let Ok(service_name) = std::env::var("INGESTION_SERVICE_NAME") {
            info!("Using Kubernetes service name for ingestion endpoint: {}", service_name);
            format!("http://{}:{}", service_name, grpc_port)
        } else if let Some(ingestion_endpoint) = &config.networking.service_endpoints.ingestion {
            // Extract hostname from the ingestion service endpoint
            if let Some(host) = ingestion_endpoint.strip_prefix("http://").or_else(|| ingestion_endpoint.strip_prefix("https://")) {
                let service_name = host.split(':').next().unwrap_or(host).split('/').next().unwrap_or(host);
                info!("Using ingestion service endpoint from config: {}", service_name);
                format!("http://{}:{}", service_name, grpc_port)
            } else {
                format!("http://{}:{}", ingestion_endpoint, grpc_port)
            }
        } else {
            // Fallback to pod IP
            let pod_ip = std::env::var("POD_IP").unwrap_or_else(|_| "localhost".to_string());
            warn!("No ingestion service name configured, using pod IP: {}", pod_ip);
            format!("http://{}:{}", pod_ip, grpc_port)
        }
    } else {
        // Use the service-specific advertise address from networking config
        let advertise_addr = config.networking.get_service_advertise_address("ingestion");
        format!("http://{}:{}", advertise_addr, grpc_port)
    };
    
    // Create registration metadata
    let mut metadata = std::collections::HashMap::new();
    metadata.insert("grpc_endpoint".to_string(), ingestion_endpoint.clone());
    metadata.insert("status".to_string(), "accepting".to_string());
    metadata.insert("service_type".to_string(), "ingestion".to_string());
    metadata.insert("grpc_port".to_string(), grpc_port.to_string());
    
    // Add Arrow Flight endpoint for ingestion
    let flight_port = config.service_ports.ingestion.arrow_flight;
    let arrow_flight_endpoint = ingestion_endpoint.replace(&grpc_port.to_string(), &flight_port.to_string()).replace("http://", "grpc://");
    metadata.insert("arrow_flight_endpoint".to_string(), arrow_flight_endpoint);
    
    // Add pod information if in Kubernetes
    if std::env::var("KUBERNETES_SERVICE_HOST").is_ok() {
        if let Ok(pod_ip) = std::env::var("POD_IP") {
            metadata.insert("pod_ip".to_string(), pod_ip);
        }
        if let Ok(pod_name) = std::env::var("HOSTNAME") {
            metadata.insert("pod_name".to_string(), pod_name);
        }
    }
    
    // Register with control plane
    match client.register_node(node_id, "ingest", metadata).await {
        Ok(_) => {
            info!("✅ Successfully registered ingestion node {} with control plane", node_id);
            info!("   gRPC endpoint: {}", ingestion_endpoint);
        }
        Err(e) => {
            warn!("⚠️ Failed to register ingestion node {} with control plane: {}", node_id, e);
        }
    }
}

// Simplified storage node implementation for testing
use arrow_flight::{
    flight_service_server::FlightService,
    FlightData, FlightDescriptor, FlightInfo, HandshakeRequest, HandshakeResponse,
    PutResult, SchemaResult, Ticket, Action, ActionType, Criteria, Empty, PollInfo,
};
use futures::stream::BoxStream;
use arrow::record_batch::RecordBatch;

// Use storage crate's processor instead of local parquet module
use openwit_storage::storage_processor::StorageProcessor;
use openwit_inter_node::ArrowFlightBatch;

/// Concatenate multiple record batches into one
fn concat_record_batches(batches: &[RecordBatch]) -> Result<RecordBatch> {
    if batches.is_empty() {
        return Err(anyhow::anyhow!("No batches to concatenate"));
    }
    
    let schema = batches[0].schema();
    let mut arrays: Vec<Vec<Arc<dyn arrow::array::Array>>> = vec![];
    
    // Initialize arrays for each column
    for _ in 0..schema.fields().len() {
        arrays.push(Vec::new());
    }
    
    // Collect arrays from each batch
    for batch in batches {
        for (i, column) in batch.columns().iter().enumerate() {
            arrays[i].push(column.clone());
        }
    }
    
    // Concatenate arrays for each column
    let mut columns = Vec::new();
    for (_i, field_arrays) in arrays.iter().enumerate() {
        let refs: Vec<&dyn arrow::array::Array> = field_arrays.iter()
            .map(|a| a.as_ref())
            .collect();
        let concatenated = arrow::compute::concat(&refs)?;
        columns.push(concatenated);
    }
    
    Ok(RecordBatch::try_new(schema, columns)?)
}

/// Arrow Flight storage service using proper StorageProcessor
struct StorageFlightService {
    #[allow(dead_code)]
    storage_processor: Arc<StorageProcessor>,
    batch_sender: mpsc::Sender<ArrowFlightBatch>,
}

impl StorageFlightService {
    async fn new(
        node_id: String,
        config: UnifiedConfig,
        _cluster_handle: openwit_network::ClusterHandle,
    ) -> Result<(Self, StorageProcessor)> {
        // Extract necessary config for simplified StorageProcessor
        let data_dir = config.storage.local.path.clone();
        
        let arrow_flight_port = config.service_ports.storage.arrow_flight;
        
        // Create storage processor with simplified API
        let (processor, batch_sender) = StorageProcessor::new_with_config(
            node_id.clone(),
            data_dir.clone(),
            arrow_flight_port,
            Some(&config),
        ).await?;
        
        // Create a second processor for the service (since we need two instances)
        let (processor2, _) = StorageProcessor::new_with_config(
            node_id,
            data_dir,
            arrow_flight_port + 1, // Use different port for second instance
            Some(&config),
        ).await?;
        
        Ok((Self {
            storage_processor: Arc::new(processor2),
            batch_sender,
        }, processor))
    }
}

#[tonic::async_trait]
impl FlightService for StorageFlightService {
    type HandshakeStream = BoxStream<'static, std::result::Result<HandshakeResponse, tonic::Status>>;
    type ListFlightsStream = BoxStream<'static, std::result::Result<FlightInfo, tonic::Status>>;
    type DoGetStream = BoxStream<'static, std::result::Result<FlightData, tonic::Status>>;
    type DoPutStream = BoxStream<'static, std::result::Result<PutResult, tonic::Status>>;
    type DoActionStream = BoxStream<'static, std::result::Result<arrow_flight::Result, tonic::Status>>;
    type ListActionsStream = BoxStream<'static, std::result::Result<ActionType, tonic::Status>>;
    type DoExchangeStream = BoxStream<'static, std::result::Result<FlightData, tonic::Status>>;

    async fn handshake(
        &self,
        _request: tonic::Request<tonic::Streaming<HandshakeRequest>>,
    ) -> std::result::Result<tonic::Response<Self::HandshakeStream>, tonic::Status> {
        Err(tonic::Status::unimplemented("Handshake not implemented"))
    }

    async fn list_flights(
        &self,
        _request: tonic::Request<Criteria>,
    ) -> std::result::Result<tonic::Response<Self::ListFlightsStream>, tonic::Status> {
        Err(tonic::Status::unimplemented("ListFlights not implemented"))
    }

    async fn get_flight_info(
        &self,
        _request: tonic::Request<FlightDescriptor>,
    ) -> std::result::Result<tonic::Response<FlightInfo>, tonic::Status> {
        Err(tonic::Status::unimplemented("GetFlightInfo not implemented"))
    }

    async fn poll_flight_info(
        &self,
        _request: tonic::Request<FlightDescriptor>,
    ) -> std::result::Result<tonic::Response<PollInfo>, tonic::Status> {
        Err(tonic::Status::unimplemented("PollFlightInfo not implemented"))
    }

    async fn get_schema(
        &self,
        _request: tonic::Request<FlightDescriptor>,
    ) -> std::result::Result<tonic::Response<SchemaResult>, tonic::Status> {
        Err(tonic::Status::unimplemented("GetSchema not implemented"))
    }

    async fn do_get(
        &self,
        _request: tonic::Request<Ticket>,
    ) -> std::result::Result<tonic::Response<Self::DoGetStream>, tonic::Status> {
        Err(tonic::Status::unimplemented("DoGet not implemented"))
    }

    async fn do_put(
        &self,
        request: tonic::Request<tonic::Streaming<FlightData>>,
    ) -> std::result::Result<tonic::Response<Self::DoPutStream>, tonic::Status> {
        use futures::StreamExt;
        use tokio::sync::mpsc;
        
        let mut stream = request.into_inner();
        let batch_sender = self.batch_sender.clone();
        
        // Create response channel
        let (tx, rx) = mpsc::channel(16);
        
        tokio::spawn(async move {
            let mut batch_id = String::new();
            let mut source_node = String::new();
            let mut metadata = std::collections::HashMap::new();
            let mut schema: Option<Arc<arrow::datatypes::Schema>> = None;
            let mut received_batches = Vec::new();
            
            while let Some(result) = stream.next().await {
                match result {
                    Ok(data) => {
                        // Extract metadata from first message
                        if batch_id.is_empty() && data.flight_descriptor.is_some() {
                            if let Some(descriptor) = &data.flight_descriptor {
                                if let Ok(cmd_str) = String::from_utf8(descriptor.cmd.to_vec()) {
                                    for pair in cmd_str.split(';') {
                                        if let Some((key, value)) = pair.split_once('=') {
                                            metadata.insert(key.to_string(), value.to_string());
                                            if key == "batch_id" {
                                                batch_id = value.to_string();
                                            } else if key == "source_node" {
                                                source_node = value.to_string();
                                            }
                                        }
                                    }
                                }
                            }
                            
                            // Also extract metadata from app_metadata if available
                            if !data.app_metadata.is_empty() {
                                match serde_json::from_slice::<HashMap<String, String>>(&data.app_metadata) {
                                    Ok(app_meta) => {
                                        info!("Found app_metadata with {} entries", app_meta.len());
                                        // Merge app_metadata into metadata, preserving existing values
                                        for (key, value) in app_meta {
                                            metadata.insert(key, value);
                                        }
                                    }
                                    Err(e) => {
                                        warn!("Failed to parse app_metadata as JSON: {}", e);
                                    }
                                }
                            }
                            
                            info!(
                                "Storage receiving batch: batch_id={}, metadata={:?}",
                                batch_id, metadata
                            );
                        }
                        
                        // Try to extract schema
                        if schema.is_none() && !data.data_header.is_empty() {
                            info!("Attempting to extract schema from data_header (length: {})", data.data_header.len());
                            if let Ok(message) = arrow::ipc::root_as_message(&data.data_header) {
                                info!("Successfully parsed IPC message, header type: {:?}", message.header_type());
                                if message.header_type() == arrow::ipc::MessageHeader::Schema {
                                    if let Some(header) = message.header_as_schema() {
                                        let decoded_schema = arrow::ipc::convert::fb_to_schema(header);
                                        info!("Successfully decoded schema with {} fields", decoded_schema.fields().len());
                                        schema = Some(Arc::new(decoded_schema));
                                        continue;
                                    }
                                }
                            } else {
                                warn!("Failed to parse IPC message from data_header");
                            }
                        }
                        
                        // Decode RecordBatch
                        if let Some(ref schema_arc) = schema {
                            info!("Attempting to decode record batch with existing schema");
                            match arrow_flight::utils::flight_data_to_arrow_batch(
                                &data,
                                schema_arc.clone(),
                                &Default::default(),
                            ) {
                                Ok(batch) => {
                                    info!("Successfully decoded batch with {} rows", batch.num_rows());
                                    received_batches.push(batch);
                                    
                                    // Send ack
                                    let _ = tx.send(Ok(PutResult {
                                        app_metadata: bytes::Bytes::new(),
                                    })).await;
                                }
                                Err(e) => {
                                    warn!("Failed to decode record batch: {}", e);
                                }
                            }
                        } else {
                            info!("No schema available yet, skipping batch decoding");
                        }
                    }
                    Err(e) => {
                        error!("Stream error: {}", e);
                        break;
                    }
                }
            }
            
            // Send combined batch to storage processor
            if !batch_id.is_empty() && !received_batches.is_empty() {
                info!("Attempting to combine {} record batches for batch {}", received_batches.len(), batch_id);
                if let Ok(combined_batch) = concat_record_batches(&received_batches) {
                    info!("Successfully combined batches. Total rows: {}, columns: {}", 
                        combined_batch.num_rows(), combined_batch.num_columns());
                    
                    // Set source_node if not provided
                    if source_node.is_empty() {
                        source_node = metadata.get("source_node").cloned().unwrap_or_else(|| "unknown".to_string());
                    }
                    
                    let arrow_batch = ArrowFlightBatch {
                        batch_id: batch_id.clone(),
                        source_node,
                        target_node: String::new(), // Will be filled by storage processor
                        record_batch: combined_batch,
                        metadata,
                    };
                    
                    if let Err(e) = batch_sender.send(arrow_batch).await {
                        error!("Failed to send batch to storage processor: {}", e);
                    } else {
                        info!("Successfully sent batch {} to storage processor", batch_id);
                    }
                } else {
                    error!("Failed to combine record batches for batch {}", batch_id);
                }
            } else {
                warn!("Skipping empty batch or batch without ID. batch_id: '{}', batches: {}", batch_id, received_batches.len());
            }
        });
        
        Ok(tonic::Response::new(Box::pin(
            tokio_stream::wrappers::ReceiverStream::new(rx)
        )))
    }

    async fn do_action(
        &self,
        _request: tonic::Request<Action>,
    ) -> std::result::Result<tonic::Response<Self::DoActionStream>, tonic::Status> {
        Err(tonic::Status::unimplemented("DoAction not implemented"))
    }

    async fn list_actions(
        &self,
        _request: tonic::Request<Empty>,
    ) -> std::result::Result<tonic::Response<Self::ListActionsStream>, tonic::Status> {
        Err(tonic::Status::unimplemented("ListActions not implemented"))
    }

    async fn do_exchange(
        &self,
        _request: tonic::Request<tonic::Streaming<FlightData>>,
    ) -> std::result::Result<tonic::Response<Self::DoExchangeStream>, tonic::Status> {
        Err(tonic::Status::unimplemented("DoExchange not implemented"))
    }
}

/// Start health reporting for ingestion node
fn start_ingestion_health_reporter(
    config: &UnifiedConfig,
    node_id: String,
    grpc_port: u16,
) {
    let config_clone = config.clone();
    
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
        
        loop {
            interval.tick().await;
            
            // Create control plane client
            match openwit_control_plane::client::ControlPlaneClient::new("ingest-health-reporter", &config_clone).await {
                Ok(mut client) => {
                    // Calculate advertise address - use pod IP in Kubernetes, localhost for local dev
                    let advertise_addr = if std::env::var("KUBERNETES_SERVICE_HOST").is_ok() {
                        // In Kubernetes, get pod IP
                        std::env::var("POD_IP").unwrap_or_else(|_| {
                            // Fallback: try to get hostname or use localhost
                            std::env::var("HOSTNAME").unwrap_or_else(|_| "localhost".to_string())
                        })
                    } else {
                        "localhost".to_string()
                    };
                    let grpc_endpoint = format!("http://{}:{}", advertise_addr, grpc_port);
                    
                    // Create health report with gRPC endpoint and pod info
                    let mut metadata = std::collections::HashMap::new();
                    metadata.insert("grpc_endpoint".to_string(), grpc_endpoint.clone());
                    metadata.insert("status".to_string(), "accepting".to_string());
                    metadata.insert("grpc_port".to_string(), grpc_port.to_string());
                    
                    // Add pod IP information for Kubernetes
                    if std::env::var("KUBERNETES_SERVICE_HOST").is_ok() {
                        if let Ok(pod_ip) = std::env::var("POD_IP") {
                            metadata.insert("pod_ip".to_string(), pod_ip);
                        }
                        if let Ok(host_ip) = std::env::var("HOST_IP") {
                            metadata.insert("host_ip".to_string(), host_ip);
                        }
                        // Add namespace information
                        if let Ok(namespace) = std::env::var("POD_NAMESPACE") {
                            metadata.insert("namespace".to_string(), namespace);
                        }
                    }
                    
                    // Register ingestion node with control plane
                    match client.register_node(&node_id, "ingest", metadata).await {
                        Ok(_) => {
                            info!("Successfully registered ingestion node with control plane at endpoint: {}", grpc_endpoint);
                        }
                        Err(e) => {
                            warn!("Failed to register ingestion node with control plane: {}", e);
                        }
                    }
                }
                Err(e) => {
                    warn!("Failed to create control plane client for health reporting: {}", e);
                }
            }
        }
    });
}
