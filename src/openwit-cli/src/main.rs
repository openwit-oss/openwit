use clap::{Parser, Subcommand};
use tracing_subscriber::{fmt, EnvFilter};
use tracing_subscriber::prelude::*;
use std::env;
use std::path::Path;
use anyhow::{Result};
use tracing::{info, debug, warn, error};

// Import the config loader
use openwit_cli::config_loader::ConfigLoader;
use openwit_cli::ingestion_type::IngestionType;
use openwit_cli::node_startup;
use openwit_http::control_plane_integration::ControlPlaneIntegration;

/// Detects if the application is running in a Kubernetes environment
fn detect_kubernetes_environment() -> bool {
    if Path::new("/var/run/secrets/kubernetes.io/serviceaccount/token").exists() {
        return true;
    }
    
    if env::var("KUBERNETES_PORT").is_ok() ||
       env::var("KUBERNETES_PORT_443_TCP").is_ok() ||
       env::var("KUBERNETES_SERVICE_PORT").is_ok() ||
       env::var("KUBERNETES_SERVICE_PORT_HTTPS").is_ok() {
        return true;
    }
    
    // Check if running with a Kubernetes-style hostname
    if let Ok(hostname) = env::var("HOSTNAME") {
        if hostname.contains('-') && (hostname.len() > 20 || hostname.chars().filter(|c| *c == '-').count() >= 2) {
            // This is a heuristic - pods often have longer names with multiple hyphens
            return true;
        }
    }
    
    false
}

#[derive(Parser)]
#[command(name = "openwit", about = "OpenWit Distributed Log Ingestion System", version, author)]
struct Args {
    #[arg(short, long)]
    config: Option<String>,

    #[arg(long)]
    create_sample_config: bool,

    #[arg(short, long, value_enum, default_value = "grpc")]
    ingestion: IngestionType,

    #[arg(long)]
    kafka_brokers: Option<String>,

    #[arg(long)]
    grpc_port: Option<u16>,

    #[arg(long)]
    http_port: Option<u16>,

    #[command(subcommand)]
    node_type: NodeCommand,
}

#[derive(Subcommand)]
enum NodeCommand {
    Control {
        #[arg(long)]
        port: Option<u16>,
        #[arg(long)]
        node_id: Option<String>,
    },

    Proxy {
        #[arg(long, default_value = "8080")]
        port: u16,
        #[arg(long)]
        node_id: Option<String>,
    },

    Ingest {
        #[arg(long)]
        port: Option<u16>,
        #[arg(long)]
        node_id: Option<String>,
        #[arg(long)]
        grpc_bind: Option<String>,

        #[arg(long)]
        force_grpc: bool,
    },

    Grpc {
        #[arg(long)]
        port: Option<u16>,
        #[arg(long)]
        node_id: Option<String>,
        #[arg(long)]
        grpc_bind: Option<String>,
    },

    Storage {
        #[arg(long, default_value = "8081")]
        port: u16,
        #[arg(long)]
        node_id: Option<String>,
    },

    Indexer {
        #[arg(long, default_value = "50060")]
        port: u16,
        #[arg(long)]
        node_id: Option<String>,
    },

    Search {
        #[arg(long)]
        port: Option<u16>,
        #[arg(long)]
        node_id: Option<String>,
    },

    Janitor {
        #[arg(long, default_value = "9090")]
        port: u16,
        #[arg(long)]
        node_id: Option<String>,
    },

    Kafka {
        #[arg(long)]
        node_id: Option<String>,
        #[arg(short, long)]
        debug: bool,
    },

    Http {
        #[arg(long, default_value = "0.0.0.0:9087")]
        bind: String,
        #[arg(long)]
        node_id: Option<String>,
        #[arg(long, default_value = "5000")]
        max_concurrent_requests: u32,
        #[arg(long, default_value = "30000")]
        request_timeout_ms: u64,
        #[arg(long, default_value = "100")]
        max_payload_size_mb: u32,
    },
}


fn generate_node_id(node_type: &str) -> String {
    let hostname = env::var("HOSTNAME").unwrap_or_else(|_| "localhost".to_string());
    format!("{}-{}-{}", node_type, hostname, uuid::Uuid::new_v4().to_string().split('-').next().unwrap())
}

#[tokio::main]
async fn main() -> Result<()> {
    // Setup tracing
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(env_filter)
        .init();
    
    let args = Args::parse();
    
    // Handle sample config creation
    if args.create_sample_config {
        match ConfigLoader::create_sample_config("openwit-config-sample.yaml") {
            Ok(_) => {
                return Ok(());
            }
            Err(e) => {
                error!("Failed to create sample configuration: {}", e);
                return Err(e);
            }
        }
    }

    let node_type = args.node_type;

    // Detect environment
    let is_kubernetes = detect_kubernetes_environment();
    let environment = if is_kubernetes {
        info!("Kubernetes environment detected");
        "kubernetes"
    } else {
        info!("Local environment detected");
        "local"
    };
    
    // Load configuration using the config loader
    info!("Initializing configuration loader...");
    let loader = ConfigLoader::new();
    
    let mut config = match loader.load_with_env_overrides_no_validation(args.config.as_deref()).await {
        Ok(cfg) => {
            info!("Congratulations! Configuration found and loaded successfully");
            debug!("Configuration environment: {}", cfg.environment);
            debug!("Deployment mode: {}", cfg.deployment.mode);
            cfg
        }
        Err(e) => {
            error!("Configuration loading failed: {}", e);
            error!("Please check that the configuration file exists and is valid YAML");
            return Err(e);
        }
    };
    
    // Override environment if detected differently
    if config.environment != environment {
        warn!("Configuration environment '{}' differs from detected environment '{}', using detected", 
             config.environment, environment);
        config.environment = environment.to_string();
    }

    // For local environment, adjust deployment settings for distributed mode
    if environment == "local" {
        info!("Adjusting deployment settings for local distributed mode");
        config.deployment.mode = "standalone".to_string();
        config.deployment.kubernetes.enabled = false;
    }

    // Apply ingestion type to configuration
    // For proxy nodes, override to enable all ingestion types
    let effective_ingestion_type = match &node_type {
        NodeCommand::Proxy { .. } => {
            info!("Proxy node detected, overriding ingestion type to 'all'");
            IngestionType::All
        }
        _ => args.ingestion
    };
    
    info!("Applying ingestion type: {:?} - {}", effective_ingestion_type, effective_ingestion_type.description());
    effective_ingestion_type.apply_to_config(&mut config)?;
    
    // For distributed nodes (non-monolith, non-proxy), adjust the ingestion meaning
    match &node_type {
        NodeCommand::Control { .. } | NodeCommand::Ingest { .. } | NodeCommand::Grpc { .. } |
        NodeCommand::Storage { .. } | NodeCommand::Indexer { .. } | 
        NodeCommand::Search { .. } | NodeCommand::Janitor { .. } => {
            info!("Note: For {} node, ingestion type defines inter-node communication protocol", 
                match &node_type {
                    NodeCommand::Control { .. } => "control",
                    NodeCommand::Ingest { .. } => "ingest",
                    NodeCommand::Grpc { .. } => "grpc",
                    NodeCommand::Storage { .. } => "storage",
                    NodeCommand::Indexer { .. } => "indexer",
                    NodeCommand::Search { .. } => "search",
                    NodeCommand::Janitor { .. } => "janitor",
                    _ => "unknown",
                });
        }
        _ => {}
    }
    
    // Override Kafka brokers if provided via command line
    if let Some(brokers) = args.kafka_brokers {
        if matches!(args.ingestion, IngestionType::Kafka | IngestionType::All) {
            info!("Overriding Kafka brokers from command line: {}", brokers);
            config.ingestion.kafka.brokers = Some(brokers);
        } else {
            warn!("--kafka-brokers was provided but ingestion type {:?} doesn't use Kafka", args.ingestion);
        }
    }
    
    // Override gRPC port if provided via command line
    if let Some(port) = args.grpc_port {
        if matches!(args.ingestion, IngestionType::Grpc | IngestionType::All) {
            info!("Overriding gRPC port from command line: {}", port);
            config.ingestion.grpc.port = port;
            // Also update the bind address if it's using the default port
            if config.ingestion.grpc.bind.ends_with(":4317") || config.ingestion.grpc.bind.ends_with(":4321") {
                config.ingestion.grpc.bind = format!("0.0.0.0:{}", port);
            }
        } else {
            warn!("--grpc-port was provided but ingestion type {:?} doesn't use gRPC", args.ingestion);
        }
    }
    
    // Override HTTP port if provided via command line
    if let Some(port) = args.http_port {
        if matches!(args.ingestion, IngestionType::Http | IngestionType::All) {
            info!("Overriding HTTP port from command line: {}", port);
            config.ingestion.http.port = port;
            // Also update the bind address if it's using the default port
            if config.ingestion.http.bind.ends_with(":8080") || config.ingestion.http.bind.ends_with(":3000") {
                config.ingestion.http.bind = format!("0.0.0.0:{}", port);
            }
        } else {
            warn!("--http-port was provided but ingestion type {:?} doesn't use HTTP", args.ingestion);
        }
    }
    
    // Validate configuration based on ingestion type, but skip Kafka validation for non-proxy/monolith nodes
    let skip_kafka_validation = matches!(node_type, 
        NodeCommand::Control { .. } | NodeCommand::Ingest { .. } | NodeCommand::Grpc { .. } |
        NodeCommand::Storage { .. } | NodeCommand::Indexer { .. } | 
        NodeCommand::Search { .. } | NodeCommand::Janitor { .. }
    );
    
    if skip_kafka_validation && matches!(args.ingestion, IngestionType::Kafka | IngestionType::All) {
        // For non-proxy nodes using Kafka, we don't need broker configuration
        info!("Skipping Kafka broker validation for internal node communication");
    } else {
        // Normal validation for proxy and monolith nodes
        if let Err(e) = args.ingestion.validate_config(&config) {
            error!("Configuration validation failed for ingestion type {:?}", args.ingestion);
            error!("{}", e);
            return Err(e);
        }
    }
    
    // Now run the comprehensive configuration validation
    use openwit_config::unified::ConfigValidator;
    let mut validator = ConfigValidator::new();
    if !validator.validate_comprehensive(&config) {
        return Err(anyhow::anyhow!("Configuration validation failed. Please check the logs for details."));
    }
    
    // Validate control plane configuration for nodes that need it
    match &node_type {
        NodeCommand::Control { .. } => {
            // Check for empty endpoint
            if config.control_plane.grpc_endpoint.is_empty() {
                return Err(anyhow::anyhow!(
                    "Control plane endpoint is not configured. Please set 'control_plane.grpc_endpoint' in your config.yaml"
                ));
            }
            
            // Check for invalid port values
            if config.service_ports.control_plane.service == 0 {
                return Err(anyhow::anyhow!(
                    "Control plane service port is not configured. Please set 'service_ports.control_plane.service' in your config.yaml"
                ));
            }
        }
        _ => {
            // For other nodes, validate that control plane endpoint is configured if they need to connect to it
            if config.control_plane.enabled && config.control_plane.grpc_endpoint.is_empty() {
                warn!("Control plane is enabled but endpoint is not configured. This node may not be able to connect to the control plane.");
            }
        }
    }
    
    // Log key config details
    if config.ingestion.kafka.brokers.is_some() {
        info!("Kafka configuration found: {:?} | Topics: {:?}", 
             config.ingestion.kafka.brokers, 
             config.ingestion.kafka.topics);
    }
    if config.storage.azure.enabled {
        info!("Azure storage configured: {}/{}", 
             config.storage.azure.account_name, 
             config.storage.azure.container_name);
    }
    
    // Start the appropriate node type
    match node_type {
        NodeCommand::Control { port: _, node_id } => {
            let node_id = node_id.unwrap_or_else(|| generate_node_id("control"));

            // Use configured port from config
            let control_port = config.service_ports.control_plane.service;

            info!("Starting Control Plane node: {} on port {}", node_id, control_port);

            node_startup::start_control_node(config, node_id, control_port).await?;
        }
        NodeCommand::Proxy { port: _, node_id } => {
            let node_id = node_id.unwrap_or_else(|| generate_node_id("proxy"));

            // Use configured port from config
            let proxy_port = config.service_ports.proxy.service;

            info!("Starting Proxy node: {} on port {}", node_id, proxy_port);

            node_startup::start_proxy_node(config, node_id, proxy_port).await?;
        }
        NodeCommand::Ingest { port, node_id, grpc_bind, force_grpc } => {
            let node_id = node_id.unwrap_or_else(|| generate_node_id("ingest"));
            
            // Override gRPC configuration if requested
            if force_grpc {
                config.ingestion.sources.grpc.enabled = true;
                info!("Forcing gRPC ingestion mode");
            }
            
            if let Some(ref bind_addr) = grpc_bind {
                // Parse the bind address to extract host and port
                if let Some((host, port_str)) = bind_addr.rsplit_once(':') {
                    if let Ok(grpc_port) = port_str.parse::<u16>() {
                        config.ingestion.grpc.bind = host.to_string();
                        config.ingestion.grpc.port = grpc_port;
                        info!("Overriding gRPC bind: {} port: {}", host, grpc_port);
                    }
                } else {
                    // If no port specified, use default host with existing port
                    config.ingestion.grpc.bind = bind_addr.clone();
                    info!("Overriding gRPC bind host: {}", bind_addr);
                }
            }
            
            // Use the port from command line or config
            let grpc_port = if let Some(bind_addr) = &grpc_bind {
                // Extract port from bind address
                bind_addr
                    .split(':').last()
                    .and_then(|p| p.parse::<u16>().ok())
                    .or(port)
                    .unwrap_or(config.ingestion.grpc.port)
            } else {
                // Use command line port if provided, otherwise use config
                port.unwrap_or(config.ingestion.grpc.port)
            };

            info!("Starting Ingestion node: {} with gRPC on port {}", node_id, grpc_port);

            node_startup::start_ingest_node(config, node_id, grpc_port).await?;
        }
        NodeCommand::Grpc { port, node_id, grpc_bind } => {
            let node_id = node_id.unwrap_or_else(|| generate_node_id("grpc"));
            
            // gRPC command always forces gRPC mode
            config.ingestion.sources.grpc.enabled = true;
            config.ingestion.sources.kafka.enabled = false;
            config.ingestion.sources.http.enabled = false;
            info!("gRPC/OTLP ingestion mode enabled");
            
            if let Some(ref bind_addr) = grpc_bind {
                // Parse the bind address to extract host and port
                if let Some((host, port_str)) = bind_addr.rsplit_once(':') {
                    if let Ok(grpc_port) = port_str.parse::<u16>() {
                        config.ingestion.grpc.bind = host.to_string();
                        config.ingestion.grpc.port = grpc_port;
                        info!("Overriding gRPC bind: {} port: {}", host, grpc_port);
                    }
                } else {
                    // If no port specified, use default host with existing port
                    config.ingestion.grpc.bind = bind_addr.clone();
                    info!("Overriding gRPC bind host: {}", bind_addr);
                }
            }
            
            // Use the port from command line or config
            let grpc_port = if let Some(bind_addr) = &grpc_bind {
                // Extract port from bind address
                bind_addr
                    .split(':').last()
                    .and_then(|p| p.parse::<u16>().ok())
                    .or(port)
                    .unwrap_or(config.ingestion.grpc.port)
            } else {
                // Use command line port if provided, otherwise use config
                port.unwrap_or(config.ingestion.grpc.port)
            };

            info!("Starting gRPC/OTLP node: {} on port {}", node_id, grpc_port);

            node_startup::start_ingest_node(config, node_id, grpc_port).await?;
        }
        NodeCommand::Storage { port: _, node_id } => {
            let node_id = node_id.unwrap_or_else(|| generate_node_id("storage"));

            // Use configured port from config
            let storage_port = config.service_ports.storage.service;

            info!("Starting Storage node: {} on port {}", node_id, storage_port);

            node_startup::start_storage_node(config, node_id, storage_port).await?;
        }
        NodeCommand::Indexer { port, node_id } => {
            let node_id = node_id.unwrap_or_else(|| generate_node_id("indexer"));
            info!("Starting Indexer node: {} on port {}", node_id, port);
            node_startup::start_indexer_node(config, node_id, port).await?;
        }
        NodeCommand::Search { port, node_id } => {
            let node_id = node_id.unwrap_or_else(|| generate_node_id("search"));

            // Use the port from command line if provided, otherwise use config
            let search_port = port.unwrap_or(config.service_ports.search.service);

            info!("Starting Search node: {} on port {}", node_id, search_port);

            node_startup::start_search_node(config, node_id, search_port).await?;
        }
        NodeCommand::Janitor { port, node_id } => {
            let node_id = node_id.unwrap_or_else(|| generate_node_id("janitor"));
            info!("Starting Janitor node: {} on port {}", node_id, port);
            node_startup::start_janitor_node(config, node_id, port).await?;
        }
        NodeCommand::Kafka { node_id, debug } => {
            let node_id = node_id.unwrap_or_else(|| generate_node_id("kafka"));
            
            // Apply debug logging if requested
            if debug {
                info!("Enabling debug logging for Kafka node");
            }

            info!("Starting Kafka consumer node: {}", node_id);
            node_startup::start_kafka_node(config, node_id).await?;
        }
        NodeCommand::Http { bind: _, node_id, max_concurrent_requests, request_timeout_ms, max_payload_size_mb } => {
            let node_id = node_id.unwrap_or_else(|| generate_node_id("http"));

            info!("Starting OpenWit HTTP ingestion server");
            info!("  Port: {}", config.ingestion.http.port);
            info!("  Max concurrent requests: {}", max_concurrent_requests);
            info!("  Request timeout: {}ms", request_timeout_ms);
            info!("  Max payload size: {}MB", max_payload_size_mb);

            // Apply HTTP-specific config
            config.ingestion.sources.http.enabled = true;
            config.ingestion.http.max_concurrent_requests = max_concurrent_requests;
            config.ingestion.http.request_timeout_ms = request_timeout_ms;
            config.ingestion.http.max_payload_size_mb = max_payload_size_mb;
            
            // Start HTTP server with control plane integration
            match openwit_http::HttpServer::new(config.clone(), None) {
                Ok(server) => {
                    // Start control plane integration for health reporting
                    let bind_addr = server.bind_address().to_string();
                    let control_plane_endpoint = config.control_plane.grpc_endpoint.clone();
                    
                    // Create and start control plane integration
                    match ControlPlaneIntegration::new(
                        node_id.clone(),
                        bind_addr,
                        &control_plane_endpoint
                    ).await {
                        Ok(integration) => {
                            let integration = std::sync::Arc::new(integration);
                            let health_task = {
                                let integration = integration.clone();
                                tokio::spawn(async move {
                                    integration.start_health_reporting().await;
                                })
                            };
                            
                            info!("Started control plane integration for HTTP node");
                            
                            // Start the HTTP server
                            let server_task = tokio::spawn(async move {
                                if let Err(e) = server.start().await {
                                    error!("HTTP server failed: {:?}", e);
                                }
                            });
                            
                            // Wait for either task to complete
                            tokio::select! {
                                _ = server_task => {},
                                _ = health_task => {},
                            }
                        }
                        Err(e) => {
                            warn!("Failed to create control plane integration: {}, starting HTTP server without it", e);
                            if let Err(e) = server.start().await {
                                error!("HTTP server failed: {:?}", e);
                                return Err(anyhow::anyhow!("HTTP server failed: {:?}", e));
                            }
                        }
                    }
                }
                Err(e) => {
                    error!("Failed to create HTTP server: {:?}", e);
                    return Err(anyhow::anyhow!("Failed to create HTTP server: {:?}", e));
                }
            }
        }
    }
    
    Ok(())
}
