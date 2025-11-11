use clap::Parser;
use tracing_subscriber::{fmt, EnvFilter};
use tracing_subscriber::prelude::*;
use anyhow::Result;
use std::sync::Arc;
use openwit_http::{HttpServer, control_plane_integration::ControlPlaneIntegration};
use openwit_config::UnifiedConfig;

#[derive(Parser, Debug)]
#[command(name = "openwit-http", about = "OpenWit HTTP Ingestion Server", version)]
struct Args {
    /// HTTP bind address (e.g., 0.0.0.0:9087)
    #[arg(short, long, default_value = "0.0.0.0:9087")]
    bind: String,
    
    /// Maximum concurrent requests
    #[arg(short = 'c', long, default_value = "5000")]
    max_concurrent_requests: u32,
    
    /// Request timeout in milliseconds
    #[arg(short = 't', long, default_value = "30000")]
    request_timeout_ms: u64,
    
    /// Maximum payload size in MB
    #[arg(short = 's', long, default_value = "100")]
    max_payload_size_mb: u32,
    
    /// Configuration file path (optional - will use CLI args if not provided)
    #[arg(short = 'f', long)]
    config: Option<String>,
    
    /// Enable debug logging
    #[arg(short, long)]
    debug: bool,
    
    /// Ingestion endpoint (optional - for forwarding to another service)
    #[arg(short = 'i', long)]
    ingestion_endpoint: Option<String>,
    
    /// Node ID for this HTTP server instance
    #[arg(long)]
    node_id: Option<String>,
    
    /// Control plane address (optional - for health reporting, must be configured if needed)
    #[arg(long)]
    control_plane_addr: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    
    // Setup logging
    let env_filter = if args.debug {
        EnvFilter::new("debug")
    } else {
        EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| EnvFilter::new("info"))
    };
    
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(env_filter)
        .init();
    
    tracing::info!("Starting OpenWit HTTP Ingestion Server");
    tracing::info!("Configuration:");
    tracing::info!("  Bind address: {}", args.bind);
    tracing::info!("  Max concurrent requests: {}", args.max_concurrent_requests);
    tracing::info!("  Request timeout: {}ms", args.request_timeout_ms);
    tracing::info!("  Max payload size: {}MB", args.max_payload_size_mb);
    
    // Load or create configuration
    let mut config = if let Some(config_path) = args.config {
        tracing::info!("Loading configuration from: {}", config_path);
        load_config(&config_path).await?
    } else {
        tracing::info!("Using CLI parameters for configuration");
        create_config_from_args(&args)
    };
    
    // Override bind address from CLI if provided
    config.ingestion.http.bind = args.bind.clone();
    
    // Create ingestion sender if endpoint is provided
    let ingest_tx = if let Some(endpoint) = args.ingestion_endpoint {
        tracing::info!("Forwarding ingestion to: {}", endpoint);
        // TODO: Create actual ingestion client
        None
    } else {
        None
    };
    
    // Start HTTP server
    let server = HttpServer::new(config.clone(), ingest_tx)?;
    let bind_addr = server.bind_address().to_string();
    
    // Generate unique node ID if not provided
    let node_id = args.node_id.unwrap_or_else(|| {
        // Extract port from bind address to create unique ID
        let port = bind_addr.split(':').last().unwrap_or("9087");
        format!("http-node-{}", port)
    });
    
    tracing::info!("HTTP server starting with node ID: {}", node_id);
    
    // Start control plane integration if enabled
    let control_plane_handle = if let Some(control_plane_addr) = &args.control_plane_addr {
        tracing::info!("Control plane address from args: '{}'", control_plane_addr);
        match ControlPlaneIntegration::new(
            node_id.clone(),
            bind_addr.clone(),
            control_plane_addr,
        ).await {
            Ok(integration) => {
                tracing::info!("Starting control plane health reporting (will retry connection if needed)");
                let integration = Arc::new(integration);
                Some(tokio::spawn(async move {
                    integration.start_health_reporting().await;
                }))
            }
            Err(e) => {
                tracing::error!("Failed to create control plane integration: {}. This should not happen.", e);
                None
            }
        }
    } else {
        tracing::info!("Control plane integration disabled - no control plane address configured");
        None
    };
    
    // Handle graceful shutdown
    let server_handle = tokio::spawn(async move {
        if let Err(e) = server.start().await {
            tracing::error!("HTTP server error: {}", e);
        }
    });
    
    // Wait for Ctrl+C
    tokio::signal::ctrl_c().await?;
    tracing::info!("Shutting down HTTP server...");
    
    // TODO: Implement graceful shutdown
    server_handle.abort();
    if let Some(handle) = control_plane_handle {
        handle.abort();
    }
    
    Ok(())
}

async fn load_config(path: &str) -> Result<UnifiedConfig> {
    let content = tokio::fs::read_to_string(path).await?;
    let config: UnifiedConfig = serde_yaml::from_str(&content)?;
    Ok(config)
}

fn create_config_from_args(args: &Args) -> UnifiedConfig {
    let mut config = UnifiedConfig::default();
    
    // Set HTTP configuration from CLI args
    config.ingestion.sources.http.enabled = true;
    config.ingestion.http.bind = args.bind.clone();
    config.ingestion.http.max_concurrent_requests = args.max_concurrent_requests;
    config.ingestion.http.request_timeout_ms = args.request_timeout_ms;
    config.ingestion.http.max_payload_size_mb = args.max_payload_size_mb;
    
    // Set some reasonable defaults
    config.environment = "local".to_string();
    config.deployment.mode = "standalone".to_string();
    
    config
}