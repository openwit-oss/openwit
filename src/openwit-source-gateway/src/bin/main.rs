use anyhow::Result;
use clap::Parser;
use openwit_config::UnifiedConfig;
use openwit_source_gateway::{SourceGateway, handlers};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{info, error};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Parser, Debug)]
#[command(author, version, about = "OpenWit Source Gateway - Schema validation for telemetry data")]
struct Args {
    /// Configuration file path
    #[arg(short, long, default_value = "config/openwit-unified-control.yaml")]
    config: PathBuf,
    
    /// Bind address
    #[arg(short, long, default_value = "0.0.0.0:9090")]
    bind: SocketAddr,
    
    /// Schema directory path
    #[arg(short, long)]
    schema_dir: Option<PathBuf>,
    
    /// Enable debug logging
    #[arg(long)]
    debug: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    
    // Initialize logging
    let env_filter = if args.debug {
        "debug"
    } else {
        "info"
    };
    
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| env_filter.into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();
    
    info!("Starting OpenWit Source Gateway");
    info!("Configuration file: {:?}", args.config);
    info!("Bind address: {}", args.bind);
    
    // Load configuration
    let config = Arc::new(
        UnifiedConfig::from_file(&args.config)
            .map_err(|e| anyhow::anyhow!("Failed to load config: {}", e))?
    );
    
    // Override schema directory if provided
    if let Some(schema_dir) = args.schema_dir {
        info!("Using schema directory: {:?}", schema_dir);
        // TODO: Add config override for schema directory
    }
    
    // Create source gateway
    let gateway = Arc::new(
        SourceGateway::new(config.clone())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to create gateway: {}", e))?
    );
    
    info!("Source gateway initialized successfully");
    
    // Get loaded schemas
    let schema_types = gateway.get_schema_types().await;
    info!("Loaded schemas: {:?}", schema_types);
    
    // Create HTTP server
    let app = handlers::create_http_routes(gateway);
    
    // Start server
    info!("Starting HTTP server on {}", args.bind);
    
    let listener = tokio::net::TcpListener::bind(args.bind).await?;
    axum::serve(listener, app)
        .await
        .map_err(|e| anyhow::anyhow!("Server error: {}", e))?;
    
    Ok(())
}