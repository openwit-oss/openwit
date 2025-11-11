use anyhow::Result;
use clap::Parser;
use tracing_subscriber::{fmt, EnvFilter};
use tracing_subscriber::prelude::*;
use openwit_config::UnifiedConfig;
use openwit_grpc::GrpcServer;

#[derive(Parser, Debug)]
#[command(name = "openwit-grpc", about = "OpenWit gRPC OTLP Server", version)]
struct Args {
    /// Configuration file path
    #[arg(short, long)]
    config: String,
    
    /// Node ID
    #[arg(long)]
    node_id: String,
    
    /// gRPC port
    #[arg(long, default_value = "50052")]
    port: u16,
    
    /// Enable debug logging
    #[arg(short, long)]
    debug: bool,
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
    
    tracing::info!("Starting OpenWit gRPC OTLP Server");
    tracing::info!("Node ID: {}", args.node_id);
    tracing::info!("Port: {}", args.port);
    tracing::info!("Config: {}", args.config);
    
    // Load configuration
    let content = tokio::fs::read_to_string(&args.config).await?;
    let mut config: UnifiedConfig = serde_yaml::from_str(&content)?;
    
    // Override port from command line
    config.ingestion.grpc.port = args.port;
    
    // Create and start server
    let server = GrpcServer::new(config, None);
    
    tracing::info!("gRPC server starting on port {}", args.port);
    
    // Start server and wait
    server.start().await?;
    
    Ok(())
}