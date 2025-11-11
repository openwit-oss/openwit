use anyhow::Result;
use clap::Parser;
use tracing_subscriber::{fmt, EnvFilter};
use tracing_subscriber::prelude::*;
use openwit_config::UnifiedConfig;
use openwit_kafka::KafkaServer;

#[derive(Parser, Debug)]
#[command(name = "openwit-kafka", about = "OpenWit Kafka Consumer", version)]
struct Args {
    /// Configuration file path
    #[arg(short, long)]
    config: String,
    
    /// Node ID
    #[arg(long)]
    node_id: String,
    
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
    
    tracing::info!("Starting OpenWit Kafka Consumer");
    tracing::info!("Node ID: {}", args.node_id);
    tracing::info!("Config: {}", args.config);
    
    // Load configuration
    let content = tokio::fs::read_to_string(&args.config).await?;
    let config: UnifiedConfig = serde_yaml::from_str(&content)?;
    
    // Verify Kafka configuration
    if config.ingestion.kafka.brokers.is_none() {
        return Err(anyhow::anyhow!("Kafka brokers not configured"));
    }
    
    tracing::info!("Kafka brokers: {:?}", config.ingestion.kafka.brokers);
    tracing::info!("Kafka topics: {:?}", config.ingestion.kafka.topics);
    
    // Create and start Kafka server (it will handle gRPC connection to ingestion nodes)
    let server = KafkaServer::new(config);
    
    tracing::info!("Kafka consumer starting with static partition assignment...");
    tracing::info!("Node ID '{}' will be used to determine pod index", args.node_id);
    tracing::info!("Static assignment prevents rebalancing - each pod consumes fixed partitions");
    
    // Start server and wait
    server.start().await?;
    
    Ok(())
}