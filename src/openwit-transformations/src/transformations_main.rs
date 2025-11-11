//! SQL Transformation API server

use clap::Parser;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "openwit-transformations")]
#[command(about = "OpenWit SQL Transformation API", long_about = None)]
struct Cli {
    /// Port to listen on
    #[arg(short, long, default_value = "8090")]
    port: u16,
    
    /// Log level
    #[arg(long, default_value = "info")]
    log_level: String,
}

#[allow(dead_code)]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    
    // Initialize tracing
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(&cli.log_level));
    
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .init();
    
    tracing::info!("Starting OpenWit SQL Transformation API");
    
    // Run the server
    crate::api::run_server(cli.port).await?;
    
    Ok(())
}