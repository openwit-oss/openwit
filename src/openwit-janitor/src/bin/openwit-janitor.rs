use anyhow::Result;
use clap::Parser;
use std::sync::Arc;
use tracing::{info, error};

use openwit_config::UnifiedConfig;
use openwit_janitor::{JanitorActor, JanitorConfig};
use openwit_metastore::{MetaStore, sled_metastore::{SledMetaStore, SledMetastoreConfig}};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to configuration file
    #[arg(short, long, default_value = "./config/openwit.yaml")]
    config: String,

    /// Run interval in seconds
    #[arg(long, default_value = "300")]
    run_interval: u64,

    /// Compaction interval in seconds
    #[arg(long, default_value = "3600")]
    compaction_interval: u64,

    /// Retention check interval in seconds
    #[arg(long, default_value = "86400")]
    retention_interval: u64,

    /// WAL cleanup interval in seconds
    #[arg(long, default_value = "1800")]
    wal_cleanup_interval: u64,

    /// Maximum segments per partition before compaction
    #[arg(long, default_value = "10")]
    max_segments: usize,

    /// Data retention period in days
    #[arg(long, default_value = "30")]
    retention_days: u64,

    /// WAL retention period in hours
    #[arg(long, default_value = "24")]
    wal_retention_hours: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Setup observability

    info!("Starting OpenWit Janitor Service");

    // Load configuration
    let config_str = std::fs::read_to_string(&args.config)?;
    let config: UnifiedConfig = serde_yaml::from_str(&config_str)?;

    // Initialize metastore
    let metastore_path = if config.metastore.backend == "sled" {
        config.metastore.sled.path.clone()
    } else {
        "./data/metastore".to_string()
    };
    let sled_config = SledMetastoreConfig {
        path: metastore_path,
        cache_size_mb: 100,
    };
    let metastore: Arc<dyn MetaStore> = Arc::new(SledMetaStore::new(sled_config).await?);

    // Create janitor config
    let janitor_config = JanitorConfig {
        run_interval_secs: args.run_interval,
        compaction_interval_secs: args.compaction_interval,
        retention_check_interval_secs: args.retention_interval,
        wal_cleanup_interval_secs: args.wal_cleanup_interval,
        max_segments_per_partition: args.max_segments,
        retention_period_days: args.retention_days,
        wal_retention_hours: args.wal_retention_hours,
    };

    // Create and run janitor actor
    let janitor = JanitorActor::new(janitor_config, metastore, &config);

    info!("Janitor configuration:");
    info!("  Run interval: {}s", args.run_interval);
    info!("  Compaction interval: {}s", args.compaction_interval);
    info!("  Retention check interval: {}s", args.retention_interval);
    info!("  WAL cleanup interval: {}s", args.wal_cleanup_interval);
    info!("  Max segments per partition: {}", args.max_segments);
    info!("  Retention period: {} days", args.retention_days);
    info!("  WAL retention: {} hours", args.wal_retention_hours);

    // Run the janitor
    if let Err(e) = janitor.run().await {
        error!("Janitor failed: {}", e);
        return Err(e);
    }

    Ok(())
}