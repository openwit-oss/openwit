use anyhow::Result;
use tracing::{info, error, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use openwit_storage::StorageProcessor;
use openwit_config::UnifiedConfig;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize comprehensive tracing with enhanced formatting
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer()
            .with_target(false)
            .with_thread_ids(false)
            .with_level(true)
        )
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    info!("Starting OpenWit Storage Service");

    // Load configuration - prioritize unified config file
    let config_path = std::env::var("OPENWIT_CONFIG")
        .unwrap_or_else(|_| "config/openwit-unified-control.yaml".to_string());

    let config = if std::path::Path::new(&config_path).exists() {
        info!("Loading configuration from: {}", config_path);
        match UnifiedConfig::from_file(&config_path) {
            Ok(cfg) => {
                info!("✅ Configuration loaded successfully from unified config");
                Some(cfg)
            }
            Err(e) => {
                error!("❌ Failed to load unified config: {}", e);
                warn!("Falling back to environment variables");
                None
            }
        }
    } else {
        warn!("Config file not found at: {}, using environment variables", config_path);
        None
    };

    // Extract configuration values with intelligent fallbacks
    let node_id = std::env::var("NODE_ID")
        .unwrap_or_else(|_| {
            if config.is_some() {
                "storage-node-1".to_string()
            } else {
                "storage-1".to_string()
            }
        });

    let data_dir = config
        .as_ref()
        .map(|c| c.storage.local.path.clone())
        .or_else(|| std::env::var("STORAGE_DATA_DIR").ok())
        .unwrap_or_else(|| "./data/storage".to_string());

    let arrow_flight_port = config
        .as_ref()
        .and_then(|c| {
            c.storage_node.flight.bind_addr
                .split(':')
                .last()
                .and_then(|s| s.parse::<u16>().ok())
        })
        .or_else(|| {
            std::env::var("ARROW_FLIGHT_PORT")
                .ok()
                .and_then(|p| p.parse::<u16>().ok())
        })
        .unwrap_or(9401);

    // Display configuration summary
    info!("🚀 Storage Service Configuration:");
    info!("  Node ID: {}", node_id);
    info!("  Data directory: {}", data_dir);
    info!("  Arrow Flight port: {}", arrow_flight_port);
    
    if let Some(ref cfg) = config {
        info!("  File rotation: {} MB / {} minutes", 
              cfg.storage.file_rotation.file_size_mb,
              cfg.storage.file_rotation.file_duration_minutes);
        info!("  Upload delay: {} minutes", 
              cfg.storage.file_rotation.upload_delay_minutes);
        
        // Log cloud storage status
        if cfg.storage.azure.enabled {
            info!("  ☁️  Azure storage: {} / {}", 
                  cfg.storage.azure.account_name, cfg.storage.azure.container_name);
        }
        if cfg.storage.s3.enabled {
            info!("  ☁️  S3 storage: {} / {}", 
                  cfg.storage.s3.bucket, cfg.storage.s3.region);
        }
        if !cfg.storage.azure.enabled && !cfg.storage.s3.enabled {
            info!("  💾 Local storage only");
        }
    } else {
        info!("  📝 Using environment variable configuration");
    }

    // Ensure data directory exists
    tokio::fs::create_dir_all(&data_dir).await?;

    // Create and initialize storage processor
    match StorageProcessor::new_with_config(
        node_id.clone(),
        data_dir,
        arrow_flight_port,
        config.as_ref()
    ).await {
        Ok((processor, _batch_tx)) => {
            info!("✅ Storage processor initialized successfully");
            
            // Set up graceful shutdown handling
            let processor_handle = tokio::spawn(async move {
                if let Err(e) = processor.run().await {
                    error!("❌ Storage processor error: {}", e);
                }
            });
            
            info!("🎯 Storage service is ready and listening");
            info!("   Arrow Flight endpoint: 0.0.0.0:{}", arrow_flight_port);
            info!("   Press Ctrl+C to shutdown gracefully");
            
            // Wait for shutdown signal or processor termination
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    info!("🛑 Received shutdown signal (Ctrl+C)");
                }
                result = processor_handle => {
                    match result {
                        Ok(_) => info!("✅ Storage processor completed normally"),
                        Err(e) => error!("❌ Storage processor task failed: {}", e),
                    }
                }
            }
        }
        Err(e) => {
            error!("❌ Failed to initialize storage processor: {}", e);
            return Err(e);
        }
    }

    info!("🏁 Storage service shutdown complete");
    Ok(())
}