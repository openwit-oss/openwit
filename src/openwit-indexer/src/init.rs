use anyhow::{Context, Result};
use std::path::{Path};
use tracing::{info, debug};

/// Initialize indexer working directories
pub async fn initialize_directories(work_dir: &Path) -> Result<()> {
    info!(work_dir = ?work_dir, "Initializing indexer directories");

    let dirs = vec![
        work_dir.join("wal"),
        work_dir.join("delta_work"),
        work_dir.join("combined_work"),
        work_dir.join("cache"),
        work_dir.join("tantivy"),
    ];

    for dir in dirs {
        std::fs::create_dir_all(&dir)
            .with_context(|| format!("Failed to create directory: {:?}", dir))?;
        debug!(dir = ?dir, "Created directory");
    }

    // Verify directories are writable
    let test_file = work_dir.join("wal").join(".write_test");
    std::fs::write(&test_file, b"test")
        .context("Failed to write test file - directory not writable")?;
    std::fs::remove_file(&test_file)?;

    info!("Indexer directories initialized successfully");
    Ok(())
}

/// Check connectivity to required services
pub async fn check_connectivity(
    postgres_dsn: &str,
    artifact_uri: &str,
) -> Result<()> {
    info!("Checking connectivity to required services");

    // Check PostgreSQL
    debug!("Checking PostgreSQL connectivity");
    let pool = sqlx::PgPool::connect(postgres_dsn).await
        .context("Failed to connect to PostgreSQL")?;
    
    sqlx::query("SELECT 1").execute(&pool).await
        .context("Failed to execute test query on PostgreSQL")?;
    
    pool.close().await;
    info!("PostgreSQL connectivity verified");

    // Check OpenDAL storage
    debug!(artifact_uri = %artifact_uri, "Checking storage connectivity");
    let storage = crate::storage::StorageClient::new(artifact_uri, None)?;
    
    let health = storage.health_check().await
        .context("Storage health check failed")?;
    
    if health.get("write_test").map(|v| v.as_str()) != Some("ok") {
        anyhow::bail!("Storage write test failed");
    }
    
    info!("Storage connectivity verified");

    Ok(())
}

/// Initialize tracing and observability
pub fn init_tracing(log_level: &str) -> Result<()> {
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

    let level = log_level.parse::<tracing::Level>()
        .with_context(|| format!("Invalid log level: {}", log_level))?;

    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| format!("openwit_indexer={}", level).into());

    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_target(true)
        .with_thread_ids(true)
        .with_file(true)
        .with_line_number(true); // JSON format for production

    let registry = tracing_subscriber::registry()
        .with(filter)
        .with(fmt_layer);

    // TODO: Fix OpenTelemetry version mismatch
    // For now, just initialize without OpenTelemetry
    registry.init();

    Ok(())
}

/// Setup graceful shutdown hooks
pub fn setup_shutdown_hooks() -> tokio::sync::broadcast::Receiver<()> {
    let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel(1);
    
    let shutdown_tx_clone = shutdown_tx.clone();
    ctrlc::set_handler(move || {
        info!("Received shutdown signal");
        let _ = shutdown_tx_clone.send(());
    })
    .expect("Error setting Ctrl-C handler");

    // Also handle SIGTERM
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        
        let shutdown_tx_clone = shutdown_tx.clone();
        tokio::spawn(async move {
            let mut sigterm = signal(SignalKind::terminate())
                .expect("Failed to register SIGTERM handler");
            
            sigterm.recv().await;
            info!("Received SIGTERM");
            let _ = shutdown_tx_clone.send(());
        });
    }

    shutdown_rx
}