use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand};
use tracing_subscriber::prelude::*;
use crate::{
    IndexerConfig, DbClient, StorageClient, ParquetReader, ArtifactBuilder, WAL,
    cluster::ClusterManager, flight::{start_flight_server},
    init::{initialize_directories, check_connectivity, setup_shutdown_hooks},
    metrics, events::{EventSubscriber, ControlEvent}, combine::CombineWorker,
    grpc_service::start_grpc_server, http::start_metrics_server,
};
use std::path::PathBuf;
use tracing::{info, error, warn, debug};
use std::sync::Arc;
use std::collections::HashMap;
use tokio::sync::mpsc;

/// Start the indexer service with the given configuration
pub async fn start_indexer_service(
    config: IndexerConfig,
    node_id: String,
    grpc_addr: String,
    http_addr: String,
) -> Result<()> {
    let args = ServeArgs {
        mode: None,
        http_addr,
        grpc_addr,
        no_recovery: false,
        check: false,
    };
    
    serve(config, args).await
}

/// OpenWit Indexer - Production-ready indexing service for observability data
#[derive(Parser)]
#[command(name = "openwit-indexer")]
#[command(about = "High-performance indexing service for OpenWit observability platform")]
#[command(version)]
struct Cli {
    /// Configuration file path
    #[arg(short, long, default_value = "openwit-unified-control.yaml")]
    config: PathBuf,

    /// Log level
    #[arg(short, long, default_value = "info")]
    log_level: String,

    /// Working directory
    #[arg(short, long)]
    work_dir: Option<PathBuf>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run the indexer service
    Serve(ServeArgs),
    /// Local mode commands
    Local(LocalArgs),
    /// Recovery and maintenance commands
    Recover(RecoverArgs),
    /// Health check
    Health(HealthArgs),
}

#[derive(Args)]
struct ServeArgs {
    /// Override mode (local or prod)
    #[arg(long)]
    mode: Option<String>,

    /// HTTP bind address
    #[arg(long, default_value = "0.0.0.0:8080")]
    http_addr: String,

    /// gRPC bind address (production mode only)
    #[arg(long, default_value = "0.0.0.0:8081")]
    grpc_addr: String,

    /// Disable recovery on startup
    #[arg(long)]
    no_recovery: bool,
    
    /// Check mode - verify connectivity and exit
    #[arg(long)]
    check: bool,
}

#[derive(Args)]
struct LocalArgs {
    #[command(subcommand)]
    command: LocalCommands,
}

#[derive(Subcommand)]
enum LocalCommands {
    /// Index parquet files from local directory
    IndexFiles {
        /// Input directory containing parquet files
        #[arg(short, long)]
        input_dir: PathBuf,
        
        /// Output directory for index artifacts
        #[arg(short, long)]
        output_dir: PathBuf,
        
        /// Tenant name
        #[arg(short, long, default_value = "default")]
        tenant: String,
        
        /// Signal type (logs, traces, metrics)
        #[arg(short, long, default_value = "logs")]
        signal: String,
    },
    /// Query coverage for a time range
    Coverage {
        /// Tenant name
        #[arg(short, long, default_value = "default")]
        tenant: String,
        
        /// Signal type
        #[arg(short, long, default_value = "logs")]
        signal: String,
        
        /// Start timestamp (Unix milliseconds)
        #[arg(long)]
        from_ms: i64,
        
        /// End timestamp (Unix milliseconds)
        #[arg(long)]
        to_ms: i64,
    },
}

#[derive(Args)]
struct RecoverArgs {
    /// Show recovery information without executing
    #[arg(long)]
    dry_run: bool,
    
    /// Force recovery even if already completed
    #[arg(long)]
    force: bool,
}

#[derive(Args)]
struct HealthArgs {
    /// Output format (json, text)
    #[arg(long, default_value = "text")]
    format: String,
}

#[allow(unused)]
#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Initialize tracing with OTLP support
    init_tracing(&cli.log_level)?;

    info!(
        version = env!("CARGO_PKG_VERSION"),
        config = ?cli.config,
        "Starting OpenWit Indexer"
    );

    // Load configuration
    let config = IndexerConfig::from_file(&cli.config)
        .with_context(|| format!("Failed to load config from {:?}", cli.config))?;

    // Override work directory if provided
    let config = if let Some(work_dir) = cli.work_dir {
        let mut config = config;
        config.indexer.general.work_dir = work_dir;
        config
    } else {
        config
    };
    
    // Initialize directories
    initialize_directories(&config.indexer.general.work_dir).await?;
    
    // Check mode
    if let Commands::Serve(ref args) = cli.command {
        if args.check {
            info!("Running in check mode");
            check_connectivity(&config.postgres_dsn(), &config.artifact_uri()).await?;
            info!("All checks passed");
            return Ok(());
        }
    }

    // Execute commands
    match cli.command {
        Commands::Serve(args) => serve(config, args).await,
        Commands::Local(args) => local_command(config, args).await,
        Commands::Recover(args) => recover(config, args).await,
        Commands::Health(args) => health_check(config, args).await,
    }
}

/// Initialize tracing/logging
#[allow(unused)]
fn init_tracing(log_level: &str) -> Result<()> {
    let level = log_level.parse::<tracing::Level>()
        .with_context(|| format!("Invalid log level: {}", log_level))?;

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| format!("openwit_indexer={}", level).into())
        )
        .with(
            tracing_subscriber::fmt::layer()
                .with_target(true)
                .with_thread_ids(true)
                .with_file(true)
                .with_line_number(true)
        )
        .init();

    info!(level = %level, "Tracing initialized");
    Ok(())
}

/// Run the indexer service
#[allow(unused)]
async fn serve(config: IndexerConfig, args: ServeArgs) -> Result<()> {
    info!(
        mode = ?config.indexer.general.mode,
        http_addr = %args.http_addr,
        grpc_addr = %args.grpc_addr,
        "Starting indexer service"
    );
    
    // Setup shutdown signal
    let shutdown_rx = setup_shutdown_hooks();

    // Initialize core components
    let db_client = Arc::new(DbClient::new(&config.postgres_dsn()).await
        .context("Failed to initialize database client")?);
    
    let storage_client = Arc::new(StorageClient::new(&config.artifact_uri(), None)
        .context("Failed to initialize storage client")?);
    
    let wal = Arc::new(WAL::new(&config.indexer.general.work_dir.join("wal"), 100 * 1024 * 1024) // 100MB
        .context("Failed to initialize WAL")?);
    
    let artifact_builder = Arc::new(ArtifactBuilder::new(config.indexer.artifacts.clone()));

    // Recovery if enabled
    if !args.no_recovery {
        info!("Running WAL recovery");
        run_recovery(wal.as_ref(), db_client.as_ref(), storage_client.as_ref()).await?;
        metrics::record_wal_replay_resume("startup");
    }
    
    // Start metrics endpoint
    let _metrics_handle = tokio::spawn(start_metrics_server(args.http_addr.clone()));

    // Start appropriate mode
    let result = if config.is_local_mode() {
        run_local_mode(config, db_client, storage_client, wal.clone(), artifact_builder, shutdown_rx).await
    } else {
        run_production_mode(config, args, db_client, storage_client, wal.clone(), artifact_builder, shutdown_rx).await
    };
    
    // Cleanup
    wal.shutdown()?;
    info!("Indexer shutdown complete");
    result
}

/// Run local mode (single machine, no cluster)
#[allow(unused)]
async fn run_local_mode(
    config: IndexerConfig,
    db_client: Arc<DbClient>,
    storage_client: Arc<StorageClient>,
    wal: Arc<WAL>,
    artifact_builder: Arc<ArtifactBuilder>,
    mut shutdown_rx: tokio::sync::broadcast::Receiver<()>,
) -> Result<()> {
    info!("Running in local mode");

    // In local mode, we poll for new files and index them
    let parquet_reader = ParquetReader::new(std::sync::Arc::new(
        opendal::Operator::new(opendal::services::Fs::default())?.finish()
    ));

    // Main processing loop
    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(30));
    
    loop {
        tokio::select! {
            _ = interval.tick() => {
                // Get unindexed files
                let unindexed_files = db_client
                    .list_data_files_without_deltas("default", "logs", 10)
                    .await?;

                if unindexed_files.is_empty() {
                    debug!("No unindexed files found");
                } else {
                    info!(count = unindexed_files.len(), "Processing unindexed files");

                    for data_file in unindexed_files {
                        if let Err(e) = process_file(
                            &data_file,
                            artifact_builder.as_ref(),
                            &parquet_reader,
                            storage_client.as_ref(),
                            db_client.as_ref(),
                            wal.as_ref(),
                            &config,
                        ).await {
                            error!(
                                file_ulid = %data_file.file_ulid,
                                error = %e,
                                "Failed to process file"
                            );
                            metrics::record_file_index_failed(&data_file.tenant, &data_file.signal);
                        }
                    }
                }
            }
            
            _ = shutdown_rx.recv() => {
                info!("Received shutdown signal");
                break;
            }
        }
    }
    
    Ok(())
}

/// Run production mode with gRPC API and cluster coordination
#[allow(unused)]
async fn run_production_mode(
    config: IndexerConfig,
    args: ServeArgs,
    db_client: Arc<DbClient>,
    storage_client: Arc<StorageClient>,
    wal: Arc<WAL>,
    artifact_builder: Arc<ArtifactBuilder>,
    mut shutdown_rx: tokio::sync::broadcast::Receiver<()>,
) -> Result<()> {
    info!("Starting production mode with cluster coordination");
    
    // Initialize cluster manager with chitchat
    let cluster_manager = Arc::new(ClusterManager::new(&config).await
        .context("Failed to initialize cluster manager")?);
    
    // Start event subscription from control plane
    // TODO: Get control plane address from a proper config field
    let control_plane_addr = "http://localhost:50051".to_string();
    let (event_subscriber, event_receiver) = EventSubscriber::new(
        control_plane_addr,
        db_client.clone(),
    );
    let event_handle = tokio::spawn(event_subscriber.start());
    
    // Start combine worker
    let (combine_worker, combine_receiver) = CombineWorker::new(
        Arc::new(config.clone()),
        db_client.clone(),
        storage_client.clone(),
        wal.clone(),
    );
    let combine_handle = tokio::spawn(combine_worker.clone().start(combine_receiver, shutdown_rx.resubscribe()));
    
    // Start processing queue
    let (process_tx, process_rx) = mpsc::channel::<ControlEvent>(1000);
    let process_handle = tokio::spawn(run_event_processor(
        process_rx,
        event_receiver,
        Arc::new(config.clone()),
        db_client.clone(),
        storage_client.clone(),
        wal.clone(),
        artifact_builder.clone(),
        combine_worker,
    ));
    
    // Start Arrow Flight server
    let flight_addr = args.grpc_addr.parse::<std::net::SocketAddr>()
        .context("Invalid gRPC address")?;
    
    let flight_handle = tokio::spawn(start_flight_server(
        Arc::new(config.clone()),
        db_client.clone(),
        storage_client.clone(),
        artifact_builder.clone(),
        wal.clone(),
        flight_addr,
    ));
    
    // Start gRPC IndexService with event sender
    let grpc_addr = format!("0.0.0.0:8082").parse::<std::net::SocketAddr>()?;
    let grpc_handle = tokio::spawn(start_grpc_server(
        Arc::new(config.clone()),
        db_client.clone(),
        grpc_addr,
        Some(process_tx.clone()),
    ));
    
    info!(
        node_id = %config.indexer.general.node_id,
        flight_addr = %args.grpc_addr,
        grpc_addr = %grpc_addr,
        "Indexer running in production mode"
    );
    
    // Update cluster status
    cluster_manager.update_status("running").await?;
    
    // Main production loop with health monitoring
    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(30));
    
    loop {
        tokio::select! {
            _ = interval.tick() => {
                // Update cluster metrics
                let mut cluster_metrics = HashMap::new();
                cluster_metrics.insert(
                    "build_queue_depth".to_string(),
                    metrics::BUILD_QUEUE_DEPTH.with_label_values(&["normal"]).get().to_string()
                );
                cluster_metrics.insert(
                    "accepting".to_string(),
                    metrics::ACCEPTING.with_label_values(&["backpressure"]).get().to_string()
                );
                
                cluster_manager.update_metrics(cluster_metrics).await?;
                
                // Check backpressure
                let queue_depth = metrics::BUILD_QUEUE_DEPTH.with_label_values(&["normal"]).get() as usize;
                let memory_mb = metrics::get_memory_usage_mb();
                
                let should_accept = queue_depth < 1000 && memory_mb < 8000.0;
                metrics::update_accepting(should_accept, "backpressure");
                
                if !should_accept {
                    cluster_manager.update_status("backpressured").await?;
                }
            }
            
            _ = shutdown_rx.recv() => {
                info!("Received shutdown signal");
                break;
            }
        }
    }
    
    // Graceful shutdown
    info!("Starting graceful shutdown");
    cluster_manager.shutdown().await?;
    
    // Wait for components to finish
    tokio::select! {
        _ = event_handle => {},
        _ = combine_handle => {},
        _ = process_handle => {},
        _ = flight_handle => {},
        _ = grpc_handle => {},
        _ = tokio::time::sleep(tokio::time::Duration::from_secs(30)) => {
            warn!("Shutdown timeout reached");
        }
    }
    
    Ok(())
}

/// Process events from control plane
#[allow(unused_mut)]
#[allow(unused)]
async fn run_event_processor(
    mut process_rx: mpsc::Receiver<ControlEvent>,
    mut event_rx: mpsc::Receiver<ControlEvent>,
    config: Arc<IndexerConfig>,
    db_client: Arc<DbClient>,
    storage_client: Arc<StorageClient>,
    wal: Arc<WAL>,
    artifact_builder: Arc<ArtifactBuilder>,
    combine_worker: CombineWorker,
) -> Result<()> {
    let parquet_reader = ParquetReader::new(Arc::new(
        opendal::Operator::new(opendal::services::Fs::default())?.finish()
    ));
    
    loop {
        tokio::select! {
            Some(event) = event_rx.recv() => {
                match event {
                    ControlEvent::DataFilePublished {
                        file_ulid,
                        tenant,
                        signal,
                        partition_key,
                        parquet_url,
                        size_bytes,
                        row_count,
                        min_timestamp,
                        max_timestamp,
                    } => {
                        debug!(
                            file_ulid = %file_ulid,
                            tenant = %tenant,
                            signal = %signal,
                            "Processing DataFilePublished event"
                        );
                        
                        // Create data file record
                        let data_file = crate::DataFile {
                            file_ulid: file_ulid.clone(),
                            tenant: tenant.clone(),
                            signal: signal.clone(),
                            partition_key: partition_key.clone(),
                            parquet_url: parquet_url.clone(),
                            size_bytes: size_bytes as i64,
                            row_count: row_count as i64,
                            min_timestamp,
                            max_timestamp,
                            ingested_at: chrono::Utc::now(),
                            indexed_at: None,
                            metadata: serde_json::json!({}),
                        };
                        
                        // Store in database
                        db_client.upsert_data_file(&data_file).await?;
                        
                        // Process file
                        let start = std::time::Instant::now();
                        match process_file(
                            &data_file,
                            artifact_builder.as_ref(),
                            &parquet_reader,
                            storage_client.as_ref(),
                            db_client.as_ref(),
                            wal.as_ref(),
                            &config,
                        ).await {
                            Ok(()) => {
                                metrics::record_file_indexed(&tenant, &signal, start.elapsed());
                                
                                // Track for combining
                                combine_worker.track_file(
                                    tenant.clone(),
                                    signal.clone(),
                                    partition_key,
                                    file_ulid.clone(),
                                    size_bytes,
                                ).await?;
                                
                                // Report back to control plane
                                // TODO: Get control plane address from proper config
                                if let Err(e) = crate::events::report_batch_indexed(
                                    "http://localhost:50051",
                                    &file_ulid,
                                    "INDEXED",
                                    vec![], // TODO: collect actual URLs
                                ).await {
                                    warn!(error = %e, "Failed to report batch indexed");
                                }
                            }
                            Err(e) => {
                                error!(
                                    file_ulid = %file_ulid,
                                    error = %e,
                                    "Failed to process file"
                                );
                                metrics::record_file_index_failed(&tenant, &signal);
                            }
                        }
                    }
                    ControlEvent::PartitionSealed { tenant, signal, partition_key } => {
                        info!(
                            tenant = %tenant,
                            signal = %signal,
                            partition = %partition_key,
                            "Partition sealed, triggering combine"
                        );
                        // The combine worker will pick this up in its next check
                    }
                }
            }
            
            else => {
                debug!("Event processor channel closed");
                break;
            }
        }
    }
    
    Ok(())
}

/// Process a single data file (create all index artifacts)
#[allow(unused)]
async fn process_file(
    data_file: &crate::DataFile,
    artifact_builder: &ArtifactBuilder,
    parquet_reader: &ParquetReader,
    storage_client: &StorageClient,
    db_client: &DbClient,
    wal: &WAL,
    config: &IndexerConfig,
) -> Result<()> {
    let operation_id = format!("index_file_{}", data_file.file_ulid);

    // Check if already completed
    if wal.is_operation_completed(&operation_id) {
        debug!(file_ulid = %data_file.file_ulid, "File already indexed, skipping");
        return Ok(());
    }

    // Log operation start
    let operation = crate::WALOperation::IndexDataFile {
        file_ulid: data_file.file_ulid.clone(),
        tenant: data_file.tenant.clone(),
        signal: data_file.signal.clone(),
        partition_key: data_file.partition_key.clone(),
        parquet_url: data_file.parquet_url.clone(),
        size_bytes: data_file.size_bytes as u64,
    };

    wal.log_operation_start(operation_id.clone(), operation)?;

    info!(
        file_ulid = %data_file.file_ulid,
        parquet_url = %data_file.parquet_url,
        "Processing file for indexing"
    );

    // Read file statistics for zone map
    let file_stats = parquet_reader.read_file_stats(&data_file.parquet_url).await?;

    // Create zone map
    let zone_map = artifact_builder.build_zone_map(&file_stats)?;
    let zone_map_data = zone_map.serialize()?;
    
    let zone_map_metadata = crate::ArtifactMetadata {
        file_ulid: data_file.file_ulid.clone(),
        tenant: data_file.tenant.clone(),
        signal: data_file.signal.clone(),
        partition_key: data_file.partition_key.clone(),
        artifact_type: crate::ArtifactType::ZoneMap,
        size_bytes: zone_map_data.len() as u64,
        content_hash: crate::storage::calculate_hash(&zone_map_data),
        created_at: chrono::Utc::now(),
    };

    let zone_map_url = storage_client.store_artifact(&zone_map_metadata, zone_map_data).await?;
    
    // Store zone map metadata in database
    let zone_map_artifact = crate::IndexArtifact {
        artifact_id: uuid::Uuid::new_v4(),
        file_ulid: data_file.file_ulid.clone(),
        tenant: data_file.tenant.clone(),
        signal: data_file.signal.clone(),
        partition_key: data_file.partition_key.clone(),
        artifact_type: "zone_map".to_string(),
        artifact_url: zone_map_url,
        size_bytes: zone_map_metadata.size_bytes as i64,
        created_at: chrono::Utc::now(),
        metadata: serde_json::json!({}),
    };

    db_client.insert_index_artifact(&zone_map_artifact).await?;

    // Read column data for other artifacts (simplified - in practice you'd be more selective)
    let column_names = config.indexer.artifacts.bloom_columns
        .iter()
        .chain(config.indexer.artifacts.bitmap_columns.iter())
        .chain(config.indexer.artifacts.tantivy_text_fields.iter())
        .chain(config.indexer.artifacts.tantivy_keyword_fields.iter())
        .cloned()
        .collect::<Vec<_>>();

    let record_batches = parquet_reader.read_columns(
        &data_file.parquet_url,
        &column_names,
        None,
    ).await?;

    // Create bloom filter
    if !config.indexer.artifacts.bloom_columns.is_empty() {
        let bloom_filter = artifact_builder.build_bloom_filter(
            &data_file.file_ulid,
            &data_file.partition_key,
            &record_batches,
        )?;

        let bloom_data = bloom_filter.serialize()?;
        let bloom_metadata = crate::ArtifactMetadata {
            file_ulid: data_file.file_ulid.clone(),
            tenant: data_file.tenant.clone(),
            signal: data_file.signal.clone(),
            partition_key: data_file.partition_key.clone(),
            artifact_type: crate::ArtifactType::Bloom,
            size_bytes: bloom_data.len() as u64,
            content_hash: crate::storage::calculate_hash(&bloom_data),
            created_at: chrono::Utc::now(),
        };

        let bloom_url = storage_client.store_artifact(&bloom_metadata, bloom_data).await?;

        let bloom_artifact = crate::IndexArtifact {
            artifact_id: uuid::Uuid::new_v4(),
            file_ulid: data_file.file_ulid.clone(),
            tenant: data_file.tenant.clone(),
            signal: data_file.signal.clone(),
            partition_key: data_file.partition_key.clone(),
            artifact_type: "bloom".to_string(),
            artifact_url: bloom_url,
            size_bytes: bloom_metadata.size_bytes as i64,
            created_at: chrono::Utc::now(),
            metadata: serde_json::json!({}),
        };

        db_client.insert_index_artifact(&bloom_artifact).await?;
    }

    // Create bitmap index
    if !config.indexer.artifacts.bitmap_columns.is_empty() {
        let bitmap_index = artifact_builder.build_bitmap_index(
            &data_file.file_ulid,
            &data_file.partition_key,
            &record_batches,
        )?;

        let bitmap_data = bitmap_index.serialize()?;
        let bitmap_metadata = crate::ArtifactMetadata {
            file_ulid: data_file.file_ulid.clone(),
            tenant: data_file.tenant.clone(),
            signal: data_file.signal.clone(),
            partition_key: data_file.partition_key.clone(),
            artifact_type: crate::ArtifactType::Bitmap,
            size_bytes: bitmap_data.len() as u64,
            content_hash: crate::storage::calculate_hash(&bitmap_data),
            created_at: chrono::Utc::now(),
        };

        let bitmap_url = storage_client.store_artifact(&bitmap_metadata, bitmap_data).await?;

        let bitmap_artifact = crate::IndexArtifact {
            artifact_id: uuid::Uuid::new_v4(),
            file_ulid: data_file.file_ulid.clone(),
            tenant: data_file.tenant.clone(),
            signal: data_file.signal.clone(),
            partition_key: data_file.partition_key.clone(),
            artifact_type: "bitmap".to_string(),
            artifact_url: bitmap_url,
            size_bytes: bitmap_metadata.size_bytes as i64,
            created_at: chrono::Utc::now(),
            metadata: serde_json::json!({}),
        };

        db_client.insert_index_artifact(&bitmap_artifact).await?;
    }

    // Mark file as indexed
    db_client.mark_file_indexed(&data_file.file_ulid).await?;

    // Log completion
    wal.log_operation_completed(operation_id)?;

    info!(
        file_ulid = %data_file.file_ulid,
        "Successfully processed file and created index artifacts"
    );

    Ok(())
}

/// Handle local mode commands
#[allow(unused)]
async fn local_command(config: IndexerConfig, args: LocalArgs) -> Result<()> {
    let db_client = DbClient::new(&config.postgres_dsn()).await?;
    
    match args.command {
        LocalCommands::IndexFiles { input_dir, output_dir, tenant, signal } => {
            index_local_files(config, db_client, input_dir, output_dir, tenant, signal).await
        }
        LocalCommands::Coverage { tenant, signal, from_ms, to_ms } => {
            show_coverage(db_client, tenant, signal, from_ms, to_ms).await
        }
    }
}

/// Index files from a local directory
#[allow(unused)]
async fn index_local_files(
    _config: IndexerConfig,
    _db_client: DbClient,
    input_dir: PathBuf,
    output_dir: PathBuf,
    tenant: String,
    signal: String,
) -> Result<()> {
    info!(
        input_dir = ?input_dir,
        output_dir = ?output_dir,
        tenant = %tenant,
        signal = %signal,
        "Indexing local files"
    );

    // This would scan the input directory for parquet files,
    // process them, and output index artifacts to the output directory
    warn!("Local file indexing not fully implemented yet");
    
    Ok(())
}

/// Show coverage information
#[allow(unused)]
async fn show_coverage(
    db_client: DbClient,
    tenant: String,
    signal: String,
    from_ms: i64,
    to_ms: i64,
) -> Result<()> {
    info!(
        tenant = %tenant,
        signal = %signal,
        from_ms = from_ms,
        to_ms = to_ms,
        "Querying coverage"
    );

    let manifests = db_client.list_visible_deltas(&tenant, &signal, from_ms, to_ms).await?;
    
    println!("Coverage manifests found: {}", manifests.len());
    for manifest in manifests {
        println!(
            "Partition: {}, Level: {}, Deltas: {}",
            manifest.partition_key,
            manifest.level,
            manifest.deltas.len()
        );
        
        for delta in &manifest.deltas[..5.min(manifest.deltas.len())] {
            println!("  - File: {}", delta.file_ulid);
        }
        
        if manifest.deltas.len() > 5 {
            println!("  ... and {} more files", manifest.deltas.len() - 5);
        }
    }

    Ok(())
}

/// Run recovery from WAL
async fn run_recovery(
    wal: &WAL,
    _db_client: &DbClient,
    _storage_client: &StorageClient,
) -> Result<()> {
    let recovery_info = wal.get_recovery_info()?;
    
    info!(
        total_records = recovery_info.total_records,
        completed = recovery_info.completed_records,
        failed = recovery_info.failed_records,
        pending = recovery_info.pending_records,
        "WAL recovery information"
    );

    if recovery_info.pending_records > 0 {
        warn!(
            pending = recovery_info.pending_records,
            "Found pending operations in WAL - recovery not fully implemented"
        );
    }

    // In a full implementation, you would replay pending operations here
    Ok(())
}

/// Recovery command
#[allow(unused)]
async fn recover(config: IndexerConfig, args: RecoverArgs) -> Result<()> {
    let wal = WAL::new(&config.indexer.general.work_dir.join("wal"), 100 * 1024 * 1024)?;
    
    let recovery_info = wal.get_recovery_info()?;
    
    println!("WAL Recovery Information:");
    println!("  Total records: {}", recovery_info.total_records);
    println!("  Completed: {}", recovery_info.completed_records);
    println!("  Failed: {}", recovery_info.failed_records);
    println!("  Pending: {}", recovery_info.pending_records);
    println!("  Last sequence: {}", recovery_info.last_sequence);

    if args.dry_run {
        println!("\nDry run - no recovery operations performed");
        return Ok(());
    }

    if recovery_info.pending_records == 0 && !args.force {
        println!("No recovery needed");
        return Ok(());
    }

    warn!("Recovery execution not fully implemented yet");
    Ok(())
}

/// Health check command
#[allow(unused)]
async fn health_check(config: IndexerConfig, args: HealthArgs) -> Result<()> {
    let mut health = std::collections::HashMap::new();
    health.insert("indexer".to_string(), "ok".to_string());

    // Check database
    match DbClient::new(&config.postgres_dsn()).await {
        Ok(db) => {
            match db.health_check().await {
                Ok(db_health) => {
                    for (key, value) in db_health {
                        health.insert(format!("db.{}", key), value);
                    }
                }
                Err(e) => {
                    health.insert("db.connection".to_string(), format!("error: {}", e));
                }
            }
        }
        Err(e) => {
            health.insert("db.connection".to_string(), format!("error: {}", e));
        }
    }

    // Check storage
    match StorageClient::new(&config.artifact_uri(), None) {
        Ok(storage) => {
            match storage.health_check().await {
                Ok(storage_health) => {
                    for (key, value) in storage_health {
                        health.insert(format!("storage.{}", key), value);
                    }
                }
                Err(e) => {
                    health.insert("storage.health".to_string(), format!("error: {}", e));
                }
            }
        }
        Err(e) => {
            health.insert("storage.connection".to_string(), format!("error: {}", e));
        }
    }

    // Output
    match args.format.as_str() {
        "json" => {
            println!("{}", serde_json::to_string_pretty(&health)?);
        }
        _ => {
            println!("Health Check Results:");
            for (key, value) in health {
                println!("  {}: {}", key, value);
            }
        }
    }

    Ok(())
}