pub mod format_helpers;
pub mod forward_ingest;
pub mod indexing_connector;
pub mod search_service;
pub mod datafusion_search_service;
pub mod integrated_search_service;
pub mod sql_rewriter;
pub mod enhanced_sql_rewriter;
pub mod backpressure;
use std::sync::Arc;
use tracing::{info, warn, error};

use std::net::SocketAddr;
use anyhow::{ Result };
use opendal::Operator;

use openwit_config::UnifiedConfig;
use openwit_common;
use openwit_actors::{
    // spawn_monitors,
    // spawn_decision_actor,
    // spawn_scale_actor,
    // spawn_ingestion_pipeline,
    // spawn_indexer_components,
    // spawn_storage_components,
    spawn_query_server,
    // StorageMessage,
    // IndexerMessage,
    // scale::ScaleActor,
};

use openwit_network::ClusterRuntime;

pub async fn run_server(config: UnifiedConfig) -> Result<(), Box<dyn std::error::Error>> {
    // Log the ingestion source configuration
    tracing::info!("🔌 Ingestion configuration:");
    tracing::info!("  Kafka: {}", if config.ingestion.kafka.brokers.is_some() { "✓" } else { "✗" });
    tracing::info!("  gRPC:  {}", if config.ingestion.sources.grpc.enabled { "✓" } else { "✗" });
    tracing::info!("  HTTP:  {}", if config.ingestion.sources.http.enabled { "✓" } else { "✗" });

    // --- 1. Prometheus/Metrics Server (conditional) ---

    tokio::spawn(openwit_common::system_metrics::start_system_metrics_collector());

    let pod_role = if !config.deployment.kubernetes.pod_role.is_empty() {
        config.deployment.kubernetes.pod_role.clone()
    } else {
        "monolith".to_string()
    };

    let roles: Vec<String> = pod_role
    .split(',')
    .map(|s| s.trim().to_lowercase())
    .collect();

    tracing::info!("💡 Pod role resolved as: {:?}", roles);

    // Network configuration (gossip functionality deprecated)
    let self_node_name = "server-1".to_string();
    let gossip_addr: SocketAddr = "127.0.0.1:7946".parse()?;
    let seed_nodes: Vec<SocketAddr> = Vec::new();

        let runtime = ClusterRuntime::start(&self_node_name.clone(), gossip_addr, seed_nodes,"main").await?;
        let _cluster_handle = runtime.handle(); // ClusterHandle
        
    // Create a single shared metastore instance
    let shared_metastore: Arc<dyn openwit_metastore::MetaStore> = match config.metastore.backend.as_str() {
        "postgres" | "postgresql" => {
            let pg_config = openwit_metastore::postgres_metastore::PostgresMetastoreConfig {
                connection_string: config.metastore.postgres.connection_string.clone(),
                max_connections: config.metastore.postgres.max_connections,
                schema_name: config.metastore.postgres.schema_name.clone(),
            };
            Arc::new(openwit_metastore::postgres_metastore::PostgresMetaStore::new(pg_config).await?)
        }
        "sled" => {
            let sled_config = openwit_metastore::sled_metastore::SledMetastoreConfig {
                path: config.metastore.sled.path.clone(),
                cache_size_mb: config.metastore.sled.cache_size_mb as usize,
            };
            Arc::new(openwit_metastore::sled_metastore::SledMetaStore::new(sled_config).await?)
        }
        _ => return Err(anyhow::anyhow!("Unknown metastore backend: {}", config.metastore.backend).into()),
    };
    // REMOVED: Scale/Decision actors - not needed for single-node ingestion
    // These were causing spam in logs every 5 seconds
    // let alert_rx = spawn_monitors();
    // let scale_rx = spawn_decision_actor(alert_rx);
    // let scale_handle = spawn_scale_actor(scale_rx, cluster_handle.clone()).await;
    
    // Create a shared ingestion sender for all sources
    let mut pipeline_ingest_tx: Option<tokio::sync::mpsc::Sender<openwit_ingestion::IngestedMessage>> = None;
    let mut storage_rx: Option<tokio::sync::mpsc::Receiver<Vec<openwit_ingestion::IngestedMessage>>> = None;
    let mut segment_rx_holder: Option<tokio::sync::mpsc::Receiver<String>> = None;


    if roles.contains(&"ingest".to_string()) || roles.contains(&"monolith".to_string()) {
        let node_id = "ingest-1".to_string();
        
        // Create the ingestion config
        let ingestion_config = openwit_ingestion::IngestionConfig {
            max_buffer_memory: (config.memory.buffer_pool.size_mb * 1024 * 1024) as usize,
            batch_size: config.processing.as_ref()
                .map(|p| p.buffer.flush_size_messages as usize)
                .unwrap_or(10000), // Default 10K messages
            batch_timeout_ms: config.processing.as_ref()
                .map(|p| p.buffer.batch_timeout_ms)
                .unwrap_or(30000), // Default 30 seconds
            // WAL configuration removed - no longer needed
        };
        
        // Create storage channel with adaptive backpressure
        let backpressure_config = backpressure::BackpressureConfig {
            initial_size: 100,
            min_size: 50,
            max_size: 10000,  // Allow up to 10k batches in flight
            target_utilization: 70.0,
            check_interval_secs: 10,
            growth_factor: 1.5,
            shrink_factor: 0.8,
        };
        
        info!("🎯 Creating adaptive channel with initial size: {}, max: {}", 
            backpressure_config.initial_size, 
            backpressure_config.max_size
        );
        
        // For now, use regular channel with larger buffer based on config
        let channel_size = backpressure_config.initial_size;
        let (storage_tx, storage_rx_temp) = tokio::sync::mpsc::channel::<Vec<openwit_ingestion::IngestedMessage>>(channel_size);
        
        // Monitor channel health
        let storage_metrics = Arc::new(backpressure::ChannelMetrics::new(channel_size));
        let metrics_clone = storage_metrics.clone();
        let tx_clone = storage_tx.clone();
        
        // Spawn monitoring task
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(10));
            
            loop {
                interval.tick().await;
                
                // Check channel capacity
                let capacity = tx_clone.capacity();
                let is_full = capacity == 0;
                
                if is_full {
                    metrics_clone.record_full();
                    warn!("⚠️ Storage channel is full! Capacity: 0/{}", channel_size);
                }
                
                let (sent, dropped, blocked, full_events) = metrics_clone.get_stats();
                if full_events > 0 || blocked > 0 {
                    info!(
                        "📊 Channel pressure detected - Sent: {}, Dropped: {}, Blocked: {}, Full events: {}",
                        sent, dropped, blocked, full_events
                    );
                }
                
                metrics_clone.reset_period_stats();
            }
        });
        
        // IngestionPipeline removed - using direct OTLP processing instead
        // let (pipeline, ingest_sender) = openwit_ingestion::IngestionPipeline::new(
        //     node_id.clone(),
        //     ingestion_config,
        //     storage_tx,
        // ).await?;
        
        // Store the sender for other sources to use
        // pipeline_ingest_tx = Some(ingest_sender.clone());
        storage_rx = Some(storage_rx_temp);
        // Note: index_rx is now created later by storage handler as segment_rx_holder
        
        // Pipeline removed - using direct OTLP processing
        // let _pipeline_handle = tokio::spawn(async move {
        //     if let Err(e) = pipeline.start().await {
        //         error!("Ingestion pipeline failed: {:?}", e);
        //     }
        // });
        
        // Start Kafka source if enabled
        info!("🔍 Checking Kafka configuration:");
        info!("   Brokers: {:?}", config.ingestion.kafka.brokers);
        info!("   Enabled: {}", config.ingestion.sources.kafka.enabled);
        
        if config.ingestion.kafka.brokers.is_some() && config.ingestion.sources.kafka.enabled {
            let kafka_cfg = &config.ingestion.kafka;
            info!("📡 Starting Kafka ingestion from topics: {:?}", kafka_cfg.topics);
            
            let kafka_config = config.clone();
            
            tokio::spawn(async move {
                let kafka_server = openwit_kafka::KafkaServer::new(kafka_config);
                if let Err(e) = kafka_server.start().await {
                    error!("Kafka server failed: {:?}", e);
                }
            });
            
            info!("✅ Kafka ingestion service started");
        }
        
        
        // TODO: Remove forward_ingest setup once REST endpoint is migrated to new ingestion
        // forward_ingest::set_ingestor_sender(ingestor_tx);
        tracing::info!("✅ Ingestor pipeline launched");
    }
    
    // Spawn storage handler if we have a storage receiver
    if false && storage_rx.is_some() {
        let mut storage_rx = storage_rx.unwrap();
        let _storage_config = config.storage.clone();
        
        // Use shared metastore instance
        let storage_metastore = shared_metastore.clone();
        
        // Create channel to send segment IDs to indexer
        let (segment_tx, segment_rx_temp) = tokio::sync::mpsc::channel::<String>(100);
        segment_rx_holder = Some(segment_rx_temp);
        
        // Configure storage
        let storage_config_for_actor = openwit_storage::config::StorageConfig {
            local_storage_path: "./data/storage".to_string(),
            parquet: openwit_storage::config::ParquetConfig {
                row_group_size: 10000,
                max_rows_per_file: 100000,
                target_file_size_bytes: 100 * 1024 * 1024, // 100MB
                compression: openwit_storage::config::CompressionType::Snappy,
                enable_bloom_filter: true,
                enable_statistics: true,
                bloom_filter_fpp: 0.01,
                enable_dictionary: true,
                page_size_bytes: 1024 * 1024,
            },
            object_store: None, // Will be set below if Azure is configured
            ..Default::default()
        };
        // Setup cloud operator if configured (using OpenDAL)
        let cloud_operator = if config.storage.azure.enabled {
                info!("☁️ Initializing Azure Blob Storage with OpenDAL...");
                
                // Create cloud operator directly
                match create_cloud_operator(&config).await {
                    Ok(operator) => {
                        info!("✅ Azure storage connected: {}/{}", 
                            config.storage.azure.account_name, 
                            config.storage.azure.container_name);
                        operator
                    }
                    Err(e) => {
                        error!("❌ Failed to connect to Azure: {:?}", e);
                        None
                    }
                }
            } else {
                info!("💾 Using local storage only");
                None
            };
            
            // Spawn storage actor
            let storage_tx = openwit_actors::spawn_storage_components(
                storage_config_for_actor,
                storage_metastore.clone(),
                segment_tx,
                cloud_operator,
            );
            
            // Forward messages from ingestion to storage actor
            tokio::spawn(async move {
                while let Some(messages) = storage_rx.recv().await {
                    if let Err(e) = storage_tx.send(openwit_actors::StorageMessage::StoreBatch(messages)).await {
                        error!("Failed to send messages to storage actor: {:?}", e);
                        break;
                    }
                }
                
                info!("Storage handler stopped");
            });
    }

    // Spawn indexing service if enabled
    if (roles.contains(&"index".to_string()) || false) && segment_rx_holder.is_some() {
        let mut segment_rx = segment_rx_holder.unwrap();
        
        // Use shared metastore instance
        let metastore = shared_metastore.clone();
        
        // Start indexing actor
        let index_path = "./data/index".to_string();
        let indexer_tx = openwit_actors::spawn_indexer_components(
            metastore,
            index_path,
        );
        
        // Forward segment IDs from storage to indexer
        tokio::spawn(async move {
            while let Some(segment_id) = segment_rx.recv().await {
                if let Err(e) = indexer_tx.send(openwit_actors::IndexerMessage::IndexSegment(segment_id)).await {
                    error!("Failed to send segment to indexer: {:?}", e);
                    break;
                }
            }
        });
    }
    
    if roles.contains(&"query".to_string()) || false {
        // Use shared metastore instance
        let search_metastore = shared_metastore.clone();
        
        if let Err(e) = search_service::init_search_service(
            "./data/index".to_string(),
            search_metastore.clone(),
        ).await {
            error!("Failed to initialize search service: {:?}", e);
        } else {
            info!("✅ Search service initialized");
        }
        
        // Initialize DataFusion search service for querying parquet files
        if let Err(e) = datafusion_search_service::init_datafusion_service(
            "./data/storage".to_string()
        ).await {
            error!("Failed to initialize DataFusion search service: {:?}", e);
        } else {
            info!("✅ DataFusion search service initialized");
        }
        
        // Initialize integrated search service
        if let Err(e) = integrated_search_service::init_integrated_service(
            "./data/index".to_string(),
            "./data/storage".to_string(),
            search_metastore,
        ).await {
            error!("Failed to initialize integrated search service: {:?}", e);
        } else {
            info!("✅ Integrated search service initialized");
        }
        
        spawn_query_server(); // stub for now
    }
    // Storage functionality is now handled by the storage actor spawned above
    // Metrics/Prometheus monitoring removed
    let metrics_handle: Option<tokio::task::JoinHandle<()>> = None;
    
    // --- HTTP REST API Server (conditional) ---
    let http_cfg = &config.ingestion.sources.http;
    let http_handle = if http_cfg.enabled {
        // Use the dedicated openwit-http crate for HTTP ingestion
        let http_ingest_tx = pipeline_ingest_tx.clone();
        let http_config = config.clone();
        
        Some(
            tokio::spawn(async move {
                match openwit_http::HttpServer::new(http_config, http_ingest_tx) {
                    Ok(server) => {
                        if let Err(e) = server.start().await {
                            error!("HTTP server failed: {:?}", e);
                        }
                    }
                    Err(e) => {
                        error!("Failed to create HTTP server: {:?}", e);
                    }
                }
            })
        )
    } else {
        tracing::info!("HTTP REST API disabled");
        None
    };


    // --- 2. gRPC Server (conditional) ---
    let grpc_cfg = &config.ingestion.sources.grpc;
    let grpc_handle = if grpc_cfg.enabled {
        let grpc_config = config.clone();
        let grpc_ingest_tx = pipeline_ingest_tx.clone();

        Some(
            tokio::spawn(async move {
                let grpc_server = openwit_grpc::GrpcServer::new(grpc_config, grpc_ingest_tx);
                if let Err(e) = grpc_server.start().await {
                    error!("gRPC server failed: {:?}", e);
                }
                Ok::<(), anyhow::Error>(())
            })
        )
    } else {
        tracing::info!("gRPC server disabled via config");
        None
    };

    // --- 3. Await all enabled servers ---
    let mut handles: Vec<(&str, tokio::task::JoinHandle<Result<(), anyhow::Error>>)> = vec![];
    if let Some(handle) = http_handle {
        handles.push(("HTTP", tokio::spawn(async move { handle.await.map_err(|e| anyhow::anyhow!("HTTP server error: {}", e))?; Ok(()) })));
    }
    if let Some(handle) = grpc_handle {
        handles.push(("gRPC", handle));
    }

    if handles.is_empty() {
        tracing::warn!("No servers enabled; exiting immediately.");
    } else {
        // Wait for all servers
        for (name, handle) in handles {
            match handle.await {
                Ok(_) => tracing::info!("{} server shut down successfully", name),
                Err(e) => tracing::error!("{} server task panicked: {:?}", name, e),
            }
        }
    }
    Ok(())
}

/// Create cloud operator for storage (copied from removed pipeline module)
async fn create_cloud_operator(config: &UnifiedConfig) -> Result<Option<opendal::Operator>> {
    if config.storage.azure.enabled {
        let builder = opendal::services::Azblob::default()
            .account_name(&config.storage.azure.account_name)
            .account_key(&config.storage.azure.access_key)
            .container(&config.storage.azure.container_name)
            .endpoint(&config.storage.azure.endpoint);
        
        let operator = Operator::new(builder)?.finish();
        
        Ok(Some(operator))
    } else {
        Ok(None)
    }
}
