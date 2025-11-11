use clap::{Parser, Subcommand};
use tracing_subscriber::{fmt, EnvFilter};
use tracing_subscriber::prelude::*;
use tokio::signal;
use std::sync::Arc;
use std::env;
use std::path::Path;
use anyhow::{Result, Context};

mod mode_based_startup;
mod mode_based_startup_v2;

#[derive(Parser)]
#[command(name = "openwit", about = "OpenWit Distributed Log Ingestion System", version, author)]
struct Args {
    #[command(subcommand)]
    command: Option<Commands>,
    
    /// Configuration file path
    #[arg(long, default_value = "config/openwit-azure.yaml")]
    config: String,
    
    /// Log level
    #[arg(long, default_value = "info")]
    log_level: String,
}

#[derive(Subcommand)]
enum Commands {
    /// Start all services (default if no command specified)
    All {
        /// GRPC bind address for ingestion
        #[arg(long, default_value = "0.0.0.0:4317")]
        grpc_addr: String,
        
        /// Control plane bind address  
        #[arg(long, default_value = "0.0.0.0:8080")]
        control_addr: String,
        
        /// Inter-node communication port
        #[arg(long, default_value = "9090")]
        inter_node_port: u16,
    },
    
    /// Start only the ingestion service
    Ingestion {
        /// GRPC bind address
        #[arg(long, default_value = "0.0.0.0:4317")]
        grpc_addr: String,
        
        /// Use high-throughput configuration (8GB memory, 50k batch size)
        #[arg(long)]
        high_throughput: bool,
        
        /// Use sustained load configuration
        #[arg(long)]
        sustained_load: bool,
        
        /// Use memory stress configuration
        #[arg(long)]
        memory_stress: bool,
    },
    
    /// Start only the control plane
    ControlPlane {
        /// Control plane bind address
        #[arg(long, default_value = "0.0.0.0:8080")]
        addr: String,
    },
    
    /// Start only the storage service
    Storage {
        /// Storage service bind address
        #[arg(long, default_value = "0.0.0.0:8081")]
        addr: String,
    },
    
    /// Start only the inter-node communication service
    InterNode {
        /// Inter-node service port
        #[arg(long, default_value = "9090")]
        port: u16,
        
        /// Node ID for this instance
        #[arg(long)]
        node_id: Option<String>,
    },
    
    /// Performance testing commands
    Perf {
        #[command(subcommand)]
        test_command: PerfCommands,
    },
}

#[derive(Subcommand)]
enum PerfCommands {
    /// Run baseline performance test
    Baseline {
        /// Target endpoint
        #[arg(long, default_value = "localhost:4317")]
        endpoint: String,
        
        /// Number of messages to send
        #[arg(long, default_value = "100000")]
        messages: u64,
    },
    
    /// Run extreme throughput test (target: 700k/sec)
    Extreme {
        /// Target endpoint
        #[arg(long, default_value = "localhost:4317")]
        endpoint: String,
    },
    
    /// Run billion message test (target: 5 minutes)
    Billion {
        /// Target endpoint
        #[arg(long, default_value = "localhost:4317")]
        endpoint: String,
    },
    
    /// Run comprehensive test suite
    Suite {
        /// Target endpoint
        #[arg(long, default_value = "localhost:4317")]
        endpoint: String,
    },
}

/// Detects if the application is running in a Kubernetes environment
fn detect_kubernetes_environment() -> bool {
    // Check for Kubernetes service account token
    if Path::new("/var/run/secrets/kubernetes.io/serviceaccount/token").exists() {
        return true;
    }
    
    // Check for Kubernetes environment variables
    if env::var("KUBERNETES_SERVICE_HOST").is_ok() {
        return true;
    }
    
    // Check for common Kubernetes pod environment variables
    if env::var("KUBERNETES_PORT").is_ok() || 
       env::var("KUBERNETES_PORT_443_TCP").is_ok() ||
       env::var("KUBERNETES_SERVICE_PORT").is_ok() ||
       env::var("KUBERNETES_SERVICE_PORT_HTTPS").is_ok() {
        return true;
    }
    
    // Check if running with a Kubernetes-style hostname
    if let Ok(hostname) = env::var("HOSTNAME") {
        // Kubernetes pod names often follow patterns like "app-name-deployment-id-pod-id"
        if hostname.contains('-') && (hostname.len() > 20 || hostname.chars().filter(|c| *c == '-').count() >= 2) {
            // This is a heuristic - pods often have longer names with multiple hyphens
            return true;
        }
    }
    
    false
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    
    // Initialize tracing
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(&args.log_level));

    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(env_filter)
        .init();

    tracing::info!("Starting OpenWit Distributed Log Ingestion System");
    
    // Detect Kubernetes environment
    let is_kubernetes = detect_kubernetes_environment();
    if is_kubernetes {
        tracing::info!("Kubernetes environment detected");
    } else {
        tracing::info!("Kubernetes environment not detected");
    }
    
    // Load configuration
    let config = openwit_config::load_config_from_yaml(&args.config)?;
    tracing::info!("Configuration loaded from: {}", args.config);
    
    // Log key configuration details
    if let Some(kafka) = &config.kafka {
        tracing::info!("Kafka Configuration:");
        tracing::info!("  Brokers: {}", kafka.brokers);
        tracing::info!("  Topics: {:?}", kafka.topics);
        tracing::info!("  Group ID: {}", kafka.group_id);
    }
    
    if let Some(storage) = &config.storage {
        tracing::info!("Storage Configuration:");
        if let Some(azure) = &storage.azure {
            tracing::info!("  Type: Azure Blob Storage");
            tracing::info!("  Account: {}", azure.account_name);
            tracing::info!("  Container: {}", azure.container_name);
        }
    }

    match args.command {
        None | Some(Commands::All { grpc_addr, control_addr, inter_node_port }) => {
            let grpc_addr = args.command.as_ref()
                .and_then(|cmd| if let Commands::All { grpc_addr, .. } = cmd { Some(grpc_addr.clone()) } else { None })
                .unwrap_or_else(|| "0.0.0.0:4317".to_string());
            let control_addr = args.command.as_ref()
                .and_then(|cmd| if let Commands::All { control_addr, .. } = cmd { Some(control_addr.clone()) } else { None })
                .unwrap_or_else(|| "0.0.0.0:8080".to_string());
            let inter_node_port = args.command.as_ref()
                .and_then(|cmd| if let Commands::All { inter_node_port, .. } = cmd { Some(*inter_node_port) } else { None })
                .unwrap_or(9090);
            
            mode_based_startup_v2::start_by_mode(config, grpc_addr, control_addr, inter_node_port).await?;
        },
        
        Some(Commands::Ingestion { grpc_addr, high_throughput, sustained_load, memory_stress }) => {
            tracing::info!("Starting ingestion service on {}", grpc_addr);
            start_ingestion_service(config, grpc_addr, high_throughput, sustained_load, memory_stress).await?;
        },
        
        Some(Commands::ControlPlane { addr }) => {
            tracing::info!("Starting control plane on {}", addr);
            start_control_plane(config, addr).await?;
        },
        
        Some(Commands::Storage { addr }) => {
            tracing::info!("Starting storage service on {}", addr);
            start_storage_service(config, addr).await?;
        },
        
        Some(Commands::InterNode { port, node_id }) => {
            let node_id = node_id.unwrap_or_else(|| format!("node-{}", uuid::Uuid::new_v4()));
            tracing::info!("Starting inter-node service on port {} with ID {}", port, node_id);
            start_inter_node_service(config, port, node_id).await?;
        },
        
        Some(Commands::Perf { test_command }) => {
            tracing::info!("Running performance tests");
            run_performance_tests(test_command).await?;
        },
    }

    Ok(())
}

async fn start_all_services(
    config: openwit_config::Config,
    grpc_addr: String,
    control_addr: String, 
    inter_node_port: u16,
) -> Result<()> {
    tracing::info!("Initializing all OpenWit services...");
    tracing::info!("  Ingestion Service: {}", grpc_addr);
    tracing::info!("  Control Plane: {}", control_addr);
    tracing::info!("  Inter-Node: port {}", inter_node_port);
    tracing::info!("  Storage Service: integrated");

    // Spawn all services concurrently
    let mut handles = vec![];

    // 1. Start ingestion service
    {
        let config = config.clone();
        let grpc_addr = grpc_addr.clone();
        let handle = tokio::spawn(async move {
            tracing::info!("Starting ingestion service...");
            if let Err(e) = start_ingestion_service(config, grpc_addr, false, false, false).await {
                tracing::error!("Ingestion service failed: {:?}", e);
            }
        });
        handles.push(handle);
    }

    // 2. Start control plane
    {
        let config = config.clone();
        let control_addr = control_addr.clone();
        let handle = tokio::spawn(async move {
            tracing::info!("Starting control plane...");
            if let Err(e) = start_control_plane(config, control_addr).await {
                tracing::error!("Control plane failed: {:?}", e);
            }
        });
        handles.push(handle);
    }

    // 3. Start inter-node service  
    {
        let config = config.clone();
        let node_id = format!("node-{}", uuid::Uuid::new_v4());
        let handle = tokio::spawn(async move {
            tracing::info!("Starting inter-node service...");
            if let Err(e) = start_inter_node_service(config, inter_node_port, node_id).await {
                tracing::error!("Inter-node service failed: {:?}", e);
            }
        });
        handles.push(handle);
    }

    // 4. Start storage service
    {
        let config = config.clone();
        let handle = tokio::spawn(async move {
            tracing::info!("Starting storage service...");
            if let Err(e) = start_storage_service(config, "0.0.0.0:8081".to_string()).await {
                tracing::error!("Storage service failed: {:?}", e);
            }
        });
        handles.push(handle);
    }

    tracing::info!("All services started. System ready for ingestion!");
    tracing::info!("💡 Performance Testing:");
    tracing::info!("  • Baseline: cargo run -- perf baseline");
    tracing::info!("  • Extreme: cargo run -- perf extreme");
    tracing::info!("  • Billion logs: cargo run -- perf billion");
    
    // Wait for interrupt signal
    signal::ctrl_c().await?;
    tracing::info!("🛑 Received shutdown signal, stopping all services...");
    
    for handle in handles {
        handle.abort();
    }
    
    tracing::info!("All services stopped gracefully");
    Ok(())
}

async fn start_ingestion_service(
    config: openwit_config::Config,
    grpc_addr: String,
    high_throughput: bool,
    sustained_load: bool,
    memory_stress: bool,
) -> Result<()> {
    // Create ingestion configuration based on performance flags
    let ingestion_config = if high_throughput {
        tracing::info!("Using HIGH THROUGHPUT configuration (8GB memory, 50k batch size)");
        openwit_ingestion::types::IngestionConfig::high_throughput()
    } else if sustained_load {
        tracing::info!("🔄 Using SUSTAINED LOAD configuration (4GB memory, 20k batch size)");
        openwit_ingestion::types::IngestionConfig::sustained_load()
    } else if memory_stress {
        tracing::info!("🧠 Using MEMORY STRESS configuration (16GB memory, 100k batch size)");
        openwit_ingestion::types::IngestionConfig::memory_stress()
    } else {
        tracing::info!("📊 Using DEFAULT configuration (256MB memory, 1k batch size)");
        openwit_ingestion::types::IngestionConfig::default()
    };

    tracing::info!("📊 Ingestion Configuration:");
    tracing::info!("  • Memory: {:.1} GB", ingestion_config.max_buffer_memory as f64 / (1024.0 * 1024.0 * 1024.0));
    tracing::info!("  • Batch size: {:?}", ingestion_config.batch_size);
    tracing::info!("  • Timeout: {}ms", ingestion_config.batch_timeout_ms);
    tracing::info!("  • WAL file size: {:.1} MB", ingestion_config.wal_file_size as f64 / (1024.0 * 1024.0));

    // Create storage channel (placeholder for now)
    let (storage_tx, mut storage_rx) = tokio::sync::mpsc::channel(1000);
    
    // Create node ID
    let node_id = format!("ingestion-{}", uuid::Uuid::new_v4());
    
    // Start the ingestion pipeline
    let (pipeline, ingest_tx) = openwit_ingestion::pipeline::IngestionPipeline::new(
        node_id.clone(), 
        ingestion_config, 
        storage_tx
    ).await?;
    
    // Start gRPC server for ingestion
    let grpc_handle = {
        let ingest_tx = ingest_tx.clone();
        let addr = grpc_addr.clone();
        tokio::spawn(async move {
            tracing::info!("Starting gRPC ingestion server on {}", addr);
            if let Err(e) = openwit_ingestion::pipeline::start_grpc_source(addr, ingest_tx).await {
                tracing::error!("gRPC server failed: {:?}", e);
            }
        })
    };
    
    // Start Kafka consumer if configured
    // DISABLED: Kafka is started by the ingestion actors to prevent duplicate consumers
    let kafka_handle = None;
    /* Commented out to prevent duplicate Kafka consumer spawning
    let kafka_handle = if config.kafka.is_some() {
        let ingest_tx = ingest_tx.clone();
        let kafka_config = config.kafka.clone().unwrap();
        Some(tokio::spawn(async move {
            tracing::info!("Starting Kafka consumers...");
            if let Err(e) = openwit_ingestion::pipeline::start_kafka_source(
                kafka_config.brokers.split(',').map(|s| s.to_string()).collect(),
                kafka_config.topics,
                kafka_config.group_id,
                ingest_tx
            ).await {
                tracing::error!("Kafka consumers failed: {:?}", e);
            }
        }))
    } else {
        None
    };
    */
    
    // Start storage handler (placeholder)
    let storage_handle = tokio::spawn(async move {
        while let Some(messages) = storage_rx.recv().await {
            tracing::debug!("📦 Received {} messages for storage", messages.len());
            // TODO: Forward to actual storage service
        }
    });
    
    // Start the pipeline
    let pipeline_handle = tokio::spawn(async move {
        if let Err(e) = pipeline.start().await {
            tracing::error!("Pipeline failed: {:?}", e);
        }
    });
    
    tracing::info!("Ingestion service started successfully");
    tracing::info!("  • gRPC endpoint: {}", grpc_addr);
    tracing::info!("  • Node ID: {}", node_id);
    
    // Wait for completion or shutdown
    tokio::select! {
        _ = grpc_handle => tracing::info!("gRPC server stopped"),
        _ = storage_handle => tracing::info!("Storage handler stopped"), 
        _ = pipeline_handle => tracing::info!("Pipeline stopped"),
        _ = async {
            if let Some(handle) = kafka_handle {
                handle.await
            } else {
                futures::future::pending::<()>().await
            }
        } => tracing::info!("Kafka consumers stopped"),
        _ = signal::ctrl_c() => tracing::info!("Received shutdown signal"),
    }
    
    Ok(())
}

async fn start_control_plane(
    config: openwit_config::Config,
    addr: String,
) -> Result<()> {
    tracing::info!("Control plane starting on {}", addr);
    
    // Parse address
    let addr = addr.parse::<std::net::SocketAddr>()
        .context("Invalid control plane address")?;
    
    // Get node ID from config or generate
    let node_id = config.gossip.as_ref()
        .map(|g| g.self_node_name.clone())
        .unwrap_or_else(|| format!("control-{}", uuid::Uuid::new_v4()));
    
    // Get cluster handle from global context or create a minimal one
    let cluster_handle = if let Some(gossip_config) = &config.gossip {
        // Use the existing cluster runtime if available
        openwit_network::ClusterRuntime::new(
            &gossip_config.self_node_name,
            &gossip_config.listen_addr,
            gossip_config.seed_nodes.clone(),
        )
        .await?
        .handle()
    } else {
        // Create a minimal cluster handle for standalone mode
        tracing::warn!("No gossip config found, control plane running in standalone mode");
        // This would need a mock/minimal implementation
        return Err(anyhow::anyhow!("Control plane requires gossip configuration"));
    };
    
    // Start the actual control plane
    openwit_control_plane::start_control_plane(
        node_id,
        cluster_handle,
        addr,
    )
    .await?;
    
    Ok(())
}

async fn start_storage_service(
    _config: openwit_config::Config,
    _addr: String,
) -> Result<()> {
    tracing::info!("💾 Storage service initializing...");
    // TODO: Implement storage service startup
    // This would start the WAL readers, Parquet writers, and object storage
    tracing::warn!("Storage service implementation pending");
    
    // Keep running
    signal::ctrl_c().await?;
    Ok(())
}

async fn start_inter_node_service(
    _config: openwit_config::Config,
    _port: u16,
    node_id: String,
) -> Result<()> {
    tracing::info!("🔗 Inter-node service starting with ID: {}", node_id);
    // TODO: Implement inter-node service startup
    // This would start the node server, data transfer actors, and command executors
    tracing::warn!("Inter-node service implementation pending");
    
    // Keep running
    signal::ctrl_c().await?;
    Ok(())
}

async fn run_performance_tests(test_command: PerfCommands) -> Result<()> {
    use openwit_perf_test::{HighPerformanceConfigs, PerformanceTestRunner, LoadGenerator, load_generator::LoadConfig};
    use std::time::Instant;

    match test_command {
        PerfCommands::Baseline { endpoint, messages } => {
            tracing::info!("📊 Running baseline performance test");
            tracing::info!("  • Endpoint: {}", endpoint);
            tracing::info!("  • Messages: {:?}", messages);
            
            let mut config = LoadConfig::default();
            config.total_messages = messages;
            
            let start = Instant::now();
            let metrics = Arc::new(openwit_perf_test::MetricsCollector::new());
            let generator = LoadGenerator::new(endpoint, config.clone(), metrics);
            
            generator.run().await?;
            
            let duration = start.elapsed();
            let throughput = messages as f64 / duration.as_secs_f64();
            
            tracing::info!("Baseline test completed!");
            tracing::info!("  • Duration: {:.2}s", duration.as_secs_f64());
            tracing::info!("  • Throughput: {:.0} msg/sec", throughput);
            tracing::info!("  • Target (700k/sec): {:.1}%", (throughput / 700_000.0) * 100.0);
        },
        
        PerfCommands::Extreme { endpoint } => {
            tracing::info!("🔥 Running extreme throughput test (Target: 700k/sec)");
            let config = HighPerformanceConfigs::extreme_throughput();
            
            let start = Instant::now();
            let metrics = Arc::new(openwit_perf_test::MetricsCollector::new());
            let generator = LoadGenerator::new(endpoint, config.clone(), metrics);
            
            generator.run().await?;
            
            let duration = start.elapsed();
            let throughput = config.total_messages as f64 / duration.as_secs_f64();
            
            tracing::info!("🔥 Extreme test completed!");
            tracing::info!("  • Duration: {:.2}s", duration.as_secs_f64());
            tracing::info!("  • Throughput: {:.0} msg/sec", throughput);
            tracing::info!("  • Target (700k/sec): {:.1}%", (throughput / 700_000.0) * 100.0);
        },
        
        PerfCommands::Billion { endpoint } => {
            tracing::info!("🌟 Running billion message test (Target: 5 minutes)");
            let mut config = HighPerformanceConfigs::extreme_throughput();
            config.total_messages = 1_000_000_000;
            
            tracing::info!("📊 Configuration:");
            tracing::info!("  • Messages: 1 BILLION");
            tracing::info!("  • Batch size: {:?}", config.batch_size);
            tracing::info!("  • Connections: {}", config.concurrent_connections);
            tracing::info!("  • Target: 5 minutes (700k msg/sec)");
            
            let start = Instant::now();
            let metrics = Arc::new(openwit_perf_test::MetricsCollector::new());
            let generator = LoadGenerator::new(endpoint, config.clone(), metrics);
            
            generator.run().await?;
            
            let duration = start.elapsed();
            let throughput = config.total_messages as f64 / duration.as_secs_f64();
            let minutes = duration.as_secs_f64() / 60.0;
            
            tracing::info!("🌟 Billion message test completed!");
            tracing::info!("  • Duration: {:.2} minutes", minutes);
            tracing::info!("  • Throughput: {:.0} msg/sec", throughput);
            tracing::info!("  • Target (700k/sec): {:.1}%", (throughput / 700_000.0) * 100.0);
            
            if minutes <= 5.0 {
                tracing::info!("🎉 TARGET ACHIEVED: Billion logs in under 5 minutes!");
            } else {
                tracing::warn!("Target missed: Need {:.1}x faster to reach 5 minutes", minutes / 5.0);
            }
        },
        
        PerfCommands::Suite { endpoint } => {
            tracing::info!("🧪 Running comprehensive test suite");
            let runner = PerformanceTestRunner::new();
            runner.run_comprehensive_test(endpoint).await?;
        },
    }

    Ok(())
}