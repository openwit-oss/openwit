use std::sync::Arc;
use anyhow::Result;
use clap::{Parser, Subcommand};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use indicatif::{ProgressBar, ProgressStyle};

use openwit_perf_test::{
    MetricsCollector,
    LoadGenerator,
    load_generator::LoadConfig,
    PerformanceMonitor,
    PerformanceReport,
    report_generator::TestConfig,
};

#[derive(Parser)]
#[command(name = "openwit-perf-test")]
#[command(about = "Performance testing tool for OpenWit ingestion")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Test ingestion of 1 billion logs
    Billion {
        /// Target endpoint (host:port)
        #[arg(short, long, default_value = "localhost:4317")]
        endpoint: String,
        
        /// Number of concurrent connections
        #[arg(short, long, default_value = "50")]
        connections: usize,
        
        /// Batch size per request
        #[arg(short, long, default_value = "1000")]
        batch_size: usize,
        
        /// Ingestion metrics URL for monitoring
        #[arg(short, long)]
        metrics_url: Option<String>,
    },
    
    /// Custom load test
    Custom {
        /// Target endpoint
        #[arg(short, long, default_value = "localhost:4317")]
        endpoint: String,
        
        /// Total number of messages
        #[arg(short, long, default_value = "1000000")]
        total: u64,
        
        /// Message size in bytes
        #[arg(short, long, default_value = "1024")]
        size: usize,
        
        /// Concurrent connections
        #[arg(short, long, default_value = "10")]
        connections: usize,
        
        /// Messages per second (rate limit)
        #[arg(short, long)]
        rate: Option<u64>,
    },
    
    /// Test with Kafka
    Kafka {
        /// Kafka brokers
        #[arg(short, long, default_value = "localhost:9092")]
        brokers: String,
        
        /// Topic name
        #[arg(short, long, default_value = "traces")]
        topic: String,
        
        /// Number of messages
        #[arg(short, long, default_value = "1000000")]
        messages: u64,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .init();
    
    let cli = Cli::parse();
    
    match cli.command {
        Commands::Billion { endpoint, connections, batch_size, metrics_url } => {
            run_billion_test(endpoint, connections, batch_size, metrics_url).await?;
        }
        Commands::Custom { endpoint, total, size, connections, rate } => {
            run_custom_test(endpoint, total, size, connections, rate).await?;
        }
        Commands::Kafka { brokers, topic, messages } => {
            run_kafka_test(brokers, topic, messages).await?;
        }
    }
    
    Ok(())
}

async fn run_billion_test(
    endpoint: String,
    connections: usize,
    batch_size: usize,
    metrics_url: Option<String>,
) -> Result<()> {
    println!("\n🚀 OpenWit 1 Billion Log Ingestion Test");
    println!("========================================");
    println!("Target: {}", endpoint);
    println!("Connections: {}", connections);
    println!("Batch Size: {}", batch_size);
    println!();
    
    let mut config = LoadConfig::for_billion_logs();
    config.concurrent_connections = connections;
    config.batch_size = batch_size;
    
    let metrics = Arc::new(MetricsCollector::new());
    
    // Start performance monitor
    let monitor = PerformanceMonitor::new(metrics.clone(), metrics_url);
    monitor.start_monitoring().await?;
    
    // Create load generator
    let generator = LoadGenerator::new(endpoint, config.clone(), metrics.clone());
    
    // Start progress monitoring
    let metrics_clone = metrics.clone();
    let monitor_handle = tokio::spawn(async move {
        let monitor = PerformanceMonitor::new(metrics_clone, None);
        monitor.monitor_progress(1_000_000_000).await;
    });
    
    // Run the test
    let start = std::time::Instant::now();
    generator.run().await?;
    let duration = start.elapsed();
    
    // Wait for monitoring to complete
    monitor_handle.abort();
    
    // Generate report
    let test_config = TestConfig {
        total_messages: config.total_messages,
        message_size_bytes: config.message_size_bytes,
        concurrent_connections: config.concurrent_connections,
    };
    
    let report = PerformanceReport::generate(metrics, test_config).await?;
    report.print_summary();
    report.save_json("billion_test_report.json")?;
    
    println!("\n✅ Test completed in {:?}", duration);
    println!("📄 Report saved to billion_test_report.json");
    
    Ok(())
}

async fn run_custom_test(
    endpoint: String,
    total: u64,
    size: usize,
    connections: usize,
    rate: Option<u64>,
) -> Result<()> {
    println!("\n🔧 Custom Load Test");
    println!("===================");
    
    let config = LoadConfig {
        total_messages: total,
        message_size_bytes: size,
        concurrent_connections: connections,
        messages_per_second: rate,
        ..Default::default()
    };
    
    let metrics = Arc::new(MetricsCollector::new());
    let generator = LoadGenerator::new(endpoint, config.clone(), metrics.clone());
    
    // Progress bar
    let pb = ProgressBar::new(total);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{bar:40.cyan/blue}] {pos}/{len} ({eta})")
            .unwrap()
            .progress_chars("#>-")
    );
    
    // Update progress bar
    let metrics_clone = metrics.clone();
    let pb_clone = pb.clone();
    tokio::spawn(async move {
        loop {
            let sent = metrics_clone.messages_sent.load(std::sync::atomic::Ordering::Relaxed);
            pb_clone.set_position(sent);
            if sent >= total {
                break;
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }
    });
    
    generator.run().await?;
    pb.finish_with_message("Test completed!");
    
    // Generate report
    let test_config = TestConfig {
        total_messages: config.total_messages,
        message_size_bytes: config.message_size_bytes,
        concurrent_connections: config.concurrent_connections,
    };
    
    let report = PerformanceReport::generate(metrics, test_config).await?;
    report.print_summary();
    
    Ok(())
}

async fn run_kafka_test(
    brokers: String,
    topic: String,
    messages: u64,
) -> Result<()> {
    println!("\n📨 Kafka Direct Ingestion Test");
    println!("==============================");
    println!("Brokers: {}", brokers);
    println!("Topic: {}", topic);
    println!("Messages: {}", messages);
    
    let config = LoadConfig {
        total_messages: messages,
        ..Default::default()
    };
    
    let metrics = Arc::new(MetricsCollector::new());
    let generator = LoadGenerator::new(String::new(), config, metrics.clone());
    
    generator.generate_to_kafka(&brokers, &topic).await?;
    
    let throughput = metrics.get_throughput_stats().await;
    println!("\n📊 Results:");
    println!("  Total sent: {} messages", throughput.total_messages_sent);
    println!("  Duration: {:?}", throughput.duration);
    println!("  Rate: {:.0} messages/second", throughput.messages_per_second_sent);
    
    Ok(())
}