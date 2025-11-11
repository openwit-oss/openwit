use clap::{Arg, Command};
use openwit_perf_test::{
    LoadGenerator, 
    HighPerformanceConfigs, 
    PerformanceTestRunner,
    load_generator::LoadConfig
};
use std::time::Instant;
use tokio;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let matches = Command::new("openwit-benchmark")
        .version("1.0")
        .author("OpenWit Team")
        .about("High-performance ingestion benchmarking tool")
        .arg(Arg::new("endpoint")
            .short('e')
            .long("endpoint")
            .value_name("HOST:PORT")
            .help("Ingestion endpoint")
            .default_value("localhost:4317"))
        .arg(Arg::new("test")
            .short('t')
            .long("test")
            .value_name("TEST_TYPE")
            .help("Test type: baseline, extreme, sustained, latency, memory, or billion")
            .default_value("baseline"))
        .arg(Arg::new("messages")
            .short('m')
            .long("messages")
            .value_name("COUNT")
            .help("Number of messages to send")
            .value_parser(clap::value_parser!(u64)))
        .arg(Arg::new("batch-size")
            .short('b')
            .long("batch-size")
            .value_name("SIZE")
            .help("Batch size for messages")
            .value_parser(clap::value_parser!(usize)))
        .get_matches();

    let endpoint = matches.get_one::<String>("endpoint").unwrap().clone();
    let test_type = matches.get_one::<String>("test").unwrap();

    println!("🚀 OpenWit Performance Benchmark Tool");
    println!("=====================================");
    println!("Target endpoint: {}", endpoint);
    println!("Test type: {}", test_type);
    println!();

    let config = match test_type.as_str() {
        "baseline" => {
            println!("📊 Running baseline performance test...");
            LoadConfig::default()
        },
        "extreme" => {
            println!("🔥 Running extreme throughput test (Target: 700k/sec)...");
            HighPerformanceConfigs::extreme_throughput()
        },
        "sustained" => {
            println!("⏱️  Running sustained load test...");
            HighPerformanceConfigs::sustained_load()
        },
        "latency" => {
            println!("🎯 Running latency-focused test...");
            HighPerformanceConfigs::latency_focused()
        },
        "memory" => {
            println!("🧠 Running memory stress test...");
            HighPerformanceConfigs::memory_stress()
        },
        "billion" => {
            println!("🌟 Running billion message test (Target: 5 minutes)...");
            let mut config = HighPerformanceConfigs::extreme_throughput();
            config.total_messages = 1_000_000_000;
            config
        },
        _ => {
            eprintln!("❌ Unknown test type: {}", test_type);
            std::process::exit(1);
        }
    };

    // Override with custom values if provided
    let mut final_config = config;
    if let Some(&messages) = matches.get_one::<u64>("messages") {
        final_config.total_messages = messages;
    }
    if let Some(&batch_size) = matches.get_one::<usize>("batch-size") {
        final_config.batch_size = batch_size;
    }

    println!("Configuration:");
    println!("  • Total messages: {}", final_config.total_messages);
    println!("  • Batch size: {}", final_config.batch_size);
    println!("  • Connections: {}", final_config.concurrent_connections);
    println!("  • Message size: {} bytes", final_config.message_size_bytes);
    println!("  • Rate limit: {:?}/sec", final_config.messages_per_second);
    println!();

    // Create metrics collector
    let metrics = std::sync::Arc::new(openwit_perf_test::MetricsCollector::new());
    
    // Run the test
    let start_time = Instant::now();
    
    println!("⚡ Starting performance test...");
    let generator = LoadGenerator::new(endpoint, final_config.clone(), metrics.clone());
    
    match generator.run().await {
        Ok(_) => {
            let duration = start_time.elapsed();
            let throughput = final_config.total_messages as f64 / duration.as_secs_f64();
            
            println!();
            println!("✅ Test completed successfully!");
            println!("Results:");
            println!("  • Duration: {:.2} seconds", duration.as_secs_f64());
            println!("  • Throughput: {:.0} messages/sec", throughput);
            println!("  • Target (700k/sec): {:.1}%", (throughput / 700_000.0) * 100.0);
            
            if final_config.total_messages == 1_000_000_000 {
                let billion_time_minutes = duration.as_secs_f64() / 60.0;
                println!("  • Billion logs time: {:.2} minutes", billion_time_minutes);
                if billion_time_minutes <= 5.0 {
                    println!("  🎉 TARGET ACHIEVED: Billion logs in under 5 minutes!");
                } else {
                    println!("  ⚠️  Target missed: Need {:.1}x faster to reach 5 minutes", billion_time_minutes / 5.0);
                }
            }
            
            // Calculate bandwidth
            let total_bytes = final_config.total_messages * final_config.message_size_bytes as u64;
            let bandwidth_mbps = (total_bytes as f64 * 8.0) / (duration.as_secs_f64() * 1_000_000.0);
            println!("  • Bandwidth: {:.2} Mbps", bandwidth_mbps);
            
            // System resource usage (if available)
            println!();
            println!("💡 Performance Tips:");
            if throughput < 100_000.0 {
                println!("  • Increase batch size (current: {})", final_config.batch_size);
                println!("  • Increase concurrent connections (current: {})", final_config.concurrent_connections);
                println!("  • Check system resources (CPU, memory, network)");
            } else if throughput < 500_000.0 {
                println!("  • Consider using high_throughput() IngestionConfig");
                println!("  • Optimize WAL settings (disable compression, larger files)");
                println!("  • Ensure sufficient memory allocation");
            } else {
                println!("  • Great performance! Monitor system stability under sustained load");
                println!("  • Consider multi-node deployment for higher scale");
            }
        },
        Err(e) => {
            eprintln!("❌ Test failed: {:?}", e);
            std::process::exit(1);
        }
    }

    Ok(())
}