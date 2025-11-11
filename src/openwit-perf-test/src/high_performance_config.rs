use crate::load_generator::LoadConfig;
use crate::metrics_collector::MetricsCollector;
use std::sync::Arc;

/// High-performance configurations for extreme throughput testing
pub struct HighPerformanceConfigs;

impl HighPerformanceConfigs {
    /// Configuration optimized for 700k+ messages/second
    pub fn extreme_throughput() -> LoadConfig {
        LoadConfig {
            total_messages: 1_000_000_000,
            batch_size: 10_000,              // Large batches reduce overhead
            concurrent_connections: 200,      // High concurrency
            messages_per_second: None,       // No rate limiting
            message_size_bytes: 256,         // Smaller messages for volume
            span_count_per_message: 1,       // Minimal complexity
            attributes_per_span: 1,          // Minimal attributes
        }
    }

    /// Configuration for sustained high load testing
    pub fn sustained_load() -> LoadConfig {
        LoadConfig {
            total_messages: 100_000_000,     // 100M messages
            batch_size: 5_000,               // Medium batches
            concurrent_connections: 100,     // Moderate concurrency
            messages_per_second: Some(500_000), // 500k/sec target
            message_size_bytes: 512,         // Realistic message size
            span_count_per_message: 2,       // Some complexity
            attributes_per_span: 3,          // Realistic attributes
        }
    }

    /// Configuration for latency testing under load
    pub fn latency_focused() -> LoadConfig {
        LoadConfig {
            total_messages: 10_000_000,      // 10M messages
            batch_size: 1_000,               // Smaller batches for latency
            concurrent_connections: 50,      // Lower concurrency
            messages_per_second: Some(100_000), // 100k/sec
            message_size_bytes: 1024,        // Larger messages
            span_count_per_message: 5,       // Complex traces
            attributes_per_span: 10,         // Rich attributes
        }
    }

    /// Configuration for memory pressure testing
    pub fn memory_stress() -> LoadConfig {
        LoadConfig {
            total_messages: 50_000_000,      // 50M messages
            batch_size: 20_000,              // Very large batches
            concurrent_connections: 300,     // High connection count
            messages_per_second: None,       // Unlimited
            message_size_bytes: 2048,        // Large messages
            span_count_per_message: 1,       // Simple structure
            attributes_per_span: 1,          // Minimal attributes
        }
    }

    /// Configuration for network bandwidth testing
    pub fn bandwidth_test() -> LoadConfig {
        LoadConfig {
            total_messages: 20_000_000,      // 20M messages
            batch_size: 50_000,              // Huge batches
            concurrent_connections: 100,     // Moderate connections
            messages_per_second: None,       // Unlimited
            message_size_bytes: 4096,        // Large messages
            span_count_per_message: 3,       // Moderate complexity
            attributes_per_span: 15,         // Rich data
        }
    }
}

/// Performance test runner with advanced monitoring
pub struct PerformanceTestRunner {
    pub metrics: Arc<MetricsCollector>,
}

impl PerformanceTestRunner {
    pub fn new() -> Self {
        Self {
            metrics: Arc::new(MetricsCollector::new()),
        }
    }

    /// Run comprehensive performance test suite
    pub async fn run_comprehensive_test(&self, endpoint: String) -> anyhow::Result<()> {
        println!("🚀 Starting Comprehensive Performance Test Suite");
        println!("=================================================");

        // Test 1: Baseline throughput
        println!("\n📊 Test 1: Baseline Throughput Test");
        self.run_single_test("baseline", endpoint.clone(), LoadConfig::default()).await?;

        // Test 2: Extreme throughput
        println!("\n🔥 Test 2: Extreme Throughput Test (Target: 700k/sec)");
        self.run_single_test("extreme", endpoint.clone(), HighPerformanceConfigs::extreme_throughput()).await?;

        // Test 3: Sustained load
        println!("\n⏱️  Test 3: Sustained Load Test");
        self.run_single_test("sustained", endpoint.clone(), HighPerformanceConfigs::sustained_load()).await?;

        // Test 4: Latency under load
        println!("\n🎯 Test 4: Latency Test");
        self.run_single_test("latency", endpoint.clone(), HighPerformanceConfigs::latency_focused()).await?;

        // Test 5: Memory stress
        println!("\n🧠 Test 5: Memory Stress Test");
        self.run_single_test("memory", endpoint.clone(), HighPerformanceConfigs::memory_stress()).await?;

        println!("\n✅ All performance tests completed!");
        Ok(())
    }

    async fn run_single_test(
        &self, 
        test_name: &str, 
        endpoint: String, 
        config: LoadConfig
    ) -> anyhow::Result<()> {
        use crate::LoadGenerator;
        use std::time::Instant;

        println!("  Configuration:");
        println!("    • Total messages: {}", config.total_messages);
        println!("    • Batch size: {}", config.batch_size);
        println!("    • Connections: {}", config.concurrent_connections);
        println!("    • Message size: {} bytes", config.message_size_bytes);

        let start = Instant::now();
        let generator = LoadGenerator::new(endpoint, config.clone(), self.metrics.clone());
        
        generator.run().await?;
        
        let duration = start.elapsed();
        let throughput = config.total_messages as f64 / duration.as_secs_f64();
        
        println!("  Results:");
        println!("    • Duration: {:.2}s", duration.as_secs_f64());
        println!("    • Throughput: {:.0} msg/sec", throughput);
        println!("    • Target (700k/sec): {:.1}%", (throughput / 700_000.0) * 100.0);

        // Save detailed results
        let report_file = format!("performance_test_{}.json", test_name);
        self.save_test_results(&report_file, test_name, &config, duration, throughput).await?;

        Ok(())
    }

    async fn save_test_results(
        &self,
        filename: &str,
        test_name: &str,
        config: &LoadConfig,
        duration: std::time::Duration,
        throughput: f64,
    ) -> anyhow::Result<()> {
        use serde_json::json;
        use tokio::fs;

        let results = json!({
            "test_name": test_name,
            "config": {
                "total_messages": config.total_messages,
                "batch_size": config.batch_size,
                "concurrent_connections": config.concurrent_connections,
                "message_size_bytes": config.message_size_bytes,
                "span_count_per_message": config.span_count_per_message,
                "attributes_per_span": config.attributes_per_span
            },
            "results": {
                "duration_seconds": duration.as_secs_f64(),
                "throughput_msg_per_sec": throughput,
                "target_percentage": (throughput / 700_000.0) * 100.0,
                "billion_logs_eta_minutes": (1_000_000_000.0 / throughput) / 60.0
            },
            "timestamp": chrono::Utc::now().to_rfc3339()
        });

        fs::write(filename, serde_json::to_string_pretty(&results)?).await?;
        println!("    • Results saved to: {}", filename);

        Ok(())
    }
}

/// System resource monitoring during tests
pub struct ResourceMonitor;

impl ResourceMonitor {
    /// Monitor system resources during test execution
    pub async fn monitor_during_test<F, Fut>(test_func: F) -> anyhow::Result<()>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = anyhow::Result<()>>,
    {
        use tokio::time::{interval, Duration};
        use sysinfo::System;

        let mut sys = System::new_all();
        let mut monitor_interval = interval(Duration::from_secs(1));

        // Start monitoring task
        let monitor_handle = tokio::spawn(async move {
            loop {
                monitor_interval.tick().await;
                sys.refresh_all();

                let cpu_usage = sys.global_cpu_info().cpu_usage();
                let memory_used = sys.used_memory();
                let memory_total = sys.total_memory();
                let memory_percent = (memory_used as f64 / memory_total as f64) * 100.0;

                println!(
                    "📊 System: CPU {:.1}% | Memory {:.1}% ({} MB / {} MB)",
                    cpu_usage,
                    memory_percent,
                    memory_used / 1024 / 1024,
                    memory_total / 1024 / 1024
                );
            }
        });

        // Run the test
        let test_result = test_func().await;

        // Stop monitoring
        monitor_handle.abort();

        test_result
    }
}