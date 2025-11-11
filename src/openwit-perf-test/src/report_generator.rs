use std::sync::Arc;
use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Serialize, Deserialize};
use num_format::{Locale, ToFormattedString};

use crate::metrics_collector::{MetricsCollector, ThroughputStats, LatencyStats, ResourceStats};

#[derive(Debug, Serialize, Deserialize)]
pub struct PerformanceReport {
    pub test_info: TestInfo,
    pub throughput: ThroughputReport,
    pub latency: LatencyReport,
    pub resources: ResourceReport,
    pub summary: SummaryReport,
    pub recommendations: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TestInfo {
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
    pub duration_seconds: f64,
    pub total_messages: u64,
    pub message_size_bytes: usize,
    pub concurrent_connections: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ThroughputReport {
    pub messages_per_second: f64,
    pub megabytes_per_second: f64,
    pub total_messages: u64,
    pub total_gigabytes: f64,
    pub ingestion_rate: f64,
    pub ingestion_lag: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LatencyReport {
    pub min_ms: f64,
    pub max_ms: f64,
    pub mean_ms: f64,
    pub p50_ms: f64,
    pub p90_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
    pub p999_ms: f64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ResourceReport {
    pub avg_cpu_percent: f32,
    pub max_cpu_percent: f32,
    pub avg_memory_gb: f64,
    pub max_memory_gb: f64,
    pub total_disk_io_gb: f64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SummaryReport {
    pub time_to_billion: String,
    pub bottlenecks: Vec<String>,
    pub error_rate: f64,
    pub success_rate: f64,
}

impl PerformanceReport {
    pub async fn generate(
        metrics: Arc<MetricsCollector>,
        test_config: TestConfig,
    ) -> Result<Self> {
        let throughput_stats = metrics.get_throughput_stats().await;
        let latency_stats = metrics.get_latency_stats().await;
        let resource_stats = metrics.get_resource_stats().await;
        
        let test_info = TestInfo {
            start_time: Utc::now() - chrono::Duration::seconds(throughput_stats.duration.as_secs() as i64),
            end_time: Utc::now(),
            duration_seconds: throughput_stats.duration.as_secs_f64(),
            total_messages: test_config.total_messages,
            message_size_bytes: test_config.message_size_bytes,
            concurrent_connections: test_config.concurrent_connections,
        };
        
        let throughput = ThroughputReport {
            messages_per_second: throughput_stats.messages_per_second_sent,
            megabytes_per_second: throughput_stats.bytes_per_second_sent / 1_000_000.0,
            total_messages: throughput_stats.total_messages_sent,
            total_gigabytes: throughput_stats.total_bytes_sent as f64 / 1_000_000_000.0,
            ingestion_rate: throughput_stats.messages_per_second_ingested,
            ingestion_lag: throughput_stats.total_messages_sent - throughput_stats.total_messages_ingested,
        };
        
        let latency = LatencyReport {
            min_ms: latency_stats.min_us as f64 / 1000.0,
            max_ms: latency_stats.max_us as f64 / 1000.0,
            mean_ms: latency_stats.mean_us / 1000.0,
            p50_ms: latency_stats.p50_us as f64 / 1000.0,
            p90_ms: latency_stats.p90_us as f64 / 1000.0,
            p95_ms: latency_stats.p95_us as f64 / 1000.0,
            p99_ms: latency_stats.p99_us as f64 / 1000.0,
            p999_ms: latency_stats.p999_us as f64 / 1000.0,
        };
        
        let resources = ResourceReport {
            avg_cpu_percent: resource_stats.avg_cpu_percent,
            max_cpu_percent: resource_stats.max_cpu_percent,
            avg_memory_gb: resource_stats.avg_memory_bytes as f64 / 1_000_000_000.0,
            max_memory_gb: resource_stats.max_memory_bytes as f64 / 1_000_000_000.0,
            total_disk_io_gb: resource_stats.total_disk_io_bytes as f64 / 1_000_000_000.0,
        };
        
        let errors = metrics.errors.load(std::sync::atomic::Ordering::Relaxed);
        let error_rate = errors as f64 / throughput_stats.total_messages_sent as f64;
        
        // Calculate time to 1 billion
        let time_to_billion_seconds = if throughput_stats.messages_per_second_sent > 0.0 {
            1_000_000_000.0 / throughput_stats.messages_per_second_sent
        } else {
            0.0
        };
        
        let mut bottlenecks = Vec::new();
        let mut recommendations = Vec::new();
        
        // Analyze bottlenecks
        if throughput.ingestion_lag > throughput.total_messages * 10 / 100 {
            bottlenecks.push("Ingestion is lagging behind generation".to_string());
            recommendations.push("Increase ingestion buffer size or add more ingestion nodes".to_string());
        }
        
        if resource_stats.avg_cpu_percent > 80.0 {
            bottlenecks.push("High CPU utilization".to_string());
            recommendations.push("Add more CPU cores or optimize processing".to_string());
        }
        
        if latency.p99_ms > 100.0 {
            bottlenecks.push("High tail latency".to_string());
            recommendations.push("Investigate GC pauses or network issues".to_string());
        }
        
        if throughput.messages_per_second < 100_000.0 {
            bottlenecks.push("Low throughput".to_string());
            recommendations.push("Increase batch sizes and concurrent connections".to_string());
        }
        
        let summary = SummaryReport {
            time_to_billion: format_duration(time_to_billion_seconds),
            bottlenecks,
            error_rate: error_rate * 100.0,
            success_rate: (1.0 - error_rate) * 100.0,
        };
        
        Ok(PerformanceReport {
            test_info,
            throughput,
            latency,
            resources,
            summary,
            recommendations,
        })
    }
    
    pub fn print_summary(&self) {
        println!("\n{}", "=".repeat(80));
        println!("                    PERFORMANCE TEST REPORT");
        println!("{}", "=".repeat(80));
        
        println!("\n📊 TEST CONFIGURATION");
        println!("  Duration: {:.2} seconds", self.test_info.duration_seconds);
        println!("  Total Messages: {}", self.test_info.total_messages.to_formatted_string(&Locale::en));
        println!("  Message Size: {} bytes", self.test_info.message_size_bytes);
        println!("  Concurrent Connections: {}", self.test_info.concurrent_connections);
        
        println!("\n🚀 THROUGHPUT");
        println!("  Messages/Second: {}", (self.throughput.messages_per_second as u64).to_formatted_string(&Locale::en));
        println!("  MB/Second: {:.2}", self.throughput.megabytes_per_second);
        println!("  Ingestion Rate: {} msg/s", (self.throughput.ingestion_rate as u64).to_formatted_string(&Locale::en));
        println!("  Ingestion Lag: {}", self.throughput.ingestion_lag.to_formatted_string(&Locale::en));
        
        println!("\n⏱️  LATENCY (ms)");
        println!("  Min: {:.2} | Mean: {:.2} | Max: {:.2}", 
            self.latency.min_ms, self.latency.mean_ms, self.latency.max_ms);
        println!("  P50: {:.2} | P90: {:.2} | P95: {:.2} | P99: {:.2} | P99.9: {:.2}",
            self.latency.p50_ms, self.latency.p90_ms, self.latency.p95_ms, 
            self.latency.p99_ms, self.latency.p999_ms);
        
        println!("\n💻 RESOURCES");
        println!("  CPU: {:.1}% avg, {:.1}% max", 
            self.resources.avg_cpu_percent, self.resources.max_cpu_percent);
        println!("  Memory: {:.2} GB avg, {:.2} GB max", 
            self.resources.avg_memory_gb, self.resources.max_memory_gb);
        println!("  Disk I/O: {:.2} GB total", self.resources.total_disk_io_gb);
        
        println!("\n📈 SUMMARY");
        println!("  Time to 1 Billion Messages: {}", self.summary.time_to_billion);
        println!("  Success Rate: {:.2}%", self.summary.success_rate);
        
        if !self.summary.bottlenecks.is_empty() {
            println!("\n⚠️  BOTTLENECKS");
            for bottleneck in &self.summary.bottlenecks {
                println!("  • {}", bottleneck);
            }
        }
        
        if !self.recommendations.is_empty() {
            println!("\n💡 RECOMMENDATIONS");
            for rec in &self.recommendations {
                println!("  • {}", rec);
            }
        }
        
        println!("\n{}", "=".repeat(80));
    }
    
    pub fn save_json(&self, path: &str) -> Result<()> {
        let json = serde_json::to_string_pretty(self)?;
        std::fs::write(path, json)?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct TestConfig {
    pub total_messages: u64,
    pub message_size_bytes: usize,
    pub concurrent_connections: usize,
}

fn format_duration(seconds: f64) -> String {
    let hours = (seconds / 3600.0) as u64;
    let minutes = ((seconds % 3600.0) / 60.0) as u64;
    let secs = (seconds % 60.0) as u64;
    
    if hours > 0 {
        format!("{} hours {} minutes", hours, minutes)
    } else if minutes > 0 {
        format!("{} minutes {} seconds", minutes, secs)
    } else {
        format!("{:.2} seconds", seconds)
    }
}