use std::sync::Arc;
use std::time::Duration;
use anyhow::Result;
use tokio::time::interval;
use tracing::{info, debug};
use sysinfo::System;

use crate::metrics_collector::MetricsCollector;

pub struct PerformanceMonitor {
    metrics: Arc<MetricsCollector>,
    ingestion_metrics_url: Option<String>,
    sample_interval: Duration,
}

impl PerformanceMonitor {
    pub fn new(
        metrics: Arc<MetricsCollector>,
        ingestion_metrics_url: Option<String>,
    ) -> Self {
        Self {
            metrics,
            ingestion_metrics_url,
            sample_interval: Duration::from_secs(1),
        }
    }
    
    pub async fn start_monitoring(&self) -> Result<()> {
        let metrics = self.metrics.clone();
        let ingestion_url = self.ingestion_metrics_url.clone();
        let interval_duration = self.sample_interval;
        
        tokio::spawn(async move {
            let mut sys = System::new_all();
            let mut ticker = interval(interval_duration);
            
            loop {
                ticker.tick().await;
                
                // Update system info
                sys.refresh_all();
                
                // CPU usage
                let cpu_usage = sys.global_cpu_info().cpu_usage();
                metrics.record_cpu_sample(cpu_usage).await;
                
                // Memory usage
                let used_memory = sys.used_memory();
                metrics.record_memory_sample(used_memory).await;
                
                // Disk I/O - skip for now as sysinfo API changed
                // TODO: Update to new sysinfo disk API
                let total_io: u64 = 0;
                metrics.record_disk_io_sample(total_io).await;
                
                debug!("System metrics - CPU: {:.2}%, Memory: {} MB, Disk I/O: {} MB", 
                    cpu_usage, used_memory / 1024 / 1024, total_io / 1024 / 1024);
                
                // Fetch ingestion metrics if available
                if let Some(ref url) = ingestion_url {
                    if let Ok(ingestion_metrics) = Self::fetch_ingestion_metrics(url).await {
                        metrics.record_message_ingested(ingestion_metrics.messages_processed);
                    }
                }
            }
        });
        
        Ok(())
    }
    
    async fn fetch_ingestion_metrics(url: &str) -> Result<IngestionMetrics> {
        // Parse Prometheus metrics from ingestion node
        let response = reqwest::get(url).await?;
        let body = response.text().await?;
        
        let mut messages_processed = 0u64;
        let mut bytes_processed = 0u64;
        
        for line in body.lines() {
            if line.starts_with("ingestion_messages_received") && !line.starts_with("#") {
                if let Some(value) = line.split_whitespace().last() {
                    messages_processed = value.parse().unwrap_or(0);
                }
            }
            if line.starts_with("ingestion_bytes_received") && !line.starts_with("#") {
                if let Some(value) = line.split_whitespace().last() {
                    bytes_processed = value.parse().unwrap_or(0);
                }
            }
        }
        
        Ok(IngestionMetrics {
            messages_processed,
            bytes_processed,
        })
    }
    
    pub async fn monitor_progress(&self, total_target: u64) {
        let mut ticker = interval(Duration::from_secs(5));
        let start_time = std::time::Instant::now();
        
        loop {
            ticker.tick().await;
            
            let sent = self.metrics.messages_sent.load(std::sync::atomic::Ordering::Relaxed);
            let ingested = self.metrics.messages_ingested.load(std::sync::atomic::Ordering::Relaxed);
            let errors = self.metrics.errors.load(std::sync::atomic::Ordering::Relaxed);
            
            let elapsed = start_time.elapsed();
            let rate = sent as f64 / elapsed.as_secs_f64();
            let ingestion_rate = ingested as f64 / elapsed.as_secs_f64();
            
            let progress_percent = (sent as f64 / total_target as f64) * 100.0;
            let eta_seconds = if rate > 0.0 {
                ((total_target - sent) as f64 / rate) as u64
            } else {
                0
            };
            
            info!("Progress: {:.2}% ({}/{}) | Rate: {:.0} msg/s | Ingested: {} ({:.0} msg/s) | Errors: {} | ETA: {}",
                progress_percent,
                format_number(sent),
                format_number(total_target),
                rate,
                format_number(ingested),
                ingestion_rate,
                errors,
                format_duration(Duration::from_secs(eta_seconds))
            );
            
            if sent >= total_target {
                info!("All messages sent! Waiting for ingestion to complete...");
                
                // Wait for ingestion to catch up
                while ingested < sent {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    let ingested = self.metrics.messages_ingested.load(std::sync::atomic::Ordering::Relaxed);
                    info!("Ingestion progress: {}/{}", format_number(ingested), format_number(sent));
                }
                
                break;
            }
        }
    }
}

#[derive(Debug)]
struct IngestionMetrics {
    messages_processed: u64,
    bytes_processed: u64,
}

fn format_number(n: u64) -> String {
    use num_format::{Locale, ToFormattedString};
    n.to_formatted_string(&Locale::en)
}

fn format_duration(d: Duration) -> String {
    let total_seconds = d.as_secs();
    let hours = total_seconds / 3600;
    let minutes = (total_seconds % 3600) / 60;
    let seconds = total_seconds % 60;
    
    if hours > 0 {
        format!("{}h {}m {}s", hours, minutes, seconds)
    } else if minutes > 0 {
        format!("{}m {}s", minutes, seconds)
    } else {
        format!("{}s", seconds)
    }
}