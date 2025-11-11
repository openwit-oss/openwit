use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use hdrhistogram::Histogram;
use tokio::sync::RwLock;
use chrono::{DateTime, Utc};

#[derive(Debug, Clone)]
pub struct MetricsCollector {
    // Throughput metrics
    pub messages_sent: Arc<AtomicU64>,
    pub messages_ingested: Arc<AtomicU64>,
    pub bytes_sent: Arc<AtomicU64>,
    pub bytes_ingested: Arc<AtomicU64>,
    
    // Latency histogram (microseconds)
    pub latency_histogram: Arc<RwLock<Histogram<u64>>>,
    
    // Timing
    pub start_time: Arc<RwLock<Option<Instant>>>,
    pub end_time: Arc<RwLock<Option<Instant>>>,
    
    // Error tracking
    pub errors: Arc<AtomicU64>,
    pub timeouts: Arc<AtomicU64>,
    
    // System metrics snapshots
    pub cpu_samples: Arc<RwLock<Vec<f32>>>,
    pub memory_samples: Arc<RwLock<Vec<u64>>>,
    pub disk_io_samples: Arc<RwLock<Vec<u64>>>,
}

impl MetricsCollector {
    pub fn new() -> Self {
        Self {
            messages_sent: Arc::new(AtomicU64::new(0)),
            messages_ingested: Arc::new(AtomicU64::new(0)),
            bytes_sent: Arc::new(AtomicU64::new(0)),
            bytes_ingested: Arc::new(AtomicU64::new(0)),
            latency_histogram: Arc::new(RwLock::new(
                Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).unwrap() // 1µs to 60s
            )),
            start_time: Arc::new(RwLock::new(None)),
            end_time: Arc::new(RwLock::new(None)),
            errors: Arc::new(AtomicU64::new(0)),
            timeouts: Arc::new(AtomicU64::new(0)),
            cpu_samples: Arc::new(RwLock::new(Vec::new())),
            memory_samples: Arc::new(RwLock::new(Vec::new())),
            disk_io_samples: Arc::new(RwLock::new(Vec::new())),
        }
    }
    
    pub async fn start(&self) {
        *self.start_time.write().await = Some(Instant::now());
    }
    
    pub async fn stop(&self) {
        *self.end_time.write().await = Some(Instant::now());
    }
    
    pub fn record_message_sent(&self, size_bytes: u64) {
        self.messages_sent.fetch_add(1, Ordering::Relaxed);
        self.bytes_sent.fetch_add(size_bytes, Ordering::Relaxed);
    }
    
    pub fn record_message_ingested(&self, size_bytes: u64) {
        self.messages_ingested.fetch_add(1, Ordering::Relaxed);
        self.bytes_ingested.fetch_add(size_bytes, Ordering::Relaxed);
    }
    
    pub async fn record_latency(&self, latency_us: u64) {
        let mut hist = self.latency_histogram.write().await;
        let _ = hist.record(latency_us);
    }
    
    pub fn record_error(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }
    
    pub fn record_timeout(&self) {
        self.timeouts.fetch_add(1, Ordering::Relaxed);
    }
    
    pub async fn record_cpu_sample(&self, cpu_percent: f32) {
        self.cpu_samples.write().await.push(cpu_percent);
    }
    
    pub async fn record_memory_sample(&self, memory_bytes: u64) {
        self.memory_samples.write().await.push(memory_bytes);
    }
    
    pub async fn record_disk_io_sample(&self, io_bytes: u64) {
        self.disk_io_samples.write().await.push(io_bytes);
    }
    
    pub async fn get_duration(&self) -> Option<Duration> {
        let start = *self.start_time.read().await;
        let end = *self.end_time.read().await;
        
        match (start, end) {
            (Some(s), Some(e)) => Some(e.duration_since(s)),
            (Some(s), None) => Some(Instant::now().duration_since(s)),
            _ => None,
        }
    }
    
    pub async fn get_throughput_stats(&self) -> ThroughputStats {
        let duration = self.get_duration().await.unwrap_or(Duration::from_secs(1));
        let duration_secs = duration.as_secs_f64();
        
        let messages_sent = self.messages_sent.load(Ordering::Relaxed);
        let bytes_sent = self.bytes_sent.load(Ordering::Relaxed);
        let messages_ingested = self.messages_ingested.load(Ordering::Relaxed);
        let bytes_ingested = self.bytes_ingested.load(Ordering::Relaxed);
        
        ThroughputStats {
            messages_per_second_sent: messages_sent as f64 / duration_secs,
            messages_per_second_ingested: messages_ingested as f64 / duration_secs,
            bytes_per_second_sent: bytes_sent as f64 / duration_secs,
            bytes_per_second_ingested: bytes_ingested as f64 / duration_secs,
            total_messages_sent: messages_sent,
            total_messages_ingested: messages_ingested,
            total_bytes_sent: bytes_sent,
            total_bytes_ingested: bytes_ingested,
            duration,
        }
    }
    
    pub async fn get_latency_stats(&self) -> LatencyStats {
        let hist = self.latency_histogram.read().await;
        
        LatencyStats {
            min_us: hist.min(),
            max_us: hist.max(),
            mean_us: hist.mean(),
            p50_us: hist.value_at_percentile(50.0),
            p90_us: hist.value_at_percentile(90.0),
            p95_us: hist.value_at_percentile(95.0),
            p99_us: hist.value_at_percentile(99.0),
            p999_us: hist.value_at_percentile(99.9),
            count: hist.len(),
        }
    }
    
    pub async fn get_resource_stats(&self) -> ResourceStats {
        let cpu_samples = self.cpu_samples.read().await;
        let memory_samples = self.memory_samples.read().await;
        let disk_io_samples = self.disk_io_samples.read().await;
        
        ResourceStats {
            avg_cpu_percent: if cpu_samples.is_empty() { 0.0 } else {
                cpu_samples.iter().sum::<f32>() / cpu_samples.len() as f32
            },
            max_cpu_percent: cpu_samples.iter().cloned().fold(0.0, f32::max),
            avg_memory_bytes: if memory_samples.is_empty() { 0 } else {
                memory_samples.iter().sum::<u64>() / memory_samples.len() as u64
            },
            max_memory_bytes: memory_samples.iter().cloned().max().unwrap_or(0),
            total_disk_io_bytes: disk_io_samples.iter().sum(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ThroughputStats {
    pub messages_per_second_sent: f64,
    pub messages_per_second_ingested: f64,
    pub bytes_per_second_sent: f64,
    pub bytes_per_second_ingested: f64,
    pub total_messages_sent: u64,
    pub total_messages_ingested: u64,
    pub total_bytes_sent: u64,
    pub total_bytes_ingested: u64,
    pub duration: Duration,
}

#[derive(Debug, Clone)]
pub struct LatencyStats {
    pub min_us: u64,
    pub max_us: u64,
    pub mean_us: f64,
    pub p50_us: u64,
    pub p90_us: u64,
    pub p95_us: u64,
    pub p99_us: u64,
    pub p999_us: u64,
    pub count: u64,
}

#[derive(Debug, Clone)]
pub struct ResourceStats {
    pub avg_cpu_percent: f32,
    pub max_cpu_percent: f32,
    pub avg_memory_bytes: u64,
    pub max_memory_bytes: u64,
    pub total_disk_io_bytes: u64,
}