use lazy_static::lazy_static;
use prometheus::{
    register_counter_vec, register_gauge_vec, register_histogram_vec,
    CounterVec, GaugeVec, HistogramVec, TextEncoder, Encoder,
};
use std::time::Duration;

lazy_static! {
    // Counters
    pub static ref FILES_BUILT_TOTAL: CounterVec = register_counter_vec!(
        "indexer_files_built_total",
        "Total number of files indexed",
        &["tenant", "signal", "status"]
    ).unwrap();

    pub static ref COMBINES_TOTAL: CounterVec = register_counter_vec!(
        "indexer_combines_total",
        "Total number of combined indexes created",
        &["tenant", "signal", "level", "status"]
    ).unwrap();

    pub static ref EVENTS_CONSUMED_TOTAL: CounterVec = register_counter_vec!(
        "indexer_events_consumed_total",
        "Total number of events consumed",
        &["event_type", "source"]
    ).unwrap();

    pub static ref S3_PUT_FAILURES_TOTAL: CounterVec = register_counter_vec!(
        "indexer_s3_put_failures_total",
        "Total number of S3 PUT failures",
        &["operation", "error_type"]
    ).unwrap();

    pub static ref DB_TXN_FAILURES_TOTAL: CounterVec = register_counter_vec!(
        "indexer_db_txn_failures_total",
        "Total number of database transaction failures",
        &["operation", "error_type"]
    ).unwrap();

    pub static ref WAL_REPLAY_RESUMES_TOTAL: CounterVec = register_counter_vec!(
        "indexer_wal_replay_resumes_total",
        "Total number of WAL replay resumes",
        &["reason"]
    ).unwrap();

    // Histograms
    pub static ref INDEX_BUILD_SECONDS: HistogramVec = register_histogram_vec!(
        "indexer_index_build_seconds",
        "Time taken to build index artifacts",
        &["artifact_type"],
        vec![0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0]
    ).unwrap();

    pub static ref COMBINE_SECONDS: HistogramVec = register_histogram_vec!(
        "indexer_combine_seconds",
        "Time taken to create combined indexes",
        &["level"],
        vec![1.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0]
    ).unwrap();

    pub static ref ARTIFACT_UPLOAD_BYTES: HistogramVec = register_histogram_vec!(
        "indexer_artifact_upload_bytes",
        "Size of uploaded artifacts in bytes",
        &["artifact_type"],
        vec![1024.0, 10240.0, 102400.0, 1048576.0, 10485760.0, 104857600.0] // 1KB to 100MB
    ).unwrap();

    // Gauges
    pub static ref BUILD_QUEUE_DEPTH: GaugeVec = register_gauge_vec!(
        "indexer_build_queue_depth",
        "Current depth of the build queue",
        &["priority"]
    ).unwrap();

    pub static ref COMBINE_QUEUE_DEPTH: GaugeVec = register_gauge_vec!(
        "indexer_combine_queue_depth", 
        "Current depth of the combine queue",
        &["level"]
    ).unwrap();

    pub static ref ACCEPTING: GaugeVec = register_gauge_vec!(
        "indexer_accepting",
        "Whether the indexer is accepting new work (1 = yes, 0 = no)",
        &["reason"]
    ).unwrap();

    pub static ref MEMORY_USAGE_BYTES: GaugeVec = register_gauge_vec!(
        "indexer_memory_usage_bytes",
        "Current memory usage in bytes",
        &["category"]
    ).unwrap();

    pub static ref CPU_USAGE_PERCENT: GaugeVec = register_gauge_vec!(
        "indexer_cpu_usage_percent",
        "Current CPU usage percentage",
        &["core"]
    ).unwrap();
}

/// Record successful file indexing
pub fn record_file_indexed(tenant: &str, signal: &str, duration: Duration) {
    FILES_BUILT_TOTAL
        .with_label_values(&[tenant, signal, "success"])
        .inc();
    
    INDEX_BUILD_SECONDS
        .with_label_values(&["total"])
        .observe(duration.as_secs_f64());
}

/// Record failed file indexing
pub fn record_file_index_failed(tenant: &str, signal: &str) {
    FILES_BUILT_TOTAL
        .with_label_values(&[tenant, signal, "failed"])
        .inc();
}

/// Record artifact build time
pub fn record_artifact_build_time(artifact_type: &str, duration: Duration) {
    INDEX_BUILD_SECONDS
        .with_label_values(&[artifact_type])
        .observe(duration.as_secs_f64());
}

/// Record artifact upload
pub fn record_artifact_upload(artifact_type: &str, size_bytes: u64) {
    ARTIFACT_UPLOAD_BYTES
        .with_label_values(&[artifact_type])
        .observe(size_bytes as f64);
}

/// Record successful combine operation
pub fn record_combine_success(tenant: &str, signal: &str, level: &str, duration: Duration) {
    COMBINES_TOTAL
        .with_label_values(&[tenant, signal, level, "success"])
        .inc();
    
    COMBINE_SECONDS
        .with_label_values(&[level])
        .observe(duration.as_secs_f64());
}

/// Record failed combine operation
pub fn record_combine_failed(tenant: &str, signal: &str, level: &str) {
    COMBINES_TOTAL
        .with_label_values(&[tenant, signal, level, "failed"])
        .inc();
}

/// Record event consumption
pub fn record_event_consumed(event_type: &str, source: &str) {
    EVENTS_CONSUMED_TOTAL
        .with_label_values(&[event_type, source])
        .inc();
}

/// Record S3 failure
pub fn record_s3_failure(operation: &str, error_type: &str) {
    S3_PUT_FAILURES_TOTAL
        .with_label_values(&[operation, error_type])
        .inc();
}

/// Record database transaction failure
pub fn record_db_failure(operation: &str, error_type: &str) {
    DB_TXN_FAILURES_TOTAL
        .with_label_values(&[operation, error_type])
        .inc();
}

/// Record WAL replay resume
pub fn record_wal_replay_resume(reason: &str) {
    WAL_REPLAY_RESUMES_TOTAL
        .with_label_values(&[reason])
        .inc();
}

/// Update queue depths
pub fn update_queue_depths(build_queue: usize, combine_queue_hour: usize, combine_queue_day: usize) {
    BUILD_QUEUE_DEPTH
        .with_label_values(&["normal"])
        .set(build_queue as f64);
    
    COMBINE_QUEUE_DEPTH
        .with_label_values(&["hour"])
        .set(combine_queue_hour as f64);
    
    COMBINE_QUEUE_DEPTH
        .with_label_values(&["day"])
        .set(combine_queue_day as f64);
}

/// Update accepting status
pub fn update_accepting(accepting: bool, reason: &str) {
    ACCEPTING
        .with_label_values(&[reason])
        .set(if accepting { 1.0 } else { 0.0 });
}

/// Update memory usage
pub fn update_memory_usage(category: &str, bytes: usize) {
    MEMORY_USAGE_BYTES
        .with_label_values(&[category])
        .set(bytes as f64);
}

/// Get metrics for export
pub fn gather_metrics() -> Result<String, prometheus::Error> {
    let encoder = TextEncoder::new();
    let metric_families = prometheus::gather();
    let mut buffer = Vec::new();
    encoder.encode(&metric_families, &mut buffer)?;
    Ok(String::from_utf8(buffer).unwrap())
}

/// Health status for monitoring
#[derive(Debug, Clone, serde::Serialize)]
pub struct HealthStatus {
    pub status: String,
    pub accepting: bool,
    pub build_queue_depth: usize,
    pub combine_queue_depth: usize,
    pub memory_usage_mb: f64,
    pub cpu_usage_percent: f64,
    pub errors_last_minute: usize,
    pub version: String,
    pub uptime_seconds: u64,
}

impl HealthStatus {
    pub fn healthy(
        accepting: bool,
        build_queue_depth: usize,
        combine_queue_depth: usize,
        uptime: Duration,
    ) -> Self {
        Self {
            status: "healthy".to_string(),
            accepting,
            build_queue_depth,
            combine_queue_depth,
            memory_usage_mb: get_memory_usage_mb(),
            cpu_usage_percent: get_cpu_usage_percent(),
            errors_last_minute: 0, // TODO: implement error tracking
            version: env!("CARGO_PKG_VERSION").to_string(),
            uptime_seconds: uptime.as_secs(),
        }
    }

    pub fn unhealthy(reason: String) -> Self {
        Self {
            status: format!("unhealthy: {}", reason),
            accepting: false,
            build_queue_depth: 0,
            combine_queue_depth: 0,
            memory_usage_mb: get_memory_usage_mb(),
            cpu_usage_percent: get_cpu_usage_percent(),
            errors_last_minute: 0,
            version: env!("CARGO_PKG_VERSION").to_string(),
            uptime_seconds: 0,
        }
    }
}

// Helper functions for system metrics
pub fn get_memory_usage_mb() -> f64 {
    // Simple implementation - in production use proper system metrics
    #[cfg(target_os = "linux")]
    {
        if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
            for line in status.lines() {
                if line.starts_with("VmRSS:") {
                    if let Some(kb_str) = line.split_whitespace().nth(1) {
                        if let Ok(kb) = kb_str.parse::<f64>() {
                            return kb / 1024.0;
                        }
                    }
                }
            }
        }
    }
    0.0
}

fn get_cpu_usage_percent() -> f64 {
    // Placeholder - implement proper CPU tracking
    0.0
}