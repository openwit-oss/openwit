use prometheus::{
    register_int_counter_vec, register_histogram_vec,
    IntCounterVec, HistogramVec,
};
use lazy_static::lazy_static;
use std::time::Instant;
use openwit_metrics::{
    record_ingestion, update_ingestion_rates,
    INGESTION_MESSAGES_TOTAL, INGESTION_BYTES_TOTAL,
    HTTP_REQUESTS_TOTAL as GLOBAL_HTTP_REQUESTS_TOTAL,
    HTTP_REQUEST_SIZE_BYTES, HTTP_RESPONSE_TIME,
};

lazy_static! {
    pub static ref HTTP_REQUESTS_TOTAL: IntCounterVec = register_int_counter_vec!(
        "openwit_http_requests_total",
        "Total number of HTTP requests",
        &["endpoint", "method", "status"]
    ).unwrap();
    
    pub static ref HTTP_REQUEST_DURATION: HistogramVec = register_histogram_vec!(
        "openwit_http_request_duration_seconds",
        "HTTP request duration in seconds",
        &["endpoint", "method"]
    ).unwrap();
    
    pub static ref OTLP_ITEMS_RECEIVED: IntCounterVec = register_int_counter_vec!(
        "openwit_http_otlp_items_received_total",
        "Total number of OTLP items received via HTTP",
        &["type"] // traces, logs, metrics
    ).unwrap();
}

// Helper to record HTTP ingestion with comprehensive metrics
pub fn record_http_ingestion(
    endpoint: &str,
    method: &str,
    status_code: u16,
    request_size: usize,
    items_count: usize,
    start_time: Instant,
) {
    let duration = start_time.elapsed();
    let success = status_code >= 200 && status_code < 300;
    
    // Update local HTTP metrics
    HTTP_REQUESTS_TOTAL
        .with_label_values(&[endpoint, method, &status_code.to_string()])
        .inc();
    
    HTTP_REQUEST_DURATION
        .with_label_values(&[endpoint, method])
        .observe(duration.as_secs_f64());
    
    // Update global comprehensive metrics
    record_ingestion("http", request_size, success, start_time);
    
    GLOBAL_HTTP_REQUESTS_TOTAL
        .with_label_values(&[method, &status_code.to_string()])
        .inc();
    
    HTTP_REQUEST_SIZE_BYTES
        .with_label_values(&[endpoint])
        .observe(request_size as f64);
    
    HTTP_RESPONSE_TIME
        .with_label_values(&[endpoint, &status_code.to_string()])
        .observe(duration.as_secs_f64());
    
    if success && items_count > 0 {
        INGESTION_MESSAGES_TOTAL
            .with_label_values(&["http", "success"])
            .inc_by(items_count as u64);
    }
}

// Periodic rate updater for HTTP ingestion
pub struct HttpMetricsUpdater {
    last_messages: u64,
    last_bytes: f64,
    last_errors: u64,
    last_update: Instant,
}

impl HttpMetricsUpdater {
    pub fn new() -> Self {
        Self {
            last_messages: 0,
            last_bytes: 0.0,
            last_errors: 0,
            last_update: Instant::now(),
        }
    }
    
    pub fn update_rates(&mut self) {
        let elapsed = self.last_update.elapsed().as_secs_f64();
        if elapsed < 1.0 {
            return;
        }
        
        // Get current values from metrics
        let current_messages = INGESTION_MESSAGES_TOTAL
            .with_label_values(&["http", "success"])
            .get();
        let current_bytes = INGESTION_BYTES_TOTAL
            .with_label_values(&["http"])
            .get();
        let current_errors = INGESTION_MESSAGES_TOTAL
            .with_label_values(&["http", "error"])
            .get();
        
        // Calculate rates
        let messages_per_sec = (current_messages - self.last_messages) as f64 / elapsed;
        let bytes_per_sec = (current_bytes - self.last_bytes) / elapsed;
        let errors_per_sec = (current_errors - self.last_errors) as f64 / elapsed;
        
        // Update rate metrics
        update_ingestion_rates("http", messages_per_sec, bytes_per_sec, errors_per_sec);
        
        // Update state
        self.last_messages = current_messages;
        self.last_bytes = current_bytes;
        self.last_errors = current_errors;
        self.last_update = Instant::now();
    }
}