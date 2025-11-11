//! Comprehensive ingestion metrics for HTTP, gRPC, and Kafka sources
//! Works across all deployment modes (monolith, control plane, proxy)

use once_cell::sync::Lazy;
use prometheus::{
    HistogramOpts, HistogramVec, IntCounterVec, IntGaugeVec, 
    CounterVec, GaugeVec
};
use crate::REGISTRY;

// ============================================================================
// Unified Ingestion Metrics (across all sources)
// ============================================================================

/// Total messages ingested by source and status
pub static INGESTION_MESSAGES_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    let c = IntCounterVec::new(
        prometheus::opts!(
            "openwit_ingestion_messages_total",
            "Total messages ingested by source and status"
        ),
        &["source", "status"]  // source: http/grpc/kafka, status: success/error
    ).unwrap();
    REGISTRY.register(Box::new(c.clone())).unwrap();
    c
});

/// Total bytes ingested by source
pub static INGESTION_BYTES_TOTAL: Lazy<CounterVec> = Lazy::new(|| {
    let c = CounterVec::new(
        prometheus::opts!(
            "openwit_ingestion_bytes_total",
            "Total bytes ingested by source"
        ),
        &["source"]  // source: http/grpc/kafka
    ).unwrap();
    REGISTRY.register(Box::new(c.clone())).unwrap();
    c
});

/// Current ingestion rate per source (messages/sec)
pub static INGESTION_RATE_CURRENT: Lazy<GaugeVec> = Lazy::new(|| {
    let g = GaugeVec::new(
        prometheus::opts!(
            "openwit_ingestion_rate_messages_per_sec",
            "Current ingestion rate in messages per second by source"
        ),
        &["source"]  // source: http/grpc/kafka
    ).unwrap();
    REGISTRY.register(Box::new(g.clone())).unwrap();
    g
});

/// Current ingestion throughput per source (bytes/sec)
pub static INGESTION_THROUGHPUT_CURRENT: Lazy<GaugeVec> = Lazy::new(|| {
    let g = GaugeVec::new(
        prometheus::opts!(
            "openwit_ingestion_throughput_bytes_per_sec",
            "Current ingestion throughput in bytes per second by source"
        ),
        &["source"]  // source: http/grpc/kafka
    ).unwrap();
    REGISTRY.register(Box::new(g.clone())).unwrap();
    g
});

/// Error rate per source (errors/sec)
pub static INGESTION_ERROR_RATE: Lazy<GaugeVec> = Lazy::new(|| {
    let g = GaugeVec::new(
        prometheus::opts!(
            "openwit_ingestion_error_rate_per_sec",
            "Current error rate per second by source"
        ),
        &["source"]  // source: http/grpc/kafka
    ).unwrap();
    REGISTRY.register(Box::new(g.clone())).unwrap();
    g
});

/// Ingestion latency histogram by source
pub static INGESTION_LATENCY: Lazy<HistogramVec> = Lazy::new(|| {
    let h = HistogramVec::new(
        HistogramOpts::new(
            "openwit_ingestion_latency_seconds",
            "Ingestion latency in seconds by source"
        ),
        &["source"]  // source: http/grpc/kafka
    ).unwrap();
    REGISTRY.register(Box::new(h.clone())).unwrap();
    h
});

/// Active connections/consumers by source
pub static INGESTION_ACTIVE_CONNECTIONS: Lazy<IntGaugeVec> = Lazy::new(|| {
    let g = IntGaugeVec::new(
        prometheus::opts!(
            "openwit_ingestion_active_connections",
            "Number of active connections/consumers by source"
        ),
        &["source"]  // source: http/grpc/kafka
    ).unwrap();
    REGISTRY.register(Box::new(g.clone())).unwrap();
    g
});

// ============================================================================
// HTTP-Specific Metrics
// ============================================================================

/// HTTP requests by method and status code
pub static HTTP_REQUESTS_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    let c = IntCounterVec::new(
        prometheus::opts!(
            "openwit_http_requests_total",
            "Total HTTP requests by method and status code"
        ),
        &["method", "status_code"]
    ).unwrap();
    REGISTRY.register(Box::new(c.clone())).unwrap();
    c
});

/// HTTP request size histogram
pub static HTTP_REQUEST_SIZE_BYTES: Lazy<HistogramVec> = Lazy::new(|| {
    let h = HistogramVec::new(
        HistogramOpts::new(
            "openwit_http_request_size_bytes",
            "HTTP request size in bytes"
        ),
        &["endpoint"]
    ).unwrap();
    REGISTRY.register(Box::new(h.clone())).unwrap();
    h
});

/// HTTP response time histogram
pub static HTTP_RESPONSE_TIME: Lazy<HistogramVec> = Lazy::new(|| {
    let h = HistogramVec::new(
        HistogramOpts::new(
            "openwit_http_response_time_seconds",
            "HTTP response time in seconds"
        ),
        &["endpoint", "status_code"]
    ).unwrap();
    REGISTRY.register(Box::new(h.clone())).unwrap();
    h
});

// ============================================================================
// gRPC-Specific Metrics
// ============================================================================

/// gRPC requests by method and status
pub static GRPC_REQUESTS_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    let c = IntCounterVec::new(
        prometheus::opts!(
            "openwit_grpc_requests_total",
            "Total gRPC requests by method and status"
        ),
        &["method", "status"]
    ).unwrap();
    REGISTRY.register(Box::new(c.clone())).unwrap();
    c
});

/// gRPC streaming messages received
pub static GRPC_STREAMING_MESSAGES: Lazy<IntCounterVec> = Lazy::new(|| {
    let c = IntCounterVec::new(
        prometheus::opts!(
            "openwit_grpc_streaming_messages_total",
            "Total streaming messages received by method"
        ),
        &["method"]
    ).unwrap();
    REGISTRY.register(Box::new(c.clone())).unwrap();
    c
});

/// gRPC active streams
pub static GRPC_ACTIVE_STREAMS: Lazy<IntGaugeVec> = Lazy::new(|| {
    let g = IntGaugeVec::new(
        prometheus::opts!(
            "openwit_grpc_active_streams",
            "Number of active gRPC streams by method"
        ),
        &["method"]
    ).unwrap();
    REGISTRY.register(Box::new(g.clone())).unwrap();
    g
});

// ============================================================================
// Kafka-Specific Metrics
// ============================================================================

/// Kafka consumer lag by topic and partition
pub static KAFKA_CONSUMER_LAG: Lazy<IntGaugeVec> = Lazy::new(|| {
    let g = IntGaugeVec::new(
        prometheus::opts!(
            "openwit_kafka_consumer_lag",
            "Kafka consumer lag by topic and partition"
        ),
        &["topic", "partition", "consumer_group"]
    ).unwrap();
    REGISTRY.register(Box::new(g.clone())).unwrap();
    g
});

/// Kafka messages consumed by topic
pub static KAFKA_MESSAGES_CONSUMED: Lazy<IntCounterVec> = Lazy::new(|| {
    let c = IntCounterVec::new(
        prometheus::opts!(
            "openwit_kafka_messages_consumed_total",
            "Total Kafka messages consumed by topic"
        ),
        &["topic", "consumer_id"]
    ).unwrap();
    REGISTRY.register(Box::new(c.clone())).unwrap();
    c
});

/// Kafka consumer rebalances
pub static KAFKA_REBALANCES_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    let c = IntCounterVec::new(
        prometheus::opts!(
            "openwit_kafka_rebalances_total",
            "Total Kafka consumer rebalances"
        ),
        &["consumer_group", "type"]  // type: assign/revoke
    ).unwrap();
    REGISTRY.register(Box::new(c.clone())).unwrap();
    c
});

/// Kafka commit latency
pub static KAFKA_COMMIT_LATENCY: Lazy<HistogramVec> = Lazy::new(|| {
    let h = HistogramVec::new(
        HistogramOpts::new(
            "openwit_kafka_commit_latency_seconds",
            "Kafka offset commit latency"
        ),
        &["consumer_group"]
    ).unwrap();
    REGISTRY.register(Box::new(h.clone())).unwrap();
    h
});

// ============================================================================
// Buffer and Queue Metrics
// ============================================================================

/// Ingestion buffer size by source
pub static INGESTION_BUFFER_SIZE: Lazy<IntGaugeVec> = Lazy::new(|| {
    let g = IntGaugeVec::new(
        prometheus::opts!(
            "openwit_ingestion_buffer_size",
            "Current number of messages in buffer by source"
        ),
        &["source", "buffer_type"]  // buffer_type: memory/disk/channel
    ).unwrap();
    REGISTRY.register(Box::new(g.clone())).unwrap();
    g
});

/// Ingestion buffer capacity utilization
pub static INGESTION_BUFFER_UTILIZATION: Lazy<GaugeVec> = Lazy::new(|| {
    let g = GaugeVec::new(
        prometheus::opts!(
            "openwit_ingestion_buffer_utilization_percent",
            "Buffer capacity utilization percentage by source"
        ),
        &["source", "buffer_type"]
    ).unwrap();
    REGISTRY.register(Box::new(g.clone())).unwrap();
    g
});

// ============================================================================
// Helper functions to update metrics
// ============================================================================

use std::time::Instant;

/// Record ingestion event with automatic metric updates
pub fn record_ingestion(source: &str, bytes: usize, success: bool, start_time: Instant) {
    let status = if success { "success" } else { "error" };
    
    // Update counters
    INGESTION_MESSAGES_TOTAL
        .with_label_values(&[source, status])
        .inc();
    
    if success {
        INGESTION_BYTES_TOTAL
            .with_label_values(&[source])
            .inc_by(bytes as f64);
        
        // Record latency
        let duration = start_time.elapsed();
        INGESTION_LATENCY
            .with_label_values(&[source])
            .observe(duration.as_secs_f64());
    }
}

/// Update ingestion rate metrics (call this periodically)
pub fn update_ingestion_rates(
    source: &str,
    messages_per_sec: f64,
    bytes_per_sec: f64,
    errors_per_sec: f64,
) {
    INGESTION_RATE_CURRENT
        .with_label_values(&[source])
        .set(messages_per_sec);
    
    INGESTION_THROUGHPUT_CURRENT
        .with_label_values(&[source])
        .set(bytes_per_sec);
    
    INGESTION_ERROR_RATE
        .with_label_values(&[source])
        .set(errors_per_sec);
}

/// Update buffer metrics
pub fn update_buffer_metrics(source: &str, buffer_type: &str, size: usize, capacity: usize) {
    INGESTION_BUFFER_SIZE
        .with_label_values(&[source, buffer_type])
        .set(size as i64);
    
    let utilization = if capacity > 0 {
        (size as f64 / capacity as f64) * 100.0
    } else {
        0.0
    };
    
    INGESTION_BUFFER_UTILIZATION
        .with_label_values(&[source, buffer_type])
        .set(utilization);
}