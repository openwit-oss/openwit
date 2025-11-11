//! Prometheus instrumentation for all OpenWIT binaries.
//!
//! Re-exports the counters / histograms defined in [`counters`] and the
//! Axum router for the `/metrics` endpoint provided by [`exporter`].

#![deny(missing_docs)]

mod counters;
pub mod exporter;
/// Health monitoring module for tracking node health status
pub mod health;
/// Comprehensive ingestion metrics for all sources
pub mod ingestion_metrics;

pub use counters::{
    EXPORT_LATENCY, INGEST_LOG_COUNTER, INGEST_METRIC_DP_COUNTER, INGEST_SPAN_COUNTER, REGISTRY,
    SYSTEM_MEM_GAUGE, SYSTEM_MEMORY_TOTAL_GAUGE, SYSTEM_CPU_GAUGE, SYSTEM_THREADS_GAUGE,
    SYSTEM_SWAP_USED_GAUGE, SYSTEM_SWAP_TOTAL_GAUGE,
    INGEST_RATE_GAUGE, BUFFER_SIZE_GAUGE, KAFKA_LAG_GAUGE,
    WAL_SUCCESS_COUNTER, WAL_FAILURE_COUNTER, ROLE_INGEST_RATE,
    STORAGE_TIER_SIZE_BYTES, STORAGE_TIER_PROMOTIONS, STORAGE_TIER_DEMOTIONS,
    STORAGE_TIER_HITS, STORAGE_TIER_MISSES, STORAGE_TIER_QUERY_LATENCY
};

pub use health::{HealthMonitor, NodeHealth, HealthStatus};

// Export all ingestion metrics
pub use ingestion_metrics::{
    // Unified metrics
    INGESTION_MESSAGES_TOTAL, INGESTION_BYTES_TOTAL, INGESTION_RATE_CURRENT,
    INGESTION_THROUGHPUT_CURRENT, INGESTION_ERROR_RATE, INGESTION_LATENCY,
    INGESTION_ACTIVE_CONNECTIONS, INGESTION_BUFFER_SIZE, INGESTION_BUFFER_UTILIZATION,
    // HTTP metrics
    HTTP_REQUESTS_TOTAL, HTTP_REQUEST_SIZE_BYTES, HTTP_RESPONSE_TIME,
    // gRPC metrics
    GRPC_REQUESTS_TOTAL, GRPC_STREAMING_MESSAGES, GRPC_ACTIVE_STREAMS,
    // Kafka metrics
    KAFKA_CONSUMER_LAG, KAFKA_MESSAGES_CONSUMED, KAFKA_REBALANCES_TOTAL, KAFKA_COMMIT_LATENCY,
    // Helper functions
    record_ingestion, update_ingestion_rates, update_buffer_metrics
};
