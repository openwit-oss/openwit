//! counters.rs  – corrected
use once_cell::sync::Lazy;             // ← ADD THIS LINE
use prometheus::{
    HistogramOpts, HistogramVec, IntCounter, Registry, IntGauge, IntCounterVec, GaugeVec
};

/// Global registry holding all metric families
pub static REGISTRY: Lazy<Registry> = Lazy::new(Registry::new);

/// Total OTLP log-records ingested
pub static INGEST_LOG_COUNTER: Lazy<IntCounter> = Lazy::new(|| {
    let c = IntCounter::new(
        "openwit_ingest_log_records_total",
        "Log records ingested",
    )
        .unwrap();
    REGISTRY.register(Box::new(c.clone())).unwrap();
    c
});

/// Total OTLP spans ingested
pub static INGEST_SPAN_COUNTER: Lazy<IntCounter> = Lazy::new(|| {
    let c = IntCounter::new("openwit_ingest_spans_total", "Spans ingested").unwrap();
    REGISTRY.register(Box::new(c.clone())).unwrap();
    c
});

/// Total OTLP metric data-points ingested
pub static INGEST_METRIC_DP_COUNTER: Lazy<IntCounter> = Lazy::new(|| {
    let c = IntCounter::new(
        "openwit_ingest_metric_datapoints_total",
        "Metric datapoints ingested",
    )
        .unwrap();
    REGISTRY.register(Box::new(c.clone())).unwrap();
    c
});

/// Export RPC latency histogram (label = data type)
pub static EXPORT_LATENCY: Lazy<HistogramVec> = Lazy::new(|| {
    let opts = HistogramOpts::new(
        "openwit_export_latency_seconds",
        "End-to-end latency of OTLP Export calls",
    );
    let h = HistogramVec::new(opts, &["datatype"]).unwrap();
    REGISTRY.register(Box::new(h.clone())).unwrap();
    h
});
/// System CPU gauge used in bytes
pub static SYSTEM_CPU_GAUGE: Lazy<IntGauge> = Lazy::new(|| {
    let g = IntGauge::new(
        "openwit_system_cpu_percent",
        "System CPU usage percentage",
    ).unwrap();
    REGISTRY.register(Box::new(g.clone())).unwrap();
    g
});

/// System memory used in bytes
pub static SYSTEM_MEM_GAUGE: Lazy<IntGauge> = Lazy::new(|| {
    let g = IntGauge::new(
        "openwit_system_memory_used_bytes",
        "System memory used in bytes",
    ).unwrap();
    REGISTRY.register(Box::new(g.clone())).unwrap();
    g
});


/// System memory total gauge in bytes
pub static SYSTEM_MEMORY_TOTAL_GAUGE: Lazy<IntGauge> = Lazy::new(|| {
    let g = IntGauge::new(
        "openwit_system_memory_total_bytes",
        "Total system memory in bytes",
    ).unwrap();
    REGISTRY.register(Box::new(g.clone())).unwrap();
    g
});

/// Number of OS threads in the process
pub static SYSTEM_THREADS_GAUGE: Lazy<IntGauge> = Lazy::new(|| {
    let g = IntGauge::new(
        "openwit_system_thread_count",
        "Number of OS threads in the process",
    ).unwrap();
    REGISTRY.register(Box::new(g.clone())).unwrap();
    g
});


/// System swap used in bytes
pub static SYSTEM_SWAP_USED_GAUGE: Lazy<IntGauge> = Lazy::new(|| {
    let g = IntGauge::new(
        "openwit_system_swap_used_bytes",
        "System swap used in bytes",
    ).unwrap();
    REGISTRY.register(Box::new(g.clone())).unwrap();
    g
});

/// Total system swap in bytes
pub static SYSTEM_SWAP_TOTAL_GAUGE: Lazy<IntGauge> = Lazy::new(|| {
    let g = IntGauge::new(
        "openwit_system_swap_total_bytes",
        "Total system swap in bytes",
    ).unwrap();
    REGISTRY.register(Box::new(g.clone())).unwrap();
    g
});

/// Ingestion rate (messages per second)
pub static INGEST_RATE_GAUGE: Lazy<IntGauge> = Lazy::new(|| {
    let g = IntGauge::new(
        "openwit_ingest_rate",
        "Current ingestion rate in messages per second",
    ).unwrap();
    REGISTRY.register(Box::new(g.clone())).unwrap();
    g
});

/// Buffer size (current messages in buffer)
pub static BUFFER_SIZE_GAUGE: Lazy<IntGauge> = Lazy::new(|| {
    let g = IntGauge::new(
        "openwit_buffer_size",
        "Current number of messages in ingestion buffer",
    ).unwrap();
    REGISTRY.register(Box::new(g.clone())).unwrap();
    g
});

/// Kafka consumer lag per partition
pub static KAFKA_LAG_GAUGE: Lazy<IntGauge> = Lazy::new(|| {
    let g = IntGauge::new(
        "openwit_kafka_lag",
        "Total Kafka consumer lag across all partitions",
    ).unwrap();
    REGISTRY.register(Box::new(g.clone())).unwrap();
    g
});

/// WAL write success counter
pub static WAL_SUCCESS_COUNTER: Lazy<IntCounter> = Lazy::new(|| {
    let c = IntCounter::new(
        "openwit_wal_write_success_total",
        "Total successful WAL writes",
    ).unwrap();
    REGISTRY.register(Box::new(c.clone())).unwrap();
    c
});

/// WAL write failure counter
pub static WAL_FAILURE_COUNTER: Lazy<IntCounter> = Lazy::new(|| {
    let c = IntCounter::new(
        "openwit_wal_write_failure_total",
        "Total failed WAL writes",
    ).unwrap();
    REGISTRY.register(Box::new(c.clone())).unwrap();
    c
});

/// Per-role ingestion rate (for HPA scaling)
pub static ROLE_INGEST_RATE: Lazy<IntCounterVec> = Lazy::new(|| {
    let c = IntCounterVec::new(
        prometheus::opts!(
            "openwit_role_ingest_rate_total",
            "Total messages ingested per role"
        ),
        &["role"]
    ).unwrap();
    REGISTRY.register(Box::new(c.clone())).unwrap();
    c
});

// Storage tier management metrics

/// Size of data in each storage tier
pub static STORAGE_TIER_SIZE_BYTES: Lazy<GaugeVec> = Lazy::new(|| {
    let g = GaugeVec::new(
        prometheus::opts!(
            "openwit_storage_tier_size_bytes",
            "Size of data in each storage tier in bytes"
        ),
        &["tier"]
    ).unwrap();
    REGISTRY.register(Box::new(g.clone())).unwrap();
    g
});

/// Number of tier promotions
pub static STORAGE_TIER_PROMOTIONS: Lazy<IntCounterVec> = Lazy::new(|| {
    let c = IntCounterVec::new(
        prometheus::opts!(
            "openwit_storage_tier_promotions_total",
            "Total number of tier promotions"
        ),
        &["from_tier", "to_tier"]
    ).unwrap();
    REGISTRY.register(Box::new(c.clone())).unwrap();
    c
});

/// Number of tier demotions
pub static STORAGE_TIER_DEMOTIONS: Lazy<IntCounterVec> = Lazy::new(|| {
    let c = IntCounterVec::new(
        prometheus::opts!(
            "openwit_storage_tier_demotions_total",
            "Total number of tier demotions"
        ),
        &["from_tier", "to_tier"]
    ).unwrap();
    REGISTRY.register(Box::new(c.clone())).unwrap();
    c
});

/// Tier cache hits
pub static STORAGE_TIER_HITS: Lazy<IntCounterVec> = Lazy::new(|| {
    let c = IntCounterVec::new(
        prometheus::opts!(
            "openwit_storage_tier_hits_total",
            "Total number of cache hits per tier"
        ),
        &["tier"]
    ).unwrap();
    REGISTRY.register(Box::new(c.clone())).unwrap();
    c
});

/// Tier cache misses
pub static STORAGE_TIER_MISSES: Lazy<IntCounterVec> = Lazy::new(|| {
    let c = IntCounterVec::new(
        prometheus::opts!(
            "openwit_storage_tier_misses_total",
            "Total number of cache misses per tier"
        ),
        &["tier"]
    ).unwrap();
    REGISTRY.register(Box::new(c.clone())).unwrap();
    c
});

/// Storage tier query latency
pub static STORAGE_TIER_QUERY_LATENCY: Lazy<HistogramVec> = Lazy::new(|| {
    let h = HistogramVec::new(
        HistogramOpts::new(
            "openwit_storage_tier_query_latency_seconds",
            "Query latency for each storage tier"
        ),
        &["tier"]
    ).unwrap();
    REGISTRY.register(Box::new(h.clone())).unwrap();
    h
});

