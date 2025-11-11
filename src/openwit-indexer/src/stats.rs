//! Statistics collection and monitoring for indexes

use anyhow::Result;
use chrono::{DateTime, Utc};
use prometheus::{Histogram, IntCounter, IntGauge, Registry};
use std::collections::HashMap;
use std::sync::Arc;

/// Collects and exposes index statistics
pub struct StatsCollector {
    // Counters
    docs_indexed: IntCounter,
    docs_queried: IntCounter,
    index_errors: IntCounter,
    query_errors: IntCounter,
    
    // Gauges
    index_size_bytes: IntGauge,
    doc_count: IntGauge,
    field_count: IntGauge,
    cache_hit_ratio: IntGauge,
    
    // Histograms
    index_latency: Histogram,
    query_latency: Histogram,
    compaction_duration: Histogram,
    
    // Field-specific stats
    field_stats: Arc<dashmap::DashMap<String, FieldStatistics>>,
}

#[derive(Debug, Clone)]
pub struct FieldStatistics {
    pub cardinality: u64,
    pub null_count: u64,
    pub total_values: u64,
    pub avg_size_bytes: f64,
    pub index_size_bytes: u64,
    pub query_count: u64,
    pub last_queried: Option<DateTime<Utc>>,
}

impl StatsCollector {
    pub fn new(registry: &Registry) -> Result<Self> {
        let docs_indexed = IntCounter::new("index_docs_total", "Total documents indexed")?;
        let docs_queried = IntCounter::new("query_docs_total", "Total documents queried")?;
        let index_errors = IntCounter::new("index_errors_total", "Total indexing errors")?;
        let query_errors = IntCounter::new("query_errors_total", "Total query errors")?;
        
        let index_size_bytes = IntGauge::new("index_size_bytes", "Index size in bytes")?;
        let doc_count = IntGauge::new("index_doc_count", "Number of documents in index")?;
        let field_count = IntGauge::new("index_field_count", "Number of indexed fields")?;
        let cache_hit_ratio = IntGauge::new("cache_hit_ratio_percent", "Cache hit ratio percentage")?;
        
        let index_latency = Histogram::with_opts(
            prometheus::HistogramOpts::new("index_latency_seconds", "Indexing latency")
                .buckets(vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0])
        )?;
        
        let query_latency = Histogram::with_opts(
            prometheus::HistogramOpts::new("query_latency_seconds", "Query latency")
                .buckets(vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0])
        )?;
        
        let compaction_duration = Histogram::with_opts(
            prometheus::HistogramOpts::new("compaction_duration_seconds", "Compaction duration")
                .buckets(vec![1.0, 5.0, 10.0, 30.0, 60.0, 300.0, 600.0])
        )?;
        
        // Register all metrics
        registry.register(Box::new(docs_indexed.clone()))?;
        registry.register(Box::new(docs_queried.clone()))?;
        registry.register(Box::new(index_errors.clone()))?;
        registry.register(Box::new(query_errors.clone()))?;
        registry.register(Box::new(index_size_bytes.clone()))?;
        registry.register(Box::new(doc_count.clone()))?;
        registry.register(Box::new(field_count.clone()))?;
        registry.register(Box::new(cache_hit_ratio.clone()))?;
        registry.register(Box::new(index_latency.clone()))?;
        registry.register(Box::new(query_latency.clone()))?;
        registry.register(Box::new(compaction_duration.clone()))?;
        
        Ok(Self {
            docs_indexed,
            docs_queried,
            index_errors,
            query_errors,
            index_size_bytes,
            doc_count,
            field_count,
            cache_hit_ratio,
            index_latency,
            query_latency,
            compaction_duration,
            field_stats: Arc::new(dashmap::DashMap::new()),
        })
    }
    
    pub fn record_indexing(&self, doc_count: u64, duration_secs: f64) {
        self.docs_indexed.inc_by(doc_count);
        self.index_latency.observe(duration_secs);
    }
    
    pub fn record_query(&self, result_count: u64, duration_secs: f64) {
        self.docs_queried.inc_by(result_count);
        self.query_latency.observe(duration_secs);
    }
    
    pub fn record_index_error(&self) {
        self.index_errors.inc();
    }
    
    pub fn record_query_error(&self) {
        self.query_errors.inc();
    }
    
    pub fn update_index_size(&self, bytes: u64) {
        self.index_size_bytes.set(bytes as i64);
    }
    
    pub fn update_doc_count(&self, count: u64) {
        self.doc_count.set(count as i64);
    }
    
    pub fn update_field_stats(&self, field: String, stats: FieldStatistics) {
        self.field_stats.insert(field, stats);
        self.field_count.set(self.field_stats.len() as i64);
    }
    
    pub fn update_cache_hit_ratio(&self, ratio: f64) {
        self.cache_hit_ratio.set((ratio * 100.0) as i64);
    }
    
    pub fn record_compaction(&self, duration_secs: f64) {
        self.compaction_duration.observe(duration_secs);
    }
    
    pub fn get_summary(&self) -> IndexSummary {
        let field_summaries: HashMap<String, FieldStatistics> = self.field_stats
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect();
            
        IndexSummary {
            total_docs_indexed: self.docs_indexed.get(),
            total_docs_queried: self.docs_queried.get(),
            index_errors: self.index_errors.get(),
            query_errors: self.query_errors.get(),
            index_size_bytes: self.index_size_bytes.get() as u64,
            doc_count: self.doc_count.get() as u64,
            field_stats: field_summaries,
            cache_hit_ratio: self.cache_hit_ratio.get() as f64 / 100.0,
        }
    }
}

#[derive(Debug, Clone)]
pub struct IndexSummary {
    pub total_docs_indexed: u64,
    pub total_docs_queried: u64,
    pub index_errors: u64,
    pub query_errors: u64,
    pub index_size_bytes: u64,
    pub doc_count: u64,
    pub field_stats: HashMap<String, FieldStatistics>,
    pub cache_hit_ratio: f64,
}