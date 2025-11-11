//! OpenWit Indexer - High-performance indexing for observability data
//! 
//! This crate provides multiple indexing strategies optimized for different
//! query patterns and data characteristics in observability workloads.

// New production-ready modules
pub mod config;
pub mod db;
pub mod storage;
pub mod parquet;
pub mod artifacts;
pub mod wal;
pub mod cluster;
pub mod flight;
pub mod init;
pub mod metrics;
pub mod events;
pub mod combine;
pub mod grpc_service;
pub mod http;
pub mod indexer_main;

// Legacy modules
pub mod inverted;
pub mod columnar;
pub mod timeseries;
pub mod fulltext;
pub mod bloom;
pub mod bloom_wrapper;
pub mod metadata;
pub mod cache;
pub mod compaction;
pub mod query;
pub mod stats;

#[cfg(test)]
pub mod memory_test;
pub mod resource_monitor;
pub mod tantivy_indexer;
// pub mod recovery; // Disabled: depends on removed storage modules (metastore, wal_reader)
pub mod arrow_flight_receiver;

#[cfg(feature = "events")]
pub mod generated {
    pub mod openwit {
        pub mod indexer {
            pub mod v1 {
                include!("./generated/openwit.indexer.v1.rs");
            }
        }
    }
}

#[cfg(not(feature = "events"))]
pub mod generated;  // Use the placeholder module

// Re-export commonly used types from new modules
pub use config::{IndexerConfig, IndexerMode, ArtifactSettings};
pub use db::{DbClient, Snapshot, IndexArtifact, DataFile, CoverageManifest};
pub use storage::{StorageClient, ArtifactMetadata, ArtifactType};
pub use parquet::{ParquetReader, FileStats, ColumnStats, RowGroupStats};
pub use artifacts::{ArtifactBuilder, ZoneMap, BloomFilter, BitmapIndex, TantivyIndex};
pub use wal::{WAL, WALOperation, WALRecord, WALStatus};

use async_trait::async_trait;
use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Represents different types of indexes available
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum IndexType {
    /// Inverted index for exact match queries
    Inverted,
    /// Columnar index for analytical queries
    Columnar,
    /// Time-series optimized index
    TimeSeries,
    /// Full-text search index
    FullText,
    /// Bloom filter for existence checks
    Bloom,
    /// Bitmap index for low-cardinality fields
    Bitmap,
    /// Hash index for equality lookups
    Hash,
    /// Range index for numeric/timestamp ranges
    Range,
}

/// Main trait for all index implementations
#[async_trait]
pub trait Index: Send + Sync {
    /// Index a batch of documents
    async fn index_batch(&mut self, docs: Vec<Document>) -> Result<()>;
    
    /// Query the index
    async fn query(&self, query: &Query) -> Result<QueryResult>;
    
    /// Get index statistics
    async fn stats(&self) -> Result<IndexStats>;
    
    /// Compact/optimize the index
    async fn compact(&mut self) -> Result<()>;
    
    /// Flush any pending writes
    async fn flush(&mut self) -> Result<()>;
}

/// Represents a document to be indexed
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Document {
    pub id: String,
    pub timestamp: DateTime<Utc>,
    pub fields: HashMap<String, FieldValue>,
    pub raw_size: usize,
}

/// Field values in documents
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FieldValue {
    String(String),
    Number(f64),
    Integer(i64),
    Boolean(bool),
    Timestamp(DateTime<Utc>),
    Array(Vec<FieldValue>),
    Object(HashMap<String, FieldValue>),
    Null,
}

/// Query representation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Query {
    pub filters: Vec<Filter>,
    pub time_range: Option<TimeRange>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub sort: Option<Vec<SortField>>,
    pub aggregations: Option<Vec<Aggregation>>,
}

/// Filter types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Filter {
    Equals { field: String, value: FieldValue },
    NotEquals { field: String, value: FieldValue },
    In { field: String, values: Vec<FieldValue> },
    NotIn { field: String, values: Vec<FieldValue> },
    GreaterThan { field: String, value: FieldValue },
    LessThan { field: String, value: FieldValue },
    Contains { field: String, value: String },
    Regex { field: String, pattern: String },
    And(Vec<Filter>),
    Or(Vec<Filter>),
    Not(Box<Filter>),
}

/// Time range for queries
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeRange {
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
}

/// Sort configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SortField {
    pub field: String,
    pub ascending: bool,
}

/// Aggregation types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Aggregation {
    Count,
    Sum(String),
    Avg(String),
    Min(String),
    Max(String),
    GroupBy { field: String, aggs: Vec<Aggregation> },
    Histogram { field: String, interval: f64 },
    DateHistogram { field: String, interval: String },
}

/// Query results
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResult {
    pub hits: Vec<Document>,
    pub total_hits: usize,
    pub aggregations: Option<HashMap<String, AggregationResult>>,
    pub query_time_ms: u64,
}

/// Aggregation results
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AggregationResult {
    Count(u64),
    Sum(f64),
    Avg(f64),
    Min(f64),
    Max(f64),
    Buckets(Vec<Bucket>),
}

/// Bucket for histogram/group-by results
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Bucket {
    pub key: FieldValue,
    pub doc_count: u64,
    pub aggregations: Option<HashMap<String, AggregationResult>>,
}

/// Index statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexStats {
    pub index_type: IndexType,
    pub doc_count: u64,
    pub size_bytes: u64,
    pub field_stats: HashMap<String, FieldStats>,
    pub last_updated: DateTime<Utc>,
    pub query_performance: QueryPerformance,
}

/// Per-field statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldStats {
    pub cardinality: u64,
    pub null_count: u64,
    pub min_value: Option<FieldValue>,
    pub max_value: Option<FieldValue>,
    pub avg_size_bytes: u64,
}

/// Query performance metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryPerformance {
    pub avg_query_time_ms: f64,
    pub p50_query_time_ms: f64,
    pub p95_query_time_ms: f64,
    pub p99_query_time_ms: f64,
    pub queries_per_second: f64,
}

/// Index configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexConfig {
    pub index_type: IndexType,
    pub fields: Vec<FieldConfig>,
    pub storage_path: String,
    pub cache_size_mb: usize,
    pub compression: CompressionType,
    pub compaction_threshold: f64,
    pub bloom_filter_fpp: f64,
}

impl Default for IndexConfig {
    fn default() -> Self {
        Self {
            index_type: IndexType::FullText,
            fields: vec![],
            storage_path: "data/index".to_string(),
            cache_size_mb: 1024,
            compression: CompressionType::Zstd,
            compaction_threshold: 0.7,
            bloom_filter_fpp: 0.01,
        }
    }
}

/// Field-specific configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldConfig {
    pub name: String,
    pub field_type: FieldType,
    pub indexed: bool,
    pub stored: bool,
    pub tokenized: bool,
    pub faceted: bool,
    pub fast: bool,  // For columnar storage
}

/// Field data types
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum FieldType {
    String,
    Number,
    Integer,
    Boolean,
    Timestamp,
    Text,  // For full-text search
    Keyword,  // For exact match
    Ip,
    Geo,
}

/// Compression types
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum CompressionType {
    None,
    Snappy,
    Lz4,
    Zstd,
    Gzip,
}

/// Factory for creating indexes
pub struct IndexFactory;

impl IndexFactory {
    /// Create a new index based on configuration
    pub async fn create(config: IndexConfig) -> Result<Box<dyn Index>> {
        match config.index_type {
            IndexType::Inverted => {
                // Create inverted index
                todo!("Implement inverted index creation")
            },
            IndexType::Columnar => {
                // Create columnar index
                todo!("Implement columnar index creation")
            },
            IndexType::TimeSeries => {
                // Create time-series index
                todo!("Implement time-series index creation")
            },
            IndexType::FullText => {
                // Create full-text index
                todo!("Implement full-text index creation")
            },
            IndexType::Bloom => {
                // Create bloom filter index
                todo!("Implement bloom filter index creation")
            },
            _ => {
                todo!("Implement other index types")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_types() {
        // Add tests
    }
}