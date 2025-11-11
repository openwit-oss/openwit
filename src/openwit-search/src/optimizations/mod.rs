// Search performance optimizations module

pub mod partition_pruning;
pub mod bloom_filters;
pub mod query_cache;
pub mod statistics;
pub mod pre_aggregation;

use std::sync::Arc;
use datafusion::prelude::*;
use arrow::datatypes::Schema;
use chrono::{DateTime, Utc};

/// Optimization configuration
#[derive(Debug, Clone)]
pub struct OptimizationConfig {
    /// Enable partition pruning
    pub enable_partition_pruning: bool,
    /// Enable bloom filters
    pub enable_bloom_filters: bool,
    /// Enable query result caching
    pub enable_query_cache: bool,
    /// Enable statistics-based optimization
    pub enable_statistics: bool,
    /// Cache size in MB
    pub cache_size_mb: usize,
    /// Cache TTL in seconds
    pub cache_ttl_seconds: u64,
    /// Number of parallel partitions
    pub target_partitions: usize,
}

impl Default for OptimizationConfig {
    fn default() -> Self {
        Self {
            enable_partition_pruning: true,
            enable_bloom_filters: true,
            enable_query_cache: true,
            enable_statistics: true,
            cache_size_mb: 8192, // 8GB
            cache_ttl_seconds: 3600, // 1 hour
            target_partitions: 16,
        }
    }
}

/// Query optimizer that applies various optimization strategies
pub struct QueryOptimizer {
    config: OptimizationConfig,
    partition_pruner: partition_pruning::PartitionPruner,
    bloom_filter_manager: bloom_filters::BloomFilterManager,
    query_cache: query_cache::QueryCache,
    statistics_collector: statistics::StatisticsCollector,
}

impl QueryOptimizer {
    pub fn new(config: OptimizationConfig) -> Self {
        Self {
            partition_pruner: partition_pruning::PartitionPruner::new(),
            bloom_filter_manager: bloom_filters::BloomFilterManager::new(),
            query_cache: query_cache::QueryCache::new(
                config.cache_size_mb,
                config.cache_ttl_seconds,
            ),
            statistics_collector: statistics::StatisticsCollector::new(),
            config,
        }
    }
    
    /// Optimize a query before execution
    pub async fn optimize_query(
        &self,
        ctx: &SessionContext,
        query: &str,
        time_range: Option<(DateTime<Utc>, DateTime<Utc>)>,
    ) -> Result<LogicalPlan, Box<dyn std::error::Error>> {
        // Parse the query
        let mut plan = ctx.sql(query).await?.into_optimized_plan()?;
        
        // Apply partition pruning if time range is specified
        if let Some((start, end)) = time_range {
            if self.config.enable_partition_pruning {
                plan = self.partition_pruner.prune_partitions(plan, start, end)?;
            }
        }
        
        // Apply bloom filter optimization
        if self.config.enable_bloom_filters {
            plan = self.bloom_filter_manager.optimize_with_bloom_filters(plan)?;
        }
        
        // Apply statistics-based optimization
        if self.config.enable_statistics {
            plan = self.statistics_collector.optimize_with_statistics(plan)?;
        }
        
        Ok(plan)
    }
    
    /// Check cache before executing query
    pub async fn get_cached_result(
        &self,
        query_hash: &str,
    ) -> Option<Arc<dyn RecordBatch>> {
        if self.config.enable_query_cache {
            self.query_cache.get(query_hash).await
        } else {
            None
        }
    }
    
    /// Cache query result
    pub async fn cache_result(
        &self,
        query_hash: &str,
        result: Arc<dyn RecordBatch>,
    ) {
        if self.config.enable_query_cache {
            self.query_cache.put(query_hash, result).await;
        }
    }
}