use std::sync::Arc;
use moka::future::Cache;
use arrow::record_batch::RecordBatch;
use serde::{Serialize, Deserialize};
use sha2::{Sha256, Digest};

/// Cached query result
#[derive(Clone)]
pub struct CachedResult {
    pub data: Vec<RecordBatch>,
    pub total_rows: u64,
    pub cached_at: chrono::DateTime<chrono::Utc>,
}

/// Multi-level query cache
pub struct QueryCache {
    /// L1: In-memory cache using moka
    l1_cache: Cache<String, Arc<CachedResult>>,
    /// Cache statistics
    hits: std::sync::atomic::AtomicU64,
    misses: std::sync::atomic::AtomicU64,
}

impl QueryCache {
    pub fn new(size_mb: usize, ttl_seconds: u64) -> Self {
        // Calculate max items based on average result size
        let max_items = (size_mb * 1024 * 1024) / (100 * 1024); // Assume 100KB average
        
        let cache = Cache::builder()
            .max_capacity(max_items as u64)
            .time_to_live(std::time::Duration::from_secs(ttl_seconds))
            .build();
            
        Self {
            l1_cache: cache,
            hits: std::sync::atomic::AtomicU64::new(0),
            misses: std::sync::atomic::AtomicU64::new(0),
        }
    }
    
    /// Generate cache key from query and parameters
    pub fn generate_cache_key(
        query: &str,
        params: &QueryParams,
    ) -> String {
        let mut hasher = Sha256::new();
        hasher.update(query.as_bytes());
        hasher.update(format!("{:?}", params).as_bytes());
        
        // Add time bucket for time-based invalidation
        if let Some(time_range) = &params.time_range {
            let bucket = time_range.0.timestamp() / 300; // 5-minute buckets
            hasher.update(bucket.to_string().as_bytes());
        }
        
        format!("{:x}", hasher.finalize())
    }
    
    /// Get cached result
    pub async fn get(&self, key: &str) -> Option<Arc<CachedResult>> {
        if let Some(result) = self.l1_cache.get(key).await {
            self.hits.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Some(result)
        } else {
            self.misses.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            None
        }
    }
    
    /// Put result in cache
    pub async fn put(&self, key: String, result: Arc<CachedResult>) {
        self.l1_cache.insert(key, result).await;
    }
    
    /// Get cache statistics
    pub fn stats(&self) -> CacheStats {
        CacheStats {
            hits: self.hits.load(std::sync::atomic::Ordering::Relaxed),
            misses: self.misses.load(std::sync::atomic::Ordering::Relaxed),
            size: self.l1_cache.entry_count(),
        }
    }
    
    /// Invalidate cache entries matching pattern
    pub async fn invalidate_pattern(&self, pattern: &str) {
        // This would need to track keys for pattern matching
        // For now, invalidate all
        self.l1_cache.invalidate_all();
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct QueryParams {
    pub client_id: String,
    pub time_range: Option<(chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>)>,
    pub filters: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct CacheStats {
    pub hits: u64,
    pub misses: u64,
    pub size: u64,
}

impl CacheStats {
    pub fn hit_rate(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            0.0
        } else {
            self.hits as f64 / total as f64
        }
    }
}