use std::sync::Arc;
use std::time::{Duration, Instant};
use anyhow::Result;
use tokio::sync::RwLock;
use lru::LruCache;
use sha2::{Sha256, Digest};
use tracing::{debug, info};

/// Query cache key
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
struct CacheKey {
    sql_hash: String,
    client_name: Option<String>,
    limit: Option<usize>,
}

impl CacheKey {
    fn new(sql: &str, client_name: Option<String>, limit: Option<usize>) -> Self {
        // Create hash of SQL query for efficient lookup
        let mut hasher = Sha256::new();
        hasher.update(sql.as_bytes());
        let sql_hash = format!("{:x}", hasher.finalize());
        
        Self {
            sql_hash,
            client_name,
            limit,
        }
    }
}

/// Cached query result
struct CachedResult {
    data: Vec<serde_json::Value>,
    schema: Option<String>,
    row_count: usize,
    cached_at: Instant,
    execution_time_ms: u64,
}

/// Query result cache with TTL
pub struct QueryCache {
    cache: Arc<RwLock<LruCache<CacheKey, CachedResult>>>,
    ttl: Duration,
}

impl QueryCache {
    pub fn new(max_entries: usize, ttl_seconds: u64) -> Self {
        let cache = LruCache::new(max_entries.try_into().unwrap_or(std::num::NonZeroUsize::new(1000).unwrap()));
        Self {
            cache: Arc::new(RwLock::new(cache)),
            ttl: Duration::from_secs(ttl_seconds),
        }
    }
    
    /// Get cached result if available and not expired
    pub async fn get(
        &self,
        sql: &str,
        client_name: Option<String>,
        limit: Option<usize>,
    ) -> Option<(Vec<serde_json::Value>, Option<String>, usize, u64)> {
        let key = CacheKey::new(sql, client_name, limit);
        
        let mut cache = self.cache.write().await;
        
        if let Some(cached) = cache.get_mut(&key) {
            // Check if cache entry is still valid
            if cached.cached_at.elapsed() < self.ttl {
                debug!(
                    "[QUERY CACHE] Cache hit for query (hash: {}, age: {:?})",
                    key.sql_hash,
                    cached.cached_at.elapsed()
                );
                return Some((
                    cached.data.clone(),
                    cached.schema.clone(),
                    cached.row_count,
                    cached.execution_time_ms,
                ));
            } else {
                debug!("[QUERY CACHE] Cache entry expired for query (hash: {})", key.sql_hash);
                cache.pop(&key);
            }
        }
        
        None
    }
    
    /// Store query result in cache
    pub async fn put(
        &self,
        sql: &str,
        client_name: Option<String>,
        limit: Option<usize>,
        data: Vec<serde_json::Value>,
        schema: Option<String>,
        row_count: usize,
        execution_time_ms: u64,
    ) {
        let key = CacheKey::new(sql, client_name.clone(), limit);
        
        let cached = CachedResult {
            data,
            schema,
            row_count,
            cached_at: Instant::now(),
            execution_time_ms,
        };
        
        let mut cache = self.cache.write().await;
        cache.put(key.clone(), cached);
        
        debug!(
            "[QUERY CACHE] Cached query result (hash: {}, rows: {}, client: {:?})",
            key.sql_hash, row_count, client_name
        );
    }
    
    /// Clear all cached entries
    pub async fn clear(&self) {
        let mut cache = self.cache.write().await;
        cache.clear();
        info!("[QUERY CACHE] Cleared all cached entries");
    }
    
    /// Get cache statistics
    pub async fn stats(&self) -> (usize, usize) {
        let cache = self.cache.read().await;
        (cache.len(), cache.cap().try_into().unwrap_or(0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_query_cache() {
        let cache = QueryCache::new(10, 60);
        
        // Test cache miss
        let result = cache.get("SELECT * FROM test", None, None).await;
        assert!(result.is_none());
        
        // Store result
        let data = vec![serde_json::json!({"id": 1, "name": "test"})];
        cache.put(
            "SELECT * FROM test",
            None,
            None,
            data.clone(),
            Some("schema".to_string()),
            1,
            100,
        ).await;
        
        // Test cache hit
        let result = cache.get("SELECT * FROM test", None, None).await;
        assert!(result.is_some());
        let (cached_data, schema, count, time) = result.unwrap();
        assert_eq!(cached_data.len(), 1);
        assert_eq!(schema, Some("schema".to_string()));
        assert_eq!(count, 1);
        assert_eq!(time, 100);
    }
    
    #[tokio::test]
    async fn test_cache_expiry() {
        let cache = QueryCache::new(10, 1); // 1 second TTL
        
        let data = vec![serde_json::json!({"id": 1})];
        cache.put("SELECT 1", None, None, data, None, 1, 50).await;
        
        // Should be in cache
        assert!(cache.get("SELECT 1", None, None).await.is_some());
        
        // Wait for expiry
        tokio::time::sleep(Duration::from_secs(2)).await;
        
        // Should be expired
        assert!(cache.get("SELECT 1", None, None).await.is_none());
    }
}