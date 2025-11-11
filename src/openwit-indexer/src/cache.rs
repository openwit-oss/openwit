//! Caching layer for indexes

use moka::future::Cache;
use std::sync::Arc;
use std::time::Duration;

use crate::{Document, QueryResult};

/// Multi-level cache for index operations
pub struct IndexCache {
    /// Query result cache
    query_cache: Cache<String, Arc<QueryResult>>,
    /// Document cache
    doc_cache: Cache<String, Arc<Document>>,
    /// Field value cache (for low cardinality fields)
    field_cache: Cache<(String, Vec<u8>), Arc<roaring::RoaringBitmap>>,
}

impl IndexCache {
    pub fn new(config: CacheConfig) -> Self {
        let query_cache = Cache::builder()
            .max_capacity(config.query_cache_size)
            .time_to_live(Duration::from_secs(config.query_cache_ttl_secs))
            .build();
            
        let doc_cache = Cache::builder()
            .max_capacity(config.doc_cache_size)
            .time_to_live(Duration::from_secs(config.doc_cache_ttl_secs))
            .build();
            
        let field_cache = Cache::builder()
            .max_capacity(config.field_cache_size)
            .time_to_live(Duration::from_secs(config.field_cache_ttl_secs))
            .build();
            
        Self {
            query_cache,
            doc_cache,
            field_cache,
        }
    }
    
    pub async fn get_query_result(&self, key: &str) -> Option<Arc<QueryResult>> {
        self.query_cache.get(key).await
    }
    
    pub async fn put_query_result(&self, key: String, result: Arc<QueryResult>) {
        self.query_cache.insert(key, result).await;
    }
    
    pub async fn get_document(&self, id: &str) -> Option<Arc<Document>> {
        self.doc_cache.get(id).await
    }
    
    pub async fn put_document(&self, doc: Arc<Document>) {
        self.doc_cache.insert(doc.id.clone(), doc).await;
    }
    
    pub async fn get_field_bitmap(&self, field: &str, value: &[u8]) -> Option<Arc<roaring::RoaringBitmap>> {
        self.field_cache.get(&(field.to_string(), value.to_vec())).await
    }
    
    pub async fn put_field_bitmap(&self, field: String, value: Vec<u8>, bitmap: Arc<roaring::RoaringBitmap>) {
        self.field_cache.insert((field, value), bitmap).await;
    }
    
    pub async fn invalidate_query_cache(&self) {
        self.query_cache.invalidate_all();
    }
    
    pub async fn stats(&self) -> CacheStats {
        CacheStats {
            query_cache_size: self.query_cache.entry_count(),
            query_cache_hits: self.query_cache.weighted_size(),
            doc_cache_size: self.doc_cache.entry_count(),
            field_cache_size: self.field_cache.entry_count(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CacheConfig {
    pub query_cache_size: u64,
    pub query_cache_ttl_secs: u64,
    pub doc_cache_size: u64,
    pub doc_cache_ttl_secs: u64,
    pub field_cache_size: u64,
    pub field_cache_ttl_secs: u64,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            query_cache_size: 10_000,
            query_cache_ttl_secs: 300, // 5 minutes
            doc_cache_size: 100_000,
            doc_cache_ttl_secs: 3600, // 1 hour
            field_cache_size: 1_000,
            field_cache_ttl_secs: 3600, // 1 hour
        }
    }
}

#[derive(Debug)]
pub struct CacheStats {
    pub query_cache_size: u64,
    pub query_cache_hits: u64,
    pub doc_cache_size: u64,
    pub field_cache_size: u64,
}