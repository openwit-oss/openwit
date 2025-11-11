use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use arrow::datatypes::SchemaRef;
use parquet::file::statistics::Statistics;
use anyhow::Result;

#[derive(Clone)]
pub struct FileMetadata {
    pub path: String,
    pub schema: SchemaRef,
    pub row_count: i64,
    pub size_bytes: i64,
    pub min_timestamp: Option<i64>,
    pub max_timestamp: Option<i64>,
    pub cached_at: Instant,
}

#[derive(Clone)]
pub struct ClientMetadata {
    pub files: Vec<FileMetadata>,
    pub total_rows: i64,
    pub total_bytes: i64,
    pub cached_at: Instant,
}

pub struct MetadataCache {
    client_metadata: Arc<RwLock<HashMap<String, ClientMetadata>>>,
    file_schemas: Arc<RwLock<HashMap<String, (SchemaRef, Instant)>>>,
    cache_ttl: Duration,
}

impl MetadataCache {
    pub fn new(cache_ttl: Duration) -> Self {
        Self {
            client_metadata: Arc::new(RwLock::new(HashMap::new())),
            file_schemas: Arc::new(RwLock::new(HashMap::new())),
            cache_ttl,
        }
    }
    
    pub async fn get_client_metadata(&self, client: &str) -> Option<ClientMetadata> {
        let cache = self.client_metadata.read().await;
        cache.get(client).and_then(|metadata| {
            if metadata.cached_at.elapsed() < self.cache_ttl {
                Some(metadata.clone())
            } else {
                None
            }
        })
    }
    
    pub async fn set_client_metadata(&self, client: String, metadata: ClientMetadata) {
        let mut cache = self.client_metadata.write().await;
        cache.insert(client, metadata);
    }
    
    pub async fn get_file_schema(&self, file_path: &str) -> Option<SchemaRef> {
        let cache = self.file_schemas.read().await;
        cache.get(file_path).and_then(|(schema, cached_at)| {
            if cached_at.elapsed() < self.cache_ttl {
                Some(schema.clone())
            } else {
                None
            }
        })
    }
    
    pub async fn set_file_schema(&self, file_path: String, schema: SchemaRef) {
        let mut cache = self.file_schemas.write().await;
        cache.insert(file_path, (schema, Instant::now()));
    }
    
    pub async fn invalidate_client(&self, client: &str) {
        let mut cache = self.client_metadata.write().await;
        cache.remove(client);
    }
    
    pub async fn clear_expired(&self) {
        let now = Instant::now();
        
        // Clear expired client metadata
        {
            let mut cache = self.client_metadata.write().await;
            cache.retain(|_, metadata| now.duration_since(metadata.cached_at) < self.cache_ttl);
        }
        
        // Clear expired file schemas
        {
            let mut cache = self.file_schemas.write().await;
            cache.retain(|_, (_, cached_at)| now.duration_since(*cached_at) < self.cache_ttl);
        }
    }
    
    pub async fn warm_cache(&self, client: &str, files: Vec<FileMetadata>) {
        let total_rows: i64 = files.iter().map(|f| f.row_count).sum();
        let total_bytes: i64 = files.iter().map(|f| f.size_bytes).sum();
        
        let metadata = ClientMetadata {
            files,
            total_rows,
            total_bytes,
            cached_at: Instant::now(),
        };
        
        self.set_client_metadata(client.to_string(), metadata).await;
    }
}