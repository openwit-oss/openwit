//! Search service that queries the Tantivy index

use std::sync::Arc;
use tokio::sync::RwLock;
use anyhow::Result;
use chrono::{DateTime, Utc};
use tracing::{info};

use openwit_indexer::{
    tantivy_indexer::TantivyIndexer,
    Index as IndexTrait,
    Query as IndexQuery,
    Filter,
    TimeRange,
    FieldValue,
    QueryResult,
    IndexConfig,
    IndexType,
    CompressionType,
};
use openwit_metastore::MetaStore;

/// Search service that provides query functionality
pub struct SearchService {
    indexer: Arc<RwLock<TantivyIndexer>>,
}

impl SearchService {
    pub async fn new(
        index_path: String,
        metastore: Arc<dyn MetaStore>,
    ) -> Result<Self> {
        // Create index configuration (should match indexing service)
        let config = IndexConfig {
            index_type: IndexType::TimeSeries,
            fields: vec![], // Fields are defined in TantivyIndexer
            storage_path: index_path,
            cache_size_mb: 512,
            compression: CompressionType::Snappy,
            compaction_threshold: 0.8,
            bloom_filter_fpp: 0.01,
        };
        
        let indexer = TantivyIndexer::new(config, metastore).await?;
        
        Ok(Self {
            indexer: Arc::new(RwLock::new(indexer)),
        })
    }
    
    /// Execute a search query
    pub async fn search(&self, 
        _sql_query: &str,
        tantivy_query: &str,
        time_range: Option<(DateTime<Utc>, DateTime<Utc>)>,
        limit: Option<usize>,
    ) -> Result<QueryResult> {
        // Build index query from Tantivy query string
        let filters = self.parse_tantivy_query(tantivy_query)?;
        
        let query = IndexQuery {
            filters,
            time_range: time_range.map(|(start, end)| TimeRange { start, end }),
            limit,
            offset: None,
            sort: None,
            aggregations: None,
        };
        
        // Execute query
        let indexer = self.indexer.read().await;
        indexer.query(&query).await
    }
    
    /// Parse Tantivy query string into filters
    fn parse_tantivy_query(&self, query: &str) -> Result<Vec<Filter>> {
        let mut filters = Vec::new();
        
        // Simple parser for basic queries like "service:auth AND level:ERROR"
        let parts: Vec<&str> = query.split(" AND ").collect();
        
        for part in parts {
            if let Some((field, value)) = part.split_once(':') {
                let field = field.trim();
                let value = value.trim().trim_matches('"');
                
                filters.push(Filter::Equals {
                    field: field.to_string(),
                    value: FieldValue::String(value.to_string()),
                });
            } else if part.contains(" OR ") {
                // Handle OR conditions
                let or_parts: Vec<&str> = part.split(" OR ").collect();
                let mut or_filters = Vec::new();
                
                for or_part in or_parts {
                    if let Some((field, value)) = or_part.split_once(':') {
                        let field = field.trim();
                        let value = value.trim().trim_matches('"');
                        
                        or_filters.push(Filter::Equals {
                            field: field.to_string(),
                            value: FieldValue::String(value.to_string()),
                        });
                    }
                }
                
                if !or_filters.is_empty() {
                    filters.push(Filter::Or(or_filters));
                }
            }
        }
        
        // If no filters were parsed, search all with a wildcard
        if filters.is_empty() && query != "*" {
            // Try to use it as a message contains filter
            filters.push(Filter::Contains {
                field: "message".to_string(),
                value: query.to_string(),
            });
        }
        
        Ok(filters)
    }
}

// Global search service instance
static SEARCH_SERVICE: tokio::sync::OnceCell<Arc<SearchService>> = tokio::sync::OnceCell::const_new();

/// Initialize the search service
pub async fn init_search_service(
    index_path: String,
    metastore: Arc<dyn MetaStore>,
) -> Result<()> {
    let service = SearchService::new(index_path, metastore).await?;
    SEARCH_SERVICE.set(Arc::new(service))
        .map_err(|_| anyhow::anyhow!("Search service already initialized"))?;
    info!("✅ Search service initialized");
    Ok(())
}

/// Get the search service instance
pub fn get_search_service() -> Option<Arc<SearchService>> {
    SEARCH_SERVICE.get().cloned()
}