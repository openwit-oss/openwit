//! Tantivy-based time-partitioned indexer implementation

use std::path::{PathBuf};
use std::sync::Arc;
use std::collections::HashMap;
use async_trait::async_trait;
use anyhow::{Result};
use chrono::{DateTime, Utc, Datelike, Timelike};
use tantivy::{
    schema::{Schema, SchemaBuilder, Field, Value, TextOptions, TextFieldIndexing, NumericOptions},
    Index, IndexWriter, doc,
    query::QueryParser,
    collector::TopDocs,
};
use tokio::sync::RwLock;
use tracing::{info};

use crate::{
    Index as IndexTrait, Document, Query, QueryResult, IndexStats, 
    FieldValue, Filter, IndexConfig,
};
use openwit_metastore::{MetaStore, PartitionMetadata, SegmentMetadata, TimeRange as MetaTimeRange};

/// Time-partitioned Tantivy indexer
pub struct TantivyIndexer {
    config: IndexConfig,
    schema: Schema,
    field_map: HashMap<String, Field>,
    partitions: Arc<RwLock<HashMap<String, PartitionIndex>>>,
    metastore: Arc<dyn MetaStore>,
    base_path: PathBuf,
}

/// Individual partition index
struct PartitionIndex {
    partition_id: String,
    index: Index,
    writer: Arc<RwLock<IndexWriter>>,
    doc_count: u64,
    bytes_indexed: u64,
}

impl TantivyIndexer {
    pub async fn new(
        config: IndexConfig,
        metastore: Arc<dyn MetaStore>,
    ) -> Result<Self> {
        // Build Tantivy schema
        let (schema, field_map) = Self::build_schema(&config)?;
        let base_path = PathBuf::from(&config.storage_path);
        
        // Create base directory
        tokio::fs::create_dir_all(&base_path).await?;
        
        Ok(Self {
            config,
            schema,
            field_map,
            partitions: Arc::new(RwLock::new(HashMap::new())),
            metastore,
            base_path,
        })
    }
    
    fn build_schema(_config: &IndexConfig) -> Result<(Schema, HashMap<String, Field>)> {
        let mut builder = SchemaBuilder::new();
        let mut field_map = HashMap::new();
        
        // Add timestamp field (always required)
        let timestamp_field = builder.add_u64_field("timestamp", NumericOptions::default().set_fast().set_stored());
        field_map.insert("timestamp".to_string(), timestamp_field);
        
        // Add trace/span ID fields
        let trace_id_field = builder.add_text_field("trace_id", TextOptions::default().set_stored().set_indexing_options(TextFieldIndexing::default().set_tokenizer("raw")));
        field_map.insert("trace_id".to_string(), trace_id_field);
        
        let span_id_field = builder.add_text_field("span_id", TextOptions::default().set_stored().set_indexing_options(TextFieldIndexing::default().set_tokenizer("raw")));
        field_map.insert("span_id".to_string(), span_id_field);
        
        // Add common observability fields
        let service_field = builder.add_text_field("service", TextOptions::default()
            .set_stored()
            .set_fast(None)
            .set_indexing_options(TextFieldIndexing::default().set_tokenizer("raw")));
        field_map.insert("service".to_string(), service_field);
        
        let level_field = builder.add_text_field("level", TextOptions::default()
            .set_stored()
            .set_fast(None)
            .set_indexing_options(TextFieldIndexing::default().set_tokenizer("raw")));
        field_map.insert("level".to_string(), level_field);
        
        let message_field = builder.add_text_field("message", TextOptions::default()
            .set_stored()
            .set_indexing_options(TextFieldIndexing::default()));
        field_map.insert("message".to_string(), message_field);
        
        // Add resource attributes as JSON
        let resource_field = builder.add_text_field("resource", TextOptions::default()
            .set_stored());
        field_map.insert("resource".to_string(), resource_field);
        
        // Add attributes as JSON
        let attributes_field = builder.add_text_field("attributes", TextOptions::default()
            .set_stored());
        field_map.insert("attributes".to_string(), attributes_field);
        
        Ok((builder.build(), field_map))
    }
    
    /// Get or create partition for a timestamp
    async fn get_partition(&self, timestamp: DateTime<Utc>) -> Result<String> {
        // Format partition based on config (e.g., "2024/01/15/14" for hourly)
        let partition_id = match self.config.index_type {
            crate::IndexType::TimeSeries => {
                format!("{:04}/{:02}/{:02}/{:02}", 
                    timestamp.year(),
                    timestamp.month(),
                    timestamp.day(),
                    timestamp.hour()
                )
            }
            _ => {
                format!("{:04}/{:02}/{:02}", 
                    timestamp.year(),
                    timestamp.month(),
                    timestamp.day()
                )
            }
        };
        
        let mut partitions = self.partitions.write().await;
        
        if !partitions.contains_key(&partition_id) {
            // Create new partition
            let partition_path = self.base_path.join(&partition_id);
            tokio::fs::create_dir_all(&partition_path).await?;
            
            let index = Index::create_in_dir(&partition_path, self.schema.clone())?;
            let writer = index.writer(100_000_000)?; // 100MB buffer
            
            let partition_index = PartitionIndex {
                partition_id: partition_id.clone(),
                index,
                writer: Arc::new(RwLock::new(writer)),
                doc_count: 0,
                bytes_indexed: 0,
            };
            
            partitions.insert(partition_id.clone(), partition_index);
            
            // Register in metastore
            let metadata = PartitionMetadata {
                partition_id: partition_id.clone(),
                time_range: MetaTimeRange {
                    start: timestamp.with_minute(0).unwrap().with_second(0).unwrap(),
                    end: timestamp.with_minute(59).unwrap().with_second(59).unwrap(),
                },
                segment_count: 0,
                row_count: 0,
                compressed_bytes: 0,
                uncompressed_bytes: 0,
                created_at: Utc::now(),
                updated_at: Utc::now(),
            };
            
            self.metastore.create_partition(metadata).await?;
            
            info!("Created new partition: {}", partition_id);
        }
        
        Ok(partition_id)
    }
    
    /// Convert our Document to Tantivy document
    fn to_tantivy_doc(&self, doc: &Document) -> Result<tantivy::TantivyDocument> {
        let mut tantivy_doc = doc!();
        
        // Add timestamp
        if let Some(field) = self.field_map.get("timestamp") {
            tantivy_doc.add_u64(*field, doc.timestamp.timestamp_millis() as u64);
        }
        
        // Add fields
        for (name, value) in &doc.fields {
            if let Some(field) = self.field_map.get(name) {
                match value {
                    FieldValue::String(s) => {
                        tantivy_doc.add_text(*field, s);
                    }
                    FieldValue::Integer(i) => {
                        tantivy_doc.add_u64(*field, *i as u64);
                    }
                    _ => {
                        // Convert to JSON for complex types
                        let json = serde_json::to_string(value)?;
                        tantivy_doc.add_text(*field, json);
                    }
                }
            }
        }
        
        Ok(tantivy_doc)
    }
}

#[allow(unused_mut)]
#[async_trait]
impl IndexTrait for TantivyIndexer {
    async fn index_batch(&mut self, docs: Vec<Document>) -> Result<()> {
        let start = std::time::Instant::now();
        let mut docs_by_partition: HashMap<String, Vec<Document>> = HashMap::new();
        
        // Group documents by partition
        for doc in docs {
            let partition_id = self.get_partition(doc.timestamp).await?;
            docs_by_partition.entry(partition_id).or_insert_with(Vec::new).push(doc);
        }
        
        // Index each partition's documents
        for (partition_id, partition_docs) in docs_by_partition {
            let doc_count = partition_docs.len();
            let total_bytes: usize = partition_docs.iter().map(|d| d.raw_size).sum();
            
            let partitions = self.partitions.read().await;
            if let Some(partition) = partitions.get(&partition_id) {
                let mut writer = partition.writer.write().await;
                
                for doc in partition_docs {
                    let tantivy_doc = self.to_tantivy_doc(&doc)?;
                    writer.add_document(tantivy_doc)?;
                }
                
                // Update stats
                let mut partitions_mut = self.partitions.write().await;
                if let Some(partition_mut) = partitions_mut.get_mut(&partition_id) {
                    partition_mut.doc_count += doc_count as u64;
                    partition_mut.bytes_indexed += total_bytes as u64;
                }
            }
        }
        
        let duration = start.elapsed();
        info!("Indexed batch in {:?}", duration);
        
        Ok(())
    }
    
    async fn query(&self, query: &Query) -> Result<QueryResult> {
        let start = std::time::Instant::now();
        let mut all_hits = Vec::new();
        
        // Determine partitions to search
        let partitions_to_search = if let Some(time_range) = &query.time_range {
            // Get partitions that overlap with time range
            self.metastore.list_partitions(MetaTimeRange {
                start: time_range.start,
                end: time_range.end,
            }).await?
        } else {
            // Search all partitions
            self.metastore.list_partitions(MetaTimeRange {
                start: DateTime::from_timestamp(0, 0).unwrap(),
                end: Utc::now(),
            }).await?
        };
        
        let partitions = self.partitions.read().await;
        
        for partition_meta in partitions_to_search {
            if let Some(partition) = partitions.get(&partition_meta.partition_id) {
                // Build Tantivy query
                let searcher = partition.index.reader()?.searcher();
                let query_parser = QueryParser::for_index(&partition.index, vec![
                    *self.field_map.get("message").unwrap(),
                    *self.field_map.get("service").unwrap(),
                    *self.field_map.get("level").unwrap(),
                ]);
                
                // Convert our query to Tantivy query string (simplified)
                let query_str = self.build_query_string(query)?;
                let tantivy_query = query_parser.parse_query(&query_str)?;
                
                let top_docs = TopDocs::with_limit(query.limit.unwrap_or(100));
                let results = searcher.search(&tantivy_query, &top_docs)?;
                
                for (_score, doc_address) in results {
                    let retrieved_doc = searcher.doc(doc_address)?;
                    // Convert back to our Document type
                    if let Some(doc) = self.from_tantivy_doc(&retrieved_doc)? {
                        all_hits.push(doc);
                    }
                }
            }
        }
        
        // Apply limit if needed
        if let Some(limit) = query.limit {
            all_hits.truncate(limit);
        }
        
        let query_time_ms = start.elapsed().as_millis() as u64;
        
        Ok(QueryResult {
            total_hits: all_hits.len(),
            hits: all_hits,
            aggregations: None, // TODO: Implement aggregations
            query_time_ms,
        })
    }
    
    async fn stats(&self) -> Result<IndexStats> {
        let partitions = self.partitions.read().await;
        let mut total_docs = 0u64;
        let mut total_bytes = 0u64;
        
        for partition in partitions.values() {
            total_docs += partition.doc_count;
            total_bytes += partition.bytes_indexed;
        }
        
        Ok(IndexStats {
            index_type: self.config.index_type.clone(),
            doc_count: total_docs,
            size_bytes: total_bytes,
            field_stats: HashMap::new(),
            last_updated: Utc::now(),
            query_performance: crate::QueryPerformance {
                avg_query_time_ms: 0.0,
                p50_query_time_ms: 0.0,
                p95_query_time_ms: 0.0,
                p99_query_time_ms: 0.0,
                queries_per_second: 0.0,
            },
        })
    }
    
    async fn compact(&mut self) -> Result<()> {
        let partitions = self.partitions.read().await;
        
        for partition in partitions.values() {
            let mut writer = partition.writer.write().await;
            writer.commit()?;
            // Note: wait_merging_threads() consumes the writer, so we can't call it here
            // Tantivy handles merging in the background automatically
        }
        
        Ok(())
    }
    
    async fn flush(&mut self) -> Result<()> {
        let partitions = self.partitions.read().await;
        
        for partition in partitions.values() {
            let mut writer = partition.writer.write().await;
            writer.commit()?;
            
            // Record segment in metastore
            let segment_id = format!("{}-{}", partition.partition_id, Utc::now().timestamp_millis());
            let segment_meta = SegmentMetadata {
                segment_id: segment_id.clone(),
                partition_id: partition.partition_id.clone(),
                file_path: format!("{}/segments", partition.partition_id),
                index_path: Some(format!("{}/tantivy", partition.partition_id)),
                row_count: partition.doc_count,
                compressed_bytes: partition.bytes_indexed,
                uncompressed_bytes: partition.bytes_indexed,
                min_timestamp: Utc::now(), // TODO: Track actual min/max
                max_timestamp: Utc::now(),
                storage_status: openwit_metastore::SegmentStatus::Stored,
                index_status: openwit_metastore::SegmentStatus::Stored,
                created_at: Utc::now(),
                updated_at: Utc::now(),
                schema_version: 1,
                is_cold: false,
                is_archived: false,
                storage_class: None,
                transitioned_at: None,
                archived_at: None,
                restore_status: None,
            };
            
            self.metastore.create_segment(segment_meta).await?;
        }
        
        info!("Flushed all partitions");
        Ok(())
    }
}

impl TantivyIndexer {
    /// Build Tantivy query string from our query structure
    fn build_query_string(&self, query: &Query) -> Result<String> {
        // Simplified conversion - in production, would properly convert all filter types
        let mut parts = Vec::new();
        
        for filter in &query.filters {
            match filter {
                Filter::Equals { field, value } => {
                    if let FieldValue::String(s) = value {
                        parts.push(format!("{}:{}", field, s));
                    }
                }
                Filter::Contains { field, value } => {
                    parts.push(format!("{}:{}", field, value));
                }
                _ => {
                    // TODO: Implement other filter types
                }
            }
        }
        
        Ok(if parts.is_empty() {
            "*".to_string()
        } else {
            parts.join(" AND ")
        })
    }
    
    /// Convert Tantivy document back to our Document type
    fn from_tantivy_doc(&self, tantivy_doc: &tantivy::TantivyDocument) -> Result<Option<Document>> {
        let mut fields = HashMap::new();
        
        // Extract timestamp
        let timestamp = if let Some(field) = self.field_map.get("timestamp") {
            let field_values = tantivy_doc.field_values();
            let mut timestamp_val = None;
            for field_value in field_values {
                if field_value.field() == *field {
                    if let Some(ts) = field_value.value().as_u64() {
                        timestamp_val = Some(DateTime::from_timestamp_millis(ts as i64).unwrap_or(Utc::now()));
                        break;
                    }
                }
            }
            if let Some(ts) = timestamp_val {
                ts
            } else {
                return Ok(None);
            }
        } else {
            return Ok(None);
        };
        
        // Extract other fields
        let field_values = tantivy_doc.field_values();
        for field_value in field_values {
            let doc_field = field_value.field();
            let value = field_value.value();
            
            // Find the field name
            for (name, mapped_field) in &self.field_map {
                if name == "timestamp" {
                    continue;
                }
                
                if doc_field == *mapped_field {
                    if let Some(text) = value.as_str() {
                        fields.insert(name.clone(), FieldValue::String(text.to_string()));
                    } else if let Some(num) = value.as_u64() {
                        fields.insert(name.clone(), FieldValue::Integer(num as i64));
                    } else if let Some(num) = value.as_i64() {
                        fields.insert(name.clone(), FieldValue::Integer(num));
                    } else if let Some(num) = value.as_f64() {
                        fields.insert(name.clone(), FieldValue::Number(num));
                    }
                    break;
                }
            }
        }
        
        Ok(Some(Document {
            id: format!("{}-{}", timestamp.timestamp_millis(), rand::random::<u32>()),
            timestamp,
            fields,
            raw_size: 0, // Not tracked in index
        }))
    }
}