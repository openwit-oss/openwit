//! Integrated search service that combines Tantivy indexing with DataFusion queries

use crate::enhanced_sql_rewriter::EnhancedSqlRewriter;
use anyhow::Result;
use chrono::{DateTime, Utc};
use datafusion::arrow::array::Array;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::ParquetReadOptions;
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info};

use openwit_indexer::{tantivy_indexer::TantivyIndexer, CompressionType, IndexConfig, IndexType};
use openwit_metastore::MetaStore;

/// Integrated search service that uses Tantivy for index lookups and DataFusion for data queries
#[allow(dead_code)]
pub struct IntegratedSearchService {
    indexer: Arc<RwLock<TantivyIndexer>>,
    metastore: Arc<dyn MetaStore>,
    data_path: String,
    ctx: SessionContext,
    sql_rewriter: EnhancedSqlRewriter,
}

impl IntegratedSearchService {
    pub async fn new(
        index_path: String,
        data_path: String,
        metastore: Arc<dyn MetaStore>,
    ) -> Result<Self> {
        info!("Initializing integrated search service");
        info!("  Index path: {}", index_path);
        info!("  Data path: {}", data_path);

        // Initialize Tantivy indexer
        let config = IndexConfig {
            index_type: IndexType::TimeSeries,
            fields: vec![],
            storage_path: index_path,
            cache_size_mb: 512,
            compression: CompressionType::Snappy,
            compaction_threshold: 0.8,
            bloom_filter_fpp: 0.01,
        };

        let indexer = TantivyIndexer::new(config, metastore.clone()).await?;

        // Initialize DataFusion context
        let ctx = SessionContext::new();

        Ok(Self {
            indexer: Arc::new(RwLock::new(indexer)),
            metastore,
            data_path,
            ctx,
            sql_rewriter: EnhancedSqlRewriter::new(),
        })
    }

    /// Execute a search query using the integrated approach
    pub async fn search(
        &self,
        sql_query: &str,
        time_range: Option<(DateTime<Utc>, DateTime<Utc>)>,
        _limit: Option<usize>,
    ) -> Result<Vec<JsonValue>> {
        info!("Executing integrated search query: {}", sql_query);

        // Step 1: Rewrite SQL query for DataFusion compatibility
        let rewritten_query = self.sql_rewriter.rewrite(sql_query)?;
        if rewritten_query != sql_query {
            info!(
                "SQL query rewritten for compatibility: {} -> {}",
                sql_query, rewritten_query
            );
        }

        // Step 2: Extract time range from SQL if not provided
        let time_range = time_range.or_else(|| self.extract_time_range_from_sql(&rewritten_query));

        // Step 3: Use Tantivy to find relevant splits/partitions
        let relevant_files = self.find_relevant_parquet_files(time_range).await?;

        if relevant_files.is_empty() {
            info!("No relevant files found for the time range");
            return Ok(vec![]);
        }

        info!("Found {} relevant parquet files", relevant_files.len());

        // Step 4: Register only the relevant parquet files with DataFusion
        self.register_parquet_files(&relevant_files).await?;

        // Step 5: Execute the SQL query with DataFusion
        let results = self.execute_datafusion_query(&rewritten_query).await?;

        Ok(results)
    }

    /// Find relevant parquet files based on time range using Tantivy index
    async fn find_relevant_parquet_files(
        &self,
        _time_range: Option<(DateTime<Utc>, DateTime<Utc>)>,
    ) -> Result<Vec<String>> {
        // For now, just find all parquet files in the directory
        // TODO: Integrate with metastore for time-based filtering
        let mut relevant_files = Vec::new();

        info!("Checking directory for parquet files: {}", self.data_path);
        let path = std::path::Path::new(&self.data_path);
        if path.exists() && path.is_dir() {
            if let Ok(entries) = std::fs::read_dir(path) {
                for entry in entries.flatten() {
                    if let Some(name) = entry.file_name().to_str() {
                        if name.ends_with(".parquet") {
                            relevant_files.push(entry.path().to_string_lossy().to_string());
                        }
                    }
                }
            }
        }

        info!("Found {} parquet files", relevant_files.len());
        Ok(relevant_files)
    }

    /// Register parquet files with DataFusion
    async fn register_parquet_files(&self, files: &[String]) -> Result<()> {
        // Drop existing table if it exists
        let _ = self.ctx.deregister_table("logs");

        if files.is_empty() {
            return Err(anyhow::anyhow!("No parquet files to register"));
        }

        // For multiple files, we can use a glob pattern
        if files.len() > 1 {
            // Register using glob pattern - assuming all files are in same directory
            let glob_pattern = format!("{}/*.parquet", self.data_path);
            self.ctx
                .register_parquet("logs", &glob_pattern, ParquetReadOptions::default())
                .await?;
            debug!(
                "Registered parquet files using glob pattern: {}",
                glob_pattern
            );
        } else {
            // Register single file
            self.ctx
                .register_parquet("logs", &files[0], ParquetReadOptions::default())
                .await?;
            debug!("Registered single parquet file: {}", files[0]);
        }

        Ok(())
    }

    /// Execute DataFusion query and convert results to JSON
    async fn execute_datafusion_query(&self, sql: &str) -> Result<Vec<JsonValue>> {
        let df = self.ctx.sql(sql).await?;
        let batches = df.collect().await?;

        let mut results = Vec::new();
        for batch in batches {
            let json_rows = self.batch_to_json(&batch)?;
            results.extend(json_rows);
        }

        Ok(results)
    }

    /// Convert RecordBatch to JSON
    fn batch_to_json(
        &self,
        batch: &datafusion::arrow::record_batch::RecordBatch,
    ) -> Result<Vec<JsonValue>> {
        let mut rows = Vec::new();
        let schema = batch.schema();
        let columns = batch.columns();

        for row_idx in 0..batch.num_rows() {
            let mut row_map = HashMap::new();

            for (col_idx, field) in schema.fields().iter().enumerate() {
                let column = &columns[col_idx];
                let value = self.get_cell_value(column, row_idx)?;
                row_map.insert(field.name().clone(), value);
            }

            rows.push(serde_json::to_value(row_map)?);
        }

        Ok(rows)
    }

    /// Extract cell value as JSON
    fn get_cell_value(
        &self,
        column: &dyn datafusion::arrow::array::Array,
        row_idx: usize,
    ) -> Result<JsonValue> {
        use datafusion::arrow::array::{
            BinaryArray, BinaryViewArray, BooleanArray, Float64Array, Int64Array, LargeStringArray,
            StringArray, StringViewArray, UInt64Array,
        };
        use datafusion::arrow::datatypes::DataType;

        if column.is_null(row_idx) {
            return Ok(JsonValue::Null);
        }

        match column.data_type() {
            DataType::Utf8 => {
                if let Some(array) = column.as_any().downcast_ref::<StringArray>() {
                    if array.is_null(row_idx) {
                        Ok(JsonValue::Null)
                    } else {
                        Ok(JsonValue::String(array.value(row_idx).to_string()))
                    }
                } else {
                    Ok(JsonValue::Null)
                }
            }
            DataType::Utf8View => {
                if let Some(array) = column.as_any().downcast_ref::<StringViewArray>() {
                    if array.is_null(row_idx) {
                        Ok(JsonValue::Null)
                    } else {
                        Ok(JsonValue::String(array.value(row_idx).to_string()))
                    }
                } else {
                    Ok(JsonValue::Null)
                }
            }
            DataType::Int64 => {
                if let Some(array) = column.as_any().downcast_ref::<Int64Array>() {
                    if array.is_null(row_idx) {
                        Ok(JsonValue::Null)
                    } else {
                        Ok(JsonValue::Number(array.value(row_idx).into()))
                    }
                } else {
                    Ok(JsonValue::Null)
                }
            }
            DataType::UInt64 => {
                if let Some(array) = column.as_any().downcast_ref::<UInt64Array>() {
                    if array.is_null(row_idx) {
                        Ok(JsonValue::Null)
                    } else {
                        Ok(JsonValue::Number(array.value(row_idx).into()))
                    }
                } else {
                    Ok(JsonValue::Null)
                }
            }
            DataType::Float64 => {
                if let Some(array) = column.as_any().downcast_ref::<Float64Array>() {
                    if array.is_null(row_idx) {
                        Ok(JsonValue::Null)
                    } else {
                        Ok(serde_json::Number::from_f64(array.value(row_idx))
                            .map(JsonValue::Number)
                            .unwrap_or(JsonValue::Null))
                    }
                } else {
                    Ok(JsonValue::Null)
                }
            }
            DataType::Boolean => {
                if let Some(array) = column.as_any().downcast_ref::<BooleanArray>() {
                    if array.is_null(row_idx) {
                        Ok(JsonValue::Null)
                    } else {
                        Ok(JsonValue::Bool(array.value(row_idx)))
                    }
                } else {
                    Ok(JsonValue::Null)
                }
            }
            DataType::Timestamp(unit, _) => {
                // Handle different timestamp units
                match unit {
                    datafusion::arrow::datatypes::TimeUnit::Nanosecond => {
                        if let Some(array) = column
                            .as_any()
                            .downcast_ref::<datafusion::arrow::array::TimestampNanosecondArray>(
                        ) {
                            let timestamp = array.value(row_idx);
                            Ok(JsonValue::Number(timestamp.into()))
                        } else {
                            Ok(JsonValue::Null)
                        }
                    }
                    datafusion::arrow::datatypes::TimeUnit::Microsecond => {
                        if let Some(array) = column
                            .as_any()
                            .downcast_ref::<datafusion::arrow::array::TimestampMicrosecondArray>(
                        ) {
                            let timestamp = array.value(row_idx);
                            Ok(JsonValue::Number(timestamp.into()))
                        } else {
                            Ok(JsonValue::Null)
                        }
                    }
                    datafusion::arrow::datatypes::TimeUnit::Millisecond => {
                        if let Some(array) = column
                            .as_any()
                            .downcast_ref::<datafusion::arrow::array::TimestampMillisecondArray>(
                        ) {
                            let timestamp = array.value(row_idx);
                            Ok(JsonValue::Number(timestamp.into()))
                        } else {
                            Ok(JsonValue::Null)
                        }
                    }
                    datafusion::arrow::datatypes::TimeUnit::Second => {
                        if let Some(array) = column
                            .as_any()
                            .downcast_ref::<datafusion::arrow::array::TimestampSecondArray>(
                        ) {
                            let timestamp = array.value(row_idx);
                            Ok(JsonValue::Number(timestamp.into()))
                        } else {
                            Ok(JsonValue::Null)
                        }
                    }
                }
            }
            DataType::Binary => {
                if let Some(array) = column.as_any().downcast_ref::<BinaryArray>() {
                    if array.is_null(row_idx) {
                        Ok(JsonValue::Null)
                    } else {
                        // Convert binary to base64 string
                        let bytes = array.value(row_idx);
                        use base64::{engine::general_purpose, Engine as _};
                        Ok(JsonValue::String(general_purpose::STANDARD.encode(bytes)))
                    }
                } else {
                    Ok(JsonValue::Null)
                }
            }
            DataType::BinaryView => {
                if let Some(array) = column.as_any().downcast_ref::<BinaryViewArray>() {
                    if array.is_null(row_idx) {
                        Ok(JsonValue::Null)
                    } else {
                        // Convert binary to base64 string
                        let bytes = array.value(row_idx);
                        use base64::{engine::general_purpose, Engine as _};
                        Ok(JsonValue::String(general_purpose::STANDARD.encode(bytes)))
                    }
                } else {
                    Ok(JsonValue::Null)
                }
            }
            DataType::LargeUtf8 => {
                if let Some(array) = column.as_any().downcast_ref::<LargeStringArray>() {
                    if array.is_null(row_idx) {
                        Ok(JsonValue::Null)
                    } else {
                        Ok(JsonValue::String(array.value(row_idx).to_string()))
                    }
                } else {
                    Ok(JsonValue::Null)
                }
            }
            _ => {
                // For unknown types, just return the type name
                Ok(JsonValue::String(format!("<{:?}>", column.data_type())))
            }
        }
    }

    /// Extract time range from SQL query (simple implementation)
    fn extract_time_range_from_sql(&self, _sql: &str) -> Option<(DateTime<Utc>, DateTime<Utc>)> {
        // This is a simplified implementation
        // In production, you'd want to properly parse the SQL
        // For now, return None to query all files
        None
    }

    /// Get available tables and their schemas
    pub async fn get_schema_info(&self) -> Result<HashMap<String, Vec<String>>> {
        let mut schema_info = HashMap::new();

        // Register a sample file to get schema
        let files = self.find_relevant_parquet_files(None).await?;
        if !files.is_empty() {
            self.register_parquet_files(&files[..1]).await?;

            // Get schema from the table
            if let Ok(df) = self.ctx.sql("SELECT * FROM logs LIMIT 0").await {
                let schema = df.schema();
                let fields: Vec<String> = schema
                    .fields()
                    .iter()
                    .map(|f| format!("{}: {:?}", f.name(), f.data_type()))
                    .collect();
                schema_info.insert("logs".to_string(), fields);
            }
        }

        Ok(schema_info)
    }
}

// Global integrated search service instance
static INTEGRATED_SERVICE: tokio::sync::OnceCell<Arc<IntegratedSearchService>> =
    tokio::sync::OnceCell::const_new();

/// Initialize the integrated search service
pub async fn init_integrated_service(
    index_path: String,
    data_path: String,
    metastore: Arc<dyn MetaStore>,
) -> Result<()> {
    let service = IntegratedSearchService::new(index_path, data_path, metastore).await?;
    INTEGRATED_SERVICE
        .set(Arc::new(service))
        .map_err(|_| anyhow::anyhow!("Integrated search service already initialized"))?;
    info!("✅ Integrated search service initialized");
    Ok(())
}

/// Get the integrated search service instance
pub fn get_integrated_service() -> Option<Arc<IntegratedSearchService>> {
    INTEGRATED_SERVICE.get().cloned()
}
