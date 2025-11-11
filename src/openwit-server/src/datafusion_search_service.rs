//! DataFusion-based search service that queries parquet files directly

use std::sync::Arc;
use anyhow::Result;
use tracing::{info, error};
use datafusion::execution::context::SessionContext;
use datafusion::prelude::ParquetReadOptions;
use datafusion::arrow::record_batch::RecordBatch;
use serde_json::Value as JsonValue;
use std::collections::HashMap;

/// DataFusion search service that queries parquet files
#[allow(dead_code)]
pub struct DataFusionSearchService {
    ctx: SessionContext,
    data_path: String,
}

impl DataFusionSearchService {
    pub async fn new(data_path: String) -> Result<Self> {
        info!("Initializing DataFusion search service with path: {}", data_path);
        
        // Check if directory exists
        let path = std::path::Path::new(&data_path);
        if !path.exists() {
            error!("DataFusion: Data path does not exist: {}", data_path);
            return Err(anyhow::anyhow!("Data path does not exist: {}", data_path));
        }
        
        let ctx = SessionContext::new();
        
        // Register the parquet files as a table
        let parquet_path = format!("{}/*.parquet", data_path);
        info!("DataFusion: Attempting to register parquet files from: {}", parquet_path);
        
        match ctx.register_parquet("logs", &parquet_path, ParquetReadOptions::default()).await {
            Ok(_) => {
                info!("✅ DataFusion search service initialized with parquet files from: {}", parquet_path);
            }
            Err(e) => {
                error!("Failed to register parquet files: {}", e);
                error!("Error details: {:?}", e);
                return Err(anyhow::anyhow!("Failed to register parquet files: {}", e));
            }
        }
        
        Ok(Self {
            ctx,
            data_path,
        })
    }
    
    /// Execute a SQL query and return results as JSON
    pub async fn execute_sql(&self, sql: &str) -> Result<Vec<JsonValue>> {
        info!("Executing SQL query: {}", sql);
        
        // Execute the query
        let df = self.ctx.sql(sql).await?;
        let batches = df.collect().await?;
        
        // Convert results to JSON
        let mut results = Vec::new();
        for batch in batches {
            let json_rows = self.batch_to_json(&batch)?;
            results.extend(json_rows);
        }
        
        info!("Query returned {} rows", results.len());
        Ok(results)
    }
    
    /// Convert a RecordBatch to JSON values
    fn batch_to_json(&self, batch: &RecordBatch) -> Result<Vec<JsonValue>> {
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
    
    /// Get a cell value as JSON
    fn get_cell_value(&self, column: &dyn datafusion::arrow::array::Array, row_idx: usize) -> Result<JsonValue> {
        use datafusion::arrow::array::{
            StringArray, Int8Array, Int16Array, Int32Array, Int64Array,
            Float32Array, Float64Array, BooleanArray, LargeStringArray
        };
        use datafusion::arrow::datatypes::DataType;
        
        if column.is_null(row_idx) {
            return Ok(JsonValue::Null);
        }
        
        match column.data_type() {
            DataType::Boolean => {
                let array = column.as_any().downcast_ref::<BooleanArray>().unwrap();
                Ok(JsonValue::Bool(array.value(row_idx)))
            }
            DataType::Int8 => {
                let array = column.as_any().downcast_ref::<Int8Array>().unwrap();
                Ok(JsonValue::Number(array.value(row_idx).into()))
            }
            DataType::Int16 => {
                let array = column.as_any().downcast_ref::<Int16Array>().unwrap();
                Ok(JsonValue::Number(array.value(row_idx).into()))
            }
            DataType::Int32 => {
                let array = column.as_any().downcast_ref::<Int32Array>().unwrap();
                Ok(JsonValue::Number(array.value(row_idx).into()))
            }
            DataType::Int64 => {
                let array = column.as_any().downcast_ref::<Int64Array>().unwrap();
                Ok(JsonValue::Number(array.value(row_idx).into()))
            }
            DataType::Float32 => {
                let array = column.as_any().downcast_ref::<Float32Array>().unwrap();
                Ok(serde_json::Number::from_f64(array.value(row_idx) as f64)
                    .map(JsonValue::Number)
                    .unwrap_or(JsonValue::Null))
            }
            DataType::Float64 => {
                let array = column.as_any().downcast_ref::<Float64Array>().unwrap();
                Ok(serde_json::Number::from_f64(array.value(row_idx))
                    .map(JsonValue::Number)
                    .unwrap_or(JsonValue::Null))
            }
            DataType::Utf8 => {
                let array = column.as_any().downcast_ref::<StringArray>().unwrap();
                Ok(JsonValue::String(array.value(row_idx).to_string()))
            }
            DataType::LargeUtf8 => {
                let array = column.as_any().downcast_ref::<LargeStringArray>().unwrap();
                Ok(JsonValue::String(array.value(row_idx).to_string()))
            }
            _ => {
                // For other types, try to convert to string
                Ok(JsonValue::String(format!("{:?}", column)))
            }
        }
    }
    
    /// Get table schema information
    pub async fn get_table_info(&self) -> Result<Vec<String>> {
        let tables = self.ctx.catalog_names();
        let mut table_info = Vec::new();
        
        for catalog in tables {
            let schema_names = self.ctx.catalog(&catalog).unwrap().schema_names();
            for schema in schema_names {
                let table_names = self.ctx.catalog(&catalog).unwrap()
                    .schema(&schema).unwrap()
                    .table_names();
                for table in table_names {
                    table_info.push(format!("{}.{}.{}", catalog, schema, table));
                }
            }
        }
        
        Ok(table_info)
    }
}

// Global DataFusion search service instance
static DATAFUSION_SERVICE: tokio::sync::OnceCell<Arc<DataFusionSearchService>> = tokio::sync::OnceCell::const_new();

/// Initialize the DataFusion search service
pub async fn init_datafusion_service(data_path: String) -> Result<()> {
    let service = DataFusionSearchService::new(data_path).await?;
    DATAFUSION_SERVICE.set(Arc::new(service))
        .map_err(|_| anyhow::anyhow!("DataFusion service already initialized"))?;
    info!("✅ DataFusion search service initialized");
    Ok(())
}

/// Get the DataFusion search service instance
pub fn get_datafusion_service() -> Option<Arc<DataFusionSearchService>> {
    DATAFUSION_SERVICE.get().cloned()
}