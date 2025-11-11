use anyhow::{Result, Context};
use datafusion::prelude::*;
use std::sync::Arc;
use tracing::{info, warn, error, debug};
use tokio::sync::RwLock;
use std::collections::HashMap;
use serde_json::Value as JsonValue;

/// Simple DataFusion query executor for customer-specific parquet files
pub struct SimpleQueryExecutor {
    data_dir: String,
    session_cache: Arc<RwLock<HashMap<String, Arc<SessionContext>>>>,
}

impl SimpleQueryExecutor {
    pub fn new(data_dir: String) -> Result<Self> {
        info!("Creating SimpleQueryExecutor with data_dir: {}", data_dir);
        Ok(Self {
            data_dir,
            session_cache: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// Execute a SQL query for a specific client
    pub async fn execute_query(
        &self,
        sql: String,
        client_id: String,
        limit: Option<usize>,
    ) -> Result<QueryResponse> {
        info!("Executing query for client: {}, SQL: {}", client_id, sql);

        // Get or create session for this client
        let session = self.get_or_create_session(&client_id).await?;

        // Apply limit if specified
        let final_sql = if let Some(limit_val) = limit {
            if !sql.to_lowercase().contains("limit") {
                format!("{} LIMIT {}", sql, limit_val)
            } else {
                sql.clone()
            }
        } else {
            sql.clone()
        };

        debug!("Final SQL after limit: {}", final_sql);

        // Execute the query
        let df = match session.sql(&final_sql).await {
            Ok(df) => df,
            Err(e) => {
                error!("Failed to execute SQL '{}': {}", final_sql, e);

                // Check if it's a table not found error
                let error_str = e.to_string();
                if error_str.contains("table") || error_str.contains("Table") {
                    // Provide helpful error message
                    return Err(anyhow::anyhow!(
                        "Table not found. Error: {}. Please ensure parquet files exist in {}/{}/(traces|metrics|logs)/ and the table name in your SQL is one of: traces, metrics, logs",
                        error_str, self.data_dir, client_id
                    ));
                }

                return Err(anyhow::anyhow!("Failed to parse SQL: {}", e));
            }
        };

        // Collect results
        let batches = df.collect().await
            .context("Failed to execute query")?;

        // Convert to JSON
        let mut results = Vec::new();
        let mut total_rows = 0;

        for batch in &batches {
            total_rows += batch.num_rows();

            // Convert each row to JSON
            for row_idx in 0..batch.num_rows() {
                let mut row_json = serde_json::Map::new();

                for (col_idx, field) in batch.schema().fields().iter().enumerate() {
                    let col_name = field.name();
                    let array = batch.column(col_idx);

                    // Convert arrow value to JSON
                    let value = self.arrow_value_to_json(array, row_idx)?;
                    row_json.insert(col_name.clone(), value);
                }

                results.push(JsonValue::Object(row_json));
            }
        }

        info!("Query returned {} rows", total_rows);

        Ok(QueryResponse {
            results,
            row_count: total_rows,
        })
    }

    /// Get or create a DataFusion session for a client
    async fn get_or_create_session(&self, client_id: &str) -> Result<Arc<SessionContext>> {
        let mut cache = self.session_cache.write().await;

        if let Some(session) = cache.get(client_id) {
            debug!("Using cached session for client: {}", client_id);
            return Ok(Arc::clone(session));
        }

        info!("Creating new session for client: {}", client_id);
        let session = self.create_session_for_client(client_id).await?;
        cache.insert(client_id.to_string(), Arc::clone(&session));

        Ok(session)
    }

    /// Create a new DataFusion session and register parquet files for a client
    async fn create_session_for_client(&self, client_id: &str) -> Result<Arc<SessionContext>> {
        info!("Creating DataFusion session for client: {}", client_id);
        let ctx = SessionContext::new();

        // Find parquet files for this client
        let client_dir = format!("{}/{}", self.data_dir, client_id);
        info!("Looking for client data in directory: {}", client_dir);

        // Check if client directory exists
        if !std::path::Path::new(&client_dir).exists() {
            warn!("Client directory does not exist: {}", client_dir);
            warn!("Creating empty session - queries will fail");
            // Return empty session - queries will return no results
            return Ok(Arc::new(ctx));
        }

        // Register parquet files
        let mut traces_registered = false;
        let mut metrics_registered = false;
        let mut logs_registered = false;
        let mut parquet_files = Vec::new();

        // First, look for parquet files directly in the client directory
        info!("Scanning for parquet files in client directory: {}", client_dir);

        if let Ok(entries) = std::fs::read_dir(&client_dir) {
            for entry in entries {
                if let Ok(entry) = entry {
                    let path = entry.path();
                    if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("parquet") {
                        parquet_files.push(path);
                    }
                }
            }
        }

        // If we found parquet files in the root directory, register them as "traces" table
        if !parquet_files.is_empty() {
            info!("Found {} parquet files in client root directory", parquet_files.len());

            // Register all parquet files as a single "traces" table
            // DataFusion can handle multiple files for the same table
            for (idx, path) in parquet_files.iter().enumerate() {
                info!("Registering parquet file #{}: {}", idx + 1, path.display());

                if idx == 0 {
                    // Register first file
                    match ctx.register_parquet(
                        "traces",
                        path.to_str().unwrap(),
                        ParquetReadOptions::default(),
                    ).await {
                        Ok(_) => {
                            info!("Successfully registered first parquet file as 'traces' table");
                            traces_registered = true;
                        }
                        Err(e) => {
                            error!("Failed to register parquet file: {}", e);
                            return Err(anyhow::anyhow!("Failed to register parquet file: {}", e));
                        }
                    }
                } else {
                    // For subsequent files, we need to append to the existing table
                    // For now, we'll just log that we're skipping them
                    // TODO: Implement multi-file table support
                    info!("Note: Only first parquet file is registered. Multi-file support pending.");
                    break;
                }
            }
        }

        // Also check subdirectories if nothing was registered from root
        if !traces_registered && !metrics_registered && !logs_registered {
            info!("No parquet files found in root directory, checking subdirectories...");
            // Look for telemetry type subdirectories
            for telemetry_type in &["traces", "metrics", "logs"] {
                let type_dir = format!("{}/{}", client_dir, telemetry_type);
                if std::path::Path::new(&type_dir).exists() {
                    info!("Found subdirectory: {}", type_dir);

                    // Find all parquet files in the directory
                    let mut parquet_files = Vec::new();
                    if let Ok(entries) = std::fs::read_dir(&type_dir) {
                        for entry in entries {
                            if let Ok(entry) = entry {
                                let path = entry.path();
                                if path.extension().and_then(|s| s.to_str()) == Some("parquet") {
                                    parquet_files.push(path.to_string_lossy().to_string());
                                    info!("Found parquet file: {}", path.display());
                                }
                            }
                        }
                    }

                    // Register all found parquet files
                    if !parquet_files.is_empty() {
                        info!("Registering {} parquet files for table '{}'", parquet_files.len(), telemetry_type);

                        // Register each file individually
                        for file_path in &parquet_files {
                            match ctx.register_parquet(
                                *telemetry_type,
                                file_path.as_str(),
                                ParquetReadOptions::default(),
                            ).await {
                                Ok(_) => info!("Successfully registered: {}", file_path),
                                Err(e) => warn!("Failed to register {}: {}", file_path, e),
                            }
                            break; // Only register the first file for now (DataFusion limitation)
                        }
                    } else {
                        warn!("No parquet files found in {}", type_dir);
                    }
                }
            }
        }

        // Log summary of what was registered
        let mut registered_tables = Vec::new();
        if traces_registered {
            registered_tables.push("traces");
        }
        if metrics_registered {
            registered_tables.push("metrics");
        }
        if logs_registered {
            registered_tables.push("logs");
        }

        if registered_tables.is_empty() {
            warn!("No tables registered for client {}. No parquet files found in expected locations.", client_id);
            warn!("Expected directory structure: {}/{{traces,metrics,logs}}/*.parquet", client_dir);
        } else {
            info!("Successfully registered tables for client {}: {:?}", client_id, registered_tables);
        }

        Ok(Arc::new(ctx))
    }

    /// Convert Arrow array value to JSON
    fn arrow_value_to_json(&self, array: &dyn arrow::array::Array, row_idx: usize) -> Result<JsonValue> {
        use arrow::array::*;
        use arrow::datatypes::DataType;

        if array.is_null(row_idx) {
            return Ok(JsonValue::Null);
        }

        match array.data_type() {
            DataType::Utf8 => {
                let array = array.as_any().downcast_ref::<StringArray>().unwrap();
                Ok(JsonValue::String(array.value(row_idx).to_string()))
            }
            DataType::Int64 => {
                let array = array.as_any().downcast_ref::<Int64Array>().unwrap();
                Ok(JsonValue::Number(array.value(row_idx).into()))
            }
            DataType::Float64 => {
                let array = array.as_any().downcast_ref::<Float64Array>().unwrap();
                let val = array.value(row_idx);
                Ok(serde_json::Number::from_f64(val)
                    .map(JsonValue::Number)
                    .unwrap_or(JsonValue::Null))
            }
            DataType::Boolean => {
                let array = array.as_any().downcast_ref::<BooleanArray>().unwrap();
                Ok(JsonValue::Bool(array.value(row_idx)))
            }
            DataType::Timestamp(_, _) => {
                // Convert timestamp to string representation
                let display = arrow::util::display::array_value_to_string(array, row_idx)?;
                Ok(JsonValue::String(display))
            }
            _ => {
                // For other types, use Arrow's string representation
                let display = arrow::util::display::array_value_to_string(array, row_idx)?;
                Ok(JsonValue::String(display))
            }
        }
    }

    /// Clear the session cache for a specific client or all clients
    pub async fn clear_cache(&self, client_id: Option<&str>) {
        let mut cache = self.session_cache.write().await;
        if let Some(id) = client_id {
            cache.remove(id);
            info!("Cleared session cache for client: {}", id);
        } else {
            cache.clear();
            info!("Cleared all session caches");
        }
    }
}

/// Response from query execution
#[derive(Debug)]
pub struct QueryResponse {
    pub results: Vec<JsonValue>,
    pub row_count: usize,
}