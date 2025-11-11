use anyhow::{Result, Context};
use datafusion::prelude::*;
use datafusion::datasource::listing::ListingOptions;
use object_store::azure::MicrosoftAzureBuilder;
use std::sync::Arc;
use tracing::info;
use url::Url;

/// Azure query configuration
pub struct AzureQueryConfig {
    pub account_name: String,
    pub account_key: String,
    pub container_name: String,
    pub prefix: Option<String>,
}

/// Query executor for Azure Blob Storage
pub struct AzureQueryExecutor {
    session_ctx: SessionContext,
    container_url: String,
    prefix: Option<String>,
}

impl AzureQueryExecutor {
    /// Create a new Azure query executor
    pub async fn new(config: AzureQueryConfig) -> Result<Self> {
        info!("[AZURE QUERY] Initializing Azure query executor");
        info!("[AZURE QUERY] Account: {}", config.account_name);
        info!("[AZURE QUERY] Container: {}", config.container_name);
        
        // Create session context
        let session_ctx = SessionContext::new();
        
        // Create Azure object store
        let azure_store = MicrosoftAzureBuilder::new()
            .with_account(&config.account_name)
            .with_access_key(&config.account_key)
            .with_container_name(&config.container_name)
            .build()?;
        
        // Register object store with URL scheme
        let container_url = format!("az://{}", config.container_name);
        let url = Url::parse(&container_url)?;
        session_ctx.runtime_env()
            .register_object_store(&url, Arc::new(azure_store));
        
        info!("[AZURE QUERY] Successfully connected to Azure Blob Storage");
        
        Ok(Self {
            session_ctx,
            container_url,
            prefix: config.prefix,
        })
    }
    
    /// Execute a query against parquet files in Azure
    pub async fn execute_query(
        &self,
        sql: &str,
        client_name: Option<&str>,
    ) -> Result<Vec<serde_json::Value>> {
        info!("[AZURE QUERY] Executing query: {}", sql);
        info!("[AZURE QUERY] Client filter: {:?}", client_name);
        
        // Register parquet files from Azure
        let files_registered = self.register_azure_parquet_files(client_name).await?;
        
        if files_registered == 0 {
            info!("[AZURE QUERY] No parquet files found in Azure");
            return Ok(vec![]);
        }
        
        info!("[AZURE QUERY] Registered {} files from Azure", files_registered);
        
        // Execute SQL query
        let df = self.session_ctx.sql(sql).await
            .context("Failed to create dataframe from SQL")?;
        
        // Collect results
        let results = df.collect().await
            .context("Failed to execute query")?;
        
        // Convert to JSON
        let mut json_rows = Vec::new();
        for batch in results {
            let num_rows = batch.num_rows();
            let num_cols = batch.num_columns();
            
            for row in 0..num_rows {
                let mut json_row = serde_json::Map::new();
                
                for col in 0..num_cols {
                    let column = batch.column(col);
                    let schema = batch.schema();
                    let field = schema.field(col);
                    let value = arrow_value_to_json(column, row)?;
                    json_row.insert(field.name().clone(), value);
                }
                
                json_rows.push(serde_json::Value::Object(json_row));
            }
        }
        
        info!("[AZURE QUERY] Query returned {} rows", json_rows.len());
        Ok(json_rows)
    }
    
    /// Register parquet files from Azure blob storage
    async fn register_azure_parquet_files(&self, client_name: Option<&str>) -> Result<usize> {
        // Clear existing table registration
        let _ = self.session_ctx.deregister_table("logs");
        
        // Build path pattern with prefix support
        let base_path = if let Some(prefix) = &self.prefix {
            format!("{}/{}", self.container_url, prefix)
        } else {
            self.container_url.clone()
        };
        
        let path_pattern = if let Some(client) = client_name {
            format!("{}/{}/", base_path, client)
        } else {
            format!("{}/", base_path)
        };
        
        info!("[AZURE QUERY] Looking for parquet files at: {}", path_pattern);
        
        // Create listing table options for parquet files
        use datafusion::datasource::file_format::parquet::ParquetFormat;
        let file_format = Arc::new(ParquetFormat::default());
        let listing_options = ListingOptions::new(file_format)
            .with_file_extension(".parquet")
            .with_collect_stat(false);
        
        // Register the table
        self.session_ctx
            .register_listing_table(
                "logs",
                &path_pattern,
                listing_options,
                None,
                None,
            )
            .await
            .context("Failed to register Azure parquet files")?;
        
        // Count files (approximate)
        let df = self.session_ctx.sql("SELECT COUNT(*) as count FROM logs LIMIT 1").await?;
        let results = df.collect().await?;
        
        if let Some(batch) = results.first() {
            if batch.num_rows() > 0 {
                return Ok(1); // At least one file found
            }
        }
        
        Ok(0)
    }
}

/// Convert Arrow value to JSON
fn arrow_value_to_json(column: &dyn arrow_array::Array, row: usize) -> Result<serde_json::Value> {
    use arrow_array::*;
    use arrow_schema::DataType;
    
    if column.is_null(row) {
        return Ok(serde_json::Value::Null);
    }
    
    match column.data_type() {
        DataType::Utf8 => {
            let array = column.as_any().downcast_ref::<StringArray>().unwrap();
            Ok(serde_json::Value::String(array.value(row).to_string()))
        }
        DataType::Int64 => {
            let array = column.as_any().downcast_ref::<Int64Array>().unwrap();
            Ok(serde_json::Value::Number(array.value(row).into()))
        }
        DataType::Float64 => {
            let array = column.as_any().downcast_ref::<Float64Array>().unwrap();
            let val = array.value(row);
            Ok(serde_json::json!(val))
        }
        DataType::Boolean => {
            let array = column.as_any().downcast_ref::<BooleanArray>().unwrap();
            Ok(serde_json::Value::Bool(array.value(row)))
        }
        DataType::Timestamp(_, _) => {
            let array = column.as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
            let timestamp = array.value(row);
            Ok(serde_json::Value::String(format!("{}", timestamp)))
        }
        _ => Ok(serde_json::Value::String(format!("{:?}", column.data_type()))),
    }
}