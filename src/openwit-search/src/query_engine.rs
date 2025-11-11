use anyhow::{Result};
use tracing::{info};
use std::time::Instant;
use openwit_proto::control::control_plane_service_client::ControlPlaneServiceClient;
use tonic::transport::Channel;
use openwit_config::UnifiedConfig;

use crate::types::*;
use crate::storage_grpc_client::StorageGrpcClientV2;

pub struct QueryEngineV2 {
    _config: QueryEngineConfig,
    storage_client: StorageGrpcClientV2,
}

#[derive(Debug, Clone)]
pub struct QueryEngineConfig {
    /// Query timeout in seconds
    pub query_timeout_secs: u64,
    /// Node ID for this search service
    pub node_id: String,
}

impl Default for QueryEngineConfig {
    fn default() -> Self {
        Self {
            query_timeout_secs: 60, // 1 minute - much more reasonable for search queries
            node_id: "search-node".to_string(),
        }
    }
}

impl QueryEngineV2 {
    pub async fn new(
        config: QueryEngineConfig,
        _control_client: Option<ControlPlaneServiceClient<Channel>>,
        unified_config: Option<UnifiedConfig>,
    ) -> Result<Self> {
        // Create control plane client
        let control_plane_client = if let Some(ref cfg) = unified_config {
            // Create new client from config
            openwit_control_plane::client::ControlPlaneClient::new(&config.node_id, cfg).await?
        } else {
            return Err(anyhow::anyhow!("No control plane config provided"));
        };
        
        // Create storage client with control plane integration
        let storage_client = StorageGrpcClientV2::new(
            config.node_id.clone(),
            control_plane_client,
        ).await?;
        
        Ok(Self {
            _config: config,
            storage_client,
        })
    }

    /// Execute a search query using the new storage client
    pub async fn search(&self, query: SearchQuery) -> Result<SearchResponse> {
        let start_time = Instant::now();
        let request_id = query.request_id.clone();

        info!("[QUERY ENGINE V2] New search request");
        info!("[QUERY ENGINE V2] Request ID: {}", request_id);
        info!("[QUERY ENGINE V2] SQL: {}", query.query);
        info!("[QUERY ENGINE V2] Client ID: {:?}", query.client_id);
        info!("[QUERY ENGINE V2] About to delegate to storage client...");

        let sql = query.query.clone();
        let request_sql = sql.clone(); // Keep a copy for later use
        
        // Use client ID from query if provided, otherwise try to extract from SQL
        let mut client_name = query.client_id.clone()
            .or_else(|| self.extract_client_from_sql(&sql));
        
        // Sanitize client name to prevent duplicate prefixes
        if let Some(ref mut name) = client_name {
            // Remove telemetry type prefixes if they exist
            let telemetry_prefixes = vec!["logs_", "traces_", "metrics_"];
            for prefix in telemetry_prefixes {
                if name.starts_with(prefix) {
                    *name = name.strip_prefix(prefix).unwrap().to_string();
                    info!("[QUERY ENGINE V2] Stripped '{}' prefix from client name: {}", prefix, name);
                    break;
                }
            }
            
            // Handle potential duplicate pattern (e.g., "kbuin_kbuin" -> "kbuin")
            if name.contains('_') {
                let parts: Vec<&str> = name.split('_').collect();
                if parts.len() > 1 && parts.iter().all(|&p| p == parts[0]) {
                    *name = parts[0].to_string();
                    info!("[QUERY ENGINE V2] Fixed duplicate client name pattern: {}", name);
                }
            }
        }
        
        info!("[QUERY ENGINE V2] Client: {:?}, Limit: {:?}", client_name, query.limit);

        // Don't rewrite SQL - table names should be traces/metrics/logs
        // The client_name is used to locate the correct data directory, not as table name
        let final_sql = sql.clone();

        // Execute query using the new storage client with control plane integration and top-level timeout
        let storage_result = tokio::time::timeout(
            std::time::Duration::from_secs(self._config.query_timeout_secs),
            self.storage_client.execute_query(
                final_sql,
                client_name,
                query.limit,
                request_id.clone(),
            )
        ).await
        .map_err(|_| anyhow::anyhow!("Query timed out after {} seconds", self._config.query_timeout_secs))??;
        
        // Convert storage response to search result
        // Pass through raw results without transformation
        let search_result = SearchResult {
            schema: None,
            schema_json: None,  // Don't include schema_json as requested
            rows: storage_result.results,
            total_count: Some(storage_result.row_count as u64),
            has_more: false,
            record_batches: vec![],
        };
        
        let execution_time_ms = start_time.elapsed().as_millis() as u64;
        
        info!("[QUERY ENGINE V2] Query completed: {} rows in {}ms", 
              storage_result.row_count, execution_time_ms);
        
        Ok(SearchResponse {
            request_id,
            status: QueryStatus::Success,
            result: Some(search_result),
            error: None,
            execution_time_ms,
            rows_scanned: storage_result.rows_scanned,
            bytes_scanned: storage_result.bytes_scanned,
            partition_info: None, // Can be enhanced later
        })
    }
    
    /// Extract client name from SQL query (if specified)
    fn extract_client_from_sql(&self, sql: &str) -> Option<String> {
        // Simple extraction - look for FROM <client>.<table> pattern
        if let Some(from_pos) = sql.to_lowercase().find("from ") {
            let after_from = &sql[from_pos + 5..].trim();
            if let Some(dot_pos) = after_from.find('.') {
                let client_name = after_from[..dot_pos].trim();
                // Remove quotes if present
                let client_name = client_name.trim_matches('"').trim_matches('\'').trim_matches('`').to_string();
                
                // Also handle table names that might have telemetry type prefix
                // e.g., FROM datafusion.logs_kbuin -> extract "kbuin"
                // e.g., FROM datafusion.traces_kbuin -> extract "kbuin"
                if let Some(table_part) = after_from.get(dot_pos + 1..) {
                    let table_name = table_part.split_whitespace().next().unwrap_or("");
                    let table_name = table_name.trim_matches('"').trim_matches('\'').trim_matches('`');
                    
                    if client_name == "datafusion" {
                        // Check for any telemetry type prefix
                        let telemetry_prefixes = vec!["logs_", "traces_", "metrics_"];
                        for prefix in telemetry_prefixes {
                            if table_name.starts_with(prefix) {
                                if let Some(actual_client) = table_name.strip_prefix(prefix) {
                                    return Some(actual_client.to_string());
                                }
                            }
                        }
                    }
                }
                
                return Some(client_name);
            }
        }
        None
    }
    
    /// Rewrite SQL table names to use client name instead of generic table names
    fn rewrite_sql_table_names(&self, sql: &str, client_name: &str) -> String {
        // List of generic table names that should be replaced with client name
        let generic_tables = vec!["traces", "logs", "metrics"];
        
        let mut rewritten = sql.to_string();
        
        for table in &generic_tables {
            // Replace "FROM table" with "FROM client_name" (case insensitive)
            let patterns = vec![
                format!("FROM {}", table),
                format!("from {}", table),
                format!("FROM {}", table.to_uppercase()),
                format!("from {}", table.to_uppercase()),
                format!("FROM `{}`", table),
                format!("from `{}`", table),
                format!("FROM \"{}\"", table),
                format!("from \"{}\"", table),
                format!("FROM '{}'", table),
                format!("from '{}'", table),
            ];
            
            for pattern in &patterns {
                let replacement = pattern.replace(table, client_name);
                rewritten = rewritten.replace(pattern, &replacement);
            }
            
            // Also handle JOIN cases
            let join_patterns = vec![
                format!("JOIN {}", table),
                format!("join {}", table),
                format!("JOIN {}", table.to_uppercase()),
                format!("join {}", table.to_uppercase()),
            ];
            
            for pattern in &join_patterns {
                let replacement = pattern.replace(table, client_name);
                rewritten = rewritten.replace(pattern, &replacement);
            }
        }
        
        rewritten
    }
}

pub use crate::types::SearchResult;