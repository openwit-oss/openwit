use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchQuery {
    /// SQL query string
    pub query: String,
    /// Maximum results to return
    pub limit: Option<usize>,
    /// Offset for pagination
    pub offset: Option<usize>,
    /// Request ID for tracking (auto-generated if not provided)
    #[serde(default = "default_request_id")]
    pub request_id: String,
    /// Optional client ID filter
    #[serde(alias = "client_name")]  // Accept both client_id and client_name for backward compatibility
    pub client_id: Option<String>,
}

/// Generate a default request ID if not provided
fn default_request_id() -> String {
    use uuid::Uuid;
    format!("auto-{}", Uuid::new_v4())
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResponse {
    pub request_id: String,
    pub status: QueryStatus,
    pub result: Option<SearchResult>,
    pub error: Option<String>,
    pub execution_time_ms: u64,
    pub rows_scanned: u64,
    pub bytes_scanned: u64,
    pub partition_info: Option<PartitionInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum QueryStatus {
    Success,
    Failed,
    Timeout,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResult {
    /// Arrow schema
    #[serde(skip)]
    pub schema: Option<arrow_schema::Schema>,
    /// Schema for JSON response
    pub schema_json: Option<serde_json::Value>,
    /// Result rows as JSON array
    pub rows: Vec<serde_json::Value>,
    /// Total row count (before limit)
    pub total_count: Option<u64>,
    /// Whether more results are available
    pub has_more: bool,
    /// Arrow record batches (for internal use)
    #[serde(skip)]
    pub record_batches: Vec<arrow_array::RecordBatch>,
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionInfo {
    pub partitions_scanned: usize,
    pub partition_details: Vec<PartitionDetail>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionDetail {
    pub partition_id: String,
    pub node: Option<String>,
    pub rows_scanned: u64,
    pub processing_time_ms: u64,
}

