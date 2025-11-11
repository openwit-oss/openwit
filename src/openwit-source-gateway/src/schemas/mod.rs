use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use anyhow::Result;

pub mod loader;
pub use loader::SchemaLoader;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaDefinition {
    pub schema_version: String,
    #[serde(rename = "type")]
    pub schema_type: String,
    pub resource: ResourceSchema,
    pub scope: ScopeSchema,
    #[serde(flatten)]
    pub type_specific: TypeSpecificSchema,
    pub batch: BatchSchema,
    pub rate_limits: RateLimits,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceSchema {
    pub required: bool,
    pub attributes: AttributeSchema,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AttributeSchema {
    pub required_keys: Vec<String>,
    pub optional_keys: Vec<String>,
    pub custom_keys_allowed: bool,
    pub max_attributes: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScopeSchema {
    pub required: bool,
    pub fields: HashMap<String, FieldSchema>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldSchema {
    pub required: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_length: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_length: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pattern: Option<String>,
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub field_type: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum TypeSpecificSchema {
    Logs {
        log_record: LogRecordSchema,
    },
    Traces {
        span: SpanSchema,
        duration: DurationSchema,
    },
    Metrics {
        metric: MetricSchema,
        data_points: DataPointsSchema,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogRecordSchema {
    pub required_fields: Vec<String>,
    pub optional_fields: Vec<String>,
    pub field_validations: HashMap<String, FieldValidation>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpanSchema {
    pub required_fields: Vec<String>,
    pub optional_fields: Vec<String>,
    pub field_validations: HashMap<String, FieldValidation>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricSchema {
    pub required_fields: Vec<String>,
    pub field_validations: HashMap<String, FieldValidation>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DurationSchema {
    pub min_duration_nano: u64,
    pub max_duration_nano: u64,
    pub warn_threshold_nano: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataPointsSchema {
    pub gauge: DataPointTypeSchema,
    pub sum: DataPointTypeSchema,
    pub histogram: HistogramSchema,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataPointTypeSchema {
    pub required_fields: Vec<String>,
    pub max_data_points: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistogramSchema {
    pub required_fields: Vec<String>,
    pub max_buckets: usize,
    pub max_data_points: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldValidation {
    #[serde(rename = "type")]
    pub field_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub length: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub format: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pattern: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub values: Option<Vec<serde_json::Value>>,
    #[serde(rename = "enum", skip_serializing_if = "Option::is_none")]
    pub enum_values: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_length: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchSchema {
    pub max_size_bytes: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_log_records: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_spans: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_metrics: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimits {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_logs_per_second: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_spans_per_second: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_metrics_per_second: Option<usize>,
    pub max_bytes_per_second: usize,
    pub per_service_limits: PerServiceLimits,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerServiceLimits {
    pub enabled: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_max_logs_per_second: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_max_spans_per_second: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_max_metrics_per_second: Option<usize>,
}