use serde::Deserialize;
use std::collections::HashMap;

#[derive(Debug, Clone, Deserialize, Default)]
pub struct TlsConfig {
    pub mode: Option<bool>,          // TLS enabled or not
    pub cert_path: Option<String>,   // Path to certificate
    pub key_path: Option<String>,    // Path to private key
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct GrpcConfig {
    pub enabled: Option<bool>,       // Whether gRPC is enabled
    pub bind: Option<String>,        // Address to bind (host:port)
    pub runtime_size: Option<usize>, // Thread pool size
    pub tls: Option<TlsConfig>,      // TLS config (nested)
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct HttpConfig {
    pub enabled: Option<bool>,       // Whether HTTP REST API is enabled
    pub bind: Option<String>,        // Address to bind (host:port), e.g. "0.0.0.0:3000"
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct PrometheusConfig {
    pub enabled: Option<bool>,       // Whether Prometheus metrics endpoint is enabled
    pub bind: Option<String>,        // Address to bind (host:port), e.g. "0.0.0.0:9090"
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum IngestionSource {
    Kafka,   // Only Kafka ingestion
    Grpc,    // Only gRPC/OTLP ingestion
    Http,    // Only HTTP ingestion
    All,     // All sources enabled
}

#[derive(Debug, Clone, Deserialize)]
pub struct GossipConfig {
    pub self_node_name: String,
    pub listen_addr: String,
    pub seed_nodes: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct KubernetesConfig {
    pub enabled: Option<bool>,
    pub pod_role: Option<String>,      // e.g., "ingest,index"
    pub node_group: Option<String>,    // optional logical grouping
    pub headless_service: Option<String>, // headless service name for discovery
    pub namespace: Option<String>,     // k8s namespace
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum NodeMode {
    Monolith,  // Default - runs all components
    Ingest,    // Only ingestion (Kafka consumers, WAL)
    Storage,   // Only storage (Parquet, S3/Azure upload)
    Index,     // Only indexing 
    Search,    // Only search/query
    Control,   // Only control plane
}



#[derive(Debug, Clone, Deserialize)]
pub struct KafkaConfig {
    pub brokers: String,
    pub group_id: String,
    pub topics: Vec<String>,
    pub pool_size: usize,
    pub client_options: Option<HashMap<String, String>>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MemTableConfig {
    pub max_size_mb: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BufferConfig {
    pub batch_size: usize,
    pub flush_seconds: usize,
    pub output_dir: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AlertingThresholds {
    pub kafka_messages_per_sec: u64,
    pub memtable_usage_percent: u8,
    pub flush_queue_depth: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AlertingConfig {
    pub alert_url: String,
    pub thresholds: AlertingThresholds,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AzureConfig {
    pub account_name: String,
    pub container_name: Option<String>,
    pub access_key: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct StorageConfig {
    pub azure: Option<AzureConfig>,
    pub concurrent_uploads: Option<usize>,
    pub retry_attempts: Option<usize>,
    pub upload_timeout_seconds: Option<u64>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MetastoreConfig {
    pub backend: String,  // "postgres" or "sled"
    pub connection_string: Option<String>,  // For PostgreSQL
    pub max_connections: Option<u32>,       // For PostgreSQL
    pub schema_name: Option<String>,        // For PostgreSQL
    pub path: Option<String>,               // For Sled
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct OpenWitConfig {
    pub source: Option<IngestionSource>,  // Ingestion source selector
    pub grpc: Option<GrpcConfig>,
    pub http: Option<HttpConfig>,
    pub prometheus: Option<PrometheusConfig>,
    pub gossip: Option<GossipConfig>,
    pub kafka: Option<KafkaConfig>,
    pub mem_table: Option<MemTableConfig>,
    pub buffer: Option<BufferConfig>,
    pub alerting: Option<AlertingConfig>,
    pub kubernetes: Option<KubernetesConfig>,
    pub storage: Option<StorageConfig>,
    pub metastore: Option<MetastoreConfig>,
    pub node_mode: Option<NodeMode>,  // Deployment mode
}

impl Default for NodeMode {
    fn default() -> Self {
        NodeMode::Monolith
    }
}

impl Default for IngestionSource {
    fn default() -> Self {
        IngestionSource::All
    }
}

impl OpenWitConfig {
    pub fn get_node_mode(&self) -> NodeMode {
        self.node_mode.clone().unwrap_or_default()
    }
    
    pub fn get_ingestion_source(&self) -> IngestionSource {
        self.source.clone().unwrap_or_default()
    }
    
    pub fn is_kubernetes_enabled(&self) -> bool {
        self.kubernetes
            .as_ref()
            .and_then(|k| k.enabled)
            .unwrap_or(false)
    }
    
    pub fn should_enable_kafka(&self) -> bool {
        matches!(self.get_ingestion_source(), IngestionSource::Kafka | IngestionSource::All)
    }
    
    pub fn should_enable_grpc(&self) -> bool {
        matches!(self.get_ingestion_source(), IngestionSource::Grpc | IngestionSource::All)
    }
    
    pub fn should_enable_http(&self) -> bool {
        matches!(self.get_ingestion_source(), IngestionSource::Http | IngestionSource::All)
    }
}

pub type Config = OpenWitConfig;
