use serde::{Deserialize, Serialize};
use crate::unified::validation::{Validatable, ValidationResult, limits, safe_defaults};
use tracing::info;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct IngestionConfig {
    pub sources: IngestionSources,
    
    #[serde(default)]
    pub kafka: KafkaConfig,
    
    #[serde(default)]
    pub grpc: GrpcIngestionConfig,
    
    #[serde(default)]
    pub http: HttpIngestionConfig,
    
    #[serde(default)]
    pub batch_tracker: BatchTrackerConfig,
    
    /// Telemetry type - determines the table name for queries (logs, traces, or metrics)
    #[serde(default = "default_telemetry_type")]
    pub telemetry_type: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct IngestionSources {
    #[serde(default)]
    pub kafka: SourceConfig,
    
    #[serde(default)]
    pub grpc: SourceConfig,
    
    #[serde(default)]
    pub http: SourceConfig,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SourceConfig {
    #[serde(default)]
    pub enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaConfig {
    pub brokers: Option<String>,
    pub group_id: Option<String>,
    
    #[serde(default)]
    pub topics: Vec<String>,
    
    #[serde(default = "default_consumer_type")]
    pub consumer_type: String,
    
    #[serde(default)]
    pub high_performance: KafkaHighPerformanceConfig,
    
    #[serde(default = "default_kafka_pool_size")]
    pub pool_size: u32,
    
    #[serde(default)]
    pub timeouts: KafkaTimeouts,
    
    #[serde(default)]
    pub batching: KafkaBatching,
    
    #[serde(default)]
    pub network: KafkaNetwork,
    
    #[serde(default)]
    pub connections: KafkaConnections,
    
    #[serde(default)]
    pub performance: KafkaPerformanceConfig,
    
    #[serde(default)]
    pub topic_index_config: TopicIndexConfig,
}

impl Default for KafkaConfig {
    fn default() -> Self {
        Self {
            brokers: None,
            group_id: None,
            topics: Vec::new(),
            consumer_type: default_consumer_type(),
            high_performance: KafkaHighPerformanceConfig::default(),
            pool_size: default_kafka_pool_size(),
            timeouts: KafkaTimeouts::default(),
            batching: KafkaBatching::default(),
            network: KafkaNetwork::default(),
            connections: KafkaConnections::default(),
            performance: KafkaPerformanceConfig::default(),
            topic_index_config: TopicIndexConfig::default(),
        }
    }
}

fn default_kafka_pool_size() -> u32 { safe_defaults::KAFKA_POOL_SIZE }

impl Validatable for KafkaConfig {
    fn validate(&self) -> Vec<ValidationResult> {
        let mut results = Vec::new();
        
        // Validate pool size
        results.push(limits::kafka_pool_size().validate(self.pool_size as i64));
        
        // Validate timeouts
        results.extend(self.timeouts.validate());
        
        // Validate batching
        results.extend(self.batching.validate());
        
        results.into_iter().filter(|r| !r.is_valid()).collect()
    }
    
    fn apply_safe_defaults(&mut self) {
        info!("Applying safe defaults to Kafka configuration");
        self.pool_size = safe_defaults::KAFKA_POOL_SIZE;
        self.timeouts.apply_safe_defaults();
        self.batching.apply_safe_defaults();
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaTimeouts {
    #[serde(default = "default_batch_timeout_ms")]
    pub batch_timeout_ms: u64,
    
    #[serde(default = "default_commit_interval_seconds")]
    pub commit_interval_seconds: u64,
    
    #[serde(default = "default_poll_timeout_ms")]
    pub poll_timeout_ms: u64,
    
    #[serde(default = "default_session_timeout_ms")]
    pub session_timeout_ms: u64,
    
    #[serde(default = "default_heartbeat_interval_ms")]
    pub heartbeat_interval_ms: u64,
    
    #[serde(default = "default_max_poll_interval_ms")]
    pub max_poll_interval_ms: u64,
}

impl Default for KafkaTimeouts {
    fn default() -> Self {
        Self {
            batch_timeout_ms: default_batch_timeout_ms(),
            commit_interval_seconds: default_commit_interval_seconds(),
            poll_timeout_ms: default_poll_timeout_ms(),
            session_timeout_ms: default_session_timeout_ms(),
            heartbeat_interval_ms: default_heartbeat_interval_ms(),
            max_poll_interval_ms: default_max_poll_interval_ms(),
        }
    }
}

fn default_batch_timeout_ms() -> u64 { 100 }
fn default_commit_interval_seconds() -> u64 { 5 }
fn default_poll_timeout_ms() -> u64 { 10 }
fn default_session_timeout_ms() -> u64 { 60000 } // 1 minute for safe mode
fn default_heartbeat_interval_ms() -> u64 { 3000 }
fn default_max_poll_interval_ms() -> u64 { safe_defaults::KAFKA_MAX_POLL_INTERVAL_MS }

impl Validatable for KafkaTimeouts {
    fn validate(&self) -> Vec<ValidationResult> {
        let mut results = Vec::new();
        results.push(limits::kafka_session_timeout_ms().validate(self.session_timeout_ms as i64));
        results
    }
    
    fn apply_safe_defaults(&mut self) {
        self.session_timeout_ms = 60000;  // 1 minute for safe mode
        self.max_poll_interval_ms = safe_defaults::KAFKA_MAX_POLL_INTERVAL_MS;
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaBatching {
    #[serde(default = "default_batch_size")]
    pub batch_size: u32,
    
    #[serde(default = "default_chunk_size")]
    pub chunk_size: u32,
    
    #[serde(default = "default_min_fetch_bytes")]
    pub min_fetch_bytes: u64,
    
    #[serde(default = "default_max_fetch_bytes")]
    pub max_fetch_bytes: u64,
    
    #[serde(default = "default_fetch_wait_max_ms")]
    pub fetch_wait_max_ms: u64,
}

impl Default for KafkaBatching {
    fn default() -> Self {
        Self {
            batch_size: default_batch_size(),
            chunk_size: default_chunk_size(),
            min_fetch_bytes: default_min_fetch_bytes(),
            max_fetch_bytes: default_max_fetch_bytes(),
            fetch_wait_max_ms: default_fetch_wait_max_ms(),
        }
    }
}

fn default_batch_size() -> u32 { safe_defaults::KAFKA_BATCH_SIZE }
fn default_chunk_size() -> u32 { safe_defaults::KAFKA_CHUNK_SIZE }
fn default_min_fetch_bytes() -> u64 { 1048576 }
fn default_max_fetch_bytes() -> u64 { 52428800 }
fn default_fetch_wait_max_ms() -> u64 { 500 }

impl Validatable for KafkaBatching {
    fn validate(&self) -> Vec<ValidationResult> {
        let mut results = Vec::new();
        results.push(limits::kafka_batch_size().validate(self.batch_size as i64));
        results
    }
    
    fn apply_safe_defaults(&mut self) {
        self.batch_size = safe_defaults::KAFKA_BATCH_SIZE;
        self.chunk_size = safe_defaults::KAFKA_CHUNK_SIZE;
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaNetwork {
    #[serde(default = "default_socket_buffer_bytes")]
    pub socket_receive_buffer_bytes: u64,
    
    #[serde(default = "default_socket_buffer_bytes")]
    pub socket_send_buffer_bytes: u64,
    
    #[serde(default = "default_queued_max_messages_kbytes")]
    pub queued_max_messages_kbytes: u64,
    
    #[serde(default = "default_true")]
    pub socket_keepalive_enable: bool,
    
    #[serde(default = "default_true")]
    pub socket_nagle_disable: bool,
}

impl Default for KafkaNetwork {
    fn default() -> Self {
        Self {
            socket_receive_buffer_bytes: default_socket_buffer_bytes(),
            socket_send_buffer_bytes: default_socket_buffer_bytes(),
            queued_max_messages_kbytes: default_queued_max_messages_kbytes(),
            socket_keepalive_enable: default_true(),
            socket_nagle_disable: default_true(),
        }
    }
}

fn default_socket_buffer_bytes() -> u64 { 8388608 }
fn default_queued_max_messages_kbytes() -> u64 { 2097152 }
fn default_true() -> bool { true }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaPerformanceConfig {
    /// Maximum batch size in bytes for Kafka processing (default: 15MB)
    #[serde(default = "default_max_batch_bytes")]
    pub max_batch_bytes: usize,
    
    /// Maximum message size in bytes for gRPC transport (default: 10MB)
    #[serde(default = "default_max_message_bytes")]  
    pub max_message_bytes: usize,
    
    /// Maximum number of retries for operations (default: 5)
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,
    
    /// Retry delay in milliseconds (default: 2000ms)
    #[serde(default = "default_retry_delay_ms")]
    pub retry_delay_ms: u64,
}

impl Default for KafkaPerformanceConfig {
    fn default() -> Self {
        Self {
            max_batch_bytes: default_max_batch_bytes(),
            max_message_bytes: default_max_message_bytes(),
            max_retries: default_max_retries(),
            retry_delay_ms: default_retry_delay_ms(),
        }
    }
}

fn default_max_batch_bytes() -> usize { 15_728_640 } // 15MB
fn default_max_message_bytes() -> usize { 10 * 1024 * 1024 } // 10MB
fn default_max_retries() -> u32 { 5 }
fn default_retry_delay_ms() -> u64 { 2000 }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaConnections {
    #[serde(default = "default_max_idle_ms")]
    pub max_idle_ms: u64,
    
    #[serde(default = "default_reconnect_backoff_ms")]
    pub reconnect_backoff_ms: u64,
    
    #[serde(default = "default_reconnect_backoff_max_ms")]
    pub reconnect_backoff_max_ms: u64,
}

impl Default for KafkaConnections {
    fn default() -> Self {
        Self {
            max_idle_ms: default_max_idle_ms(),
            reconnect_backoff_ms: default_reconnect_backoff_ms(),
            reconnect_backoff_max_ms: default_reconnect_backoff_max_ms(),
        }
    }
}

fn default_max_idle_ms() -> u64 { 540000 }
fn default_reconnect_backoff_ms() -> u64 { 50 }
fn default_reconnect_backoff_max_ms() -> u64 { 1000 }

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct GrpcIngestionConfig {
    #[serde(default = "default_grpc_bind")]
    pub bind: String,

    #[serde(default = "default_grpc_port")]
    pub port: u16,

    #[serde(default = "default_max_message_size")]
    pub max_message_size: usize,

    #[serde(default)]
    pub tls: TlsConfig,
}

fn default_grpc_bind() -> String { "0.0.0.0".to_string() }
fn default_grpc_port() -> u16 { 4317 }
fn default_max_message_size() -> usize { 4194304 }
fn default_request_timeout_ms() -> u64 { 30000 }

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TlsConfig {
    #[serde(default)]
    pub enabled: bool,
    
    pub cert_path: Option<String>,
    pub key_path: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct HttpIngestionConfig {
    #[serde(default = "default_http_bind")]
    pub bind: String,
    
    #[serde(default = "default_http_port")]
    pub port: u16,
    
    #[serde(default = "default_http_max_concurrent_requests")]
    pub max_concurrent_requests: u32,
    
    #[serde(default = "default_request_timeout_ms")]
    pub request_timeout_ms: u64,
    
    #[serde(default = "default_max_payload_size_mb")]
    pub max_payload_size_mb: u32,
    
    #[serde(default)]
    pub tls: TlsConfig,
}

fn default_http_bind() -> String { "0.0.0.0:3000".to_string() }
fn default_http_port() -> u16 { 3000 }
fn default_http_max_concurrent_requests() -> u32 { 5000 }
fn default_max_payload_size_mb() -> u32 { 100 }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicIndexConfig {
    #[serde(default = "default_index_position")]
    pub default_index_position: usize,
    
    #[serde(default)]
    pub patterns: Vec<TopicIndexPattern>,
    
    #[serde(default)]
    pub auto_generate: AutoGenerateConfig,
    
    #[serde(default)]
    pub client_name_config: ClientNameConfig,
}

impl Default for TopicIndexConfig {
    fn default() -> Self {
        Self {
            default_index_position: default_index_position(),
            patterns: Vec::new(),
            auto_generate: AutoGenerateConfig::default(),
            client_name_config: ClientNameConfig::default(),
        }
    }
}

impl TopicIndexConfig {
    /// Extract index name from topic name using configured patterns and positions
    pub fn extract_index_name(&self, topic: &str) -> String {
        tracing::debug!("Extracting index name from topic: '{}'", topic);
        tracing::debug!("Available patterns: {:?}", self.patterns);
        tracing::debug!("Default index position: {}", self.default_index_position);
        
        // Check if topic matches any pattern
        for pattern in &self.patterns {
            tracing::debug!("Checking pattern: '{}'", pattern.r#match);
            if self.matches_pattern(&pattern.r#match, topic) {
                tracing::debug!("Pattern matched! Using index position: {}", pattern.index_position);
                if let Some(index) = self.extract_at_position(topic, pattern.index_position) {
                    tracing::debug!("Extracted index: '{}'", index);
                    // Apply prefix if configured
                    if let Some(ref prefix) = pattern.index_prefix {
                        let result = format!("{}{}", prefix, index);
                        tracing::debug!("Applied prefix '{}', final result: '{}'", prefix, result);
                        return result;
                    } else {
                        tracing::debug!("No prefix, returning: '{}'", index);
                        return index;
                    }
                }
            } else {
                tracing::debug!("Pattern did not match");
            }
        }
        
        // No pattern matched, use default position
        tracing::debug!("No patterns matched, using default position: {}", self.default_index_position);
        if let Some(index) = self.extract_at_position(topic, self.default_index_position) {
            tracing::debug!("Extracted using default position: '{}'", index);
            index
        } else {
            // Fallback to full topic name if extraction fails
            tracing::debug!("Default position extraction failed, using full topic: '{}'", topic);
            topic.to_string()
        }
    }
    
    /// Check if topic matches a pattern (supports simple glob patterns)
    fn matches_pattern(&self, pattern: &str, topic: &str) -> bool {
        // Simple pattern matching - supports * wildcards
        if pattern.contains('*') {
            // Convert glob pattern to check if it matches
            // For v6.qtw.*.*.* we want to match v6.qtw.traces.kbuin.0
            
            // Split on dots to match the pattern structure
            let topic_parts: Vec<&str> = topic.split('.').collect();
            let pattern_parts: Vec<&str> = pattern.split('.').collect();
            
            // Check if we have the right number of parts
            if topic_parts.len() != pattern_parts.len() {
                return false;
            }
            
            // Check each part
            for (topic_part, pattern_part) in topic_parts.iter().zip(pattern_parts.iter()) {
                if pattern_part == &"*" {
                    // Wildcard matches anything
                    continue;
                } else if topic_part != pattern_part {
                    // Non-wildcard part must match exactly
                    return false;
                }
            }
            
            return true;
        }
        
        // Exact match fallback
        pattern == topic
    }
    
    /// Extract part at specified position (0-indexed, dot-separated)
    fn extract_at_position(&self, topic: &str, position: usize) -> Option<String> {
        let parts: Vec<&str> = topic.split('.').collect();
        if position < parts.len() {
            Some(parts[position].to_string())
        } else {
            None
        }
    }
}

fn default_index_position() -> usize { 3 }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicIndexPattern {
    pub r#match: String,
    pub index_position: usize,
    
    #[serde(default)]
    pub client_position: Option<usize>,
    
    #[serde(default)]
    pub index_prefix: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutoGenerateConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    
    #[serde(default = "default_auto_prefix")]
    pub prefix: String,
    
    #[serde(default = "default_auto_strategy")]
    pub strategy: String,
}

impl Default for AutoGenerateConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            prefix: default_auto_prefix(),
            strategy: default_auto_strategy(),
        }
    }
}

fn default_auto_prefix() -> String { "auto_".to_string() }
fn default_auto_strategy() -> String { "hash".to_string() }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientNameConfig {
    /// Position in topic parts to extract client name for Kafka (e.g., if topic is "client.app.logs", position 0 = "client")
    #[serde(default = "default_client_position")]
    pub kafka_client_position: usize,
    
    /// Separator for splitting topic parts (default: ".")
    #[serde(default = "default_topic_separator")]
    pub topic_separator: String,
    
    /// How to extract client name from HTTP/gRPC index_name
    #[serde(default)]
    pub http_grpc_extraction: HttpGrpcClientExtraction,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpGrpcClientExtraction {
    /// Method to extract client name from index_name
    #[serde(default = "default_extraction_method")]
    pub method: String, // "prefix", "suffix", "position", "direct"
    
    /// Separator for splitting index_name (e.g., "-", "_", ".")
    #[serde(default = "default_http_separator")]
    pub separator: String,
    
    /// Position to extract client name (for "position" method)
    #[serde(default)]
    pub position: usize,
    
    /// Prefix to remove (for "prefix" method)
    #[serde(default)]
    pub prefix: String,
    
    /// Suffix to remove (for "suffix" method)
    #[serde(default)]
    pub suffix: String,
}

impl Default for ClientNameConfig {
    fn default() -> Self {
        Self {
            kafka_client_position: default_client_position(),
            topic_separator: default_topic_separator(),
            http_grpc_extraction: HttpGrpcClientExtraction::default(),
        }
    }
}

impl Default for HttpGrpcClientExtraction {
    fn default() -> Self {
        Self {
            method: default_extraction_method(),
            separator: default_http_separator(),
            position: 0,
            prefix: String::new(),
            suffix: String::new(),
        }
    }
}

fn default_client_position() -> usize { 0 } // First part of topic
fn default_topic_separator() -> String { ".".to_string() }
fn default_extraction_method() -> String { "direct".to_string() } // Use index_name directly as client
fn default_http_separator() -> String { "-".to_string() }

fn default_consumer_type() -> String { "standard".to_string() }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaHighPerformanceConfig {
    #[serde(default = "default_num_consumers")]
    pub num_consumers: usize,
    
    #[serde(default = "default_processing_threads")]
    pub processing_threads: usize,
    
    #[serde(default = "default_num_grpc_clients")]
    pub num_grpc_clients: usize,
    
    #[serde(default = "default_channel_buffer_size")]
    pub channel_buffer_size: usize,
    
    #[serde(default = "default_fetch_min_bytes")]
    pub fetch_min_bytes: usize,
    
    #[serde(default = "default_fetch_max_bytes")]
    pub fetch_max_bytes: usize,
    
    #[serde(default = "default_max_partition_fetch_bytes")]
    pub max_partition_fetch_bytes: usize,
    
    #[serde(default = "default_true")]
    pub enable_zero_copy: bool,
    
    #[serde(default = "default_zero_copy_threshold_bytes")]
    pub zero_copy_threshold_bytes: usize,
    
    #[serde(default = "default_true")]
    pub preallocate_buffers: bool,
    
    #[serde(default = "default_buffer_pool_size")]
    pub buffer_pool_size: usize,
}

impl Default for KafkaHighPerformanceConfig {
    fn default() -> Self {
        Self {
            num_consumers: default_num_consumers(),
            processing_threads: default_processing_threads(),
            num_grpc_clients: default_num_grpc_clients(),
            channel_buffer_size: default_channel_buffer_size(),
            fetch_min_bytes: default_fetch_min_bytes(),
            fetch_max_bytes: default_fetch_max_bytes(),
            max_partition_fetch_bytes: default_max_partition_fetch_bytes(),
            enable_zero_copy: true,
            zero_copy_threshold_bytes: default_zero_copy_threshold_bytes(),
            preallocate_buffers: true,
            buffer_pool_size: default_buffer_pool_size(),
        }
    }
}

fn default_num_consumers() -> usize { 8 }
fn default_processing_threads() -> usize { 16 }
fn default_num_grpc_clients() -> usize { 4 }
fn default_channel_buffer_size() -> usize { 100000 }
fn default_fetch_min_bytes() -> usize { 1048576 } // 1MB
fn default_fetch_max_bytes() -> usize { 52428800 } // 50MB
fn default_max_partition_fetch_bytes() -> usize { 10485760 } // 10MB
fn default_buffer_pool_size() -> usize { 1000 }
fn default_zero_copy_threshold_bytes() -> usize { 102400 } // 100KB
fn default_telemetry_type() -> String { "logs".to_string() }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchTrackerConfig {
    #[serde(default = "default_batch_tracker_enabled")]
    pub enabled: bool,
    
    #[serde(default = "default_postgres_url")]
    pub postgres_url: String,
    
    #[serde(default)]
    pub batch_config: BatchConfig,
    
    #[serde(default = "default_wal_directory")]
    pub wal_directory: String,
}

impl Default for BatchTrackerConfig {
    fn default() -> Self {
        Self {
            enabled: default_batch_tracker_enabled(),
            postgres_url: default_postgres_url(),
            batch_config: BatchConfig::default(),
            wal_directory: default_wal_directory(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchConfig {
    #[serde(default = "default_max_size_bytes")]
    pub max_size_bytes: usize,
    
    #[serde(default = "default_max_messages")]
    pub max_messages: usize,
    
    #[serde(default = "default_max_age_seconds")]
    pub max_age_seconds: u64,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            max_size_bytes: default_max_size_bytes(),
            max_messages: default_max_messages(),
            max_age_seconds: default_max_age_seconds(),
        }
    }
}

fn default_batch_tracker_enabled() -> bool { true }
fn default_postgres_url() -> String { "postgresql://localhost/openwit".to_string() }
fn default_wal_directory() -> String { "./data/wal".to_string() }
fn default_max_size_bytes() -> usize { 10 * 1024 * 1024 } // 10MB
fn default_max_messages() -> usize { 10000 }
fn default_max_age_seconds() -> u64 { 30 }