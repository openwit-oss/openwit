use bytes::Bytes;
use chrono::{DateTime, Utc};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct KafkaMessage {
    pub id: String,
    pub timestamp: DateTime<Utc>,
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub key: Option<Vec<u8>>,
    pub payload: Bytes,
    pub headers: HashMap<String, String>,
    // Fields for JSON processing
    pub payload_json: String,     // JSON string representation
    pub payload_type: String,     // "trace", "log", or "metric"
    pub size_bytes: usize,
    // Zero-copy optimization flag
    pub is_zero_copy: bool,       // True if using zero-copy for large messages
}

// Zero-copy message variant that holds borrowed data
pub struct ZeroCopyKafkaMessage<'a> {
    pub id: String,
    pub timestamp: DateTime<Utc>,
    pub topic: &'a str,
    pub partition: i32,
    pub offset: i64,
    pub key: Option<&'a [u8]>,
    pub payload: &'a [u8],
    pub headers: HashMap<String, String>,
    pub payload_json: String,
    pub payload_type: String,
    pub size_bytes: usize,
}

#[derive(Clone, Debug)]
pub struct KafkaConfig {
    pub brokers: String,
    pub group_id: String,
    pub topics: Vec<String>,
    pub batch_size: usize,
    pub batch_timeout_ms: u64,  // Timeout for batch processing
    pub commit_interval_ms: u64,
    pub enable_auto_commit: bool,
    pub session_timeout_ms: u64,
    pub max_poll_interval_ms: u64,
    pub topic_index_config: Option<TopicIndexConfig>,
}

#[derive(Debug, Clone)]
pub struct TopicIndexConfig {
    pub default_index_position: usize,
    pub patterns: Vec<TopicIndexPattern>,
    pub auto_generate: AutoGenerateConfig,
    pub client_name_config: ClientNameConfig,
}

#[derive(Debug, Clone)]
pub struct TopicIndexPattern {
    pub pattern: String,
    pub index_position: usize,
    pub index_prefix: Option<String>,
}

#[derive(Debug, Clone)]
pub struct AutoGenerateConfig {
    pub enabled: bool,
    pub prefix: String,
    pub strategy: String,
}

#[derive(Debug, Clone)]
pub struct ClientNameConfig {
    pub kafka_client_position: usize,
    pub topic_separator: String,
}

impl Default for KafkaConfig {
    fn default() -> Self {
        Self {
            brokers: "localhost:9092".to_string(),
            group_id: "openwit-kafka".to_string(),
            topics: vec![],
            batch_size: 10000,  // Increased for high throughput
            batch_timeout_ms: 100,  // 100ms batch timeout
            commit_interval_ms: 1000,  // More frequent commits
            enable_auto_commit: false,
            session_timeout_ms: 30000,
            max_poll_interval_ms: 300000,
            topic_index_config: None,
        }
    }
}

impl From<openwit_config::unified::ingestion::TopicIndexConfig> for TopicIndexConfig {
    fn from(config: openwit_config::unified::ingestion::TopicIndexConfig) -> Self {
        Self {
            default_index_position: config.default_index_position,
            patterns: config.patterns.iter().map(|p| TopicIndexPattern {
                pattern: p.r#match.clone(),
                index_position: p.index_position,
                index_prefix: p.index_prefix.clone(),
            }).collect(),
            auto_generate: AutoGenerateConfig {
                enabled: config.auto_generate.enabled,
                prefix: config.auto_generate.prefix.clone(),
                strategy: config.auto_generate.strategy.clone(),
            },
            client_name_config: ClientNameConfig {
                kafka_client_position: config.client_name_config.kafka_client_position,
                topic_separator: config.client_name_config.topic_separator.clone(),
            },
        }
    }
}

impl From<&openwit_config::unified::IngestionConfig> for KafkaConfig {
    fn from(config: &openwit_config::unified::IngestionConfig) -> Self {
        Self {
            brokers: config.kafka.brokers.clone().unwrap_or_else(|| "localhost:9092".to_string()),
            group_id: config.kafka.group_id.clone().unwrap_or_else(|| "openwit-kafka".to_string()),
            topics: config.kafka.topics.clone(),
            batch_size: 10000, // High throughput default
            batch_timeout_ms: 100,  // 100ms batch timeout
            commit_interval_ms: 5000,
            enable_auto_commit: false,
            session_timeout_ms: 30000,
            max_poll_interval_ms: 300000,
            topic_index_config: Some(TopicIndexConfig {
                default_index_position: config.kafka.topic_index_config.default_index_position,
                patterns: config.kafka.topic_index_config.patterns.iter().map(|p| TopicIndexPattern {
                    pattern: p.r#match.clone(),
                    index_position: p.index_position,
                    index_prefix: p.index_prefix.clone(),
                }).collect(),
                auto_generate: AutoGenerateConfig {
                    enabled: config.kafka.topic_index_config.auto_generate.enabled,
                    prefix: config.kafka.topic_index_config.auto_generate.prefix.clone(),
                    strategy: config.kafka.topic_index_config.auto_generate.strategy.clone(),
                },
                client_name_config: ClientNameConfig {
                    kafka_client_position: config.kafka.topic_index_config.client_name_config.kafka_client_position,
                    topic_separator: config.kafka.topic_index_config.client_name_config.topic_separator.clone(),
                },
            }),
        }
    }
}