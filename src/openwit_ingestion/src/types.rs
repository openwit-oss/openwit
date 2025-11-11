use serde::{Serialize, Deserialize};
// opentelemetry_proto removed - unused OTLP imports
use std::time::{SystemTime, UNIX_EPOCH};

/// Unified message type for all ingestion sources
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestedMessage {
    /// Unique identifier for deduplication
    pub id: String,
    /// Timestamp when message was received
    pub received_at: u64,
    /// Size in bytes for memory accounting
    pub size_bytes: usize,
    /// Source of the message
    pub source: MessageSource,
    /// Actual payload
    pub payload: MessagePayload,
    /// Index name for data organization
    pub index_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MessageSource {
    Kafka {
        topic: String,
        partition: i32,
        offset: i64,
    },
    Grpc {
        peer_addr: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MessagePayload {
    Trace(Vec<u8>),    // Serialized ExportTraceServiceRequest
    Logs(Vec<u8>),     // Serialized ExportLogsServiceRequest  
    Metrics(Vec<u8>),  // Serialized ExportMetricsServiceRequest
}

impl IngestedMessage {
    pub fn from_kafka_trace(
        topic: String,
        partition: i32,
        offset: i64,
        data: Vec<u8>,
        index_name: Option<String>,
    ) -> Self {
        let id = format!("kafka:{}:{}:{}", topic, partition, offset);
        
        Self {
            id,
            received_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            size_bytes: data.len(),
            source: MessageSource::Kafka { topic, partition, offset },
            payload: MessagePayload::Trace(data),
            index_name,
        }
    }

    // from_grpc_trace removed - unused OTLP method
}

/// Configuration for ingestion (can be updated by control plane)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestionConfig {
    /// Maximum memory for batching buffer (in bytes)
    pub max_buffer_memory: usize,
    /// Target batch size before flush
    pub batch_size: usize,
    /// Maximum time to hold a batch before flush (ms)
    pub batch_timeout_ms: u64,
    // WAL configuration removed - no longer needed
}

impl Default for IngestionConfig {
    fn default() -> Self {
        Self {
            max_buffer_memory: 2 * 1024 * 1024 * 1024,  // 2GB (increased from 256MB)
            batch_size: 5000,                           // 5k messages per batch (increased from 1k)
            batch_timeout_ms: 2000,                     // 2 seconds (reduced from 5s for faster flushing)
            // WAL configuration removed
        }
    }
}

impl IngestionConfig {
    /// Create from unified config
    pub fn from_unified(config: &openwit_config::UnifiedConfig) -> Self {
        Self {
            max_buffer_memory: config.memory.buffer_pool.size_mb as usize * 1024 * 1024,
            batch_size: config.processing.as_ref()
                .map(|p| p.buffer.flush_size_messages as usize)
                .unwrap_or(10000), // Default 10K messages
            batch_timeout_ms: config.processing.as_ref()
                .map(|p| p.buffer.flush_interval_seconds as u64 * 1000)
                .unwrap_or(30000), // Default 30 seconds
            // WAL configuration removed
        }
    }
    
    /// High-performance configuration optimized for 700k+ messages/second
    pub fn high_throughput() -> Self {
        Self {
            max_buffer_memory: 8 * 1024 * 1024 * 1024,  // 8GB
            batch_size: 50_000,                          // 50k messages per batch
            batch_timeout_ms: 100,                       // 100ms timeout
            // WAL configuration removed
        }
    }

    /// Optimized for sustained high load testing
    pub fn sustained_load() -> Self {
        Self {
            max_buffer_memory: 4 * 1024 * 1024 * 1024,   // 4GB
            batch_size: 20_000,                          // 20k messages per batch
            batch_timeout_ms: 200,                       // 200ms timeout
            // WAL configuration removed
        }
    }

    /// Configuration for memory stress testing
    pub fn memory_stress() -> Self {
        Self {
            max_buffer_memory: 16 * 1024 * 1024 * 1024,  // 16GB
            batch_size: 100_000,                         // 100k messages per batch
            batch_timeout_ms: 50,                        // 50ms timeout
            // WAL configuration removed
        }
    }
}

// IngestionMetricsSnapshot removed - monitoring not required at this stage

/// Commands from control plane
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ControlCommand {
    UpdateConfig(IngestionConfig),
    FlushBuffer,
    PauseIngestion,
    ResumeIngestion,
    Shutdown,
}

// WAL acknowledgment removed - no longer needed
/*
#[derive(Debug, Clone)]
pub struct WalAck {
    pub batch_id: String,
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub success: bool,
}
*/