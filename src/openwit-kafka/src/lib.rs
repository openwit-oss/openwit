pub mod kafka_server;
pub mod kafka_consumer;
pub mod high_performance_consumer;
pub mod types;
pub mod grpc_client;
pub mod batch_tracker;
pub mod client_batch_manager;
pub mod client_extractor;
pub mod wal_writer;

// Safe commit implementations
pub mod safe_kafka_consumer;
pub mod safe_commit_consumer;
pub mod kafka_consumer_safe;

pub use kafka_server::KafkaServer;
pub use kafka_consumer::KafkaConsumer;
pub use high_performance_consumer::{HighPerformanceKafkaConsumer, HighPerfKafkaConfig};
pub use types::{KafkaMessage, KafkaConfig};
pub use batch_tracker::{BatchTracker, BatchRecord};
pub use client_batch_manager::{ClientBatchManager, CompletedBatch, BufferStats};
pub use client_extractor::ClientExtractor;
pub use wal_writer::{WalWriter, WalHeader, WalEntry, WalBatch};

// Safe commit exports
pub use safe_kafka_consumer::{SafeKafkaConsumer, OffsetCommitMode};
pub use safe_commit_consumer::{SafeCommitKafkaConsumer, CommitStats};
pub use kafka_consumer_safe::KafkaConsumerSafe;