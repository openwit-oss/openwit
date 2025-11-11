// Core modules
pub mod arrow_flight_receiver;
pub mod storage_processor;
pub mod parquet_file_manager;
pub mod cloud_upload;

// Query modules
pub mod query_cache;
pub mod metadata_cache;
pub mod simple_query_executor;
pub mod simple_grpc_service;

// Schema management
pub mod schema_evolution;
pub mod otlp_ingestion;
pub mod json_to_kv_converter;

// Compatibility stubs (minimal implementations to avoid breaking other crates)
pub mod config;

// Re-exports
pub use arrow_flight_receiver::StorageArrowFlightReceiver;
pub use storage_processor::StorageProcessor;
pub use parquet_file_manager::ParquetFileManager;
pub use simple_query_executor::SimpleQueryExecutor;
pub use simple_grpc_service::{SimpleStorageQueryService, start_simple_grpc_query_service};