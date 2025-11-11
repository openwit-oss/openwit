// Core modules
pub mod types;
pub mod storage_grpc_client;
pub mod query_engine;
pub mod search_main;
pub mod azure_query;
pub mod batch_monitoring_api;

// Core exports
pub use types::{SearchQuery, SearchResponse};
pub use query_engine::{QueryEngineV2 as QueryEngine, QueryEngineConfig};
pub use search_main::{main_v2, run_search_service_v2};
pub use batch_monitoring_api::create_batch_monitoring_router;