pub mod messages;
pub mod monitor;
pub mod decision;
pub mod scale;
// pub mod kafka; // Removed: Only proxy consumes from Kafka
pub mod memtable;
pub mod flusher;
pub mod kafka_messages;
pub mod memschema;
pub mod ingestor;
pub mod clusteractor;
pub mod heartbeat;
// simple_ingest removed - uses old IngestionPipeline
// pub mod simple_ingest;
pub mod storage;
pub mod indexer;
pub mod janitor;
// ingestion removed - uses old BatchingBuffer
// pub mod ingestion;
// pipeline removed - depends on ingestion module
// pub mod pipeline;

use crate::clusteractor::{spawn_cluster_actor, ClusterCommand}; // Make sure this is imported
use openwit_network::{ClusterHandle};
use openwit_common::messages::ClusterMessage;

use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::mpsc::{self, channel, Sender, Receiver};
use tokio::task;

use crate::monitor::start_monitor_actor;
use crate::decision::start_decision_actor;
use crate::scale::ScaleActor;

// Removed unused imports
use crate::messages::{ResourceReportMessage, ScaleCommand};
// Removed unused imports
// use crate::simple_ingest::SimpleIngestActor;
// use openwit_ingestion::{IngestedMessage, ControlCommand};


use openwit_config::UnifiedConfig;
use opendal::Operator;

/// Spawns the monitor actor and returns a receiver for `ResourceReportMessage`s
pub fn spawn_monitors() -> Receiver<ResourceReportMessage> {
    let (tx, rx) = channel(32);
    let threshold = 30;

    task::spawn(start_monitor_actor(tx, threshold));
    rx
}

/// Spawns the decision actor given a ResourceReportMessage stream
pub fn spawn_decision_actor(alert_rx: Receiver<ResourceReportMessage>) -> Receiver<ScaleCommand> {
    let (scale_tx, scale_rx) = channel(32);
    task::spawn(start_decision_actor(alert_rx, scale_tx));
    scale_rx
}

/// Spawns the scale actor that listens for scale commands
pub async fn spawn_scale_actor(
    scale_rx: Receiver<ScaleCommand>,
    cluster: ClusterHandle,
) -> ClusterHandle {
    let scale_actor = Arc::new(ScaleActor::new(cluster.self_id.clone(), cluster.clone()));

    // Clone `Arc` into the task to ensure it lives long enough
    let actor = scale_actor.clone();
    tokio::task::spawn(async move {
        actor.run(scale_rx).await;
    });

    cluster
}


// Re-export new actor types and spawn functions
pub use storage::{StorageActor, StorageMessage, StorageStats, spawn_storage_actor};
pub use indexer::{IndexerActor, IndexerMessage, IndexerStats, spawn_indexer_actor};
pub use janitor::{JanitorActor, JanitorMessage, JanitorConfig, JanitorStats, spawn_janitor_actor};
// pub use ingestion::{IngestionActor, IngestionActorMessage, IngestionActorResponse, IngestionStats, spawn_ingestion_actor};
// pub use pipeline::PipelineCoordinator;

/// Spawn storage components including storage actor
pub fn spawn_storage_components(
    config: openwit_storage::config::StorageConfig,
    metastore: Arc<dyn openwit_metastore::MetaStore>,
    segment_tx: mpsc::Sender<String>,
    cloud_operator: Option<Operator>,
) -> mpsc::Sender<StorageMessage> {
    tracing::info!("Storage components starting...");
    spawn_storage_actor(config, metastore, segment_tx, cloud_operator)
}

/// Spawn indexer components including indexer actor
pub fn spawn_indexer_components(
    metastore: Arc<dyn openwit_metastore::MetaStore>,
    index_path: String,
) -> mpsc::Sender<IndexerMessage> {
    tracing::info!("Indexing components starting...");
    spawn_indexer_actor(metastore, index_path)
}

/// Spawn janitor components for cleanup and maintenance
pub fn spawn_janitor_components(
    config: JanitorConfig,
    metastore: Arc<dyn openwit_metastore::MetaStore>,
) -> mpsc::Sender<JanitorMessage> {
    tracing::info!("Janitor components starting...");
    spawn_janitor_actor(config, metastore)
}

pub fn spawn_query_server() {
    tracing::info!("Query engine starting...");
    // TODO: bind Axum SQL endpoint or grpc query API
}





// spawn_ingestion_pipeline removed - uses SimpleIngestActor which depends on removed IngestionPipeline
// /// Spawns the simplified ingestion pipeline using openwit_ingestion
// pub async fn spawn_ingestion_pipeline(
//     config: Arc<UnifiedConfig>,
//     node_id: String,
// ) -> (Sender<ControlCommand>, Receiver<Vec<IngestedMessage>>) {
//     // Create channels
//     let (control_tx, control_rx) = channel::<ControlCommand>(32);
//     let (storage_tx, storage_rx) = channel::<Vec<IngestedMessage>>(100);
//     
//     // Create and spawn the simple ingest actor
//     let actor = SimpleIngestActor::new(
//         node_id,
//         config,
//         control_rx,
//         storage_tx,
//     );
//     
//     task::spawn(async move {
//         if let Err(e) = actor.run().await {
//             tracing::error!("Ingestion pipeline failed: {:?}", e);
//         }
//     });
//     
//     (control_tx, storage_rx)
// }

pub fn spawn_cluster_runtime(
    self_node_name: String,
    gossip_addr: SocketAddr,
    seed_nodes: Vec<SocketAddr>,
    role: String,
    ingest_tx: Sender<ClusterMessage>,
) -> Sender<ClusterCommand> {
    spawn_cluster_actor(self_node_name, gossip_addr, seed_nodes, role, ingest_tx)
}





