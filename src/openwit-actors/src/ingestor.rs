use crate::kafka_messages::KafkaTraceMessage;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::info;

/// This actor receives externally forwarded messages (e.g., from `/forward-alert`)
/// and sends them to the memtable ingestion pipeline.
pub struct IngestorActor;

impl IngestorActor {
    pub async fn run(mut rx: Receiver<KafkaTraceMessage>, mem_tx: Sender<KafkaTraceMessage>) {
        info!("IngestorActor started.");
        while let Some(msg) = rx.recv().await {
            info!("IngestorActor received: topic={}", msg.topic);

            if let Err(e) = mem_tx.send(msg).await {
                tracing::error!("Failed to forward to memtable: {:?}", e);
            }
        }
    }
}

