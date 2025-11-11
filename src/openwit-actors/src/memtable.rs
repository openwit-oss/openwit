use std::collections::HashMap;
use tokio::sync::mpsc::{Receiver, Sender};

use crate::kafka_messages::{KafkaTraceMessage, MemtableFlushMessage};
use crate::memschema::{ArrowMemTable, span_row_from_otel};

pub struct MemtableActor;

impl MemtableActor {
    pub async fn run(
        mut rx: Receiver<KafkaTraceMessage>,
        flush_tx: Sender<MemtableFlushMessage>,
        max_size_mb: usize,
    ) {
        let mut memtables: HashMap<String, (ArrowMemTable, usize)> = HashMap::new();

        tracing::info!("MemtableActor started with max_size_mb = {}", max_size_mb);

        while let Some(msg) = rx.recv().await {
            let topic = &msg.topic;
            for resource_spans in &msg.export_req.resource_spans {
                for scope_spans in &resource_spans.scope_spans {
                    for span in &scope_spans.spans {
                        let row = span_row_from_otel(resource_spans, scope_spans, span);
                        let (table, batch_no) = memtables
                            .entry(topic.clone())
                            .or_insert_with(|| (ArrowMemTable::new(), 0));

                        table.push(&row);

                        if table.estimated_memory_mb() > max_size_mb as f64 {
                            if let Some(batch) = table.flush() {
                                *batch_no += 1;
                                tracing::info!(
                                    topic = %topic,
                                    batch_number = *batch_no,
                                    "🚰 Memtable flushed (exceeded {} MB)",
                                    max_size_mb
                                );
                                let _ = flush_tx
                                    .send(MemtableFlushMessage {
                                        topic: topic.clone(),
                                        batch,
                                        batch_number: *batch_no,
                                    })
                                    .await;
                            }
                        }
                    }
                }
            }
        }

        tracing::warn!("❗ MemtableActor channel closed — exiting");
    }
}
