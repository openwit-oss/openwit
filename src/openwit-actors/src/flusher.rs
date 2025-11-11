use tokio::sync::mpsc::Receiver;
use std::fs;
use tokio::task;
use tracing::{info, error};
use crate::kafka_messages::MemtableFlushMessage;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

pub struct FlushActor;

impl FlushActor {
    pub async fn run(mut rx: Receiver<MemtableFlushMessage>, output_dir: String) {
        while let Some(msg) = rx.recv().await {
            let folder = format!("{}/{}", output_dir, msg.topic);
            if let Err(e) = fs::create_dir_all(&folder) {
                error!(?e, "Failed to create output folder: {folder}");
                continue;
            }

            let file_path = format!("{}/batch_{:05}.parquet", folder, msg.batch_number);
            let batch = msg.batch.clone();

            task::spawn_blocking(move || {
                match fs::File::create(&file_path) {
                    Ok(file) => {
                        let props = WriterProperties::builder().build();
                        match ArrowWriter::try_new(file, batch.schema(), Some(props)) {
                            Ok(mut writer) => {
                                if let Err(e) = writer.write(&batch) {
                                    error!(?e, "Failed to write batch to Parquet");
                                }
                                if let Err(e) = writer.close() {
                                    error!(?e, "Failed to close Parquet writer");
                                } else {
                                    info!("✅ Flushed Parquet batch to {}", file_path);
                                }
                            }
                            Err(e) => error!(?e, "Failed to create ArrowWriter"),
                        }
                    }
                    Err(e) => error!(?e, "Failed to create file: {}", file_path),
                }
            });
        }
    }
}
