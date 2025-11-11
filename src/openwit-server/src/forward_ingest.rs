use axum::{Json, response::IntoResponse, http::StatusCode};
use serde::Deserialize;
use tracing::info;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;

use std::sync::OnceLock;
use tokio::sync::mpsc::Sender;
use openwit_actors::kafka_messages::KafkaTraceMessage;

static INGESTOR_TX: OnceLock<Sender<KafkaTraceMessage>> = OnceLock::new();

pub fn set_ingestor_sender(sender: Sender<KafkaTraceMessage>) {
    let _ = INGESTOR_TX.set(sender);
}

#[derive(Debug, Deserialize)]
pub struct ForwardedIngestPayload {
    pub reason: String,
    pub resource: String,
    pub usage_percent: u8,
}

pub async fn handle_forwarded_ingest(
    Json(payload): Json<ForwardedIngestPayload>,
) -> impl IntoResponse {
    info!("Received forwarded ingest: {:?}", payload);

    let fake_trace = KafkaTraceMessage {
        topic: "forwarded".to_string(),
        key: Some(payload.resource.clone()),
        export_req: ExportTraceServiceRequest::default(), // Placeholder
    };

    if let Some(sender) = INGESTOR_TX.get() {
        if let Err(e) = sender.send(fake_trace).await {
            tracing::error!("Failed to send to IngestorActor: {:?}", e);
            return StatusCode::INTERNAL_SERVER_ERROR;
        }
    } else {
        tracing::warn!("Ingestor sender not initialized");
        return StatusCode::SERVICE_UNAVAILABLE;
    }

    StatusCode::OK
}