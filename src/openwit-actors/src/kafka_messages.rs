use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
 use arrow::record_batch::RecordBatch;
use serde::Serialize;

#[derive(Debug)]
pub struct KafkaTraceMessage {
    pub topic: String,
    pub key: Option<String>,
    pub export_req: ExportTraceServiceRequest,
}

#[derive(Debug)]
pub struct IngestTraceForPartition {
    pub topic: String,
    pub partition: i32,
    pub data: ExportTraceServiceRequest,
}

#[derive(Debug)]
pub struct MemtableFlushMessage {
    pub topic: String,
    pub batch: RecordBatch,
    pub batch_number: usize,
}

#[derive(Debug, Serialize)]
pub struct ResourceReportMessage {
    pub name: String,
    pub usage_percent: f64,
}

