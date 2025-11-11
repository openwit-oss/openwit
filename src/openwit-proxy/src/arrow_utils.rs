use anyhow::Result;
use arrow::array::{
    StringBuilder, TimestampNanosecondArray, ArrayRef,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

use crate::proxy_buffer::ProxyMessage;

/// Convert proxy messages to Arrow RecordBatch for efficient transfer
pub fn convert_to_arrow_batch(messages: &[ProxyMessage]) -> Result<RecordBatch> {
    // Define schema for the batch
    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
        Field::new("source", DataType::Utf8, false),
        Field::new("message_type", DataType::Utf8, false),
        Field::new("trace_id", DataType::Utf8, true),
        Field::new("span_id", DataType::Utf8, true),
        Field::new("severity", DataType::Utf8, true),
        Field::new("body", DataType::Utf8, false),
        Field::new("resource_attributes", DataType::Utf8, true), // JSON string
        Field::new("log_attributes", DataType::Utf8, true), // JSON string
    ]));
    
    // Build arrays for each field
    let mut timestamp_builder = TimestampNanosecondArray::builder(messages.len());
    let mut source_builder = StringBuilder::new();
    let mut message_type_builder = StringBuilder::new();
    let mut trace_id_builder = StringBuilder::new();
    let mut span_id_builder = StringBuilder::new();
    let mut severity_builder = StringBuilder::new();
    let mut body_builder = StringBuilder::new();
    let mut resource_attrs_builder = StringBuilder::new();
    let mut log_attrs_builder = StringBuilder::new();
    
    for msg in messages {
        // Convert timestamp
        let timestamp_ns = msg.timestamp.timestamp_nanos_opt()
            .ok_or_else(|| anyhow::anyhow!("Invalid timestamp"))?;
        timestamp_builder.append_value(timestamp_ns);
        
        // Source
        let source_str = match &msg.source {
            crate::proxy_buffer::MessageSource::Grpc { endpoint } => format!("grpc:{}", endpoint),
            crate::proxy_buffer::MessageSource::Http { endpoint, method } => format!("http:{}:{}", method, endpoint),
            crate::proxy_buffer::MessageSource::Kafka { topic, partition, offset } => format!("kafka:{}:{}:{}", topic, partition, offset),
        };
        source_builder.append_value(&source_str);
        
        // For now, treat all messages as logs since we just have raw bytes
        message_type_builder.append_value("log");
        
        // Extract metadata fields
        if let Some(trace_id) = msg.metadata.get("trace_id") {
            trace_id_builder.append_value(trace_id);
        } else {
            trace_id_builder.append_null();
        }
        
        if let Some(span_id) = msg.metadata.get("span_id") {
            span_id_builder.append_value(span_id);
        } else {
            span_id_builder.append_null();
        }
        
        if let Some(severity) = msg.metadata.get("severity") {
            severity_builder.append_value(severity);
        } else {
            severity_builder.append_null();
        }
        
        // Body - convert bytes to string
        let body_str = String::from_utf8_lossy(&msg.data);
        body_builder.append_value(&body_str);
        
        // Resource attributes - serialize metadata as JSON
        let resource_attrs = serde_json::to_string(&msg.metadata).unwrap_or_else(|_| "{}".to_string());
        resource_attrs_builder.append_value(&resource_attrs);
        
        // Log attributes - empty for now
        log_attrs_builder.append_value("{}");
    }
    
    // Create arrays
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(timestamp_builder.finish()),
        Arc::new(source_builder.finish()),
        Arc::new(message_type_builder.finish()),
        Arc::new(trace_id_builder.finish()),
        Arc::new(span_id_builder.finish()),
        Arc::new(severity_builder.finish()),
        Arc::new(body_builder.finish()),
        Arc::new(resource_attrs_builder.finish()),
        Arc::new(log_attrs_builder.finish()),
    ];
    
    // Create record batch
    RecordBatch::try_new(schema, arrays)
        .map_err(|e| anyhow::anyhow!("Failed to create RecordBatch: {}", e))
}