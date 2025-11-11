use tokio::sync::mpsc;
use chrono::Utc;
use serde_json::json;
use tracing::{info, warn};
use std::time::{SystemTime, UNIX_EPOCH};
use prost::Message;

use openwit_ingestion::{IngestedMessage, MessageSource, IngestPayload as MessagePayload};
use opentelemetry_proto::tonic::{
    collector::logs::v1::ExportLogsServiceRequest,
    collector::metrics::v1::ExportMetricsServiceRequest,
    collector::trace::v1::ExportTraceServiceRequest,
    metrics::v1 as otlp_metrics,
    common::v1::any_value,
};

/// Convert OTLP logs to IngestedMessage format
pub async fn process_otlp_logs(
    logs: ExportLogsServiceRequest,
    ingest_tx: &mpsc::Sender<IngestedMessage>,
) -> Result<usize, Box<dyn std::error::Error>> {
    let mut count = 0;
    let total_logs = logs.resource_logs.iter()
        .flat_map(|rl| &rl.scope_logs)
        .flat_map(|sl| &sl.log_records)
        .count();
    
    info!(" OTLP LOGS INGESTION: Processing {} log records", total_logs);
    
    for resource_logs in logs.resource_logs {
        let resource_attrs = extract_resource_attributes(&resource_logs.resource);
        
        for scope_logs in resource_logs.scope_logs {
            let scope_name = scope_logs.scope.as_ref().map(|s| s.name.clone()).unwrap_or_default();
            
            for log_record in scope_logs.log_records {
                // Extract index_name from attributes if present
                let mut index_name = None;
                for attr in &log_record.attributes {
                    if attr.key == "index_name" {
                        match extract_any_value(&attr.value) {
                            serde_json::Value::String(idx) => {
                                index_name = Some(idx);
                                break;
                            }
                            _ => {}
                        }
                    }
                }
                
                let mut payload = json!({
                    "timestamp": format_timestamp(log_record.time_unix_nano),
                    "observed_timestamp": format_timestamp(log_record.observed_time_unix_nano),
                    "severity_text": log_record.severity_text,
                    "severity_number": log_record.severity_number,
                    "body": extract_any_value(&log_record.body),
                    "scope": scope_name,
                    "flags": log_record.flags,
                });
                
                // Add resource attributes
                if let serde_json::Value::Object(ref mut map) = payload {
                    map.insert("resource".to_string(), resource_attrs.clone());
                    
                    // Add log attributes
                    let mut attrs = serde_json::Map::new();
                    for attr in &log_record.attributes {
                        attrs.insert(attr.key.clone(), extract_any_value(&attr.value));
                    }
                    map.insert("attributes".to_string(), serde_json::Value::Object(attrs));
                    
                    // Add trace context if present
                    if !log_record.trace_id.is_empty() {
                        map.insert("trace_id".to_string(), json!(hex::encode(&log_record.trace_id)));
                    }
                    if !log_record.span_id.is_empty() {
                        map.insert("span_id".to_string(), json!(hex::encode(&log_record.span_id)));
                    }
                }
                
                let payload_bytes = payload.to_string().into_bytes();
                let message = IngestedMessage {
                    id: format!("grpc:logs:{}:{}", Utc::now().timestamp_millis(), count),
                    received_at: SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as u64,
                    size_bytes: payload_bytes.len(),
                    source: MessageSource::Grpc { peer_addr: "unknown".to_string() },
                    payload: MessagePayload::Logs(payload_bytes),
                    index_name,
                };
                
                if let Err(e) = ingest_tx.send(message).await {
                    warn!("Failed to send log message to ingestion: {}", e);
                } else {
                    count += 1;
                    if count == 1 || count % 100 == 0 {
                        info!("  └─ Sent {} log messages to ingestion pipeline", count);
                    }
                }
            }
        }
    }
    
    Ok(count)
}

/// Convert OTLP traces to IngestedMessage format
pub async fn process_otlp_traces(
    traces: ExportTraceServiceRequest,
    ingest_tx: &mpsc::Sender<IngestedMessage>,
) -> Result<usize, Box<dyn std::error::Error>> {
    let mut count = 0;
    
    // Extract index_name from resource attributes if present
    let mut index_name = None;
    if let Some(first_resource) = traces.resource_spans.first() {
        if let Some(resource) = &first_resource.resource {
            for attr in &resource.attributes {
                if attr.key == "index_name" {
                    match extract_any_value(&attr.value) {
                        serde_json::Value::String(idx) => {
                            index_name = Some(idx);
                            break;
                        }
                        _ => {}
                    }
                }
            }
        }
    }
    
    // For now, we'll convert the entire request to a single message
    // In production, you might want to split by resource or scope
    let payload_bytes = traces.encode_to_vec();
    let trace_id = traces.resource_spans.first()
        .and_then(|rs| rs.scope_spans.first())
        .and_then(|ss| ss.spans.first())
        .map(|s| hex::encode(&s.trace_id))
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    
    let message = IngestedMessage {
        id: format!("grpc:trace:{}", trace_id),
        received_at: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64,
        size_bytes: payload_bytes.len(),
        source: MessageSource::Grpc { peer_addr: "unknown".to_string() },
        payload: MessagePayload::Trace(payload_bytes),
        index_name,
    };
    
    if let Err(e) = ingest_tx.send(message).await {
        warn!("Failed to send trace to ingestion: {}", e);
    } else {
        count = 1;
    }
    
    Ok(count)
}

/// Convert OTLP metrics to IngestedMessage format
pub async fn process_otlp_metrics(
    metrics: ExportMetricsServiceRequest,
    ingest_tx: &mpsc::Sender<IngestedMessage>,
) -> Result<usize, Box<dyn std::error::Error>> {
    let mut count = 0;
    
    // For now, convert each metric to a separate message as JSON
    for resource_metrics in metrics.resource_metrics {
        let resource_attrs = extract_resource_attributes(&resource_metrics.resource);
        
        // Extract index_name from resource attributes if present
        let mut index_name = None;
        if let Some(resource) = &resource_metrics.resource {
            for attr in &resource.attributes {
                if attr.key == "index_name" {
                    match extract_any_value(&attr.value) {
                        serde_json::Value::String(idx) => {
                            index_name = Some(idx.clone());
                            break;
                        }
                        _ => {}
                    }
                }
            }
        }
        
        for scope_metrics in resource_metrics.scope_metrics {
            let scope_name = scope_metrics.scope.as_ref().map(|s| s.name.clone()).unwrap_or_default();
            
            for metric in scope_metrics.metrics {
                let metric_name = metric.name.clone();
                
                // Create a JSON representation of the metric
                let payload = json!({
                    "metric_name": metric_name,
                    "unit": metric.unit,
                    "description": metric.description,
                    "scope": scope_name,
                    "resource": resource_attrs,
                    "data": serialize_metric_data(&metric.data),
                });
                
                let payload_bytes = payload.to_string().into_bytes();
                let message = IngestedMessage {
                    id: format!("grpc:metric:{}:{}:{}", metric_name, Utc::now().timestamp_millis(), count),
                    received_at: SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as u64,
                    size_bytes: payload_bytes.len(),
                    source: MessageSource::Grpc { peer_addr: "unknown".to_string() },
                    payload: MessagePayload::Metrics(payload_bytes),
                    index_name: index_name.clone(),
                };
                
                if let Err(e) = ingest_tx.send(message).await {
                    warn!("Failed to send metric to ingestion: {}", e);
                } else {
                    count += 1;
                }
            }
        }
    }
    
    Ok(count)
}

// Helper functions

fn extract_resource_attributes(resource: &Option<opentelemetry_proto::tonic::resource::v1::Resource>) -> serde_json::Value {
    if let Some(res) = resource {
        let mut attrs = serde_json::Map::new();
        for attr in &res.attributes {
            attrs.insert(attr.key.clone(), extract_any_value(&attr.value));
        }
        serde_json::Value::Object(attrs)
    } else {
        json!({})
    }
}

fn extract_any_value(value: &Option<opentelemetry_proto::tonic::common::v1::AnyValue>) -> serde_json::Value {
    match value {
        Some(av) => match &av.value {
            Some(any_value::Value::StringValue(s)) => json!(s),
            Some(any_value::Value::BoolValue(b)) => json!(b),
            Some(any_value::Value::IntValue(i)) => json!(i),
            Some(any_value::Value::DoubleValue(d)) => json!(d),
            Some(any_value::Value::ArrayValue(arr)) => {
                json!(arr.values.iter().map(|v| extract_any_value(&Some(v.clone()))).collect::<Vec<_>>())
            }
            Some(any_value::Value::KvlistValue(kv)) => {
                let mut map = serde_json::Map::new();
                for pair in &kv.values {
                    map.insert(pair.key.clone(), extract_any_value(&pair.value));
                }
                serde_json::Value::Object(map)
            }
            Some(any_value::Value::BytesValue(b)) => {
                use base64::{Engine as _, engine::general_purpose::STANDARD};
                json!(STANDARD.encode(b))
            }
            None => json!(null),
        },
        None => json!(null),
    }
}

fn format_timestamp(nanos: u64) -> String {
    if nanos == 0 {
        return Utc::now().to_rfc3339();
    }
    let secs = (nanos / 1_000_000_000) as i64;
    let nanos = (nanos % 1_000_000_000) as u32;
    chrono::DateTime::<Utc>::from_timestamp(secs, nanos)
        .map(|dt| dt.to_rfc3339())
        .unwrap_or_else(|| Utc::now().to_rfc3339())
}

fn serialize_metric_data(data: &Option<otlp_metrics::metric::Data>) -> serde_json::Value {
    match data {
        Some(otlp_metrics::metric::Data::Gauge(gauge)) => {
            json!({
                "type": "gauge",
                "data_points": gauge.data_points.iter().map(|dp| {
                    json!({
                        "time": format_timestamp(dp.time_unix_nano),
                        "value": extract_number_value(&dp.value),
                        "attributes": dp.attributes.iter().map(|attr| {
                            (attr.key.clone(), extract_any_value(&attr.value))
                        }).collect::<serde_json::Map<_, _>>(),
                    })
                }).collect::<Vec<_>>()
            })
        }
        Some(otlp_metrics::metric::Data::Sum(sum)) => {
            json!({
                "type": if sum.is_monotonic { "counter" } else { "sum" },
                "is_monotonic": sum.is_monotonic,
                "aggregation_temporality": sum.aggregation_temporality,
                "data_points": sum.data_points.iter().map(|dp| {
                    json!({
                        "time": format_timestamp(dp.time_unix_nano),
                        "value": extract_number_value(&dp.value),
                        "attributes": dp.attributes.iter().map(|attr| {
                            (attr.key.clone(), extract_any_value(&attr.value))
                        }).collect::<serde_json::Map<_, _>>(),
                    })
                }).collect::<Vec<_>>()
            })
        }
        Some(otlp_metrics::metric::Data::Histogram(hist)) => {
            json!({
                "type": "histogram",
                "aggregation_temporality": hist.aggregation_temporality,
                "data_points": hist.data_points.iter().map(|dp| {
                    json!({
                        "time": format_timestamp(dp.time_unix_nano),
                        "count": dp.count,
                        "sum": dp.sum,
                        "min": dp.min,
                        "max": dp.max,
                        "bucket_counts": dp.bucket_counts,
                        "explicit_bounds": dp.explicit_bounds,
                        "attributes": dp.attributes.iter().map(|attr| {
                            (attr.key.clone(), extract_any_value(&attr.value))
                        }).collect::<serde_json::Map<_, _>>(),
                    })
                }).collect::<Vec<_>>()
            })
        }
        _ => json!(null),
    }
}

fn extract_number_value(value: &Option<otlp_metrics::number_data_point::Value>) -> serde_json::Value {
    match value {
        Some(otlp_metrics::number_data_point::Value::AsDouble(d)) => json!(d),
        Some(otlp_metrics::number_data_point::Value::AsInt(i)) => json!(i),
        None => json!(null),
    }
}