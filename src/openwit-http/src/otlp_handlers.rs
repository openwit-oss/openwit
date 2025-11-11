use axum::{
    extract::{Json, State},
    http::StatusCode,
    response::IntoResponse,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::{info, debug, warn};
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use tokio::sync::RwLock;

use crate::error::Result;
use crate::ingestion_client::IngestionClient;


// Request types for OTLP HTTP JSON format
#[derive(Debug, Deserialize, Serialize)]
pub struct OtlpTracesRequest {
    #[serde(rename = "resourceSpans")]
    pub resource_spans: Vec<Value>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct OtlpLogsRequest {
    #[serde(rename = "resourceLogs")]
    pub resource_logs: Vec<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index_name: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct OtlpMetricsRequest {
    #[serde(rename = "resourceMetrics")]
    pub resource_metrics: Vec<Value>,
}

#[derive(Debug, Serialize)]
pub struct OtlpResponse {
    pub status: String,
    pub message: String,
}

// Shared state for handlers
#[derive(Clone)]
pub struct HandlerState {
    pub ingest_tx: Option<Arc<RwLock<Sender<openwit_ingestion::IngestedMessage>>>>,
    pub ingestion_client: Option<Arc<IngestionClient>>,
}

impl HandlerState {
    pub fn new(
        ingest_tx: Option<Sender<openwit_ingestion::IngestedMessage>>,
        ingestion_client: Option<IngestionClient>,
    ) -> Self {
        Self {
            ingest_tx: ingest_tx.map(|tx| Arc::new(RwLock::new(tx))),
            ingestion_client: ingestion_client.map(Arc::new),
        }
    }
}

// Handler for /v1/traces
pub async fn handle_traces(
    State(state): State<HandlerState>,
    Json(payload): Json<OtlpTracesRequest>,
) -> Result<impl IntoResponse> {
    info!("Received OTLP traces via HTTP");
    
    // Forward to ingestion node if client is available
    if let Some(client) = &state.ingestion_client {
        let traces_value = serde_json::to_value(&payload)?;
        if let Err(e) = client.send_traces(traces_value).await {
            warn!("Failed to forward traces to ingestion node: {}", e);
            // Continue processing even if forwarding fails
        } else {
            info!("Successfully forwarded traces to ingestion node");
        }
    }
    
    let mut span_count = 0;
    
    // Process traces
    for (idx, resource_span) in payload.resource_spans.iter().enumerate() {
        info!("Processing ResourceSpan #{}", idx);
        
        // Extract service name if available
        if let Some(resource) = resource_span.get("resource") {
            if let Some(attributes) = resource.get("attributes") {
                if let Some(attrs_array) = attributes.as_array() {
                    for attr in attrs_array {
                        if let Some(key) = attr.get("key").and_then(|k| k.as_str()) {
                            if key == "service.name" {
                                if let Some(value) = attr.get("value").and_then(|v| v.get("stringValue")).and_then(|s| s.as_str()) {
                                    info!("  Service: {}", value);
                                }
                            }
                        }
                    }
                }
            }
        }
        
        // Process spans
        if let Some(scope_spans) = resource_span.get("scopeSpans") {
            if let Some(scope_spans_array) = scope_spans.as_array() {
                for scope_span in scope_spans_array {
                    if let Some(spans) = scope_span.get("spans") {
                        if let Some(spans_array) = spans.as_array() {
                            info!("  Found {} spans", spans_array.len());
                            span_count += spans_array.len();
                            for span in spans_array {
                                if let Some(trace_id) = span.get("traceId").and_then(|t| t.as_str()) {
                                    debug!("Successfully ingested trace - TraceID: {} (HTTP endpoint working correctly)", trace_id);
                                }
                                if let Some(name) = span.get("name").and_then(|n| n.as_str()) {
                                    info!("    Span: {}", name);
                                    if let Some(span_id) = span.get("spanId").and_then(|s| s.as_str()) {
                                        info!("      SpanID: {}", span_id);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    
    let response = OtlpResponse {
        status: "success".to_string(),
        message: format!("Traces received: {} spans in this request", span_count),
    };
    
    info!("Trace ingestion stats - This request: {} spans", span_count);
    
    Ok((StatusCode::OK, Json(response)))
}

// Handler for /v1/logs
pub async fn handle_logs(
    State(state): State<HandlerState>,
    Json(payload): Json<OtlpLogsRequest>,
) -> Result<impl IntoResponse> {
    info!("Received OTLP logs via HTTP");
    
    // Forward to ingestion node if client is available
    if let Some(client) = &state.ingestion_client {
        let logs_value = serde_json::to_value(&payload)?;
        if let Err(e) = client.send_logs(logs_value).await {
            warn!("Failed to forward logs to ingestion node: {}", e);
        } else {
            info!("Successfully forwarded logs to ingestion node");
        }
    }
    
    let mut log_count = 0;
    
    // Process logs
    for (idx, resource_log) in payload.resource_logs.iter().enumerate() {
        info!("Processing ResourceLog #{}", idx);
        
        // Extract service name if available
        if let Some(resource) = resource_log.get("resource") {
            if let Some(attributes) = resource.get("attributes") {
                if let Some(attrs_array) = attributes.as_array() {
                    for attr in attrs_array {
                        if let Some(key) = attr.get("key").and_then(|k| k.as_str()) {
                            if key == "service.name" {
                                if let Some(value) = attr.get("value").and_then(|v| v.get("stringValue")).and_then(|s| s.as_str()) {
                                    info!("  Service: {}", value);
                                }
                            }
                        }
                    }
                }
            }
        }
        
        // Process log records
        if let Some(scope_logs) = resource_log.get("scopeLogs") {
            if let Some(scope_logs_array) = scope_logs.as_array() {
                for scope_log in scope_logs_array {
                    if let Some(log_records) = scope_log.get("logRecords") {
                        if let Some(logs_array) = log_records.as_array() {
                            info!("  Found {} log records", logs_array.len());
                            log_count += logs_array.len();
                            for log in logs_array {
                                if let Some(body) = log.get("body").and_then(|b| b.get("stringValue")).and_then(|s| s.as_str()) {
                                    info!("    Log: {}", body);
                                    debug!("Successfully ingested log record (HTTP endpoint working correctly)");
                                }
                                if let Some(severity) = log.get("severityText").and_then(|s| s.as_str()) {
                                    info!("      Severity: {}", severity);
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    
    let response = OtlpResponse {
        status: "success".to_string(),
        message: format!("Logs received: {} log records in this request", log_count),
    };
    
    info!("Log ingestion stats - This request: {} logs", log_count);
    
    Ok((StatusCode::OK, Json(response)))
}

// Handler for /v1/metrics
pub async fn handle_metrics(
    State(state): State<HandlerState>,
    Json(payload): Json<OtlpMetricsRequest>,
) -> Result<impl IntoResponse> {
    info!("Received OTLP metrics via HTTP");
    
    // Forward to ingestion node if client is available
    if let Some(client) = &state.ingestion_client {
        let metrics_value = serde_json::to_value(&payload)?;
        if let Err(e) = client.send_metrics(metrics_value).await {
            warn!("Failed to forward metrics to ingestion node: {}", e);
        } else {
            info!("Successfully forwarded metrics to ingestion node");
        }
    }
    
    let mut metric_count = 0;
    
    // Process metrics
    for (idx, resource_metric) in payload.resource_metrics.iter().enumerate() {
        info!("Processing ResourceMetric #{}", idx);
        
        // Extract service name if available
        if let Some(resource) = resource_metric.get("resource") {
            if let Some(attributes) = resource.get("attributes") {
                if let Some(attrs_array) = attributes.as_array() {
                    for attr in attrs_array {
                        if let Some(key) = attr.get("key").and_then(|k| k.as_str()) {
                            if key == "service.name" {
                                if let Some(value) = attr.get("value").and_then(|v| v.get("stringValue")).and_then(|s| s.as_str()) {
                                    info!("  Service: {}", value);
                                }
                            }
                        }
                    }
                }
            }
        }
        
        // Process metrics
        if let Some(scope_metrics) = resource_metric.get("scopeMetrics") {
            if let Some(scope_metrics_array) = scope_metrics.as_array() {
                for scope_metric in scope_metrics_array {
                    if let Some(metrics) = scope_metric.get("metrics") {
                        if let Some(metrics_array) = metrics.as_array() {
                            info!("  Found {} metrics", metrics_array.len());
                            metric_count += metrics_array.len();
                            for metric in metrics_array {
                                if let Some(name) = metric.get("name").and_then(|n| n.as_str()) {
                                    info!("    Metric: {}", name);
                                    debug!("Successfully ingested metric: {} (HTTP endpoint working correctly)", name);
                                    if let Some(desc) = metric.get("description").and_then(|d| d.as_str()) {
                                        info!("      Description: {}", desc);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    
    let response = OtlpResponse {
        status: "success".to_string(),
        message: format!("Metrics received: {} metrics in this request", metric_count),
    };
    
    info!("Metric ingestion stats - This request: {} metrics", metric_count);
    
    Ok((StatusCode::OK, Json(response)))
}