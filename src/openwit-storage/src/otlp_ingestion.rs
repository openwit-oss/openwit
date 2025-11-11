use std::sync::Arc;
use arrow::array::{
    ArrayRef, StringArray, Int64Array, UInt64Array, Float64Array, BooleanArray,
    Int32Array, TimestampNanosecondArray, MapArray, StringBuilder
};
use arrow::datatypes::{Schema, Field, DataType, TimeUnit};
use arrow::record_batch::RecordBatch;
use serde_json::Value;
use anyhow::{Result, Context};
use tracing::{info, debug, warn};
use std::collections::HashMap;
use chrono::Utc;
use arrow::array::MapBuilder;

/// Create the complete OTLP schema for Arrow storage
pub fn create_complete_otlp_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![

        Field::new("timestamp", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
        Field::new("message_id", DataType::Utf8, false),
        Field::new("ingestion_node", DataType::Utf8, false),
        Field::new("client_name", DataType::Utf8, false),
        Field::new("account_key", DataType::Utf8, true),
        

        Field::new("trace_id", DataType::Utf8, false),
        Field::new("span_id", DataType::Utf8, false),
        Field::new("parent_span_id", DataType::Utf8, true),
        Field::new("trace_state", DataType::Utf8, true),
        

        Field::new("span_name", DataType::Utf8, false),
        Field::new("span_kind", DataType::Int32, false),
        Field::new("start_time_unix_nano", DataType::UInt64, false),
        Field::new("end_time_unix_nano", DataType::UInt64, false),
        Field::new("duration_nanos", DataType::UInt64, false),

        Field::new("service_name", DataType::Utf8, false),
        Field::new("service_namespace", DataType::Utf8, true),
        Field::new("service_instance_id", DataType::Utf8, true),
        Field::new("service_version", DataType::Utf8, true),

        Field::new("status_code", DataType::Int32, true),
        Field::new("status_message", DataType::Utf8, true),

        Field::new("scope_name", DataType::Utf8, true),
        Field::new("scope_version", DataType::Utf8, true),
        
        // === ATTRIBUTES AS MAP (key-value pairs) ===
        // Note: MapBuilder creates fields named "keys" and "values" (plural)
        Field::new("resource_attributes", DataType::Map(
            Arc::new(Field::new("entries", DataType::Struct(vec![
                Field::new("keys", DataType::Utf8, false),   // MapBuilder uses "keys" not "key"
                Field::new("values", DataType::Utf8, true),  // MapBuilder uses "values" not "value"
            ].into()), false)),
            false // sorted_keys = false
        ), true),
        Field::new("span_attributes", DataType::Map(
            Arc::new(Field::new("entries", DataType::Struct(vec![
                Field::new("keys", DataType::Utf8, false),   // MapBuilder uses "keys" not "key"
                Field::new("values", DataType::Utf8, true),  // MapBuilder uses "values" not "value"
            ].into()), false)),
            false // sorted_keys = false
        ), true),
        Field::new("scope_attributes", DataType::Map(
            Arc::new(Field::new("entries", DataType::Struct(vec![
                Field::new("keys", DataType::Utf8, false),   // MapBuilder uses "keys" not "key"
                Field::new("values", DataType::Utf8, true),  // MapBuilder uses "values" not "value"
            ].into()), false)),
            false // sorted_keys = false
        ), true),
        
        // === EVENTS AS JSON ARRAY ===
        Field::new("events", DataType::Utf8, true),
        
        // === LINKS AS JSON ARRAY ===
        Field::new("links", DataType::Utf8, true),
        
        // === COMPUTED FIELDS FOR EFFICIENT QUERIES ===
        Field::new("is_error", DataType::Boolean, false),
        Field::new("latency_ms", DataType::Float64, false),
        Field::new("has_events", DataType::Boolean, false),
        Field::new("event_count", DataType::Int32, false),
        Field::new("link_count", DataType::Int32, false),
        
        // === COMMONLY QUERIED ATTRIBUTES (extracted for performance) ===
        Field::new("http_method", DataType::Utf8, true),
        Field::new("http_url", DataType::Utf8, true),
        Field::new("http_status_code", DataType::Int32, true),
        Field::new("http_route", DataType::Utf8, true),
        Field::new("db_system", DataType::Utf8, true),
        Field::new("db_operation", DataType::Utf8, true),
        Field::new("db_name", DataType::Utf8, true),
        Field::new("messaging_system", DataType::Utf8, true),
        Field::new("messaging_destination", DataType::Utf8, true),
        Field::new("rpc_system", DataType::Utf8, true),
        Field::new("rpc_service", DataType::Utf8, true),
        Field::new("rpc_method", DataType::Utf8, true),
        Field::new("exception_type", DataType::Utf8, true),
        Field::new("exception_message", DataType::Utf8, true),
        
        // === CLOUD/INFRASTRUCTURE ===
        Field::new("cloud_provider", DataType::Utf8, true),
        Field::new("cloud_region", DataType::Utf8, true),
        Field::new("k8s_namespace", DataType::Utf8, true),
        Field::new("k8s_pod_name", DataType::Utf8, true),
        Field::new("container_name", DataType::Utf8, true),
        Field::new("host_name", DataType::Utf8, true),
        
        // === USER/SESSION ===
        Field::new("user_id", DataType::Utf8, true),
        Field::new("session_id", DataType::Utf8, true),
    ]))
}

/// Convert OTLP JSON data to Arrow RecordBatch
pub fn process_otlp_json_batch(
    otlp_json: &Value,
    message_id: &str,
    ingestion_node: &str,
    client_name: &str,
    account_key: Option<&str>,
) -> Result<Vec<RecordBatch>> {
    let schema = create_complete_otlp_schema();
    let mut batches = Vec::new();
    
    // Parse resourceSpans
    let resource_spans = otlp_json["resourceSpans"]
        .as_array()
        .context("No resourceSpans found in OTLP data")?;
    
    debug!("Processing {} resourceSpans", resource_spans.len());
    
    for (rs_idx, resource_span) in resource_spans.iter().enumerate() {
        // Extract resource attributes once per resource
        let resource = &resource_span["resource"];
        let resource_attrs = extract_attributes(&resource["attributes"]);
        
        // Extract service info from resource
        let service_name = resource_attrs.get("service.name")
            .or_else(|| resource_attrs.get("service_name"))
            .cloned()
            .unwrap_or_else(|| "unknown".to_string());
        let service_namespace = resource_attrs.get("service.namespace").cloned();
        let service_instance_id = resource_attrs.get("service.instance.id").cloned();
        let service_version = resource_attrs.get("service.version").cloned();
        
        // Extract cloud/k8s info
        let cloud_provider = resource_attrs.get("cloud.provider").cloned();
        let cloud_region = resource_attrs.get("cloud.region").cloned();
        let k8s_namespace = resource_attrs.get("k8s.namespace.name").cloned();
        let k8s_pod_name = resource_attrs.get("k8s.pod.name").cloned();
        let container_name = resource_attrs.get("container.name").cloned();
        let host_name = resource_attrs.get("host.name").cloned();
        
        // Process scopeSpans
        let empty_scope_spans = Vec::new();
        let scope_spans = resource_span["scopeSpans"]
            .as_array()
            .unwrap_or(&empty_scope_spans);
        
        for (ss_idx, scope_span) in scope_spans.iter().enumerate() {
            // Extract scope info
            let scope = &scope_span["scope"];
            let scope_name = scope["name"].as_str().map(|s| s.to_string());
            let scope_version = scope["version"].as_str().map(|s| s.to_string());
            let scope_attrs = extract_attributes(&scope["attributes"]);
            
            // Process spans
            let empty_spans = Vec::new();
            let spans = scope_span["spans"]
                .as_array()
                .unwrap_or(&empty_spans);
            
            if spans.is_empty() {
                continue;
            }
            
            debug!("Processing {} spans in scopeSpan[{}]", spans.len(), ss_idx);
            
            // Prepare arrays for batch
            let mut timestamps = Vec::with_capacity(spans.len());
            let mut message_ids = Vec::with_capacity(spans.len());
            let mut ingestion_nodes = Vec::with_capacity(spans.len());
            let mut client_names = Vec::with_capacity(spans.len());
            let mut account_keys = Vec::with_capacity(spans.len());
            
            let mut trace_ids = Vec::with_capacity(spans.len());
            let mut span_ids = Vec::with_capacity(spans.len());
            let mut parent_span_ids = Vec::with_capacity(spans.len());
            let mut trace_states = Vec::with_capacity(spans.len());
            
            let mut span_names = Vec::with_capacity(spans.len());
            let mut span_kinds = Vec::with_capacity(spans.len());
            let mut start_times = Vec::with_capacity(spans.len());
            let mut end_times = Vec::with_capacity(spans.len());
            let mut durations = Vec::with_capacity(spans.len());
            
            let mut service_names = Vec::with_capacity(spans.len());
            let mut service_namespaces = Vec::with_capacity(spans.len());
            let mut service_instance_ids = Vec::with_capacity(spans.len());
            let mut service_versions = Vec::with_capacity(spans.len());
            
            let mut status_codes = Vec::with_capacity(spans.len());
            let mut status_messages = Vec::with_capacity(spans.len());
            
            let mut scope_names = Vec::with_capacity(spans.len());
            let mut scope_versions = Vec::with_capacity(spans.len());
            
            // Use MapBuilder for attributes instead of Vec
            let mut resource_attributes_builder = MapBuilder::new(
                None,
                StringBuilder::with_capacity(100, 1024),
                StringBuilder::with_capacity(100, 1024)
            );
            let mut span_attributes_builder = MapBuilder::new(
                None,
                StringBuilder::with_capacity(100, 1024),
                StringBuilder::with_capacity(100, 1024)
            );
            let mut scope_attributes_builder = MapBuilder::new(
                None,
                StringBuilder::with_capacity(100, 1024),
                StringBuilder::with_capacity(100, 1024)
            );
            
            let mut events_list = Vec::with_capacity(spans.len());
            let mut links_list = Vec::with_capacity(spans.len());
            
            let mut is_errors = Vec::with_capacity(spans.len());
            let mut latencies_ms = Vec::with_capacity(spans.len());
            let mut has_events_list = Vec::with_capacity(spans.len());
            let mut event_counts = Vec::with_capacity(spans.len());
            let mut link_counts = Vec::with_capacity(spans.len());
            
            let mut http_methods = Vec::with_capacity(spans.len());
            let mut http_urls = Vec::with_capacity(spans.len());
            let mut http_status_codes = Vec::with_capacity(spans.len());
            let mut http_routes = Vec::with_capacity(spans.len());
            let mut db_systems = Vec::with_capacity(spans.len());
            let mut db_operations = Vec::with_capacity(spans.len());
            let mut db_names = Vec::with_capacity(spans.len());
            let mut messaging_systems = Vec::with_capacity(spans.len());
            let mut messaging_destinations = Vec::with_capacity(spans.len());
            let mut rpc_systems = Vec::with_capacity(spans.len());
            let mut rpc_services = Vec::with_capacity(spans.len());
            let mut rpc_methods = Vec::with_capacity(spans.len());
            let mut exception_types = Vec::with_capacity(spans.len());
            let mut exception_messages = Vec::with_capacity(spans.len());
            
            let mut cloud_providers = Vec::with_capacity(spans.len());
            let mut cloud_regions = Vec::with_capacity(spans.len());
            let mut k8s_namespaces = Vec::with_capacity(spans.len());
            let mut k8s_pod_names = Vec::with_capacity(spans.len());
            let mut container_names_list = Vec::with_capacity(spans.len());
            let mut host_names = Vec::with_capacity(spans.len());
            
            let mut user_ids = Vec::with_capacity(spans.len());
            let mut session_ids = Vec::with_capacity(spans.len());
            
            let timestamp = Utc::now().timestamp_nanos_opt().unwrap_or(0);
            
            for span in spans {
                // Metadata
                timestamps.push(timestamp);
                message_ids.push(message_id.to_string());
                ingestion_nodes.push(ingestion_node.to_string());
                client_names.push(client_name.to_string());
                account_keys.push(account_key.map(|s| s.to_string()));
                
                // Trace/Span IDs
                trace_ids.push(span["traceId"].as_str().unwrap_or("").to_string());
                span_ids.push(span["spanId"].as_str().unwrap_or("").to_string());
                parent_span_ids.push(span["parentSpanId"].as_str().map(|s| s.to_string()));
                trace_states.push(span["traceState"].as_str().map(|s| s.to_string()));
                
                // Span info
                span_names.push(span["name"].as_str().unwrap_or("").to_string());
                span_kinds.push(span["kind"].as_i64().unwrap_or(1) as i32);
                
                // Times
                let start_time = parse_time_nanos(&span["startTimeUnixNano"]);
                let end_time = parse_time_nanos(&span["endTimeUnixNano"]);
                let duration = if end_time > start_time { end_time - start_time } else { 0 };
                
                start_times.push(start_time);
                end_times.push(end_time);
                durations.push(duration);
                
                // Service info
                service_names.push(service_name.clone());
                service_namespaces.push(service_namespace.clone());
                service_instance_ids.push(service_instance_id.clone());
                service_versions.push(service_version.clone());
                
                // Status
                let status = &span["status"];
                let status_code = status["code"].as_i64().map(|c| c as i32);
                status_codes.push(status_code);
                status_messages.push(status["message"].as_str().map(|s| s.to_string()));
                
                // Scope
                scope_names.push(scope_name.clone());
                scope_versions.push(scope_version.clone());
                
                // Attributes
                let span_attrs = extract_attributes(&span["attributes"]);
                
                // Extract common attributes
                http_methods.push(span_attrs.get("http.method").cloned());
                http_urls.push(span_attrs.get("http.url").cloned());
                http_status_codes.push(
                    span_attrs.get("http.status_code")
                        .and_then(|s| s.parse::<i32>().ok())
                );
                http_routes.push(span_attrs.get("http.route").cloned());
                db_systems.push(span_attrs.get("db.system").cloned());
                db_operations.push(span_attrs.get("db.operation").cloned());
                db_names.push(span_attrs.get("db.name").cloned());
                messaging_systems.push(span_attrs.get("messaging.system").cloned());
                messaging_destinations.push(span_attrs.get("messaging.destination").cloned());
                rpc_systems.push(span_attrs.get("rpc.system").cloned());
                rpc_services.push(span_attrs.get("rpc.service").cloned());
                rpc_methods.push(span_attrs.get("rpc.method").cloned());
                exception_types.push(span_attrs.get("exception.type").cloned());
                exception_messages.push(span_attrs.get("exception.message").cloned());
                user_ids.push(span_attrs.get("user.id").or_else(|| span_attrs.get("enduser.id")).cloned());
                session_ids.push(span_attrs.get("session.id").cloned());
                
                // Events
                let empty_events = Vec::new();
                let events = span["events"].as_array().unwrap_or(&empty_events);
                let events_json = if events.is_empty() {
                    None
                } else {
                    Some(serde_json::to_string(events)?)
                };
                let event_count = events.len() as i32;
                let has_events = event_count > 0;
                
                events_list.push(events_json);
                event_counts.push(event_count);
                has_events_list.push(has_events);
                
                // Links
                let empty_links = Vec::new();
                let links = span["links"].as_array().unwrap_or(&empty_links);
                let links_json = if links.is_empty() {
                    None
                } else {
                    Some(serde_json::to_string(links)?)
                };
                let link_count = links.len() as i32;
                
                links_list.push(links_json);
                link_counts.push(link_count);
                
                // Computed fields
                let is_error = status_code.map(|c| c == 2).unwrap_or(false) || 
                              http_status_codes.last().unwrap().map(|c| c >= 400).unwrap_or(false) ||
                              exception_types.last().unwrap().is_some();
                is_errors.push(is_error);
                
                let latency_ms = duration as f64 / 1_000_000.0;
                latencies_ms.push(latency_ms);
                
                // Add attributes to MapBuilders
                // Resource attributes
                for (key, value) in &resource_attrs {
                    resource_attributes_builder.keys().append_value(key);
                    resource_attributes_builder.values().append_value(value);
                }
                resource_attributes_builder.append(!resource_attrs.is_empty()).unwrap();

                // Span attributes
                for (key, value) in &span_attrs {
                    span_attributes_builder.keys().append_value(key);
                    span_attributes_builder.values().append_value(value);
                }
                span_attributes_builder.append(!span_attrs.is_empty()).unwrap();

                // Scope attributes
                for (key, value) in &scope_attrs {
                    scope_attributes_builder.keys().append_value(key);
                    scope_attributes_builder.values().append_value(value);
                }
                scope_attributes_builder.append(!scope_attrs.is_empty()).unwrap();
                
                // Cloud/Infrastructure
                cloud_providers.push(cloud_provider.clone());
                cloud_regions.push(cloud_region.clone());
                k8s_namespaces.push(k8s_namespace.clone());
                k8s_pod_names.push(k8s_pod_name.clone());
                container_names_list.push(container_name.clone());
                host_names.push(host_name.clone());
            }
            
            // Create arrays
            let arrays: Vec<ArrayRef> = vec![
                Arc::new(TimestampNanosecondArray::from(timestamps)),
                Arc::new(StringArray::from(message_ids)),
                Arc::new(StringArray::from(ingestion_nodes)),
                Arc::new(StringArray::from(client_names)),
                Arc::new(StringArray::from(account_keys)),
                
                Arc::new(StringArray::from(trace_ids)),
                Arc::new(StringArray::from(span_ids)),
                Arc::new(StringArray::from(parent_span_ids)),
                Arc::new(StringArray::from(trace_states)),
                
                Arc::new(StringArray::from(span_names)),
                Arc::new(Int32Array::from(span_kinds)),
                Arc::new(UInt64Array::from(start_times)),
                Arc::new(UInt64Array::from(end_times)),
                Arc::new(UInt64Array::from(durations)),
                
                Arc::new(StringArray::from(service_names)),
                Arc::new(StringArray::from(service_namespaces)),
                Arc::new(StringArray::from(service_instance_ids)),
                Arc::new(StringArray::from(service_versions)),
                
                Arc::new(Int32Array::from(status_codes)),
                Arc::new(StringArray::from(status_messages)),
                
                Arc::new(StringArray::from(scope_names)),
                Arc::new(StringArray::from(scope_versions)),
                
                Arc::new(resource_attributes_builder.finish()),
                Arc::new(span_attributes_builder.finish()),
                Arc::new(scope_attributes_builder.finish()),
                
                Arc::new(StringArray::from(events_list)),
                Arc::new(StringArray::from(links_list)),
                
                Arc::new(BooleanArray::from(is_errors)),
                Arc::new(Float64Array::from(latencies_ms)),
                Arc::new(BooleanArray::from(has_events_list)),
                Arc::new(Int32Array::from(event_counts)),
                Arc::new(Int32Array::from(link_counts)),
                
                Arc::new(StringArray::from(http_methods)),
                Arc::new(StringArray::from(http_urls)),
                Arc::new(Int32Array::from(http_status_codes)),
                Arc::new(StringArray::from(http_routes)),
                Arc::new(StringArray::from(db_systems)),
                Arc::new(StringArray::from(db_operations)),
                Arc::new(StringArray::from(db_names)),
                Arc::new(StringArray::from(messaging_systems)),
                Arc::new(StringArray::from(messaging_destinations)),
                Arc::new(StringArray::from(rpc_systems)),
                Arc::new(StringArray::from(rpc_services)),
                Arc::new(StringArray::from(rpc_methods)),
                Arc::new(StringArray::from(exception_types)),
                Arc::new(StringArray::from(exception_messages)),
                
                Arc::new(StringArray::from(cloud_providers)),
                Arc::new(StringArray::from(cloud_regions)),
                Arc::new(StringArray::from(k8s_namespaces)),
                Arc::new(StringArray::from(k8s_pod_names)),
                Arc::new(StringArray::from(container_names_list)),
                Arc::new(StringArray::from(host_names)),
                
                Arc::new(StringArray::from(user_ids)),
                Arc::new(StringArray::from(session_ids)),
            ];
            
            let batch = RecordBatch::try_new(schema.clone(), arrays)?;
            batches.push(batch);
        }
    }
    
    if batches.is_empty() {
        warn!("No spans found in OTLP data");
        // Return empty batch with schema
        let arrays: Vec<ArrayRef> = schema.fields()
            .iter()
            .map(|field| create_empty_array(field.data_type()))
            .collect();
        let empty_batch = RecordBatch::try_new(schema, arrays)?;
        batches.push(empty_batch);
    }
    
    Ok(batches)
}

/// Extract attributes from OTLP attribute array
fn extract_attributes(attrs: &Value) -> HashMap<String, String> {
    let mut map = HashMap::new();
    
    if let Some(attr_array) = attrs.as_array() {
        for attr in attr_array {
            if let Some(key) = attr["key"].as_str() {
                let value = extract_attribute_value(&attr["value"]);
                if !value.is_empty() {
                    map.insert(key.to_string(), value);
                }
            }
        }
    }
    
    map
}

/// Extract attribute value from OTLP value object
fn extract_attribute_value(value: &Value) -> String {
    if let Some(s) = value["stringValue"].as_str() {
        s.to_string()
    } else if let Some(b) = value["boolValue"].as_bool() {
        b.to_string()
    } else if let Some(i) = value["intValue"].as_i64() {
        i.to_string()
    } else if let Some(i) = value["intValue"].as_str() {
        i.to_string()
    } else if let Some(d) = value["doubleValue"].as_f64() {
        d.to_string()
    } else if let Some(bytes) = value["bytesValue"].as_str() {
        bytes.to_string()
    } else if value["arrayValue"].is_object() {
        // Handle array values
        if let Some(values) = value["arrayValue"]["values"].as_array() {
            let strings: Vec<String> = values.iter()
                .map(|v| extract_attribute_value(v))
                .collect();
            format!("[{}]", strings.join(","))
        } else {
            "[]".to_string()
        }
    } else if value["kvlistValue"].is_object() {
        // Handle key-value list
        "kvlist".to_string()
    } else {
        "".to_string()
    }
}

/// Parse time from various formats
fn parse_time_nanos(value: &Value) -> u64 {
    if let Some(s) = value.as_str() {
        s.parse::<u64>().unwrap_or(0)
    } else if let Some(i) = value.as_u64() {
        i
    } else if let Some(i) = value.as_i64() {
        i as u64
    } else {
        0
    }
}

/// Create empty array of given type
fn create_empty_array(data_type: &DataType) -> ArrayRef {
    match data_type {
        DataType::Utf8 => Arc::new(StringArray::new_null(0)),
        DataType::Int32 => Arc::new(Int32Array::new_null(0)),
        DataType::Int64 => Arc::new(Int64Array::new_null(0)),
        DataType::UInt64 => Arc::new(UInt64Array::new_null(0)),
        DataType::Float64 => Arc::new(Float64Array::new_null(0)),
        DataType::Boolean => Arc::new(BooleanArray::new_null(0)),
        DataType::Timestamp(TimeUnit::Nanosecond, None) => Arc::new(TimestampNanosecondArray::new_null(0)),
        DataType::Map(_, _) => {
            // Create empty MapArray with string key-value pairs
            let mut builder = MapBuilder::new(
                None,
                StringBuilder::with_capacity(0, 0),
                StringBuilder::with_capacity(0, 0)
            );
            Arc::new(builder.finish())
        },
        _ => Arc::new(StringArray::new_null(0)), // Fallback
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    
    #[test]
    fn test_create_complete_otlp_schema() {
        let schema = create_complete_otlp_schema();
        
        // Verify schema has expected number of fields
        let field_count = schema.fields().len();
        assert!(field_count >= 50, "Schema should have at least 50 fields, has {}", field_count);
        
        // Check core fields exist
        assert!(schema.column_with_name("trace_id").is_some());
        assert!(schema.column_with_name("span_id").is_some());
        assert!(schema.column_with_name("service_name").is_some());
        assert!(schema.column_with_name("span_name").is_some());
        assert!(schema.column_with_name("start_time_unix_nano").is_some());
        assert!(schema.column_with_name("end_time_unix_nano").is_some());
        
        // Check computed fields
        assert!(schema.column_with_name("is_error").is_some());
        assert!(schema.column_with_name("latency_ms").is_some());
        
        // Check attribute fields
        assert!(schema.column_with_name("http_method").is_some());
        assert!(schema.column_with_name("http_status_code").is_some());
    }
    
    #[test]
    fn test_process_otlp_json_batch() {
        let otlp_json = json!({
            "resourceSpans": [{
                "resource": {
                    "attributes": [
                        {"key": "service.name", "value": {"stringValue": "test-service"}},
                        {"key": "service.version", "value": {"stringValue": "1.0.0"}}
                    ]
                },
                "scopeSpans": [{
                    "scope": {
                        "name": "test-scope",
                        "version": "1.0"
                    },
                    "spans": [{
                        "traceId": "0123456789abcdef0123456789abcdef",
                        "spanId": "0123456789abcdef",
                        "name": "test-operation",
                        "kind": 2,
                        "startTimeUnixNano": "1700000000000000000",
                        "endTimeUnixNano": "1700000001000000000",
                        "attributes": [
                            {"key": "http.method", "value": {"stringValue": "GET"}},
                            {"key": "http.status_code", "value": {"intValue": "200"}}
                        ],
                        "status": {"code": 1}
                    }]
                }]
            }]
        });
        
        let result = process_otlp_json_batch(
            &otlp_json,
            "test-msg-001",
            "test-node",
            "test-client",
            Some("test-account")
        );
        
        assert!(result.is_ok(), "Processing should succeed");
        
        let batches = result.unwrap();
        assert!(!batches.is_empty(), "Should produce at least one batch");
        
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 1, "Should have one span");
        
        // Verify data was extracted correctly
        let schema = batch.schema();
        let trace_id_idx = schema.column_with_name("trace_id").unwrap().0;
        let trace_id_array = batch.column(trace_id_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        
        assert_eq!(trace_id_array.value(0), "0123456789abcdef0123456789abcdef");
    }
    
    #[test] 
    fn test_empty_otlp_data() {
        let empty_json = json!({
            "resourceSpans": []
        });
        
        let result = process_otlp_json_batch(
            &empty_json,
            "test-msg-002",
            "test-node",
            "test-client",
            None
        );
        
        assert!(result.is_ok(), "Should handle empty data");
        let batches = result.unwrap();
        assert_eq!(batches.len(), 1, "Should return one empty batch");
        assert_eq!(batches[0].num_rows(), 0, "Batch should be empty");
    }
    
    #[test]
    fn test_extract_attributes() {
        let attrs_json = json!([
            {"key": "http.method", "value": {"stringValue": "POST"}},
            {"key": "http.status_code", "value": {"intValue": "201"}},
            {"key": "db.system", "value": {"stringValue": "postgresql"}}
        ]);
        
        let attrs = extract_attributes(&attrs_json);
        
        assert_eq!(attrs.get("http.method"), Some(&"POST".to_string()));
        assert_eq!(attrs.get("http.status_code"), Some(&"201".to_string()));
        assert_eq!(attrs.get("db.system"), Some(&"postgresql".to_string()));
    }
}