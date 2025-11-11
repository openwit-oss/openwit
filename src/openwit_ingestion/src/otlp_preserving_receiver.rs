use tokio::sync::mpsc;
use anyhow::{Result, Context};
use tracing::{info, debug, error, warn};
use serde_json::Value;
use base64::{engine::general_purpose, Engine as _};
use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{StringArray, ArrayRef};
use arrow::datatypes::{Schema, Field, DataType};
use arrow::record_batch::RecordBatch;

use openwit_storage::otlp_ingestion::{process_otlp_json_batch, create_complete_otlp_schema};
use openwit_inter_node::ArrowFlightBatch;

/// OTLP-preserving ingestion receiver that maintains OpenTelemetry structure
pub struct OtlpPreservingReceiver {
    node_id: String,
    batch_sender: mpsc::Sender<ArrowFlightBatch>,
    schema: Arc<Schema>,
}

impl OtlpPreservingReceiver {
    pub fn new(
        node_id: String,
        batch_sender: mpsc::Sender<ArrowFlightBatch>,
    ) -> Self {
        Self {
            node_id,
            batch_sender,
            schema: create_complete_otlp_schema(),
        }
    }
    
    /// Process incoming Kafka message with OTLP data
    pub async fn process_message(
        &self,
        message_id: &str,
        payload: &[u8],
        headers: HashMap<String, String>,
    ) -> Result<()> {
        debug!("Processing OTLP message {}", message_id);
        
        // Extract metadata from headers
        let client_name = headers.get("client_name")
            .or_else(|| headers.get("index_name"))
            .map(|s| s.as_str())
            .or_else(|| headers.get("topic").map(|t| extract_client_from_topic(t)))
            .unwrap_or("default");
        
        let account_key = headers.get("account_key")
            .or_else(|| headers.get("tenant"));
        
        // Check for empty payload
        if payload.is_empty() {
            error!("Empty payload received for message {}", message_id);
            return Err(anyhow::anyhow!("Empty payload"));
        }
        
        // Try different parsing strategies
        let otlp_json = if let Ok(json) = serde_json::from_slice::<Value>(payload) {
            debug!("Parsed payload as JSON directly");
            json
        } else if let Ok(json) = try_parse_otlp_protobuf(payload) {
            // Try as raw protobuf first (most common case for traces)
            debug!("Successfully parsed as OTLP protobuf");
            json
        } else {
            debug!("Failed to parse as JSON or protobuf, attempting base64 decode. Payload length: {}", payload.len());
            
            // Log first few bytes to help debug
            let preview = if payload.len() <= 100 {
                String::from_utf8_lossy(payload)
            } else {
                String::from_utf8_lossy(&payload[..100])
            };
            debug!("Payload preview: {:?}", preview);
            
            // Try base64 decoding
            let decoded = match general_purpose::STANDARD.decode(payload) {
                Ok(bytes) => {
                    debug!("Successfully decoded base64, decoded size: {} bytes", bytes.len());
                    bytes
                },
                Err(e) => {
                    error!("Failed to decode base64 payload: {}. Payload length: {}, preview: {:?}", 
                           e, payload.len(), preview);
                    return Err(anyhow::anyhow!("Failed to decode payload: not valid JSON, protobuf, or base64 - {}", e));
                }
            };
            
            // Try decoded data as protobuf
            match try_parse_otlp_protobuf(&decoded) {
                Ok(json) => {
                    debug!("Successfully parsed base64-decoded data as OTLP protobuf");
                    json
                },
                Err(protobuf_err) => {
                    debug!("Failed to parse decoded as protobuf: {}, trying as JSON", protobuf_err);
                    
                    // Final attempt - parse decoded as JSON
                    match serde_json::from_slice::<Value>(&decoded) {
                        Ok(json) => {
                            debug!("Successfully parsed decoded data as JSON");
                            json
                        },
                        Err(json_err) => {
                            error!("Failed to parse decoded payload as JSON. Size: {}, preview: {:?}", 
                                   decoded.len(), String::from_utf8_lossy(&decoded[..decoded.len().min(100)]));
                            return Err(anyhow::anyhow!("Failed to parse payload: protobuf error: {}, json error: {}", 
                                                      protobuf_err, json_err));
                        }
                    }
                }
            }
        };
        
        // Validate OTLP structure
        if !otlp_json["resourceSpans"].is_array() {
            return Err(anyhow::anyhow!("Invalid OTLP structure: missing resourceSpans array"));
        }
        
        // Process OTLP JSON to Arrow batches
        let batches = process_otlp_json_batch(
            &otlp_json,
            message_id,
            &self.node_id,
            client_name,
            account_key.map(|s| s.as_str()),
        )?;
        
        // Send batches through Arrow Flight
        for (idx, batch) in batches.into_iter().enumerate() {
            let batch_id = format!("{}_{}", message_id, idx);
            let row_count = batch.num_rows();
            
            if row_count == 0 {
                debug!("Skipping empty batch {}", batch_id);
                continue;
            }
            
            let mut metadata = headers.clone();
            metadata.insert("batch_id".to_string(), batch_id.clone());
            metadata.insert("message_id".to_string(), message_id.to_string());
            metadata.insert("client_name".to_string(), client_name.to_string());
            metadata.insert("otlp_format".to_string(), "true".to_string());
            if let Some(key) = account_key {
                metadata.insert("account_key".to_string(), key.to_string());
            }
            
            let flight_batch = ArrowFlightBatch {
                batch_id,
                source_node: self.node_id.clone(),
                target_node: "storage".to_string(), // Will be resolved by the ingestion service
                record_batch: batch,
                metadata,
            };
            
            self.batch_sender.send(flight_batch).await
                .context("Failed to send batch to storage")?;
            
            debug!("Sent OTLP batch with {} rows to storage", row_count);
        }
        
        Ok(())
    }
    
    /// Process legacy flattened format for backward compatibility
    pub async fn process_legacy_message(
        &self,
        message_id: &str,
        flattened_data: HashMap<String, String>,
        headers: HashMap<String, String>,
    ) -> Result<()> {
        warn!("Processing legacy flattened message {} - consider migrating to OTLP format", message_id);
        
        // Create a simple batch with the flattened data
        let schema = create_legacy_schema();
        let num_rows = 1;
        
        // Convert HashMap to arrays
        let mut field_names = Vec::new();
        let mut field_values = Vec::new();
        
        for (key, value) in flattened_data.iter() {
            field_names.push(key.clone());
            field_values.push(value.clone());
        }
        
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec![message_id])),
            Arc::new(StringArray::from(vec![serde_json::to_string(&field_names)?])),
            Arc::new(StringArray::from(vec![serde_json::to_string(&field_values)?])),
        ];
        
        let batch = RecordBatch::try_new(schema, arrays)?;
        
        let mut metadata = headers;
        metadata.insert("batch_id".to_string(), message_id.to_string());
        metadata.insert("legacy_format".to_string(), "true".to_string());
        
        let flight_batch = ArrowFlightBatch {
            batch_id: message_id.to_string(),
            source_node: self.node_id.clone(),
            target_node: "storage".to_string(),
            record_batch: batch,
            metadata,
        };
        
        self.batch_sender.send(flight_batch).await
            .context("Failed to send legacy batch to storage")?;
        
        Ok(())
    }
}

/// Try to parse OTLP protobuf data
fn try_parse_otlp_protobuf(data: &[u8]) -> Result<Value> {
    use opentelemetry_proto::tonic::trace::v1::TracesData;
    use prost::Message;
    
    // Try to decode as OTLP TracesData protobuf
    match TracesData::decode(data) {
        Ok(traces_data) => {
            // Convert protobuf to JSON
            let json_str = serde_json::to_string(&traces_data)
                .context("Failed to serialize TracesData to JSON")?;
            serde_json::from_str(&json_str)
                .context("Failed to parse serialized TracesData as Value")
        },
        Err(e) => {
            debug!("Failed to decode as TracesData protobuf: {}", e);
            Err(anyhow::anyhow!("Failed to decode protobuf: {}", e))
        }
    }
}

/// Extract client from topic name
fn extract_client_from_topic(topic: &str) -> &str {
    let parts: Vec<&str> = topic.split('.').collect();
    if parts.len() > 3 {
        parts[3]
    } else {
        "default"
    }
}

/// Create schema for legacy flattened data
fn create_legacy_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("message_id", DataType::Utf8, false),
        Field::new("field_names", DataType::Utf8, false),
        Field::new("field_values", DataType::Utf8, false),
    ]))
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_otlp_processing() {
        let (tx, mut rx) = mpsc::channel(10);
        let receiver = OtlpPreservingReceiver::new("test-node".to_string(), tx);
        
        let otlp_json = r#"{
            "resourceSpans": [
                {
                    "resource": {
                        "attributes": [
                            {
                                "key": "service.name",
                                "value": {"stringValue": "test-service"}
                            }
                        ]
                    },
                    "scopeSpans": [
                        {
                            "scope": {
                                "name": "test-scope"
                            },
                            "spans": [
                                {
                                    "traceId": "12345678901234567890123456789012",
                                    "spanId": "1234567890123456",
                                    "name": "test-span",
                                    "kind": 2,
                                    "startTimeUnixNano": "1700000000000000000",
                                    "endTimeUnixNano": "1700000001000000000",
                                    "attributes": [
                                        {
                                            "key": "http.method",
                                            "value": {"stringValue": "GET"}
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
                }
            ]
        }"#;
        
        let mut headers = HashMap::new();
        headers.insert("client_name".to_string(), "test-client".to_string());
        
        receiver.process_message("test-msg-1", otlp_json.as_bytes(), headers).await.unwrap();
        
        // Check that batch was sent
        let batch = rx.recv().await.unwrap();
        assert_eq!(batch.record_batch.num_rows(), 1);
        assert_eq!(batch.metadata.get("client_name").unwrap(), "test-client");
        assert_eq!(batch.metadata.get("otlp_format").unwrap(), "true");
    }
}