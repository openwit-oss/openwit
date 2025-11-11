use std::sync::Arc;
use std::time::{Duration, Instant};
use anyhow::Result;
use rand::{Rng, thread_rng};
use tokio::sync::mpsc;
use tokio::time::{interval, sleep};
use tracing::{info, warn, error};
use uuid::Uuid;

use opentelemetry_proto::tonic::collector::trace::v1::{
    ExportTraceServiceRequest,
    trace_service_client::TraceServiceClient,
};
use opentelemetry_proto::tonic::trace::v1::{
    ResourceSpans, ScopeSpans, Span, Status as SpanStatus,
};
use opentelemetry_proto::tonic::common::v1::{
    AnyValue, KeyValue, InstrumentationScope,
    any_value,
};
use opentelemetry_proto::tonic::resource::v1::Resource;

use crate::metrics_collector::MetricsCollector;

pub struct LoadGenerator {
    target_endpoint: String,
    metrics: Arc<MetricsCollector>,
    config: LoadConfig,
}

#[derive(Debug, Clone)]
pub struct LoadConfig {
    pub total_messages: u64,
    pub batch_size: usize,
    pub concurrent_connections: usize,
    pub messages_per_second: Option<u64>, // Rate limiting
    pub message_size_bytes: usize,
    pub span_count_per_message: usize,
    pub attributes_per_span: usize,
}

impl Default for LoadConfig {
    fn default() -> Self {
        Self {
            total_messages: 1_000_000,
            batch_size: 100,
            concurrent_connections: 10,
            messages_per_second: None, // No rate limit by default
            message_size_bytes: 1024, // 1KB average
            span_count_per_message: 10,
            attributes_per_span: 5,
        }
    }
}

impl LoadConfig {
    pub fn for_billion_logs() -> Self {
        Self {
            total_messages: 1_000_000_000,
            batch_size: 1000,
            concurrent_connections: 50,
            messages_per_second: None, // Max speed
            message_size_bytes: 512, // Smaller for volume
            span_count_per_message: 1,
            attributes_per_span: 3,
        }
    }
}

impl LoadGenerator {
    pub fn new(target_endpoint: String, config: LoadConfig, metrics: Arc<MetricsCollector>) -> Self {
        Self {
            target_endpoint,
            metrics,
            config,
        }
    }
    
    pub async fn run(&self) -> Result<()> {
        info!("Starting load generation to {} with {} connections", 
            self.target_endpoint, self.config.concurrent_connections);
        
        self.metrics.start().await;
        
        let messages_per_connection = self.config.total_messages / self.config.concurrent_connections as u64;
        let mut handles = Vec::new();
        
        for conn_id in 0..self.config.concurrent_connections {
            let endpoint = self.target_endpoint.clone();
            let config = self.config.clone();
            let metrics = self.metrics.clone();
            let messages_to_send = if conn_id == self.config.concurrent_connections - 1 {
                // Last connection handles remainder
                messages_per_connection + (self.config.total_messages % self.config.concurrent_connections as u64)
            } else {
                messages_per_connection
            };
            
            let handle = tokio::spawn(async move {
                if let Err(e) = Self::run_connection(conn_id, endpoint, config, metrics, messages_to_send).await {
                    error!("Connection {} failed: {:?}", conn_id, e);
                }
            });
            
            handles.push(handle);
        }
        
        // Wait for all connections to complete
        for handle in handles {
            let _ = handle.await;
        }
        
        self.metrics.stop().await;
        info!("Load generation completed");
        
        Ok(())
    }
    
    async fn run_connection(
        conn_id: usize,
        endpoint: String,
        config: LoadConfig,
        metrics: Arc<MetricsCollector>,
        messages_to_send: u64,
    ) -> Result<()> {
        let mut client = TraceServiceClient::connect(format!("http://{}", endpoint)).await?;
        
        let mut sent = 0u64;
        let mut rate_limiter = config.messages_per_second.map(|mps| {
            let interval_ms = 1000.0 / (mps as f64 / config.concurrent_connections as f64);
            interval(Duration::from_millis(interval_ms as u64))
        });
        
        while sent < messages_to_send {
            // Rate limiting
            if let Some(ref mut limiter) = rate_limiter {
                limiter.tick().await;
            }
            
            let batch_size = (config.batch_size as u64).min(messages_to_send - sent);
            let request = Self::generate_batch(&config, batch_size as usize);
            let request_size = prost::Message::encoded_len(&request) as u64;
            
            let start = Instant::now();
            
            match client.export(request).await {
                Ok(_) => {
                    let latency_us = start.elapsed().as_micros() as u64;
                    metrics.record_latency(latency_us).await;
                    metrics.record_message_sent(request_size);
                    sent += batch_size;
                }
                Err(e) => {
                    warn!("Failed to send batch: {:?}", e);
                    metrics.record_error();
                }
            }
        }
        
        info!("Connection {} completed: sent {} messages", conn_id, sent);
        Ok(())
    }
    
    fn generate_batch(config: &LoadConfig, batch_size: usize) -> ExportTraceServiceRequest {
        let mut rng = thread_rng();
        let mut resource_spans = Vec::new();
        
        for _ in 0..batch_size {
            let trace_id = Uuid::new_v4().as_bytes().to_vec();
            let mut spans: Vec<Span> = Vec::new();
            
            for span_idx in 0..config.span_count_per_message {
                let span_id = rng.gen_range(0..u64::MAX).to_ne_bytes().to_vec();
                let mut attributes = Vec::new();
                
                // Add random attributes
                for attr_idx in 0..config.attributes_per_span {
                    attributes.push(KeyValue {
                        key: format!("attr_{}", attr_idx),
                        value: Some(AnyValue {
                            value: Some(any_value::Value::StringValue(
                                format!("value_{}", rng.gen_range(0..1000000))
                            )),
                        }),
                    });
                }
                
                // Add some padding to reach target message size
                let padding_needed = config.message_size_bytes.saturating_sub(attributes.len() * 50) / config.span_count_per_message;
                if padding_needed > 0 {
                    attributes.push(KeyValue {
                        key: "padding".to_string(),
                        value: Some(AnyValue {
                            value: Some(any_value::Value::StringValue(
                                "x".repeat(padding_needed)
                            )),
                        }),
                    });
                }
                
                spans.push(Span {
                    trace_id: trace_id.clone(),
                    span_id,
                    parent_span_id: if span_idx > 0 { spans[0].span_id.clone() } else { vec![] },
                    name: format!("span_{}", span_idx),
                    kind: 1, // SPAN_KIND_INTERNAL
                    start_time_unix_nano: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64,
                    end_time_unix_nano: (chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) + 1000000) as u64,
                    attributes,
                    status: Some(SpanStatus {
                        code: 0, // STATUS_CODE_OK
                        message: String::new(),
                    }),
                    ..Default::default()
                });
            }
            
            resource_spans.push(ResourceSpans {
                resource: Some(Resource {
                    attributes: vec![
                        KeyValue {
                            key: "service.name".to_string(),
                            value: Some(AnyValue {
                                value: Some(any_value::Value::StringValue("load-test".to_string())),
                            }),
                        },
                        KeyValue {
                            key: "host.name".to_string(),
                            value: Some(AnyValue {
                                value: Some(any_value::Value::StringValue("load-generator".to_string())),
                            }),
                        },
                    ],
                    ..Default::default()
                }),
                scope_spans: vec![ScopeSpans {
                    scope: Some(InstrumentationScope {
                        name: "load-generator".to_string(),
                        version: "1.0.0".to_string(),
                        ..Default::default()
                    }),
                    spans,
                    ..Default::default()
                }],
                ..Default::default()
            });
        }
        
        ExportTraceServiceRequest { resource_spans }
    }
    
    pub async fn generate_to_kafka(
        &self,
        kafka_brokers: &str,
        topic: &str,
    ) -> Result<()> {
        // Alternative: Generate directly to Kafka for even higher throughput
        use rdkafka::producer::{FutureProducer, FutureRecord};
        use rdkafka::ClientConfig;
        
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", kafka_brokers)
            .set("message.timeout.ms", "30000")
            .set("compression.type", "snappy")
            .set("batch.size", "1000000") // 1MB batches
            .create()?;
        
        info!("Generating {} messages directly to Kafka topic: {}", 
            self.config.total_messages, topic);
        
        self.metrics.start().await;
        
        for i in 0..self.config.total_messages {
            let request = Self::generate_batch(&self.config, 1);
            let payload = prost::Message::encode_to_vec(&request);
            
            let key = format!("key-{}", i);
            let record = FutureRecord::to(topic)
                .key(&key)
                .payload(&payload);
            
            match producer.send(record, Duration::from_secs(10)).await {
                Ok(_) => {
                    self.metrics.record_message_sent(payload.len() as u64);
                }
                Err((e, _)) => {
                    error!("Kafka send error: {:?}", e);
                    self.metrics.record_error();
                }
            }
            
            if i % 10000 == 0 {
                info!("Progress: {}/{} messages", i, self.config.total_messages);
            }
        }
        
        self.metrics.stop().await;
        Ok(())
    }
}