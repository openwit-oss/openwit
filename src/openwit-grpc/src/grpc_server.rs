use tokio::sync::mpsc;
use tonic::transport::Server;
use anyhow::{Context as AnyhowContext, Result};
use std::net::SocketAddr;
use std::sync::Arc;

use opentelemetry_proto::tonic::collector::{
    logs::v1::logs_service_server::LogsServiceServer as OtlpLogsServiceServer,
    metrics::v1::metrics_service_server::MetricsServiceServer as OtlpMetricsServiceServer,
    trace::v1::trace_service_server::TraceServiceServer as OtlpTraceServiceServer,
};

use openwit_config::UnifiedConfig;
use openwit_ingestion::types::IngestionConfig;
use openwit_postgres::BatchTracker;
use crate::otlp_services::{OtlpTraceService, OtlpMetricsService, OtlpLogsService};
use crate::batcher::{Batcher, BatcherConfig};
use crate::flush_pool::{FlushWorkerPool, FlushPoolConfig};
use crate::wal::WalManager;

type TelemetryOutputSender = mpsc::Sender<String>;

pub struct GrpcServer {
    config: Arc<UnifiedConfig>,
    ingest_tx: Option<mpsc::Sender<openwit_ingestion::IngestedMessage>>,
}

impl GrpcServer {
    pub fn new(config: UnifiedConfig, ingest_tx: Option<mpsc::Sender<openwit_ingestion::IngestedMessage>>) -> Self {
        Self {
            config: Arc::new(config),
            ingest_tx,
        }
    }

    pub async fn start(self) -> Result<()> {
        let grpc_cfg = &self.config.ingestion.sources.grpc;
        
        if !grpc_cfg.enabled {
            tracing::info!("gRPC server disabled via config");
            return Ok(());
        }

        let grpc_bind_addr = if self.config.ingestion.grpc.bind.contains(':') {
            self.config.ingestion.grpc.bind.clone()
        } else {
            format!("{}:{}", self.config.ingestion.grpc.bind, self.config.ingestion.grpc.port)
        };
        tracing::info!("Starting gRPC OTLP server on {}", grpc_bind_addr);

        // Create telemetry output channel
        let (output_tx, mut output_rx): (TelemetryOutputSender, mpsc::Receiver<String>) =
            mpsc::channel(100);

        // Spawn task for printing received telemetry data
        tokio::spawn(async move {
            tracing::info!("Telemetry Output Processor: Task started.");
            while let Some(message) = output_rx.recv().await {
                println!("{}", message);
            }
            tracing::info!("Telemetry Output Processor: Channel closed. Task finished.");
        });

        // Create services
        let mut trace_service = OtlpTraceService::new(output_tx.clone());
        let mut metrics_service = OtlpMetricsService::new(output_tx.clone());
        let mut logs_service = OtlpLogsService::new(output_tx);

        // Create batcher if ingestion is available
        if let Some(ingest_tx) = self.ingest_tx {
            // Get ingestion config from unified config
            let ingestion_config = IngestionConfig::from_unified(&self.config);

            // Create batcher configuration
            let batcher_config = BatcherConfig::from_ingestion_config(&ingestion_config);

            tracing::info!(
                "Creating gRPC batcher with batch_size={} and timeout={:?}",
                batcher_config.max_batch_size,
                batcher_config.batch_timeout
            );

            // Create WAL manager for batch persistence
            let wal_base_path = std::env::var("DATA_DIR")
                .unwrap_or_else(|_| "./data".to_string());

            let wal_manager = WalManager::new(&wal_base_path)
                .await
                .context("Failed to create WAL manager")?;

            let wal_manager = Arc::new(wal_manager);

            // Initialize batch tracker for PostgreSQL monitoring
            let batch_tracker = match std::env::var("POSTGRES_URL") {
                Ok(postgres_url) => {
                    tracing::info!("Initializing batch tracker with PostgreSQL");
                    match BatchTracker::new(&postgres_url).await {
                        Ok(tracker) => {
                            tracing::info!("Batch tracker initialized successfully");
                            Some(Arc::new(tracker))
                        }
                        Err(e) => {
                            tracing::warn!("Failed to initialize batch tracker: {}. Continuing without batch tracking.", e);
                            None
                        }
                    }
                }
                Err(_) => {
                    tracing::info!("POSTGRES_URL not set, batch tracking disabled");
                    None
                }
            };

            // Create flush worker pool configuration
            let flush_pool_config = FlushPoolConfig::default();

            // Create flush worker pool (5 workers by default) with WAL and batch tracker
            let flush_pool = FlushWorkerPool::new(flush_pool_config, ingest_tx, wal_manager, batch_tracker.clone());

            // Create batcher with flush pool (returns Arc<Batcher>)
            let batcher = Batcher::new(batcher_config, flush_pool);

            // Start background flush task
            batcher.clone().start_flush_task();

            // Add batcher to services
            trace_service = trace_service.with_batcher(batcher.clone());
            metrics_service = metrics_service.with_batcher(batcher.clone());
            logs_service = logs_service.with_batcher(batcher);
        }

        let otlp_grpc_listen_address: SocketAddr = grpc_bind_addr
            .parse()
            .with_context(|| format!("Invalid OTLP gRPC listen address: '{}'", grpc_bind_addr))?;

        tracing::info!(
            "OTLP/gRPC Server: Preparing to listen on {}",
            otlp_grpc_listen_address
        );

        // Build server with optional TLS support
        let server_builder = if self.config.ingestion.grpc.tls.enabled {
            // Load TLS certificates
            let cert_path = &self.config.ingestion.grpc.tls.cert_path;
            let key_path = &self.config.ingestion.grpc.tls.key_path;
            
            if cert_path.is_none() || key_path.is_none() {
                tracing::warn!("TLS enabled but cert_path or key_path is not configured, falling back to unencrypted");
                Server::builder()
            } else {
                let cert_path = cert_path.as_ref().unwrap();
                let key_path = key_path.as_ref().unwrap();
                // TLS support requires tonic tls feature - disabled for now
                tracing::warn!("TLS is configured but not implemented - requires tonic TLS feature. Using unencrypted connection.");
                Server::builder()
            }
        } else {
            tracing::info!("TLS disabled for gRPC server");
            Server::builder()
        };
        
        server_builder
            .trace_fn(|http_request: &http::Request<()>| {
                tracing::info_span!(
                    "OTLP gRPC Request",
                    grpc.method = %http_request.method(),
                    grpc.path = %http_request.uri().path(),
                    grpc.version = ?http_request.version(),
                )
            })
            .add_service(OtlpTraceServiceServer::new(trace_service)
                    .max_encoding_message_size(self.config.ingestion.grpc.max_message_size)
                    .max_decoding_message_size(self.config.ingestion.grpc.max_message_size)
                )
            .add_service(OtlpMetricsServiceServer::new(metrics_service)
                    .max_encoding_message_size(self.config.ingestion.grpc.max_message_size)
                    .max_decoding_message_size(self.config.ingestion.grpc.max_message_size)
                )
            .add_service(OtlpLogsServiceServer::new(logs_service)
                    .max_encoding_message_size(self.config.ingestion.grpc.max_message_size)
                    .max_decoding_message_size(self.config.ingestion.grpc.max_message_size)
                )
            .serve(otlp_grpc_listen_address)
            .await
            .context("OTLP gRPC server failed to run")
    }
}
