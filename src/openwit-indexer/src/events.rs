use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use sqlx::Row;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tokio::time::{interval, Duration};
use tracing::{debug, error, info, warn};

use crate::db::DbClient;
use crate::metrics;

/// Event types from control plane
#[derive(Debug, Clone)]
pub enum ControlEvent {
    DataFilePublished {
        file_ulid: String,
        tenant: String,
        signal: String,
        partition_key: String,
        parquet_url: String,
        size_bytes: u64,
        row_count: u64,
        min_timestamp: chrono::DateTime<chrono::Utc>,
        max_timestamp: chrono::DateTime<chrono::Utc>,
    },
    PartitionSealed {
        tenant: String,
        signal: String,
        partition_key: String,
    },
}

/// Manages event subscription from control plane
#[derive(Clone)]
pub struct EventSubscriber {
    control_plane_addr: String,
    db_client: Arc<DbClient>,
    event_sender: mpsc::Sender<ControlEvent>,
    processed_files: Arc<RwLock<HashSet<String>>>,
}

impl EventSubscriber {
    pub fn new(
        control_plane_addr: String,
        db_client: Arc<DbClient>,
    ) -> (Self, mpsc::Receiver<ControlEvent>) {
        let (event_sender, event_receiver) = mpsc::channel(1000);
        
        let subscriber = Self {
            control_plane_addr,
            db_client,
            event_sender,
            processed_files: Arc::new(RwLock::new(HashSet::new())),
        };
        
        (subscriber, event_receiver)
    }

    /// Start event subscription with fallback polling
    pub async fn start(self) -> Result<()> {
        info!(
            control_plane = %self.control_plane_addr,
            "Starting event subscription with fallback polling"
        );

        // Try streaming first
        let streaming_subscriber = self.clone();
        let streaming_handle = tokio::spawn(streaming_subscriber.subscribe_stream());

        // Start fallback polling
        let polling_subscriber = self.clone();
        let polling_handle = tokio::spawn(polling_subscriber.poll_for_events());

        // Wait for either to fail
        tokio::select! {
            res = streaming_handle => {
                if let Err(e) = res? {
                    error!(error = %e, "Streaming subscription failed");
                }
            }
            res = polling_handle => {
                if let Err(e) = res? {
                    error!(error = %e, "Polling failed");
                }
            }
        }

        Ok(())
    }

    /// Subscribe to control plane event stream
    async fn subscribe_stream(self) -> Result<()> {
        // In production, this would connect to the control plane gRPC service
        // For now, this is a placeholder that shows the structure
        
        info!("Attempting to subscribe to control plane event stream");
        
        // Simulated connection - replace with actual gRPC client
        loop {
            match self.connect_to_control_plane().await {
                Ok(mut stream) => {
                    info!("Connected to control plane event stream");
                    
                    while let Some(event) = stream.recv().await {
                        if let Err(e) = self.process_event(event).await {
                            error!(error = %e, "Failed to process event");
                        }
                    }
                    
                    warn!("Event stream disconnected, reconnecting...");
                }
                Err(e) => {
                    error!(error = %e, "Failed to connect to control plane");
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            }
        }
    }

    /// Poll for missed events (fallback mechanism)
    async fn poll_for_events(self) -> Result<()> {
        let mut poll_interval = interval(Duration::from_secs(30));
        let mut last_poll = chrono::Utc::now() - chrono::Duration::hours(1);

        loop {
            poll_interval.tick().await;
            
            debug!("Polling for missed events");
            
            match self.fetch_stored_files(last_poll).await {
                Ok(files) => {
                    let new_files = files.len();
                    if new_files > 0 {
                        info!(count = new_files, "Found new files via polling");
                        
                        for file in files {
                            if let Err(e) = self.process_event(file).await {
                                error!(error = %e, "Failed to process polled file");
                            }
                        }
                    }
                    
                    last_poll = chrono::Utc::now();
                }
                Err(e) => {
                    error!(error = %e, "Polling failed");
                    metrics::record_db_failure("poll_events", "query_error");
                }
            }
        }
    }

    /// Fetch recently stored files from database
    async fn fetch_stored_files(
        &self,
        since: chrono::DateTime<chrono::Utc>,
    ) -> Result<Vec<ControlEvent>> {
        // Query for data files without corresponding index artifacts
        let query = r#"
            SELECT df.file_ulid, df.tenant, df.signal, df.partition_key,
                   df.parquet_url, df.size_bytes, df.row_count,
                   df.min_timestamp, df.max_timestamp
            FROM indexer.data_files df
            LEFT JOIN indexer.index_artifacts ia ON df.file_ulid = ia.file_ulid
            WHERE df.ingested_at >= $1
              AND ia.artifact_id IS NULL
            ORDER BY df.ingested_at
            LIMIT 100
        "#;

        let rows = sqlx::query(query)
            .bind(since)
            .fetch_all(self.db_client.pool())
            .await?;

        let mut events = Vec::new();
        let processed = self.processed_files.read().await;

        for row in rows {
            let file_ulid: String = row.get::<String, _>("file_ulid");
            
            // Skip if already processed
            if processed.contains(&file_ulid) {
                continue;
            }

            events.push(ControlEvent::DataFilePublished {
                file_ulid,
                tenant: row.get::<String, _>("tenant"),
                signal: row.get::<String, _>("signal"),
                partition_key: row.get::<String, _>("partition_key"),
                parquet_url: row.get::<String, _>("parquet_url"),
                size_bytes: row.get::<i64, _>("size_bytes") as u64,
                row_count: row.get::<i64, _>("row_count") as u64,
                min_timestamp: row.get::<DateTime<Utc>, _>("min_timestamp"),
                max_timestamp: row.get::<DateTime<Utc>, _>("max_timestamp"),
            });
        }

        Ok(events)
    }

    /// Process a control event
    async fn process_event(&self, event: ControlEvent) -> Result<()> {
        match &event {
            ControlEvent::DataFilePublished { file_ulid, .. } => {
                // Check if already processed
                {
                    let processed = self.processed_files.read().await;
                    if processed.contains(file_ulid) {
                        debug!(file_ulid = %file_ulid, "File already processed");
                        return Ok(());
                    }
                }

                // Send to processing queue
                self.event_sender.send(event.clone()).await
                    .context("Failed to send event to processing queue")?;

                // Mark as processed
                self.processed_files.write().await.insert(file_ulid.clone());
                
                metrics::record_event_consumed("DataFilePublished", "control_plane");
            }
            ControlEvent::PartitionSealed { partition_key, .. } => {
                debug!(partition_key = %partition_key, "Partition sealed event");
                self.event_sender.send(event.clone()).await?;
                metrics::record_event_consumed("PartitionSealed", "control_plane");
            }
        }

        Ok(())
    }

    /// Connect to control plane (placeholder)
    async fn connect_to_control_plane(&self) -> Result<mpsc::Receiver<ControlEvent>> {
        // In production, this would create a gRPC client and subscribe to events
        // For now, return a dummy channel
        let (_tx, _rx) = mpsc::channel::<ControlEvent>(1);
        Err(anyhow::anyhow!("Control plane streaming not implemented"))
    }

    /// Clone for spawning
    fn clone(&self) -> Self {
        Self {
            control_plane_addr: self.control_plane_addr.clone(),
            db_client: self.db_client.clone(),
            event_sender: self.event_sender.clone(),
            processed_files: self.processed_files.clone(),
        }
    }
}

/// Report indexing status back to control plane
pub async fn report_batch_indexed(
    _control_plane_addr: &str,
    file_ulid: &str,
    status: &str,
    index_urls: Vec<String>,
) -> Result<()> {
    // In production, this would call the control plane gRPC API
    debug!(
        file_ulid = %file_ulid,
        status = %status,
        urls = ?index_urls,
        "Reporting batch indexed to control plane"
    );
    
    // Placeholder for gRPC call
    // control_client.report_batch(ReportBatchRequest { ... }).await?;
    
    Ok(())
}