use std::sync::Arc;
use std::time::{Duration, Instant};
use anyhow::Result;
use tokio::sync::mpsc;
use tokio::time::interval;
use tracing::{info, warn, error, debug};
use chrono::{DateTime, Utc};

use openwit_metastore::{MetaStore, SegmentMetadata, SegmentStatus};
use openwit_indexer::tantivy_indexer::TantivyIndexer;

/// Messages that the IndexerActor can receive
#[derive(Debug)]
pub enum IndexerMessage {
    /// Index a segment by ID
    IndexSegment(String),
    /// Batch index multiple segments
    IndexBatch(Vec<String>),
    /// Get indexing statistics
    GetStats(mpsc::Sender<IndexerStats>),
    /// Retry failed segments
    RetryFailed,
    /// Shutdown the actor
    Shutdown,
}

/// Indexing statistics
#[derive(Debug, Default, Clone)]
pub struct IndexerStats {
    pub segments_indexed: u64,
    pub segments_failed: u64,
    pub segments_pending: u64,
    pub messages_indexed: u64,
    pub bytes_indexed: u64,
    pub last_index_time: Option<DateTime<Utc>>,
    pub average_index_time_ms: f64,
}

/// Indexer actor that handles creating search indexes
#[allow(unused)]
pub struct IndexerActor {
    /// Metastore for segment metadata
    metastore: Arc<dyn MetaStore>,
    /// Tantivy indexer
    indexer: Arc<TantivyIndexer>,
    /// Index path
    index_path: String,
    /// Indexing statistics
    stats: IndexerStats,
    /// Track indexing times for average calculation
    index_times: Vec<u128>,
    /// Maximum retries for failed segments
    max_retries: u32,
    /// Retry queue for failed segments
    retry_queue: Vec<(String, u32)>, // (segment_id, retry_count)
}

impl IndexerActor {
    /// Create a new indexer actor
    pub async fn new(
        metastore: Arc<dyn MetaStore>,
        index_path: String,
    ) -> Result<Self> {
        // Create index directory
        tokio::fs::create_dir_all(&index_path).await?;
        
        // Initialize Tantivy indexer
        let index_config = openwit_indexer::IndexConfig {
            index_type: openwit_indexer::IndexType::Inverted,
            fields: vec![],
            storage_path: index_path.clone(),
            cache_size_mb: 100,
            compression: openwit_indexer::CompressionType::Zstd,
            compaction_threshold: 0.7,
            bloom_filter_fpp: 0.01,
        };
        let indexer = Arc::new(TantivyIndexer::new(index_config, metastore.clone()).await?);
        
        Ok(Self {
            metastore,
            indexer,
            index_path,
            stats: Default::default(),
            index_times: Vec::with_capacity(100),
            max_retries: 3,
            retry_queue: Vec::new(),
        })
    }

    /// Run the indexer actor
    pub async fn run(
        mut self,
        mut rx: mpsc::Receiver<IndexerMessage>,
    ) -> Result<()> {
        info!("🔍 Indexer actor starting");
        info!("  Index path: {}", self.index_path);
        info!("  Max retries: {}", self.max_retries);

        // Spawn retry timer
        let (retry_tx, mut retry_rx) = mpsc::channel(1);
        tokio::spawn(async move {
            let mut ticker = interval(Duration::from_secs(60)); // Check every minute
            loop {
                ticker.tick().await;
                if retry_tx.send(()).await.is_err() {
                    break;
                }
            }
        });

        // Check for pending segments on startup
        self.check_pending_segments().await?;

        loop {
            tokio::select! {
                Some(msg) = rx.recv() => {
                    match msg {
                        IndexerMessage::IndexSegment(segment_id) => {
                            self.handle_index_segment(segment_id).await?;
                        }
                        IndexerMessage::IndexBatch(segment_ids) => {
                            for segment_id in segment_ids {
                                self.handle_index_segment(segment_id).await?;
                            }
                        }
                        IndexerMessage::GetStats(tx) => {
                            self.update_stats().await?;
                            let _ = tx.send(self.stats.clone()).await;
                        }
                        IndexerMessage::RetryFailed => {
                            self.handle_retry_failed().await?;
                        }
                        IndexerMessage::Shutdown => {
                            info!("Indexer actor shutting down");
                            // Commit any pending changes
                            // TODO: Implement commit functionality
                            break;
                        }
                    }
                }
                Some(_) = retry_rx.recv() => {
                    // Periodic retry check
                    if !self.retry_queue.is_empty() {
                        self.handle_retry_failed().await?;
                    }
                }
            }
        }

        info!("Indexer actor stopped");
        Ok(())
    }

    /// Check for pending segments on startup
    async fn check_pending_segments(&mut self) -> Result<()> {
        info!("Checking for pending segments...");
        
        let pending_segments = self.metastore.list_segments_by_status(
            Some(SegmentStatus::Stored),
            Some(SegmentStatus::Pending),
        ).await?;
        
        if !pending_segments.is_empty() {
            info!("Found {} pending segments to index", pending_segments.len());
            for segment in pending_segments {
                self.handle_index_segment(segment.segment_id).await?;
            }
        }
        
        Ok(())
    }

    /// Handle indexing a single segment
    async fn handle_index_segment(&mut self, segment_id: String) -> Result<()> {
        let start = Instant::now();
        info!("📄 Indexing segment: {}", segment_id);

        // Get segment metadata
        let segment = match self.metastore.get_segment(&segment_id).await? {
            Some(seg) => seg,
            None => {
                error!("Segment {} not found in metastore", segment_id);
                return Ok(());
            }
        };

        // Check if already indexed
        if segment.index_status == SegmentStatus::Stored {
            debug!("Segment {} already indexed", segment_id);
            return Ok(());
        }

        // Check storage status
        if segment.storage_status != SegmentStatus::Stored {
            warn!("Segment {} not yet stored (status: {:?})", 
                segment_id, segment.storage_status);
            return Ok(());
        }

        // Index the segment
        match self.index_segment_file(&segment).await {
            Ok(messages_indexed) => {
                // Update metastore
                let _index_path = format!("{}/segment_{}", self.index_path, segment_id);
                // TODO: Implement mark_segment_indexed functionality

                // Update stats
                self.stats.segments_indexed += 1;
                self.stats.messages_indexed += messages_indexed;
                self.stats.bytes_indexed += segment.compressed_bytes;
                self.stats.last_index_time = Some(Utc::now());

                // Track indexing time
                let elapsed = start.elapsed().as_millis();
                self.index_times.push(elapsed);
                if self.index_times.len() > 100 {
                    self.index_times.remove(0);
                }

                info!("✅ Indexed segment {} in {}ms ({} messages)", 
                    segment_id, elapsed, messages_indexed);

                // Remove from retry queue if present
                self.retry_queue.retain(|(id, _)| id != &segment_id);

                Ok(())
            }
            Err(e) => {
                error!("Failed to index segment {}: {:?}", segment_id, e);
                self.stats.segments_failed += 1;

                // Add to retry queue
                let retry_count = self.retry_queue.iter()
                    .find(|(id, _)| id == &segment_id)
                    .map(|(_, count)| *count)
                    .unwrap_or(0);

                if retry_count < self.max_retries {
                    self.retry_queue.push((segment_id.clone(), retry_count + 1));
                    info!("Added segment {} to retry queue (attempt {})", 
                        segment_id, retry_count + 1);
                } else {
                    // Mark as permanently failed
                    self.metastore.mark_segment_index_failed(
                        &segment_id,
                        &format!("Failed after {} retries: {}", self.max_retries, e),
                    ).await?;
                    warn!("Segment {} permanently failed after {} retries", 
                        segment_id, self.max_retries);
                }

                Err(e)
            }
        }
    }

    /// Index a segment file
    async fn index_segment_file(&self, segment: &SegmentMetadata) -> Result<u64> {
        // Determine file location
        let _file_path = if segment.file_path.starts_with("s3://")
            || segment.file_path.starts_with("azure://") 
            || segment.file_path.starts_with("gs://") {
            // For cloud storage, download to temp file first
            // In a real implementation, you'd stream directly
            warn!("Cloud storage indexing not yet implemented for: {}", segment.file_path);
            return Err(anyhow::anyhow!("Cloud storage indexing not implemented"));
        } else {
            segment.file_path.clone()
        };

        // Read parquet file and index
        // TODO: Implement index_parquet_file functionality
        let messages_indexed = 0;

        // TODO: Implement commit functionality
        // self.indexer.commit().await?;

        Ok(messages_indexed)
    }

    /// Handle retrying failed segments
    async fn handle_retry_failed(&mut self) -> Result<()> {
        if self.retry_queue.is_empty() {
            return Ok(());
        }

        info!("Retrying {} failed segments", self.retry_queue.len());
        
        // Take segments to retry (avoid borrowing issues)
        let segments_to_retry: Vec<_> = self.retry_queue.drain(..).collect();
        
        for (segment_id, retry_count) in segments_to_retry {
            info!("Retrying segment {} (attempt {})", segment_id, retry_count);
            
            // Re-add to queue with updated count if it fails again
            match self.handle_index_segment(segment_id.clone()).await {
                Ok(_) => {
                    // Success - already removed from retry queue
                }
                Err(_) => {
                    // Failure - will be re-added by handle_index_segment
                }
            }
        }

        Ok(())
    }

    /// Update statistics
    async fn update_stats(&mut self) -> Result<()> {
        // Calculate average indexing time
        if !self.index_times.is_empty() {
            let sum: u128 = self.index_times.iter().sum();
            self.stats.average_index_time_ms = sum as f64 / self.index_times.len() as f64;
        }

        // Get pending count from metastore
        let pending = self.metastore.list_segments_by_status(
            Some(SegmentStatus::Stored),
            Some(SegmentStatus::Pending),
        ).await?;
        self.stats.segments_pending = pending.len() as u64;

        Ok(())
    }
}

/// Spawn an indexer actor
pub fn spawn_indexer_actor(
    metastore: Arc<dyn MetaStore>,
    index_path: String,
) -> mpsc::Sender<IndexerMessage> {
    let (tx, rx) = mpsc::channel(1000);
    
    tokio::spawn(async move {
        let actor = match IndexerActor::new(metastore, index_path).await {
            Ok(a) => a,
            Err(e) => {
                error!("Failed to create indexer actor: {:?}", e);
                return;
            }
        };
        
        if let Err(e) = actor.run(rx).await {
            error!("Indexer actor failed: {:?}", e);
        }
    });
    
    tx
}