use anyhow::{Context, Result};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::Row;
use std::collections::{HashMap};
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tokio::time::{interval, Duration};
use tracing::{debug, error, info};
use uuid::Uuid;

use crate::{
    artifacts::{ZoneMap, ZoneMapEntry, BitmapIndex, SerializableBitmap},
    config::IndexerConfig,
    db::{DbClient, IndexArtifact, Snapshot},
    metrics,
    storage::{ArtifactMetadata, ArtifactType, StorageClient},
    wal::{WAL, WALOperation},
};

/// Partition accumulator for tracking deltas to combine
#[derive(Debug, Clone)]
pub struct PartitionAccumulator {
    pub tenant: String,
    pub signal: String,
    pub partition_key: String,
    pub level: CombineLevel,
    pub file_ulids: Vec<String>,
    pub total_bytes: u64,
    pub first_seen: DateTime<Utc>,
    pub last_updated: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CombineLevel {
    Hour,
    Day,
}

impl CombineLevel {
    pub fn as_str(&self) -> &'static str {
        match self {
            CombineLevel::Hour => "hour",
            CombineLevel::Day => "day",
        }
    }
}

/// Combined index manifest
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CombinedManifest {
    pub version: u32,
    pub level: String,
    pub partition_key: String,
    pub tenant: String,
    pub signal: String,
    pub file_ulids: Vec<String>,
    pub combined_ulid: String,
    pub artifacts: HashMap<String, String>, // artifact_type -> url
    pub statistics: ManifestStatistics,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestStatistics {
    pub total_rows: u64,
    pub total_bytes: u64,
    pub min_timestamp: DateTime<Utc>,
    pub max_timestamp: DateTime<Utc>,
    pub file_count: usize,
}

/// Manages combining of delta indexes into larger indexes
#[derive(Clone)]
pub struct CombineWorker {
    config: Arc<IndexerConfig>,
    db_client: Arc<DbClient>,
    storage_client: Arc<StorageClient>,
    wal: Arc<WAL>,
    accumulators: Arc<RwLock<HashMap<String, PartitionAccumulator>>>,
    combine_sender: mpsc::Sender<PartitionAccumulator>,
}

impl CombineWorker {
    pub fn new(
        config: Arc<IndexerConfig>,
        db_client: Arc<DbClient>,
        storage_client: Arc<StorageClient>,
        wal: Arc<WAL>,
    ) -> (Self, mpsc::Receiver<PartitionAccumulator>) {
        let (combine_sender, combine_receiver) = mpsc::channel(100);
        
        let worker = Self {
            config,
            db_client,
            storage_client,
            wal,
            accumulators: Arc::new(RwLock::new(HashMap::new())),
            combine_sender,
        };
        
        (worker, combine_receiver)
    }

    #[allow(unused_mut)]
    /// Start the combine worker
    pub async fn start(self, mut combine_receiver: mpsc::Receiver<PartitionAccumulator>, mut shutdown_rx: tokio::sync::broadcast::Receiver<()>) {
        info!("Starting combine worker");

        // Start accumulator checker
        let checker_handle = tokio::spawn(self.clone().run_accumulator_checker());

        // Start combine processor
        let processor_handle = tokio::spawn(self.clone().run_combine_processor(combine_receiver));

        // Wait for shutdown
        tokio::select! {
            _ = shutdown_rx.recv() => {
                info!("Combine worker received shutdown signal");
            }
            res = checker_handle => {
                if let Err(e) = res {
                    error!(error = %e, "Accumulator checker failed");
                }
            }
            res = processor_handle => {
                if let Err(e) = res {
                    error!(error = %e, "Combine processor failed");
                }
            }
        }
    }

    /// Check accumulators and trigger combines
    async fn run_accumulator_checker(self) {
        let mut check_interval = interval(Duration::from_secs(30));
        
        loop {
            check_interval.tick().await;
            
            let accumulators = self.accumulators.read().await;
            let mut to_combine = Vec::new();
            
            for (key, acc) in accumulators.iter() {
                if self.should_combine(acc) {
                    debug!(
                        partition = %key,
                        files = acc.file_ulids.len(),
                        bytes = acc.total_bytes,
                        "Triggering combine"
                    );
                    to_combine.push(acc.clone());
                }
            }
            drop(accumulators);
            
            // Update metrics
            let hour_count = to_combine.iter()
                .filter(|a| a.level == CombineLevel::Hour)
                .count();
            let day_count = to_combine.iter()
                .filter(|a| a.level == CombineLevel::Day)
                .count();
            
            metrics::update_queue_depths(0, hour_count, day_count);
            
            // Send to combine queue
            for acc in to_combine {
                if let Err(e) = self.combine_sender.send(acc).await {
                    error!(error = %e, "Failed to send to combine queue");
                }
            }
        }
    }

    /// Process combine requests
    async fn run_combine_processor(self, mut combine_receiver: mpsc::Receiver<PartitionAccumulator>) {
        while let Some(accumulator) = combine_receiver.recv().await {
            let start = std::time::Instant::now();
            
            match self.combine_partition(accumulator.clone()).await {
                Ok(manifest_url) => {
                    info!(
                        partition = %accumulator.partition_key,
                        level = %accumulator.level.as_str(),
                        files = accumulator.file_ulids.len(),
                        manifest_url = %manifest_url,
                        "Successfully combined partition"
                    );
                    
                    metrics::record_combine_success(
                        &accumulator.tenant,
                        &accumulator.signal,
                        accumulator.level.as_str(),
                        start.elapsed(),
                    );
                    
                    // Remove from accumulators
                    self.accumulators.write().await.remove(&accumulator.partition_key);
                }
                Err(e) => {
                    error!(
                        partition = %accumulator.partition_key,
                        error = %e,
                        "Failed to combine partition"
                    );
                    
                    metrics::record_combine_failed(
                        &accumulator.tenant,
                        &accumulator.signal,
                        accumulator.level.as_str(),
                    );
                }
            }
        }
    }

    /// Check if accumulator should trigger a combine
    fn should_combine(&self, acc: &PartitionAccumulator) -> bool {
        let settings = &self.config.indexer.merge;
        
        // Check triggers
        let count_trigger = acc.file_ulids.len() >= settings.trigger_count as usize;
        let size_trigger = acc.total_bytes >= (settings.trigger_bytes_gb as u64) * 1_000_000_000;
        let time_trigger = Utc::now().signed_duration_since(acc.first_seen)
            > chrono::Duration::seconds(settings.trigger_interval_sec as i64);
        
        // Check if partition is sealed (hour/day boundary passed)
        let sealed = is_partition_sealed(&acc.partition_key);
        
        count_trigger || size_trigger || time_trigger || sealed
    }

    /// Combine indexes for a partition
    async fn combine_partition(&self, accumulator: PartitionAccumulator) -> Result<String> {
        let combined_ulid = ulid::Ulid::new().to_string();
        
        // Log WAL operation
        let wal_op = WALOperation::MergeDeltas {
            merge_id: Uuid::new_v4(),
            tenant: accumulator.tenant.clone(),
            signal: accumulator.signal.clone(),
            level: accumulator.level.as_str().to_string(),
            partition_key: accumulator.partition_key.clone(),
            delta_file_ulids: accumulator.file_ulids.clone(),
            combined_manifest_url: String::new(), // Will be filled after upload
        };
        
        let operation_id = format!("combine_{}_{}", accumulator.partition_key, combined_ulid);
        self.wal.log_operation_start(operation_id.clone(), wal_op)?;
        
        info!(
            partition = %accumulator.partition_key,
            level = %accumulator.level.as_str(),
            files = accumulator.file_ulids.len(),
            "Starting combine operation"
        );
        
        // Fetch all artifacts for the files
        let artifacts = self.fetch_artifacts_for_files(&accumulator.file_ulids).await?;
        
        // Combine each artifact type
        let mut combined_artifacts = HashMap::new();
        
        // Combine zone maps
        let zone_map_url = self.combine_zone_maps(
            &combined_ulid,
            &accumulator,
            &artifacts,
        ).await?;
        combined_artifacts.insert("zone_map".to_string(), zone_map_url);
        
        // Combine bitmaps
        let bitmap_url = self.combine_bitmaps(
            &combined_ulid,
            &accumulator,
            &artifacts,
        ).await?;
        combined_artifacts.insert("bitmap".to_string(), bitmap_url);
        
        // Combine bloom filters
        let bloom_url = self.combine_blooms(
            &combined_ulid,
            &accumulator,
            &artifacts,
        ).await?;
        combined_artifacts.insert("bloom".to_string(), bloom_url);
        
        // Create and upload manifest
        let manifest = CombinedManifest {
            version: 1,
            level: accumulator.level.as_str().to_string(),
            partition_key: accumulator.partition_key.clone(),
            tenant: accumulator.tenant.clone(),
            signal: accumulator.signal.clone(),
            file_ulids: accumulator.file_ulids.clone(),
            combined_ulid: combined_ulid.clone(),
            artifacts: combined_artifacts.clone(),
            statistics: ManifestStatistics {
                total_rows: 0, // TODO: calculate from artifacts
                total_bytes: accumulator.total_bytes,
                min_timestamp: Utc::now(), // TODO: get from zone maps
                max_timestamp: Utc::now(), // TODO: get from zone maps
                file_count: accumulator.file_ulids.len(),
            },
            created_at: Utc::now(),
        };
        
        let manifest_url = self.storage_client.store_manifest(
            &accumulator.tenant,
            &accumulator.signal,
            accumulator.level.as_str(),
            &accumulator.partition_key,
            &serde_json::to_value(&manifest)?,
        ).await?;
        
        // Store in database
        let snapshot = Snapshot {
            snapshot_id: Uuid::new_v4(),
            tenant: accumulator.tenant.clone(),
            signal: accumulator.signal.clone(),
            level: accumulator.level.as_str().to_string(),
            partition_key: accumulator.partition_key.clone(),
            file_ulids: accumulator.file_ulids.clone(),
            created_at: Utc::now(),
            published_at: Some(Utc::now()),
            metadata: serde_json::to_value(&manifest)?,
        };
        
        let snapshot_id = self.db_client.insert_snapshot(&snapshot).await?;
        
        // Insert artifact records
        for (artifact_type, url) in combined_artifacts {
            let artifact = IndexArtifact {
                artifact_id: Uuid::new_v4(),
                file_ulid: combined_ulid.clone(),
                tenant: accumulator.tenant.clone(),
                signal: accumulator.signal.clone(),
                partition_key: accumulator.partition_key.clone(),
                artifact_type,
                artifact_url: url,
                size_bytes: 0, // TODO: track actual sizes
                created_at: Utc::now(),
                metadata: serde_json::json!({
                    "level": accumulator.level.as_str(),
                    "snapshot_id": snapshot_id,
                }),
            };
            
            self.db_client.insert_index_artifact(&artifact).await?;
        }
        
        // Mark snapshot as published
        self.db_client.publish_snapshot(snapshot_id).await?;
        
        // Log completion
        self.wal.log_operation_completed(operation_id)?;
        
        Ok(manifest_url)
    }

    /// Fetch all artifacts for given files
    async fn fetch_artifacts_for_files(
        &self,
        file_ulids: &[String],
    ) -> Result<HashMap<String, Vec<(String, Bytes)>>> {
        let mut artifacts = HashMap::new();
        
        // This is simplified - in production you'd batch fetch
        for file_ulid in file_ulids {
            // Query artifacts from database
            let query = r#"
                SELECT artifact_type, artifact_url
                FROM indexer.index_artifacts
                WHERE file_ulid = $1
            "#;
            
            let rows = sqlx::query(query)
                .bind(file_ulid)
                .fetch_all(self.db_client.pool())
                .await?;
            
            for row in rows {
                let artifact_type: String = row.get::<String, _>("artifact_type");
                let artifact_url: String = row.get::<String, _>("artifact_url");
                
                // Fetch artifact data
                if let Ok(data) = self.storage_client.get_artifact(&artifact_url).await {
                    artifacts
                        .entry(artifact_type)
                        .or_insert_with(Vec::new)
                        .push((file_ulid.clone(), data));
                }
            }
        }
        
        Ok(artifacts)
    }

    /// Combine zone maps
    async fn combine_zone_maps(
        &self,
        combined_ulid: &str,
        accumulator: &PartitionAccumulator,
        artifacts: &HashMap<String, Vec<(String, Bytes)>>,
    ) -> Result<String> {
        let zone_maps = artifacts.get("zone_map")
            .context("No zone maps found")?;
        
        let mut combined_entries: HashMap<String, ZoneMapEntry> = HashMap::new();
        
        for (_file_ulid, data) in zone_maps {
            let zone_map = ZoneMap::deserialize(data)?;
            
            for entry in zone_map.row_group_stats {
                let key = format!("{}-{}", entry.column_name, entry.row_group_id);
                
                match combined_entries.get_mut(&key) {
                    Some(existing) => {
                        // Merge min/max values
                        if let (Some(min1), Some(min2)) = (&existing.min_value, &entry.min_value) {
                            existing.min_value = Some(min1.min(min2).to_string());
                        }
                        if let (Some(max1), Some(max2)) = (&existing.max_value, &entry.max_value) {
                            existing.max_value = Some(max1.max(max2).to_string());
                        }
                        existing.null_count += entry.null_count;
                        existing.row_count += entry.row_count;
                    }
                    None => {
                        combined_entries.insert(key, entry);
                    }
                }
            }
        }
        
        let combined_zone_map = ZoneMap {
            file_ulid: combined_ulid.to_string(),
            partition_key: accumulator.partition_key.clone(),
            row_group_stats: combined_entries.into_values().collect(),
            created_at: Utc::now(),
        };
        
        let data = combined_zone_map.serialize()?;
        let metadata = ArtifactMetadata {
            file_ulid: combined_ulid.to_string(),
            tenant: accumulator.tenant.clone(),
            signal: accumulator.signal.clone(),
            partition_key: accumulator.partition_key.clone(),
            artifact_type: ArtifactType::ZoneMap,
            size_bytes: data.len() as u64,
            content_hash: crate::storage::calculate_hash(&data),
            created_at: Utc::now(),
        };
        
        let url = self.storage_client.store_artifact(&metadata, data).await?;
        Ok(url)
    }

    /// Combine bitmap indexes
    async fn combine_bitmaps(
        &self,
        combined_ulid: &str,
        accumulator: &PartitionAccumulator,
        artifacts: &HashMap<String, Vec<(String, Bytes)>>,
    ) -> Result<String> {
        let bitmaps = artifacts.get("bitmap")
            .context("No bitmaps found")?;
        
        let mut combined_bitmaps: HashMap<String, HashMap<String, SerializableBitmap>> = HashMap::new();
        
        for (_file_ulid, data) in bitmaps {
            let bitmap_index = BitmapIndex::deserialize(data)?;
            
            for (column, value_bitmaps) in bitmap_index.column_bitmaps {
                for (value, bitmap) in value_bitmaps {
                    combined_bitmaps
                        .entry(column.clone())
                        .or_insert_with(HashMap::new)
                        .entry(value.clone())
                        .and_modify(|existing| {
                            let existing_sb = existing as &mut SerializableBitmap;
                            existing_sb.0 |= &bitmap.0;
                        })
                        .or_insert(bitmap);
                }
            }
        }
        
        let combined_bitmap = BitmapIndex {
            file_ulid: combined_ulid.to_string(),
            partition_key: accumulator.partition_key.clone(),
            column_bitmaps: combined_bitmaps,
            created_at: Utc::now(),
        };
        
        let data = combined_bitmap.serialize()?;
        let metadata = ArtifactMetadata {
            file_ulid: combined_ulid.to_string(),
            tenant: accumulator.tenant.clone(),
            signal: accumulator.signal.clone(),
            partition_key: accumulator.partition_key.clone(),
            artifact_type: ArtifactType::Bitmap,
            size_bytes: data.len() as u64,
            content_hash: crate::storage::calculate_hash(&data),
            created_at: Utc::now(),
        };
        
        let url = self.storage_client.store_artifact(&metadata, data).await?;
        Ok(url)
    }

    /// Combine bloom filters
    async fn combine_blooms(
        &self,
        combined_ulid: &str,
        accumulator: &PartitionAccumulator,
        artifacts: &HashMap<String, Vec<(String, Bytes)>>,
    ) -> Result<String> {
        // For bloom filters, we bundle them rather than merge
        // as merging would increase false positive rate
        
        let blooms = artifacts.get("bloom")
            .context("No bloom filters found")?;
        
        #[derive(Serialize)]
        struct BloomBundle {
            file_ulid: String,
            partition_key: String,
            bloom_data: Vec<(String, Vec<u8>)>, // file_ulid -> compressed bloom
            created_at: DateTime<Utc>,
        }
        
        let mut bloom_data = Vec::new();
        for (file_ulid, data) in blooms {
            bloom_data.push((file_ulid.clone(), data.to_vec()));
        }
        
        let bundle = BloomBundle {
            file_ulid: combined_ulid.to_string(),
            partition_key: accumulator.partition_key.clone(),
            bloom_data,
            created_at: Utc::now(),
        };
        
        let data = Bytes::from(zstd::encode_all(
            serde_json::to_vec(&bundle)?.as_slice(),
            3,
        )?);
        
        let metadata = ArtifactMetadata {
            file_ulid: combined_ulid.to_string(),
            tenant: accumulator.tenant.clone(),
            signal: accumulator.signal.clone(),
            partition_key: accumulator.partition_key.clone(),
            artifact_type: ArtifactType::Bloom,
            size_bytes: data.len() as u64,
            content_hash: crate::storage::calculate_hash(&data),
            created_at: Utc::now(),
        };
        
        let url = self.storage_client.store_artifact(&metadata, data).await?;
        Ok(url)
    }

    /// Track a new file for potential combining
    pub async fn track_file(
        &self,
        tenant: String,
        signal: String,
        partition_key: String,
        file_ulid: String,
        size_bytes: u64,
    ) -> Result<()> {
        let level = determine_combine_level(&partition_key);
        let key = format!("{}/{}/{}/{:?}", tenant, signal, partition_key, level);
        
        let mut accumulators = self.accumulators.write().await;
        
        let accumulator = accumulators
            .entry(key)
            .or_insert_with(|| PartitionAccumulator {
                tenant: tenant.clone(),
                signal: signal.clone(),
                partition_key: partition_key.clone(),
                level,
                file_ulids: Vec::new(),
                total_bytes: 0,
                first_seen: Utc::now(),
                last_updated: Utc::now(),
            });
        
        accumulator.file_ulids.push(file_ulid);
        accumulator.total_bytes += size_bytes;
        accumulator.last_updated = Utc::now();
        
        debug!(
            partition = %partition_key,
            files = accumulator.file_ulids.len(),
            total_bytes = accumulator.total_bytes,
            "Tracked file for combining"
        );
        
        Ok(())
    }

    /// Clone for spawning
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            db_client: self.db_client.clone(),
            storage_client: self.storage_client.clone(),
            wal: self.wal.clone(),
            accumulators: self.accumulators.clone(),
            combine_sender: self.combine_sender.clone(),
        }
    }
}

/// Determine combine level based on partition key
fn determine_combine_level(partition_key: &str) -> CombineLevel {
    // Partition keys are like "2024-01-01-00"
    // Hour partitions get combined to hour level
    // Day boundaries get combined to day level
    if partition_key.ends_with("-00") || partition_key.ends_with("-23") {
        CombineLevel::Day
    } else {
        CombineLevel::Hour
    }
}

/// Check if a partition is sealed (past its time boundary)
fn is_partition_sealed(partition_key: &str) -> bool {
    // Parse partition key to check if hour/day has passed
    // Format: YYYY-MM-DD-HH
    if let Ok(partition_time) = chrono::NaiveDateTime::parse_from_str(
        &format!("{} 00:00:00", partition_key.replace('-', " ")),
        "%Y %m %d %H %M %S",
    ) {
        let partition_dt = DateTime::<Utc>::from_naive_utc_and_offset(partition_time, Utc);
        let hour_passed = Utc::now() > partition_dt + chrono::Duration::hours(1);
        hour_passed
    } else {
        false
    }
}