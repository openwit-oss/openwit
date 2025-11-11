use std::path::PathBuf;
use std::sync::Arc;
use anyhow::Result;
use chrono::{DateTime, Utc, Duration};
use tokio::sync::RwLock;
use tokio::time::interval;
use tracing::{info, warn, error};
use metrics::{counter, gauge};

use openwit_config::UnifiedConfig;
use openwit_metastore::{MetaStore, TimeRange};

use crate::{CompactionPolicy, RetentionPolicy, WalCleanupPolicy};

#[derive(Clone)]
pub struct JanitorConfig {
    pub run_interval_secs: u64,
    pub compaction_interval_secs: u64,
    pub retention_check_interval_secs: u64,
    pub wal_cleanup_interval_secs: u64,
    pub max_segments_per_partition: usize,
    pub retention_period_days: u64,
    pub wal_retention_hours: u64,
}

impl Default for JanitorConfig {
    fn default() -> Self {
        Self {
            run_interval_secs: 300, // 5 minutes
            compaction_interval_secs: 3600, // 1 hour
            retention_check_interval_secs: 86400, // 24 hours
            wal_cleanup_interval_secs: 1800, // 30 minutes
            max_segments_per_partition: 10,
            retention_period_days: 30,
            wal_retention_hours: 24,
        }
    }
}

pub struct JanitorActor {
    config: JanitorConfig,
    metastore: Arc<dyn MetaStore>,
    compaction_policy: CompactionPolicy,
    retention_policy: RetentionPolicy,
    wal_cleanup_policy: WalCleanupPolicy,
    last_compaction_run: RwLock<DateTime<Utc>>,
    last_retention_run: RwLock<DateTime<Utc>>,
    last_wal_cleanup_run: RwLock<DateTime<Utc>>,
}

impl JanitorActor {
    pub fn new(
        config: JanitorConfig,
        metastore: Arc<dyn MetaStore>,
        openwit_config: &UnifiedConfig,
    ) -> Self {
        let data_dir = PathBuf::from(&openwit_config.storage.local.path);
        
        Self {
            config: config.clone(),
            metastore: metastore.clone(),
            compaction_policy: CompactionPolicy::new(
                config.max_segments_per_partition,
                data_dir.join("index"),
                metastore.clone(),
            ),
            retention_policy: RetentionPolicy::new(
                config.retention_period_days,
                data_dir.join("storage"),
                metastore.clone(),
            ),
            wal_cleanup_policy: WalCleanupPolicy::new(
                config.wal_retention_hours,
                data_dir.join("wal"),
                metastore,
            ),
            last_compaction_run: RwLock::new(Utc::now()),
            last_retention_run: RwLock::new(Utc::now()),
            last_wal_cleanup_run: RwLock::new(Utc::now()),
        }
    }

    pub async fn run(&self) -> Result<()> {
        info!("Starting JanitorActor with run interval: {}s", self.config.run_interval_secs);
        
        let mut ticker = interval(std::time::Duration::from_secs(self.config.run_interval_secs));
        
        loop {
            ticker.tick().await;
            
            // Run cleanup tasks based on their intervals
            let now = Utc::now();
            
            // Check if compaction is due
            if self.should_run_compaction(now).await {
                if let Err(e) = self.run_compaction().await {
                    error!("Compaction failed: {}", e);
                    counter!("janitor_compaction_errors", 1);
                }
            }
            
            // Check if retention cleanup is due
            if self.should_run_retention(now).await {
                if let Err(e) = self.run_retention_cleanup().await {
                    error!("Retention cleanup failed: {}", e);
                    counter!("janitor_retention_errors", 1);
                }
            }
            
            // Check if WAL cleanup is due
            if self.should_run_wal_cleanup(now).await {
                if let Err(e) = self.run_wal_cleanup().await {
                    error!("WAL cleanup failed: {}", e);
                    counter!("janitor_wal_cleanup_errors", 1);
                }
            }
        }
    }

    async fn should_run_compaction(&self, now: DateTime<Utc>) -> bool {
        let last_run = self.last_compaction_run.read().await;
        now.signed_duration_since(*last_run).num_seconds() >= self.config.compaction_interval_secs as i64
    }

    async fn should_run_retention(&self, now: DateTime<Utc>) -> bool {
        let last_run = self.last_retention_run.read().await;
        now.signed_duration_since(*last_run).num_seconds() >= self.config.retention_check_interval_secs as i64
    }

    async fn should_run_wal_cleanup(&self, now: DateTime<Utc>) -> bool {
        let last_run = self.last_wal_cleanup_run.read().await;
        now.signed_duration_since(*last_run).num_seconds() >= self.config.wal_cleanup_interval_secs as i64
    }

    async fn run_compaction(&self) -> Result<()> {
        info!("Starting compaction run");
        let start = Utc::now();
        counter!("janitor_compaction_runs", 1);
        
        let time_range = TimeRange {
            start: DateTime::<Utc>::MIN_UTC,
            end: DateTime::<Utc>::MAX_UTC,
        };
        let partitions = self.metastore.list_partitions(time_range).await?;
        let mut total_segments_compacted = 0;
        
        for partition in partitions {
            match self.compaction_policy.compact_partition(&partition).await {
                Ok(segments_compacted) => {
                    total_segments_compacted += segments_compacted;
                    if segments_compacted > 0 {
                        info!("Compacted {} segments in partition {}", segments_compacted, partition.partition_id);
                    }
                }
                Err(e) => {
                    warn!("Failed to compact partition {}: {}", partition.partition_id, e);
                }
            }
        }
        
        let duration = Utc::now().signed_duration_since(start);
        gauge!("janitor_compaction_duration_ms", duration.num_milliseconds() as f64);
        gauge!("janitor_compaction_segments_compacted", total_segments_compacted as f64);
        
        *self.last_compaction_run.write().await = Utc::now();
        info!("Compaction run completed in {}ms, compacted {} segments", 
              duration.num_milliseconds(), total_segments_compacted);
        
        Ok(())
    }

    async fn run_retention_cleanup(&self) -> Result<()> {
        info!("Starting retention cleanup run");
        let start = Utc::now();
        counter!("janitor_retention_runs", 1);
        
        let cutoff_time = Utc::now() - Duration::days(self.config.retention_period_days as i64);
        let (partitions_removed, files_removed, bytes_removed) = 
            self.retention_policy.cleanup_old_data(cutoff_time).await?;
        
        let duration = Utc::now().signed_duration_since(start);
        gauge!("janitor_retention_duration_ms", duration.num_milliseconds() as f64);
        gauge!("janitor_retention_partitions_removed", partitions_removed as f64);
        gauge!("janitor_retention_files_removed", files_removed as f64);
        gauge!("janitor_retention_bytes_removed", bytes_removed as f64);
        
        *self.last_retention_run.write().await = Utc::now();
        info!("Retention cleanup completed in {}ms, removed {} partitions, {} files, {} bytes",
              duration.num_milliseconds(), partitions_removed, files_removed, bytes_removed);
        
        Ok(())
    }

    async fn run_wal_cleanup(&self) -> Result<()> {
        info!("Starting WAL cleanup run");
        let start = Utc::now();
        counter!("janitor_wal_cleanup_runs", 1);
        
        let (files_removed, bytes_removed) = self.wal_cleanup_policy.cleanup().await?;
        
        let duration = Utc::now().signed_duration_since(start);
        gauge!("janitor_wal_cleanup_duration_ms", duration.num_milliseconds() as f64);
        gauge!("janitor_wal_cleanup_files_removed", files_removed as f64);
        gauge!("janitor_wal_cleanup_bytes_removed", bytes_removed as f64);
        
        *self.last_wal_cleanup_run.write().await = Utc::now();
        info!("WAL cleanup completed in {}ms, removed {} files, {} bytes",
              duration.num_milliseconds(), files_removed, bytes_removed);
        
        Ok(())
    }
}