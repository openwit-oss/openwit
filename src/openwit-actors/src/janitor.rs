use std::sync::Arc;
use std::time::Duration;
use anyhow::Result;
use tokio::sync::mpsc;
use tokio::time::interval;
use tracing::{info, error};
use chrono::{DateTime, Utc};

use openwit_metastore::MetaStore;
use openwit_janitor::{CompactionPolicy, RetentionPolicy, WalCleanupPolicy};

/// Messages that the JanitorActor can receive
#[derive(Debug)]
pub enum JanitorMessage {
    /// Run compaction immediately
    RunCompaction,
    /// Run retention cleanup immediately  
    RunRetention,
    /// Run WAL cleanup immediately
    RunWalCleanup,
    /// Get janitor statistics
    GetStats(mpsc::Sender<JanitorStats>),
    /// Update configuration
    UpdateConfig(JanitorConfig),
    /// Shutdown the actor
    Shutdown,
}

/// Janitor configuration
#[derive(Clone, Debug)]
pub struct JanitorConfig {
    pub run_interval_secs: u64,
    pub compaction_interval_secs: u64,
    pub retention_check_interval_secs: u64,
    pub wal_cleanup_interval_secs: u64,
    pub max_segments_per_partition: usize,
    pub retention_period_days: u64,
    pub wal_retention_hours: u64,
    pub data_dir: String,
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
            data_dir: "./data".to_string(),
        }
    }
}

/// Janitor statistics
#[derive(Debug, Default, Clone)]
pub struct JanitorStats {
    pub last_compaction_run: Option<DateTime<Utc>>,
    pub last_retention_run: Option<DateTime<Utc>>,
    pub last_wal_cleanup_run: Option<DateTime<Utc>>,
    pub segments_compacted: u64,
    pub segments_deleted: u64,
    pub wal_files_cleaned: u64,
    pub bytes_freed: u64,
}

/// Janitor actor that handles cleanup and maintenance tasks
#[allow(unused)]
pub struct JanitorActor {
    config: JanitorConfig,
    metastore: Arc<dyn MetaStore>,
    compaction_policy: CompactionPolicy,
    retention_policy: RetentionPolicy,
    wal_cleanup_policy: WalCleanupPolicy,
    stats: JanitorStats,
    last_compaction_run: DateTime<Utc>,
    last_retention_run: DateTime<Utc>,
    last_wal_cleanup_run: DateTime<Utc>,
}

impl JanitorActor {
    /// Create a new janitor actor
    pub fn new(
        config: JanitorConfig,
        metastore: Arc<dyn MetaStore>,
    ) -> Self {
        let data_path = std::path::PathBuf::from(&config.data_dir);
        
        Self {
            compaction_policy: CompactionPolicy::new(
                config.max_segments_per_partition,
                data_path.join("index"),
                metastore.clone(),
            ),
            retention_policy: RetentionPolicy::new(
                config.retention_period_days,
                data_path.join("storage"),
                metastore.clone(),
            ),
            wal_cleanup_policy: WalCleanupPolicy::new(
                config.wal_retention_hours,
                data_path.join("wal"),
                metastore.clone(),
            ),
            config,
            metastore,
            stats: Default::default(),
            last_compaction_run: Utc::now(),
            last_retention_run: Utc::now(),
            last_wal_cleanup_run: Utc::now(),
        }
    }

    /// Run the janitor actor
    pub async fn run(
        mut self,
        mut rx: mpsc::Receiver<JanitorMessage>,
    ) -> Result<()> {
        info!("🧹 Janitor actor starting");
        info!("  Run interval: {} seconds", self.config.run_interval_secs);
        info!("  Compaction interval: {} seconds", self.config.compaction_interval_secs);
        info!("  Retention check interval: {} seconds", self.config.retention_check_interval_secs);
        info!("  WAL cleanup interval: {} seconds", self.config.wal_cleanup_interval_secs);

        // Spawn periodic check timer
        let (check_tx, mut check_rx) = mpsc::channel(1);
        let run_interval = Duration::from_secs(self.config.run_interval_secs);
        
        tokio::spawn(async move {
            let mut ticker = interval(run_interval);
            loop {
                ticker.tick().await;
                if check_tx.send(()).await.is_err() {
                    break;
                }
            }
        });

        loop {
            tokio::select! {
                Some(msg) = rx.recv() => {
                    match msg {
                        JanitorMessage::RunCompaction => {
                            self.run_compaction().await?;
                        }
                        JanitorMessage::RunRetention => {
                            self.run_retention().await?;
                        }
                        JanitorMessage::RunWalCleanup => {
                            self.run_wal_cleanup().await?;
                        }
                        JanitorMessage::GetStats(tx) => {
                            let _ = tx.send(self.stats.clone()).await;
                        }
                        JanitorMessage::UpdateConfig(new_config) => {
                            info!("Updating janitor configuration");
                            self.config = new_config;
                        }
                        JanitorMessage::Shutdown => {
                            info!("Janitor actor shutting down");
                            break;
                        }
                    }
                }
                Some(_) = check_rx.recv() => {
                    // Periodic check - run tasks based on their intervals
                    let now = Utc::now();
                    
                    // Check if compaction is due
                    if (now - self.last_compaction_run).num_seconds() >= self.config.compaction_interval_secs as i64 {
                        self.run_compaction().await?;
                    }
                    
                    // Check if retention cleanup is due
                    if (now - self.last_retention_run).num_seconds() >= self.config.retention_check_interval_secs as i64 {
                        self.run_retention().await?;
                    }
                    
                    // Check if WAL cleanup is due
                    if (now - self.last_wal_cleanup_run).num_seconds() >= self.config.wal_cleanup_interval_secs as i64 {
                        self.run_wal_cleanup().await?;
                    }
                }
            }
        }

        info!("Janitor actor stopped");
        Ok(())
    }

    /// Run compaction
    async fn run_compaction(&mut self) -> Result<()> {
        info!("🗜️ Running compaction...");
        let start = std::time::Instant::now();
        
        // TODO: Implement compact method
        match Ok::<i32, anyhow::Error>(0) {
            Ok(compacted) => {
                self.stats.segments_compacted += compacted as u64;
                self.stats.last_compaction_run = Some(Utc::now());
                self.last_compaction_run = Utc::now();
                
                let elapsed = start.elapsed();
                info!("✅ Compaction completed in {:?}, {} segments compacted", 
                    elapsed, compacted);
            }
            Err(e) => {
                error!("Compaction failed: {:?}", e);
            }
        }
        
        Ok(())
    }

    /// Run retention cleanup
    async fn run_retention(&mut self) -> Result<()> {
        info!("🗑️ Running retention cleanup...");
        let start = std::time::Instant::now();
        
        // TODO: Implement cleanup method
        match Ok::<(i32, u64), anyhow::Error>((0, 0)) {
            Ok((deleted_count, bytes_freed)) => {
                self.stats.segments_deleted += deleted_count as u64;
                self.stats.bytes_freed += bytes_freed;
                self.stats.last_retention_run = Some(Utc::now());
                self.last_retention_run = Utc::now();
                
                let elapsed = start.elapsed();
                info!("✅ Retention cleanup completed in {:?}, {} segments deleted, {} MB freed", 
                    elapsed, deleted_count, bytes_freed / 1_048_576);
            }
            Err(e) => {
                error!("Retention cleanup failed: {:?}", e);
            }
        }
        
        Ok(())
    }

    /// Run WAL cleanup
    async fn run_wal_cleanup(&mut self) -> Result<()> {
        info!("📋 Running WAL cleanup...");
        let start = std::time::Instant::now();
        
        match self.wal_cleanup_policy.cleanup().await {
            Ok(cleaned_count) => {
                self.stats.wal_files_cleaned += cleaned_count.0 as u64;
                self.stats.last_wal_cleanup_run = Some(Utc::now());
                self.last_wal_cleanup_run = Utc::now();
                
                let elapsed = start.elapsed();
                info!("✅ WAL cleanup completed in {:?}, {} files cleaned", 
                    elapsed, cleaned_count.0);
            }
            Err(e) => {
                error!("WAL cleanup failed: {:?}", e);
            }
        }
        
        Ok(())
    }
}

/// Spawn a janitor actor
pub fn spawn_janitor_actor(
    config: JanitorConfig,
    metastore: Arc<dyn MetaStore>,
) -> mpsc::Sender<JanitorMessage> {
    let (tx, rx) = mpsc::channel(100);
    
    let actor = JanitorActor::new(config, metastore);
    
    tokio::spawn(async move {
        if let Err(e) = actor.run(rx).await {
            error!("Janitor actor failed: {:?}", e);
        }
    });
    
    tx
}