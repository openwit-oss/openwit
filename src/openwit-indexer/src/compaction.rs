//! Index compaction and optimization strategies

use chrono::{DateTime, Utc};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Manages index compaction across different index types
pub struct CompactionManager {
    /// Compaction policies
    policies: Arc<RwLock<Vec<Box<dyn CompactionPolicy>>>>,
    /// Compaction statistics
    stats: Arc<RwLock<CompactionStats>>,
}

#[async_trait::async_trait]
pub trait CompactionPolicy: Send + Sync {
    /// Check if compaction should run
    async fn should_compact(&self, stats: &IndexHealth) -> bool;
    
    /// Get compaction configuration
    fn config(&self) -> CompactionConfig;
}

#[derive(Debug, Clone)]
pub struct IndexHealth {
    pub total_size_bytes: u64,
    pub file_count: usize,
    pub fragmentation_ratio: f64,
    pub last_compaction: Option<DateTime<Utc>>,
    pub write_amplification: f64,
    pub avg_query_time_ms: f64,
}

#[derive(Debug, Clone)]
pub struct CompactionConfig {
    pub target_file_size: u64,
    pub max_concurrent_compactions: usize,
    pub compression_level: i32,
    pub delete_tombstone_age_secs: u64,
}

#[derive(Debug, Default)]
pub struct CompactionStats {
    pub total_compactions: u64,
    pub bytes_compacted: u64,
    pub bytes_saved: u64,
    pub last_compaction: Option<DateTime<Utc>>,
    pub avg_compaction_time_ms: f64,
}

impl CompactionManager {
    pub fn new() -> Self {
        Self {
            policies: Arc::new(RwLock::new(Vec::new())),
            stats: Arc::new(RwLock::new(CompactionStats::default())),
        }
    }
    
    pub async fn add_policy(&self, policy: Box<dyn CompactionPolicy>) {
        self.policies.write().await.push(policy);
    }
    
    pub async fn should_compact(&self, health: &IndexHealth) -> Option<CompactionConfig> {
        let policies = self.policies.read().await;
        
        for policy in policies.iter() {
            if policy.should_compact(health).await {
                return Some(policy.config());
            }
        }
        
        None
    }
    
    pub async fn record_compaction(&self, bytes_before: u64, bytes_after: u64, duration_ms: u64) {
        let mut stats = self.stats.write().await;
        stats.total_compactions += 1;
        stats.bytes_compacted += bytes_before;
        stats.bytes_saved += bytes_before.saturating_sub(bytes_after);
        stats.last_compaction = Some(Utc::now());
        
        // Update rolling average
        let avg = stats.avg_compaction_time_ms;
        let count = stats.total_compactions as f64;
        stats.avg_compaction_time_ms = (avg * (count - 1.0) + duration_ms as f64) / count;
    }
}

/// Size-based compaction policy
#[allow(unused)]
pub struct SizeBasedPolicy {
    min_file_size: u64,
    max_file_size: u64,
    min_files_for_compaction: usize,
}

#[async_trait::async_trait]
impl CompactionPolicy for SizeBasedPolicy {
    async fn should_compact(&self, stats: &IndexHealth) -> bool {
        stats.file_count >= self.min_files_for_compaction ||
        stats.total_size_bytes > self.max_file_size * stats.file_count as u64
    }
    
    fn config(&self) -> CompactionConfig {
        CompactionConfig {
            target_file_size: self.max_file_size,
            max_concurrent_compactions: 2,
            compression_level: 6,
            delete_tombstone_age_secs: 86400, // 1 day
        }
    }
}

/// Time-based compaction policy
pub struct TimeBasedPolicy {
    compaction_interval: Duration,
}

#[async_trait::async_trait]
impl CompactionPolicy for TimeBasedPolicy {
    async fn should_compact(&self, stats: &IndexHealth) -> bool {
        match stats.last_compaction {
            Some(last) => Utc::now().signed_duration_since(last).to_std().unwrap_or_default() > self.compaction_interval,
            None => true,
        }
    }
    
    fn config(&self) -> CompactionConfig {
        CompactionConfig {
            target_file_size: 64 * 1024 * 1024, // 64MB
            max_concurrent_compactions: 1,
            compression_level: 9, // Max compression for time-based
            delete_tombstone_age_secs: 3600, // 1 hour
        }
    }
}

use std::time::Duration;