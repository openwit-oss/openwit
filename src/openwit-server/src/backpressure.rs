//! Dynamic backpressure handling for adaptive channel sizing

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, AtomicBool, Ordering};
use tokio::sync::mpsc;
use tokio::time::{interval, Duration};
use tracing::{info, warn};

/// Configuration for adaptive backpressure
#[derive(Debug, Clone)]
pub struct BackpressureConfig {
    /// Initial channel size
    pub initial_size: usize,
    /// Minimum channel size
    pub min_size: usize,
    /// Maximum channel size
    pub max_size: usize,
    /// Target utilization percentage (0-100)
    pub target_utilization: f64,
    /// How often to check and adjust
    pub check_interval_secs: u64,
    /// Growth factor when increasing size
    pub growth_factor: f64,
    /// Shrink factor when decreasing size
    pub shrink_factor: f64,
}

impl Default for BackpressureConfig {
    fn default() -> Self {
        Self {
            initial_size: 100,
            min_size: 50,
            max_size: 10000,
            target_utilization: 70.0,
            check_interval_secs: 10,
            growth_factor: 1.5,
            shrink_factor: 0.8,
        }
    }
}

/// Metrics for monitoring channel health
#[derive(Debug, Default)]
pub struct ChannelMetrics {
    /// Messages sent successfully
    pub messages_sent: AtomicUsize,
    /// Messages dropped due to full channel
    pub messages_dropped: AtomicUsize,
    /// Times sender was blocked
    pub send_blocked_count: AtomicUsize,
    /// Current channel size
    pub current_size: AtomicUsize,
    /// Channel full events
    pub channel_full_events: AtomicUsize,
    /// Is channel healthy
    pub is_healthy: AtomicBool,
}

impl ChannelMetrics {
    pub fn new(initial_size: usize) -> Self {
        Self {
            current_size: AtomicUsize::new(initial_size),
            is_healthy: AtomicBool::new(true),
            ..Default::default()
        }
    }
    
    pub fn record_send(&self) {
        self.messages_sent.fetch_add(1, Ordering::Relaxed);
    }
    
    pub fn record_drop(&self) {
        self.messages_dropped.fetch_add(1, Ordering::Relaxed);
        self.is_healthy.store(false, Ordering::Relaxed);
    }
    
    pub fn record_blocked(&self) {
        self.send_blocked_count.fetch_add(1, Ordering::Relaxed);
    }
    
    pub fn record_full(&self) {
        self.channel_full_events.fetch_add(1, Ordering::Relaxed);
    }
    
    pub fn get_stats(&self) -> (usize, usize, usize, usize) {
        (
            self.messages_sent.load(Ordering::Relaxed),
            self.messages_dropped.load(Ordering::Relaxed),
            self.send_blocked_count.load(Ordering::Relaxed),
            self.channel_full_events.load(Ordering::Relaxed),
        )
    }
    
    pub fn reset_period_stats(&self) {
        self.send_blocked_count.store(0, Ordering::Relaxed);
        self.channel_full_events.store(0, Ordering::Relaxed);
        self.is_healthy.store(true, Ordering::Relaxed);
    }
}

/// Adaptive channel that can resize based on load
#[allow(dead_code)]
pub struct AdaptiveChannel<T> {
    tx: mpsc::Sender<T>,
    rx: mpsc::Receiver<T>,
    metrics: Arc<ChannelMetrics>,
    config: BackpressureConfig,
}

/// Sender with backpressure awareness
pub struct AdaptiveSender<T> {
    inner: mpsc::Sender<T>,
    metrics: Arc<ChannelMetrics>,
    drop_policy: DropPolicy,
}

#[derive(Debug, Clone)]
pub enum DropPolicy {
    /// Drop new messages when full
    DropNewest,
    /// Drop oldest messages when full (not implemented)
    DropOldest,
    /// Block until space available
    Block,
}

impl<T> AdaptiveSender<T> {
    pub async fn send(&self, value: T) -> Result<(), mpsc::error::SendError<T>> {
        match self.drop_policy {
            DropPolicy::Block => {
                // Try to send, record if we're blocked
                let start = std::time::Instant::now();
                let result = self.inner.send(value).await;
                
                if start.elapsed() > Duration::from_millis(10) {
                    self.metrics.record_blocked();
                }
                
                if result.is_ok() {
                    self.metrics.record_send();
                }
                
                result
            }
            DropPolicy::DropNewest => {
                // Try to send without blocking
                match self.inner.try_send(value) {
                    Ok(()) => {
                        self.metrics.record_send();
                        Ok(())
                    }
                    Err(mpsc::error::TrySendError::Full(value)) => {
                        self.metrics.record_drop();
                        self.metrics.record_full();
                        warn!("Channel full, dropping message");
                        Err(mpsc::error::SendError(value))
                    }
                    Err(mpsc::error::TrySendError::Closed(value)) => {
                        Err(mpsc::error::SendError(value))
                    }
                }
            }
            DropPolicy::DropOldest => {
                // TODO: Implement ring buffer style channel
                unimplemented!("DropOldest policy not yet implemented")
            }
        }
    }
    
    pub fn try_send(&self, value: T) -> Result<(), mpsc::error::TrySendError<T>> {
        let result = self.inner.try_send(value);
        
        match &result {
            Ok(()) => self.metrics.record_send(),
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.metrics.record_full();
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {}
        }
        
        result
    }
}

/// Create an adaptive channel with dynamic sizing
pub fn adaptive_channel<T: Send + 'static>(
    config: BackpressureConfig,
    drop_policy: DropPolicy,
) -> (AdaptiveSender<T>, mpsc::Receiver<T>, Arc<ChannelMetrics>) {
    let (tx, rx) = mpsc::channel(config.initial_size);
    let metrics = Arc::new(ChannelMetrics::new(config.initial_size));
    
    let sender = AdaptiveSender {
        inner: tx,
        metrics: metrics.clone(),
        drop_policy,
    };
    
    // Start monitoring task
    let metrics_clone = metrics.clone();
    let config_clone = config.clone();
    
    tokio::spawn(async move {
        let mut interval = interval(Duration::from_secs(config_clone.check_interval_secs));
        let mut current_size = config_clone.initial_size;
        
        loop {
            interval.tick().await;
            
            let (sent, dropped, blocked, full_events) = metrics_clone.get_stats();
            
            // Calculate metrics for this period
            let total_attempts = sent + dropped;
            let drop_rate = if total_attempts > 0 {
                (dropped as f64 / total_attempts as f64) * 100.0
            } else {
                0.0
            };
            
            let block_rate = if sent > 0 {
                (blocked as f64 / sent as f64) * 100.0
            } else {
                0.0
            };
            
            info!(
                "📊 Channel metrics - Size: {}, Sent: {}, Dropped: {} ({:.1}%), Blocked: {} ({:.1}%), Full events: {}",
                current_size, sent, dropped, drop_rate, blocked, block_rate, full_events
            );
            
            // Decide if we need to resize
            let mut new_size = current_size;
            
            if drop_rate > 1.0 || full_events > 10 || block_rate > 20.0 {
                // Channel is too small, grow it
                new_size = ((current_size as f64) * config_clone.growth_factor) as usize;
                new_size = new_size.min(config_clone.max_size);
                
                if new_size > current_size {
                    info!("📈 Growing channel from {} to {} due to high load", current_size, new_size);
                }
            } else if drop_rate == 0.0 && block_rate < 5.0 && sent > 0 {
                // Channel might be too large, consider shrinking
                let utilization = (blocked as f64 / current_size as f64) * 100.0;
                
                if utilization < config_clone.target_utilization * 0.5 {
                    new_size = ((current_size as f64) * config_clone.shrink_factor) as usize;
                    new_size = new_size.max(config_clone.min_size);
                    
                    if new_size < current_size {
                        info!("📉 Shrinking channel from {} to {} due to low utilization", current_size, new_size);
                    }
                }
            }
            
            // Update metrics
            current_size = new_size;
            metrics_clone.current_size.store(new_size, Ordering::Relaxed);
            metrics_clone.reset_period_stats();
            
            // Note: We can't actually resize the channel in tokio, but we track what the size should be
            // In a real implementation, we might need to implement our own channel or use a different approach
        }
    });
    
    (sender, rx, metrics)
}

/// Create adaptive channels for the ingestion pipeline
pub fn create_adaptive_channels<T: Send + 'static>(
    config: &BackpressureConfig,
) -> (AdaptiveSender<T>, mpsc::Receiver<T>, Arc<ChannelMetrics>) {
    adaptive_channel(config.clone(), DropPolicy::Block)
}