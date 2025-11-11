use sysinfo::{System, Disks};
use openwit_config::unified::NodeHealthThresholds;
use serde::{Serialize, Deserialize};
use std::time::{Duration, Instant};
use tracing::{info, warn, debug};

/// Health status of a node
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum HealthStatus {
    /// Node is operating normally
    Healthy,
    /// Node is experiencing minor issues
    Warning,
    /// Node is experiencing critical issues
    Unhealthy,
}

/// Detailed health metrics for a node
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeHealth {
    /// Overall health status
    pub status: HealthStatus,
    /// CPU usage percentage
    pub cpu_percent: f32,
    /// Memory usage percentage
    pub memory_percent: f32,
    /// Disk usage percentage
    pub disk_percent: f32,
    /// Network usage percentage
    pub network_percent: f32,
    /// Timestamp of the health check
    pub timestamp: String,
    /// Detailed health information
    pub details: String,
}

/// Health monitor that tracks system metrics
pub struct HealthMonitor {
    thresholds: NodeHealthThresholds,
    system: System,
    disks: Disks,
    last_unhealthy: Option<Instant>,
    last_healthy: Option<Instant>,
}

impl HealthMonitor {
    /// Create a new health monitor with the given thresholds
    pub fn new(thresholds: NodeHealthThresholds) -> Self {
        Self {
            thresholds,
            system: System::new_all(),
            disks: Disks::new_with_refreshed_list(),
            last_unhealthy: None,
            last_healthy: None,
        }
    }
    
    /// Calculate current node health
    pub fn calculate_health(&mut self) -> NodeHealth {
        // Refresh system info
        self.system.refresh_all();
        self.disks.refresh(true);
        
        // Calculate CPU usage
        let cpu_percent = self.calculate_cpu_usage();
        
        // Calculate memory usage
        let memory_percent = self.calculate_memory_usage();
        
        // Calculate disk usage
        let disk_percent = self.calculate_disk_usage();
        
        // Network usage (placeholder - would need actual network monitoring)
        let network_percent = 0.0; // TODO: Implement actual network monitoring
        
        // Determine overall health status
        let status = self.determine_health_status(
            cpu_percent,
            memory_percent,
            disk_percent,
            network_percent,
        );
        
        // Build details string
        let details = format!(
            "CPU: {:.1}%, Memory: {:.1}%, Disk: {:.1}%, Network: {:.1}%",
            cpu_percent, memory_percent, disk_percent, network_percent
        );
        
        // Update health tracking
        match status {
            HealthStatus::Unhealthy => {
                if self.last_unhealthy.is_none() {
                    self.last_unhealthy = Some(Instant::now());
                    warn!("Node became unhealthy: {}", details);
                }
                self.last_healthy = None;
            }
            HealthStatus::Healthy => {
                if self.last_healthy.is_none() {
                    self.last_healthy = Some(Instant::now());
                    info!("Node became healthy: {}", details);
                }
                self.last_unhealthy = None;
            }
            HealthStatus::Warning => {
                debug!("Node health warning: {}", details);
            }
        }
        
        // Update Prometheus metrics
        super::SYSTEM_CPU_GAUGE.set(cpu_percent as i64);
        super::SYSTEM_MEM_GAUGE.set((memory_percent * self.system.total_memory() as f32 / 100.0) as i64);
        
        NodeHealth {
            status,
            cpu_percent,
            memory_percent,
            disk_percent,
            network_percent,
            timestamp: chrono::Utc::now().to_rfc3339(),
            details,
        }
    }
    
    /// Check if node should be in the routing pool
    pub fn is_routeable(&self) -> bool {
        match (self.last_unhealthy, self.last_healthy) {
            // Currently unhealthy - check if we've been unhealthy long enough
            (Some(unhealthy_since), None) => {
                let unhealthy_duration = unhealthy_since.elapsed();
                let should_remove = unhealthy_duration >= Duration::from_millis(self.thresholds.unhealthy_duration_ms);
                if should_remove {
                    warn!("Node has been unhealthy for {:?}, removing from routing pool", unhealthy_duration);
                }
                !should_remove
            }
            // Currently healthy - check if we've recovered long enough
            (None, Some(healthy_since)) => {
                let healthy_duration = healthy_since.elapsed();
                let is_recovered = healthy_duration >= Duration::from_millis(self.thresholds.recovery_duration_ms);
                if !is_recovered {
                    debug!("Node recovering, healthy for {:?} (need {:?})", 
                           healthy_duration, 
                           Duration::from_millis(self.thresholds.recovery_duration_ms));
                }
                is_recovered
            }
            // Warning state or initial state - allow routing
            _ => true,
        }
    }
    
    fn calculate_cpu_usage(&self) -> f32 {
        self.system.global_cpu_usage()
    }
    
    fn calculate_memory_usage(&self) -> f32 {
        let used = self.system.used_memory();
        let total = self.system.total_memory();
        if total > 0 {
            (used as f32 / total as f32) * 100.0
        } else {
            0.0
        }
    }
    
    fn calculate_disk_usage(&self) -> f32 {
        // Get root disk usage
        for disk in &self.disks {
            if disk.mount_point().to_str() == Some("/") {
                let used = disk.total_space() - disk.available_space();
                let total = disk.total_space();
                if total > 0 {
                    return (used as f32 / total as f32) * 100.0;
                }
            }
        }
        0.0
    }
    
    fn determine_health_status(
        &self,
        cpu: f32,
        memory: f32,
        disk: f32,
        _network: f32,
    ) -> HealthStatus {
        // Check critical thresholds first
        if cpu > self.thresholds.critical_cpu_percent as f32 ||
           memory > self.thresholds.critical_memory_percent as f32 ||
           disk > self.thresholds.critical_disk_percent as f32 {
            return HealthStatus::Unhealthy;
        }
        
        // Check warning thresholds
        if cpu > self.thresholds.warning_cpu_percent as f32 ||
           memory > self.thresholds.warning_memory_percent as f32 ||
           disk > self.thresholds.warning_disk_percent as f32 {
            return HealthStatus::Warning;
        }
        
        // If all metrics are below warning thresholds, we're healthy
        HealthStatus::Healthy
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_health_status_determination() {
        let thresholds = NodeHealthThresholds {
            warning_cpu_percent: 70,
            warning_memory_percent: 75,
            warning_disk_percent: 80,
            critical_cpu_percent: 90,
            critical_memory_percent: 95,
            critical_disk_percent: 95,
            check_interval_ms: 1000,
            unhealthy_duration_ms: 5000,
            recovery_duration_ms: 3000,
        };
        
        let monitor = HealthMonitor::new(thresholds);
        
        // Test healthy state
        assert_eq!(
            monitor.determine_health_status(50.0, 60.0, 70.0, 40.0),
            HealthStatus::Healthy
        );
        
        // Test warning state
        assert_eq!(
            monitor.determine_health_status(80.0, 60.0, 70.0, 40.0),
            HealthStatus::Warning
        );
        
        // Test unhealthy state
        assert_eq!(
            monitor.determine_health_status(96.0, 60.0, 70.0, 40.0),
            HealthStatus::Unhealthy
        );
    }
}