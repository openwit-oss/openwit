use serde::{Deserialize, Serialize};

/// Health thresholds for node monitoring
/// Moved from monitoring module since monitoring configuration was removed
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeHealthThresholds {
    pub warning_cpu_percent: u32,
    pub warning_memory_percent: u32,
    pub warning_disk_percent: u32,
    pub critical_cpu_percent: u32,
    pub critical_memory_percent: u32,
    pub critical_disk_percent: u32,
    pub check_interval_ms: u64,
    pub unhealthy_duration_ms: u64,
    pub recovery_duration_ms: u64,
}

impl Default for NodeHealthThresholds {
    fn default() -> Self {
        Self {
            warning_cpu_percent: 70,
            warning_memory_percent: 75,
            warning_disk_percent: 80,
            critical_cpu_percent: 90,
            critical_memory_percent: 95,
            critical_disk_percent: 95,
            check_interval_ms: 1000,
            unhealthy_duration_ms: 5000,
            recovery_duration_ms: 3000,
        }
    }
}