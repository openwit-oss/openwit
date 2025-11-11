use serde::{Deserialize, Serialize};
use crate::unified::validation::{Validatable, ValidationResult, limits, safe_defaults};
use tracing::info;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ControlPlaneConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    
    /// gRPC endpoint for the control plane service - must be explicitly configured
    pub grpc_endpoint: String,
    
    #[serde(default)]
    pub leader_election: LeaderElectionConfig,
    
    #[serde(default)]
    pub monitoring: ControlPlaneMonitoringConfig,
    
    #[serde(default)]
    pub optimization: OptimizationConfig,
    
    #[serde(default)]
    pub autoscaling: AutoscalingConfig,
    
    #[serde(default)]
    pub backpressure_advanced: BackpressureAdvancedConfig,
}

impl Default for ControlPlaneConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            grpc_endpoint: String::new(), // No default - must be explicitly configured
            leader_election: LeaderElectionConfig::default(),
            monitoring: ControlPlaneMonitoringConfig::default(),
            optimization: OptimizationConfig::default(),
            autoscaling: AutoscalingConfig::default(),
            backpressure_advanced: BackpressureAdvancedConfig::default(),
        }
    }
}

impl Validatable for ControlPlaneConfig {
    fn validate(&self) -> Vec<ValidationResult> {
        let mut results = Vec::new();
        results.extend(self.autoscaling.validate());
        results
    }
    
    fn apply_safe_defaults(&mut self) {
        info!("Applying safe defaults to Control Plane configuration");
        self.autoscaling.apply_safe_defaults();
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LeaderElectionConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    
    #[serde(default = "default_lease_duration_seconds")]
    pub lease_duration_seconds: u64,
    
    #[serde(default = "default_renew_deadline_seconds")]
    pub renew_deadline_seconds: u64,
    
    #[serde(default = "default_retry_period_seconds")]
    pub retry_period_seconds: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ControlPlaneMonitoringConfig {
    #[serde(default = "default_monitoring_interval_seconds")]
    pub interval_seconds: u64,
    
    #[serde(default)]
    pub thresholds: MonitoringThresholds,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MonitoringThresholds {
    #[serde(default = "default_cpu_threshold_percent")]
    pub cpu_usage_percent: u32,
    
    #[serde(default = "default_memory_threshold_percent")]
    pub memory_usage_percent: u32,
    
    #[serde(default = "default_queue_depth_warning")]
    pub queue_depth_warning: u32,
    
    #[serde(default = "default_queue_depth_critical")]
    pub queue_depth_critical: u32,
    
    #[serde(default = "default_error_rate_percent")]
    pub error_rate_percent: u32,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct OptimizationConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    
    #[serde(default = "default_decision_interval_seconds")]
    pub decision_interval_seconds: u64,
    
    #[serde(default)]
    pub targets: OptimizationTargets,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct OptimizationTargets {
    #[serde(default = "default_max_latency_p99_ms")]
    pub max_latency_p99_ms: u32,
    
    #[serde(default = "default_min_throughput_messages_per_sec")]
    pub min_throughput_messages_per_sec: u32,
    
    #[serde(default = "default_max_memory_usage_percent")]
    pub max_memory_usage_percent: u32,
    
    #[serde(default = "default_max_cpu_usage_percent")]
    pub max_cpu_usage_percent: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutoscalingConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    
    #[serde(default)]
    pub scale_up: ScaleThresholds,
    
    #[serde(default)]
    pub scale_down: ScaleThresholds,
    
    #[serde(default = "default_min_replicas")]
    pub min_replicas: u32,
    
    #[serde(default = "default_max_replicas")]
    pub max_replicas: u32,
    
    #[serde(default = "default_cooldown_seconds")]
    pub cooldown_seconds: u64,
}

impl Default for AutoscalingConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            scale_up: ScaleThresholds::default(),
            scale_down: ScaleThresholds::default(),
            min_replicas: default_min_replicas(),
            max_replicas: default_max_replicas(),
            cooldown_seconds: default_cooldown_seconds(),
        }
    }
}

impl Validatable for AutoscalingConfig {
    fn validate(&self) -> Vec<ValidationResult> {
        let mut results = Vec::new();
        results.push(limits::autoscaling_min_replicas().validate(self.min_replicas as i64));
        results.push(limits::autoscaling_max_replicas().validate(self.max_replicas as i64));
        
        // Validate that min <= max
        if self.min_replicas > self.max_replicas {
            results.push(ValidationResult::Warning {
                field: "autoscaling.replicas".to_string(),
                value: self.min_replicas as i64,
                recommended: 1,
                message: "min_replicas is greater than max_replicas".to_string(),
            });
        }
        
        results
    }
    
    fn apply_safe_defaults(&mut self) {
        self.min_replicas = safe_defaults::MIN_REPLICAS;
        self.max_replicas = safe_defaults::MAX_REPLICAS;
        self.cooldown_seconds = 300; // 5 minutes
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ScaleThresholds {
    #[serde(default = "default_scale_cpu_threshold")]
    pub cpu_threshold_percent: u32,
    
    #[serde(default = "default_scale_memory_threshold")]
    pub memory_threshold_percent: u32,
    
    #[serde(default = "default_scale_queue_threshold")]
    pub queue_depth_threshold: u32,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BackpressureAdvancedConfig {
    #[serde(default = "default_advanced_max_buffer_size")]
    pub max_buffer_size: u32,
    
    #[serde(default = "default_high_watermark")]
    pub high_watermark: f64,
    
    #[serde(default = "default_low_watermark")]
    pub low_watermark: f64,
    
    #[serde(default = "default_backoff_duration_ms")]
    pub backoff_duration_ms: u64,
    
    #[serde(default = "default_max_backoff_ms")]
    pub max_backoff_ms: u64,
    
    #[serde(default)]
    pub retry_config: RetryConfig,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RetryConfig {
    #[serde(default = "default_max_attempts")]
    pub max_attempts: u32,
    
    #[serde(default = "default_base_delay_ms")]
    pub base_delay_ms: u64,
    
    #[serde(default = "default_max_delay_ms")]
    pub max_delay_ms: u64,
    
    #[serde(default = "default_exponential_base")]
    pub exponential_base: f64,
}

// Default functions
fn default_true() -> bool { true }
// Removed default_grpc_endpoint - control plane endpoint must be explicitly configured
fn default_lease_duration_seconds() -> u64 { 30 }
fn default_renew_deadline_seconds() -> u64 { 20 }
fn default_retry_period_seconds() -> u64 { 5 }
fn default_monitoring_interval_seconds() -> u64 { 10 }
fn default_cpu_threshold_percent() -> u32 { 80 }
fn default_memory_threshold_percent() -> u32 { 85 }
fn default_queue_depth_warning() -> u32 { 10000 }
fn default_queue_depth_critical() -> u32 { 50000 }
fn default_error_rate_percent() -> u32 { 5 }
fn default_decision_interval_seconds() -> u64 { 60 }
fn default_max_latency_p99_ms() -> u32 { 1000 }
fn default_min_throughput_messages_per_sec() -> u32 { 1000 } // Safe default
fn default_max_memory_usage_percent() -> u32 { 80 }
fn default_max_cpu_usage_percent() -> u32 { 75 }
fn default_min_replicas() -> u32 { safe_defaults::MIN_REPLICAS }
fn default_max_replicas() -> u32 { safe_defaults::MAX_REPLICAS }
fn default_cooldown_seconds() -> u64 { 300 }
fn default_scale_cpu_threshold() -> u32 { 70 }
fn default_scale_memory_threshold() -> u32 { 75 }
fn default_scale_queue_threshold() -> u32 { 5000 }
fn default_advanced_max_buffer_size() -> u32 { 5000 } // Safe default
fn default_high_watermark() -> f64 { 0.8 }
fn default_low_watermark() -> f64 { 0.3 }
fn default_backoff_duration_ms() -> u64 { 100 }
fn default_max_backoff_ms() -> u64 { 5000 }
fn default_max_attempts() -> u32 { 3 }
fn default_base_delay_ms() -> u64 { 1000 }
fn default_max_delay_ms() -> u64 { 30000 }
fn default_exponential_base() -> f64 { 2.0 }