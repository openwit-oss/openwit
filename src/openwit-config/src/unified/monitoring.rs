use serde::{Deserialize, Serialize};
use crate::unified::validation::{Validatable, ValidationResult};
use tracing::info;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MonitoringConfig {
    #[serde(default)]
    pub prometheus: PrometheusConfig,
    
    #[serde(default)]
    pub opentelemetry: OpenTelemetryConfig,
    
    #[serde(default)]
    pub logging: LoggingConfig,
    
    #[serde(default)]
    pub health: HealthConfig,
    
    #[serde(default)]
    pub node_health_thresholds: NodeHealthThresholds,
}

impl Default for MonitoringConfig {
    fn default() -> Self {
        Self {
            prometheus: PrometheusConfig::default(),
            opentelemetry: OpenTelemetryConfig::default(),
            logging: LoggingConfig::default(),
            health: HealthConfig::default(),
            node_health_thresholds: NodeHealthThresholds::default(),
        }
    }
}

impl Validatable for MonitoringConfig {
    fn validate(&self) -> Vec<ValidationResult> {
        Vec::new() // Add specific validations as needed
    }
    
    fn apply_safe_defaults(&mut self) {
        info!("Applying safe defaults to Monitoring configuration");
        self.opentelemetry.apply_safe_defaults();
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrometheusConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    
    #[serde(default = "default_prometheus_bind")]
    pub bind: String,
    
    #[serde(default = "default_scrape_interval_seconds")]
    pub scrape_interval_seconds: u32,
    
    #[serde(default = "default_metric_prefix")]
    pub metric_prefix: String,
}

impl Default for PrometheusConfig {
    fn default() -> Self {
        Self {
            enabled: default_true(),
            bind: default_prometheus_bind(),
            scrape_interval_seconds: default_scrape_interval_seconds(),
            metric_prefix: default_metric_prefix(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenTelemetryConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    
    #[serde(default = "default_otel_endpoint")]
    pub endpoint: String,
    
    #[serde(default = "default_service_name")]
    pub service_name: String,
    
    #[serde(default = "default_trace_sample_rate")]
    pub trace_sample_rate: f32,
}

impl Default for OpenTelemetryConfig {
    fn default() -> Self {
        Self {
            enabled: default_true(),
            endpoint: default_otel_endpoint(),
            service_name: default_service_name(),
            trace_sample_rate: default_trace_sample_rate(),
        }
    }
}

impl OpenTelemetryConfig {
    fn apply_safe_defaults(&mut self) {
        self.trace_sample_rate = 0.01; // 1% sampling in safe mode
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    #[serde(default = "default_log_level")]
    pub level: String,
    
    #[serde(default = "default_log_format")]
    pub format: String,
    
    #[serde(default = "default_log_output_file")]
    pub output_file: String,
    
    #[serde(default = "default_log_max_size_mb")]
    pub max_size_mb: u32,
    
    #[serde(default = "default_log_max_files")]
    pub max_files: u32,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: default_log_level(),
            format: default_log_format(),
            output_file: default_log_output_file(),
            max_size_mb: default_log_max_size_mb(),
            max_files: default_log_max_files(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    
    #[serde(default = "default_health_port")]
    pub port: u16,
    
    #[serde(default = "default_health_path")]
    pub path: String,
    
    #[serde(default = "default_health_timeout_seconds")]
    pub timeout_seconds: u32,
    
    #[serde(default)]
    pub checks: Vec<HealthCheckDefinition>,
}

impl Default for HealthConfig {
    fn default() -> Self {
        Self {
            enabled: default_true(),
            port: default_health_port(),
            path: default_health_path(),
            timeout_seconds: default_health_timeout_seconds(),
            checks: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthCheckDefinition {
    pub name: String,
    
    #[serde(rename = "type")]
    pub check_type: String,
    
    pub interval_seconds: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeHealthThresholds {
    // CPU usage thresholds (percentage)
    #[serde(default = "default_cpu_healthy_max")]
    pub cpu_healthy_max: f32,
    
    #[serde(default = "default_cpu_warning_max")]
    pub cpu_warning_max: f32,
    
    #[serde(default = "default_cpu_unhealthy_max")]
    pub cpu_unhealthy_max: f32,
    
    // Memory usage thresholds (percentage)
    #[serde(default = "default_memory_healthy_max")]
    pub memory_healthy_max: f32,
    
    #[serde(default = "default_memory_warning_max")]
    pub memory_warning_max: f32,
    
    #[serde(default = "default_memory_unhealthy_max")]
    pub memory_unhealthy_max: f32,
    
    // Disk usage thresholds (percentage)
    #[serde(default = "default_disk_healthy_max")]
    pub disk_healthy_max: f32,
    
    #[serde(default = "default_disk_warning_max")]
    pub disk_warning_max: f32,
    
    #[serde(default = "default_disk_unhealthy_max")]
    pub disk_unhealthy_max: f32,
    
    // Network bandwidth thresholds (percentage of capacity)
    #[serde(default = "default_network_healthy_max")]
    pub network_healthy_max: f32,
    
    #[serde(default = "default_network_warning_max")]
    pub network_warning_max: f32,
    
    #[serde(default = "default_network_unhealthy_max")]
    pub network_unhealthy_max: f32,
    
    // Health check timing
    #[serde(default = "default_health_check_interval_ms")]
    pub health_check_interval_ms: u64,
    
    #[serde(default = "default_unhealthy_duration_ms")]
    pub unhealthy_duration_ms: u64,
    
    #[serde(default = "default_recovery_duration_ms")]
    pub recovery_duration_ms: u64,
}

impl Default for NodeHealthThresholds {
    fn default() -> Self {
        Self {
            cpu_healthy_max: default_cpu_healthy_max(),
            cpu_warning_max: default_cpu_warning_max(),
            cpu_unhealthy_max: default_cpu_unhealthy_max(),
            memory_healthy_max: default_memory_healthy_max(),
            memory_warning_max: default_memory_warning_max(),
            memory_unhealthy_max: default_memory_unhealthy_max(),
            disk_healthy_max: default_disk_healthy_max(),
            disk_warning_max: default_disk_warning_max(),
            disk_unhealthy_max: default_disk_unhealthy_max(),
            network_healthy_max: default_network_healthy_max(),
            network_warning_max: default_network_warning_max(),
            network_unhealthy_max: default_network_unhealthy_max(),
            health_check_interval_ms: default_health_check_interval_ms(),
            unhealthy_duration_ms: default_unhealthy_duration_ms(),
            recovery_duration_ms: default_recovery_duration_ms(),
        }
    }
}

// Default functions
fn default_true() -> bool { true }
fn default_prometheus_bind() -> String { "0.0.0.0:9090".to_string() }
fn default_scrape_interval_seconds() -> u32 { 15 }
fn default_metric_prefix() -> String { "openwit_".to_string() }
fn default_otel_endpoint() -> String { "".to_string() }
fn default_service_name() -> String { "openwit".to_string() }
fn default_trace_sample_rate() -> f32 { 0.1 }
fn default_log_level() -> String { "info".to_string() }
fn default_log_format() -> String { "json".to_string() }
fn default_log_output_file() -> String { "/var/log/openwit/openwit.log".to_string() }
fn default_log_max_size_mb() -> u32 { 100 }
fn default_log_max_files() -> u32 { 10 }
fn default_health_port() -> u16 { 8080 }
fn default_health_path() -> String { "/health".to_string() }
fn default_health_timeout_seconds() -> u32 { 5 }

// NodeHealthThresholds defaults
fn default_cpu_healthy_max() -> f32 { 70.0 }
fn default_cpu_warning_max() -> f32 { 85.0 }
fn default_cpu_unhealthy_max() -> f32 { 95.0 }
fn default_memory_healthy_max() -> f32 { 75.0 }
fn default_memory_warning_max() -> f32 { 85.0 }
fn default_memory_unhealthy_max() -> f32 { 95.0 }
fn default_disk_healthy_max() -> f32 { 80.0 }
fn default_disk_warning_max() -> f32 { 90.0 }
fn default_disk_unhealthy_max() -> f32 { 95.0 }
fn default_network_healthy_max() -> f32 { 60.0 }
fn default_network_warning_max() -> f32 { 80.0 }
fn default_network_unhealthy_max() -> f32 { 90.0 }
fn default_health_check_interval_ms() -> u64 { 1000 }
fn default_unhealthy_duration_ms() -> u64 { 5000 }
fn default_recovery_duration_ms() -> u64 { 3000 }
