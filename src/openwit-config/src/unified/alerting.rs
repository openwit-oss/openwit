use serde::{Deserialize, Serialize};
use crate::unified::validation::{Validatable, ValidationResult};
use tracing::info;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertingConfig {
    #[serde(default = "default_alert_endpoint")]
    pub endpoint: String,
    
    #[serde(default = "default_alert_method")]
    pub method: String,
    
    #[serde(default = "default_alert_timeout_seconds")]
    pub timeout_seconds: u32,
    
    #[serde(default)]
    pub thresholds: AlertThresholds,
    
    #[serde(default)]
    pub routing: AlertRouting,
}

impl Default for AlertingConfig {
    fn default() -> Self {
        Self {
            endpoint: default_alert_endpoint(),
            method: default_alert_method(),
            timeout_seconds: default_alert_timeout_seconds(),
            thresholds: AlertThresholds::default(),
            routing: AlertRouting::default(),
        }
    }
}

impl Validatable for AlertingConfig {
    fn validate(&self) -> Vec<ValidationResult> {
        Vec::new() // Add specific validations as needed
    }
    
    fn apply_safe_defaults(&mut self) {
        info!("Applying safe defaults to Alerting configuration");
        self.thresholds.apply_safe_defaults();
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertThresholds {
    #[serde(default = "default_kafka_messages_per_sec")]
    pub kafka_messages_per_sec: u32,
    
    #[serde(default = "default_kafka_lag_messages")]
    pub kafka_lag_messages: u32,
    
    #[serde(default = "default_memory_usage_percent")]
    pub memory_usage_percent: u32,
    
    #[serde(default = "default_cpu_usage_percent")]
    pub cpu_usage_percent: u32,
    
    #[serde(default = "default_disk_usage_percent")]
    pub disk_usage_percent: u32,
    
    #[serde(default = "default_flush_queue_depth")]
    pub flush_queue_depth: u32,
    
    #[serde(default = "default_processing_queue_depth")]
    pub processing_queue_depth: u32,
    
    #[serde(default = "default_error_rate_percent")]
    pub error_rate_percent: u32,
    
    #[serde(default = "default_timeout_rate_percent")]
    pub timeout_rate_percent: u32,
}

impl Default for AlertThresholds {
    fn default() -> Self {
        Self {
            kafka_messages_per_sec: default_kafka_messages_per_sec(),
            kafka_lag_messages: default_kafka_lag_messages(),
            memory_usage_percent: default_memory_usage_percent(),
            cpu_usage_percent: default_cpu_usage_percent(),
            disk_usage_percent: default_disk_usage_percent(),
            flush_queue_depth: default_flush_queue_depth(),
            processing_queue_depth: default_processing_queue_depth(),
            error_rate_percent: default_error_rate_percent(),
            timeout_rate_percent: default_timeout_rate_percent(),
        }
    }
}

impl AlertThresholds {
    fn apply_safe_defaults(&mut self) {
        self.memory_usage_percent = 70; // Lower threshold for safe mode
        self.cpu_usage_percent = 70;
        self.disk_usage_percent = 80;
        self.error_rate_percent = 3;
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertRouting {
    #[serde(default = "default_critical_alerts")]
    pub critical_alerts: String,
    
    #[serde(default = "default_warning_alerts")]
    pub warning_alerts: String,
}

impl Default for AlertRouting {
    fn default() -> Self {
        Self {
            critical_alerts: default_critical_alerts(),
            warning_alerts: default_warning_alerts(),
        }
    }
}

// Default functions
fn default_alert_endpoint() -> String { "".to_string() }
fn default_alert_method() -> String { "POST".to_string() }
fn default_alert_timeout_seconds() -> u32 { 10 }
fn default_kafka_messages_per_sec() -> u32 { 100000 }
fn default_kafka_lag_messages() -> u32 { 1000000 }
fn default_memory_usage_percent() -> u32 { 85 }
fn default_cpu_usage_percent() -> u32 { 80 }
fn default_disk_usage_percent() -> u32 { 90 }
fn default_flush_queue_depth() -> u32 { 10000 }
fn default_processing_queue_depth() -> u32 { 50000 }
fn default_error_rate_percent() -> u32 { 5 }
fn default_timeout_rate_percent() -> u32 { 10 }
fn default_critical_alerts() -> String { "".to_string() }
fn default_warning_alerts() -> String { "".to_string() }
