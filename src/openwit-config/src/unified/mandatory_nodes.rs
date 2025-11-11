use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MandatoryNodesConfig {
    #[serde(default)]
    pub required_types: Vec<String>,
    #[serde(default)]
    pub minimum_counts: HashMap<String, u32>,
    #[serde(default)]
    pub health_check: HealthCheckConfig,
    
    #[serde(default)]
    pub alerts: AlertsConfig,
    
    #[serde(default)]
    pub missing_node_policy: MissingNodePolicy,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct HealthCheckConfig {
    #[serde(default = "default_health_check_interval")]
    pub interval_seconds: u64,
    
    #[serde(default = "default_health_check_timeout")]
    pub timeout_seconds: u64,
    
    #[serde(default = "default_max_failures")]
    pub max_failures: u32,
}

fn default_health_check_interval() -> u64 { 30 }
fn default_health_check_timeout() -> u64 { 10 }
fn default_max_failures() -> u32 { 3 }

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AlertsConfig {
    pub endpoint: Option<String>,
    
    #[serde(default = "default_alert_method")]
    pub method: String,
    
    #[serde(default)]
    pub headers: HashMap<String, String>,
}

fn default_alert_method() -> String {
    "POST".to_string()
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MissingNodePolicy {
    #[serde(default)]
    pub allow_partial_operation: bool,
    
    #[serde(default = "default_grace_period")]
    pub grace_period_seconds: u64,
    
    #[serde(default)]
    pub emergency_shutdown: bool,
}

fn default_grace_period() -> u64 { 300 }