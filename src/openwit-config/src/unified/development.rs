use serde::{Deserialize, Serialize};
use crate::unified::validation::{Validatable, ValidationResult};
use tracing::info;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DevelopmentConfig {
    #[serde(default)]
    pub debug: DebugConfig,
    
    #[serde(default)]
    pub testing: TestingConfig,
    
    #[serde(default)]
    pub features: FeatureFlags,
}

impl Default for DevelopmentConfig {
    fn default() -> Self {
        Self {
            debug: DebugConfig::default(),
            testing: TestingConfig::default(),
            features: FeatureFlags::default(),
        }
    }
}

impl Validatable for DevelopmentConfig {
    fn validate(&self) -> Vec<ValidationResult> {
        Vec::new() // Add specific validations as needed
    }
    
    fn apply_safe_defaults(&mut self) {
        info!("Applying safe defaults to Development configuration");
        self.debug.enabled = false; // Disable debug in safe mode
        self.features.apply_safe_defaults();
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DebugConfig {
    #[serde(default)]
    pub enabled: bool,
    
    #[serde(default = "default_debug_log_level")]
    pub log_level: String,
    
    #[serde(default)]
    pub enable_profiling: bool,
    
    #[serde(default = "default_profile_port")]
    pub profile_port: u16,
}

impl Default for DebugConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            log_level: default_debug_log_level(),
            enable_profiling: false,
            profile_port: default_profile_port(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestingConfig {
    #[serde(default)]
    pub disable_external_deps: bool,
    
    #[serde(default)]
    pub mock_kafka: bool,
    
    #[serde(default)]
    pub mock_storage: bool,
}

impl Default for TestingConfig {
    fn default() -> Self {
        Self {
            disable_external_deps: false,
            mock_kafka: false,
            mock_storage: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureFlags {
    #[serde(default)]
    pub experimental_query_engine: bool,
    
    #[serde(default)]
    pub beta_compression_algorithms: bool,
    
    #[serde(default = "default_true")]
    pub preview_auto_scaling: bool,
}

impl Default for FeatureFlags {
    fn default() -> Self {
        Self {
            experimental_query_engine: false,
            beta_compression_algorithms: false,
            preview_auto_scaling: default_true(),
        }
    }
}

impl FeatureFlags {
    fn apply_safe_defaults(&mut self) {
        self.experimental_query_engine = false;
        self.beta_compression_algorithms = false;
        self.preview_auto_scaling = false; // Disable all preview features
    }
}

// Default functions
fn default_true() -> bool { true }
fn default_debug_log_level() -> String { "debug".to_string() }
fn default_profile_port() -> u16 { 6060 }
