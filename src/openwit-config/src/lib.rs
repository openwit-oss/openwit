// OpenWit Configuration System
//
// This module provides a unified configuration system for the entire OpenWit platform.
// All configuration is managed through the UnifiedConfig struct which aggregates
// all subsystem configurations.

pub mod unified;
pub mod unified_config;
pub mod unified_config_processing;

// Re-export unified config types for convenient access
pub use unified_config::UnifiedConfig;
pub use unified_config_processing::{ConfigError, Result};

// Re-export validation types
pub use unified::{ConfigLimits, ValidationResult, Validatable, ConfigValidator, ConfigWarning};
