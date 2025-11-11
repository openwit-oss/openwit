pub mod models;
pub mod loader;
pub mod unified;
pub mod unified_config;
pub mod unified_config_processing;
pub mod compatibility;
pub mod unified_loader;

// Re-export loader functions - comment out old loader to avoid conflicts
// pub use loader::*;

// Export new unified loader that provides backward compatibility
pub use unified_loader::{load_config_from_yaml, load_unified_config_as_legacy, load_unified_config_as_legacy_safe_mode};

// Re-export unified config types
pub use unified_config::UnifiedConfig;
pub use unified_config_processing::{ConfigError, Result};

// Re-export validation types
pub use unified::{ConfigLimits, ValidationResult, Validatable, ConfigValidator, ConfigWarning};

// Re-export legacy models under a namespace to avoid conflicts
pub mod legacy {
    pub use crate::models::*;
}
