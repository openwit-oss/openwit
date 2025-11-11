use crate::legacy::{OpenWitConfig};
use crate::unified_config::UnifiedConfig;
use crate::unified_config_processing::Result;
use std::path::Path;
use tracing::{info, warn};

/// Load configuration using the new unified system but return legacy format for compatibility
pub fn load_unified_config_as_legacy<P: AsRef<Path>>(path: P) -> Result<OpenWitConfig> {
    info!("Loading configuration using unified system with legacy compatibility");
    
    // Load the unified config
    let unified_config = UnifiedConfig::from_file(path)?;
    
    // Convert to legacy format
    let legacy_config = unified_config.to_legacy();
    
    info!("Configuration loaded and converted to legacy format successfully");
    Ok(legacy_config)
}

/// Load configuration in safe mode and return legacy format
pub fn load_unified_config_as_legacy_safe_mode<P: AsRef<Path>>(path: P) -> Result<OpenWitConfig> {
    info!("Loading configuration in SAFE MODE with legacy compatibility");
    
    // Load the unified config in safe mode
    let unified_config = UnifiedConfig::from_file_safe_mode(path)?;
    
    // Convert to legacy format
    let legacy_config = unified_config.to_legacy();
    
    info!("Safe mode configuration loaded and converted to legacy format successfully");
    Ok(legacy_config)
}

/// Wrapper to make migration easier - replaces load_config_from_yaml
pub fn load_config_from_yaml(yaml_path: &str) -> Result<OpenWitConfig> {
    warn!("Using legacy load_config_from_yaml - consider migrating to UnifiedConfig");
    load_unified_config_as_legacy(yaml_path)
}