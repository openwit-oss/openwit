use crate::models::OpenWitConfig;
use std::fs;

pub fn load_config_from_yaml(path: &str) -> Result<OpenWitConfig, Box<dyn std::error::Error>> {
    let contents = fs::read_to_string(path)?;
    let config: OpenWitConfig = serde_yaml::from_str(&contents)?;
    Ok(config)
}
