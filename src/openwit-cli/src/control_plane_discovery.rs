use std::path::PathBuf;
use std::fs;
use anyhow::Result;
use tracing::{info, debug, warn};

const CONTROL_PLANE_FILE: &str = ".openwit_control_plane";

#[derive(serde::Serialize, serde::Deserialize)]
struct ControlPlaneInfo {
    gossip_port: u16,
    grpc_port: u16,
    started_at: String,
}

/// Register the control plane gossip port when starting
pub fn register_control_plane(gossip_port: u16, grpc_port: u16) -> Result<()> {
    let info = ControlPlaneInfo {
        gossip_port,
        grpc_port,
        started_at: chrono::Utc::now().to_rfc3339(),
    };
    
    let path = get_control_plane_file_path();
    let content = serde_json::to_string_pretty(&info)?;
    fs::write(&path, content)?;
    
    info!("Registered control plane: gossip={}, grpc={}", gossip_port, grpc_port);
    Ok(())
}

/// Discover the control plane gossip port
pub fn discover_control_plane_gossip_port() -> Option<u16> {
    let path = get_control_plane_file_path();
    
    if !path.exists() {
        debug!("Control plane file not found");
        return None;
    }
    
    match fs::read_to_string(&path) {
        Ok(content) => {
            match serde_json::from_str::<ControlPlaneInfo>(&content) {
                Ok(info) => {
                    info!("Discovered control plane on gossip port {}", info.gossip_port);
                    Some(info.gossip_port)
                }
                Err(e) => {
                    warn!("Failed to parse control plane file: {}", e);
                    None
                }
            }
        }
        Err(e) => {
            warn!("Failed to read control plane file: {}", e);
            None
        }
    }
}

/// Get control plane seed address
pub fn get_control_plane_seed() -> String {
    if let Some(port) = discover_control_plane_gossip_port() {
        format!("127.0.0.1:{}", port)
    } else {
        // Fallback to default
        "127.0.0.1:9092".to_string()
    }
}

/// Clean up control plane file on shutdown
pub fn cleanup_control_plane_file() -> Result<()> {
    let path = get_control_plane_file_path();
    if path.exists() {
        fs::remove_file(&path)?;
        debug!("Cleaned up control plane file");
    }
    Ok(())
}

fn get_control_plane_file_path() -> PathBuf {
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
    PathBuf::from(home).join(CONTROL_PLANE_FILE)
}