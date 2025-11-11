use std::sync::Arc;
use std::time::Duration;
use anyhow::Result;
use tokio::time::{interval};
use tonic::transport::Channel;
use tracing::{info, error, warn, debug};
use tokio::sync::RwLock;

use openwit_proto::control::{
    control_plane_service_client::ControlPlaneServiceClient,
};

/// Manages HTTP server's integration with the control plane
pub struct ControlPlaneIntegration {
    node_id: String,
    node_role: String,
    bind_addr: String,
    control_plane_addr: String,
    client: Arc<RwLock<Option<ControlPlaneServiceClient<Channel>>>>,
}

impl ControlPlaneIntegration {
    /// Create a new control plane integration (without immediate connection)
    pub async fn new(
        node_id: String,
        bind_addr: String,
        control_plane_addr: &str,
    ) -> Result<Self> {
        info!("Creating control plane integration for {}", control_plane_addr);
        
        Ok(Self {
            node_id,
            node_role: "ingest".to_string(), // HTTP servers are ingest nodes
            bind_addr,
            control_plane_addr: control_plane_addr.to_string(),
            client: Arc::new(RwLock::new(None)),
        })
    }
    
    /// Connect to control plane with retries
    async fn ensure_connected(&self) -> Result<()> {
        let mut client_guard = self.client.write().await;
        
        // If already connected, return
        if client_guard.is_some() {
            return Ok(());
        }
        
        // Try to connect
        info!("Attempting to connect to control plane at {}", self.control_plane_addr);
        
        match Channel::from_shared(self.control_plane_addr.clone()) {
            Ok(endpoint) => {
                match endpoint
                    .timeout(Duration::from_secs(10))
                    .connect_timeout(Duration::from_secs(5))
                    .connect()
                    .await
                {
                    Ok(channel) => {
                        let client = ControlPlaneServiceClient::new(channel);
                        *client_guard = Some(client);
                        info!("Successfully connected to control plane");
                        Ok(())
                    }
                    Err(e) => {
                        warn!("Failed to connect to control plane: {}", e);
                        Err(anyhow::anyhow!("Connection failed: {}", e))
                    }
                }
            }
            Err(e) => {
                error!("Invalid control plane address: {}", e);
                Err(anyhow::anyhow!("Invalid address: {}", e))
            }
        }
    }
    
    /// Start periodic health reporting to control plane with reconnection logic
    pub async fn start_health_reporting(self: Arc<Self>) {
        let mut ticker = interval(Duration::from_secs(5)); // Report every 5 seconds
        interval(Duration::from_secs(30)); // Try reconnect every 30 seconds
        let mut consecutive_failures = 0;
        
        info!("Starting health reporting for HTTP node {}", self.node_id);
        
        loop {
            ticker.tick().await;
            
            // First ensure we're connected
            if self.ensure_connected().await.is_err() {
                consecutive_failures += 1;
                if consecutive_failures % 6 == 0 { // Log every 30 seconds
                    warn!("Still unable to connect to control plane after {} attempts", consecutive_failures);
                }
                continue;
            }
            
            // If we just reconnected, reset failure count
            if consecutive_failures > 0 {
                info!("Reconnected to control plane after {} failures", consecutive_failures);
                consecutive_failures = 0;
            }
            
            // Try to report health
            if let Err(e) = self.report_health().await {
                error!("Failed to report health: {}", e);
                // Mark client as disconnected so we'll reconnect next time
                let mut client_guard = self.client.write().await;
                *client_guard = None;
            }
        }
    }
    
    /// Report node health to control plane
    /// NOTE: This method is disabled as control plane has been simplified to only service discovery
    async fn report_health(&self) -> Result<()> {
        // Health reporting disabled - control plane only provides service discovery now
        debug!("HTTP node {} is healthy at {}", self.node_id, self.bind_addr);
        Ok(())
    }
    
    /// Get system metrics (simplified version)
    fn get_system_metrics(&self) -> (f64, f64, f64) {
        // TODO: Implement actual system metrics collection
        // For now, return mock values
        let cpu = 20.0 + rand::random::<f64>() * 10.0; // 20-30%
        let memory = 40.0 + rand::random::<f64>() * 10.0; // 40-50%
        let disk = 60.0 + rand::random::<f64>() * 5.0; // 60-65%
        
        (cpu, memory, disk)
    }
    
    /// Extract host from bind address
    fn get_host_from_bind(&self) -> String {
        // Extract IP from bind address
        if let Some(colon_pos) = self.bind_addr.rfind(':') {
            let host = &self.bind_addr[..colon_pos];
            // If it's 0.0.0.0, use localhost for endpoint
            if host == "0.0.0.0" {
                "localhost".to_string()
            } else {
                host.to_string()
            }
        } else {
            "localhost".to_string()
        }
    }
}