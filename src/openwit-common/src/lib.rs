//

/// Common configuration for Openwit (server settings).
pub mod rate_limiter;
pub mod system_metrics;
pub mod observability;
pub mod messages;

#[derive(Debug, Clone)]
pub struct OpenWitConfig {
    /// Address on which the gRPC server will listen (host:port).
    pub grpc_bind_addr: String,
    // Future: additional settings (e.g., for metrics/trace endpoints) can be added here.
}

impl Default for OpenWitConfig {
    fn default() -> Self {
        OpenWitConfig {
            // Default OTLP gRPC endpoint (standard collector port 4317)
            grpc_bind_addr: "0.0.0.0:4317".to_string(),
        }
    }
}
