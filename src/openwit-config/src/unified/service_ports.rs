use serde::{Deserialize, Serialize};

/// Service-specific port configurations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServicePorts {
    // Control plane ports must be explicitly configured
    pub control_plane: ControlPlaneServicePorts,
    #[serde(default)]
    pub storage: StorageServicePorts,
    #[serde(default)]
    pub ingestion: IngestionServicePorts,
    #[serde(default)]
    pub proxy: ProxyServicePorts,
    #[serde(default)]
    pub search: SearchServicePorts,
    #[serde(default)]
    pub indexer: IndexerServicePorts,
}

impl Default for ServicePorts {
    fn default() -> Self {
        Self {
            // No defaults for control plane - will need to be configured
            control_plane: ControlPlaneServicePorts {
                service: 0,  // Invalid port - must be configured
            },
            storage: StorageServicePorts::default(),
            ingestion: IngestionServicePorts::default(),
            proxy: ProxyServicePorts::default(),
            search: SearchServicePorts::default(),
            indexer: IndexerServicePorts::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ControlPlaneServicePorts {
    // No defaults - must be explicitly configured
    pub service: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageServicePorts {
    #[serde(default = "default_storage_service_port")]
    pub service: u16,
    #[serde(default = "default_storage_arrow_flight_port")]
    pub arrow_flight: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestionServicePorts {
    #[serde(default = "default_ingestion_grpc_port")]
    pub grpc: u16,
    #[serde(default = "default_ingestion_http_port")]
    pub http: u16,
    #[serde(default = "default_ingestion_arrow_flight_port")]
    pub arrow_flight: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProxyServicePorts {
    #[serde(default = "default_proxy_service_port")]
    pub service: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchServicePorts {
    #[serde(default = "default_search_service_port")]
    pub service: u16,
    #[serde(default = "default_search_grpc_port")]
    pub grpc: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexerServicePorts {
    #[serde(default = "default_indexer_service_port")]
    pub service: u16,
    #[serde(default = "default_indexer_arrow_flight_port")]
    pub arrow_flight: u16,
}

// No default implementation for ControlPlaneServicePorts
// This forces users to explicitly configure control plane ports

impl Default for StorageServicePorts {
    fn default() -> Self {
        Self {
            service: default_storage_service_port(),
            arrow_flight: default_storage_arrow_flight_port(),
        }
    }
}

impl Default for IngestionServicePorts {
    fn default() -> Self {
        Self {
            grpc: default_ingestion_grpc_port(),
            http: default_ingestion_http_port(),
            arrow_flight: default_ingestion_arrow_flight_port(),
        }
    }
}

impl Default for ProxyServicePorts {
    fn default() -> Self {
        Self {
            service: default_proxy_service_port(),
        }
    }
}

impl Default for SearchServicePorts {
    fn default() -> Self {
        Self {
            service: default_search_service_port(),
            grpc: default_search_grpc_port(),
        }
    }
}

impl Default for IndexerServicePorts {
    fn default() -> Self {
        Self {
            service: default_indexer_service_port(),
            arrow_flight: default_indexer_arrow_flight_port(),
        }
    }
}

// Removed default control plane port functions
// Control plane ports must be explicitly configured

fn default_storage_service_port() -> u16 { 8081 }
fn default_storage_arrow_flight_port() -> u16 { 8091 }

fn default_ingestion_grpc_port() -> u16 { 4317 }
fn default_ingestion_http_port() -> u16 { 4318 }
fn default_ingestion_arrow_flight_port() -> u16 { 8089 }

fn default_proxy_service_port() -> u16 { 8080 }

fn default_search_service_port() -> u16 { 8083 }
fn default_search_grpc_port() -> u16 { 50059 }

fn default_indexer_service_port() -> u16 { 8085 }
fn default_indexer_arrow_flight_port() -> u16 { 8090 }
