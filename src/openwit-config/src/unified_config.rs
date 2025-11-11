use serde::{Deserialize, Serialize};
use crate::unified::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnifiedConfig {
    #[serde(default = "default_environment")]
    pub environment: String,
    
    pub deployment: DeploymentConfig,
    
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub mandatory_nodes: Option<MandatoryNodesConfig>,
    
    pub ingestion: IngestionConfig,

    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub processing: Option<ProcessingConfig>,

    pub memory: MemoryConfig,
    
    pub storage: StorageConfig,
    
    #[serde(default)]
    pub storage_node: StorageNodeConfig,
    
    pub indexing: IndexingConfig,
    
    pub search: SearchConfig,

    pub control_plane: ControlPlaneConfig,

    pub janitor: JanitorConfig,

    pub networking: NetworkingConfig,

    pub metastore: MetastoreConfig,

    #[serde(default)]
    pub service_ports: ServicePorts,
}

fn default_environment() -> String {
    "production".to_string()
}

impl Default for UnifiedConfig {
    fn default() -> Self {
        Self {
            environment: default_environment(),
            deployment: DeploymentConfig::default(),
            mandatory_nodes: None,
            ingestion: IngestionConfig::default(),
            processing: Some(ProcessingConfig::default()),
            memory: MemoryConfig::default(),
            storage: StorageConfig::default(),
            storage_node: StorageNodeConfig::default(),
            indexing: IndexingConfig::default(),
            search: SearchConfig::default(),
            control_plane: ControlPlaneConfig::default(),
            janitor: JanitorConfig::default(),
            networking: NetworkingConfig::default(),
            metastore: MetastoreConfig::default(),
            service_ports: ServicePorts::default(),
        }
    }
}