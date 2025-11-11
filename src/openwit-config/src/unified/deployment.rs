use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeploymentConfig {
    #[serde(default = "default_deployment_mode")]
    pub mode: String,
    
    #[serde(default)]
    pub kubernetes: KubernetesConfig,
}

impl Default for DeploymentConfig {
    fn default() -> Self {
        Self {
            mode: default_deployment_mode(),
            kubernetes: KubernetesConfig::default(),
        }
    }
}

fn default_deployment_mode() -> String {
    "distributed".to_string()
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct KubernetesConfig {
    #[serde(default)]
    pub enabled: bool,
    
    #[serde(default = "default_k8s_namespace")]
    pub namespace: String,
    
    #[serde(default)]
    pub headless_service: String,
    
    #[serde(default)]
    pub pod_role: String,
    
    #[serde(default)]
    pub node_group: String,
    
    #[serde(default)]
    pub service_account: String,
}

fn default_k8s_namespace() -> String {
    "openwit-system".to_string()
}