use serde::{Deserialize, Serialize};
use crate::unified::validation::{Validatable, ValidationResult};
use tracing::info;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecurityConfig {
    #[serde(default)]
    pub authentication: AuthenticationConfig,
    
    #[serde(default)]
    pub authorization: AuthorizationConfig,
    
    #[serde(default)]
    pub encryption: EncryptionConfig,
}

impl Default for SecurityConfig {
    fn default() -> Self {
        Self {
            authentication: AuthenticationConfig::default(),
            authorization: AuthorizationConfig::default(),
            encryption: EncryptionConfig::default(),
        }
    }
}

impl Validatable for SecurityConfig {
    fn validate(&self) -> Vec<ValidationResult> {
        Vec::new() // Add specific validations as needed
    }
    
    fn apply_safe_defaults(&mut self) {
        info!("Applying safe defaults to Security configuration");
        // Enable all security features in safe mode
        self.authentication.enabled = true;
        self.authorization.enabled = true;
        self.encryption.at_rest = true;
        self.encryption.in_transit = true;
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthenticationConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    
    #[serde(default = "default_auth_type", rename = "type")]
    pub auth_type: String,
    
    #[serde(default = "default_token_header")]
    pub token_header: String,
    
    #[serde(default = "default_tokens_file")]
    pub tokens_file: String,
}

impl Default for AuthenticationConfig {
    fn default() -> Self {
        Self {
            enabled: default_true(),
            auth_type: default_auth_type(),
            token_header: default_token_header(),
            tokens_file: default_tokens_file(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthorizationConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    
    #[serde(default = "default_rbac_config")]
    pub rbac_config: String,
}

impl Default for AuthorizationConfig {
    fn default() -> Self {
        Self {
            enabled: default_true(),
            rbac_config: default_rbac_config(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncryptionConfig {
    #[serde(default = "default_true")]
    pub at_rest: bool,
    
    #[serde(default = "default_true")]
    pub in_transit: bool,
    
    #[serde(default = "default_encryption_algorithm")]
    pub algorithm: String,
    
    #[serde(default = "default_key_rotation_days")]
    pub key_rotation_days: u32,
    
    #[serde(default = "default_kms_endpoint")]
    pub key_management_service: String,
}

impl Default for EncryptionConfig {
    fn default() -> Self {
        Self {
            at_rest: default_true(),
            in_transit: default_true(),
            algorithm: default_encryption_algorithm(),
            key_rotation_days: default_key_rotation_days(),
            key_management_service: default_kms_endpoint(),
        }
    }
}

// Default functions
fn default_true() -> bool { true }
fn default_auth_type() -> String { "token".to_string() }
fn default_token_header() -> String { "X-Auth-Token".to_string() }
fn default_tokens_file() -> String { "".to_string() }
fn default_rbac_config() -> String { "".to_string() }
fn default_encryption_algorithm() -> String { "AES-256-GCM".to_string() }
fn default_key_rotation_days() -> u32 { 30 }
fn default_kms_endpoint() -> String { "".to_string() }
