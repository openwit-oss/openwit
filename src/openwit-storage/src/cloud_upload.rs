use anyhow::{Context, Result};
use azure_storage::prelude::*;
use azure_storage_blobs::prelude::*;
use aws_config::BehaviorVersion;
use aws_sdk_s3::Client as S3Client;
use std::path::Path;
use tracing::{info, warn, error};
use tokio::fs::File;
use tokio::io::AsyncReadExt;

/// Azure storage configuration
#[derive(Clone)]
pub struct AzureStorageConfig {
    pub account_name: String,
    pub account_key: String,
    pub container_name: String,
    pub prefix: String,
    pub endpoint: Option<String>,
}

/// S3 storage configuration
#[derive(Clone)]
pub struct S3StorageConfig {
    pub bucket: String,
    pub region: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub prefix: String,
    pub endpoint: Option<String>, // For S3-compatible services like MinIO
}

/// Cloud storage uploader
pub struct CloudUploader {
    azure_config: Option<AzureStorageConfig>,
    s3_config: Option<S3StorageConfig>,
    s3_client: Option<S3Client>,
}

impl CloudUploader {
    /// Create a new cloud uploader
    pub async fn new(
        azure_config: Option<AzureStorageConfig>,
        s3_config: Option<S3StorageConfig>,
    ) -> Result<Self> {
        let s3_client = if let Some(ref config) = s3_config {
            Some(Self::create_s3_client(config).await?)
        } else {
            None
        };

        Ok(Self {
            azure_config,
            s3_config,
            s3_client,
        })
    }

    /// Create S3 client with configuration
    async fn create_s3_client(config: &S3StorageConfig) -> Result<S3Client> {
        info!("🚀 [S3] Creating S3 client for bucket: {}", config.bucket);
        info!("🚀 [S3] Region: {}", config.region);
        info!("🚀 [S3] Prefix: {}", config.prefix);
        if let Some(ref endpoint) = config.endpoint {
            info!("🚀 [S3] Custom endpoint: {}", endpoint);
        }

        // Build AWS config
        let mut aws_config_builder = aws_config::defaults(BehaviorVersion::latest())
            .region(aws_config::Region::new(config.region.clone()))
            .credentials_provider(aws_sdk_s3::config::Credentials::new(
                &config.access_key_id,
                &config.secret_access_key,
                None,
                None,
                "openwit",
            ));

        // Add custom endpoint if provided (for MinIO, LocalStack, etc.)
        if let Some(ref endpoint) = config.endpoint {
            aws_config_builder = aws_config_builder.endpoint_url(endpoint);
        }

        let aws_config = aws_config_builder.load().await;
        let s3_client = S3Client::new(&aws_config);

        info!("✅ [S3] S3 client created successfully");
        Ok(s3_client)
    }
    
    /// Upload a file to cloud storage
    pub async fn upload_file(&self, local_path: &str, remote_path: &str) -> Result<()> {
        let mut uploaded = false;

        // Try S3 upload first if configured
        if let (Some(config), Some(client)) = (&self.s3_config, &self.s3_client) {
            match self.upload_to_s3(config, client, local_path, remote_path).await {
                Ok(_) => {
                    uploaded = true;
                    info!("✅ [S3] Successfully uploaded {} to S3", local_path);
                }
                Err(e) => {
                    error!("❌ [S3] Failed to upload {} to S3: {}", local_path, e);
                    // Continue to try Azure if available
                }
            }
        }

        // Try Azure upload if S3 failed or not configured
        if !uploaded {
            if let Some(config) = &self.azure_config {
                self.upload_to_azure(config, local_path, remote_path).await?;
                uploaded = true;
                info!("✅ [AZURE] Successfully uploaded {} to Azure", local_path);
            }
        }

        if !uploaded {
            warn!("⚠️ [CLOUD] No cloud storage configured, skipping upload for: {}", local_path);
        }

        Ok(())
    }

    /// Upload file to S3 or S3-compatible storage
    async fn upload_to_s3(
        &self,
        config: &S3StorageConfig,
        client: &S3Client,
        local_path: &str,
        remote_path: &str,
    ) -> Result<()> {
        info!("🚀 [S3] Uploading {} to S3 bucket: {}", local_path, config.bucket);
        
        // Construct full S3 key with prefix
        let s3_key = format!("{}/{}", config.prefix, remote_path);
        info!("🚀 [S3] S3 key: {}", s3_key);

        // Read file contents
        let mut file = File::open(local_path)
            .await
            .context("Failed to open file for S3 upload")?;
            
        let mut contents = Vec::new();
        file.read_to_end(&mut contents)
            .await
            .context("Failed to read file contents for S3 upload")?;

        let file_size = contents.len();
        info!("🚀 [S3] File size: {} bytes", file_size);

        // Upload to S3
        let put_request = client
            .put_object()
            .bucket(&config.bucket)
            .key(&s3_key)
            .body(contents.into())
            .content_type("application/octet-stream");

        let result = put_request
            .send()
            .await
            .context("Failed to upload file to S3")?;

        if let Some(etag) = result.e_tag() {
            info!("✅ [S3] Successfully uploaded {} ({} bytes) to s3://{}/{}", 
                  local_path, file_size, config.bucket, s3_key);
            info!("✅ [S3] ETag: {}", etag);
        } else {
            info!("✅ [S3] Successfully uploaded {} ({} bytes) to s3://{}/{}", 
                  local_path, file_size, config.bucket, s3_key);
        }
        
        Ok(())
    }
    
    /// Upload file to Azure Blob Storage
    async fn upload_to_azure(
        &self,
        config: &AzureStorageConfig,
        local_path: &str,
        remote_path: &str,
    ) -> Result<()> {
        info!("[AZURE] Uploading {} to Azure blob: {}", local_path, remote_path);
        
        // Create storage credentials
        let storage_credentials = StorageCredentials::access_key(
            config.account_name.clone(),
            config.account_key.clone(),
        );
        
        // Create blob service client
        // Note: Azure SDK for Rust doesn't support custom endpoints directly in v0.20
        // The endpoint is determined by the account name
        let blob_service = BlobServiceClient::new(
            config.account_name.clone(),
            storage_credentials,
        );
        
        // Get container client
        let container_client = blob_service
            .container_client(&config.container_name);
        
        // Construct full blob path with prefix
        let blob_name = format!("{}/{}", config.prefix, remote_path);
        
        // Get blob client
        let blob_client = container_client.blob_client(&blob_name);
        
        // Read file contents
        let mut file = File::open(local_path)
            .await
            .context("Failed to open file for upload")?;
            
        let mut contents = Vec::new();
        file.read_to_end(&mut contents)
            .await
            .context("Failed to read file contents")?;
        
        // Upload blob
        blob_client
            .put_block_blob(contents)
            .content_type("application/octet-stream")
            .await
            .context("Failed to upload blob to Azure")?;
        
        info!("[AZURE] Successfully uploaded {} ({} bytes) to {}", 
              local_path, 
              Path::new(local_path).metadata()?.len(),
              blob_name);
        
        Ok(())
    }
    
    /// Delete a local file after successful upload
    pub async fn delete_local_file(&self, path: &str, delete_after_upload: bool) -> Result<()> {
        if delete_after_upload {
            tokio::fs::remove_file(path)
                .await
                .with_context(|| format!("Failed to delete local file: {}", path))?;
            info!("[AZURE] Deleted local file after upload: {}", path);
        }
        Ok(())
    }
}

/// Create Azure config from environment or unified config
pub fn create_azure_config(config: &openwit_config::UnifiedConfig) -> Option<AzureStorageConfig> {
    info!("🔍 [AZURE] Checking Azure storage configuration...");
    
    // Check if Azure is enabled
    if !config.storage.azure.enabled {
        warn!("⚠️ [AZURE] Azure storage is DISABLED in config - no uploads will occur!");
        return None;
    }
    
    info!("✅ [AZURE] Azure storage is ENABLED in config");
    
    // Try to get from config first, then fall back to environment variables
    let account_name = if !config.storage.azure.account_name.is_empty() {
        config.storage.azure.account_name.clone()
    } else {
        match std::env::var("AZURE_STORAGE_ACCOUNT") {
            Ok(val) => val,
            Err(_) => {
                warn!("[AZURE] No account name in config or AZURE_STORAGE_ACCOUNT env var");
                return None;
            }
        }
    };
    
    let account_key = if !config.storage.azure.access_key.is_empty() {
        config.storage.azure.access_key.clone()
    } else {
        match std::env::var("AZURE_STORAGE_KEY") {
            Ok(val) => val,
            Err(_) => {
                warn!("[AZURE] No access key in config or AZURE_STORAGE_KEY env var");
                return None;
            }
        }
    };
    
    let container_name = if !config.storage.azure.container_name.is_empty() {
        config.storage.azure.container_name.clone()
    } else {
        std::env::var("AZURE_CONTAINER_NAME").unwrap_or_else(|_| "openwit-data".to_string())
    };
    
    // Get prefix from environment variable or use default
    // Note: The prefix field is not in the AzureStorageConfig struct in unified config
    // So we check environment variable or use the default
    let prefix = std::env::var("AZURE_STORAGE_PREFIX")
        .unwrap_or_else(|_| "openwit/storage".to_string());
    
    let endpoint = if !config.storage.azure.endpoint.is_empty() {
        Some(config.storage.azure.endpoint.clone())
    } else {
        None
    };
    
    info!("[AZURE] Configured Azure storage:");
    info!("[AZURE]   Account: {}", account_name);
    info!("[AZURE]   Container: {}", container_name);
    info!("[AZURE]   Prefix: {}", prefix);
    info!("[AZURE]   Endpoint: {:?}", endpoint);
    
    Some(AzureStorageConfig {
        account_name,
        account_key,
        container_name,
        prefix,
        endpoint,
    })
}

/// Create S3 config from environment or unified config
pub fn create_s3_config(config: &openwit_config::UnifiedConfig) -> Option<S3StorageConfig> {
    info!("🔍 [S3] Checking S3 storage configuration...");
    
    // Check if S3 is enabled
    if !config.storage.s3.enabled {
        warn!("⚠️ [S3] S3 storage is DISABLED in config - no uploads will occur!");
        return None;
    }
    
    info!("✅ [S3] S3 storage is ENABLED in config");
    
    // Resolve environment variables or use direct values
    let bucket = resolve_env_var(&config.storage.s3.bucket)?;
    let region = resolve_env_var(&config.storage.s3.region)?;
    let access_key_id = resolve_env_var(&config.storage.s3.access_key_id)?;
    let secret_access_key = resolve_env_var(&config.storage.s3.secret_access_key)?;
    
    // Use default prefix since it's not in the config structure
    let prefix = "openwit/storage".to_string();
    
    // Get endpoint if specified (for MinIO, LocalStack, etc.)
    let endpoint = if !config.storage.s3.endpoint.is_empty() {
        Some(config.storage.s3.endpoint.clone())
    } else {
        None
    };
    
    info!("[S3] Configured S3 storage:");
    info!("[S3]   Bucket: {}", bucket);
    info!("[S3]   Region: {}", region);
    info!("[S3]   Prefix: {}", prefix);
    info!("[S3]   Access Key ID: {}***", &access_key_id[..std::cmp::min(8, access_key_id.len())]);
    if let Some(ref endpoint) = endpoint {
        info!("[S3]   Custom endpoint: {}", endpoint);
    }
    
    Some(S3StorageConfig {
        bucket,
        region,
        access_key_id,
        secret_access_key,
        prefix,
        endpoint,
    })
}

/// Resolve environment variables in config values
fn resolve_env_var(value: &str) -> Option<String> {
    if value.starts_with("${") && value.ends_with("}") {
        // Extract environment variable name
        let env_var = &value[2..value.len()-1];
        match std::env::var(env_var) {
            Ok(val) => {
                if val.is_empty() {
                    warn!("[S3] Environment variable {} is empty", env_var);
                    None
                } else {
                    Some(val)
                }
            }
            Err(_) => {
                warn!("[S3] Environment variable {} not found", env_var);
                None
            }
        }
    } else if !value.is_empty() {
        // Direct value
        Some(value.to_string())
    } else {
        warn!("[S3] Config value is empty");
        None
    }
}