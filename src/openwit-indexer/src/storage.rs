use anyhow::{Context, Result};
use bytes::Bytes;
use opendal::{Operator, services};
use std::collections::HashMap;
use tracing::{debug, info, warn};
use uuid::Uuid;

/// Storage client for index artifacts using OpenDAL
#[derive(Clone)]
pub struct StorageClient {
    operator: Operator,
    base_path: String,
}

/// Artifact metadata for storage operations
#[derive(Debug, Clone)]
pub struct ArtifactMetadata {
    pub file_ulid: String,
    pub tenant: String,
    pub signal: String,
    pub partition_key: String,
    pub artifact_type: ArtifactType,
    pub size_bytes: u64,
    pub content_hash: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

/// Types of index artifacts we can store
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArtifactType {
    ZoneMap,
    Bloom,
    Bitmap,
    Tantivy,
    Combined, // for merged indexes
}

impl ArtifactType {
    pub fn as_str(&self) -> &'static str {
        match self {
            ArtifactType::ZoneMap => "zone_map",
            ArtifactType::Bloom => "bloom",
            ArtifactType::Bitmap => "bitmap", 
            ArtifactType::Tantivy => "tantivy",
            ArtifactType::Combined => "combined",
        }
    }

    pub fn file_extension(&self) -> &'static str {
        match self {
            ArtifactType::ZoneMap => "zone.parquet",
            ArtifactType::Bloom => "bloom.dat",
            ArtifactType::Bitmap => "bitmap.dat",
            ArtifactType::Tantivy => "tantivy.tar.zst",
            ArtifactType::Combined => "manifest.json",
        }
    }
}

impl StorageClient {
    /// Create storage client from configuration
    pub fn new(storage_uri: &str, base_path: Option<String>) -> Result<Self> {
        let operator = Self::create_operator(storage_uri)?;
        let base_path = base_path.unwrap_or_else(|| "openwit/index".to_string());
        
        Ok(Self {
            operator,
            base_path,
        })
    }

    /// Create OpenDAL operator from URI
    fn create_operator(uri: &str) -> Result<Operator> {
        if uri.starts_with("fs://") {
            let path = uri.strip_prefix("fs://").unwrap_or("./data/artifacts");
            let builder = services::Fs::default().root(path);
            Ok(Operator::new(builder)?.finish())
        } else if uri.starts_with("s3://") {
            let bucket = uri.strip_prefix("s3://").unwrap();
            let mut builder = services::S3::default().bucket(bucket);
            
            // Use environment variables for AWS credentials
            if let Ok(region) = std::env::var("AWS_REGION") {
                builder = builder.region(&region);
            }
            if let Ok(endpoint) = std::env::var("AWS_ENDPOINT_URL") {
                builder = builder.endpoint(&endpoint);
            }
            
            Ok(Operator::new(builder)?.finish())
        } else if uri.starts_with("azblob://") {
            let parts: Vec<&str> = uri.strip_prefix("azblob://").unwrap().split('/').collect();
            if parts.len() < 2 {
                anyhow::bail!("Invalid Azure Blob URI format. Expected: azblob://account/container");
            }
            
            let account_name = parts[0];
            let container = parts[1];
            
            let mut builder = services::Azblob::default()
                .account_name(account_name)
                .container(container);
            
            // Use environment variables for Azure credentials
            if let Ok(account_key) = std::env::var("AZURE_STORAGE_ACCOUNT_KEY") {
                builder = builder.account_key(&account_key);
            }
            
            Ok(Operator::new(builder)?.finish())
        } else if uri.starts_with("gcs://") {
            let bucket = uri.strip_prefix("gcs://").unwrap();
            let mut builder = services::Gcs::default().bucket(bucket);
            
            // Use environment variable for service account
            if let Ok(credential) = std::env::var("GOOGLE_APPLICATION_CREDENTIALS") {
                builder = builder.credential(&credential);
            }
            
            Ok(Operator::new(builder)?.finish())
        } else {
            anyhow::bail!("Unsupported storage URI: {}", uri);
        }
    }

    /// Generate storage path for an artifact
    fn artifact_path(&self, metadata: &ArtifactMetadata) -> String {
        format!(
            "{}/tenant={}/signal={}/level=delta/partition={}/{}_{}.{}",
            self.base_path,
            metadata.tenant,
            metadata.signal,
            metadata.partition_key,
            metadata.file_ulid,
            metadata.artifact_type.as_str(),
            metadata.artifact_type.file_extension()
        )
    }

    /// Generate path for combined/merged artifacts
    pub fn combined_path(
        &self,
        tenant: &str,
        signal: &str,
        level: &str, // "hour" or "day"
        partition: &str,
        artifact_type: &ArtifactType,
    ) -> String {
        let timestamp = chrono::Utc::now().format("%Y%m%d_%H%M%S");
        format!(
            "{}/tenant={}/signal={}/level={}/partition={}/combined_{}_{}.{}",
            self.base_path,
            tenant,
            signal,
            level,
            partition,
            timestamp,
            artifact_type.as_str(),
            artifact_type.file_extension()
        )
    }

    /// Store artifact data
    pub async fn store_artifact(
        &self,
        metadata: &ArtifactMetadata,
        data: Bytes,
    ) -> Result<String> {
        let path = self.artifact_path(metadata);
        
        debug!(
            path = %path,
            size = data.len(),
            artifact_type = %metadata.artifact_type.as_str(),
            "Storing artifact"
        );

        self.operator
            .write(&path, data)
            .await
            .with_context(|| format!("Failed to store artifact at path: {}", path))?;

        info!(
            path = %path,
            size = metadata.size_bytes,
            artifact_type = %metadata.artifact_type.as_str(),
            "Successfully stored artifact"
        );

        Ok(path)
    }

    /// Retrieve artifact data
    pub async fn get_artifact(&self, path: &str) -> Result<Bytes> {
        debug!(path = %path, "Retrieving artifact");

        let data = self.operator
            .read(path)
            .await
            .with_context(|| format!("Failed to retrieve artifact from path: {}", path))?;

        let bytes = data.to_bytes();
        debug!(path = %path, size = bytes.len(), "Successfully retrieved artifact");
        Ok(bytes)
    }

    /// Check if artifact exists
    pub async fn artifact_exists(&self, path: &str) -> Result<bool> {
        match self.operator.stat(path).await {
            Ok(_) => Ok(true),
            Err(e) if e.kind() == opendal::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    /// List artifacts for a partition
    pub async fn list_artifacts(
        &self,
        tenant: &str,
        signal: &str,
        partition: &str,
    ) -> Result<Vec<String>> {
        let prefix = format!(
            "{}/tenant={}/signal={}/level=delta/partition={}/",
            self.base_path, tenant, signal, partition
        );

        debug!(prefix = %prefix, "Listing artifacts");

        let entries = self.operator.list(&prefix).await?;
        let mut artifacts = Vec::new();

        for entry in entries {
            let meta = self.operator.stat(entry.path()).await?;
            if meta.is_file() {
                artifacts.push(entry.path().to_string());
            }
        }

        debug!(prefix = %prefix, count = artifacts.len(), "Listed artifacts");
        Ok(artifacts)
    }

    /// Delete artifact
    pub async fn delete_artifact(&self, path: &str) -> Result<()> {
        debug!(path = %path, "Deleting artifact");

        self.operator
            .delete(path)
            .await
            .with_context(|| format!("Failed to delete artifact at path: {}", path))?;

        info!(path = %path, "Successfully deleted artifact");
        Ok(())
    }

    /// Store combined manifest (for merged indexes)
    pub async fn store_manifest(
        &self,
        tenant: &str,
        signal: &str,
        level: &str,
        partition: &str,
        manifest_data: &serde_json::Value,
    ) -> Result<String> {
        let path = self.combined_path(tenant, signal, level, partition, &ArtifactType::Combined);
        let data = serde_json::to_vec_pretty(manifest_data)?;

        debug!(
            path = %path,
            size = data.len(),
            level = %level,
            "Storing combined manifest"
        );

        self.operator
            .write(&path, data)
            .await
            .with_context(|| format!("Failed to store manifest at path: {}", path))?;

        info!(
            path = %path,
            level = %level,
            "Successfully stored combined manifest"
        );

        Ok(path)
    }

    /// Retrieve manifest data
    pub async fn get_manifest(&self, path: &str) -> Result<serde_json::Value> {
        debug!(path = %path, "Retrieving manifest");

        let data = self.operator
            .read(path)
            .await
            .with_context(|| format!("Failed to retrieve manifest from path: {}", path))?;

        let manifest: serde_json::Value = serde_json::from_slice(&data.to_bytes())
            .with_context(|| format!("Failed to parse manifest JSON from path: {}", path))?;

        debug!(path = %path, "Successfully retrieved manifest");
        Ok(manifest)
    }

    /// Copy artifact to new location (for reorganization)
    pub async fn copy_artifact(&self, src_path: &str, dest_path: &str) -> Result<()> {
        debug!(src = %src_path, dest = %dest_path, "Copying artifact");

        // Read from source
        let data = self.operator
            .read(src_path)
            .await
            .with_context(|| format!("Failed to read source artifact: {}", src_path))?;

        // Write to destination  
        self.operator
            .write(dest_path, data)
            .await
            .with_context(|| format!("Failed to write destination artifact: {}", dest_path))?;

        info!(src = %src_path, dest = %dest_path, "Successfully copied artifact");
        Ok(())
    }

    /// Get storage statistics for monitoring
    pub async fn get_storage_stats(&self) -> Result<HashMap<String, u64>> {
        let mut stats = HashMap::new();
        let mut total_size = 0u64;
        let mut file_count = 0u64;

        // List all artifacts under base path
        let entries = self.operator.list(&self.base_path).await?;

        for entry in entries {
            if let Ok(meta) = self.operator.stat(entry.path()).await {
                if meta.is_file() {
                    file_count += 1;
                    total_size += meta.content_length();
                }
            }
        }

        stats.insert("total_size_bytes".to_string(), total_size);
        stats.insert("file_count".to_string(), file_count);
        stats.insert("avg_file_size_bytes".to_string(), 
                    if file_count > 0 { total_size / file_count } else { 0 });

        Ok(stats)
    }

    /// Health check for storage backend
    pub async fn health_check(&self) -> Result<HashMap<String, String>> {
        let mut health = HashMap::new();

        // Test write operation
        let test_path = format!("{}/health_check_{}.txt", self.base_path, Uuid::new_v4());
        let test_data = b"health_check";

        match self.operator.write(&test_path, test_data.to_vec()).await {
            Ok(_) => {
                health.insert("write_test".to_string(), "ok".to_string());

                // Test read operation
                match self.operator.read(&test_path).await {
                    Ok(_) => {
                        health.insert("read_test".to_string(), "ok".to_string());

                        // Cleanup
                        let _ = self.operator.delete(&test_path).await;
                    }
                    Err(e) => {
                        health.insert("read_test".to_string(), format!("error: {}", e));
                    }
                }
            }
            Err(e) => {
                health.insert("write_test".to_string(), format!("error: {}", e));
            }
        }

        // Get storage stats if possible
        match self.get_storage_stats().await {
            Ok(stats) => {
                for (key, value) in stats {
                    health.insert(key, value.to_string());
                }
            }
            Err(e) => {
                warn!(error = %e, "Failed to get storage stats for health check");
            }
        }

        Ok(health)
    }
}

/// Helper function to calculate content hash
pub fn calculate_hash(data: &[u8]) -> String {
    use blake3::Hasher;
    let mut hasher = Hasher::new();
    hasher.update(data);
    hasher.finalize().to_hex().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_filesystem_storage() {
        let temp_dir = tempdir().unwrap();
        let storage_uri = format!("fs://{}", temp_dir.path().display());
        
        let client = StorageClient::new(&storage_uri, Some("test".to_string())).unwrap();

        // Test artifact storage
        let metadata = ArtifactMetadata {
            file_ulid: "test_ulid".to_string(),
            tenant: "test_tenant".to_string(),
            signal: "logs".to_string(),
            partition_key: "2024-01-01-00".to_string(),
            artifact_type: ArtifactType::Bloom,
            size_bytes: 100,
            content_hash: "test_hash".to_string(),
            created_at: chrono::Utc::now(),
        };

        let test_data = Bytes::from("test artifact data");
        let path = client.store_artifact(&metadata, test_data.clone()).await.unwrap();

        // Test retrieval
        let retrieved = client.get_artifact(&path).await.unwrap();
        assert_eq!(retrieved, test_data);

        // Test exists
        assert!(client.artifact_exists(&path).await.unwrap());

        // Test delete
        client.delete_artifact(&path).await.unwrap();
        assert!(!client.artifact_exists(&path).await.unwrap());
    }

    #[tokio::test]
    async fn test_manifest_operations() {
        let temp_dir = tempdir().unwrap();
        let storage_uri = format!("fs://{}", temp_dir.path().display());
        
        let client = StorageClient::new(&storage_uri, Some("test".to_string())).unwrap();

        let manifest = serde_json::json!({
            "version": 1,
            "level": "hour",
            "partition": "2024-01-01-00",
            "artifacts": ["file1.bloom", "file2.zone"]
        });

        let path = client.store_manifest("test", "logs", "hour", "2024-01-01-00", &manifest).await.unwrap();
        
        let retrieved = client.get_manifest(&path).await.unwrap();
        assert_eq!(retrieved, manifest);
    }
}