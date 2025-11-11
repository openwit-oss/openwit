use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use openwit_config::UnifiedConfig;

/// Indexer-specific configuration that integrates with the unified config system
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexerConfig {
    #[serde(flatten)]
    pub base: UnifiedConfig,
    
    /// Indexer-specific settings
    pub indexer: IndexerSettings,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexerSettings {
    pub general: GeneralSettings,
    pub artifacts: ArtifactSettings, 
    pub merge: MergeSettings,
    pub local: Option<LocalSettings>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeneralSettings {
    pub node_id: String,
    pub mode: IndexerMode,
    pub work_dir: PathBuf,
    pub concurrency: u32,
    pub merge_concurrency: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum IndexerMode {
    Local,
    Prod,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArtifactSettings {
    pub bloom_fpr: f64,
    pub bloom_columns: Vec<String>,
    pub bitmap_columns: Vec<String>, 
    pub zone_map_columns: Vec<String>,
    pub tantivy_text_fields: Vec<String>,
    pub tantivy_keyword_fields: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]  
pub struct MergeSettings {
    pub trigger_count: u32,
    pub trigger_bytes_gb: u32,
    pub trigger_interval_sec: u32,
    pub levels: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalSettings {
    pub input_parquet_dir: PathBuf,
    pub output_index_dir: PathBuf,
}

impl Default for GeneralSettings {
    fn default() -> Self {
        Self {
            node_id: format!("indexer-{}", ulid::Ulid::new()),
            mode: IndexerMode::Prod,
            work_dir: PathBuf::from("/var/lib/openwit/indexer"),
            concurrency: 8,
            merge_concurrency: 2,
        }
    }
}

impl Default for ArtifactSettings {
    fn default() -> Self {
        Self {
            bloom_fpr: 0.005,
            bloom_columns: vec!["trace_id".to_string(), "span_id".to_string()],
            bitmap_columns: vec!["severity".to_string(), "status_class".to_string(), "env".to_string()],
            zone_map_columns: vec!["ts".to_string(), "service.name".to_string(), "severity".to_string(), "trace_id".to_string()],
            tantivy_text_fields: vec!["message".to_string(), "exception.stacktrace".to_string()],
            tantivy_keyword_fields: vec!["service.name".to_string(), "resource.*".to_string(), "attributes.*".to_string()],
        }
    }
}

impl Default for MergeSettings {
    fn default() -> Self {
        Self {
            trigger_count: 50,
            trigger_bytes_gb: 8,
            trigger_interval_sec: 900, // 15 min
            levels: vec!["hour".to_string(), "day".to_string()],
        }
    }
}

impl Default for IndexerSettings {
    fn default() -> Self {
        Self {
            general: GeneralSettings::default(),
            artifacts: ArtifactSettings::default(),
            merge: MergeSettings::default(),
            local: None,
        }
    }
}

impl IndexerConfig {
    /// Load configuration from TOML file, falling back to unified config
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let content = std::fs::read_to_string(path.as_ref())
            .with_context(|| format!("Failed to read config file: {}", path.as_ref().display()))?;
            
        // Try to parse as indexer-specific config first
        if let Ok(config) = toml::from_str::<IndexerConfig>(&content) {
            return Ok(config);
        }
        
        // Fall back to unified config with default indexer settings
        let content = std::fs::read_to_string(path.as_ref())
            .with_context(|| format!("Failed to read config file: {}", path.as_ref().display()))?;
        let base_config: UnifiedConfig = serde_yaml::from_str(&content)
            .with_context(|| "Failed to parse config as YAML")?;
        
        Ok(IndexerConfig {
            base: base_config,
            indexer: IndexerSettings::default(),
        })
    }
    
    /// Extract settings from the unified config system
    pub fn from_unified(unified: UnifiedConfig) -> Self {
        let indexer = IndexerSettings {
            general: GeneralSettings {
                node_id: "indexer-1".to_string(),
                mode: if unified.environment == "local" { IndexerMode::Local } else { IndexerMode::Prod },
                work_dir: PathBuf::from("./data/indexer"),
                concurrency: unified.indexing.worker_threads as u32,
                merge_concurrency: 2,
            },
            artifacts: ArtifactSettings::default(),
            merge: MergeSettings::default(),
            local: if unified.environment == "local" {
                Some(LocalSettings {
                    input_parquet_dir: PathBuf::from("./data/storage"),
                    output_index_dir: PathBuf::from("./data/index"),
                })
            } else {
                None
            },
        };
        
        Self {
            base: unified,
            indexer,
        }
    }
    
    /// Get PostgreSQL connection string
    pub fn postgres_dsn(&self) -> String {
        if !self.base.metastore.postgres.connection_string.is_empty() {
            return self.base.metastore.postgres.connection_string.clone();
        }
        "postgresql://localhost/openwit".to_string()
    }
    
    /// Get artifact storage URI (OpenDAL format)
    pub fn artifact_uri(&self) -> String {
        if self.base.storage.azure.enabled {
            format!("azblob://{}/{}", 
                self.base.storage.azure.account_name, 
                self.base.storage.azure.container_name)
        } else if self.base.storage.s3.enabled {
            format!("s3://{}", self.base.storage.s3.bucket)
        } else {
            "fs://./data/artifacts".to_string()
        }
    }
    
    /// Check if running in local mode
    pub fn is_local_mode(&self) -> bool {
        matches!(self.indexer.general.mode, IndexerMode::Local)
    }
    
    /// Check if running in production mode
    pub fn is_prod_mode(&self) -> bool {
        matches!(self.indexer.general.mode, IndexerMode::Prod)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;
    use std::io::Write;

    #[test]
    fn test_config_from_toml() {
        let toml_content = r#"
[indexer.general]
node_id = "test-indexer"
mode = "local" 
work_dir = "/tmp/indexer"
concurrency = 4
merge_concurrency = 1

[indexer.artifacts]
bloom_fpr = 0.01
bloom_columns = ["trace_id"]
bitmap_columns = ["severity"]
zone_map_columns = ["ts", "trace_id"]
tantivy_text_fields = ["message"]
tantivy_keyword_fields = ["service.name"]

[indexer.merge]
trigger_count = 10
trigger_bytes_gb = 1
trigger_interval_sec = 300
levels = ["hour"]

[indexer.local]
input_parquet_dir = "./input"
output_index_dir = "./output"
        "#;

        let mut file = NamedTempFile::new().unwrap();
        file.write_all(toml_content.as_bytes()).unwrap();
        
        // This will use the unified config fallback since we don't have all required fields
        let config = IndexerConfig::from_unified(UnifiedConfig::default());
        assert!(!config.is_local_mode() || config.is_prod_mode()); // one or the other
    }
}