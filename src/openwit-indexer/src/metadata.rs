//! Metadata management for indexes

use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use tokio::fs;

/// Index metadata stored persistently
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexMetadata {
    pub index_id: String,
    pub index_type: crate::IndexType,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub field_configs: Vec<crate::FieldConfig>,
    pub stats: IndexStatsSummary,
    pub storage_info: StorageInfo,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexStatsSummary {
    pub doc_count: u64,
    pub size_bytes: u64,
    pub field_cardinalities: HashMap<String, u64>,
    pub last_compaction: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageInfo {
    pub base_path: String,
    pub data_files: Vec<DataFile>,
    pub total_size: u64,
    pub compression_ratio: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataFile {
    pub path: String,
    pub size_bytes: u64,
    pub created_at: DateTime<Utc>,
    pub row_count: u64,
    pub time_range: Option<(DateTime<Utc>, DateTime<Utc>)>,
}

/// Manages index metadata
pub struct MetadataManager {
    base_path: String,
}

impl MetadataManager {
    pub fn new(base_path: String) -> Self {
        Self { base_path }
    }
    
    pub async fn save_metadata(&self, metadata: &IndexMetadata) -> Result<()> {
        let path = Path::new(&self.base_path)
            .join(&metadata.index_id)
            .join("metadata.json");
        
        let json = serde_json::to_string_pretty(metadata)?;
        fs::create_dir_all(path.parent().unwrap()).await?;
        fs::write(path, json).await?;
        
        Ok(())
    }
    
    pub async fn load_metadata(&self, index_id: &str) -> Result<IndexMetadata> {
        let path = Path::new(&self.base_path)
            .join(index_id)
            .join("metadata.json");
        
        let json = fs::read_to_string(path).await?;
        let metadata = serde_json::from_str(&json)?;
        
        Ok(metadata)
    }
    
    pub async fn list_indexes(&self) -> Result<Vec<String>> {
        let mut indexes = Vec::new();
        let mut entries = fs::read_dir(&self.base_path).await?;
        
        while let Some(entry) = entries.next_entry().await? {
            if entry.file_type().await?.is_dir() {
                if let Some(name) = entry.file_name().to_str() {
                    indexes.push(name.to_string());
                }
            }
        }
        
        Ok(indexes)
    }
}