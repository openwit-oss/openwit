use std::path::{Path, PathBuf};
use std::fs::{self, File};
use std::io::{Write, BufWriter};
use anyhow::{Result, Context};
use serde::{Serialize, Deserialize};
use chrono::{DateTime, Utc};
use uuid::Uuid;
use tracing::{info, error, debug};
use crc32fast::Hasher;

use crate::client_batch_manager::CompletedBatch;
use crate::types::KafkaMessage;

/// WAL (Write-Ahead Log) writer for batch persistence
pub struct WalWriter {
    /// Base directory for WAL files
    wal_directory: PathBuf,
}

/// WAL file header
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalHeader {
    pub version: u8,
    pub batch_id: String,
    pub client_id: String,
    pub created_at: DateTime<Utc>,
    pub message_count: usize,
    pub total_bytes: usize,
    pub checksum: u32,
}

/// WAL entry representing a single message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalEntry {
    pub sequence: u64,
    pub kafka_offset: i64,
    pub topic: String,
    pub partition: i32,
    pub timestamp: DateTime<Utc>,
    pub key: Option<Vec<u8>>,
    pub payload: Vec<u8>,
    pub headers: std::collections::HashMap<String, String>,
    pub size_bytes: usize,
}

impl WalWriter {
    /// Create a new WAL writer
    pub fn new(wal_directory: impl AsRef<Path>) -> Result<Self> {
        let wal_directory = wal_directory.as_ref().to_path_buf();
        
        // Ensure WAL directory exists
        fs::create_dir_all(&wal_directory)
            .context("Failed to create WAL directory")?;
        
        info!("Initialized WAL writer at: {:?}", wal_directory);
        
        Ok(Self { wal_directory })
    }
    
    /// Write a completed batch to WAL
    pub async fn write_batch(&self, batch: &CompletedBatch) -> Result<PathBuf> {
        let start_time = std::time::Instant::now();
        
        // Create client-specific directory
        let client_dir = self.wal_directory.join(&batch.client_id);
        fs::create_dir_all(&client_dir)
            .context("Failed to create client WAL directory")?;
        
        // Generate WAL filename: batch_<uuid>.wal
        let wal_filename = format!("batch_{}.wal", batch.batch_id);
        let wal_path = client_dir.join(&wal_filename);
        
        debug!(
            "Writing WAL for batch {} (client: {}, messages: {}, size: {} bytes)",
            batch.batch_id, batch.client_id, batch.message_count, batch.total_bytes
        );
        
        // Create WAL file
        let file = File::create(&wal_path)
            .context("Failed to create WAL file")?;
        let mut writer = BufWriter::with_capacity(65536, file); // 64KB buffer
        
        // Calculate checksum
        let mut hasher = Hasher::new();
        
        // Create and write header
        let header = WalHeader {
            version: 1,
            batch_id: batch.batch_id.clone(),
            client_id: batch.client_id.clone(),
            created_at: Utc::now(),
            message_count: batch.message_count,
            total_bytes: batch.total_bytes,
            checksum: 0, // Will be updated after calculating
        };
        
        // Serialize header
        let header_json = serde_json::to_string(&header)?;
        hasher.update(header_json.as_bytes());
        
        // Write entries and calculate checksum
        let mut entries = Vec::with_capacity(batch.messages.len());
        for (sequence, msg) in batch.messages.iter().enumerate() {
            let entry = WalEntry {
                sequence: sequence as u64,
                kafka_offset: msg.offset,
                topic: msg.topic.clone(),
                partition: msg.partition,
                timestamp: msg.timestamp,
                key: msg.key.clone(),
                payload: msg.payload.to_vec(),
                headers: msg.headers.clone(),
                size_bytes: msg.size_bytes,
            };
            
            let entry_json = serde_json::to_string(&entry)?;
            hasher.update(entry_json.as_bytes());
            entries.push(entry);
        }
        
        // Update header with checksum
        let checksum = hasher.finalize();
        let header_with_checksum = WalHeader { checksum, ..header };
        
        // Write header
        writeln!(writer, "OPENWIT_WAL_V1")?;
        writeln!(writer, "{}", serde_json::to_string(&header_with_checksum)?)?;
        writeln!(writer, "---")?;
        
        // Write entries
        for entry in entries {
            writeln!(writer, "{}", serde_json::to_string(&entry)?)?;
        }
        
        // Ensure all data is flushed to disk
        writer.flush()?;
        writer.into_inner()?.sync_all()?;
        
        let elapsed = start_time.elapsed();
        info!(
            "WAL written successfully: {} ({} messages, {} bytes, {:?})",
            wal_path.display(), batch.message_count, batch.total_bytes, elapsed
        );
        
        Ok(wal_path)
    }
    
    /// Read a WAL file and reconstruct the batch
    pub async fn read_batch(&self, client_id: &str, batch_id: Uuid) -> Result<WalBatch> {
        let wal_filename = format!("batch_{}.wal", batch_id);
        let wal_path = self.wal_directory.join(client_id).join(wal_filename);
        
        if !wal_path.exists() {
            return Err(anyhow::anyhow!("WAL file not found: {:?}", wal_path));
        }
        
        debug!("Reading WAL file: {:?}", wal_path);
        
        let contents = fs::read_to_string(&wal_path)
            .context("Failed to read WAL file")?;
        
        let lines: Vec<&str> = contents.lines().collect();
        if lines.len() < 4 {
            return Err(anyhow::anyhow!("Invalid WAL file format"));
        }
        
        // Verify magic header
        if lines[0] != "OPENWIT_WAL_V1" {
            return Err(anyhow::anyhow!("Invalid WAL file header"));
        }
        
        // Parse header
        let header: WalHeader = serde_json::from_str(lines[1])
            .context("Failed to parse WAL header")?;
        
        // Verify separator
        if lines[2] != "---" {
            return Err(anyhow::anyhow!("Invalid WAL file separator"));
        }
        
        // Calculate checksum for verification
        let mut hasher = Hasher::new();
        let header_for_checksum = WalHeader { checksum: 0, ..header.clone() };
        hasher.update(serde_json::to_string(&header_for_checksum)?.as_bytes());
        
        // Parse entries
        let mut messages = Vec::with_capacity(header.message_count);
        for i in 3..lines.len() {
            if lines[i].is_empty() {
                continue;
            }
            
            let entry: WalEntry = serde_json::from_str(lines[i])
                .context(format!("Failed to parse WAL entry at line {}", i))?;
            
            hasher.update(lines[i].as_bytes());
            
            // Convert WalEntry back to KafkaMessage
            let msg = KafkaMessage {
                id: format!("wal:{}:{}", batch_id, entry.sequence),
                timestamp: entry.timestamp,
                topic: entry.topic,
                partition: entry.partition,
                offset: entry.kafka_offset,
                key: entry.key,
                payload: bytes::Bytes::from(entry.payload),
                headers: entry.headers,
                payload_json: String::new(), // Will be reconstructed if needed
                payload_type: "unknown".to_string(), // Will be determined from topic
                size_bytes: entry.size_bytes,
                is_zero_copy: false,
            };
            
            messages.push(msg);
        }
        
        // Verify checksum
        let calculated_checksum = hasher.finalize();
        if calculated_checksum != header.checksum {
            error!(
                "WAL checksum mismatch: expected {}, calculated {}",
                header.checksum, calculated_checksum
            );
            return Err(anyhow::anyhow!("WAL file corrupted: checksum mismatch"));
        }
        
        debug!(
            "WAL read successfully: {} messages from batch {}",
            messages.len(), batch_id
        );
        
        Ok(WalBatch {
            header,
            messages,
        })
    }
    
    /// List all WAL files for a client
    pub async fn list_client_batches(&self, client_id: &str) -> Result<Vec<WalFileInfo>> {
        let client_dir = self.wal_directory.join(client_id);
        
        if !client_dir.exists() {
            return Ok(Vec::new());
        }
        
        let mut wal_files = Vec::new();
        
        for entry in fs::read_dir(&client_dir)? {
            let entry = entry?;
            let path = entry.path();
            
            if path.extension().and_then(|s| s.to_str()) == Some("wal") {
                if let Some(filename) = path.file_name().and_then(|s| s.to_str()) {
                    // Extract batch_id from filename (batch_<uuid>.wal)
                    if filename.starts_with("batch_") && filename.ends_with(".wal") {
                        let uuid_str = &filename[6..filename.len()-4];
                        if let Ok(batch_id) = Uuid::parse_str(uuid_str) {
                            let metadata = entry.metadata()?;
                            wal_files.push(WalFileInfo {
                                batch_id,
                                path,
                                size_bytes: metadata.len() as usize,
                                created_at: metadata.created()
                                    .ok()
                                    .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                                    .map(|d| DateTime::from_timestamp(d.as_secs() as i64, 0))
                                    .flatten()
                                    .unwrap_or_else(Utc::now),
                            });
                        }
                    }
                }
            }
        }
        
        // Sort by creation time
        wal_files.sort_by(|a, b| a.created_at.cmp(&b.created_at));
        
        Ok(wal_files)
    }
    
    /// Delete a WAL file after successful processing
    pub async fn delete_batch(&self, client_id: &str, batch_id: Uuid) -> Result<()> {
        let wal_filename = format!("batch_{}.wal", batch_id);
        let wal_path = self.wal_directory.join(client_id).join(wal_filename);
        
        if wal_path.exists() {
            fs::remove_file(&wal_path)
                .context("Failed to delete WAL file")?;
            
            info!("Deleted WAL file: {:?}", wal_path);
            
            // Try to remove client directory if empty
            let client_dir = self.wal_directory.join(client_id);
            if let Ok(mut entries) = fs::read_dir(&client_dir) {
                if entries.next().is_none() {
                    let _ = fs::remove_dir(&client_dir);
                }
            }
        }
        
        Ok(())
    }
    
    /// Get total WAL disk usage
    pub async fn get_disk_usage(&self) -> Result<u64> {
        let mut total_bytes = 0u64;
        
        for entry in fs::read_dir(&self.wal_directory)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                for file_entry in fs::read_dir(entry.path())? {
                    let file_entry = file_entry?;
                    if file_entry.path().extension().and_then(|s| s.to_str()) == Some("wal") {
                        total_bytes += file_entry.metadata()?.len();
                    }
                }
            }
        }
        
        Ok(total_bytes)
    }
}

/// Represents a WAL batch read from disk
pub struct WalBatch {
    pub header: WalHeader,
    pub messages: Vec<KafkaMessage>,
}

/// Information about a WAL file
#[derive(Debug, Clone)]
pub struct WalFileInfo {
    pub batch_id: Uuid,
    pub path: PathBuf,
    pub size_bytes: usize,
    pub created_at: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    
    #[tokio::test]
    async fn test_wal_write_read() {
        let temp_dir = tempdir().unwrap();
        let wal_writer = WalWriter::new(temp_dir.path()).unwrap();
        
        // Create a test batch
        let batch = CompletedBatch {
            batch_id: Uuid::new_v4(),
            client_id: "test_client".to_string(),
            messages: vec![
                KafkaMessage {
                    id: "msg1".to_string(),
                    timestamp: Utc::now(),
                    topic: "test_topic".to_string(),
                    partition: 0,
                    offset: 100,
                    key: None,
                    payload: bytes::Bytes::from("test payload"),
                    headers: std::collections::HashMap::new(),
                    payload_json: String::new(),
                    payload_type: "test".to_string(),
                    size_bytes: 12,
                    is_zero_copy: false,
                }
            ],
            total_bytes: 12,
            message_count: 1,
            created_at: std::time::Instant::now(),
            first_offset: Some(100),
            last_offset: Some(100),
            topics: vec!["test_topic".to_string()],
        };
        
        // Write batch
        let wal_path = wal_writer.write_batch(&batch).await.unwrap();
        assert!(wal_path.exists());
        
        // Read batch back
        let read_batch = wal_writer.read_batch(&batch.client_id, batch.batch_id).await.unwrap();
        assert_eq!(read_batch.header.batch_id, batch.batch_id);
        assert_eq!(read_batch.messages.len(), 1);
        assert_eq!(read_batch.messages[0].offset, 100);
        
        // Delete batch
        wal_writer.delete_batch(&batch.client_id, batch.batch_id).await.unwrap();
        assert!(!wal_path.exists());
    }
}