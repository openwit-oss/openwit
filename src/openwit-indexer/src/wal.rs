use anyhow::{Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use tracing::{debug, info, warn, error};
use uuid::Uuid;

/// Write-Ahead Log for idempotent indexer operations
/// Ensures operations can be replayed safely and efficiently
pub struct WAL {
    log_dir: PathBuf,
    current_log: Arc<Mutex<File>>,
    sequence_number: Arc<Mutex<u64>>,
    log_rotation_size: u64,
    completed_operations: Arc<Mutex<HashMap<String, u64>>>, // operation_id -> sequence
}

/// WAL record types for different indexer operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WALRecord {
    pub sequence: u64,
    pub timestamp: DateTime<Utc>,
    pub operation_id: String, // Unique identifier for idempotency
    pub operation: WALOperation,
    pub status: WALStatus,
    pub checksum: String,
}

/// Operations that can be logged in the WAL
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WALOperation {
    /// Index a data file - create all artifacts
    IndexDataFile {
        file_ulid: String,
        tenant: String,
        signal: String,
        partition_key: String,
        parquet_url: String,
        size_bytes: u64,
    },
    /// Store artifact in storage
    StoreArtifact {
        artifact_id: Uuid,
        file_ulid: String,
        artifact_type: String,
        artifact_url: String,
        size_bytes: u64,
    },
    /// Create snapshot for atomic publishing
    CreateSnapshot {
        snapshot_id: Uuid,
        tenant: String,
        signal: String,
        level: String,
        partition_key: String,
        file_ulids: Vec<String>,
    },
    /// Publish snapshot (make visible)
    PublishSnapshot {
        snapshot_id: Uuid,
    },
    /// Merge deltas into combined index
    MergeDeltas {
        merge_id: Uuid,
        tenant: String,
        signal: String,
        level: String, // "hour" or "day"
        partition_key: String,
        delta_file_ulids: Vec<String>,
        combined_manifest_url: String,
    },
    /// Cleanup old artifacts
    CleanupArtifacts {
        cleanup_id: Uuid,
        artifact_urls: Vec<String>,
        reason: String,
    },
}

/// Status of WAL operations
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum WALStatus {
    Started,
    InProgress { step: String },
    Completed,
    Failed { error: String },
    Retrying { attempt: u32 },
}

/// WAL recovery information
#[derive(Debug, Clone)]
pub struct RecoveryInfo {
    pub total_records: usize,
    pub completed_records: usize,
    pub failed_records: usize,
    pub pending_records: usize,
    pub last_sequence: u64,
}

impl WAL {
    /// Create a new WAL instance
    pub fn new<P: AsRef<Path>>(log_dir: P, log_rotation_size: u64) -> Result<Self> {
        let log_dir = log_dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&log_dir)?;

        // Find the latest log file or create a new one
        let (current_log, sequence_number) = Self::initialize_log(&log_dir)?;
        let current_log = Arc::new(Mutex::new(current_log));
        let sequence_number = Arc::new(Mutex::new(sequence_number));

        let wal = WAL {
            log_dir,
            current_log,
            sequence_number,
            log_rotation_size,
            completed_operations: Arc::new(Mutex::new(HashMap::new())),
        };

        // Load completed operations from existing logs
        wal.load_completed_operations()?;

        info!(
            log_dir = ?wal.log_dir,
            sequence = *wal.sequence_number.lock().unwrap(),
            completed_ops = wal.completed_operations.lock().unwrap().len(),
            "WAL initialized successfully"
        );

        Ok(wal)
    }

    /// Initialize log file and sequence number
    fn initialize_log(log_dir: &Path) -> Result<(File, u64)> {
        let mut latest_sequence = 0u64;
        let mut latest_log_file = None;

        // Find the latest log file
        for entry in std::fs::read_dir(log_dir)? {
            let entry = entry?;
            let path = entry.path();
            
            if let Some(filename) = path.file_name().and_then(|n| n.to_str()) {
                if filename.starts_with("wal_") && filename.ends_with(".log") {
                    if let Some(seq_str) = filename.strip_prefix("wal_").and_then(|s| s.strip_suffix(".log")) {
                        if let Ok(seq) = seq_str.parse::<u64>() {
                            if seq > latest_sequence {
                                latest_sequence = seq;
                                latest_log_file = Some(path);
                            }
                        }
                    }
                }
            }
        }

        let current_log = if let Some(log_path) = latest_log_file {
            debug!(log_file = ?log_path, "Opening existing log file");
            
            // Read the last sequence number from the file
            if let Ok(file) = File::open(&log_path) {
                let reader = BufReader::new(file);
                for line in reader.lines() {
                    if let Ok(line) = line {
                        if let Ok(record) = serde_json::from_str::<WALRecord>(&line) {
                            latest_sequence = latest_sequence.max(record.sequence);
                        }
                    }
                }
            }

            // Open for append
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(log_path)?
        } else {
            // Create new log file
            let log_path = log_dir.join(format!("wal_{:012}.log", latest_sequence));
            debug!(log_file = ?log_path, "Creating new log file");
            
            OpenOptions::new()
                .create(true)
                .write(true)
                .append(true)
                .open(log_path)?
        };

        Ok((current_log, latest_sequence))
    }

    /// Load completed operations from all log files
    fn load_completed_operations(&self) -> Result<()> {
        debug!("Loading completed operations from WAL");
        let mut completed = HashMap::new();

        for entry in std::fs::read_dir(&self.log_dir)? {
            let entry = entry?;
            let path = entry.path();
            
            if let Some(filename) = path.file_name().and_then(|n| n.to_str()) {
                if filename.starts_with("wal_") && filename.ends_with(".log") {
                    let file = File::open(&path)?;
                    let reader = BufReader::new(file);
                    
                    for line in reader.lines() {
                        if let Ok(line) = line {
                            if let Ok(record) = serde_json::from_str::<WALRecord>(&line) {
                                if record.status == WALStatus::Completed {
                                    completed.insert(record.operation_id, record.sequence);
                                }
                            }
                        }
                    }
                }
            }
        }

        *self.completed_operations.lock().unwrap() = completed;
        
        info!(
            completed_operations = self.completed_operations.lock().unwrap().len(),
            "Loaded completed operations from WAL"
        );

        Ok(())
    }

    /// Check if an operation has already been completed (idempotency check)
    pub fn is_operation_completed(&self, operation_id: &str) -> bool {
        self.completed_operations
            .lock()
            .unwrap()
            .contains_key(operation_id)
    }

    /// Log start of an operation
    pub fn log_operation_start(
        &self,
        operation_id: String,
        operation: WALOperation,
    ) -> Result<u64> {
        // Check if already completed
        if self.is_operation_completed(&operation_id) {
            debug!(
                operation_id = %operation_id,
                "Operation already completed, skipping"
            );
            return Ok(0); // Return 0 to indicate already completed
        }

        let sequence = self.next_sequence();
        let record = WALRecord {
            sequence,
            timestamp: Utc::now(),
            operation_id: operation_id.clone(),
            operation,
            status: WALStatus::Started,
            checksum: String::new(), // Will be calculated after serialization
        };

        self.write_record(record)?;

        debug!(
            sequence = sequence,
            operation_id = %operation_id,
            "Logged operation start"
        );

        Ok(sequence)
    }

    /// Log progress update for an operation
    pub fn log_operation_progress(
        &self,
        operation_id: String,
        step: String,
    ) -> Result<()> {
        let sequence = self.next_sequence();
        let record = WALRecord {
            sequence,
            timestamp: Utc::now(),
            operation_id,
            operation: WALOperation::IndexDataFile {
                // This is a placeholder - in practice you'd store the operation
                file_ulid: "placeholder".to_string(),
                tenant: "placeholder".to_string(),
                signal: "placeholder".to_string(),
                partition_key: "placeholder".to_string(),
                parquet_url: "placeholder".to_string(),
                size_bytes: 0,
            },
            status: WALStatus::InProgress { step },
            checksum: String::new(),
        };

        self.write_record(record)?;
        Ok(())
    }

    /// Log successful completion of an operation
    pub fn log_operation_completed(
        &self,
        operation_id: String,
    ) -> Result<()> {
        let sequence = self.next_sequence();
        let record = WALRecord {
            sequence,
            timestamp: Utc::now(),
            operation_id: operation_id.clone(),
            operation: WALOperation::IndexDataFile {
                // Placeholder - in practice you'd store the actual operation
                file_ulid: "placeholder".to_string(),
                tenant: "placeholder".to_string(),
                signal: "placeholder".to_string(),
                partition_key: "placeholder".to_string(),
                parquet_url: "placeholder".to_string(),
                size_bytes: 0,
            },
            status: WALStatus::Completed,
            checksum: String::new(),
        };

        self.write_record(record.clone())?;

        // Mark as completed in memory
        self.completed_operations
            .lock()
            .unwrap()
            .insert(operation_id.clone(), sequence);

        info!(
            sequence = sequence,
            operation_id = %operation_id,
            "Logged operation completion"
        );

        Ok(())
    }

    /// Log operation failure
    pub fn log_operation_failed(
        &self,
        operation_id: String,
        error: String,
    ) -> Result<()> {
        let sequence = self.next_sequence();
        let record = WALRecord {
            sequence,
            timestamp: Utc::now(),
            operation_id,
            operation: WALOperation::IndexDataFile {
                // Placeholder
                file_ulid: "placeholder".to_string(),
                tenant: "placeholder".to_string(),
                signal: "placeholder".to_string(),
                partition_key: "placeholder".to_string(),
                parquet_url: "placeholder".to_string(),
                size_bytes: 0,
            },
            status: WALStatus::Failed { error },
            checksum: String::new(),
        };

        self.write_record(record)?;
        Ok(())
    }

    /// Log operation retry
    pub fn log_operation_retry(
        &self,
        operation_id: String,
        attempt: u32,
    ) -> Result<()> {
        let sequence = self.next_sequence();
        let record = WALRecord {
            sequence,
            timestamp: Utc::now(),
            operation_id,
            operation: WALOperation::IndexDataFile {
                // Placeholder
                file_ulid: "placeholder".to_string(),
                tenant: "placeholder".to_string(),
                signal: "placeholder".to_string(),
                partition_key: "placeholder".to_string(),
                parquet_url: "placeholder".to_string(),
                size_bytes: 0,
            },
            status: WALStatus::Retrying { attempt },
            checksum: String::new(),
        };

        self.write_record(record)?;
        Ok(())
    }

    /// Write a record to the WAL
    fn write_record(&self, mut record: WALRecord) -> Result<()> {
        // Calculate checksum
        let json_str = serde_json::to_string(&record)?;
        record.checksum = Self::calculate_checksum(&json_str);

        // Serialize with checksum
        let json_with_checksum = serde_json::to_string(&record)?;

        // Write to current log file
        {
            let mut log_file = self.current_log.lock().unwrap();
            writeln!(log_file, "{}", json_with_checksum)?;
            log_file.flush()?;
        }

        // Check if we need to rotate the log
        self.maybe_rotate_log()?;

        Ok(())
    }

    /// Get next sequence number
    fn next_sequence(&self) -> u64 {
        let mut seq = self.sequence_number.lock().unwrap();
        *seq += 1;
        *seq
    }

    /// Calculate checksum for integrity verification
    fn calculate_checksum(data: &str) -> String {
        use blake3::Hasher;
        let mut hasher = Hasher::new();
        hasher.update(data.as_bytes());
        hasher.finalize().to_hex().to_string()
    }

    /// Rotate log if it gets too large
    fn maybe_rotate_log(&self) -> Result<()> {
        let current_size = {
            let mut log_file = self.current_log.lock().unwrap();
            log_file.seek(SeekFrom::End(0))?
        };

        if current_size > self.log_rotation_size {
            self.rotate_log()?;
        }

        Ok(())
    }

    /// Rotate to a new log file
    fn rotate_log(&self) -> Result<()> {
        let new_sequence = *self.sequence_number.lock().unwrap() + 1000; // Jump sequence for new file
        let new_log_path = self.log_dir.join(format!("wal_{:012}.log", new_sequence));

        info!(
            old_sequence = *self.sequence_number.lock().unwrap(),
            new_sequence = new_sequence,
            new_log = ?new_log_path,
            "Rotating WAL log file"
        );

        let new_log_file = OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(new_log_path)?;

        // Replace the current log file
        *self.current_log.lock().unwrap() = new_log_file;
        *self.sequence_number.lock().unwrap() = new_sequence;

        Ok(())
    }

    /// Get recovery information from all WAL files
    pub fn get_recovery_info(&self) -> Result<RecoveryInfo> {
        let mut total_records = 0;
        let mut completed_records = 0;
        let mut failed_records = 0;
        let mut pending_records = 0;
        let mut last_sequence = 0;

        for entry in std::fs::read_dir(&self.log_dir)? {
            let entry = entry?;
            let path = entry.path();
            
            if let Some(filename) = path.file_name().and_then(|n| n.to_str()) {
                if filename.starts_with("wal_") && filename.ends_with(".log") {
                    let file = File::open(&path)?;
                    let reader = BufReader::new(file);
                    
                    for line in reader.lines() {
                        if let Ok(line) = line {
                            if let Ok(record) = serde_json::from_str::<WALRecord>(&line) {
                                total_records += 1;
                                last_sequence = last_sequence.max(record.sequence);
                                
                                match record.status {
                                    WALStatus::Completed => completed_records += 1,
                                    WALStatus::Failed { .. } => failed_records += 1,
                                    _ => pending_records += 1,
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(RecoveryInfo {
            total_records,
            completed_records,
            failed_records,
            pending_records,
            last_sequence,
        })
    }

    /// Replay operations from WAL (for recovery)
    pub fn replay_operations<F>(&self, mut handler: F) -> Result<usize>
    where
        F: FnMut(WALRecord) -> Result<()>,
    {
        debug!("Starting WAL replay");
        let mut replayed_count = 0;

        // Collect all log files and sort by sequence
        let mut log_files = Vec::new();
        for entry in std::fs::read_dir(&self.log_dir)? {
            let entry = entry?;
            let path = entry.path();
            
            if let Some(filename) = path.file_name().and_then(|n| n.to_str()) {
                if filename.starts_with("wal_") && filename.ends_with(".log") {
                    if let Some(seq_str) = filename.strip_prefix("wal_").and_then(|s| s.strip_suffix(".log")) {
                        if let Ok(seq) = seq_str.parse::<u64>() {
                            log_files.push((seq, path));
                        }
                    }
                }
            }
        }

        // Sort by sequence number
        log_files.sort_by_key(|(seq, _)| *seq);

        // Replay each file in order
        for (_, log_path) in log_files {
            debug!(log_file = ?log_path, "Replaying log file");
            
            let file = File::open(&log_path)?;
            let reader = BufReader::new(file);
            
            for (line_num, line) in reader.lines().enumerate() {
                match line {
                    Ok(line) => {
                        match serde_json::from_str::<WALRecord>(&line) {
                            Ok(record) => {
                                // Verify checksum
                                let json_without_checksum = serde_json::to_string(&WALRecord {
                                    checksum: String::new(),
                                    ..record.clone()
                                })?;
                                
                                let expected_checksum = Self::calculate_checksum(&json_without_checksum);
                                if record.checksum != expected_checksum {
                                    warn!(
                                        line = line_num + 1,
                                        log_file = ?log_path,
                                        "Checksum mismatch, skipping record"
                                    );
                                    continue;
                                }

                                // Call the handler
                                if let Err(e) = handler(record) {
                                    error!(
                                        line = line_num + 1,
                                        log_file = ?log_path,
                                        error = %e,
                                        "Failed to replay record"
                                    );
                                } else {
                                    replayed_count += 1;
                                }
                            }
                            Err(e) => {
                                warn!(
                                    line = line_num + 1,
                                    log_file = ?log_path,
                                    error = %e,
                                    "Failed to parse WAL record, skipping"
                                );
                            }
                        }
                    }
                    Err(e) => {
                        warn!(
                            line = line_num + 1,
                            log_file = ?log_path,
                            error = %e,
                            "Failed to read line, skipping"
                        );
                    }
                }
            }
        }

        info!(
            replayed_count = replayed_count,
            "WAL replay completed"
        );

        Ok(replayed_count)
    }

    /// Compact WAL by removing old/completed operations
    pub fn compact(&self, keep_completed_days: u32) -> Result<usize> {
        info!(keep_days = keep_completed_days, "Starting WAL compaction");
        
        let cutoff_time = Utc::now() - chrono::Duration::days(keep_completed_days as i64);
        let removed_records = 0;

        // This is a simplified implementation
        // In production, you'd want to create new compacted files
        // and remove the old ones atomically
        
        warn!("WAL compaction not fully implemented - would compact records older than {:?}", cutoff_time);

        Ok(removed_records)
    }

    /// Shutdown the WAL gracefully
    pub fn shutdown(&self) -> Result<()> {
        info!("Shutting down WAL");
        
        // Flush current log
        let mut log_file = self.current_log.lock().unwrap();
        log_file.flush()?;
        
        info!("WAL shutdown complete");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_wal_basic_operations() {
        let temp_dir = tempdir().unwrap();
        let wal = WAL::new(temp_dir.path(), 1024 * 1024).unwrap(); // 1MB rotation

        let operation_id = "test_op_123".to_string();
        let operation = WALOperation::IndexDataFile {
            file_ulid: "test_ulid".to_string(),
            tenant: "test_tenant".to_string(),
            signal: "logs".to_string(),
            partition_key: "2024-01-01-00".to_string(),
            parquet_url: "s3://bucket/file.parquet".to_string(),
            size_bytes: 1024,
        };

        // Test operation start
        let sequence = wal.log_operation_start(operation_id.clone(), operation).unwrap();
        assert!(sequence > 0);

        // Test idempotency - should return 0 if already completed
        wal.log_operation_completed(operation_id.clone()).unwrap();
        assert!(wal.is_operation_completed(&operation_id));

        // Test starting the same operation again
        let operation2 = WALOperation::IndexDataFile {
            file_ulid: "test_ulid".to_string(),
            tenant: "test_tenant".to_string(),
            signal: "logs".to_string(),
            partition_key: "2024-01-01-00".to_string(),
            parquet_url: "s3://bucket/file.parquet".to_string(),
            size_bytes: 1024,
        };
        let sequence2 = wal.log_operation_start(operation_id.clone(), operation2).unwrap();
        assert_eq!(sequence2, 0); // Should indicate already completed
    }

    #[test]
    fn test_wal_recovery_info() {
        let temp_dir = tempdir().unwrap();
        let wal = WAL::new(temp_dir.path(), 1024 * 1024).unwrap();

        // Log some operations
        for i in 0..5 {
            let op_id = format!("test_op_{}", i);
            let operation = WALOperation::IndexDataFile {
                file_ulid: format!("ulid_{}", i),
                tenant: "test".to_string(),
                signal: "logs".to_string(),
                partition_key: "2024-01-01-00".to_string(),
                parquet_url: "s3://bucket/file.parquet".to_string(),
                size_bytes: 1024,
            };
            
            wal.log_operation_start(op_id.clone(), operation).unwrap();
            
            if i % 2 == 0 {
                wal.log_operation_completed(op_id).unwrap();
            } else {
                wal.log_operation_failed(op_id, "test error".to_string()).unwrap();
            }
        }

        let recovery_info = wal.get_recovery_info().unwrap();
        assert!(recovery_info.total_records > 0);
        assert!(recovery_info.completed_records > 0);
        assert!(recovery_info.failed_records > 0);
    }

    #[test]
    fn test_wal_replay() {
        let temp_dir = tempdir().unwrap();
        let wal = WAL::new(temp_dir.path(), 1024 * 1024).unwrap();

        // Log some operations
        let op_id = "replay_test_op".to_string();
        let operation = WALOperation::IndexDataFile {
            file_ulid: "replay_ulid".to_string(),
            tenant: "test".to_string(),
            signal: "logs".to_string(),
            partition_key: "2024-01-01-00".to_string(),
            parquet_url: "s3://bucket/file.parquet".to_string(),
            size_bytes: 1024,
        };
        
        wal.log_operation_start(op_id.clone(), operation).unwrap();
        wal.log_operation_completed(op_id).unwrap();

        // Test replay
        let mut replayed_records = Vec::new();
        let replayed_count = wal.replay_operations(|record| {
            replayed_records.push(record);
            Ok(())
        }).unwrap();

        assert!(replayed_count > 0);
        assert!(!replayed_records.is_empty());
    }
}