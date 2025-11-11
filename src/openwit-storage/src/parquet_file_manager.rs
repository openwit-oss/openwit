use std::sync::Arc;
use std::collections::HashMap;
use tokio::sync::RwLock;
use anyhow::{Result, Context};
use tracing::{info, error, debug, warn};
use chrono::{DateTime, Utc};
use arrow::record_batch::RecordBatch;
use std::path::PathBuf;
use crate::cloud_upload::{CloudUploader, AzureStorageConfig, S3StorageConfig};
use crate::schema_evolution::SchemaEvolution;

/// Manages parquet files with automatic rotation based on size and time
#[derive(Clone)]
pub struct ParquetFileManager {
    /// Base data directory
    data_dir: String,
    
    /// Size threshold for file rotation (bytes)
    size_threshold_bytes: u64,
    
    /// Time threshold for file rotation (seconds)
    time_threshold_secs: u64,
    
    /// Active files by client name
    active_files: Arc<RwLock<HashMap<String, ActiveFileInfo>>>,
    
    /// Cloud uploader
    cloud_uploader: Arc<CloudUploader>,
    
    /// Whether to delete files after upload
    delete_after_upload: bool,
    
    /// Delay before uploading files (seconds)
    upload_delay_secs: u64,
}

/// Information about an active file (without the actual writer)
struct ActiveFileInfo {
    path: PathBuf,
    created_at: DateTime<Utc>,
    bytes_written: u64,
    rows_written: u64,
    estimated_file_size: u64,
    _schema: arrow::datatypes::SchemaRef,
}

impl ParquetFileManager {
    pub async fn new(
        data_dir: String,
        size_threshold_bytes: u64,
        time_threshold_secs: u64,
        azure_config: Option<AzureStorageConfig>,
        s3_config: Option<S3StorageConfig>,
        delete_after_upload: bool,
        upload_delay_secs: u64,
    ) -> Result<Self> {
        // Create cloud uploader with both Azure and S3 support
        let cloud_uploader = Arc::new(CloudUploader::new(azure_config, s3_config).await?);
        
        let manager = Self {
            data_dir,
            size_threshold_bytes,
            time_threshold_secs,
            active_files: Arc::new(RwLock::new(HashMap::new())),
            cloud_uploader,
            delete_after_upload,
            upload_delay_secs,
        };
        
        // Start periodic rotation check task
        let manager_clone = manager.clone();
        tokio::spawn(async move {
            manager_clone.periodic_rotation_task().await;
        });
        
        Ok(manager)
    }
    
    /// Write a batch for a specific client
    pub async fn write_batch(&self, client_name: &str, batch: RecordBatch) -> Result<()> {
        let mut active_files = self.active_files.write().await;
        
        // Check if we have an active file for this client
        let should_rotate = if let Some(info) = active_files.get(client_name) {
            let rotate = self.should_rotate_file(info).await?;
            if rotate {
                info!("[FILE MANAGER] Rotation triggered for client '{}' - size: {:.2}MB, age: {}s", 
                      client_name, 
                      info.estimated_file_size as f64 / 1_048_576.0,
                      (Utc::now() - info.created_at).num_seconds());
            }
            rotate
        } else {
            info!("[FILE MANAGER] No active file for client '{}', creating new file", client_name);
            true // No active file, need to create one
        };
        
        if should_rotate {
            // Close current file if exists
            if let Some(info) = active_files.remove(client_name) {
                info!("[FILE MANAGER] Finalizing active file for client '{}' before rotation", client_name);
                self.finalize_file(&info).await?;
            }
            
            // Create new file info
            let new_info = self.create_new_file_info(client_name, &batch).await?;
            active_files.insert(client_name.to_string(), new_info);
        }
        
        // Get file info and write batch
        if let Some(info) = active_files.get_mut(client_name) {
            let batch_size = batch.get_array_memory_size() as u64;
            let batch_rows = batch.num_rows() as u64;
            
            // Write batch in blocking task
            let path = info.path.clone();
            let batch_clone = batch.clone();
            tokio::task::spawn_blocking(move || {
                append_batch_to_file(&path, batch_clone)
            }).await??;
            
            info.bytes_written += batch_size;
            info.rows_written += batch_rows;
            
            // Update file size estimate more frequently for accurate rotation
            // Check actual file size after every batch to ensure we don't miss rotation threshold
            if let Ok(file_size) = tokio::fs::metadata(&info.path).await {
                info.estimated_file_size = file_size.len();
                if info.rows_written % 10 == 0 || info.estimated_file_size >= (self.size_threshold_bytes as f64 * 0.8) as u64 {
                    info!("[FILE MANAGER] Size check for client '{}': actual size: {:.2} MB (threshold: {:.2} MB)", 
                          client_name,
                          info.estimated_file_size as f64 / 1_048_576.0,
                          self.size_threshold_bytes as f64 / 1_048_576.0);
                }
            }
            
            debug!("Wrote batch to {} ({} total rows)", client_name, info.rows_written);
        }
        
        Ok(())
    }
    
    /// Check if file should be rotated
    async fn should_rotate_file(&self, info: &ActiveFileInfo) -> Result<bool> {
        // Always check actual file size before making rotation decision
        let actual_size = if let Ok(metadata) = tokio::fs::metadata(&info.path).await {
            metadata.len()
        } else {
            info.estimated_file_size
        };
        
        // Time-based rotation
        let age = Utc::now() - info.created_at;
        let time_exceeded = age.num_seconds() as u64 >= self.time_threshold_secs;
        
        // Size-based rotation - use actual size
        let size_exceeded = actual_size >= self.size_threshold_bytes;
        
        if time_exceeded {
            info!("[ROTATION] File rotation triggered by TIME for {}", info.path.display());
            info!("[ROTATION] Age: {} seconds / {} seconds threshold", age.num_seconds(), self.time_threshold_secs);
            return Ok(true);
        }
        
        if size_exceeded {
            info!("[ROTATION] File rotation triggered by SIZE for {}", info.path.display());
            info!("[ROTATION] Size: {:.2} MB / {:.2} MB threshold", 
                  actual_size as f64 / 1_048_576.0,
                  self.size_threshold_bytes as f64 / 1_048_576.0);
            return Ok(true);
        }
        
        Ok(false)
    }
    
    /// Create a new active file info
    async fn create_new_file_info(&self, client_name: &str, schema_batch: &RecordBatch) -> Result<ActiveFileInfo> {
        // Create client directory
        let client_dir = PathBuf::from(&self.data_dir).join(client_name);
        tokio::fs::create_dir_all(&client_dir).await?;
        
        // Generate filename with timestamp and ULID
        let timestamp = Utc::now().format("%Y%m%d_%H%M%S_%f");
        let ulid = ulid::Ulid::new();
        let filename = format!("{}_{}_{}_active.parquet", client_name, timestamp, ulid);
        let filepath = client_dir.join(&filename);
        
        info!("Creating new parquet file: {}", filepath.display());
        
        // Create empty file with schema
        let schema = schema_batch.schema();
        let path_clone = filepath.clone();
        let schema_clone = schema.clone();
        tokio::task::spawn_blocking(move || {
            create_parquet_file(&path_clone, schema_clone)
        }).await??;
        
        Ok(ActiveFileInfo {
            path: filepath,
            created_at: Utc::now(),
            bytes_written: 0,
            rows_written: 0,
            estimated_file_size: 0,
            _schema: schema,
        })
    }
    
    /// Finalize and close a file
    async fn finalize_file(&self, info: &ActiveFileInfo) -> Result<()> {
        info!("[FINALIZE] Starting file finalization for: {}", info.path.display());
        
        // Get actual file size
        let file_metadata = tokio::fs::metadata(&info.path).await?;
        let file_size = file_metadata.len();
        
        info!("[FINALIZE] File stats: {} ({} rows, {:.2} MB compressed)", 
             info.path.display(),
             info.rows_written,
             file_size as f64 / 1_048_576.0);
        
        // Rename from active to stable
        let stable_path = info.path.with_file_name(
            info.path.file_name()
                .and_then(|n| n.to_str())
                .map(|n| n.replace("_active.parquet", "_stable.parquet"))
                .unwrap_or_else(|| "unknown_stable.parquet".to_string())
        );
        
        tokio::fs::rename(&info.path, &stable_path).await?;
        info!("Renamed to stable: {}", stable_path.display());
        
        // Upload to cloud storage if configured
        let uploader = self.cloud_uploader.clone();
        let delete_after_upload = self.delete_after_upload;
        let stable_path_str = stable_path.to_string_lossy().to_string();
        let upload_delay_secs = self.upload_delay_secs;
        
        tokio::spawn(async move {
            info!("[UPLOAD] Starting cloud upload process for: {}", stable_path_str);
            
            // Skip upload delay - upload immediately when time criteria is met
            info!("[UPLOAD] Uploading immediately (no delay)");
            
            // Extract client name and file name for remote path
            let file_name = stable_path.file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("unknown.parquet");
            
            // Extract client name from path (parent directory name)
            let client_name = stable_path.parent()
                .and_then(|p| p.file_name())
                .and_then(|n| n.to_str())
                .unwrap_or("unknown");
            
            let remote_path = format!("{}/{}", client_name, file_name);
            
            info!("[UPLOAD] Uploading {} to cloud storage as {}", file_name, remote_path);
            info!("[UPLOAD] Local path: {}", stable_path_str);
            info!("[UPLOAD] Remote path: {}", remote_path);
            
            match uploader.upload_file(&stable_path_str, &remote_path).await {
                Ok(()) => {
                    info!("[UPLOAD] Successfully uploaded {} to cloud storage!", file_name);
                    
                    // Delete local file if configured
                    if delete_after_upload {
                        info!("[UPLOAD] Deleting local file after successful upload: {}", file_name);
                        if let Err(e) = uploader.delete_local_file(&stable_path_str, delete_after_upload).await {
                            error!("[UPLOAD] Failed to delete local file after upload: {}", e);
                        } else {
                            info!("[UPLOAD] Local file deleted successfully: {}", file_name);
                        }
                    } else {
                        info!("[UPLOAD] Keeping local file (delete_after_upload=false): {}", file_name);
                    }
                }
                Err(e) => {
                    error!("[UPLOAD] Failed to upload {} to cloud storage: {}", file_name, e);
                    error!("[UPLOAD] Upload error details: {:?}", e);
                }
            }
        });
        
        Ok(())
    }
    
    /// Flush all active files
    pub async fn flush_all(&self) -> Result<()> {
        let mut active_files = self.active_files.write().await;
        
        for (client, info) in active_files.drain() {
            info!("Flushing active file for client: {}", client);
            if let Err(e) = self.finalize_file(&info).await {
                error!("Failed to finalize file for {}: {}", client, e);
            }
        }
        
        Ok(())
    }
    
    /// Shutdown and clean up
    pub async fn shutdown(self) -> Result<()> {
        info!("Shutting down ParquetFileManager");
        
        // Flush all active files
        self.flush_all().await?;
        
        info!("ParquetFileManager shutdown complete");
        Ok(())
    }
    
    /// Periodic task to check and rotate files based on time
    async fn periodic_rotation_task(&self) {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(60)); // Check every minute
        
        loop {
            interval.tick().await;
            
            debug!("Running periodic rotation check");
            
            // Get list of files that need rotation
            let files_to_rotate = {
                let active_files = self.active_files.read().await;
                let mut to_rotate = Vec::new();
                
                for (client_name, info) in active_files.iter() {
                    let age = Utc::now() - info.created_at;
                    if age.num_seconds() as u64 >= self.time_threshold_secs {
                        info!("Periodic check: File for client '{}' exceeded time threshold ({} seconds old)", 
                              client_name, age.num_seconds());
                        to_rotate.push(client_name.clone());
                    }
                }
                
                to_rotate
            };
            
            // Rotate files that exceeded time threshold
            for client_name in files_to_rotate {
                let mut active_files = self.active_files.write().await;
                if let Some(info) = active_files.remove(&client_name) {
                    info!("Periodic rotation: Finalizing file for client '{}'", client_name);
                    if let Err(e) = self.finalize_file(&info).await {
                        error!("Failed to finalize file during periodic rotation: {}", e);
                        // Re-insert on error to avoid losing track of the file
                        active_files.insert(client_name, info);
                    }
                }
            }
            
            // Log status
            let active_count = self.active_files.read().await.len();
            if active_count > 0 {
                debug!("Periodic rotation check complete. {} active files remaining", active_count);
            }
        }
    }
}


fn create_parquet_file(path: &PathBuf, schema: arrow::datatypes::SchemaRef) -> Result<()> {
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::{WriterProperties, EnabledStatistics};
    use parquet::basic::{Compression, ZstdLevel};
    
    let file = std::fs::File::create(path)?;
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_max_row_group_size(1_000_000)
        .build();
    
    let writer = ArrowWriter::try_new(file, schema, Some(props))?;
    writer.close()?;
    
    Ok(())
}

fn append_batch_to_file(path: &PathBuf, batch: RecordBatch) -> Result<()> {
    use parquet::arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder};
    use parquet::file::properties::{WriterProperties, EnabledStatistics};
    use parquet::basic::{Compression, ZstdLevel};
    
    info!("[PARQUET] Appending batch with {} rows to {:?}", batch.num_rows(), path);
    
    // First, check if file exists and has content
    let file_exists = path.exists();
    let file_size = if file_exists {
        std::fs::metadata(path)?.len()
    } else {
        0
    };
    
    info!("[PARQUET] File exists: {}, size: {} bytes", file_exists, file_size);
    
    // If file doesn't exist or is empty, create it with the batch
    if !file_exists || file_size == 0 {
        info!("[PARQUET] Creating new file with {} rows", batch.num_rows());
        let file = std::fs::File::create(path)?;
        let schema = batch.schema();
        
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
            .set_statistics_enabled(EnabledStatistics::Page)
            .set_created_by("openwit-storage".to_string())
            .build();
        
        let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
        writer.write(&batch)?;
        writer.close()?;
        
        info!("[PARQUET] Created new file with {} rows", batch.num_rows());
        return Ok(());
    }
    
    // Read existing data
    info!("[PARQUET] Reading existing data from file");
    let mut existing_batches = vec![];
    let mut merged_schema = batch.schema();
    
    match std::fs::File::open(path) {
        Ok(file) => {
            match ParquetRecordBatchReaderBuilder::try_new(file) {
                Ok(builder) => {
                    // Get metadata first
                    let metadata = builder.metadata();
                    let num_row_groups = metadata.num_row_groups();
                    let total_rows = metadata.file_metadata().num_rows();
                    info!("[PARQUET] Existing file: {} row groups, {} total rows", num_row_groups, total_rows);
                    
                    // Store schema
                    let existing_schema = builder.schema().clone();
                    
                    // Check schema compatibility
                    let new_schema = batch.schema();
                    if existing_schema != new_schema {
                        warn!("[PARQUET] Schema evolution needed!");
                        warn!("[PARQUET] Existing schema fields: {}", existing_schema.fields().len());
                        warn!("[PARQUET] New batch schema fields: {}", new_schema.fields().len());
                        
                        // Merge schemas to create a superset
                        match SchemaEvolution::merge_schemas(existing_schema.clone(), new_schema.clone()) {
                            Ok(merged) => {
                                info!("[PARQUET] Created merged schema with {} fields", merged.fields().len());
                                merged_schema = merged;
                            }
                            Err(e) => {
                                error!("[PARQUET] Failed to merge schemas: {}", e);
                                return Err(e.context("Schema evolution failed"));
                            }
                        }
                    } else {
                        merged_schema = existing_schema.clone();
                    }
                    
                    // Read batches
                    match builder.build() {
                        Ok(reader) => {
                            let mut total_rows_read = 0;
                            for (i, batch_result) in reader.enumerate() {
                                match batch_result {
                                    Ok(existing_batch) => {
                                        let rows = existing_batch.num_rows();
                                        debug!("[PARQUET] Read batch {}: {} rows", i, rows);
                                        total_rows_read += rows;
                                        
                                        // Adapt existing batch to merged schema if needed
                                        let adapted_batch = if existing_batch.schema() != merged_schema {
                                            match SchemaEvolution::adapt_batch_to_schema(existing_batch, merged_schema.clone()) {
                                                Ok(adapted) => adapted,
                                                Err(e) => {
                                                    error!("[PARQUET] Failed to adapt existing batch {}: {}", i, e);
                                                    return Err(e);
                                                }
                                            }
                                        } else {
                                            existing_batch
                                        };
                                        
                                        existing_batches.push(adapted_batch);
                                    }
                                    Err(e) => {
                                        error!("[PARQUET] Error reading batch {}: {}", i, e);
                                        return Err(anyhow::anyhow!("Failed to read batch {}: {}", i, e));
                                    }
                                }
                            }
                            info!("[PARQUET] Successfully read {} batches with {} total rows", 
                                  existing_batches.len(), total_rows_read);
                        }
                        Err(e) => {
                            error!("[PARQUET] Failed to build reader: {}", e);
                            return Err(anyhow::anyhow!("Failed to build parquet reader: {}", e));
                        }
                    }
                }
                Err(e) => {
                    error!("[PARQUET] Failed to create reader builder: {}", e);
                    // If file is corrupted, rename it and create new
                    let backup_path = path.with_extension("corrupted");
                    error!("[PARQUET] Moving corrupted file to {:?}", backup_path);
                    std::fs::rename(path, backup_path)?;
                    
                    // Create new file with just the new batch
                    let file = std::fs::File::create(path)?;
                    let schema = batch.schema();
                    
                    let props = WriterProperties::builder()
                        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
                        .set_statistics_enabled(EnabledStatistics::Page)
                        .set_created_by("openwit-storage".to_string())
                        .build();
                    
                    let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
                    writer.write(&batch)?;
                    writer.close()?;
                    
                    info!("[PARQUET] Created new file with {} rows after corruption", batch.num_rows());
                    return Ok(());
                }
            }
        }
        Err(e) => {
            error!("[PARQUET] Failed to open file: {}", e);
            return Err(anyhow::anyhow!("Failed to open parquet file: {}", e));
        }
    }
    
    // Adapt new batch to merged schema if needed
    info!("[PARQUET] Adding new batch with {} rows", batch.num_rows());
    let final_schema = if existing_batches.is_empty() {
        batch.schema()
    } else {
        // Use the merged schema from above
        merged_schema.clone()
    };
    
    let adapted_new_batch = if batch.schema() != final_schema {
        info!("[PARQUET] Adapting new batch to merged schema");
        match SchemaEvolution::adapt_batch_to_schema(batch, final_schema.clone()) {
            Ok(adapted) => adapted,
            Err(e) => {
                error!("[PARQUET] Failed to adapt new batch: {}", e);
                return Err(e);
            }
        }
    } else {
        batch
    };
    
    existing_batches.push(adapted_new_batch);
    
    // Calculate total rows
    let total_rows: usize = existing_batches.iter().map(|b| b.num_rows()).sum();
    info!("[PARQUET] Total rows after append: {} in {} batches", total_rows, existing_batches.len());
    
    // Write to temporary file
    let temp_path = path.with_extension("tmp");
    info!("[PARQUET] Writing to temporary file: {:?}", temp_path);
    
    let file = std::fs::File::create(&temp_path)
        .context("Failed to create temporary file")?;
    
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_created_by("openwit-storage".to_string())
        .set_max_row_group_size(1_000_000)
        .build();
    
    let mut writer = ArrowWriter::try_new(file, final_schema.clone(), Some(props))?;
    
    // Write each batch with detailed logging
    let mut total_written = 0;
    for (i, batch_to_write) in existing_batches.iter().enumerate() {
        let batch_rows = batch_to_write.num_rows();
        debug!("[PARQUET] Writing batch {}: {} rows", i, batch_rows);
        
        match writer.write(batch_to_write) {
            Ok(_) => {
                total_written += batch_rows;
                debug!("[PARQUET] Successfully wrote batch {}", i);
            }
            Err(e) => {
                error!("[PARQUET] Error writing batch {}: {}", i, e);
                error!("[PARQUET] Batch schema: {:?}", batch_to_write.schema());
                error!("[PARQUET] Expected schema: {:?}", final_schema);
                
                // Clean up temp file
                let _ = std::fs::remove_file(&temp_path);
                
                return Err(anyhow::anyhow!(
                    "Failed to write batch {}: {}. Total rows written: {}, expected: {}",
                    i, e, total_written, total_rows
                ));
            }
        }
    }
    
    info!("[PARQUET] Closing writer after writing {} total rows", total_written);
    writer.close().context("Failed to close parquet writer")?;
    
    // Verify the temp file
    let temp_size = std::fs::metadata(&temp_path)?.len();
    info!("[PARQUET] Temporary file size: {} bytes", temp_size);
    
    // Atomic replace
    info!("[PARQUET] Replacing original file");
    std::fs::rename(&temp_path, path)
        .context("Failed to rename temporary file to final path")?;
    
    info!("[PARQUET] Successfully appended batch to {:?}", path);
    Ok(())
}