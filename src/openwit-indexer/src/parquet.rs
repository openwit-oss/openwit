use anyhow::{Context, Result};
use arrow::array::ArrayRef;
use arrow::datatypes::{Schema};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use opendal::Operator;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ProjectionMask;
use parquet::basic::{LogicalType, ConvertedType, Type as PhysicalType};
use parquet::file::metadata::{RowGroupMetaData, ColumnChunkMetaData};
use parquet::file::reader::{FileReader, SerializedFileReader, ChunkReader, Length};
use parquet::schema::types::{Type};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info, warn};

/// Wrapper to make Bytes work as ChunkReader
#[derive(Clone)]
struct BytesChunkReader {
    data: Bytes,
}

impl Length for BytesChunkReader {
    fn len(&self) -> u64 {
        self.data.len() as u64
    }
}

impl ChunkReader for BytesChunkReader {
    type T = std::io::Cursor<Bytes>;

    fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
        let start = start as usize;
        if start >= self.data.len() {
            return Err(parquet::errors::ParquetError::IndexOutOfBound(
                start,
                self.data.len(),
            ));
        }
        Ok(std::io::Cursor::new(self.data.slice(start..)))
    }

    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<Bytes> {
        let start = start as usize;
        let end = start + length;
        if end > self.data.len() {
            return Err(parquet::errors::ParquetError::IndexOutOfBound(
                end,
                self.data.len(),
            ));
        }
        Ok(self.data.slice(start..end))
    }
}

/// Parquet file reader for extracting metadata and column statistics
#[derive(Clone)]
pub struct ParquetReader {
    storage: Arc<Operator>,
}

/// Column statistics extracted from parquet metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnStats {
    pub column_name: String,
    pub data_type: String,
    pub null_count: Option<i64>,
    pub distinct_count: Option<i64>,
    pub min_value: Option<String>,
    pub max_value: Option<String>,
    pub total_byte_size: Option<i64>,
}

/// Row group statistics for zone map generation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RowGroupStats {
    pub row_group_id: usize,
    pub num_rows: i64,
    pub total_byte_size: i64,
    pub columns: Vec<ColumnStats>,
}

/// File-level statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileStats {
    pub file_path: String,
    pub num_rows: i64,
    pub num_row_groups: usize,
    pub total_size_bytes: i64,
    pub created_by: Option<String>,
    pub schema: Vec<FieldSchema>,
    pub row_groups: Vec<RowGroupStats>,
    pub key_value_metadata: Option<HashMap<String, String>>,
}

/// Schema field information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldSchema {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    pub metadata: Option<HashMap<String, String>>,
}

/// Column page reader for selective data access
#[allow(dead_code)]
pub struct ColumnPageReader {
    reader: Box<dyn FileReader>,
    schema: Arc<Schema>,
}

/// Page-level data for column scanning
#[derive(Debug)]
pub struct ColumnPage {
    pub column_name: String,
    pub row_group_id: usize,
    pub page_id: usize,
    pub data: ArrayRef,
    pub null_count: usize,
    pub min_value: Option<String>,
    pub max_value: Option<String>,
}

impl ParquetReader {
    pub fn new(storage: Arc<Operator>) -> Self {
        Self { storage }
    }

    /// Read parquet file metadata and statistics
    pub async fn read_file_stats(&self, file_path: &str) -> Result<FileStats> {
        debug!(path = %file_path, "Reading parquet file statistics");

        let file_data = self.storage
            .read(file_path)
            .await?;

        let chunk_reader = BytesChunkReader { data: file_data.to_bytes() };
        let file_size = chunk_reader.len();
        let reader = SerializedFileReader::new(chunk_reader)?;
        let metadata = reader.metadata();

        let file_metadata = metadata.file_metadata();
        let schema = metadata.file_metadata().schema_descr();

        // Extract schema information
        let schema_fields = self.extract_schema_fields(schema.root_schema())?;

        // Extract row group statistics
        let mut row_groups = Vec::new();
        for (rg_idx, rg_metadata) in metadata.row_groups().iter().enumerate() {
            let rg_stats = self.extract_row_group_stats(rg_idx, rg_metadata)?;
            row_groups.push(rg_stats);
        }

        // Extract key-value metadata
        let key_value_metadata = file_metadata.key_value_metadata()
            .map(|kv_meta| {
                kv_meta.iter()
                    .filter_map(|kv| {
                        kv.value.as_ref().map(|v| (kv.key.clone(), v.clone()))
                    })
                    .collect::<HashMap<String, String>>()
            });

        let stats = FileStats {
            file_path: file_path.to_string(),
            num_rows: file_metadata.num_rows(),
            num_row_groups: metadata.num_row_groups(),
            total_size_bytes: file_size as i64,
            created_by: file_metadata.created_by().map(|s| s.to_string()),
            schema: schema_fields,
            row_groups,
            key_value_metadata,
        };

        info!(
            path = %file_path,
            rows = stats.num_rows,
            row_groups = stats.num_row_groups,
            size_mb = stats.total_size_bytes / (1024 * 1024),
            "Successfully read parquet file statistics"
        );

        Ok(stats)
    }

    /// Extract schema field information
    fn extract_schema_fields(&self, schema_type: &Type) -> Result<Vec<FieldSchema>> {
        let mut field_schemas = Vec::new();

        match schema_type {
            Type::GroupType { fields, .. } => {
                for field in fields {
                    let field_schema = match field.as_ref() {
                        Type::PrimitiveType { basic_info, physical_type, .. } => {
                            // For PrimitiveType, logical_type and converted_type need to be extracted differently
                            let data_type = self.map_parquet_type_to_string(
                                physical_type,
                                None, // TODO: Extract logical type from primitive type
                                None, // TODO: Extract converted type from primitive type
                            );
                            
                            FieldSchema {
                                name: basic_info.name().to_string(),
                                data_type,
                                nullable: basic_info.repetition() == parquet::basic::Repetition::OPTIONAL,
                                metadata: None,
                            }
                        }
                        Type::GroupType { basic_info, .. } => {
                            FieldSchema {
                                name: basic_info.name().to_string(),
                                data_type: "struct".to_string(),
                                nullable: basic_info.repetition() == parquet::basic::Repetition::OPTIONAL,
                                metadata: None,
                            }
                        }
                    };
                    field_schemas.push(field_schema);
                }
            }
            Type::PrimitiveType { .. } => {
                // Root schema should be a group type
                return Err(anyhow::anyhow!("Expected group type for root schema"));
            }
        }

        Ok(field_schemas)
    }

    /// Map parquet types to string representations
    fn map_parquet_type_to_string(
        &self,
        physical_type: &PhysicalType,
        logical_type: Option<&LogicalType>,
        converted_type: Option<&ConvertedType>,
    ) -> String {
        // Check logical type first (Parquet 2.0)
        if let Some(logical) = logical_type {
            return match logical {
                LogicalType::String => "string".to_string(),
                LogicalType::Integer { bit_width, is_signed } => {
                    format!("int{}{}", bit_width, if *is_signed { "" } else { "_unsigned" })
                },
                LogicalType::Timestamp { unit, .. } => {
                    let unit_str = match unit {
                        parquet::basic::TimeUnit::MILLIS(_) => "ms",
                        parquet::basic::TimeUnit::MICROS(_) => "us", 
                        parquet::basic::TimeUnit::NANOS(_) => "ns",
                    };
                    format!("timestamp[{}]", unit_str)
                },
                LogicalType::Date => "date32".to_string(),
                LogicalType::Time { unit, .. } => {
                    let unit_str = match unit {
                        parquet::basic::TimeUnit::MILLIS(_) => "ms",
                        parquet::basic::TimeUnit::MICROS(_) => "us",
                        parquet::basic::TimeUnit::NANOS(_) => "ns",
                    };
                    format!("time64[{}]", unit_str)
                },
                LogicalType::Decimal { scale, precision } => {
                    format!("decimal({}, {})", precision, scale)
                },
                LogicalType::List => "list".to_string(),
                LogicalType::Map => "map".to_string(),
                _ => format!("{:?}", logical),
            };
        }

        // Fall back to converted type (Parquet 1.0)
        if let Some(converted) = converted_type {
            return match converted {
                ConvertedType::UTF8 => "string".to_string(),
                ConvertedType::INT_8 => "int8".to_string(),
                ConvertedType::INT_16 => "int16".to_string(),
                ConvertedType::INT_32 => "int32".to_string(),
                ConvertedType::INT_64 => "int64".to_string(),
                ConvertedType::UINT_8 => "uint8".to_string(),
                ConvertedType::UINT_16 => "uint16".to_string(),
                ConvertedType::UINT_32 => "uint32".to_string(),
                ConvertedType::UINT_64 => "uint64".to_string(),
                ConvertedType::TIMESTAMP_MILLIS => "timestamp[ms]".to_string(),
                ConvertedType::TIMESTAMP_MICROS => "timestamp[us]".to_string(),
                ConvertedType::DATE => "date32".to_string(),
                ConvertedType::TIME_MILLIS => "time32[ms]".to_string(),
                ConvertedType::TIME_MICROS => "time64[us]".to_string(),
                ConvertedType::DECIMAL => "decimal".to_string(),
                ConvertedType::LIST => "list".to_string(),
                ConvertedType::MAP | ConvertedType::MAP_KEY_VALUE => "map".to_string(),
                _ => format!("{:?}", converted),
            };
        }

        // Fall back to physical type
        match physical_type {
            PhysicalType::BOOLEAN => "bool".to_string(),
            PhysicalType::INT32 => "int32".to_string(),
            PhysicalType::INT64 => "int64".to_string(),
            PhysicalType::INT96 => "int96".to_string(),
            PhysicalType::FLOAT => "float32".to_string(),
            PhysicalType::DOUBLE => "float64".to_string(),
            PhysicalType::BYTE_ARRAY => "binary".to_string(),
            PhysicalType::FIXED_LEN_BYTE_ARRAY => "fixed_size_binary".to_string(),
        }
    }

    /// Extract row group statistics
    fn extract_row_group_stats(
        &self,
        rg_idx: usize,
        rg_metadata: &RowGroupMetaData,
    ) -> Result<RowGroupStats> {
        let mut columns = Vec::new();

        for col_chunk in rg_metadata.columns() {
            let column_stats = self.extract_column_stats(col_chunk)?;
            columns.push(column_stats);
        }

        Ok(RowGroupStats {
            row_group_id: rg_idx,
            num_rows: rg_metadata.num_rows(),
            total_byte_size: rg_metadata.total_byte_size(),
            columns,
        })
    }

    /// Extract column-level statistics
    #[allow(deprecated)]
    fn extract_column_stats(&self, col_chunk: &ColumnChunkMetaData) -> Result<ColumnStats> {
        let column_path = col_chunk.column_path();
        let column_name = column_path.parts().join(".");
        
        let stats = col_chunk.statistics();
        let (min_value, max_value, null_count, distinct_count) = if let Some(stats) = stats {
            let min_val = if stats.has_min_max_set() {
                Some(format!("{:?}", stats.min_bytes()))
            } else {
                None
            };
            let max_val = if stats.has_min_max_set() {
                Some(format!("{:?}", stats.max_bytes()))
            } else {
                None
            };
            let null_count = stats.null_count_opt().map(|c| c as i64);
            let distinct_count = stats.distinct_count_opt().map(|c| c as i64);
            (min_val, max_val, null_count, distinct_count)
        } else {
            (None, None, None, None)
        };

        let data_type = format!("{:?}", col_chunk.column_type());

        Ok(ColumnStats {
            column_name,
            data_type,
            null_count,
            distinct_count,
            min_value,
            max_value,
            total_byte_size: Some(col_chunk.compressed_size()),
        })
    }

    /// Create column page reader for selective scanning
    pub async fn create_column_reader(&self, file_path: &str) -> Result<ColumnPageReader> {
        debug!(path = %file_path, "Creating column page reader");

        let file_data = self.storage
            .read(file_path)
            .await?;

        let chunk_reader = BytesChunkReader { data: file_data.to_bytes() };
        let reader = SerializedFileReader::new(chunk_reader)?;
        
        // Build Arrow schema from Parquet schema
        let parquet_schema = reader.metadata().file_metadata().schema_descr_ptr();
        let arrow_schema = parquet::arrow::parquet_to_arrow_schema(parquet_schema.as_ref(), None)?;

        Ok(ColumnPageReader {
            reader: Box::new(reader),
            schema: Arc::new(arrow_schema),
        })
    }

    /// Read specific columns from file with projection
    pub async fn read_columns(
        &self,
        file_path: &str,
        column_names: &[String],
        row_group_indices: Option<Vec<usize>>,
    ) -> Result<Vec<RecordBatch>> {
        debug!(
            path = %file_path,
            columns = ?column_names,
            row_groups = ?row_group_indices,
            "Reading projected columns"
        );

        let file_data = self.storage
            .read(file_path)
            .await?;

        let chunk_reader = BytesChunkReader { data: file_data.to_bytes() };
        let reader = ParquetRecordBatchReaderBuilder::try_new(chunk_reader)?;
        
        // Create projection mask for specified columns
        let schema = reader.schema();
        let mut projection_indices = Vec::new();
        
        for col_name in column_names {
            if let Ok(index) = schema.index_of(col_name) {
                projection_indices.push(index);
            } else {
                warn!(column = %col_name, "Column not found in schema, skipping");
            }
        }

        let projection = ProjectionMask::roots(reader.parquet_schema(), projection_indices);
        let mut reader = reader
            .with_projection(projection)
            .build()?;

        let mut batches = Vec::new();
        while let Some(batch) = reader.next() {
            match batch {
                Ok(batch) => batches.push(batch),
                Err(e) => {
                    warn!(error = %e, "Failed to read batch, continuing");
                    continue;
                }
            }
        }

        info!(
            path = %file_path,
            batches = batches.len(),
            total_rows = batches.iter().map(|b| b.num_rows()).sum::<usize>(),
            "Successfully read projected columns"
        );

        Ok(batches)
    }
}

impl ColumnPageReader {
    /// Read specific column data by name
    pub fn read_column_data(
        &mut self,
        column_name: &str,
        row_group_id: Option<usize>,
    ) -> Result<Vec<ColumnPage>> {
        debug!(
            column = %column_name,
            row_group = ?row_group_id,
            "Reading column data"
        );

        let pages = Vec::new();

        // For now, we'll use the Arrow reader to get the data
        // In a production system, you might want to implement page-level reading
        // directly with the parquet crate for more fine-grained control
        
        let schema_ref = self.schema.clone();
        let _column_index = schema_ref.index_of(column_name)
            .with_context(|| format!("Column '{}' not found in schema", column_name))?;

        // This is a simplified implementation - in practice you'd want to read
        // at the page level for better memory efficiency
        warn!(
            column = %column_name,
            "Column page reading not fully implemented - using record batch approach"
        );

        Ok(pages)
    }

    /// Get schema information
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// Get column names
    pub fn column_names(&self) -> Vec<String> {
        self.schema.fields()
            .iter()
            .map(|f| f.name().clone())
            .collect()
    }
}

/// Utility functions for parquet processing
pub mod utils {
    use super::*;

    /// Extract timestamp range from file stats
    pub fn extract_time_range(stats: &FileStats) -> Option<(DateTime<Utc>, DateTime<Utc>)> {
        // Look for timestamp columns and their min/max values
        let mut min_ts = None;
        let mut max_ts = None;

        for rg in &stats.row_groups {
            for col in &rg.columns {
                if col.column_name.contains("timestamp") || col.column_name.contains("ts") {
                    if let (Some(min_str), Some(max_str)) = (&col.min_value, &col.max_value) {
                        // Try to parse timestamp values
                        // This is simplified - real implementation would handle different formats
                        if let (Ok(min_parsed), Ok(max_parsed)) = (
                            min_str.parse::<i64>(),
                            max_str.parse::<i64>()
                        ) {
                            let min_dt = DateTime::from_timestamp_millis(min_parsed);
                            let max_dt = DateTime::from_timestamp_millis(max_parsed);
                            
                            if let (Some(min_dt), Some(max_dt)) = (min_dt, max_dt) {
                                min_ts = Some(min_ts.map(|ts: DateTime<Utc>| ts.min(min_dt)).unwrap_or(min_dt));
                                max_ts = Some(max_ts.map(|ts: DateTime<Utc>| ts.max(max_dt)).unwrap_or(max_dt));
                            }
                        }
                    }
                }
            }
        }

        min_ts.zip(max_ts)
    }

    /// Calculate partition key from file stats
    pub fn calculate_partition_key(stats: &FileStats) -> String {
        if let Some((min_ts, _)) = extract_time_range(stats) {
            // Create hourly partitions
            min_ts.format("%Y-%m-%d-%H").to_string()
        } else {
            // Fall back to default partitioning
            "unknown".to_string()
        }
    }

    /// Check if file contains specific columns
    pub fn has_columns(stats: &FileStats, required_columns: &[String]) -> bool {
        let available_columns: std::collections::HashSet<String> = stats.schema
            .iter()
            .map(|f| f.name.clone())
            .collect();

        required_columns.iter().all(|col| available_columns.contains(col))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use parquet::arrow::ArrowWriter;
    use arrow::array::Int64Array;

    #[tokio::test]
    async fn test_parquet_reader() {
        let store = Arc::new(InMemory::new());
        let reader = ParquetReader::new(store.clone());

        // Create test parquet file in memory
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())), false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(arrow::array::TimestampMillisecondArray::from(vec![
                    1672531200000, 1672531260000, 1672531320000
                ])),
            ],
        ).unwrap();

        // Write to in-memory store
        let mut buffer = Vec::new();
        {
            let mut writer = ArrowWriter::try_new(&mut buffer, schema, None).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
        }

        let test_path = ObjectPath::from("test.parquet");
        store.put(&test_path, buffer.into()).await.unwrap();

        // Test reading stats
        let stats = reader.read_file_stats("test.parquet").await.unwrap();
        assert_eq!(stats.num_rows, 3);
        assert_eq!(stats.schema.len(), 2);
        assert_eq!(stats.schema[0].name, "id");
    }
}