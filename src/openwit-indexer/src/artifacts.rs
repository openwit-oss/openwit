use anyhow::{Context, Result};
use arrow::array::*;
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use tantivy::{
    schema::{Schema, SchemaBuilder, Field as TantivyField, TextFieldIndexing, TextOptions},
    Index,
    directory::MmapDirectory,
};
use tracing::{debug, info, warn};
use zstd::stream::{encode_all, decode_all};

use crate::bloom_wrapper::SerializableBloom;

use crate::parquet::{FileStats};

/// Zone map statistics for data pruning
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ZoneMap {
    pub file_ulid: String,
    pub partition_key: String,
    pub row_group_stats: Vec<ZoneMapEntry>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ZoneMapEntry {
    pub row_group_id: usize,
    pub column_name: String,
    pub data_type: String,
    pub min_value: Option<String>,
    pub max_value: Option<String>,
    pub null_count: u64,
    pub row_count: u64,
}

/// Bloom filter for existence checks
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BloomFilter {
    pub file_ulid: String,
    pub partition_key: String,
    pub column_blooms: HashMap<String, SerializableBloom>,
    pub false_positive_rate: f64,
    pub created_at: DateTime<Utc>,
}

/// Serializable wrapper for RoaringBitmap
#[derive(Debug, Clone)]
pub struct SerializableBitmap(pub RoaringBitmap);

impl Serialize for SerializableBitmap {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut bytes = Vec::new();
        self.0.serialize_into(&mut bytes).map_err(serde::ser::Error::custom)?;
        serializer.serialize_bytes(&bytes)
    }
}

impl<'de> Deserialize<'de> for SerializableBitmap {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let bytes = Vec::<u8>::deserialize(deserializer)?;
        let bitmap = RoaringBitmap::deserialize_from(&bytes[..])
            .map_err(serde::de::Error::custom)?;
        Ok(SerializableBitmap(bitmap))
    }
}

/// Bitmap index for categorical data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BitmapIndex {
    pub file_ulid: String,
    pub partition_key: String,
    pub column_bitmaps: HashMap<String, HashMap<String, SerializableBitmap>>,
    pub created_at: DateTime<Utc>,
}

/// Tantivy full-text index
pub struct TantivyIndex {
    pub file_ulid: String,
    pub partition_key: String,
    pub index: Index,
    pub schema: Schema,
    pub text_fields: HashMap<String, TantivyField>,
    pub keyword_fields: HashMap<String, TantivyField>,
    pub created_at: DateTime<Utc>,
}

/// Artifact builder for creating index structures from parquet data
pub struct ArtifactBuilder {
    pub config: crate::config::ArtifactSettings,
}

impl ArtifactBuilder {
    pub fn new(config: crate::config::ArtifactSettings) -> Self {
        Self { config }
    }

    /// Build zone map from file statistics
    pub fn build_zone_map(&self, stats: &FileStats) -> Result<ZoneMap> {
        debug!(
            file = %stats.file_path,
            row_groups = stats.num_row_groups,
            "Building zone map"
        );

        let mut zone_map_entries = Vec::new();

        for rg_stats in &stats.row_groups {
            for col_stats in &rg_stats.columns {
                // Only create zone map for configured columns
                if self.config.zone_map_columns.contains(&col_stats.column_name) {
                    let entry = ZoneMapEntry {
                        row_group_id: rg_stats.row_group_id,
                        column_name: col_stats.column_name.clone(),
                        data_type: col_stats.data_type.clone(),
                        min_value: col_stats.min_value.clone(),
                        max_value: col_stats.max_value.clone(),
                        null_count: col_stats.null_count.unwrap_or(0) as u64,
                        row_count: rg_stats.num_rows as u64,
                    };
                    zone_map_entries.push(entry);
                }
            }
        }

        let file_ulid = self.extract_ulid_from_path(&stats.file_path)?;
        let partition_key = crate::parquet::utils::calculate_partition_key(stats);

        let zone_map = ZoneMap {
            file_ulid,
            partition_key,
            row_group_stats: zone_map_entries,
            created_at: Utc::now(),
        };

        info!(
            file = %stats.file_path,
            entries = zone_map.row_group_stats.len(),
            "Successfully built zone map"
        );

        Ok(zone_map)
    }

    /// Build bloom filters from record batches
    pub fn build_bloom_filter(
        &self,
        file_ulid: &str,
        partition_key: &str,
        batches: &[RecordBatch],
    ) -> Result<BloomFilter> {
        debug!(
            file_ulid = %file_ulid,
            batches = batches.len(),
            "Building bloom filters"
        );

        let mut column_blooms = HashMap::new();

        // Estimate number of unique items for bloom filter sizing
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        let estimated_uniques = (total_rows as f64 * 0.7) as usize; // Conservative estimate

        for column_name in &self.config.bloom_columns {
            let mut bloom = SerializableBloom::new_for_fp_rate(
                estimated_uniques,
                self.config.bloom_fpr,
            );

            // Process all batches for this column
            for batch in batches {
                if let Ok(column_index) = batch.schema().index_of(column_name) {
                    let column = batch.column(column_index);
                    self.add_column_to_bloom(&mut bloom, column)?;
                }
            }

            column_blooms.insert(column_name.clone(), bloom);
        }

        let bloom_filter = BloomFilter {
            file_ulid: file_ulid.to_string(),
            partition_key: partition_key.to_string(),
            column_blooms,
            false_positive_rate: self.config.bloom_fpr,
            created_at: Utc::now(),
        };

        info!(
            file_ulid = %file_ulid,
            columns = bloom_filter.column_blooms.len(),
            "Successfully built bloom filters"
        );

        Ok(bloom_filter)
    }

    /// Add column values to bloom filter
    fn add_column_to_bloom(
        &self,
        bloom: &mut SerializableBloom,
        column: &ArrayRef,
    ) -> Result<()> {
        use arrow::datatypes::DataType;

        match column.data_type() {
            DataType::Utf8 => {
                let string_array = column
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .context("Failed to downcast to StringArray")?;

                for i in 0..string_array.len() {
                    if !string_array.is_null(i) {
                        bloom.set(string_array.value(i).as_bytes());
                    }
                }
            }
            DataType::Int64 => {
                let int_array = column
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .context("Failed to downcast to Int64Array")?;

                for i in 0..int_array.len() {
                    if !int_array.is_null(i) {
                        bloom.set(&int_array.value(i).to_le_bytes());
                    }
                }
            }
            DataType::Int32 => {
                let int_array = column
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .context("Failed to downcast to Int32Array")?;

                for i in 0..int_array.len() {
                    if !int_array.is_null(i) {
                        bloom.set(&int_array.value(i).to_le_bytes());
                    }
                }
            }
            DataType::Float64 => {
                let float_array = column
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .context("Failed to downcast to Float64Array")?;

                for i in 0..float_array.len() {
                    if !float_array.is_null(i) {
                        bloom.set(&float_array.value(i).to_le_bytes());
                    }
                }
            }
            DataType::Boolean => {
                let bool_array = column
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .context("Failed to downcast to BooleanArray")?;

                for i in 0..bool_array.len() {
                    if !bool_array.is_null(i) {
                        let value = if bool_array.value(i) { 1u8 } else { 0u8 };
                        bloom.set(&[value]);
                    }
                }
            }
            _ => {
                warn!(
                    data_type = ?column.data_type(),
                    "Unsupported data type for bloom filter, skipping"
                );
            }
        }

        Ok(())
    }

    /// Build bitmap index from record batches
    pub fn build_bitmap_index(
        &self,
        file_ulid: &str,
        partition_key: &str,
        batches: &[RecordBatch],
    ) -> Result<BitmapIndex> {
        debug!(
            file_ulid = %file_ulid,
            batches = batches.len(),
            "Building bitmap index"
        );

        let mut column_bitmaps: HashMap<String, HashMap<String, SerializableBitmap>> = HashMap::new();

        for column_name in &self.config.bitmap_columns {
            let mut value_bitmaps: HashMap<String, RoaringBitmap> = HashMap::new();
            let mut global_row_id = 0u32;

            for batch in batches {
                if let Ok(column_index) = batch.schema().index_of(column_name) {
                    let column = batch.column(column_index);
                    self.add_column_to_bitmap(&mut value_bitmaps, column, global_row_id)?;
                    global_row_id += batch.num_rows() as u32;
                }
            }

            let wrapped_bitmaps = value_bitmaps
                .into_iter()
                .map(|(k, v)| (k, SerializableBitmap(v)))
                .collect();
            column_bitmaps.insert(column_name.clone(), wrapped_bitmaps);
        }

        let bitmap_index = BitmapIndex {
            file_ulid: file_ulid.to_string(),
            partition_key: partition_key.to_string(),
            column_bitmaps,
            created_at: Utc::now(),
        };

        info!(
            file_ulid = %file_ulid,
            columns = bitmap_index.column_bitmaps.len(),
            "Successfully built bitmap index"
        );

        Ok(bitmap_index)
    }

    /// Add column values to bitmap index
    fn add_column_to_bitmap(
        &self,
        value_bitmaps: &mut HashMap<String, RoaringBitmap>,
        column: &ArrayRef,
        start_row_id: u32,
    ) -> Result<()> {
        use arrow::datatypes::DataType;

        match column.data_type() {
            DataType::Utf8 => {
                let string_array = column
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .context("Failed to downcast to StringArray")?;

                for i in 0..string_array.len() {
                    if !string_array.is_null(i) {
                        let value = string_array.value(i);
                        let bitmap = value_bitmaps
                            .entry(value.to_string())
                            .or_insert_with(RoaringBitmap::new);
                        bitmap.insert(start_row_id + i as u32);
                    }
                }
            }
            DataType::Int64 => {
                let int_array = column
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .context("Failed to downcast to Int64Array")?;

                for i in 0..int_array.len() {
                    if !int_array.is_null(i) {
                        let value = int_array.value(i).to_string();
                        let bitmap = value_bitmaps
                            .entry(value)
                            .or_insert_with(RoaringBitmap::new);
                        bitmap.insert(start_row_id + i as u32);
                    }
                }
            }
            DataType::Boolean => {
                let bool_array = column
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .context("Failed to downcast to BooleanArray")?;

                for i in 0..bool_array.len() {
                    if !bool_array.is_null(i) {
                        let value = bool_array.value(i).to_string();
                        let bitmap = value_bitmaps
                            .entry(value)
                            .or_insert_with(RoaringBitmap::new);
                        bitmap.insert(start_row_id + i as u32);
                    }
                }
            }
            _ => {
                warn!(
                    data_type = ?column.data_type(),
                    "Unsupported data type for bitmap index, skipping"
                );
            }
        }

        Ok(())
    }

    /// Build Tantivy full-text index from record batches
    pub fn build_tantivy_index(
        &self,
        file_ulid: &str,
        partition_key: &str,
        batches: &[RecordBatch],
        index_dir: &Path,
    ) -> Result<TantivyIndex> {
        debug!(
            file_ulid = %file_ulid,
            batches = batches.len(),
            index_dir = ?index_dir,
            "Building Tantivy full-text index"
        );

        // Build schema
        let mut schema_builder = SchemaBuilder::default();
        let mut text_fields = HashMap::new();
        let mut keyword_fields = HashMap::new();

        // Add text fields
        for field_name in &self.config.tantivy_text_fields {
            let text_options = TextOptions::default()
                .set_indexing_options(TextFieldIndexing::default())
                .set_stored();
            let field = schema_builder.add_text_field(field_name, text_options);
            text_fields.insert(field_name.clone(), field);
        }

        // Add keyword fields
        for field_name in &self.config.tantivy_keyword_fields {
            let text_options = TextOptions::default()
                .set_indexing_options(TextFieldIndexing::default())
                .set_stored()
                .set_fast(None);
            let field = schema_builder.add_text_field(field_name, text_options);
            keyword_fields.insert(field_name.clone(), field);
        }

        let schema = schema_builder.build();

        // Create index directory
        std::fs::create_dir_all(index_dir)?;
        let dir = MmapDirectory::open(index_dir)?;
        let index = Index::open_or_create(dir, schema.clone())?;

        let mut index_writer = index.writer(50_000_000)?; // 50MB memory budget

        // Index all batches
        let mut doc_count = 0;
        for batch in batches {
            for row_idx in 0..batch.num_rows() {
                let mut doc = tantivy::doc!();

                // Add text fields
                for (field_name, field) in &text_fields {
                    if let Ok(column_index) = batch.schema().index_of(field_name) {
                        let column = batch.column(column_index);
                        if let Some(value) = self.extract_string_value(column, row_idx)? {
                            doc.add_text(*field, &value);
                        }
                    }
                }

                // Add keyword fields
                for (field_name, field) in &keyword_fields {
                    if let Ok(column_index) = batch.schema().index_of(field_name) {
                        let column = batch.column(column_index);
                        if let Some(value) = self.extract_string_value(column, row_idx)? {
                            doc.add_text(*field, &value);
                        }
                    }
                }

                index_writer.add_document(doc)?;
                doc_count += 1;
            }
        }

        index_writer.commit()?;

        let tantivy_index = TantivyIndex {
            file_ulid: file_ulid.to_string(),
            partition_key: partition_key.to_string(),
            index,
            schema,
            text_fields,
            keyword_fields,
            created_at: Utc::now(),
        };

        info!(
            file_ulid = %file_ulid,
            documents = doc_count,
            text_fields = tantivy_index.text_fields.len(),
            keyword_fields = tantivy_index.keyword_fields.len(),
            "Successfully built Tantivy index"
        );

        Ok(tantivy_index)
    }

    /// Extract string value from Arrow array
    fn extract_string_value(&self, column: &ArrayRef, row_idx: usize) -> Result<Option<String>> {
        use arrow::datatypes::DataType;

        if column.is_null(row_idx) {
            return Ok(None);
        }

        match column.data_type() {
            DataType::Utf8 => {
                let string_array = column
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .context("Failed to downcast to StringArray")?;
                Ok(Some(string_array.value(row_idx).to_string()))
            }
            DataType::Int64 => {
                let int_array = column
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .context("Failed to downcast to Int64Array")?;
                Ok(Some(int_array.value(row_idx).to_string()))
            }
            DataType::Float64 => {
                let float_array = column
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .context("Failed to downcast to Float64Array")?;
                Ok(Some(float_array.value(row_idx).to_string()))
            }
            DataType::Boolean => {
                let bool_array = column
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .context("Failed to downcast to BooleanArray")?;
                Ok(Some(bool_array.value(row_idx).to_string()))
            }
            _ => {
                warn!(
                    data_type = ?column.data_type(),
                    "Unsupported data type for text extraction, skipping"
                );
                Ok(None)
            }
        }
    }

    /// Extract ULID from file path
    fn extract_ulid_from_path(&self, file_path: &str) -> Result<String> {
        // Extract ULID from the file path - this is a simplified implementation
        // In practice, you'd have a more robust way to extract the ULID
        if let Some(filename) = std::path::Path::new(file_path).file_stem() {
            if let Some(filename_str) = filename.to_str() {
                // Assume ULID is at the beginning of the filename
                let parts: Vec<&str> = filename_str.split('_').collect();
                if !parts.is_empty() {
                    return Ok(parts[0].to_string());
                }
            }
        }
        
        // Fallback: generate a new ULID
        Ok(ulid::Ulid::new().to_string())
    }
}

/// Serialization helpers for artifacts
impl ZoneMap {
    /// Serialize zone map to bytes
    pub fn serialize(&self) -> Result<Bytes> {
        let json_bytes = serde_json::to_vec(self)?;
        let compressed = encode_all(&json_bytes[..], 3)?; // Level 3 compression
        Ok(Bytes::from(compressed))
    }

    /// Deserialize zone map from bytes
    pub fn deserialize(data: &[u8]) -> Result<Self> {
        let decompressed = decode_all(data)?;
        let zone_map = serde_json::from_slice(&decompressed)?;
        Ok(zone_map)
    }
}

impl BloomFilter {
    /// Serialize bloom filter to bytes
    pub fn serialize(&self) -> Result<Bytes> {
        let mut data = Vec::new();
        
        // Write header
        data.extend_from_slice(&(self.column_blooms.len() as u32).to_le_bytes());
        data.extend_from_slice(&self.false_positive_rate.to_le_bytes());
        
        // Write each bloom filter
        for (column_name, bloom) in &self.column_blooms {
            let name_bytes = column_name.as_bytes();
            data.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes());
            data.extend_from_slice(name_bytes);
            
            let bloom_bytes = bincode::serialize(bloom)?;
            data.extend_from_slice(&(bloom_bytes.len() as u32).to_le_bytes());
            data.extend_from_slice(&bloom_bytes);
        }

        let compressed = encode_all(&data[..], 3)?;
        Ok(Bytes::from(compressed))
    }

    /// Deserialize bloom filter from bytes
    pub fn deserialize(data: &[u8], file_ulid: String, partition_key: String) -> Result<Self> {
        let decompressed = decode_all(data)?;
        let mut cursor = std::io::Cursor::new(decompressed);
        
        use std::io::Read;
        
        // Read header
        let mut buf = [0u8; 4];
        cursor.read_exact(&mut buf)?;
        let bloom_count = u32::from_le_bytes(buf);
        
        let mut buf = [0u8; 8];
        cursor.read_exact(&mut buf)?;
        let false_positive_rate = f64::from_le_bytes(buf);
        
        let mut column_blooms = HashMap::new();
        
        // Read each bloom filter
        for _ in 0..bloom_count {
            // Read column name
            let mut buf = [0u8; 4];
            cursor.read_exact(&mut buf)?;
            let name_len = u32::from_le_bytes(buf) as usize;
            
            let mut name_bytes = vec![0u8; name_len];
            cursor.read_exact(&mut name_bytes)?;
            let column_name = String::from_utf8(name_bytes)?;
            
            // Read bloom data
            let mut buf = [0u8; 4];
            cursor.read_exact(&mut buf)?;
            let bloom_len = u32::from_le_bytes(buf) as usize;
            
            let mut bloom_bytes = vec![0u8; bloom_len];
            cursor.read_exact(&mut bloom_bytes)?;
            
            let bloom: SerializableBloom = bincode::deserialize(&bloom_bytes)?;
            column_blooms.insert(column_name, bloom);
        }

        Ok(BloomFilter {
            file_ulid,
            partition_key,
            column_blooms,
            false_positive_rate,
            created_at: Utc::now(),
        })
    }

    /// Check if value might exist in bloom filter
    pub fn might_contain(&self, column: &str, value: &[u8]) -> bool {
        if let Some(bloom) = self.column_blooms.get(column) {
            bloom.check(value)
        } else {
            false
        }
    }
}

impl BitmapIndex {
    /// Serialize bitmap index to bytes
    pub fn serialize(&self) -> Result<Bytes> {
        let json_bytes = serde_json::to_vec(self)?;
        let compressed = encode_all(&json_bytes[..], 3)?;
        Ok(Bytes::from(compressed))
    }

    /// Deserialize bitmap index from bytes
    pub fn deserialize(data: &[u8]) -> Result<Self> {
        let decompressed = decode_all(data)?;
        let bitmap_index = serde_json::from_slice(&decompressed)?;
        Ok(bitmap_index)
    }

    /// Get row IDs for a specific column value
    pub fn get_row_ids(&self, column: &str, value: &str) -> Option<&RoaringBitmap> {
        self.column_bitmaps
            .get(column)
            .and_then(|value_map| value_map.get(value))
            .map(|sb| &sb.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{StringArray, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;
    use tempfile::tempdir;

    #[test]
    fn test_zone_map_serialization() {
        let zone_map = ZoneMap {
            file_ulid: "test_ulid".to_string(),
            partition_key: "2024-01-01-00".to_string(),
            row_group_stats: vec![],
            created_at: Utc::now(),
        };

        let serialized = zone_map.serialize().unwrap();
        let deserialized = ZoneMap::deserialize(&serialized).unwrap();
        
        assert_eq!(zone_map.file_ulid, deserialized.file_ulid);
        assert_eq!(zone_map.partition_key, deserialized.partition_key);
    }

    #[tokio::test]
    async fn test_artifact_builder() {
        let config = crate::config::ArtifactSettings::default();
        let builder = ArtifactBuilder::new(config);

        // Create test record batch
        let schema = Arc::new(Schema::new(vec![
            Field::new("trace_id", DataType::Utf8, false),
            Field::new("message", DataType::Utf8, false),
            Field::new("severity", DataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["trace1", "trace2", "trace3"])),
                Arc::new(StringArray::from(vec!["hello", "world", "test"])),
                Arc::new(StringArray::from(vec!["INFO", "ERROR", "INFO"])),
            ],
        ).unwrap();

        // Test bloom filter
        let bloom = builder.build_bloom_filter(
            "test_ulid",
            "2024-01-01-00",
            &[batch.clone()],
        ).unwrap();

        assert!(!bloom.column_blooms.is_empty());
        assert!(bloom.might_contain("trace_id", b"trace1"));
        assert!(!bloom.might_contain("trace_id", b"nonexistent"));

        // Test bitmap index
        let bitmap = builder.build_bitmap_index(
            "test_ulid",
            "2024-01-01-00",
            &[batch.clone()],
        ).unwrap();

        assert!(!bitmap.column_bitmaps.is_empty());
        if let Some(row_ids) = bitmap.get_row_ids("severity", "INFO") {
            assert_eq!(row_ids.len(), 2); // Row 0 and 2 have "INFO"
            assert!(row_ids.contains(0));
            assert!(row_ids.contains(2));
        }
    }

    #[test]
    fn test_tantivy_index() {
        let config = crate::config::ArtifactSettings::default();
        let builder = ArtifactBuilder::new(config);
        let temp_dir = tempdir().unwrap();

        // Create test record batch
        let schema = Arc::new(Schema::new(vec![
            Field::new("message", DataType::Utf8, false),
            Field::new("service.name", DataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["hello world", "test message", "error occurred"])),
                Arc::new(StringArray::from(vec!["web-service", "api-service", "auth-service"])),
            ],
        ).unwrap();

        // Test tantivy index
        let tantivy_index = builder.build_tantivy_index(
            "test_ulid",
            "2024-01-01-00",
            &[batch.clone()],
            temp_dir.path(),
        ).unwrap();

        assert!(!tantivy_index.text_fields.is_empty());
        assert!(!tantivy_index.keyword_fields.is_empty());

        // Verify we can search the index
        let reader = tantivy_index.index.reader().unwrap();
        let searcher = reader.searcher();
        
        if let Some(message_field) = tantivy_index.text_fields.get("message") {
            let query_parser = QueryParser::for_index(&tantivy_index.index, vec![*message_field]);
            let query = query_parser.parse_query("hello").unwrap();
            let top_docs = searcher.search(&query, &TopDocs::with_limit(10)).unwrap();
            assert!(!top_docs.is_empty());
        }
    }
}