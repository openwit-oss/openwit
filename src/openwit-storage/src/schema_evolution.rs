use std::sync::Arc;
use arrow::array::{ArrayRef, new_null_array};
use arrow::datatypes::{Schema, Field, DataType};
use arrow::record_batch::RecordBatch;
use anyhow::Result;
use std::collections::HashMap;
use tracing::debug;

/// Handle schema evolution for dynamic OTLP data
pub struct SchemaEvolution;

impl SchemaEvolution {
    /// Merge two schemas, creating a superset that includes all fields from both
    pub fn merge_schemas(schema1: Arc<Schema>, schema2: Arc<Schema>) -> Result<Arc<Schema>> {
        let mut field_map: HashMap<String, Field> = HashMap::new();
        
        // Add all fields from schema1
        for field in schema1.fields() {
            field_map.insert(field.name().clone(), field.as_ref().clone());
        }
        
        // Add fields from schema2, updating if they differ
        for field in schema2.fields() {
            match field_map.get(field.name()) {
                Some(existing_field) => {
                    // If data types differ, keep the field but make it nullable
                    if existing_field.data_type() != field.data_type() {
                        let merged_field = Field::new(
                            field.name(),
                            field.data_type().clone(),
                            true // Make nullable to handle missing values
                        );
                        field_map.insert(field.name().clone(), merged_field);
                    } else if !existing_field.is_nullable() && field.is_nullable() {
                        // If nullability differs, make it nullable
                        field_map.insert(field.name().clone(), field.as_ref().clone());
                    }
                }
                None => {
                    // New field - add it as nullable
                    let new_field = Field::new(
                        field.name(),
                        field.data_type().clone(),
                        true // Make nullable since it doesn't exist in all records
                    );
                    field_map.insert(field.name().clone(), new_field);
                }
            }
        }
        
        // Sort fields by name for consistency
        let mut fields: Vec<Field> = field_map.into_values().collect();
        fields.sort_by(|a, b| a.name().cmp(b.name()));
        
        Ok(Arc::new(Schema::new(fields)))
    }
    
    /// Adapt a RecordBatch to match a target schema
    pub fn adapt_batch_to_schema(batch: RecordBatch, target_schema: Arc<Schema>) -> Result<RecordBatch> {
        let source_schema = batch.schema();
        
        if source_schema == target_schema {
            // Schemas match, no adaptation needed
            return Ok(batch);
        }
        
        debug!("Adapting batch from {} fields to {} fields", 
               source_schema.fields().len(), 
               target_schema.fields().len());
        
        // Create a map of source columns by name
        let mut source_columns: HashMap<String, ArrayRef> = HashMap::new();
        for (i, field) in source_schema.fields().iter().enumerate() {
            source_columns.insert(field.name().clone(), batch.column(i).clone());
        }
        
        // Build new columns according to target schema
        let mut new_columns: Vec<ArrayRef> = Vec::with_capacity(target_schema.fields().len());
        
        for target_field in target_schema.fields() {
            let column = match source_columns.get(target_field.name()) {
                Some(source_column) => {
                    // Check if types match
                    if source_column.data_type() == target_field.data_type() {
                        source_column.clone()
                    } else {
                        // Type mismatch - create null array
                        debug!("Type mismatch for field {}: {:?} vs {:?}, using nulls",
                               target_field.name(), 
                               source_column.data_type(), 
                               target_field.data_type());
                        new_null_array(target_field.data_type(), batch.num_rows())
                    }
                }
                None => {
                    // Field doesn't exist in source - create null array
                    debug!("Field {} not found in source batch, creating null array", 
                           target_field.name());
                    new_null_array(target_field.data_type(), batch.num_rows())
                }
            };
            
            new_columns.push(column);
        }
        
        RecordBatch::try_new(target_schema, new_columns)
            .map_err(|e| anyhow::anyhow!("Failed to create adapted batch: {}", e))
    }
    
    /// Create a stable schema for flattened OTLP data by normalizing field names
    pub fn normalize_schema(schema: Arc<Schema>) -> Arc<Schema> {
        let fields: Vec<Field> = schema.fields()
            .iter()
            .map(|field| {
                let normalized_name = Self::normalize_field_name(field.name());
                if &normalized_name != field.name() {
                    Field::new(normalized_name, field.data_type().clone(), field.is_nullable())
                } else {
                    field.as_ref().clone()
                }
            })
            .collect();
        
        Arc::new(Schema::new(fields))
    }
    
    /// Normalize field names to handle array index variations
    /// e.g., "attributes[34].key" -> "attributes[*].key"
    fn normalize_field_name(name: &str) -> String {
        // Replace array indices with wildcards
        let re = regex::Regex::new(r"\[(\d+)\]").unwrap();
        re.replace_all(name, "[*]").to_string()
    }
    
    /// Check if two schemas are compatible (ignoring array indices)
    pub fn schemas_compatible(schema1: &Schema, schema2: &Schema) -> bool {
        let normalized1 = Self::normalize_schema(Arc::new(schema1.clone()));
        let normalized2 = Self::normalize_schema(Arc::new(schema2.clone()));
        
        normalized1.fields().len() == normalized2.fields().len() &&
        normalized1.fields().iter().zip(normalized2.fields().iter())
            .all(|(f1, f2)| f1.name() == f2.name() && f1.data_type() == f2.data_type())
    }
}