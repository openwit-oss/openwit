use std::collections::HashMap;
use anyhow::Result;
use serde_json::{Value, json};
use tracing::debug;
use arrow::array::{MapBuilder, StringBuilder};

/// Converter for flat JSON attributes to byte array key-value format
pub struct JsonToKvConverter;

impl JsonToKvConverter {
    /// Convert string to byte array
    fn string_to_bytes(s: &str) -> Vec<u8> {
        s.bytes().collect()
    }
    
    /// Convert flat JSON attributes to byte array format
    pub fn convert_flat_json_to_kv_array(attributes: &HashMap<String, String>) -> Result<Value> {
        let mut result = Vec::new();
        
        for (key, value) in attributes {
            // Convert key to bytes
            let key_bytes: Vec<u64> = Self::string_to_bytes(key)
                .into_iter()
                .map(|b| b as u64)
                .collect();
            
            // Convert value to bytes
            let value_bytes: Vec<u64> = Self::string_to_bytes(value)
                .into_iter()
                .map(|b| b as u64)
                .collect();
            
            result.push(json!({
                "key": key_bytes,
                "value": value_bytes
            }));
        }
        
        debug!("Converted {} attributes to KV byte array format", result.len());
        Ok(Value::Array(result))
    }
    
    /// Convert serde_json::Value object to byte array format
    pub fn convert_json_object_to_kv_array(json_obj: &Value) -> Result<Value> {
        let obj = json_obj.as_object()
            .ok_or_else(|| anyhow::anyhow!("Expected JSON object"))?;
        
        let mut attributes = HashMap::new();
        for (key, value) in obj {
            let value_str = match value {
                Value::String(s) => s.clone(),
                Value::Number(n) => n.to_string(),
                Value::Bool(b) => b.to_string(),
                Value::Null => "null".to_string(),
                Value::Array(_) | Value::Object(_) => {
                    serde_json::to_string(value)
                        .map_err(|e| anyhow::anyhow!("Failed to serialize complex value: {}", e))?
                }
            };
            attributes.insert(key.clone(), value_str);
        }
        
        Self::convert_flat_json_to_kv_array(&attributes)
    }
    
    /// Convert from flat dot-notation attributes to byte array format
    /// This is useful when you have OTLP span attributes that need to be converted
    pub fn convert_span_attributes_to_kv_array(
        span_attributes: &HashMap<String, String>
    ) -> Result<Value> {
        Self::convert_flat_json_to_kv_array(span_attributes)
    }
    
    /// Convert HashMap attributes to Arrow MapBuilder format (like in your trace processing)
    /// This is compatible with your existing MapBuilder approach for Parquet storage
    pub fn add_attributes_to_map_builder(
        attributes: &HashMap<String, String>,
        map_builder: &mut MapBuilder<StringBuilder, StringBuilder>
    ) -> Result<()> {
        for (key, value) in attributes {
            map_builder.keys().append_value(key);
            map_builder.values().append_value(value);
        }
        map_builder.append(!attributes.is_empty())?;
        debug!("Added {} attributes to MapBuilder", attributes.len());
        Ok(())
    }
    
    /// Create a new MapBuilder for key-value attributes (matching your trace schema)
    pub fn create_attributes_map_builder() -> MapBuilder<StringBuilder, StringBuilder> {
        MapBuilder::new(
            None,
            StringBuilder::with_capacity(100, 1024),
            StringBuilder::with_capacity(100, 1024)
        )
    }
    
    /// Convert byte array KV format back to HashMap for MapBuilder processing
    pub fn convert_kv_array_to_attributes(kv_array: &Value) -> Result<HashMap<String, String>> {
        let array = kv_array.as_array()
            .ok_or_else(|| anyhow::anyhow!("Expected JSON array"))?;
        
        let mut attributes = HashMap::new();
        
        for item in array {
            let obj = item.as_object()
                .ok_or_else(|| anyhow::anyhow!("Expected object with 'key' and 'value' fields"))?;
            
            let key_array = obj.get("key")
                .and_then(|v| v.as_array())
                .ok_or_else(|| anyhow::anyhow!("Missing or invalid 'key' field"))?;
            
            let value_array = obj.get("value")
                .and_then(|v| v.as_array())
                .ok_or_else(|| anyhow::anyhow!("Missing or invalid 'value' field"))?;
            
            // Convert byte arrays back to strings
            let key_bytes: Result<Vec<u8>> = key_array.iter()
                .map(|v| v.as_u64()
                    .ok_or_else(|| anyhow::anyhow!("Invalid byte value in key"))
                    .map(|n| n as u8))
                .collect();
            
            let value_bytes: Result<Vec<u8>> = value_array.iter()
                .map(|v| v.as_u64()
                    .ok_or_else(|| anyhow::anyhow!("Invalid byte value in value"))
                    .map(|n| n as u8))
                .collect();
            
            let key = String::from_utf8(key_bytes?)
                .map_err(|e| anyhow::anyhow!("Failed to convert key bytes to string: {}", e))?;
            
            let value = String::from_utf8(value_bytes?)
                .map_err(|e| anyhow::anyhow!("Failed to convert value bytes to string: {}", e))?;
            
            attributes.insert(key, value);
        }
        
        debug!("Converted {} KV pairs back to attributes HashMap", attributes.len());
        Ok(attributes)
    }
}
