use crate::errors::ValidationError;
use crate::schemas::{AttributeSchema, FieldValidation};
use serde_json::Value;
use regex::Regex;
use std::collections::HashMap;
use lazy_static::lazy_static;

lazy_static! {
    static ref HEX_PATTERN: Regex = Regex::new(r"^[0-9a-fA-F]+$").unwrap();
}

pub struct CommonValidator;

impl CommonValidator {
    /// Validate resource attributes
    pub fn validate_attributes(
        attributes: &Value,
        schema: &AttributeSchema,
    ) -> Result<(), ValidationError> {
        let attrs = attributes.as_array()
            .ok_or_else(|| ValidationError::InvalidFieldType {
                field: "attributes".to_string(),
                expected: "array".to_string(),
                actual: format!("{:?}", attributes),
            })?;
            
        // Check max attributes
        if attrs.len() > schema.max_attributes {
            return Err(ValidationError::TooManyAttributes {
                max: schema.max_attributes,
            });
        }
        
        // Collect attribute keys
        let mut found_keys = HashMap::new();
        
        for attr in attrs {
            let key = attr.get("key")
                .and_then(|k| k.as_str())
                .ok_or_else(|| ValidationError::MissingRequiredField {
                    field: "attribute.key".to_string(),
                })?;
                
            // Check for duplicate keys
            if found_keys.contains_key(key) {
                return Err(ValidationError::SchemaValidation(
                    format!("Duplicate attribute key: {}", key)
                ));
            }
            
            found_keys.insert(key.to_string(), attr);
        }
        
        // Validate required keys
        for required_key in &schema.required_keys {
            if !found_keys.contains_key(required_key) {
                return Err(ValidationError::MissingRequiredField {
                    field: format!("attribute.{}", required_key),
                });
            }
        }
        
        // Validate custom keys
        if !schema.custom_keys_allowed {
            for key in found_keys.keys() {
                if !schema.required_keys.contains(key) && !schema.optional_keys.contains(key) {
                    return Err(ValidationError::SchemaValidation(
                        format!("Unknown attribute key: {}. Custom keys not allowed.", key)
                    ));
                }
            }
        }
        
        Ok(())
    }
    
    /// Validate a field against its schema
    pub fn validate_field(
        field_name: &str,
        field_value: &Value,
        validation: &FieldValidation,
    ) -> Result<(), ValidationError> {
        match validation.field_type.as_str() {
            "string" => Self::validate_string_field(field_name, field_value, validation),
            "uint64" => Self::validate_uint64_field(field_name, field_value, validation),
            "int32" => Self::validate_int32_field(field_name, field_value, validation),
            "bytes" => Self::validate_bytes_field(field_name, field_value, validation),
            "enum" => Self::validate_enum_field(field_name, field_value, validation),
            "any_value" => Ok(()), // Any value is accepted
            _ => Err(ValidationError::SchemaValidation(
                format!("Unknown field type: {}", validation.field_type)
            )),
        }
    }
    
    fn validate_string_field(
        field_name: &str,
        value: &Value,
        validation: &FieldValidation,
    ) -> Result<(), ValidationError> {
        let string_value = value.as_str()
            .ok_or_else(|| ValidationError::InvalidFieldType {
                field: field_name.to_string(),
                expected: "string".to_string(),
                actual: format!("{:?}", value),
            })?;
            
        // Check max length
        if let Some(max_length) = validation.max_length {
            if string_value.len() > max_length {
                return Err(ValidationError::FieldTooLong {
                    field: field_name.to_string(),
                    max_length,
                });
            }
        }
        
        // Check pattern
        if let Some(pattern) = &validation.pattern {
            let regex = Regex::new(pattern)
                .map_err(|_| ValidationError::SchemaValidation(
                    format!("Invalid regex pattern: {}", pattern)
                ))?;
                
            if !regex.is_match(string_value) {
                return Err(ValidationError::PatternMismatch {
                    field: field_name.to_string(),
                    pattern: pattern.clone(),
                });
            }
        }
        
        Ok(())
    }
    
    fn validate_uint64_field(
        field_name: &str,
        value: &Value,
        validation: &FieldValidation,
    ) -> Result<(), ValidationError> {
        let num_value = if let Some(s) = value.as_str() {
            // Try to parse string as u64
            s.parse::<u64>()
                .map_err(|_| ValidationError::InvalidFieldType {
                    field: field_name.to_string(),
                    expected: "uint64".to_string(),
                    actual: format!("{:?}", value),
                })?
        } else if let Some(n) = value.as_u64() {
            n
        } else {
            return Err(ValidationError::InvalidFieldType {
                field: field_name.to_string(),
                expected: "uint64".to_string(),
                actual: format!("{:?}", value),
            });
        };
        
        // Check min/max
        if let Some(min) = validation.min {
            if (num_value as i64) < min {
                return Err(ValidationError::SchemaValidation(
                    format!("{} value {} is less than minimum {}", field_name, num_value, min)
                ));
            }
        }
        
        if let Some(max) = validation.max {
            if (num_value as i64) > max {
                return Err(ValidationError::SchemaValidation(
                    format!("{} value {} exceeds maximum {}", field_name, num_value, max)
                ));
            }
        }
        
        Ok(())
    }
    
    fn validate_int32_field(
        field_name: &str,
        value: &Value,
        validation: &FieldValidation,
    ) -> Result<(), ValidationError> {
        let num_value = value.as_i64()
            .ok_or_else(|| ValidationError::InvalidFieldType {
                field: field_name.to_string(),
                expected: "int32".to_string(),
                actual: format!("{:?}", value),
            })?;
            
        // Check if fits in i32
        if num_value < i32::MIN as i64 || num_value > i32::MAX as i64 {
            return Err(ValidationError::SchemaValidation(
                format!("{} value {} doesn't fit in int32", field_name, num_value)
            ));
        }
        
        // Check min/max
        if let Some(min) = validation.min {
            if num_value < min {
                return Err(ValidationError::SchemaValidation(
                    format!("{} value {} is less than minimum {}", field_name, num_value, min)
                ));
            }
        }
        
        if let Some(max) = validation.max {
            if num_value > max {
                return Err(ValidationError::SchemaValidation(
                    format!("{} value {} exceeds maximum {}", field_name, num_value, max)
                ));
            }
        }
        
        Ok(())
    }
    
    fn validate_bytes_field(
        field_name: &str,
        value: &Value,
        validation: &FieldValidation,
    ) -> Result<(), ValidationError> {
        let string_value = value.as_str()
            .ok_or_else(|| ValidationError::InvalidFieldType {
                field: field_name.to_string(),
                expected: "string (hex)".to_string(),
                actual: format!("{:?}", value),
            })?;
            
        // Check if hex format
        if validation.format.as_deref() == Some("hex") {
            if !HEX_PATTERN.is_match(string_value) {
                return Err(ValidationError::SchemaValidation(
                    format!("{} must be hexadecimal", field_name)
                ));
            }
            
            // Check length (hex string is 2x the byte length)
            if let Some(expected_length) = validation.length {
                let actual_length = string_value.len() / 2;
                if actual_length != expected_length {
                    return Err(ValidationError::SchemaValidation(
                        format!(
                            "{} must be {} bytes ({} hex chars), got {} bytes",
                            field_name,
                            expected_length,
                            expected_length * 2,
                            actual_length
                        )
                    ));
                }
            }
        }
        
        Ok(())
    }
    
    fn validate_enum_field(
        field_name: &str,
        value: &Value,
        validation: &FieldValidation,
    ) -> Result<(), ValidationError> {
        if let Some(values) = &validation.values {
            if !values.contains(value) {
                return Err(ValidationError::InvalidEnumValue {
                    field: field_name.to_string(),
                    value: format!("{:?}", value),
                });
            }
        }
        
        if let Some(enum_values) = &validation.enum_values {
            let string_value = value.as_str()
                .ok_or_else(|| ValidationError::InvalidFieldType {
                    field: field_name.to_string(),
                    expected: "string".to_string(),
                    actual: format!("{:?}", value),
                })?;
                
            if !enum_values.contains(&string_value.to_string()) {
                return Err(ValidationError::InvalidEnumValue {
                    field: field_name.to_string(),
                    value: string_value.to_string(),
                });
            }
        }
        
        Ok(())
    }
    
    /// Get current timestamp in nanoseconds
    pub fn current_time_nanos() -> u64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64
    }
}