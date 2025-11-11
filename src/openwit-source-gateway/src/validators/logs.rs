use crate::errors::ValidationError;
use crate::schemas::{SchemaDefinition, TypeSpecificSchema};
use crate::validators::{SchemaValidator, CommonValidator};
use serde_json::Value;
use async_trait::async_trait;
use tracing::{debug, warn};

pub struct LogsValidator {
    schema: SchemaDefinition,
}

impl LogsValidator {
    pub fn new(schema: SchemaDefinition) -> Self {
        Self { schema }
    }
    
    fn validate_log_record(&self, record: &Value) -> Result<(), ValidationError> {
        let log_schema = match &self.schema.type_specific {
            TypeSpecificSchema::Logs { log_record } => log_record,
            _ => return Err(ValidationError::SchemaValidation(
                "Invalid schema type for logs validator".to_string()
            )),
        };
        
        // Check required fields
        for field in &log_schema.required_fields {
            if record.get(field).is_none() {
                return Err(ValidationError::MissingRequiredField {
                    field: format!("logRecord.{}", field),
                });
            }
        }
        
        // Validate specific fields
        for (field_name, validation) in &log_schema.field_validations {
            if let Some(field_value) = record.get(field_name) {
                CommonValidator::validate_field(
                    &format!("logRecord.{}", field_name),
                    field_value,
                    validation
                )?;
            }
        }
        
        // Validate severity
        if let Some(severity_text) = record.get("severityText").and_then(|v| v.as_str()) {
            let valid_severities = ["TRACE", "DEBUG", "INFO", "WARN", "ERROR", "FATAL"];
            if !valid_severities.iter().any(|&s| s.eq_ignore_ascii_case(severity_text)) {
                return Err(ValidationError::InvalidEnumValue {
                    field: "severityText".to_string(),
                    value: severity_text.to_string(),
                });
            }
        }
        
        // Validate timestamp
        if let Some(time_value) = record.get("timeUnixNano") {
            let timestamp = if let Some(s) = time_value.as_str() {
                s.parse::<u64>().map_err(|_| ValidationError::InvalidTimestamp {
                    field: "timeUnixNano".to_string(),
                    reason: "Invalid number format".to_string(),
                })?
            } else if let Some(n) = time_value.as_u64() {
                n
            } else {
                return Err(ValidationError::InvalidTimestamp {
                    field: "timeUnixNano".to_string(),
                    reason: "Must be a number or string".to_string(),
                });
            };
            
            // Check if timestamp is reasonable (not too far in future)
            let current_time = CommonValidator::current_time_nanos();
            let max_future = 24 * 60 * 60 * 1_000_000_000; // 24 hours in nanos
            
            if timestamp > current_time + max_future {
                return Err(ValidationError::InvalidTimestamp {
                    field: "timeUnixNano".to_string(),
                    reason: "Timestamp too far in the future".to_string(),
                });
            }
        }
        
        Ok(())
    }
}

#[async_trait]
impl SchemaValidator for LogsValidator {
    async fn validate(&self, data: &Value) -> Result<(), ValidationError> {
        debug!("Validating logs data");
        
        // Validate resourceLogs structure
        let resource_logs = data.get("resourceLogs")
            .and_then(|rl| rl.as_array())
            .ok_or_else(|| ValidationError::MissingRequiredField {
                field: "resourceLogs".to_string(),
            })?;
            
        let mut total_log_records = 0;
        let mut errors = Vec::new();
        
        for (rl_idx, resource_log) in resource_logs.iter().enumerate() {
            // Validate resource
            if let Some(resource) = resource_log.get("resource") {
                if let Some(attributes) = resource.get("attributes") {
                    if let Err(e) = CommonValidator::validate_attributes(
                        attributes,
                        &self.schema.resource.attributes
                    ) {
                        errors.push(ValidationError::SchemaValidation(
                            format!("resourceLogs[{}].resource: {}", rl_idx, e)
                        ));
                    }
                }
            } else if self.schema.resource.required {
                errors.push(ValidationError::MissingRequiredField {
                    field: format!("resourceLogs[{}].resource", rl_idx),
                });
            }
            
            // Validate scopeLogs
            let scope_logs = resource_log.get("scopeLogs")
                .and_then(|sl| sl.as_array())
                .ok_or_else(|| ValidationError::MissingRequiredField {
                    field: format!("resourceLogs[{}].scopeLogs", rl_idx),
                })?;
                
            for (sl_idx, scope_log) in scope_logs.iter().enumerate() {
                // Validate scope
                if self.schema.scope.required {
                    if scope_log.get("scope").is_none() {
                        errors.push(ValidationError::MissingRequiredField {
                            field: format!("resourceLogs[{}].scopeLogs[{}].scope", rl_idx, sl_idx),
                        });
                    }
                }
                
                // Validate log records
                let log_records = scope_log.get("logRecords")
                    .and_then(|lr| lr.as_array())
                    .ok_or_else(|| ValidationError::MissingRequiredField {
                        field: format!("resourceLogs[{}].scopeLogs[{}].logRecords", rl_idx, sl_idx),
                    })?;
                    
                total_log_records += log_records.len();
                
                for (lr_idx, log_record) in log_records.iter().enumerate() {
                    if let Err(e) = self.validate_log_record(log_record) {
                        errors.push(ValidationError::SchemaValidation(
                            format!(
                                "resourceLogs[{}].scopeLogs[{}].logRecords[{}]: {}",
                                rl_idx, sl_idx, lr_idx, e
                            )
                        ));
                    }
                }
            }
        }
        
        // Check batch limits
        if let Some(max_records) = self.schema.batch.max_log_records {
            if total_log_records > max_records {
                errors.push(ValidationError::SchemaValidation(
                    format!("Total log records {} exceeds maximum {}", total_log_records, max_records)
                ));
            }
        }
        
        // Return errors if any
        if !errors.is_empty() {
            if errors.len() == 1 {
                return Err(errors.into_iter().next().unwrap());
            } else {
                return Err(ValidationError::Multiple(errors));
            }
        }
        
        debug!("Logs validation successful: {} records", total_log_records);
        Ok(())
    }
    
    fn schema_type(&self) -> &str {
        "logs"
    }
    
    async fn preprocess(&self, data: &mut Value) -> Result<(), ValidationError> {
        // Add timestamp if missing
        if let Some(resource_logs) = data.get_mut("resourceLogs").and_then(|rl| rl.as_array_mut()) {
            for resource_log in resource_logs {
                if let Some(scope_logs) = resource_log.get_mut("scopeLogs").and_then(|sl| sl.as_array_mut()) {
                    for scope_log in scope_logs {
                        if let Some(log_records) = scope_log.get_mut("logRecords").and_then(|lr| lr.as_array_mut()) {
                            for log_record in log_records {
                                // Add timestamp if missing
                                if !log_record.get("timeUnixNano").is_some() {
                                    let current_time = CommonValidator::current_time_nanos().to_string();
                                    log_record["timeUnixNano"] = Value::String(current_time);
                                    warn!("Added missing timestamp to log record");
                                }
                                
                                // Normalize severity text
                                if let Some(severity) = log_record.get_mut("severityText").and_then(|s| s.as_str()) {
                                    let severity_upper = severity.to_uppercase();
                                    let normalized = match severity_upper.as_str() {
                                        "WARNING" => "WARN",
                                        "CRITICAL" => "FATAL",
                                        other => other,
                                    };
                                    log_record["severityText"] = Value::String(normalized.to_string());
                                }
                            }
                        }
                    }
                }
            }
        }
        
        Ok(())
    }
}