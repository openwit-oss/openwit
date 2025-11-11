use crate::errors::ValidationError;
use crate::schemas::{SchemaDefinition, TypeSpecificSchema};
use crate::validators::{SchemaValidator, CommonValidator};
use serde_json::Value;
use async_trait::async_trait;
use tracing::debug;

pub struct TracesValidator {
    schema: SchemaDefinition,
}

impl TracesValidator {
    pub fn new(schema: SchemaDefinition) -> Self {
        Self { schema }
    }
    
    fn validate_span(&self, span: &Value) -> Result<(), ValidationError> {
        let (span_schema, duration_schema) = match &self.schema.type_specific {
            TypeSpecificSchema::Traces { span, duration } => (span, duration),
            _ => return Err(ValidationError::SchemaValidation(
                "Invalid schema type for traces validator".to_string()
            )),
        };
        
        // Check required fields
        for field in &span_schema.required_fields {
            if span.get(field).is_none() {
                return Err(ValidationError::MissingRequiredField {
                    field: format!("span.{}", field),
                });
            }
        }
        
        // Validate specific fields
        for (field_name, validation) in &span_schema.field_validations {
            if let Some(field_value) = span.get(field_name) {
                CommonValidator::validate_field(
                    &format!("span.{}", field_name),
                    field_value,
                    validation
                )?;
            }
        }
        
        // Validate trace and span IDs
        if let Some(trace_id) = span.get("traceId").and_then(|v| v.as_str()) {
            if trace_id.len() != 32 {
                return Err(ValidationError::InvalidTraceId);
            }
        }
        
        if let Some(span_id) = span.get("spanId").and_then(|v| v.as_str()) {
            if span_id.len() != 16 {
                return Err(ValidationError::InvalidSpanId);
            }
        }
        
        // Validate timestamps and duration
        let start_time = span.get("startTimeUnixNano")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<u64>().ok())
            .or_else(|| span.get("startTimeUnixNano").and_then(|v| v.as_u64()))
            .ok_or_else(|| ValidationError::InvalidTimestamp {
                field: "startTimeUnixNano".to_string(),
                reason: "Invalid or missing timestamp".to_string(),
            })?;
            
        let end_time = span.get("endTimeUnixNano")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<u64>().ok())
            .or_else(|| span.get("endTimeUnixNano").and_then(|v| v.as_u64()))
            .ok_or_else(|| ValidationError::InvalidTimestamp {
                field: "endTimeUnixNano".to_string(),
                reason: "Invalid or missing timestamp".to_string(),
            })?;
            
        // Validate duration
        if end_time < start_time {
            return Err(ValidationError::InvalidTimestamp {
                field: "endTimeUnixNano".to_string(),
                reason: "End time must be after start time".to_string(),
            });
        }
        
        let duration = end_time - start_time;
        if duration > duration_schema.max_duration_nano {
            return Err(ValidationError::SchemaValidation(
                format!("Span duration {} exceeds maximum {}", duration, duration_schema.max_duration_nano)
            ));
        }
        
        Ok(())
    }
}

#[async_trait]
impl SchemaValidator for TracesValidator {
    async fn validate(&self, data: &Value) -> Result<(), ValidationError> {
        debug!("Validating traces data");
        
        // Validate resourceSpans structure
        let resource_spans = data.get("resourceSpans")
            .and_then(|rs| rs.as_array())
            .ok_or_else(|| ValidationError::MissingRequiredField {
                field: "resourceSpans".to_string(),
            })?;
            
        let mut total_spans = 0;
        let mut errors = Vec::new();
        
        for (rs_idx, resource_span) in resource_spans.iter().enumerate() {
            // Validate resource
            if let Some(resource) = resource_span.get("resource") {
                if let Some(attributes) = resource.get("attributes") {
                    if let Err(e) = CommonValidator::validate_attributes(
                        attributes,
                        &self.schema.resource.attributes
                    ) {
                        errors.push(ValidationError::SchemaValidation(
                            format!("resourceSpans[{}].resource: {}", rs_idx, e)
                        ));
                    }
                }
            } else if self.schema.resource.required {
                errors.push(ValidationError::MissingRequiredField {
                    field: format!("resourceSpans[{}].resource", rs_idx),
                });
            }
            
            // Validate scopeSpans
            let scope_spans = resource_span.get("scopeSpans")
                .and_then(|ss| ss.as_array())
                .ok_or_else(|| ValidationError::MissingRequiredField {
                    field: format!("resourceSpans[{}].scopeSpans", rs_idx),
                })?;
                
            for (ss_idx, scope_span) in scope_spans.iter().enumerate() {
                // Validate spans
                let spans = scope_span.get("spans")
                    .and_then(|s| s.as_array())
                    .ok_or_else(|| ValidationError::MissingRequiredField {
                        field: format!("resourceSpans[{}].scopeSpans[{}].spans", rs_idx, ss_idx),
                    })?;
                    
                total_spans += spans.len();
                
                for (s_idx, span) in spans.iter().enumerate() {
                    if let Err(e) = self.validate_span(span) {
                        errors.push(ValidationError::SchemaValidation(
                            format!(
                                "resourceSpans[{}].scopeSpans[{}].spans[{}]: {}",
                                rs_idx, ss_idx, s_idx, e
                            )
                        ));
                    }
                }
            }
        }
        
        // Check batch limits
        if let Some(max_spans) = self.schema.batch.max_spans {
            if total_spans > max_spans {
                errors.push(ValidationError::SchemaValidation(
                    format!("Total spans {} exceeds maximum {}", total_spans, max_spans)
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
        
        debug!("Traces validation successful: {} spans", total_spans);
        Ok(())
    }
    
    fn schema_type(&self) -> &str {
        "traces"
    }
}