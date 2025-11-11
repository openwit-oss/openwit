use crate::errors::ValidationError;
use crate::schemas::{SchemaDefinition, TypeSpecificSchema};
use crate::validators::{SchemaValidator, CommonValidator};
use serde_json::Value;
use async_trait::async_trait;
use tracing::debug;

pub struct MetricsValidator {
    schema: SchemaDefinition,
}

impl MetricsValidator {
    pub fn new(schema: SchemaDefinition) -> Self {
        Self { schema }
    }
    
    fn validate_metric(&self, metric: &Value) -> Result<(), ValidationError> {
        let metric_schema = match &self.schema.type_specific {
            TypeSpecificSchema::Metrics { metric, .. } => metric,
            _ => return Err(ValidationError::SchemaValidation(
                "Invalid schema type for metrics validator".to_string()
            )),
        };
        
        // Check required fields
        for field in &metric_schema.required_fields {
            if metric.get(field).is_none() {
                return Err(ValidationError::MissingRequiredField {
                    field: format!("metric.{}", field),
                });
            }
        }
        
        // Validate specific fields
        for (field_name, validation) in &metric_schema.field_validations {
            if let Some(field_value) = metric.get(field_name) {
                CommonValidator::validate_field(
                    &format!("metric.{}", field_name),
                    field_value,
                    validation
                )?;
            }
        }
        
        // Validate metric type and data points
        let has_gauge = metric.get("gauge").is_some();
        let has_sum = metric.get("sum").is_some();
        let has_histogram = metric.get("histogram").is_some();
        let has_exponential_histogram = metric.get("exponentialHistogram").is_some();
        let has_summary = metric.get("summary").is_some();
        
        let type_count = [has_gauge, has_sum, has_histogram, has_exponential_histogram, has_summary]
            .iter()
            .filter(|&&x| x)
            .count();
            
        if type_count == 0 {
            return Err(ValidationError::SchemaValidation(
                "Metric must have exactly one type (gauge, sum, histogram, exponentialHistogram, or summary)".to_string()
            ));
        }
        
        if type_count > 1 {
            return Err(ValidationError::SchemaValidation(
                "Metric can only have one type".to_string()
            ));
        }
        
        // Validate data points based on type
        if has_gauge {
            self.validate_gauge_data_points(metric.get("gauge").unwrap())?;
        } else if has_sum {
            self.validate_sum_data_points(metric.get("sum").unwrap())?;
        } else if has_histogram {
            self.validate_histogram_data_points(metric.get("histogram").unwrap())?;
        }
        
        Ok(())
    }
    
    fn validate_gauge_data_points(&self, gauge: &Value) -> Result<(), ValidationError> {
        let data_points = gauge.get("dataPoints")
            .and_then(|dp| dp.as_array())
            .ok_or_else(|| ValidationError::MissingRequiredField {
                field: "gauge.dataPoints".to_string(),
            })?;
            
        for (idx, dp) in data_points.iter().enumerate() {
            // Must have either asInt or asDouble
            let has_int = dp.get("asInt").is_some();
            let has_double = dp.get("asDouble").is_some();
            
            if !has_int && !has_double {
                return Err(ValidationError::MissingRequiredField {
                    field: format!("gauge.dataPoints[{}].asInt or asDouble", idx),
                });
            }
            
            // Must have timestamp
            if dp.get("timeUnixNano").is_none() {
                return Err(ValidationError::MissingRequiredField {
                    field: format!("gauge.dataPoints[{}].timeUnixNano", idx),
                });
            }
        }
        
        Ok(())
    }
    
    fn validate_sum_data_points(&self, sum: &Value) -> Result<(), ValidationError> {
        // Check aggregation temporality
        let temporality = sum.get("aggregationTemporality")
            .and_then(|at| at.as_i64())
            .ok_or_else(|| ValidationError::MissingRequiredField {
                field: "sum.aggregationTemporality".to_string(),
            })?;
            
        if temporality < 0 || temporality > 2 {
            return Err(ValidationError::InvalidEnumValue {
                field: "sum.aggregationTemporality".to_string(),
                value: temporality.to_string(),
            });
        }
        
        // Check isMonotonic
        if sum.get("isMonotonic").is_none() {
            return Err(ValidationError::MissingRequiredField {
                field: "sum.isMonotonic".to_string(),
            });
        }
        
        // Validate data points
        let data_points = sum.get("dataPoints")
            .and_then(|dp| dp.as_array())
            .ok_or_else(|| ValidationError::MissingRequiredField {
                field: "sum.dataPoints".to_string(),
            })?;
            
        for (idx, dp) in data_points.iter().enumerate() {
            // Must have either asInt or asDouble
            let has_int = dp.get("asInt").is_some();
            let has_double = dp.get("asDouble").is_some();
            
            if !has_int && !has_double {
                return Err(ValidationError::MissingRequiredField {
                    field: format!("sum.dataPoints[{}].asInt or asDouble", idx),
                });
            }
            
            // Must have timestamp
            if dp.get("timeUnixNano").is_none() {
                return Err(ValidationError::MissingRequiredField {
                    field: format!("sum.dataPoints[{}].timeUnixNano", idx),
                });
            }
        }
        
        Ok(())
    }
    
    fn validate_histogram_data_points(&self, histogram: &Value) -> Result<(), ValidationError> {
        // Check aggregation temporality
        let temporality = histogram.get("aggregationTemporality")
            .and_then(|at| at.as_i64())
            .ok_or_else(|| ValidationError::MissingRequiredField {
                field: "histogram.aggregationTemporality".to_string(),
            })?;
            
        if temporality < 0 || temporality > 2 {
            return Err(ValidationError::InvalidEnumValue {
                field: "histogram.aggregationTemporality".to_string(),
                value: temporality.to_string(),
            });
        }
        
        // Validate data points
        let data_points = histogram.get("dataPoints")
            .and_then(|dp| dp.as_array())
            .ok_or_else(|| ValidationError::MissingRequiredField {
                field: "histogram.dataPoints".to_string(),
            })?;
            
        for (idx, dp) in data_points.iter().enumerate() {
            // Required fields
            for field in ["timeUnixNano", "count", "sum", "bucketCounts", "explicitBounds"] {
                if dp.get(field).is_none() {
                    return Err(ValidationError::MissingRequiredField {
                        field: format!("histogram.dataPoints[{}].{}", idx, field),
                    });
                }
            }
            
            // Validate bucket counts match bounds + 1
            let bucket_counts = dp.get("bucketCounts")
                .and_then(|bc| bc.as_array())
                .ok_or_else(|| ValidationError::SchemaValidation(
                    format!("histogram.dataPoints[{}].bucketCounts must be an array", idx)
                ))?;
                
            let explicit_bounds = dp.get("explicitBounds")
                .and_then(|eb| eb.as_array())
                .ok_or_else(|| ValidationError::SchemaValidation(
                    format!("histogram.dataPoints[{}].explicitBounds must be an array", idx)
                ))?;
                
            if bucket_counts.len() != explicit_bounds.len() + 1 {
                return Err(ValidationError::SchemaValidation(
                    format!(
                        "histogram.dataPoints[{}]: bucketCounts length {} must be explicitBounds length {} + 1",
                        idx, bucket_counts.len(), explicit_bounds.len()
                    )
                ));
            }
        }
        
        Ok(())
    }
}

#[async_trait]
impl SchemaValidator for MetricsValidator {
    async fn validate(&self, data: &Value) -> Result<(), ValidationError> {
        debug!("Validating metrics data");
        
        // Validate resourceMetrics structure
        let resource_metrics = data.get("resourceMetrics")
            .and_then(|rm| rm.as_array())
            .ok_or_else(|| ValidationError::MissingRequiredField {
                field: "resourceMetrics".to_string(),
            })?;
            
        let mut total_metrics = 0;
        let mut total_data_points = 0;
        let mut errors = Vec::new();
        
        for (rm_idx, resource_metric) in resource_metrics.iter().enumerate() {
            // Validate resource
            if let Some(resource) = resource_metric.get("resource") {
                if let Some(attributes) = resource.get("attributes") {
                    if let Err(e) = CommonValidator::validate_attributes(
                        attributes,
                        &self.schema.resource.attributes
                    ) {
                        errors.push(ValidationError::SchemaValidation(
                            format!("resourceMetrics[{}].resource: {}", rm_idx, e)
                        ));
                    }
                }
            } else if self.schema.resource.required {
                errors.push(ValidationError::MissingRequiredField {
                    field: format!("resourceMetrics[{}].resource", rm_idx),
                });
            }
            
            // Validate scopeMetrics
            let scope_metrics = resource_metric.get("scopeMetrics")
                .and_then(|sm| sm.as_array())
                .ok_or_else(|| ValidationError::MissingRequiredField {
                    field: format!("resourceMetrics[{}].scopeMetrics", rm_idx),
                })?;
                
            for (sm_idx, scope_metric) in scope_metrics.iter().enumerate() {
                // Validate metrics
                let metrics = scope_metric.get("metrics")
                    .and_then(|m| m.as_array())
                    .ok_or_else(|| ValidationError::MissingRequiredField {
                        field: format!("resourceMetrics[{}].scopeMetrics[{}].metrics", rm_idx, sm_idx),
                    })?;
                    
                total_metrics += metrics.len();
                
                for (m_idx, metric) in metrics.iter().enumerate() {
                    if let Err(e) = self.validate_metric(metric) {
                        errors.push(ValidationError::SchemaValidation(
                            format!(
                                "resourceMetrics[{}].scopeMetrics[{}].metrics[{}]: {}",
                                rm_idx, sm_idx, m_idx, e
                            )
                        ));
                    }
                    
                    // Count data points
                    if let Some(gauge) = metric.get("gauge") {
                        if let Some(dps) = gauge.get("dataPoints").and_then(|dp| dp.as_array()) {
                            total_data_points += dps.len();
                        }
                    } else if let Some(sum) = metric.get("sum") {
                        if let Some(dps) = sum.get("dataPoints").and_then(|dp| dp.as_array()) {
                            total_data_points += dps.len();
                        }
                    } else if let Some(histogram) = metric.get("histogram") {
                        if let Some(dps) = histogram.get("dataPoints").and_then(|dp| dp.as_array()) {
                            total_data_points += dps.len();
                        }
                    }
                }
            }
        }
        
        // Check batch limits
        if let Some(max_metrics) = self.schema.batch.max_metrics {
            if total_metrics > max_metrics {
                errors.push(ValidationError::SchemaValidation(
                    format!("Total metrics {} exceeds maximum {}", total_metrics, max_metrics)
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
        
        debug!("Metrics validation successful: {} metrics, {} data points", total_metrics, total_data_points);
        Ok(())
    }
    
    fn schema_type(&self) -> &str {
        "metrics"
    }
}