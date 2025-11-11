//! Convert logical plan to Tantivy query format

use crate::logical_plan::{Filter, LogicalQuery, Value};
use crate::error::QueryError;

/// Convert logical query to Tantivy query string
pub fn to_tantivy_query(query: &LogicalQuery) -> Result<String, QueryError> {
    if query.filters.is_empty() {
        // Match all documents if no filters
        return Ok("*".to_string());
    }
    
    // Convert all filters and combine with AND
    let filter_queries: Vec<String> = query.filters
        .iter()
        .map(filter_to_tantivy)
        .collect::<Result<Vec<_>, _>>()?;
    
    // If multiple filters at top level, combine with AND
    if filter_queries.len() == 1 {
        Ok(filter_queries[0].clone())
    } else {
        Ok(format!("({})", filter_queries.join(" AND ")))
    }
}

/// Convert a filter to Tantivy query string
fn filter_to_tantivy(filter: &Filter) -> Result<String, QueryError> {
    match filter {
        Filter::Equals(field, value) => {
            Ok(format!("{}:{}", field, value_to_tantivy(value)?))
        }
        
        Filter::NotEquals(field, value) => {
            Ok(format!("NOT {}:{}", field, value_to_tantivy(value)?))
        }
        
        Filter::GreaterThan(field, value) => {
            Ok(format!("{}:>{}", field, value_to_tantivy(value)?))
        }
        
        Filter::GreaterThanOrEqual(field, value) => {
            Ok(format!("{}:>={}", field, value_to_tantivy(value)?))
        }
        
        Filter::LessThan(field, value) => {
            Ok(format!("{}:<{}", field, value_to_tantivy(value)?))
        }
        
        Filter::LessThanOrEqual(field, value) => {
            Ok(format!("{}:<={}", field, value_to_tantivy(value)?))
        }
        
        Filter::In(field, values) => {
            let value_strings: Vec<String> = values
                .iter()
                .map(value_to_tantivy)
                .collect::<Result<Vec<_>, _>>()?;
            Ok(format!("{}:({})", field, value_strings.join(" OR ")))
        }
        
        Filter::NotIn(field, values) => {
            let value_strings: Vec<String> = values
                .iter()
                .map(value_to_tantivy)
                .collect::<Result<Vec<_>, _>>()?;
            Ok(format!("NOT {}:({})", field, value_strings.join(" OR ")))
        }
        
        Filter::Between { field, low, high } => {
            Ok(format!("{}:[{} TO {}]", 
                field, 
                value_to_tantivy(low)?, 
                value_to_tantivy(high)?
            ))
        }
        
        Filter::Like(field, pattern) => {
            // Convert SQL LIKE to Tantivy wildcard
            let tantivy_pattern = pattern
                .replace('%', "*")
                .replace('_', "?");
            Ok(format!("{}:{}", field, tantivy_pattern))
        }
        
        Filter::FullTextContains(field, text) => {
            // Full-text search
            Ok(format!("{}:{}", field, escape_tantivy_value(text)))
        }
        
        Filter::IsNull(field) => {
            // Tantivy doesn't have direct NULL support, use special value
            Ok(format!("NOT {}:*", field))
        }
        
        Filter::IsNotNull(field) => {
            Ok(format!("{}:*", field))
        }
        
        Filter::And(filters) => {
            let sub_queries: Vec<String> = filters
                .iter()
                .map(filter_to_tantivy)
                .collect::<Result<Vec<_>, _>>()?;
            
            // Always use parentheses for AND groups to preserve precedence
            Ok(format!("({})", sub_queries.join(" AND ")))
        }
        
        Filter::Or(filters) => {
            let sub_queries: Vec<String> = filters
                .iter()
                .map(filter_to_tantivy)
                .collect::<Result<Vec<_>, _>>()?;
            
            // Always use parentheses for OR groups to preserve precedence
            Ok(format!("({})", sub_queries.join(" OR ")))
        }
        
        Filter::Not(inner) => {
            Ok(format!("NOT ({})", filter_to_tantivy(inner)?))
        }
    }
}

/// Convert value to Tantivy format
fn value_to_tantivy(value: &Value) -> Result<String, QueryError> {
    match value {
        Value::String(s) => Ok(escape_tantivy_value(s)),
        Value::Integer(i) => Ok(i.to_string()),
        Value::Float(f) => Ok(f.to_string()),
        Value::Boolean(b) => Ok(b.to_string()),
        Value::Timestamp(ts) => Ok(ts.to_rfc3339()),
        Value::RelativeTime(rt) => Ok(rt.clone()), // Keep as-is, handled by indexer
        Value::Null => Ok("NULL".to_string()),
    }
}

/// Escape special characters in Tantivy values
fn escape_tantivy_value(value: &str) -> String {
    // Check if value needs quotes (contains spaces or special chars)
    if value.contains(' ') || value.chars().any(|c| "+-&|!(){}[]^\"~*?:\\/".contains(c)) {
        format!("\"{}\"", value.replace('\"', "\\\""))
    } else {
        value.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logical_plan::{LogicalQuery, UseIndex};

    #[test]
    fn test_simple_equality() {
        let query = LogicalQuery {
            source: "logs".to_string(),
            projection: vec![],
            filters: vec![
                Filter::Equals("service".to_string(), Value::String("auth".to_string()))
            ],
            group_by: vec![],
            order_by: vec![],
            limit: None,
            offset: None,
            index_hints: vec![],
        };
        
        let tantivy = to_tantivy_query(&query).unwrap();
        assert_eq!(tantivy, "service:auth");
    }
    
    #[test]
    fn test_complex_precedence() {
        // service = 'auth' OR (service = 'search' AND level = 'ERROR')
        let query = LogicalQuery {
            source: "logs".to_string(),
            projection: vec![],
            filters: vec![
                Filter::Or(vec![
                    Filter::Equals("service".to_string(), Value::String("auth".to_string())),
                    Filter::And(vec![
                        Filter::Equals("service".to_string(), Value::String("search".to_string())),
                        Filter::Equals("level".to_string(), Value::String("ERROR".to_string())),
                    ])
                ])
            ],
            group_by: vec![],
            order_by: vec![],
            limit: None,
            offset: None,
            index_hints: vec![],
        };
        
        let tantivy = to_tantivy_query(&query).unwrap();
        assert_eq!(tantivy, "(service:auth OR (service:search AND level:ERROR))");
    }
    
    #[test]
    fn test_different_precedence() {
        // (service = 'auth' OR service = 'search') AND level = 'ERROR'
        let query = LogicalQuery {
            source: "logs".to_string(),
            projection: vec![],
            filters: vec![
                Filter::And(vec![
                    Filter::Or(vec![
                        Filter::Equals("service".to_string(), Value::String("auth".to_string())),
                        Filter::Equals("service".to_string(), Value::String("search".to_string())),
                    ]),
                    Filter::Equals("level".to_string(), Value::String("ERROR".to_string())),
                ])
            ],
            group_by: vec![],
            order_by: vec![],
            limit: None,
            offset: None,
            index_hints: vec![],
        };
        
        let tantivy = to_tantivy_query(&query).unwrap();
        assert_eq!(tantivy, "((service:auth OR service:search) AND level:ERROR)");
    }
    
    #[test]
    fn test_in_clause() {
        let query = LogicalQuery {
            source: "logs".to_string(),
            projection: vec![],
            filters: vec![
                Filter::In("level".to_string(), vec![
                    Value::String("ERROR".to_string()),
                    Value::String("WARN".to_string()),
                ])
            ],
            group_by: vec![],
            order_by: vec![],
            limit: None,
            offset: None,
            index_hints: vec![],
        };
        
        let tantivy = to_tantivy_query(&query).unwrap();
        assert_eq!(tantivy, "level:(ERROR OR WARN)");
    }
    
    #[test]
    fn test_full_text_search() {
        let query = LogicalQuery {
            source: "logs".to_string(),
            projection: vec![],
            filters: vec![
                Filter::FullTextContains("message".to_string(), "timeout error".to_string())
            ],
            group_by: vec![],
            order_by: vec![],
            limit: None,
            offset: None,
            index_hints: vec![],
        };
        
        let tantivy = to_tantivy_query(&query).unwrap();
        assert_eq!(tantivy, "message:\"timeout error\"");
    }
    
    #[test]
    fn test_range_query() {
        let query = LogicalQuery {
            source: "logs".to_string(),
            projection: vec![],
            filters: vec![
                Filter::Between {
                    field: "status_code".to_string(),
                    low: Value::Integer(400),
                    high: Value::Integer(599),
                }
            ],
            group_by: vec![],
            order_by: vec![],
            limit: None,
            offset: None,
            index_hints: vec![],
        };
        
        let tantivy = to_tantivy_query(&query).unwrap();
        assert_eq!(tantivy, "status_code:[400 TO 599]");
    }
}