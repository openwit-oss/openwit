//! OpenWit SQL Transformations - SQL Parser and Query Planner

pub mod error;
pub mod parser;
pub mod planner;
pub mod logical_plan;
pub mod api;
pub mod tantivy_converter;
pub mod transformations_main;

// Re-export commonly used items
pub use parser::parse_sql;
pub use planner::{build_logical_plan, validate_plan, add_index_hints};
pub use logical_plan::{LogicalQuery, Predicate};
pub use tantivy_converter::to_tantivy_query;

// Placeholder functions that need implementation
pub fn to_predicates(_ast: &sqlparser::ast::Statement) -> Vec<logical_plan::Predicate> {
    vec![]
}

pub fn columns_for_pruning(_ast: &sqlparser::ast::Statement) -> Vec<String> {
    vec![]
}

pub fn to_datafusion_plan(_query: &LogicalQuery) -> Result<String, error::QueryError> {
    Ok("TODO: DataFusion plan".to_string())
}

use serde::{Deserialize, Serialize};

/// Main entry point for SQL transformation
pub async fn transform_sql(sql: &str) -> Result<logical_plan::LogicalQuery, error::QueryError> {
    // Parse SQL to AST
    let ast = parser::parse_sql(sql)?;
    
    // Convert AST to logical plan
    let logical_query = planner::build_logical_plan(ast)?;
    
    // Validate the plan
    planner::validate_plan(&logical_query)?;
    
    // Add index hints
    let optimized = planner::add_index_hints(logical_query)?;
    
    Ok(optimized)
}

/// Supported SQL features
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqlFeatures {
    pub select: bool,
    pub where_clause: bool,
    pub group_by: bool,
    pub order_by: bool,
    pub limit: bool,
    pub aggregates: Vec<String>,
    pub functions: Vec<String>,
    pub fulltext_search: bool,
    pub time_ranges: bool,
}

impl Default for SqlFeatures {
    fn default() -> Self {
        Self {
            select: true,
            where_clause: true,
            group_by: true,
            order_by: true,
            limit: true,
            aggregates: vec![
                "COUNT".to_string(),
                "SUM".to_string(),
                "AVG".to_string(),
                "MIN".to_string(),
                "MAX".to_string(),
            ],
            functions: vec![
                "NOW".to_string(),
                "DATE_TRUNC".to_string(),
                "LOWER".to_string(),
                "UPPER".to_string(),
            ],
            fulltext_search: true,
            time_ranges: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_basic_transformation() {
        let sql = "SELECT level, COUNT(*) FROM logs WHERE service = 'auth' GROUP BY level";
        let result = transform_sql(sql).await;
        assert!(result.is_ok());
    }
}