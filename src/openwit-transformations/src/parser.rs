//! SQL Parser using sqlparser-rs

use sqlparser::ast::*;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::error::QueryError;

/// Parse SQL string into AST
pub fn parse_sql(sql: &str) -> Result<Statement, QueryError> {
    let dialect = GenericDialect {};
    let ast = Parser::parse_sql(&dialect, sql)?;
    
    if ast.len() != 1 {
        return Err(QueryError::SyntaxError(
            "Only single statements are supported".to_string()
        ));
    }
    
    Ok(ast.into_iter().next().unwrap())
}

/// Extract table name from FROM clause
pub fn extract_table_name(from: &[TableWithJoins]) -> Result<String, QueryError> {
    if from.len() != 1 {
        return Err(QueryError::JoinsNotSupported);
    }
    
    match &from[0].relation {
        TableFactor::Table { name, .. } => {
            let table_name = name.0.iter()
                .map(|ident| ident.value.clone())
                .collect::<Vec<_>>()
                .join(".");
            
            // Validate table name
            match table_name.as_str() {
                "logs" | "traces" | "metrics" => Ok(table_name),
                _ => Err(QueryError::UnknownTable(table_name)),
            }
        }
        _ => Err(QueryError::UnsupportedClause("Complex FROM clauses not supported".to_string())),
    }
}

/// Check if expression contains aggregates
pub fn contains_aggregate(expr: &Expr) -> bool {
    match expr {
        Expr::Function(func) => {
            let name = func.name.to_string().to_uppercase();
            matches!(name.as_str(), "COUNT" | "SUM" | "AVG" | "MIN" | "MAX" | "STDDEV" | "VARIANCE")
        }
        Expr::BinaryOp { left, right, .. } => {
            contains_aggregate(left) || contains_aggregate(right)
        }
        Expr::UnaryOp { expr, .. } => contains_aggregate(expr),
        Expr::Case { operand, conditions, results, else_result } => {
            operand.as_ref().map_or(false, |e| contains_aggregate(e)) ||
            conditions.iter().any(contains_aggregate) ||
            results.iter().any(contains_aggregate) ||
            else_result.as_ref().map_or(false, |e| contains_aggregate(e))
        }
        _ => false,
    }
}

/// Parse time interval expressions
pub fn parse_interval(expr: &Expr) -> Result<String, QueryError> {
    match expr {
        Expr::Interval(interval) => {
            // Convert SQL interval to our format
            let value = interval.value.to_string();
            let unit = format!("{:?}", interval.leading_field).to_lowercase();
            Ok(format!("{} {}", value, unit))
        }
        _ => Err(QueryError::InvalidTimeExpression("Expected INTERVAL".to_string())),
    }
}

/// Check if LIKE pattern should use full-text search
pub fn is_fulltext_pattern(pattern: &str) -> bool {
    pattern.starts_with('%') && pattern.ends_with('%') && pattern.len() > 2
}

/// Extract column name from expression
pub fn extract_column_name(expr: &Expr) -> Result<String, QueryError> {
    match expr {
        Expr::Identifier(ident) => Ok(ident.value.clone()),
        Expr::CompoundIdentifier(idents) => {
            Ok(idents.iter().map(|i| i.value.clone()).collect::<Vec<_>>().join("."))
        }
        _ => Err(QueryError::InvalidAggregation("Expected column name".to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_select() {
        let sql = "SELECT * FROM logs";
        let result = parse_sql(sql);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_with_where() {
        let sql = "SELECT level FROM logs WHERE service = 'auth'";
        let result = parse_sql(sql);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_with_aggregation() {
        let sql = "SELECT level, COUNT(*) FROM logs GROUP BY level";
        let result = parse_sql(sql);
        assert!(result.is_ok());
    }

    #[test]
    fn test_invalid_syntax() {
        let sql = "SELECT FROM WHERE";
        let result = parse_sql(sql);
        assert!(result.is_err());
    }

    #[test]
    fn test_multiple_statements() {
        let sql = "SELECT * FROM logs; SELECT * FROM traces";
        let result = parse_sql(sql);
        assert!(matches!(result, Err(QueryError::SyntaxError(_))));
    }
}