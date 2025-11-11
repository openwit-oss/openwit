//! Error types for SQL parsing and planning

use thiserror::Error;

#[derive(Error, Debug)]
pub enum QueryError {
    #[error("Parse error: {0}")]
    ParseError(String),
    
    #[error("Unknown field: {0}")]
    UnknownField(String),
    
    #[error("Unknown table: {0}")]
    UnknownTable(String),
    
    #[error("Invalid function usage: {function} - {reason}")]
    InvalidFunctionUsage { function: String, reason: String },
    
    #[error("Type mismatch: expected {expected}, found {found}")]
    TypeMismatch { expected: String, found: String },
    
    #[error("Unsupported SQL clause: {0}")]
    UnsupportedClause(String),
    
    #[error("Invalid time expression: {0}")]
    InvalidTimeExpression(String),
    
    #[error("Grouping error: {0}")]
    GroupingError(String),
    
    #[error("Ambiguous column: {0}")]
    AmbiguousColumn(String),
    
    #[error("Invalid aggregation: {0}")]
    InvalidAggregation(String),
    
    #[error("Syntax error: {0}")]
    SyntaxError(String),
    
    #[error("Limit/Offset error: {0}")]
    LimitOffsetError(String),
    
    #[error("Subqueries not supported")]
    SubqueriesNotSupported,
    
    #[error("Joins not supported")]
    JoinsNotSupported,
    
    #[error("Internal error: {0}")]
    InternalError(String),
}

impl From<sqlparser::parser::ParserError> for QueryError {
    fn from(err: sqlparser::parser::ParserError) -> Self {
        QueryError::ParseError(err.to_string())
    }
}

impl From<anyhow::Error> for QueryError {
    fn from(err: anyhow::Error) -> Self {
        QueryError::InternalError(err.to_string())
    }
}