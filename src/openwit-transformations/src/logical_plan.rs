//! Logical query plan structures

use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};

/// The main logical query structure that represents parsed SQL
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogicalQuery {
    /// Source table (logs, traces, metrics)
    pub source: String,
    
    /// Projected fields (SELECT clause)
    pub projection: Vec<Field>,
    
    /// Filter conditions (WHERE clause)
    pub filters: Vec<Filter>,
    
    /// Grouping fields (GROUP BY)
    pub group_by: Vec<String>,
    
    /// Ordering (ORDER BY)
    pub order_by: Vec<Order>,
    
    /// Result limit
    pub limit: Option<u64>,
    
    /// Result offset
    pub offset: Option<u64>,
    
    /// Index hints for optimization
    pub index_hints: Vec<UseIndex>,
}

/// Field in projection
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Field {
    /// Simple column reference
    Column(String),
    
    /// Aggregate function
    Aggregate {
        function: String,
        argument: String,
        alias: Option<String>,
    },
    
    /// Scalar function
    Function {
        name: String,
        args: Vec<Field>,
        alias: Option<String>,
    },
    
    /// Literal value
    Literal(Value),
}

/// Filter conditions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Filter {
    /// Equality check
    Equals(String, Value),
    
    /// Not equals
    NotEquals(String, Value),
    
    /// Greater than
    GreaterThan(String, Value),
    
    /// Greater than or equal
    GreaterThanOrEqual(String, Value),
    
    /// Less than
    LessThan(String, Value),
    
    /// Less than or equal
    LessThanOrEqual(String, Value),
    
    /// IN clause
    In(String, Vec<Value>),
    
    /// NOT IN clause
    NotIn(String, Vec<Value>),
    
    /// BETWEEN clause
    Between {
        field: String,
        low: Value,
        high: Value,
    },
    
    /// LIKE pattern matching
    Like(String, String),
    
    /// Full-text search (transformed from LIKE '%pattern%')
    FullTextContains(String, String),
    
    /// IS NULL
    IsNull(String),
    
    /// IS NOT NULL
    IsNotNull(String),
    
    /// Logical AND
    And(Vec<Filter>),
    
    /// Logical OR
    Or(Vec<Filter>),
    
    /// Logical NOT
    Not(Box<Filter>),
}

/// Values in filters and expressions
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Value {
    /// String literal
    String(String),
    
    /// Integer
    Integer(i64),
    
    /// Float
    Float(f64),
    
    /// Boolean
    Boolean(bool),
    
    /// Timestamp
    Timestamp(DateTime<Utc>),
    
    /// Relative time (e.g., "now() - 1h")
    RelativeTime(String),
    
    /// NULL
    Null,
}

/// Order by specification
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Order {
    pub field: String,
    pub direction: Direction,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Direction {
    Ascending,
    Descending,
}

/// Index usage hints
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum UseIndex {
    /// Use inverted index for exact matches
    Inverted(String),
    
    /// Use time-series index for time ranges
    TimeSeries(String),
    
    /// Use full-text index for text search
    FullText(String),
    
    /// Use bloom filter for existence checks
    Bloom(String),
    
    /// Use bitmap index for low-cardinality fields
    Bitmap(String),
    
    /// Use columnar storage for analytics
    Columnar,
}

/// Predicate for query filters
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Predicate {
    pub field: String,
    pub operator: ComparisonOp,
    pub value: Value,
}

/// Comparison operators
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ComparisonOp {
    Equals,
    NotEquals,
    GreaterThan,
    LessThan,
    GreaterOrEqual,
    LessOrEqual,
    Like,
    In,
    Between,
}

impl LogicalQuery {
    /// Check if query requires aggregation
    pub fn has_aggregates(&self) -> bool {
        self.projection.iter().any(|f| matches!(f, Field::Aggregate { .. }))
    }
    
    /// Check if query has time-based filters
    pub fn has_time_filters(&self) -> bool {
        self.filters.iter().any(|f| Self::is_time_filter(f))
    }
    
    fn is_time_filter(filter: &Filter) -> bool {
        match filter {
            Filter::GreaterThan(field, _) | 
            Filter::LessThan(field, _) |
            Filter::Between { field, .. } => field == "timestamp",
            Filter::And(filters) | Filter::Or(filters) => {
                filters.iter().any(Self::is_time_filter)
            }
            Filter::Not(f) => Self::is_time_filter(f),
            _ => false,
        }
    }
}