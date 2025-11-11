//! Query planning and execution

use anyhow::Result;
use std::sync::Arc;

use crate::{Filter, Index, Query, QueryResult, TimeRange};

/// Query planner that optimizes and routes queries
pub struct QueryPlanner {
    indexes: Vec<Arc<dyn Index>>,
}

impl QueryPlanner {
    pub fn new(indexes: Vec<Arc<dyn Index>>) -> Self {
        Self { indexes }
    }
    
    /// Plan and execute a query across multiple indexes
    pub async fn execute(&self, query: &Query) -> Result<QueryResult> {
        // Analyze query to determine best index
        let execution_plan = self.create_plan(query)?;
        
        // Execute based on plan
        match execution_plan {
            ExecutionPlan::SingleIndex { index, optimized_query } => {
                self.indexes[index].query(&optimized_query).await
            }
            ExecutionPlan::MultiIndex { index_queries } => {
                self.execute_multi_index(index_queries).await
            }
            ExecutionPlan::Hybrid { primary, secondary } => {
                self.execute_hybrid(primary, secondary).await
            }
        }
    }
    
    fn create_plan(&self, query: &Query) -> Result<ExecutionPlan> {
        // Analyze query characteristics
        let has_time_range = query.time_range.is_some();
        let has_text_search = self.has_text_search(&query.filters);
        let has_exact_match = self.has_exact_match(&query.filters);
        let has_aggregations = query.aggregations.is_some();
        
        // Choose execution strategy
        if has_time_range && !has_text_search {
            // Use time-series index for time-based queries
            Ok(ExecutionPlan::SingleIndex {
                index: 2, // TimeSeries index
                optimized_query: query.clone(),
            })
        } else if has_text_search {
            // Use full-text index
            Ok(ExecutionPlan::SingleIndex {
                index: 3, // FullText index
                optimized_query: query.clone(),
            })
        } else if has_exact_match && !has_aggregations {
            // Use inverted index for exact matches
            Ok(ExecutionPlan::SingleIndex {
                index: 0, // Inverted index
                optimized_query: query.clone(),
            })
        } else if has_aggregations {
            // Use columnar for analytics
            Ok(ExecutionPlan::SingleIndex {
                index: 1, // Columnar index
                optimized_query: query.clone(),
            })
        } else {
            // Complex query - use multiple indexes
            Ok(ExecutionPlan::MultiIndex {
                index_queries: vec![
                    (0, self.extract_exact_match_query(query)),
                    (1, self.extract_analytical_query(query)),
                ],
            })
        }
    }
    
    fn has_text_search(&self, filters: &[Filter]) -> bool {
        filters.iter().any(|f| matches!(f, 
            Filter::Contains { .. } | 
            Filter::Regex { .. }
        ))
    }
    
    fn has_exact_match(&self, filters: &[Filter]) -> bool {
        filters.iter().any(|f| matches!(f,
            Filter::Equals { .. } |
            Filter::In { .. }
        ))
    }
    
    fn extract_exact_match_query(&self, query: &Query) -> Query {
        // Extract only exact match filters
        Query {
            filters: query.filters.iter()
                .filter(|f| matches!(f, Filter::Equals { .. } | Filter::In { .. }))
                .cloned()
                .collect(),
            ..query.clone()
        }
    }
    
    fn extract_analytical_query(&self, query: &Query) -> Query {
        // Keep aggregations and complex filters
        query.clone()
    }
    
    async fn execute_multi_index(&self, _index_queries: Vec<(usize, Query)>) -> Result<QueryResult> {
        // Execute queries in parallel and merge results
        todo!("Implement multi-index execution")
    }
    
    async fn execute_hybrid(&self, _primary: Box<ExecutionPlan>, _secondary: Box<ExecutionPlan>) -> Result<QueryResult> {
        // Execute primary first, then filter with secondary
        todo!("Implement hybrid execution")
    }
}

enum ExecutionPlan {
    SingleIndex {
        index: usize,
        optimized_query: Query,
    },
    MultiIndex {
        index_queries: Vec<(usize, Query)>,
    },
    #[allow(dead_code)]
    Hybrid {
        primary: Box<ExecutionPlan>,
        secondary: Box<ExecutionPlan>,
    },
}

/// Query optimizer that rewrites queries for better performance
pub struct QueryOptimizer;

impl QueryOptimizer {
    pub fn optimize(query: &Query) -> Query {
        let mut optimized = query.clone();
        
        // Reorder filters by selectivity
        optimized.filters = Self::reorder_filters(&query.filters);
        
        // Push down time range filters
        if let Some(time_range) = &query.time_range {
            optimized.filters.insert(0, Self::time_range_to_filter(time_range));
        }
        
        optimized
    }
    
    fn reorder_filters(filters: &[Filter]) -> Vec<Filter> {
        // Order by estimated selectivity (most selective first)
        let mut reordered = filters.to_vec();
        reordered.sort_by_key(|f| match f {
            Filter::Equals { .. } => 1,  // Most selective
            Filter::In { values, .. } => 2 + values.len(),
            Filter::GreaterThan { .. } | Filter::LessThan { .. } => 10,
            Filter::Contains { .. } => 20,
            Filter::Regex { .. } => 30,  // Least selective
            Filter::And(_) => 5,
            Filter::Or(_) => 15,
            Filter::Not(_) => 25,
            _ => 50,
        });
        reordered
    }
    
    fn time_range_to_filter(range: &TimeRange) -> Filter {
        Filter::And(vec![
            Filter::GreaterThan {
                field: "timestamp".to_string(),
                value: crate::FieldValue::Timestamp(range.start),
            },
            Filter::LessThan {
                field: "timestamp".to_string(),
                value: crate::FieldValue::Timestamp(range.end),
            },
        ])
    }
}