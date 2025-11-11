//! Query planner that converts SQL AST to logical plan

use sqlparser::ast::*;

use crate::error::QueryError;
use crate::logical_plan::*;
use crate::parser;

/// Build logical plan from SQL AST
pub fn build_logical_plan(stmt: Statement) -> Result<LogicalQuery, QueryError> {
    match stmt {
        Statement::Query(query) => build_from_query(*query),
        _ => Err(QueryError::UnsupportedClause("Only SELECT queries are supported".to_string())),
    }
}

/// Build from Query AST
fn build_from_query(query: Query) -> Result<LogicalQuery, QueryError> {
    // Handle WITH clause if present
    if !query.with.is_none() {
        return Err(QueryError::UnsupportedClause("WITH clause not supported".to_string()));
    }
    
    match *query.body {
        SetExpr::Select(select) => build_from_select(*select, query.order_by, query.limit, query.offset),
        _ => Err(QueryError::UnsupportedClause("Only simple SELECT supported".to_string())),
    }
}

/// Build from SELECT statement
fn build_from_select(
    select: Select,
    order_by: Option<OrderBy>,
    limit: Option<Expr>,
    offset: Option<Offset>,
) -> Result<LogicalQuery, QueryError> {
    // Check for unsupported features
    if select.distinct.is_some() {
        return Err(QueryError::UnsupportedClause("DISTINCT not yet supported".to_string()));
    }
    
    if !select.having.is_none() {
        return Err(QueryError::UnsupportedClause("HAVING not yet supported".to_string()));
    }
    
    // Extract table name
    let source = parser::extract_table_name(&select.from)?;
    
    // Build projection
    let projection = build_projection(&select.projection)?;
    
    // Build filters
    let filters = if let Some(selection) = select.selection {
        build_filters(selection)?
    } else {
        Vec::new()
    };
    
    // Build group by
    let group_by = build_group_by(&select.group_by)?;
    
    // Build order by
    let order_by = match order_by {
        Some(ob) => build_order_by(ob.exprs)?,
        None => Vec::new(),
    };
    
    // Parse limit and offset
    let limit = parse_limit(limit)?;
    let offset = parse_offset(offset)?;
    
    Ok(LogicalQuery {
        source,
        projection,
        filters,
        group_by,
        order_by,
        limit,
        offset,
        index_hints: Vec::new(), // Will be added by optimizer
    })
}

/// Build projection from SELECT items
fn build_projection(projection: &[SelectItem]) -> Result<Vec<Field>, QueryError> {
    let mut fields = Vec::new();
    
    for item in projection {
        match item {
            SelectItem::UnnamedExpr(expr) => {
                fields.push(expr_to_field(expr, None)?);
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                fields.push(expr_to_field(expr, Some(alias.value.clone()))?);
            }
            SelectItem::Wildcard(_) => {
                // For now, we'll expand * to common fields
                // In production, this would query schema
                fields.extend(vec![
                    Field::Column("timestamp".to_string()),
                    Field::Column("level".to_string()),
                    Field::Column("service".to_string()),
                    Field::Column("message".to_string()),
                ]);
            }
            _ => return Err(QueryError::UnsupportedClause("Complex projection not supported".to_string())),
        }
    }
    
    Ok(fields)
}

/// Convert expression to field
fn expr_to_field(expr: &Expr, alias: Option<String>) -> Result<Field, QueryError> {
    match expr {
        Expr::Identifier(ident) => Ok(Field::Column(ident.value.clone())),
        
        Expr::Function(func) => {
            let name = func.name.to_string().to_uppercase();
            
            // Check if it's an aggregate
            if matches!(name.as_str(), "COUNT" | "SUM" | "AVG" | "MIN" | "MAX") {
                let argument = match &func.args {
                    FunctionArguments::List(args) if args.args.is_empty() => "*".to_string(),
                    FunctionArguments::List(args) => {
                        // Convert first argument to string
                        match &args.args[0] {
                            FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => expr_to_string(e)?,
                            FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => "*".to_string(),
                            _ => return Err(QueryError::InvalidFunctionUsage {
                                function: name,
                                reason: "Invalid argument".to_string(),
                            }),
                        }
                    },
                    _ => "*".to_string(),
                };
                
                Ok(Field::Aggregate {
                    function: name,
                    argument,
                    alias,
                })
            } else {
                // Scalar function
                let args = match &func.args {
                    FunctionArguments::List(args) => {
                        args.args.iter()
                            .map(|arg| match arg {
                                FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => expr_to_field(e, None),
                                _ => Err(QueryError::InvalidFunctionUsage {
                                    function: name.clone(),
                                    reason: "Invalid argument".to_string(),
                                }),
                            })
                            .collect::<Result<Vec<_>, _>>()?
                    },
                    _ => Vec::new(),
                };
                
                Ok(Field::Function {
                    name,
                    args,
                    alias,
                })
            }
        }
        
        Expr::Value(val) => Ok(Field::Literal(sql_value_to_value(val)?)),
        
        _ => Err(QueryError::UnsupportedClause(format!("Expression type not supported: {:?}", expr))),
    }
}

/// Convert expression to string representation
fn expr_to_string(expr: &Expr) -> Result<String, QueryError> {
    match expr {
        Expr::Identifier(ident) => Ok(ident.value.clone()),
        Expr::CompoundIdentifier(idents) => {
            Ok(idents.iter().map(|i| i.value.clone()).collect::<Vec<_>>().join("."))
        }
        Expr::Value(sqlparser::ast::Value::SingleQuotedString(s)) => Ok(s.clone()),
        _ => Ok(expr.to_string()),
    }
}

/// Build filters from WHERE clause
fn build_filters(expr: Expr) -> Result<Vec<Filter>, QueryError> {
    Ok(vec![expr_to_filter(expr)?])
}

/// Convert expression to filter
fn expr_to_filter(expr: Expr) -> Result<Filter, QueryError> {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            match op {
                BinaryOperator::Eq => {
                    let field = parser::extract_column_name(&left)?;
                    let value = expr_to_value(&right)?;
                    Ok(Filter::Equals(field, value))
                }
                BinaryOperator::NotEq => {
                    let field = parser::extract_column_name(&left)?;
                    let value = expr_to_value(&right)?;
                    Ok(Filter::NotEquals(field, value))
                }
                BinaryOperator::Gt => {
                    let field = parser::extract_column_name(&left)?;
                    let value = expr_to_value(&right)?;
                    Ok(Filter::GreaterThan(field, value))
                }
                BinaryOperator::GtEq => {
                    let field = parser::extract_column_name(&left)?;
                    let value = expr_to_value(&right)?;
                    Ok(Filter::GreaterThanOrEqual(field, value))
                }
                BinaryOperator::Lt => {
                    let field = parser::extract_column_name(&left)?;
                    let value = expr_to_value(&right)?;
                    Ok(Filter::LessThan(field, value))
                }
                BinaryOperator::LtEq => {
                    let field = parser::extract_column_name(&left)?;
                    let value = expr_to_value(&right)?;
                    Ok(Filter::LessThanOrEqual(field, value))
                }
                BinaryOperator::And => {
                    let left_filter = expr_to_filter(*left)?;
                    let right_filter = expr_to_filter(*right)?;
                    Ok(Filter::And(vec![left_filter, right_filter]))
                }
                BinaryOperator::Or => {
                    let left_filter = expr_to_filter(*left)?;
                    let right_filter = expr_to_filter(*right)?;
                    Ok(Filter::Or(vec![left_filter, right_filter]))
                }
                _ => Err(QueryError::UnsupportedClause(format!("Operator {:?} not supported", op))),
            }
        }
        
        Expr::Like { expr, pattern, .. } | Expr::ILike { expr, pattern, .. } => {
            let field = parser::extract_column_name(&expr)?;
            let pattern_str = match pattern.as_ref() {
                Expr::Value(sqlparser::ast::Value::SingleQuotedString(s)) => s.clone(),
                _ => return Err(QueryError::InvalidFunctionUsage {
                    function: "LIKE".to_string(),
                    reason: "Pattern must be a string".to_string(),
                }),
            };
            
            // Check if it's a full-text pattern
            if parser::is_fulltext_pattern(&pattern_str) {
                let search_term = pattern_str.trim_matches('%');
                Ok(Filter::FullTextContains(field, search_term.to_string()))
            } else {
                Ok(Filter::Like(field, pattern_str))
            }
        }
        
        Expr::InList { expr, list, negated } => {
            let field = parser::extract_column_name(&expr)?;
            let values = list.into_iter()
                .map(|e| expr_to_value(&e))
                .collect::<Result<Vec<_>, _>>()?;
            
            if negated {
                Ok(Filter::NotIn(field, values))
            } else {
                Ok(Filter::In(field, values))
            }
        }
        
        Expr::Between { expr, low, high, negated } => {
            let field = parser::extract_column_name(&expr)?;
            let low_value = expr_to_value(&low)?;
            let high_value = expr_to_value(&high)?;
            
            let between = Filter::Between {
                field,
                low: low_value,
                high: high_value,
            };
            
            if negated {
                Ok(Filter::Not(Box::new(between)))
            } else {
                Ok(between)
            }
        }
        
        Expr::IsNull(expr) => {
            let field = parser::extract_column_name(&expr)?;
            Ok(Filter::IsNull(field))
        }
        
        Expr::IsNotNull(expr) => {
            let field = parser::extract_column_name(&expr)?;
            Ok(Filter::IsNotNull(field))
        }
        
        Expr::UnaryOp { op: UnaryOperator::Not, expr } => {
            let inner_filter = expr_to_filter(*expr)?;
            Ok(Filter::Not(Box::new(inner_filter)))
        }
        
        // Handle nested/parenthesized expressions
        Expr::Nested(expr) => {
            expr_to_filter(*expr)
        }
        
        _ => {
            // For unsupported expressions, try to extract what we can
            // Return a placeholder or skip the filter
            Err(QueryError::UnsupportedClause(format!("Expression type not supported in WHERE: {:?}", expr)))
        }
    }
}

/// Convert expression to value
fn expr_to_value(expr: &Expr) -> Result<crate::logical_plan::Value, QueryError> {
    match expr {
        Expr::Value(val) => sql_value_to_value(val),
        
        // Handle NOW() - INTERVAL expressions
        Expr::BinaryOp { left, op, right } if matches!(op, BinaryOperator::Minus) => {
            match (left.as_ref(), right.as_ref()) {
                (Expr::Function(func), Expr::Interval(_)) if func.name.to_string().to_uppercase() == "NOW" => {
                    let interval = parser::parse_interval(right)?;
                    Ok(crate::logical_plan::Value::RelativeTime(format!("now() - {}", interval)))
                }
                _ => Err(QueryError::InvalidTimeExpression("Unsupported time expression".to_string())),
            }
        }
        
        Expr::Function(func) if func.name.to_string().to_uppercase() == "NOW" => {
            Ok(crate::logical_plan::Value::RelativeTime("now()".to_string()))
        }
        
        _ => Err(QueryError::TypeMismatch {
            expected: "value".to_string(),
            found: format!("{:?}", expr),
        }),
    }
}

/// Convert SQL value to our Value type
fn sql_value_to_value(val: &sqlparser::ast::Value) -> Result<crate::logical_plan::Value, QueryError> {
    match val {
        sqlparser::ast::Value::SingleQuotedString(s) => Ok(crate::logical_plan::Value::String(s.clone())),
        sqlparser::ast::Value::Number(n, _) => {
            if n.contains('.') {
                Ok(crate::logical_plan::Value::Float(n.parse().map_err(|_| QueryError::TypeMismatch {
                    expected: "float".to_string(),
                    found: n.clone(),
                })?))
            } else {
                Ok(crate::logical_plan::Value::Integer(n.parse().map_err(|_| QueryError::TypeMismatch {
                    expected: "integer".to_string(),
                    found: n.clone(),
                })?))
            }
        }
        sqlparser::ast::Value::Boolean(b) => Ok(crate::logical_plan::Value::Boolean(*b)),
        sqlparser::ast::Value::Null => Ok(crate::logical_plan::Value::Null),
        _ => Err(QueryError::TypeMismatch {
            expected: "simple value".to_string(),
            found: format!("{:?}", val),
        }),
    }
}

/// Build GROUP BY
fn build_group_by(group_by: &GroupByExpr) -> Result<Vec<String>, QueryError> {
    match group_by {
        GroupByExpr::Expressions(exprs, _) => {
            exprs.iter()
                .map(|e| parser::extract_column_name(e))
                .collect()
        }
        GroupByExpr::All(_) => Err(QueryError::UnsupportedClause("GROUP BY ALL not supported".to_string())),
    }
}

/// Build ORDER BY
fn build_order_by(order_by: Vec<OrderByExpr>) -> Result<Vec<Order>, QueryError> {
    order_by.into_iter()
        .map(|ob| {
            let field = expr_to_string(&ob.expr)?;
            let direction = if ob.asc.unwrap_or(true) {
                Direction::Ascending
            } else {
                Direction::Descending
            };
            Ok(Order { field, direction })
        })
        .collect()
}

/// Parse LIMIT
fn parse_limit(limit: Option<Expr>) -> Result<Option<u64>, QueryError> {
    match limit {
        Some(Expr::Value(sqlparser::ast::Value::Number(n, _))) => {
            n.parse().map(Some).map_err(|_| QueryError::LimitOffsetError("Invalid LIMIT value".to_string()))
        }
        Some(_) => Err(QueryError::LimitOffsetError("LIMIT must be a number".to_string())),
        None => Ok(None),
    }
}

/// Parse OFFSET
fn parse_offset(offset: Option<Offset>) -> Result<Option<u64>, QueryError> {
    match offset {
        Some(Offset { value, .. }) => parse_limit(Some(value)),
        None => Ok(None),
    }
}

/// Validate the logical plan
pub fn validate_plan(plan: &LogicalQuery) -> Result<(), QueryError> {
    // Check GROUP BY validity
    if !plan.group_by.is_empty() || plan.has_aggregates() {
        validate_grouping(plan)?;
    }
    
    // Validate ORDER BY references
    validate_order_by(plan)?;
    
    Ok(())
}

/// Validate GROUP BY rules
fn validate_grouping(plan: &LogicalQuery) -> Result<(), QueryError> {
    // If we have aggregates, all non-aggregate fields must be in GROUP BY
    for field in &plan.projection {
        match field {
            Field::Column(name) => {
                if !plan.group_by.contains(name) {
                    return Err(QueryError::GroupingError(
                        format!("Column '{}' must be in GROUP BY clause or be an aggregate", name)
                    ));
                }
            }
            Field::Aggregate { .. } => {} // OK
            Field::Function { .. } => {} // TODO: Check if function contains columns
            Field::Literal(_) => {} // OK
        }
    }
    
    Ok(())
}

/// Validate ORDER BY references valid fields
fn validate_order_by(plan: &LogicalQuery) -> Result<(), QueryError> {
    for order in &plan.order_by {
        // Check if field is in projection or is an aggregate
        let valid = plan.projection.iter().any(|f| match f {
            Field::Column(name) => name == &order.field,
            Field::Aggregate { function, argument, .. } => {
                order.field == format!("{}({})", function, argument)
            }
            _ => false,
        });
        
        if !valid && !plan.group_by.contains(&order.field) {
            return Err(QueryError::UnknownField(
                format!("ORDER BY field '{}' not in SELECT or GROUP BY", order.field)
            ));
        }
    }
    
    Ok(())
}

/// Add index hints based on query patterns
pub fn add_index_hints(mut plan: LogicalQuery) -> Result<LogicalQuery, QueryError> {
    let mut hints = Vec::new();
    
    // Analyze filters for index opportunities
    for filter in &plan.filters {
        add_filter_hints(filter, &mut hints);
    }
    
    // Add time-series hint if time range detected
    if plan.has_time_filters() {
        hints.push(UseIndex::TimeSeries("timestamp".to_string()));
    }
    
    // Add columnar hint if aggregations present
    if plan.has_aggregates() {
        hints.push(UseIndex::Columnar);
    }
    
    // Deduplicate hints
    hints.sort_by_key(|h| format!("{:?}", h));
    hints.dedup();
    
    plan.index_hints = hints;
    Ok(plan)
}

/// Add hints based on filter type
fn add_filter_hints(filter: &Filter, hints: &mut Vec<UseIndex>) {
    match filter {
        Filter::Equals(field, _) | Filter::In(field, _) => {
            // Use inverted index for exact matches
            hints.push(UseIndex::Inverted(field.clone()));
            
            // Use bloom filter for high-cardinality fields
            if matches!(field.as_str(), "trace_id" | "span_id" | "user_id") {
                hints.push(UseIndex::Bloom(field.clone()));
            }
            
            // Use bitmap for low-cardinality fields
            if matches!(field.as_str(), "level" | "status" | "method") {
                hints.push(UseIndex::Bitmap(field.clone()));
            }
        }
        
        Filter::FullTextContains(field, _) => {
            hints.push(UseIndex::FullText(field.clone()));
        }
        
        Filter::Between { field, .. } if field == "timestamp" => {
            hints.push(UseIndex::TimeSeries(field.clone()));
        }
        
        Filter::And(filters) | Filter::Or(filters) => {
            for f in filters {
                add_filter_hints(f, hints);
            }
        }
        
        Filter::Not(f) => add_filter_hints(f, hints),
        
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::parse_sql;

    #[test]
    fn test_simple_select() {
        let sql = "SELECT level, message FROM logs WHERE service = 'auth'";
        let ast = parse_sql(sql).unwrap();
        let plan = build_logical_plan(ast).unwrap();
        
        assert_eq!(plan.source, "logs");
        assert_eq!(plan.projection.len(), 2);
        assert_eq!(plan.filters.len(), 1);
    }

    #[test]
    fn test_aggregation_query() {
        let sql = "SELECT level, COUNT(*) FROM logs GROUP BY level ORDER BY COUNT(*) DESC";
        let ast = parse_sql(sql).unwrap();
        let plan = build_logical_plan(ast).unwrap();
        
        assert!(plan.has_aggregates());
        assert_eq!(plan.group_by, vec!["level"]);
        assert_eq!(plan.order_by.len(), 1);
    }

    #[test]
    fn test_time_range_query() {
        let sql = "SELECT * FROM logs WHERE timestamp > NOW() - INTERVAL '1 hour'";
        let ast = parse_sql(sql).unwrap();
        let plan = build_logical_plan(ast).unwrap();
        
        assert!(plan.has_time_filters());
        
        // Should have time-series index hint
        let optimized = add_index_hints(plan).unwrap();
        assert!(optimized.index_hints.contains(&UseIndex::TimeSeries("timestamp".to_string())));
    }

    #[test]
    fn test_fulltext_search() {
        let sql = "SELECT message FROM logs WHERE message LIKE '%timeout%'";
        let ast = parse_sql(sql).unwrap();
        let plan = build_logical_plan(ast).unwrap();
        
        // Should convert to full-text search
        match &plan.filters[0] {
            Filter::FullTextContains(field, pattern) => {
                assert_eq!(field, "message");
                assert_eq!(pattern, "timeout");
            }
            _ => panic!("Expected FullTextContains filter"),
        }
    }

    #[test]
    fn test_invalid_grouping() {
        let sql = "SELECT level, service, COUNT(*) FROM logs GROUP BY level";
        let ast = parse_sql(sql).unwrap();
        let plan = build_logical_plan(ast).unwrap();
        let result = validate_plan(&plan);
        
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), QueryError::GroupingError(_)));
    }
}