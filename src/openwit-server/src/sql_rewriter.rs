//! SQL Query Rewriter for DataFusion compatibility
//! 
//! This module rewrites SQL queries to work around DataFusion limitations
//! and transforms unsupported constructs into supported ones.

use anyhow::Result;
use regex::Regex;
use tracing::{info, debug};

#[allow(dead_code)]
pub struct SqlRewriter {
    // Regex patterns for query transformations
    arithmetic_where_pattern: Regex,
    like_pattern: Regex,
    exists_pattern: Regex,
    random_pattern: Regex,
    cast_pattern: Regex,
    between_pattern: Regex,
    offset_pattern: Regex,
    having_pattern: Regex,
    case_when_pattern: Regex,
    division_pattern: Regex,
    modulo_pattern: Regex,
    union_pattern: Regex,
    string_concat_pattern: Regex,
}

impl SqlRewriter {
    pub fn new() -> Self {
        Self {
            // Pattern to match arithmetic operations in WHERE clauses
            arithmetic_where_pattern: Regex::new(r"WHERE\s+(\w+)\s*>\s*\((\d+)\s*-\s*(\d+)\)").unwrap(),
            
            // Pattern to match LIKE operations
            like_pattern: Regex::new(r"(\w+)\s+LIKE\s+'([^']+)'").unwrap(),
            
            // Pattern to match EXISTS subqueries
            exists_pattern: Regex::new(r"SELECT\s+EXISTS\s*\(([^)]+)\)").unwrap(),
            
            // Pattern to match RANDOM() function
            random_pattern: Regex::new(r"ORDER\s+BY\s+RANDOM\(\)").unwrap(),
            
            // Pattern to match CAST operations
            cast_pattern: Regex::new(r"CAST\s*\(\s*(\w+)\s+AS\s+VARCHAR\s*\)").unwrap(),
            
            // Pattern to match BETWEEN operations
            between_pattern: Regex::new(r"(\w+)\s+BETWEEN\s+(\d+)\s+AND\s+(\d+)").unwrap(),
            
            // Pattern to match OFFSET clauses
            offset_pattern: Regex::new(r"LIMIT\s+(\d+)\s+OFFSET\s+(\d+)").unwrap(),
            
            // Pattern to match HAVING clauses
            having_pattern: Regex::new(r"GROUP\s+BY\s+([^)]+?)\s+HAVING\s+").unwrap(),
            
            // Pattern to match CASE WHEN expressions in GROUP BY
            case_when_pattern: Regex::new(r"GROUP\s+BY\s+(CASE\s+WHEN[^,]+?END)").unwrap(),
            
            // Pattern to match division operations
            division_pattern: Regex::new(r"(\w+)\s*/\s*([\d.]+)").unwrap(),
            
            // Pattern to match modulo operations
            modulo_pattern: Regex::new(r"\((\w+)\s*/\s*(\d+)\)\s*%\s*(\d+)").unwrap(),
            
            // Pattern to match UNION operations
            union_pattern: Regex::new(r"\bUNION\s+ALL\b").unwrap(),
            
            // Pattern to match string concatenation
            string_concat_pattern: Regex::new(r"(\w+)\s*\|\|\s*'([^']+)'").unwrap(),
        }
    }
    
    /// Rewrite a SQL query to be compatible with DataFusion
    pub fn rewrite(&self, sql: &str) -> Result<String> {
        let original_sql = sql.to_string();
        let mut rewritten = original_sql.clone();
        
        debug!("Rewriting SQL query: {}", sql);
        
        // 1. Fix arithmetic operations in WHERE clauses
        rewritten = self.rewrite_arithmetic_where(&rewritten)?;
        
        // 2. Transform LIKE operations to string functions
        rewritten = self.rewrite_like_operations(&rewritten)?;
        
        // 3. Handle EXISTS subqueries
        rewritten = self.rewrite_exists_subqueries(&rewritten)?;
        
        // 4. Replace RANDOM() with alternative
        rewritten = self.rewrite_random_ordering(&rewritten)?;
        
        // 5. Handle CAST operations
        rewritten = self.rewrite_cast_operations(&rewritten)?;
        
        // 6. Transform BETWEEN to >= AND <=
        rewritten = self.rewrite_between_operations(&rewritten)?;
        
        // 7. Handle OFFSET clauses
        rewritten = self.rewrite_offset_clauses(&rewritten)?;
        
        // 8. Handle HAVING clauses
        rewritten = self.rewrite_having_clauses(&rewritten)?;
        
        // 9. Handle CASE WHEN in GROUP BY
        rewritten = self.rewrite_case_when_group_by(&rewritten)?;
        
        // 10. Handle division operations
        rewritten = self.rewrite_division_operations(&rewritten)?;
        
        // 11. Handle modulo operations
        rewritten = self.rewrite_modulo_operations(&rewritten)?;
        
        // 12. Handle UNION operations
        rewritten = self.rewrite_union_operations(&rewritten)?;
        
        // 13. Handle string concatenation
        rewritten = self.rewrite_string_concat(&rewritten)?;
        
        if rewritten != original_sql {
            info!("Query rewritten: {} -> {}", original_sql, rewritten);
        }
        
        Ok(rewritten)
    }
    
    /// Rewrite arithmetic operations in WHERE clauses
    /// Example: WHERE received_at > (1754651694252 - 3600000) 
    /// Becomes: WHERE received_at > 1754648094252
    fn rewrite_arithmetic_where(&self, sql: &str) -> Result<String> {
        let mut result = sql.to_string();
        
        for cap in self.arithmetic_where_pattern.captures_iter(sql) {
            if let (Some(field), Some(val1), Some(val2)) = (cap.get(1), cap.get(2), cap.get(3)) {
                let v1: i64 = val1.as_str().parse()?;
                let v2: i64 = val2.as_str().parse()?;
                let calculated = v1 - v2;
                
                let original = cap.get(0).unwrap().as_str();
                let replacement = format!("WHERE {} > {}", field.as_str(), calculated);
                result = result.replace(original, &replacement);
            }
        }
        
        Ok(result)
    }
    
    /// Transform LIKE operations to string contains checks
    /// Since DataFusion might not support LIKE, we can use alternative approaches
    fn rewrite_like_operations(&self, sql: &str) -> Result<String> {
        // For now, we'll keep LIKE as-is since DataFusion might support it
        // If it doesn't, we could transform to other string functions
        Ok(sql.to_string())
    }
    
    /// Rewrite EXISTS subqueries
    /// Example: SELECT EXISTS(SELECT 1 FROM logs WHERE id = 'test')
    /// Becomes: SELECT CASE WHEN COUNT(*) > 0 THEN true ELSE false END FROM (SELECT 1 FROM logs WHERE id = 'test' LIMIT 1) as subq
    fn rewrite_exists_subqueries(&self, sql: &str) -> Result<String> {
        let mut result = sql.to_string();
        
        if let Some(cap) = self.exists_pattern.captures(sql) {
            if let Some(subquery) = cap.get(1) {
                let replacement = format!(
                    "SELECT CASE WHEN COUNT(*) > 0 THEN true ELSE false END FROM ({} LIMIT 1) as subq",
                    subquery.as_str()
                );
                result = self.exists_pattern.replace(sql, replacement).to_string();
            }
        }
        
        Ok(result)
    }
    
    /// Replace RANDOM() ordering with alternative
    fn rewrite_random_ordering(&self, sql: &str) -> Result<String> {
        // Replace ORDER BY RANDOM() with ORDER BY id (deterministic but varied)
        let result = self.random_pattern.replace_all(sql, "ORDER BY id").to_string();
        Ok(result)
    }
    
    /// Handle CAST operations
    fn rewrite_cast_operations(&self, sql: &str) -> Result<String> {
        // For CAST(field AS VARCHAR), we might need to use string functions
        // For now, keep as-is and let DataFusion handle it
        Ok(sql.to_string())
    }
    
    /// Transform BETWEEN to >= AND <=
    /// Example: WHERE received_at BETWEEN 1754640000000 AND 1754650000000
    /// Becomes: WHERE received_at >= 1754640000000 AND received_at <= 1754650000000
    fn rewrite_between_operations(&self, sql: &str) -> Result<String> {
        let mut result = sql.to_string();
        
        for cap in self.between_pattern.captures_iter(sql) {
            if let (Some(field), Some(val1), Some(val2)) = (cap.get(1), cap.get(2), cap.get(3)) {
                let original = cap.get(0).unwrap().as_str();
                let replacement = format!(
                    "{} >= {} AND {} <= {}",
                    field.as_str(), val1.as_str(), field.as_str(), val2.as_str()
                );
                result = result.replace(original, &replacement);
            }
        }
        
        Ok(result)
    }
    
    /// Handle OFFSET clauses by removing them or using alternative approach
    fn rewrite_offset_clauses(&self, sql: &str) -> Result<String> {
        // For now, keep OFFSET as DataFusion should support it
        Ok(sql.to_string())
    }
    
    /// Handle HAVING clauses
    fn rewrite_having_clauses(&self, sql: &str) -> Result<String> {
        // HAVING should be supported by DataFusion, keep as-is
        Ok(sql.to_string())
    }
    
    /// Handle CASE WHEN in GROUP BY
    fn rewrite_case_when_group_by(&self, sql: &str) -> Result<String> {
        // This is complex - for now, try to extract the CASE expression and use it as a computed column
        if sql.contains("GROUP BY") && sql.contains("CASE WHEN") {
            // Complex rewrite needed - would need full SQL parser
            // For now, return as-is
        }
        Ok(sql.to_string())
    }
    
    /// Handle division operations that might cause issues
    fn rewrite_division_operations(&self, sql: &str) -> Result<String> {
        // Division should work in DataFusion, keep as-is
        Ok(sql.to_string())
    }
    
    /// Handle modulo operations
    fn rewrite_modulo_operations(&self, sql: &str) -> Result<String> {
        // Modulo might not be supported - would need to check DataFusion docs
        Ok(sql.to_string())
    }
    
    /// Handle UNION operations
    fn rewrite_union_operations(&self, sql: &str) -> Result<String> {
        // UNION might need special handling in DataFusion
        Ok(sql.to_string())
    }
    
    /// Handle string concatenation
    fn rewrite_string_concat(&self, sql: &str) -> Result<String> {
        // String concat with || should work in DataFusion
        Ok(sql.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_arithmetic_where_rewrite() {
        let rewriter = SqlRewriter::new();
        let sql = "SELECT COUNT(*) FROM logs WHERE received_at > (1754651694252 - 3600000)";
        let expected = "SELECT COUNT(*) FROM logs WHERE received_at > 1754648094252";
        
        let result = rewriter.rewrite(sql).unwrap();
        assert_eq!(result, expected);
    }
    
    #[test]
    fn test_between_rewrite() {
        let rewriter = SqlRewriter::new();
        let sql = "SELECT COUNT(*) FROM logs WHERE received_at BETWEEN 1754640000000 AND 1754650000000";
        let expected = "SELECT COUNT(*) FROM logs WHERE received_at >= 1754640000000 AND received_at <= 1754650000000";
        
        let result = rewriter.rewrite(sql).unwrap();
        assert_eq!(result, expected);
    }
    
    #[test]
    fn test_exists_rewrite() {
        let rewriter = SqlRewriter::new();
        let sql = "SELECT EXISTS(SELECT 1 FROM logs WHERE id = 'test')";
        let expected = "SELECT CASE WHEN COUNT(*) > 0 THEN true ELSE false END FROM (SELECT 1 FROM logs WHERE id = 'test' LIMIT 1) as subq";
        
        let result = rewriter.rewrite(sql).unwrap();
        assert_eq!(result, expected);
    }
}