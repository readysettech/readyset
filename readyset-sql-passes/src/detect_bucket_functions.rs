use readyset_errors::{ReadySetError, ReadySetResult, invalid_query};
use readyset_sql::analysis::visit::{Visitor, walk_function_expr};
use readyset_sql::ast::{CacheInner, CreateCacheStatement, FunctionExpr, SelectStatement};

/// A trait for detecting bucket functions in queries and validating that the ALWAYS keyword
/// is present. This is because the bucket function is Readyset-specific; it doesn't exist
/// upstream.
pub trait DetectBucketFunctions {
    fn detect_and_validate_bucket_always(&self) -> ReadySetResult<()>;
}

impl DetectBucketFunctions for CreateCacheStatement {
    fn detect_and_validate_bucket_always(&self) -> ReadySetResult<()> {
        let has_bucket_function = match &self.inner {
            CacheInner::Statement {
                deep: Ok(select_stmt),
                ..
            } => has_bucket_function_in_select(select_stmt)?,
            CacheInner::Statement { deep: Err(_), .. } => return Ok(()),
            // shouldn't be possible since bucket is not an upstream function and therefor can't
            // have an ID reference.
            CacheInner::Id(_) => return Ok(()),
        };

        if has_bucket_function && !self.always {
            invalid_query!(
                "CREATE CACHE statements containing Bucket function must use the ALWAYS keyword (CREATE CACHE ALWAYS ...)"
            )
        }

        Ok(())
    }
}

fn has_bucket_function_in_select(stmt: &SelectStatement) -> ReadySetResult<bool> {
    let mut visitor = BucketFunctionVisitor::new();
    visitor.visit_select_statement(stmt)?;
    Ok(visitor.found_bucket)
}

/// Visitor that searches for the `bucket` function in the AST
struct BucketFunctionVisitor {
    found_bucket: bool,
}

impl BucketFunctionVisitor {
    fn new() -> Self {
        Self {
            found_bucket: false,
        }
    }
}

impl<'ast> Visitor<'ast> for BucketFunctionVisitor {
    type Error = ReadySetError;

    fn visit_function_expr(
        &mut self,
        function_expr: &'ast FunctionExpr,
    ) -> Result<(), Self::Error> {
        if matches!(function_expr, FunctionExpr::Bucket { .. }) {
            self.found_bucket = true;
        }

        walk_function_expr(self, function_expr)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use readyset_sql::ast::{
        CacheInner, Column, Expr, FieldDefinitionExpr, Literal, Relation, SqlIdentifier, TableExpr,
        TableExprInner,
    };

    #[test]
    fn test_bucket_function_detection() {
        let bucket_expr = Expr::Call(FunctionExpr::Bucket {
            expr: Box::new(Expr::Column(readyset_sql::ast::Column {
                name: SqlIdentifier::from("timestamp_col"),
                table: None,
            })),
            interval: Box::new(Expr::Literal(readyset_sql::ast::Literal::String(
                "1 hour".to_string(),
            ))),
        });

        let select_stmt = SelectStatement {
            fields: vec![FieldDefinitionExpr::Expr {
                expr: bucket_expr,
                alias: Some(SqlIdentifier::from("bucket_result")),
            }],
            tables: vec![TableExpr {
                inner: TableExprInner::Table(Relation {
                    schema: None,
                    name: SqlIdentifier::from("test_table"),
                }),
                alias: None,
            }],
            ..Default::default()
        };

        let has_bucket = has_bucket_function_in_select(&select_stmt).unwrap();
        assert!(has_bucket, "Should detect bucket function in SELECT clause");
    }

    #[test]
    fn test_create_cache_bucket_without_always_fails() {
        let bucket_expr = Expr::Call(FunctionExpr::Bucket {
            expr: Box::new(Expr::Column(readyset_sql::ast::Column {
                name: SqlIdentifier::from("event_time"),
                table: None,
            })),
            interval: Box::new(Expr::Literal(readyset_sql::ast::Literal::String(
                "1 hour".to_string(),
            ))),
        });

        let select_stmt = SelectStatement {
            fields: vec![FieldDefinitionExpr::Expr {
                expr: bucket_expr,
                alias: Some(SqlIdentifier::from("hour_bucket")),
            }],
            tables: vec![TableExpr {
                inner: TableExprInner::Table(Relation {
                    schema: None,
                    name: SqlIdentifier::from("test_table"),
                }),
                alias: None,
            }],
            ..Default::default()
        };

        let cache_stmt = CreateCacheStatement {
            name: Some(readyset_sql::ast::Relation {
                schema: None,
                name: SqlIdentifier::from("test_cache"),
            }),
            cache_type: None,
            policy: None,
            coalesce_ms: None,
            inner: CacheInner::Statement {
                deep: Ok(Box::new(select_stmt)),
                shallow: Err("shallow".to_string()),
            },
            unparsed_create_cache_statement: None,
            always: false, // should cause error
            concurrently: false,
        };

        let result = cache_stmt.detect_and_validate_bucket_always();
        assert!(
            result.is_err(),
            "Should fail when bucket function is present but ALWAYS is false"
        );

        let error = result.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("Bucket function must use the ALWAYS keyword")
        );
    }

    #[test]
    fn test_create_cache_bucket_with_always_succeeds() {
        let bucket_expr = Expr::Call(FunctionExpr::Bucket {
            expr: Box::new(Expr::Column(readyset_sql::ast::Column {
                name: SqlIdentifier::from("event_time"),
                table: None,
            })),
            interval: Box::new(Expr::Literal(readyset_sql::ast::Literal::String(
                "1 hour".to_string(),
            ))),
        });

        let select_stmt = SelectStatement {
            fields: vec![FieldDefinitionExpr::Expr {
                expr: bucket_expr,
                alias: Some(SqlIdentifier::from("hour_bucket")),
            }],
            tables: vec![TableExpr {
                inner: TableExprInner::Table(Relation {
                    schema: None,
                    name: SqlIdentifier::from("test_table"),
                }),
                alias: None,
            }],
            ..Default::default()
        };

        let cache_stmt = CreateCacheStatement {
            name: Some(readyset_sql::ast::Relation {
                schema: None,
                name: SqlIdentifier::from("test_cache"),
            }),
            cache_type: None,
            policy: None,
            coalesce_ms: None,
            inner: CacheInner::Statement {
                deep: Ok(Box::new(select_stmt)),
                shallow: Err("shallow".to_string()),
            },
            unparsed_create_cache_statement: None,
            always: true, // should succeed
            concurrently: false,
        };

        let result = cache_stmt.detect_and_validate_bucket_always();
        assert!(
            result.is_ok(),
            "Should succeed when bucket function is present and ALWAYS is true"
        );
    }

    #[test]
    fn test_create_cache_without_bucket_function() {
        let regular_expr = Expr::Column(readyset_sql::ast::Column {
            name: SqlIdentifier::from("regular_column"),
            table: None,
        });

        let select_stmt = SelectStatement {
            fields: vec![FieldDefinitionExpr::Expr {
                expr: regular_expr,
                alias: Some(SqlIdentifier::from("col_alias")),
            }],
            tables: vec![TableExpr {
                inner: TableExprInner::Table(Relation {
                    schema: None,
                    name: SqlIdentifier::from("test_table"),
                }),
                alias: None,
            }],
            ..Default::default()
        };

        let cache_stmt = CreateCacheStatement {
            name: Some(readyset_sql::ast::Relation {
                schema: None,
                name: SqlIdentifier::from("test_cache"),
            }),
            cache_type: None,
            policy: None,
            coalesce_ms: None,
            inner: CacheInner::Statement {
                deep: Ok(Box::new(select_stmt)),
                shallow: Err("shallow".to_string()),
            },
            unparsed_create_cache_statement: None,
            always: false,
            concurrently: false,
        };

        let result = cache_stmt.detect_and_validate_bucket_always();
        assert!(
            result.is_ok(),
            "Should succeed when no bucket function is present, regardless of ALWAYS setting"
        );

        assert!(
            !cache_stmt.always,
            "ALWAYS should remain unchanged when no bucket function is detected"
        );
    }

    #[test]
    fn test_bucket_function_in_where_clause() {
        let bucket_expr = Expr::Call(FunctionExpr::Bucket {
            expr: Box::new(Expr::Column(readyset_sql::ast::Column {
                name: SqlIdentifier::from("event_time"),
                table: None,
            })),
            interval: Box::new(Expr::Literal(Literal::String("1 day".to_string()))),
        });

        let where_condition = Expr::BinaryOp {
            lhs: Box::new(bucket_expr),
            op: readyset_sql::ast::BinaryOperator::Equal,
            rhs: Box::new(Expr::Literal(Literal::String("2023-01-01".to_string()))),
        };

        let select_stmt = SelectStatement {
            fields: vec![FieldDefinitionExpr::Expr {
                expr: Expr::Column(Column {
                    name: SqlIdentifier::from("count_col"),
                    table: None,
                }),
                alias: Some(SqlIdentifier::from("count")),
            }],
            tables: vec![TableExpr {
                inner: TableExprInner::Table(Relation {
                    schema: None,
                    name: SqlIdentifier::from("events"),
                }),
                alias: None,
            }],
            where_clause: Some(where_condition),
            ..Default::default()
        };

        let has_bucket = has_bucket_function_in_select(&select_stmt).unwrap();
        assert!(has_bucket, "Should detect bucket function in WHERE clause");
    }
}
