//! Rewrites postgres array constructor syntax [0] as a lateral join
//! and coalesce around an `array_agg()`.
//!
//! Each occurrence of an array constructor with a query becomes its own
//! lateral join to avoid cross product confusion.
//!
//! The original subquery is preserved as-is within a nested FROM clause,
//! and we wrap it with an SELECT that applies array_agg(). In order to
//! support PostLookupAggregations (our good old friend):
//! - If query has ORDER BY clauses, copy them to the `array_agg()`.
//! - If query has DISTINCT keyword, copy it into `array_agg()`.
//!
//! Note that we cannot strip out the DISTINCT and ORDER BY from the subquery
//! as they are necessary for correctness if the query contains a LIMIT clause
//! (a/k/a TopK).
//!
//! -- Original query with ARRAY constructor:
//! ```sql
//! SELECT
//!     u.name,
//!     ARRAY(SELECT p.title FROM posts p WHERE p.user_id = u.id) as post_titles
//! FROM users u;
//! ```
//!
//! -- Rewritten using LATERAL join:
//! ```sql
//! SELECT
//!     u.name,
//!     COALESCE(array_subq.agg_result, ARRAY[]) as post_titles
//! FROM users u
//! LEFT JOIN LATERAL (
//!     SELECT array_agg(inner_subq.title) as agg_result
//!     FROM (
//!         SELECT p.title FROM posts p WHERE p.user_id = u.id
//!     ) inner_subq
//! ) array_subq ON true;
//! ```
//!
//! The pattern is consistent regardless of query complexity. The original
//! subquery (with all its clauses: WHERE, ORDER BY, DISTINCT, LIMIT, etc.)
//! is preserved in the inner FROM clause, and we reference its fields via
//! the inner_subq alias.
//!
//! -- Example with ORDER BY and DISTINCT:
//! ```sql
//! SELECT
//!     u.name,
//!     ARRAY(SELECT DISTINCT p.title FROM posts p WHERE p.user_id = u.id ORDER BY p.title) as post_titles
//! FROM users u;
//! ```
//!
//! -- Rewritten to LATERAL:
//! ```sql
//! SELECT
//!     u.name,
//!     COALESCE(array_subq.agg_result, ARRAY[]) as post_titles
//! FROM users u
//! LEFT JOIN LATERAL (
//!     SELECT array_agg(DISTINCT inner_subq.title ORDER BY inner_subq.title) as agg_result
//!     FROM (
//!         SELECT DISTINCT p.title FROM posts p WHERE p.user_id = u.id ORDER BY p.title
//!     ) inner_subq
//! ) array_subq ON true;
//! ```
//!
//! -- Example with LIMIT:
//! ```sql
//! SELECT
//!     u.name,
//!     ARRAY(SELECT p.title FROM posts p WHERE p.user_id = u.id ORDER BY p.created_at DESC LIMIT 3) as recent_titles
//! FROM users u;
//! ```
//!
//! -- Rewritten to LATERAL:
//! ```sql
//! SELECT
//!     u.name,
//!     COALESCE(array_subq.agg_result, ARRAY[]) as recent_titles
//! FROM users u
//! LEFT JOIN LATERAL (
//!     SELECT array_agg(inner_subq.title ORDER BY inner_subq.created_at DESC) as agg_result
//!     FROM (
//!         SELECT p.title
//!         FROM posts p
//!         WHERE p.user_id = u.id
//!         ORDER BY p.created_at DESC
//!         LIMIT 3
//!     ) inner_subq
//! ) array_subq ON true;
//! ```
//!
//! [0] https://www.postgresql.org/docs/17/sql-expressions.html#SQL-SYNTAX-ARRAY-CONSTRUCTORS

use itertools::Either;
use readyset_errors::{ReadySetError, ReadySetResult, unsupported};
use readyset_sql::analysis::visit::{Visitor, walk_expr, walk_select_statement};
use readyset_sql::ast::{
    ArrayArguments, Expr, FieldDefinitionExpr, FieldReference, FunctionExpr, GroupByClause,
    JoinClause, JoinConstraint, JoinOperator, JoinRightSide, Literal, OrderClause, Relation,
    SelectStatement, SqlIdentifier, SqlQuery, TableExpr, TableExprInner,
};
use readyset_sql::{Dialect, DialectDisplay};

use crate::get_local_from_items_iter_mut;
use crate::rewrite_utils::{
    as_sub_query_with_alias_mut, collect_local_from_items,
    default_alias_for_select_item_expression, get_from_item_reference_name, get_unique_alias,
};
use std::iter;
use std::mem;

/// A trait to drive to the rewrite of array constructors with subqueries.
pub trait ArrayConstructorRewrite {
    /// Does the needful.
    fn rewrite_array_constructors(&mut self) -> ReadySetResult<&mut Self>
    where
        Self: Sized;
}

/// Extract field information from the subquery for array constructor rewriting.
/// Validates the subquery has exactly one field and returns its alias.
fn extract_array_subquery_field_info(query: &SelectStatement) -> ReadySetResult<SqlIdentifier> {
    // Validate subquery has exactly one field
    if query.fields.len() != 1 {
        unsupported!("subquery in array constructor must have exactly 1 select field");
    }

    let field = &query.fields[0];
    let field_alias = match field {
        FieldDefinitionExpr::Expr { expr, alias } => alias
            .clone()
            .unwrap_or_else(|| default_alias_for_select_item_expression(expr)),
        _ => unsupported!("array constructor subquery must be an expression field"),
    };

    Ok(field_alias)
}

/// Extract possible ORDER BY clause and update column references to use `inner_subq_alias`.
fn extract_array_subquery_order_by_clause(
    query: &SelectStatement,
    inner_subq_alias: &SqlIdentifier,
) -> ReadySetResult<Option<OrderClause>> {
    // Fix up namespacing of referenced columns in ORDER BY to use inner_subq_alias
    let order_by = if let Some(mut orig) = query.order.clone() {
        // Use try_fold to allow proper error propagation
        let updated_order_by: ReadySetResult<Vec<_>> = orig
            .order_by
            .into_iter()
            .map(|mut o| {
                // Update the field reference to use inner_subq_alias when it's an Expr
                if let FieldReference::Expr(expr) = o.field {
                    if let Expr::Column(col) = expr {
                        o.field = FieldReference::Expr(Expr::Column(readyset_sql::ast::Column {
                            name: col.name,
                            table: Some(inner_subq_alias.clone().into()),
                        }));
                        Ok(o)
                    } else {
                        unsupported!("ORDER BY expr must be a column")
                    }
                } else {
                    Ok(o)
                }
            })
            .collect();

        orig.order_by = updated_order_by?;
        Some(orig)
    } else {
        None
    };

    Ok(order_by)
}

/// Checks if an expression (or any nested expression) contains an array constructor with a subquery.
#[derive(Default)]
struct ArrayConstructorDetector {
    found: bool,
}

impl<'a> Visitor<'a> for ArrayConstructorDetector {
    type Error = ReadySetError;

    fn visit_expr(&mut self, expr: &'a Expr) -> Result<(), Self::Error> {
        match expr {
            Expr::Array(ArrayArguments::Subquery(_)) => {
                self.found = true;
                Ok(())
            }
            _ => walk_expr(self, expr),
        }
    }
}

fn contains_array_constructor_in_expr(expr: &Expr) -> bool {
    let mut detector = ArrayConstructorDetector::default();
    let _ = detector.visit_expr(expr);
    detector.found
}

/// Visitor that validates array constructors are only in allowed locations.
struct ArrayConstructorValidator;

impl<'ast> Visitor<'ast> for ArrayConstructorValidator {
    type Error = ReadySetError;

    fn visit_where_clause(&mut self, expr: &'ast Expr) -> ReadySetResult<()> {
        if contains_array_constructor_in_expr(expr) {
            unsupported!("Array constructor not supported in WHERE clause")
        }
        Ok(())
    }

    fn visit_having_clause(&mut self, expr: &'ast Expr) -> ReadySetResult<()> {
        if contains_array_constructor_in_expr(expr) {
            unsupported!("Array constructor not supported in HAVING clause")
        }
        Ok(())
    }

    fn visit_group_by_clause(&mut self, group_by: &'ast GroupByClause) -> ReadySetResult<()> {
        let mut detector = ArrayConstructorDetector::default();
        let _ = detector.visit_group_by_clause(group_by);
        if detector.found {
            unsupported!("Array constructor not supported in GROUP BY clause");
        }
        Ok(())
    }

    fn visit_order_clause(&mut self, order: &'ast OrderClause) -> ReadySetResult<()> {
        let mut detector = ArrayConstructorDetector::default();
        let _ = detector.visit_order_clause(order);
        if detector.found {
            unsupported!("Array constructor not supported in ORDER BY clause");
        }
        Ok(())
    }

    fn visit_join_constraint(&mut self, constraint: &'ast JoinConstraint) -> ReadySetResult<()> {
        if let JoinConstraint::On(expr) = constraint
            && contains_array_constructor_in_expr(expr)
        {
            unsupported!("Array constructor not supported in ON constraints");
        }
        Ok(())
    }

    fn visit_select_statement(&mut self, stmt: &'ast SelectStatement) -> ReadySetResult<()> {
        // Special handling for SELECT fields: top-level array constructors are OK,
        // but nested ones within other expressions are not
        for field in &stmt.fields {
            if let FieldDefinitionExpr::Expr { expr, .. } = field {
                match expr {
                    Expr::Array(ArrayArguments::Subquery(query)) => {
                        // Top-level array constructor is allowed, but check if the subquery
                        // itself contains nested array constructors
                        let mut detector = ArrayConstructorDetector::default();
                        let _ = detector.visit_select_statement(query);
                        if detector.found {
                            unsupported!(
                                "Nested array constructors (array constructor within array constructor) are not supported"
                            );
                        }
                    }
                    other => {
                        // Check for nested array constructors
                        if contains_array_constructor_in_expr(other) {
                            unsupported!(
                                "Array constructor nested within expressions is not yet supported"
                            );
                        }
                    }
                }
            }
        }

        // Walk the rest of the statement (automatically recurses into subqueries)
        walk_select_statement(self, stmt)
    }
}

/// Validates that array constructors with subqueries are not present in disallowed
/// locations within the SELECT statement. We currently only support array constructors
/// in the SELECT list.
fn validate_no_disallowed_array_constructors(stmt: &SelectStatement) -> ReadySetResult<()> {
    ArrayConstructorValidator.visit_select_statement(stmt)
}

/// Rewrite a single array constructor into a lateral join with `array_agg()`.
/// Returns (new_field_expr, lateral_table) for the rewritten array constructor.
///
/// The `existing_aliases` argument is to allow for multiple array constructors at this level,
/// as we have not added the derived tables into the `stmt` yet.
fn rewrite_single_array_constructor(
    query: Box<SelectStatement>,
    alias: Option<SqlIdentifier>,
    stmt: &SelectStatement,
    existing_aliases: &[Relation],
    in_lateral_context: bool,
) -> ReadySetResult<(FieldDefinitionExpr, TableExpr)> {
    let mut stmt_locals = collect_local_from_items(stmt)?;
    stmt_locals.extend(existing_aliases.iter().cloned());

    let lateral_alias = get_unique_alias(&stmt_locals, "array_subq");
    let inner_subq_alias = "inner_subq".into();

    // Extract field info and process ORDER BY from the original subquery
    let inner_field_alias = extract_array_subquery_field_info(&query)?;
    let inner_order_by = extract_array_subquery_order_by_clause(&query, &inner_subq_alias)?;

    // Build array_agg with DISTINCT and ORDER BY that match the query.
    // DISTINCT and ORDER BY and retained in the query to ensure TopK correctness.
    let inner_col_ref = Expr::Column(readyset_sql::ast::Column {
        name: inner_field_alias.clone(),
        table: Some(inner_subq_alias.clone().into()),
    });

    let array_agg_expr = Expr::Call(FunctionExpr::ArrayAgg {
        expr: Box::new(inner_col_ref),
        distinct: query.distinct.into(),
        order_by: inner_order_by,
    });

    let agg_alias: SqlIdentifier = "agg_result".into();

    // The lateral subquery returns ONE aggregated row.
    // The original subquery is wrapped in a FROM clause with alias inner_subq,
    // and we apply array_agg over it.
    //
    // IMPORTANT: Only mark as LATERAL if we're NOT already inside a LATERAL context.
    // If stmt.lateral == true, we're already inside a LATERAL subquery, so we don't
    // need (and don't want) nested LATERAL - the outer LATERAL handles the correlation.
    let lateral_subquery = SelectStatement {
        fields: vec![FieldDefinitionExpr::Expr {
            expr: array_agg_expr,
            alias: Some(agg_alias.clone()),
        }],
        tables: vec![TableExpr {
            inner: TableExprInner::Subquery(query),
            alias: Some(inner_subq_alias.clone()),
        }],
        lateral: !stmt.lateral && !in_lateral_context, // Only set lateral if not already lateral context
        ..Default::default()
    };

    let lateral_table = TableExpr {
        inner: TableExprInner::Subquery(Box::new(lateral_subquery)),
        alias: Some(lateral_alias.clone()),
    };

    // In the outer SELECT, reference the aggregated result from the lateral join
    // and wrap in COALESCE to match ARRAY constructor behavior:
    // ARRAY(...) returns empty array for no rows, but array_agg returns NULL
    let lateral_result_ref = Expr::Column(readyset_sql::ast::Column {
        name: agg_alias,
        table: Some(lateral_alias.clone().into()),
    });

    let coalesced_expr = Expr::Call(FunctionExpr::Call {
        name: "coalesce".into(),
        arguments: Some(vec![
            lateral_result_ref,
            Expr::Array(ArrayArguments::List(vec![])),
        ]),
    });

    let new_field = FieldDefinitionExpr::Expr {
        expr: coalesced_expr,
        alias,
    };

    Ok((new_field, lateral_table))
}

fn rewrite_array_constructors_in_select(
    stmt: &mut SelectStatement,
    in_lateral_context: bool,
) -> ReadySetResult<()> {
    // if we are already in a lateral context, retain that state;
    // otherwise, check if current statement declares it is lateral.
    let in_lateral_context = in_lateral_context || stmt.lateral;

    // process nested SELECT statements in allowed positions
    for cte in &mut stmt.ctes {
        rewrite_array_constructors_in_select(&mut cte.statement, in_lateral_context)?;
    }

    for t in get_local_from_items_iter_mut!(stmt) {
        if let Some((subquery, _)) = as_sub_query_with_alias_mut(t) {
            rewrite_array_constructors_in_select(subquery, in_lateral_context)?;
        }
    }

    // rewrite array constructors at this level
    let mut new_fields = Vec::new();
    let mut lateral_tables = Vec::new();
    // if there are multiple array constructors at the same level, they all need unique aliases
    let mut lateral_table_aliases = Vec::new();

    for field in mem::take(&mut stmt.fields) {
        match field {
            FieldDefinitionExpr::Expr {
                expr: Expr::Array(ArrayArguments::Subquery(query)),
                alias,
            } => {
                let (new_field, lateral_table) = rewrite_single_array_constructor(
                    query,
                    alias,
                    stmt,
                    &lateral_table_aliases,
                    in_lateral_context,
                )?;
                new_fields.push(new_field);
                lateral_table_aliases.push(get_from_item_reference_name(&lateral_table)?);
                lateral_tables.push(lateral_table);
            }
            other => {
                // Not an array constructor, keep the field as-is
                new_fields.push(other);
            }
        }
    }

    stmt.fields = new_fields;

    // Add lateral tables to the query
    // If there are no existing tables, add the first lateral to FROM, rest become joins
    if stmt.tables.is_empty() && !lateral_tables.is_empty() {
        stmt.tables.push(lateral_tables.remove(0));
    }
    // All remaining laterals become LEFT OUTER JOINs
    for lateral_table in lateral_tables {
        stmt.join.push(JoinClause {
            operator: JoinOperator::LeftOuterJoin,
            right: JoinRightSide::Table(lateral_table),
            constraint: JoinConstraint::On(Expr::Literal(Literal::Boolean(true))),
        });
    }

    Ok(())
}

impl ArrayConstructorRewrite for SelectStatement {
    fn rewrite_array_constructors(&mut self) -> ReadySetResult<&mut Self> {
        tracing::trace!(
            "ArrayConstructorRewrite - BEFORE: {}",
            self.display(Dialect::PostgreSQL)
        );
        // fail fast if array constructors in disallowed locations
        validate_no_disallowed_array_constructors(self)?;

        rewrite_array_constructors_in_select(self, false)?;
        tracing::trace!(
            "ArrayConstructorRewrite - AFTER: {}",
            self.display(Dialect::PostgreSQL)
        );
        Ok(self)
    }
}

impl ArrayConstructorRewrite for SqlQuery {
    fn rewrite_array_constructors(&mut self) -> ReadySetResult<&mut Self> {
        if let SqlQuery::Select(select) = self {
            select.rewrite_array_constructors()?;
        }
        Ok(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use readyset_sql::Dialect;
    use readyset_sql_parsing::{ParsingPreset, parse_select_with_config};

    const PARSING_CONFIG: ParsingPreset = ParsingPreset::OnlySqlparser;

    fn test_validation_error(sql: &str) {
        let stmt = match parse_select_with_config(PARSING_CONFIG, Dialect::PostgreSQL, sql) {
            Ok(stmt) => stmt,
            Err(e) => panic!("PARSE ERROR: {e}"),
        };

        let result = validate_no_disallowed_array_constructors(&stmt);

        assert!(
            result.is_err(),
            "Expected validation error but validation succeeded"
        );
    }

    fn test_validation_succeeds(sql: &str) {
        let stmt = match parse_select_with_config(PARSING_CONFIG, Dialect::PostgreSQL, sql) {
            Ok(stmt) => stmt,
            Err(e) => panic!("PARSE ERROR: {e}"),
        };

        let result = validate_no_disallowed_array_constructors(&stmt);

        assert!(
            result.is_ok(),
            "Expected validation to succeed but got error: {:?}",
            result.unwrap_err()
        );
    }

    #[test]
    fn test_array_ctor_in_where_clause() {
        let sql = r#"
            SELECT name
            FROM foo
            WHERE id = ANY(ARRAY(SELECT id FROM users))
        "#;
        test_validation_error(sql);
    }

    #[test]
    fn test_array_ctor_in_having_clause() {
        let sql = r#"
            SELECT count(*) as cnt
            FROM foo
            GROUP BY name
            HAVING cnt > ANY(ARRAY(SELECT threshold FROM config))
        "#;
        test_validation_error(sql);
    }

    #[test]
    fn test_array_ctor_in_group_by_clause() {
        let sql = r#"
            SELECT name, count(*)
            FROM foo
            GROUP BY ARRAY(SELECT x FROM bar WHERE bar.id = foo.id)
        "#;
        test_validation_error(sql);
    }

    #[test]
    fn test_array_ctor_in_order_by_clause() {
        let sql = r#"
            SELECT name
            FROM foo
            ORDER BY ARRAY(SELECT tag FROM tags WHERE user_id = foo.id)
        "#;
        test_validation_error(sql);
    }

    #[test]
    fn test_array_ctor_in_join_on_constraint() {
        let sql = r#"
            SELECT foo.name
            FROM foo
            JOIN bar ON foo.id = ANY(ARRAY(SELECT user_id FROM user_tags))
        "#;
        test_validation_error(sql);
    }

    #[test]
    fn test_nested_array_ctor_in_select_field() {
        let sql = r#"
            SELECT array_length(ARRAY(SELECT id FROM users), 1)
            FROM foo
        "#;
        test_validation_error(sql);
    }

    #[test]
    fn test_top_level_array_ctor_in_select_field() {
        let sql = r#"
            SELECT ARRAY(SELECT id FROM users) as user_ids
            FROM foo
        "#;
        test_validation_succeeds(sql);
    }

    #[test]
    fn test_array_ctor_in_nested_subquery_where_clause() {
        let sql = r#"
            SELECT name
            FROM (
                SELECT id, name
                FROM bar
                WHERE id = ANY(ARRAY(SELECT user_id FROM banned))
            ) AS sub
        "#;
        test_validation_error(sql);
    }

    #[test]
    fn test_multiple_top_level_array_ctors() {
        let sql = r#"
            SELECT
                ARRAY(SELECT tag FROM tags WHERE user_id = 1) as tags1,
                ARRAY(SELECT tag FROM tags WHERE user_id = 2) as tags2
            FROM foo
        "#;
        test_validation_succeeds(sql);
    }

    #[test]
    fn test_nested_array_ctor_in_group_by() {
        let sql = r#"
            SELECT count(*)
            FROM foo
            GROUP BY array_length(ARRAY(SELECT x FROM bar WHERE bar.id = foo.id), 1)
        "#;
        test_validation_error(sql);
    }

    #[test]
    fn test_nested_array_ctor_in_order_by() {
        let sql = r#"
            SELECT name
            FROM foo
            ORDER BY array_length(ARRAY(SELECT tag FROM tags WHERE user_id = foo.id), 1)
        "#;
        test_validation_error(sql);
    }

    #[test]
    fn test_array_ctor_in_where_with_and() {
        let sql = r#"
            SELECT name
            FROM foo
            WHERE active = true AND id = ANY(ARRAY(SELECT user_id FROM allowed))
        "#;
        test_validation_error(sql);
    }

    #[test]
    fn test_array_ctor_in_where_case_expression() {
        let sql = r#"
            SELECT name
            FROM foo
            WHERE CASE WHEN status = 'active'
                       THEN id = ANY(ARRAY(SELECT user_id FROM users))
                       ELSE false END
        "#;
        test_validation_error(sql);
    }

    #[test]
    fn test_array_ctor_in_having_with_aggregates() {
        let sql = r#"
            SELECT user_id, count(*)
            FROM posts
            GROUP BY user_id
            HAVING count(*) > ANY(ARRAY(SELECT threshold FROM quotas))
        "#;
        test_validation_error(sql);
    }

    #[test]
    fn test_array_ctor_in_having_case_expression() {
        let sql = r#"
            SELECT category, count(*) as cnt
            FROM items
            GROUP BY category
            HAVING CASE WHEN category = 'special'
                        THEN cnt > ANY(ARRAY(SELECT threshold FROM config))
                        ELSE cnt > 10 END
        "#;
        test_validation_error(sql);
    }

    #[test]
    fn test_array_ctor_in_multiple_group_by_fields() {
        let sql = r#"
            SELECT name, count(*)
            FROM foo
            GROUP BY name, ARRAY(SELECT tag FROM tags WHERE user_id = foo.id)
        "#;
        test_validation_error(sql);
    }

    #[test]
    fn test_array_ctor_in_group_by_case_expression() {
        let sql = r#"
            SELECT count(*)
            FROM foo
            GROUP BY CASE WHEN active THEN ARRAY(SELECT id FROM users) ELSE ARRAY[1] END
        "#;
        test_validation_error(sql);
    }

    #[test]
    fn test_array_ctor_in_order_by_with_desc() {
        let sql = r#"
            SELECT name
            FROM foo
            ORDER BY ARRAY(SELECT score FROM scores WHERE user_id = foo.id) DESC
        "#;
        test_validation_error(sql);
    }

    #[test]
    fn test_array_ctor_in_multiple_order_by_fields() {
        let sql = r#"
            SELECT name
            FROM foo
            ORDER BY name ASC, ARRAY(SELECT tag FROM tags WHERE user_id = foo.id) DESC
        "#;
        test_validation_error(sql);
    }

    #[test]
    fn test_array_ctor_in_order_by_case_expression() {
        let sql = r#"
            SELECT name
            FROM foo
            ORDER BY CASE WHEN active
                          THEN ARRAY(SELECT priority FROM priorities WHERE user_id = foo.id)
                          ELSE ARRAY[0] END
        "#;
        test_validation_error(sql);
    }

    #[test]
    fn test_array_ctor_in_left_join_on() {
        let sql = r#"
            SELECT foo.name
            FROM foo
            LEFT JOIN bar ON foo.id = ANY(ARRAY(SELECT user_id FROM mappings))
        "#;
        test_validation_error(sql);
    }

    #[test]
    fn test_array_ctor_in_join_on_complex_condition() {
        let sql = r#"
            SELECT foo.name
            FROM foo
            JOIN bar ON foo.id = bar.id AND bar.user_id = ANY(ARRAY(SELECT user_id FROM allowed_users))
        "#;
        test_validation_error(sql);
    }

    #[test]
    fn test_array_ctor_in_multiple_joins() {
        let sql = r#"
            SELECT foo.name
            FROM foo
            JOIN bar ON foo.id = ANY(ARRAY(SELECT id FROM set1))
            JOIN baz ON bar.id = ANY(ARRAY(SELECT id FROM set2))
        "#;
        test_validation_error(sql);
    }

    #[test]
    fn test_nested_array_ctor_in_binary_op() {
        let sql = r#"
            SELECT ARRAY(SELECT id FROM users) || ARRAY[1, 2, 3]
            FROM foo
        "#;
        test_validation_error(sql);
    }

    #[test]
    fn test_nested_array_ctor_in_case_when() {
        let sql = r#"
            SELECT CASE WHEN active
                        THEN ARRAY(SELECT id FROM active_users)
                        ELSE ARRAY[0] END as user_ids
            FROM foo
        "#;
        test_validation_error(sql);
    }

    #[test]
    fn test_nested_array_ctor_in_cast() {
        let sql = r#"
            SELECT CAST(ARRAY(SELECT id FROM users) AS text[])
            FROM foo
        "#;
        test_validation_error(sql);
    }

    #[test]
    fn test_nested_array_ctor_in_coalesce() {
        let sql = r#"
            SELECT COALESCE(ARRAY(SELECT id FROM users), ARRAY[0])
            FROM foo
        "#;
        test_validation_error(sql);
    }

    #[test]
    fn test_array_ctor_in_cte_where_clause() {
        let sql = r#"
            WITH filtered AS (
                SELECT id, name
                FROM users
                WHERE id = ANY(ARRAY(SELECT user_id FROM allowed))
            )
            SELECT * FROM filtered
        "#;
        test_validation_error(sql);
    }

    #[test]
    fn test_array_ctor_in_deeply_nested_subquery() {
        let sql = r#"
            SELECT name
            FROM (
                SELECT id, name
                FROM (
                    SELECT id, name
                    FROM users
                    WHERE id = ANY(ARRAY(SELECT user_id FROM banned))
                ) AS inner_sub
            ) AS outer_sub
        "#;
        test_validation_error(sql);
    }

    #[test]
    fn test_array_ctor_in_subquery_join() {
        let sql = r#"
            SELECT foo.name
            FROM foo
            JOIN (
                SELECT id
                FROM bar
                WHERE id = ANY(ARRAY(SELECT user_id FROM filtered))
            ) AS sub ON foo.id = sub.id
        "#;
        test_validation_error(sql);
    }

    #[test]
    fn test_nested_array_ctor_in_aggregate_function() {
        let sql = r#"
            SELECT count(ARRAY(SELECT id FROM users))
            FROM foo
        "#;
        test_validation_error(sql);
    }

    #[test]
    fn test_nested_array_ctor_in_window_function() {
        let sql = r#"
            SELECT row_number() OVER (ORDER BY ARRAY(SELECT score FROM scores WHERE user_id = foo.id))
            FROM foo
        "#;
        test_validation_error(sql);
    }

    #[test]
    fn test_multiple_nested_array_ctors_in_select() {
        let sql = r#"
            SELECT
                array_length(ARRAY(SELECT id FROM users), 1) as len1,
                array_length(ARRAY(SELECT tag FROM tags), 1) as len2
            FROM foo
        "#;
        test_validation_error(sql);
    }

    #[test]
    fn test_array_ctor_in_where_exists() {
        let sql = r#"
            SELECT name
            FROM foo
            WHERE EXISTS (SELECT 1 FROM bar WHERE bar.id = ANY(ARRAY(SELECT user_id FROM allowed)))
        "#;
        test_validation_error(sql);
    }

    #[test]
    fn test_array_ctor_in_where_in_subquery() {
        let sql = r#"
            SELECT name
            FROM foo
            WHERE id IN (SELECT id FROM bar WHERE user_id = ANY(ARRAY(SELECT id FROM users)))
        "#;
        test_validation_error(sql);
    }

    #[test]
    fn test_deeply_nested_array_ctor_in_expression() {
        let sql = r#"
            SELECT upper(concat('user_', array_to_string(ARRAY(SELECT name FROM users), ',')))
            FROM foo
        "#;
        test_validation_error(sql);
    }

    #[test]
    fn test_array_ctor_within_array_ctor() {
        let sql = r#"
            SELECT ARRAY(SELECT ARRAY(SELECT id FROM inner_table) FROM outer_table)
            FROM foo
        "#;
        test_validation_error(sql);
    }

    #[test]
    fn test_array_ctor_within_array_ctor_in_where() {
        let sql = r#"
            SELECT ARRAY(
                SELECT name
                FROM users
                WHERE id = ANY(ARRAY(SELECT user_id FROM allowed))
            )
            FROM foo
        "#;
        test_validation_error(sql);
    }

    #[test]
    fn test_correlated_array_ctor() {
        // Matches the basic example from documentation
        let sql = r#"
            SELECT
                u.name,
                ARRAY(SELECT p.title FROM posts p WHERE p.user_id = u.id) as post_titles
            FROM users u
        "#;
        test_validation_succeeds(sql);
    }

    #[test]
    fn test_array_ctor_with_distinct() {
        // Matches the DISTINCT example from documentation
        let sql = r#"
            SELECT
                u.name,
                ARRAY(SELECT DISTINCT p.title FROM posts p WHERE p.user_id = u.id) as post_titles
            FROM users u
        "#;
        test_validation_succeeds(sql);
    }

    #[test]
    fn test_correlated_array_ctor_with_where() {
        let sql = r#"
            SELECT
                d.name,
                ARRAY(SELECT tag FROM tags WHERE d_id = d.id) as tags
            FROM dogs d
        "#;
        test_validation_succeeds(sql);
    }

    #[test]
    fn test_array_ctor_with_distinct_and_correlation() {
        let sql = r#"
            SELECT
                d.id,
                d.name,
                ARRAY(SELECT DISTINCT tag FROM tags WHERE d_id = d.id) as unique_tags
            FROM dogs d
        "#;
        test_validation_succeeds(sql);
    }
}
