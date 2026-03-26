//! Pre-pipeline semantic validation pass.
//!
//! Checks SQL semantic rules that the rewrite pipeline assumes hold.  Must run
//! **after** the normalization passes (`resolve_schemas`, `expand_stars`,
//! `expand_implied_tables`, `expand_join_on_using`) and **before** the main
//! rewrite pipeline (`derived_tables_rewrite`, etc.).
//!
//! This is purely a read-only check — it never mutates the AST.
//!
//! ## Checks performed (in execution order)
//!
//! 1. **No duplicate FROM aliases** — each effective alias in a scope's FROM
//!    clause must be unique.
//! 2. **Supported join operators** — only INNER and LEFT OUTER joins are
//!    accepted; RIGHT JOIN is rejected early.
//! 3. **No aggregates in WHERE** — aggregate functions belong in HAVING,
//!    not WHERE.  Aggregates inside subqueries in WHERE are fine (independent
//!    scope).
//! 4. **No nested aggregates** — `AVG(SUM(x))` is invalid in standard SQL.
//!    Checked across all top-level expressions (SELECT, JOIN ON, WHERE,
//!    HAVING, GROUP BY, ORDER BY).
//! 5. **No subqueries in JOIN ON** — the rewrite pipeline does not handle
//!    subqueries in ON position (`unnest_subqueries` only processes WHERE,
//!    SELECT, and LATERAL FROM).
//! 6. **No aggregates in JOIN ON** — aggregate functions in ON predicates are
//!    invalid SQL and would confuse join normalization.
//! 7. **GROUP BY semantic validation** — in an aggregate query, every column
//!    reference outside an aggregate function in SELECT, HAVING, and ORDER BY
//!    must appear in the GROUP BY clause (or match a GROUP BY expression).
//!    HAVING and ORDER BY have three additional exemptions: (a) sub-expressions
//!    that structurally match a GROUP BY expression cover their internal
//!    columns, (b) unqualified columns matching SELECT aliases are treated as
//!    alias references, and (c) outer-scope (correlated) columns are constant
//!    per group and always valid.  An aggregate in ORDER BY (without GROUP BY
//!    or aggregates in SELECT) also triggers aggregate-query classification.
//! 8. **No duplicate derived-table column names** — each projected column in a
//!    derived table must have a unique effective name.

use std::collections::HashSet;
use std::iter;

use itertools::Either;
use readyset_errors::{ReadySetError, ReadySetResult, invalid_query, unsupported};
use readyset_sql::analysis::is_aggregate;
use readyset_sql::analysis::visit::{
    Visitor, walk_expr, walk_function_expr, walk_select_statement,
};
use readyset_sql::ast::{
    Column, Expr, FieldDefinitionExpr, FieldReference, FunctionExpr, JoinConstraint, JoinOperator,
    JoinRightSide, Relation, SelectStatement, SqlIdentifier, SqlQuery,
};
use readyset_sql::{Dialect, DialectDisplay};

use crate::rewrite_utils::{
    alias_for_expr, as_sub_query_with_alias, collect_local_from_items, columns_iter,
    contains_select, deep_columns_visitor, for_each_aggregate, for_each_window_function,
    get_from_item_reference_name, is_aggregated_expr, is_aggregated_select, outermost_expression,
    resolve_group_by_exprs,
};
use crate::{as_column, get_local_from_items_iter, is_column_of};

pub trait ValidateQuerySemantics: Sized {
    /// Check that the AST satisfies SQL semantic rules required by the
    /// downstream rewrite pipeline.  Returns `Ok(())` on success, or an
    /// error describing the first violation found.
    fn validate_query_semantics(&self, dialect: Dialect) -> ReadySetResult<()>;
}

impl ValidateQuerySemantics for SelectStatement {
    fn validate_query_semantics(&self, dialect: Dialect) -> ReadySetResult<()> {
        let mut visitor = SemanticValidator { dialect };
        visitor.visit_select_statement(self)
    }
}

impl ValidateQuerySemantics for SqlQuery {
    fn validate_query_semantics(&self, dialect: Dialect) -> ReadySetResult<()> {
        match self {
            SqlQuery::Select(stmt) => stmt.validate_query_semantics(dialect),
            SqlQuery::CompoundSelect(csq) => {
                for (_op, stmt) in &csq.selects {
                    stmt.validate_query_semantics(dialect)?;
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }
}

// ─── Visitor ────────────────────────────────────────────────────────────────

#[derive(Debug)]
struct SemanticValidator {
    dialect: Dialect,
}

impl<'ast> Visitor<'ast> for SemanticValidator {
    type Error = ReadySetError;

    fn visit_select_statement(
        &mut self,
        select_statement: &'ast SelectStatement,
    ) -> Result<(), Self::Error> {
        validate_no_duplicate_from_aliases(select_statement, self.dialect)?;
        validate_join_operators(select_statement)?;
        validate_no_aggregates_in_where(select_statement)?;
        validate_no_nested_aggregates(select_statement)?;
        validate_no_subqueries_in_join_on(select_statement)?;
        validate_no_aggregates_in_join_on(select_statement)?;
        validate_group_by_semantics(select_statement, self.dialect)?;
        validate_no_duplicate_derived_table_columns(select_statement, self.dialect)?;

        // Walk children — this recurses into subqueries (FROM, WHERE, etc.)
        walk_select_statement(self, select_statement)
    }
}

// ─── Supported join operators ────────────────────────────────────────────────

/// Rejects join operators that ReadySet does not support.  Only INNER
/// (including `JOIN`, `CROSS JOIN`, `STRAIGHT_JOIN`) and LEFT OUTER are
/// accepted.
fn validate_join_operators(stmt: &SelectStatement) -> ReadySetResult<()> {
    for jc in &stmt.join {
        match jc.operator {
            JoinOperator::Join
            | JoinOperator::InnerJoin
            | JoinOperator::CrossJoin
            | JoinOperator::StraightJoin
            | JoinOperator::LeftJoin
            | JoinOperator::LeftOuterJoin => {}
            JoinOperator::RightJoin | JoinOperator::RightOuterJoin => {
                unsupported!("RIGHT JOIN is not supported");
            }
        }
    }
    Ok(())
}

// ─── No aggregates in WHERE ─────────────────────────────────────────────────

/// Aggregate functions are not allowed in WHERE — they belong in HAVING.
/// Standard SQL forbids this; sqlparser does not enforce it.
fn validate_no_aggregates_in_where(stmt: &SelectStatement) -> ReadySetResult<()> {
    if let Some(where_expr) = &stmt.where_clause
        && contains_aggregate_outside_subqueries(where_expr)
    {
        invalid_query!("aggregate functions are not allowed in WHERE (use HAVING instead)");
    }
    Ok(())
}

// ─── No nested aggregates ──────────────────────────────────────────────────

/// Nested aggregate calls like `AVG(SUM(x))` are invalid in standard SQL.
/// Checks all top-level expressions (SELECT, JOIN ON, WHERE, HAVING, GROUP BY,
/// ORDER BY) — nested aggregates are invalid everywhere.
///
/// Aggregates inside window function boundaries are excluded: `SUM(SUM(x)) OVER()`
/// is valid SQL — the inner SUM is a GROUP BY aggregate, the outer is a window
/// function.  `for_each_aggregate` with `visit_window_functions: false` handles
/// this by skipping `Expr::WindowFunction` subtrees.
fn validate_no_nested_aggregates(stmt: &SelectStatement) -> ReadySetResult<()> {
    /// Returns true if any aggregate's arguments contain another aggregate.
    /// Uses `visit_window_functions = false` so WF boundaries are not crossed.
    fn has_nested(expr: &Expr) -> bool {
        let mut found = false;
        let _ = for_each_aggregate(expr, false, &mut |agg| {
            if agg
                .arguments()
                .any(|a| is_aggregated_expr(a).unwrap_or(false))
            {
                found = true;
            }
        });
        found
    }

    for expr in outermost_expression(stmt) {
        if has_nested(expr) {
            invalid_query!("aggregate function calls cannot be nested");
        }

        // Inside WF arguments: the WF's own function is not an aggregate
        // level, but its args / partition_by / order_by must not contain
        // nested aggregates (e.g., SUM(SUM(x)) OVER() is valid but
        // SUM(AVG(SUM(x))) OVER() is not).
        let mut nested_in_wf = false;
        let _ = for_each_window_function(expr, &mut |wf| {
            if let Expr::WindowFunction {
                function,
                partition_by,
                order_by,
            } = wf
            {
                nested_in_wf |= function.arguments().any(has_nested)
                    || partition_by.iter().any(has_nested)
                    || order_by.iter().any(|(o, _, _)| has_nested(o));
            }
        });
        if nested_in_wf {
            invalid_query!("aggregate function calls cannot be nested");
        }
    }
    Ok(())
}

// ─── No subqueries in JOIN ON ──────────────────────────────────────────────

/// Subqueries in JOIN ON position are not supported by the rewrite pipeline.
/// `unnest_subqueries` only handles subqueries in WHERE, SELECT, and LATERAL
/// FROM — not in ON clauses.  See `known_core_limitations.md` §4.1 and §13.
fn validate_no_subqueries_in_join_on(stmt: &SelectStatement) -> ReadySetResult<()> {
    for jc in &stmt.join {
        if let JoinConstraint::On(on_expr) = &jc.constraint
            && contains_select(on_expr)
        {
            unsupported!("subqueries in JOIN ON are not supported");
        }
    }
    Ok(())
}

// ─── No aggregates in JOIN ON ──────────────────────────────────────────────

/// Aggregate functions in JOIN ON are invalid SQL.  The pipeline's join
/// normalization (`classify_on_atom`, `is_supported_join_condition`) does not
/// expect aggregates in ON predicates.
fn validate_no_aggregates_in_join_on(stmt: &SelectStatement) -> ReadySetResult<()> {
    for jc in &stmt.join {
        if let JoinConstraint::On(on_expr) = &jc.constraint
            && contains_aggregate_outside_subqueries(on_expr)
        {
            invalid_query!("aggregate functions are not allowed in JOIN ON");
        }
    }
    Ok(())
}

/// Returns `true` if `expr` contains an aggregate function call, ignoring
/// any aggregates nested inside subqueries (which have independent scope).
/// Window functions are not visited — they are already rejected outside
/// SELECT by `validate_window_functions` which runs before this validator.
fn contains_aggregate_outside_subqueries(expr: &Expr) -> bool {
    is_aggregated_expr(expr).unwrap_or(false)
}

// ─── No duplicate FROM aliases ──────────────────────────────────────────────

fn validate_no_duplicate_from_aliases(
    stmt: &SelectStatement,
    dialect: Dialect,
) -> ReadySetResult<()> {
    let mut seen = HashSet::new();
    for te in get_local_from_items_iter!(stmt) {
        let name = get_from_item_reference_name(te)?;
        if !seen.insert(name.clone()) {
            invalid_query!("Not unique table/alias: {}", name.display(dialect));
        }
    }
    Ok(())
}

// ─── GROUP BY semantic validation ───────────────────────────────────────────

/// Validates that all non-aggregated column references in SELECT, HAVING,
/// and ORDER BY appear in the GROUP BY clause (either as direct columns or
/// as matching expressions).
///
/// HAVING and ORDER BY validation is more permissive than SELECT — see
/// [`validate_having_group_by`] for the additional exemptions (SELECT alias
/// references, GROUP BY sub-expression matching, outer-scope columns).
///
/// Only applies to aggregate queries — those with GROUP BY, HAVING, or
/// aggregate functions in the SELECT list or ORDER BY (`is_aggregated_select`).
fn validate_group_by_semantics(stmt: &SelectStatement, dialect: Dialect) -> ReadySetResult<()> {
    // Use `is_aggregated_select` which checks SELECT, HAVING, AND ORDER BY —
    // not just `contains_aggregate_select()` which only checks SELECT.
    // A query like `SELECT x FROM t ORDER BY AVG(x)` is an aggregate query.
    let is_aggregate_query =
        stmt.group_by.is_some() || stmt.having.is_some() || is_aggregated_select(stmt)?;

    if !is_aggregate_query {
        return Ok(());
    }

    let gb_exprs = resolve_group_by_exprs(stmt)?;
    let gb_columns: HashSet<&Column> = gb_exprs
        .iter()
        .filter_map(|e| match e {
            Expr::Column(col) => Some(col),
            _ => None,
        })
        .collect();

    // ── Check SELECT fields ──
    for field in &stmt.fields {
        if let FieldDefinitionExpr::Expr { expr, .. } = field {
            validate_expr_group_by(expr, &gb_exprs, &gb_columns, dialect)?;
        }
    }

    // Shared context for HAVING and ORDER BY: both allow SELECT alias references,
    // GROUP BY sub-expression matching, and outer-scope (correlated) columns.
    let select_aliases: HashSet<&SqlIdentifier> = stmt
        .fields
        .iter()
        .filter_map(|f| match f {
            FieldDefinitionExpr::Expr {
                alias: Some(alias), ..
            } => Some(alias),
            _ => None,
        })
        .collect();
    let local_tables = collect_local_from_items(stmt)?;

    // ── Check HAVING ──
    // HAVING validation has three exemptions beyond the SELECT-list rules:
    // (1) unqualified columns matching SELECT aliases are alias references,
    // (2) sub-expressions matching GROUP BY keys cover their internal columns,
    // (3) outer-scope (correlated) columns are constant per group.
    if let Some(having) = &stmt.having {
        validate_having_group_by(
            having,
            &gb_exprs,
            &gb_columns,
            &select_aliases,
            &local_tables,
            dialect,
        )?;
    }

    // ── Check ORDER BY ──
    // ORDER BY follows the same rules as HAVING: non-aggregated columns must
    // be GROUP BY keys, SELECT alias references are allowed, and GROUP BY
    // sub-expression matching applies.
    //
    // MySQL exception: MySQL allows ORDER BY with non-grouped columns in
    // aggregate queries — it picks an arbitrary value from the group.
    // PostgreSQL rejects this.  Skip ORDER BY validation for MySQL.
    if !matches!(dialect, Dialect::MySQL)
        && let Some(order) = &stmt.order
    {
        for ob in &order.order_by {
            if let FieldReference::Expr(expr) = &ob.field {
                validate_having_group_by(
                    expr,
                    &gb_exprs,
                    &gb_columns,
                    &select_aliases,
                    &local_tables,
                    dialect,
                )?;
            }
        }
    }

    Ok(())
}

/// Validates a SELECT expression against the GROUP BY targets.
///
/// If the expression structurally matches a GROUP BY expression, it is
/// accepted.  Otherwise, every column reference outside an aggregate must
/// individually appear in the GROUP BY column set.
fn validate_expr_group_by(
    expr: &Expr,
    gb_exprs: &[Expr],
    gb_columns: &HashSet<&Column>,
    dialect: Dialect,
) -> ReadySetResult<()> {
    // Expression-level match — e.g. SELECT t.x + 1 ... GROUP BY t.x + 1
    if gb_exprs.contains(expr) {
        return Ok(());
    }

    // Column-level check — collect non-aggregated columns
    for col in collect_non_aggregated_columns(expr, None) {
        if !gb_columns.contains(col) {
            invalid_query!(
                "column {} must appear in the GROUP BY clause or be used in an aggregate function",
                col.display(dialect)
            );
        }
    }

    Ok(())
}

/// Like `validate_expr_group_by`, but with additional exemptions for columns
/// that are valid in HAVING and ORDER BY despite not appearing directly in
/// `gb_columns`.  Used for both HAVING and ORDER BY validation.
///
/// Exemptions beyond the SELECT-list rules:
///
/// 1. **SELECT alias references** — unqualified columns matching a SELECT
///    alias are alias references to already-validated expressions.
///
/// 2. **Outer-scope (correlated) columns** — columns whose table is not in
///    the local FROM scope are constant per group (they come from an outer
///    row) and are always valid.
///
/// 3. **GROUP BY sub-expression matching** — before decomposing to individual
///    columns, the function checks whether sub-expressions structurally match
///    a GROUP BY expression.  This handles cases like
///    `GROUP BY t.x + t.y HAVING t.x + t.y > 10`, where neither `t.x` nor
///    `t.y` is individually a GROUP BY column, but the composite expression is.
fn validate_having_group_by(
    expr: &Expr,
    gb_exprs: &[Expr],
    gb_columns: &HashSet<&Column>,
    select_aliases: &HashSet<&SqlIdentifier>,
    local_tables: &HashSet<Relation>,
    dialect: Dialect,
) -> ReadySetResult<()> {
    // Whole-expression match — identical to validate_expr_group_by.
    if gb_exprs.contains(expr) {
        return Ok(());
    }

    // Collect non-aggregated columns, but stop recursing into
    // sub-expressions that match a GROUP BY expression (their columns
    // are "covered" by the grouping key).
    for col in collect_non_aggregated_columns(expr, Some(gb_exprs)) {
        // Unqualified columns matching SELECT aliases are alias references —
        // the referenced SELECT expression was already validated above.
        if col.table.is_none() && select_aliases.contains(&col.name) {
            continue;
        }

        // Outer-scope (correlated) columns are constant per group —
        // they come from an outer row and never vary across the inner
        // query's rows.  Valid in HAVING unconditionally.
        if let Some(table) = &col.table
            && !local_tables.contains(table)
        {
            continue;
        }

        if !gb_columns.contains(col) {
            invalid_query!(
                "column {} must appear in the GROUP BY clause or be used in an aggregate function",
                col.display(dialect)
            );
        }
    }

    Ok(())
}

/// Collects all column references in `expr` that are NOT inside an aggregate
/// function call and NOT inside a sub-expression that matches a GROUP BY key.
///
/// When `gb_exprs` is `Some`, sub-expressions that structurally match a
/// GROUP BY expression are treated as opaque (their internal columns are
/// "covered" by the grouping key and not collected).  This handles cases
/// like `GROUP BY t.x + t.y HAVING t.x + t.y > 10`.  Pass `None` for
/// SELECT-list validation where this is not needed.
///
/// Boundaries:
/// - `visit_column` — collects the column.
/// - `visit_function_expr` — stops at aggregate boundaries (`is_aggregate`),
///   so e.g. `t.x` inside `SUM(t.x)` is NOT collected.  For `WindowFunction`,
///   `walk_expr` calls `visit_function_expr` on the inner function (stopped if
///   aggregate) then visits partition_by / order_by normally.
/// - `visit_expr` — stops at GROUP BY expression boundaries when `gb_exprs`
///   is provided.
/// - `visit_select_statement` — returns immediately (subqueries have their
///   own scope).
fn collect_non_aggregated_columns<'a>(
    expr: &'a Expr,
    gb_exprs: Option<&[Expr]>,
) -> Vec<&'a Column> {
    struct Collector<'c, 'g> {
        cols: Vec<&'c Column>,
        gb_exprs: Option<&'g [Expr]>,
    }

    impl<'ast> Visitor<'ast> for Collector<'ast, '_> {
        type Error = std::convert::Infallible;

        fn visit_column(&mut self, column: &'ast Column) -> Result<(), Self::Error> {
            self.cols.push(column);
            Ok(())
        }

        fn visit_function_expr(
            &mut self,
            function_expr: &'ast FunctionExpr,
        ) -> Result<(), Self::Error> {
            if is_aggregate(function_expr) {
                return Ok(());
            }
            walk_function_expr(self, function_expr)
        }

        fn visit_expr(&mut self, expr: &'ast Expr) -> Result<(), Self::Error> {
            if let Some(gb) = self.gb_exprs
                && gb.contains(expr)
            {
                return Ok(());
            }
            walk_expr(self, expr)
        }

        fn visit_select_statement(
            &mut self,
            _select_statement: &'ast SelectStatement,
        ) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    let mut collector = Collector {
        cols: Vec::new(),
        gb_exprs,
    };
    let _ = collector.visit_expr(expr);
    collector.cols
}

// ─── No duplicate derived-table column names ────────────────────────────────

/// Checks that no column reference targets an ambiguous (duplicated)
/// projected name from a derived table.  Two kinds of references are checked:
///
/// 1. **Internal** — the subquery's own GROUP BY, ORDER BY, or HAVING
///    contains an unqualified alias reference that matches a duplicate
///    projected name (e.g. `(SELECT t.a AS x, t.b AS x FROM t ORDER BY x)`).
/// 2. **External** — the outer scope references `alias.name` where `name`
///    appears more than once in the subquery's SELECT list.
///
/// PostgreSQL allows duplicate column names in a derived table's projection
/// as long as the ambiguous name is not actually referenced.  This check
/// mirrors that behaviour.
fn ambiguous_column_err(col: &Column, alias_rel: &Relation, dialect: Dialect) -> ReadySetError {
    ReadySetError::InvalidQuery(format!(
        "ambiguous column reference {}: column {} appears more than once in subquery {}",
        col.display(dialect),
        col.name,
        alias_rel.display(dialect)
    ))
}

fn validate_no_duplicate_derived_table_columns(
    stmt: &SelectStatement,
    dialect: Dialect,
) -> ReadySetResult<()> {
    // Phase 1: collect duplicate projected names per derived-table alias,
    // and check for ambiguous internal references within the subquery.
    let mut alias_duplicates: Vec<(Relation, HashSet<SqlIdentifier>)> = Vec::new();

    for te in get_local_from_items_iter!(stmt) {
        if let Some((subquery, alias)) = as_sub_query_with_alias(te) {
            let mut seen = HashSet::new();
            let mut duplicates = HashSet::new();
            for field in &subquery.fields {
                if let FieldDefinitionExpr::Expr {
                    expr,
                    alias: field_alias,
                } = field
                {
                    let name = alias_for_expr(expr, field_alias);
                    if !seen.insert(name.clone()) {
                        duplicates.insert(name);
                    }
                }
            }
            if !duplicates.is_empty() {
                let alias_rel: Relation = alias.into();
                check_subquery_internal_ambiguity(subquery, &alias_rel, &duplicates, dialect)?;
                alias_duplicates.push((alias_rel, duplicates));
            }
        }
    }

    // Phase 2: for each alias with duplicates, walk the statement using
    // `deep_columns_visitor` which enters correlated subqueries but
    // respects shadowing (skips subqueries that locally rebind the alias).
    for (alias_rel, duplicates) in &alias_duplicates {
        let mut ambiguous_ref: Option<Column> = None;
        deep_columns_visitor(stmt, alias_rel, &mut |expr| {
            if ambiguous_ref.is_some() {
                return;
            }
            let column = as_column!(expr);
            if is_column_of!(column, *alias_rel) && duplicates.contains(&column.name) {
                ambiguous_ref = Some(column.clone());
            }
        })?;
        if let Some(col) = ambiguous_ref {
            return Err(ambiguous_column_err(&col, alias_rel, dialect));
        }
    }

    Ok(())
}

/// Checks that a subquery's own GROUP BY, ORDER BY, and HAVING clauses do
/// not contain unqualified alias references that match a duplicate projected
/// name.  For example, `(SELECT t.a AS x, t.b AS x FROM t ORDER BY x)` is
/// ambiguous because `ORDER BY x` could refer to either projected column.
fn check_subquery_internal_ambiguity(
    subquery: &SelectStatement,
    alias_rel: &Relation,
    duplicates: &HashSet<SqlIdentifier>,
    dialect: Dialect,
) -> ReadySetResult<()> {
    // Helper: check a FieldReference for an ambiguous unqualified alias.
    let check_field_ref = |fr: &FieldReference| -> ReadySetResult<()> {
        if let FieldReference::Expr(Expr::Column(col)) = fr
            && col.table.is_none()
            && duplicates.contains(&col.name)
        {
            return Err(ambiguous_column_err(col, alias_rel, dialect));
        }
        Ok(())
    };

    // GROUP BY
    if let Some(gb) = &subquery.group_by {
        for field in &gb.fields {
            check_field_ref(field)?;
        }
    }

    // ORDER BY
    if let Some(order) = &subquery.order {
        for ob in &order.order_by {
            check_field_ref(&ob.field)?;
        }
    }

    // HAVING — `columns_iter` skips subqueries and enters aggregates,
    // so `SUM(x)` where `x` is ambiguous is still caught.
    if let Some(having) = &subquery.having
        && let Some(col) =
            columns_iter(having).find(|col| col.table.is_none() && duplicates.contains(&col.name))
    {
        return Err(ambiguous_column_err(col, alias_rel, dialect));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use readyset_sql::Dialect;
    use readyset_sql_parsing::{ParsingPreset, parse_query_with_config};

    use super::*;

    fn validate_postgres(sql: &str) -> ReadySetResult<()> {
        validate_with_dialect(sql, Dialect::PostgreSQL)
    }

    fn validate_mysql(sql: &str) -> ReadySetResult<()> {
        validate_with_dialect(sql, Dialect::MySQL)
    }

    fn validate_with_dialect(sql: &str, dialect: Dialect) -> ReadySetResult<()> {
        let q = parse_query_with_config(ParsingPreset::OnlySqlparser, dialect, sql).unwrap();
        match q {
            SqlQuery::Select(stmt) => stmt.validate_query_semantics(dialect),
            SqlQuery::CompoundSelect(csq) => {
                for (_op, stmt) in &csq.selects {
                    stmt.validate_query_semantics(dialect)?;
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    // ── Duplicate FROM aliases ──

    #[test]
    fn duplicate_from_alias_fails() {
        assert!(validate_postgres("SELECT a.x FROM t AS a JOIN s AS a ON a.x = a.y").is_err());
    }

    #[test]
    fn distinct_aliases_pass() {
        assert!(validate_postgres("SELECT a.x FROM t AS a JOIN s AS b ON a.x = b.x").is_ok());
    }

    // ── Join operators ──

    #[test]
    fn inner_join_passes() {
        assert!(validate_postgres("SELECT t.x FROM t JOIN s ON t.x = s.x").is_ok());
    }

    #[test]
    fn left_join_passes() {
        assert!(validate_postgres("SELECT t.x FROM t LEFT JOIN s ON t.x = s.x").is_ok());
    }

    #[test]
    fn cross_join_passes() {
        assert!(validate_postgres("SELECT t.x FROM t CROSS JOIN s").is_ok());
    }

    #[test]
    fn right_join_fails() {
        assert!(validate_postgres("SELECT s.x FROM t RIGHT JOIN s ON t.x = s.x").is_err());
    }

    // ── Aggregates in WHERE ──

    #[test]
    fn aggregate_in_where_fails() {
        assert!(validate_postgres("SELECT t.x FROM t WHERE SUM(t.x) > 5").is_err());
    }

    #[test]
    fn aggregate_in_where_subquery_ok() {
        // Aggregate inside a subquery in WHERE is valid — the subquery has its own scope.
        assert!(
            validate_postgres(
                "SELECT t.x FROM t WHERE t.x IN (SELECT s.x FROM s GROUP BY s.x HAVING COUNT(*) > 1)"
            )
            .is_ok()
        );
    }

    // ── Nested aggregates ──

    #[test]
    fn nested_aggregate_in_select_fails() {
        assert!(validate_postgres("SELECT AVG(SUM(t.x)) FROM t GROUP BY t.y").is_err());
    }

    #[test]
    fn nested_aggregate_in_having_fails() {
        assert!(
            validate_postgres("SELECT t.x FROM t GROUP BY t.x HAVING AVG(SUM(t.y)) > 5").is_err()
        );
    }

    #[test]
    fn nested_aggregate_in_order_by_fails() {
        assert!(
            validate_postgres("SELECT t.x FROM t GROUP BY t.x ORDER BY AVG(SUM(t.y))").is_err()
        );
    }

    #[test]
    fn non_nested_aggregate_passes() {
        assert!(validate_postgres("SELECT SUM(t.x), AVG(t.y) FROM t").is_ok());
    }

    // ── Subqueries in JOIN ON ──

    #[test]
    fn subquery_in_join_on_fails() {
        assert!(
            validate_postgres("SELECT t.x FROM t JOIN s ON t.x = (SELECT MAX(u.x) FROM u)")
                .is_err()
        );
    }

    #[test]
    fn exists_in_join_on_fails() {
        assert!(
            validate_postgres(
                "SELECT t.x FROM t JOIN s ON EXISTS (SELECT 1 FROM u WHERE u.x = t.x)"
            )
            .is_err()
        );
    }

    // ── Aggregates in JOIN ON ──

    #[test]
    fn aggregate_in_join_on_fails() {
        assert!(validate_postgres("SELECT t.x FROM t JOIN s ON SUM(t.x) = s.x").is_err());
    }

    #[test]
    fn column_equality_in_join_on_passes() {
        assert!(validate_postgres("SELECT t.x FROM t JOIN s ON t.x = s.x").is_ok());
    }

    // ── GROUP BY semantics ──

    #[test]
    fn grouped_column_in_select_passes() {
        assert!(validate_postgres("SELECT t.x, SUM(t.y) FROM t GROUP BY t.x").is_ok());
    }

    #[test]
    fn ungrouped_column_in_select_fails() {
        assert!(validate_postgres("SELECT t.x, t.y, SUM(t.z) FROM t GROUP BY t.x").is_err());
    }

    #[test]
    fn pure_aggregate_select_without_group_by_passes() {
        assert!(validate_postgres("SELECT COUNT(*) FROM t").is_ok());
    }

    #[test]
    fn mixed_aggregate_and_column_without_group_by_fails() {
        assert!(validate_postgres("SELECT t.x, COUNT(*) FROM t").is_err());
    }

    #[test]
    fn non_aggregate_query_passes() {
        assert!(validate_postgres("SELECT t.x, t.y FROM t WHERE t.x = 1").is_ok());
    }

    #[test]
    fn group_by_ordinal_passes() {
        assert!(validate_postgres("SELECT t.x, SUM(t.y) FROM t GROUP BY 1").is_ok());
    }

    #[test]
    fn group_by_expression_match_passes() {
        assert!(validate_postgres("SELECT t.x + t.y, SUM(t.z) FROM t GROUP BY t.x + t.y").is_ok());
    }

    #[test]
    fn having_with_alias_passes() {
        assert!(
            validate_postgres("SELECT t.x, SUM(t.y) AS s FROM t GROUP BY t.x HAVING s > 10")
                .is_ok()
        );
    }

    #[test]
    fn having_with_ungrouped_column_fails() {
        assert!(
            validate_postgres("SELECT t.x, SUM(t.y) FROM t GROUP BY t.x HAVING t.z > 10").is_err()
        );
    }

    #[test]
    fn having_with_aggregate_passes() {
        assert!(
            validate_postgres("SELECT t.x, SUM(t.y) FROM t GROUP BY t.x HAVING SUM(t.y) > 10")
                .is_ok()
        );
    }

    #[test]
    fn column_inside_aggregate_not_required_in_group_by() {
        assert!(validate_postgres("SELECT t.x, AVG(t.y + t.z) FROM t GROUP BY t.x").is_ok());
    }

    #[test]
    fn literal_in_aggregate_query_passes() {
        assert!(validate_postgres("SELECT 1, COUNT(*) FROM t").is_ok());
    }

    #[test]
    fn having_implies_aggregation() {
        // HAVING without GROUP BY — t.x is not grouped
        assert!(validate_postgres("SELECT t.x FROM t HAVING t.x > 10").is_err());
    }

    // ── HAVING with expression GROUP BY keys and correlated columns ──

    #[test]
    fn having_with_expression_group_by_key_passes() {
        // HAVING references a sub-expression that IS a GROUP BY key —
        // the composite expression is functionally determined by the
        // grouping, so individual columns need not be GROUP BY keys.
        assert!(
            validate_postgres("SELECT COUNT(*) FROM t GROUP BY t.x + t.y HAVING t.x + t.y > 10")
                .is_ok()
        );
    }

    #[test]
    fn having_correlated_column_in_subquery_passes() {
        // Outer-scope column in HAVING of a correlated subquery is constant
        // per group (it comes from the outer row) and is always valid.
        assert!(
            validate_postgres(
                "SELECT t.a, (SELECT COUNT(*) FROM s WHERE s.x = t.a GROUP BY s.x HAVING t.a > 0) FROM t GROUP BY t.a"
            )
            .is_ok()
        );
    }

    // ── ORDER BY in aggregate queries ──

    #[test]
    fn order_by_aggregate_in_non_aggregate_query_fails() {
        // ORDER BY AVG(x) without GROUP BY or aggregate in SELECT — PostgreSQL rejects this.
        assert!(validate_postgres("SELECT t.x FROM t ORDER BY AVG(t.x)").is_err());
    }

    #[test]
    fn order_by_ungrouped_column_fails() {
        // ORDER BY t.z is not in GROUP BY and not inside an aggregate.
        assert!(
            validate_postgres("SELECT t.x, SUM(t.y) FROM t GROUP BY t.x ORDER BY t.z").is_err()
        );
    }

    #[test]
    fn order_by_grouped_column_passes() {
        assert!(validate_postgres("SELECT t.x, SUM(t.y) FROM t GROUP BY t.x ORDER BY t.x").is_ok());
    }

    #[test]
    fn order_by_aggregate_expression_passes() {
        assert!(
            validate_postgres("SELECT t.x, SUM(t.y) FROM t GROUP BY t.x ORDER BY SUM(t.y)").is_ok()
        );
    }

    #[test]
    fn order_by_alias_in_aggregate_query_passes() {
        assert!(
            validate_postgres("SELECT t.x, SUM(t.y) AS s FROM t GROUP BY t.x ORDER BY s").is_ok()
        );
    }

    #[test]
    fn order_by_expression_group_by_key_passes() {
        // ORDER BY references a sub-expression that IS a GROUP BY key.
        assert!(
            validate_postgres("SELECT COUNT(*) FROM t GROUP BY t.x + t.y ORDER BY t.x + t.y")
                .is_ok()
        );
    }

    // ── MySQL dialect: ORDER BY tolerance ──

    #[test]
    fn mysql_order_by_ungrouped_column_passes() {
        // MySQL allows ORDER BY with non-grouped columns — picks arbitrary value.
        assert!(validate_mysql("SELECT t.x, SUM(t.y) FROM t GROUP BY t.x ORDER BY t.z").is_ok());
    }

    #[test]
    fn mysql_order_by_aggregate_in_non_aggregate_query_passes() {
        // MySQL allows this: aggregate-only produces 1 row, ORDER BY is ignored.
        assert!(validate_mysql("SELECT AVG(t.x) FROM t ORDER BY t.y LIMIT 10").is_ok());
    }

    #[test]
    fn postgres_order_by_ungrouped_column_still_fails() {
        // PostgreSQL rejects non-grouped ORDER BY (sanity check).
        assert!(
            validate_postgres("SELECT t.x, SUM(t.y) FROM t GROUP BY t.x ORDER BY t.z").is_err()
        );
    }

    #[test]
    fn subquery_group_by_independent_scope() {
        // Outer query groups by t.x; subquery has its own GROUP BY
        assert!(
            validate_postgres(
                "SELECT t.x, (SELECT COUNT(*) FROM s WHERE s.y = t.x) FROM t GROUP BY t.x"
            )
            .is_ok()
        );
    }

    // ── Duplicate derived-table column names ──

    #[test]
    fn duplicate_derived_table_column_referenced_fails() {
        // sub.x is ambiguous — two columns named x in subquery
        assert!(
            validate_postgres("SELECT sub.x FROM (SELECT t.a AS x, t.b AS x FROM t) AS sub")
                .is_err()
        );
    }

    #[test]
    fn duplicate_derived_table_column_unreferenced_passes() {
        // Duplicate names exist but are never referenced — valid per PostgreSQL
        assert!(
            validate_postgres("SELECT 1 FROM (SELECT t.a AS x, t.b AS x FROM t) AS sub").is_ok()
        );
    }

    #[test]
    fn unique_derived_table_columns_pass() {
        assert!(
            validate_postgres("SELECT sub.x, sub.y FROM (SELECT t.a AS x, t.b AS y FROM t) AS sub")
                .is_ok()
        );
    }

    // ── Duplicate derived-table columns: internal ambiguity ──

    #[test]
    fn duplicate_derived_table_order_by_ambiguous_fails() {
        // ORDER BY x is ambiguous within the subquery
        assert!(
            validate_postgres("SELECT 1 FROM (SELECT t.a AS x, t.b AS x FROM t ORDER BY x) AS sub")
                .is_err()
        );
    }

    #[test]
    fn duplicate_derived_table_group_by_ambiguous_fails() {
        assert!(
            validate_postgres(
                "SELECT 1 FROM (SELECT t.a AS x, t.b AS x, COUNT(*) FROM t GROUP BY x) AS sub"
            )
            .is_err()
        );
    }

    #[test]
    fn duplicate_derived_table_having_ambiguous_fails() {
        assert!(
            validate_postgres(
                "SELECT 1 FROM (SELECT t.a AS x, t.b AS x, COUNT(*) FROM t GROUP BY t.a, t.b HAVING x > 10) AS sub"
            )
            .is_err()
        );
    }

    #[test]
    fn duplicate_derived_table_having_aggregate_ambiguous_fails() {
        // SUM(x) is equally ambiguous — x refers to two projected names
        assert!(
            validate_postgres(
                "SELECT 1 FROM (SELECT t.a AS x, t.b AS x, COUNT(*) FROM t GROUP BY t.a, t.b HAVING SUM(x) > 10) AS sub"
            )
            .is_err()
        );
    }

    #[test]
    fn duplicate_derived_table_order_by_qualified_passes() {
        // ORDER BY t.a is qualified — not an alias reference, so not ambiguous
        assert!(
            validate_postgres(
                "SELECT 1 FROM (SELECT t.a AS x, t.b AS x FROM t ORDER BY t.a) AS sub"
            )
            .is_ok()
        );
    }

    #[test]
    fn duplicate_derived_table_order_by_ordinal_passes() {
        // ORDER BY 1 is positional — not ambiguous
        assert!(
            validate_postgres("SELECT 1 FROM (SELECT t.a AS x, t.b AS x FROM t ORDER BY 1) AS sub")
                .is_ok()
        );
    }

    // ─── Coverage gap tests ────────────────────────────────────────────────

    // ORDER BY with expression containing both grouped and ungrouped columns.
    #[test]
    fn order_by_expr_with_ungrouped_column_fails() {
        assert!(
            validate_postgres("SELECT t.x, SUM(t.y) FROM t GROUP BY t.x ORDER BY t.x + t.z")
                .is_err()
        );
    }

    // ORDER BY with simple grouped column — should pass.
    #[test]
    fn order_by_simple_grouped_column_passes() {
        assert!(validate_postgres("SELECT t.x, SUM(t.y) FROM t GROUP BY t.x ORDER BY t.x").is_ok());
    }

    // ORDER BY with aggregate expression — should pass.
    #[test]
    fn order_by_aggregate_passes() {
        assert!(
            validate_postgres("SELECT t.x, SUM(t.y) FROM t GROUP BY t.x ORDER BY SUM(t.y)").is_ok()
        );
    }

    // HAVING with subquery referencing grouped column — should pass.
    #[test]
    fn having_subquery_grouped_column_passes() {
        assert!(
            validate_postgres(
                "SELECT t.x, SUM(t.y) AS s FROM t GROUP BY t.x \
                 HAVING (SELECT COUNT(*) FROM s WHERE s.z = t.x) > 0"
            )
            .is_ok()
        );
    }

    // SELECT with expression mixing grouped + ungrouped columns — should fail.
    #[test]
    fn select_expr_mixed_grouped_ungrouped_fails() {
        assert!(validate_postgres("SELECT t.x + t.z, SUM(t.y) FROM t GROUP BY t.x").is_err());
    }

    // SELECT aggregate wrapping ungrouped column — should pass (aggregate boundary).
    #[test]
    fn select_aggregate_ungrouped_passes() {
        assert!(validate_postgres("SELECT t.x, SUM(t.z) FROM t GROUP BY t.x").is_ok());
    }

    // Nested aggregate in HAVING — should fail.
    #[test]
    fn having_nested_aggregate_fails() {
        assert!(
            validate_postgres("SELECT t.x, SUM(t.y) FROM t GROUP BY t.x HAVING AVG(SUM(t.y)) > 10")
                .is_err()
        );
    }

    // Duplicate derived-table column referenced via qualified name — should fail.
    #[test]
    fn duplicate_derived_column_qualified_ref_fails() {
        assert!(
            validate_postgres("SELECT sub.x FROM (SELECT t.a AS x, t.b AS x FROM t) AS sub")
                .is_err()
        );
    }
}
