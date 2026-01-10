//! Inline Leading Derived Table
//!
//! This pass inlines (or "hoists") the leftmost derived table (subquery in FROM) into
//! the outer query, flattening the query structure when semantically safe.
//!
//! # Motivation
//!
//! Queries are often structured with a "core" subquery that handles filtering, grouping,
//! or pagination, wrapped by an outer query that joins additional data. Inlining the
//! leading derived table can:
//!
//! - Reduce query nesting depth
//! - Expose filter predicates for earlier application
//! - Enable further optimization passes to see the full query structure
//!
//! # Example
//!
//! **Before:**
//! ```sql
//! SELECT s.id, s.total, tags.t
//! FROM (
//!     SELECT o.id, SUM(o.amount) AS total
//!     FROM orders AS o
//!     GROUP BY o.id
//!     ORDER BY o.id
//!     LIMIT 100
//! ) AS s
//! CROSS JOIN LATERAL (
//!     SELECT ARRAY_AGG(t.name) AS t
//!     FROM tags AS t
//!     WHERE t.order_id = s.id
//! ) AS tags
//! ORDER BY s.id
//!
//!
//! After:
//! sql
//! SELECT o.id, SUM(o.amount) AS total, tags.t
//! FROM orders AS o
//! CROSS JOIN LATERAL (
//!     SELECT ARRAY_AGG(t.name) AS t
//!     FROM tags AS t
//!     WHERE t.order_id = o.id
//! ) AS tags
//! GROUP BY o.id
//! ORDER BY o.id
//! LIMIT 100
//!
//!
//! # Key Transformations
//!
//! - Column rebinding: References to the subquery alias are replaced with the
//! actual projected expressions from the inner query.
//! - Alias deduplication: Inner FROM aliases that conflict with outer names are
//! renamed to avoid ambiguity.
//! - WHERE → HAVING migration: When the inner query is aggregated, outer WHERE
//! predicates that reference only the inner alias move to HAVING.
//! - LIMIT/OFFSET composition: When both inner and outer have numeric limits,
//! they are combined (e.g., inner LIMIT 50 OFFSET 5 + outer LIMIT 10 OFFSET 3
//! becomes LIMIT 10 OFFSET 8).
//!
//! # Safety Constraints
//!
//! The pass bails out (leaves the query unchanged) when inlining would alter semantics:
//!
//! - Cardinality preservation: Downstream joins must not change row counts.
//! CROSS/INNER joins require RHS to produce exactly one row; LEFT joins allow
//! at most one row.
//! - Window functions: Queries with window functions are not inlined.
//! - ORDER BY conflicts: If inner has LIMIT and outer imposes a different ORDER,
//! inlining is unsafe.
//! - Nested aggregation: Both inner and outer being aggregated is not supported.
//! - Non-literal limits: LIMIT/OFFSET with placeholders or expressions cannot
//! be composed.
use crate::drop_redundant_join::{deep_columns_visitor, deep_columns_visitor_mut};
use crate::rewrite_utils::{
    alias_for_expr, analyse_fix_correlated_subquery_group_by, and_predicates_skip_true,
    as_sub_query_with_alias, collect_local_from_items, contains_select,
    default_alias_for_select_item_expression, expect_field_as_expr, expect_field_as_expr_mut,
    expect_only_subquery_from_with_alias, expect_sub_query_with_alias_mut,
    for_each_window_function, get_from_item_reference_name, get_select_item_alias,
    get_unique_alias, hoist_parametrizable_join_filters_to_where, is_aggregated_select,
    split_correlated_constraint, split_correlated_expression, split_expr,
};
use crate::unnest_subqueries::{
    AggNoGbyCardinality, agg_only_no_gby_cardinality, has_limit_one_deep,
};
use crate::{as_column, get_local_from_items_iter, get_local_from_items_iter_mut, is_column_of};
use itertools::Either;
use itertools::Itertools;
use readyset_errors::{ReadySetResult, internal, invalid_query, invalid_query_err, unsupported};
use readyset_sql::ast::{
    Column, Expr, FieldDefinitionExpr, FieldReference, GroupByClause, JoinClause, JoinConstraint,
    JoinOperator, JoinRightSide, LimitClause, LimitValue, Literal, OrderBy, OrderClause, Relation,
    SelectStatement, SqlIdentifier, TableExpr, TableExprInner,
};
use readyset_sql::{Dialect, DialectDisplay};
use std::collections::{HashMap, HashSet};
use std::iter;
use std::mem;
use tracing::trace;

pub trait InlineLeadingDerivedTable: Sized {
    fn inline_leading_derived_table(&mut self) -> ReadySetResult<&mut Self>;
}

impl InlineLeadingDerivedTable for SelectStatement {
    fn inline_leading_derived_table(&mut self) -> ReadySetResult<&mut Self> {
        let mut rewritten = hoist_lhsmost_derived_table_rewrite_impl(self, true)?;

        if rewritten {
            // If hoisting happened, try to move the parametrizable filters, that
            // existed in the hoistable internal join structure, and after hoisting
            // became available at the main level.
            // **NOTE**: if hoisting actually happened, we had verified all downstream
            // joins have empty join conditions.
            rewritten |= hoist_parametrizable_join_filters_to_where(self)?;
        }

        if rewritten {
            trace!(target: "inline_leading_derived_tables",
                statement = %self.display(Dialect::PostgreSQL),
                ">LHS-most derived table hoisted");
        }

        Ok(self)
    }
}

/// When inlining a FROM item, ensure that any alias it used doesn’t collide
/// with names in the outer query—if so, consistently rename them.
fn make_inner_aliases_distinct_from_base_statement(
    base_stmt: &SelectStatement,
    inner_dt: &mut TableExpr,
) -> ReadySetResult<bool> {
    // Collect base statement FROM item names, excluding the one we are going to inline
    let mut base_locals = get_local_from_items_iter!(base_stmt)
        .map(get_from_item_reference_name)
        .collect::<ReadySetResult<HashSet<Relation>>>()?;
    base_locals.remove(&get_from_item_reference_name(inner_dt)?);

    let (inner_stmt, _inner_stmt_alias) = expect_sub_query_with_alias_mut(inner_dt);

    // Iterate the inlinable statement FROM clause, detect the collided names and replace it with
    // the new distinct ones.
    // Collect the collided old and new names to replace the columns referencing the old names inside the
    // inlinable statement outside the loop.
    let mut update_alias_map = HashMap::new();

    // Helper to efficiently build a combined alias space
    fn build_combined_aliases(base: &HashSet<Relation>, inl: &[Relation]) -> HashSet<Relation> {
        let mut out = HashSet::with_capacity(base.len() + inl.len());
        out.extend(base.iter().cloned());
        out.extend(inl.iter().cloned());
        out
    }

    let mut inner_locals = get_local_from_items_iter!(inner_stmt)
        .rev()
        .map(get_from_item_reference_name)
        .collect::<ReadySetResult<Vec<_>>>()?;

    while let Some(inl_ref_name) = inner_locals.pop() {
        if !base_locals.insert(inl_ref_name.clone()) {
            // Make sure the new alias does not collide with either existing base or inlinable aliases.
            let update_inner_ref_name: Relation = get_unique_alias(
                &build_combined_aliases(&base_locals, &inner_locals),
                inl_ref_name.name.as_str(),
            )
            .into();
            let existing = update_alias_map.insert(inl_ref_name, update_inner_ref_name.clone());
            debug_assert!(existing.is_none());
            base_locals.insert(update_inner_ref_name);
        }
    }

    let has_duplicate_aliases = !update_alias_map.is_empty();

    // Update the columns referencing the collided FROM item names inside the inlinable statement
    for (exist_ref_name, update_ref_name) in update_alias_map {
        // Skip subqueries that locally bind the very alias we’re renaming, so we don’t rewrite shadowed references.
        deep_columns_visitor_mut(inner_stmt, &exist_ref_name, &mut |expr| {
            let column = as_column!(expr);
            if column.table.as_ref() == Some(&exist_ref_name) {
                column.table = Some(update_ref_name.clone());
            }
        })?;
        if let Some(inner_rel) = get_local_from_items_iter_mut!(inner_stmt).find(|rel| {
            get_from_item_reference_name(rel).is_ok_and(|rel_name| rel_name == exist_ref_name)
        }) {
            inner_rel.alias = Some(update_ref_name.name);
        } else {
            internal!(
                "Inner local FROM item {} not found",
                exist_ref_name.display_unquoted()
            );
        }
    }

    Ok(has_duplicate_aliases)
}

/// Return (expression, alias) for a select field.
fn get_expr_with_alias(fe: &FieldDefinitionExpr) -> (Expr, SqlIdentifier) {
    let (expr, maybe_alias) = expect_field_as_expr(fe);
    (expr.clone(), alias_for_expr(expr, maybe_alias))
}

/// Build a map from an outer column alias to the inner expression,
/// so we can replace references after inlining.
fn build_ext_to_int_fields_map(
    stmt: &SelectStatement,
    stmt_alias: SqlIdentifier,
) -> ReadySetResult<HashMap<Column, Expr>> {
    let mut ext_to_int_map = HashMap::new();
    for field in stmt.fields.iter() {
        let (expr, alias) = get_expr_with_alias(field);
        if ext_to_int_map
            .insert(
                Column {
                    name: alias.clone(),
                    table: Some(stmt_alias.clone().into()),
                },
                expr.clone(),
            )
            .is_some()
        {
            invalid_query!("Duplicate select field alias {}", alias.as_str())
        }
    }
    Ok(ext_to_int_map)
}

fn rebind_column_refs(
    stmt: &mut SelectStatement,
    lhs_rel: &Relation,
    col_to_expr: &HashMap<Column, Expr>,
) -> ReadySetResult<()> {
    deep_columns_visitor_mut(stmt, lhs_rel, &mut |expr| {
        if let Some(to_expr) = col_to_expr.get(as_column!(expr)) {
            let _ = mem::replace(expr, to_expr.clone());
        }
    })
}

/// Replace references to aliased subquery columns with their actual expressions.
/// Used to substitute projected fields from the inlined subquery into the outer scope.
fn replace_columns_with_inlinable_expr(
    base_stmt: &mut SelectStatement,
    lhs_rel: &Relation,
    ext_to_int_fields: &HashMap<Column, Expr>,
    is_top_select: bool,
) -> ReadySetResult<()> {
    for select_item in &mut base_stmt.fields {
        let (expr, maybe_alias) = expect_field_as_expr_mut(select_item);
        // Determine if this select item will be rewritten and, at top level,
        // whether the replacement would change the visible column name.
        if maybe_alias.is_none()
            && let Expr::Column(orig_col) = expr
            && let Some(mapped) = ext_to_int_fields.get(orig_col)
        {
            // Will the visible name change?
            let name_changes = match mapped {
                Expr::Column(mapped_col) => mapped_col.name != orig_col.name,
                _ => true,
            };
            if name_changes {
                *maybe_alias = Some(if is_top_select {
                    // Preserve the original projected column name at the top level.
                    orig_col.name.clone()
                } else {
                    // Non-top level: keep stable naming for parents by giving a deterministic alias.
                    default_alias_for_select_item_expression(mapped)
                });
            }
        }
    }
    rebind_column_refs(base_stmt, lhs_rel, ext_to_int_fields)
}

fn literal_as_number(lit: &Literal) -> ReadySetResult<u64> {
    Ok(match lit {
        Literal::Integer(i) => {
            if *i < 0 {
                invalid_query!("LIMIT/OFFSET must be non-negative")
            }
            *i as u64
        }
        Literal::UnsignedInteger(i) => *i,
        Literal::Number(s) | Literal::String(s) => {
            s.parse::<u64>().map_err(|e| invalid_query_err!("{e}"))?
        }
        _ => invalid_query!("Invalid LIMIT/OFFSET value"),
    })
}

fn limit_clause_as_numbers(limit_clause: &LimitClause) -> ReadySetResult<(u64, u64)> {
    let lim = limit_clause
        .limit()
        .cloned()
        .unwrap_or(Literal::UnsignedInteger(u64::MAX));
    let offs = limit_clause
        .offset()
        .cloned()
        .unwrap_or(Literal::UnsignedInteger(0u64));
    Ok((literal_as_number(&lim)?, literal_as_number(&offs)?))
}

fn deep_columns_expr_visitor(
    expr: &Expr,
    shadow_rel: &Relation,
    visitor: &mut impl FnMut(&Expr),
) -> ReadySetResult<()> {
    let dummy_stmt = SelectStatement {
        where_clause: Some(expr.clone()),
        ..SelectStatement::default()
    };
    deep_columns_visitor(&dummy_stmt, shadow_rel, visitor)
}

/// Collect the set of base FROM-item relations at this level, excluding `lhs_rel`.
fn visible_base_rels_except(
    base_stmt: &SelectStatement,
    lhs_rel: &Relation,
) -> ReadySetResult<HashSet<Relation>> {
    let mut base = collect_local_from_items(base_stmt)?;
    base.remove(lhs_rel);
    Ok(base)
}

/// Return true if `expr` references `rel` anywhere, descending into subqueries but
/// skipping subqueries that bind (shadow) `rel` locally.
fn refs_rel_anywhere(expr: &Expr, rel: &Relation) -> ReadySetResult<bool> {
    let mut seen = false;
    deep_columns_expr_visitor(expr, rel, &mut |e| {
        if is_column_of!(as_column!(e), *rel) {
            seen = true;
        }
    })?;
    Ok(seen)
}

/// Return true if `expr` references any relation in `rels` (scope-aware).
fn refs_any_of_rels_anywhere(expr: &Expr, rels: &HashSet<Relation>) -> ReadySetResult<bool> {
    for r in rels {
        if refs_rel_anywhere(expr, r)? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn group_by_keys_all_projected(stmt: &SelectStatement) -> ReadySetResult<bool> {
    let Some(group_by) = &stmt.group_by else {
        return Ok(false);
    };

    // All normalized GROUP BY keys must appear verbatim in the projection for DISTINCT to be redundant.
    for fr in &group_by.fields {
        match fr {
            FieldReference::Expr(e) => {
                if !stmt.fields.iter().any(|fde| {
                    let (fde_expr, _) = expect_field_as_expr(fde);
                    fde_expr.eq(e)
                }) {
                    return Ok(false);
                }
            }
            // GROUP BY should have been normalized already; if not, be conservative.
            FieldReference::Numeric(_) => return Ok(false),
        }
    }
    Ok(true)
}

/// Collect aliases of LHS projection items that are *aggregate-derived*.
/// For GROUP BY queries, an alias is aggregate-derived iff its expression is
/// not exactly one of the normalized GROUP BY key expressions.
/// For aggregate-only (no GROUP BY) queries, we conservatively mark all
/// projected aliases as aggregate-derived.
fn collect_agg_derived_aliases(
    lhs_stmt: &SelectStatement,
) -> ReadySetResult<HashSet<SqlIdentifier>> {
    let mut out = HashSet::new();

    // Fast return when the inner statement is not aggregated / grouped.
    if !is_aggregated_select(lhs_stmt)? && lhs_stmt.group_by.is_none() {
        return Ok(out);
    }

    if let Some(group_by) = &lhs_stmt.group_by {
        // Normalize GROUP BY references against the LHS projection
        let norm_gb = normalize_group_by(&lhs_stmt.fields, group_by)?;
        // Materialize normalized key expressions
        let mut key_exprs: Vec<Expr> = Vec::with_capacity(norm_gb.fields.len());
        for fr in norm_gb.fields {
            match fr {
                FieldReference::Expr(e) => key_exprs.push(e),
                // Shouldn't happen after normalization, but be conservative
                FieldReference::Numeric(_) => return Ok(out), // fallback: treat nothing as safe
            }
        }

        for fe in &lhs_stmt.fields {
            let (e, alias) = get_expr_with_alias(fe);
            // If the projected expression is not exactly a GBY key, treat it as aggregate-derived.
            let is_key = key_exprs.contains(&e);
            if !is_key {
                out.insert(alias);
            }
        }
    } else {
        // Aggregate-only, no GROUP BY: conservatively mark all projected aliases.
        for fe in &lhs_stmt.fields {
            let (_e, alias) = get_expr_with_alias(fe);
            out.insert(alias);
        }
    }

    Ok(out)
}

/// Scan all downstream derived tables (bare FROM items after the first and all JOIN RHS)
/// and return true if *any* of them reference the LHS alias using a column whose name
/// is one of the aggregate-derived aliases computed by `collect_agg_derived_aliases`.
fn downstream_uses_agg_derived_aliases(
    base_stmt: &SelectStatement,
    lhs_alias: &SqlIdentifier,
    agg_aliases: &HashSet<SqlIdentifier>,
) -> ReadySetResult<bool> {
    if agg_aliases.is_empty() {
        return Ok(false);
    }

    let lhs_rel: Relation = lhs_alias.clone().into();

    for rhs_stmt in get_local_from_items_iter!(base_stmt).filter_map(|dt| {
        if let Some((rhs_stmt, _rhs_stmt_alias)) = as_sub_query_with_alias(dt) {
            Some(rhs_stmt)
        } else {
            None
        }
    }) {
        // Visit all column references in `rhs_stmt`, descending into subqueries but
        // skipping scopes that shadow `lhs_rel`.
        let mut found = false;
        deep_columns_visitor(rhs_stmt, &lhs_rel, &mut |expr| {
            let col = as_column!(expr);
            if is_column_of!(col, lhs_rel) && agg_aliases.contains(&col.name) {
                found = true;
            }
        })?;
        if found {
            return Ok(true);
        }
    }

    Ok(false)
}

fn hoist_lhsmost_from_item_internals(
    base_stmt: &mut SelectStatement,
    mut lhs_dt: TableExpr,
    ext_to_int_fields: &HashMap<Column, Expr>,
    is_top_select: bool,
) -> ReadySetResult<()> {
    // Get the inlinable FROM item's statement
    let (lhs_stmt, lhs_stmt_alias) = expect_sub_query_with_alias_mut(&mut lhs_dt);
    let mut lhs_stmt = mem::take(lhs_stmt);

    let is_lhs_stmt_aggregation_or_grouped = is_aggregation_or_grouped(&lhs_stmt)?;

    // In case the hoistable statement is aggregated, move only the WHERE conjuncts that
    // reference the hoistable relation (lhs) and *no other base relations* into HAVING.
    // We classify per conjunct with scope-aware reference checks.
    if is_lhs_stmt_aggregation_or_grouped && let Some(where_expr) = base_stmt.where_clause.take() {
        let lhs_rel: Relation = lhs_stmt_alias.clone().into();
        let base_other_rels = visible_base_rels_except(base_stmt, &lhs_rel)?;

        // Flatten WHERE into conjuncts
        let mut conjuncts = Vec::new();
        let _ = split_expr(&where_expr, &|_| true, &mut conjuncts);

        // Rebuild WHERE from the conjuncts we *keep*; push lhs-only into HAVING
        let mut kept_where = None;
        let mut move_to_having = Vec::new();

        for e in conjuncts {
            let refs_lhs = refs_rel_anywhere(&e, &lhs_rel)?;
            let refs_other = refs_any_of_rels_anywhere(&e, &base_other_rels)?;
            if refs_lhs && !refs_other {
                if contains_select(&e) {
                    invalid_query!("Cannot hoist: would push a subquery into HAVING");
                }
                move_to_having.push(e);
            } else {
                kept_where = and_predicates_skip_true(kept_where, e);
            }
        }

        base_stmt.where_clause = kept_where;
        for e in move_to_having {
            base_stmt.having = and_predicates_skip_true(base_stmt.having.take(), e);
        }
    }

    replace_columns_with_inlinable_expr(
        base_stmt,
        &lhs_stmt_alias.clone().into(),
        ext_to_int_fields,
        is_top_select,
    )?;

    // Replace the base LHS-most item with the inlinable tables
    base_stmt
        .tables
        .splice(0..=0, mem::take(&mut lhs_stmt.tables));
    if !lhs_stmt.join.is_empty() {
        // Insert inlinable joins at the bottom
        base_stmt.join.splice(0..0, mem::take(&mut lhs_stmt.join));
    }

    // Handle aggregated subquery case
    if is_lhs_stmt_aggregation_or_grouped {
        // DISTINCT + GROUP BY: drop DISTINCT only when the projection contains all group keys.
        if lhs_stmt.distinct && !group_by_keys_all_projected(&lhs_stmt)? {
            base_stmt.distinct = true;
        }
        base_stmt.group_by = mem::take(&mut lhs_stmt.group_by);
        if let Some(lhs_having) = lhs_stmt.having {
            base_stmt.having =
                and_predicates_skip_true(mem::take(&mut base_stmt.having), lhs_having);
        }
        if let Some(lhs_where) = lhs_stmt.where_clause {
            base_stmt.where_clause =
                and_predicates_skip_true(mem::take(&mut base_stmt.where_clause), lhs_where);
        }
    } else if let Some(lhs_where) = mem::take(&mut lhs_stmt.where_clause) {
        base_stmt.where_clause =
            and_predicates_skip_true(mem::take(&mut base_stmt.where_clause), lhs_where);
    }

    // ORDER BY handling:
    //
    // When the LHS subquery has LIMIT/OFFSET, its ORDER BY is part of Top-K semantics (it defines
    // which rows survive). After hoisting, the LIMIT/OFFSET is applied at the base level (and may
    // be composed with an outer LIMIT), so we must also carry the LHS ORDER BY upward; otherwise
    // we could select a different subset of rows (especially on ties).
    //
    // Safety: we only do this after verifying the outer ORDER BY is either absent or compatible
    // with the LHS ORDER BY under projection rebinding (see `orders_equivalent_under_projection`).
    if !lhs_stmt.limit_clause.is_empty() {
        base_stmt.order = mem::take(&mut lhs_stmt.order);
    }

    if !lhs_stmt.limit_clause.is_empty() {
        if base_stmt.limit_clause.is_empty() {
            base_stmt.limit_clause = lhs_stmt.limit_clause;
        } else {
            let (base_lim, base_offs) = limit_clause_as_numbers(&base_stmt.limit_clause)?;
            let (lhs_lim, lhs_offs) = limit_clause_as_numbers(&lhs_stmt.limit_clause)?;

            let lim_num = base_lim.min(lhs_lim.saturating_sub(base_offs));
            let offs_num = lhs_offs.saturating_add(base_offs);

            let limit = if lim_num == u64::MAX {
                None
            } else {
                Some(LimitValue::Literal(
                    if matches!(base_stmt.limit_clause.limit(), Some(Literal::Integer(_))) {
                        Literal::Integer(lim_num as i64)
                    } else {
                        Literal::UnsignedInteger(lim_num)
                    },
                ))
            };

            let offset = if offs_num == 0 {
                None
            } else {
                Some(
                    if matches!(base_stmt.limit_clause.offset(), Some(Literal::Integer(_))) {
                        Literal::Integer(offs_num as i64)
                    } else {
                        Literal::UnsignedInteger(offs_num)
                    },
                )
            };

            base_stmt.limit_clause = LimitClause::LimitOffset { limit, offset };
        }
    }

    Ok(())
}

fn normalize_group_and_order_by(stmt: &mut SelectStatement) -> ReadySetResult<()> {
    // Resolve numeric and alais references in ORDER BY
    if let Some(order_by) = &stmt.order {
        stmt.order = Some(normalize_order_by(&stmt.fields, order_by)?);
    }

    // Resolve numeric and alais references in GROUP BY
    if let Some(group_by) = &stmt.group_by {
        stmt.group_by = Some(normalize_group_by(&stmt.fields, group_by)?);
    }

    Ok(())
}

/// Inline a subquery FROM item: rename aliases, hoist filters, substitute columns, and splice in tables/joins.
fn hoist_lhsmost_from_item(
    base_stmt: &mut SelectStatement,
    ext_to_int_fields: &HashMap<Column, Expr>,
    is_top_select: bool,
) -> ReadySetResult<()> {
    // Get the LHS-most derived table
    let Some(lhs_dt) = base_stmt.tables.first_mut() else {
        unsupported!("FROM-less statement found");
    };

    // Take the LHS-most derived table out, replace it with a dummy relation.
    // **IMPORTANT**: keep the original alias.
    let mut lhs_dt = mem::replace(
        lhs_dt,
        TableExpr {
            inner: TableExprInner::Table("$being_inlined$".into()),
            alias: lhs_dt.alias.clone(),
        },
    );

    // Resolve the numeric and alais references in GROUP BY and ORDER BY for the LHS-most statement
    {
        let (lhs_stmt, _) = expect_sub_query_with_alias_mut(&mut lhs_dt);
        normalize_group_and_order_by(lhs_stmt)?;
    }

    // Make sure the inner FROM item's aliases of the LHS-most statement do not clash with
    // the existing base statement's FROM items
    make_inner_aliases_distinct_from_base_statement(base_stmt, &mut lhs_dt)?;

    // Convert comma separated tables into cross-joined sequence.
    // This will help to preserve Postgres compatible join shape after hoisting the LHS-most derived table.
    normalize_comma_separated_lhs(base_stmt)?;

    // Resolve the numeric and alais references in GROUP BY and ORDER BY for the base statement
    normalize_group_and_order_by(base_stmt)?;

    // Embed the LHS-most derived table into the base statement
    hoist_lhsmost_from_item_internals(base_stmt, lhs_dt, ext_to_int_fields, is_top_select)?;

    Ok(())
}

fn is_window_function_select(stmt: &SelectStatement) -> ReadySetResult<bool> {
    let mut has_wf = false;
    for fe in &stmt.fields {
        let (fe_expr, _) = expect_field_as_expr(fe);
        for_each_window_function(fe_expr, &mut |_| has_wf = true)?;
    }
    Ok(has_wf)
}

fn is_aggregation_or_grouped(stmt: &SelectStatement) -> ReadySetResult<bool> {
    Ok(stmt.distinct || is_aggregated_select(stmt)? || stmt.group_by.is_some())
}

/// Classify whether a SELECT is **AtMostOne** (0..1 rows) possibly **under single-projecting wrappers**.
/// This "drills" through wrappers that cannot increase cardinality and uses three independent proofs:
///   1) `LIMIT 1` found anywhere under a chain of single-subquery wrappers (`has_limit_one_deep`);
///   2) aggregate-only / no GROUP BY classification (`agg_only_no_gby_cardinality`);
///   3) GROUP BY keys fully pinned by correlated equalities to the outer scope
///      (`split_correlated_constraint` + `align_group_by_and_windows_with_correlation`).
///
/// We conservatively **only** descend through wrappers that:
///   • have exactly one FROM item which is a subquery with alias, and
///   • have **no GROUP BY** at the wrapper level (filters/DISTINCT/ORDER/LIMIT can only reduce).
fn is_at_most_one_deep(stmt: &SelectStatement) -> ReadySetResult<bool> {
    // Fast path: any LIMIT 1 (optionally OFFSET 0) under wrapper chain
    if has_limit_one_deep(stmt) {
        return Ok(true);
    }

    // Walk down single-projecting wrappers while checking AtMostOne at each level.
    let mut cur = stmt;
    loop {
        // 0..1 via aggregate-only without GROUP BY (wrapper-aware)
        if agg_only_no_gby_cardinality(cur)?.is_some() {
            return Ok(true);
        }

        // GROUP BY pinned by correlation: if all GBY keys are fixed by correlated =-pairs,
        // output is 0..1 per outer row.
        if let Some(group_by) = &cur.group_by {
            if let Some(where_expr) = &cur.where_clause {
                let locals = collect_local_from_items(cur)?;
                let (maybe_corr, _remaining) =
                    split_correlated_expression(where_expr, &|rel| !locals.contains(rel));
                if let Some(corr) = maybe_corr {
                    let cols_set = split_correlated_constraint(&corr, &locals)?;
                    if analyse_fix_correlated_subquery_group_by(&cols_set, &mut group_by.clone())? {
                        return Ok(true);
                    }
                }
            }
        } else {
            // Descend only through a single-child projecting wrapper **without GROUP BY**;
            // WHERE/DISTINCT/ORDER/LIMIT cannot increase cardinality.
            match expect_only_subquery_from_with_alias(cur) {
                Ok((inner, _alias)) => {
                    cur = inner;
                    continue;
                }
                Err(_) => { /* no wrapper to descend */ }
            }
        }

        // No proof found at this level, and no safe wrapper to descend through.
        return Ok(false);
    }
}

fn is_exactly_one_card(stmt: &SelectStatement) -> ReadySetResult<bool> {
    // EXACTLY ONE is only sound to assert when the subquery is aggregate-only with no GROUP BY
    // and our cardinality analysis proves it yields exactly one row. Heuristics based on
    // projection shape (e.g., ARRAY(Subquery)) are unsafe because they do not constrain the
    // number of *rows* produced by the SELECT (a FROM clause could still replicate rows).
    Ok(matches!(
        agg_only_no_gby_cardinality(stmt)?,
        Some(AggNoGbyCardinality::ExactlyOne)
    ))
}

fn is_at_most_one_card(stmt: &SelectStatement) -> ReadySetResult<bool> {
    is_at_most_one_deep(stmt)
}

fn is_exactly_one_subquery(dt: &TableExpr) -> ReadySetResult<bool> {
    Ok(matches!(dt, TableExpr {
        inner: TableExprInner::Subquery(rhs_stmt),
        alias: Some(_),
    } if is_exactly_one_card(rhs_stmt)?))
}

fn is_at_most_one_subquery(dt: &TableExpr) -> ReadySetResult<bool> {
    Ok(matches!(dt, TableExpr {
        inner: TableExprInner::Subquery(rhs_stmt),
        alias: Some(_),
    } if is_at_most_one_card(rhs_stmt)?))
}

fn is_on_nonrejecting(c: &JoinConstraint) -> bool {
    matches!(
        c,
        JoinConstraint::Empty | JoinConstraint::On(Expr::Literal(Literal::Boolean(true)))
    )
}

fn all_downstream_joins_cardinality_preserving(stmt: &SelectStatement) -> ReadySetResult<bool> {
    // Bare FROM items after the first behave as CROSS joins.
    // For hoist safety, RHS must be ExactlyOne; AtMostOne (0..1) is UNSAFE since 0 filters all rows.
    for dt in stmt.tables.iter().skip(1) {
        if !is_exactly_one_subquery(dt)? {
            return Ok(false);
        }
    }

    for jc in &stmt.join {
        // We only support a single right-hand table expr per join here.
        let Ok(dt) = jc.right.table_exprs().exactly_one() else {
            return Ok(false);
        };
        if !is_on_nonrejecting(&jc.constraint) {
            return Ok(false);
        }
        if jc.operator.is_inner_join() {
            if !is_exactly_one_subquery(dt)? {
                return Ok(false);
            }
        } else if matches!(
            jc.operator,
            JoinOperator::LeftJoin | JoinOperator::LeftOuterJoin
        ) {
            if !is_at_most_one_subquery(dt)? {
                return Ok(false);
            }
        } else {
            return Ok(false);
        }
    }

    Ok(true)
}

fn normalize_field_reference(
    fields: &[FieldDefinitionExpr],
    fde: &FieldReference,
) -> ReadySetResult<FieldReference> {
    let expr = match fde {
        FieldReference::Numeric(i) => {
            if *i < 1 || *i > fields.len() as u64 {
                invalid_query!("Order field reference {} out of bounds", *i);
            }
            let (field_expr, _) = expect_field_as_expr(&fields[*i as usize - 1]);
            field_expr.clone()
        }
        FieldReference::Expr(e) => match e {
            Expr::Column(Column { name, table: None }) => {
                if let Some(field_expr) = fields.iter().find_map(|fde| {
                    let field_alias = get_select_item_alias(fde);
                    if field_alias == *name {
                        let (field_expr, _) = expect_field_as_expr(fde);
                        Some(field_expr.clone())
                    } else {
                        None
                    }
                }) {
                    field_expr
                } else {
                    invalid_query!(
                        "Order field references non-existing alias \"{}\"",
                        name.as_str()
                    );
                }
            }
            expr => expr.clone(),
        },
    };
    Ok(FieldReference::Expr(expr))
}

fn normalize_order_by(
    fields: &[FieldDefinitionExpr],
    order: &OrderClause,
) -> ReadySetResult<OrderClause> {
    let mut norm_order = OrderClause {
        order_by: Vec::with_capacity(order.order_by.len()),
    };
    for ord_by in order.order_by.iter() {
        norm_order.order_by.push(OrderBy {
            field: normalize_field_reference(fields, &ord_by.field)?,
            order_type: Some(ord_by.order_type.unwrap_or_default()),
            null_order: ord_by.null_order,
        });
    }
    Ok(norm_order)
}

fn normalize_group_by(
    fields: &[FieldDefinitionExpr],
    group_by: &GroupByClause,
) -> ReadySetResult<GroupByClause> {
    let mut norm_group_by = GroupByClause {
        fields: Vec::with_capacity(group_by.fields.len()),
    };
    for gf in group_by.fields.iter() {
        norm_group_by
            .fields
            .push(normalize_field_reference(fields, gf)?);
    }
    Ok(norm_group_by)
}

/// Returns true if the ORDER BY lists of lhs_stmt and outer_stmt are equivalent,
/// after rebinding any references to the LHS subquery's alias in the outer_stmt
/// to the actual expressions from the inner (LHS) projection.
fn orders_equivalent_under_projection(
    outer_stmt: &SelectStatement,
    inner_stmt: &SelectStatement,
    inner_alias: &SqlIdentifier,
    outer_to_inner_fields: &HashMap<Column, Expr>,
) -> ReadySetResult<bool> {
    // Both sides must actually have ORDER BY lists
    let Some(outer_order_clause) = &outer_stmt.order else {
        return Ok(false);
    };
    let Some(inner_order_clause) = &inner_stmt.order else {
        return Ok(false);
    };

    let norm_inner_order = normalize_order_by(&inner_stmt.fields, inner_order_clause)?.order_by;

    let norm_outer_order = {
        let mut outer_stmt = SelectStatement {
            order: Some(normalize_order_by(&outer_stmt.fields, outer_order_clause)?),
            ..SelectStatement::default()
        };
        rebind_column_refs(
            &mut outer_stmt,
            &inner_alias.clone().into(),
            outer_to_inner_fields,
        )?;
        outer_stmt.order.expect("must have order clause").order_by
    };

    Ok(norm_outer_order.len() <= norm_inner_order.len()
        && norm_inner_order
            .iter()
            .take(norm_outer_order.len())
            .eq(norm_outer_order.iter()))
}

fn normalize_comma_separated_lhs(stmt: &mut SelectStatement) -> ReadySetResult<bool> {
    Ok(if stmt.tables.len() > 1 {
        stmt.join.splice(
            0..0,
            stmt.tables.drain(1..).map(|dt| JoinClause {
                operator: JoinOperator::CrossJoin,
                right: JoinRightSide::Table(dt),
                constraint: JoinConstraint::Empty,
            }),
        );
        true
    } else {
        false
    })
}

fn hoist_lhsmost_derived_table(
    stmt: &mut SelectStatement,
    is_top_select: bool,
) -> ReadySetResult<bool> {
    let Some(lhs_dt) = stmt.tables.first() else {
        return Ok(false);
    };

    let Some((lhs_stmt, lhs_alias)) = as_sub_query_with_alias(lhs_dt) else {
        return Ok(false);
    };

    let Ok(ext_to_int_fields) = build_ext_to_int_fields_map(lhs_stmt, lhs_alias.clone()) else {
        return Ok(false);
    };

    if is_window_function_select(stmt)? || is_window_function_select(lhs_stmt)? {
        return Ok(false);
    }

    let is_lhs_stmt_aggregation_or_grouped = is_aggregation_or_grouped(lhs_stmt)?;
    let is_base_stmt_aggregation_or_grouped = is_aggregation_or_grouped(stmt)?;

    if is_lhs_stmt_aggregation_or_grouped && is_base_stmt_aggregation_or_grouped {
        return Ok(false);
    }

    if !lhs_stmt.limit_clause.is_empty() {
        match (lhs_stmt.order.is_some(), stmt.order.is_some()) {
            (true, true) => {
                if !orders_equivalent_under_projection(
                    stmt,
                    lhs_stmt,
                    &lhs_alias,
                    &ext_to_int_fields,
                )? {
                    return Ok(false);
                }
            }
            (true, false) => {
                // OK: we’ll carry inner ORDER up to the outer.
            }
            (false, true) => {
                // Outer imposes ORDER but inner LIMIT exists: unsafe to reorder rows post-limit.
                return Ok(false);
            }
            (false, false) => { /* fine */ }
        }
    }

    // If both inner and outer have LIMIT/OFFSET, we need to compose them; only do this
    // when all values are numeric literals, otherwise bail out gracefully.
    if !lhs_stmt.limit_clause.is_empty() && !stmt.limit_clause.is_empty() {
        macro_rules! is_number {
            ($lit_opt:expr) => {
                $lit_opt.map(literal_as_number).transpose().is_ok()
            };
        }
        let ok_limits = is_number!(lhs_stmt.limit_clause.limit())
            && is_number!(lhs_stmt.limit_clause.offset())
            && is_number!(stmt.limit_clause.limit())
            && is_number!(stmt.limit_clause.offset());
        if !ok_limits {
            return Ok(false);
        }
    }

    if !all_downstream_joins_cardinality_preserving(stmt)? {
        return Ok(false);
    }

    // Guard against any downstream use of aggregate-derived outputs from the LHS.
    // After inlining, those would expand to raw aggregate expressions in non-aggregate scopes,
    // which is illegal / semantically unsafe.
    if is_lhs_stmt_aggregation_or_grouped {
        let agg_aliases = collect_agg_derived_aliases(lhs_stmt)?;
        if downstream_uses_agg_derived_aliases(stmt, &lhs_alias, &agg_aliases)? {
            return Ok(false);
        }
    }

    if is_lhs_stmt_aggregation_or_grouped && let Some(where_expr) = &stmt.where_clause {
        let lhs_has_limit = !lhs_stmt.limit_clause.is_empty();

        let lhs_rel: Relation = lhs_alias.clone().into();
        let base_other_rels = visible_base_rels_except(stmt, &lhs_rel)?;

        // Extract all conjuncts without modifying the original WHERE
        let mut conjuncts = Vec::new();
        let _ = split_expr(where_expr, &|_| true, &mut conjuncts);

        // Bail if any conjunct references both lhs and a base "other" relation.
        for e in conjuncts {
            let refs_lhs = refs_rel_anywhere(&e, &lhs_rel)?;
            let refs_other = refs_any_of_rels_anywhere(&e, &base_other_rels)?;

            // Intent-revealing flags used in a single combined guard:
            // - mixed_scope: predicate mentions both the LHS alias and some other base relation -> never movable.
            // - lhs_only_with_limit: inner has LIMIT and the predicate mentions only LHS -> would have to push into HAVING -> unsafe.
            // - lhs_only_with_subquery: subqueries in HAVING not supported, so LHS-only predicate containing a subquery is unsafe.
            let mixed_scope = refs_lhs && refs_other;
            let lhs_only = refs_lhs && !refs_other;
            let lhs_only_with_limit = lhs_has_limit && lhs_only;
            let lhs_only_with_subquery = lhs_only && contains_select(&e);

            if mixed_scope || lhs_only_with_limit || lhs_only_with_subquery {
                return Ok(false);
            }
        }
    }

    hoist_lhsmost_from_item(stmt, &ext_to_int_fields, is_top_select)?;

    Ok(true)
}

/// Recursively inlines nested FROM subqueries, then inline items at this query level.
fn hoist_lhsmost_derived_table_rewrite_impl(
    stmt: &mut SelectStatement,
    is_top_select: bool,
) -> ReadySetResult<bool> {
    let mut rewritten = false;

    // Recurse down the left spine exactly once to expose a hoistable core,
    // then attempt a single hoist at this level. We intentionally *do not*
    // call this recursively inside a loop: doing so repeats the same work
    // without changing the AST until a hoist happens.
    if let Some(TableExpr {
        inner: TableExprInner::Subquery(inner_stmt),
        alias: Some(_),
    }) = stmt.tables.first_mut()
    {
        // Normalize the inner left-spine only; do not repeat per-iteration.
        rewritten |= hoist_lhsmost_derived_table_rewrite_impl(inner_stmt, false)?;
    }

    // Single hoist attempt for this level.
    rewritten |= hoist_lhsmost_derived_table(stmt, is_top_select)?;

    Ok(rewritten)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ArrayConstructorRewrite;
    use readyset_sql::{Dialect, DialectDisplay};
    use readyset_sql_parsing::{ParsingPreset, parse_select_with_config};

    const PARSING_CONFIG: ParsingPreset = ParsingPreset::OnlySqlparser;

    fn rewrite_statement(sql_text: &str) -> ReadySetResult<SelectStatement> {
        let mut stmt = parse_select_with_config(PARSING_CONFIG, Dialect::PostgreSQL, sql_text)?;
        stmt.rewrite_array_constructors()?;
        hoist_lhsmost_derived_table_rewrite_impl(&mut stmt, true)?;
        Ok(stmt)
    }

    fn test_it(test_name: &str, original_text: &str, expect_text: &str) {
        match rewrite_statement(original_text) {
            Ok(rewritten_stmt) => {
                println!(
                    ">>>>>> Hoisted: {}",
                    rewritten_stmt.display(Dialect::PostgreSQL)
                );
                let expected_stmt = match parse_select_with_config(
                    PARSING_CONFIG,
                    Dialect::PostgreSQL,
                    expect_text,
                ) {
                    Ok(stmt) => stmt,
                    Err(e) => panic!("> {test_name}: REWRITTEN STATEMENT PARSE ERROR: {e}"),
                };
                assert_eq!(rewritten_stmt, expected_stmt);
            }
            Err(e) => {
                println!("> {test_name}: REWRITE ERROR: {e}");
                assert!(expect_text.is_empty(), "> {test_name}: REWRITE ERROR: {e}")
            }
        }
    }

    // Bail: Window function in the inner subquery
    #[test]
    fn test1() {
        let original = r#"
        SELECT s.x, s.rn FROM (SELECT t.x, row_number() over() AS rn FROM t) AS s"#;
        let expected = original;
        test_it("test1", original, expected);
    }

    // Bail: Non-cardinality preserving downstream join
    #[test]
    fn test2() {
        let original = r#"SELECT s.x, u.y FROM (SELECT t.x, t.z FROM t) AS s JOIN u ON true"#;
        let expected = original;
        test_it("test2", original, expected);
    }

    // Hoist: Combined LIMIT + OFFSET
    #[test]
    fn test3() {
        let original = r#"
        SELECT s.x
          FROM (
              SELECT t.x FROM t ORDER BY x ASC LIMIT 10 OFFSET 5
          ) AS s
        ORDER BY s.x ASC
        LIMIT 7 OFFSET 2"#;
        let expected =
            r#"SELECT "t"."x" FROM "t" ORDER BY "t"."x" ASC NULLS LAST LIMIT 7 OFFSET 7"#;
        test_it("test3", original, expected);
    }

    // Bail: ORDER BY mismatch
    #[test]
    fn test4() {
        let original = r#"
        SELECT s.x
          FROM (
              SELECT t.x, t.y FROM t ORDER BY y DESC LIMIT 10
          ) AS s
        ORDER BY s.x ASC"#;
        let expected = original;
        test_it("test4", original, expected);
    }

    // Hoist: WHERE conjunct references only lhs alias -> should move to HAVING
    #[test]
    fn test5() {
        let original = r#"
        SELECT s.sumx
          FROM (
            SELECT t.a, SUM(t.x) AS sumx FROM t GROUP BY a
          ) AS s
        WHERE s.sumx > 10"#;
        let expected =
            r#"SELECT sum("t"."x") AS "sumx" FROM "t" GROUP BY "t"."a" HAVING (sum("t"."x") > 10)"#;
        test_it("test5", original, expected);
    }

    // Hoist: LEFT join with LIMIT 1 subquery (AtMostOne)
    #[test]
    fn test6() {
        let original = r#"
        SELECT s.x
          FROM (SELECT t.x FROM t) AS s
        LEFT JOIN LATERAL (SELECT u.y FROM u WHERE u.k = s.x LIMIT 1) AS l ON true"#;
        let expected = r#"SELECT "t"."x" FROM "t" LEFT JOIN LATERAL
        (SELECT "u"."y" FROM "u" WHERE ("u"."k" = "t"."x") LIMIT 1) AS "l" ON TRUE"#;
        test_it("test6", original, expected);
    }

    // Bail: INNER join with LIMIT 1 (ExactlyOne?) <= AtMostOne; not provably ExactlyOne
    #[test]
    fn test7() {
        let original = r#"
        SELECT s.x
          FROM (SELECT x FROM t) AS s
        JOIN LATERAL (SELECT y FROM u WHERE u.k = s.x LIMIT 1) AS l ON true"#;
        let expected = original;
        test_it("test7", original, expected);
    }

    // Hoist: Aggregated + TOP-K inner: nothing from outer WHERE should be moved to HAVING -> safe
    #[test]
    fn test8() {
        let original = r#"
        SELECT
            "inner".jn, "inner".sum_qty, tags.tags
        FROM (
            SELECT s.jn, SUM(spj.qty) AS sum_qty, s.sn AS _o0, s.jn AS _o1
            FROM qa.s AS s JOIN qa.spj AS spj ON spj.sn = s.sn
            GROUP BY s.sn, s.jn
            ORDER BY _o0 ASC NULLS LAST, _o1 ASC NULLS LAST
            LIMIT 20
        ) AS "inner",
        LATERAL (
            SELECT ARRAY(
                SELECT p.pn FROM qa.p AS p
                WHERE p.jn = "inner".jn
                ORDER BY p.pn ASC
            ) AS tags
        ) AS tags
        ORDER BY "inner"._o0 ASC NULLS LAST, "inner"._o1 ASC NULLS LAST"#;
        let expected = r#"SELECT "s"."jn", sum("spj"."qty") AS "sum_qty", "tags"."tags" FROM "qa"."s" AS "s"
        JOIN "qa"."spj" AS "spj" ON ("spj"."sn" = "s"."sn") CROSS JOIN LATERAL
        (SELECT coalesce("array_subq"."agg_result", ARRAY[]) AS "tags" FROM
        (SELECT array_agg("inner_subq"."pn" ORDER BY "inner_subq"."pn" ASC NULLS LAST) AS "agg_result" FROM
        (SELECT "p"."pn" FROM "qa"."p" AS "p" WHERE ("p"."jn" = "s"."jn") ORDER BY "p"."pn" ASC NULLS LAST)
        AS "inner_subq") AS "array_subq") AS "tags"  GROUP BY "s"."sn", "s"."jn" ORDER BY "s"."sn" ASC NULLS LAST,
        "s"."jn" ASC NULLS LAST LIMIT 20"#;
        test_it("test8", original, expected);
    }

    // Bail: Aggregated + TOP-K inner: WHERE on inner-only field must be moved to HAVING -> change the inner TOP-K order
    #[test]
    fn test9() {
        let original = r#"
        SELECT
            "inner".jn, "inner".sum_qty, tags.tags
        FROM (
            SELECT s.jn, SUM(spj.qty) AS sum_qty, s.sn AS _o0, s.jn AS _o1
            FROM qa.s AS s JOIN qa.spj AS spj ON spj.sn = s.sn
            GROUP BY s.sn, s.jn
            ORDER BY _o0 ASC NULLS LAST, _o1 ASC NULLS LAST
            LIMIT 20
        ) AS "inner",
        LATERAL (
            SELECT ARRAY(
                SELECT p.pn FROM qa.p AS p
                WHERE p.jn = "inner".jn
                ORDER BY p.pn ASC
            ) AS tags
        ) AS tags
        WHERE "inner".sum_qty > 10
        ORDER BY "inner"._o0 ASC NULLS LAST, "inner"._o1 ASC NULLS LAST
        "#;
        let expected = r#"SELECT "inner"."jn", "inner"."sum_qty", "tags"."tags" FROM
        (SELECT "s"."jn", sum("spj"."qty") AS "sum_qty", "s"."sn" AS "_o0", "s"."jn" AS "_o1" FROM "qa"."s" AS "s"
        JOIN "qa"."spj" AS "spj" ON ("spj"."sn" = "s"."sn") GROUP BY "s"."sn", "s"."jn" ORDER BY "_o0" ASC NULLS LAST,
        "_o1" ASC NULLS LAST LIMIT 20) AS "inner", LATERAL (SELECT coalesce("array_subq"."agg_result", ARRAY[]) AS "tags"
        FROM (SELECT array_agg("inner_subq"."pn" ORDER BY "inner_subq"."pn" ASC NULLS LAST) AS "agg_result" FROM
        (SELECT "p"."pn" FROM "qa"."p" AS "p" WHERE ("p"."jn" = "inner"."jn") ORDER BY "p"."pn" ASC NULLS LAST)
        AS "inner_subq") AS "array_subq") AS "tags" WHERE ("inner"."sum_qty" > 10) ORDER BY "inner"."_o0" ASC NULLS LAST,
        "inner"."_o1" ASC NULLS LAST"#;
        test_it("test9", original, expected);
    }

    // Complex QA-shaped hoist: LHS-most subquery with two CROSS LATERAL array aggregations
    // (simulates the early example with tags / concepts lists)
    #[test]
    fn test10() {
        let original = r#"
        SELECT
            "inner".sn, "inner".pn, "inner".jn, tags.tags, concepts.concepts
        FROM (
            SELECT s.sn, s.pn, s.jn, d1.test_varchar AS _o0, s.sn AS _o1
            FROM qa.s AS s
            INNER JOIN qa.datatypes1 AS d1 ON d1.test_integer = s.status
            WHERE s.city != 'PARIS' AND s.pn >= 'P00000'
            ORDER BY _o0 ASC NULLS LAST, _o1 ASC NULLS LAST
            LIMIT 41
        ) AS "inner",
        LATERAL (
            SELECT ARRAY(
                SELECT p.pn FROM qa.p AS p
                WHERE p.jn = "inner".jn
                ORDER BY p.pn ASC
            ) AS tags
        ) AS tags,
        LATERAL (
            SELECT ARRAY(
                SELECT spj.sn FROM qa.spj AS spj
                WHERE spj.pn = "inner".pn
                ORDER BY spj.sn ASC
            ) AS concepts
        ) AS concepts
        ORDER BY "inner"._o0 ASC NULLS LAST, "inner"._o1 ASC NULLS LAST
        "#;
        let expected = r#"SELECT "s"."sn", "s"."pn", "s"."jn", "tags"."tags", "concepts"."concepts"
        FROM "qa"."s" AS "s" INNER JOIN "qa"."datatypes1" AS "d1" ON ("d1"."test_integer" = "s"."status")
        CROSS JOIN LATERAL (SELECT coalesce("array_subq"."agg_result", ARRAY[]) AS "tags" FROM
        (SELECT array_agg("inner_subq"."pn" ORDER BY "inner_subq"."pn" ASC NULLS LAST) AS "agg_result" FROM
        (SELECT "p"."pn" FROM "qa"."p" AS "p" WHERE ("p"."jn" = "s"."jn")
        ORDER BY "p"."pn" ASC NULLS LAST) AS "inner_subq") AS "array_subq") AS "tags"  CROSS JOIN LATERAL
        (SELECT coalesce("array_subq"."agg_result", ARRAY[]) AS "concepts" FROM (SELECT array_agg("inner_subq"."sn"
        ORDER BY "inner_subq"."sn" ASC NULLS LAST) AS "agg_result" FROM (SELECT "spj"."sn" FROM "qa"."spj" AS "spj"
        WHERE ("spj"."pn" = "s"."pn") ORDER BY "spj"."sn" ASC NULLS LAST) AS "inner_subq") AS "array_subq") AS "concepts"
        WHERE (("s"."city" != 'PARIS') AND ("s"."pn" >= 'P00000'))
        ORDER BY "d1"."test_varchar" ASC NULLS LAST, "s"."sn" ASC NULLS LAST LIMIT 41"#;
        test_it("test10", original, expected);
    }

    // Limit composition: inner LIMIT/OFFSET with matching ORDER BY and outer LIMIT/OFFSET.
    // After hoist, LIMIT/OFFSET must be composed correctly: limit = min(outer.limit, inner.limit - outer.offset);
    // offset = inner.offset + outer.offset.
    #[test]
    fn test11() {
        let original = r#"
        SELECT inner.sn
        FROM (
          SELECT s.sn, d1.test_varchar AS _o0, s.sn AS _o1
          FROM qa.s AS s
          LEFT JOIN qa.datatypes1 AS d1 ON d1.test_integer = s.status
          ORDER BY _o0 ASC NULLS LAST, _o1 ASC NULLS LAST
          LIMIT 50 OFFSET 5
        ) AS inner
        ORDER BY inner._o0 ASC NULLS LAST, inner._o1 ASC NULLS LAST
        LIMIT 10 OFFSET 3
        "#;
        let expected = r#"SELECT "s"."sn" FROM "qa"."s" AS "s" LEFT JOIN "qa"."datatypes1" AS "d1" ON ("d1"."test_integer" = "s"."status") ORDER BY "d1"."test_varchar" ASC NULLS LAST, "s"."sn" ASC NULLS LAST LIMIT 10 OFFSET 8"#;
        test_it("test11", original, expected);
    }

    // Bail: CROSS LATERAL with an AtMostOne RHS (0 or 1 rows) must *not* hoist.
    // Here the RHS groups by p.jn and will yield 0 rows if no matching p exists.
    #[test]
    fn test12() {
        let original = r#"
        SELECT inner.sn
        FROM (
          SELECT s.sn, s.jn, s.sn AS _o0, s.sn AS _o1
          FROM qa.s AS s
          ORDER BY _o0 ASC NULLS LAST, _o1 ASC NULLS LAST
          LIMIT 100
        ) AS inner,
        LATERAL (
          SELECT max(p.pn) AS mx
          FROM qa.p AS p
          WHERE p.jn = inner.jn
          GROUP BY p.jn
        ) AS lmt
        ORDER BY inner._o0 ASC NULLS LAST, inner._o1 ASC NULLS LAST
        "#;
        let expected = original;
        test_it("test12", original, expected);
    }

    // Hoist: CROSS LATERAL with ExactlyOne RHS (aggregate-only, no GROUP BY)
    #[test]
    fn test13() {
        let original = r#"
        SELECT
            inner.sn, l.cnt
        FROM (
            SELECT s.sn, s.sn AS _o0, s.sn AS _o1
            FROM qa.s AS s
            ORDER BY _o0 ASC NULLS LAST, _o1 ASC NULLS LAST
            LIMIT 10
        ) AS inner,
        LATERAL (
            SELECT COUNT(*) AS cnt
            FROM qa.j AS j
            WHERE j.sn = inner.sn
        ) AS l
        ORDER BY inner._o0 ASC NULLS LAST, inner._o1 ASC NULLS LAST
        "#;
        let expected = r#"SELECT "s"."sn", "l"."cnt" FROM "qa"."s" AS "s" CROSS JOIN LATERAL
        (SELECT count(*) AS "cnt" FROM "qa"."j" AS "j" WHERE ("j"."sn" = "s"."sn")) AS "l"
        ORDER BY "s"."sn" ASC NULLS LAST, "s"."sn" ASC NULLS LAST
        LIMIT 10"#;
        test_it("test13", original, expected);
    }

    // Bail: INNER JOIN LATERAL with non-tautological ON predicate (rejecting ON)
    #[test]
    fn test14() {
        let original = r#"
        SELECT inner.sn
        FROM (
          SELECT s.sn FROM qa.s AS s
        ) AS inner
        JOIN LATERAL (
          SELECT COUNT(*) AS cnt FROM qa.j AS j WHERE j.sn = inner.sn
        ) AS l ON (l.cnt > 0)
        "#;
        let expected = original;
        test_it("test14", original, expected);
    }

    // Hoist: INNER JOIN with ExactlyOne RHS (aggregate-only) and ON TRUE
    #[test]
    fn test15() {
        let original = r#"
        SELECT inner.sn, c.cnt
        FROM (
          SELECT s.sn FROM qa.s AS s
        ) AS inner
        JOIN (SELECT COUNT(*) AS cnt FROM qa.p AS p) AS c ON TRUE
        "#;
        let expected = r#"SELECT "s"."sn", "c"."cnt" FROM "qa"."s" AS "s" JOIN (SELECT count(*) AS "cnt" FROM "qa"."p" AS "p") AS "c" ON TRUE"#;
        test_it("test15", original, expected);
    }

    // Hoist: additional bare FROM item (CROSS) that is ExactlyOne -> allowed
    #[test]
    fn test16() {
        let original = r#"
        SELECT inner.sn, c.cnt
        FROM (
          SELECT s.sn, s.sn AS _o0, s.sn AS _o1
          FROM qa.s AS s
          ORDER BY _o0 ASC NULLS LAST, _o1 ASC NULLS LAST
          LIMIT 3
        ) AS inner,
        (SELECT COUNT(*) AS cnt FROM qa.p AS p) AS c
        ORDER BY inner._o0 ASC NULLS LAST, inner._o1 ASC NULLS LAST
        "#;
        let expected = r#"SELECT "s"."sn", "c"."cnt" FROM "qa"."s" AS "s" CROSS JOIN (SELECT count(*) AS "cnt"
        FROM "qa"."p" AS "p") AS "c"
        ORDER BY "s"."sn" ASC NULLS LAST, "s"."sn" ASC NULLS LAST
        LIMIT 3"#;
        test_it("test16", original, expected);
    }

    // Bail: additional bare FROM item (CROSS) that is AtMostOne (LIMIT 1) -> unsafe
    #[test]
    fn test17() {
        let original = r#"
        SELECT inner.sn
        FROM (
          SELECT s.sn, s.sn AS _o0, s.sn AS _o1
          FROM qa.s AS s
          ORDER BY _o0 ASC NULLS LAST, _o1 ASC NULLS LAST
          LIMIT 5
        ) AS inner,
        (SELECT pn FROM qa.p AS p WHERE p.weight > 100 LIMIT 1) AS c
        ORDER BY inner._o0 ASC NULLS LAST, inner._o1 ASC NULLS LAST
        "#;
        let expected = original;
        test_it("test17", original, expected);
    }

    // Hoist: LEFT JOIN LATERAL with AtMostOne RHS (GROUP BY key) -> allowed
    #[test]
    fn test18() {
        let original = r#"
        SELECT inner.sn, l.maxpn
        FROM (
          SELECT s.sn, s.pn, s.sn AS _o0, s.sn AS _o1
          FROM qa.s AS s
          ORDER BY _o0 ASC NULLS LAST, _o1 ASC NULLS LAST
          LIMIT 5
        ) AS inner
        LEFT JOIN LATERAL (
          SELECT MAX(p.pn) AS maxpn
          FROM qa.p AS p
          WHERE p.sn = inner.sn
          GROUP BY p.sn
        ) AS l ON TRUE
        ORDER BY inner._o0 ASC NULLS LAST, inner._o1 ASC NULLS LAST
        "#;
        let expected = r#"SELECT "s"."sn", "l"."maxpn" FROM "qa"."s" AS "s" LEFT JOIN LATERAL
        (SELECT max("p"."pn") AS "maxpn" FROM "qa"."p" AS "p" WHERE ("p"."sn" = "s"."sn") GROUP BY "p"."sn") AS "l" ON TRUE
        ORDER BY "s"."sn" ASC NULLS LAST, "s"."sn" ASC NULLS LAST
        LIMIT 5"#;
        test_it("test18", original, expected);
    }

    // Hoist: ORDER BY alias in outer rebinding to inner expression
    #[test]
    fn test19() {
        let original = r#"
        SELECT s.a
        FROM (
            SELECT t.a, (t.a + 1) AS ax
            FROM t
            ORDER BY ax ASC NULLS LAST
            LIMIT 10
        ) AS s
        ORDER BY s.ax ASC NULLS LAST
        LIMIT 5
        "#;
        let expected = r#"SELECT "t"."a" FROM "t" ORDER BY ("t"."a" + 1) ASC NULLS LAST LIMIT 5"#;
        test_it("test19", original, expected);
    }

    // Bail: Aggregated inner with TOP-K; WHERE has a scalar subquery (cannot push to HAVING)
    #[test]
    fn test20() {
        let original = r#"
        SELECT s.sumx
        FROM (
            SELECT t.a, SUM(t.x) AS sumx
            FROM t
            GROUP BY t.a
            ORDER BY t.a ASC NULLS LAST
            LIMIT 10
        ) AS s
        WHERE s.sumx > (SELECT COUNT(*) FROM u)
        "#;
        let expected = original;
        test_it("test20", original, expected);
    }

    // Hoist: LIMIT/OFFSET composition corner case where resulting LIMIT becomes 0
    #[test]
    fn test21() {
        let original = r#"
        SELECT inner.sn
        FROM (
            SELECT s.sn, s.sn AS _o0, s.sn AS _o1
            FROM qa.s AS s
            ORDER BY _o0 ASC NULLS LAST, _o1 ASC NULLS LAST
            LIMIT 3 OFFSET 10
        ) AS inner
        ORDER BY inner._o0 ASC NULLS LAST, inner._o1 ASC NULLS LAST
        LIMIT 5 OFFSET 7
        "#;
        let expected = r#"SELECT "s"."sn" FROM "qa"."s" AS "s" ORDER BY "s"."sn" ASC NULLS LAST, "s"."sn" ASC NULLS LAST LIMIT 0 OFFSET 17"#;
        test_it("test21", original, expected);
    }

    // Hoist: Outer ORDER is a prefix of inner ORDER (allowed)
    #[test]
    fn test22() {
        let original = r#"
        SELECT s.a
        FROM (
            SELECT t.a, t.b
            FROM t
            ORDER BY t.a ASC NULLS LAST, t.b DESC NULLS LAST
            LIMIT 10
        ) AS s
        ORDER BY s.a ASC NULLS LAST
        LIMIT 4
        "#;
        let expected = r#"SELECT "t"."a" FROM "t" ORDER BY "t"."a" ASC NULLS LAST, "t"."b" DESC NULLS LAST LIMIT 4"#;
        test_it("test22", original, expected);
    }

    // Bail: Inner has LIMIT without ORDER, outer imposes ORDER -> unsafe, must not hoist
    #[test]
    fn test23() {
        let original = r#"
        SELECT s.x
        FROM (
            SELECT t.x
            FROM t
            LIMIT 10
        ) AS s
        ORDER BY s.x ASC NULLS LAST
        "#;
        let expected = original;
        test_it("test23", original, expected);
    }

    // Bail: Aggregated inner with TOP-K; outer WHERE conjunct mixes LHS and other base rel -> must not hoist
    #[test]
    fn test24() {
        let original = r#"
        SELECT s.sumx
        FROM (
            SELECT t.a, SUM(t.x) AS sumx
            FROM t
            GROUP BY t.a
            ORDER BY t.a ASC NULLS LAST
            LIMIT 10
        ) AS s,
        (SELECT COUNT(*) AS c FROM u) AS ctab
        WHERE s.sumx > ctab.c
        "#;
        let expected = original;
        test_it("test24", original, expected);
    }

    // AtMostOne via aggregate-only core wrapped by a single-projecting SELECT (no GROUP BY at wrapper)
    #[test]
    fn test25() {
        let original = r#"
    SELECT inner.sn, l.cnt
    FROM (
      SELECT s.sn, s.sn AS _o0, s.sn AS _o1
      FROM qa.s AS s
      ORDER BY _o0 ASC NULLS LAST, _o1 ASC NULLS LAST
      LIMIT 10
    ) AS inner
    LEFT JOIN LATERAL (
      SELECT a1.cnt
      FROM (
        SELECT COUNT(*) AS cnt
        FROM qa.j AS j
        WHERE j.sn = inner.sn
        GROUP BY j.sn
      ) AS a1
    ) AS l ON TRUE
    ORDER BY inner._o0 ASC NULLS LAST, inner._o1 ASC NULLS LAST
    "#;
        let expected = r#"SELECT "s"."sn", "l"."cnt" FROM "qa"."s" AS "s" LEFT JOIN LATERAL
        (SELECT "a1"."cnt" FROM (SELECT count(*) AS "cnt" FROM "qa"."j" AS "j" WHERE ("j"."sn" = "s"."sn") GROUP BY "j"."sn") AS "a1") AS "l" ON TRUE
        ORDER BY "s"."sn" ASC NULLS LAST, "s"."sn" ASC NULLS LAST
        LIMIT 10"#;
        test_it("test25", original, expected);
    }

    // AtMostOne via LIMIT 1 deep inside a wrapper (detected by has_limit_one_deep)
    #[test]
    fn test26() {
        let original = r#"
    SELECT inner.sn, l.y
    FROM (
      SELECT s.sn, s.sn AS _o0, s.sn AS _o1
      FROM qa.s AS s
      ORDER BY _o0 ASC NULLS LAST, _o1 ASC NULLS LAST
      LIMIT 10
    ) AS inner
    LEFT JOIN LATERAL (
      SELECT o.y
      FROM (
        SELECT p.pn AS y
        FROM qa.p AS p
        WHERE p.sn = inner.sn
        ORDER BY p.pn ASC NULLS LAST
        LIMIT 1
      ) AS o
    ) AS l ON TRUE
    ORDER BY inner._o0 ASC NULLS LAST, inner._o1 ASC NULLS LAST
    "#;
        let expected = r#"SELECT "s"."sn", "l"."y" FROM "qa"."s" AS "s" LEFT JOIN LATERAL
        (SELECT "o"."y" FROM (SELECT "p"."pn" AS "y" FROM "qa"."p" AS "p" WHERE ("p"."sn" = "s"."sn")
        ORDER BY "p"."pn" ASC NULLS LAST LIMIT 1) AS "o") AS "l" ON TRUE
        ORDER BY "s"."sn" ASC NULLS LAST, "s"."sn" ASC NULLS LAST
        LIMIT 10"#;
        test_it("test26", original, expected);
    }

    // Bail: INNER JOIN with wrapper whose core is only AtMostOne (LIMIT 1 deep) – not ExactlyOne
    #[test]
    fn test27() {
        let original = r#"
    SELECT inner.sn
    FROM (
      SELECT s.sn, s.sn AS _o0, s.sn AS _o1
      FROM qa.s AS s
      ORDER BY _o0 ASC NULLS LAST, _o1 ASC NULLS LAST
      LIMIT 7
    ) AS inner
    JOIN LATERAL (
      SELECT o.y
      FROM (
        SELECT p.pn AS y
        FROM qa.p AS p
        WHERE p.sn = inner.sn
        ORDER BY p.pn ASC NULLS LAST
        LIMIT 1
      ) AS o
    ) AS l ON TRUE
    ORDER BY inner._o0 ASC NULLS LAST, inner._o1 ASC NULLS LAST
    "#;
        let expected = original;
        test_it("test27", original, expected);
    }

    // Bail: CROSS (bare FROM) item that is only AtMostOne (LIMIT 1) – CROSS requires ExactlyOne
    #[test]
    fn test28() {
        let original = r#"
    SELECT inner.sn
    FROM (
      SELECT s.sn, s.sn AS _o0, s.sn AS _o1
      FROM qa.s AS s
      ORDER BY _o0 ASC NULLS LAST, _o1 ASC NULLS LAST
      LIMIT 5
    ) AS inner,
    (
      SELECT o.y
      FROM (
        SELECT p.pn AS y
        FROM qa.p AS p
        ORDER BY p.pn ASC NULLS LAST
        LIMIT 1
      ) AS o
    ) AS c
    ORDER BY inner._o0 ASC NULLS LAST, inner._o1 ASC NULLS LAST
    "#;
        let expected = original;
        test_it("test28", original, expected);
    }

    // AtMostOne via GROUP BY pinned by correlation, wrapped once (no GROUP BY at wrapper level)
    #[test]
    fn test29() {
        let original = r#"
    SELECT inner.sn, l.mx
    FROM (
      SELECT s.sn, s.sn AS _o0, s.sn AS _o1
      FROM qa.s AS s
      ORDER BY _o0 ASC NULLS LAST, _o1 ASC NULLS LAST
      LIMIT 5
    ) AS inner
    LEFT JOIN LATERAL (
      SELECT w.mx
      FROM (
        SELECT MAX(p.pn) AS mx
        FROM qa.p AS p
        WHERE p.sn = inner.sn
        GROUP BY p.sn
      ) AS w
    ) AS l ON TRUE
    ORDER BY inner._o0 ASC NULLS LAST, inner._o1 ASC NULLS LAST
    "#;
        let expected = r#"SELECT "s"."sn", "l"."mx" FROM "qa"."s" AS "s" LEFT JOIN LATERAL
        (SELECT "w"."mx" FROM (SELECT max("p"."pn") AS "mx" FROM "qa"."p" AS "p" WHERE ("p"."sn" = "s"."sn")
        GROUP BY "p"."sn") AS "w") AS "l" ON TRUE
        ORDER BY "s"."sn" ASC NULLS LAST, "s"."sn" ASC NULLS LAST
        LIMIT 5"#;
        test_it("test29", original, expected);
    }

    // Bail: wrapper introduces unpinned GROUP BY (not fixed by correlation) -> not AtMostOne
    #[test]
    fn test30() {
        let original = r#"
    SELECT inner.sn
    FROM (
      SELECT s.sn, s.sn AS _o0, s.sn AS _o1
      FROM qa.s AS s
      ORDER BY _o0 ASC NULLS LAST, _o1 ASC NULLS LAST
      LIMIT 5
    ) AS inner
    LEFT JOIN LATERAL (
      SELECT w.mx
      FROM (
        SELECT MAX(p.pn) AS mx, p.sn
        FROM qa.p AS p
        GROUP BY p.sn
      ) AS w
    ) AS l ON TRUE
    ORDER BY inner._o0 ASC NULLS LAST, inner._o1 ASC NULLS LAST
    "#;
        let expected = original;
        test_it("test30", original, expected);
    }

    // Bail: wrapper adds a join that can replicate rows -> not AtMostOne even though core is agg-only
    #[test]
    fn test31() {
        let original = r#"
    SELECT inner.sn
    FROM (
      SELECT s.sn, s.sn AS _o0, s.sn AS _o1
      FROM qa.s AS s
      ORDER BY _o0 ASC NULLS LAST, _o1 ASC NULLS LAST
      LIMIT 6
    ) AS inner
    LEFT JOIN LATERAL (
      SELECT a1.cnt, p2.pn
      FROM (
        SELECT COUNT(*) AS cnt
        FROM qa.j AS j
        WHERE j.sn = inner.sn
      ) AS a1
      JOIN qa.p AS p2 ON TRUE
    ) AS l ON TRUE
    ORDER BY inner._o0 ASC NULLS LAST, inner._o1 ASC NULLS LAST
    "#;
        let expected = original;
        test_it("test31", original, expected);
    }

    // AtMostOne via LIMIT 1 two levels deep under wrappers
    #[test]
    fn test32() {
        let original = r#"
    SELECT inner.sn, l.v
    FROM (
      SELECT s.sn, s.sn AS _o0, s.sn AS _o1
      FROM qa.s AS s
      ORDER BY _o0 ASC NULLS LAST, _o1 ASC NULLS LAST
      LIMIT 9
    ) AS inner
    LEFT JOIN LATERAL (
      SELECT lvl1.v
      FROM (
        SELECT o.y AS v
        FROM (
          SELECT p.pn AS y
          FROM qa.p AS p
          WHERE p.sn = inner.sn
          ORDER BY p.pn ASC NULLS LAST
          LIMIT 1
        ) AS o
      ) AS lvl1
    ) AS l ON TRUE
    ORDER BY inner._o0 ASC NULLS LAST, inner._o1 ASC NULLS LAST
    "#;
        let expected = r#"SELECT "s"."sn", "l"."v" FROM "qa"."s" AS "s" LEFT JOIN LATERAL
        (SELECT "lvl1"."v" FROM (SELECT "o"."y" AS "v" FROM (SELECT "p"."pn" AS "y" FROM "qa"."p" AS "p"
        WHERE ("p"."sn" = "s"."sn") ORDER BY "p"."pn" ASC NULLS LAST LIMIT 1) AS "o") AS "lvl1") AS "l" ON TRUE
        ORDER BY "s"."sn" ASC NULLS LAST, "s"."sn" ASC NULLS LAST
        LIMIT 9"#;
        test_it("test32", original, expected);
    }
}
