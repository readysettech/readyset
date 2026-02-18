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
//! ```
//!
//! **After:**
//! ```sql
//! SELECT o.id, SUM(o.amount) AS total, tags.t
//! FROM orders AS o
//! CROSS JOIN LATERAL (
//!     SELECT ARRAY_AGG(t.name) AS t
//!     FROM tags AS t
//!     WHERE t.order_id = o.id
//! ) AS tags
//! GROUP BY o.id, tags.t
//! ORDER BY o.id
//! LIMIT 100
//! ```
//!
//! # Key Transformations
//!
//! - Column rebinding: References to the subquery alias are replaced with the
//!   actual projected expressions from the inner query.
//! - Alias deduplication: Inner FROM aliases that conflict with outer names are
//!   renamed to avoid ambiguity.
//! - WHERE → HAVING migration: When the inner query is aggregated, outer WHERE
//!   predicates that reference only the inner alias move to HAVING.
//! - LIMIT/OFFSET composition: When both inner and outer have numeric limits,
//!   they are combined (e.g., inner LIMIT 50 OFFSET 5 + outer LIMIT 10 OFFSET 3
//!   becomes LIMIT 10 OFFSET 8).
//!
//! # Safety Constraints
//!
//! The pass bails out (leaves the query unchanged) when inlining would alter semantics:
//!
//! - Cardinality preservation: Downstream joins must not change row counts.
//!   CROSS/INNER joins require RHS to produce exactly one row; LEFT joins allow
//!   at most one row.
//! - Window functions: Queries with window functions are not inlined.
//! - ORDER BY conflicts: If inner has LIMIT and outer imposes a different ORDER,
//!   inlining is unsafe.
//! - Nested aggregation: Both inner and outer being aggregated is not supported.
//! - Non-literal limits: LIMIT/OFFSET with placeholders or expressions cannot
//!   be composed.
//!
//! # Invariants
//!
//! This pass is intentionally conservative and relies on these invariants:
//!
//! * **Single-shot hoist**: we recurse only down the left spine once and then attempt at most one hoist per level.
//! * **No semantic reordering under TOP-K**: if the inner has LIMIT/OFFSET, we only hoist when ORDER BY is absent in the outer
//!   or the outer ORDER BY is a prefix-compatible view of the inner ORDER BY after projection rebinding.
//! * **Cardinality preservation**: downstream joins must be 1:1 (ExactlyOne for CROSS/INNER; AtMostOne for LEFT) and must have
//!   non-rejecting join constraints (ON TRUE / empty).
//! * **Guard downstream unnesting**: we bail if hoisting would turn a supported subquery-predicate LHS column into NULL or into a
//!   non-column in a context where later unnesting cannot legally keep it in WHERE.
//! * **GROUP BY compatibility**: when hoisting a grouped LHS into a non-grouped outer, any downstream output referenced by the
//!   outermost expressions must be a bare `rel.col` so it can become a GROUP BY key; mixed expressions over downstream outputs are rejected.
//! * **Engine constraint**: GROUP BY must project at least one aggregate-derived field; we bail if hoisting would produce a GROUP BY
//!   query with no aggregate-derived projection.
use crate::drop_redundant_join::{deep_columns_visitor, deep_columns_visitor_mut};
use crate::rewrite_utils::{
    alias_for_expr, analyse_fix_correlated_subquery_group_by, and_predicates_skip_true,
    as_sub_query_with_alias, collect_local_from_items, columns_iter, contains_select,
    default_alias_for_select_item_expression, expect_field_as_expr, expect_field_as_expr_mut,
    expect_only_subquery_from_with_alias, expect_sub_query_with_alias_mut,
    for_each_window_function, get_from_item_reference_name, get_select_item_alias,
    get_unique_alias, hoist_parametrizable_join_filters_to_where, is_aggregated_expr,
    is_aggregated_select, is_simple_parametrizable_filter, normalize_comma_separated_lhs,
    outermost_expression, split_correlated_constraint, split_correlated_expression, split_expr,
    split_expr_mut,
};
use crate::unnest_subqueries::{
    AggNoGbyCardinality, agg_only_no_gby_cardinality, has_limit_one_deep,
    is_supported_subquery_predicate,
};
use crate::{as_column, get_local_from_items_iter, get_local_from_items_iter_mut, is_column_of};
use itertools::Either;
use itertools::Itertools;
use readyset_errors::{
    ReadySetError, ReadySetResult, internal, invalid_query, invalid_query_err, unsupported,
};
use readyset_sql::analysis::visit::{Visitor, walk_select_statement};
use readyset_sql::ast::{
    Column, Expr, FieldDefinitionExpr, FieldReference, GroupByClause, InValue, JoinConstraint,
    JoinOperator, JoinRightSide, LimitClause, LimitValue, Literal, OrderBy, OrderClause, Relation,
    SelectStatement, SqlIdentifier, TableExpr, TableExprInner, UnaryOperator,
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
/// Also reserves aliases bound inside downstream subquery scopes to avoid name-capture after hoist.
fn make_inner_aliases_distinct_from_base_statement(
    base_stmt: &SelectStatement,
    inner_dt: &mut TableExpr,
    reserved_aliases: &HashSet<Relation>,
) -> ReadySetResult<bool> {
    // Collect base statement FROM item names, excluding the one we are going to inline
    let mut base_locals = get_local_from_items_iter!(base_stmt)
        .map(get_from_item_reference_name)
        .collect::<ReadySetResult<HashSet<Relation>>>()?;
    base_locals.remove(&get_from_item_reference_name(inner_dt)?);

    // Also reserve aliases that appear inside downstream subquery scopes. After hoisting,
    // inlinable FROM-item aliases become visible at the base level; if a downstream subquery
    // already binds the same alias locally, the new base-level alias could be shadowed.
    base_locals.extend(reserved_aliases.iter().cloned());

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

/// Collect all local FROM-item reference names in `stmt`, descending into **all** nested SELECT
/// statements (including subqueries inside expressions such as EXISTS/IN/scalar subqueries).
fn collect_local_from_item_refs_deep(
    stmt: &SelectStatement,
    out: &mut HashSet<Relation>,
) -> ReadySetResult<()> {
    struct AliasCollector<'a> {
        out: &'a mut HashSet<Relation>,
    }

    impl<'ast> Visitor<'ast> for AliasCollector<'_> {
        type Error = ReadySetError;

        fn visit_select_statement(
            &mut self,
            stmt: &'ast SelectStatement,
        ) -> Result<(), Self::Error> {
            // Collect aliases/tables bound in this SELECT's FROM/JOIN scope.
            for dt in get_local_from_items_iter!(stmt) {
                self.out.insert(get_from_item_reference_name(dt)?);
            }
            // Recurse into all nested SELECTs (FROM-subqueries and expression subqueries).
            walk_select_statement(self, stmt)
        }
    }

    let mut v = AliasCollector { out };
    v.visit_select_statement(stmt)?;

    Ok(())
}

/// Collect aliases bound *inside* downstream derived-table subqueries.
///
/// Why: After hoisting the LHS-most derived table, its internal FROM aliases become visible at the
/// base level. If a downstream subquery already binds the same alias locally, that alias would be
/// shadowed (name capture) inside the downstream scope. We therefore reserve all aliases that appear
/// inside downstream subquery scopes when deduplicating inner aliases.
///
/// Note: we intentionally descend into **all** nested SELECTs, including expression subqueries.
fn collect_downstream_scoped_aliases(
    base_stmt: &SelectStatement,
) -> ReadySetResult<HashSet<Relation>> {
    let mut out = HashSet::new();
    for dt in get_local_from_items_iter!(base_stmt).skip(1) {
        if let Some((dt_stmt, _dt_alias)) = as_sub_query_with_alias(dt) {
            collect_local_from_item_refs_deep(dt_stmt, &mut out)?;
        }
    }
    Ok(out)
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

/// Return true if any downstream derived table references a LHS output column that would expand to a
/// non-trivial expression after hoisting.
///
/// Why: Later passes (notably subquery unnesting / decorrelation) often require join predicates in the
/// shape `col = col`. If a downstream subquery references `lhs_alias.col` and that column maps to an
/// expression (not a column) after hoisting, we may block later rewrites or create unsupported join
/// conditions. This is a hard bail-out guardrail.
fn downstream_reference_non_trivial_lhs_output(
    base_stmt: &SelectStatement,
    lhs_alias: &SqlIdentifier,
    outer_to_inner_fields: &HashMap<Column, Expr>,
) -> ReadySetResult<bool> {
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
            if is_column_of!(col, lhs_rel)
                && let Some(expr) = outer_to_inner_fields.get(col)
                && !matches!(expr, Expr::Column(_))
            {
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
    is_top_select: bool,
    downstream_group_by_additions: Vec<Expr>,
) -> ReadySetResult<()> {
    // Get the inlinable FROM item's statement
    let (lhs_stmt, lhs_stmt_alias) = expect_sub_query_with_alias_mut(&mut lhs_dt);
    let mut lhs_stmt = mem::take(lhs_stmt);

    // IMPORTANT: build the rebinding map after any alias de-duplication performed on the
    // inlinable statement, otherwise the mapped expressions can contain stale table aliases.
    let ext_to_int_fields = build_ext_to_int_fields_map(&lhs_stmt, lhs_stmt_alias.clone())?;

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
        &ext_to_int_fields,
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
        if !downstream_group_by_additions.is_empty() {
            let gb = base_stmt.group_by.get_or_insert_default();
            for e in downstream_group_by_additions {
                let fr = FieldReference::Expr(e);
                if !gb.fields.contains(&fr) {
                    gb.fields.push(fr);
                }
            }
        }
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
    // Compatibility is checked earlier using `orders_equivalent_under_projection` function.
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
    // Resolve numeric and alias references in ORDER BY
    if let Some(order_by) = &stmt.order {
        stmt.order = Some(normalize_order_by(&stmt.fields, order_by)?);
    }

    // Resolve numeric and alias references in GROUP BY
    if let Some(group_by) = &stmt.group_by {
        stmt.group_by = Some(normalize_group_by(&stmt.fields, group_by)?);
    }

    Ok(())
}

/// Inline a subquery FROM item: rename aliases, hoist filters, substitute columns, and splice in tables/joins.
fn hoist_lhsmost_from_item(
    base_stmt: &mut SelectStatement,
    is_top_select: bool,
    downstream_group_by_additions: Vec<Expr>,
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

    // Resolve the numeric and alias references in GROUP BY and ORDER BY for the LHS-most statement
    {
        let (lhs_stmt, _) = expect_sub_query_with_alias_mut(&mut lhs_dt);
        normalize_group_and_order_by(lhs_stmt)?;
    }

    // Convert comma separated tables into cross-joined sequence.
    // This will help to preserve Postgres compatible join shape after hoisting the LHS-most derived table.
    normalize_comma_separated_lhs(base_stmt)?;

    // Resolve the numeric and alias references in GROUP BY and ORDER BY for the base statement
    normalize_group_and_order_by(base_stmt)?;

    // Make sure the inner FROM item's aliases of the LHS-most statement do not clash with
    // the existing base statement's FROM items
    let reserved_aliases = collect_downstream_scoped_aliases(base_stmt)?;
    make_inner_aliases_distinct_from_base_statement(base_stmt, &mut lhs_dt, &reserved_aliases)?;

    // Embed the LHS-most derived table into the base statement
    hoist_lhsmost_from_item_internals(
        base_stmt,
        lhs_dt,
        is_top_select,
        downstream_group_by_additions,
    )?;

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

/// Verify downstream joins do not change the row count of the hoistable LHS.
///
/// This is the primary semantic safety condition:
/// * CROSS / INNER joins must be ExactlyOne (1 row per outer row), since 0 rows would filter out the
///   base row and >1 would replicate it.
/// * LEFT joins may be AtMostOne (0..1), which preserves the base row count.
/// * Join constraints must be non-rejecting (ON TRUE / empty), otherwise hoisting can change which
///   rows survive.
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
    // Both sides must have ORDER BY lists. When inner has LIMIT/OFFSET, its ORDER BY defines Top-K
    // semantics. We only allow hoisting when the outer ORDER BY (after projection rebinding) is a
    // prefix of the inner ORDER BY. Prefix is enough because outer ORDER BY may omit tie-breakers
    // that inner ORDER BY uses for Top-K stability.
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

fn is_agg_derived_outputs(
    expr: &Expr,
    lhs_rel: &Relation,
    outer_to_inner_fields: &HashMap<Column, Expr>,
) -> ReadySetResult<bool> {
    Ok(columns_iter(expr)
        .filter_map(|col| {
            if is_column_of!(col, *lhs_rel) {
                Some(col.clone())
            } else {
                None
            }
        })
        .all(|col| {
            if let Some(e) = outer_to_inner_fields.get(&col) {
                is_aggregated_expr(e).is_ok_and(|is_agg| is_agg)
            } else {
                false
            }
        }))
}

/// Guardrail: ensure hoisting will not break later unnesting of supported subquery predicates.
///
/// We only allow hoisting when:
/// * the inner subquery predicate is not correlated with the hoistable relation at the base level;
/// * if the predicate LHS is a simple `lhs_alias.col`, rebinding does not turn it into NULL;
/// * if rebinding would turn it into a non-column expression, we only allow that in WHERE and only
///   for the non-negated path, because `unnest_subqueries::join_derived_table` can keep the remainder
///   comparison in WHERE for INNER joins but rejects expression LHS for anti-joins.
fn is_base_subquery_predicate_allow_hoisting(
    subquery_predicate: &Expr,
    in_where: bool,
    hoist_rel: &Relation,
    outer_to_inner_fields: &HashMap<Column, Expr>,
) -> ReadySetResult<bool> {
    // Get the LHS and SUBQUERY parts of the subquery predicate.
    // **NOTE**: the caller site has already guaranteed the supported shape of this construct.
    let (lhs, negated, stmt) = match subquery_predicate {
        Expr::UnaryOp {
            op: UnaryOperator::Not,
            rhs,
        } if matches!(rhs.as_ref(), Expr::Exists(_)) => (
            None,
            true,
            match rhs.as_ref() {
                Expr::Exists(sq) => sq.as_ref(),
                // It should not be reachable, be conservative, just bail out
                _ => return Ok(false),
            },
        ),
        Expr::Exists(sq) => (None, false, sq.as_ref()),
        Expr::In {
            lhs,
            rhs: InValue::Subquery(sq),
            negated,
        } => (Some(lhs.as_ref()), *negated, sq.as_ref()),
        Expr::BinaryOp { lhs, rhs, .. } => match (lhs.as_ref(), rhs.as_ref()) {
            (_, Expr::NestedSelect(sq)) => (Some(lhs.as_ref()), false, sq.as_ref()),
            (Expr::NestedSelect(sq), _) => (Some(rhs.as_ref()), false, sq.as_ref()),
            // It should not be reachable, be conservative, just bail out
            _ => return Ok(false),
        },
        Expr::NestedSelect(sq) => (None, false, sq.as_ref()),
        // It should not be reachable, be conservative, just bail out
        _ => return Ok(false),
    };

    // Bail, *if* a subquery predicate is correlated with the hoistable relation at the base level
    let mut found = false;
    deep_columns_visitor(stmt, hoist_rel, &mut |expr| {
        let col = as_column!(expr);
        if is_column_of!(col, *hoist_rel) {
            found = true;
        }
    })?;
    if found {
        return Ok(false);
    }

    // The LHS of the subquery predicate is a simple column from the hoist-able relation.
    // Bail, *if* the hoisting might stop it from un-nesting.
    // **NOTE**: if the LHS originally was an expression, the hoisting should not impact the un-nesting.
    if let Some(lhs) = lhs
        && let Expr::Column(col) = lhs
        && let Some(rebound_expr) = outer_to_inner_fields.get(col)
    {
        match rebound_expr {
            // Bail: it will be rebound into NULL literal
            Expr::Literal(Literal::Null) => return Ok(false),
            Expr::Column(_) | Expr::Literal(_) => {
                // OK: It will be rebound into a simple column or non-null literal.
            }
            // It will be rebound into an expression, though originally was a simple column.
            // Bail, if it cannot be transitioned to WHERE
            _ => {
                if !in_where || negated {
                    return Ok(false);
                }
                // **NOTE** later unnesting must keep expression comparisons in WHERE and never generate ON!=(col=col).
            }
        }
    }

    Ok(true)
}

/// Guardrail for the engine's parametrizable-filter shape (WHERE-only).
///
/// `is_simple_parametrizable_filter` recognizes filters like `col OP literal` that the engine can
/// parameterize efficiently. If such a filter references `lhs_alias.col`, hoisting must not turn that
/// column into an expression; otherwise the filter stops being parametrizable and can regress planning.
/// This check applies only to WHERE conjuncts (SELECT/ORDER are allowed to become expressions).
fn is_base_param_filter_allow_hoisting(
    expr: &Expr,
    hoist_rel: &Relation,
    outer_to_inner_fields: &HashMap<Column, Expr>,
) -> ReadySetResult<bool> {
    let mut is_allow = true;
    let is_filter = is_simple_parametrizable_filter(
        expr,
        &mut |col_tab: &Relation, col_name: &SqlIdentifier| {
            is_allow = if hoist_rel.eq(col_tab) {
                if let Some(e) = outer_to_inner_fields.get(&Column {
                    table: Some(hoist_rel.clone()),
                    name: col_name.clone(),
                }) {
                    // OK: the column remains a simple column after hoisting
                    matches!(e, Expr::Column(_))
                } else {
                    // The column has no mapping: be conservative, just bail.
                    false
                }
            } else {
                // OK: the filter doesn't reference the hoistable relation
                true
            };
            true
        },
    );
    Ok(!is_filter || is_allow)
}

/// Return true if hoisting would block downstream subquery-unnesting.
///
/// We analyze each top-level conjunct:
/// * supported subquery predicates are checked via `is_base_subquery_predicate_allow_hoisting`;
/// * in WHERE only, simple parametrizable filters are checked via `is_base_param_filter_allow_hoisting`.
///
/// The visitor is scope-aware (skips nested scopes that shadow the hoisted relation).
fn is_hoisting_block_unnesting(
    expr: &Expr,
    in_where: bool,
    hoist_rel: &Relation,
    outer_to_inner_fields: &HashMap<Column, Expr>,
) -> ReadySetResult<bool> {
    let mut is_block = false;
    split_expr_mut(
        expr,
        &mut |conjunct| {
            if is_block {
                // stop analysis if already found blocking conjunct
                return false;
            }
            if is_supported_subquery_predicate(conjunct) {
                if !is_base_subquery_predicate_allow_hoisting(
                    conjunct,
                    in_where,
                    hoist_rel,
                    outer_to_inner_fields,
                )
                .is_ok_and(|is_ok| is_ok)
                {
                    is_block = true;
                }
            } else if in_where
                && !is_base_param_filter_allow_hoisting(conjunct, hoist_rel, outer_to_inner_fields)
                    .is_ok_and(|is_ok| is_ok)
            {
                is_block = true;
            }
            false
        },
        &mut vec![],
    );

    Ok(is_block)
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

    let lhs_rel: Relation = lhs_alias.clone().into();

    // Build a projection-rebinding map for *analysis* (ORDER BY compatibility, unnesting guards).
    // IMPORTANT: we rebuild the map later (after alias de-duplication) for the actual substitution;
    // using a pre-dedup map for rewriting would be unsound because table aliases may change.
    let Ok(outer_to_inner_fields) = build_ext_to_int_fields_map(lhs_stmt, lhs_alias.clone()) else {
        return Ok(false);
    };

    if !all_downstream_joins_cardinality_preserving(stmt)? {
        return Ok(false);
    }

    if downstream_reference_non_trivial_lhs_output(stmt, &lhs_alias, &outer_to_inner_fields)? {
        return Ok(false);
    }

    if let Some(where_expr) = &stmt.where_clause
        && is_hoisting_block_unnesting(where_expr, true, &lhs_rel, &outer_to_inner_fields)?
    {
        return Ok(false);
    }

    for fe in &stmt.fields {
        let (fe_expr, _) = expect_field_as_expr(fe);
        if is_hoisting_block_unnesting(fe_expr, false, &lhs_rel, &outer_to_inner_fields)? {
            return Ok(false);
        }
    }

    if is_window_function_select(stmt)? || is_window_function_select(lhs_stmt)? {
        return Ok(false);
    }

    let is_lhs_stmt_aggregation_or_grouped = is_aggregation_or_grouped(lhs_stmt)?;
    let is_base_stmt_aggregation_or_grouped = is_aggregation_or_grouped(stmt)?;

    if is_lhs_stmt_aggregation_or_grouped && is_base_stmt_aggregation_or_grouped {
        return Ok(false);
    }

    // Grouped-LHS into non-grouped outer:
    // After hoist, the base query becomes grouped. To remain valid (and match engine constraints),
    // every outermost expression must be either:
    //   (a) a bare downstream `rel.col` (becomes a GROUP BY key), or
    //   (b) an expression over LHS outputs where every referenced LHS column is aggregate-derived.
    // Mixed expressions that mention downstream outputs (e.g., `inner.sum + tags.x`) are rejected.
    let mut downstream_group_by_additions: Vec<Expr> = Vec::new();
    if is_lhs_stmt_aggregation_or_grouped && !is_base_stmt_aggregation_or_grouped {
        // Collect all RHS downstream relations references
        let mut other_rels = collect_local_from_items(stmt)?;
        other_rels.remove(&lhs_rel);

        for e in outermost_expression(stmt) {
            let is_refs_other_rels = refs_any_of_rels_anywhere(e, &other_rels)?;
            if is_refs_other_rels {
                if matches!(e, Expr::Column(Column { table: Some(t), .. }) if other_rels.contains(t))
                {
                    downstream_group_by_additions.push(e.clone());
                } else {
                    return Ok(false);
                }
            }
            if let Expr::Column(c) = e
                && is_column_of!(c, lhs_rel)
            {
                // Simple `lhs_rel.col` reference: OK
            } else if refs_rel_anywhere(e, &lhs_rel)? {
                // Expression referencing `lhs_rel` columns: OK *if* all `lhs_rel` columns are aggregate-derived.
                if !is_agg_derived_outputs(e, &lhs_rel, &outer_to_inner_fields)? {
                    return Ok(false);
                }
            }
        }

        // Engine constraint: GROUP BY queries must project at least one aggregate-derived expression.
        // The outer query is originally non-grouped (no aggregates). After hoist, the only way to
        // satisfy this is for the outer SELECT list to reference at least one aggregate-derived LHS
        // output (directly or inside an expression that only uses aggregate-derived LHS columns).
        let mut is_agg_present = false;
        for fe in &stmt.fields {
            let (fe_expr, _) = expect_field_as_expr(fe);
            if refs_rel_anywhere(fe_expr, &lhs_rel)?
                && is_agg_derived_outputs(fe_expr, &lhs_rel, &outer_to_inner_fields)?
            {
                is_agg_present = true;
                break;
            }
        }
        if !is_agg_present {
            return Ok(false);
        }
    }

    if !lhs_stmt.limit_clause.is_empty() {
        match (lhs_stmt.order.is_some(), stmt.order.is_some()) {
            (true, true) => {
                if !orders_equivalent_under_projection(
                    stmt,
                    lhs_stmt,
                    &lhs_alias,
                    &outer_to_inner_fields,
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

    hoist_lhsmost_from_item(stmt, is_top_select, downstream_group_by_additions)?;

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

    // Bail: WHERE contains a parametrizable filter that would change after hoisting
    #[test]
    fn test5() {
        let original = r#"
        SELECT s.sumx
          FROM (
            SELECT t.a, SUM(t.x) AS sumx FROM t GROUP BY a
          ) AS s
        WHERE s.sumx > 10"#;
        let expected = original;
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
        (SELECT "p"."pn" AS "pn" FROM "qa"."p" AS "p" WHERE ("p"."jn" = "s"."jn") ORDER BY "p"."pn" ASC NULLS LAST)
        AS "inner_subq") AS "array_subq") AS "tags"  GROUP BY "s"."sn", "s"."jn", "tags"."tags" ORDER BY "s"."sn" ASC NULLS LAST,
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
        (SELECT "p"."pn" AS "pn" FROM "qa"."p" AS "p" WHERE ("p"."jn" = "inner"."jn") ORDER BY "p"."pn" ASC NULLS LAST)
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
        (SELECT "p"."pn" AS "pn" FROM "qa"."p" AS "p" WHERE ("p"."jn" = "s"."jn")
        ORDER BY "p"."pn" ASC NULLS LAST) AS "inner_subq") AS "array_subq") AS "tags"  CROSS JOIN LATERAL
        (SELECT coalesce("array_subq"."agg_result", ARRAY[]) AS "concepts" FROM (SELECT array_agg("inner_subq"."sn"
        ORDER BY "inner_subq"."sn" ASC NULLS LAST) AS "agg_result" FROM (SELECT "spj"."sn" AS "sn" FROM "qa"."spj" AS "spj"
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

    #[test]
    fn test33() {
        let original = r#"
    SELECT s.rownum, s.test_dec, tags.test_dec
    FROM (SELECT o.rownum, SUM(o.test_dec) AS test_dec
       FROM qa.datatypes AS o
       GROUP BY o.rownum
       ORDER BY o.rownum
       LIMIT 10
    ) AS s
    CROSS JOIN LATERAL (SELECT ARRAY_AGG(o.test_varchar) AS test_dec
       FROM qa.datatypes1 AS o
       WHERE o.rownum = s.rownum
    ) AS tags
    ORDER BY s.rownum;
    "#;
        let expected = r#"SELECT "o1"."rownum", sum("o1"."test_dec") AS "test_dec", "tags"."test_dec" FROM "qa"."datatypes" AS "o1"
        CROSS JOIN LATERAL (SELECT array_agg("o"."test_varchar") AS "test_dec" FROM "qa"."datatypes1" AS "o"
        WHERE ("o"."rownum" = "o1"."rownum")) AS "tags"
        GROUP BY "o1"."rownum", "tags"."test_dec"
        ORDER BY "o1"."rownum" ASC NULLS LAST
        LIMIT 10"#;
        test_it("test33", original, expected);
    }

    // Bail: hoisting will block un-nesting of the `NOT IN`
    #[test]
    fn test34() {
        let original = r#"
    SELECT count(*) AS should_equal_baseline
    FROM (SELECT
        dt.RowNum,
        CONCAT(NULL, NULL) AS x
      FROM DataTypes dt
    ) AS v
    WHERE v.x NOT IN (SELECT d1.test_char
      FROM DataTypes1 d1
      LIMIT 1
    );
    "#;
        let expected = original;
        test_it("test34", original, expected);
    }

    // Bail: hoisting would cause `Using group by without aggregates is not yet supported`
    #[test]
    fn test35() {
        let original = r#"
    SELECT
        "inner".sn,
        tags.ts
    FROM (
        SELECT s.sn, SUM(spj.qty) AS sum_qty
        FROM qa.s AS s
        JOIN qa.spj AS spj ON spj.sn = s.sn
        GROUP BY s.sn
    ) AS "inner",
    (
        SELECT min(p.pn) AS ts
        FROM qa.p AS p
    ) AS tags
    ORDER BY "inner".sn;
    "#;
        let expected = original;
        test_it("test35", original, expected);
    }

    // Hoist: **Note**, duplicate alias `o` should be deduplicated.
    #[test]
    fn test36() {
        let original = r#"
      SELECT s.rownum, s.test_dec, tags.test_dec
      FROM (SELECT o.rownum, SUM(o.test_dec) AS test_dec
           FROM qa.datatypes AS o
           GROUP BY o.rownum
           ORDER BY o.rownum
           LIMIT 10
      ) AS s
      CROSS JOIN LATERAL (SELECT ARRAY_AGG(o.test_varchar) AS test_dec
           FROM qa.datatypes1 AS o
           WHERE o.rownum = s.rownum
      ) AS tags
      ORDER BY s.rownum;
      "#;
        let expected = r#"SELECT "o1"."rownum", sum("o1"."test_dec") AS "test_dec", "tags"."test_dec"
        FROM "qa"."datatypes" AS "o1" CROSS JOIN LATERAL (SELECT array_agg("o"."test_varchar") AS "test_dec"
        FROM "qa"."datatypes1" AS "o" WHERE ("o"."rownum" = "o1"."rownum")) AS "tags"
        GROUP BY "o1"."rownum", "tags"."test_dec"
        ORDER BY "o1"."rownum" ASC NULLS LAST
        LIMIT 10"#;
        test_it("test36", original, expected);
    }
}
