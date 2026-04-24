//! Subquery inlining infrastructure.
//!
//! This module provides the building blocks for inlining a derived-table subquery
//! into a base statement:
//!
//! - [`InlineCandidate`] — holds the extracted inner `SelectStatement`, its alias,
//!   and the column-rebinding map.
//! - [`InliningContext`] — shared context carrying the inner/outer statements,
//!   rebinding map, downstream-position info, and pre-computed derived values
//!   used by every eligibility check.
//! - [`prepare_inline`] — extracts the inner statement from a `TableExpr` subquery
//!   and builds the rebinding map.
//! - [`compute_downstream_for_position`] — computes `(downstream_tables,
//!   downstream_joins)` for the cardinality-preservation analysis from the
//!   flat ordinal position of the inlined item.
//! - [`can_inline_subquery`] — canonical eligibility entry point.  Composes the
//!   eligibility checks in canonical order; returns `Ok(Some(downstream_group_by_
//!   additions))` on accept, `Ok(None)` on reject.  Caller-preference flags on
//!   [`InliningContext`] (e.g. `skip_unnesting_guard`) tune the check sequence
//!   without requiring callers to compose the individual checks themselves.
//! - [`inline_from_item_position_checks`] — position-dependent eligibility
//!   checks layered on top of [`can_inline_subquery`].
//! - [`apply_inline`] — performs all position-independent transformations
//!   (column rebinding, WHERE/HAVING merge, GROUP BY/DISTINCT merge,
//!   ORDER BY carry-up, LIMIT/OFFSET composition).
//!
//! These are consumed by `inline_leading_derived_table`, `derived_tables_rewrite`,
//! and `lateral_join` (LATERAL flattening).

use crate::derived_tables_rewrite::{
    can_inline_left_join_rhs_safe, can_move_joins_on_nontrivial_expr_to_where,
};
use crate::rewrite_utils::{
    and_predicates_skip_true, are_group_by_keys_pinned_by_correlation, as_sub_query_with_alias,
    build_ext_to_int_fields_map, collect_local_from_items, columns_iter, contains_select,
    deep_columns_expr_visitor, deep_columns_visitor, deep_columns_visitor_mut,
    default_alias_for_select_item_expression, expect_field_as_expr, expect_field_as_expr_mut,
    expect_only_subquery_from_with_alias, expect_sub_query_with_alias_mut,
    extract_correlation_keys, find_rhs_join_clause, for_each_window_function,
    get_select_item_alias, is_aggregated_expr, is_aggregation_or_grouped,
    is_simple_parametrizable_filter, outermost_expression, partition_correlated_predicates,
    resolve_field_reference, split_expr, split_expr_mut, substitute_columns_in_expr,
};
use crate::unnest_subqueries::{
    AggNoGbyCardinality, agg_only_no_gby_cardinality, has_limit_one_deep,
    is_supported_join_condition, is_supported_subquery_predicate,
};
use crate::util::would_create_self_join;
use crate::{as_column, get_local_from_items_iter, is_column_of, is_single_from_item};
use itertools::{Either, Itertools};
use readyset_errors::{
    ReadySetError, ReadySetResult, internal_err, invalid_query, invalid_query_err,
};
use readyset_sql::ast::{
    Column, Expr, FieldDefinitionExpr, FieldReference, InValue, JoinClause, JoinConstraint,
    JoinOperator, JoinRightSide, LimitClause, LimitValue, Literal, OrderBy, OrderClause, Relation,
    SelectStatement, SqlIdentifier, TableExpr, TableExprInner, UnaryOperator,
};
use std::collections::{HashMap, HashSet};
use std::{iter, mem};

/// Holds the extracted inner `SelectStatement`, its alias, and the column-rebinding map
/// built from the subquery's projection.
pub(crate) struct InlineCandidate {
    pub(crate) stmt: SelectStatement,
    pub(crate) alias: SqlIdentifier,
    pub(crate) ext_to_int: HashMap<Column, Expr>,
}

/// Shared context for every eligibility-check helper.
///
/// Constructed once by the caller at the entry of [`can_inline_subquery`],
/// passed by reference internally.
///
/// The `is_*` / `inner_rel` fields are derived values computed at construction
/// time — callers that already have the underlying statements can reuse the
/// derived values for their own pre-checks (e.g.
/// `derived_tables_rewrite::can_inline_from_item`'s agg + multi-FROM guard
/// uses `ctx.is_inner_agg` instead of re-calling
/// `is_aggregation_or_grouped(inl_stmt)?`).
pub(crate) struct InliningContext<'a> {
    pub(crate) inner_stmt: &'a SelectStatement,
    pub(crate) outer_stmt: &'a SelectStatement,
    pub(crate) inner_alias: &'a SqlIdentifier,
    pub(crate) ext_to_int: &'a HashMap<Column, Expr>,
    pub(crate) inl_from_item_ord_idx: usize,
    pub(crate) downstream_tables: &'a [TableExpr],
    pub(crate) downstream_joins: &'a [JoinClause],
    pub(crate) is_top_select: bool,
    pub(crate) skip_unnesting_guard: bool,

    // Derived values — computed once at construction time.
    pub(crate) inner_rel: Relation,
    pub(crate) is_inner_agg: bool,
    pub(crate) is_outer_agg: bool,
}

/// Extract the inner statement from a `TableExpr` subquery and build the
/// external-to-internal column rebinding map.
pub(crate) fn prepare_inline(mut lhs_dt: TableExpr) -> ReadySetResult<InlineCandidate> {
    let (lhs_stmt, lhs_alias) = expect_sub_query_with_alias_mut(&mut lhs_dt);
    let stmt = mem::take(lhs_stmt);
    let alias = lhs_alias.clone();
    let ext_to_int = build_ext_to_int_fields_map(&stmt, alias.clone())?;
    Ok(InlineCandidate {
        stmt,
        alias,
        ext_to_int,
    })
}

// ---------------------------------------------------------------------------
// Local helpers shared by checks and `apply_inline`
// ---------------------------------------------------------------------------

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
pub(crate) fn replace_columns_with_inlinable_expr(
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

/// Collect the set of base FROM-item relations at this level, excluding `lhs_rel`.
pub(crate) fn visible_base_rels_except(
    base_stmt: &SelectStatement,
    lhs_rel: &Relation,
) -> ReadySetResult<HashSet<Relation>> {
    let mut base = collect_local_from_items(base_stmt)?;
    base.remove(lhs_rel);
    Ok(base)
}

/// Return true if `expr` references `rel` anywhere, descending into nested subqueries
/// but skipping subqueries that shadow `rel` with a local FROM-item of the same name.
pub(crate) fn refs_rel_anywhere(expr: &Expr, rel: &Relation) -> ReadySetResult<bool> {
    let mut seen = false;
    deep_columns_expr_visitor(expr, rel, &mut |e| {
        if is_column_of!(as_column!(e), *rel) {
            seen = true;
        }
    })?;
    Ok(seen)
}

/// Return true if `expr` references any relation in `rels` (scope-aware).
pub(crate) fn refs_any_of_rels_anywhere(
    expr: &Expr,
    rels: &HashSet<Relation>,
) -> ReadySetResult<bool> {
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

    // All GROUP BY keys must appear verbatim in the projection for DISTINCT to be
    // redundant. Resolve each field reference (numeric or alias) to its underlying
    // expression so the check is shape-agnostic.
    for fr in &group_by.fields {
        let resolved = resolve_field_reference(&stmt.fields, fr)?;
        if !stmt.fields.iter().any(|fde| {
            let (fde_expr, _) = expect_field_as_expr(fde);
            fde_expr.eq(&resolved)
        }) {
            return Ok(false);
        }
    }
    Ok(true)
}

pub(crate) fn literal_as_number(lit: &Literal) -> ReadySetResult<u64> {
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

pub(crate) fn normalize_field_reference(
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

pub(crate) fn normalize_order_by(
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

/// Returns true if the ORDER BY lists of lhs_stmt and outer_stmt are equivalent,
/// after rebinding any references to the LHS subquery's alias in the outer_stmt
/// to the actual expressions from the inner (LHS) projection.
pub(crate) fn orders_equivalent_under_projection(
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
        outer_stmt
            .order
            .ok_or_else(|| {
                ReadySetError::Internal("order clause removed unexpectedly during rebind".into())
            })?
            .order_by
    };

    Ok(norm_outer_order.len() <= norm_inner_order.len()
        && norm_inner_order
            .iter()
            .take(norm_outer_order.len())
            .eq(norm_outer_order.iter()))
}

// ---------------------------------------------------------------------------
// Eligibility predicate helpers (consumed by the eligibility checks below)
// ---------------------------------------------------------------------------

/// Precise window-function inlinability check. Returns `true` (= reject)
/// if inlining would change the semantics of any window function or land
/// the engine in an unsupported shape.
///
/// Two invariants:
///
/// 1. **No outer reference to an inlinable projection that contains a
///    window function.** Inlining substitutes outer references with the
///    inlinable-side projection expression. If that expression contains
///    a window function, the WF would evaluate over the outer's joined
///    FROM rather than the inlinable's isolated FROM — partition sizes
///    and row counts differ, so RANK/ROW_NUMBER/SUM-OVER values change.
///    The check walks each projection expression recursively: a WF
///    nested inside any structure (`RANK() OVER (...) + 1`, etc.)
///    counts. "Referenced" means "appears in any outermost-scope
///    expression" — SELECT, WHERE, HAVING, ORDER BY, JOIN ON.
///    If no outer expression references the column, substitution has
///    no insertion site and the projection's WF simply doesn't appear
///    in the rewritten query.
///
/// 2. **Outer WFs must not reference inner aggregate outputs.** After
///    inlining, an outer WF's PARTITION BY / ORDER BY (or any other
///    sub-expression of the WF) would contain a nested aggregate, e.g.
///    `ROW_NUMBER() OVER (ORDER BY SUM(...))`, which the engine does
///    not support.
fn window_functions_block_inlining(
    outer_stmt: &SelectStatement,
    inner_rel: &Relation,
    ext_to_int: &HashMap<Column, Expr>,
) -> ReadySetResult<bool> {
    // Collect all columns from the inner alias that the outer actually references.
    let outer_refs_to_inner: HashSet<Column> = outermost_expression(outer_stmt)
        .flat_map(columns_iter)
        .filter(|c| c.table.as_ref() == Some(inner_rel))
        .cloned()
        .collect();

    // Check A: reject if any referenced inner column maps to a WF expression.
    for (col, expr) in ext_to_int {
        let mut is_wf = false;
        for_each_window_function(expr, &mut |_| is_wf = true)?;
        if is_wf && outer_refs_to_inner.contains(col) {
            return Ok(true); // reject
        }
    }

    // Check B: reject if an outer WF references an inner aggregate-derived column.
    let mut wf_columns: HashSet<Column> = HashSet::new();
    for expr in outermost_expression(outer_stmt) {
        for_each_window_function(expr, &mut |wf_expr| {
            wf_columns.extend(columns_iter(wf_expr).cloned());
        })?;
    }
    for col in &wf_columns {
        if col.table.as_ref() == Some(inner_rel)
            && let Some(inl_expr) = ext_to_int.get(col)
            && is_aggregated_expr(inl_expr)?
        {
            return Ok(true); // reject
        }
    }

    Ok(false) // allow
}

/// Compute `(downstream_tables, downstream_joins)` for the cardinality-
/// preservation analysis in [`can_inline_subquery`], given the flat ordinal
/// position of the inlined item.
///
/// Precise: uses `find_rhs_join_clause` for the join-RHS walk rather than
/// the blanket "all joins" over-approximation previously inlined at the
/// LATERAL flatten call site.
#[allow(dead_code)] // Used once lateral_join / DTR migrate (commits 3-4).
pub(crate) fn compute_downstream_for_position(
    base_stmt: &SelectStatement,
    inl_from_item_ord_idx: usize,
) -> (&[TableExpr], &[JoinClause]) {
    if inl_from_item_ord_idx < base_stmt.tables.len() {
        return (
            &base_stmt.tables[inl_from_item_ord_idx + 1..],
            &base_stmt.join[..],
        );
    }
    match find_rhs_join_clause(base_stmt, inl_from_item_ord_idx) {
        Some((jc_idx, _)) => (&[], &base_stmt.join[jc_idx + 1..]),
        None => (&[], &base_stmt.join[..]),
    }
}

/// Shared cardinality-barrier eligibility predicate used by both
/// [`can_inline_subquery`] and `derived_tables_rewrite::can_inline_from_item`.
///
/// Rejects inlining (returns `true`) when the inner statement carries a
/// cardinality barrier (a `LIMIT`/`OFFSET`, or aggregation/grouping that
/// collapses rows) **and** the outer has a cardinality-sensitive operation
/// (aggregation/`GROUP BY`/`DISTINCT`, or a window function in the SELECT list).
///
/// Absorbing the inner into such an outer either changes the row set those
/// operations see (LIMIT moves outward → WF/DISTINCT/aggregate evaluate over
/// un-bounded rows) or lands the WF in an engine-unsupported context
/// (inner GROUP BY → outer becomes aggregated + WF, which violates §9).
///
/// `is_outer_agg` and `is_inner_agg` are expected to equal
/// `is_aggregation_or_grouped(...)` for their respective statements; callers
/// typically already have them computed.
// TODO(commit-5): replace this stub with the real cardinality-barrier guard.
// The scaffolding commit keeps the predicate a no-op so behavior is preserved
// while the three callers retain their bespoke eligibility logic.  The real
// implementation lands in the "Activate cardinality-barrier guard" commit.
fn cardinality_barrier_blocks_inlining(
    _outer_stmt: &SelectStatement,
    _inner_stmt: &SelectStatement,
    _is_outer_agg: bool,
    _is_inner_agg: bool,
) -> bool {
    // Stub for Commit 1; real implementation lands in Commit 5
    // ("Activate cardinality-barrier guard").
    false
}

/// Return true if any downstream derived table (other than the inlinable itself)
/// references a column from the inlined alias that maps to a non-column expression
/// after rebinding.
///
/// The inlinable's own body is excluded from the scan: its internal column
/// references may use the same alias name as `lhs_alias` (e.g. a subquery
/// aliased `t2` that internally also queries `test2 AS t2`), but those are a
/// different scope and should not trigger the guard.
fn downstream_reference_non_trivial_lhs_output(
    base_stmt: &SelectStatement,
    lhs_alias: &SqlIdentifier,
    outer_to_inner_fields: &HashMap<Column, Expr>,
) -> ReadySetResult<bool> {
    let lhs_rel: Relation = lhs_alias.clone().into();

    for rhs_stmt in get_local_from_items_iter!(base_stmt).filter_map(|dt| {
        if let Some((rhs_stmt, rhs_alias)) = as_sub_query_with_alias(dt) {
            // Skip the inlinable itself: its internal body may reference its own
            // tables using the same alias name as lhs_alias, which would cause
            // false positives.  Only check OTHER subqueries that survive inlining
            // and could legitimately reference the inlinable via its outer alias.
            if rhs_alias != *lhs_alias {
                Some(rhs_stmt)
            } else {
                None
            }
        } else {
            None
        }
    }) {
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

fn is_base_subquery_predicate_allow_hoisting(
    subquery_predicate: &Expr,
    in_where: bool,
    hoist_rel: &Relation,
    outer_to_inner_fields: &HashMap<Column, Expr>,
) -> ReadySetResult<bool> {
    let (lhs, negated, stmt) = match subquery_predicate {
        Expr::UnaryOp {
            op: UnaryOperator::Not,
            rhs,
        } if matches!(rhs.as_ref(), Expr::Exists(_)) => (
            None,
            true,
            match rhs.as_ref() {
                Expr::Exists(sq) => sq.as_ref(),
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
            _ => return Ok(false),
        },
        Expr::NestedSelect(sq) => (None, false, sq.as_ref()),
        _ => return Ok(false),
    };

    // Bail if correlated with the hoistable relation at the base level
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

    if let Some(lhs) = lhs
        && let Expr::Column(col) = lhs
        && let Some(rebound_expr) = outer_to_inner_fields.get(col)
    {
        match rebound_expr {
            Expr::Literal(Literal::Null) => return Ok(false),
            Expr::Column(_) | Expr::Literal(_) => {}
            _ => {
                if !in_where || negated {
                    return Ok(false);
                }
            }
        }
    }

    Ok(true)
}

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
                    matches!(e, Expr::Column(_))
                } else {
                    false
                }
            } else {
                true
            };
            true
        },
    );
    Ok(!is_filter || is_allow)
}

/// Return true if inlining would block downstream subquery-unnesting.
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

// ---------------------------------------------------------------------------
// Downstream-join cardinality analysis (consumed by the cardinality-
// preservation eligibility check)
// ---------------------------------------------------------------------------

fn is_exactly_one_card(stmt: &SelectStatement) -> ReadySetResult<bool> {
    Ok(matches!(
        agg_only_no_gby_cardinality(stmt)?,
        Some(AggNoGbyCardinality::ExactlyOne)
    ))
}

fn is_at_most_one_deep(stmt: &SelectStatement) -> ReadySetResult<bool> {
    if has_limit_one_deep(stmt) {
        return Ok(true);
    }
    let mut cur = stmt;
    loop {
        if agg_only_no_gby_cardinality(cur)?.is_some() {
            return Ok(true);
        }
        if cur.group_by.is_some() {
            if let Some(where_expr) = &cur.where_clause {
                let locals = collect_local_from_items(cur)?;
                let (maybe_corr, _remaining) =
                    partition_correlated_predicates(where_expr, &|rel| !locals.contains(rel));
                if let Some(corr) = maybe_corr {
                    let cols_set = extract_correlation_keys(&corr, &locals)?;
                    if are_group_by_keys_pinned_by_correlation(&cols_set, cur)? {
                        return Ok(true);
                    }
                }
            }
        } else {
            if let Ok((inner, _alias)) = expect_only_subquery_from_with_alias(cur) {
                cur = inner;
                continue;
            }
        }
        return Ok(false);
    }
}

fn is_on_nonrejecting(c: &JoinConstraint) -> bool {
    matches!(
        c,
        JoinConstraint::Empty | JoinConstraint::On(Expr::Literal(Literal::Boolean(true)))
    )
}

/// Verify that downstream joins do not change the row count.
///
/// Only needed when the inlined subquery is aggregated or has LIMIT — in those
/// cases, GROUP BY / LIMIT move to the outer level, and downstream joins that
/// change cardinality would alter group membership or which rows survive.
///
/// * CROSS / INNER joins: RHS must produce exactly 1 row.
/// * LEFT joins: RHS may produce 0..1 rows.
/// * Join constraints must be non-rejecting (ON TRUE / empty).
fn downstream_joins_cardinality_preserving(
    downstream_tables: &[TableExpr],
    downstream_joins: &[JoinClause],
) -> ReadySetResult<bool> {
    for dt in downstream_tables {
        let Some((rhs_stmt, _)) = as_sub_query_with_alias(dt) else {
            return Ok(false);
        };
        if !is_exactly_one_card(rhs_stmt)? {
            return Ok(false);
        }
    }
    for jc in downstream_joins {
        let Ok(dt) = jc.right.table_exprs().exactly_one() else {
            return Ok(false);
        };
        if !is_on_nonrejecting(&jc.constraint) {
            return Ok(false);
        }
        let Some((rhs_stmt, _)) = as_sub_query_with_alias(dt) else {
            return Ok(false);
        };
        if jc.operator.is_inner_join() {
            if !is_exactly_one_card(rhs_stmt)? {
                return Ok(false);
            }
        } else if matches!(
            jc.operator,
            JoinOperator::LeftJoin | JoinOperator::LeftOuterJoin
        ) {
            if !is_at_most_one_deep(rhs_stmt)? {
                return Ok(false);
            }
        } else {
            return Ok(false);
        }
    }
    Ok(true)
}

// ---------------------------------------------------------------------------
// Eligibility checks (composed by `can_inline_subquery`)
// ---------------------------------------------------------------------------

/// Window-function interaction check.  Rejects when (a) an outer-referenced
/// inner column maps to a window-function expression, or (b) an outer window
/// function references an inner aggregate output.
///
/// Delegates to `window_functions_block_inlining`, which returns `true`
/// when the combination is unsafe — we invert that to the "accept"
/// convention every check uses here.
fn check_window_function_interaction(ctx: &InliningContext) -> ReadySetResult<bool> {
    let blocked = window_functions_block_inlining(ctx.outer_stmt, &ctx.inner_rel, ctx.ext_to_int)?;
    Ok(!blocked)
}

/// Nested-aggregation rejection.  If BOTH the inner subquery and the outer
/// statement are aggregated/grouped (or DISTINCT), hoisting the inner's
/// aggregation into the outer produces nested aggregate expressions the
/// engine cannot plan.
fn check_nested_aggregation(ctx: &InliningContext) -> ReadySetResult<bool> {
    Ok(!(ctx.is_inner_agg && ctx.is_outer_agg))
}

/// Grouped-engine validation.  Engine constraint: after inlining,
/// if the outer acquires a GROUP BY from the inner, the outer SELECT
/// must EITHER reference at least one aggregate-derived inner column
/// OR project every GROUP BY key as a standalone column reference
/// (the downstream `fix_groupby_without_aggregates` pass converts the
/// latter to DISTINCT, matching each GROUP BY key against a complete
/// SELECT field verbatim).
fn check_grouped_engine_validation(ctx: &InliningContext) -> ReadySetResult<bool> {
    let Some(group_by) = &ctx.inner_stmt.group_by else {
        return Ok(true);
    };

    let has_select_aggregate = ctx.outer_stmt.fields.iter().any(|f| {
        let (expr, _) = expect_field_as_expr(f);
        columns_iter(expr).any(|c| {
            ctx.ext_to_int
                .get(c)
                .is_some_and(|e| is_aggregated_expr(e).unwrap_or(false))
        })
    });

    if has_select_aggregate {
        return Ok(true);
    }

    let standalone_select_inner: Vec<&Expr> = ctx
        .outer_stmt
        .fields
        .iter()
        .filter_map(|f| {
            let (expr, _) = expect_field_as_expr(f);
            if let Expr::Column(col) = expr
                && col.table.as_ref() == Some(&ctx.inner_rel)
            {
                ctx.ext_to_int.get(col)
            } else {
                None
            }
        })
        .collect();

    let all_gb_keys_standalone = group_by.fields.iter().all(|gf| {
        resolve_field_reference(&ctx.inner_stmt.fields, gf)
            .is_ok_and(|e| standalone_select_inner.contains(&&e))
    });

    Ok(all_gb_keys_standalone)
}

/// Returns `true` if every `lhs_rel` column referenced by `expr` maps to an
/// aggregated inner expression.
///
/// Used by `check_group_by_compatibility` to accept mixed expressions in the
/// outer SELECT over a grouped inner only when **every** `lhs_rel` column in
/// the expression is aggregate-derived.  This is deliberately conservative
/// and stricter than standard SQL requires.
///
/// **Known over-restriction / follow-up.**  SQL accepts any grouped-query
/// SELECT expression whose every column reference is **either** aggregate-
/// derived **or** a `GROUP BY` key of the grouping.  The current predicate
/// rejects valid cases like:
///
/// ```sql
/// -- inner = SELECT k, SUM(x) AS sum FROM t GROUP BY k
/// SELECT s.sum + s.k FROM (...) s  -- SUM(x) + k over GROUP BY k → valid
/// SELECT s.k1 + s.k2  FROM (...) s  -- k1 + k2 over GROUP BY k1, k2 → valid
/// ```
///
/// The principled fix would change the `.all()` predicate to
/// `is_aggregated || is_gb_key`, which requires threading the inner's
/// `group_by` clause into this helper and comparing each column's mapped
/// expression against the grouping key set.  That's a signature change and
/// a behavior change for all `can_inline_subquery` callers, so it belongs
/// in its own commit with dedicated tests — not bundled with the current
/// eligibility-convergence stack.  Until then, the conservative rejection
/// silently over-rejects mixed-expression SELECT fields involving both
/// aggregates and GROUP BY keys.
fn is_agg_derived_outputs(
    expr: &Expr,
    lhs_rel: &Relation,
    outer_to_inner_fields: &HashMap<Column, Expr>,
) -> ReadySetResult<bool> {
    Ok(columns_iter(expr)
        .filter_map(|col| {
            if crate::is_column_of!(col, *lhs_rel) {
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

/// GROUP BY compatibility check — grouped inner into non-grouped outer.
///
/// When the inner is aggregated/grouped and the outer is not, inlining
/// lifts the inner's GROUP BY into the outer.  Two passes:
///
/// - Pass 1 (all outermost positions): collect `downstream_group_by_
///   additions` — bare column references to relations OTHER than
///   `inner_rel`, which will need to become GROUP BY keys in the
///   post-inline outer.  Rejects expressions that reference other
///   relations in any non-bare-column shape.
/// - Pass 2 (SELECT fields only): enforce that any SELECT expression
///   touching `inner_rel` has all of its `inner_rel` columns mapped to
///   aggregate-derived inner expressions.  Narrowed to SELECT only
///   (since the 2026-04-21 bug fix); WHERE/HAVING/ORDER-BY/ON
///   references to inner GROUP BY keys are fine because those clauses
///   accept bare column refs on grouped-query semantics.
///
/// Returns `None` on rejection, `Some(additions)` on accept — the
/// additions are threaded out as the return value of the overall
/// `can_inline_subquery` call.
fn check_group_by_compatibility(ctx: &InliningContext) -> ReadySetResult<Option<Vec<Expr>>> {
    let mut downstream_group_by_additions: Vec<Expr> = Vec::new();

    if ctx.is_inner_agg && !ctx.is_outer_agg {
        // Pass 1: scan all outermost positions.
        let mut other_rels = collect_local_from_items(ctx.outer_stmt)?;
        other_rels.remove(&ctx.inner_rel);

        for e in outermost_expression(ctx.outer_stmt) {
            let is_refs_other_rels = refs_any_of_rels_anywhere(e, &other_rels)?;
            if is_refs_other_rels {
                if matches!(e, Expr::Column(Column { table: Some(t), .. }) if other_rels.contains(t))
                {
                    downstream_group_by_additions.push(e.clone());
                } else {
                    return Ok(None);
                }
            }
        }

        // Pass 2: SELECT fields only — enforce is_agg_derived_outputs.
        for fe in &ctx.outer_stmt.fields {
            let (fe_expr, _) = expect_field_as_expr(fe);
            if let Expr::Column(c) = fe_expr
                && crate::is_column_of!(c, ctx.inner_rel)
            {
                // Simple `inner_rel.col` reference: OK.
            } else if refs_rel_anywhere(fe_expr, &ctx.inner_rel)?
                && !is_agg_derived_outputs(fe_expr, &ctx.inner_rel, ctx.ext_to_int)?
            {
                return Ok(None);
            }
        }
    }

    Ok(Some(downstream_group_by_additions))
}

/// Cardinality-barrier check.  Delegates to
/// `cardinality_barrier_blocks_inlining`; inverts the "blocks" result to
/// the "accept" convention.
fn check_cardinality_barrier(ctx: &InliningContext) -> ReadySetResult<bool> {
    let blocked = cardinality_barrier_blocks_inlining(
        ctx.outer_stmt,
        ctx.inner_stmt,
        ctx.is_outer_agg,
        ctx.is_inner_agg,
    );
    Ok(!blocked)
}

/// ORDER BY / LIMIT safety check.  When the inner has a LIMIT, ORDER BY
/// semantics have to align or be carryable — otherwise the inlining would
/// reorder rows post-limit in ways that change which rows survive.
fn check_order_limit_safety(ctx: &InliningContext) -> ReadySetResult<bool> {
    if ctx.inner_stmt.limit_clause.is_empty() {
        return Ok(true);
    }

    match (
        ctx.inner_stmt.order.is_some(),
        ctx.outer_stmt.order.is_some(),
    ) {
        (true, true) => {
            if !orders_equivalent_under_projection(
                ctx.outer_stmt,
                ctx.inner_stmt,
                ctx.inner_alias,
                ctx.ext_to_int,
            )? {
                return Ok(false);
            }
        }
        (true, false) => {
            // OK: we'll carry inner ORDER up to the outer.
        }
        (false, true) => {
            // Outer imposes ORDER but inner LIMIT exists: unsafe.
            return Ok(false);
        }
        (false, false) => {}
    }
    Ok(true)
}

/// LIMIT composition check.  If both inner and outer have LIMIT clauses,
/// both the limit and offset values must be numeric literals — otherwise
/// we cannot algebraically compose them into a single LIMIT/OFFSET at the
/// outer.
fn check_limit_composition(ctx: &InliningContext) -> ReadySetResult<bool> {
    if ctx.inner_stmt.limit_clause.is_empty() || ctx.outer_stmt.limit_clause.is_empty() {
        return Ok(true);
    }

    let is_number = |lit_opt: Option<&readyset_sql::ast::Literal>| {
        lit_opt.map(literal_as_number).transpose().is_ok()
    };

    let ok = is_number(ctx.inner_stmt.limit_clause.limit())
        && is_number(ctx.inner_stmt.limit_clause.offset())
        && is_number(ctx.outer_stmt.limit_clause.limit())
        && is_number(ctx.outer_stmt.limit_clause.offset());
    Ok(ok)
}

/// Mixed-scope WHERE + aggregated-inner-with-LIMIT check.  When the outer
/// WHERE's conjuncts reference both the inlinable alias and other base
/// relations, moving them through the inlining is unsafe.  Also unsafe:
/// LHS-only conjuncts that include a subquery (would end up in HAVING,
/// which §4.3 forbids).  And: LHS-only conjuncts when inner has LIMIT
/// (same HAVING unsupportability; also TOP-K changes row set).
fn check_mixed_scope_where_with_agg_limit(ctx: &InliningContext) -> ReadySetResult<bool> {
    let Some(where_expr) = &ctx.outer_stmt.where_clause else {
        return Ok(true);
    };
    if !ctx.is_inner_agg {
        return Ok(true);
    }

    let lhs_has_limit = !ctx.inner_stmt.limit_clause.is_empty();
    let base_other_rels = visible_base_rels_except(ctx.outer_stmt, &ctx.inner_rel)?;

    let mut conjuncts = Vec::new();
    let _ = split_expr(where_expr, &|_| true, &mut conjuncts);

    for e in conjuncts {
        let refs_lhs = refs_rel_anywhere(&e, &ctx.inner_rel)?;
        let refs_other = refs_any_of_rels_anywhere(&e, &base_other_rels)?;

        let mixed_scope = refs_lhs && refs_other;
        let lhs_only = refs_lhs && !refs_other;
        let lhs_only_with_limit = lhs_has_limit && lhs_only;
        let lhs_only_with_subquery = lhs_only && contains_select(&e);

        if mixed_scope || lhs_only_with_limit || lhs_only_with_subquery {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Downstream-join cardinality-preservation check.  Only fires when inner
/// is aggregated or has LIMIT.  In those cases, GROUP BY / LIMIT move to
/// the outer level after inlining — downstream joins that change
/// cardinality would alter group membership or which rows survive.
/// Current check: every downstream table is an `ExactlyOne` subquery;
/// every downstream join is INNER-with-ExactlyOne-RHS or
/// LEFT-with-AtMostOne-RHS, with `ON TRUE`/empty constraint.
fn check_downstream_joins_cardinality_preserving(ctx: &InliningContext) -> ReadySetResult<bool> {
    if !ctx.is_inner_agg && ctx.inner_stmt.limit_clause.is_empty() {
        return Ok(true);
    }
    downstream_joins_cardinality_preserving(ctx.downstream_tables, ctx.downstream_joins)
}

/// Self-join detection.  Reject if inlining would introduce the same base
/// table twice in the outer FROM.
fn check_self_join(ctx: &InliningContext) -> ReadySetResult<bool> {
    Ok(!would_create_self_join(
        ctx.outer_stmt,
        ctx.inner_stmt,
        ctx.inl_from_item_ord_idx,
    ))
}

/// Downstream non-trivial LHS output detection.  Reject if any OTHER FROM
/// item (not the inlinable itself) references a column from the inlinable's
/// alias that maps to a non-column expression after rebinding — such a
/// reference would break downstream unnesting.
fn check_downstream_non_trivial_lhs_output(ctx: &InliningContext) -> ReadySetResult<bool> {
    Ok(!downstream_reference_non_trivial_lhs_output(
        ctx.outer_stmt,
        ctx.inner_alias,
        ctx.ext_to_int,
    )?)
}

/// Unnesting guards (WHERE / HAVING / SELECT-list parametrizable filter
/// + subquery predicates).
///
/// Caller-gated via `ctx.skip_unnesting_guard` — post-`unnest_subqueries`
/// callers (specifically `derived_tables_rewrite`) pass `true` to opt out.
fn check_unnesting_guards(ctx: &InliningContext) -> ReadySetResult<bool> {
    if ctx.skip_unnesting_guard {
        return Ok(true);
    }

    if let Some(where_expr) = &ctx.outer_stmt.where_clause
        && is_hoisting_block_unnesting(where_expr, true, &ctx.inner_rel, ctx.ext_to_int)?
    {
        return Ok(false);
    }
    if let Some(having_expr) = &ctx.outer_stmt.having
        && is_hoisting_block_unnesting(having_expr, false, &ctx.inner_rel, ctx.ext_to_int)?
    {
        return Ok(false);
    }
    for fe in &ctx.outer_stmt.fields {
        let (fe_expr, _) = expect_field_as_expr(fe);
        if is_hoisting_block_unnesting(fe_expr, false, &ctx.inner_rel, ctx.ext_to_int)? {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Post-substitution ON shape check.  After substitution, outer joins'
/// ON clauses must remain a supported shape (no subqueries; non-INNER
/// joins require `is_supported_join_condition`).  LATERAL outer ON is a
/// structural `TRUE` placeholder — carved out for
/// `is_supported_join_condition`, but `contains_select` remains active
/// (subqueries in ON are universally unsupported).
fn check_post_substitution_on_shape(ctx: &InliningContext) -> ReadySetResult<bool> {
    for jc in &ctx.outer_stmt.join {
        if let JoinConstraint::On(expr) = &jc.constraint {
            let substituted = substitute_columns_in_expr(expr, ctx.ext_to_int, ctx.is_top_select)?;
            let is_lateral_rhs = matches!(
                &jc.right,
                JoinRightSide::Table(te)
                    if matches!(&te.inner, TableExprInner::Subquery(sq) if sq.lateral)
            );
            if !is_lateral_rhs
                && !is_supported_join_condition(&substituted)
                && !jc.operator.is_inner_join()
            {
                return Ok(false);
            }
            if contains_select(&substituted) {
                return Ok(false);
            }
        }
    }
    Ok(true)
}

/// ON → WHERE safety check.  After inlining, some base ON conditions may
/// need to migrate to WHERE; reject if that migration would leave a
/// non-trivial ON that shape-normalization cannot repair.
fn check_on_to_where_safety(ctx: &InliningContext) -> ReadySetResult<bool> {
    can_move_joins_on_nontrivial_expr_to_where(ctx.outer_stmt, ctx.ext_to_int)
}

// ---------------------------------------------------------------------------
// Eligibility orchestration (entry points)
// ---------------------------------------------------------------------------

/// Canonical eligibility entry point for subquery inlining.
///
/// Composes the eligibility checks in canonical order.  Returns
/// `Ok(None)` on rejection, `Ok(Some(downstream_group_by_additions))`
/// on acceptance.
///
/// Caller-preference flags on [`InliningContext`] (e.g.
/// `skip_unnesting_guard`) tune the check sequence without requiring
/// callers to compose the individual checks themselves.
pub(crate) fn can_inline_subquery(ctx: &InliningContext) -> ReadySetResult<Option<Vec<Expr>>> {
    if !check_window_function_interaction(ctx)? {
        return Ok(None);
    }
    if !check_nested_aggregation(ctx)? {
        return Ok(None);
    }
    let Some(downstream_group_by_additions) = check_group_by_compatibility(ctx)? else {
        return Ok(None);
    };
    if !check_grouped_engine_validation(ctx)? {
        return Ok(None);
    }
    if !check_cardinality_barrier(ctx)? {
        return Ok(None);
    }
    if !check_order_limit_safety(ctx)? {
        return Ok(None);
    }
    if !check_limit_composition(ctx)? {
        return Ok(None);
    }
    if !check_mixed_scope_where_with_agg_limit(ctx)? {
        return Ok(None);
    }
    if !check_downstream_joins_cardinality_preserving(ctx)? {
        return Ok(None);
    }
    if !check_self_join(ctx)? {
        return Ok(None);
    }
    if !check_downstream_non_trivial_lhs_output(ctx)? {
        return Ok(None);
    }
    if !check_unnesting_guards(ctx)? {
        return Ok(None);
    }
    if !check_post_substitution_on_shape(ctx)? {
        return Ok(None);
    }
    if !check_on_to_where_safety(ctx)? {
        return Ok(None);
    }
    Ok(Some(downstream_group_by_additions))
}

/// Position-dependent eligibility checks layered on top of [`can_inline_subquery`].
/// Each check fires only for the position/shape combination it applies to.
///
/// The scope of this helper is strictly **position-dependent P invariants**:
/// - Non-INNER RHS safety (RHS of non-INNER join).
/// - First-base-join shape (complex LHS into non-INNER first join).
///
/// Splice-code preconditions that are specific to a single caller's splice
/// path (e.g., DTR's `inline_from_item_in_place`) live in that caller's
/// wrapper, not here.
///
/// # LHS vs. RHS taxonomy
///
/// Inlining a derived table must preserve the semantics of JOINs in the
/// base query.  Two cases:
///
/// 1. **RHS inlining** — the derived table appears on the right-hand side
///    of a JOIN.  Requires stricter validation when the join is non-INNER
///    (e.g. LEFT): `can_inline_left_join_rhs_safe` ensures (a) the RHS is
///    a single-table non-joined subquery, (b) its WHERE clause contains
///    only simple pushable filters, and (c) any non-base projected
///    expressions are NOT used in the outer query.  This prevents
///    semantics-breaking rewrites in anti-join patterns or null-extending
///    JOINs.
///
/// 2. **LHS inlining** — the derived table is inlined into the base FROM
///    clause, prior to any JOIN.
///    - A single-table subquery (no joins, no filters besides a local
///      WHERE) is safe to inline even when the first base join is
///      non-INNER: the inlinable's WHERE only references its own local
///      columns, so absorbing it into the base WHERE is equivalent to
///      pre-filtering the LHS before the join.  LEFT JOIN never
///      NULL-extends LHS columns, so the filter position (before vs.
///      after the join) doesn't change the result.
///    - A complex (multi-table or filtered) inlinable is blocked when
///      the first join is non-INNER: splicing additional join structure
///      into the LHS of a LEFT JOIN could change the join topology
///      unpredictably.
///
/// The second `is_safe_lhs && !base_stmt.join.is_empty() && ...` branch
/// is a defense-in-depth check: it re-validates the first base join's
/// ON constraint before accepting a single-table LHS inline into a
/// non-INNER first join.  The downstream post-substitution ON and
/// nontrivial-ON checks also validate this, but checking early avoids
/// subtle breakage if those guards are ever relaxed.
#[allow(dead_code)] // Used once DTR migrates (commit 4).
pub(crate) fn inline_from_item_position_checks(
    base_stmt: &SelectStatement,
    inl_stmt: &SelectStatement,
    inl_from_item_ord_idx: usize,
    ext_to_int_fields: &HashMap<Column, Expr>,
) -> ReadySetResult<bool> {
    if inl_from_item_ord_idx >= base_stmt.tables.len() {
        // Non-INNER RHS path
        let (jc_idx, _) = find_rhs_join_clause(base_stmt, inl_from_item_ord_idx)
            .ok_or_else(|| internal_err!("Invalid FROM item index"))?;
        let jc = &base_stmt.join[jc_idx];
        if !jc.operator.is_inner_join()
            && !can_inline_left_join_rhs_safe(base_stmt, jc, inl_stmt, ext_to_int_fields)?
        {
            return Ok(false);
        }
    } else {
        // LHS path
        let is_safe_lhs = is_single_from_item!(inl_stmt);
        if !(is_safe_lhs || base_stmt.join.is_empty() || base_stmt.join[0].operator.is_inner_join())
        {
            return Ok(false);
        }

        if is_safe_lhs && !base_stmt.join.is_empty() && !base_stmt.join[0].operator.is_inner_join()
        {
            match &base_stmt.join[0].constraint {
                JoinConstraint::On(expr) if is_supported_join_condition(expr) => {}
                _ => return Ok(false),
            }
        }
    }
    Ok(true)
}

// ---------------------------------------------------------------------------
// `apply_inline` — position-independent transformation
// ---------------------------------------------------------------------------

/// Perform all position-independent transformations required to inline a prepared
/// subquery into `base_stmt`.
///
/// This includes:
/// 1. WHERE-to-HAVING migration (when the inner query is aggregated)
/// 2. Column rebinding via `replace_columns_with_inlinable_expr`
/// 3. GROUP BY / HAVING / DISTINCT merge (aggregated case) or WHERE merge
/// 4. ORDER BY carry-up (when inner has LIMIT)
/// 5. LIMIT/OFFSET composition
///
/// The caller is responsible for splicing the inner tables/joins into `base_stmt`
/// before or after calling this function.
pub(crate) fn apply_inline(
    base_stmt: &mut SelectStatement,
    mut prepared: InlineCandidate,
    is_top_select: bool,
    downstream_group_by_additions: Vec<Expr>,
) -> ReadySetResult<()> {
    let is_lhs_stmt_aggregation_or_grouped = is_aggregation_or_grouped(&prepared.stmt)?;

    // Normalize inner HAVING-without-aggregates-or-GROUP-BY to WHERE.
    // When `is_aggregation_or_grouped` is false (no DISTINCT, no GROUP BY,
    // and no aggregates anywhere including HAVING), a HAVING predicate is
    // semantically equivalent to a WHERE filter. Fuse it into the inner's
    // WHERE so downstream merging treats it uniformly.
    if !is_lhs_stmt_aggregation_or_grouped
        && let Some(having_as_where) = prepared.stmt.having.take()
    {
        prepared.stmt.where_clause =
            and_predicates_skip_true(prepared.stmt.where_clause.take(), having_as_where);
    }

    // In case the hoistable statement is aggregated, move only the WHERE conjuncts that
    // reference the hoistable relation (lhs) and *no other base relations* into HAVING.
    // We classify per conjunct with scope-aware reference checks.
    if is_lhs_stmt_aggregation_or_grouped && let Some(where_expr) = base_stmt.where_clause.take() {
        let lhs_rel: Relation = prepared.alias.clone().into();
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
        &prepared.alias.clone().into(),
        &prepared.ext_to_int,
        is_top_select,
    )?;

    // Handle aggregated subquery case
    if is_lhs_stmt_aggregation_or_grouped {
        // DISTINCT handling: when the inner has DISTINCT AND not all GROUP BY keys
        // are projected, propagate DISTINCT to the outer (deduplication is needed).
        // When all GROUP BY keys ARE projected, DISTINCT is intentionally NOT
        // propagated — GROUP BY already deduplicates on those keys, making DISTINCT
        // redundant. Downstream `fix_groupby_without_aggregates` may convert the
        // GROUP BY to DISTINCT if appropriate.
        if prepared.stmt.distinct && !group_by_keys_all_projected(&prepared.stmt)? {
            base_stmt.distinct = true;
        }
        // Normalize any numeric or alias field references in the inner GROUP BY
        // before moving to the outer statement. Inner field positions are not
        // meaningful in the outer context, and inner aliases are being rebound
        // to inner expressions by `replace_columns_with_inlinable_expr` above.
        if let Some(group_by) = &mut prepared.stmt.group_by {
            for f in group_by.fields.iter_mut() {
                *f = FieldReference::Expr(resolve_field_reference(&prepared.stmt.fields, f)?);
            }
        }
        base_stmt.group_by = mem::take(&mut prepared.stmt.group_by);
        if !downstream_group_by_additions.is_empty() {
            let gb = base_stmt.group_by.get_or_insert_default();
            for e in downstream_group_by_additions {
                let fr = FieldReference::Expr(e);
                if !gb.fields.contains(&fr) {
                    gb.fields.push(fr);
                }
            }
        }
        if let Some(lhs_having) = prepared.stmt.having {
            base_stmt.having =
                and_predicates_skip_true(mem::take(&mut base_stmt.having), lhs_having);
        }
        if let Some(lhs_where) = prepared.stmt.where_clause {
            base_stmt.where_clause =
                and_predicates_skip_true(mem::take(&mut base_stmt.where_clause), lhs_where);
        }
    } else if let Some(lhs_where) = mem::take(&mut prepared.stmt.where_clause) {
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
    // When the inner has ORDER BY but NO LIMIT, the ORDER BY is intentionally NOT carried up.
    // ORDER BY without LIMIT is a no-op in intermediate subqueries — it only affects
    // presentation order, which is determined by the outermost query's ORDER BY. Silently
    // dropping the inner ORDER BY in this case is correct SQL semantics.
    //
    // Safety: we only do this after verifying the outer ORDER BY is either absent or compatible
    // with the LHS ORDER BY under projection rebinding (see `orders_equivalent_under_projection`).
    // Compatibility is checked earlier using `orders_equivalent_under_projection` function.
    if !prepared.stmt.limit_clause.is_empty() {
        base_stmt.order = mem::take(&mut prepared.stmt.order);
    }

    if !prepared.stmt.limit_clause.is_empty() {
        if base_stmt.limit_clause.is_empty() {
            base_stmt.limit_clause = prepared.stmt.limit_clause;
        } else {
            let (base_lim, base_offs) = limit_clause_as_numbers(&base_stmt.limit_clause)?;
            let (lhs_lim, lhs_offs) = limit_clause_as_numbers(&prepared.stmt.limit_clause)?;

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
