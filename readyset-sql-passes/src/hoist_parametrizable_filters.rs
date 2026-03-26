use crate::rewrite_utils::{
    ConstraintKind, and_predicates_skip_true, as_sub_query_with_alias, as_sub_query_with_alias_mut,
    collect_local_from_items, columns_iter, conjoin_all_dedup, denormalize_having_and_group_by,
    expect_field_as_expr, find_rhs_join_clause, fix_groupby_without_aggregates,
    for_each_window_function, hoist_parametrizable_join_filters_to_where, is_aggregated_expr,
    is_aggregation_or_grouped, is_parametrizable_filter_candidate, is_simple_parametrizable_filter,
    normalize_having_and_group_by, project_columns,
    project_columns_if_not_exist_fix_duplicate_aliases, project_statement_columns_if,
    resolve_group_by_exprs, split_expr, split_expr_mut,
};
use crate::{
    contains_wf, get_local_from_items_iter, get_local_from_items_iter_mut, is_window_function_expr,
};
use itertools::Either;
use readyset_errors::{ReadySetResult, internal, invariant};
use readyset_sql::ast::{
    BinaryOperator, Column, Expr, JoinOperator, JoinRightSide, Relation, SelectStatement,
    SqlIdentifier,
};
use std::collections::{HashMap, HashSet};
use std::iter;
use std::mem;

pub(crate) fn hoist_parametrizable_filters(stmt: &mut SelectStatement) -> ReadySetResult<bool> {
    let mut rewritten = false;

    // De-normalize GROUP BY, HAVING and ORDER BY, as the filter hoisting expect
    // actual expressions, not aliases in the affected clauses.
    rewritten |= denormalize_having_and_group_by(stmt)?;

    // Use the comprehensive filter hoisting logic
    rewritten |= hoist_parametrizable_filters_globally(stmt, None, false)?;

    // Re-normalize GROUP BY, HAVING and ORDER BY again
    rewritten |= normalize_having_and_group_by(stmt)?;

    // Fix applicable GROUP BY w/o aggregates to DISTINCT
    rewritten |= fix_groupby_without_aggregates(stmt)?;

    Ok(rewritten)
}

/// Goal: push *parametrizable* single-relation filters all the way to the **top-level** WHERE.
///
/// This pass exists for ReadySet's planner model: only predicates in the final, outer WHERE
/// could be parametrized. Hoisting a filter only partway (e.g., from a deep derived table to an
/// intermediate wrapper) can *hurt* performance by reducing selectivity in lower layers without
/// enabling top-level selectivity/costing.
///
/// What this function does:
/// - Performs a small set of safe, local normalizations at the current query level (ON→WHERE
///   promotion and a limited HAVING→WHERE move for group-key filters).
/// - Recursively walks FROM-subqueries and hoists qualifying conjuncts from child WHERE clauses
///   into the current statement's WHERE, **but only** when the path from the child to the
///   outermost WHERE is guaranteed to be clear.
///
/// Hoisting barriers (we stop and do nothing across these):
/// - Non-INNER join edges (e.g., LEFT JOIN): would cross a null-extending boundary.
/// - Aggregation/grouping boundaries: moving predicates can change grouping semantics.
/// - Window functions (WF): treated as a conservative boundary for this pass to avoid
///   evaluation/order surprises and mid-stack reshaping under WF contexts.
///
/// Preconditions:
/// - Subqueries are already decorrelated (post-unnesting).
/// - Filter shape recognition is handled by `is_simple_parametrizable_filter`.
///
/// Returns `Ok(true)` iff we actually added at least one new conjunct to `stmt.where_clause`.
fn hoist_parametrizable_filters_globally(
    stmt: &mut SelectStatement,
    join_kind_to_parent: Option<JoinOperator>,
    is_under_aggregation: bool,
) -> ReadySetResult<bool> {
    let mut did_hoist = false;

    // Window functions are a conservative boundary for Goal 1.
    // If this statement contains a WF, do not perform any hoisting work at this level and do not
    // descend: we want to avoid changing evaluation/order-sensitive shapes under WF contexts.
    if contains_wf!(stmt) {
        return Ok(did_hoist);
    }

    // If this node cannot propagate predicates to its parent, then nothing in this subtree can
    // reach the outermost WHERE. Under Goal 1 we must not hoist anything (even locally) in that
    // case, to avoid partial/mid-stack movement.
    let can_reach_top_where = join_kind_to_parent
        .map(|op| op.is_inner_join())
        .unwrap_or(true)
        && !is_under_aggregation;

    if !can_reach_top_where {
        return Ok(did_hoist);
    }

    // Promote ON -> WHERE to expose more filters
    if hoist_parametrizable_join_filters_to_where(stmt)? {
        did_hoist = true;
    }

    if move_group_key_filters_from_having_to_where(stmt)? {
        did_hoist = true;
    }

    // Hoist filters from aggregated subqueries at this level
    if hoist_parametrizable_filters_from_aggregated_subqueries(stmt)? {
        did_hoist = true;
    }

    let mut idx_to_join_op = HashMap::new();
    for (idx, _) in get_local_from_items_iter!(stmt).enumerate() {
        let join_op = find_rhs_join_clause(stmt, idx).map(|(jc_idx, _)| stmt.join[jc_idx].operator);
        idx_to_join_op.insert(idx, join_op);
    }

    // Recurse into FROM-subqueries only when this child can contribute predicates that will
    // continue moving upward to the outermost WHERE (Goal 1).
    for (idx, from_item) in get_local_from_items_iter_mut!(stmt).enumerate() {
        if let Some((sub_stmt, alias)) = as_sub_query_with_alias_mut(from_item) {
            let join_op = idx_to_join_op.get(&idx).and_then(|join_op| *join_op);

            let sub_is_agg = is_aggregation_or_grouped(sub_stmt)?;

            // Child → parent hop must be safe, and the child must be eligible for Goal 1 hoisting.
            // If this is false, we *also* skip recursion into the child to avoid partial hoisting
            // inside a subtree that cannot reach the outermost WHERE.
            let can_hoist_child_where_to_parent_where = !contains_wf!(sub_stmt) // WF is a conservative boundary
                && join_op.map(|op| op.is_inner_join()).unwrap_or(true) // do not cross OUTER-join edges
                && !sub_is_agg; // do not hoist across aggregation/grouping

            if !can_hoist_child_where_to_parent_where {
                continue;
            }

            // Recurse into subquery
            if hoist_parametrizable_filters_globally(sub_stmt, join_op, sub_is_agg)? {
                did_hoist = true;
            }

            // Now try to hoist filters from subquery WHERE.
            //
            // After unnest_subqueries + derived_tables_rewrite, all columns in the
            // subquery's WHERE should reference its own internal FROM items (no
            // correlated outer references remain).  The `sub_locals` check below is
            // therefore expected to match every qualified column.  We keep it as a
            // defensive guard: if a pipeline bug left an outer-scope column reference
            // inside the subquery, we must not hoist that filter — rebinding it to
            // the subquery alias would produce a dangling reference.
            //
            // Note: the analogous aggregated-subquery path
            // (`extract_filters_from_aggregated_subquery_stmt`) uses `true` as its
            // column predicate for the same reason — all columns are internal.  We
            // use `sub_locals` here for extra safety since this non-aggregated path
            // is newer and covers cases that survived `derived_tables_rewrite`
            // (e.g., self-join bail-outs).
            if let Some(where_expr) = sub_stmt.where_clause.take() {
                let sub_locals = collect_local_from_items(sub_stmt)?;

                let mut matched_conjuncts = vec![];
                let remaining_expr = split_expr(
                    &where_expr,
                    &|expr| {
                        is_simple_parametrizable_filter(expr, |col_tab, _| {
                            sub_locals.contains(col_tab)
                        })
                    },
                    &mut matched_conjuncts,
                );

                for mut conjunct in matched_conjuncts {
                    project_statement_columns_if(sub_stmt, alias.clone(), &mut conjunct, |col| {
                        col.table.as_ref().is_some_and(|t| sub_locals.contains(t))
                    })?;

                    let mut parent_where = mem::take(&mut stmt.where_clause);

                    let c_kind = ConstraintKind::new(&conjunct);
                    let already_present = parent_where
                        .as_ref()
                        .map(|w| c_kind.is_contained_in(w))
                        .unwrap_or(false);

                    if !already_present {
                        parent_where = and_predicates_skip_true(parent_where, conjunct);
                        did_hoist = true;
                    }

                    stmt.where_clause = parent_where;
                }

                sub_stmt.where_clause = remaining_expr;
            }
        }
    }

    Ok(did_hoist)
}

/// Replace extracted filter expressions with column references for inlining.
fn replace_expressions_with_column(
    mut filters: Vec<Expr>,
    from_item_ref: Relation,
    column_names_it: impl Iterator<Item = SqlIdentifier>,
) -> ReadySetResult<Vec<Expr>> {
    let mut new_expressions = Vec::new();
    for (expr, column_name) in filters.iter_mut().zip(column_names_it) {
        let column = Expr::Column(Column {
            name: column_name,
            table: Some(from_item_ref.clone()),
        });
        match expr {
            Expr::BinaryOp { lhs, rhs, .. } => match (lhs.as_ref(), rhs.as_ref()) {
                (_, Expr::Literal(_)) => **lhs = column,
                (Expr::Literal(_), _) => **rhs = column,
                _ => internal!("BinaryOp shape validated by is_parametrizable_filter_candidate"),
            },
            Expr::Between { operand, .. } => **operand = column,
            Expr::In { lhs, .. } => **lhs = column,
            _ => internal!("Filter shape validated by is_parametrizable_filter_candidate"),
        }
        new_expressions.push(expr.clone());
    }
    Ok(new_expressions)
}

/// Scans an aggregated subquery for simple **equality** filters (e.g., `col = ?`) in its `WHERE` clause,
/// and any parametrizable filter (e.g., `col = ?`, `col IN (...)`, `col BETWEEN ...`) in its `HAVING` clause,
/// and rewrites them to be applied at the outer query level. This allows predicates like
/// `value = ?`, `value > ?`, `value BETWEEN ? AND ?`, or `value IN (?, ?, ?)` to be pulled out of the subquery
/// and used as top-level filters.
///
/// All referenced operands are projected by the subquery and properly qualified with the subquery alias,
/// so that the outer scope can safely reference them.
///
/// Returns a single expression combining the extracted filters with AND, or `None` if no filters were found.
fn extract_filters_from_aggregated_subquery_stmt(
    stmt: &mut SelectStatement,
    from_item_ref: Relation,
) -> ReadySetResult<Option<Expr>> {
    invariant!(is_aggregation_or_grouped(stmt)?);

    let mut outer_filters = Vec::new();

    // Pre-step: move parametrizable WHERE filters on GROUP BY keys to HAVING.
    // A GROUP BY key has one value per group, so filtering before or after
    // aggregation on it is equivalent — the filter eliminates entire groups.
    // Moving these to HAVING lets the existing HAVING extraction path (which
    // accepts all parametrizable shapes) pick them up naturally.
    let gb_exprs = resolve_group_by_exprs(stmt).unwrap_or_default();
    if !gb_exprs.is_empty()
        && let Some(where_expr) = stmt.where_clause.take()
    {
        let mut to_having = Vec::new();
        stmt.where_clause = split_expr(
            &where_expr,
            &|expr: &Expr| {
                is_parametrizable_filter_candidate(expr, |operand| {
                    gb_exprs.iter().any(|g| g.eq(operand))
                })
            },
            &mut to_having,
        );
        if let Some(conjoined) = conjoin_all_dedup(to_having) {
            stmt.having = and_predicates_skip_true(stmt.having.take(), conjoined);
        }
    }

    // WHERE clause: extract simple equality filters (col = ?), where one side
    // is a literal.  Only equalities are safe here — the extracted column may
    // not be a GROUP BY key and needs promotion, which requires a single value.
    // (GROUP BY key filters were already moved to HAVING above.)
    if let Some(where_expr) = stmt.where_clause.take() {
        let mut expressions_from_where = Vec::new();
        let mut extracted_filters = Vec::new();

        stmt.where_clause = split_expr_mut(
            &where_expr,
            &mut |expr: &Expr| {
                matches!(
                    expr,
                    Expr::BinaryOp {
                        op: BinaryOperator::Equal,
                        ..
                    }
                ) && is_parametrizable_filter_candidate(expr, |operand| {
                    expressions_from_where.push(operand.clone());
                    true
                })
            },
            &mut extracted_filters,
        );

        if !expressions_from_where.is_empty() {
            let project_aliases = project_columns(stmt, &expressions_from_where)?;
            outer_filters.extend(replace_expressions_with_column(
                extracted_filters,
                from_item_ref.clone(),
                project_aliases.into_iter(),
            )?);
        }
    }

    // HAVING clause: extract any simple filter that compares a column to literals (e.g., col = ?, col IN (...), col BETWEEN ...).
    if let Some(having_expr) = stmt.having.take() {
        let mut expressions_from_having = Vec::new();
        let mut extracted_filters = Vec::new();

        stmt.having = split_expr_mut(
            &having_expr,
            &mut |expr: &Expr| {
                is_parametrizable_filter_candidate(expr, &mut |operand: &Expr| {
                    expressions_from_having.push(operand.clone());
                    true
                })
            },
            &mut extracted_filters,
        );

        if !expressions_from_having.is_empty() {
            let project_aliases =
                project_columns_if_not_exist_fix_duplicate_aliases(stmt, &expressions_from_having);

            outer_filters.extend(replace_expressions_with_column(
                extracted_filters,
                from_item_ref.clone(),
                project_aliases.into_iter().map(|(_, alias)| alias),
            )?);
        }
    }

    // Conjoin everything we extracted
    Ok(conjoin_all_dedup(outer_filters))
}

/// Move filters based on GROUP BY keys from HAVING to WHERE within `stmt`,
/// so they can be applied pre-aggregation.
///
/// This optimization is semantically safe because filters on GROUP BY columns have
/// values that are constant within each group. Moving such filters from HAVING
/// (post-aggregation) to WHERE (pre-aggregation) produces identical results while
/// reducing the number of rows that need to be aggregated.
///
/// # Two-Case Strategy
///
/// **Case 1**: Parametrizable filters where the operand exactly matches a GROUP BY expression
/// - Example: `HAVING dept_id = ?` where `GROUP BY dept_id`
/// - Example: `HAVING a + b > 10` where `GROUP BY a + b`
///
/// **Case 2**: Any non-aggregated expression that only references GROUP BY columns
/// - Example: `HAVING a + b > 10` where `GROUP BY a, b`
/// - Example: `HAVING dept > 100 OR status = 'active'` where `GROUP BY dept, status`
/// - Safe because all columns in the expression are GROUP BY keys, so the expression's
///   value is constant within each group
///
/// # What Is NOT Moved
///
/// - Aggregated expressions (e.g., `COUNT(*) > 10`, `SUM(x) > 100`)
/// - Expressions referencing columns not in GROUP BY
/// - Subquery predicates (handled conservatively)
fn move_group_key_filters_from_having_to_where(stmt: &mut SelectStatement) -> ReadySetResult<bool> {
    if stmt.having.is_none() || stmt.group_by.is_none() {
        return Ok(false);
    }

    let gb_exprs = resolve_group_by_exprs(stmt)?;
    if gb_exprs.is_empty() {
        return Ok(false);
    }

    let gb_cols = gb_exprs
        .iter()
        .flat_map(columns_iter)
        .collect::<HashSet<_>>();

    let mut moved = false;

    if let Some(having_expr) = &mut stmt.having {
        let mut extracted: Vec<Expr> = Vec::new();

        // Extract HAVING conjuncts using a two-case strategy:
        let rem = split_expr(
            having_expr,
            &|expr: &Expr| {
                // Case 1: Parametrizable filter (col = ?, col IN (?), col BETWEEN ? AND ?)
                // where the operand exactly matches a GROUP BY expression
                if is_parametrizable_filter_candidate(expr, |operand: &Expr| {
                    gb_exprs.iter().any(|g| g.eq(operand))
                }) {
                    true
                // Case 2: Non-aggregated expression using ONLY GROUP BY columns
                // Safe to move because the expression's value is constant within each group
                // (determined by GROUP BY keys), so filtering before or after grouping
                // produces identical results. Pre-aggregation filtering is more efficient.
                } else if let Ok(is_agg) = is_aggregated_expr(expr) {
                    if is_agg {
                        false // Never move aggregated expressions (e.g., COUNT(*) > 10)
                    } else {
                        // Move if all columns in the expression are GROUP BY columns
                        columns_iter(expr).all(|col| gb_cols.contains(col))
                    }
                } else {
                    false
                }
            },
            &mut extracted,
        );

        // Update HAVING with remainder (or drop entirely)
        if let Some(rem) = rem {
            *having_expr = rem;
        } else {
            stmt.having = None;
        }

        if !extracted.is_empty()
            && let Some(conjoined) = conjoin_all_dedup(extracted)
        {
            stmt.where_clause =
                and_predicates_skip_true(mem::take(&mut stmt.where_clause), conjoined);
            moved = true;
        }
    }

    Ok(moved)
}

/// Hoist filters extracted from aggregated subqueries into the top-level WHERE.
fn hoist_parametrizable_filters_from_aggregated_subqueries(
    stmt: &mut SelectStatement,
) -> ReadySetResult<bool> {
    // Move group key param filters from HAVING to WHERE for the parent statement once.
    let mut did_rewrite = false;

    if contains_wf!(stmt) {
        return Ok(did_rewrite); // Skip: cannot hoist into a statement with WF
    }

    let mut from_items_filters_pushability = Vec::new();
    for (from_item_idx, _) in get_local_from_items_iter!(stmt).enumerate() {
        from_items_filters_pushability.push(match find_rhs_join_clause(stmt, from_item_idx) {
            // Base FROM item(s): not introduced by any join → safe
            None => true,
            // Introduced by a join: safe only if that join is INNER
            Some((jc_idx, _rhs_pos)) => stmt.join[jc_idx].operator.is_inner_join(),
        });
    }

    let mut add_to_where = None;
    // Iterate over aggregated subqueries, extract and accumulate hoistable filters.
    for from_item in
        get_local_from_items_iter_mut!(stmt)
            .enumerate()
            .filter_map(|(from_item_idx, from_item)| {
                if let Some((from_item_stmt, _)) = as_sub_query_with_alias(from_item) {
                    if contains_wf!(from_item_stmt) {
                        return None; // Skip: subquery contains WF — hoisting is unsafe
                    }
                    if is_aggregation_or_grouped(from_item_stmt).is_ok_and(|v| v)
                        && from_items_filters_pushability[from_item_idx]
                    {
                        return Some(from_item);
                    }
                }
                None
            })
    {
        let (from_item_stmt, from_alias) =
            // SAFETY: the `find_map` iterator above only yields `from_item` when
            // `as_sub_query_with_alias(from_item).is_some()` is true.
            as_sub_query_with_alias_mut(from_item).expect("filter guarantees subquery");
        if let Some(extracted) =
            extract_filters_from_aggregated_subquery_stmt(from_item_stmt, from_alias.into())?
        {
            add_to_where = and_predicates_skip_true(add_to_where, extracted);
        }
    }

    if let Some(add_to_where) = add_to_where {
        stmt.where_clause =
            and_predicates_skip_true(mem::take(&mut stmt.where_clause), add_to_where);
        did_rewrite = true;
    }

    Ok(did_rewrite)
}
