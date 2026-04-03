use crate::detect_problematic_self_joins::contains_problematic_self_joins;
use crate::get_local_from_items_iter_mut;
use crate::rewrite_utils::{
    add_expression_to_join_constraint, and_predicates_skip_true, as_sub_query_with_alias_mut,
    columns_iter, conjoin_all_dedup, get_from_item_reference_name,
    get_lhs_rhs_tables_from_eq_constraint, is_always_true_filter,
    is_filter_pushable_from_join_clause, matches_eq_constraint, split_expr,
};
use crate::unnest_subqueries::{
    is_supported_join_condition, split_on_for_rhs_against_preceding_lhs,
};
use itertools::{Either, EitherOrBoth};
use readyset_errors::{ReadySetError, ReadySetResult, internal, invariant, unsupported};
use readyset_sql::analysis::visit_mut;
use readyset_sql::analysis::visit_mut::VisitorMut;
use readyset_sql::ast::JoinOperator::{CrossJoin, InnerJoin, LeftOuterJoin, StraightJoin};
use readyset_sql::ast::{
    BinaryOperator, Expr, JoinClause, JoinConstraint, JoinOperator, JoinRightSide, Literal,
    Relation, SelectStatement, TableExpr,
};
use readyset_sql::{Dialect, DialectDisplay};
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::mem;
use std::{cmp, iter};

/// Predicate matching equi-join constraints of the form `table_a.col = table_b.col` where both
/// sides reference *different* relations. This is the default `is_joinable_predicate` passed to
/// [`reorder_joins_in_stmt`]: it defines the class of WHERE filters eligible to be lifted into
/// the predicate pool and reassigned as ON conditions during join reordering.
pub(crate) const IS_CROSS_TABLE_EQUALITY: fn(&Expr) -> bool = |constraint: &Expr| -> bool {
    matches_eq_constraint(constraint, |left_rel, right_rel| left_rel != right_rel)
};

/// Trait alias for the core within-segment join reorder function injected into
/// [`reorder_joins_in_stmt`].
///
/// # Contract
///
/// When invoked for the **first prefix segment**, `preceding_tables` and `preceding_joins` are
/// both empty, and the first element of the returned `Vec<JoinClause>` represents the new leading
/// table (previously `stmt.tables[0]`). That element **must** have `JoinConstraint::Empty` so it
/// can be safely extracted back to `stmt.tables`. The structural [`reorder_joins`] satisfies this
/// by construction: a join with no reachable LHS relations receives no ON predicate and is written
/// as `CrossJoin` / `Empty`. Any alternative implementation must uphold the same guarantee.
pub(crate) trait JoinsReorderFunc:
    FnMut(
    Vec<JoinClause>,
    &[TableExpr],
    &[JoinClause],
    &mut Vec<Expr>,
) -> ReadySetResult<(bool, Vec<JoinClause>)>
{
}

impl<F> JoinsReorderFunc for F where
    F: FnMut(
        Vec<JoinClause>,
        &[TableExpr],
        &[JoinClause],
        &mut Vec<Expr>,
    ) -> ReadySetResult<(bool, Vec<JoinClause>)>
{
}

/// Returns the relation names introduced on the RHS of `jc`. Typically one for a plain table;
/// multiple for a subquery or lateral join that exposes several table expressions.
pub(crate) fn get_join_rhs_relations(jc: &JoinClause) -> Vec<Relation> {
    jc.right
        .table_exprs()
        .map(get_from_item_reference_name)
        .collect::<ReadySetResult<Vec<_>>>()
        // SAFETY: `get_from_item_reference_name` only fails on structurally invalid AST
        // nodes (missing alias on subquery). JoinClause RHS table exprs are well-formed
        // at this point — they come from parsed SQL or prior normalization passes.
        // TODO: refactor to return ReadySetResult once all 10+ call sites are updated.
        .expect("join RHS table exprs must have valid reference names")
}

/// Drains from `predicates` all equi-join constraints that bind the RHS of `jc` to a relation
/// already in `lhs_rels`, returning them as `Some(Vec<Expr>)`.
/// Returns `None` if no such predicate exists, indicating `jc` cannot yet be joined on a
/// predicate and must be deferred or treated as a cross join.
/// Predicates are removed via `swap_remove` to avoid O(n) shifting on each extraction.
fn take_candidate_predicates_for_join(
    lhs_rels: &[Relation],
    jc: &JoinClause,
    predicates: &mut Vec<Expr>,
) -> Option<Vec<Expr>> {
    let rhs_rels = get_join_rhs_relations(jc)
        .into_iter()
        .collect::<HashSet<_>>();

    let mut candidates = Vec::new();

    let mut idx = 0;
    while idx < predicates.len() {
        let pred = &predicates[idx];
        if let Some((lhs, rhs)) = get_lhs_rhs_tables_from_eq_constraint(pred)
            && ((lhs_rels.contains(&lhs) && rhs_rels.contains(&rhs))
                || (lhs_rels.contains(&rhs) && rhs_rels.contains(&lhs)))
        {
            candidates.push(predicates.swap_remove(idx));
        } else {
            idx += 1;
        }
    }

    if candidates.is_empty() {
        None
    } else {
        Some(candidates)
    }
}

/// Lexicographic comparator on join clause RHS relation names.
/// Used as a deterministic tie-breaker in all join candidate selection functions.
fn cmp_rhs_names(jc1: &JoinClause, jc2: &JoinClause) -> Ordering {
    let mut ak = get_join_rhs_relations(jc1);
    let mut bk = get_join_rhs_relations(jc2);
    if ak.len() == 1 && bk.len() == 1 {
        ak[0].cmp(&bk[0])
    } else {
        ak.sort();
        bk.sort();
        ak.cmp(&bk)
    }
}

/// Ranks two join clauses by two metrics: prefers smaller `metric1` (primary), larger `metric2`
/// (secondary), then lexicographically smaller RHS relation name as a deterministic tie-breaker.
/// Used consistently by both candidate selection functions to guarantee a stable total order.
fn stable_join_clause_2m_comparator<T1: Ord, T2: Ord>(
    jc_a: &JoinClause,
    a1: T1,
    a2: T2,
    jc_b: &JoinClause,
    b1: T1,
    b2: T2,
) -> Ordering {
    a1.cmp(&b1) // prefer smaller 1st metric
        .then_with(|| b2.cmp(&a2)) // prefer bigger 2nd metric
        .then_with(|| cmp_rhs_names(jc_a, jc_b)) // Deterministic tie-breaker: lexicographic RHS relation names
}

/// Given an equi-join predicate `pred = (a.col = b.col)`, returns the relation on the *opposite*
/// side from `rels`: if one side's relation is in `rels`, returns the other; returns `None` if
/// neither side is in `rels`.
///
/// Panics if `pred` is not a column equality predicate.
fn get_relation_equated_to(pred: &Expr, rels: &HashSet<Relation>) -> Option<Relation> {
    // SAFETY: this function is only called with predicates that passed `matches_eq_constraint`
    // in `take_candidate_predicates_for_join`, guaranteeing column-equality shape.
    let (pred_lhs, pred_rhs) =
        get_lhs_rhs_tables_from_eq_constraint(pred).expect("pred verified as column equality");
    if rels.contains(&pred_lhs) {
        Some(pred_rhs)
    } else if rels.contains(&pred_rhs) {
        Some(pred_lhs)
    } else {
        None
    }
}

/// Selects the best join to place next when no candidate is predicate-reachable from the current
/// LHS (a cross join step is unavoidable). The heuristic picks the join whose RHS, *if treated
/// as a hypothetical LHS*, would equate to the fewest distinct other relations (minimising
/// cross-join fan-out) while having the most predicates available (maximising subsequent join
/// opportunities). This greedily positions the join that best unlocks future predicate-bound
/// placements.
///
/// If no join has any relevant predicates (fully disconnected graph), falls back to the
/// lexicographically smallest RHS relation name for determinism.
fn choose_best_cross_join_candidate<'a>(
    joins: &'a [JoinClause],
    available_predicates: &[Expr],
) -> ReadySetResult<(&'a JoinClause, Vec<Expr>)> {
    let joins_rels = joins
        .iter()
        .flat_map(get_join_rhs_relations)
        .collect::<HashSet<_>>();
    joins
        .iter()
        .filter_map(|jc| {
            // Get the join RHS, and assume it would be LHS for the follow-up logic
            let lhs_rels = get_join_rhs_relations(jc)
                .into_iter()
                .collect::<HashSet<_>>();

            // Assuming the current join being LHS, collect 2 metrics based on `available_predicates`:
            let mut rhs_rels = HashSet::new(); // distinct RHS relations binding the assumed LHS
            let mut pred_cnt = 0usize; // number of predicates referencing the assumed LHS

            for pred in available_predicates {
                // Get the opposite to `lhs_rels` side, that is not the current join RHS and
                // references one of the join candidates
                if let Some(pred_rhs) = get_relation_equated_to(pred, &lhs_rels)
                    && !lhs_rels.contains(&pred_rhs)
                    && joins_rels.contains(&pred_rhs)
                {
                    rhs_rels.insert(pred_rhs);
                    pred_cnt += 1;
                } else {
                    continue;
                }
            }

            if rhs_rels.is_empty() {
                None
            } else {
                Some((jc, (rhs_rels.len(), pred_cnt)))
            }
        })
        .min_by(|(jc_a, a), (jc_b, b)| {
            // Prefer the join that, being LHS, binds minimal number of join clauses (distinct RHS)
            // with larger number of predicates.
            stable_join_clause_2m_comparator(jc_a, a.0, a.1, jc_b, b.0, b.1)
        })
        .map_or_else(
            || {
                // Entirely disconnected joins. Deterministically return lex-minimal name.
                // SAFETY: `joins` is non-empty (caller checks `!joins.is_empty()` before calling).
                Ok((
                    joins
                        .iter()
                        .min_by(|jc_a, jc_b| cmp_rhs_names(jc_a, jc_b))
                        .expect("joins verified non-empty by caller"),
                    vec![],
                ))
            },
            |(jc, _)| Ok((jc, vec![])),
        )
}

/// Selects the best join from `jc_to_pred` to place next. All candidates are predicate-reachable
/// from the current LHS. The ranking heuristic:
///
/// 1. Prefer the join whose RHS is equated to the *most recently added* LHS relation (smallest
///    reverse index in `lhs_rels`) — keeps local join chains tight.
/// 2. Among equal-distance ties, prefer the candidate with more predicates at that distance.
/// 3. Final tie-break: lexicographically smallest RHS relation name.
///
/// Predicates of non-selected candidates are returned to `available_predicates`.
fn choose_best_join_candidate<'a>(
    mut jc_to_pred: HashMap<&'a JoinClause, Vec<Expr>>,
    lhs_rels: &[Relation],
    available_predicates: &mut Vec<Expr>,
) -> ReadySetResult<(&'a JoinClause, Vec<Expr>)> {
    let best_jc = jc_to_pred
        .iter()
        .filter_map(|(jc, preds)| {
            if preds.is_empty() {
                return None;
            }
            // Get the join RHS
            let rhs_rels = get_join_rhs_relations(jc)
                .into_iter()
                .collect::<HashSet<_>>();

            // Collect 2 metrics, based on the predicates associated with the join:
            let mut min = usize::MAX; // minimal topological distance to some LHS within `lhs_rels`
            let mut cnt_min = 0usize; // number of predicates binding the join RHS with the minimal distance LHS

            for pred in preds {
                let Some(pred_lhs) = get_relation_equated_to(pred, &rhs_rels) else {
                    return None; // skip this candidate; the join's' RHS is not referenced
                };
                let Some(d) = lhs_rels.iter().rev().position(|r| r == &pred_lhs) else {
                    return None; // skip this candidate; caller’s LHS doesn’t contain it
                };
                if d < min {
                    min = d;
                    cnt_min = 1;
                } else if d == min {
                    cnt_min += 1;
                }
            }

            Some((*jc, (min, cnt_min)))
        })
        .min_by(|(jc_a, a), (jc_b, b)| {
            // Prefer the join whose RHS is equated to LHS that has minimal distance from within `lhs_rels`,
            // and is associated with larger number of predicates binding join RHS with the minimal distance LHS
            stable_join_clause_2m_comparator(jc_a, a.0, a.1, jc_b, b.0, b.1)
        })
        .map(|(jc, _)| jc);

    if let Some(best_jc) = best_jc {
        // Extract the entry to return
        let best = jc_to_pred.remove_entry(best_jc);
        // Place all other predicates back to the available_predicates pool
        jc_to_pred
            .into_iter()
            .for_each(|(_, predicates)| available_predicates.extend(predicates));
        // SAFETY: `best_jc` was obtained from `jc_to_pred.iter()` above, so it must still be in the map.
        Ok(best.expect("best_jc obtained from jc_to_pred iterator"))
    } else {
        internal!("No join candidate available from non-empty jc_to_pred map")
    }
}

/// Builds a map from each join in `chain` to the subset of `join_predicates` that bind its RHS
/// to a relation already in `lhs_relations`. Matching predicates are drained from `join_predicates`
/// and placed in the map. Joins with no matching predicates are excluded — the caller falls back
/// to [`choose_best_cross_join_candidate`] when the returned map is empty.
fn collect_join_candidates<'a>(
    chain: &'a [JoinClause],
    lhs_relations: &[Relation],
    join_predicates: &mut Vec<Expr>,
) -> HashMap<&'a JoinClause, Vec<Expr>> {
    chain
        .iter()
        .filter_map(|jc| {
            take_candidate_predicates_for_join(lhs_relations, jc, join_predicates)
                .map(|predicates| (jc, predicates))
        })
        .collect::<HashMap<_, _>>()
}

/// Structural (greedy, predicate-proximity) join reorder for a single non-SJ sub-segment.
///
/// At each step, picks the join whose RHS is bound to the most recently visible LHS relation
/// (topological proximity heuristic), assigns matching predicates from `available_predicates`
/// as its ON clause, and adds its RHS to the visible set. When no join is reachable from the
/// current LHS via a predicate, falls back to [`choose_best_cross_join_candidate`] to advance
/// past the gap.
///
/// All joins are rewritten on output: those with assigned ON predicates become `InnerJoin`;
/// those without become `CrossJoin` / `JoinConstraint::Empty`.
///
/// This is the default [`JoinsReorderFunc`] used by [`normalize_inner_joins_order`].
fn reorder_joins(
    mut joins: Vec<JoinClause>,
    preceding_tables: &[TableExpr],
    preceding_joins: &[JoinClause],
    available_predicates: &mut Vec<Expr>,
) -> ReadySetResult<(bool, Vec<JoinClause>)> {
    let mut did_reorder = false;
    let mut reordered_joins = Vec::with_capacity(joins.len());

    // Seed the visible-LHS set from all tables and joins that precede this segment.
    let mut lhs_relations = collect_preceding_relations(preceding_tables, preceding_joins)?;

    while !joins.is_empty() {
        let jc_to_pred = collect_join_candidates(&joins, &lhs_relations, available_predicates);

        let (best_jc, best_jc_predicates) = if jc_to_pred.is_empty() {
            // No join is predicate-reachable from the current LHS; advance via a cross join step.
            choose_best_cross_join_candidate(&joins, available_predicates)?
        } else {
            choose_best_join_candidate(jc_to_pred, &lhs_relations, available_predicates)?
        };

        // SAFETY: `best_jc` was selected from `joins` by one of the choose_* functions above.
        let best_jc_idx = joins
            .iter()
            .position(|j| std::ptr::eq(j as *const _, best_jc as *const _))
            .expect("best_jc was selected from joins");

        // Combine all assigned predicates into a single ON expression.
        let on_expr = conjoin_all_dedup(best_jc_predicates);

        let mut best_jc = joins.remove(best_jc_idx);

        if let Some(on_expr) = on_expr {
            best_jc.operator = InnerJoin;
            best_jc.constraint = add_expression_to_join_constraint(best_jc.constraint, on_expr);
        } else {
            best_jc.operator = CrossJoin;
            best_jc.constraint = JoinConstraint::Empty;
        }

        lhs_relations.extend(get_join_rhs_relations(&best_jc));
        reordered_joins.push(best_jc);
        did_reorder = true;
    }

    Ok((did_reorder, reordered_joins))
}

/// Strips predicates matching `is_joinable_predicate` from the ON clauses of all inner joins
/// in `joins`, preparing them for reuse by the reorder algorithm.
///
/// Returns `(pool, remainder)`:
/// - `pool`: extracted join predicates, to be added to `available_predicates`.
/// - `remainder`: non-joinable sub-expressions from ON clauses (e.g., single-table filters),
///   to be folded back into WHERE.
///
/// Each processed inner join is reset to `CrossJoin` / `JoinConstraint::Empty` so the reorder
/// algorithm can freely reassign its operator and ON clause. Outer joins are left untouched.
pub(crate) fn strip_inner_joins_predicates(
    joins: &mut Vec<JoinClause>,
    is_joinable_predicate: &impl Fn(&Expr) -> bool,
) -> (Vec<Expr>, Option<Expr>) {
    let mut remaining_expr = None;
    let mut predicates = Vec::new();
    for jc in joins {
        if !jc.operator.is_inner_join() {
            continue;
        }
        if let JoinConstraint::On(on_expr) = &mut jc.constraint
            && let Some(remainder) = split_expr(on_expr, is_joinable_predicate, &mut predicates)
        {
            remaining_expr = and_predicates_skip_true(remaining_expr, remainder);
        }
        jc.operator = CrossJoin;
        jc.constraint = JoinConstraint::Empty;
    }
    (predicates, remaining_expr)
}

/// Returns the flat list of relation names visible to the left of a join segment: all FROM
/// tables plus all RHS relations introduced by preceding joins. Forms the "already reachable"
/// set used by the reorder algorithm to evaluate predicate applicability.
pub(crate) fn collect_preceding_relations(
    preceding_tables: &[TableExpr],
    preceding_joins: &[JoinClause],
) -> ReadySetResult<Vec<Relation>> {
    preceding_tables
        .iter()
        .chain(preceding_joins.iter().flat_map(|jc| jc.right.table_exprs()))
        .map(get_from_item_reference_name)
        .collect::<ReadySetResult<Vec<_>>>()
}

/// Strips joinable predicates from `joins` ON clauses and distributes them: matching predicates
/// go into `available_predicates`; non-matching remainders are folded into `remaining_where`.
/// Thin orchestration wrapper around [`strip_inner_joins_predicates`].
fn strip_joins_preds(
    joins: &mut Vec<JoinClause>,
    is_joinable_predicate: &impl Fn(&Expr) -> bool,
    available_predicates: &mut Vec<Expr>,
    remaining_where: &mut Option<Expr>,
) {
    let (predicates, back_to_where) = strip_inner_joins_predicates(joins, is_joinable_predicate);
    available_predicates.extend(predicates);
    if let Some(expr) = back_to_where {
        *remaining_where = and_predicates_skip_true(mem::take(remaining_where), expr);
    }
}

/// Partitions `joins` into alternating `(sub_segment, sj_barrier)` pairs.
///
/// Each sub-segment is a maximal run of contiguous non-STRAIGHT_JOIN joins. The trailing
/// STRAIGHT_JOIN barrier (if any) follows immediately. The final entry always has
/// `sj_barrier = None`.
///
/// Example: `[INNER, SJ, INNER, INNER, SJ, INNER]`
/// → `[(vec![INNER], Some(SJ)), (vec![INNER, INNER], Some(SJ)), (vec![INNER], None)]`
fn split_joins_by_straight_join_barriers(
    joins: Vec<JoinClause>,
) -> Vec<(Vec<JoinClause>, Option<JoinClause>)> {
    let mut result: Vec<(Vec<JoinClause>, Option<JoinClause>)> = Vec::new();
    let mut segment: Vec<JoinClause> = Vec::new();
    for jc in joins {
        if matches!(jc.operator, StraightJoin) {
            result.push((mem::take(&mut segment), Some(jc)));
        } else {
            segment.push(jc);
        }
    }
    result.push((segment, None));
    result
}

/// Reorders each non-SJ sub-segment in place, accumulating results directly into `stmt.join`.
/// STRAIGHT_JOIN barriers are reinserted after each sub-segment, preserving the user's ordering
/// hints. Single-join segments are appended as-is.
///
/// For each segment with ≥2 joins: strips existing ON predicates into the pool via
/// [`strip_joins_preds`], calls `reorder_func`, then extends `stmt.join`. Passing `&stmt.join`
/// as `preceding_joins` is safe because `reorder_func`'s immutable borrow ends before the
/// subsequent `stmt.join.extend()` (sequential, non-overlapping borrows).
fn process_segments(
    segments: impl IntoIterator<Item = (Vec<JoinClause>, Option<JoinClause>)>,
    stmt: &mut SelectStatement,
    reorder_func: &mut dyn JoinsReorderFunc,
    is_joinable_predicate: &impl Fn(&Expr) -> bool,
    remaining_where: &mut Option<Expr>,
    did_reorder: &mut bool,
    available_predicates: &mut Vec<Expr>,
) -> ReadySetResult<()> {
    for (mut seg, sj_barrier) in segments {
        if seg.len() >= 2 {
            // Strip predicates off the joins with existing ON expressions, and add them to the join predicates pool.
            strip_joins_preds(
                &mut seg,
                is_joinable_predicate,
                available_predicates,
                remaining_where,
            );
            let (seg_reordered, reordered_seg) =
                reorder_func(seg, &stmt.tables, &stmt.join, available_predicates)?;
            *did_reorder |= seg_reordered;
            stmt.join.extend(reordered_seg);
        } else {
            stmt.join.extend(seg);
        }
        if let Some(sj) = sj_barrier {
            stmt.join.push(sj);
        }
    }
    Ok(())
}

/// Driver for join reordering. Partitions the join chain into reorderable regions, applies
/// `reorder_func` to each non-SJ sub-segment, and reassembles the result.
///
/// # Partitioning
/// The chain is split into three contiguous regions:
/// - **Prefix**: inner joins before the first outer join — fully reorderable.
/// - **Middle**: from the first to the last outer join inclusive — preserved in original order.
/// - **Suffix**: inner joins after the last outer join — reorderable with full preceding context.
///
/// Within prefix and suffix, `STRAIGHT_JOIN` barriers further divide each region into independent
/// sub-segments reordered in isolation.
///
/// # Predicate lifecycle
/// 1. `is_joinable_predicate`-matching expressions are extracted from WHERE into a pool.
/// 2. Existing ON clauses of inner joins are stripped into the same pool before each segment call.
/// 3. `reorder_func` consumes pool entries when assigning ON clauses to reordered joins.
/// 4. Unused pool entries are written back to WHERE.
///
/// # Leading table
/// The first prefix sub-segment is special: `stmt.tables[0]` is temporarily inserted as a
/// synthetic `CrossJoin` so it participates in reordering alongside the joins. The element
/// at position 0 of `reorder_func`'s output is extracted back to `stmt.tables` afterward
/// (see [`JoinsReorderFunc`] contract).
///
/// Returns `true` if any segment was reordered.
pub(crate) fn reorder_joins_in_stmt(
    stmt: &mut SelectStatement,
    reorder_func: &mut dyn JoinsReorderFunc,
    is_joinable_predicate: &impl Fn(&Expr) -> bool,
) -> ReadySetResult<bool> {
    invariant!(
        stmt.tables.len() == 1,
        "normalize_comma_separated_lhs(stmt) should have been called before"
    );

    if stmt.join.is_empty() {
        return Ok(false);
    }

    // === Extract predicates from WHERE clause that are candidate join conditions ===
    let mut available_predicates = Vec::new();
    let mut remaining_where = if let Some(where_expr) = stmt.where_clause.as_ref() {
        split_expr(where_expr, is_joinable_predicate, &mut available_predicates)
    } else {
        None
    };

    let mut did_reorder = false;

    // === Determine boundaries of reorderable join chains ===
    // The join chain is partitioned into:
    //   - Prefix inner joins (before the first outer join)
    //   - Middle joins (non-reorderable outer/inner joins)
    //   - Suffix inner joins (after the last outer join)
    let first_outer_idx = stmt
        .join
        .iter()
        .position(|jc| !jc.operator.is_inner_join())
        .unwrap_or(stmt.join.len());

    let last_outer_idx = stmt
        .join
        .iter()
        .rposition(|jc| !jc.operator.is_inner_join());

    let suffix_start = match last_outer_idx {
        Some(idx) => idx + 1,
        None => 0,
    };

    let prefix_joins = stmt.join.drain(0..first_outer_idx).collect::<Vec<_>>();

    let middle_joins = if suffix_start > prefix_joins.len() {
        stmt.join
            .drain(0..(suffix_start - prefix_joins.len()))
            .collect::<Vec<_>>()
    } else {
        vec![]
    };

    let suffix_joins = stmt.join.drain(..).collect::<Vec<_>>();

    // === Reorder prefix inner joins if applicable ===
    // These are the joins at the beginning of the join chain, up to the first outer join.
    // STRAIGHT_JOIN barriers within the prefix are kept in place; the contiguous non-SJ
    // sub-segments between them are independently reordered.
    // The first sub-segment is special: stmt.tables[0] is temporarily inserted as a synthetic
    // CrossJoin so the leading table can participate in reordering, then restored afterward.
    // All subsequent prefix sub-segments are handled by process_segments.
    if !prefix_joins.is_empty() {
        let mut segments = split_joins_by_straight_join_barriers(prefix_joins).into_iter();

        if let Some((mut seg, sj_barrier)) = segments.next() {
            // Insert stmt.tables[0] as a synthetic CrossJoin so the leading table
            // participates in reordering, then extract the winner back afterward.
            seg.insert(
                0,
                JoinClause {
                    operator: CrossJoin,
                    // SAFETY: this block is only entered when `stmt.tables` is non-empty
                    // (the function requires at least one FROM item to operate on).
                    right: JoinRightSide::Table(stmt.tables.pop().expect("stmt has FROM item")),
                    constraint: JoinConstraint::Empty,
                },
            );

            if seg.len() >= 2 {
                strip_joins_preds(
                    &mut seg,
                    is_joinable_predicate,
                    &mut available_predicates,
                    &mut remaining_where,
                );
                let (seg_reordered, mut reordered_seg) =
                    reorder_func(seg, &[], &[], &mut available_predicates)?;
                did_reorder |= seg_reordered;
                // Extract the winner back to stmt.tables; position 0 must have Empty constraint
                // per the JoinsReorderFunc contract (no LHS exists to build an ON clause against).
                let lhs = reordered_seg.remove(0);
                invariant!(
                    matches!(lhs.constraint, JoinConstraint::Empty),
                    "The leftmost join for prefixing reorder should have Empty constraint"
                );
                match lhs.right {
                    JoinRightSide::Table(table) => stmt.tables.push(table),
                    JoinRightSide::Tables(tables) => stmt.tables.extend(tables),
                }
                stmt.join.extend(reordered_seg);
            } else {
                // First sub-segment was empty (SJ barrier at position 0); just restore
                // the synthetic join back to stmt.tables without adding anything to result.
                debug_assert_eq!(seg.len(), 1, "only the synthetic CrossJoin should remain");
                let lhs = seg.remove(0);
                match lhs.right {
                    JoinRightSide::Table(table) => stmt.tables.push(table),
                    JoinRightSide::Tables(tables) => stmt.tables.extend(tables),
                }
            }

            if let Some(sj) = sj_barrier {
                stmt.join.push(sj);
            }
        }

        // Remaining prefix sub-segments share the same structure as suffix sub-segments.
        process_segments(
            segments,
            stmt,
            reorder_func,
            is_joinable_predicate,
            &mut remaining_where,
            &mut did_reorder,
            &mut available_predicates,
        )?;
    }

    // === Restore middle (outer) joins ===
    // These are untouched — preserved in their original order.
    stmt.join.extend(middle_joins);

    // === Reorder suffix inner joins if applicable ===
    // These are inner joins following the last outer join. As with the prefix, STRAIGHT_JOIN
    // barriers within the suffix are kept in place; non-SJ sub-segments are independently
    // reordered. The full preceding context (stmt.tables + prefix + middle + suffix processed
    // so far) is passed so predicates spanning the boundary can be correctly evaluated.
    process_segments(
        split_joins_by_straight_join_barriers(suffix_joins),
        stmt,
        reorder_func,
        is_joinable_predicate,
        &mut remaining_where,
        &mut did_reorder,
        &mut available_predicates,
    )?;

    // === Place all remaining unused predicates back into WHERE clause
    if let Some(conjoined) = conjoin_all_dedup(available_predicates) {
        remaining_where = and_predicates_skip_true(remaining_where, conjoined);
    }
    stmt.where_clause = remaining_where;

    Ok(did_reorder)
}

/// Reorders INNER JOINs (including CROSS and ON TRUE joins) to maximize application
/// of WHERE clause constraints as join conditions.
///
/// This transformation seeks to bind joins as early as possible using filters
/// already available in WHERE, enabling better pruning and pushdown.
///
/// Assumes:
/// - `normalize_comma_separated_lhs_to_joins` has already run
/// - No OUTER joins are reordered or modified
fn normalize_inner_joins_order(stmt: &mut SelectStatement) -> ReadySetResult<bool> {
    reorder_joins_in_stmt(stmt, &mut reorder_joins, &IS_CROSS_TABLE_EQUALITY)
}

pub(crate) fn normalize_comma_separated_lhs(stmt: &mut SelectStatement) -> ReadySetResult<bool> {
    Ok(if stmt.tables.len() > 1 {
        stmt.join.splice(
            0..0,
            stmt.tables.drain(1..).map(|dt| JoinClause {
                operator: CrossJoin,
                right: JoinRightSide::Table(dt),
                constraint: JoinConstraint::Empty,
            }),
        );
        true
    } else {
        false
    })
}

/// Canonicalize join operators and constraints into minimal, deterministic form.
///
/// This pass rewrites joins to normalize both:
///   - The **join operator and ON clause syntax**, and
///   - The **directionality of equality predicates** in ON clauses.
///
/// Specifically, it:
/// - Rewrites ambiguous join syntax into canonical forms:
///   - `JOIN` → `INNER JOIN`
///   - `INNER JOIN ON TRUE` → `CROSS JOIN`
///   - `INNER JOIN` with no ON → `CROSS JOIN`
///   - `LEFT JOIN` → `LEFT OUTER JOIN`
///   - `LEFT OUTER JOIN ON TRUE` → `LEFT OUTER JOIN` with `Empty`
///
/// - Rewrites ON expressions to normalize operand order in binary equalities:
///   - Ensures columns from the accumulated LHS appear on the left-hand side,
///     and columns from the current join RHS appear on the right-hand side,
///     for all `col = col` comparisons within the ON clause.
///
/// This transformation runs **after** all semantic rewrites (e.g., ON propagation,
/// WHERE-to-ON movement, join reordering) and is the final step in shaping a stable,
/// engine-compliant join structure.
///
/// Returns `true` if any join clause or ON expression was rewritten.
fn canonicalize_join_syntax_and_predicates(stmt: &mut SelectStatement) -> ReadySetResult<bool> {
    let mut rewritten = false;

    let mut lhs_rels = stmt
        .tables
        .iter()
        .map(get_from_item_reference_name)
        .collect::<ReadySetResult<Vec<_>>>()?;

    for jc in &mut stmt.join {
        let rhs_rels = get_join_rhs_relations(jc);

        // STRAIGHT_JOIN is excluded from operator canonicalization: its operator must be
        // preserved intact (bare SJ must not become CrossJoin; SJ with ON must not become
        // InnerJoin). ON predicate directionality normalization below still applies to SJ.
        if jc.operator.is_inner_join() && !matches!(jc.operator, StraightJoin) {
            if matches!(
                jc.constraint,
                JoinConstraint::Empty | JoinConstraint::On(Expr::Literal(Literal::Boolean(true)))
            ) {
                jc.operator = CrossJoin;
                jc.constraint = JoinConstraint::Empty;
                rewritten = true;
            } else if matches!(jc.operator, JoinOperator::Join) {
                jc.operator = InnerJoin;
                rewritten = true;
            }
        } else if !jc.operator.is_inner_join() {
            if matches!(
                jc.constraint,
                JoinConstraint::Empty | JoinConstraint::On(Expr::Literal(Literal::Boolean(true)))
            ) {
                jc.constraint = JoinConstraint::Empty;
                rewritten = true;
            }
            if matches!(jc.operator, JoinOperator::LeftJoin) {
                jc.operator = LeftOuterJoin;
                rewritten = true;
            }
        }

        if let JoinConstraint::On(expr) = &mut jc.constraint {
            struct ExprVisitor<'a> {
                lhs_rels: &'a [Relation],
                rhs_rels: &'a [Relation],
                rewritten: bool,
            }

            impl<'a> VisitorMut<'a> for ExprVisitor<'a> {
                type Error = ReadySetError;

                fn visit_expr(&mut self, expr: &'a mut Expr) -> Result<(), Self::Error> {
                    let mut swap_ops = false;
                    if matches_eq_constraint(expr, |lr, rr| {
                        if self.lhs_rels.contains(rr) && self.rhs_rels.contains(lr) {
                            swap_ops = true;
                        }
                        true
                    }) {
                        if swap_ops {
                            *expr = swap_eq_constraint_operands(expr)?;
                            self.rewritten = true;
                        }
                        return Ok(());
                    }
                    visit_mut::walk_expr(self, expr)
                }
            }

            let mut expr_visitor = ExprVisitor {
                lhs_rels: &lhs_rels,
                rhs_rels: &rhs_rels,
                rewritten: false,
            };

            expr_visitor.visit_expr(expr)?;
            rewritten |= expr_visitor.rewritten;
        }

        lhs_rels.extend(rhs_rels);
    }

    Ok(rewritten)
}

pub(crate) fn try_normalize_joins_conditions(stmt: &mut SelectStatement) -> ReadySetResult<bool> {
    let mut did_rewrite = false;

    // Normalize the joins ON, ensuring the `no more than 2-table join` invariant.
    let mut move_to_where_items: Vec<Expr> = Vec::new();

    let mut lhs_relations = stmt
        .tables
        .iter()
        .map(get_from_item_reference_name)
        .collect::<ReadySetResult<Vec<_>>>()?;

    for jc in stmt.join.iter_mut() {
        if jc.operator.is_inner_join()
            && let JoinConstraint::On(expr) = &jc.constraint
        {
            let (on_expr, to_where) =
                repair_join_constraint_for_two_table_limit(expr, &jc.right, &lhs_relations)?;
            if on_expr.as_ref() != Some(expr) {
                did_rewrite = true;
            }
            jc.constraint = on_expr.map_or(JoinConstraint::Empty, JoinConstraint::On);
            if let Some(tw) = to_where {
                move_to_where_items.push(tw);
            }
        }
        lhs_relations.extend(get_join_rhs_relations(jc));
    }

    // Move back to WHERE inappropriate join ON constraints
    if let Some(expr) = conjoin_all_dedup(move_to_where_items) {
        stmt.where_clause = and_predicates_skip_true(mem::take(&mut stmt.where_clause), expr);
        did_rewrite = true;
    }

    Ok(did_rewrite)
}

/// Repair a join constraint to satisfy the "two-table ON" ReadySet requirement.
/// Will move excess constraints (referencing >2 tables) to the base WHERE clause.
/// Returns an optional expression to AND into base_stmt.where_clause.
fn repair_join_constraint_for_two_table_limit(
    expr: &Expr,
    right_side: &JoinRightSide,
    lhs_relations: &[Relation],
) -> ReadySetResult<(Option<Expr>, Option<Expr>)> {
    // Determine RHS relation: the relation used in the ON that exists in right_side
    let rhs_candidates = right_side
        .table_exprs()
        .map(get_from_item_reference_name)
        .collect::<ReadySetResult<Vec<_>>>()?;

    let maybe_rhs_rel = rhs_candidates
        .iter()
        .find(|rel| columns_iter(expr).any(|col| col.table.as_ref() == Some(*rel)));

    if let Some(rhs_rel) = maybe_rhs_rel {
        let split = split_on_for_rhs_against_preceding_lhs(expr, rhs_rel, lhs_relations);
        Ok((split.on_expr, split.to_where))
    } else {
        // No RHS relation in ON: move entire ON clause to WHERE
        Ok((None, Some(expr.clone())))
    }
}

/// Swap the left and right operands in a `col = col` expression.
fn swap_eq_constraint_operands(constraint: &Expr) -> ReadySetResult<Expr> {
    match constraint {
        Expr::BinaryOp {
            lhs,
            op: BinaryOperator::Equal,
            rhs,
        } => Ok(Expr::BinaryOp {
            lhs: rhs.clone(),
            op: BinaryOperator::Equal,
            rhs: lhs.clone(),
        }),
        _ => internal!(
            "Expected equality constraint, got: {}",
            constraint.display(Dialect::PostgreSQL)
        ),
    }
}

/// Check if a list of tables contains the lhs and/or rhs relation, returning their indices.
fn contain_lhs_rhs(
    tables: &[TableExpr],
    lhs: &Relation,
    rhs: &Relation,
) -> ReadySetResult<Option<EitherOrBoth<usize>>> {
    let mut lhs_idx = None;
    let mut rhs_idx = None;
    for (table_idx, table) in tables.iter().enumerate() {
        let table_ref_name = get_from_item_reference_name(table)?;
        if table_ref_name.eq(lhs) {
            lhs_idx = Some(table_idx);
        }
        if table_ref_name.eq(rhs) {
            rhs_idx = Some(table_idx);
        }
    }
    Ok(match (lhs_idx, rhs_idx) {
        (Some(lhs_idx), Some(rhs_idx)) => Some(EitherOrBoth::Both(lhs_idx, rhs_idx)),
        (Some(lhs_idx), None) => Some(EitherOrBoth::Left(lhs_idx)),
        (None, Some(rhs_idx)) => Some(EitherOrBoth::Right(rhs_idx)),
        (None, None) => None,
    })
}

/// Locate an existing INNER join clause where a new rhs constraint can be added.
fn find_join_candidate_for_rhs(
    stmt: &SelectStatement,
    rhs: &Relation,
) -> ReadySetResult<Option<usize>> {
    for (jc_idx, jc) in stmt.join.iter().enumerate() {
        if !jc.operator.is_inner_join() {
            continue;
        }
        let contains_rhs = match &jc.right {
            JoinRightSide::Table(table) => get_from_item_reference_name(table)?.eq(rhs),
            JoinRightSide::Tables(tables) => {
                let mut contains_rhs = false;
                for table in tables {
                    if get_from_item_reference_name(table)?.eq(rhs) {
                        contains_rhs = true;
                        break;
                    }
                }
                contains_rhs
            }
        };
        if contains_rhs {
            return Ok(Some(jc_idx));
        }
    }
    Ok(None)
}

fn try_add_to_existing_join(
    stmt: &mut SelectStatement,
    jc_idx: usize,
    eq_constraint: Expr,
) -> bool {
    let join_constraint = mem::replace(&mut stmt.join[jc_idx].constraint, JoinConstraint::Empty);

    stmt.join[jc_idx].constraint =
        add_expression_to_join_constraint(join_constraint.clone(), eq_constraint);

    if contains_problematic_self_joins(stmt) {
        stmt.join[jc_idx].constraint = join_constraint;
        false
    } else {
        true
    }
}

fn insert_new_join_at(
    stmt: &mut SelectStatement,
    jc_idx: usize,
    join_rhs: TableExpr,
    on_expr: Expr,
) -> bool {
    stmt.join.insert(
        jc_idx,
        JoinClause {
            operator: InnerJoin,
            right: JoinRightSide::Table(join_rhs),
            constraint: JoinConstraint::On(on_expr),
        },
    );
    !contains_problematic_self_joins(stmt)
}

fn remove_new_join_at(stmt: &mut SelectStatement, jc_idx: usize) -> ReadySetResult<TableExpr> {
    if let JoinRightSide::Table(rhs) = stmt.join.remove(jc_idx).right {
        Ok(rhs)
    } else {
        internal!("Expected JoinRightSide::Table at join index {}", jc_idx)
    }
}

/// Try to move an equality constraint into an existing or new join clause.
fn add_to_statement_join(stmt: &mut SelectStatement, eq_constraint: Expr) -> ReadySetResult<bool> {
    //
    let Some((lhs, rhs)) = get_lhs_rhs_tables_from_eq_constraint(&eq_constraint) else {
        return Ok(false);
    };

    macro_rules! with_rhs_tables_do {
        ($jc_idx:expr, $method:ident ( $($args:tt)* )) => {
            if let JoinRightSide::Tables(tables) = &mut stmt.join[$jc_idx].right {
                tables.$method($($args)*)
            } else {
                // SAFETY: callers only invoke this macro on join clauses that were verified
                // to have `JoinRightSide::Tables` by the `contain_lhs_rhs` check above.
                internal!("Expected JoinRightSide::Tables at join index {}", $jc_idx)
            }
        };
    }

    match contain_lhs_rhs(&stmt.tables, &lhs, &rhs)? {
        Some(EitherOrBoth::Both(_, rhs_idx)) => {
            if is_filter_pushable_from_join_clause(stmt, 0) {
                let rhs = stmt.tables.remove(rhs_idx);
                return Ok(if insert_new_join_at(stmt, 0, rhs, eq_constraint) {
                    true
                } else {
                    let rhs = remove_new_join_at(stmt, 0)?;
                    stmt.tables.insert(rhs_idx, rhs);
                    false
                });
            }
        }
        Some(EitherOrBoth::Left(_)) => {
            if let Some(jc_idx) = find_join_candidate_for_rhs(stmt, &rhs)? {
                return Ok(try_add_to_existing_join(stmt, jc_idx, eq_constraint));
            }
        }
        Some(EitherOrBoth::Right(_)) => {
            if let Some(jc_idx) = find_join_candidate_for_rhs(stmt, &lhs)? {
                return Ok(try_add_to_existing_join(
                    stmt,
                    jc_idx,
                    swap_eq_constraint_operands(&eq_constraint)?,
                ));
            }
        }
        None => {}
    }

    let mut join_to_insert = None;
    for (jc_idx, jc) in stmt.join.iter().enumerate() {
        if let JoinRightSide::Tables(tables) = &jc.right
            && let Some(EitherOrBoth::Both(_, rhs_idx)) = contain_lhs_rhs(tables, &lhs, &rhs)?
        {
            if !jc.operator.is_inner_join() {
                return Ok(false);
            }
            join_to_insert = Some((jc_idx, rhs_idx));
            break;
        }
    }
    if let Some((jc_idx, rhs_idx)) = join_to_insert {
        let rhs = with_rhs_tables_do!(jc_idx, remove(rhs_idx));
        return Ok(
            if insert_new_join_at(stmt, jc_idx + 1, rhs, eq_constraint) {
                true
            } else {
                let rhs = remove_new_join_at(stmt, jc_idx + 1)?;
                with_rhs_tables_do!(jc_idx, insert(rhs_idx, rhs));
                false
            },
        );
    }

    let jc_idx = match (
        find_join_candidate_for_rhs(stmt, &lhs)?,
        find_join_candidate_for_rhs(stmt, &rhs)?,
    ) {
        (Some(lhs_jc_idx), Some(rhs_jc_idx)) => cmp::max(lhs_jc_idx, rhs_jc_idx),
        (Some(lhs_jc_idx), None) => lhs_jc_idx,
        (None, Some(rhs_jc_idx)) => rhs_jc_idx,
        (None, None) => return Ok(false),
    };
    Ok(try_add_to_existing_join(stmt, jc_idx, eq_constraint))
}

// This function tries to move cross-table equality predicates from WHERE into JOIN ... ON
// constraints if the target JOIN clause can safely accommodate them.
//
// It is a best-effort optimization — not a correctness requirement. If a predicate can't be
// moved (e.g., no matching join clause, join not inner, or causing `self-join` issue),
// it remains in WHERE.
//
// === Important Distinction ===
// Do NOT refactor this via `split_on_for_rhs_against_preceding_lhs`. That function performs
// the *inverse* operation: it removes JOIN ON predicates that are semantically invalid due to
// ReadySet's 2-table ON constraint. It is meant to *repair* joins after rewriting.
//
// This function, in contrast, performs *opportunistic cleanup*, not invariant enforcement.
pub(crate) fn move_applicable_where_conditions_to_joins(
    stmt: &mut SelectStatement,
) -> ReadySetResult<bool> {
    if let Some(where_expr) = &stmt.where_clause {
        let mut constraints_candidates = Vec::new();
        let mut remaining_where_expr = split_expr(
            where_expr,
            &IS_CROSS_TABLE_EQUALITY,
            &mut constraints_candidates,
        );
        if !constraints_candidates.is_empty() {
            let mut rejected = Vec::new();
            for constraint in constraints_candidates {
                if !add_to_statement_join(stmt, constraint.clone())? {
                    rejected.push(constraint);
                }
            }
            if let Some(conjoined) = conjoin_all_dedup(rejected) {
                remaining_where_expr = and_predicates_skip_true(remaining_where_expr, conjoined);
            }
            stmt.where_clause = remaining_where_expr;
            return Ok(true);
        }
    }
    Ok(false)
}

fn repair_problematic_self_join_issue(stmt: &mut SelectStatement) -> ReadySetResult<()> {
    // Strip off join predicates from INNER joins and place them to WHERE clause
    let (predicates, mut back_to_where) =
        strip_inner_joins_predicates(&mut stmt.join, &IS_CROSS_TABLE_EQUALITY);
    if let Some(conjoined) = conjoin_all_dedup(predicates) {
        back_to_where = and_predicates_skip_true(back_to_where, conjoined);
    }
    if let Some(back_to_where) = back_to_where {
        stmt.where_clause = and_predicates_skip_true(stmt.where_clause.take(), back_to_where);
    }

    // Re-adding applicable constraints back to the appropriate INNER join,
    // one by one avoiding the `problematic self-join` issue.
    // **NOTE** This function does not factor in possibility of `more than 2 tables join`.
    move_applicable_where_conditions_to_joins(stmt)?;

    // Detect and repair the `more than 2 tables join` issue
    try_normalize_joins_conditions(stmt)?;

    Ok(())
}

/// Moves applicable WHERE cross-table equality predicates onto `STRAIGHT_JOIN` ON constraints.
///
/// STRAIGHT_JOIN carries a join-order hint: its position and constraints must be preserved
/// through the rewrite pipeline. This pass runs before join reordering so that any WHERE
/// predicate that naturally belongs to a SJ’s ON is anchored there before the rest of the
/// pipeline treats the SJ as a fixed barrier.
///
/// Only cross-table equality predicates are considered — single-table filters cannot be
/// join ON predicates per the engine’s 2-table ON constraint (see known_core_limitations.md).
/// Predicates that reference a SJ’s RHS and some table already in the accumulated LHS at
/// that SJ’s position are consumed from WHERE and appended to the SJ’s ON via AND.
/// All other WHERE predicates (including cross-barrier ones) are left untouched.
///
/// Returns `true` if at least one predicate was moved.
fn move_applicable_where_conditions_to_straight_joins(
    stmt: &mut SelectStatement,
) -> ReadySetResult<bool> {
    if !stmt
        .join
        .iter()
        .any(|jc| matches!(jc.operator, StraightJoin))
    {
        return Ok(false);
    }
    let Some(where_expr) = &stmt.where_clause else {
        return Ok(false);
    };

    // Extract all cross-table equality candidates from WHERE into a working pool.
    let mut candidates: Vec<Expr> = Vec::new();
    let remaining = split_expr(
        where_expr,
        &|expr| matches_eq_constraint(expr, |t1, t2| !t1.eq(t2)),
        &mut candidates,
    );

    if candidates.is_empty() {
        return Ok(false);
    }

    // Walk the join chain left-to-right, accumulating the LHS relations seen so far.
    // For each SJ encountered, drain any candidates that equate its RHS with an LHS table
    // into the SJ’s ON constraint.
    let mut lhs_rels: Vec<Relation> = stmt
        .tables
        .iter()
        .map(get_from_item_reference_name)
        .collect::<ReadySetResult<Vec<_>>>()?;

    let mut moved = false;
    for jc in &mut stmt.join {
        if matches!(jc.operator, StraightJoin)
            && let Some(preds) = take_candidate_predicates_for_join(&lhs_rels, jc, &mut candidates)
        {
            for pred in preds {
                jc.constraint = add_expression_to_join_constraint(
                    mem::replace(&mut jc.constraint, JoinConstraint::Empty),
                    pred,
                );
            }
            moved = true;
        }
        lhs_rels.extend(get_join_rhs_relations(jc));
    }

    if moved {
        // Put unconsumed candidates back into WHERE alongside any non-candidate remainder.
        stmt.where_clause = if let Some(conjoined) = conjoin_all_dedup(candidates) {
            and_predicates_skip_true(remaining, conjoined)
        } else {
            remaining
        };
    }

    Ok(moved)
}

/// Normalize the join structure of a single SELECT statement in-place.
///
/// This transformation rewrites and canonicalizes the join sequence of the current
/// `SelectStatement`, without recursing into subqueries. It performs:
///
/// 1. Rewriting comma-separated FROM items into CROSS JOINs.
/// 2. Attaching applicable WHERE predicates to STRAIGHT_JOIN ON constraints.
/// 3. Reordering INNER JOIN chains using equality constraints from the WHERE clause.
/// 4. Repairing multi-table ON constraints that violate ReadySet’s 2-table ON limit.
/// 5. Fixing problematic self-joins by demoting ON clauses to WHERE and reattaching safely.
/// 6. Canonicalizing all JOIN operator + constraint combinations into minimal, deterministic form.
///
/// This is a **local rewrite pass** — intended to operate on a single flat SELECT.
/// It is called by [`normalize_joins_shape`] to apply normalization recursively across the query.
fn normalize_statement_joins_shape(
    stmt: &mut SelectStatement,
    dialect: Dialect,
) -> ReadySetResult<bool> {
    let mut was_rewritten = false;

    if stmt.tables.is_empty() {
        // FROM-less statements (e.g., SELECT 1) have nothing to normalize.
        // Let downstream passes (query_graph) handle the rejection.
        return Ok(false);
    }

    // Rewrite comma separated LHS followed by explicit joins with CROSS JOIN,
    // keeping only single LHS in `stmt.tables`
    was_rewritten |= normalize_comma_separated_lhs(stmt)?;

    // Attach applicable WHERE predicates to STRAIGHT_JOIN ON constraints before reordering.
    // SJ joins are treated as fixed barriers; their ON must be enriched first so the
    // available_predicates pool for normalize_inner_joins_order is already SJ-clean.
    was_rewritten |= move_applicable_where_conditions_to_straight_joins(stmt)?;

    // Reorder the joins using constraints from WHERE clause to attach as ON predicates.
    // STRAIGHT_JOIN barriers are respected: only contiguous non-SJ inner-join sub-segments
    // between barriers are reordered.
    was_rewritten |= normalize_inner_joins_order(stmt)?;

    // Detect and repair the `more than 2 tables join` issue
    was_rewritten |= try_normalize_joins_conditions(stmt)?;

    // Detect and fix problematic self-joins that remain even after ON normalization.
    // These can arise from joins with multiple-table ON constraints, often due to
    // reordering or constraint attachment from WHERE clause.
    //
    // To recover a canonical shape:
    //   1. Strip all INNER JOIN ON constraints and move them to WHERE.
    //   2. Reassign each constraint back only if it does not cause ambiguity.
    //   3. Temporarily strip WHERE to allow safe reordering of INNER JOINs only using ONs.
    if contains_problematic_self_joins(stmt) {
        repair_problematic_self_join_issue(stmt)?;

        let where_clause = stmt.where_clause.take();
        normalize_inner_joins_order(stmt)?;
        stmt.where_clause = where_clause;

        was_rewritten = true;
    }

    // Canonicalize join operator and constraint syntax.
    // This pass runs *last* to ensure syntactic determinism across semantically equivalent
    // joins, after all semantic rewrites (reordering, repair, demotion) have completed.
    // It rewrites ambiguous or redundant constructs (e.g., INNER JOIN ON TRUE → CROSS JOIN)
    // into a canonical form that is minimal, stable, and engine-compliant.
    was_rewritten |= canonicalize_join_syntax_and_predicates(stmt)?;

    // Guardrail to protect against:
    // [REA-6129: Some LEFT OUTER JOINs with conditions addressing only 1 side of the JOIN execute but produce incorrect results.]
    // (https://linear.app/readyset/issue/REA-6129/some-left-outer-joins-with-conditions-addressing-only-1-side-of-the)
    for jc in &stmt.join {
        if let JoinConstraint::On(join_expr) = &jc.constraint
            && !is_supported_join_condition(join_expr)
        {
            unsupported!("Join expression {}", join_expr.display(Dialect::PostgreSQL))
        }
    }

    // Eliminate WHERE TRUE — a redundant leftover from constraint movement or rewrites.
    if let Some(where_expr) = &stmt.where_clause
        && is_always_true_filter(where_expr, dialect)
    {
        stmt.where_clause = None;
        was_rewritten = true;
    }

    Ok(was_rewritten)
}

/// Recursively normalize the join structure of a SELECT query and its subqueries.
///
/// This function traverses all local derived tables (i.e., subqueries in the FROM clause)
/// and applies [`normalize_statement_joins_shape`] to each one, ensuring full normalization
/// of the join structure across the entire query tree.
///
/// This is the top-level entry point for join normalization and produces a canonical,
/// semantically equivalent join shape compatible with ReadySet’s engine.
///
/// Returns `true` if any rewrites were applied across any subquery.
pub(crate) fn normalize_joins_shape(
    stmt: &mut SelectStatement,
    dialect: Dialect,
) -> ReadySetResult<bool> {
    let mut was_rewritten = false;

    for dt in get_local_from_items_iter_mut!(stmt) {
        if let Some((dt_stmt, _)) = as_sub_query_with_alias_mut(dt) {
            was_rewritten |= normalize_joins_shape(dt_stmt, dialect)?;
        }
    }

    was_rewritten |= normalize_statement_joins_shape(stmt, dialect)?;

    Ok(was_rewritten)
}
