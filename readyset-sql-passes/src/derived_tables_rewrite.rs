use crate::infer_nullability::{derive_from_stmt, is_expr_null_preserving};
use crate::inline_subquery::{
    can_inline_subquery, carry_inner_limit_and_order, compute_downstream_for_position,
    inline_from_item_position_checks, refs_any_of_rels_anywhere, refs_rel_anywhere,
    visible_base_rels_except,
};
use crate::rewrite_joins::{
    get_join_rhs_relations, move_applicable_where_conditions_to_joins,
    normalize_comma_separated_lhs, normalize_joins_shape, try_normalize_joins_conditions,
};
use crate::rewrite_utils::{
    OnAtom, add_expression_to_join_constraint, and_predicates_skip_true, as_sub_query_with_alias,
    as_sub_query_with_alias_mut, build_ext_to_int_fields_map, classify_on_atom, columns_iter,
    conjoin_all_dedup, expect_sub_query_with_alias_mut, find_rhs_join_clause,
    fix_groupby_without_aggregates, is_aggregation_or_grouped,
    make_aliases_distinct_from_base_statement, normalize_having_and_group_by, outermost_expression,
    resolve_field_reference, resolve_group_by_exprs, split_expr,
};
use crate::unnest_subqueries::{NonNullSchema, agg_only_no_gby_cardinality};
use crate::{get_local_from_items_iter, get_local_from_items_iter_mut, is_single_from_item};
use itertools::Either;
use readyset_errors::{ReadySetError, ReadySetResult, internal, internal_err};
use readyset_sql::analysis::visit::Visitor;
use readyset_sql::ast::JoinOperator::InnerJoin;
use readyset_sql::ast::{
    Column, Expr, FieldReference, GroupByClause, JoinClause, JoinConstraint, JoinOperator,
    JoinRightSide, Literal, Relation, SelectStatement, TableExpr, TableExprInner,
};
use readyset_sql::{Dialect, DialectDisplay};
use std::collections::{HashMap, HashSet};
use std::iter;
use std::mem;
use tracing::trace;

/// Entry-point trait for FROM subqueries rewriting.
///
/// After schema resolution and subquery unnesting, this will recursively
/// inline nested subqueries, augment join structures, and optimize filters.
///
/// ## Prerequisite Rewrite Passes
///
/// This module requires the following prerequisite rewrite passes to be applied, before any of its transformations:
/// - `resolve_schemas`
/// - `scalar_optimize_expressions`
/// - `expand_stars`
/// - `expand_implied_tables`
/// - `expand_join_on_using`
///
/// Failing to apply these passes will result in panics, invalid transformations, or incorrect semantics.
///
pub trait DerivedTablesRewrite: Sized {
    /// Rewrite this `SelectStatement` into a flat join form suitable for ReadySet.
    ///
    /// Steps performed:
    /// 1. Subqueries unnesting & inlining
    /// 2. Join normalization and filter hoisting
    /// 3. Aggregated‐subquery filter extraction
    fn derived_tables_rewrite(&mut self, dialect: Dialect) -> ReadySetResult<&mut Self>;
}

impl DerivedTablesRewrite for SelectStatement {
    fn derived_tables_rewrite(&mut self, dialect: Dialect) -> ReadySetResult<&mut Self> {
        if derived_tables_rewrite_main(self, dialect)? {
            trace!(
                name = "Derived tables rewritten",
                "{}",
                self.display(Dialect::PostgreSQL)
            );
        }
        Ok(self)
    }
}

/// Returns `true` if `expr` references any column that maps to a non-column expression in
/// `ext_to_int_fields_map` (i.e., the column expands to an aggregate, arithmetic, or other
/// complex expression rather than a plain column reference). Used to identify ON predicates
/// that cannot remain in a JOIN clause after the subquery they reference is inlined.
fn contains_nontrivial_columns(expr: &Expr, ext_to_int_fields_map: &HashMap<Column, Expr>) -> bool {
    columns_iter(expr).any(|col| {
        ext_to_int_fields_map
            .get(col)
            .is_some_and(|expr| !matches!(expr, Expr::Column(_)))
    })
}

/// Splits an ON predicate into two parts:
/// - nontrivial constraints: sub-expressions referencing columns that map to non-column
///   expressions in `ext_to_int_fields_map` — these must move to WHERE after inlining
///   because they can no longer be expressed as simple join predicates.
/// - the remainder: sub-expressions that can stay in the JOIN ON clause.
fn split_joins_on_nontrivial_expr(
    on_expr: &Expr,
    ext_to_int_fields_map: &HashMap<Column, Expr>,
) -> (Vec<Expr>, Option<Expr>) {
    let mut nontrivial_constraints = Vec::new();
    let remaining_expr = split_expr(
        on_expr,
        &|constraint| contains_nontrivial_columns(constraint, ext_to_int_fields_map),
        &mut nontrivial_constraints,
    );
    (nontrivial_constraints, remaining_expr)
}

/// For each join with a nontrivial ON clause, move those predicates into the
/// top-level WHERE clause, leaving any trivial parts behind (or emptying the join).
fn move_joins_on_nontrivial_expr_to_where(
    stmt: &mut SelectStatement,
    ext_to_int_fields_map: &HashMap<Column, Expr>,
) {
    for jc in stmt.join.iter_mut() {
        if let JoinConstraint::On(on_expr) = &jc.constraint {
            let (nontrivial_constraints, remaining_expr) =
                split_joins_on_nontrivial_expr(on_expr, ext_to_int_fields_map);
            if !nontrivial_constraints.is_empty() {
                if let Some(expr) = conjoin_all_dedup(nontrivial_constraints) {
                    stmt.where_clause =
                        and_predicates_skip_true(mem::take(&mut stmt.where_clause), expr);
                }
                jc.constraint = remaining_expr.map_or(JoinConstraint::Empty, JoinConstraint::On);
            }
        }
    }
}

/// Returns `true` if it is safe to move all nontrivial ON-clause predicates to WHERE for
/// every join in `stmt`. Returns `false` (blocking inlining) if either condition holds:
/// - The non-nontrivial remainder of an ON clause itself still references inlinable columns —
///   the split would leave an unresolvable ON expression in place.
/// - A nontrivial predicate belongs to a non-inner (OUTER/LEFT) join — moving it to WHERE
///   would change null-extension semantics.
pub(crate) fn can_move_joins_on_nontrivial_expr_to_where(
    stmt: &SelectStatement,
    ext_to_int_fields_map: &HashMap<Column, Expr>,
) -> ReadySetResult<bool> {
    for jc in stmt.join.iter() {
        if let JoinConstraint::On(on_expr) = &jc.constraint {
            let (nontrivial_constraints, remaining_expr) =
                split_joins_on_nontrivial_expr(on_expr, ext_to_int_fields_map);
            if let Some(remaining_expr) = remaining_expr
                && contains_nontrivial_columns(&remaining_expr, ext_to_int_fields_map)
            {
                return Ok(false);
            }
            // If remaining_expr is None, the entire ON clause was nontrivial and the join will
            // get JoinConstraint::Empty after the move. Safe for inner joins (the reorder pass
            // reattaches predicates from WHERE), but blocked for outer joins below.
            if !nontrivial_constraints.is_empty() && !jc.operator.is_inner_join() {
                return Ok(false);
            }
        }
    }

    Ok(true)
}
/// Returns `true` if inlining `inl_stmt` into the RHS of a non-inner (e.g., LEFT) join is
/// semantically safe. All three conditions must hold:
///
/// 1. `inl_stmt` is a single-table subquery with no joins — multi-table RHS inlining would
///    expand the null-extended set unpredictably.
/// 2. `inl_stmt`'s WHERE clause (if any) contains only `SingleRelFilter` predicates pushable
///    into the JOIN ON clause — and the base join already has a non-trivial ON condition to
///    absorb them (an empty or `ON TRUE` base condition has nowhere to push filters into).
/// 3. Every column in the outermost expressions of `base_stmt` that maps through
///    `ext_to_int_fields` is null-preserving — prevents literal or computed projections from
///    silently dropping NULL rows that LEFT JOIN semantics requires to propagate.
pub(crate) fn can_inline_left_join_rhs_safe(
    base_stmt: &SelectStatement,
    base_jc: &JoinClause,
    inl_stmt: &SelectStatement,
    ext_to_int_fields: &HashMap<Column, Expr>,
) -> ReadySetResult<bool> {
    // Must be a single-table subquery with no joins
    if !is_single_from_item!(inl_stmt) {
        return Ok(false);
    }

    // WHERE clause must be either empty or a conjunction of single relation filters supported in join ON
    if let Some(where_expr) = &inl_stmt.where_clause {
        let remaining = split_expr(
            where_expr,
            &|e| matches!(classify_on_atom(e), OnAtom::SingleRelFilter { .. }),
            &mut vec![],
        );
        if remaining.is_some()
            || matches!(
                base_jc.constraint,
                JoinConstraint::Empty | JoinConstraint::On(Expr::Literal(Literal::Boolean(true)))
            )
        {
            return Ok(false);
        }
    }

    // Prevent RHS inlining if literal or not null-preserving projections are used in SELECT
    if !outermost_expression(base_stmt)
        .flat_map(columns_iter)
        .all(|c| ext_to_int_fields.get(c).is_none_or(is_expr_null_preserving))
    {
        return Ok(false);
    }

    Ok(true)
}

/// Thin wrapper over [`can_inline_subquery`] + [`inline_from_item_position_checks`]
/// for the `derived_tables_rewrite` inlining path.
///
/// Returns `Ok(None)` if the subquery cannot be inlined.
/// Returns `Ok(Some(downstream_group_by_additions))` if it can — the vec
/// contains additional GROUP BY keys needed when hoisting a grouped inner
/// into a non-grouped outer (passed through from `can_inline_subquery`).
///
/// DTR-specific pre-conditions checked here (before delegating to the shared
/// contract):
/// - FROM-less subquery reject (pre-condition for `build_ext_to_int_fields_map`).
/// - LATERAL bail (defensive; `lateral_join` clears the flag in the full
///   pipeline, but test-harness direct-DTR paths may bypass it).
/// - LIMIT unconditional reject (`hoist_parametrizable_filters` runs after
///   DTR and relies on aggregated+LIMIT subqueries surviving this pass).
///
/// `inl_from_item_ord_idx` is a flat ordinal over `base_stmt.tables ++ join_rhs_items`,
/// computed fresh before each inlining pass and never reused after mutation.
fn can_inline_from_item(
    base_stmt: &SelectStatement,
    inl_from_item: &TableExpr,
    inl_from_item_ord_idx: usize,
    is_top_select: bool,
) -> ReadySetResult<Option<Vec<Expr>>> {
    let Some((inl_stmt, inl_stmt_alias)) = as_sub_query_with_alias(inl_from_item) else {
        return Ok(None);
    };

    // FROM-less subquery reject — can't splice nothing.  Kept here as a
    // pre-condition for `build_ext_to_int_fields_map` to succeed.
    if inl_stmt.tables.is_empty() {
        return Ok(None);
    }

    // Reject LATERAL subqueries.  LATERAL bodies have per-outer-row
    // semantics (LIMIT applies per outer row, not globally).  In the full
    // pipeline, `lateral_join` runs before `derived_tables_rewrite` and
    // handles correlation extraction and TOP-K rewriting, clearing the
    // `lateral` flag.  This defensive bail protects the test harness and
    // any path that bypasses `lateral_join`.
    if inl_stmt.lateral {
        return Ok(None);
    }

    let ext_to_int = match build_ext_to_int_fields_map(inl_stmt, inl_stmt_alias.clone()) {
        Ok(map) => map,
        Err(_) => return Ok(None),
    };

    let (ds_tables, ds_joins) = compute_downstream_for_position(base_stmt, inl_from_item_ord_idx);

    let ctx = crate::inline_subquery::InliningContext::<
        crate::drop_redundant_join::UniqueColumnsSchemaImpl,
    > {
        inner_stmt: inl_stmt,
        outer_stmt: base_stmt,
        inner_alias: &inl_stmt_alias,
        ext_to_int: &ext_to_int,
        inl_from_item_ord_idx,
        downstream_tables: ds_tables,
        downstream_joins: ds_joins,
        is_top_select,
        // DTR runs after `unnest_subqueries`; no subquery predicates remain
        // in expression positions, so `can_inline_subquery`'s
        // `check_unnesting_guards` cannot help and may false-reject.  Opt out.
        skip_unnesting_guard: true,
        inner_rel: inl_stmt_alias.clone().into(),
        is_inner_agg: is_aggregation_or_grouped(inl_stmt)?,
        is_outer_agg: is_aggregation_or_grouped(base_stmt)?,
        pre_hoist_lateral_exactly_one: None,
        pre_hoist_lateral_at_most_one: None,
        preceding_flattened_lateral_aliases: None,
        unique_cols_schema: None,
    };

    let Some(downstream_group_by_additions) = can_inline_subquery(&ctx)? else {
        return Ok(None);
    };

    if !inline_from_item_position_checks(base_stmt, inl_stmt, inl_from_item_ord_idx, &ext_to_int)? {
        return Ok(None);
    }

    Ok(Some(downstream_group_by_additions))
}

/// Applies WHERE-to-ON filter migration and normalizes join constraints
/// to enforce ReadySet's "two-table ON" invariant.
fn migrate_and_normalize_joins(stmt: &mut SelectStatement) -> ReadySetResult<bool> {
    let mut did_rewrite = false;

    // Move any shape and placement suited constraints from WHERE to appropriate joins ON
    if move_applicable_where_conditions_to_joins(stmt)? {
        did_rewrite = true;
    }

    // Now, normalize the joins w.r.t. to the supported semantics, move the unsupported
    // conjuncts back to WHERE, if possible.
    if try_normalize_joins_conditions(stmt)? {
        did_rewrite = true;
    }

    // Re-attempt WHERE → ON: normalization may have pushed some ON predicates back to WHERE,
    // potentially unlocking new migrations that were not possible in the first pass.
    if move_applicable_where_conditions_to_joins(stmt)? {
        // Normalize again to canonicalize anything newly migrated to ON.
        try_normalize_joins_conditions(stmt)?;
        did_rewrite = true;
    }

    Ok(did_rewrite)
}

/// Inlines a subquery `FROM` item into the base query.
///
/// This function replaces a derived table with its internal structure (FROM items and joins),
/// preserving query semantics whether the item is part of the initial FROM clause (LHS)
/// or the right-hand side of a JOIN (RHS).
///
/// # LHS vs RHS in SQL Join Structures
///
/// - **LHS (Left-hand side)**: Comma-separated FROM items (`base_stmt.tables`). These are
///   cross-joined by default unless explicitly joined later.
/// - **RHS (Right-hand side)**: FROM items that appear inside a JOIN's right operand. These
///   are explicitly joined and carry join semantics (e.g., INNER, LEFT OUTER).
///
/// This distinction matters because RHS inlining must **preserve null-extending behavior**
/// of OUTER joins, while LHS inlining must **maintain join topology** and join constraints.
///
/// # Index Semantics
///
/// The `inl_from_item_ord_idx` identifies the inlinable item’s position **before mutation**:
/// - If `< base_stmt.tables.len()`: refers to LHS
/// - Otherwise: refers to RHS, resolvable via `find_rhs_join_clause`
///
/// **Why this is safe**:
/// 1. Inlining is performed one item per rewrite pass.
/// 2. AST traversal and index computation occur fresh before each pass.
/// 3. No reuse of stale indexes after mutation.
///
/// Thus, the index remains valid through the rewrite, and a newtype is not strictly necessary
/// (though it would help self-document intent).
/// `is_inl_aggregated` and `is_base_aggregated` are pre-computed from the
/// **pre-substitution** state of the inlinable and base statements.  They are
/// passed in rather than recomputed here because `replace_columns_with_inlinable_expr`
/// runs before this function (to avoid alias-collision bugs during WHERE/HAVING
/// absorption), and substitution can change the aggregation classification of the base.
/// Splice the inlinable subquery's tables and joins into `base_stmt` at LHS-position.
/// LHS-position means `inl_from_item_ord_idx < base_stmt.tables.len()`: the inlinable
/// is part of the comma-separated FROM list, not on the RHS of a JOIN.
///
/// After this returns, `inl_stmt.tables` and `inl_stmt.join` are emptied and the
/// caller can pass the (now-table-and-join-less) `inl_stmt` to `apply_inline` for
/// the post-splice transformation.
fn splice_lhs_in_place(
    base_stmt: &mut SelectStatement,
    inl_stmt: &mut SelectStatement,
    inl_from_item_ord_idx: usize,
) {
    let inl_tables = mem::take(&mut inl_stmt.tables);
    if inl_stmt.join.is_empty() {
        // Splice inlined subquery's FROM items directly into base query tables.
        base_stmt
            .tables
            .splice(inl_from_item_ord_idx..=inl_from_item_ord_idx, inl_tables);
    } else {
        // The subquery contains both FROM items and JOINs; lift both into base query.
        base_stmt.tables.remove(inl_from_item_ord_idx);
        base_stmt.tables.extend(inl_tables);
        let inl_joins = mem::take(&mut inl_stmt.join);
        base_stmt.join.splice(0..0, inl_joins);
    }
}

fn inline_rhs_in_place(
    base_stmt: &mut SelectStatement,
    mut inl_from_item: TableExpr,
    inl_from_item_ord_idx: usize,
    is_inl_aggregated: bool,
    is_base_aggregated: bool,
    downstream_group_by_additions: Vec<Expr>,
) -> ReadySetResult<()> {
    // Get the inlinable FROM item's statement
    let (inl_stmt, inl_alias) = expect_sub_query_with_alias_mut(&mut inl_from_item);
    let inl_alias = inl_alias.clone();
    let mut inl_stmt = mem::take(inl_stmt);

    // Normalize HAVING-without-aggregates-or-GROUP-BY to WHERE. When the inner
    // is not aggregated, HAVING is semantically equivalent to WHERE. Fuse it
    // into inner WHERE so downstream branches treat it uniformly (the non-
    // aggregated branch below only migrates WHERE).
    if !is_inl_aggregated && let Some(having_as_where) = inl_stmt.having.take() {
        inl_stmt.where_clause =
            and_predicates_skip_true(inl_stmt.where_clause.take(), having_as_where);
    }

    // RHS inlining — the FROM item is inside a JOIN clause (JoinRightSide of some JoinClause).

    // Collect the inlinable join structure.
    let mut inl_joins = Vec::new();

    // Locate the join clause and position inside the JoinRightSide where this FROM item lives.
    // This lookup is based on flat ordinal index captured before inlining mutations.
    let Some((jc_idx, inl_from_item_pos)) = find_rhs_join_clause(base_stmt, inl_from_item_ord_idx)
    else {
        internal!(
            "Inlinable FROM item {} not found in join RHS at ordinal {}",
            inl_from_item.display(Dialect::PostgreSQL),
            inl_from_item_ord_idx
        )
    };

    // Inline the inlinable FROM item's tables (the former LHS inside the inlinable) into the base RHS.
    let mut inl_tables = mem::take(&mut inl_stmt.tables);
    if let Some(replace_jc_right_with) = match &mut base_stmt.join[jc_idx].right {
        JoinRightSide::Table(table) => {
            // If the inlined subquery had only one FROM item, we can directly replace the slot.
            if inl_tables.len() == 1 {
                *table = inl_tables.pop().expect("inl_tables verified non-empty");
                None
            } else {
                Some(JoinRightSide::Tables(inl_tables))
            }
        }
        JoinRightSide::Tables(tables) => {
            // If the inlined subquery had only one FROM item, we can directly replace the slot.
            if inl_tables.len() == 1 {
                Some(JoinRightSide::Table(
                    inl_tables.pop().expect("inl_tables verified non-empty"),
                ))
            } else {
                tables.splice(inl_from_item_pos..=inl_from_item_pos, inl_tables);
                None
            }
        }
    } {
        base_stmt.join[jc_idx].right = replace_jc_right_with;
    }

    // If after inlining, the base RHS becomes or remains a `JoinRightSide::Tables` structure,
    // turn it into a chain of `JoinRightSide::Table` cross joined relations,
    // preserving the 1st relation as the current RHS.
    if let Some(new_rhs) = match &mut base_stmt.join[jc_idx].right {
        JoinRightSide::Table(_) => {
            // The post inlining base RHS is already a single relation construct.
            None
        }
        JoinRightSide::Tables(tables) => {
            // Convert the nested Tables structure into a flat chain of cross-joined Table entries.
            let mut rhs_tables_iter = mem::take(tables).into_iter();
            // SAFETY: `JoinRightSide::Tables` invariant guarantees at least one element.
            let first = rhs_tables_iter.next().expect("Tables variant is non-empty");
            for tbl in rhs_tables_iter {
                inl_joins.push(JoinClause {
                    operator: JoinOperator::InnerJoin,
                    right: JoinRightSide::Table(tbl),
                    constraint: JoinConstraint::Empty,
                });
            }
            Some(first)
        }
    } {
        base_stmt.join[jc_idx].right = JoinRightSide::Table(new_rhs);
    }

    // Add the inlinable join structure after the synthetic cross-joins
    inl_joins.extend(mem::take(&mut inl_stmt.join));

    // Splice in inlinable join structure right after its position
    if !inl_joins.is_empty() {
        base_stmt.join.splice(jc_idx + 1..jc_idx + 1, inl_joins);
    }

    // Aggregated subquery: lift GROUP BY, HAVING, DISTINCT, and ORDER BY
    // from the inlined subquery into the base.
    //
    // The base's existing WHERE is partitioned per-conjunct: conjuncts that
    // reference only the inlinable's alias (lhs) migrate to HAVING because
    // they now apply post-aggregation (the inner's GROUP BY is the outer's).
    // Conjuncts that reference any other base relation stay in WHERE — they
    // remain pre-aggregation filters on rows feeding the joined cross-
    // product, and the GROUP BY lift doesn't change their semantics.
    //
    // The inner's own WHERE is then conjoined into the kept base WHERE
    // (not used to replace it), so neither side's filters are lost.
    //
    // `downstream_group_by_additions` carries bare-Column refs to non-
    // inlinable rels that appeared in post-aggregation positions of the
    // outer (SELECT/HAVING/ORDER BY) and need to become outer GROUP BY
    // keys for the rewritten query to be valid.  The cardinality gate
    // ensures these additions are functionally dependent on the inner's
    // GROUP BY keys, so they don't split groups — see the design memo
    // §5.4 for the composition argument.
    if is_inl_aggregated {
        base_stmt.distinct = inl_stmt.distinct;
        base_stmt.having = mem::take(&mut inl_stmt.having);

        let inl_rel: Relation = inl_alias.into();
        let base_other_rels = visible_base_rels_except(base_stmt, &inl_rel)?;
        let mut kept_where: Option<Expr> = None;
        let mut move_to_having: Vec<Expr> = Vec::new();
        if let Some(base_where_expr) = base_stmt.where_clause.take() {
            let mut conjuncts = Vec::new();
            let _ = split_expr(&base_where_expr, &|_| true, &mut conjuncts);
            for e in conjuncts {
                let refs_inl = refs_rel_anywhere(&e, &inl_rel)?;
                let refs_other = refs_any_of_rels_anywhere(&e, &base_other_rels)?;
                if refs_inl && !refs_other {
                    move_to_having.push(e);
                } else {
                    kept_where = and_predicates_skip_true(kept_where, e);
                }
            }
        }
        for e in move_to_having {
            base_stmt.having = and_predicates_skip_true(base_stmt.having.take(), e);
        }
        if let Some(inl_where) = mem::take(&mut inl_stmt.where_clause) {
            kept_where = and_predicates_skip_true(kept_where, inl_where);
        }
        base_stmt.where_clause = kept_where;

        // Resolve numeric/alias GROUP BY references against the inlined
        // subquery's own SELECT fields before transferring to the outer
        // statement, whose field list may differ in length or order.
        // `resolved_gb` is then reused as the dedup tracker for
        // `downstream_group_by_additions`: each accepted addition is
        // pushed back into it, so subsequent additions can dedup
        // against both the original inner-GB keys AND previously-
        // pushed additions in one membership check.
        let mut resolved_gb = resolve_group_by_exprs(&inl_stmt)?;
        base_stmt.group_by = if resolved_gb.is_empty() {
            None
        } else {
            Some(GroupByClause {
                fields: resolved_gb
                    .iter()
                    .cloned()
                    .map(FieldReference::Expr)
                    .collect(),
            })
        };
        if !downstream_group_by_additions.is_empty() {
            let gb = base_stmt.group_by.get_or_insert_default();
            for e in downstream_group_by_additions {
                if resolved_gb.contains(&e) {
                    continue;
                }
                gb.fields.push(FieldReference::Expr(e.clone()));
                resolved_gb.push(e);
            }
        }
    } else if let Some(inl_where_expr) = mem::take(&mut inl_stmt.where_clause) {
        if inl_from_item_ord_idx >= base_stmt.tables.len() {
            // For RHS: incorporate inlined WHERE clause into ON condition.
            let (jc_idx, _) = find_rhs_join_clause(base_stmt, inl_from_item_ord_idx)
                .ok_or_else(|| internal_err!("Inlined item not found in JOINs"))?;
            let existing_constraint = mem::replace(
                &mut base_stmt.join[jc_idx].constraint,
                JoinConstraint::Empty,
            );
            base_stmt.join[jc_idx].constraint =
                add_expression_to_join_constraint(existing_constraint, inl_where_expr);
        } else {
            // For LHS: incorporate inlined WHERE clause into base WHERE.
            base_stmt.where_clause =
                and_predicates_skip_true(mem::take(&mut base_stmt.where_clause), inl_where_expr);
        }
    }

    // Resolve numeric and alias ORDER BY references against the inlined
    // subquery's own SELECT fields before transferring (same rationale as
    // GROUP BY above).  Done unconditionally so the references are valid
    // whether the order ends up carried up below or dropped silently.
    if let Some(order) = &mut inl_stmt.order {
        for ord_by in &mut order.order_by {
            ord_by.field =
                FieldReference::Expr(resolve_field_reference(&inl_stmt.fields, &ord_by.field)?);
        }
    }

    if !inl_stmt.limit_clause.is_empty() {
        // Inner has LIMIT — carry both ORDER and LIMIT up to base via the
        // shared `apply_inline` helper.  The gate chain in
        // `can_inline_from_item` already verified outer ORDER is None or
        // equivalent under projection (`check_order_limit_safety`), both
        // LIMITs are numeric (`check_limit_composition`), and other FROM
        // items are cardinality-preserving
        // (`check_join_partners_cardinality_preserving`), so the carry-up
        // and LIMIT composition are sound.
        carry_inner_limit_and_order(base_stmt, &mut inl_stmt)?;
    } else if is_single_from_item!(base_stmt)
        && !is_base_aggregated
        && base_stmt.order.is_none()
        && base_stmt.limit_clause.is_empty()
    {
        // Inner has ORDER but no LIMIT.  ORDER BY without LIMIT is a no-op
        // in intermediate subqueries (only the outermost ORDER BY determines
        // result order), so this adoption is presentation-only.  Narrow
        // conditions match prior behavior: base must be single-FROM, not
        // aggregated, with no own ORDER or LIMIT.
        base_stmt.order = mem::take(&mut inl_stmt.order);
    }

    Ok(())
}

/// Inlines the FROM item at `inl_from_item_ord_idx` into `base_stmt`.
///
/// Dispatches by FROM-item position:
/// - **LHS** (`inl_from_item_ord_idx < base_stmt.tables.len()`): splices tables/joins via
///   [`splice_lhs_in_place`], then delegates post-splice transformation to [`apply_inline`].
/// - **RHS** (JOIN right-hand side): substitutes columns first, then splices and transforms
///   via [`inline_rhs_in_place`].
///
/// In both paths, nontrivial ON predicates referencing the inlinable are hoisted to WHERE
/// before any structural mutation, and joins are normalized after inlining completes.
fn inline_from_item(
    base_stmt: &mut SelectStatement,
    inl_from_item_ord_idx: usize,
    is_top_select: bool,
    downstream_group_by_additions: Vec<Expr>,
) -> ReadySetResult<()> {
    let Some(inl_from_item) = get_local_from_items_iter_mut!(base_stmt).nth(inl_from_item_ord_idx)
    else {
        internal!(
            "FROM item at ordinal {} not found in statement",
            inl_from_item_ord_idx
        )
    };
    let mut inl_from_item = mem::replace(
        inl_from_item,
        TableExpr {
            inner: TableExprInner::Table("to_be_inlined".into()),
            alias: inl_from_item.alias.clone(),
            column_aliases: vec![],
        },
    );

    // Dedupe inner aliases against the base.
    {
        let (inl_stmt, inl_alias) = expect_sub_query_with_alias_mut(&mut inl_from_item);
        let inl_alias = inl_alias.clone();
        make_aliases_distinct_from_base_statement(
            base_stmt,
            &inl_alias,
            inl_stmt,
            &HashSet::new(),
        )?;
    }

    // Build the InlineCandidate (also computes ext_to_int as a side effect).
    let mut candidate = crate::inline_subquery::prepare_inline(inl_from_item)?;
    let is_inl_aggregated = is_aggregation_or_grouped(&candidate.stmt)?;
    let is_base_aggregated = is_aggregation_or_grouped(base_stmt)?;

    // Hoist nontrivial-ON predicates that reference the inlinable into WHERE.
    move_joins_on_nontrivial_expr_to_where(base_stmt, &candidate.ext_to_int);

    // Dispatch by FROM-item position.
    if inl_from_item_ord_idx < base_stmt.tables.len() {
        // LHS path: splice tables/joins into base, then apply_inline (which does
        // WHERE-migration, substitute, GROUP-BY/HAVING merge, ORDER-BY carry-up, LIMIT
        // composition — all the post-splice transformations).
        splice_lhs_in_place(base_stmt, &mut candidate.stmt, inl_from_item_ord_idx);
        crate::inline_subquery::apply_inline(
            base_stmt,
            candidate,
            is_top_select,
            downstream_group_by_additions,
        )?;
    } else {
        // RHS path: substitute first (inline_rhs_in_place doesn't), then RHS splice
        // + bespoke RHS transformation in inline_rhs_in_place.
        let inl_rel: Relation = candidate.alias.clone().into();
        crate::inline_subquery::replace_columns_with_inlinable_expr(
            base_stmt,
            &inl_rel,
            &candidate.ext_to_int,
            is_top_select,
        )?;
        let inl_from_item = TableExpr {
            inner: TableExprInner::Subquery(Box::new(candidate.stmt)),
            alias: Some(candidate.alias),
            column_aliases: vec![],
        };
        inline_rhs_in_place(
            base_stmt,
            inl_from_item,
            inl_from_item_ord_idx,
            is_inl_aggregated,
            is_base_aggregated,
            downstream_group_by_additions,
        )?;
    }

    // Post-pass: normalize joins.
    migrate_and_normalize_joins(base_stmt)?;
    Ok(())
}

/// Repeatedly inline all eligible FROM items in the statement until stabilization.
fn try_inline_from_items(
    base_stmt: &mut SelectStatement,
    is_top_select: bool,
) -> ReadySetResult<bool> {
    let mut inlined_count = 0;
    loop {
        // === Stable index computation ===
        // We walk the current FROM items to compute the ordinal index of a safely inlinable item.
        // This index is used *once* per rewrite pass, and never reused after mutation.
        // Safe due to:
        // 1. One inlining per pass
        // 2. Full re-walk after each pass
        // 3. No reuse across mutations
        let mut inl_from_item_idx_and_additions: Option<(usize, Vec<Expr>)> = None;
        for (from_item_idx, from_item) in get_local_from_items_iter!(base_stmt).enumerate() {
            if let Some(additions) =
                can_inline_from_item(base_stmt, from_item, from_item_idx, is_top_select)?
            {
                inl_from_item_idx_and_additions = Some((from_item_idx, additions));
                break;
            }
        }
        if let Some((inl_from_item_idx, additions)) = inl_from_item_idx_and_additions {
            inline_from_item(base_stmt, inl_from_item_idx, is_top_select, additions)?;
            inlined_count += 1;
        } else {
            // We have no inlinable candidates.
            break;
        }
    }

    Ok(inlined_count > 0)
}

/// Inlines FROM subqueries bottom-up (depth-first): recurses into each direct subquery first,
/// then attempts inlining at the current level via [`try_inline_from_items`]. Bottom-up
/// ordering ensures inner subqueries are already flattened before the outer level tries to
/// inline them, maximising the candidates visible at each level.
fn derived_tables_rewrite_impl(
    stmt: &mut SelectStatement,
    is_top_select: bool,
) -> ReadySetResult<bool> {
    let mut rewritten = false;
    // Recurse into each subquery
    for from_item in get_local_from_items_iter_mut!(stmt) {
        if let Some((from_stmt, _)) = as_sub_query_with_alias_mut(from_item)
            && derived_tables_rewrite_impl(from_stmt, false)?
        {
            rewritten = true;
        }
    }
    // Try inlining at this level — but only if the current statement
    // has no residual subquery expressions in SELECT/WHERE/HAVING/etc.
    // Column substitution during inlining doesn't enter expression
    // subqueries (visitor stops at SelectStatement boundaries), so a
    // correlated reference inside an expression subquery that uses the
    // alias of a FROM item being inlined would become dangling.
    //
    // This is a per-level check: residual subqueries deep inside a
    // non-inlined FROM item do not block inlining at this level.
    if !contains_subquery_predicates(stmt)? {
        rewritten |= try_inline_from_items(stmt, is_top_select)?;
    }
    Ok(rewritten)
}

/// Recursively promotes LEFT JOINs to INNER JOINs where null-rejection is provable.
///
/// A LEFT JOIN can be promoted when every RHS relation has at least one column that is both:
/// - referenced by a null-rejecting predicate (WHERE, HAVING, or a later INNER JOIN ON), and
/// - proven non-NULL by [`derive_from_stmt`].
///
/// The pass iterates to a fixed point at each query level (one promotion may enable another),
/// then recurses into FROM subqueries. Each level is analyzed independently — no non-null
/// information is inherited from outer scopes, as subqueries have independent null contexts.
///
/// Returns `true` if any join was promoted.
pub(crate) fn normalize_null_rejecting_outer_joins(
    stmt: &mut SelectStatement,
) -> ReadySetResult<bool> {
    // Stub schema used with derive_from_stmt: no columns are statically known non-null from
    // DDL at this stage; nullability is derived purely from the query structure.
    struct EmptySchema;

    impl NonNullSchema for EmptySchema {
        fn not_null_columns_of(&self, _rel: &Relation) -> HashSet<Column> {
            HashSet::new()
        }
    }

    let mut did_rewrite = false;

    loop {
        // Columns that null-reject based on the INNER JOINs, WHERE and HAVING
        let proven_non_null = derive_from_stmt(stmt, &EmptySchema)?;

        // Collect indexes of eligible LEFT JOINs to promote to INNER JOINs (per-join predicate gate)
        let mut jc_idx_to_promote = Vec::new();
        for (i, join) in stmt.join.iter().enumerate() {
            if join.operator.is_inner_join() {
                continue;
            }

            let rhs_rels = get_join_rhs_relations(join);

            // Columns that might null-reject at this level (WHERE + later INNER ONs)
            let mut pred_cols = HashSet::new();
            if let Some(w) = &stmt.where_clause {
                for c in columns_iter(w) {
                    pred_cols.insert(c.clone());
                }
            }
            if let Some(h) = &stmt.having {
                for c in columns_iter(h) {
                    pred_cols.insert(c.clone());
                }
            }
            for jc in stmt.join.iter().skip(i + 1) {
                if jc.operator.is_inner_join()
                    && let JoinConstraint::On(on) = &jc.constraint
                {
                    for c in columns_iter(on) {
                        pred_cols.insert(c.clone());
                    }
                }
            }

            // Prove *each* RHS relation present only if one of its columns
            // is both used by null-rejecting predicates and is proven non-NULL.
            let rhs_proven_present = !rhs_rels.is_empty()
                && rhs_rels.iter().all(|rel| {
                    pred_cols
                        .iter()
                        .any(|c| c.table.as_ref() == Some(rel) && proven_non_null.contains(c))
                });

            if rhs_proven_present {
                jc_idx_to_promote.push(i);
            }
        }

        // No eligible LEFT JOINs found
        if jc_idx_to_promote.is_empty() {
            break;
        }

        // Promote eligible LEFT JOINs to INNER JOINs
        for i in jc_idx_to_promote {
            stmt.join[i].operator = InnerJoin;
        }

        did_rewrite = true;
    }

    // Recurse into subqueries with a fresh (empty) context
    for from_item in get_local_from_items_iter_mut!(stmt) {
        if let Some((sub_stmt, _)) = as_sub_query_with_alias_mut(from_item)
            && normalize_null_rejecting_outer_joins(sub_stmt)?
        {
            did_rewrite = true;
        }
    }

    Ok(did_rewrite)
}

/// Returns `true` if `stmt` or any of its nested FROM subqueries contains a subquery predicate
/// (a `SELECT` appearing in WHERE, HAVING, or the SELECT list — not in the FROM clause).
///
/// Used as a pre-flight guard in [`derived_tables_rewrite_main`]: if subquery predicates exist,
/// `unnest_subqueries` has not yet run and inlining passes are skipped.
///
/// The visitor is run after temporarily removing `stmt.tables` and `stmt.join` to prevent
/// descending into FROM-level subqueries (which are handled by the explicit recursive loop
/// over `get_local_from_items_iter_mut!`). This keeps FROM subqueries and predicate subqueries
/// strictly separate.
/// Returns `true` if `stmt` has subquery expressions in its own expression
/// positions (SELECT, WHERE, HAVING, JOIN ON, GROUP BY, ORDER BY, LIMIT, CTEs)
/// — but does NOT recurse into FROM-item subqueries.  This is a per-level
/// check: residual subqueries deep inside a non-inlined FROM item do not
/// block inlining at the current level.
fn contains_subquery_predicates(stmt: &SelectStatement) -> ReadySetResult<bool> {
    struct Vis {
        select_found: bool,
    }

    impl<'a> Visitor<'a> for Vis {
        type Error = ReadySetError;
        fn visit_select_statement(&mut self, _: &'a SelectStatement) -> Result<(), Self::Error> {
            self.select_found = true;
            Ok(())
        }
    }

    let mut vis = Vis {
        select_found: false,
    };

    for cte in &stmt.ctes {
        vis.visit_common_table_expr(cte)?;
    }
    for field in &stmt.fields {
        vis.visit_field_definition_expr(field)?;
    }
    // Defense-in-depth: also check JOIN ON constraints.  `validate_query_semantics`
    // rejects subqueries in JOIN ON before Block B (§13), so this should never fire
    // in the adapter path — but covers the server path (§14.1) which may bypass the
    // semantic validator.
    for jc in &stmt.join {
        vis.visit_join_constraint(&jc.constraint)?;
    }
    if let Some(where_clause) = &stmt.where_clause {
        vis.visit_where_clause(where_clause)?;
    }
    if let Some(having_clause) = &stmt.having {
        vis.visit_having_clause(having_clause)?;
    }
    if let Some(group_by_clause) = &stmt.group_by {
        vis.visit_group_by_clause(group_by_clause)?;
    }
    if let Some(order_clause) = &stmt.order {
        vis.visit_order_clause(order_clause)?;
    }
    vis.visit_limit_clause(&stmt.limit_clause)?;

    Ok(vis.select_found)
}

/// Strip redundant DISTINCT from FROM-item subqueries that produce at most
/// one row.  `agg_only_no_gby_cardinality` classifies aggregate-only-no-GROUP-BY
/// shapes (including wrapper pass-throughs) as ExactlyOne, AtMostOne, or
/// ExactlyZero.  DISTINCT on ≤1 row can never eliminate anything, so stripping
/// it is a semantic no-op that produces cleaner ASTs and simplifies downstream
/// inlining logic.
fn strip_redundant_distinct(stmt: &mut SelectStatement) -> ReadySetResult<bool> {
    let mut stripped = false;
    for te in get_local_from_items_iter_mut!(stmt) {
        if let Some((sub_stmt, _)) = as_sub_query_with_alias_mut(te)
            && sub_stmt.distinct
            && agg_only_no_gby_cardinality(sub_stmt)?.is_some()
        {
            sub_stmt.distinct = false;
            stripped = true;
        }
    }
    Ok(stripped)
}

/// Top-level entry point for derived-table rewriting.  Applies all
/// normalization passes in order:
///
/// 1. **Guard** — bail early if any subquery predicate remains unnested (safety pre-check).
/// 2. **Redundant DISTINCT strip** — remove DISTINCT from ≤1-row subqueries.
/// 3. **Comma normalization** — convert comma-separated FROM items to CROSS JOINs.
/// 4. **NULL promotion** — promote LEFT JOINs to INNER JOINs where null-rejection is provable.
/// 5. **Derived table inlining** — recursively flatten FROM subqueries into the base query.
/// 6. **Join shape normalization** — canonicalize join order and operator forms.
/// 7. **GROUP BY / HAVING normalization** — resolve aliases, canonicalize HAVING expressions.
/// 8. **DISTINCT fix** — convert `GROUP BY` without aggregates to `SELECT DISTINCT`.
///
/// Returns `true` if any rewrite was applied.
pub(crate) fn derived_tables_rewrite_main(
    stmt: &mut SelectStatement,
    dialect: Dialect,
) -> ReadySetResult<bool> {
    let mut rewritten = false;

    // NOTE: no global bail-out for residual expression subqueries.  The
    // subquery guard is per-level inside derived_tables_rewrite_impl —
    // only the inlining step is skipped at levels that have expression
    // subqueries.  All other passes (DISTINCT strip, comma normalization,
    // NULL promotion, join normalization, GROUP BY/HAVING normalization,
    // DISTINCT fix) are safe with residual subqueries because their
    // visitors stop at SelectStatement boundaries and predicate movement
    // only affects col=col equalities.

    rewritten |= strip_redundant_distinct(stmt)?;

    // Normalize comma-separated FROM items into explicit CROSS JOINs before
    // inlining, so can_inline_from_item sees a uniform structure (single
    // tables[0] + explicit joins).  The same normalization runs again inside
    // normalize_joins_shape after inlining to catch comma-separated items
    // introduced by inline_from_item_in_place.
    rewritten |= normalize_comma_separated_lhs(stmt)?;

    // Promote LEFT -> INNER where safe
    if normalize_null_rejecting_outer_joins(stmt)? {
        rewritten = true;
    }

    // Flatten derived tables
    rewritten |= derived_tables_rewrite_impl(stmt, true)?;

    // Structural join reordering (canonicalization)
    rewritten |= normalize_joins_shape(stmt, dialect)?;

    // Normalize GROUP BY, HAVING and ORDER BY
    rewritten |= normalize_having_and_group_by(stmt)?;

    // Fix applicable GROUP BY w/o aggregates to DISTINCT
    rewritten |= fix_groupby_without_aggregates(stmt)?;

    Ok(rewritten)
}
