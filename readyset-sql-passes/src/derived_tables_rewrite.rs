use crate::infer_nullability::{derive_from_stmt, is_expr_null_preserving};
use crate::rewrite_joins::{
    get_join_rhs_relations, move_applicable_where_conditions_to_joins,
    normalize_comma_separated_lhs, normalize_joins_shape, try_normalize_joins_conditions,
};
use crate::rewrite_utils::{
    OnAtom, add_expression_to_join_constraint, and_predicates_skip_true, as_sub_query_with_alias,
    as_sub_query_with_alias_mut, build_ext_to_int_fields_map, classify_on_atom,
    collect_columns_in_expr_mut, collect_outermost_columns_mut, columns_iter, conjoin_all_dedup,
    contains_select, default_alias_for_select_item_expression, expect_field_as_expr_mut,
    expect_sub_query_with_alias, expect_sub_query_with_alias_mut, find_rhs_join_clause,
    fix_groupby_without_aggregates, for_each_window_function, is_aggregated_expr,
    is_aggregation_or_grouped, make_aliases_distinct_from_base_statement,
    normalize_having_and_group_by, outermost_expression, resolve_field_reference,
    resolve_group_by_exprs, split_expr,
};
use crate::unnest_subqueries::{
    NonNullSchema, agg_only_no_gby_cardinality, is_supported_join_condition,
};
use crate::{
    get_local_from_items_iter, get_local_from_items_iter_mut, is_single_from_item,
    is_window_function_expr,
};
use itertools::Either;
use readyset_errors::{ReadySetError, ReadySetResult, internal, internal_err, invariant};
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
fn can_move_joins_on_nontrivial_expr_to_where(
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
/// Substitutes columns in `expr` using `ext_to_int_fields`, returning the rewritten expression.
///
/// Walks the expression tree directly via [`collect_columns_in_expr_mut`], replacing each
/// `Expr::Column` that appears in `ext_to_int_fields` with its mapped expression.
fn substitute_columns_in_expr(
    expr: &Expr,
    ext_to_int_fields: &HashMap<Column, Expr>,
    _is_top_select: bool,
) -> ReadySetResult<Expr> {
    let mut result = expr.clone();
    for col_expr in collect_columns_in_expr_mut(&mut result) {
        if let Expr::Column(col) = col_expr
            && let Some(inl_expr) = ext_to_int_fields.get(col)
        {
            *col_expr = inl_expr.clone();
        }
    }
    Ok(result)
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
fn can_inline_left_join_rhs_safe(
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

/// Returns `true` if the FROM item at `inl_from_item_ord_idx` in `base_stmt` can be safely
/// inlined (flattened into `base_stmt`). Rejects inlining when any of the following hold:
///
/// - The subquery has a LIMIT clause (TopK subqueries cannot be flattened).
/// - The subquery is aggregated and the base is also aggregated or multi-table.
/// - The subquery projects a window function column referenced in the outer query.
/// - The base query has a window function referencing an aggregated inlinable column.
/// - RHS inlining into a non-inner join is unsafe (delegates to [`can_inline_left_join_rhs_safe`]).
/// - LHS inlining of a complex subquery (multi-table or with WHERE) when the first base join
///   is not INNER.
/// - Post-substitution join ON shapes are unsupported and the join is not INNER.
/// - Nontrivial ON-clause predicates cannot be safely lifted to WHERE after inlining.
///
/// `inl_from_item_ord_idx` is a flat ordinal over `base_stmt.tables ++ join_rhs_items`,
/// computed fresh before each inlining pass and never reused after mutation.
fn can_inline_from_item(
    base_stmt: &SelectStatement,
    inl_from_item: &TableExpr,
    inl_from_item_ord_idx: usize,
    is_top_select: bool,
) -> ReadySetResult<bool> {
    let Some((inl_stmt, inl_stmt_alias)) = as_sub_query_with_alias(inl_from_item) else {
        return Ok(false);
    };

    // === Reject FROM-less subqueries (e.g., SELECT 1 AS alias)
    // These have no tables to splice into the base join structure.
    if inl_stmt.tables.is_empty() {
        return Ok(false);
    }

    // === Reject LIMIT (TopK) subqueries
    if !inl_stmt.limit_clause.is_empty() {
        return Ok(false);
    }

    // === Aggregates allowed only if base is single, non-aggregated FROM
    if is_aggregation_or_grouped(inl_stmt)?
        && (is_aggregation_or_grouped(base_stmt)?
            || !base_stmt.join.is_empty()
            || base_stmt.tables.len() > 1)
    {
        return Ok(false);
    }

    // === Reject if inlining would introduce a self-join (same base table twice)
    if crate::util::would_create_self_join(base_stmt, inl_stmt, inl_from_item_ord_idx) {
        return Ok(false);
    }

    let inl_stmt_rel: Relation = inl_stmt_alias.clone().into();
    // Duplicate projected names make the ext_to_int mapping ambiguous — skip inlining
    // and let the subquery survive as a derived table.  The semantic validator handles
    // the user-facing error if the ambiguous name is externally referenced.
    let ext_to_int_fields = match build_ext_to_int_fields_map(inl_stmt, inl_stmt_alias) {
        Ok(map) => map,
        Err(_) => return Ok(false),
    };

    // === Reject inlining if WF-projected field is referenced in base
    let base_columns_for_alias: HashSet<Column> = outermost_expression(base_stmt)
        .flat_map(columns_iter)
        .filter_map(|c| {
            if c.table.as_ref() == Some(&inl_stmt_rel) {
                Some(c.clone())
            } else {
                None
            }
        })
        .collect();

    for (col, expr) in &ext_to_int_fields {
        if base_columns_for_alias.contains(col) && is_window_function_expr!(expr) {
            return Ok(false);
        }
    }

    // === Reject inlining a grouped subquery when unreferenced GROUP BY
    // keys would produce an invalid "GROUP BY without aggregates" shape.
    // After inlining, the GROUP BY is spliced into the outer statement.
    // If any GROUP BY key is not referenced by the outer SELECT, that's
    // only safe when at least one referenced inner field is an aggregate
    // (SUM, COUNT, etc.) — the aggregate justifies the GROUP BY.
    // Otherwise, the result has bare GROUP BY keys with no aggregates,
    // which downstream passes reject.
    if let Some(group_by) = &inl_stmt.group_by {
        let referenced_inner: Vec<&Expr> = ext_to_int_fields
            .iter()
            .filter(|(col, _)| base_columns_for_alias.contains(col))
            .map(|(_, expr)| expr)
            .collect();
        let has_unreferenced_gb_key = group_by.fields.iter().any(|gf| {
            resolve_field_reference(&inl_stmt.fields, gf)
                .map_or(true, |e| !referenced_inner.contains(&&e))
        });
        if has_unreferenced_gb_key {
            let has_referenced_aggregate = referenced_inner
                .iter()
                .any(|e| is_aggregated_expr(e).unwrap_or(false));
            if !has_referenced_aggregate {
                return Ok(false);
            }
        }
    }

    // === Prevent inlining when base contains WF that references an inlinable aggregate
    {
        // Step 1: Collect all columns used inside WFs in the base statement
        let mut wf_columns = HashSet::new();
        outermost_expression(base_stmt).for_each(|expr| {
            let _ = for_each_window_function(expr, &mut |wf_expr: &Expr| {
                wf_columns.extend(columns_iter(wf_expr).cloned());
            });
        });

        // Step 2: For each WF-referenced column, if it points to an inlinable alias
        // and is mapped to an aggregated expression, block the inlining
        for col in wf_columns {
            if col.table.as_ref() == Some(&inl_stmt_rel)
                && let Some(inl_expr) = ext_to_int_fields.get(&col)
                && is_aggregated_expr(inl_expr)?
            {
                return Ok(false);
            }
        }
    }

    // === Join location and operator safety check
    //
    // Inlining a derived table must preserve the semantics of JOINs in the base query.
    // We distinguish two cases:
    //
    // 1. RHS Inlining:
    //    - If the derived table appears in the right-hand side (RHS) of a JOIN,
    //      we require *stricter validation* when the join is non-INNER (e.g., LEFT).
    //    - This prevents semantics-breaking rewrites in anti-join patterns or
    //      null-extending JOINs.
    //    - We use `can_inline_left_join_rhs_safe` to ensure:
    //        a) The RHS is a single-table, non-joined subquery
    //        b) Its WHERE clause contains only simple, pushable filters
    //        c) Any non-base projected expressions are NOT used in the outer query
    //
    // 2. LHS Inlining:
    //    - When inlining into the base `FROM` clause (prior to any JOIN),
    //      we make this distinction:
    //        a) If the inlinable is simple (no joins, no WHERE), allow unconditionally
    //        b) If the inlinable is complex (multi-table or with filters), allow *only if the *first* join is INNER*.
    if inl_from_item_ord_idx >= base_stmt.tables.len() {
        // RHS inlining
        let (jc_idx, _) = find_rhs_join_clause(base_stmt, inl_from_item_ord_idx)
            .ok_or_else(|| internal_err!("Invalid FROM item index"))?;
        let jc = &base_stmt.join[jc_idx];
        if !jc.operator.is_inner_join()
            && !can_inline_left_join_rhs_safe(base_stmt, jc, inl_stmt, &ext_to_int_fields)?
        {
            return Ok(false);
        }
    } else {
        // LHS inlining.
        //
        // A single-table subquery (no joins) is safe to inline even when the
        // first base join is LEFT/RIGHT — the inlinable's WHERE only references
        // its own local columns (derived tables are uncorrelated at this stage),
        // so absorbing it into the base WHERE is equivalent to pre-filtering
        // the LHS before the join.  LEFT JOIN never NULL-extends LHS columns,
        // so the filter position (before vs. after the join) doesn't change
        // the result.
        //
        // Multi-table or multi-join inlinables are still blocked when the first
        // join is not INNER, because splicing additional join structure into the
        // LHS could change the join topology unpredictably.
        let is_safe_lhs = inl_stmt.tables.len() == 1 && inl_stmt.join.is_empty();
        if !(is_safe_lhs || base_stmt.join.is_empty() || base_stmt.join[0].operator.is_inner_join())
        {
            return Ok(false);
        }

        // Defense-in-depth: when inlining a single-table LHS into a non-inner
        // join base, verify the adjacent join has a supported ON constraint.
        // The downstream post-substitution and nontrivial-ON checks also
        // validate this, but checking early avoids subtle breakage if those
        // guards are ever relaxed.
        if is_safe_lhs && !base_stmt.join.is_empty() && !base_stmt.join[0].operator.is_inner_join()
        {
            match &base_stmt.join[0].constraint {
                JoinConstraint::On(expr) if is_supported_join_condition(expr) => {}
                _ => return Ok(false),
            }
        }
    }

    // === Validate all join ON expressions for supported shape (after substitution)
    for jc in &base_stmt.join {
        if let JoinConstraint::On(expr) = &jc.constraint {
            let substituted = substitute_columns_in_expr(expr, &ext_to_int_fields, is_top_select)?;
            if !is_supported_join_condition(&substituted) && !jc.operator.is_inner_join() {
                return Ok(false); // Not repairable for non-inner joins
            }
            // Reject subquery expressions in ON for ALL join types —
            // migrate_and_normalize_joins cannot repair a subquery in ON,
            // even for INNER joins.  This catches the case where a
            // subquery from the inlinable's SELECT gets substituted into
            // a base JOIN ON clause.
            if contains_select(&substituted) {
                return Ok(false);
            }
        }
    }

    // === Final ON → WHERE safety check for multi-table ON clauses
    if !can_move_joins_on_nontrivial_expr_to_where(base_stmt, &ext_to_int_fields)? {
        return Ok(false);
    }

    Ok(true)
}

/// Substitutes all column references that appear in `ext_to_int_fields` with their mapped
/// expressions throughout `base_stmt` (SELECT list, WHERE, JOIN ON, GROUP BY, HAVING, ORDER BY).
///
/// Also assigns synthetic aliases to any SELECT-list item whose visible name would change
/// after substitution: if a column maps to a differently-named or non-column expression,
/// an alias is injected to preserve the output column identity. At the top-level select,
/// aliases are only added where the substitution would actually change the visible name.
fn replace_columns_with_inlinable_expr(
    base_stmt: &mut SelectStatement,
    ext_to_int_fields: &HashMap<Column, Expr>,
    is_top_select: bool,
) -> ReadySetResult<()> {
    for select_item in &mut base_stmt.fields {
        let (expr, maybe_alias) = expect_field_as_expr_mut(select_item);
        if maybe_alias.is_none()
            && (!is_top_select
                || matches!(expr, Expr::Column(col) if ext_to_int_fields.get(col).is_some_and(|e| match e {
                    Expr::Column(inl_col) => col.name != inl_col.name,
                    _ => true,
                })))
        {
            *maybe_alias = Some(default_alias_for_select_item_expression(expr));
        }
    }

    for expr in collect_outermost_columns_mut(base_stmt) {
        if let Expr::Column(col) = expr
            && let Some(inl_expr) = ext_to_int_fields.get(col)
        {
            *expr = inl_expr.clone();
        }
    }

    Ok(())
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
fn inline_from_item_in_place(
    base_stmt: &mut SelectStatement,
    mut inl_from_item: TableExpr,
    inl_from_item_ord_idx: usize,
    is_inl_aggregated: bool,
    is_base_aggregated: bool,
) -> ReadySetResult<()> {
    let is_single_from_item_base_select = base_stmt.join.is_empty() && base_stmt.tables.len() == 1;

    // Get the inlinable FROM item's statement
    let (inl_stmt, _) = expect_sub_query_with_alias_mut(&mut inl_from_item);
    let mut inl_stmt = mem::take(inl_stmt);

    // Based on the index, locate the item being inlined: `base_stmt.tables` (LHS) or `base_stmt.join` (RHS)
    // Case 1: LHS inlining — the FROM item belongs to the base `tables` list (comma-separated FROM).
    if inl_from_item_ord_idx < base_stmt.tables.len() {
        let inl_tables = mem::take(&mut inl_stmt.tables);
        if inl_stmt.join.is_empty() {
            // Splice inlined subquery's FROM items directly into base query tables.
            base_stmt
                .tables
                .splice(inl_from_item_ord_idx..=inl_from_item_ord_idx, inl_tables);
        } else {
            // The subquery contains both FROM items and JOINs; lift both into base query.
            // Remove the subquery FROM item and extend tables with inlinable's tables
            base_stmt.tables.remove(inl_from_item_ord_idx);
            base_stmt.tables.extend(inl_tables);

            // Insert inlinable joins at the bottom
            let inl_joins = mem::take(&mut inl_stmt.join);
            base_stmt.join.splice(0..0, inl_joins);
        }
    } else {
        // Case 2: RHS inlining — the FROM item is inside a JOIN clause (JoinRightSide of some JoinClause).

        // Collect the inlinable join structure.
        let mut inl_joins = Vec::new();

        // Locate the join clause and position inside the JoinRightSide where this FROM item lives.
        // This lookup is based on flat ordinal index captured before inlining mutations.
        let Some((jc_idx, inl_from_item_pos)) =
            find_rhs_join_clause(base_stmt, inl_from_item_ord_idx)
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
    }

    // Aggregated subquery: lift GROUP BY, HAVING, DISTINCT, and ORDER BY from the inlined
    // subquery into the base. The base's existing WHERE becomes part of HAVING (since it now
    // applies post-aggregation), and the subquery's WHERE replaces the base's WHERE.
    if is_inl_aggregated {
        invariant!(
            is_single_from_item_base_select,
            "Expected single FROM base select"
        );
        base_stmt.distinct = inl_stmt.distinct;
        base_stmt.having = mem::take(&mut inl_stmt.having);
        if let Some(base_stmt_where_expr) = &base_stmt.where_clause {
            base_stmt.having = and_predicates_skip_true(
                mem::take(&mut base_stmt.having),
                base_stmt_where_expr.clone(),
            );
        }
        base_stmt.where_clause = mem::take(&mut inl_stmt.where_clause);
        // Resolve numeric/alias GROUP BY references against the inlined
        // subquery's own SELECT fields before transferring to the outer
        // statement, whose field list may differ in length or order.
        let resolved_gb = resolve_group_by_exprs(&inl_stmt)?;
        base_stmt.group_by = if resolved_gb.is_empty() {
            None
        } else {
            Some(GroupByClause {
                fields: resolved_gb.into_iter().map(FieldReference::Expr).collect(),
            })
        };
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

    // Take ORDER BY if eligible, with aggregation-aware guard.
    // - If the parent (`base_stmt`) is aggregated or grouped, any ORDER BY inside the
    //   inlined child does not affect the parent's semantics (grouping/aggregates or
    //   ordered-aggregates dictate result order), so we DROP the child's ORDER BY.
    // - If the parent already has an explicit ORDER BY, it prevails (keep it) and
    //   we do not adopt the child's.
    // - Only when the parent is NOT aggregated and has NO ORDER BY do we adopt the
    //   child's ORDER BY to preserve presentation order.
    if is_single_from_item_base_select
        && !is_base_aggregated
        && base_stmt.order.is_none()
        && base_stmt.limit_clause.is_empty()
    {
        // Resolve numeric and alias ORDER BY references against the
        // inlined subquery's own SELECT fields before transferring,
        // same rationale as GROUP BY above.
        if let Some(order) = &mut inl_stmt.order {
            for ord_by in &mut order.order_by {
                ord_by.field =
                    FieldReference::Expr(resolve_field_reference(&inl_stmt.fields, &ord_by.field)?);
            }
        }
        base_stmt.order = mem::take(&mut inl_stmt.order);
    }

    Ok(())
}

/// Inlines the FROM item at `inl_from_item_ord_idx` into `base_stmt` in five ordered steps:
///
/// 1. **Rename aliases** — make all internal aliases distinct from the base statement to
///    prevent name collisions after flattening.
/// 2. **Hoist nontrivial ON predicates** — move ON-clause expressions that reference inlinable
///    columns into WHERE, so they remain valid once the subquery alias disappears.
/// 3. **Substitute columns** — replace all base-scope references to the former subquery alias
///    with their actual underlying expressions.  This runs BEFORE structural splicing so that
///    expressions absorbed from the inlinable (WHERE, HAVING, ORDER BY) are never exposed to
///    substitution — they already use internal-scope names and must not be rewritten.
/// 4. **Splice structure** — inline the subquery's tables, joins, WHERE, HAVING, and ORDER BY
///    into `base_stmt` via [`inline_from_item_in_place`].
/// 5. **Normalize joins** — migrate applicable WHERE predicates back to ON and canonicalize
///    join constraint shapes.
fn inline_from_item(
    base_stmt: &mut SelectStatement,
    inl_from_item_ord_idx: usize,
    is_top_select: bool,
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

    make_aliases_distinct_from_base_statement(base_stmt, &mut inl_from_item, &HashSet::new())?;

    let (inl_stmt, inl_stmt_alias) = expect_sub_query_with_alias(&inl_from_item);
    let ext_to_int_fields = build_ext_to_int_fields_map(inl_stmt, inl_stmt_alias)?;

    // Snapshot aggregation flags BEFORE substitution — substitution can introduce
    // aggregate expressions into the base (e.g., sq.total → SUM(t.amount)), which
    // would change the is_aggregated classification.
    let is_inl_aggregated = is_aggregation_or_grouped(inl_stmt)?;
    let is_base_aggregated = is_aggregation_or_grouped(base_stmt)?;

    move_joins_on_nontrivial_expr_to_where(base_stmt, &ext_to_int_fields);

    // Substitute BEFORE splicing: only base-scope columns are present at this point.
    // The inlinable's WHERE/HAVING/ORDER BY (which use internal-scope names) have not
    // been absorbed yet, so they cannot be incorrectly matched by the ext_to_int map.
    replace_columns_with_inlinable_expr(base_stmt, &ext_to_int_fields, is_top_select)?;

    inline_from_item_in_place(
        base_stmt,
        inl_from_item,
        inl_from_item_ord_idx,
        is_inl_aggregated,
        is_base_aggregated,
    )?;

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
        let mut inl_from_item_idx = None;
        for (from_item_idx, from_item) in get_local_from_items_iter!(base_stmt).enumerate() {
            if can_inline_from_item(base_stmt, from_item, from_item_idx, is_top_select)? {
                inl_from_item_idx = Some(from_item_idx);
                break;
            }
        }
        if let Some(inl_from_item_idx) = inl_from_item_idx {
            inline_from_item(base_stmt, inl_from_item_idx, is_top_select)?;
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
