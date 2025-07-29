use crate::detect_problematic_self_joins::contains_problematic_self_joins;
use crate::lateral_join::unnest_lateral_subqueries;
use crate::rewrite_utils::{
    NO_REWRITES_STATUS, OnAtom, RewriteStatus, SINGLE_REWRITE_STATUS,
    align_group_by_and_windows_with_correlation, analyse_lone_aggregates_subquery_fields,
    and_predicates_skip_true, as_sub_query_with_alias_mut, bubble_alias_to_anchor_top,
    classify_on_atom, collect_local_from_items, columns_iter, construct_is_not_null_expr,
    construct_projecting_wrapper, construct_scalar_expr, contains_select, decompose_conjuncts,
    default_alias_for_select_item_expression, ensure_first_field_alias, expect_field_as_expr,
    expect_field_as_expr_mut, expect_only_subquery_from_with_alias,
    expect_only_subquery_from_with_alias_mut, expect_sub_query_with_alias,
    expect_sub_query_with_alias_mut, find_group_by_key, find_rhs_join_clause, for_each_aggregate,
    get_from_item_reference_name, get_unique_alias, has_alias, is_aggregate_only_without_group_by,
    is_aggregated_expr, is_aggregated_select, make_first_field_ref_name,
    move_correlated_constraints_from_join_to_where, project_statement_columns_if,
    rewrite_top_k_in_place, rewrite_top_k_in_place_with_partition, split_correlated_constraint,
    split_correlated_expression, split_expr_mut,
};
use crate::unnest_subqueries_3vl::{
    ProbeRegistry, RhsContext, SelectList3vlFlags, SelectList3vlInput,
    add_3vl_for_not_in_where_subquery, add_3vl_for_select_list_in_subquery,
    is_first_field_null_free, is_select_expr_null_free,
};
use crate::{
    RewriteContext, get_local_from_items_iter, get_local_from_items_iter_mut, is_column_of,
};
use itertools::{Either, Itertools};
use readyset_errors::{
    ReadySetError, ReadySetResult, internal, invalid_query, invalid_query_err, invariant,
    unsupported,
};
use readyset_sql::analysis::visit_mut::{VisitorMut, walk_expr};
use readyset_sql::ast::JoinOperator::{InnerJoin, LeftOuterJoin};
use readyset_sql::ast::{
    BinaryOperator, Column, ColumnConstraint, Expr, FieldDefinitionExpr, FieldReference,
    FunctionExpr, InValue, JoinClause, JoinConstraint, JoinOperator, Literal, SelectStatement,
    SqlIdentifier, TableExpr, UnaryOperator,
};
use readyset_sql::ast::{JoinRightSide, Relation, TableExprInner};
use readyset_sql::{Dialect, DialectDisplay};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::iter;
use std::mem;
use tracing::trace;

#[derive(Default, Copy, Clone, Debug)]
pub(crate) enum SubqueryContext {
    #[default]
    In,
    Scalar,
    Exists,
}

#[derive(Default)]
pub(crate) struct SubqueryPredicateDesc {
    ctx: SubqueryContext,
    negated: bool,
    lhs_and_op: Option<(Expr, BinaryOperator)>,
    stmt: SelectStatement,
}

pub(crate) enum DeriveTableJoinKind {
    Join(JoinOperator),
    AntiJoin,
}

/// Result of splitting a JOIN condition into ON and WHERE parts under the
/// "RHS with preceding LHS chain" policy.
#[derive(Default)]
pub(crate) struct JoinConditionSplit {
    /// Conjunction that should remain in the JOIN ... ON (None means empty/TRUE).
    pub(crate) on_expr: Option<Expr>,
    /// Conjunction that should be moved to WHERE (None means nothing to move).
    pub(crate) to_where: Option<Expr>,
    /// True iff:
    ///   - RHS is bound to exactly one LHS (or chosen LHS when multiple),
    ///   - ON contains at least one RHS<->chosen-LHS equality,
    ///   - and no remainder had to be moved to WHERE.
    pub(crate) fully_supported: bool,
}

/// Options controlling how a subquery is turned into a joinable derived table.
#[derive(Default)]
pub(crate) struct AsJoinableOpts {
    // Preserve the subquery shape for EXISTS - for probe use
    pub(crate) preserve_top_k_for_exists: bool,
    // Always return a wrapper with a single anchor subquery for probe use
    pub(crate) force_wrapper: bool,
    // Name of the *original RHS first field* to bubble to the ANCHOR TOP
    pub(crate) bubble_alias_to_anchor_top: Option<SqlIdentifier>,
}

pub(crate) const PRESENT_COL_NAME: &str = "present_";

macro_rules! present_field_expr {
    () => {
        FieldDefinitionExpr::Expr {
            expr: Expr::Literal(Literal::Integer(1)),
            alias: Some(PRESENT_COL_NAME.into()),
        }
    };
}

pub(crate) trait NonNullSchema {
    fn not_null_columns_of(&self, rel: &Relation) -> HashSet<Column>;
}

pub(crate) struct UnnestContext<'a> {
    pub(crate) schema: &'a dyn NonNullSchema,
    pub(crate) probes: ProbeRegistry,
}

pub trait UnnestSubqueries: Sized {
    fn unnest_subqueries(&mut self, ctx: &RewriteContext) -> ReadySetResult<&mut Self>;
}

impl UnnestSubqueries for SelectStatement {
    fn unnest_subqueries(&mut self, ctx: &RewriteContext) -> ReadySetResult<&mut Self> {
        let schema = <&RewriteContext<'_> as Into<NonNullSchemaImpl>>::into(ctx);
        if unnest_subqueries_main(self, &schema)?.has_rewrites() {
            trace!(target: "unnest_subqueries",
                statement = %self.display(Dialect::PostgreSQL),
                ">Decorrelated statement"
            );
        }
        Ok(self)
    }
}

// The entry point for the entire unnesting pass.
// **NOTE**: This IS NOT a helper function to use internally.
// This is the main entry point and should be called once per statement.
pub(crate) fn unnest_subqueries_main(
    stmt: &mut SelectStatement,
    schema: &dyn NonNullSchema,
) -> ReadySetResult<RewriteStatus> {
    let mut ctx = UnnestContext {
        schema,
        probes: ProbeRegistry::new(),
    };

    let mut rewrite_status = RewriteStatus::default();

    // NEW: run the hoister first; it now also handles TOP-K in all nested derived tables
    if hoist_correlated_from_nested_and_rewrite_top_k(stmt)? {
        rewrite_status.rewrite();
    }

    // Rewrite supported cases of subqueries found in `lateral` FROM, WHERE and select list
    let rewrite_status_1 = unnest_all_subqueries(stmt, &mut ctx)?;

    Ok(rewrite_status.combine(rewrite_status_1))
}

struct NonNullSchemaImpl {
    nonnull_schema: HashMap<Relation, HashSet<Column>>,
}

impl From<&RewriteContext<'_>> for NonNullSchemaImpl {
    fn from(ctx: &RewriteContext<'_>) -> Self {
        let mut schema = NonNullSchemaImpl {
            nonnull_schema: HashMap::new(),
        };

        for (rel, body) in ctx.base_schemas.iter() {
            let nonnull_cols = body
                .fields
                .iter()
                .filter_map(|col_spec| {
                    if col_spec.constraints.contains(&ColumnConstraint::NotNull)
                        || col_spec.constraints.contains(&ColumnConstraint::PrimaryKey)
                    {
                        Some(col_spec.column.clone())
                    } else {
                        None
                    }
                })
                .collect::<HashSet<_>>();
            if !nonnull_cols.is_empty() {
                schema.nonnull_schema.insert((*rel).clone(), nonnull_cols);
            }
        }

        schema
    }
}

impl NonNullSchema for NonNullSchemaImpl {
    fn not_null_columns_of(&self, rel: &Relation) -> HashSet<Column> {
        self.nonnull_schema.get(rel).cloned().unwrap_or_default()
    }
}

fn is_outer_from_item(from_item: &Relation, local_from_items: &HashSet<Relation>) -> bool {
    !local_from_items.contains(from_item)
}

fn split_correlated_expr(
    expr: &Expr,
    local_from_items: &HashSet<Relation>,
) -> (Option<Expr>, Option<Expr>) {
    split_correlated_expression(expr, &|rel| is_outer_from_item(rel, local_from_items))
}

fn contains_outer_columns(expr: &Expr, local_from_items: &HashSet<Relation>) -> bool {
    columns_iter(expr).any(|col| {
        matches!(col, Column {table: Some(table), ..} if is_outer_from_item(table, local_from_items))
    })
}

fn grouping_key_is_projected(fields: &[FieldDefinitionExpr], fe: &FieldReference) -> bool {
    match fe {
        FieldReference::Expr(Expr::Column(alias)) if alias.table.is_none() => {
            fields.iter().any(|f| {
                let (_, field_alias) = expect_field_as_expr(f);
                if let Some(field_alias) = field_alias {
                    field_alias.eq(&alias.name)
                } else {
                    false
                }
            })
        }
        FieldReference::Expr(expr) => fields.iter().any(|f| {
            let (field_expr, _) = expect_field_as_expr(f);
            field_expr.eq(expr)
        }),
        FieldReference::Numeric(proj_idx) => *proj_idx > 0 && *proj_idx <= fields.len() as u64,
    }
}

fn contains_other_than_extremum_window_functions(stmt: &SelectStatement) -> ReadySetResult<bool> {
    for fe in &stmt.fields {
        let (expr, _) = expect_field_as_expr(fe);
        let mut contains = false;
        for_each_aggregate(expr, true, &mut |agg| {
            if !matches!(agg, FunctionExpr::Min(_) | FunctionExpr::Max(_)) {
                contains = true;
            }
        })?;
        if contains {
            return Ok(true);
        }
    }
    Ok(false)
}

fn make_subquery_distinct(stmt: &mut SelectStatement) -> ReadySetResult<()> {
    if let Some(group_by) = &stmt.group_by {
        if !group_by
            .fields
            .iter()
            .all(|fe| grouping_key_is_projected(&stmt.fields, fe))
        {
            stmt.distinct = true;
            if !is_aggregated_select(stmt)? && !contains_other_than_extremum_window_functions(stmt)?
            {
                stmt.group_by = None;
            }
        }
    } else if !is_aggregated_select(stmt)? {
        stmt.distinct = true;
    }

    invariant!(stmt.limit_clause.is_empty());
    stmt.order = None;

    Ok(())
}

fn rewrite_correlated_top_k_in_place(
    stmt: &mut SelectStatement,
    cols_set: &HashSet<(Column, Column)>,
) -> ReadySetResult<()> {
    rewrite_top_k_in_place_with_partition(
        stmt,
        cols_set
            .iter()
            .map(|(l, _)| Expr::Column(l.clone()))
            .collect(),
    )
}

fn hoist_correlated_from_where_clause_and_rewrite_top_k(
    stmt: &mut SelectStatement,
    stmt_alias: SqlIdentifier,
    locals: &HashSet<Relation>,
    mut check_for_local_cols_eq_group_keys: impl FnMut(bool) -> ReadySetResult<()>,
    mut check_for_uncorrelated_where: impl FnMut(&mut SelectStatement) -> ReadySetResult<()>,
) -> ReadySetResult<Option<Expr>> {
    if let Some(where_expr) = &stmt.where_clause {
        let (mut correlated, remaining) = split_correlated_expr(where_expr, locals);

        if let Some(remaining_expr) = &remaining
            && contains_outer_columns(remaining_expr, locals)
        {
            unsupported!(
                "Unsupported correlation in subquery: {}",
                remaining_expr.display(Dialect::PostgreSQL)
            );
        }

        if let Some(corr) = &mut correlated {
            // (1) Build a set of pairs (local_column : correlated_column)
            let cols_set = split_correlated_constraint(corr, locals)?;

            // (2) align against original local shape, and get a flag if either `stmt`
            // has no GROUP BY or all local columns are the only grouping keys, and
            // fix group by correlated columns occurrences and window functions
            let are_local_columns_eq_grouping_keys =
                align_group_by_and_windows_with_correlation(stmt, &cols_set)?;

            check_for_local_cols_eq_group_keys(are_local_columns_eq_grouping_keys)?;

            // (3) project needed local cols & retarget ON to the statement alias (stable after wrap)
            let mut corr_for_join = corr.clone();
            project_statement_columns_if(
                stmt,
                stmt_alias.clone(),
                &mut corr_for_join,
                |col| matches!(&col.table, Some(t) if locals.contains(t)),
            )?;

            // (4) keep only local remainder inside the subquery
            stmt.where_clause = remaining;

            // (5) apply TOP-K now
            if !stmt.limit_clause.is_empty() {
                rewrite_correlated_top_k_in_place(stmt, &cols_set)?;
            }

            // return hoisted ON
            return Ok(Some(corr_for_join));
        } else {
            stmt.where_clause = remaining;
        }
    }

    check_for_uncorrelated_where(stmt)?;

    if !stmt.limit_clause.is_empty() {
        rewrite_top_k_in_place(stmt)?;
    }

    Ok(None)
}

/// Force a SELECT to be **effectively empty** by pushing `WHERE FALSE` at the *deepest* level
/// that actually has `LIMIT 0`, while clearing `ORDER/LIMIT/OFFSET` on wrapper levels.
///
/// Rationale:
/// - `has_limit_zero_deep(stmt)` walks through single-child projecting wrappers to detect
///   an *effective* LIMIT 0, but wrapper `WHERE` clauses (e.g., correlation in LATERAL bodies)
///   may still be semantically relevant to ON-shaping. Emptiness should therefore be
///   introduced **at the inner level** where `LIMIT 0` appears, not by wiping the outer `WHERE`.
/// - We still clear `ORDER/LIMIT/OFFSET` on all levels along the descent path so that
///   later passes (e.g., validations that disallow LIMIT in certain contexts) don't trip.
/// - Aggregate-only wrappers (no GROUP BY) intentionally *break* the descent; callers are
///   expected to have gated this function with `has_limit_zero_deep(stmt)` which respects that.
pub(crate) fn force_empty_select(stmt: &mut SelectStatement) {
    // Clear ORDER/LIMIT/OFFSET at this level only.
    fn clear_limits_here(s: &mut SelectStatement) {
        s.limit_clause = Default::default();
        s.order = None;
    }

    // Recursively clear ORDER/LIMIT/OFFSET in **children** (used after we inject WHERE FALSE
    // at the deepest level) to avoid leaving stray LIMIT/ORDER inside an already-empty subtree.
    fn clear_limits_children_rec(s: &mut SelectStatement) {
        for t in get_local_from_items_iter_mut!(s) {
            if let Some((inner, _)) = as_sub_query_with_alias_mut(t) {
                clear_limits_here(inner);
                clear_limits_children_rec(inner);
            }
        }
    }

    // Descend through a chain of single-subquery FROM wrappers to find the **deepest** LIMIT 0.
    // Inject WHERE FALSE there; on the way *up*, clear LIMIT/ORDER at each wrapper but keep its WHERE.
    fn force_deep(s: &mut SelectStatement) -> bool {
        // If LIMIT 0 is local, inject emptiness here and sanitize children.
        if matches!(s.limit_clause.limit(), Some(Literal::Integer(0))) {
            clear_limits_here(s);
            clear_limits_children_rec(s);
            // ⟵ choose HAVING FALSE for aggregate-only/no-GBY, else WHERE FALSE
            if is_aggregate_only_without_group_by(s).is_ok_and(|r| r) {
                s.having = Some(Expr::Literal(Literal::Boolean(false)));
            } else {
                s.where_clause = Some(Expr::Literal(Literal::Boolean(false)));
            }
            return true;
        }
        // Attempt to descend through a single projecting wrapper (aggregate-only wrappers are
        // assumed to be filtered out by the caller's `has_limit_zero_deep` check).
        if let Ok((inner, _alias)) = expect_only_subquery_from_with_alias_mut(s)
            && force_deep(inner)
        {
            // Deepest level handled; clear LIMIT/ORDER at this wrapper to avoid later LIMIT checks.
            clear_limits_here(s);
            return true;
        }
        false
    }

    let _ = force_deep(stmt);
}

/// If this SELECT is aggregate-only without GROUP BY and HAVING is literal FALSE,
/// scrub LIMIT/OFFSET/ORDER locally and pin emptiness via HAVING FALSE (or WHERE FALSE
/// as a conservative fallback). Returns true if it made a change.
fn scrub_empty_agg_no_gby(stmt: &mut SelectStatement) -> ReadySetResult<bool> {
    Ok(
        if matches!(
            agg_only_no_gby_cardinality(stmt)?,
            Some(AggNoGbyCardinality::ExactlyZero)
        ) {
            // Clear LIMIT/OFFSET and ORDER at this level only; no deep descent here.
            stmt.limit_clause = Default::default();
            stmt.order = None;
            // Shape is guaranteed by `agg_only_no_gby_cardinality` above; pin emptiness via HAVING FALSE.
            #[cfg(debug_assertions)]
            debug_assert!(
                is_aggregate_only_without_group_by(stmt).is_ok_and(|r| r),
                "scrub_empty_agg_no_gby: expected agg-only/no-GBY shape"
            );
            stmt.having = Some(Expr::Literal(Literal::Boolean(false)));
            true
        } else {
            false
        },
    )
}

pub(crate) fn rewrite_top_k_for_lateral(
    stmt: &mut SelectStatement,
    stmt_locals: &HashSet<Relation>,
) -> ReadySetResult<bool> {
    // Early LIMIT 0: force emptiness before any TOP-K rewrite clears LIMIT/OFFSET.
    if has_limit_zero_deep(stmt) {
        force_empty_select(stmt);
        return Ok(true);
    }

    // Optional local scrub: agg-only/no-GBY with HAVING FALSE → empty; clear LIMIT/ORDER here.
    if scrub_empty_agg_no_gby(stmt)? {
        return Ok(true);
    }

    // Inspect correlation in the inner WHERE but DO NOT hoist it; use it only
    // to align windows and to derive PARTITION BY for TOP‑K.
    if let Some(where_expr) = &stmt.where_clause {
        let (maybe_corr, _remaining) = split_correlated_expr(where_expr, stmt_locals);

        if let Some(corr) = maybe_corr {
            // Build (local_col, outer_col) set and align before TOP‑K
            let cols_set = split_correlated_constraint(&corr, stmt_locals)?;
            align_group_by_and_windows_with_correlation(stmt, &cols_set)?;

            // Apply partitioned TOP‑K if present
            if !stmt.limit_clause.is_empty() {
                rewrite_correlated_top_k_in_place(stmt, &cols_set)?;
                return Ok(true);
            }
        }
    }

    if !stmt.limit_clause.is_empty() {
        // No WHERE, or no correlation in WHERE, but TOP‑K present → global RN
        rewrite_top_k_in_place(stmt)?;
        return Ok(true);
    }

    Ok(false)
}

fn is_from_item_inner_joined(stmt: &SelectStatement, from_idx: usize) -> bool {
    let base_count = stmt.tables.len();

    // Items in the initial comma-FROM segment behave like INNER joins.
    if from_idx < base_count {
        return true;
    }
    // Map this RHS item to its join clause and check the operator.
    if let Some((jc_idx, _)) = find_rhs_join_clause(stmt, from_idx) {
        stmt.join[jc_idx].operator.is_inner_join()
    } else {
        // Shouldn’t happen; be conservative.
        false
    }
}

/// For each nested derived table inside `stmt`, hoist any predicates in that nested
/// subquery that reference *outer* relations for that nested subquery into `stmt.where_clause`.
fn hoist_correlated_from_nested_and_rewrite_top_k(
    stmt: &mut SelectStatement,
) -> ReadySetResult<bool> {
    // Track if any rewrite (e.g., TOP-K inside LATERAL) happened, even if not hoisting
    let mut any_rewrite = false;

    // Set of 0-based indexes of all inner-joined relations.
    let mut inner_joined_from_items_indexes = HashSet::new();
    for (tab_expr_idx, _) in get_local_from_items_iter!(stmt).enumerate() {
        if is_from_item_inner_joined(stmt, tab_expr_idx) {
            inner_joined_from_items_indexes.insert(tab_expr_idx);
        }
    }

    // Set of all base-local relations.
    let stmt_locals = collect_local_from_items(stmt)?;

    let mut hoist_expr = None;
    for (tab_expr_idx, tab_expr) in get_local_from_items_iter_mut!(stmt).enumerate() {
        if let Some((inner_stmt, inner_alias)) = as_sub_query_with_alias_mut(tab_expr) {
            // Early: LIMIT 0 short-circuit for LATERAL bodies **before** any recursion.
            // Prevent downstream hoisting/Top‑K from erasing the LIMIT 0 signal inside the
            // LATERAL body (e.g., when LIMIT 0 is nested under a projecting wrapper).
            // Aggregate‑only wrappers still yield one row, so `has_limit_zero_deep` accounts for that.
            if inner_stmt.lateral && has_limit_zero_deep(inner_stmt) {
                force_empty_select(inner_stmt);
                any_rewrite = true;
                continue;
            }
            // Optional local scrub: agg-only/no-GBY with HAVING FALSE → empty; clear LIMIT/ORDER here.
            if inner_stmt.lateral && scrub_empty_agg_no_gby(inner_stmt)? {
                any_rewrite = true;
                continue;
            }

            // Recurse next so deeper subqueries are handled once the short‑circuit is settled
            hoist_correlated_from_nested_and_rewrite_top_k(inner_stmt)?;

            // Compute locals for the inner subquery statement
            let inner_locals = collect_local_from_items(inner_stmt)?;

            // Normalize: move any correlations in JOINs to WHERE within the inner subquery
            move_correlated_constraints_from_join_to_where(inner_stmt, &|rel| {
                is_outer_from_item(rel, &inner_locals)
            })?;

            if inner_stmt.lateral {
                if rewrite_top_k_for_lateral(inner_stmt, &inner_locals)? {
                    any_rewrite = true;
                }
            } else if let Some(correlated_expr) =
                hoist_correlated_from_where_clause_and_rewrite_top_k(
                    inner_stmt,
                    inner_alias.clone(),
                    &inner_locals,
                    |_| Ok(()),
                    |_| Ok(()),
                )?
            {
                if !inner_joined_from_items_indexes.contains(&tab_expr_idx) {
                    unsupported!(
                        "Cannot hoist across a LEFT OUTER join attachment: {}",
                        correlated_expr.display(Dialect::PostgreSQL)
                    );
                }

                // The inner subquery’s own relation
                let inner_rel: Relation = inner_alias.into();

                // SAFE TO HOIST (non-LATERAL): every base-local column must be the inner alias;
                // ancestors (not in stmt_locals) are allowed. Base-local siblings are illegal in PostgreSQL
                // for non-LATERAL FROM items and must not appear here.
                let mut has_unqualified_cols = false;
                let safe_to_hoist = columns_iter(&correlated_expr).all(|col| {
                    match &col.table {
                        None => {
                            // unqualified column
                            has_unqualified_cols = true;
                            false
                        }
                        Some(t) if *t == inner_rel => true, // inner alias (retargeted earlier)
                        Some(t) if !stmt_locals.contains(t) => true, // ancestor of base → allowed
                        Some(_) => false, // base-local sibling → illegal for non-LATERAL
                    }
                });

                if !safe_to_hoist {
                    unsupported!(
                        "Non-LATERAL FROM item produced a predicate referencing {}: {}",
                        if has_unqualified_cols {
                            "unqualified column"
                        } else {
                            "a base-local sibling"
                        },
                        correlated_expr.display(Dialect::PostgreSQL)
                    );
                }

                hoist_expr = and_predicates_skip_true(hoist_expr, correlated_expr);
                any_rewrite = true;
            }
        }
    }

    if let Some(hoist_expr) = hoist_expr {
        stmt.where_clause = and_predicates_skip_true(mem::take(&mut stmt.where_clause), hoist_expr);
        Ok(true)
    } else {
        Ok(any_rewrite)
    }
}

fn rebuild_projecting_wrapper_fields(
    wrapper_stmt: &mut SelectStatement,
    names: &[SqlIdentifier],
    inner_rel: &Relation,
) {
    wrapper_stmt.fields.clear();
    for name in names {
        wrapper_stmt.fields.push(FieldDefinitionExpr::Expr {
            expr: Expr::Column(Column {
                table: Some(inner_rel.clone()),
                name: name.clone(),
            }),
            alias: Some(name.clone()),
        });
    }
}

/// Returns true iff `stmt` **or** a chain of single-child projecting wrappers over a subquery
/// contains an explicit `LIMIT 1` (with optional `OFFSET 0`).
///
/// Used to relax scalar validation (allow non-aggregated scalar with LIMIT 1),
/// to enable WHERE `IN (... LIMIT 1)` → scalar `=` conversion, and to skip DISTINCT
/// after TOP-K materialization in those cases.
///
/// The check is purely structural: we walk downward as long as the SELECT has exactly
/// one FROM item that is a subquery with an alias, and stop on the first failure.
/// Such wrappers cannot increase cardinality; they may reproject or filter, so an
/// inner `LIMIT 1` still guarantees single-row shape once TOP-K is materialized.
///
/// NOTE: Call this *before* any TOP-K materialization, because
/// `rewrite_top_k_in_place(_with_partition)` will consume ORDER/LIMIT into ROW_NUMBER()
/// and clear them in inner children.
/// Cardinality classification for **aggregate-only without GROUP BY** shapes.
///
/// This refines the previous use of `is_aggregate_only_without_group_by` (a *shape* check)
/// into actionable cardinality when a HAVING is present:
///   - `ExactlyOne`: no HAVING, or HAVING is literal TRUE ⇒ exactly one row.
///   - `ExactlyZero`: HAVING is literal FALSE ⇒ zero rows.
///   - `AtMostOne`: HAVING is present but non-constant ⇒ 0 or 1 row.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AggNoGbyCardinality {
    AtMostOne,
    ExactlyOne,
    ExactlyZero,
}

/// If `stmt` is **aggregate-only without GROUP BY**, return its cardinality classification
/// based on a conservative inspection of HAVING. Otherwise, return `None`.
pub(crate) fn agg_only_no_gby_cardinality(
    stmt: &SelectStatement,
) -> ReadySetResult<Option<AggNoGbyCardinality>> {
    if !is_aggregate_only_without_group_by(stmt)? {
        return Ok(None);
    }
    Ok(match &stmt.having {
        None => Some(AggNoGbyCardinality::ExactlyOne),
        Some(Expr::Literal(Literal::Boolean(true))) => Some(AggNoGbyCardinality::ExactlyOne),
        Some(Expr::Literal(Literal::Boolean(false))) => Some(AggNoGbyCardinality::ExactlyZero),
        // Any non-literal-boolean HAVING may filter or not ⇒ 0 or 1 rows.
        Some(_) => Some(AggNoGbyCardinality::AtMostOne),
    })
}

fn has_limit_one_deep(stmt: &SelectStatement) -> bool {
    // Walk down a chain of single-subquery FROM items, if present.
    let mut cur = stmt;
    loop {
        // Local LIMIT 1 (optionally OFFSET 0)
        if matches!(cur.limit_clause.limit(), Some(Literal::Integer(1)))
            && matches!(cur.limit_clause.offset(), None | Some(Literal::Integer(0)))
        {
            return true;
        }
        // Descend if this SELECT has exactly one FROM item which is a subquery with alias.
        match expect_only_subquery_from_with_alias(cur) {
            Ok((inner, _alias)) => {
                cur = inner;
            }
            Err(_) => return false,
        }
    }
}

/// Returns true iff this SELECT **or** a chain of single‑child projecting wrappers
/// beneath it contains an explicit `LIMIT 0` (with optional `OFFSET 0`).
///
/// IMPORTANT: If we encounter an *aggregate‑only without GROUP BY* wrapper on the way down,
/// we STOP and return `false`. Such a wrapper produces **one row** even when its child is empty
/// (e.g., SUM → NULL, COUNT(*) → 0), so an inner `LIMIT 0` does **not** imply the outer
/// subquery is empty.
///
/// Order of checks:
///   1) If the **current** SELECT has LIMIT 0 → empty.
///   2) Else, if the **current** SELECT is aggregate‑only (no GROUP BY) → inner LIMIT 0 no longer
///      forces emptiness here → return false.
///   3) Else, descend through a single projecting wrapper `(FROM (subquery) AS alias)` if present.
pub(crate) fn has_limit_zero_deep(stmt: &SelectStatement) -> bool {
    let mut cur = stmt;
    loop {
        // (1) Local LIMIT 0 (regardless of OFFSET)
        if matches!(cur.limit_clause.limit(), Some(Literal::Integer(0))) {
            return true;
        }
        // (2) Aggregate‑only without GROUP BY: outer still yields one row → not empty.
        // Do not descend past aggregate‑only wrapper; inner LIMIT 0 no longer implies empty (one‑row aggregate).
        if matches!(is_aggregate_only_without_group_by(cur), Ok(true)) {
            return false;
        }
        // (3) Descend if this SELECT has exactly one FROM item which is a subquery with alias.
        match expect_only_subquery_from_with_alias(cur) {
            Ok((inner, _alias)) => {
                cur = inner;
            }
            Err(_) => return false,
        }
    }
}

/// Returns `true` iff the subquery is **definitely empty** by quick inspection:
///   • contains a `LIMIT 0` at or under a projecting wrapper (`has_limit_zero_deep`), OR
///   • is aggregate-only without GROUP BY and `HAVING` is literal FALSE.
///
/// This unifies the two early short-circuit checks so call sites don’t duplicate logic.
fn is_definitely_empty_subquery(stmt: &SelectStatement) -> ReadySetResult<bool> {
    Ok(has_limit_zero_deep(stmt)
        || matches!(
            agg_only_no_gby_cardinality(stmt)?,
            Some(AggNoGbyCardinality::ExactlyZero)
        ))
}

/// Build a joinable derived table from a subquery, with optional probe shaping for 3VL guards.
///
/// This function centralizes the transformation of a predicate subquery into a joinable
/// derived table, while also handling **probe shaping** needs for 3VL (three-valued logic)
/// guards used by `NOT IN` and select-list subqueries.
///
/// ## Why probe shaping?
/// When the outer query must emulate SQL 3VL semantics (e.g., `NOT IN` or certain
/// select-list cases), we generate *auxiliary* `EXISTS`/`NOT EXISTS` probes that check:
///   1. **presence** of RHS rows per outer group (via a synthetic `present_` column), and
///   2. whether any RHS rows have **NULL in the compared column** (sometimes requiring a
///      window function to detect non-nulls per partition).
///
/// Those probes require a **stable, predictable FROM shape** and, in some cases, access
/// to the original subquery’s **first projected expression** at the *anchor* level.
/// This function supplies that shape when callers pass `SubqueryContext::Exists` along with
/// `AsJoinableOpts`:
/// - `force_wrapper`: force a single-child projecting wrapper so the probe SELECT always
///   has exactly one FROM item (the **anchor top**), regardless of the user’s original shape.
///   **Note**: If `bubble_alias_to_anchor_top` is set but the probe SELECT does not already
///   have a single anchor subquery, a projecting wrapper will be inserted on-demand to
///   satisfy that requirement (even when `force_wrapper` is `false`).
/// - `bubble_alias_to_anchor_top`: ensure the **original first field**’s alias is available
///   at the anchor top by bubbling it through nested wrappers (the expression itself is not recomputed).
/// - `preserve_top_k_for_exists`: if the probe needs to respect a subquery TOP-K, keep LIMIT/OFFSET
///   and ORDER; otherwise, normalize them away (set `DISTINCT`, drop ORDER/LIMIT).
///
/// ## Behavior by context
/// - `Exists`: we inject/protect a `present_` column (1 literal) in the projection so probes
///   can perform presence tests. If `bubble_alias_to_anchor_top` is set, we **do not** wipe the
///   original select list; otherwise, for plain EXISTS we may reduce projection to just `present_`.
/// - `Scalar` / `In`: we validate scalar shape and hoist correlation as needed; 3VL handling for
///   these contexts is done by higher-level routines (`add_3vl_for_not_in_where_subquery`,
///   `add_3vl_for_select_list_in_subquery`) that may call this function in `Exists` mode to shape
///   the auxiliary probe.
///
/// ## Invariants established (when `Exists` with `force_wrapper`)
/// - "present_" is always present in the probe SELECT.
/// - The returned derived table’s SELECT (the “probe SELECT”) has **one** anchor subquery in FROM.
/// - The anchor subquery keeps stable aliasing, with `present_` projected, and (optionally) the
///   **original first field**’s alias bubbled to the anchor top.
/// - If `preserve_top_k_for_exists == false`, TOP-K and ORDER are removed to make the probe set-like.
///
/// These invariants are consumed by the 3VL helpers in `unnest_subqueries_3vl.rs` (which compute
/// null-presence guards using `RhsContext`, and may rely on a window function in the probe when
/// RHS is not null-free).
///
/// ## Errors & limits
/// - Same limitations as the decorrelation framework (equality correlations, no outer-join correlation, etc.).
/// - Bubble requires an aliasable first field; this function ensures such an alias before any field clearing.
pub(crate) fn as_joinable_derived_table_with_opts(
    ctx: SubqueryContext,
    stmt: &mut SelectStatement,
    stmt_alias: SqlIdentifier,
    opts: AsJoinableOpts,
) -> ReadySetResult<(TableExpr, Option<Expr>)> {
    // True iff this SELECT **or** a chain of single-child projecting wrappers beneath it
    // contains an explicit `LIMIT 1` (with optional `OFFSET 0`). Capture this *before*
    // TOP-K materialization; afterward, ORDER/LIMIT are consumed into ROW_NUMBER() and gone.
    // We use this flag to:
    //  - relax scalar validation (allow non-aggregated scalar with LIMIT 1),
    //  - convert WHERE `IN (... LIMIT 1)` into scalar `=`, and
    //  - skip (and explicitly drop) DISTINCT as redundant after RN <= 1.
    let has_limit_one = has_limit_one_deep(stmt);

    // Bubble up correlation from nested derived tables (one level, recursively)
    // (Runs *after* we captured LIMIT 1 information.)
    hoist_correlated_from_nested_and_rewrite_top_k(stmt)?;

    // Base-local FROM items
    let local_from_items = collect_local_from_items(stmt)?;

    // Placeholder for bubbling the original first-field alias; computed in Exists arm if needed.
    let mut bubble_ff_alias: Option<SqlIdentifier> = None;

    // Context-aware handling of LIMIT/OFFSET inside predicate subqueries.
    // Validate scalar shape on the original statement; then, if needed, materialize TOP-K.
    match ctx {
        SubqueryContext::Exists => {
            // Decide if we need to preserve the original first-field alias for NP/no-WF path.
            if opts.bubble_alias_to_anchor_top.is_some() {
                bubble_ff_alias = Some(ensure_first_field_alias(stmt));
                // If bubble_alias_to_anchor_top is set, we prepend present_ and keep the existing fields
                // (so we can bubble the original first field later); otherwise we reduce to just present_
                // Avoid duplicating present_ if a previous pass already inserted it.
                if !has_alias(stmt, &PRESENT_COL_NAME.into()) {
                    stmt.fields.insert(0, present_field_expr!());
                }
            } else {
                // Plain EXISTS probes
                stmt.fields.clear();
                stmt.fields.push(present_field_expr!());
            }

            // If 3VL needs per-partition TOP-K semantics preserved in the probe, keep ORDER/LIMIT.
            // Otherwise, normalize to set semantics: DISTINCT + drop ORDER/LIMIT.
            if !opts.preserve_top_k_for_exists {
                stmt.distinct = true;
                stmt.limit_clause = Default::default();
                stmt.order = None;
            }
        }
        SubqueryContext::Scalar | SubqueryContext::In => {
            // Scalar validation and WHERE-correlation/top-k handling
            let (field_expr, field_expr_alias) = expect_field_as_expr_mut(
                stmt.fields
                    .iter_mut()
                    .exactly_one()
                    .map_err(|_| invalid_query_err!("Subquery has too many columns"))?,
            );
            if field_expr_alias.is_none() {
                *field_expr_alias = Some(default_alias_for_select_item_expression(field_expr));
            }
            if matches!(ctx, SubqueryContext::Scalar) {
                if !is_aggregated_expr(field_expr)? && !has_limit_one {
                    invalid_query!("Subquery should be aggregated or have LIMIT 1");
                }
                if !stmt.limit_clause.is_empty() && !has_limit_one {
                    invalid_query!("Subquery returns more than 1 row");
                }
            }
            if contains_outer_columns(field_expr, &local_from_items) {
                unsupported!(
                    "Subquery select field references outer columns: {}",
                    field_expr.display(Dialect::PostgreSQL)
                );
            }
        }
    }

    move_correlated_constraints_from_join_to_where(stmt, &|rel| {
        is_outer_from_item(rel, &local_from_items)
    })?;

    // Hoist correlation from WHERE and rewrite TOP-K if present (same as before).
    let derived_table_join_on_expr = {
        let is_scalar_ctx = matches!(ctx, SubqueryContext::Scalar);

        let check_for_local_cols_eq_group_keys = |are_local_columns_eq_grouping_keys: bool| {
            if is_scalar_ctx && !are_local_columns_eq_grouping_keys && !has_limit_one {
                invalid_query!("Subquery returns more than 1 row")
            } else {
                Ok(())
            }
        };

        let check_for_uncorrelated_where = |stmt: &mut SelectStatement| {
            if is_scalar_ctx {
                if stmt.group_by.is_some() && !has_limit_one {
                    invalid_query!("Subquery returns more than 1 row")
                }
                if !has_limit_one {
                    // Only when not LIMIT 1: safe to drop
                    stmt.limit_clause = Default::default();
                    stmt.order = None;
                }
            }
            Ok(())
        };

        hoist_correlated_from_where_clause_and_rewrite_top_k(
            stmt,
            stmt_alias.clone(),
            &local_from_items,
            check_for_local_cols_eq_group_keys,
            check_for_uncorrelated_where,
        )?
    };

    // IMPORTANT: Distinct + set normalization is **not** applied for probe-EXISTS with TOP-K.
    // Additionally, for Scalar with explicit LIMIT 1, DISTINCT is redundant:
    // after rewrite_top_k_in_place(_with_partition), LIMIT/ORDER are consumed and
    // RN <= 1 ensures at most one row (globally or per partition), so de-dup is unnecessary.
    if !(matches!(ctx, SubqueryContext::Exists) && opts.preserve_top_k_for_exists) {
        #[cfg(debug_assertions)]
        {
            // Invariant for DISTINCT phase: TOP-K must have been materialized already.
            // ORDER BY and LIMIT/OFFSET are expected to be cleared at this point.
            debug_assert!(
                stmt.limit_clause.is_empty() && stmt.order.is_none(),
                "TOP-K should have been materialized before DISTINCT handling"
            );
        }
        if matches!(ctx, SubqueryContext::Scalar) && has_limit_one {
            // Drop any pre-existing DISTINCT explicitly; RN <= 1 already enforces single-row shape.
            // Keeping DISTINCT could force unnecessary dedup or block planner choices.
            stmt.distinct = false;
        } else {
            // **NOTE**: We may have normalized earlier in the Exists branch; this second pass is idempotent
            // and also harmonizes GROUP BY when needed. Skipped entirely when preserving TOP-K or scalar LIMIT 1.
            make_subquery_distinct(stmt)?;
        }
    }

    // ── MATERIALIZE DERIVED TABLE & (EXISTS-only) PROBE SHAPING ────────────────────────────────
    // At this point `stmt` is normalized (correlation hoisted, TOP-K materialized if any,
    // DISTINCT handled per context). Now:
    //   1) Wrap the mutated `stmt` into a `TableExpr` with `stmt_alias`.
    //   2) If this is an EXISTS probe, enforce probe invariants via `apply_exists_probe_shaping`:
    //        • assert/provide `present_` in the projection,
    //        • optionally bubble the original first-field alias to the ANCHOR TOP (`bubble_ff_alias`),
    //        • optionally force a single-anchor wrapper (stable FROM shape),
    //        • if `preserve_top_k_for_exists`: de-dup on a minimal projection (present_ + ON cols)
    //          at the probe level while preserving child ORDER/LIMIT/GB; otherwise no extra shaping.
    // Returns the final `derived_table` (and the ON expr above) for the join assembly.

    let mut derived_table = TableExpr {
        inner: TableExprInner::Subquery(Box::new(mem::take(stmt))),
        alias: Some(stmt_alias),
    };

    if matches!(ctx, SubqueryContext::Exists) {
        #[cfg(debug_assertions)]
        {
            let (probe_stmt_chk, _) = expect_sub_query_with_alias(&derived_table);
            debug_assert!(
                has_alias(probe_stmt_chk, &PRESENT_COL_NAME.into()),
                "Exists probe must project `present_`"
            );
        }
        derived_table = apply_exists_probe_shaping(
            derived_table,
            &derived_table_join_on_expr,
            &opts,
            bubble_ff_alias,
        )?;
    }

    Ok((derived_table, derived_table_join_on_expr))
}

/// Apply EXISTS-probe shaping invariants to the already materialized derived table and return it.
///
/// - Optionally force a single-anchor wrapper (`opts.force_wrapper`).
/// - Optionally bubble the original first-field alias to the ANCHOR TOP (`bubble_ff_alias`).
/// - If `opts.preserve_top_k_for_exists`, de-duplicate rows on a *minimal* projection
///   (present_ + ON columns) via DISTINCT, adding a wrapper when necessary to preserve
///   child ORDER/LIMIT or GROUP BY.
/// - EP typically passes preserve_top_k_for_exists = false and force_wrapper = false,
///   so this helper returns the table unchanged (dedupe already normalized earlier).
/// - NP uses preserve_top_k_for_exists = true and may require force_wrapper and bubbling.
fn apply_exists_probe_shaping(
    mut derived_table: TableExpr,
    derived_table_join_on_expr: &Option<Expr>,
    opts: &AsJoinableOpts,
    bubble_ff_alias: Option<SqlIdentifier>,
) -> ReadySetResult<TableExpr> {
    // (1) Optionally ensure a predictable outer shape (no Default needed; move-by-value).
    if opts.force_wrapper {
        derived_table = construct_projecting_wrapper(derived_table)?;
    }

    // (2) Optionally bubble the original first-field alias to the anchor top.
    if let Some(ref ff_alias) = bubble_ff_alias {
        // Get the probe SELECT (outer wrapper)…
        let (probe_stmt, _) = expect_sub_query_with_alias_mut(&mut derived_table);
        // …and its single anchor subquery in FROM; make it stable if needed.
        let (anchor_top_stmt, _anchor_top_alias) =
            match expect_only_subquery_from_with_alias_mut(probe_stmt) {
                Ok(t) => t,
                Err(_) => {
                    derived_table = construct_projecting_wrapper(derived_table)?;
                    let (probe_stmt2, _) = expect_sub_query_with_alias_mut(&mut derived_table);
                    expect_only_subquery_from_with_alias_mut(probe_stmt2)?
                }
            };
        // Bubble the original first-field alias
        bubble_alias_to_anchor_top(anchor_top_stmt, ff_alias)?;
        debug_assert!(has_alias(anchor_top_stmt, ff_alias));
    }

    // (3) If TOP-K must be preserved in the child, dedupe at the probe level
    //     on a *minimal* projection (present_ + ON columns). Otherwise do nothing;
    //     earlier normalization already handled the non-TOP-K case.
    if opts.preserve_top_k_for_exists {
        // Outer alias of the derived table (this is what ON uses for RHS)
        let (probe_stmt, probe_alias) = expect_sub_query_with_alias_mut(&mut derived_table);
        let probe_rel = Relation::from(probe_alias.clone());

        // Build ordered minimal set: present_ first, then ON columns in appearance order.
        let mut needed_ordered: Vec<SqlIdentifier> = vec![PRESENT_COL_NAME.into()];
        let mut seen: BTreeSet<SqlIdentifier> = iter::once(PRESENT_COL_NAME.into()).collect();
        if let Some(on_expr) = derived_table_join_on_expr {
            for col in columns_iter(on_expr) {
                if is_column_of!(col, probe_rel) && seen.insert(col.name.clone()) {
                    needed_ordered.push(col.name.clone());
                }
            }
        }

        // Ensure we have a single ANCHOR subquery in FROM and get its alias.
        let (probe_stmt, anchor_alias) = match expect_only_subquery_from_with_alias_mut(probe_stmt)
        {
            Ok((_anchor_stmt, anchor_alias)) => (probe_stmt, anchor_alias),
            Err(_) => {
                derived_table = construct_projecting_wrapper(derived_table)?;
                let (probe_stmt2, _) = expect_sub_query_with_alias_mut(&mut derived_table);
                let (_, anchor_alias2) = expect_only_subquery_from_with_alias_mut(probe_stmt2)?;
                (probe_stmt2, anchor_alias2)
            }
        };
        let anchor_rel: Relation = anchor_alias.into();

        // Is the current probe projection already *exactly* minimal in the desired order,
        // and qualified with the ANCHOR alias?
        let is_minimal = probe_stmt.fields.len() == needed_ordered.len()
            && probe_stmt
                .fields
                .iter()
                .zip(&needed_ordered)
                .all(|(fe, want)| match fe {
                    FieldDefinitionExpr::Expr {
                        expr: Expr::Column(col),
                        ..
                    } => is_column_of!(col, anchor_rel) && col.name == *want,
                    _ => false,
                });

        let has_local_top_k = !probe_stmt.limit_clause.is_empty() || probe_stmt.order.is_some();
        let no_group_by = probe_stmt.group_by.is_none();

        if is_minimal && !has_local_top_k {
            // Already minimal and no local TOP-K: DISTINCT here is safe and sufficient.
            probe_stmt.distinct = true;
        } else if !has_local_top_k && no_group_by {
            // Safe in-place rebuild: reduce to the minimal set (qualified with ANCHOR) and DISTINCT here.
            // Safe because no ORDER/LIMIT/GB to preserve; modifying the projection cannot
            // perturb child TOP-K or grouping semantics.
            rebuild_projecting_wrapper_fields(probe_stmt, &needed_ordered, &anchor_rel);
            probe_stmt.distinct = true;
        } else {
            // General path: preserve child ORDER/LIMIT or GROUP BY via projecting in a *new* wrapper
            // and deduping there. After wrapping, the ANCHOR alias becomes the previous probe alias.
            derived_table = construct_projecting_wrapper(derived_table)?;
            let (outer_probe_stmt, _) = expect_sub_query_with_alias_mut(&mut derived_table);
            let (_, inner_anchor_alias) =
                expect_only_subquery_from_with_alias_mut(outer_probe_stmt)?;
            rebuild_projecting_wrapper_fields(
                outer_probe_stmt,
                &needed_ordered,
                &inner_anchor_alias.into(),
            );
            outer_probe_stmt.distinct = true;
        }
    }

    Ok(derived_table)
}

/// Backwards-compatible wrapper (existing callers stay unchanged).
pub(crate) fn as_joinable_derived_table(
    ctx: SubqueryContext,
    stmt: &mut SelectStatement,
    stmt_alias: SqlIdentifier,
) -> ReadySetResult<(TableExpr, Option<Expr>)> {
    as_joinable_derived_table_with_opts(ctx, stmt, stmt_alias, AsJoinableOpts::default())
}

fn is_comparison_op(op: &BinaryOperator) -> bool {
    matches!(op, BinaryOperator::Equal | BinaryOperator::NotEqual) || op.is_ordering_comparison()
}

fn is_supported_scalar_comparison_against_subquery(
    lhs: &Expr,
    op: &BinaryOperator,
    rhs: &Expr,
) -> bool {
    if is_comparison_op(op) {
        match (lhs, rhs) {
            (lhs_expr, Expr::NestedSelect(_)) => !contains_select(lhs_expr),
            (Expr::NestedSelect(_), rhs_expr) => !contains_select(rhs_expr),
            _ => false,
        }
    } else {
        false
    }
}

// Verify if <expr> is a supported subquery predicate:
// EXISTS, NOT EXISTS, IN, NOT IN, SCALAR
// **NOTE**: This function just checks on the input expression shape,
// w/o cloning or deeper analysis, hence cheap to execute.
fn is_supported_subquery_predicate(expr: &Expr) -> bool {
    match expr {
        Expr::UnaryOp {
            op: UnaryOperator::Not,
            rhs,
        } if matches!(rhs.as_ref(), Expr::Exists(_)) => true,
        Expr::Exists(_) => true,
        Expr::In {
            lhs,
            rhs: InValue::Subquery(_),
            ..
        } if !contains_select(lhs.as_ref()) => true,
        Expr::BinaryOp { lhs, op, rhs, .. }
            if is_supported_scalar_comparison_against_subquery(lhs, op, rhs) =>
        {
            true
        }
        Expr::NestedSelect(_) => true,
        _ => false,
    }
}

// This method extract a supported subquery predicate into a unified format,
// cloning the original statement and adding more contextual data.
// **NOTE**: Any changes regarding what subquery predicates we support, should
// be reflected in the function above `is_supported_subquery_predicate()`
fn as_supported_subquery_predicate(expr: &Expr) -> ReadySetResult<SubqueryPredicateDesc> {
    Ok(match expr {
        Expr::UnaryOp {
            op: UnaryOperator::Not,
            rhs,
        } if matches!(rhs.as_ref(), Expr::Exists(_)) => match rhs.as_ref() {
            Expr::Exists(sq) => SubqueryPredicateDesc {
                ctx: SubqueryContext::Exists,
                stmt: sq.as_ref().clone(),
                negated: true,
                ..SubqueryPredicateDesc::default()
            },
            _ => unreachable!("Just checked this"),
        },
        Expr::Exists(sq) => SubqueryPredicateDesc {
            ctx: SubqueryContext::Exists,
            stmt: sq.as_ref().clone(),
            ..SubqueryPredicateDesc::default()
        },
        Expr::In {
            lhs,
            rhs: InValue::Subquery(sq),
            negated,
        } if !contains_select(lhs.as_ref()) => SubqueryPredicateDesc {
            ctx: SubqueryContext::In,
            negated: *negated,
            lhs_and_op: Some((lhs.as_ref().clone(), BinaryOperator::Equal)),
            stmt: sq.as_ref().clone(),
        },
        Expr::BinaryOp { lhs, op, rhs }
            if is_supported_scalar_comparison_against_subquery(lhs, op, rhs) =>
        {
            let (lhs, op, sq) = match (lhs.as_ref(), rhs.as_ref()) {
                (_, Expr::NestedSelect(sq)) => (lhs.as_ref(), *op, sq.as_ref()),
                (Expr::NestedSelect(sq), _) => (
                    rhs.as_ref(),
                    op.flip_ordering_comparison().unwrap_or(*op),
                    sq.as_ref(),
                ),
                _ => internal!("`is_supported_subquery_predicate()` checked this"),
            };
            SubqueryPredicateDesc {
                ctx: SubqueryContext::Scalar,
                lhs_and_op: Some((lhs.clone(), op)),
                stmt: sq.clone(),
                ..SubqueryPredicateDesc::default()
            }
        }
        Expr::NestedSelect(sq) => SubqueryPredicateDesc {
            ctx: SubqueryContext::Scalar,
            stmt: sq.as_ref().clone(),
            ..SubqueryPredicateDesc::default()
        },
        _ => internal!("`is_supported_subquery_predicate()` checked this"),
    })
}

fn build_rhs_expr_for_aggregate_only_derived_table(
    derived_table_stmt: &SelectStatement,
    derived_table_alias: SqlIdentifier,
    first_field_ref: &Expr,
) -> ReadySetResult<Option<Expr>> {
    let mut fields_map = HashMap::default();
    analyse_lone_aggregates_subquery_fields(
        derived_table_stmt,
        derived_table_alias,
        &mut fields_map,
    )?;
    if let Expr::Column(rhs_col) = &first_field_ref {
        match fields_map.remove(rhs_col) {
            Some(Err(err)) => Err(err.clone()),
            Some(Ok(mapped_expr)) => Ok(Some(mapped_expr)),
            None => Ok(None),
        }
    } else {
        unreachable!("Subquery select field reference should be a column")
    }
}

pub(crate) fn is_supported_join_condition(join_expr: &Expr) -> bool {
    let atoms = match decompose_conjuncts(join_expr) {
        Some(a) => a,
        None => return false, // not a pure AND
    };

    let mut rels = HashSet::new();
    let mut found_cross_eq = false;

    for a in atoms {
        match classify_on_atom(&a) {
            OnAtom::CrossEq { lhs, rhs } => {
                rels.insert(lhs);
                rels.insert(rhs);
                found_cross_eq = true;
            }
            OnAtom::SingleRelFilter { rel } => {
                rels.insert(rel);
            }
            OnAtom::Other => return false,
        }
    }

    found_cross_eq && rels.len() == 2
}

/// Pick the LHS "closest" to RHS: among bound LHS candidates, choose the one
/// with the greatest index in the `preceding_lhs` chain (i.e., appears latest
/// before RHS in FROM).
fn pick_closest_lhs<'a>(
    preceding_lhs: &'a [Relation],
    bound_idxs: &HashSet<usize>,
) -> Option<&'a Relation> {
    for (idx, rel) in preceding_lhs.iter().enumerate().rev() {
        if bound_idxs.contains(&idx) {
            return Some(rel);
        }
    }
    None
}

/// Split a JOIN predicate `join_expr` for a derived table `rhs` against the **ordered** chain
/// of all preceding LHS relations (`preceding_lhs`).
///
/// This constructor **normalizes** the predicate into two parts:
///  * `on_expr`  – atoms that are allowed to remain in `JOIN ... ON` for `rhs` and the *chosen* LHS;
///  * `to_where` – the remainder, intended to be pushed into the outer statement's `WHERE`.
///
/// ### Normal form produced for `on_expr`
/// * `on_expr` is a **pure AND** of atoms (no OR/NOT/subqueries/functions as top-level connectors).
/// * Each atom is either:
///   1. a cross-table equality `Column = Column` **between `rhs` and the chosen LHS**, or
///   2. a *simple parametrizable filter* that references **only one** of `{rhs, chosen}`.
/// * Across all atoms in `on_expr`, the set of referenced relations is **exactly two**: `{rhs, chosen}`.
/// * When `fully_supported == true`, `on_expr` contains **at least one** cross-equality between `rhs` and `chosen`.
///
/// ### LHS selection
/// * If `rhs` binds to **no** LHS via cross-equality, then `on_expr = None`, `to_where = join_expr`, and
///   `fully_supported = false`.
/// * If `rhs` binds to **multiple** LHS, the splitter chooses the **closest** LHS (latest in `preceding_lhs`).
/// * Any atom that violates the normal form (e.g., equalities to another LHS, filters touching a third table,
///   ORs, non-AND structure) is moved into `to_where`.
///
/// ### Invariants guaranteed by this function
/// * `on_expr` (when present) references **only** `{rhs, chosen}`.
/// * `to_where.is_none()` **iff** the original `join_expr` already satisfied the normal form for the chosen LHS.
/// * `fully_supported` is `true` **iff** `to_where.is_none()` **and** `on_expr` contains ≥ 1 cross-equality between
///   `rhs` and `chosen`.
/// * `on_expr ∧ to_where` is logically equivalent to the input `join_expr` modulo AND associativity/commutativity.
///
/// ### Caller contract (semantics & safety)
/// * **INNER joins only** may safely push `to_where` to the outer `WHERE` (both sides are null-rejecting).
/// * For **LEFT/RIGHT/FULL OUTER** joins, callers must require `fully_supported == true` (i.e., `to_where == None`),
///   otherwise moving remainder to `WHERE` can change which rows are null-extended and break OUTER semantics.
/// * After assembling the join, callers should validate the final `ON` via `is_supported_join_condition(on_expr)`.
///
/// ### Example
/// Input:
/// ```sql
/// ... JOIN RHS ON (RHS.a = A.a AND A.flag = 1 AND RHS.b = B.b)
/// ```
/// With `preceding_lhs = [A, B]` and `chosen = A`, the result is:
/// * `on_expr`: `RHS.a = A.a AND A.flag = 1`
/// * `to_where`: `RHS.b = B.b`  (moved because it binds RHS to *another* LHS)
/// * `fully_supported`: `false` (since something was moved)
pub(crate) fn split_on_for_rhs_against_preceding_lhs(
    join_expr: &Expr,
    rhs: &Relation,
    preceding_lhs: &[Relation],
) -> JoinConditionSplit {
    // Map each LHS relation to its position for O(1) lookups.
    let lhs_pos: HashMap<&Relation, usize> = preceding_lhs
        .iter()
        .enumerate()
        .map(|(i, r)| (r, i))
        .collect();

    // Pass 1: discover which LHS are bound to RHS via Column=Column equalities.
    let mut bound_idxs: HashSet<usize> = HashSet::new();
    let _ = split_expr_mut(
        join_expr,
        &mut |constraint| match classify_on_atom(constraint) {
            // RHS on the left, some LHS on the right
            OnAtom::CrossEq {
                lhs: l_rel,
                rhs: r_rel,
            } if &l_rel == rhs => {
                if let Some(&idx) = lhs_pos.get(&r_rel) {
                    bound_idxs.insert(idx);
                }
                true
            }
            // RHS on the right, some LHS on the left
            OnAtom::CrossEq {
                lhs: l_rel,
                rhs: r_rel,
            } if &r_rel == rhs => {
                if let Some(&idx) = lhs_pos.get(&l_rel) {
                    bound_idxs.insert(idx);
                }
                true
            }
            _ => false,
        },
        &mut Vec::new(),
    );

    // Decide which LHS (if any) is chosen for ON.
    let chosen_lhs: Option<&Relation> = if bound_idxs.is_empty() {
        None
    } else if bound_idxs.len() == 1 {
        let idx = *bound_idxs.iter().next().unwrap();
        Some(&preceding_lhs[idx])
    } else {
        pick_closest_lhs(preceding_lhs, &bound_idxs)
    };

    // If nothing binds RHS → everything moves to WHERE.
    if chosen_lhs.is_none() {
        return JoinConditionSplit {
            on_expr: None,
            to_where: Some(join_expr.clone()),
            fully_supported: false,
        };
    }
    let chosen = chosen_lhs.unwrap();

    // Pass 2: extract atoms that are allowed to remain in ON for the chosen LHS,
    // and leave the rest as remainder to move to WHERE.
    let mut accepted_atoms: Vec<Expr> = Vec::new();
    let mut found_cross_eq = false;

    let remainder = split_expr_mut(
        join_expr,
        &mut |constraint| {
            match classify_on_atom(constraint) {
                // Keep only cross-equalities between RHS and the **chosen** LHS
                OnAtom::CrossEq {
                    lhs: l_rel,
                    rhs: r_rel,
                } => {
                    let keep =
                        (&l_rel == rhs && &r_rel == chosen) || (&l_rel == chosen && &r_rel == rhs);
                    if keep {
                        found_cross_eq = true;
                    }
                    keep
                }
                // Keep single-rel filters touching **only** RHS or **only** chosen
                OnAtom::SingleRelFilter { rel } => &rel == rhs || &rel == chosen,
                // Anything else moves to WHERE
                OnAtom::Other => false,
            }
        },
        &mut accepted_atoms,
    );

    // Rebuild ON out of accepted atoms; remainder becomes WHERE.
    let mut on_expr: Option<Expr> = None;
    for atom in accepted_atoms {
        on_expr = and_predicates_skip_true(on_expr, atom);
    }

    // Fully supported iff we kept at least one rhs<->chosen equality AND nothing was moved.
    let fully_supported = found_cross_eq && remainder.is_none();

    JoinConditionSplit {
        on_expr,
        to_where: remainder,
        fully_supported,
    }
}

pub(crate) fn join_derived_table(
    base_stmt: &mut SelectStatement,
    lhs_and_op: Option<(Expr, BinaryOperator)>,
    mut derived_table: TableExpr,
    mut join_expr: Option<Expr>,
    join_kind: DeriveTableJoinKind,
) -> ReadySetResult<bool> {
    //
    macro_rules! is_inner_join {
        ($join_kind:expr) => {
            matches!($join_kind, DeriveTableJoinKind::Join(join_op) if join_op.is_inner_join())
        };
    }

    let (derived_table_stmt, derived_table_alias) =
        expect_sub_query_with_alias_mut(&mut derived_table);

    let mut add_to_where = None;

    if let Some((lhs, op)) = lhs_and_op {
        // TODO: When `lhs` is always NULL, the rewrite for `IN` and `NOT IN` can be
        // TODO: simplified to only checking the `rhs` for emptiness (see 3VL rules)
        if let Expr::Literal(Literal::Null) = &lhs {
            return Ok(false);
        }

        if !is_comparison_op(&op) {
            internal!(
                "Expected a comparison operator for present LHS: {}. Found: {op}",
                lhs.display(Dialect::PostgreSQL)
            );
        }

        let lhs_is_literal = matches!(lhs, Expr::Literal(_));

        let is_join_acceptable = lhs_is_literal /* simple filter */
            || (matches!(op, BinaryOperator::Equal) && matches!(lhs, Expr::Column(_)));

        let rhs = make_first_field_ref_name(derived_table_stmt, derived_table_alias.clone())?;

        let pred = if lhs_is_literal {
            construct_scalar_expr(rhs, op.flip_ordering_comparison().unwrap_or(op), lhs)
        } else {
            construct_scalar_expr(lhs, op, rhs)
        };

        if (lhs_is_literal || !is_join_acceptable) && is_inner_join!(join_kind) {
            add_to_where = Some(pred);
        } else if is_join_acceptable {
            join_expr = and_predicates_skip_true(join_expr, pred);
        } else {
            return Ok(false);
        }
    }

    if let Some(join_on_expr) = join_expr.take() {
        // Build RHS and the ordered chain of all preceding LHS
        let rhs_rel: Relation = derived_table_alias.clone().into();

        let preceding_lhs = get_local_from_items_iter!(base_stmt)
            .map(get_from_item_reference_name)
            .collect::<ReadySetResult<Vec<_>>>()?;

        // Split what should stay in ON vs move to WHERE based on RHS + ordered LHS list
        let split = split_on_for_rhs_against_preceding_lhs(&join_on_expr, &rhs_rel, &preceding_lhs);

        // SAFETY / SEMANTICS:
        // Moving any remainder from ON → WHERE is only semantics‑preserving for INNER joins.
        // For INNER joins, predicates in ON and WHERE are equivalent (both null‑reject) and
        // applying the remainder in WHERE cannot change which rows are null‑extended, nor
        // can it resurrect rows filtered by the JOIN, so earlier OUTER joins remain correct.
        // For LEFT/RIGHT/FULL OUTER joins, however, moving ON → WHERE can *change* which rows are
        // null‑extended and effectively turn the OUTER join into an INNER join. Therefore we
        // only allow remainder when `join_kind` is INNER (see debug_assert! below), and for
        // non‑INNER joins we require `split.fully_supported` (i.e., no remainder at all).
        // Do not relax this without a formal reassociation proof or a dedicated transformation
        // that keeps OUTER‑join semantics intact.
        // Bail early for non-INNER joins unless ON is fully supported (≥1 cross eq, no remainder)
        if !split.fully_supported && !is_inner_join!(join_kind) {
            return Ok(false);
        }

        // Defensive check: if this is not an INNER join, `split` must have produced no remainder.
        // This enforces the policy documented above: never move ON → WHERE for OUTER joins.
        debug_assert!(
            is_inner_join!(join_kind) || split.to_where.is_none(),
            "Unexpected remainder to move to WHERE for non-INNER join: {}",
            split
                .to_where
                .as_ref()
                .map(|e| e.display(Dialect::PostgreSQL).to_string())
                .unwrap_or_default()
        );

        // Now it’s safe to unconditionally push remainder to WHERE;
        // for non-INNER this is a no-op because fully_supported ⇒ to_where == None.
        if let Some(to_where) = split.to_where {
            add_to_where = and_predicates_skip_true(add_to_where, to_where);
        }

        // Keep the accepted ON atoms
        join_expr = split.on_expr;
    }

    derived_table_stmt.lateral = true;

    let join_clause = match join_kind {
        DeriveTableJoinKind::Join(join_operator) => JoinClause {
            operator: join_operator,
            right: JoinRightSide::Table(derived_table),
            constraint: join_expr.map_or_else(|| JoinConstraint::Empty, JoinConstraint::On),
        },
        DeriveTableJoinKind::AntiJoin => {
            if join_expr.is_some() {
                // `DISTINCT` is unnecessary for anti-join with ON; the left-null filter enforces semi/anti semantics
                derived_table_stmt.distinct = false;
            }
            add_to_where = and_predicates_skip_true(
                add_to_where,
                construct_is_not_null_expr(
                    make_first_field_ref_name(derived_table_stmt, derived_table_alias.clone())?,
                    true,
                ),
            );
            JoinClause {
                operator: LeftOuterJoin,
                right: JoinRightSide::Table(derived_table),
                constraint: join_expr.map_or_else(|| JoinConstraint::Empty, JoinConstraint::On),
            }
        }
    };

    if let JoinConstraint::On(join_expr) = &join_clause.constraint
        && !is_supported_join_condition(join_expr)
    {
        internal!(
            "Unexpected unsupported ON after split_join_condition_for_rhs_and_lhs: {}",
            join_expr.display(Dialect::PostgreSQL)
        );
    }

    base_stmt.join.push(join_clause);

    Ok(if contains_problematic_self_joins(base_stmt) {
        // The only mutation so far was pushing a new JOIN, so pop it to restore `base_stmt`
        base_stmt.join.pop();
        false
    } else {
        if let Some(add_to_where) = add_to_where {
            base_stmt.where_clause =
                and_predicates_skip_true(mem::take(&mut base_stmt.where_clause), add_to_where);
        }
        true
    })
}

fn collect_subquery_predicates(expr: &Expr) -> ReadySetResult<(Vec<Expr>, Option<Expr>)> {
    let mut subquery_predicates = Vec::new();
    let remaining_expr = split_expr_mut(
        expr,
        &mut |constraint| is_supported_subquery_predicate(constraint),
        &mut subquery_predicates,
    );
    Ok((subquery_predicates, remaining_expr))
}

fn find_local_column_not_group_by_key<'a>(
    expr: &'a Expr,
    select_fields: &[FieldDefinitionExpr],
    group_by_fields: &[FieldReference],
    local_from_items: &HashSet<Relation>,
) -> ReadySetResult<Option<&'a Column>> {
    for col in columns_iter(expr) {
        if let Some(table) = &col.table
            && local_from_items.contains(table)
            && find_group_by_key(
                select_fields,
                group_by_fields,
                &Expr::Column((*col).clone()),
                &"".into(),
            )?
            .is_none()
        {
            return Ok(Some(col));
        }
    }
    Ok(None)
}

fn turn_into_not_eq_scalar(subquery_desc: &mut SubqueryPredicateDesc) {
    subquery_desc.negated = false;
    if let Some((_, op)) = &mut subquery_desc.lhs_and_op {
        *op = BinaryOperator::NotEqual;
    }
}

fn unnest_subqueries_in_where(
    stmt: &mut SelectStatement,
    ctx: &mut UnnestContext,
) -> ReadySetResult<RewriteStatus> {
    let Some(where_expr) = &stmt.where_clause else {
        return Ok(NO_REWRITES_STATUS);
    };

    // Collect the subquery predicates combined with AND
    let (subquery_predicates, remaining_expr) = collect_subquery_predicates(where_expr)?;

    if subquery_predicates.is_empty() {
        return Ok(NO_REWRITES_STATUS);
    }

    let mut rewrite_status = RewriteStatus::default();

    stmt.where_clause = remaining_expr;

    for subquery_predicate in subquery_predicates {
        //
        let mut subquery_desc = as_supported_subquery_predicate(&subquery_predicate)?;

        // EARLY SHORT‑CIRCUIT for **definite emptiness**:
        //   • LIMIT 0 detected through wrappers, OR
        //   • aggregate-only/no-GBY with HAVING FALSE.
        if is_definitely_empty_subquery(&subquery_desc.stmt)? {
            match subquery_desc.ctx {
                SubqueryContext::Exists | SubqueryContext::In => {
                    if subquery_desc.negated {
                        // NOT EXISTS(empty) / NOT IN(empty) ⇒ TRUE (drop this conjunct)
                        rewrite_status.rewrite();
                        continue;
                    } else {
                        // EXISTS(empty) / IN(empty) ⇒ FALSE (whole WHERE becomes FALSE)
                        stmt.where_clause = Some(Expr::Literal(false.into()));
                        return Ok(SINGLE_REWRITE_STATUS);
                    }
                }
                SubqueryContext::Scalar => {
                    // WHERE (scalar with zero rows) ⇒ NULL → filtered → FALSE
                    stmt.where_clause = Some(Expr::Literal(false.into()));
                    return Ok(SINGLE_REWRITE_STATUS);
                }
            }
        }

        // Shape-only cardinality (aggregate-only/no-GBY) is **per-outer-row** and safe to use
        // for certain transforms even when correlated (see how `is_exactly_one` flag used down bellow).
        let agg_card_shape = agg_only_no_gby_cardinality(&subquery_desc.stmt)?;
        if let Some(card) = agg_card_shape {
            match card {
                AggNoGbyCardinality::ExactlyOne => {
                    // EXISTS/NOT EXISTS folding is safe regardless of correlation in agg-only no-GBY.
                    if matches!(subquery_desc.ctx, SubqueryContext::Exists) {
                        if subquery_desc.negated {
                            stmt.where_clause = Some(Expr::Literal(false.into()));
                            return Ok(SINGLE_REWRITE_STATUS);
                        } else {
                            // EXISTS(always-one-row) → TRUE (drop this conjunct)
                            rewrite_status.rewrite();
                            continue;
                        }
                    }
                    if matches!(subquery_desc.ctx, SubqueryContext::In if subquery_desc.negated) {
                        // NOT IN(one-row) ≡ !=  (3VL-correct even if RHS is NULL)
                        turn_into_not_eq_scalar(&mut subquery_desc);
                    }
                }
                AggNoGbyCardinality::AtMostOne => {
                    // 0..1 rows: no global constant folds; single-valued RHS path handled below.
                }
                AggNoGbyCardinality::ExactlyZero => {
                    // handled by early short-circuit above
                }
            }
        }

        // WHERE-only relaxation: `IN (subq LIMIT 1)` ⇒ treat as scalar `=` when truly uncorrelated
        // after hoist (global property) and structurally `LIMIT 1` is present.
        if !subquery_desc.negated
            && matches!(subquery_desc.ctx, SubqueryContext::In)
            && has_limit_one_deep(&subquery_desc.stmt)
        {
            subquery_desc.ctx = SubqueryContext::Scalar;
        }

        // **IMPORTANT**: Preserve the agg-only-no_GBY property before un-nesting inside the subquery
        let is_exactly_one = matches!(agg_card_shape, Some(AggNoGbyCardinality::ExactlyOne));

        if unnest_all_subqueries(&mut subquery_desc.stmt, ctx)?.has_rollbacks() {
            stmt.where_clause =
                and_predicates_skip_true(mem::take(&mut stmt.where_clause), subquery_predicate);
            rewrite_status.rollback();
            continue;
        }

        let preserved_subquery_stmt = if matches!(subquery_desc.ctx, SubqueryContext::In if subquery_desc.negated)
        {
            Some(subquery_desc.stmt.clone())
        } else {
            None
        };

        let (mut derived_table, join_on) = as_joinable_derived_table(
            subquery_desc.ctx,
            &mut subquery_desc.stmt,
            get_unique_alias(&collect_local_from_items(stmt)?, "GNL"),
        )?;

        let mut apply_3vl_guard = None;
        let mut add_to_where = None;

        let (subquery_stmt, subquery_stmt_alias) =
            expect_sub_query_with_alias_mut(&mut derived_table);

        let (mut join_op, mut lhs_and_op) = (InnerJoin, subquery_desc.lhs_and_op);

        // ── Single‑valued RHS handling in WHERE (Scalar and non‑negated IN) ───────────────────────────
        // We **only** special‑case the following situation:
        //
        //   • The subquery is aggregate‑only **without GROUP BY** and classified as **ExactlyOne**
        //     (no HAVING or HAVING TRUE);
        //   • Correlation from the subquery WHERE was hoisted (i.e., `join_on.is_some()` is true);
        //   • The analyzer can produce a **mapped RHS** for the first projected field
        //     (currently only `COUNT(*)` / `COUNT(expr)` → `COALESCE(cnt, 0)`).
        //
        // Rationale:
        //   • ExactlyOne guarantees a scalar per outer row. After hoist + grouping by the local keys,
        //     a **missing key** manifests as **no joined row**. For COUNT, the original scalar over an
        //     empty input is **0**, not NULL; for other aggregates the correct empty‑input result is NULL.
        //   • Therefore we flip to **LEFT OUTER JOIN** **only when** we actually use the mapped RHS,
        //     and we push `lhs <op> mapped_rhs` into the **outer WHERE**. Using COALESCE prevents
        //     null‑rejection, so this LEFT does **not** degenerate to INNER.
        //   • In **all other cases** (AtMostOne/HAVING present, or ExactlyOne but non‑COUNT, or
        //     uncorrelated), we do **nothing** here and delegate to `join_derived_table(..)`, which
        //     assembles an **INNER** join and places the comparison in ON/WHERE as appropriate,
        //     ensuring 0‑row cases filter out and preserving original WHERE semantics.
        //
        // Important invariants:
        //   • `build_rhs_expr_for_aggregate_only_derived_table(..)` returns `Some(..)` only for the
        //     COUNT family and `None` for SUM/MIN/MAX/etc.
        //   • We consume `lhs_and_op` (via `mem::take`) in the mapped COUNT case so
        //     `join_derived_table(..)` will not also try to add the comparison to ON.
        //   • We never move ON → WHERE for OUTER joins except in this controlled LEFT+COALESCE path.
        //   • EXISTS/NOT EXISTS and NOT IN 3VL are handled elsewhere.
        if is_exactly_one && lhs_and_op.is_some() && join_on.is_some() && !subquery_desc.negated {
            let first_field =
                make_first_field_ref_name(subquery_stmt, subquery_stmt_alias.clone())?;

            if let Some(rhs_for_where) = build_rhs_expr_for_aggregate_only_derived_table(
                subquery_stmt,
                subquery_stmt_alias.clone(),
                &first_field,
            )? {
                join_op = LeftOuterJoin; // mapped COUNT needs LEFT; COALESCE avoids null‑reject
                let (lhs, op) = mem::take(&mut lhs_and_op).unwrap();
                add_to_where = and_predicates_skip_true(
                    add_to_where,
                    construct_scalar_expr(lhs, op, rhs_for_where),
                ); // enforce original WHERE truth using the mapped RHS
            }
        }

        // Install NOT IN 3VL guard when needed (independent of aggregate-only shape).
        if let Some((lhs, _)) = &lhs_and_op
            && let Some(preserved_subquery_stmt) = preserved_subquery_stmt
        {
            invariant!(matches!(subquery_desc.ctx, SubqueryContext::In if subquery_desc.negated));

            let is_rhs_null_free = is_first_field_null_free(subquery_stmt, ctx.schema)?;
            let rhs_ctx = RhsContext::new(is_rhs_null_free);

            let lhs = lhs.clone();

            apply_3vl_guard = Some(
                move |base_stmt: &mut SelectStatement, ctx: &mut UnnestContext| {
                    let is_lhs_null_free = is_select_expr_null_free(&lhs, base_stmt, ctx.schema)?;
                    if !is_lhs_null_free || !rhs_ctx.is_null_free() {
                        Some(add_3vl_for_not_in_where_subquery(
                            base_stmt,
                            lhs,
                            is_lhs_null_free,
                            preserved_subquery_stmt,
                            rhs_ctx,
                            &mut ctx.probes,
                        ))
                        .transpose()
                    } else {
                        Ok(None)
                    }
                },
            );
        }

        if join_derived_table(
            stmt,
            lhs_and_op,
            derived_table,
            join_on,
            if subquery_desc.negated {
                DeriveTableJoinKind::AntiJoin
            } else {
                DeriveTableJoinKind::Join(join_op)
            },
        )? {
            if let Some(apply_3vl_guard) = apply_3vl_guard
                && let Some(guard_expr) = apply_3vl_guard(stmt, ctx)?
            {
                add_to_where = and_predicates_skip_true(add_to_where, guard_expr);
            }
            if let Some(add_to_where) = add_to_where {
                stmt.where_clause =
                    and_predicates_skip_true(mem::take(&mut stmt.where_clause), add_to_where);
            }
            rewrite_status.rewrite();
        } else {
            stmt.where_clause =
                and_predicates_skip_true(mem::take(&mut stmt.where_clause), subquery_predicate);
            rewrite_status.rollback();
        }
    }

    Ok(rewrite_status)
}

fn collect_outermost_subquery_predicates_mut(expr: &mut Expr) -> ReadySetResult<Vec<&mut Expr>> {
    #[derive(Default)]
    struct SPVisitor<'a> {
        subquery_predicates: Vec<&'a mut Expr>,
    }

    impl<'a> VisitorMut<'a> for SPVisitor<'a> {
        type Error = ReadySetError;

        fn visit_expr(&mut self, expr: &'a mut Expr) -> Result<(), Self::Error> {
            if is_supported_subquery_predicate(expr) {
                self.subquery_predicates.push(expr);
                Ok(())
            } else {
                walk_expr(self, expr)
            }
        }

        fn visit_select_statement(
            &mut self,
            _: &'a mut SelectStatement,
        ) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    let mut visitor = SPVisitor::default();
    visitor.visit_expr(expr)?;

    Ok(visitor.subquery_predicates)
}

fn assert_local_columns_are_grouped(
    expr: &Expr,
    stmt: &SelectStatement,
    local_from_items: &HashSet<Relation>,
) -> ReadySetResult<()> {
    if let Some(group_by) = &stmt.group_by
        && let Some(col) = find_local_column_not_group_by_key(
            expr,
            &stmt.fields,
            &group_by.fields,
            local_from_items,
        )?
    {
        invalid_query!(
            "Subquery uses ungrouped column {} from outer query",
            col.display(Dialect::PostgreSQL)
        )
    }
    Ok(())
}

fn unnest_subqueries_in_fields(
    stmt: &mut SelectStatement,
    ctx: &mut UnnestContext,
) -> ReadySetResult<RewriteStatus> {
    let mut rewrite_status = RewriteStatus::default();

    let mut stmt_fields = Vec::new();

    for mut current_field in mem::take(&mut stmt.fields).into_iter() {
        let (field_expr, _) = expect_field_as_expr_mut(&mut current_field);

        for subquery_predicate in collect_outermost_subquery_predicates_mut(field_expr)? {
            let mut subquery_desc = as_supported_subquery_predicate(subquery_predicate)?;

            // EARLY SHORT-CIRCUIT for **definite emptiness** in SELECT-list:
            //   • LIMIT 0 detected through wrappers, OR
            //   • aggregate-only/no-GBY with HAVING FALSE.
            if is_definitely_empty_subquery(&subquery_desc.stmt)? {
                let replacement = match subquery_desc.ctx {
                    // EXISTS(empty) ⇒ FALSE; NOT EXISTS(empty) ⇒ TRUE
                    SubqueryContext::Exists => Expr::Literal((subquery_desc.negated).into()),
                    // IN(empty) ⇒ FALSE; NOT IN(empty) ⇒ TRUE
                    SubqueryContext::In => Expr::Literal((subquery_desc.negated).into()),
                    // Scalar with zero rows yields NULL as a value
                    SubqueryContext::Scalar => Expr::Literal(Literal::Null),
                };
                let _ = mem::replace(subquery_predicate, replacement);
                rewrite_status.rewrite();
                continue;
            }

            // Shape-level cardinality for aggregate-only/no-GBY: safe per outer row even if correlated.
            let agg_card_shape = agg_only_no_gby_cardinality(&subquery_desc.stmt)?;
            match agg_card_shape {
                Some(AggNoGbyCardinality::ExactlyOne) => {
                    if matches!(subquery_desc.ctx, SubqueryContext::Exists) {
                        let _ = mem::replace(
                            subquery_predicate,
                            Expr::Literal((!subquery_desc.negated).into()),
                        );
                        rewrite_status.rewrite();
                        continue;
                    }
                    if matches!(subquery_desc.ctx, SubqueryContext::In if subquery_desc.negated) {
                        // NOT IN(one-row) ≡ != (3VL-correct)
                        turn_into_not_eq_scalar(&mut subquery_desc);
                    }
                }
                Some(AggNoGbyCardinality::AtMostOne) => { /* no single-row constant transforms */ }
                Some(AggNoGbyCardinality::ExactlyZero) => { /* handled earlier by emptiness short-circuit */
                }
                None => { /* not aggregate-only/no-GBY: no shape-based folds here */ }
            }

            // Exactly-one per outer row (agg-only/no-GBY) lets IN reduce to scalar compare in SELECT-list
            // without 3VL probes; use shape to drive this, not correlation.
            let is_exactly_one = matches!(agg_card_shape, Some(AggNoGbyCardinality::ExactlyOne));

            if unnest_all_subqueries(&mut subquery_desc.stmt, ctx)?.has_rollbacks() {
                rewrite_status.rollback();
                continue;
            }

            let preserved_subquery_stmt = if matches!(subquery_desc.ctx, SubqueryContext::In) {
                Some(subquery_desc.stmt.clone())
            } else {
                None
            };

            let local_from_items = collect_local_from_items(stmt)?;

            let (derived_table, join_on) = as_joinable_derived_table(
                subquery_desc.ctx,
                &mut subquery_desc.stmt,
                get_unique_alias(&local_from_items, "GNL"),
            )?;

            if let Some(join_on_expr) = &join_on {
                assert_local_columns_are_grouped(join_on_expr, stmt, &local_from_items)?;
            }

            let (subquery_stmt, subquery_stmt_alias) = expect_sub_query_with_alias(&derived_table);

            let first_field =
                make_first_field_ref_name(subquery_stmt, subquery_stmt_alias.clone())?;

            // NOTE: Only apply COALESCE-style mapping in SELECT-list when the subquery is
            // aggregate-only/no-GBY **and** classified as ExactlyOne. For AtMostOne (HAVING
            // may filter the group out) we must preserve NULL (no mapping). Also skip mapping
            // entirely when uncorrelated (join_on.is_none()).
            let rhs = if is_exactly_one && join_on.is_some() {
                build_rhs_expr_for_aggregate_only_derived_table(
                    subquery_stmt,
                    subquery_stmt_alias.clone(),
                    &first_field,
                )?
                .unwrap_or(first_field)
            } else {
                first_field
            };

            let mut lhs_and_op = None;

            let replace_subquery_predicate_with =
                if matches!(subquery_desc.ctx, SubqueryContext::Exists) {
                    // EXISTS in SELECT-list → use presence via left-join null extension
                    Either::Left(construct_is_not_null_expr(rhs, subquery_desc.negated))
                } else if let Some((lhs, op)) = subquery_desc.lhs_and_op {
                    invariant!(matches!(
                        subquery_desc.ctx,
                        SubqueryContext::Scalar | SubqueryContext::In
                    ));
                    if matches!(subquery_desc.ctx, SubqueryContext::Scalar) {
                        // Scalar context: single value (or NULL) per outer row suffices regardless of agg shape.
                        // Keep comparison as expression; LEFT JOIN ensures zero-row ⇒ rhs NULL.
                        Either::Left(construct_scalar_expr(lhs, op, rhs))
                    } else {
                        // IN / NOT IN context
                        if is_exactly_one {
                            // Exactly one row guaranteed: IN(one-row) ≡ (lhs <op> rhs), where <op> may be NotEqual
                            // if we earlier transformed NOT IN(one-row) → !=
                            Either::Left(construct_scalar_expr(lhs, op, rhs))
                        } else {
                            // 0..1 rows (aggregate-only with non-constant HAVING) **or** non-aggregate:
                            // need a presence-sensitive 3VL guard (cannot just do lhs = rhs since 0 rows ⇒ FALSE, not NULL).
                            // Build equality probe in JOIN and compute guard using preserved RHS stmt.
                            lhs_and_op = Some((lhs.clone(), BinaryOperator::Equal));

                            let is_rhs_null_free =
                                is_first_field_null_free(subquery_stmt, ctx.schema)?;
                            let rhs_ctx = RhsContext::new(is_rhs_null_free);
                            let is_not_in = subquery_desc.negated;

                            Either::Right(
                                move |base_stmt: &mut SelectStatement, ctx: &mut UnnestContext| {
                                    let is_lhs_null_free =
                                        is_select_expr_null_free(&lhs, base_stmt, ctx.schema)?;
                                    if is_lhs_null_free && rhs_ctx.is_null_free() {
                                        // With left-joined equality, membership reduces to "rhs IS [NOT] NULL"
                                        Ok(construct_is_not_null_expr(rhs.clone(), is_not_in))
                                    } else {
                                        add_3vl_for_select_list_in_subquery(
                                            base_stmt,
                                            SelectList3vlInput {
                                                lhs,
                                                rhs: rhs.clone(),
                                                preserved_rhs_stmt: preserved_subquery_stmt
                                                    .expect("Should be Some"),
                                                rhs_ctx,
                                                flags: SelectList3vlFlags {
                                                    is_lhs_null_free,
                                                    is_not_in,
                                                },
                                            },
                                            &mut ctx.probes,
                                        )
                                    }
                                },
                            )
                        }
                    }
                } else {
                    // Scalar subquery as a value; no comparison on LHS
                    invariant!(matches!(subquery_desc.ctx, SubqueryContext::Scalar));
                    Either::Left(rhs)
                };

            if join_derived_table(
                stmt,
                lhs_and_op,
                derived_table,
                join_on,
                DeriveTableJoinKind::Join(LeftOuterJoin),
            )? {
                let _ = mem::replace(
                    subquery_predicate,
                    match replace_subquery_predicate_with {
                        Either::Left(expr) => expr,
                        Either::Right(f) => f(stmt, ctx)?,
                    },
                );
                rewrite_status.rewrite();
            } else {
                rewrite_status.rollback();
            }
        }

        stmt_fields.push(current_field);
    }

    stmt.fields = stmt_fields;

    Ok(rewrite_status)
}

// **NOTE**: The order of unnesting is important:
//  - WHERE should always be done first, as the replacement joins are simulating rows filtering,
//  - The select-list should apply after, as their replacement joins never filter
//  - LATERAL should be the last operation.
pub(crate) fn unnest_all_subqueries(
    stmt: &mut SelectStatement,
    ctx: &mut UnnestContext,
) -> ReadySetResult<RewriteStatus> {
    let status1 = unnest_subqueries_in_where(stmt, ctx)?;
    let status2 = unnest_subqueries_in_fields(stmt, ctx)?;
    let status3 = unnest_lateral_subqueries(stmt, ctx)?;
    Ok(status1.combine(status2).combine(status3))
}
