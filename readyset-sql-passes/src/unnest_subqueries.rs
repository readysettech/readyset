use crate::derived_tables_rewrite::promote_null_rejecting_outer_joins_where;
use crate::drop_redundant_join::{UniqueColumnsSchema, UniqueColumnsSchemaImpl};
use crate::inline_subquery::limit_clause_as_numbers;
use crate::lateral_join::unnest_lateral_subqueries;
use crate::rewrite_utils::{
    NO_REWRITES_STATUS, OnAtom, RewriteStatus, SINGLE_REWRITE_STATUS,
    align_group_by_and_windows_with_correlation, analyse_lone_aggregates_subquery_fields,
    and_predicates_skip_true, are_group_by_keys_pinned_by_correlation, as_sub_query_with_alias,
    as_sub_query_with_alias_mut, bubble_alias_to_anchor_top, classify_on_atom,
    collect_local_from_items, columns_iter, conjoin_all_dedup, construct_null_check_expr,
    construct_projecting_wrapper, construct_scalar_expr, contains_nested_select, contains_select,
    correlated_relations, decompose_conjuncts, default_alias_for_select_item_expression,
    ensure_first_field_alias, expect_field_as_expr, expect_field_as_expr_mut,
    expect_only_subquery_from_with_alias, expect_only_subquery_from_with_alias_mut,
    expect_sub_query_with_alias, expect_sub_query_with_alias_mut,
    extract_aggregate_fallback_for_expr, extract_correlation_keys, find_group_by_key,
    find_rhs_join_clause, for_each_window_function, get_from_item_reference_name,
    get_select_item_alias, get_unique_alias, has_alias, is_aggregate_only_without_group_by,
    is_aggregated_expr, is_aggregated_select, is_literal_one, is_literal_positive, is_literal_zero,
    make_first_field_ref_name, move_correlated_constraints_from_join_to_where,
    partition_correlated_predicates, preserve_uncorrelated_top_k, project_statement_columns_if,
    resolve_field_expr_by_alias, resolve_group_by_exprs, rewrite_top_k_in_place,
    rewrite_top_k_in_place_with_partition, split_expr_mut,
};
use crate::unnest_subqueries_3vl::{
    ProbeRegistry, RhsContext, SelectList3vlFlags, SelectList3vlInput,
    add_3vl_for_not_in_where_subquery, add_3vl_for_select_list_in_subquery,
    is_first_field_null_free, is_select_expr_null_free,
};
use crate::util::is_correlated;
use crate::{
    BaseSchemasContext, get_local_from_items_iter, get_local_from_items_iter_mut, is_column_of,
};
use itertools::{Either, Itertools};
use readyset_errors::{
    ReadySetError, ReadySetResult, internal, invalid_query_err, invariant, unsupported,
};

use readyset_sql::analysis::{
    is_aggregate,
    visit::Visitor,
    visit_mut::{VisitorMut, walk_expr},
};
use readyset_sql::ast::JoinOperator::{InnerJoin, LeftOuterJoin};
use readyset_sql::ast::{
    BinaryOperator, Column, Expr, FieldDefinitionExpr, FieldReference, FunctionExpr, InValue,
    JoinClause, JoinConstraint, JoinOperator, LimitClause, Literal, SelectStatement, SqlIdentifier,
    TableExpr, UnaryOperator,
};
use readyset_sql::ast::{JoinRightSide, Relation, TableExprInner};
use readyset_sql::{Dialect, DialectDisplay};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::iter;
use std::mem;
use tracing::trace;

/// True when the engine's `extract_limit_offset`
/// (`readyset-server/src/controller/sql/query_graph.rs`) accepts this
/// `LimitClause` in subquery pagination position.  Mirrors the engine's
/// acceptance set so the SQL rewrite pipeline preserves LIMIT only when
/// the engine can lower it; engine-incompatible shapes route through
/// `rewrite_top_k_in_place` and reach the engine as materialised
/// `__rn`-filter predicates.
///
/// Eligible shapes:
///   - LIMIT: literal non-negative integer (`Integer` / `UnsignedInteger`).
///   - OFFSET: absent or literal `0`.
///
/// `Literal::Placeholder` cannot appear in LIMIT/OFFSET at this point in
/// the pipeline: question-mark placeholders short-circuit the entire
/// rewrite pipeline at the adapter gate (`adapter_rewrites/mod.rs`), and
/// dollar-number / colon-number placeholders are synthesised by
/// autoparameterisation which runs after this pass.
/// `limit_clause_as_numbers` returns `Err` on any non-numeric literal
/// regardless, routing them to materialisation.
fn limit_clause_eligible_for_native_pagination(limit_clause: &LimitClause) -> bool {
    matches!(limit_clause_as_numbers(limit_clause), Ok((_, 0)))
}

#[derive(Default, Copy, Clone, Debug)]
pub(crate) enum SubqueryContext {
    #[default]
    In,
    Scalar,
    Exists,
}

#[derive(Default)]
pub(crate) struct SubqueryPredicateDesc {
    pub(crate) ctx: SubqueryContext,
    pub(crate) negated: bool,
    pub(crate) lhs_and_op: Option<(Expr, BinaryOperator)>,
    pub(crate) stmt: SelectStatement,
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

pub(crate) struct UnnestContext<'a, U: UniqueColumnsSchema> {
    pub(crate) schema: &'a dyn NonNullSchema,
    pub(crate) unique_cols_schema: &'a U,
    pub(crate) probes: ProbeRegistry,
    // Pre-hoist hints keyed by LATERAL alias (as Relations)
    pub(crate) pre_hoist_lateral_exactly_one: HashSet<Relation>,
    /// LATERAL aliases whose body is correlation-pinned GROUP BY (or
    /// otherwise AtMostOne per outer row).  Distinct from
    /// `pre_hoist_lateral_exactly_one` because these bodies produce
    /// ZERO rows (not "one row with default values") for outer rows
    /// without a match — so the existing COALESCE-zero / LEFT-OUTER-
    /// JOIN promotion in `get_join_operator_for_lateral` MUST NOT be
    /// applied to them.
    pub(crate) pre_hoist_lateral_at_most_one: HashSet<Relation>,
    pub(crate) lateral_trivial_on: HashSet<Relation>,
    /// Relations from ancestor LATERAL scopes, for correlation
    /// detection. Each nesting level pushes its preceding items
    /// before recursing, pops on return.
    pub(crate) ancestor_scope: HashSet<Relation>,
    /// Same relations in left-to-right FROM order, for ON
    /// normalization LHS selection via
    /// `split_on_for_rhs_against_preceding_lhs`.
    pub(crate) ancestor_scope_ordered: Vec<Relation>,
}

pub trait UnnestSubqueries: Sized {
    /// Rewrites the subqueries in FROM, WHERE and the select list into joins.
    ///
    /// On `Err` the statement may be left in an unspecified state rather than as it was given:
    /// a refusal reached before the first mutation leaves it untouched, but the pass
    /// takes the select list apart and splices each join in as it goes, and a refusal reached
    /// after the statement has been moved into a wrapper leaves it empty -- no fields, no FROM.
    /// A caller that needs the original on a refusal keeps its own copy.
    fn unnest_subqueries<U: UniqueColumnsSchema>(
        &mut self,
        schema: &dyn NonNullSchema,
        unique_cols_schema: &U,
    ) -> ReadySetResult<&mut Self>;
}

impl UnnestSubqueries for SelectStatement {
    fn unnest_subqueries<U: UniqueColumnsSchema>(
        &mut self,
        schema: &dyn NonNullSchema,
        unique_cols_schema: &U,
    ) -> ReadySetResult<&mut Self> {
        // This pass only rewrites subqueries in FROM, WHERE and the select list.
        if !contains_nested_select(self) {
            return Ok(self);
        }
        if unnest_subqueries_main(self, schema, unique_cols_schema)?.has_rewrites() {
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
pub(crate) fn unnest_subqueries_main<U: UniqueColumnsSchema>(
    stmt: &mut SelectStatement,
    schema: &dyn NonNullSchema,
    unique_cols_schema: &U,
) -> ReadySetResult<RewriteStatus> {
    let mut ctx = UnnestContext {
        schema,
        unique_cols_schema,
        probes: ProbeRegistry::new(),
        pre_hoist_lateral_exactly_one: HashSet::new(),
        pre_hoist_lateral_at_most_one: HashSet::new(),
        lateral_trivial_on: HashSet::new(),
        ancestor_scope: HashSet::new(),
        ancestor_scope_ordered: Vec::new(),
    };

    let mut rewrite_status = RewriteStatus::default();

    // Collect LATERAL hints from the **pre-hoist** AST.  Pre-hoist
    // visibility is load-bearing for the ExactlyOne classification
    // (aggregate-only-no-GROUP-BY) on TOP-K-bearing bodies:
    // `rewrite_top_k_for_lateral` (called by the hoister below)
    // materializes LIMIT/ORDER via partitioned `ROW_NUMBER`, which
    // injects a window function into SELECT — destroying the
    // aggregate-only-no-GROUP-BY shape that consumers like
    // `get_join_operator_for_lateral`'s COALESCE-zero promotion rely on.
    // The `lateral_trivial_on` classification (outer-wrap JOIN ON
    // shape) is also captured here.
    collect_lateral_hints(stmt, &mut ctx)?;

    // Run the hoister; it also handles TOP-K in nested derived tables
    if hoist_correlated_from_nested_and_rewrite_top_k(stmt)? {
        rewrite_status.rewrite();
    }

    // Refresh LATERAL hints against the **post-hoist** AST.  Set
    // inserts are additive — the pre-hoist call's entries are
    // preserved; this call only adds.  Both timings cover different
    // structural concerns:
    //   - ExactlyOne: aggregate-only-no-GBY shape is captured pre-hoist
    //     (the hoist may demote via TOP-K materialization but never
    //     promotes a non-agg body to agg).
    //   - AtMostOne: `is_correlation_pinned_at_most_one` internally
    //     clones the body and runs `move_correlated_constraints_from_
    //     join_to_where` before classifying, so JOIN-ON correlation is
    //     admitted at either timing.  The post-hoist call still helps
    //     when the hoist enables a previously-blocked classification
    //     (e.g., new groupings exposed by TOP-K materialization).
    //   - trivial_on: outer-wrap JOIN ON, untouched by either timing.
    //
    // Background: closes the I3 pass-ordering gap documented in
    // `known_core_limitations.md` §5.5.
    collect_lateral_hints(stmt, &mut ctx)?;

    // Rewrite supported cases of subqueries found in `lateral` FROM, WHERE and select list
    let rewrite_status_1 = unnest_all_subqueries(stmt, &mut ctx)?;

    Ok(rewrite_status.combine(rewrite_status_1))
}

/// A [`NonNullSchema`] over a catalog, derived for each relation as it is looked up.
pub(crate) struct NonNullSchemaImpl<C> {
    ctx: C,
}

impl<C: BaseSchemasContext> From<C> for NonNullSchemaImpl<C> {
    fn from(ctx: C) -> Self {
        NonNullSchemaImpl { ctx }
    }
}

impl<C: BaseSchemasContext> NonNullSchema for NonNullSchemaImpl<C> {
    fn not_null_columns_of(&self, rel: &Relation) -> HashSet<Column> {
        UniqueColumnsSchema::not_null_columns_of(&UniqueColumnsSchemaImpl::from(&self.ctx), rel)
            .unwrap_or_default()
    }
}

fn is_outer_from_item(from_item: &Relation, local_from_items: &HashSet<Relation>) -> bool {
    !local_from_items.contains(from_item)
}

fn split_correlated_expr(
    expr: &Expr,
    local_from_items: &HashSet<Relation>,
) -> (Option<Expr>, Option<Expr>) {
    partition_correlated_predicates(expr, &|rel| is_outer_from_item(rel, local_from_items))
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

/// A window function is duplicate-sensitive when its output depends on the exact
/// row multiplicity fed to the window (as opposed to the set of distinct values).
/// MIN and MAX are duplicate-insensitive: repeating rows does not change their
/// output.  Everything else — COUNT, SUM, AVG, and ranking functions like RANK,
/// ROW_NUMBER, DENSE_RANK, LEAD/LAG, etc. — is duplicate-sensitive.
///
/// `wf_expr` must be `Expr::WindowFunction`; anything else is not a WF and the
/// classifier returns `false`.
fn wf_is_duplicate_sensitive(wf_expr: &Expr) -> bool {
    let Expr::WindowFunction { function, .. } = wf_expr else {
        return false;
    };
    !matches!(function, FunctionExpr::Min(_) | FunctionExpr::Max(_))
}

/// True when any SELECT-list field contains a duplicate-sensitive window function.
/// Guard used by `make_subquery_distinct` before dropping `stmt.group_by`: if a
/// duplicate-sensitive WF is present, dropping GROUP BY changes the WF's input
/// row multiplicity and would produce different output values.
fn contains_duplicate_sensitive_window_function(stmt: &SelectStatement) -> ReadySetResult<bool> {
    for fe in &stmt.fields {
        let (expr, _) = expect_field_as_expr(fe);
        let mut found = false;
        for_each_window_function(expr, &mut |wf| {
            if wf_is_duplicate_sensitive(wf) {
                found = true;
            }
        })?;
        if found {
            return Ok(true);
        }
    }
    Ok(false)
}

/// The outermost subquery predicates of `expr`, each paired with whether no aggregate encloses
/// it.  A subquery an aggregate encloses is read by that aggregate, where an empty relation
/// yields NULL upstream too; one beside it is read on the row the aggregate emits.  A window
/// function's own call is not an enclosing aggregate, which is the reading `is_aggregated_expr`
/// takes of the same question.
fn collect_subquery_predicates_with_placement_mut(
    expr: &mut Expr,
) -> ReadySetResult<Vec<(bool, &mut Expr)>> {
    struct PlacementVisitor<'a> {
        enclosing_aggregates: usize,
        found: Vec<(bool, &'a mut Expr)>,
    }

    impl<'a> VisitorMut<'a> for PlacementVisitor<'a> {
        type Error = ReadySetError;

        fn visit_expr(&mut self, expr: &'a mut Expr) -> Result<(), Self::Error> {
            if is_supported_subquery_predicate(expr) {
                self.found.push((self.enclosing_aggregates == 0, expr));
                return Ok(());
            }
            let encloses = matches!(expr, Expr::Call(function) if is_aggregate(function));
            if encloses {
                self.enclosing_aggregates += 1;
            }
            let walked = walk_expr(self, expr);
            if encloses {
                self.enclosing_aggregates -= 1;
            }
            walked
        }

        fn visit_select_statement(
            &mut self,
            _: &'a mut SelectStatement,
        ) -> Result<(), Self::Error> {
            // Predicates written inside a subquery belong to it, not to the statement
            // projecting it, and an aggregate out here does not enclose them.
            Ok(())
        }
    }

    let mut vis = PlacementVisitor {
        enclosing_aggregates: 0,
        found: Vec::new(),
    };
    vis.visit_expr(expr)?;
    Ok(vis.found)
}

/// Whether a client parameter is bound anywhere hoisting would enclose: in `stmt`, which becomes
/// the derived table the wrapper selects from, or in the `hoisted` fields it lifts out above it.
fn hoist_encloses_a_parameter(
    stmt: &SelectStatement,
    hoisted: &[(usize, FieldDefinitionExpr)],
) -> bool {
    struct FindPlaceholder;
    // The walk answers by stopping: one placeholder settles the question, and `Err` is the only
    // way a visitor says so.
    impl<'ast> Visitor<'ast> for FindPlaceholder {
        type Error = ();

        fn visit_literal(&mut self, literal: &'ast Literal) -> Result<(), Self::Error> {
            if literal.is_placeholder() {
                Err(())
            } else {
                Ok(())
            }
        }
    }
    FindPlaceholder.visit_select_statement(stmt).is_err()
        || hoisted.iter().any(|(_, field)| {
            FindPlaceholder
                .visit_expr(expect_field_as_expr(field).0)
                .is_err()
        })
}

/// Whether anything but the ORDER BY aggregates.  A statement whose only aggregate is written in
/// its ordering is held to one row by that ordering alone, so the ordering cannot be dropped.
fn aggregates_outside_order_by(stmt: &SelectStatement) -> ReadySetResult<bool> {
    if let Some(having) = &stmt.having
        && is_aggregated_expr(having)?
    {
        return Ok(true);
    }
    for field in &stmt.fields {
        if is_aggregated_expr(expect_field_as_expr(field).0)? {
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
            if !is_aggregated_select(stmt)? && !contains_duplicate_sensitive_window_function(stmt)?
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
    // True when `stmt` is a FROM-position derived-table subquery whose LIMIT
    // (if uncorrelated) is eligible for native MIR lowering — see
    // `preserve_uncorrelated_top_k`.  False when called from the predicate-
    // subquery path (`as_joinable_derived_table_with_opts` for IN / EXISTS /
    // Scalar): the predicate-rewrite logic that follows depends on
    // `make_subquery_distinct` to enforce set semantics over the subquery's
    // output, which is incompatible with retaining the LIMIT inside.
    top_k_in_subquery_position: bool,
    mut check_for_local_cols_eq_group_keys: impl FnMut(bool) -> ReadySetResult<()>,
    mut check_for_uncorrelated_where: impl FnMut(&mut SelectStatement) -> ReadySetResult<()>,
) -> ReadySetResult<Option<Expr>> {
    // Strip redundant LIMIT/ORDER BEFORE the correlation hoist.  At this point
    // the subquery is still in its original shape — `agg_only_no_gby_cardinality`
    // correctly classifies it as ExactlyOne/AtMostOne.  After the hoist,
    // `align_group_by_and_windows_with_correlation` may inject a GROUP BY (for
    // correlated aggregates), which would make the subquery no longer
    // aggregate-only-no-GBY, and the strip would miss the redundant LIMIT.
    //
    // NOTE: when called from `as_joinable_derived_table_with_opts` (line 1068),
    // this strip is redundant — an earlier strip already ran on the base stmt
    // before `hoist_correlated_from_nested_and_rewrite_top_k`.  It remains
    // necessary here for the other call site: inner FROM-item subqueries
    // processed by `hoist_correlated_from_nested_and_rewrite_top_k` (line 596),
    // which calls this function directly without going through
    // `as_joinable_derived_table_with_opts`.
    strip_redundant_limit_for_agg_no_gby(stmt)?;

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
            let cols_set = extract_correlation_keys(corr, locals)?;

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

            // (5) apply TOP-K if present (redundant agg-only LIMIT already stripped at entry)
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

    // Apply TOP-K if present (redundant agg-only LIMIT already stripped at entry).
    // Gated by `preserve_uncorrelated_top_k()` AND the caller's position flag
    // AND the per-query pagination-eligibility check: when all three indicate
    // preservation, leave LIMIT/ORDER on the (FROM-position) uncorrelated
    // subquery so MIR can lower it to a native `TopK`/`Paginate` node instead
    // of materialising a `ROW_NUMBER() <= K` filter wrapper.
    //
    // The two callers set `top_k_in_subquery_position` to encode their context:
    //
    // - The FROM-loop in `hoist_correlated_from_nested_and_rewrite_top_k`
    //   passes `true`.  This is the load-bearing setting for FROM-position
    //   preservation.
    //
    // - `as_joinable_derived_table_with_opts` (predicate-subquery context: IN,
    //   NOT IN, Scalar) passes `false`.  Today this is a defensive backstop:
    //   the predicate-subquery entry point pre-wraps LIMIT-bearing eligible
    //   subqueries into a FROM-position derived table BEFORE reaching this
    //   function, so the stmt at this point either has no LIMIT (pre-wrap
    //   fired) or is in a case that must materialise anyway (correlated, or
    //   engine-incompatible pagination).  The `false` setting ensures
    //   materialisation if a future path bypasses the pre-wrap and reaches
    //   here with a preservation-eligible LIMIT still in place.
    //
    // Engine-incompatible pagination shapes (literal non-zero OFFSET,
    // parameterised LIMIT, NULL literals, etc.) fall back to materialisation
    // via the eligibility check so they reach the engine as `__rn`-filter
    // predicates rather than tripping `extract_limit_offset`'s `unsupported!`
    // exits at recipe-extend time.
    let preserve_limit = top_k_in_subquery_position
        && preserve_uncorrelated_top_k()
        && limit_clause_eligible_for_native_pagination(&stmt.limit_clause);
    if !stmt.limit_clause.is_empty() && !preserve_limit {
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
        if s.limit_clause.limit().is_some_and(is_literal_zero) {
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
        if is_aggregate_only_without_group_by(stmt)?
            && matches!(
                agg_only_no_gby_cardinality(stmt)?,
                Some(AggNoGbyCardinality::ExactlyZero)
            )
        {
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

/// If this SELECT is aggregate-only without GROUP BY for a reason other than its ordering, and
/// produces at most one row (ExactlyOne or AtMostOne), LIMIT and ORDER BY are redundant -- you
/// can't order or limit a single row.  Strip them to prevent unnecessary TOP-K (ROW_NUMBER)
/// materialization and to avoid blocking downstream optimizations (e.g., DISTINCT
/// normalization checks `order.is_some()` as a TOP-K signal).
///
/// Returns `true` if LIMIT or ORDER BY were stripped.  Refuses the statement instead where the
/// ordering is what holds it to one row, which stripping would make false.
fn strip_redundant_limit_for_agg_no_gby(stmt: &mut SelectStatement) -> ReadySetResult<bool> {
    let holds_one_row = |card: AggNoGbyCardinality| {
        matches!(
            card,
            AggNoGbyCardinality::ExactlyOne | AggNoGbyCardinality::AtMostOne
        )
    };
    if (!stmt.limit_clause.is_empty() || stmt.order.is_some())
        && agg_only_no_gby_cardinality(stmt)?.is_some_and(holds_one_row)
    {
        // The one row can be the ordering's own doing: an aggregate written only there is what
        // holds the statement to it, and stripping the ordering would leave a plain scan behind.
        // Read the shape again without the ordering rather than assume it survives.  Only worth
        // asking when there is an ordering to remove: with none, the shape is the one the
        // classification above already read.
        if let Some(ordering) = stmt.order.take() {
            let without_ordering = agg_only_no_gby_cardinality(stmt);
            if !matches!(&without_ordering, Ok(card) if card.is_some_and(holds_one_row)) {
                stmt.order = Some(ordering);
                without_ordering?;
                unsupported!("subquery held to one row by an aggregate written only in ORDER BY");
            }
        }
        stmt.limit_clause = Default::default();
        Ok(true)
    } else {
        Ok(false)
    }
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

    // Aggregate-only/no-GBY produces at most 1 row — LIMIT/ORDER are redundant.
    // Strip them to avoid unnecessary ROW_NUMBER materialization.
    if strip_redundant_limit_for_agg_no_gby(stmt)? {
        return Ok(false); // no TOP-K rewrite needed
    }

    // Inspect correlation in the inner WHERE but DO NOT hoist it; use it only
    // to align windows and to derive PARTITION BY for TOP‑K.
    if let Some(where_expr) = &stmt.where_clause {
        let (maybe_corr, _remaining) = split_correlated_expr(where_expr, stmt_locals);

        if let Some(corr) = maybe_corr {
            // Build (local_col, outer_col) set and align before TOP‑K
            let cols_set = extract_correlation_keys(&corr, stmt_locals)?;
            align_group_by_and_windows_with_correlation(stmt, &cols_set)?;

            // Apply partitioned TOP‑K if present
            if !stmt.limit_clause.is_empty() {
                rewrite_correlated_top_k_in_place(stmt, &cols_set)?;
                return Ok(true);
            }
        }
    }

    // No WHERE, or no correlation in WHERE, but TOP‑K present → global RN.
    // Gated by `preserve_uncorrelated_top_k()` and the per-query pagination-
    // eligibility check: when both indicate we should preserve, leave
    // LIMIT/ORDER on the (uncorrelated) LATERAL body so MIR can lower it
    // natively.  Engine-incompatible pagination shapes (literal non-zero
    // OFFSET, parameterised LIMIT, etc.) fall back to materialisation so
    // they reach the engine as `__rn`-filter predicates rather than tripping
    // `extract_limit_offset`'s `unsupported!` exits at recipe-extend time.
    let preserve_limit = preserve_uncorrelated_top_k()
        && limit_clause_eligible_for_native_pagination(&stmt.limit_clause);
    if !stmt.limit_clause.is_empty() && !preserve_limit {
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

    // Clear the LEFT-OUTER hoist barrier for spuriously-outer joins that attach a
    // subquery derived table: when a downstream null-rejecting predicate proves the
    // RHS present, the join is equivalent to INNER, so the correlated subquery can
    // hoist normally instead of bailing below. Restricted to subquery-RHS joins (the
    // only shapes that reach the barrier) so base-table LOJs are left untouched for
    // the dedicated post-unnest pass. Local -- this function recurses per level.
    if promote_null_rejecting_outer_joins_where(
        stmt,
        |s, i| matches!(&s.join[i].right, JoinRightSide::Table(te) if as_sub_query_with_alias(te).is_some()),
    )? {
        any_rewrite = true;
    }

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
                    // FROM-position derived table: eligible for LIMIT-preservation
                    // when `preserve_uncorrelated_top_k()` returns `true`.
                    true,
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
    // 1) This-level: classic aggregate-only without GROUP BY
    if is_aggregate_only_without_group_by(stmt)? {
        // Aggregate-only SELECT produce at most one row. Any OFFSET > 0 or LIMIT 0
        // eliminates that single row, so the cardinality is ExactlyZero.
        if stmt.limit_clause.limit().is_some_and(is_literal_zero) {
            return Ok(Some(AggNoGbyCardinality::ExactlyZero));
        }
        if stmt.limit_clause.offset().is_some_and(is_literal_positive) {
            return Ok(Some(AggNoGbyCardinality::ExactlyZero));
        }
        return Ok(match &stmt.having {
            None => Some(AggNoGbyCardinality::ExactlyOne),
            Some(Expr::Literal(Literal::Boolean(true))) => Some(AggNoGbyCardinality::ExactlyOne),
            Some(Expr::Literal(Literal::Boolean(false))) => Some(AggNoGbyCardinality::ExactlyZero),
            // Any non-literal-boolean HAVING may filter or not ⇒ 0 or 1 rows.
            Some(_) => Some(AggNoGbyCardinality::AtMostOne),
        });
    }

    // 2) Wrapper pass-through: if this SELECT is a projection-only wrapper over a single
    //    subquery FROM item (no GROUP BY at this level), try to inherit the inner
    //    aggregate-only/no-GBY cardinality, provided the wrapper cannot *increase* cardinality.
    //
    //    Safe wrapper shapes:
    //      - no GROUP BY (already ensured)
    //      - WHERE absent or literal TRUE  → preserves inner cardinality
    //      - WHERE literal FALSE            → forces ExactlyZero
    //      - LIMIT 0 or OFFSET > 0          → forces ExactlyZero (only one inner row at most)
    //      - LIMIT 1 / ORDER / DISTINCT     → do not increase cardinality; preserve inner
    //
    //    If there is a non-trivial WHERE (not a literal), the wrapper can only *reduce*
    //    the inner 0/1 row to 0, so we conservatively degrade ExactlyOne→AtMostOne.
    if stmt.group_by.is_none()
        && let Ok((inner, _alias)) = expect_only_subquery_from_with_alias(stmt)
    {
        // This-level LIMIT/OFFSET checks for quick zero-row outcome.
        if stmt.limit_clause.limit().is_some_and(is_literal_zero) {
            return Ok(Some(AggNoGbyCardinality::ExactlyZero));
        }
        if stmt.limit_clause.offset().is_some_and(is_literal_positive) {
            return Ok(Some(AggNoGbyCardinality::ExactlyZero));
        }

        // Classify the inner; if it's not agg-only/no-GBY, we can't help here.
        let inner_card = agg_only_no_gby_cardinality(inner)?;
        if let Some(mut card) = inner_card {
            // Wrapper WHERE effects
            match &stmt.where_clause {
                None => { /* preserve inner card */ }
                Some(Expr::Literal(Literal::Boolean(true))) => { /* preserve */ }
                Some(Expr::Literal(Literal::Boolean(false))) => {
                    return Ok(Some(AggNoGbyCardinality::ExactlyZero));
                }
                Some(_) => {
                    // Non-trivial filter: inner ExactlyOne/AtMostOne becomes AtMostOne;
                    // inner ExactlyZero stays ExactlyZero.
                    if matches!(card, AggNoGbyCardinality::ExactlyOne) {
                        card = AggNoGbyCardinality::AtMostOne;
                    }
                }
            }
            return Ok(Some(card));
        }
    }

    // Not an agg-only/no-GBY at this level, and no safe wrapper to inherit from.
    Ok(None)
}

pub(crate) fn has_limit_one_deep(stmt: &SelectStatement) -> bool {
    // Walk down a chain of single-subquery FROM items, if present.
    let mut cur = stmt;
    loop {
        // Local LIMIT 1 (any OFFSET) => AtMostOne row after TOP-K materialization
        if cur.limit_clause.limit().is_some_and(is_literal_one) {
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

/// Returns true if the subquery WHERE clause or any INNER JOIN ON clause contains
/// correlation to outer columns.  JOIN ON correlations are checked because
/// `move_correlated_constraints_from_join_to_where` (which normalizes them into
/// WHERE) runs after this check in `as_joinable_derived_table_with_opts`.
fn subquery_has_correlation(stmt: &SelectStatement, locals: &HashSet<Relation>) -> bool {
    if let Some(expr) = &stmt.where_clause
        && contains_outer_columns(expr, locals)
    {
        return true;
    }
    for jc in &stmt.join {
        if let JoinConstraint::On(on_expr) = &jc.constraint
            && contains_outer_columns(on_expr, locals)
        {
            return true;
        }
    }
    false
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
        if cur.limit_clause.limit().is_some_and(is_literal_zero) {
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
/// - `preserve_top_k_for_exists`: if the probe needs to respect subquery TOP-K (NP path), keep
///   LIMIT/OFFSET and ORDER. Otherwise (EP/regular EXISTS normalization), we drop ORDER BY and
///   normalize away LIMIT/OFFSET only when OFFSET is absent/0 (set semantics via `DISTINCT`).
///   When OFFSET > 0 we keep OFFSET and avoid `DISTINCT` to preserve bag semantics for emptiness.
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
pub(crate) fn as_joinable_derived_table_with_opts<U: UniqueColumnsSchema>(
    ctx: SubqueryContext,
    stmt: &mut SelectStatement,
    stmt_alias: SqlIdentifier,
    opts: AsJoinableOpts,
    unique_cols_schema: &U,
) -> ReadySetResult<(TableExpr, Option<Expr>)> {
    // True iff this SELECT **or** a chain of single-child projecting wrappers beneath it
    // contains an explicit `LIMIT 1` (with optional `OFFSET 0`). Capture this *before*
    // TOP-K materialization; afterward, ORDER/LIMIT are consumed into ROW_NUMBER() and gone.
    // We use this flag to:
    //  - relax scalar validation (allow non-aggregated scalar with LIMIT 1),
    //  - convert WHERE `IN (... LIMIT 1)` into scalar `=`, and
    //  - skip (and explicitly drop) DISTINCT as redundant after RN <= 1.
    let has_limit_one = has_limit_one_deep(stmt);

    // Strip redundant LIMIT/ORDER for aggregate-only-no-GBY subqueries (ExactlyOne
    // or AtMostOne) BEFORE the nested hoist.  The nested hoist may inject GROUP BY
    // into inner subqueries (for correlated aggregates), which would change the
    // inner from aggregate-only-no-GBY to grouped — making `agg_only_no_gby_cardinality`
    // fail on the wrapper and preventing the strip later.  Stripping now, while the
    // inner is still in its original shape, is semantically safe: a ≤1-row subquery's
    // LIMIT and ORDER BY are no-ops.
    strip_redundant_limit_for_agg_no_gby(stmt)?;

    // Pre-wrap eligibility: lift a LIMIT-bearing uncorrelated predicate subquery
    // into a FROM-position derived table so the LIMIT is preserved at the inner
    // level (the nested-hoist FROM-loop's `top_k_in_subquery_position = true`
    // gate preserves it for native MIR TopK lowering) while the outer level
    // has empty `limit_clause` — satisfying `make_subquery_distinct`'s invariant
    // and giving the IN-unnest / Scalar machinery a clean outer SELECT to add
    // DISTINCT and marker columns to.  `has_limit_one_deep` walks projecting
    // wrappers, so the LIMIT-1 detection above remains correct across the wrap.
    //
    // Runs AFTER `strip_redundant_limit_for_agg_no_gby` so ExactlyOne/AtMostOne
    // aggregate-only-no-GBY shapes (where LIMIT is semantically redundant) take
    // the cheaper stripped path instead of incurring the wrap.
    //
    // For `SubqueryContext::Exists`, the wrap fires only when the caller has
    // requested TOP-K preservation in the probe (`preserve_top_k_for_exists`)
    // — i.e., the NP_3VL probe path, which needs to detect NULL in the
    // LIMITed bag specifically and so requires the LIMIT to survive.  The EP
    // probe path (`!preserve_top_k_for_exists`) takes the LIMIT-stripping
    // simplification later in this function (LIMIT and ORDER BY don't affect
    // emptiness; stripping yields cheaper dataflow than preservation).
    let wrap_eligible_for_exists =
        matches!(ctx, SubqueryContext::Exists) && opts.preserve_top_k_for_exists;
    if (!matches!(ctx, SubqueryContext::Exists) || wrap_eligible_for_exists)
        && !stmt.limit_clause.is_empty()
        && preserve_uncorrelated_top_k()
        && limit_clause_eligible_for_native_pagination(&stmt.limit_clause)
        && !is_correlated(stmt)
    {
        let inner_alias = get_unique_alias(&collect_local_from_items(stmt)?, "INNER");
        let mut wrapped = construct_projecting_wrapper(TableExpr {
            inner: TableExprInner::Subquery(Box::new(mem::take(stmt))),
            alias: Some(inner_alias),
            column_aliases: vec![],
        })?;
        let (outer, _) = expect_sub_query_with_alias_mut(&mut wrapped);
        *stmt = mem::take(outer);
    }

    // Bubble up correlation from nested derived tables (one level, recursively)
    // (Runs *after* we captured LIMIT 1 information and stripped redundant LIMIT/ORDER.)
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

            // If 3VL needs TOP-K semantics preserved in the probe (NP path), keep ORDER/LIMIT/OFFSET.
            // Otherwise (EP/regular EXISTS normalization), we may drop TOP-K *only when that does not
            // change emptiness*:
            //   - ORDER BY never affects emptiness for EXISTS, so we always drop it here.
            //   - When OFFSET is absent or 0, EXISTS is insensitive to multiplicity; we can normalize to
            //     set semantics by setting DISTINCT and clearing LIMIT/OFFSET.
            //   - When OFFSET > 0, multiplicity affects emptiness (OFFSET applies to row *count*), so we
            //     must NOT set DISTINCT. We keep OFFSET (and drop LIMIT, since LIMIT>0 does not affect
            //     emptiness for EXISTS; LIMIT 0 is short-circuited earlier by `is_definitely_empty_subquery`).
            // Keeping a non-empty `limit_clause` in the OFFSET>0 case also ensures TOP-K will be
            // materialized later via ROW_NUMBER() in `hoist_correlated_from_where_clause_and_rewrite_top_k`.
            if !opts.preserve_top_k_for_exists {
                match stmt.limit_clause.offset() {
                    None => {
                        stmt.distinct = true;
                        stmt.limit_clause = Default::default();
                    }
                    Some(lit) if is_literal_zero(lit) => {
                        stmt.distinct = true;
                        stmt.limit_clause = Default::default();
                    }
                    offs => {
                        stmt.limit_clause = LimitClause::LimitOffset {
                            limit: None,
                            offset: offs.cloned(),
                        };
                    }
                }
                stmt.order = None;
            }
        }
        SubqueryContext::Scalar | SubqueryContext::In => {
            // Single-valued acceptance for Scalar by shape only; correlation is deferred
            // and verified post-hoist via grouping alignment (see callbacks below).
            let is_single_by_shape = matches!(ctx, SubqueryContext::Scalar)
                && matches!(
                    agg_only_no_gby_cardinality(stmt)?,
                    Some(AggNoGbyCardinality::ExactlyOne | AggNoGbyCardinality::AtMostOne)
                );
            // If the subquery WHERE is correlated, hoisting may align GROUP BY with the correlated keys,
            // yielding "exactly one row per outer row" even when the top-level field is not an aggregate
            // and there's no explicit LIMIT 1. Defer rejection to the post-hoist checks in that case.
            let may_be_single_via_corr = subquery_has_correlation(stmt, &local_from_items);
            // (d') Structural single-row proof via unique-key pinning on the inner FROM.
            // `WHERE base.pk_col = <rhs>` (or every column of a composite unique key) with
            // an RHS that doesn't reference the base table — see `is_unique_key_pinned_scalar`.
            // Independent of correlation: a constant equi-pin on a PK proves at-most-one row.
            let is_single_by_unique_key = matches!(ctx, SubqueryContext::Scalar)
                && is_unique_key_pinned_scalar(stmt, unique_cols_schema)?;
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
                // Accept four ways to be single-valued for Scalar:
                //  (a) the projected field is an aggregated expression, OR
                //  (b) there is an explicit LIMIT 1 (possibly under wrappers), OR
                //  (c) the subquery is aggregate-only without GROUP BY and HAVING is TRUE/absent
                //      (wrapper-aware via agg_only_no_gby_cardinality), OR
                //  (d) structural unique-key pinning by WHERE equi-predicates.
                if !is_aggregated_expr(field_expr)?
                    && !has_limit_one
                    && !is_single_by_shape
                    && !may_be_single_via_corr
                    && !is_single_by_unique_key
                {
                    // Not a statement the client wrote wrongly: a subquery that yields one row
                    // is legal, and this pass cannot prove that it does not.  Refusing as
                    // unsupported lets the query fall back and be parked, where calling it
                    // invalid is both untrue and re-attempted on every eligible read.
                    unsupported!("Subquery should be aggregated or have LIMIT 1");
                }
                if !stmt.limit_clause.is_empty()
                    && !has_limit_one
                    && !is_single_by_shape
                    && !may_be_single_via_corr
                    && !is_single_by_unique_key
                {
                    unsupported!("Subquery returns more than 1 row");
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
                unsupported!("Subquery returns more than 1 row")
            } else {
                Ok(())
            }
        };

        let check_for_uncorrelated_where = |stmt: &mut SelectStatement| {
            if is_scalar_ctx {
                if stmt.group_by.is_some() && !has_limit_one {
                    unsupported!("Subquery returns more than 1 row")
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
            // Predicate-subquery (IN / EXISTS / Scalar) context: keep
            // materialising LIMIT — the downstream unnest relies on the
            // RN-rewritten shape (DISTINCT + flat row set) for set semantics.
            false,
            check_for_local_cols_eq_group_keys,
            check_for_uncorrelated_where,
        )?
    };

    // IMPORTANT: Distinct + set normalization is **not** applied for probe-EXISTS with TOP-K.
    // Additionally, for Scalar with explicit LIMIT 1, DISTINCT is redundant:
    // after rewrite_top_k_in_place(_with_partition), LIMIT/ORDER are consumed and
    // RN <= 1 ensures at most one row (globally or per partition), so de-dup is unnecessary.
    //
    // The `stmt.limit_clause.is_empty()` guard makes this block self-defending against
    // future call paths that bypass both materialisation (`rewrite_top_k_in_place`,
    // `rewrite_correlated_top_k_in_place`) and the pre-wrap above: setting
    // `stmt.distinct = true` on a LIMIT-bearing subquery would silently turn "first N
    // rows" into "first N distinct rows" — a different query.  `make_subquery_distinct`
    // would `invariant!` on that input; the gate here keeps the failure mode local.
    if !(matches!(ctx, SubqueryContext::Exists) && opts.preserve_top_k_for_exists)
        && stmt.limit_clause.is_empty()
    {
        #[cfg(debug_assertions)]
        {
            // ORDER BY is expected to be cleared alongside LIMIT at this point: both
            // `rewrite_top_k_in_place(_with_partition)` and the pre-wrap clear it.
            debug_assert!(
                stmt.order.is_none(),
                "ORDER BY should have been cleared with LIMIT before DISTINCT handling"
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
        column_aliases: vec![],
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
pub(crate) fn as_joinable_derived_table<U: UniqueColumnsSchema>(
    ctx: SubqueryContext,
    stmt: &mut SelectStatement,
    stmt_alias: SqlIdentifier,
    unique_cols_schema: &U,
) -> ReadySetResult<(TableExpr, Option<Expr>)> {
    as_joinable_derived_table_with_opts(
        ctx,
        stmt,
        stmt_alias,
        AsJoinableOpts::default(),
        unique_cols_schema,
    )
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
pub(crate) fn is_supported_subquery_predicate(expr: &Expr) -> bool {
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
pub(crate) fn as_supported_subquery_predicate(
    expr: &Expr,
) -> ReadySetResult<SubqueryPredicateDesc> {
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
            // SAFETY: the outer guard `matches!(rhs.as_ref(), Expr::Exists(_))` guarantees
            // this arm is unreachable — the inner match is exhaustive for Expr::Exists.
            _ => unreachable!("guard guarantees Expr::Exists"),
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
    // Get the RHS first field column
    let Expr::Column(rhs_col) = &first_field_ref else {
        internal!("Subquery select field reference should be a column")
    };

    // Fast path: direct mapping for the *derived* first-field column via aggregate analysis.
    let mut fields_map = HashMap::new();
    analyse_lone_aggregates_subquery_fields(
        derived_table_stmt,
        derived_table_alias.clone(),
        &mut fields_map,
    )?;

    match fields_map.remove(rhs_col) {
        Some(Err(err)) => return Err(err.clone()),
        Some(Ok(mapped_expr)) => return Ok(Some(mapped_expr)),
        None => { /* fall through to wrapper/descend path */ }
    }

    // Locate the first-field expression by its alias (the column name of `first_field_ref`).
    let Some(ff_expr) = resolve_field_expr_by_alias(derived_table_stmt, &rhs_col.name) else {
        return Ok(None);
    };

    // Try to compute a zero-argument substitute for the first-field expression
    // by peeling allowed wrappers and/or descending through single-child wrappers.
    if let Some(f_zero) = extract_aggregate_fallback_for_expr(ff_expr, derived_table_stmt)? {
        return Ok(Some(Expr::Call(FunctionExpr::Coalesce(vec![
            first_field_ref.clone(),
            f_zero,
        ]))));
    }

    Ok(None)
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
        // SAFETY: guarded by `bound_idxs.len() == 1` on the line above.
        let idx = *bound_idxs
            .iter()
            .next()
            .expect("bound_idxs has exactly one element");
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
    // SAFETY: the `if chosen_lhs.is_none() { return ... }` guard above guarantees `Some` here.
    let chosen = chosen_lhs.expect("chosen_lhs verified Some above");

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
    let on_expr = conjoin_all_dedup(accepted_atoms);

    // Fully supported iff we kept at least one rhs<->chosen equality AND nothing was moved.
    let fully_supported = found_cross_eq && remainder.is_none();

    JoinConditionSplit {
        on_expr,
        to_where: remainder,
        fully_supported,
    }
}
/// Whether any field of `stmt` is a bare literal.  A projection carrying a literal keeps state of
/// its own, so the entry already has a node between its base table and the join.
fn projects_a_literal(stmt: &SelectStatement) -> bool {
    stmt.fields.iter().any(|field| {
        matches!(
            field,
            FieldDefinitionExpr::Expr {
                expr: Expr::Literal(_),
                ..
            }
        )
    })
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
            // The left-null filter enforces anti-join semantics, so set semantics never change the
            // anti-join's result.  They keep the entry a node of its own, so it never flattens into
            // a join on a base table another join also reads.  An entry that aggregates, groups or
            // projects a literal already has a node of its own and needs none.
            if join_expr.is_some()
                && (is_aggregated_select(derived_table_stmt)?
                    || derived_table_stmt.group_by.is_some()
                    || projects_a_literal(derived_table_stmt))
            {
                derived_table_stmt.distinct = false;
            }
            add_to_where = and_predicates_skip_true(
                add_to_where,
                construct_null_check_expr(
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

    if let Some(add_to_where) = add_to_where {
        base_stmt.where_clause =
            and_predicates_skip_true(mem::take(&mut base_stmt.where_clause), add_to_where);
    }
    Ok(true)
}

pub(crate) fn collect_subquery_predicates(
    expr: &Expr,
) -> ReadySetResult<(Vec<Expr>, Option<Expr>)> {
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
    grouping_keys: &[Expr],
    local_from_items: &HashSet<Relation>,
) -> Option<&'a Column> {
    columns_iter(expr).find(|col| {
        col.table
            .as_ref()
            .is_some_and(|table| local_from_items.contains(table))
            && !grouping_keys
                .iter()
                .any(|key| matches!(key, Expr::Column(key_col) if key_col == *col))
    })
}

fn turn_into_not_eq_scalar(subquery_desc: &mut SubqueryPredicateDesc) {
    subquery_desc.negated = false;
    if let Some((_, op)) = &mut subquery_desc.lhs_and_op {
        *op = BinaryOperator::NotEqual;
    }
}

fn unnest_subqueries_in_where<U: UniqueColumnsSchema>(
    stmt: &mut SelectStatement,
    ctx: &mut UnnestContext<U>,
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

    // Snapshot the JOIN count before the loop.  If a constant-fold short-circuits
    // the entire WHERE to FALSE (e.g., NOT EXISTS on an ExactlyOne aggregate),
    // we truncate back to discard any JOINs added by earlier loop iterations —
    // they are semantically dead under WHERE FALSE and would be dangling.
    let join_len_before = stmt.join.len();

    // Fold the entire WHERE to FALSE: discard dangling JOINs and return.
    macro_rules! fold_where_false {
        () => {{
            stmt.join.truncate(join_len_before);
            stmt.where_clause = Some(Expr::Literal(false.into()));
            return Ok(SINGLE_REWRITE_STATUS);
        }};
    }

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
                        fold_where_false!();
                    }
                }
                SubqueryContext::Scalar => {
                    // WHERE (scalar with zero rows) ⇒ NULL → filtered → FALSE
                    fold_where_false!();
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
                            fold_where_false!();
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
            ctx.unique_cols_schema,
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
            // Attempt COUNT-aware RHS mapping directly; if it succeeds, flip to LEFT and
            // move the comparison to the outer WHERE. This decouples the decision from
            // pre-hoist hinting and relies solely on structural ExactlyOne + successful mapping.
            let first_field =
                make_first_field_ref_name(subquery_stmt, subquery_stmt_alias.clone())?;
            if let Some(rhs_for_where) = build_rhs_expr_for_aggregate_only_derived_table(
                subquery_stmt,
                subquery_stmt_alias.clone(),
                &first_field,
            )? {
                join_op = LeftOuterJoin; // avoid null-reject; preserve zero-on-empty
                // SAFETY: guarded by `lhs_and_op.is_some()` in the enclosing `if` condition.
                let (lhs, op) = mem::take(&mut lhs_and_op).expect("lhs_and_op verified Some");
                add_to_where = and_predicates_skip_true(
                    add_to_where,
                    construct_scalar_expr(lhs, op, rhs_for_where),
                );
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
                move |base_stmt: &mut SelectStatement, ctx: &mut UnnestContext<U>| {
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

/// Takes the grouping keys already resolved to expressions.  A key written as a position or an
/// alias can only be read against the projection, which a caller partway through rewriting the
/// select list no longer has.
///
/// Reports `unsupported` rather than an invalid query: the comparison is syntactic, so it cannot
/// see a column that Postgres accepts as functionally dependent on the grouping keys.  Sending
/// the query upstream serves those, and lets Postgres raise the error on the rest.
fn assert_local_columns_are_grouped(
    expr: &Expr,
    grouping_keys: &[Expr],
    local_from_items: &HashSet<Relation>,
) -> ReadySetResult<()> {
    if !grouping_keys.is_empty()
        && let Some(col) = find_local_column_not_group_by_key(expr, grouping_keys, local_from_items)
    {
        unsupported!(
            "Subquery uses ungrouped column {} from outer query",
            col.display(Dialect::PostgreSQL)
        )
    }
    Ok(())
}

fn unnest_subqueries_in_fields<U: UniqueColumnsSchema>(
    stmt: &mut SelectStatement,
    ctx: &mut UnnestContext<U>,
) -> ReadySetResult<RewriteStatus> {
    let mut rewrite_status = RewriteStatus::default();

    let mut stmt_fields = Vec::new();
    let mut fields_needing_group_by_key = Vec::new();
    // Fields to move into a wrapper, paired with the position they hold in the projection.
    let mut fields_to_hoist: Vec<(usize, FieldDefinitionExpr)> = Vec::new();
    // Grouping keys may be written as positions or aliases, and only the projection can say what
    // they name -- so resolve them here, before the loop moves the field list out of `stmt`.
    let grouping_keys = resolve_group_by_exprs(stmt)?;
    // Read for the same reason: once the loop takes the field list, a statement whose only
    // aggregates live in the projection reads as having none.
    let outer_is_ungrouped_aggregate = is_aggregate_only_without_group_by(stmt)?;
    // Read before the loop takes the field list, which would leave the projection's aggregates
    // unreadable.
    let outer_aggregates_outside_order_by =
        outer_is_ungrouped_aggregate && aggregates_outside_order_by(stmt)?;
    // A statement an OFFSET or a LIMIT 0 skips past emits no row, so there is no row for a
    // hoisted field to be read on.  Hoisting would bury the clause that says so inside the
    // derived table, where the passes that answer it no longer see it, and the server then
    // refuses an OFFSET it cannot read (`query_graph.rs`, "OFFSET without LIMIT") after a round
    // trip.  The cardinality is already settled here, so ask it here.
    let outer_may_lack_a_row = outer_is_ungrouped_aggregate
        && !matches!(
            agg_only_no_gby_cardinality(stmt)?,
            Some(AggNoGbyCardinality::ExactlyOne)
        );

    for (field_index, mut current_field) in mem::take(&mut stmt.fields).into_iter().enumerate() {
        let mut needs_group_by_key = false;
        let mut hoist_field = false;
        let (field_expr, _) = expect_field_as_expr_mut(&mut current_field);
        // Whether an aggregate is written anywhere in the field.  One enclosing the subquery
        // reads its value, where an empty relation yields NULL upstream too, so the rewrite
        // below needs no guarding; one written beside it is refused just under this.
        let field_is_aggregated = is_aggregated_expr(field_expr)?;

        // Collect what the field projects once, and answer every question below from that one
        // set.  A second walk would have to agree with this one about which subqueries the
        // rewrite places, and nothing would hold them in step.
        let collected = collect_subquery_predicates_with_placement_mut(field_expr)?;

        // The folds answer a subquery with a literal, which places nothing.  Run them first, so
        // what remains is exactly the set the rewrite below has to serve.
        let mut placed = Vec::with_capacity(collected.len());
        for (subquery_is_beside_aggregate, subquery_predicate) in collected {
            let subquery_desc = as_supported_subquery_predicate(subquery_predicate)?;

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

            // Shape-level cardinality for aggregate-only/no-GBY: safe per outer row even if
            // correlated.  EXISTS over a one-row aggregate is the second fold.
            let agg_card_shape = agg_only_no_gby_cardinality(&subquery_desc.stmt)?;
            if matches!(agg_card_shape, Some(AggNoGbyCardinality::ExactlyOne))
                && matches!(subquery_desc.ctx, SubqueryContext::Exists)
            {
                let _ = mem::replace(
                    subquery_predicate,
                    Expr::Literal((!subquery_desc.negated).into()),
                );
                rewrite_status.rewrite();
                continue;
            }

            placed.push((
                subquery_is_beside_aggregate,
                subquery_predicate,
                subquery_desc,
                agg_card_shape,
            ));
        }

        // Hoisting moves every subquery the field places together, so one correlated among them
        // refuses the field.  Asked of the placed set: a subquery the folds answered carries no
        // correlation into a hoist it takes no part in.
        let mut field_subquery_is_correlated = false;
        if outer_is_ungrouped_aggregate && !field_is_aggregated {
            for (_, _, subquery_desc, _) in &placed {
                if !correlated_relations(&subquery_desc.stmt)?.is_empty() {
                    field_subquery_is_correlated = true;
                    break;
                }
            }
        }

        for (subquery_is_beside_aggregate, subquery_predicate, mut subquery_desc, agg_card_shape) in
            placed
        {
            if matches!(agg_card_shape, Some(AggNoGbyCardinality::ExactlyOne))
                && matches!(subquery_desc.ctx, SubqueryContext::In if subquery_desc.negated)
            {
                // NOT IN(one-row) is equivalent to != (3VL-correct)
                turn_into_not_eq_scalar(&mut subquery_desc);
            }

            let is_exactly_one = matches!(agg_card_shape, Some(AggNoGbyCardinality::ExactlyOne));

            // A field writing an aggregate and reading a subquery beside it wants two
            // placements at once: the aggregate below, reading this statement's rows, and the
            // subquery above, on the one row the aggregate emits.  Answering it beside the
            // aggregate projects a column the statement does not group, which an exit check
            // refuses as a statement the client wrote wrongly.  Refuse it here as a shape this
            // does not serve.  Reached only by a subquery the folds above left standing.
            if outer_is_ungrouped_aggregate && field_is_aggregated && subquery_is_beside_aggregate {
                unsupported!("select-list subquery read beside an aggregate in one field");
            }

            // This pass answers a select-list subquery by joining it into the FROM, where each
            // outer row carries the joined value.  An ungrouped aggregate has no such row: it
            // emits its one row even when its filter matches nothing, while the join sits below
            // it.
            //
            //     SELECT COUNT(*), (SELECT COUNT(*) FROM b) FROM a WHERE a.s = 'nope'
            //
            // must answer with a count of `b`; joined below the aggregate it answers NULL.  The
            // value is absent from that row rather than wrong in it, so only the placement can
            // change -- hoisting the field into a wrapper puts the join above the aggregate,
            // whose one guaranteed row is then the join's preserved side.  The subquery's own
            // shape is not consulted: one that yields no row is what the outer join above the
            // aggregate null-extends.  Checked after the folds above, which leave a literal with
            // no row to read and so keep their rewrite, and before the rewrite below, which a
            // hoisted field never reaches: the wrapper rewrites the field where it lands.
            if outer_is_ungrouped_aggregate && !field_is_aggregated {
                if outer_may_lack_a_row {
                    unsupported!(
                        "ungrouped aggregate this pass cannot prove emits exactly one row"
                    );
                }
                // A statement that aggregates only in its ORDER BY holds itself to one row by
                // that ordering alone, and hoisting drops the ordering, which is written against
                // a projection this splits in two.  Dropping it would leave a plain scan
                // answering with every row.  Inside an `IN` a later pass drops it too, which is
                // the same loss reached another way.
                if !outer_aggregates_outside_order_by {
                    unsupported!(
                        "select-list subquery beside an aggregate written only in ORDER BY"
                    );
                }
                // Hoisting is the only correct rewrite here, and a reference out of the
                // subquery cannot take it: one to a column of this statement presumes a
                // particular outer row, which a group-less aggregate does not have, and one to
                // a scope enclosing this statement is legal but not separable from it here.
                // Both are correlation read relative to the subquery, which is what this
                // answers, at every depth and accounting for what each level shadows.
                if field_subquery_is_correlated {
                    unsupported!(
                        "correlated select-list subquery projected beside an ungrouped aggregate"
                    );
                }
                hoist_field = true;
                break;
            }

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
            let subquery_was_correlated = is_correlated(&subquery_desc.stmt);

            let (derived_table, join_on) = as_joinable_derived_table(
                subquery_desc.ctx,
                &mut subquery_desc.stmt,
                get_unique_alias(&local_from_items, "GNL"),
                ctx.unique_cols_schema,
            )?;

            if let Some(join_on_expr) = &join_on {
                assert_local_columns_are_grouped(join_on_expr, &grouping_keys, &local_from_items)?;
            }

            // Once the subquery becomes a reference to the joined derived table, a grouped outer
            // statement projects a value that is neither grouped nor aggregated: valid before the
            // rewrite, invalid after.  Record that this field needs its value as a grouping key.
            //
            // Adding it preserves the result only when the value cannot split or merge groups.
            // An uncorrelated subquery yields one value for the whole relation.  Supported
            // correlation is equality pairs in WHERE, which always yield a join condition, so it
            // is the correlation whose outer columns the assertion above has established are
            // themselves grouping keys.  Correlation elsewhere reaches here only in transit --
            // `unnest_lateral_subqueries` rejects it -- and has no join condition to vouch for
            // it, so it must not become a grouping key.
            let correlation_without_join_condition = subquery_was_correlated && join_on.is_none();
            let field_needs_group_by_key =
                stmt.group_by.is_some() && !correlation_without_join_condition;

            let (subquery_stmt, subquery_stmt_alias) = expect_sub_query_with_alias(&derived_table);

            let first_field =
                make_first_field_ref_name(subquery_stmt, subquery_stmt_alias.clone())?;

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
                    Either::Left(construct_null_check_expr(rhs, subquery_desc.negated))
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
                                move |base_stmt: &mut SelectStatement,
                                      ctx: &mut UnnestContext<U>| {
                                    let is_lhs_null_free =
                                        is_select_expr_null_free(&lhs, base_stmt, ctx.schema)?;
                                    if is_lhs_null_free && rhs_ctx.is_null_free() {
                                        // With left-joined equality, membership reduces to "rhs IS [NOT] NULL"
                                        Ok(construct_null_check_expr(rhs.clone(), is_not_in))
                                    } else {
                                        add_3vl_for_select_list_in_subquery(
                                            base_stmt,
                                            SelectList3vlInput {
                                                lhs,
                                                rhs: rhs.clone(),
                                                preserved_rhs_stmt: preserved_subquery_stmt
                                                    // SAFETY: `preserved_subquery_stmt` is set to `Some` in the
                                                    // `SubqueryContext::In` guard above; this closure is only
                                                    // constructed in that same `In` branch.
                                                    .expect("set to Some for SubqueryContext::In"),
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
                needs_group_by_key |= field_needs_group_by_key;
            } else {
                rewrite_status.rollback();
            }
        }

        if hoist_field {
            fields_to_hoist.push((field_index, current_field));
            continue;
        }

        if needs_group_by_key {
            fields_needing_group_by_key.push(stmt_fields.len());
        }

        stmt_fields.push(current_field);
    }

    stmt.fields = stmt_fields;

    // Group by the projected expression, whatever wraps the subquery, once the field list is back
    // in place: a GROUP BY here may reference an item by alias or by position, and
    // `find_group_by_key` recognizes every form against the projection.  Positional references
    // survive this far because `remove_numeric_field_references` runs server-side, after this
    // pipeline.  The emitted form is not ours to pick -- `derived_tables_rewrite` and
    // `hoist_parametrizable_filters` rewrite grouping expressions that match an aliased select
    // item into alias references -- so a key added here only has to name the same value.
    if let Some(group_by) = &mut stmt.group_by {
        // Stands in for an unaliased item: no grouping key can name the empty identifier, so
        // matching falls to the expression and position forms.
        let unaliased = SqlIdentifier::default();
        for field_idx in fields_needing_group_by_key {
            let (expr, alias) = expect_field_as_expr(&stmt.fields[field_idx]);
            let alias = alias.as_ref().unwrap_or(&unaliased);
            if find_group_by_key(&stmt.fields, &group_by.fields, expr, alias)?.is_none() {
                group_by.fields.push(FieldReference::Expr(expr.clone()));
            }
        }
    }

    if !fields_to_hoist.is_empty() {
        rewrite_status =
            rewrite_status.combine(hoist_fields_above_aggregate(stmt, fields_to_hoist, ctx)?);
    }

    Ok(rewrite_status)
}

/// Moves `hoisted` into a wrapper that selects from `stmt` and decorrelates them there, so the
/// joins replacing their subqueries sit above `stmt`'s aggregate.  Each field carries the position
/// it held, which the wrapper restores, so the statement answers with the same columns in the
/// same order.  A field the statement named keeps that name; one it left unnamed is named after
/// the column it ends up reading.
///
/// Recursion terminates because the wrapper projects no aggregate of its own, so the gate that
/// hoists cannot fire again.
fn hoist_fields_above_aggregate<U: UniqueColumnsSchema>(
    stmt: &mut SelectStatement,
    hoisted: Vec<(usize, FieldDefinitionExpr)>,
    ctx: &mut UnnestContext<U>,
) -> ReadySetResult<RewriteStatus> {
    // A parameter anywhere this encloses is one the client cannot bind: the view key it becomes
    // has to reach the leaf, and `pull_keys` pushes a key through neither side of the LEFT JOIN
    // the hoist introduces.  `stmt` goes under its left side and the hoisted fields under its
    // right, so read both -- including a filter still sitting in a join's ON.
    if hoist_encloses_a_parameter(stmt, &hoisted) {
        unsupported!("client parameter in a statement hoisted above an ungrouped aggregate");
    }

    // The statement being wrapped can itself read an enclosing scope -- a LATERAL body does, and
    // the lateral step answers such a reference by turning the body's leading FROM item into a
    // join, which after wrapping is the wrapper's only one, leaving joins with nothing to join
    // from.  Read with the hoisted fields moved back on and taken off again: a reference written
    // bare in one of them is in neither the statement that is left nor the subqueries already
    // checked.
    let (hoisted_indices, hoisted_fields): (Vec<usize>, Vec<FieldDefinitionExpr>) =
        hoisted.into_iter().unzip();
    let kept_fields = stmt.fields.len();
    stmt.fields.extend(hoisted_fields);
    let correlated = correlated_relations(stmt);
    let hoisted_fields = stmt.fields.split_off(kept_fields);
    if !correlated?.is_empty() {
        unsupported!("correlated statement hoisted above an ungrouped aggregate");
    }

    // A statement projecting nothing is not a statement.  Reaching it needs every field hoisted,
    // which the rules above and at entry already prevent; stated rather than inherited, so
    // relaxing either of those reaches the client as a refusal rather than as a bug.
    if stmt.fields.is_empty() {
        unsupported!("every field of an ungrouped aggregate hoisted out of it");
    }

    // The ordering is written against a projection this splits in two and cannot change a single
    // row's answer.  One ordered by its only aggregate is declined before here, so none is
    // load-bearing.
    stmt.order = None;
    // An `OFFSET 0` skips no row, but the normalization that erases it reads the top level only,
    // which this is about to empty.  Erase it here, or the server is handed an OFFSET with no
    // LIMIT, which it refuses -- a round trip this pass exists to save.
    stmt.limit_clause.strip_zero_offset();

    // What the statement answers with.  The wrapper stands in for the statement rather than
    // becoming a table someone selects from, so it answers with these names -- read back below,
    // since uniquification renames the second of two fields deriving one name.
    let answered = stmt
        .fields
        .iter()
        .map(|fe| {
            (
                expect_field_as_expr(fe).1.clone(),
                get_select_item_alias(fe),
            )
        })
        .collect::<Vec<_>>();

    let wrap_alias = get_unique_alias(&collect_local_from_items(stmt)?, "W");
    let wrapper = construct_projecting_wrapper(TableExpr {
        inner: TableExprInner::Subquery(Box::new(mem::take(stmt))),
        alias: Some(wrap_alias),
        column_aliases: vec![],
    })?;
    let TableExprInner::Subquery(mut wrapper) = wrapper.inner else {
        internal!("a projecting wrapper is a subquery");
    };
    // The name each field answers under.  A kept field answers under the name it had before the
    // wrap, not the one uniquification gave it inside the derived table.
    let mut answers = Vec::with_capacity(wrapper.fields.len());
    for (fe, (alias, named)) in wrapper.fields.iter_mut().zip(answered) {
        *expect_field_as_expr_mut(fe).1 = alias;
        answers.push(Some(named));
    }

    for (field_index, field) in hoisted_indices.into_iter().zip(hoisted_fields) {
        // The indices are the positions the fields held in a projection the wrapper now carries
        // one entry of per field left behind, so each lands back where it was.  Said rather than
        // assumed: `Vec::insert` panics past the end, and a pass that synthesized a field would
        // make it reachable.
        if field_index > wrapper.fields.len() {
            internal!("hoisted field index {field_index} is past the wrapper's projection");
        }
        wrapper.fields.insert(field_index, field);
        // A hoisted field is re-pointed by the rewrite below, so the name it answers under is
        // the one its result carries rather than one read here.
        answers.insert(field_index, None);
    }

    // Only the select list needs rewriting: the wrapper has no filter, grouping, ordering or
    // join of its own, and what it encloses was rewritten before it was built.
    // A probe is remembered by the shape it probes and answered with the alias of a join the
    // asking statement carries.  The wrapper is a different statement carrying different joins,
    // so it must not be offered the ones this statement built -- the rule REA-6843 states for
    // every statement, which this call site does not inherit because it reaches a pass directly
    // rather than through the entry that keeps it.  Put back afterwards for the lateral step,
    // which descends into the derived table the wrap just built, where those joins still are.
    //
    // No shape is known that answers differently either way: the probe a wrapper field asks for
    // keys on an ON predicate the enclosed statement's probes do not share, so the lookup misses
    // rather than hits.  Kept because the rule holds whether or not a shape reaches it.
    let outer_probes = mem::replace(&mut ctx.probes, ProbeRegistry::new());
    let rewrite_status = unnest_subqueries_in_fields(&mut wrapper, ctx);
    ctx.probes = outer_probes;
    let rewrite_status = rewrite_status?;

    // Hoisting answers the field by joining it in above the aggregate, so a field left holding a
    // subquery is one the rewrite could not move.  The wrapper then projects the aggregate
    // without adding anything, which the leading derived table inliner folds away, putting the
    // field back beside the aggregate -- the placement this exists to avoid.  The wrapper also
    // has to leave the aggregate's one row alone: it stands above it, so a filter it carries can
    // delete the row the hoist exists to carry a value onto.  A wrapper that filters nothing and
    // projects no subquery is one where every hoisted field became a join.  Its FROM and joins
    // are excluded, the derived tables there being what the rewrite produces.
    //
    // This reads the wrapper as it stands here, which is the only state it can speak for.  A
    // LIMIT or an ordering a hoisted subquery carries is still inside its own derived table at
    // this point; the inliner flattens that later and lifts the clause to the top, above the
    // aggregate.  Asking about them here would answer about a statement that does not exist yet.
    if wrapper.where_clause.is_some() || wrapper.having.is_some() {
        unsupported!("filter above an ungrouped aggregate the hoist must not drop a row from");
    }
    if wrapper
        .fields
        .iter()
        .any(|fe| contains_select(expect_field_as_expr(fe).0))
    {
        unsupported!(
            "select-list subquery beside an ungrouped aggregate that hoisting cannot move"
        );
    }

    // Two outputs may answer under one name only where each is read together with a different
    // relation: below MIR an alias is read by name alone, so it leaves nothing to tell a pair
    // apart, while an unaliased field is read with its relation.  The hoist produces both cases
    // -- a hoisted field named after the column it reads, and kept fields that all read the
    // derived table.
    let mut answered: HashMap<SqlIdentifier, (bool, HashSet<Option<Relation>>)> = HashMap::new();
    for (fe, answer) in wrapper.fields.iter().zip(&answers) {
        let (expr, alias) = expect_field_as_expr(fe);
        let read_with = match expr {
            Expr::Column(column) => column.table.clone(),
            _ => None,
        };
        let name = match answer {
            Some(kept) => kept.clone(),
            None => get_select_item_alias(fe),
        };
        // Reaching an entry means another field already answers under this name, so the pair is
        // the question; the first of a name has nothing to collide with.
        match answered.get_mut(&name) {
            Some((any_aliased, relations)) => {
                *any_aliased |= alias.is_some();
                if *any_aliased || !relations.insert(read_with) {
                    unsupported!(
                        "fields sharing the output name {name} beside an ungrouped aggregate"
                    );
                }
            }
            None => {
                answered.insert(name, (alias.is_some(), HashSet::from([read_with])));
            }
        }
    }

    *stmt = *wrapper;

    Ok(rewrite_status.combine(SINGLE_REWRITE_STATUS))
}

// **NOTE**: The order of unnesting is important:
//  - WHERE should always be done first, as the replacement joins are simulating rows filtering,
//  - The select-list should apply after, as their replacement joins never filter
//  - LATERAL should be the last operation.
pub(crate) fn unnest_all_subqueries<U: UniqueColumnsSchema>(
    stmt: &mut SelectStatement,
    ctx: &mut UnnestContext<U>,
) -> ReadySetResult<RewriteStatus> {
    // Subqueries in HAVING are handled upstream by
    // `normalize_subquery_positions`, which wraps the SELECT and migrates
    // supported HAVING subquery predicates into the wrapper's WHERE.
    // Backstop for shapes outside `is_supported_subquery_predicate` (e.g. a
    // top-level OR-conjunct predicate the wrap's per-conjunct split does not
    // touch): a HAVING that still carries a subquery here has no cacheable
    // decorrelation path, so reject with a clean adapter-fallback error
    // instead of letting it drift through the pipeline as latent wrong-
    // results potential.
    if let Some(having) = &stmt.having
        && contains_select(having)
    {
        unsupported!("Subquery in HAVING outside the supported predicate set");
    }

    // GROUP BY subquery guardrail.  Standard SQL allows subqueries in
    // GROUP BY (uncorrelated -> single group; correlated -> per-row-
    // derived key), but no rewrite pass in this pipeline decorrelates
    // GROUP BY subqueries.  Reject cleanly here rather than letting the
    // shape drift to MIR lowering with a worse error.
    if let Some(group_by) = &stmt.group_by
        && group_by
            .fields
            .iter()
            .any(|f| matches!(f, FieldReference::Expr(e) if contains_select(e)))
    {
        unsupported!("Subquery in GROUP BY is not supported");
    }

    // The three passes below rewrite this statement, and a probe one of them builds is a join
    // this statement carries.  Give the statement a registry of its own so the three share what
    // they build for it, while a statement nested inside any of them starts from nothing and
    // builds the probes it reads itself.
    let outer_probes = mem::replace(&mut ctx.probes, ProbeRegistry::new());
    let statuses = unnest_subqueries_at_this_level(stmt, ctx);
    ctx.probes = outer_probes;
    statuses
}

fn unnest_subqueries_at_this_level<U: UniqueColumnsSchema>(
    stmt: &mut SelectStatement,
    ctx: &mut UnnestContext<U>,
) -> ReadySetResult<RewriteStatus> {
    let status1 = unnest_subqueries_in_where(stmt, ctx)?;
    let status2 = unnest_subqueries_in_fields(stmt, ctx)?;
    let status3 = unnest_lateral_subqueries(stmt, ctx)?;
    Ok(status1.combine(status2).combine(status3))
}

/// A `UniqueColumnsSchema` impl that reports no unique keys for any relation.
/// Used as a placeholder by call sites that route through
/// `as_joinable_derived_table_with_opts` exclusively with `SubqueryContext::Exists`
/// (where the unique-key cardinality proof never fires) and that do not have a
/// real schema handy.
pub(crate) struct UnusedUniqueColumnsSchema;

impl UniqueColumnsSchema for UnusedUniqueColumnsSchema {
    fn unique_columns_of(&self, _rel: &Relation) -> Option<HashSet<Column>> {
        None
    }
}

/// Returns `true` if `stmt` is structurally proven to return at most one row,
/// based on the WHERE clause pinning every column of some unique key on the
/// inner FROM's base table.
///
/// Shape:
///   - exactly one local FROM item, which is a base `Table` (not a subquery
///     / VALUES / nested derived);
///   - no `GROUP BY` (those cases route through
///     [`is_correlation_pinned_at_most_one`] / `is_at_most_one_deep`'s GBY arm);
///   - WHERE is a top-level conjunction whose equi-predicates of shape
///     `base_alias.col = <rhs>` collectively name every column of some unique
///     key on `base_table` per `unique_keys_of`.
///
/// The right-hand side of each equi-predicate must not itself reference the
/// base table — otherwise the predicate constrains the join of `base` with
/// itself rather than pinning to a single row.  Outer-scope references,
/// placeholders, literals, and other-relation references are all admissible
/// RHS shapes.  Non-equi atoms (range, IS NULL, function predicates) are
/// ignored: they may further restrict the row set but do not contribute to
/// the cardinality proof.
///
/// Both operand orders are accepted (`col = rhs` and `rhs = col`): the join
/// canonicalization pass that swaps eq-operands runs later in the pipeline,
/// so the check must be order-agnostic here.
fn is_unique_key_pinned_scalar<U: UniqueColumnsSchema>(
    stmt: &SelectStatement,
    unique_cols_schema: &U,
) -> ReadySetResult<bool> {
    let [table_expr] = stmt.tables.as_slice() else {
        return Ok(false);
    };
    if !stmt.join.is_empty() || stmt.group_by.is_some() {
        return Ok(false);
    }
    let TableExprInner::Table(base_table) = &table_expr.inner else {
        return Ok(false);
    };
    let base_alias = get_from_item_reference_name(table_expr)?;
    let Some(where_expr) = &stmt.where_clause else {
        return Ok(false);
    };

    let mut pinned: HashSet<SqlIdentifier> = HashSet::new();
    let atoms = decompose_conjuncts(where_expr).unwrap_or_else(|| vec![where_expr.clone()]);
    for atom in &atoms {
        let Expr::BinaryOp {
            lhs,
            op: BinaryOperator::Equal,
            rhs,
        } = atom
        else {
            continue;
        };
        let (col_expr, other_expr) = match (lhs.as_ref(), rhs.as_ref()) {
            (Expr::Column(c), other) if c.table.as_ref() == Some(&base_alias) => (c, other),
            (other, Expr::Column(c)) if c.table.as_ref() == Some(&base_alias) => (c, other),
            _ => continue,
        };
        if columns_iter(other_expr).any(|c| c.table.as_ref() == Some(&base_alias)) {
            continue;
        }
        pinned.insert(col_expr.name.clone());
    }

    let Some(keys) = unique_cols_schema.unique_keys_of(base_table) else {
        return Ok(false);
    };
    Ok(keys.iter().any(|key| {
        key.iter()
            .all(|k| k.table.as_ref() == Some(base_table) && pinned.contains(&k.name))
    }))
}

/// Returns `true` if the body's GROUP BY is fully pinned by correlation
/// predicates referencing relations OUTSIDE the body's local FROM, so each
/// outer row produces at most one body group.
///
/// Adapts the correlation-pinning branch of
/// `inline_subquery_checks::is_at_most_one_deep` to the per-LATERAL-body
/// context.  Distinct from `agg_only_no_gby_cardinality(stmt) == ExactlyOne`
/// (which detects aggregate-only-no-GROUP-BY bodies that always yield 1 row
/// regardless of match): this function detects bodies that yield 0 OR 1 row
/// per outer.
///
/// Requirements:
///   - body has a GROUP BY,
///   - every GROUP BY column is correlation-pinned by at least one
///     equality predicate against an outside relation, supplied via
///     WHERE OR via an INNER JOIN ON (which is structurally equivalent
///     to WHERE under selection-pushdown commutativity),
///   - HAVING is permitted regardless of its predicate: when HAVING drops
///     the single group the body yields 0 rows, which still satisfies
///     AtMostOne (0 OR 1 row per outer).
///
/// JOIN-ON correlation is admitted by running
/// `move_correlated_constraints_from_join_to_where` on a clone of `stmt`
/// before classifying.  The clone keeps the caller's `stmt` untouched,
/// so the function is safe to invoke at any pipeline phase without
/// disturbing later passes.
pub(crate) fn is_correlation_pinned_at_most_one(stmt: &SelectStatement) -> ReadySetResult<bool> {
    let Some(_group_by) = &stmt.group_by else {
        return Ok(false);
    };
    let locals = collect_local_from_items(stmt)?;
    let mut normalized = stmt.clone();
    move_correlated_constraints_from_join_to_where(&mut normalized, &|rel| !locals.contains(rel))?;
    let Some(where_expr) = &normalized.where_clause else {
        return Ok(false);
    };
    let (Some(corr), _remaining) =
        partition_correlated_predicates(where_expr, &|rel| !locals.contains(rel))
    else {
        return Ok(false);
    };
    let cols_set = extract_correlation_keys(&corr, &locals)?;
    are_group_by_keys_pinned_by_correlation(&cols_set, &normalized)
}

/// Walk the statement and collect, for each LATERAL subquery, three hints:
///  (1) whether its body is agg-only/no-GBY and **ExactlyOne** (wrapper-aware),
///  (2) whether its body is correlation-pinned **AtMostOne** GROUP BY, and
///  (3) whether its original ON was trivial (Empty / ON TRUE / comma segment).
///
/// Hints (1) and (2) are mutually exclusive: agg-only/no-GBY bodies and
/// grouped-with-correlation bodies are different shapes, and conflating
/// them on the ExactlyOne side would silently corrupt the COALESCE-zero
/// promotion in `get_join_operator_for_lateral`.
///
/// The three hint sets are keyed by alias-as-`Relation` with no scope
/// discriminator.  When two LATERAL subqueries at different nesting levels
/// in the input share an alias, a consumer at one scope would silently
/// inherit the classification recorded from the other scope's LATERAL and
/// misapply it (`get_join_operator_for_lateral` promoting a JOIN that
/// doesn't have agg-empty defaults; ILDT/DTR admitting an inlining not
/// backed by AtMostOne cardinality; the hoister skipping ON-clause
/// preservation on a non-trivial JOIN).  Skip classification entirely for
/// aliases that occur on LATERAL FROM items at more than one nesting level
/// — the absent hint routes consumers through the conservative fallback
/// path already used when no classification fires.
pub(crate) fn collect_lateral_hints<U: UniqueColumnsSchema>(
    stmt: &SelectStatement,
    ctx: &mut UnnestContext<U>,
) -> ReadySetResult<()> {
    let mut lateral_counts: HashMap<Relation, usize> = HashMap::new();
    count_lateral_aliases(stmt, &mut lateral_counts);
    let collision_aliases: HashSet<Relation> = lateral_counts
        .into_iter()
        .filter_map(|(rel, n)| (n > 1).then_some(rel))
        .collect();

    collect_lateral_hints_impl(stmt, ctx, &collision_aliases)
}

fn collect_lateral_hints_impl<U: UniqueColumnsSchema>(
    stmt: &SelectStatement,
    ctx: &mut UnnestContext<U>,
    collision_aliases: &HashSet<Relation>,
) -> ReadySetResult<()> {
    for (tab_expr_idx, tab_expr) in get_local_from_items_iter!(stmt).enumerate() {
        if let Some((inner, alias)) = as_sub_query_with_alias(tab_expr) {
            // Recurse first to collect nested hints
            collect_lateral_hints_impl(inner, ctx, collision_aliases)?;

            if inner.lateral {
                let alias_rel: Relation = alias.clone().into();

                // Skip classification for cross-scope alias collisions; the
                // three hint sets can't distinguish scopes and a consumer at
                // this scope would inherit a classification recorded from
                // another.  See the doc comment on `collect_lateral_hints`.
                if collision_aliases.contains(&alias_rel) {
                    continue;
                }

                // Existing ExactlyOne detection (aggregate-only-no-GROUP-BY).
                if matches!(
                    agg_only_no_gby_cardinality(inner)?,
                    Some(AggNoGbyCardinality::ExactlyOne)
                ) {
                    ctx.pre_hoist_lateral_exactly_one.insert(alias_rel.clone());
                }

                // AtMostOne via correlation-pinned GROUP BY.
                // Mutually exclusive with the ExactlyOne set: aggregate-only-
                // no-GROUP-BY and grouped-with-correlation are different
                // shapes, and the existing ExactlyOne consumer
                // (`get_join_operator_for_lateral`) relies on the
                // ExactlyOne-with-aggregate-empty-defaults contract that
                // grouped bodies do NOT satisfy.
                if !ctx.pre_hoist_lateral_exactly_one.contains(&alias_rel)
                    && is_correlation_pinned_at_most_one(inner)?
                {
                    ctx.pre_hoist_lateral_at_most_one.insert(alias_rel.clone());
                }

                let trivial = if let Some((jc_idx, _)) = find_rhs_join_clause(stmt, tab_expr_idx) {
                    matches!(
                        stmt.join[jc_idx].constraint,
                        JoinConstraint::Empty
                            | JoinConstraint::On(Expr::Literal(Literal::Boolean(true)))
                    )
                } else {
                    true // comma/CROSS segment behaves like ON TRUE
                };
                if trivial {
                    ctx.lateral_trivial_on.insert(alias_rel);
                }
            }
        }
    }
    Ok(())
}

/// Count LATERAL subquery aliases across every scope reachable from `stmt`
/// (nested subquery bodies included).  Non-LATERAL subquery aliases are not
/// counted — only LATERAL FROM items feed `collect_lateral_hints`'s
/// classification, so a non-LATERAL alias that happens to match a LATERAL
/// alias elsewhere can't cause a hint-set misapplication.
fn count_lateral_aliases(stmt: &SelectStatement, out: &mut HashMap<Relation, usize>) {
    for tab_expr in get_local_from_items_iter!(stmt) {
        if let Some((inner, alias)) = as_sub_query_with_alias(tab_expr) {
            if inner.lateral {
                *out.entry(alias.into()).or_insert(0) += 1;
            }
            count_lateral_aliases(inner, out);
        }
    }
}

#[cfg(test)]
mod nonnull_schema_impl_tests {
    use super::*;
    use readyset_sql::ast::{
        ColumnConstraint, ColumnSpecification, CreateTableBody, IndexKeyPart, SqlType, TableKey,
    };

    /// Build a minimal `CreateTableBody` with the given column names, inline
    /// constraints, and table-level keys.  Mirrors the helper in
    /// `drop_redundant_join`'s test module.
    fn make_body(
        cols: &[(&str, &str, Vec<ColumnConstraint>)],
        keys: Vec<TableKey>,
    ) -> CreateTableBody {
        CreateTableBody {
            fields: cols
                .iter()
                .map(|(table, name, constraints)| ColumnSpecification {
                    column: Column {
                        name: (*name).into(),
                        table: Some(Relation::from(*table)),
                    },
                    sql_type: SqlType::Int(None),
                    constraints: constraints.clone(),
                    generated: None,
                    comment: None,
                    invisible: false,
                })
                .collect(),
            keys: if keys.is_empty() { None } else { Some(keys) },
        }
    }

    fn mk_col(table: &str, name: &str) -> Column {
        Column {
            name: name.into(),
            table: Some(Relation::from(table)),
        }
    }

    /// Inline `PRIMARY KEY` on a single column produces a NOT NULL entry.
    /// Baseline coverage of the existing field-walk branch.
    #[test]
    fn inline_pk_column_is_not_null() {
        let rel: Relation = "t".into();
        let body = make_body(
            &[
                ("t", "id", vec![ColumnConstraint::PrimaryKey]),
                ("t", "name", vec![]),
            ],
            vec![],
        );
        let schema = NonNullSchemaImpl::from(HashMap::from([(rel.clone(), body)]));
        let nn = schema.not_null_columns_of(&rel);
        assert!(nn.contains(&mk_col("t", "id")));
        assert!(!nn.contains(&mk_col("t", "name")));
    }

    /// Inline `NOT NULL` produces a NOT NULL entry.  Baseline coverage of
    /// the existing field-walk branch.
    #[test]
    fn inline_not_null_column_is_not_null() {
        let rel: Relation = "t".into();
        let body = make_body(&[("t", "email", vec![ColumnConstraint::NotNull])], vec![]);
        let schema = NonNullSchemaImpl::from(HashMap::from([(rel.clone(), body)]));
        assert!(
            schema
                .not_null_columns_of(&rel)
                .contains(&mk_col("t", "email"))
        );
    }

    /// Single-column table-level `PRIMARY KEY` produces a NOT NULL entry.
    /// Pins the table-level PK walk.  Without it, schemas using the
    /// `CREATE TABLE t (id INT, PRIMARY KEY(id))` form miss the NOT NULL
    /// fact entirely.
    #[test]
    fn table_level_pk_single_column_is_not_null() {
        let rel: Relation = "t".into();
        let body = make_body(
            &[("t", "id", vec![]), ("t", "name", vec![])],
            vec![TableKey::PrimaryKey {
                constraint_name: None,
                constraint_timing: None,
                index_name: None,
                columns: vec![IndexKeyPart::Column(mk_col("t", "id"))],
            }],
        );
        let schema = NonNullSchemaImpl::from(HashMap::from([(rel.clone(), body)]));
        let nn = schema.not_null_columns_of(&rel);
        assert!(nn.contains(&mk_col("t", "id")));
        assert!(!nn.contains(&mk_col("t", "name")));
    }

    /// Composite table-level `PRIMARY KEY` produces a NOT NULL entry for
    /// EVERY column member.  Asymmetric vs `UniqueColumnsSchemaImpl`: a
    /// composite UNIQUE does NOT imply per-column uniqueness, but a
    /// composite PRIMARY KEY DOES imply per-column NOT NULL (SQL standard).
    #[test]
    fn table_level_composite_pk_all_columns_are_not_null() {
        let rel: Relation = "t".into();
        let body = make_body(
            &[
                ("t", "tenant_id", vec![]),
                ("t", "row_id", vec![]),
                ("t", "data", vec![]),
            ],
            vec![TableKey::PrimaryKey {
                constraint_name: None,
                constraint_timing: None,
                index_name: None,
                columns: vec![
                    IndexKeyPart::Column(mk_col("t", "tenant_id")),
                    IndexKeyPart::Column(mk_col("t", "row_id")),
                ],
            }],
        );
        let schema = NonNullSchemaImpl::from(HashMap::from([(rel.clone(), body)]));
        let nn = schema.not_null_columns_of(&rel);
        assert!(nn.contains(&mk_col("t", "tenant_id")));
        assert!(nn.contains(&mk_col("t", "row_id")));
        assert!(!nn.contains(&mk_col("t", "data")));
    }

    /// Table-level `UNIQUE` alone does NOT produce a NOT NULL entry.
    /// UNIQUE permits multiple NULL rows under `NULLS DISTINCT` (the SQL
    /// default), and `NULLS NOT DISTINCT` only limits the count, not the
    /// nullability.  Pins the asymmetry against the PK branch.
    #[test]
    fn table_level_unique_alone_is_not_not_null() {
        let rel: Relation = "t".into();
        let body = make_body(
            &[("t", "id", vec![])],
            vec![TableKey::UniqueKey {
                constraint_name: None,
                constraint_timing: None,
                index_name: None,
                columns: vec![IndexKeyPart::Column(mk_col("t", "id"))],
                index_type: None,
                nulls_distinct: None,
            }],
        );
        let schema = NonNullSchemaImpl::from(HashMap::from([(rel.clone(), body)]));
        assert!(
            !schema
                .not_null_columns_of(&rel)
                .contains(&mk_col("t", "id"))
        );
    }
}

#[cfg(test)]
mod collect_lateral_hints_tests {
    use super::*;
    use crate::unnest_subqueries_3vl::ProbeRegistry;
    use readyset_sql::Dialect;
    use readyset_sql_parsing::parse_select;

    struct NoNonNulls;
    impl NonNullSchema for NoNonNulls {
        fn not_null_columns_of(&self, _rel: &Relation) -> HashSet<Column> {
            HashSet::new()
        }
    }

    struct PermissiveUniqueSchema;
    impl UniqueColumnsSchema for PermissiveUniqueSchema {
        fn unique_columns_of(&self, rel: &Relation) -> Option<HashSet<Column>> {
            Some(HashSet::from([Column {
                name: "k".into(),
                table: Some(rel.clone()),
            }]))
        }
    }

    fn empty_ctx<'a>(
        schema: &'a NoNonNulls,
        unique: &'a PermissiveUniqueSchema,
    ) -> UnnestContext<'a, PermissiveUniqueSchema> {
        UnnestContext {
            schema,
            unique_cols_schema: unique,
            probes: ProbeRegistry::new(),
            pre_hoist_lateral_exactly_one: HashSet::new(),
            pre_hoist_lateral_at_most_one: HashSet::new(),
            lateral_trivial_on: HashSet::new(),
            ancestor_scope: HashSet::new(),
            ancestor_scope_ordered: Vec::new(),
        }
    }

    /// A LATERAL subquery whose body is agg-only-no-GBY at the outer scope
    /// gets classified as `ExactlyOne`; a differently-shaped LATERAL with
    /// the SAME alias at a nested scope must not inherit the classification.
    /// Skip classification for both — the hint sets are alias-keyed with no
    /// scope discriminator, so applying either scope's classification to
    /// the other's LATERAL would silently produce wrong results.
    #[test]
    fn cross_scope_alias_collision_skips_classification() {
        // Outer's LATERAL `sub`: agg-only-no-GBY → would be ExactlyOne.
        // Inner's LATERAL `sub`: correlated non-agg-non-grouped → neither
        // ExactlyOne nor AtMostOne, but same alias.
        let stmt = parse_select(
            Dialect::PostgreSQL,
            "SELECT outer_t.k, sub.m, wrap.x
             FROM outer_t,
                  LATERAL (SELECT MAX(b.v) AS m FROM b WHERE b.k = outer_t.k) AS sub,
                  (SELECT inner_t.k, sub.x
                   FROM inner_t,
                        LATERAL (SELECT c.v AS x FROM c WHERE c.k = inner_t.k) AS sub) AS wrap",
        )
        .expect("parse");

        let schema = NoNonNulls;
        let unique = PermissiveUniqueSchema;
        let mut ctx = empty_ctx(&schema, &unique);
        collect_lateral_hints(&stmt, &mut ctx).expect("collect");

        let sub_rel: Relation = "sub".into();
        assert!(
            !ctx.pre_hoist_lateral_exactly_one.contains(&sub_rel),
            "colliding alias must not be classified as ExactlyOne"
        );
        assert!(
            !ctx.pre_hoist_lateral_at_most_one.contains(&sub_rel),
            "colliding alias must not be classified as AtMostOne"
        );
        assert!(
            !ctx.lateral_trivial_on.contains(&sub_rel),
            "colliding alias must not be classified as trivial-ON"
        );
    }

    /// A LATERAL alias that appears at exactly one scope should still be
    /// classified normally — the collision filter only fires on multi-scope
    /// occurrences.
    #[test]
    fn unique_lateral_alias_classifies_normally() {
        let stmt = parse_select(
            Dialect::PostgreSQL,
            "SELECT outer_t.k, sub.m
             FROM outer_t,
                  LATERAL (SELECT MAX(b.v) AS m FROM b WHERE b.k = outer_t.k) AS sub",
        )
        .expect("parse");

        let schema = NoNonNulls;
        let unique = PermissiveUniqueSchema;
        let mut ctx = empty_ctx(&schema, &unique);
        collect_lateral_hints(&stmt, &mut ctx).expect("collect");

        let sub_rel: Relation = "sub".into();
        // Non-colliding agg-only-no-GBY body classifies as ExactlyOne.
        assert!(
            ctx.pre_hoist_lateral_exactly_one.contains(&sub_rel),
            "unique-alias agg-only-no-GBY LATERAL should classify as ExactlyOne"
        );
        // Comma segment: treated as trivial-ON.
        assert!(
            ctx.lateral_trivial_on.contains(&sub_rel),
            "comma-segment LATERAL should classify as trivial-ON"
        );
    }

    /// Non-LATERAL subquery aliases don't participate in classification, so
    /// they don't count toward the collision check either — a LATERAL and a
    /// non-LATERAL sharing an alias should leave the LATERAL classified.
    #[test]
    fn non_lateral_shadow_does_not_trigger_collision() {
        let stmt = parse_select(
            Dialect::PostgreSQL,
            "SELECT outer_t.k, sub.m
             FROM outer_t,
                  LATERAL (SELECT MAX(b.v) AS m FROM b WHERE b.k = outer_t.k) AS sub,
                  (SELECT inner_t.k, sub.n
                   FROM inner_t,
                        (SELECT c.v AS n FROM c) AS sub) AS wrap",
        )
        .expect("parse");

        let schema = NoNonNulls;
        let unique = PermissiveUniqueSchema;
        let mut ctx = empty_ctx(&schema, &unique);
        collect_lateral_hints(&stmt, &mut ctx).expect("collect");

        let sub_rel: Relation = "sub".into();
        assert!(
            ctx.pre_hoist_lateral_exactly_one.contains(&sub_rel),
            "outer LATERAL should classify despite a same-alias non-LATERAL elsewhere"
        );
    }
}
