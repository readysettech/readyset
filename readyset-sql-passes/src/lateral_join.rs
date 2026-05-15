use crate::drop_redundant_join::UniqueColumnsSchema;
use crate::get_local_from_items_iter_mut;
use crate::inline_subquery::{
    InlineCandidate, apply_inline, can_inline_subquery, compute_downstream_for_position,
    prepare_inline,
};
use crate::rewrite_utils::{
    RewriteStatus, add_expression_to_join_constraint, align_group_by_and_windows_with_correlation,
    analyse_lone_aggregates_subquery_fields, and_predicates_skip_true, as_sub_query_with_alias,
    as_sub_query_with_alias_mut, collect_columns_in_expr_mut, collect_local_from_items,
    columns_iter, columns_iter_mut, contain_subqueries_with_limit_clause,
    default_alias_for_select_item_expression, expect_field_as_expr, expect_field_as_expr_mut,
    expect_sub_query_with_alias_mut, extract_aggregate_fallback_for_expr, extract_correlation_keys,
    get_from_item_reference_name, is_aggregation_or_grouped, is_always_true_filter,
    is_column_eq_column, is_filter_pushable_from_item, make_aliases_distinct_from_base_statement,
    move_correlated_constraints_from_join_to_where, outermost_expression_mut,
    partition_correlated_predicates, project_columns_if, split_expr,
};
use crate::unnest_subqueries::{
    AggNoGbyCardinality, UnnestContext, agg_only_no_gby_cardinality, force_empty_select,
    has_limit_zero_deep, is_supported_join_condition, rewrite_top_k_for_lateral,
    split_on_for_rhs_against_preceding_lhs, unnest_all_subqueries,
};
use itertools::Either;
use readyset_errors::{ReadySetResult, unsupported};
use readyset_sql::ast::JoinOperator::InnerJoin;
use readyset_sql::ast::{
    Column, Expr, FunctionExpr, JoinClause, JoinConstraint, JoinOperator, JoinRightSide, Literal,
    Relation, SelectStatement, SqlIdentifier, TableExpr, TableExprInner,
};
use readyset_sql::{Dialect, DialectDisplay};
use std::collections::{HashMap, HashSet};
use std::{iter, mem};

/// Checks if a table relation should be considered an "outer" table for correlation analysis,
/// as opposed to a local table within a subquery or join.
fn is_outer_from_item(
    from_item: &Relation,
    local_tables: &HashSet<Relation>,
    outer_tables: &HashSet<Relation>,
) -> bool {
    !local_tables.contains(from_item) && outer_tables.contains(from_item)
}

/// Splits a predicate expression into a correlated part (references outer tables)
/// and a non-correlated part, for extracting join predicates during rewriting.
fn split_correlated_expr(
    expr: &Expr,
    local_tables: &HashSet<Relation>,
    outer_tables: &HashSet<Relation>,
) -> (Option<Expr>, Option<Expr>) {
    partition_correlated_predicates(expr, &|rel| {
        is_outer_from_item(rel, local_tables, outer_tables)
    })
}

/// Checks if a column belongs to any of the specified FROM items.
/// Returns `false` for unqualified columns (`table: None`).
fn column_belongs_to(col: &Column, from_items: &HashSet<Relation>) -> bool {
    col.table.as_ref().is_some_and(|t| from_items.contains(t))
}

/// Checks if a qualified column references a table NOT in the given set.
/// Returns `false` for unqualified columns — absence of a table qualifier
/// is not evidence of correlation.
fn is_correlated_column(col: &Column, local_from_items: &HashSet<Relation>) -> bool {
    col.table
        .as_ref()
        .is_some_and(|t| !local_from_items.contains(t))
}

/// Determines whether a column should be treated as local (requiring
/// projection through or replacement with the subquery alias).
///
/// Returns `true` when:
///   (a) the column explicitly belongs to `local_from_items` — this
///       handles shadowing, where the same table name appears in both
///       inner and outer scopes (inner scope wins per SQL semantics), OR
///   (b) the column does not belong to `outer_from_items` — the
///       fallback for columns from nested subquery aliases or other
///       non-outer sources.
fn is_local_column(
    col: &Column,
    local_from_items: &HashSet<Relation>,
    outer_from_items: &HashSet<Relation>,
) -> bool {
    column_belongs_to(col, local_from_items) || !column_belongs_to(col, outer_from_items)
}

/// Updates local column references in an expression to use a given
/// table alias. See [`is_local_column`] for the locality classification.
fn replace_local_columns_table(
    expr: &mut Expr,
    from_item: Relation,
    local_from_items: &HashSet<Relation>,
    outer_from_items: &HashSet<Relation>,
) {
    columns_iter_mut(expr).for_each(|col| {
        if is_local_column(col, local_from_items, outer_from_items) {
            col.table = Some(from_item.clone());
        }
    });
}

/// For a given subquery, ensures all local columns referenced in an outer
/// expression are present in its SELECT projection and rewrites them to
/// use the subquery’s alias. See [`is_local_column`] for the locality
/// classification.
fn project_local_columns(
    tab_expr: &mut TableExpr,
    expr: &mut Expr,
    local_from_items: &HashSet<Relation>,
    outer_from_items: &HashSet<Relation>,
) -> ReadySetResult<()> {
    project_columns_if(tab_expr, expr, |col| {
        is_local_column(col, local_from_items, outer_from_items)
    })
}

/// Moves predicates from INNER JOIN ON clauses to the WHERE clause if they reference outer tables.
///
/// For each INNER JOIN, extracts correlated predicates from the ON condition and combines them with the WHERE clause.
/// Ensures correlated conditions are evaluated at the correct query level during LATERAL join rewrites.
fn move_correlated_join_on_to_where(
    stmt: &mut SelectStatement,
    local_from_items: &HashSet<Relation>,
    outer_from_items: &HashSet<Relation>,
) -> ReadySetResult<()> {
    move_correlated_constraints_from_join_to_where(stmt, &|rel| {
        is_outer_from_item(rel, local_from_items, outer_from_items)
    })
}

/// Scans a SELECT statement for correlated predicates in it's WHERE clause that reference outer tables.
/// If found, removes them from the subquery, builds a new subquery TableExpr, and returns it
/// alongside the correlated expression to be hoisted into a join condition.
/// Ensures any columns needed for the correlated predicate are projected by the subquery.
fn extract_correlated_subquery(
    stmt: &SelectStatement,
    stmt_alias: SqlIdentifier,
    outer_from_items: &HashSet<Relation>,
) -> ReadySetResult<Option<(TableExpr, Expr)>> {
    let mut stmt = stmt.clone();

    let local_from_items = collect_local_from_items(&stmt)?;

    // Try to move correlated constraints from the join ONs over to WHERE clause
    if !stmt.join.is_empty() {
        move_correlated_join_on_to_where(&mut stmt, &local_from_items, outer_from_items)?;
    }

    // Verify there are no correlation outside the WHERE clause
    // **NOTE**: We are visiting the outermost columns only, w/o walking into sub-queries.
    let where_clause = mem::take(&mut stmt.where_clause);
    let has_unsupported_correlation = stmt
        .outermost_referred_columns()
        .any(|col| is_correlated_column(col, &local_from_items));
    let _ = mem::replace(&mut stmt.where_clause, where_clause);

    if has_unsupported_correlation {
        unsupported!(
            "Unsupported correlation outside of WHERE clause: {}",
            stmt.display(Dialect::PostgreSQL)
        );
    }

    if let Some(where_clause) = &stmt.where_clause {
        // Extract that piece of the WHERE clause, which correlates with the legit outer scope.
        let (correlated_expr, remaining_expr) =
            split_correlated_expr(where_clause, &local_from_items, outer_from_items);

        // Verify the remaining expression has no correlation.
        // **NOTE**: We are visiting the outermost columns only, w/o walking into sub-queries.
        if let Some(remaining_expr) = &remaining_expr
            && columns_iter(remaining_expr).any(|col| is_correlated_column(col, &local_from_items))
        {
            unsupported!(
                "Statement: {}. Unsupported correlation in subquery: {}",
                stmt.display(Dialect::PostgreSQL),
                remaining_expr.display(Dialect::PostgreSQL)
            );
        }

        // `correlated_expr` contains equality constraints and simple filters,
        // involving columns from `outer_from_items`
        if let Some(mut correlated_expr) = correlated_expr {
            stmt.where_clause = remaining_expr;

            align_group_by_and_windows_with_correlation(
                &mut stmt,
                &extract_correlation_keys(&correlated_expr, &local_from_items)?,
            )?;

            let mut tab_expr = TableExpr {
                inner: TableExprInner::Subquery(Box::new(stmt)),
                alias: Some(stmt_alias.clone()),
                column_aliases: vec![],
            };

            project_local_columns(
                &mut tab_expr,
                &mut correlated_expr,
                &local_from_items,
                outer_from_items,
            )?;

            Ok(Some((tab_expr, correlated_expr)))
        } else {
            Ok(None)
        }
    } else {
        Ok(None)
    }
}

/// Recursively attempts to extract correlated subqueries from a TableExpr and its nested subqueries.
/// Returns the reference and join predicate for any subquery that can be rewritten as a join,
/// or None if no correlation is found.
fn try_extract_correlated_subquery(
    tab_expr: &mut TableExpr,
    outer_tables: &HashSet<Relation>,
) -> ReadySetResult<Option<(Relation, Expr)>> {
    let Some((stmt, stmt_alias)) = as_sub_query_with_alias_mut(tab_expr) else {
        return Ok(None);
    };

    if let Some((subquery_tab_expr, outer_join_on)) =
        extract_correlated_subquery(stmt, stmt_alias.clone(), outer_tables)?
    {
        // SAFETY: `extract_correlated_subquery` constructs the returned `TableExpr` with
        // `alias: Some(stmt_alias.clone())` (L163-166), so the alias is always present.
        let subquery_alias = subquery_tab_expr
            .alias
            .clone()
            .expect("extract_correlated_subquery always sets alias");
        let _ = mem::replace(tab_expr, subquery_tab_expr);
        return Ok(Some((subquery_alias.into(), outer_join_on)));
    }

    let local_from_items = collect_local_from_items(stmt)?;

    for (idx, local_tab_expr) in get_local_from_items_iter_mut!(stmt).enumerate() {
        if let Some((subquery_ref_name, mut outer_join_on)) =
            try_extract_correlated_subquery(local_tab_expr, outer_tables)?
        {
            if !is_filter_pushable_from_item(stmt, idx)? {
                unsupported!("LATERAL sub-query contains LEFT OUTER JOIN")
            }

            align_group_by_and_windows_with_correlation(
                stmt,
                &extract_correlation_keys(&outer_join_on, &local_from_items)?,
            )?;

            project_local_columns(
                tab_expr,
                &mut outer_join_on,
                &local_from_items,
                outer_tables,
            )?;
            return Ok(Some((subquery_ref_name, outer_join_on)));
        }
    }

    Ok(None)
}

/// Iteratively extracts and combines all correlated predicates from a FROM item’s subqueries,
/// producing a single join predicate for flattening a LATERAL join.
fn resolve_lateral_subquery(
    local_from_item: &mut TableExpr,
    preceding_outer_from_items: &HashSet<Relation>,
) -> ReadySetResult<Option<Expr>> {
    let mut outer_join_on = None;
    while let Some((_, join_on)) =
        try_extract_correlated_subquery(local_from_item, preceding_outer_from_items)?
    {
        outer_join_on = and_predicates_skip_true(outer_join_on, join_on);
    }
    Ok(outer_join_on)
}

/// Result of resolving a single LATERAL subquery.
enum LateralResolution {
    /// Standard: rewritten subquery + extracted ON expression.
    Resolved(TableExpr, Expr),
    /// Flatten: the wrapper qualifies for inlining into the parent.
    Flatten(Box<InlineCandidate>),
}

/// Returns `true` when `stmt` has at least one non-INNER JOIN whose ON
/// references a column not in `local_tables` (i.e. an outer-scope column).
fn has_outer_left_join_on(stmt: &SelectStatement, local_tables: &HashSet<Relation>) -> bool {
    stmt.join.iter().any(|jc| {
        if jc.operator.is_inner_join() {
            return false;
        }
        if let JoinConstraint::On(on_expr) = &jc.constraint {
            columns_iter(on_expr).any(|col| {
                col.table
                    .as_ref()
                    .is_some_and(|t| !local_tables.contains(t))
            })
        } else {
            false
        }
    })
}

/// Absorbs a `LateralResolution::Flatten` into the parent scope: runs the
/// common eligibility check, takes tables/joins from the prepared inline,
/// updates preceding tracking, and defers `apply_inline`.
///
/// `out_joins` is the target for absorbed JoinClauses (`new_joins` or `were_lateral`).
/// Tables that precede any LATERAL go to `out_tables`; once `had_lateral` is true,
/// they go to `out_joins` as INNER JOINs with empty constraint.
/// Parameter `inl_from_item_ord_idx` gives the ordinal position of the LATERAL
/// wrapper in `outer_stmt`'s outermost table exprs (index into the sequence
/// produced by `outermost_table_exprs`).
///
/// Returns the `InlineCandidate` with its `stmt.tables` and `stmt.join` taken
/// (absorbed into `out_tables`/`out_joins`); the remaining fields (WHERE,
/// HAVING, GROUP BY, ORDER, ext_to_int, alias) are intact for deferred
/// `apply_inline`. The caller pushes the returned value into its deferred
/// queue.
#[allow(clippy::too_many_arguments)]
fn absorb_flatten<'a, U: UniqueColumnsSchema>(
    mut prepared: InlineCandidate,
    outer_stmt: &SelectStatement,
    inl_from_item_ord_idx: usize,
    mut out_tables: Option<&mut Vec<TableExpr>>,
    out_joins: &mut Vec<JoinClause>,
    preceding_from_items: &mut HashSet<Relation>,
    preceding_to_rhs: &mut Vec<Relation>,
    pre_hoist_lateral_exactly_one: &'a HashSet<Relation>,
    pre_hoist_lateral_at_most_one: &'a HashSet<Relation>,
    unique_cols_schema: &'a U,
) -> ReadySetResult<(InlineCandidate, Vec<Expr>)> {
    let (ds_tables, ds_joins) = compute_downstream_for_position(outer_stmt, inl_from_item_ord_idx);

    // Full eligibility check with the outer statement.
    // `is_top_select = false`: `absorb_flatten` passes a conservative `false`.
    // In can_inline_subquery, this value is only consumed by
    // substitute_columns_in_expr (in `check_post_substitution_on_shape`),
    // whose receiver parameter is currently unused (pre-existing dead
    // plumbing — see the docstring on can_inline_subquery for details).
    // Passing `false` matches prior behavior here; revisit if
    // substitute_columns_in_expr is activated or the parameter is dropped
    // end-to-end.
    let ctx = crate::inline_subquery::InliningContext {
        inner_stmt: &prepared.stmt,
        outer_stmt,
        inner_alias: &prepared.alias,
        ext_to_int: &prepared.ext_to_int,
        inl_from_item_ord_idx,
        downstream_tables: ds_tables,
        downstream_joins: ds_joins,
        is_top_select: false,
        skip_unnesting_guard: false,
        inner_rel: prepared.alias.clone().into(),
        is_inner_agg: is_aggregation_or_grouped(&prepared.stmt)?,
        is_outer_agg: is_aggregation_or_grouped(outer_stmt)?,
        pre_hoist_lateral_exactly_one: Some(pre_hoist_lateral_exactly_one),
        pre_hoist_lateral_at_most_one: Some(pre_hoist_lateral_at_most_one),
        preceding_flattened_lateral_aliases: Some(preceding_from_items),
        unique_cols_schema: Some(unique_cols_schema),
    };

    let Some(downstream_group_by_additions) = can_inline_subquery(&ctx)? else {
        unsupported!("LATERAL wrapper not eligible for flattening after full eligibility check");
    };

    // Alias deduplication: rename any inner FROM-item alias that collides
    // with an outer top-level FROM-item alias. Must run BEFORE tables/joins
    // are absorbed. The inner `ext_to_int` map was built from the inner's
    // pre-dedup SELECT fields; since dedup renames FROM-item aliases (not
    // projected column aliases), `ext_to_int` remains valid.
    make_aliases_distinct_from_base_statement(
        outer_stmt,
        &prepared.alias,
        &mut prepared.stmt,
        &HashSet::new(),
    )?;

    let inner_tables = mem::take(&mut prepared.stmt.tables);
    let inner_joins = mem::take(&mut prepared.stmt.join);

    for t in inner_tables {
        let t_rel = get_from_item_reference_name(&t)?;
        preceding_from_items.insert(t_rel.clone());
        preceding_to_rhs.push(t_rel);
        if let Some(ref mut tables) = out_tables {
            tables.push(t);
        } else {
            out_joins.push(JoinClause {
                operator: JoinOperator::InnerJoin,
                right: JoinRightSide::Table(t),
                constraint: JoinConstraint::Empty,
            });
        }
    }
    for jc in inner_joins {
        for rhs_te in jc.right.table_exprs() {
            let rhs_rel = get_from_item_reference_name(rhs_te)?;
            preceding_from_items.insert(rhs_rel.clone());
            preceding_to_rhs.push(rhs_rel);
        }
        out_joins.push(jc);
    }
    Ok((prepared, downstream_group_by_additions))
}

/// LATERAL RHS sanitation (before limit/offset guard):
///  • LIMIT 0 ⇒ force RHS empty (recursively clear LIMIT/OFFSET/ORDER and set FALSE at the right level).
///  • Move RHS-internal correlated atoms from JOIN .. ON to WHERE so TOP‑K partitioning sees correlation.
///  • Rewrite TOP‑K (ORDER/LIMIT) to ROW_NUMBER filters (RN ≤ N), clearing raw ORDER/LIMIT for the legacy guard.
/// This ensures the legacy `contain_subqueries_with_limit_clause` check will pass for sanitized LATERAL bodies.
fn try_resolve_as_lateral_subquery<U: UniqueColumnsSchema>(
    from_item: &mut TableExpr,
    preceding_from_items: &HashSet<Relation>,
    preceding_to_rhs: &[Relation],
    ctx: &mut UnnestContext<U>,
    allow_flatten: bool,
    allow_aggregated_flatten: &mut bool,
) -> ReadySetResult<(bool, Option<LateralResolution>)> {
    let mut has_transformed;

    // Descend and resolve inner subqueries first; bail if not actually LATERAL
    let Some((stmt, body_alias_ident)) = as_sub_query_with_alias_mut(from_item) else {
        return Ok((false, None));
    };

    // Push current preceding items into ancestor scope so nested
    // LATERALs inside this subquery can correlate with them.
    let saved_scope = ctx.ancestor_scope.clone();
    let saved_ordered = ctx.ancestor_scope_ordered.clone();
    ctx.ancestor_scope
        .extend(preceding_from_items.iter().cloned());
    ctx.ancestor_scope_ordered
        .extend(preceding_to_rhs.iter().cloned());

    has_transformed = unnest_all_subqueries(stmt, ctx)?.has_rewrites();

    // Restore ancestor scope after recursion.
    ctx.ancestor_scope = saved_scope;
    ctx.ancestor_scope_ordered = saved_ordered;
    if stmt.lateral {
        stmt.lateral = false;
    } else {
        return Ok((has_transformed, None));
    }

    // --- Attempt flattening for projection-only wrappers with ancestor-scope LEFT JOINs ---
    // Only allowed when the outer position is a cross-join (comma-join), per the
    // algebraic identity A × (B ⟕_p C) = (A × B) ⟕_p C.
    // We use `stmt` (already borrowed mutably from `from_item`) to check the inner
    // statement's joins; if eligible we drop the mutable borrow before calling
    // `prepare_inline` on the whole `from_item`.
    //
    // LATERAL-specific: per-outer-row semantics that differ from global semantics
    // are blanket-rejected:
    //   - LIMIT / OFFSET: applied per outer row in LATERAL, global after flatten.
    //   - DISTINCT: deduplicated per outer row in LATERAL, globally after.
    //   - ORDER BY: per-outer ordering is meaningless at outer scope post-flatten.
    //
    // Aggregation/GROUP BY is conditionally accepted: only bodies whose
    // alias is in `pre_hoist_lateral_at_most_one` (correlation-pinned
    // GROUP BY → 0..1 row per outer) may take the Flatten path.  The
    // join-partner cardinality validation in
    // `check_join_partners_cardinality_preserving` (called from
    // `absorb_flatten`) ensures the lifted aggregation produces correct
    // results when the items joined to the LATERAL position are also
    // cardinality-preserving.
    //
    // `pre_hoist_lateral_exactly_one` (aggregate-only-no-GROUP-BY) is
    // deliberately rejected here even though the body returns exactly
    // one row per outer.  Post-flatten the body's outer-correlated
    // WHERE moves up to outer scope and drops outer rows whose body
    // input was empty — diverging from the pre-flatten per-outer-row
    // semantics where the aggregate over empty input yields the
    // empty-set value (e.g. `count(*) = 0`) and the outer row is
    // preserved.  No catalog fact proves "every outer row has at
    // least one matching body row," so ExactlyOne bodies fall through
    // to the Resolve path, which decorrelates as a LEFT OUTER JOIN
    // with COALESCE on the aggregate — preserving outer rows.
    //
    // `allow_aggregated_flatten` is an in-out gate owned by
    // `unnest_lateral_subqueries`.  Read here as a precondition; the
    // callee clears it to `false` if it actually returns `Flatten` for
    // an aggregated body, so the caller does not have to re-derive
    // whether the body was aggregated.  This rejects composition of
    // two aggregated LATERAL flattens onto the same outer FROM (which
    // would aggregate over the cross-product of both bodies — yielding
    // the *product* of the row counts, not either per-LATERAL
    // aggregate).
    //
    // HAVING is handled by `apply_inline`: with aggregates/GROUP BY,
    // HAVING is merged into outer HAVING; without them, HAVING is
    // semantically a WHERE filter and is merged into outer WHERE.
    let body_alias_rel: Relation = body_alias_ident.clone().into();
    let body_is_aggregated = is_aggregation_or_grouped(stmt)?;
    let lateral_flatten_safe = allow_flatten
        && stmt.limit_clause.is_empty()
        && stmt.order.is_none()
        && !stmt.distinct
        && (!body_is_aggregated || ctx.pre_hoist_lateral_at_most_one.contains(&body_alias_rel))
        && (!body_is_aggregated || *allow_aggregated_flatten);
    if lateral_flatten_safe {
        let local_tables = collect_local_from_items(stmt)?;
        if has_outer_left_join_on(stmt, &local_tables) {
            let prepared = prepare_inline(from_item.clone())?;
            if body_is_aggregated {
                *allow_aggregated_flatten = false;
            }
            return Ok((true, Some(LateralResolution::Flatten(Box::new(prepared)))));
        }
    }

    // --- Sanitize RHS prior to legacy LIMIT/ORDER guard ---
    // LIMIT 0 anywhere reachable (respecting aggregate-only cutoff) ⇒ force empty RHS deterministically.
    if has_limit_zero_deep(stmt) {
        force_empty_select(stmt);
        has_transformed = true;
    } else {
        // Move RHS-internal correlated atoms from JOIN ON to WHERE so TOP‑K can infer partitions.
        let locals = collect_local_from_items(stmt)?;
        move_correlated_join_on_to_where(stmt, &locals, preceding_from_items)?;

        // Materialize TOP‑K (ORDER/LIMIT) into RN (partitioned if correlation present in WHERE)
        if rewrite_top_k_for_lateral(stmt, &locals)? {
            has_transformed = true;
        }
    }

    if let Some(mut lateral_join_on) = resolve_lateral_subquery(from_item, preceding_from_items)? {
        let (stmt, stmt_alias) = expect_sub_query_with_alias_mut(from_item);
        // With RHS sanitized, we should not see raw LIMIT/OFFSET/ORDER here anymore.
        if contain_subqueries_with_limit_clause(stmt)? {
            unsupported!("LATERAL sub-queries with LIMIT clause")
        }
        let local_from_items = collect_local_from_items(stmt)?;
        replace_local_columns_table(
            &mut lateral_join_on,
            stmt_alias.into(),
            &local_from_items,
            preceding_from_items,
        );
        Ok((
            true,
            Some(LateralResolution::Resolved(
                from_item.clone(),
                lateral_join_on,
            )),
        ))
    } else {
        Ok((has_transformed, None))
    }
}

/// Populate COUNT→COALESCE mappings for wrapper SELECT lists by recursively chasing
/// projection-only wrappers down to a base expression that can yield a zero substitute.
/// Adds mappings for *outer* columns (qualified by `stmt_alias`).
fn build_select_count_zero_mappings_recursive(
    stmt: &SelectStatement,
    stmt_alias: SqlIdentifier,
    out_map: &mut HashMap<Column, ReadySetResult<Expr>>,
) -> ReadySetResult<()> {
    // Keep existing behavior for direct COUNT-like fields at this level
    analyse_lone_aggregates_subquery_fields(stmt, stmt_alias.clone(), out_map)?;

    // Supplement for wrapper-by-projection cases
    for f in &stmt.fields {
        let (expr, alias) = expect_field_as_expr(f);
        if let Some(zero_expr) = extract_aggregate_fallback_for_expr(expr, stmt)? {
            // OUTER column: this stmt’s alias + effective name
            let eff_name = alias
                .clone()
                .unwrap_or_else(|| default_alias_for_select_item_expression(expr));
            let out_col = Column {
                table: Some(stmt_alias.clone().into()),
                name: eff_name,
            };
            // COALESCE(out_col, zero_expr)
            let mapped = Expr::Call(FunctionExpr::Coalesce(vec![
                Expr::Column(out_col.clone()),
                zero_expr,
            ]));
            out_map.entry(out_col).or_insert(Ok(mapped));
        }
    }
    Ok(())
}

/// Selects the join operator for a rewritten LATERAL subquery.
fn get_join_operator_for_lateral<U: UniqueColumnsSchema>(
    tab_expr: &TableExpr,
    join_operator: JoinOperator,
    join_constraint: &JoinConstraint,
    ctx: &UnnestContext<U>,
    fields_map: &mut HashMap<Column, ReadySetResult<Expr>>,
) -> ReadySetResult<JoinOperator> {
    let Some((stmt, alias)) = as_sub_query_with_alias(tab_expr) else {
        return Ok(InnerJoin);
    };

    let rel: Relation = alias.clone().into();
    let have_prehoist_effective_one = ctx.pre_hoist_lateral_exactly_one.contains(&rel);
    let prehoist_on_trivial = ctx.lateral_trivial_on.contains(&rel);

    // True when the RHS is structurally ExactlyOne: either an aggregate-only/no-GROUP-BY core that
    // returns one row, or a chain of projection-only single-subquery wrappers around such a core.
    let rhs_is_structurally_exactly_one = matches!(
        agg_only_no_gby_cardinality(stmt)?,
        Some(AggNoGbyCardinality::ExactlyOne)
    );

    // Build COUNT/literal → fallback mappings only when the RHS is effectively ExactlyOne.
    //
    // This relies on the structural ExactlyOne classifier (`agg_only_no_gby_cardinality`) and/or
    // the pre-hoist ExactlyOne hint collected in Pass 2. When neither is true, we must not attempt
    // to infer empty-input fallbacks through wrappers (see `extract_aggregate_fallback_for_expr`’s
    // safety contract).
    let mut prehoist_fields_map = HashMap::new();
    if have_prehoist_effective_one || rhs_is_structurally_exactly_one {
        build_select_count_zero_mappings_recursive(stmt, alias.clone(), &mut prehoist_fields_map)?;
    }

    if have_prehoist_effective_one && prehoist_on_trivial {
        fields_map.extend(prehoist_fields_map);
        return Ok(JoinOperator::LeftOuterJoin);
    }

    // Reject only when ON references a mapped COUNT field **and** the RHS is
    // effectively ExactlyOne (pre-hoist hint or local classification).
    // For AtMostOne (e.g., HAVING present), no mapping is applied and ON over the field is legitimate.
    if (have_prehoist_effective_one || rhs_is_structurally_exactly_one)
        && !prehoist_fields_map.is_empty()
        && let JoinConstraint::On(join_expr) = join_constraint
        && columns_iter(join_expr)
            .into_iter()
            .any(|c| prehoist_fields_map.contains_key(c))
    {
        unsupported!("LATERAL join condition references a column requiring mapping to COALESCE")
    }

    if rhs_is_structurally_exactly_one {
        return Ok(match join_constraint {
            JoinConstraint::Empty | JoinConstraint::On(Expr::Literal(Literal::Boolean(true))) => {
                fields_map.extend(prehoist_fields_map);
                // ** cross-join/empty ON/ON TRUE ** - always emit LEFT JOIN to preserve
                JoinOperator::LeftOuterJoin
            }
            JoinConstraint::On(_) => {
                // Early guard above rejects ON when it references mapped COUNT fields under effective ExactlyOne.
                // Preserve caller's operator.
                join_operator
            }
            JoinConstraint::Using(_) => {
                // SAFETY: `expand_join_on_using` runs unconditionally in Block A
                // before all Block B passes, guaranteeing no USING constraints remain.
                // TODO: refactor to return `ReadySetResult` with `internal!()`.
                unreachable!("USING should have been desugared by expand_join_on_using")
            }
        });
    }

    if !join_operator.is_inner_join() {
        return Ok(join_operator);
    }

    Ok(InnerJoin)
}

/// Apply `fields_map` replacements to all `Expr::Column` nodes in `expr`.
/// Returns the first `Err` from `fields_map` if any matched column's mapping is an error.
fn apply_fields_map_to_expr(
    expr: &mut Expr,
    fields_map: &HashMap<Column, ReadySetResult<Expr>>,
) -> ReadySetResult<()> {
    for col_expr in collect_columns_in_expr_mut(expr) {
        if let Expr::Column(col) = col_expr
            && let Some(inl_expr) = fields_map.get(col)
        {
            match inl_expr {
                Ok(inl_expr) => *col_expr = inl_expr.clone(),
                Err(e) => return Err(e.clone()),
            }
        }
    }
    Ok(())
}

/// Check each top-level AND conjunct of a JOIN `ON` expression and reject if any conjunct
/// is a cross-table `col_a = col_b` equality where one column is in `fields_map`; those are
/// equi-join keys (§2.1 of known_core_limitations.md) that must stay as bare column references.
/// Otherwise apply `fields_map` in-place.
fn apply_or_reject_in_on_expr(
    expr: &mut Expr,
    fields_map: &HashMap<Column, ReadySetResult<Expr>>,
) -> ReadySetResult<()> {
    let mut conjuncts = Vec::new();
    split_expr(&*expr, &|_| true, &mut conjuncts);
    for conjunct in &conjuncts {
        if is_column_eq_column(conjunct, |left_col, right_col| {
            left_col.table.as_ref() != right_col.table.as_ref()
                && (fields_map.contains_key(left_col) || fields_map.contains_key(right_col))
        }) {
            unsupported!(
                "JOIN ON cross-table equality references a COUNT column requiring COALESCE mapping"
            );
        }
    }
    apply_fields_map_to_expr(expr, fields_map)
}

/// Replace column references from `fields_map` throughout the statement.
///
/// Background — why COALESCE is needed.  In the original LATERAL form
/// `(SELECT COUNT(*) FROM t WHERE t.k = outer.k)`, the inner aggregate
/// returns the SQL-standard value `0` for outer rows whose match set is
/// empty (COUNT-over-empty = 0, never NULL).  Our pipeline rewrites this
/// LATERAL into `LEFT JOIN (SELECT COUNT(*), k FROM t GROUP BY k) ON ...`
/// — i.e. LEFT-JOIN-of-pre-grouped-subquery.  For outer rows whose group
/// is absent from the pre-aggregation, LEFT JOIN propagates NULL into the
/// COUNT column.  That NULL is an artifact of our rewrite, not of
/// standard SQL semantics; left untreated it leaks into every position
/// the column appears in (SELECT list, filters, ORDER BY, GROUP BY,
/// single-relation JOIN ON conjuncts), where NULL changes which rows
/// survive, how they're ordered, and how they group.  Wrapping the
/// column with `COALESCE(col, 0)` in every such position restores the
/// original 0-on-empty semantics the user wrote.
///
/// Applies COUNT→COALESCE mappings to:
/// - SELECT list (with aliasing as needed)
/// - WHERE, HAVING, ORDER BY, GROUP BY (scalar evaluation contexts —
///   safe to wrap because they consume the column as a value)
/// - JOIN ON single-relation filters (only one table referenced — no
///   hash-join key shape issue)
///
/// Rejects JOIN ON cross-table equalities that reference a mapped column,
/// because those are hash-join keys that must remain bare column
/// references (§2.1 of known_core_limitations.md).
///
/// # Arguments
/// * `stmt` – mutable reference to the `SelectStatement`.
/// * `fields_map` – map from `Column` to `Ok(coalesce_expr)` or `Err(ReadySetError)`.
///
/// # Errors
/// Returns the first `Err` from `fields_map`, or an `Unsupported` error for cross-table
/// equality ON predicates that reference a mapped column.
fn coalesce_fields_references(
    stmt: &mut SelectStatement,
    fields_map: &HashMap<Column, ReadySetResult<Expr>>,
) -> ReadySetResult<()> {
    // 1: alias SELECT items that will be replaced where the replacement is a renamed column.
    //    In practice fields_map always holds COALESCE expressions (not column renames), so this
    //    loop is a no-op for the current callers. Kept as a safety guard for future reuse.
    for select_item in &mut stmt.fields {
        let (expr, maybe_alias) = expect_field_as_expr_mut(select_item);
        if maybe_alias.is_none()
            && matches!(expr, Expr::Column(col) if fields_map.get(col).is_some_and(|e| matches!(e,
                                Ok(Expr::Column(inl_col)) if col.name != inl_col.name)
            ))
        {
            *maybe_alias = Some(default_alias_for_select_item_expression(expr));
        }
    }
    // 2: handle JOIN ON first — walk conjuncts, reject cross-table equalities, apply elsewhere.
    for join in &mut stmt.join {
        if let JoinConstraint::On(on_expr) = &mut join.constraint {
            apply_or_reject_in_on_expr(on_expr, fields_map)?;
        }
    }
    // 3: apply COALESCE to every remaining column reference (SELECT, WHERE, HAVING, ORDER BY,
    //    GROUP BY). Temporarily take joins so outermost_expression_mut skips the ON expressions
    //    we already rewrote — preventing double-wrapping.
    //
    // outermost_expression_mut does not descend into nested subqueries. This is sound because:
    //
    // a) WHERE/SELECT-list subqueries: unnest_subqueries_in_where and
    //    unnest_subqueries_in_fields run before LATERAL inlining (see unnest_all_subqueries).
    //    Any subquery referencing a LATERAL COUNT column was either already unnested (reference
    //    is now at the outer scope) or rejected with Unsupported before we reach here.
    //
    // b) Downstream LATERAL bodies: a downstream LATERAL that references an upstream COUNT
    //    column will have that reference hoisted to its JOIN ON as a cross-table equality
    //    (e.g. lat2._corr = lat1.cnt), which is caught and rejected by
    //    `check_nested_aggregation` above.
    //    If the reference survives in the body SELECT, the downstream join's own ON condition
    //    is `lat2._corr = lat1.cnt = NULL` for no-match rows, so the body result is never
    //    observed — LEFT JOIN NULL-propagation makes the missing COALESCE irrelevant.
    let saved_joins = mem::take(&mut stmt.join);
    let result = outermost_expression_mut(stmt)
        .try_for_each(|expr| apply_fields_map_to_expr(expr, fields_map));
    stmt.join = saved_joins;
    result
}

/// Identifies which single RHS table in a JoinClause is referenced
/// by the ON-condition.
/// Scans the ON expression to collect all table qualifiers, then checks
/// which table from `jc.right` appears.
/// Returns:
/// - `Ok(Some(Relation))` if exactly one RHS table is mentioned
/// - `Ok(None)` if no RHS table appears in the ON condition
/// - `Err(ReadySetError::Unsupported)` if multiple RHS tables appear
fn get_rhs_referenced_in_join_expression(jc: &JoinClause) -> ReadySetResult<Option<Relation>> {
    Ok(if let JoinConstraint::On(jc_expr) = &jc.constraint {
        let jc_on_from_items = columns_iter(jc_expr)
            .filter_map(|col| col.table.clone())
            .collect::<HashSet<_>>();
        let mut rhs_from_jc_on = None;
        for rhs_from_item in jc.right.table_exprs() {
            let rhs_ref_name = get_from_item_reference_name(rhs_from_item)?;
            if jc_on_from_items.contains(&rhs_ref_name) {
                if rhs_from_jc_on.is_none() {
                    rhs_from_jc_on = Some(rhs_ref_name);
                } else {
                    unsupported!("Join condition on more than 2 tables")
                }
            }
        }
        rhs_from_jc_on
    } else {
        None
    })
}

/// Main transformation function for a SELECT statement:
/// Converts all LATERAL subqueries in the FROM and JOIN clauses to regular joins,
/// updating join constraints and projections as needed to maintain semantic equivalence.
/// Return `true` if any transformation happened to the statement itself or any of its inner subqueries.
fn resolve_lateral_subqueries<U: UniqueColumnsSchema>(
    stmt: &mut SelectStatement,
    ctx: &mut UnnestContext<U>,
) -> ReadySetResult<RewriteStatus> {
    let mut rewrite_status = RewriteStatus::default();

    // The LATERAL subqueries can be correlated with the preceding FROM items only,
    // As we are moving along the FROM items, the number of preceding FROM items might increase.
    // This HashSet maintains the current set of items, the remaining LATERAL subqueries can be correlated with.
    // Seeded from ancestor scope so nested LATERALs see grandparent tables.
    let mut preceding_from_items = ctx.ancestor_scope.clone();

    // Ordered FROM items, preceding current RHS. Maintained as we advance over the `stmt` FROM items.
    // Seeded from ancestor scope for ON normalization LHS selection.
    let mut preceding_to_rhs = ctx.ancestor_scope_ordered.clone();

    // Build new comma separated tables/sub-queries and joins
    let mut new_tables = Vec::new();
    let mut new_joins = Vec::new();

    // The pieces of transformed `LATERAL` join ON expressions which have to be moved to the base statement WHERE.
    let mut add_to_where = None;

    // ── SELECT‑list COALESCE mapping policy for LATERAL lone‑aggregate (COUNT) ─────────────
    // We only use a SELECT‑list replacement map when **all** of the following hold:
    //   • RHS is aggregate‑only without GROUP BY and classified as **ExactlyOne** (no HAVING / HAVING TRUE);
    //   • The join constraint was `Empty` or `ON TRUE` (i.e., CROSS/comma semantics), so
    //     `get_join_operator_for_lateral` promoted the join to **LeftOuterJoin** and populated this map;
    //   • The first projected field on RHS is from the **COUNT** family; SUM/MIN/MAX are **not** mapped.
    //
    // Rationale:
    //   After correlation hoist + grouping by local keys, an **absent key** yields **no RHS row**.
    //   For COUNT, the original scalar over empty input is **0** (not NULL); using
    //   `coalesce_fields_references(..)` replaces occurrences of that COUNT column with
    //   `COALESCE(col, 0)` in SELECT, WHERE, HAVING, ORDER BY, GROUP BY, and single-relation
    //   JOIN ON conjuncts. Cross-table equality ON predicates are rejected (§2.1).
    //
    // Non‑goals / exclusions:
    //   • **AtMostOne** (HAVING present): we do **not** populate this map—projections should remain NULL when
    //     HAVING filters the group out.
    //   • Non‑trivial `ON` joins: we preserve the caller’s operator and **do not** populate this map so LEFT+ON
    //     semantics (including NULL RHS) remain visible.
    let mut coalesce_fields_map: HashMap<Column, ReadySetResult<Expr>> = HashMap::new();

    // Used to preserve the original joining order for comma separated tables/sub-queries in the loop below.
    // After the first handled `lateral`, all items from `stmt.tables` will be added to `new_joins`,
    // regardless of being `lateral` or not.
    let mut had_lateral = false;

    // Deferred apply_inline calls for flattened LATERAL wrappers.
    // We cannot mutably borrow `stmt` during iteration, so we collect them
    // and apply after the loops splice tables/joins into place.  Each entry
    // pairs the prepared inline with the `downstream_group_by_additions`
    // captured by `check_group_by_compatibility`; the additions
    // must be threaded into `apply_inline` so that the outer GROUP BY is
    // extended with bare-column references from the outer SELECT (which
    // is required when the inner is aggregated/grouped — without the
    // additions the lifted aggregation would collapse all outer rows into
    // a single group).
    let mut deferred_inlines: Vec<(InlineCandidate, Vec<Expr>)> = Vec::new();

    // In-out gate threaded through `try_resolve_as_lateral_subquery`.
    // Starts `true`; the callee clears it to `false` after queueing
    // the first aggregated LATERAL flatten in this FROM list, so
    // subsequent aggregated-body candidates fall through to Resolve.
    // The gate is per-FROM-list scope; nested LATERAL un-nesting
    // (via `unnest_all_subqueries`) creates its own local in its own
    // `unnest_lateral_subqueries` frame.
    let mut allow_aggregated_flatten = true;

    // Normalizes the ON for a rewritten LATERAL:
    // - Keep only atoms over {RHS, chosen LHS} in ON; push the remainder to outer WHERE.
    // - Safe to push only when the **current** join is INNER. With only LEFT supported later,
    //   filters over already-joined relations are equivalent before/after later LEFT joins.
    // - For current LEFT joins, do not push (would null-reject).
    macro_rules! normalize_lateral_join_constraint {
        ($stmt:expr, $from_item_rel:expr, $join_op:expr, $candidate_on:expr) => {{
            let split = split_on_for_rhs_against_preceding_lhs(
                &$candidate_on,
                &$from_item_rel.into(),
                &preceding_to_rhs,
            );

            // Safe: push to WHERE only for INNER joins.
            if $join_op.is_inner_join() {
                if let Some(to_where) = split.to_where {
                    add_to_where = and_predicates_skip_true(add_to_where, to_where);
                }
            } else {
                // Current join is LEFT OUTER: moving ON → WHERE would null-reject.
                if split.to_where.is_some() {
                    unsupported!(
                        "LATERAL normalization would move ON filters out of a LEFT OUTER join"
                    )
                }
            }

            if let Some(join_on) = split.on_expr {
                debug_assert!(
                    is_supported_join_condition(&join_on),
                    "Expected RHS↔LHS equality in ON"
                );
                JoinConstraint::On(join_on)
            } else {
                JoinConstraint::Empty
            }
        }};
    }

    // Handle the left-hand side comma separated tables/sub-queries.
    for (stmt_tables_idx, stmt_from_item) in stmt.tables.iter().enumerate() {
        //
        let mut fields_map = HashMap::new();
        let join_operator_for_lateral = get_join_operator_for_lateral(
            stmt_from_item,
            JoinOperator::CrossJoin,
            &JoinConstraint::Empty,
            ctx,
            &mut fields_map,
        );

        let mut from_item = stmt_from_item.clone();
        let from_item_rel = get_from_item_reference_name(&from_item)?;

        // Comma-join position is always a cross-join, so flattening is allowed.
        let (transformed, resolved_option) = try_resolve_as_lateral_subquery(
            &mut from_item,
            &preceding_from_items,
            &preceding_to_rhs,
            ctx,
            true, // allow_flatten
            &mut allow_aggregated_flatten,
        )?;

        preceding_from_items.insert(from_item_rel.clone());

        match resolved_option {
            Some(LateralResolution::Flatten(prepared)) => {
                deferred_inlines.push(absorb_flatten(
                    *prepared,
                    stmt,
                    stmt_tables_idx,
                    if had_lateral {
                        None
                    } else {
                        Some(&mut new_tables)
                    },
                    &mut new_joins,
                    &mut preceding_from_items,
                    &mut preceding_to_rhs,
                    &ctx.pre_hoist_lateral_exactly_one,
                    &ctx.pre_hoist_lateral_at_most_one,
                    ctx.unique_cols_schema,
                )?);
                had_lateral = true;
                rewrite_status.rewrite();
            }
            Some(LateralResolution::Resolved(resolved_from_item, lateral_join_on)) => {
                let join_operator = join_operator_for_lateral?;
                let join_constraint = normalize_lateral_join_constraint!(
                    stmt,
                    from_item_rel.clone(),
                    join_operator,
                    lateral_join_on
                );
                new_joins.push(JoinClause {
                    operator: join_operator,
                    right: JoinRightSide::Table(resolved_from_item),
                    constraint: join_constraint,
                });
                coalesce_fields_map.extend(fields_map);
                had_lateral = true;
            }
            None => {
                if had_lateral {
                    new_joins.push(JoinClause {
                        operator: JoinOperator::InnerJoin,
                        right: JoinRightSide::Table(from_item),
                        constraint: JoinConstraint::Empty,
                    });
                } else {
                    new_tables.push(from_item);
                }
            }
        }

        preceding_to_rhs.push(from_item_rel);

        if transformed {
            rewrite_status.rewrite();
        }
    }

    // Running absolute position in `outermost_table_exprs(stmt)` for the next
    // RHS item we will process. Starts AFTER all `stmt.tables` items, then
    // advances through each join's RHS table exprs.
    let mut outermost_idx_cursor = stmt.tables.len();

    for stmt_jc in stmt.join.iter() {
        // Figure out the RHS item, which is referenced in the join expression
        let rhs_from_jc_on = get_rhs_referenced_in_join_expression(stmt_jc)?;

        // Collect the items which were regular (not LATERAL), and which were LATERAL before the transformation.
        let mut were_regular = Vec::new();
        let mut were_lateral = Vec::new();

        // It will be `true` if the RHS item, referenced in the join condition, was regular - not LATERAL
        let mut rhs_from_item_is_regular = false;

        // We might have multiple items inside `stmt_jc.right`.
        // The current join condition can only connect one of them as `RHS` and a preceding `LHS` item,
        // while the others are just cross joined (or comma separated) ones.
        // Each of these items will either be added to `were_regular` if it was not `lateral`, or
        // to `were_lateral` otherwise.
        for (rhs_from_item_index, rhs_from_item) in stmt_jc.right.table_exprs().enumerate() {
            // Check if the current RHS item is the one referenced in the current join condition.
            let is_rhs_from_item = if let Some(rhs_from_jc_on) = &rhs_from_jc_on {
                get_from_item_reference_name(rhs_from_item)?.eq(rhs_from_jc_on)
            } else {
                // This is a special case when the join constraint is Empty, or is a PG favorite join ON TRUE,
                // or the join constraint is not referencing any of the RHS at all.
                // In this case, only the first RHS item will carry over the current join operator.
                rhs_from_item_index == 0
            };

            // Figure out what join operator should be used if `rhs_from_item` turns out to be LATERAL.
            // **NOTE**: it should be determined before transformation, as it might update its format.
            let mut fields_map = HashMap::new();
            let join_operator_for_lateral = get_join_operator_for_lateral(
                rhs_from_item,
                stmt_jc.operator,
                if is_rhs_from_item {
                    &stmt_jc.constraint
                } else {
                    &JoinConstraint::Empty
                },
                ctx,
                &mut fields_map,
            );

            let mut from_item = rhs_from_item.clone();
            let from_item_rel = get_from_item_reference_name(&from_item)?;

            // Compute allow_flatten: flattening is only valid for cross-join positions,
            // i.e. when the effective join is INNER with Empty/ON TRUE constraint.
            let effective_constraint = if is_rhs_from_item {
                &stmt_jc.constraint
            } else {
                &JoinConstraint::Empty
            };
            let allow_flatten = stmt_jc.operator.is_inner_join()
                && match effective_constraint {
                    JoinConstraint::Empty => true,
                    JoinConstraint::On(expr) => is_always_true_filter(expr, Dialect::MySQL),
                    _ => false,
                };

            let (transformed, resolved_option) = try_resolve_as_lateral_subquery(
                &mut from_item,
                &preceding_from_items,
                &preceding_to_rhs,
                ctx,
                allow_flatten,
                &mut allow_aggregated_flatten,
            )?;

            preceding_from_items.insert(from_item_rel.clone());

            match resolved_option {
                Some(LateralResolution::Flatten(prepared)) => {
                    deferred_inlines.push(absorb_flatten(
                        *prepared,
                        stmt,
                        outermost_idx_cursor,
                        None, // JOIN position: always push to out_joins
                        &mut were_lateral,
                        &mut preceding_from_items,
                        &mut preceding_to_rhs,
                        &ctx.pre_hoist_lateral_exactly_one,
                        &ctx.pre_hoist_lateral_at_most_one,
                        ctx.unique_cols_schema,
                    )?);
                    rewrite_status.rewrite();
                }
                Some(LateralResolution::Resolved(resolved_from_item, lateral_join_on)) => {
                    // The current RHS item was LATERAL, generate a join clause for it.
                    // In case it was referenced in the current join condition,
                    // combine `lateral_join_on` with the current join condition, and use it.
                    let join_operator = join_operator_for_lateral?;
                    let join_constraint = if is_rhs_from_item {
                        if let JoinConstraint::On(join_expr) = add_expression_to_join_constraint(
                            stmt_jc.constraint.clone(),
                            lateral_join_on,
                        ) {
                            normalize_lateral_join_constraint!(
                                stmt,
                                from_item_rel.clone(),
                                join_operator,
                                join_expr
                            )
                        } else {
                            JoinConstraint::Empty
                        }
                    } else {
                        normalize_lateral_join_constraint!(
                            stmt,
                            from_item_rel.clone(),
                            join_operator,
                            lateral_join_on
                        )
                    };
                    were_lateral.push(JoinClause {
                        operator: join_operator,
                        right: JoinRightSide::Table(resolved_from_item),
                        constraint: join_constraint,
                    });
                    coalesce_fields_map.extend(fields_map);
                }
                None => {
                    // Add the regular one to the preceding items, as the later LATERAL subqueries
                    // could be correlated with them.
                    if is_rhs_from_item {
                        rhs_from_item_is_regular = true;
                    }
                    were_regular.push(from_item);
                }
            }

            preceding_to_rhs.push(from_item_rel);
            outermost_idx_cursor += 1;

            if transformed {
                rewrite_status.rewrite();
            }
        }

        if !were_regular.is_empty() {
            // If the current join condition references the RHS item, which is among the regular tables/subqueries,
            // we use the original join operator and constraint.
            // Otherwise, the remaining regular items are comma separated (cross-joined with each other), and
            // have no condition to reference the LHS party of the current join.
            let (operator, constraint) = if rhs_from_item_is_regular {
                (stmt_jc.operator, stmt_jc.constraint.clone())
            } else {
                (JoinOperator::Join, JoinConstraint::Empty)
            };
            new_joins.push(JoinClause {
                operator,
                // SAFETY: the `len() == 1` guard guarantees `pop()` returns `Some`.
                right: if were_regular.len() == 1 {
                    JoinRightSide::Table(were_regular.pop().expect("len checked == 1"))
                } else {
                    JoinRightSide::Tables(were_regular)
                },
                constraint,
            });
        }

        new_joins.extend(were_lateral);
    }

    stmt.tables = new_tables;
    stmt.join = new_joins;

    if let Some(to_where) = add_to_where {
        stmt.where_clause = and_predicates_skip_true(stmt.where_clause.take(), to_where);
    }

    // Apply deferred inlines for flattened LATERAL wrappers.
    // At this point the absorbed tables/joins are already in place, so
    // column rebinding can see them.
    for (prepared, additions) in deferred_inlines {
        apply_inline(stmt, prepared, true, additions)?;
    }

    // Apply COUNT→COALESCE mappings throughout the statement (SELECT, WHERE, HAVING,
    // ORDER BY, GROUP BY, single-relation JOIN ON conjuncts).  Cross-table equality
    // ON predicates are rejected (cannot be hash-join keys).
    if !coalesce_fields_map.is_empty() {
        coalesce_fields_references(stmt, &coalesce_fields_map)?;
    }

    Ok(rewrite_status)
}

/// Top-level method to convert all LATERAL joins in a SELECT statement into equivalent standard joins.
/// Modifies the statement in-place, flattening correlated subqueries and updating joins as needed.
/// **IMPORTANT**: This rewrite pass must be called after the schema resolution, star expansion
/// and JoinConstraint::USING expansion passes.
#[inline]
pub(crate) fn unnest_lateral_subqueries<U: UniqueColumnsSchema>(
    stmt: &mut SelectStatement,
    ctx: &mut UnnestContext<U>,
) -> ReadySetResult<RewriteStatus> {
    resolve_lateral_subqueries(stmt, ctx)
}

#[cfg(test)]
mod tests {
    use crate::drop_redundant_join::UniqueColumnsSchema;
    use crate::lateral_join::unnest_lateral_subqueries;
    use crate::unnest_subqueries::{NonNullSchema, UnnestContext, collect_lateral_hints};
    use crate::unnest_subqueries_3vl::ProbeRegistry;
    use readyset_sql::ast::{Column, Relation, SqlQuery};
    use readyset_sql::{Dialect, DialectDisplay};
    use readyset_sql_parsing::{parse_query, parse_select};
    use std::collections::{HashMap, HashSet};
    /* How to create and populate the tables used in the test module:
    *
       create table test (auth_id uuid, i int, b int, t text, dt date);
       insert into test (auth_id, i, b, t, dt) values
           ('1c75691c-b02b-4fde-a049-bd6848dbf0d4', 10, 11, 'aa', '2025-02-18'),
           ('b7474fb5-007a-4d11-9a09-c062253189f1', 11, 10, 'aa', '2025-02-18'),
           ('8757f95f-4cb7-437b-9688-d11e5c943b4d', 11, 11, 'ab', '2025-02-17'),
           ('82abb461-3448-4a10-9ffa-eee8f4038582', 12, 11, 'bb', '2025-02-19');
       create table test1 as select * from test;
       create table test2 as select * from test;
       create table test3 as select * from test;
    *
    */

    struct NonNullSchemaMoke {}

    impl NonNullSchema for NonNullSchemaMoke {
        fn not_null_columns_of(&self, _: &Relation) -> HashSet<Column> {
            HashSet::new()
        }
    }

    /// Test-corpus convention: every relation has a single-column unique
    /// key on each of these names.  The names listed here are the
    /// columns the LATERAL test corpus uses as join/correlation keys
    /// where the underlying real table would actually declare a PK or
    /// UNIQUE NOT NULL constraint on that column — `k`, `sn`, `jn`, `pn`
    /// are PK columns in the `qa.*` schema; `id`, `auth_id`, `rownum`
    /// match analogous identifiers in the legacy `test`/`DataTypes`
    /// fixtures.  Content columns like `b`, `t` are intentionally NOT
    /// listed: tests that correlate via content columns over regular-
    /// table upstreams must use `test_it_with_unique_schema` with an
    /// `ExplicitUniqueSchema` declaring exactly what's intended, so
    /// schema-aware checks aren't silently masked by a lying stub.
    struct PermissiveTestUniqueSchema;

    impl UniqueColumnsSchema for PermissiveTestUniqueSchema {
        fn unique_columns_of(&self, rel: &Relation) -> Option<HashSet<Column>> {
            let pk_names = ["k", "sn", "jn", "pn", "id", "auth_id", "rownum"];
            Some(
                pk_names
                    .iter()
                    .map(|n| Column {
                        name: (*n).into(),
                        table: Some(rel.clone()),
                    })
                    .collect(),
            )
        }
    }

    fn test_it(test_name: &str, original_text: &str, expect_text: &str) {
        test_it_with_unique_schema(
            test_name,
            original_text,
            expect_text,
            &PermissiveTestUniqueSchema,
        );
    }

    fn test_it_with_unique_schema<U: UniqueColumnsSchema>(
        test_name: &str,
        original_text: &str,
        expect_text: &str,
        unique_cols_schema: &U,
    ) {
        let mut stmt = match parse_query(Dialect::PostgreSQL, original_text) {
            Ok(SqlQuery::Select(stmt)) => stmt,
            Err(e) => panic!("> {test_name}: ORIGINAL STATEMENT PARSE ERROR: {e}"),
            _ => unreachable!(),
        };

        let mut ctx = UnnestContext {
            schema: &NonNullSchemaMoke {},
            unique_cols_schema,
            probes: ProbeRegistry::new(),
            pre_hoist_lateral_exactly_one: HashSet::new(),
            pre_hoist_lateral_at_most_one: HashSet::new(),
            lateral_trivial_on: HashSet::new(),
            ancestor_scope: HashSet::new(),
            ancestor_scope_ordered: Vec::new(),
        };
        // Mirror `unnest_subqueries_main`'s pre-pass so LATERAL pre-hoist
        // hints (consumed by `lateral_flatten_safe` and the LATERAL downstream-cardinality
        // variant) are populated before rewriting.
        collect_lateral_hints(&stmt, &mut ctx)
            .unwrap_or_else(|e| panic!("> {test_name}: PRE-HOIST HINT COLLECTION ERROR: {e}"));

        match unnest_lateral_subqueries(&mut stmt, &mut ctx) {
            Ok(_) => {
                println!(">>> Resolved: {}", stmt.display(Dialect::PostgreSQL));
                assert_eq!(
                    stmt,
                    match parse_select(Dialect::PostgreSQL, expect_text) {
                        Ok(stmt) => stmt,
                        Err(e) => panic!("> {test_name}: REWRITTEN STATEMENT PARSE ERROR: {e}"),
                    }
                );
            }
            Err(e) => {
                println!("> {test_name}: REWRITE ERROR: {e}");
                assert!(expect_text.is_empty(), "> {test_name}: REWRITE ERROR: {e}")
            }
        }
    }

    #[test]
    fn test1() {
        let original_stmt = "select T1.i, T2.b from test1 T1, LATERAL(select T3.i, T3.b from test2 T3 WHERE T3.b = T1.b) T2 WHERE T1.t = 'aa'";
        let expect_stmt = r#"SELECT "t1"."i", "t2"."b" FROM "test1" AS "t1" INNER JOIN (SELECT "t3"."i", "t3"."b" FROM "test2" AS "t3") AS "t2" ON ("t2"."b" = "t1"."b") WHERE ("t1"."t" = 'aa')"#;
        test_it("test1", original_stmt, expect_stmt);
    }

    #[test]
    fn test2() {
        let original_stmt = "SELECT T1.auth_id FROM test1 T1, LATERAL(SELECT T11.auth_id, T11.i FROM test2 T11 \
                            WHERE T1.b = T11.b AND T1.t = T11.t AND T11.dt = '2025-02-18'\
                         ) TT \
                        WHERE T1.t = 'aa'";
        let expect_stmt = r#"SELECT "t1"."auth_id" FROM "test1" AS "t1" INNER JOIN
            (SELECT "t11"."auth_id", "t11"."i", "t11"."t" AS "t", "t11"."b" AS "b" FROM "test2" AS "t11"
            WHERE ("t11"."dt" = '2025-02-18')) AS "tt" ON (("t1"."b" = "tt"."b") AND ("t1"."t" = "tt"."t"))
            WHERE ("t1"."t" = 'aa')"#;
        test_it("test2", original_stmt, expect_stmt);
    }

    #[test]
    fn test3() {
        let original_stmt = "SELECT T1.auth_id FROM test1 T1, LATERAL (SELECT T11.auth_id, T11.i FROM
                              test2 T11
                                join
                              (select T22.i a from test3 T22 where T22.b = T1.b and T22.auth_id = T1.auth_id) T12
                                on T11.i = T12.a) T13
                              WHERE T1.t = 'aa'";
        let expect_stmt = r#"SELECT "t1"."auth_id" FROM "test1" AS "t1" INNER JOIN
        (SELECT "t11"."auth_id", "t11"."i", "t12"."auth_id" AS "auth_id0", "t12"."b" AS "b" FROM "test2" AS "t11" JOIN
        (SELECT "t22"."i" AS "a", "t22"."auth_id" AS "auth_id", "t22"."b" AS "b" FROM "test3" AS "t22") AS "t12" ON
        ("t11"."i" = "t12"."a")) AS "t13" ON (("t13"."b" = "t1"."b") AND ("t13"."auth_id0" = "t1"."auth_id"))
        WHERE ("t1"."t" = 'aa')"#;
        test_it("test3", original_stmt, expect_stmt);
    }

    #[test]
    fn test4() {
        let original_stmt = "SELECT T1.auth_id FROM test1 T1, LATERAL(SELECT T11.auth_id, T11.i FROM \
                       test2 T11 \
                         join \
                       (select T22.i a, T22.b d from test3 T22 where T22.b = T1.b and T22.t = T1.t and T22.dt = '2025-02-18') T12 \
                         on T11.i = T12.a and (T11.b = 11 or T12.d = 11)) T13 \
                       WHERE T1.t = 'aa'";
        let expect_stmt = r#"SELECT "t1"."auth_id" FROM "test1" AS "t1" INNER JOIN (SELECT "t11"."auth_id", "t11"."i", "t12"."t" AS "t", "t12"."d" AS "d" FROM "test2" AS "t11" JOIN (SELECT "t22"."i" AS "a", "t22"."b" AS "d", "t22"."t" AS "t" FROM "test3" AS "t22" WHERE ("t22"."dt" = '2025-02-18')) AS "t12" ON (("t11"."i" = "t12"."a") AND (("t11"."b" = 11) OR ("t12"."d" = 11)))) AS "t13" ON (("t13"."d" = "t1"."b") AND ("t13"."t" = "t1"."t")) WHERE ("t1"."t" = 'aa')"#;
        test_it("test4", original_stmt, expect_stmt);
    }

    #[test]
    fn test5() {
        let original_stmt = "SELECT * FROM
                        test1, test, (SELECT test3.i, test3.b FROM test3 where test3.t = 'aa') t33,
                            LATERAL(SELECT t3.t, t2.b, t2.i, t3.i FROM
                                    (select test2.b, test2.t, test2.i FROM test2
                                       WHERE test.i = test2.i
                                             and test1.i = test2.i
                                             and test2.i = t33.i
                                             and test2.b = 11 and
                                             test.b = 11
                                    ) t2
                                    JOIN
                                    (SELECT test3.t, test3.i FROM test3
                                       WHERE test1.i = test3.i and test3.t = 'aa'
                                    ) AS t3
                                    ON t2.t = t3.t
                                  ) AS tl";
        let expect_stmt = r#"SELECT * FROM "test1", "test", (SELECT "test3"."i", "test3"."b" FROM "test3"
        WHERE ("test3"."t" = 'aa')) AS "t33" INNER JOIN (SELECT "t3"."t", "t2"."b", "t2"."i", "t3"."i" AS "i0"
        FROM (SELECT "test2"."b", "test2"."t", "test2"."i" FROM "test2" WHERE ("test2"."b" = 11)) AS "t2"
        JOIN (SELECT "test3"."t", "test3"."i" FROM "test3" WHERE ("test3"."t" = 'aa')) AS "t3"
        ON ("t2"."t" = "t3"."t")) AS "tl" ON ("tl"."i" = "t33"."i")
        WHERE (((("test"."i" = "tl"."i") AND ("test1"."i" = "tl"."i")) AND ("test"."b" = 11)) AND ("test1"."i" = "tl"."i0"))"#;
        test_it("test5", original_stmt, expect_stmt);
    }

    // Negative test: LATERAL normalization would move ON filters out of a LEFT OUTER join.
    // Allowing the rewrite will cause: `join on more than 2 table` error
    #[test]
    fn test6() {
        let original_stmt = "SELECT * FROM
                         (test1 join test on test1.t = test.t) join
                         (SELECT test3.i, test3.b FROM test3 where test3.t = 'aa') t33 on test1.b = t33.b left join
                          LATERAL(SELECT t3.t, t2.b, t2.i as t2_i, t3.i as t3_i FROM
                              (select test2.b, test2.t, test2.i FROM test2
                                   WHERE test.i = test2.i
                                         and test1.i = test2.i
                                         and test2.b = 11
                                         and test.b = 11
                              ) t2
                              JOIN
                              (SELECT test3.t, test3.i FROM test3 WHERE test1.i = test3.i and test3.t = 'aa') AS t3
                              ON t2.t = t3.t
                         ) AS tl on t33.i = tl.t3_i";
        let expect_stmt = r#""#;
        test_it("test6", original_stmt, expect_stmt);
    }

    #[test]
    fn test7() {
        let original_stmt = "SELECT * FROM
                (test1 join test on test1.t = test.t) join
                 LATERAL (SELECT t3.t, t2.b, t2.i as t2_i, t3.i as t3_i FROM
                                 (select test2.b, test2.t, test2.i
                                     FROM test2
                                     WHERE test.i = test2.i
                                           and test1.i = test2.i
                                           and test2.b = 11
                                           and test.b = 11
                                 ) t2
                                 JOIN
                                 (SELECT test3.t, min(test3.i) i
                                     FROM test3
                                     WHERE test1.i = test3.i
                                           and test3.b = 11
                                     GROUP BY test3.t
                                 ) AS t3
                                 ON t2.t = t3.t
                             ) AS tl
                             ON test1.b = tl.b
                left join
                (SELECT test3.i, test3.b FROM test3 where test3.t = 'aa') t33 on tl.b = t33.b";
        let expect_stmt = r#"SELECT * FROM "test1" JOIN "test" ON ("test1"."t" = "test"."t") INNER JOIN
        (SELECT "t3"."t", "t2"."b", "t2"."i" AS "t2_i", "t3"."i" AS "t3_i", "t3"."i0" AS "i0" FROM
        (SELECT "test2"."b", "test2"."t", "test2"."i" FROM "test2" WHERE ("test2"."b" = 11)) AS "t2" JOIN
        (SELECT "test3"."t", min("test3"."i") AS "i", "test3"."i" AS "i0" FROM "test3"
        WHERE ("test3"."b" = 11) GROUP BY "test3"."t", "test3"."i") AS "t3" ON ("t2"."t" = "t3"."t")) AS "tl"
        ON (("test"."i" = "tl"."t2_i") AND ("test"."b" = 11)) LEFT JOIN (SELECT "test3"."i", "test3"."b" FROM "test3"
        WHERE ("test3"."t" = 'aa')) AS "t33" ON ("tl"."b" = "t33"."b")
        WHERE (("test1"."b" = "tl"."b") AND (("test1"."i" = "tl"."t2_i") AND ("test1"."i" = "tl"."i0")))"#;
        test_it("test7", original_stmt, expect_stmt);
    }

    #[test]
    fn test8() {
        let original_stmt = "select T1.i from test T1 left join \
             LATERAL(select max(T3.i) i from test3 T3 WHERE T3.b = T1.b group by T3.t) T2 on T1.i = T2.i \
             WHERE T1.t = 'aa'";
        let expect_stmt = r#"SELECT "t1"."i" FROM "test" AS "t1" LEFT JOIN
            (SELECT max("t3"."i") AS "i", "t3"."b" AS "b" FROM "test3" AS "t3" GROUP BY "t3"."t", "t3"."b") AS "t2"
            ON (("t1"."i" = "t2"."i") AND ("t2"."b" = "t1"."b")) WHERE ("t1"."t" = 'aa')"#;
        test_it("test8", original_stmt, expect_stmt);
    }

    #[test]
    fn test9() {
        let original_stmt = "select T1.i from test T1 left join \
                LATERAL(select max(T3.i) i, T3.b bb from test3 T3 WHERE T3.b = T1.b group by bb) T2 on T1.i = T2.i WHERE T1.t = 'aa'";
        let expect_stmt = r#"SELECT "t1"."i" FROM "test" AS "t1" LEFT JOIN
            (SELECT max("t3"."i") AS "i", "t3"."b" AS "bb" FROM "test3" AS "t3" GROUP BY "t3"."b") AS "t2"
            ON (("t1"."i" = "t2"."i") AND ("t2"."bb" = "t1"."b")) WHERE ("t1"."t" = 'aa')"#;
        test_it("test9", original_stmt, expect_stmt);
    }

    // Negative test: Aggregate-only LATERAL subquery (no GROUP BY) joining with ON constraint
    #[test]
    fn test10() {
        let original_stmt = "select test1.b, T1.i from test1 join \
                LATERAL(select max(T3.i) i from test3 T3 WHERE test1.t = T3.t) T2 \
                on test1.i = T2.i left join test T1 on T1.i = T2.i \
             WHERE T1.t = 'aa'";
        let expect_stmt = r#"SELECT "test1"."b", "t1"."i" FROM "test1" JOIN
        (SELECT max("t3"."i") AS "i", "t3"."t" AS "t" FROM "test3" AS "t3" GROUP BY "t3"."t") AS "t2"
        ON (("test1"."i" = "t2"."i") AND ("test1"."t" = "t2"."t")) LEFT JOIN "test" AS "t1" ON ("t1"."i" = "t2"."i")
        WHERE ("t1"."t" = 'aa');"#;
        test_it("test10", original_stmt, expect_stmt);
    }

    #[test]
    fn test11() {
        let original_stmt = "SELECT * FROM (test1 join test on test1.t = test.t)
                 join (SELECT test3.i, test3.b FROM test3 where test3.t = 'aa') t33 on test1.b = t33.b
                 join LATERAL(
                    SELECT t3.t, t2.b, t2.i as t2_i, t3.i as t3_i
                        FROM (select test2.b, test2.t, test2.i
                               FROM test2
                               WHERE test.i = test2.i and test1.i = test2.i and test2.b = 11 and test.b = 11
                                     and exists(select 1 from test3 where test3.i = test2.i)
                             ) t2
                        JOIN (SELECT test3.t, test3.i
                                FROM test3
                                WHERE test1.i = test3.i and test3.t = 'aa'
                                      and test3.b = (select max(test2.b) from test2 where test3.t = test2.t)
                             ) AS t3
                        ON t2.t = t3.t
                    ) AS tl
                 on t33.i = tl.t3_i";
        let expected_stmt = r#"SELECT * FROM "test1" JOIN "test" ON ("test1"."t" = "test"."t") JOIN
        (SELECT "test3"."i", "test3"."b" FROM "test3" WHERE ("test3"."t" = 'aa')) AS "t33" ON ("test1"."b" = "t33"."b") INNER JOIN
        (SELECT "t3"."t", "t2"."b", "t2"."i" AS "t2_i", "t3"."i" AS "t3_i" FROM
        (SELECT "test2"."b", "test2"."t", "test2"."i" FROM "test2" INNER JOIN
        (SELECT DISTINCT 1 AS "present_", "test3"."i" AS "i" FROM "test3") AS "GNL" ON ("GNL"."i" = "test2"."i")
        WHERE ("test2"."b" = 11)) AS "t2" JOIN (SELECT "test3"."t", "test3"."i" FROM "test3" INNER JOIN
        (SELECT max("test2"."b") AS "max(b)", "test2"."t" AS "t" FROM "test2" GROUP BY "test2"."t") AS "GNL"
         ON (("test3"."t" = "GNL"."t") AND ("test3"."b" = "GNL"."max(b)")) WHERE ("test3"."t" = 'aa')) AS "t3"
         ON ("t2"."t" = "t3"."t")) AS "tl" ON ("t33"."i" = "tl"."t3_i")
         WHERE (((("test"."i" = "tl"."t2_i") AND ("test1"."i" = "tl"."t2_i")) AND ("test"."b" = 11)) AND ("test1"."i" = "tl"."t3_i"))"#;
        test_it("test11", original_stmt, expected_stmt);
    }

    #[test]
    fn test12() {
        let original_stmt = "SELECT * FROM (test1 join test on test1.t = test.t) join
            (SELECT test3.i, test3.b FROM test3 where test3.t = 'aa') t33 on test1.b = t33.b
            left join LATERAL(SELECT t3.t, t2.b, t2.i as t2_i, t3.i as t3_i FROM
            (select test2.b, test2.t, test2.i FROM test2 WHERE test2.b = 11 and
            exists(select 1 from test3 where test3.i = test2.i)) t2 JOIN
            LATERAL(SELECT test3.t, test3.i FROM test3 WHERE t2.i = test3.i and test3.t = 'aa'
            and test3.b = (select max(test2.b) from test2 where test3.t = test2.t)) AS t3
            ON t2.t = t3.t) AS tl on t33.i = tl.t3_i;";
        let expect_stmt = r#"SELECT * FROM "test1" JOIN "test" ON ("test1"."t" = "test"."t") JOIN
        (SELECT "test3"."i", "test3"."b" FROM "test3" WHERE ("test3"."t" = 'aa')) AS "t33" ON ("test1"."b" = "t33"."b")
        LEFT JOIN (SELECT "t3"."t", "t2"."b", "t2"."i" AS "t2_i", "t3"."i" AS "t3_i" FROM
        (SELECT "test2"."b", "test2"."t", "test2"."i" FROM "test2" INNER JOIN
        (SELECT DISTINCT 1 AS "present_", "test3"."i" AS "i" FROM "test3") AS "GNL" ON ("GNL"."i" = "test2"."i")
        WHERE ("test2"."b" = 11)) AS "t2" INNER JOIN (SELECT "test3"."t", "test3"."i" FROM "test3" INNER JOIN
        (SELECT max("test2"."b") AS "max(b)", "test2"."t" AS "t" FROM "test2" GROUP BY "test2"."t") AS "GNL"
        ON (("test3"."t" = "GNL"."t") AND ("test3"."b" = "GNL"."max(b)")) WHERE ("test3"."t" = 'aa')) AS "t3"
        ON (("t2"."t" = "t3"."t") AND ("t2"."i" = "t3"."i"))) AS "tl" ON ("t33"."i" = "tl"."t3_i")"#;
        test_it("test12", original_stmt, expect_stmt);
    }

    // Negative test: LATERAL used without any preceding tables
    #[test]
    fn test13() {
        let original_stmt = "select T1.i from \
                LATERAL(select max(T3.i) i from test3 T3 WHERE T3.b = T1.b group by T3.t) T2 \
                left join test T1 on T1.i = T2.i WHERE T1.t = 'aa'";
        let expect_stmt = r#""#;
        test_it("test13", original_stmt, expect_stmt);
    }

    // Negative test: LATERAL sub-query using correlation on a table, which is not preceding the LATERAL
    #[test]
    fn test14() {
        let original_stmt = "select test1.b, T1.i from test1 join \
               LATERAL(select max(T3.i) i from test3 T3 WHERE T1.i = T3.i and test1.t = T3.t) T2 on test1.i = T2.i \
                left join test T1 on T1.i = T2.i \
             WHERE T1.t = 'aa'";
        let expect_stmt = r#""#;
        test_it("test14", original_stmt, expect_stmt);
    }

    // Negative test: LATERAL sub-query using correlation on a table, which is not preceding the LATERAL
    #[test]
    fn test15() {
        let original_stmt = "SELECT * FROM (test1 join test on test1.t = test.t) join \
            (SELECT test3.i, test3.b FROM test3 where test3.t = 'aa') t33 on test1.b = t33.b \
            left join LATERAL(SELECT t3.t, t2.b, t2.i as t2_i, t3.i as t3_i FROM \
            (select test2.b, test2.t, test2.i FROM test2 WHERE test2.b = 11 and \
            exists(select 1 from test3 where test3.i = test2.i)) t2 JOIN \
            (SELECT test3.t, test3.i FROM test3 WHERE t2.i = test3.i and test3.t = 'aa' \
            and test3.b = (select max(test2.b) from test2 where test3.t = test2.t)) AS t3 \
            ON t2.t = t3.t) AS tl on t33.i = tl.t3_i;";
        let expect_stmt = r#""#;
        test_it("test15", original_stmt, expect_stmt);
    }

    #[test]
    fn test16() {
        let original_stmt = r#"
SELECT
    DT.C1,
    qa.DataTypes.Test_INTEGER,
    DT.Test_INTEGER2
FROM
    qa.DataTypes
    LEFT JOIN LATERAL (
        SELECT
            qa.DataTypes3.Test_INTEGER2,
            COUNT(*) C1
        FROM
            qa.DataTypes3
        WHERE
            qa.DataTypes.Test_BOOLEAN = TRUE
        GROUP BY
            qa.DataTypes3.Test_INTEGER2
    ) DT ON qa.DataTypes.Test_INTEGER = DT.Test_INTEGER2
ORDER BY
    1,
    2;"#;
        let expect_stmt = r#"SELECT "dt"."c1", "qa"."datatypes"."test_integer", "dt"."test_integer2"
        FROM "qa"."datatypes" LEFT JOIN (SELECT "qa"."datatypes3"."test_integer2", count(*) AS "c1"
        FROM "qa"."datatypes3" GROUP BY "qa"."datatypes3"."test_integer2") AS "dt"
        ON (("qa"."datatypes"."test_integer" = "dt"."test_integer2") AND ("qa"."datatypes"."test_boolean" = TRUE))
        ORDER BY 1, 2"#;
        test_it("test16", original_stmt, expect_stmt);
    }

    #[test]
    fn test17() {
        let original_stmt = r#"
SELECT
    DT.C1,
    DataTypes.Test_INTEGER,
    DT.Test_INTEGER2
FROM
    spj,
    DataTypes
    JOIN LATERAL (
        SELECT
            DataTypes3.Test_INTEGER2,
            COUNT(*) C1
        FROM
            DataTypes3
        WHERE
            DataTypes3.test_smallint2 = spj.qty and DataTypes.Test_BOOLEAN = TRUE
        GROUP BY
            DataTypes3.Test_INTEGER2
    ) DT ON DataTypes.Test_INTEGER = DT.Test_INTEGER2
ORDER BY
    1,
    2;"#;
        let expect_stmt = r#"SELECT "dt"."c1", "datatypes"."test_integer", "dt"."test_integer2" FROM "spj", "datatypes"
        INNER JOIN (SELECT "datatypes3"."test_integer2", count(*) AS "c1", "datatypes3"."test_smallint2" AS "test_smallint2"
        FROM "datatypes3" GROUP BY "datatypes3"."test_integer2", "datatypes3"."test_smallint2") AS "dt"
        ON (("datatypes"."test_integer" = "dt"."test_integer2") AND ("datatypes"."test_boolean" = TRUE))
        WHERE ("dt"."test_smallint2" = "spj"."qty")
        ORDER BY 1 NULLS LAST, 2 NULLS LAST"#;
        test_it("test17", original_stmt, expect_stmt);
    }

    #[test]
    fn test18() {
        let original_stmt = r#"
SELECT
    dt.Test_INTEGER,
    T1.Test_INTEGER2
FROM
    qa.DataTypes AS dt
    LEFT OUTER JOIN LATERAL (
        SELECT
            dt3.RowNum, dt3.Test_INTEGER2
        FROM
            qa.DataTypes3 AS dt3
        WHERE
            dt.Test_INTEGER = dt3.Test_INTEGER2
    ) AS t1 ON TRUE
    LEFT OUTER JOIN LATERAL (
        SELECT
            s.sn, s.sname, s.status, s.city, s.pn, s.jn
        FROM
            qa.s AS s
        WHERE
            s.status = t1.RowNum
    ) AS t2 ON TRUE
ORDER BY
    1,
    2;"#;
        let expect_stmt = r#"SELECT "dt"."test_integer", "t1"."test_integer2" FROM "qa"."datatypes" AS "dt"
        LEFT OUTER JOIN (SELECT "dt3"."rownum", "dt3"."test_integer2" FROM "qa"."datatypes3" AS "dt3") AS "t1"
        ON ("dt"."test_integer" = "t1"."test_integer2") LEFT OUTER JOIN
        (SELECT "s"."sn", "s"."sname", "s"."status", "s"."city", "s"."pn", "s"."jn"
        FROM "qa"."s" AS "s") AS "t2" ON ("t2"."status" = "t1"."rownum") ORDER BY 1, 2"#;
        test_it("test18", original_stmt, expect_stmt);
    }

    #[test]
    fn test19() {
        let original_stmt = r#"
SELECT
    u.sn,
    pstats.post_count
FROM
    qa.s AS u,
    LATERAL (
        SELECT
            COUNT(*) AS post_count,
            p.jn
        FROM
            qa.p AS p
        WHERE
            p.pn = u.pn
        GROUP BY p.jn
    ) AS pstats
    join qa.spj on 0 = pstats.post_count;"#;
        let expect_stmt = r#"SELECT "u"."sn", "pstats"."post_count" FROM "qa"."s" AS "u" INNER JOIN
        (SELECT count(*) AS "post_count", "p"."jn", "p"."pn" AS "pn" FROM "qa"."p" AS "p" GROUP BY "p"."jn", "p"."pn")
        AS "pstats" ON ("pstats"."pn" = "u"."pn") JOIN "qa"."spj" ON (0 = "pstats"."post_count")"#;
        test_it("test19", original_stmt, expect_stmt);
    }

    #[test]
    fn test20() {
        let original_stmt = r#"
        SELECT a.k, l.cnt
        FROM qa.a AS a
        CROSS JOIN LATERAL (
            SELECT COUNT(*) AS cnt
            FROM qa.b AS b
            WHERE b.k = a.k
        ) AS l;
        "#;
        let expect_stmt = r#"SELECT "a"."k", coalesce("l"."cnt", 0) FROM "qa"."a" AS "a" LEFT OUTER JOIN
        (SELECT count(*) AS "cnt", "b"."k" AS "k" FROM "qa"."b" AS "b" GROUP BY "b"."k") AS "l" ON ("l"."k" = "a"."k")"#;
        test_it("test20", original_stmt, expect_stmt);
    }

    #[test]
    fn test21() {
        let original_stmt = r#"
        SELECT DataTypes.Test_INT, spj.MR
        FROM DataTypes
        JOIN LATERAL (
            SELECT count(*) MR FROM DataTypes1 WHERE DataTypes1.Test_INT = DataTypes.Test_INT HAVING MAX(DataTypes1.RowNum) > 10
        ) spj ON spj.MR = DataTypes.RowNum
        ORDER BY DataTypes.RowNum;
        "#;
        let expect_stmt = r#"SELECT "datatypes"."test_int", "spj"."mr" FROM "datatypes" INNER JOIN
        (SELECT count(*) AS "mr", "datatypes1"."test_int" AS "test_int" FROM "datatypes1" GROUP BY "datatypes1"."test_int"
        HAVING (max("datatypes1"."rownum") > 10)) AS "spj"
        ON (("spj"."mr" = "datatypes"."rownum") AND ("spj"."test_int" = "datatypes"."test_int"))
        ORDER BY "datatypes"."rownum" NULLS LAST"#;
        test_it("test21", original_stmt, expect_stmt);
    }

    // Negative test: LATERAL join condition references a column that should be mapped to COALESCE
    #[test]
    fn test22() {
        let original_stmt = r#"
        SELECT DataTypes.Test_INT, spj.MR
        FROM DataTypes
        JOIN LATERAL (
            SELECT count(*) MR FROM DataTypes1 WHERE DataTypes1.Test_INT = DataTypes.Test_INT
        ) spj ON spj.MR = DataTypes.RowNum
        ORDER BY DataTypes.RowNum;"#;
        let expect_stmt = r#""#;
        test_it("test22", original_stmt, expect_stmt);
    }

    // LATERAL (correlated COUNT) + LEFT join with a non-trivial ON (extra RHS-only filter):
    // Expect: NO COALESCE injection anywhere; ON keeps both the key equality and l.cnt > 0.
    #[test]
    fn test23() {
        let original_text = r#"
        SELECT s.sn
        FROM s AS s
        LEFT JOIN LATERAL (
          SELECT i.cnt, i.sn
          FROM (
            SELECT COUNT(j.city) AS cnt, j.sn
            FROM j AS j
            WHERE j.sn = s.sn
            GROUP BY j.sn
          ) AS i
        ) AS l ON l.sn = s.sn AND l.cnt > 0;
        "#;
        let expected_text = r#"SELECT "s"."sn" FROM "s" AS "s" LEFT JOIN (SELECT "i"."cnt", "i"."sn" FROM
        (SELECT count("j"."city") AS "cnt", "j"."sn" FROM "j" AS "j" GROUP BY "j"."sn") AS "i") AS "l"
        ON (("l"."sn" = "s"."sn") AND ("l"."cnt" > 0))"#;
        test_it("test23", original_text, expected_text);
    }

    // LATERAL (correlated COUNT) + INNER join with a non-trivial ON (BETWEEN on RHS field):
    // Expect: NO COALESCE injection; ON holds key equality and range filter over l.cnt.
    #[test]
    fn test24() {
        let original_text = r#"
        SELECT s.sn
        FROM s AS s
        JOIN LATERAL (
          SELECT i.cnt, i.sn
          FROM (
            SELECT COUNT(j.city) AS cnt, j.sn
            FROM j AS j
            WHERE j.sn = s.sn
            GROUP BY j.sn
          ) AS i
        ) AS l ON l.sn = s.sn AND l.cnt BETWEEN 1 AND 5;
        "#;
        let expected_text = r#"SELECT "s"."sn" FROM "s" AS "s" INNER JOIN (SELECT "i"."cnt", "i"."sn" FROM
        (SELECT count("j"."city") AS "cnt", "j"."sn" FROM "j" AS "j" GROUP BY "j"."sn") AS "i") AS "l"
        ON (("l"."sn" = "s"."sn") AND ("l"."cnt" BETWEEN 1 AND 5))"#;
        test_it("test24", original_text, expected_text);
    }

    // LATERAL (correlated COUNT) with a non-linear wrapper in the projection (cnt + 1 = cn),
    // LEFT join and an extra RHS-only filter in ON over that wrapped field:
    // Expect: NO COALESCE injection; ON keeps l.cn < 100 alongside the key equality.
    #[test]
    fn test25() {
        let original_text = r#"
        SELECT s.sn
        FROM s AS s
        LEFT JOIN LATERAL (
          SELECT i.cnt + 1 AS cn, i.sn
          FROM (
            SELECT COUNT(j.city) AS cnt, j.sn
            FROM j AS j
            WHERE j.sn = s.sn
            GROUP BY j.sn
          ) AS i
        ) AS l ON l.sn = s.sn AND l.cn < 100;
        "#;
        let expected_text = r#"SELECT "s"."sn" FROM "s" AS "s" LEFT JOIN (SELECT ("i"."cnt" + 1) AS "cn", "i"."sn"
        FROM (SELECT count("j"."city") AS "cnt", "j"."sn" FROM "j" AS "j" GROUP BY "j"."sn") AS "i") AS "l"
        ON (("l"."sn" = "s"."sn") AND ("l"."cn" < 100))"#;
        test_it("test25", original_text, expected_text);
    }

    // Wrapper over COUNT in a LATERAL, ON TRUE: must inject COALESCE in projection.
    #[test]
    fn test26() {
        let original_text = r#"
        SELECT
            u.sn,
            l.cn
        FROM
            qa.s AS u
            JOIN LATERAL (
                SELECT
                    "inner".cn + 15 AS cn
                FROM (
                    SELECT
                        COUNT(j.city) + 100 AS cn
                    FROM
                        qa.j AS j
                    WHERE
                        j.sn = u.sn
                ) AS "inner"
            ) AS l ON TRUE;
        "#;
        let expected_text = r#"SELECT "u"."sn", coalesce("l"."cn", 115) FROM "qa"."s" AS "u" LEFT OUTER JOIN
        (SELECT ("inner"."cn" + 15) AS "cn", "inner"."sn" AS "sn" FROM
        (SELECT (count("j"."city") + 100) AS "cn", "j"."sn" AS "sn" FROM "qa"."j" AS "j" GROUP BY "j"."sn") AS "inner") AS "l"
        ON ("l"."sn" = "u"."sn")"#;
        test_it("test26", original_text, expected_text);
    }

    // Two projection wrappers around a correlated COUNT core, with alias hops.
    // ON TRUE => must still inject COALESCE with the fully-folded empty-input constant.
    #[test]
    fn test27() {
        let original_text = r#"
        SELECT u.sn, l.cn
        FROM qa.s AS u
        JOIN LATERAL (
            SELECT mid.cn + 15 AS cn
            FROM (
                SELECT inner0.cn AS cn
                FROM (
                    SELECT COUNT(j.city) + 100 AS cn
                    FROM qa.j AS j
                    WHERE j.sn = u.sn
                ) AS inner0
            ) AS mid
        ) AS l ON TRUE;
        "#;
        // Empty-input for the COUNT core: 0 + 100 = 100; outer + 15 => 115
        let expected_text = r#"SELECT "u"."sn", coalesce("l"."cn", 115) FROM "qa"."s" AS "u" LEFT OUTER JOIN
        (SELECT ("mid"."cn" + 15) AS "cn", "mid"."sn" AS "sn" FROM
            (SELECT "inner0"."cn" AS "cn", "inner0"."sn" AS "sn" FROM
                (SELECT (count("j"."city") + 100) AS "cn", "j"."sn" AS "sn"
                 FROM "qa"."j" AS "j" GROUP BY "j"."sn") AS "inner0"
            ) AS "mid"
        ) AS "l"
        ON ("l"."sn" = "u"."sn")"#;
        test_it("test27", original_text, expected_text);
    }

    // Wrapped COUNT through a non-linear projection that should still fold on empty-input.
    // This is a “killer” case because the wrapper expression is not a plain Column.
    // ON TRUE => expect COALESCE with literal fallback.
    #[test]
    fn test28() {
        let original_text = r#"
        SELECT u.sn, l.cn
        FROM qa.s AS u
        JOIN LATERAL (
            SELECT CASE WHEN inner0.cnt > 0 THEN inner0.cnt ELSE 0 END AS cn
            FROM (
                SELECT COUNT(*) AS cnt
                FROM qa.j AS j
                WHERE j.sn = u.sn
            ) AS inner0
        ) AS l ON TRUE;
        "#;
        // Empty-input constant: COUNT(*) -> 0, CASE -> 0
        let expected_text = r#"SELECT "u"."sn", coalesce("l"."cn", 0) FROM "qa"."s" AS "u" LEFT OUTER JOIN
        (SELECT CASE WHEN ("inner0"."cnt" > 0) THEN "inner0"."cnt" ELSE 0 END AS "cn", "inner0"."sn" AS "sn" FROM
            (SELECT count(*) AS "cnt", "j"."sn" AS "sn" FROM "qa"."j" AS "j" GROUP BY "j"."sn") AS "inner0"
        ) AS "l"
        ON ("l"."sn" = "u"."sn")"#;
        test_it("test28", original_text, expected_text);
    }

    // Wrapper with an extra WHERE can break the “projection-only wrapper chain” classification.
    // In this query, the outer subquery remains aggregate-only/no-GROUP-BY, so it still returns
    // exactly one row per outer key (with NULL cn when the WHERE filters everything out).
    //
    // Expect: we must still use LEFT OUTER JOIN for ON TRUE/CROSS semantics (as implemented),
    // but NO COALESCE mapping is applied because the RHS is not structurally ExactlyOne via
    // projection-only wrappers.
    #[test]
    fn test29() {
        let original_text = r#"
        SELECT u.sn, l.cn
        FROM qa.s AS u
        JOIN LATERAL (
            SELECT max(inner0.cn) + 20 AS cn
            FROM (
                SELECT COUNT(*) + 100 AS cn
                FROM qa.j AS j
                WHERE j.sn = u.sn
            ) AS inner0
            WHERE inner0.cn = 3
        ) AS l ON TRUE;
        "#;
        // The WHERE makes this non-projection-only, so `rhs_is_structurally_exactly_one` is false and
        // we must not inject a COALESCE fallback. However, the aggregate-only/no-GROUP-BY outer layer
        // still produces one row per key (NULL on empty), so preserving ON TRUE/CROSS semantics via a
        // LEFT OUTER JOIN is correct.
        let expected_text = r#"SELECT "u"."sn", "l"."cn" FROM "qa"."s" AS "u" LEFT OUTER JOIN
        (SELECT (max("inner0"."cn") + 20) AS "cn", "inner0"."sn" AS "sn" FROM
            (SELECT (count(*) + 100) AS "cn", "j"."sn" AS "sn" FROM "qa"."j" AS "j" GROUP BY "j"."sn") AS "inner0"
         WHERE ("inner0"."cn" = 3) GROUP BY "inner0"."sn"
        ) AS "l"
        ON ("l"."sn" = "u"."sn")"#;
        test_it("test29", original_text, expected_text);
    }

    // Negative: wrapped COUNT field is referenced by ON condition.
    // Must reject (same class as test22, but through wrapper output).
    #[test]
    fn test30() {
        let original_text = r#"
        SELECT u.sn, l.cn
        FROM qa.s AS u
        JOIN LATERAL (
            SELECT inner0.cnt + 1 AS cn
            FROM (
                SELECT COUNT(*) AS cnt
                FROM qa.j AS j
                WHERE j.sn = u.sn
            ) AS inner0
        ) AS l ON l.cn = u.sn;
        "#;
        // Join condition references a column that would require COALESCE mapping => unsupported.
        let expected_text = r#""#;
        test_it("test30", original_text, expected_text);
    }

    // COALESCE relaxation: COUNT column referenced in WHERE — COALESCE is applied there too.
    // After LEFT JOIN inlining, l.cnt is NULL for no-match rows; COALESCE restores the original 0.
    #[test]
    fn test31() {
        let original_text = r#"
        SELECT a.k, l.cnt
        FROM qa.a AS a
        CROSS JOIN LATERAL (
            SELECT COUNT(*) AS cnt
            FROM qa.b AS b
            WHERE b.k = a.k
        ) AS l
        WHERE l.cnt > 2
        "#;
        let expected_text = r#"SELECT "a"."k", coalesce("l"."cnt", 0)
        FROM "qa"."a" AS "a" LEFT OUTER JOIN
        (SELECT count(*) AS "cnt", "b"."k" AS "k" FROM "qa"."b" AS "b" GROUP BY "b"."k") AS "l"
        ON ("l"."k" = "a"."k")
        WHERE (coalesce("l"."cnt", 0) > 2)"#;
        test_it("test31", original_text, expected_text);
    }

    // COALESCE relaxation: COUNT column in ORDER BY — COALESCE is applied there too.
    #[test]
    fn test32() {
        let original_text = r#"
        SELECT a.k, l.cnt
        FROM qa.a AS a
        CROSS JOIN LATERAL (
            SELECT COUNT(*) AS cnt
            FROM qa.b AS b
            WHERE b.k = a.k
        ) AS l
        ORDER BY l.cnt DESC
        "#;
        let expected_text = r#"SELECT "a"."k", coalesce("l"."cnt", 0)
        FROM "qa"."a" AS "a" LEFT OUTER JOIN
        (SELECT count(*) AS "cnt", "b"."k" AS "k" FROM "qa"."b" AS "b" GROUP BY "b"."k") AS "l"
        ON ("l"."k" = "a"."k")
        ORDER BY coalesce("l"."cnt", 0) DESC NULLS FIRST"#;
        test_it("test32", original_text, expected_text);
    }

    // COALESCE relaxation: COUNT column in a JOIN ON single-relation filter conjunct gets
    // COALESCE; the cross-table equality conjunct on a non-mapped column is left as-is.
    #[test]
    fn test33() {
        let original_text = r#"
        SELECT a.k, l.cnt
        FROM qa.a AS a
        CROSS JOIN LATERAL (
            SELECT COUNT(*) AS cnt
            FROM qa.b AS b
            WHERE b.k = a.k
        ) AS l
        JOIN qa.c AS c ON c.k = a.k AND l.cnt > 0
        "#;
        let expected_text = r#"SELECT "a"."k", coalesce("l"."cnt", 0)
        FROM "qa"."a" AS "a" LEFT OUTER JOIN
        (SELECT count(*) AS "cnt", "b"."k" AS "k" FROM "qa"."b" AS "b" GROUP BY "b"."k") AS "l"
        ON ("l"."k" = "a"."k")
        JOIN "qa"."c" AS "c" ON (("c"."k" = "a"."k") AND (coalesce("l"."cnt", 0) > 0))"#;
        test_it("test33", original_text, expected_text);
    }

    // Negative: another JOIN's ON has a cross-table equality over the COUNT column.
    // Cannot wrap a hash-join key in COALESCE (§2.1) — must remain unsupported.
    #[test]
    fn test34() {
        let original_text = r#"
        SELECT a.k, l.cnt
        FROM qa.a AS a
        CROSS JOIN LATERAL (
            SELECT COUNT(*) AS cnt
            FROM qa.b AS b
            WHERE b.k = a.k
        ) AS l
        JOIN qa.c AS c ON c.threshold = l.cnt
        "#;
        let expected_text = r#""#;
        test_it("test34", original_text, expected_text);
    }

    // Grandparent aggregate: nested LATERAL subquery whose inner aggregate correlates
    // with a grandparent-scope table. The middle wrapper has a LEFT JOIN ON referencing
    // the grandparent, so it should be flattened into the outer query.
    #[test]
    fn nested_lateral_grandparent_agg_flatten() {
        let original_text = r#"
            SELECT "s"."sn", "l"."x"
            FROM "s",
              LATERAL (
                SELECT "l1"."cnt" AS "x"
                FROM "j",
                  LATERAL (
                    SELECT COUNT(*) AS "cnt"
                    FROM "spj"
                    WHERE "spj"."sn" = "s"."sn"
                  ) AS "l1"
              ) AS "l"
        "#;
        let expected_text = r#"SELECT "s"."sn", coalesce("l1"."cnt", 0) AS "x" FROM "s", "j" LEFT OUTER JOIN (SELECT count(*) AS "cnt", "spj"."sn" AS "sn" FROM "spj" GROUP BY "spj"."sn") AS "l1" ON ("l1"."sn" = "s"."sn")"#;
        test_it(
            "nested_lateral_grandparent_agg_flatten",
            original_text,
            expected_text,
        );
    }

    // Negative: mixed parent+grandparent aggregate. The inner LATERAL correlates with
    // BOTH a grandparent ("s") and its immediate parent ("j"). After inner recursion,
    // the parent-scope correlation (j.jn) is extracted into a LEFT JOIN ON. This ON then
    // references j (the parent) which is local. But the grandparent-scope correlation
    // (s.sn) remains in the ON as well — which the outer resolve_lateral_subquery cannot
    // push to WHERE from a LEFT JOIN ON. This correctly triggers unsupported.
    #[test]
    fn nested_lateral_parent_and_grandparent_agg_unsupported() {
        let original_text = r#"
            SELECT "s"."sn", "l"."x"
            FROM "s",
              LATERAL (
                SELECT "l1"."cnt" AS "x"
                FROM "j",
                  LATERAL (
                    SELECT COUNT(*) AS "cnt"
                    FROM "spj"
                    WHERE "spj"."sn" = "s"."sn"
                      AND "spj"."jn" = "j"."jn"
                  ) AS "l1"
              ) AS "l"
        "#;
        // Expect error: the LEFT JOIN ON has outer-scope atoms that cannot be moved to WHERE.
        let expected_text = r#""#;
        test_it(
            "nested_lateral_parent_and_grandparent_agg_unsupported",
            original_text,
            expected_text,
        );
    }

    /// Flatten with WHERE in the wrapper — WHERE merges into outer.
    #[test]
    fn nested_lateral_grandparent_agg_flatten_with_where() {
        let original_text = r#"
            SELECT "s"."sn", "l"."x"
            FROM "s",
              LATERAL (
                SELECT "l1"."cnt" AS "x"
                FROM "j",
                  LATERAL (
                    SELECT COUNT(*) AS "cnt"
                    FROM "spj"
                    WHERE "spj"."sn" = "s"."sn"
                  ) AS "l1"
                WHERE "j"."jn" = 'J1'
              ) AS "l"
        "#;
        let expected_text = r#"SELECT "s"."sn", coalesce("l1"."cnt", 0) AS "x" FROM "s", "j" LEFT OUTER JOIN (SELECT count(*) AS "cnt", "spj"."sn" AS "sn" FROM "spj" GROUP BY "spj"."sn") AS "l1" ON ("l1"."sn" = "s"."sn") WHERE ("j"."jn" = 'J1')"#;
        test_it(
            "nested_lateral_grandparent_agg_flatten_with_where",
            original_text,
            expected_text,
        );
    }

    /// Negative: wrapper with LIMIT — LIMIT inside LATERAL operates per-outer-row.
    /// After flattening it would operate globally, changing semantics.
    /// `can_inline_subquery` rejects because downstream cardinality check
    /// fails (outer has `s` which is not ExactlyOne).
    #[test]
    fn nested_lateral_grandparent_agg_with_limit_bail() {
        let original_text = r#"
            SELECT "s"."sn", "l"."x"
            FROM "s",
              LATERAL (
                SELECT "l1"."cnt" AS "x"
                FROM "j",
                  LATERAL (
                    SELECT COUNT(*) AS "cnt"
                    FROM "spj"
                    WHERE "spj"."sn" = "s"."sn"
                  ) AS "l1"
                LIMIT 5
              ) AS "l"
        "#;
        let expected_text = r#""#;
        test_it(
            "nested_lateral_grandparent_agg_with_limit_bail",
            original_text,
            expected_text,
        );
    }

    /// Negative: wrapper with OFFSET — same reasoning as LIMIT.
    #[test]
    fn nested_lateral_grandparent_agg_with_offset_bail() {
        let original_text = r#"
            SELECT "s"."sn", "l"."x"
            FROM "s",
              LATERAL (
                SELECT "l1"."cnt" AS "x"
                FROM "j",
                  LATERAL (
                    SELECT COUNT(*) AS "cnt"
                    FROM "spj"
                    WHERE "spj"."sn" = "s"."sn"
                  ) AS "l1"
                OFFSET 2
              ) AS "l"
        "#;
        let expected_text = r#""#;
        test_it(
            "nested_lateral_grandparent_agg_with_offset_bail",
            original_text,
            expected_text,
        );
    }

    /// Negative: wrapper with ORDER BY + LIMIT (Top-K).
    #[test]
    fn nested_lateral_grandparent_agg_with_topk_bail() {
        let original_text = r#"
            SELECT "s"."sn", "l"."x"
            FROM "s",
              LATERAL (
                SELECT "l1"."cnt" AS "x"
                FROM "j",
                  LATERAL (
                    SELECT COUNT(*) AS "cnt"
                    FROM "spj"
                    WHERE "spj"."sn" = "s"."sn"
                  ) AS "l1"
                ORDER BY "l1"."cnt" DESC
                LIMIT 3
              ) AS "l"
        "#;
        let expected_text = r#""#;
        test_it(
            "nested_lateral_grandparent_agg_with_topk_bail",
            original_text,
            expected_text,
        );
    }

    /// Negative: wrapper with GROUP BY — the wrapper itself is aggregated.
    /// After flattening GROUP BY would move to the outer level where `s`
    /// changes group membership. Rejected by downstream cardinality check.
    #[test]
    fn nested_lateral_grandparent_agg_wrapper_with_group_by_bail() {
        let original_text = r#"
            SELECT "s"."sn", "l"."total"
            FROM "s",
              LATERAL (
                SELECT SUM("l1"."cnt") AS "total"
                FROM "j",
                  LATERAL (
                    SELECT COUNT(*) AS "cnt"
                    FROM "spj"
                    WHERE "spj"."sn" = "s"."sn"
                  ) AS "l1"
                GROUP BY "j"."jn"
              ) AS "l"
        "#;
        let expected_text = r#""#;
        test_it(
            "nested_lateral_grandparent_agg_wrapper_with_group_by_bail",
            original_text,
            expected_text,
        );
    }

    /// Nested LATERAL: inner INNER JOIN correlates with grandparent scope.
    /// The inner LATERAL produces an INNER JOIN (not aggregate), so the
    /// correlation predicate lands in WHERE and can be extracted by the
    /// outer LATERAL.
    #[test]
    fn nested_lateral_grandparent_correlation() {
        let original_text = r#"
            SELECT "s"."sn", "l"."x"
            FROM "s",
              LATERAL (
                SELECT "l1"."pn" AS "x"
                FROM "j",
                  LATERAL (
                    SELECT "spj"."pn"
                    FROM "spj"
                    WHERE "spj"."sn" = "s"."sn"
                  ) AS "l1"
              ) AS "l"
        "#;
        let expected_text = r#"SELECT "s"."sn", "l"."x" FROM "s" INNER JOIN (SELECT "l1"."pn" AS "x", "l1"."sn" AS "sn" FROM "j" INNER JOIN (SELECT "spj"."pn", "spj"."sn" AS "sn" FROM "spj") AS "l1") AS "l" ON ("l"."sn" = "s"."sn")"#;
        test_it(
            "nested_lateral_grandparent_correlation",
            original_text,
            expected_text,
        );
    }

    // ----- Commit 4 (gate relaxation) tests -----

    /// Positive: LATERAL body with correlation-pinned GROUP BY whose body
    /// has an internal LEFT JOIN whose ON references an outer-scope alias.
    /// Flattens via the `pre_hoist_lateral_at_most_one` relaxation; the
    /// internal LEFT JOIN gets pulled up to the outer via the algebraic
    /// identity A × (B ⟕_p C) = (A × B) ⟕_p C.  apply_inline lifts the
    /// body's GROUP BY and aggregate to the outer; GROUP-BY additions add
    /// the outer's bare-column SELECT references (`a.k`) to the GROUP BY.
    #[test]
    fn correlation_pinned_grouped_lateral_with_internal_left_join_flattens() {
        let original_text = r#"
        SELECT a.k, l1.cnt
        FROM qa.a AS a,
             LATERAL (
                 SELECT b.k, COUNT(c.x) AS cnt
                 FROM qa.b AS b
                 LEFT JOIN qa.c AS c ON c.k = a.k
                 WHERE b.k = a.k
                 GROUP BY b.k
             ) AS l1
        "#;
        let expected_text = r#"SELECT "a"."k", count("c"."x") AS "cnt"
            FROM "qa"."a" AS "a", "qa"."b" AS "b"
            LEFT JOIN "qa"."c" AS "c" ON ("c"."k" = "a"."k")
            WHERE ("b"."k" = "a"."k")
            GROUP BY "b"."k", "a"."k""#;
        test_it(
            "correlation_pinned_grouped_lateral_with_internal_left_join_flattens",
            original_text,
            expected_text,
        );
    }

    /// Negative: aggregate-only-no-GROUP-BY LATERAL body (ExactlyOne)
    /// with internal outer-correlated LEFT JOIN.  Pre-this-commit, the
    /// rewriter accepted this on the Flatten path via the
    /// `pre_hoist_lateral_exactly_one` relaxation and emitted
    /// `qa.a, qa.b LEFT JOIN qa.c ... WHERE b.k=a.k GROUP BY a.k` —
    /// unsound: outer `qa.a` rows with no matching `qa.b` get dropped
    /// by `WHERE b.k=a.k` at outer scope, while pre-flatten the
    /// ExactlyOne body returns one row per outer with `cnt=0` (count
    /// over empty input is 0, not no rows).  The new gate rejects
    /// ExactlyOne flatten unconditionally; the candidate falls through
    /// to Resolve, which rejects per §5.2 (correlated structure inside
    /// LEFT JOIN ON cannot be moved out), so the rewrite bails.
    #[test]
    fn aggregate_only_lateral_with_internal_left_join_bail() {
        let original_text = r#"
        SELECT a.k, l1.cnt
        FROM qa.a AS a,
             LATERAL (
                 SELECT COUNT(c.x) AS cnt
                 FROM qa.b AS b
                 LEFT JOIN qa.c AS c ON c.k = a.k
                 WHERE b.k = a.k
             ) AS l1
        "#;
        let expected_text = "";
        test_it(
            "aggregate_only_lateral_with_internal_left_join_bail",
            original_text,
            expected_text,
        );
    }

    /// Positive: LATERAL body with HAVING in addition to correlation-pinned
    /// GROUP BY.  HAVING is correctly merged into the outer HAVING after
    /// flattening (since the lifted body remains aggregated).
    #[test]
    fn correlation_pinned_grouped_lateral_with_having_flattens() {
        let original_text = r#"
        SELECT a.k, l1.cnt
        FROM qa.a AS a,
             LATERAL (
                 SELECT b.k, COUNT(c.x) AS cnt
                 FROM qa.b AS b
                 LEFT JOIN qa.c AS c ON c.k = a.k
                 WHERE b.k = a.k
                 GROUP BY b.k
                 HAVING COUNT(c.x) > 0
             ) AS l1
        "#;
        let expected_text = r#"SELECT "a"."k", count("c"."x") AS "cnt"
            FROM "qa"."a" AS "a", "qa"."b" AS "b"
            LEFT JOIN "qa"."c" AS "c" ON ("c"."k" = "a"."k")
            WHERE ("b"."k" = "a"."k")
            GROUP BY "b"."k", "a"."k"
            HAVING (count("c"."x") > 0)"#;
        test_it(
            "correlation_pinned_grouped_lateral_with_having_flattens",
            original_text,
            expected_text,
        );
    }

    /// Negative: JOIN-position variant of the ExactlyOne-with-internal-
    /// LEFT-JOIN bail.  Same body shape, same unsoundness pre-this-
    /// commit; the JOIN-loop call site of
    /// `try_resolve_as_lateral_subquery` is exercised instead of the
    /// comma-loop site.  Resolve rejects per §5.2.
    #[test]
    fn aggregate_only_lateral_in_join_position_bail() {
        let original_text = r#"
        SELECT a.k, l1.cnt
        FROM qa.a AS a
        CROSS JOIN LATERAL (
            SELECT COUNT(c.x) AS cnt
            FROM qa.b AS b
            LEFT JOIN qa.c AS c ON c.k = a.k
            WHERE b.k = a.k
        ) AS l1
        "#;
        let expected_text = "";
        test_it(
            "aggregate_only_lateral_in_join_position_bail",
            original_text,
            expected_text,
        );
    }

    /// Negative: GROUP BY is NOT correlation-pinned (no correlation predicate
    /// at all).  Body's alias is in NEITHER pre-hoist set.  Gate rejects
    /// the relaxation; falls through to Resolved.  Without correlation,
    /// Resolved emits the LATERAL-stripped subquery as a regular cross-
    /// joined derived table.
    #[test]
    fn unpinned_grouped_lateral_bails_to_resolved() {
        let original_text = r#"
        SELECT a.k, l1.cnt
        FROM qa.a AS a,
             LATERAL (
                 SELECT b.k, COUNT(*) AS cnt
                 FROM qa.b AS b
                 GROUP BY b.k
             ) AS l1
        "#;
        let expected_text = r#"SELECT "a"."k", "l1"."cnt"
            FROM "qa"."a" AS "a",
                 (SELECT "b"."k", count(*) AS "cnt" FROM "qa"."b" AS "b" GROUP BY "b"."k") AS "l1""#;
        test_it(
            "unpinned_grouped_lateral_bails_to_resolved",
            original_text,
            expected_text,
        );
    }

    /// Negative: correlation-pinned grouped body, but the outer has a
    /// downstream INNER JOIN whose ON predicate `d.k = a.k` is a non-bare-
    /// column expression referencing relations OTHER than the inner alias.
    /// `check_group_by_compatibility` rejects this — it can only
    /// add bare column refs to the outer GROUP BY when hoisting an
    /// aggregated body, and a binary equality predicate on `d.k = a.k`
    /// can't be added directly.  Falls through to Resolved (which here
    /// also rejects via correlation-in-LEFT-JOIN-ON).
    #[test]
    fn grouped_lateral_with_multi_row_downstream_bails() {
        let original_text = r#"
        SELECT a.k, l1.cnt, d.x
        FROM qa.a AS a,
             LATERAL (
                 SELECT b.k, COUNT(c.x) AS cnt
                 FROM qa.b AS b
                 LEFT JOIN qa.c AS c ON c.k = a.k
                 WHERE b.k = a.k
                 GROUP BY b.k
             ) AS l1
        JOIN qa.d AS d ON d.k = a.k
        "#;
        let expected_text = r#""#;
        test_it(
            "grouped_lateral_with_multi_row_downstream_bails",
            original_text,
            expected_text,
        );
    }

    /// Negative: LATERAL body has DISTINCT in addition to GROUP BY.  The
    /// `!stmt.distinct` clause in `lateral_flatten_safe` still blocks
    /// regardless of pre-hoist set membership — DISTINCT is its own
    /// relaxation candidate.  Gate rejects → Resolved → Resolved also
    /// rejects (correlation in LEFT JOIN ON cannot be moved to WHERE
    /// out of a LEFT OUTER without changing semantics).
    #[test]
    fn grouped_lateral_with_distinct_bails() {
        let original_text = r#"
        SELECT a.k, l1.cnt
        FROM qa.a AS a,
             LATERAL (
                 SELECT DISTINCT b.k, COUNT(c.x) AS cnt
                 FROM qa.b AS b
                 LEFT JOIN qa.c AS c ON c.k = a.k
                 WHERE b.k = a.k
                 GROUP BY b.k
             ) AS l1
        "#;
        let expected_text = r#""#;
        test_it(
            "grouped_lateral_with_distinct_bails",
            original_text,
            expected_text,
        );
    }

    /// Negative: grouped LATERAL body with ORDER BY.  `stmt.order.is_none()`
    /// in `lateral_flatten_safe` still blocks — ORDER BY without LIMIT is
    /// a no-op semantically but the gate is conservative until a separate
    /// ORDER-BY relaxation lands.  Gate rejects → Resolved → Resolved
    /// rejects (same reason as `grouped_lateral_with_distinct_bails`).
    #[test]
    fn grouped_lateral_with_order_by_bails() {
        let original_text = r#"
        SELECT a.k, l1.cnt
        FROM qa.a AS a,
             LATERAL (
                 SELECT b.k, COUNT(c.x) AS cnt
                 FROM qa.b AS b
                 LEFT JOIN qa.c AS c ON c.k = a.k
                 WHERE b.k = a.k
                 GROUP BY b.k
                 ORDER BY b.k
             ) AS l1
        "#;
        let expected_text = r#""#;
        test_it(
            "grouped_lateral_with_order_by_bails",
            original_text,
            expected_text,
        );
    }

    /// Nested LATERAL: inner correlates with BOTH parent and grandparent.
    /// This is the most common real-world pattern — e.g. a subquery that
    /// filters on a grandparent key AND joins with a parent-scope table.
    #[test]
    fn nested_lateral_parent_and_grandparent_correlation() {
        let original_text = r#"
            SELECT "s"."sn", "l"."x"
            FROM "s",
              LATERAL (
                SELECT "l1"."pn" AS "x"
                FROM "j",
                  LATERAL (
                    SELECT "spj"."pn"
                    FROM "spj"
                    WHERE "spj"."sn" = "s"."sn"
                      AND "spj"."jn" = "j"."jn"
                  ) AS "l1"
              ) AS "l"
        "#;
        let expected_text = r#"SELECT "s"."sn", "l"."x" FROM "s" INNER JOIN
        (SELECT "l1"."pn" AS "x", "l1"."sn" AS "sn" FROM "j" INNER JOIN
        (SELECT "spj"."pn", "spj"."jn" AS "jn", "spj"."sn" AS "sn" FROM "spj") AS "l1" ON ("l1"."jn" = "j"."jn")) AS "l"
        ON ("l"."sn" = "s"."sn")"#;
        test_it(
            "nested_lateral_parent_and_grandparent_correlation",
            original_text,
            expected_text,
        );
    }

    /// Negative regression for the multi-aggregated LATERAL sibling
    /// composition rejection.  Two correlated LATERAL bodies, each
    /// with an internal LEFT JOIN that uses outer correlation.  Body
    /// shape forces the Flatten path (Resolve cannot decorrelate
    /// correlated LEFT JOINs per §5.2).  `l1` is correlation-pinned
    /// (AtMostOne) and gets the first aggregated-flatten slot.  `l2`
    /// is aggregate-only-no-GROUP-BY (ExactlyOne); the new gates
    /// reject it twice (ExactlyOne never admitted, plus composition
    /// already-queued), and Resolve also rejects per §5.2.  Rewrite
    /// raises `unsupported!`.
    #[test]
    fn lateral_two_aggregated_siblings_with_correlated_left_join_bail() {
        let original_stmt = r#"
        SELECT a.k, l1.cnt1, l2.cnt2
        FROM qa.a AS a,
             LATERAL (
                 SELECT b.k, COUNT(*) AS cnt1
                 FROM qa.b AS b
                 LEFT JOIN qa.c AS c1 ON c1.k = a.k
                 WHERE b.k = a.k
                 GROUP BY b.k
             ) l1,
             LATERAL (
                 SELECT COUNT(*) AS cnt2
                 FROM qa.b AS b2
                 LEFT JOIN qa.c AS c2 ON c2.k = a.k
                 WHERE b2.k = a.k
             ) l2
        "#;
        let expect_stmt = "";
        test_it(
            "lateral_two_aggregated_siblings_with_correlated_left_join_bail",
            original_stmt,
            expect_stmt,
        );
    }

    /// Three-sibling variant of the multi-aggregated-LATERAL bail.
    /// `l1` claims the only aggregated-flatten slot; `l2` and `l3`
    /// are both denied and fall through to Resolve, which rejects
    /// the correlated LEFT JOIN per §5.2.
    #[test]
    fn lateral_three_aggregated_siblings_with_correlated_left_join_bail() {
        let original_stmt = r#"
        SELECT a.k, l1.cnt1, l2.cnt2, l3.cnt3
        FROM qa.a AS a,
             LATERAL (
                 SELECT b.k, COUNT(*) AS cnt1
                 FROM qa.b AS b
                 LEFT JOIN qa.c AS c1 ON c1.k = a.k
                 WHERE b.k = a.k
                 GROUP BY b.k
             ) l1,
             LATERAL (
                 SELECT COUNT(*) AS cnt2
                 FROM qa.b AS b2
                 LEFT JOIN qa.c AS c2 ON c2.k = a.k
                 WHERE b2.k = a.k
             ) l2,
             LATERAL (
                 SELECT COUNT(*) AS cnt3
                 FROM qa.b AS b3
                 LEFT JOIN qa.c AS c3 ON c3.k = a.k
                 WHERE b3.k = a.k
             ) l3
        "#;
        let expect_stmt = "";
        test_it(
            "lateral_three_aggregated_siblings_with_correlated_left_join_bail",
            original_stmt,
            expect_stmt,
        );
    }

    /// Negative regression for the LATERAL-arm upstream cardinality
    /// check.  ExactlyOne body with outer-correlated LEFT JOIN.
    /// Under the current eligibility gates, the ExactlyOne rejection
    /// fires ahead of the upstream check, but the test stays as a
    /// regression pin: the under-correlated-`test2` upstream shape
    /// should never flatten regardless of which gate rejects it.
    #[test]
    fn lateral_exactly_one_under_correlated_upstream_does_not_flatten() {
        let original_stmt = r#"
        SELECT t1.b, t2.i, l.cnt
        FROM test1 t1, test2 t2,
             LATERAL (
                 SELECT COUNT(*) AS cnt
                 FROM test3 ta
                 LEFT JOIN test3 tb ON tb.b = t1.b
             ) l
        "#;
        let expect_stmt = "";
        test_it(
            "lateral_exactly_one_under_correlated_upstream_does_not_flatten",
            original_stmt,
            expect_stmt,
        );
    }

    /// Negative regression for the LATERAL-arm upstream check,
    /// AtMostOne variant.  Body has correlation-pinned GROUP BY and
    /// outer-correlated LEFT JOIN — would flatten — but the outer
    /// FROM contains an uncorrelated regular `test2`.  Reject by
    /// upstream check.
    #[test]
    fn lateral_at_most_one_under_correlated_upstream_does_not_flatten() {
        let original_stmt = r#"
        SELECT t1.b, t2.i, l.cnt
        FROM test1 t1, test2 t2,
             LATERAL (
                 SELECT ta.b, COUNT(*) AS cnt
                 FROM test3 ta
                 LEFT JOIN test3 tb ON tb.b = t1.b
                 WHERE ta.b = t1.b
                 GROUP BY ta.b
             ) l
        "#;
        let expect_stmt = "";
        test_it(
            "lateral_at_most_one_under_correlated_upstream_does_not_flatten",
            original_stmt,
            expect_stmt,
        );
    }

    /// Negative regression for the LATERAL-arm upstream check: a
    /// non-INNER (LEFT) upstream join is rejected unconditionally,
    /// even if the body's correlation references both upstream
    /// items.  The interaction between a null-extending upstream
    /// join and the post-flatten aggregate is not analyzed;
    /// conservatively bail.
    #[test]
    fn lateral_with_left_join_upstream_does_not_flatten() {
        let original_stmt = r#"
        SELECT t1.b, t2.i, l.cnt
        FROM test1 t1
        LEFT JOIN test2 t2 ON t2.b = t1.b
        CROSS JOIN LATERAL (
            SELECT COUNT(*) AS cnt
            FROM test3 ta
            LEFT JOIN test3 tb ON tb.b = t1.b
        ) l
        "#;
        let expect_stmt = "";
        test_it(
            "lateral_with_left_join_upstream_does_not_flatten",
            original_stmt,
            expect_stmt,
        );
    }

    /// Positive no-regression pin for two aggregated LATERAL
    /// siblings whose bodies have correlation only in WHERE (no
    /// internal correlated LEFT JOIN, so neither attempts the
    /// Flatten path — `has_outer_left_join_on` gates Flatten on the
    /// presence of an outer-correlated LEFT JOIN inside the body).
    /// Both bodies go through Resolve, which decorrelates each as a
    /// grouped derived-table joined to the outer on the pinned
    /// correlation column.  The new gates are orthogonal here —
    /// neither candidate reaches the Flatten branch — and this test
    /// pins that the gates' introduction did not perturb the
    /// WHERE-only-correlation multi-sibling shape.
    #[test]
    fn lateral_aggregated_siblings_where_correlation_only_resolve() {
        let original_stmt = r#"
        SELECT a.k, l1.cnt1, l2.cnt2
        FROM qa.a AS a,
             LATERAL (
                 SELECT COUNT(*) AS cnt1
                 FROM qa.b AS b
                 WHERE b.k = a.k
             ) l1,
             LATERAL (
                 SELECT COUNT(*) AS cnt2
                 FROM qa.c AS c
                 WHERE c.k = a.k
             ) l2
        "#;
        let expect_stmt = r#"SELECT "a"."k",
            coalesce("l1"."cnt1", 0), coalesce("l2"."cnt2", 0)
            FROM "qa"."a" AS "a"
            LEFT OUTER JOIN (
                SELECT count(*) AS "cnt1", "b"."k" AS "k"
                FROM "qa"."b" AS "b"
                GROUP BY "b"."k"
            ) AS "l1" ON ("l1"."k" = "a"."k")
            LEFT OUTER JOIN (
                SELECT count(*) AS "cnt2", "c"."k" AS "k"
                FROM "qa"."c" AS "c"
                GROUP BY "c"."k"
            ) AS "l2" ON ("l2"."k" = "a"."k")"#;
        test_it(
            "lateral_aggregated_siblings_where_correlation_only_resolve",
            original_stmt,
            expect_stmt,
        );
    }

    /// Two LATERAL siblings, neither grouped.  No
    /// `downstream_group_by_additions` are produced (the branch in
    /// `check_group_by_compatibility` is skipped when the inner is
    /// not aggregated).  Pre-existing shape; pins it stays
    /// unchanged under the new gates.
    #[test]
    fn lateral_two_sibling_jointpos_no_grouped_bodies_unchanged() {
        let original_stmt = r#"
        SELECT t1.b, l1.x, l2.y
        FROM test1 t1,
             LATERAL (SELECT t2.b, t2.i AS x FROM test2 t2 WHERE t2.b = t1.b) l1,
             LATERAL (SELECT t3.b, t3.i AS y FROM test3 t3 WHERE t3.b = t1.b) l2
        "#;
        let expect_stmt = r#"SELECT "t1"."b", "l1"."x", "l2"."y"
            FROM "test1" AS "t1"
            INNER JOIN (SELECT "t2"."b", "t2"."i" AS "x" FROM "test2" AS "t2") AS "l1" ON ("l1"."b" = "t1"."b")
            INNER JOIN (SELECT "t3"."b", "t3"."i" AS "y" FROM "test3" AS "t3") AS "l2" ON ("l2"."b" = "t1"."b")"#;
        test_it(
            "lateral_two_sibling_jointpos_no_grouped_bodies_unchanged",
            original_stmt,
            expect_stmt,
        );
    }

    /// Stub `UniqueColumnsSchema` that returns whatever the test
    /// declares — used by the schema-aware upstream-check tests below.
    struct ExplicitUniqueSchema {
        cols: HashMap<Relation, HashSet<Column>>,
    }

    impl UniqueColumnsSchema for ExplicitUniqueSchema {
        fn unique_columns_of(&self, rel: &Relation) -> Option<HashSet<Column>> {
            self.cols.get(rel).cloned()
        }
    }

    fn mk_qualified_col(schema: &str, table: &str, name: &str) -> Column {
        Column {
            name: name.into(),
            table: Some(Relation {
                name: table.into(),
                schema: Some(schema.into()),
            }),
        }
    }

    fn mk_qualified_rel(schema: &str, table: &str) -> Relation {
        Relation {
            name: table.into(),
            schema: Some(schema.into()),
        }
    }

    /// Positive: AtMostOne LATERAL body (correlation-pinned GROUP BY)
    /// correlated on `qa.a.k`, where the catalog declares `qa.a.k` as
    /// a single-column unique key.  The upstream check finds the
    /// intersection between body-correlation columns `{a.k}` and the
    /// table's unique columns `{a.k}`, accepts `qa.a` as superkey-
    /// covered, and the flatten proceeds.  AtMostOne is the only
    /// aggregated-body cardinality admitted to the Flatten path
    /// (ExactlyOne would drop outer rows pre-vs-post, see
    /// `aggregate_only_lateral_with_internal_left_join_bail`).
    #[test]
    fn lateral_upstream_with_unique_correlation_column_flattens() {
        let schema = ExplicitUniqueSchema {
            cols: HashMap::from([(
                mk_qualified_rel("qa", "a"),
                HashSet::from([mk_qualified_col("qa", "a", "k")]),
            )]),
        };
        let original_text = r#"
        SELECT a.k, l1.cnt
        FROM qa.a AS a,
             LATERAL (
                 SELECT b.k, COUNT(c.x) AS cnt
                 FROM qa.b AS b
                 LEFT JOIN qa.c AS c ON c.k = a.k
                 WHERE b.k = a.k
                 GROUP BY b.k
             ) AS l1
        "#;
        let expected_text = r#"SELECT "a"."k", count("c"."x") AS "cnt"
            FROM "qa"."a" AS "a", "qa"."b" AS "b"
            LEFT JOIN "qa"."c" AS "c" ON ("c"."k" = "a"."k")
            WHERE ("b"."k" = "a"."k")
            GROUP BY "b"."k", "a"."k""#;
        test_it_with_unique_schema(
            "lateral_upstream_with_unique_correlation_column_flattens",
            original_text,
            expected_text,
            &schema,
        );
    }

    /// Negative: same query as the positive test, but the catalog
    /// declares NO unique columns for `qa.a`.  Without proof that the
    /// correlation column is a superkey, the flatten would
    /// row-multiply through a non-unique-key duplicate, so the
    /// upstream check rejects and the rewrite errors.
    #[test]
    fn lateral_upstream_without_known_unique_key_does_not_flatten() {
        let schema = ExplicitUniqueSchema {
            cols: HashMap::new(),
        };
        let original_text = r#"
        SELECT a.k, l1.cnt
        FROM qa.a AS a,
             LATERAL (
                 SELECT b.k, COUNT(c.x) AS cnt
                 FROM qa.b AS b
                 LEFT JOIN qa.c AS c ON c.k = a.k
                 WHERE b.k = a.k
                 GROUP BY b.k
             ) AS l1
        "#;
        let expect_text = "";
        test_it_with_unique_schema(
            "lateral_upstream_without_known_unique_key_does_not_flatten",
            original_text,
            expect_text,
            &schema,
        );
    }

    /// Negative: catalog declares `qa.a.k` as the unique column, but
    /// the LATERAL body correlates on `a.dt` (a non-unique attribute).
    /// The intersection between body-correlation columns `{a.dt}` and
    /// the table's unique columns `{a.k}` is empty, so the upstream
    /// check rejects.
    #[test]
    fn lateral_upstream_correlation_on_non_unique_column_does_not_flatten() {
        let schema = ExplicitUniqueSchema {
            cols: HashMap::from([(
                mk_qualified_rel("qa", "a"),
                HashSet::from([mk_qualified_col("qa", "a", "k")]),
            )]),
        };
        let original_text = r#"
        SELECT a.dt, l1.cnt
        FROM qa.a AS a,
             LATERAL (
                 SELECT b.dt, COUNT(c.x) AS cnt
                 FROM qa.b AS b
                 LEFT JOIN qa.c AS c ON c.dt = a.dt
                 WHERE b.dt = a.dt
                 GROUP BY b.dt
             ) AS l1
        "#;
        let expect_text = "";
        test_it_with_unique_schema(
            "lateral_upstream_correlation_on_non_unique_column_does_not_flatten",
            original_text,
            expect_text,
            &schema,
        );
    }

    /// 3-level LATERAL chain where the mid level is aggregated AND
    /// itself correlates with the great-grandparent.  Pure Resolved
    /// path (no flatten — no outer-correlated LEFT JOIN ON).  The
    /// engine handles this by cascading INNER JOINs, lifting the
    /// innermost `spj.sn` to an auxiliary `sn0` projection that the
    /// outer scope joins against `s.sn`, alongside the mid scope's
    /// own `lvl2.sn = s.sn` correlation.  Both lifted ONs land at
    /// the outermost join, producing a correct semantically-
    /// equivalent rewrite for the 3-level great-grandparent shape.
    #[test]
    fn nested_lateral_three_levels_great_grandparent_correlation() {
        let original_text = r#"
            SELECT "s"."sn", "mid"."cnt"
            FROM "s",
              LATERAL (
                SELECT "lvl2"."sn", COUNT(*) AS "cnt"
                FROM "j" AS "lvl2",
                  LATERAL (
                    SELECT "spj"."qty"
                    FROM "spj"
                    WHERE "spj"."jn" = "lvl2"."jn"
                      AND "spj"."sn" = "s"."sn"
                  ) AS "lvl3"
                WHERE "lvl2"."sn" = "s"."sn"
                GROUP BY "lvl2"."sn"
              ) AS "mid"
        "#;
        let expected_text = r#"
            SELECT "s"."sn", "mid"."cnt"
            FROM "s" INNER JOIN (
                SELECT "lvl2"."sn", count(*) AS "cnt", "lvl3"."sn" AS "sn0"
                FROM "j" AS "lvl2" INNER JOIN (
                    SELECT "spj"."qty", "spj"."sn" AS "sn", "spj"."jn" AS "jn"
                    FROM "spj"
                ) AS "lvl3" ON ("lvl3"."jn" = "lvl2"."jn")
                GROUP BY "lvl2"."sn", "lvl3"."sn"
            ) AS "mid" ON (("mid"."sn" = "s"."sn") AND ("mid"."sn0" = "s"."sn"))
        "#;
        test_it(
            "nested_lateral_three_levels_great_grandparent_correlation",
            original_text,
            expected_text,
        );
    }

    /// LATERAL body with outer-correlated LEFT JOIN that triggers the
    /// Flatten path.  Verifies that `absorb_flatten` correctly composes
    /// the outer-correlated LEFT JOIN ON predicate when lifting the
    /// LATERAL body into the outer FROM.  Post-flatten, `"s"."sn"` is
    /// added to the GROUP BY for cardinality preservation.
    #[test]
    fn lateral_body_with_outer_correlated_left_join_flattens() {
        let original_text = r#"
            SELECT "s"."sn", "mid"."cnt"
            FROM "s",
              LATERAL (
                SELECT "lvl2"."sn", COUNT(*) AS "cnt"
                FROM "j" AS "lvl2"
                LEFT JOIN "p" AS "p_outer" ON "p_outer"."pn" = "s"."pn"
                WHERE "lvl2"."sn" = "s"."sn"
                GROUP BY "lvl2"."sn"
              ) AS "mid"
        "#;
        let expected_text = r#"
            SELECT "s"."sn", count(*) AS "cnt"
            FROM "s", "j" AS "lvl2"
              LEFT JOIN "p" AS "p_outer" ON ("p_outer"."pn" = "s"."pn")
            WHERE ("lvl2"."sn" = "s"."sn")
            GROUP BY "lvl2"."sn", "s"."sn"
        "#;
        test_it(
            "lateral_body_with_outer_correlated_left_join_flattens",
            original_text,
            expected_text,
        );
    }

    /// 4-level LATERAL chain to stress chained Resolve composition
    /// through 4 levels of recursion.  Pure Resolved path.
    ///
    /// Pins a current rewriter output that produces an `INNER JOIN`
    /// with no `ON` clause at the innermost level (between `p` and
    /// the lifted `lvl4` subquery).  The semantically-relevant
    /// correlations from the innermost body (`j.jn = spj.jn`,
    /// `j.sn = s.sn`) are correctly lifted to enclosing ON predicates
    /// via `sn0` auxiliary projections; the local `lvl4` join has no
    /// local correlation to carry as ON.  The ON-less `INNER JOIN` is
    /// non-standard SQL syntax and may not round-trip through some
    /// parsers — this test pins the current behaviour so future work
    /// can decide whether to emit `CROSS JOIN`/`INNER JOIN ON TRUE`
    /// instead.
    #[test]
    fn nested_lateral_four_levels_no_flatten() {
        let original_text = r#"
            SELECT "s"."sn", "l1"."cnt"
            FROM "s",
              LATERAL (
                SELECT "spj"."sn", COUNT(*) AS "cnt"
                FROM "spj",
                  LATERAL (
                    SELECT "p"."pn"
                    FROM "p",
                      LATERAL (
                        SELECT "j"."jn"
                        FROM "j"
                        WHERE "j"."jn" = "spj"."jn"
                          AND "j"."sn" = "s"."sn"
                      ) AS "lvl4"
                    WHERE "p"."sn" = "spj"."sn"
                  ) AS "lvl3"
                WHERE "spj"."sn" = "s"."sn"
                GROUP BY "spj"."sn"
              ) AS "l1"
        "#;
        let expected_text = r#"
            SELECT "s"."sn", "l1"."cnt"
            FROM "s" INNER JOIN (
                SELECT "spj"."sn", count(*) AS "cnt", "lvl3"."sn" AS "sn0"
                FROM "spj" INNER JOIN (
                    SELECT "p"."pn",
                           "lvl4"."jn" AS "jn",
                           "lvl4"."sn" AS "sn",
                           "p"."sn"   AS "sn0"
                    FROM "p" INNER JOIN (
                        SELECT "j"."jn", "j"."sn" AS "sn"
                        FROM "j"
                    ) AS "lvl4"
                ) AS "lvl3" ON (("lvl3"."sn0" = "spj"."sn")
                           AND ("lvl3"."jn"  = "spj"."jn"))
                GROUP BY "spj"."sn", "lvl3"."sn"
            ) AS "l1" ON (("l1"."sn"  = "s"."sn")
                      AND ("l1"."sn0" = "s"."sn"))
        "#;
        test_it(
            "nested_lateral_four_levels_no_flatten",
            original_text,
            expected_text,
        );
    }
}
