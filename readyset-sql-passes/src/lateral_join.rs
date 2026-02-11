use crate::get_local_from_items_iter_mut;
use crate::rewrite_utils::{
    RewriteStatus, add_expression_to_join_constraint, align_group_by_and_windows_with_correlation,
    analyse_lone_aggregates_subquery_fields, and_predicates_skip_true, as_sub_query_with_alias,
    as_sub_query_with_alias_mut, collect_local_from_items, collect_outermost_columns_mut,
    columns_iter, columns_iter_mut, contain_subqueries_with_limit_clause,
    default_alias_for_select_item_expression, expect_field_as_expr, expect_field_as_expr_mut,
    expect_sub_query_with_alias_mut, extract_aggregate_fallback_for_expr,
    get_from_item_reference_name, is_filter_pushable_from_item,
    move_correlated_constraints_from_join_to_where, project_columns_if,
    split_correlated_constraint, split_correlated_expression,
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
    split_correlated_expression(expr, &|rel| {
        is_outer_from_item(rel, local_tables, outer_tables)
    })
}

/// Checks if a column does not belong to any of the specified FROM items.
/// Used to detect correlation or projection requirements.
fn column_does_not_belong_from_items(col: &Column, from_items: &HashSet<Relation>) -> bool {
    if let Some(table) = &col.table {
        !from_items.contains(table)
    } else {
        false
    }
}

/// Updates local column references in an expression to use a given table alias,
/// if they do not belong to any outer FROM items. Used during projection hoisting.
fn replace_local_columns_table(
    expr: &mut Expr,
    from_item: Relation,
    outer_from_items: &HashSet<Relation>,
) {
    columns_iter_mut(expr).for_each(|col| {
        if column_does_not_belong_from_items(col, outer_from_items) {
            col.table = Some(from_item.clone());
        }
    });
}

/// For a given subquery, ensures all columns from that subquery referenced in an outer expression
/// are present in its SELECT projection. Updates references in the outer expression to use the subquery’s alias.
/// Essential for flattening correlated subqueries while preserving column visibility.
fn project_local_columns(
    tab_expr: &mut TableExpr,
    expr: &mut Expr,
    outer_from_items: &HashSet<Relation>,
) -> ReadySetResult<()> {
    project_columns_if(tab_expr, expr, |col| {
        column_does_not_belong_from_items(col, outer_from_items)
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
        .any(|col| column_does_not_belong_from_items(col, &local_from_items));
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
            && columns_iter(remaining_expr)
                .any(|col| column_does_not_belong_from_items(col, &local_from_items))
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
                &split_correlated_constraint(&correlated_expr, &local_from_items)?,
            )?;

            let mut tab_expr = TableExpr {
                inner: TableExprInner::Subquery(Box::new(stmt)),
                alias: Some(stmt_alias.clone()),
            };

            project_local_columns(&mut tab_expr, &mut correlated_expr, outer_from_items)?;

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
        let subquery_alias = subquery_tab_expr.alias.clone();
        let _ = mem::replace(tab_expr, subquery_tab_expr);
        return Ok(Some((
            subquery_alias.map(|alias| alias.into()).expect("Checked"),
            outer_join_on,
        )));
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
                &split_correlated_constraint(&outer_join_on, &local_from_items)?,
            )?;

            project_local_columns(tab_expr, &mut outer_join_on, outer_tables)?;
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

/// LATERAL RHS sanitation (before limit/offset guard):
///  • LIMIT 0 ⇒ force RHS empty (recursively clear LIMIT/OFFSET/ORDER and set FALSE at the right level).
///  • Move RHS-internal correlated atoms from JOIN .. ON to WHERE so TOP‑K partitioning sees correlation.
///  • Rewrite TOP‑K (ORDER/LIMIT) to ROW_NUMBER filters (RN ≤ N), clearing raw ORDER/LIMIT for the legacy guard.
/// This ensures the legacy `contain_subqueries_with_limit_clause` check will pass for sanitized LATERAL bodies.
fn try_resolve_as_lateral_subquery(
    from_item: &mut TableExpr,
    preceding_from_items: &HashSet<Relation>,
    ctx: &mut UnnestContext,
) -> ReadySetResult<(bool, Option<(TableExpr, Expr)>)> {
    let mut has_transformed;

    // Descend and resolve inner subqueries first; bail if not actually LATERAL
    let Some((stmt, _)) = as_sub_query_with_alias_mut(from_item) else {
        return Ok((false, None));
    };

    has_transformed = unnest_all_subqueries(stmt, ctx)?.has_rewrites();
    if stmt.lateral {
        stmt.lateral = false;
    } else {
        return Ok((has_transformed, None));
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
        replace_local_columns_table(
            &mut lateral_join_on,
            stmt_alias.into(),
            preceding_from_items,
        );
        Ok((true, Some((from_item.clone(), lateral_join_on))))
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
            let mapped = Expr::Call(FunctionExpr::Call {
                name: "coalesce".into(),
                arguments: Some(vec![Expr::Column(out_col.clone()), zero_expr]),
            });
            out_map.entry(out_col).or_insert(Ok(mapped));
        }
    }
    Ok(())
}

/// Selects the join operator for a rewritten LATERAL subquery.
fn get_join_operator_for_lateral(
    tab_expr: &TableExpr,
    join_operator: JoinOperator,
    join_constraint: &JoinConstraint,
    ctx: &UnnestContext,
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
                unreachable!("USING should have been desugared earlier")
            }
        });
    }

    if !join_operator.is_inner_join() {
        return Ok(join_operator);
    }

    Ok(InnerJoin)
}

/// Replace select-list columns with coalesced expressions from `fields_map`.
///
/// Steps:
/// 1. Alias any select-item to be replaced if it lacks an alias.
/// 2. Move `stmt.fields` into a temporary `SelectStatement` to collect its outer columns.
/// 3. Replace each collected column with the mapped `Expr`, returning on error.
/// 4. Ensure no unmapped column references remain in `stmt`.
/// 5. Restore updated fields back into `stmt.fields`.
///
/// # Arguments
/// * `stmt` – mutable reference to the `SelectStatement`.
/// * `fields_map` – map from `Column` to `Ok(coalesce_expr)` or `Err(ReadySetError)`.
///
/// # Errors
/// Returns the first `Err` from `fields_map`, or an `Unsupported` error if leftover columns exist.
fn coalesce_fields_references(
    stmt: &mut SelectStatement,
    fields_map: &HashMap<Column, ReadySetResult<Expr>>,
) -> ReadySetResult<()> {
    // 1: alias fields needing replacement
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
    // 2: extract fields to a temp statement
    let mut bogo_stmt = SelectStatement {
        fields: mem::take(&mut stmt.fields),
        ..Default::default()
    };
    // 3: apply coalesce replacements
    for expr in collect_outermost_columns_mut(&mut bogo_stmt)? {
        if let Expr::Column(col) = expr
            && let Some(inl_expr) = fields_map.get(col)
        {
            match inl_expr {
                Ok(inl_expr) => {
                    let _ = mem::replace(expr, inl_expr.clone());
                }
                Err(e) => return Err(e.clone()),
            }
        }
    }
    // 4: error if unmapped columns remain
    if collect_outermost_columns_mut(stmt)?
        .into_iter()
        .any(|expr| matches!(expr, Expr::Column(col) if fields_map.get(col).is_some()))
    {
        // TODO: think of a better error message
        unsupported!("COALESCE function call in place of a column reference")
    }
    // 5: restore updated fields
    stmt.fields = mem::take(&mut bogo_stmt.fields);

    Ok(())
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
fn resolve_lateral_subqueries(
    stmt: &mut SelectStatement,
    ctx: &mut UnnestContext,
) -> ReadySetResult<RewriteStatus> {
    let mut rewrite_status = RewriteStatus::default();

    // The LATERAL subqueries can be correlated with the preceding FROM items only,
    // As we are moving along the FROM items, the number of preceding FROM items might increase.
    // This HashSet maintains the current set of items, the remaining LATERAL subqueries can be correlated with.
    let mut preceding_from_items = HashSet::new();

    // Ordered FROM items, preceding current RHS. Maintained as we advance over the `stmt` FROM items.
    let mut preceding_to_rhs = Vec::new();

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
    //   `coalesce_fields_references(..)` replaces outer SELECT‑list occurrences of that COUNT column
    //   with `COALESCE(col, 0)` so projections match original semantics. We **never** apply these
    //   mappings in WHERE/ORDER/GROUP BY—only in the outer SELECT list—to avoid changing filtering/ordering.
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
    for stmt_from_item in stmt.tables.iter() {
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

        let (transformed, resolved_option) =
            try_resolve_as_lateral_subquery(&mut from_item, &preceding_from_items, ctx)?;

        preceding_from_items.insert(from_item_rel.clone());

        if let Some((resolved_from_item, lateral_join_on)) = resolved_option {
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
        } else if had_lateral {
            new_joins.push(JoinClause {
                operator: JoinOperator::InnerJoin,
                right: JoinRightSide::Table(from_item),
                constraint: JoinConstraint::Empty,
            });
        } else {
            new_tables.push(from_item);
        }

        preceding_to_rhs.push(from_item_rel);

        if transformed {
            rewrite_status.rewrite();
        }
    }

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

            let (transformed, resolved_option) =
                try_resolve_as_lateral_subquery(&mut from_item, &preceding_from_items, ctx)?;

            preceding_from_items.insert(from_item_rel.clone());

            if let Some((resolved_from_item, lateral_join_on)) = resolved_option {
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
            } else {
                // Add the regular one to the preceding items, as the later LATERAL subqueries
                // could be correlated with them.
                if is_rhs_from_item {
                    rhs_from_item_is_regular = true;
                }
                were_regular.push(from_item);
            }

            preceding_to_rhs.push(from_item_rel);

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
                right: if were_regular.len() == 1 {
                    JoinRightSide::Table(were_regular.pop().unwrap())
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

    // Apply COUNT‑only COALESCE mappings to the **outer SELECT list** (if any were populated by
    // `get_join_operator_for_lateral` for the CROSS/ON TRUE + ExactlyOne case). This step is
    // projection‑only; WHERE/ORDER are intentionally unaffected.
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
pub(crate) fn unnest_lateral_subqueries(
    stmt: &mut SelectStatement,
    ctx: &mut UnnestContext,
) -> ReadySetResult<RewriteStatus> {
    resolve_lateral_subqueries(stmt, ctx)
}

#[cfg(test)]
mod tests {
    use crate::lateral_join::unnest_lateral_subqueries;
    use crate::unnest_subqueries::{NonNullSchema, UnnestContext};
    use crate::unnest_subqueries_3vl::ProbeRegistry;
    use readyset_sql::ast::{Column, Relation, SqlQuery};
    use readyset_sql::{Dialect, DialectDisplay};
    use readyset_sql_parsing::{parse_query, parse_select};
    use std::collections::HashSet;
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

    fn test_it(test_name: &str, original_text: &str, expect_text: &str) {
        let mut stmt = match parse_query(Dialect::PostgreSQL, original_text) {
            Ok(SqlQuery::Select(stmt)) => stmt,
            Err(e) => panic!("> {test_name}: ORIGINAL STATEMENT PARSE ERROR: {e}"),
            _ => unreachable!(),
        };

        match unnest_lateral_subqueries(
            &mut stmt,
            &mut UnnestContext {
                schema: &NonNullSchemaMoke {},
                probes: ProbeRegistry::new(),
                pre_hoist_lateral_exactly_one: HashSet::new(),
                lateral_trivial_on: HashSet::new(),
            },
        ) {
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
            (SELECT max("t3"."i") AS "i", "t3"."b" AS "bb" FROM "test3" AS "t3" GROUP BY "bb") AS "t2"
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
}
