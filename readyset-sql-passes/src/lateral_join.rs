use crate::rewrite_utils::{
    add_expression_to_join_constraint, and_predicates, and_predicates_skip_true,
    as_sub_query_with_alias, as_sub_query_with_alias_mut, collect_outermost_columns_mut,
    columns_iter, columns_iter_mut, default_alias_for_select_item_expression, expect_field_as_expr,
    expect_field_as_expr_mut, expect_sub_query_with_alias_mut, for_each_function_call,
    get_from_item_reference_name, is_aggregated_select, is_filter_pushable_from_item,
    is_simple_parametrizable_filter, matches_eq_constraint, project_columns, split_expr,
};
use crate::{get_local_from_items_iter, get_local_from_items_iter_mut};
use itertools::Either;
use readyset_errors::{unsupported, unsupported_err, ReadySetError, ReadySetResult};
use readyset_sql::analysis::is_aggregate;
use readyset_sql::analysis::visit::{walk_select_statement, Visitor};
use readyset_sql::ast::{
    Column, Expr, FunctionExpr, JoinClause, JoinConstraint, JoinOperator, JoinRightSide, Literal,
    Relation, SelectStatement, SqlIdentifier, TableExpr, TableExprInner,
};
use readyset_sql::{Dialect, DialectDisplay};
use std::collections::{HashMap, HashSet};
use std::{iter, mem};
use tracing::trace;

/// Trait for statements that can rewrite LATERAL joins as regular joins.
/// Intended to be implemented on SELECT statements after schema resolution and star expansion.
/// **IMPORTANT**: This rewrite pass must be called after the schema resolution, star expansion
/// and JoinConstraint::USING expansion passes.
pub trait RewriteLateralJoin: Sized {
    fn rewrite_lateral_joins(self) -> ReadySetResult<Self>;
}

/// Top-level method to convert all LATERAL joins in a SELECT statement into equivalent standard joins.
/// Modifies the statement in-place, flattening correlated subqueries and updating joins as needed.
impl RewriteLateralJoin for SelectStatement {
    fn rewrite_lateral_joins(mut self) -> ReadySetResult<Self> {
        if resolve_lateral_subqueries(&mut self)? {
            println!(
                "LATERAL sub-queries resolved: {}",
                self.display(Dialect::PostgreSQL)
            );
            trace!(
                name = "LATERAL sub-queries resolved",
                "{}",
                self.display(Dialect::PostgreSQL)
            );
        }
        Ok(self)
    }
}

/// Checks if a table relation should be considered an "outer" table for correlation analysis,
/// as opposed to a local table within a subquery or join.
fn is_outer_from_item(
    from_item: &Relation,
    local_tables: &HashSet<Relation>,
    outer_tables: &HashSet<Relation>,
) -> bool {
    !local_tables.contains(from_item) && outer_tables.contains(from_item)
}

fn is_correlated_lhs_rhs(
    lhs: &Relation,
    rhs: &Relation,
    local_tables: &HashSet<Relation>,
    outer_tables: &HashSet<Relation>,
) -> bool {
    is_outer_from_item(lhs, local_tables, outer_tables)
        || is_outer_from_item(rhs, local_tables, outer_tables)
}

/// Determines if an equality constraint between two columns references an outer table,
/// making it a correlated predicate that must be hoisted during LATERAL join rewriting.
fn is_correlated_eq_constraint(
    expr: &Expr,
    local_tables: &HashSet<Relation>,
    outer_tables: &HashSet<Relation>,
) -> bool {
    matches_eq_constraint(expr, |left_table, right_table| {
        is_correlated_lhs_rhs(left_table, right_table, local_tables, outer_tables)
    })
}

/// Checks if a simple (e.g., column compares literal, column in (literals), col between literal and literal)
/// filter on an outer table, identifying correlated filter predicates for extraction.
fn is_correlated_simple_filter(
    expr: &Expr,
    local_tables: &HashSet<Relation>,
    outer_tables: &HashSet<Relation>,
) -> bool {
    is_simple_parametrizable_filter(expr, |table, _| {
        is_outer_from_item(table, local_tables, outer_tables)
    })
}

/// Splits a predicate expression into a correlated part (references outer tables)
/// and a non-correlated part, for extracting join predicates during rewriting.
fn split_correlated_expression(
    expr: &Expr,
    local_tables: &HashSet<Relation>,
    outer_tables: &HashSet<Relation>,
) -> (Option<Expr>, Option<Expr>) {
    let mut correlated_constraints: Vec<Expr> = Vec::new();
    let remaining_expr = split_expr(
        expr,
        &|e| {
            is_correlated_eq_constraint(e, local_tables, outer_tables)
                || is_correlated_simple_filter(e, local_tables, outer_tables)
        },
        &mut correlated_constraints,
    );

    let mut correlated_expr = None;
    for e in correlated_constraints.into_iter() {
        correlated_expr = and_predicates(correlated_expr, e);
    }

    (correlated_expr, remaining_expr)
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
    let local_columns_refs = columns_iter_mut(expr)
        .filter_map(|col| {
            if column_does_not_belong_from_items(col, outer_from_items) {
                Some(col)
            } else {
                None
            }
        })
        .collect::<Vec<_>>();

    let (tab_expr_stmt, tab_expr_alias) = expect_sub_query_with_alias_mut(tab_expr);
    let projected_columns_alias = project_columns(
        tab_expr_stmt,
        &local_columns_refs
            .iter()
            .map(|col| Expr::Column((*col).clone()))
            .collect::<Vec<_>>(),
    )?;

    local_columns_refs
        .into_iter()
        .zip(projected_columns_alias)
        .for_each(|(col_ref, proj)| {
            col_ref.table = Some(tab_expr_alias.clone().into());
            col_ref.name = proj;
        });

    Ok(())
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
    let mut add_to_where_clause = None;
    let mut correlated_join_clauses = Vec::new();
    for (join_clause_idx, join_clause) in stmt.join.iter().enumerate() {
        match &join_clause.constraint {
            JoinConstraint::On(on_expr)
                if is_filter_pushable_from_item(stmt, stmt.tables.len() + join_clause_idx)? =>
            {
                if let (Some(correlated_expr), remaining_expr) =
                    split_correlated_expression(on_expr, local_from_items, outer_from_items)
                {
                    add_to_where_clause =
                        and_predicates_skip_true(add_to_where_clause, correlated_expr);
                    correlated_join_clauses.push((join_clause_idx, remaining_expr));
                }
            }
            _ => {}
        }
    }

    for (join_clause_idx, remaining_expr) in correlated_join_clauses {
        stmt.join[join_clause_idx].constraint = if let Some(remaining_expr) = remaining_expr {
            JoinConstraint::On(remaining_expr)
        } else {
            JoinConstraint::Empty
        };
    }

    if let Some(add_to_where_clause) = add_to_where_clause {
        stmt.where_clause =
            and_predicates_skip_true(stmt.where_clause.clone(), add_to_where_clause);
    }

    Ok(())
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

    // Collect the local FROM items
    let local_from_items = get_local_from_items_iter!(stmt)
        .map(get_from_item_reference_name)
        .collect::<ReadySetResult<HashSet<Relation>>>()?;

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
        unsupported!("Unsupported correlation outside of WHERE clause");
    }

    if let Some(where_clause) = &stmt.where_clause {
        // Extract that piece of the WHERE clause, which correlates with the legit outer scope.
        let (correlated_expr, remaining_expr) =
            split_correlated_expression(where_clause, &local_from_items, outer_from_items);

        // Verify the remaining expression has no correlation.
        // **NOTE**: We are visiting the outermost columns only, w/o walking into sub-queries.
        if let Some(remaining_expr) = &remaining_expr {
            if columns_iter(remaining_expr)
                .any(|col| column_does_not_belong_from_items(col, &local_from_items))
            {
                unsupported!(
                    "Unsupported correlation in subquery: {}",
                    remaining_expr.display(Dialect::PostgreSQL)
                );
            }
        }

        // `correlated_expr` contains equality constraints and simple filters,
        // involving columns from `outer_from_items`
        if let Some(mut correlated_expr) = correlated_expr {
            stmt.where_clause = remaining_expr;

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

    for (idx, local_tab_expr) in get_local_from_items_iter_mut!(stmt).enumerate() {
        if let Some((subquery_ref_name, mut outer_join_on)) =
            try_extract_correlated_subquery(local_tab_expr, outer_tables)?
        {
            if !is_filter_pushable_from_item(stmt, idx)? {
                unsupported!("LATERAL sub-query contains LEFT OUTER JOIN")
            }
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

/// Returns true if any subquery within the SELECT statement contains a LIMIT clause.
/// Used to detect cases where LATERAL join rewriting may be unsupported.
fn contain_subqueries_with_limit_clause(stmt: &SelectStatement) -> ReadySetResult<bool> {
    struct LookupVisitor {
        contains_limit_clause: bool,
    }

    impl<'ast> Visitor<'ast> for LookupVisitor {
        type Error = ReadySetError;

        fn visit_select_statement(
            &mut self,
            select_statement: &'ast SelectStatement,
        ) -> Result<(), Self::Error> {
            if !select_statement.limit_clause.is_empty() {
                self.contains_limit_clause = true;
            }
            walk_select_statement(self, select_statement)
        }
    }

    let mut visitor = LookupVisitor {
        contains_limit_clause: false,
    };
    visitor.visit_select_statement(stmt)?;

    Ok(visitor.contains_limit_clause)
}

/// Tries to resolve a TableExpr as a LATERAL subquery suitable for conversion to a regular join.
/// It recursively resolves all LATERAL subqueries inside the input TableExpr first,
/// and then will try to resolve the input one only if it's attributed as LATERAL.
/// If successful, return the rewritten TableExpr and the extracted join predicate, and a flag
/// if the input itself, or any of its inner sub-queries have actually been transformed.
fn try_resolve_as_lateral_subquery(
    from_item: &mut TableExpr,
    preceding_from_items: &HashSet<Relation>,
) -> ReadySetResult<(bool, Option<(TableExpr, Expr)>)> {
    let inner_laterals_has_transformed;
    if let Some((stmt, _)) = as_sub_query_with_alias_mut(from_item) {
        inner_laterals_has_transformed = resolve_lateral_subqueries(stmt)?;
        if stmt.lateral {
            stmt.lateral = false;
        } else {
            return Ok((inner_laterals_has_transformed, None));
        }
    } else {
        return Ok((false, None));
    }

    if let Some(mut lateral_join_on) = resolve_lateral_subquery(from_item, preceding_from_items)? {
        let (stmt, stmt_alias) = expect_sub_query_with_alias_mut(from_item);
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
        Ok((inner_laterals_has_transformed, None))
    }
}

/// Determines the correct join operator when rewriting a LATERAL subquery into a regular join.
///
/// - For aggregate-only LATERAL subqueries (no GROUP BY) with no ON constraint, emits LEFT OUTER JOIN
///   and populates `fields_map` with `COALESCE` expressions for COUNT/literal fields to preserve null semantics.
/// - If the original operator is a LEFT or RIGHT OUTER join (and not a lone-aggregate special case), preserves it.
/// - Otherwise defaults to INNER JOIN.
///
/// # Arguments
/// * `tab_expr` - the table expression for the LATERAL subquery.
/// * `join_operator` - the join operator from the original query context.
/// * `join_constraint` - the ON-condition or empty constraint associated with the join.
/// * `fields_map` - mapping of columns to coalesce expressions for lone aggregates.
///
/// # Returns
/// A `ReadySetResult<JoinOperator>` indicating the operator to use in the rewritten join.
fn get_join_operator_for_lateral(
    tab_expr: &TableExpr,
    join_operator: JoinOperator,
    join_constraint: &JoinConstraint,
    fields_map: &mut HashMap<Column, ReadySetResult<Expr>>,
) -> ReadySetResult<JoinOperator> {
    if let Some((stmt, stmt_alis)) = as_sub_query_with_alias(tab_expr) {
        if is_aggregated_select(stmt)? && stmt.group_by.is_none() {
            if matches!(
                join_constraint,
                JoinConstraint::Empty | JoinConstraint::On(Expr::Literal(Literal::Boolean(true)))
            ) {
                analyse_lone_aggregates_subquery_fields(stmt, stmt_alis.clone(), fields_map)?;
                return Ok(JoinOperator::LeftOuterJoin);
            } else {
                unsupported!("Lone aggregates without GROUP BY subquery joining with ON condition")
            }
        } else if matches!(
            join_operator,
            JoinOperator::LeftJoin
                | JoinOperator::LeftOuterJoin
                | JoinOperator::RightJoin
                | JoinOperator::RightOuterJoin
        ) {
            return Ok(join_operator);
        }
    }
    Ok(JoinOperator::InnerJoin)
}

/// For aggregate-only LATERAL subqueries without GROUP BY, analyzes each SELECT field and
/// populates `fields_map` so that COUNT and literal fields are coalesced to default values.
///
/// # Arguments
/// * `stmt` - The aggregate-only subquery SELECT statement.
/// * `stmt_alias` - The alias used for the subquery, used to qualify projected columns.
/// * `fields_map` - A map to be populated with columns and their coalesced expressions or errors.
///
/// For pure COUNT aggregates, insert COALESCE (col, 0).
/// For literal fields, insert COALESCE (col, literal).
/// For expressions over only COUNT aggregates, inserts an unsupported error.
fn analyse_lone_aggregates_subquery_fields(
    stmt: &SelectStatement,
    stmt_alias: SqlIdentifier,
    fields_map: &mut HashMap<Column, ReadySetResult<Expr>>,
) -> ReadySetResult<()> {
    // Constructs a COALESCE function call for a column and a literal fallback.
    macro_rules! make_coalesce {
        ($col:expr, $lit:expr) => {
            Expr::Call(FunctionExpr::Call {
                name: "coalesce".into(),
                arguments: Some(vec![Expr::Column($col), Expr::Literal($lit)]),
            })
        };
    }

    // Identifies COUNT aggregate functions.
    let is_count_aggregate =
        |f: &FunctionExpr| matches!(f, FunctionExpr::Count { .. } | FunctionExpr::CountStar);

    for fe in &stmt.fields {
        let (f_expr, f_alias) = expect_field_as_expr(fe);
        if let Some(f_alias) = f_alias {
            let f_col = Column {
                name: f_alias.clone(),
                table: Some(stmt_alias.clone().into()),
            };
            match f_expr {
                // For pure COUNT fields, coalesce to zero.
                Expr::Call(f) if is_count_aggregate(f) => {
                    fields_map.insert(
                        f_col.clone(),
                        Ok(make_coalesce!(f_col.clone(), Literal::Integer(0))),
                    );
                }
                // For literal fields, coalesce to the literal value.
                Expr::Literal(lit) => {
                    fields_map.insert(
                        f_col.clone(),
                        Ok(make_coalesce!(f_col.clone(), lit.clone())),
                    );
                }
                // For expressions over only COUNT aggregates, insert unsupported error.
                expr => {
                    // Detects if the expression is over only COUNT aggregates.
                    let mut has_count_aggregate = false;
                    let mut has_other_aggregates = false;
                    for_each_function_call(expr, &mut |f| {
                        if is_count_aggregate(f) {
                            has_count_aggregate = true;
                        } else if is_aggregate(f) {
                            has_other_aggregates = true;
                        }
                    })?;
                    if has_count_aggregate && !has_other_aggregates {
                        // The field `fe` is an expression over only aggregate(s) `COUNT()`, which we could calculate
                        // assuming `COUNT()` is 0. But, for now, let's just error out if this field is referenced.
                        fields_map.insert(
                            f_col,
                            Err(unsupported_err!("Expression over aggregate COUNT")),
                        );
                    }
                }
            }
        }
    }

    Ok(())
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
        if let Expr::Column(col) = expr {
            if let Some(inl_expr) = fields_map.get(col) {
                match inl_expr {
                    Ok(inl_expr) => {
                        let _ = mem::replace(expr, inl_expr.clone());
                    }
                    Err(e) => return Err(e.clone()),
                }
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
fn resolve_lateral_subqueries(stmt: &mut SelectStatement) -> ReadySetResult<bool> {
    let mut has_transformed = false;

    // The LATERAL subqueries can be correlated with the preceding FROM items only,
    // As we are moving along the FROM items, the number of preceding FROM items might increase.
    // This HashSet maintains the current set of items, the remaining LATERAL subqueries can be correlated with.
    let mut preceding_from_items = HashSet::new();

    // Build new comma separated tables/sub-queries and joins
    let mut new_tables = Vec::new();
    let mut new_joins = Vec::new();

    // This map is used to handle a particular use case, when a `lateral` subquery
    // is a lone aggregates statement, contains `COUNT()` and is cross-joined.
    // In such a case, the original cross-join will be replaced with left-outer-join,
    // and all outer occurrences of the column `COUNT()` should be replaced with `COALESCE(col, 0)`.
    // This is safe to do only in the outer statement select list, b/c other places can be sensitive to
    // replacing column reference with function call.
    let mut coalesce_fields_map: HashMap<Column, ReadySetResult<Expr>> = HashMap::new();

    // Used to preserve the original joining order for comma separated tables/sub-queries in the loop below.
    // After the first handled `lateral`, all items from `stmt.tables` will be added to `new_joins`,
    // regardless of being `lateral` or not.
    let mut had_lateral = false;

    // Handle the left-hand side comma separated tables/sub-queries.
    for stmt_from_item in stmt.tables.iter() {
        //
        let join_operator_for_lateral = get_join_operator_for_lateral(
            stmt_from_item,
            JoinOperator::CrossJoin,
            &JoinConstraint::Empty,
            &mut coalesce_fields_map,
        )?;

        let mut from_item = stmt_from_item.clone();

        let (transformed, resolved_option) =
            try_resolve_as_lateral_subquery(&mut from_item, &preceding_from_items)?;

        preceding_from_items.insert(get_from_item_reference_name(&from_item)?);

        if let Some((resolved_from_item, lateral_join_on)) = resolved_option {
            new_joins.push(JoinClause {
                operator: join_operator_for_lateral,
                right: JoinRightSide::Table(resolved_from_item),
                constraint: JoinConstraint::On(lateral_join_on),
            });
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

        if transformed {
            has_transformed = true;
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
            let join_operator_for_lateral = get_join_operator_for_lateral(
                rhs_from_item,
                stmt_jc.operator,
                if is_rhs_from_item {
                    &stmt_jc.constraint
                } else {
                    &JoinConstraint::Empty
                },
                &mut coalesce_fields_map,
            )?;

            let mut from_item = rhs_from_item.clone();

            let (transformed, resolved_option) =
                try_resolve_as_lateral_subquery(&mut from_item, &preceding_from_items)?;

            preceding_from_items.insert(get_from_item_reference_name(&from_item)?);

            if let Some((resolved_from_item, lateral_join_on)) = resolved_option {
                // The current RHS item was LATERAL, generate a join clause for it.
                // In case it was referenced in the current join condition,
                // combine `lateral_join_on` with the current join condition, and use it.
                were_lateral.push(JoinClause {
                    operator: join_operator_for_lateral,
                    right: JoinRightSide::Table(resolved_from_item),
                    constraint: if is_rhs_from_item {
                        add_expression_to_join_constraint(
                            stmt_jc.constraint.clone(),
                            lateral_join_on,
                        )
                    } else {
                        JoinConstraint::On(lateral_join_on)
                    },
                });
            } else {
                // Add the regular one to the preceding items, as the later LATERAL subqueries
                // could be correlated with them.
                if is_rhs_from_item {
                    rhs_from_item_is_regular = true;
                }
                were_regular.push(from_item);
            }

            if transformed {
                has_transformed = true;
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

    if !coalesce_fields_map.is_empty() {
        coalesce_fields_references(stmt, &coalesce_fields_map)?;
    }

    Ok(has_transformed)
}

#[cfg(test)]
mod tests {
    use crate::lateral_join::RewriteLateralJoin;
    use readyset_sql::ast::SqlQuery;
    use readyset_sql::Dialect;
    use readyset_sql_parsing::{parse_query, parse_select};

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

    fn test_it(test_name: &str, original_text: &str, expect_text: &str) {
        let rewritten_stmt = match parse_query(Dialect::PostgreSQL, original_text) {
            Ok(SqlQuery::Select(stmt)) => stmt,
            Err(e) => panic!("> {test_name}: ORIGINAL STATEMENT PARSE ERROR: {e}"),
            _ => unreachable!(),
        }
        .rewrite_lateral_joins();
        match rewritten_stmt {
            Ok(expect_stmt) => {
                assert_eq!(
                    expect_stmt,
                    match parse_select(Dialect::PostgreSQL, expect_text) {
                        Ok(stmt) => stmt,
                        Err(e) => panic!("> {test_name}: REWRITTEN STATEMENT PARSE ERROR: {e}"),
                    }
                );
            }
            Err(e) => {
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
        let original_stmt =
            "SELECT T1.auth_id FROM test1 T1, LATERAL(SELECT T11.auth_id, T11.i FROM test2 T11 \
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
        let original_stmt = "SELECT T1.auth_id FROM test1 T1, LATERAL (SELECT T11.auth_id, T11.i FROM \
                              test2 T11 \
                                join \
                              (select T22.i a from test3 T22 where T22.b = T1.b and T22.auth_id = T1.auth_id) T12 \
                                on T11.i = T12.a) T13 \
                              WHERE T1.t = 'aa'";
        let expect_stmt = r#"SELECT "t1"."auth_id" FROM "test1" AS "t1" INNER JOIN
            (SELECT "t11"."auth_id", "t11"."i", "t12"."auth_id" AS "auth_id_0", "t12"."b" AS "b" FROM "test2" AS "t11" JOIN
            (SELECT "t22"."i" AS "a", "t22"."auth_id" AS "auth_id", "t22"."b" AS "b" FROM "test3" AS "t22") AS "t12"
            ON ("t11"."i" = "t12"."a")) AS "t13" ON (("t13"."b" = "t1"."b") AND ("t13"."auth_id_0" = "t1"."auth_id"))
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
        let original_stmt = "SELECT * FROM \
                        test1, test, (SELECT test3.i, test3.b FROM test3 where test3.t = 'aa') t33, \
                            LATERAL(SELECT t3.t, t2.b, t2.i, t3.i FROM \
                                    (select test2.b, test2.t, test2.i FROM test2 \
                                       WHERE test.i = test2.i \
                                             and test1.i = test2.i \
                                             and test2.i = t33.i \
                                             and test2.b = 11 and \
                                             test.b = 11\
                                    ) t2 \
                                    JOIN \
                                    (SELECT test3.t, test3.i FROM test3 \
                                       WHERE test1.i = test3.i and test3.t = 'aa'\
                                    ) AS t3 \
                                    ON t2.t = t3.t \
                                  ) AS tl";
        let expect_stmt = r#"SELECT * FROM "test1", "test", (SELECT "test3"."i", "test3"."b" FROM "test3"
            WHERE ("test3"."t" = 'aa')) AS "t33" INNER JOIN (SELECT "t3"."t", "t2"."b", "t2"."i", "t3"."i" AS "i_0"
            FROM (SELECT "test2"."b", "test2"."t", "test2"."i" FROM "test2" WHERE ("test2"."b" = 11)) AS "t2" JOIN
            (SELECT "test3"."t", "test3"."i" FROM "test3" WHERE ("test3"."t" = 'aa')) AS "t3"
            ON ("t2"."t" = "t3"."t")) AS "tl" ON ((((("test"."i" = "tl"."i") AND
            ("test1"."i" = "tl"."i")) AND ("tl"."i" = "t33"."i")) AND ("test"."b" = 11)) AND
            ("test1"."i" = "tl"."i_0"))"#;
        test_it("test5", original_stmt, expect_stmt);
    }

    #[test]
    fn test6() {
        let original_stmt = "SELECT * FROM \
                         (test1 join test on test1.t = test.t) join \
                         (SELECT test3.i, test3.b FROM test3 where test3.t = 'aa') t33 on test1.b = t33.b left join \
                          LATERAL(SELECT t3.t, t2.b, t2.i as t2_i, t3.i as t3_i FROM \
                              (select test2.b, test2.t, test2.i FROM test2 \
                                   WHERE test.i = test2.i \
                                         and test1.i = test2.i \
                                         and test2.b = 11 \
                                         and test.b = 11\
                              ) t2 \
                              JOIN \
                              (SELECT test3.t, test3.i FROM test3 WHERE test1.i = test3.i and test3.t = 'aa') AS t3 \
                              ON t2.t = t3.t\
                         ) AS tl on t33.i = tl.t3_i";
        let expect_stmt = r#"SELECT * FROM "test1" JOIN "test" ON ("test1"."t" = "test"."t") JOIN
            (SELECT "test3"."i", "test3"."b" FROM "test3" WHERE ("test3"."t" = 'aa')) AS "t33" ON ("test1"."b" = "t33"."b") LEFT JOIN
            (SELECT "t3"."t", "t2"."b", "t2"."i" AS "t2_i", "t3"."i" AS "t3_i" FROM (SELECT "test2"."b", "test2"."t", "test2"."i" FROM "test2"
            WHERE ("test2"."b" = 11)) AS "t2" JOIN (SELECT "test3"."t", "test3"."i" FROM "test3"
            WHERE ("test3"."t" = 'aa')) AS "t3" ON ("t2"."t" = "t3"."t")) AS "tl"
            ON (("t33"."i" = "tl"."t3_i") AND (((("test"."i" = "tl"."t2_i") AND ("test1"."i" = "tl"."t2_i"))
            AND ("test"."b" = 11)) AND ("test1"."i" = "tl"."t3_i")))"#;
        test_it("test6", original_stmt, expect_stmt);
    }

    #[test]
    fn test7() {
        let original_stmt = "SELECT * FROM \
                (test1 join test on test1.t = test.t) join \
                 LATERAL (SELECT t3.t, t2.b, t2.i as t2_i, t3.i as t3_i FROM \
                                 (select test2.b, test2.t, test2.i \
                                     FROM test2 \
                                     WHERE test.i = test2.i \
                                           and test1.i = test2.i \
                                           and test2.b = 11 \
                                           and test.b = 11\
                                 ) t2 \
                                 JOIN \
                                 (SELECT test3.t, min(test3.i) i \
                                     FROM test3 \
                                     WHERE test1.i = test3.i \
                                           and test3.b = 11 \
                                     GROUP BY test3.t\
                                 ) AS t3 \
                                 ON t2.t = t3.t\
                             ) AS tl \
                             ON test1.b = tl.b \
                left join \
                (SELECT test3.i, test3.b FROM test3 where test3.t = 'aa') t33 on tl.b = t33.b";
        let expect_stmt = r#"SELECT * FROM "test1" JOIN "test" ON ("test1"."t" = "test"."t") INNER JOIN
            (SELECT "t3"."t", "t2"."b", "t2"."i" AS "t2_i", "t3"."i" AS "t3_i", "t3"."i_0" AS "i_0" FROM
            (SELECT "test2"."b", "test2"."t", "test2"."i" FROM "test2" WHERE ("test2"."b" = 11)) AS "t2" JOIN
            (SELECT "test3"."t", min("test3"."i") AS "i", "test3"."i" AS "i_0" FROM "test3"
            WHERE ("test3"."b" = 11) GROUP BY "test3"."t", "test3"."i") AS "t3" ON ("t2"."t" = "t3"."t")) AS "tl"
            ON (("test1"."b" = "tl"."b") AND (((("test"."i" = "tl"."t2_i") AND ("test1"."i" = "tl"."t2_i")) AND
            ("test"."b" = 11)) AND ("test1"."i" = "tl"."i_0"))) LEFT JOIN (SELECT "test3"."i", "test3"."b"
            FROM "test3" WHERE ("test3"."t" = 'aa')) AS "t33" ON ("tl"."b" = "t33"."b")"#;
        test_it("test7", original_stmt, expect_stmt);
    }

    #[test]
    fn test8() {
        let original_stmt =
            "select T1.i from test T1 left join \
             LATERAL(select max(T3.i) i from test3 T3 WHERE T3.b = T1.b group by T3.t) T2 on T1.i = T2.i \
             WHERE T1.t = 'aa'";
        let expect_stmt = r#"SELECT "t1"."i" FROM "test" AS "t1" LEFT JOIN
            (SELECT max("t3"."i") AS "i", "t3"."b" AS "b" FROM "test3" AS "t3" GROUP BY "t3"."t", "t3"."b") AS "t2"
            ON (("t1"."i" = "t2"."i") AND ("t2"."b" = "t1"."b")) WHERE ("t1"."t" = 'aa')"#;
        test_it("test8", original_stmt, expect_stmt);
    }

    #[test]
    fn test9() {
        let original_stmt =
            "select T1.i from test T1 left join \
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
        let expect_stmt = r#""#;
        test_it("test10", original_stmt, expect_stmt);
    }

    #[test]
    fn test11() {
        let original_stmt = "SELECT * FROM (test1 join test on test1.t = test.t) \
                 join (SELECT test3.i, test3.b FROM test3 where test3.t = 'aa') t33 on test1.b = t33.b \
                 left join LATERAL(\
                    SELECT t3.t, t2.b, t2.i as t2_i, t3.i as t3_i \
                        FROM (select test2.b, test2.t, test2.i \
                               FROM test2 \
                               WHERE test.i = test2.i and test1.i = test2.i and test2.b = 11 and test.b = 11 \
                                     and exists(select 1 from test3 where test3.i = test2.i) \
                             ) t2 \
                        JOIN (SELECT test3.t, test3.i \
                                FROM test3 \
                                WHERE test1.i = test3.i and test3.t = 'aa' \
                                      and test3.b = (select max(test2.b) from test2 where test3.t = test2.t) \
                             ) AS t3 \
                        ON t2.t = t3.t \
                    ) AS tl \
                 on t33.i = tl.t3_i";
        let expected_stmt = r#"SELECT * FROM "test1" JOIN "test" ON ("test1"."t" = "test"."t") JOIN
            (SELECT "test3"."i", "test3"."b" FROM "test3" WHERE ("test3"."t" = 'aa')) AS "t33" ON
            ("test1"."b" = "t33"."b") LEFT JOIN (SELECT "t3"."t", "t2"."b", "t2"."i" AS "t2_i", "t3"."i" AS "t3_i"
            FROM (SELECT "test2"."b", "test2"."t", "test2"."i" FROM "test2" WHERE (("test2"."b" = 11)
            AND EXISTS (SELECT 1 FROM "test3" WHERE ("test3"."i" = "test2"."i")))) AS "t2" JOIN
             (SELECT "test3"."t", "test3"."i" FROM "test3" WHERE (("test3"."t" = 'aa') AND
             ("test3"."b" = (SELECT max("test2"."b") FROM "test2" WHERE ("test3"."t" = "test2"."t"))))) AS "t3"
             ON ("t2"."t" = "t3"."t")) AS "tl" ON (("t33"."i" = "tl"."t3_i") AND (((("test"."i" = "tl"."t2_i")
             AND ("test1"."i" = "tl"."t2_i")) AND ("test"."b" = 11)) AND ("test1"."i" = "tl"."t3_i")))"#;
        test_it("test11", original_stmt, expected_stmt);
    }

    #[test]
    fn test12() {
        let original_stmt = "SELECT * FROM (test1 join test on test1.t = test.t) join \
            (SELECT test3.i, test3.b FROM test3 where test3.t = 'aa') t33 on test1.b = t33.b \
            left join LATERAL(SELECT t3.t, t2.b, t2.i as t2_i, t3.i as t3_i FROM \
            (select test2.b, test2.t, test2.i FROM test2 WHERE test2.b = 11 and \
            exists(select 1 from test3 where test3.i = test2.i)) t2 JOIN \
            LATERAL(SELECT test3.t, test3.i FROM test3 WHERE t2.i = test3.i and test3.t = 'aa' \
            and test3.b = (select max(test2.b) from test2 where test3.t = test2.t)) AS t3 \
            ON t2.t = t3.t) AS tl on t33.i = tl.t3_i;";
        let expect_stmt = r#"SELECT * FROM "test1" JOIN "test" ON ("test1"."t" = "test"."t") JOIN
            (SELECT "test3"."i", "test3"."b" FROM "test3" WHERE ("test3"."t" = 'aa')) AS "t33" ON
            ("test1"."b" = "t33"."b") LEFT JOIN
            (SELECT "t3"."t", "t2"."b", "t2"."i" AS "t2_i", "t3"."i" AS "t3_i" FROM
            (SELECT "test2"."b", "test2"."t", "test2"."i" FROM "test2" WHERE (("test2"."b" = 11)
            AND EXISTS (SELECT 1 FROM "test3" WHERE ("test3"."i" = "test2"."i")))) AS "t2" INNER JOIN
            (SELECT "test3"."t", "test3"."i" FROM "test3" WHERE (("test3"."t" = 'aa') AND
            ("test3"."b" = (SELECT max("test2"."b") FROM "test2" WHERE ("test3"."t" = "test2"."t"))))) AS "t3"
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
        let original_stmt =
            "select test1.b, T1.i from test1 join \
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
    LEFT JOIN LATERAL (
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
        let expect_stmt = r#"SELECT "dt"."c1", "datatypes"."test_integer", "dt"."test_integer2"
        FROM "spj", "datatypes" LEFT JOIN
        (SELECT "datatypes3"."test_integer2", count(*) AS "c1", "datatypes3"."test_smallint2" AS "test_smallint2"
        FROM "datatypes3" GROUP BY "datatypes3"."test_integer2", "datatypes3"."test_smallint2") AS "dt"
        ON (("datatypes"."test_integer" = "dt"."test_integer2") AND (("dt"."test_smallint2" = "spj"."qty")
        AND ("datatypes"."test_boolean" = TRUE)))
        ORDER BY 1, 2"#;
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
}
