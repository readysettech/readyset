pub(crate) use crate::rewrite_utils::{
    alias_for_expr, as_sub_query_with_alias_mut, deep_columns_visitor, deep_columns_visitor_mut,
    expect_field_as_expr, expect_sub_query_with_alias_mut, get_from_item_reference_name,
    is_column_eq_column, project_columns_if_not_exist_fix_duplicate_aliases,
};
use crate::{
    RewriteContext, as_column, get_local_from_items_iter, get_local_from_items_iter_mut,
    is_column_of,
};
use itertools::{Either, Itertools};
use readyset_errors::ReadySetResult;
use readyset_sql::ast::{
    Column, ColumnConstraint, Expr, JoinConstraint, JoinOperator, JoinRightSide, Relation,
    SelectStatement, SqlIdentifier, TableExpr, TableExprInner,
};
use std::collections::{HashMap, HashSet};
use std::{iter, mem};

pub trait DropRedundantSelfJoin: Sized {
    fn drop_redundant_join<C: RewriteContext>(&mut self, ctx: C) -> ReadySetResult<&mut Self>;
}

impl DropRedundantSelfJoin for SelectStatement {
    fn drop_redundant_join<C: RewriteContext>(&mut self, ctx: C) -> ReadySetResult<&mut Self> {
        let unique_cols_schema = UniqueColumnsSchemaImpl::from(ctx);
        drop_redundant_self_joins_main(self, &unique_cols_schema)?;
        Ok(self)
    }
}

pub trait UniqueColumnsSchema {
    fn unique_columns_of(&self, rel: &Relation) -> Option<HashSet<Column>>;
}

struct UniqueColumnsSchemaImpl {
    unique_cols_schema: HashMap<Relation, HashSet<Column>>,
}

impl UniqueColumnsSchema for UniqueColumnsSchemaImpl {
    fn unique_columns_of(&self, rel: &Relation) -> Option<HashSet<Column>> {
        self.unique_cols_schema.get(rel).cloned()
    }
}

impl<C: RewriteContext> From<C> for UniqueColumnsSchemaImpl {
    fn from(ctx: C) -> Self {
        let mut unique_cols_schema = HashMap::new();

        for (rel, body) in ctx.base_schemas().iter() {
            let mut unique_cols = body
                .fields
                .iter()
                .filter_map(|col_spec| {
                    if (col_spec.constraints.contains(&ColumnConstraint::NotNull)
                        && col_spec.constraints.contains(&ColumnConstraint::Unique))
                        || col_spec.constraints.contains(&ColumnConstraint::PrimaryKey)
                    {
                        Some(col_spec.column.clone())
                    } else {
                        None
                    }
                })
                .collect::<HashSet<_>>();

            if let Some(keys) = &body.keys {
                unique_cols.extend(
                    keys.iter()
                        .filter(|key| key.is_primary_key())
                        .flat_map(|key| key.get_columns().into_iter().cloned())
                        .collect::<HashSet<_>>(),
                );
            }

            if !unique_cols.is_empty() {
                unique_cols_schema.insert((*rel).clone(), unique_cols);
            }
        }

        UniqueColumnsSchemaImpl { unique_cols_schema }
    }
}

struct DropRedundantJoinContext<'a> {
    unique_cols_schema: &'a dyn UniqueColumnsSchema,
}

impl<'a> DropRedundantJoinContext<'a> {
    fn is_unique_column(&self, table: &Relation, name: &SqlIdentifier) -> bool {
        if let Some(unique_cols) = self.unique_cols_schema.unique_columns_of(table)
            && unique_cols.contains(&Column {
                name: name.clone(),
                table: Some(table.clone()),
            })
        {
            true
        } else {
            false
        }
    }
}

pub(crate) fn drop_redundant_self_joins_main(
    stmt: &mut SelectStatement,
    unique_cols_schema: &dyn UniqueColumnsSchema,
) -> ReadySetResult<()> {
    let ctx = DropRedundantJoinContext { unique_cols_schema };
    drop_redundant_self_joins(stmt, &ctx)?;
    Ok(())
}

/// Prove that the LHS key is a subset of the RHS key domain and that joining on
/// `lhs_col = rhs_col` is non-filtering and non-failable.
///
/// Concretely, in the outer `base_stmt` we expect:
///   • LHS to be a subquery alias whose first FROM item is the same base table as `rhs_rel`,
///   • `lhs_col` to be a direct projection of the base key column `rhs_col` from that table
///     (matched via `alias_for_expr` so explicit and synthetic aliases both work),
///   • no GROUP BY / HAVING in the inner LHS subquery at this stage.
///
/// We rely on schema-level guarantees that `rhs_col` is a primary/unique key.
///
/// If all checks pass, we know that every LHS key value comes from the same base key column
/// as the RHS, and the LEFT join on that key cannot remove LHS rows.
fn lhs_is_subset_of_rhs(
    base_stmt: &SelectStatement,
    lhs_rel: &Relation,
    lhs_col: &Column,
    rhs_rel: &Relation,
    rhs_col: &Column,
    ctx: &DropRedundantJoinContext,
) -> ReadySetResult<bool> {
    let mut maybe_lhs_table_expr = None;
    let mut maybe_rhs_table_expr = None;
    for t in get_local_from_items_iter!(base_stmt) {
        let rel = get_from_item_reference_name(t)?;
        if lhs_rel.eq(&rel) {
            maybe_lhs_table_expr = Some(t);
        } else if rhs_rel.eq(&rel) {
            maybe_rhs_table_expr = Some(t);
        }
    }

    let Some(TableExpr {
        inner: TableExprInner::Table(rhs_base_table),
        ..
    }) = maybe_rhs_table_expr
    else {
        return Ok(false);
    };

    if !ctx.is_unique_column(rhs_base_table, &rhs_col.name) {
        return Ok(false);
    }

    let Some(TableExpr {
        inner: TableExprInner::Subquery(lhs_subquery),
        ..
    }) = maybe_lhs_table_expr
    else {
        return Ok(false);
    };

    let lhs_base_stmt = lhs_subquery.as_ref();
    if let TableExpr {
        inner: TableExprInner::Table(lhs_base_table),
        ..
    } = &lhs_base_stmt.tables[0]
        && lhs_base_table.eq(rhs_base_table)
    {
        // Extending the projection of a grouping subquery could change its cardinality;
        // we only support aggregate-free LHS subqueries here.
        if lhs_base_stmt.group_by.is_some() || lhs_base_stmt.having.is_some() {
            return Ok(false);
        }
        let lhs_field = lhs_base_stmt.fields.iter().find_map(|fe| {
            let (f_expr, f_alias) = expect_field_as_expr(fe);
            if alias_for_expr(f_expr, f_alias).eq(&lhs_col.name) {
                Some(f_expr)
            } else {
                None
            }
        });
        if let Some(Expr::Column(Column { name, table })) = lhs_field
            && name.eq(&rhs_col.name)
            && table.as_ref() == Some(&get_from_item_reference_name(&lhs_base_stmt.tables[0])?)
        {
            return Ok(true);
        }
    }

    // Any other shape (different base table, non-column projection, mismatched names, etc.)
    // is considered unsafe for this very targeted optimization.
    Ok(false)
}

/// Collects the *names* of all columns that reference `rhs_rel` anywhere in `stmt`,
/// descending into all non-shadowing subqueries.
///
/// This is a pure analysis pass:
///   • It walks every expression in the SELECT, WHERE, GROUP BY, HAVING, ORDER BY, JOIN ON,
///     and in nested subqueries, *except* for subqueries that `shadows_rhs_alias` reports.
///   • For each `Expr::Column` bound to `rhs_rel`, we record its column name in `rhs_refs`.
///
/// The resulting set of names is later used to extend the inner LHS subquery so that it
/// can provide values for every RHS column that the outer query tree depends on.
fn collect_rhs_refs(
    stmt: &SelectStatement,
    rhs_rel: &Relation,
    rhs_refs: &mut HashSet<SqlIdentifier>,
) -> ReadySetResult<()> {
    deep_columns_visitor(stmt, rhs_rel, &mut |expr| {
        let column = as_column!(expr);
        if is_column_of!(column, *rhs_rel) {
            rhs_refs.insert(column.name.clone());
        }
    })
}

/// Rebind all references to `rhs_rel.col` to `lhs_rel.<alias>` throughout `stmt`,
/// for every column name present in `col_to_alias`, descending into all non-shadowing
/// subqueries.
///
/// Preconditions:
///   • `lhs_is_subset_of_rhs` has validated that the LHS key is a projected base key,
///   • the inner LHS subquery has been extended to project all columns in `col_to_alias`,
///   • `col_to_alias` maps original RHS column names to their projected LHS aliases.
///
/// Scope safety:
///   • We explicitly skip any subquery where `shadows_rhs_alias` is true, since that
///     means a new local alias is reusing the same `rhs_rel` identity.
///   • Within non-shadowing scopes, any `Expr::Column` whose `table` matches `rhs_rel`
///     is guaranteed to refer to the outer RHS alias and can be safely rewritten.
///
/// This is the second half of the redundant-join elimination: once all references are
/// rebound, the outer join on `rhs_rel` can be dropped.
fn rebind_rhs_refs(
    stmt: &mut SelectStatement,
    rhs_rel: &Relation,
    col_to_alias: &HashMap<SqlIdentifier, SqlIdentifier>,
    lhs_rel: &Relation,
) -> ReadySetResult<()> {
    deep_columns_visitor_mut(stmt, rhs_rel, &mut |expr| {
        let column = as_column!(expr);
        if is_column_of!(column, *rhs_rel) {
            if let Some(alias) = col_to_alias.get(&column.name) {
                column.name = alias.clone();
                column.table = Some(lhs_rel.clone());
            } else {
                unreachable!(
                    "Expected every referenced RHS column to be projected into the LHS subquery before rebinding"
                )
            }
        }
    })
}

/// Eliminate a very specific redundant LEFT self-join pattern.
///
/// Targeted shape (outer-level only):
///
///   SELECT ...
///   FROM (
///       SELECT T1.id, ...
///       FROM T1
///       WHERE ...
///   ) AS LHS
///   LEFT JOIN T1 ON LHS.id = T1.id
///   [ ... rest of query ... ]
///
/// Under the following conditions:
///   • There is exactly one FROM item and at least one JOIN in the outer statement.
///   • The first JOIN is a JOIN whose RHS is a base table `T1`.
///   • The join condition is a single, pure equality `LHS.<key> = T1.<key>` between
///     different relations (`is_column_eq_column` enforces this).
///   • `lhs_is_subset_of_rhs` proves that `LHS.<key>` is a direct projection of the
///     base key column from `T1` in the inner subquery (no GROUP BY / HAVING there).
///   • Schema metadata reports the RHS key column is PK or (UNIQUE + NOT NULL).
///
/// Rewrite strategy:
///   1. Temporarily remove the JOIN and the LHS table list from `stmt`.
///   2. `collect_rhs_refs` discovers every column of `T1` referenced anywhere in the
///      outer query tree (including correlated subqueries), skipping shadowing scopes.
///   3. Extend the inner LHS subquery to project those base columns from `T1`, and
///      record the aliases chosen for each projected column.
///   4. `rebind_rhs_refs` rewrites all references to `T1.col` to use `LHS.<alias>` in
///      all non-shadowing scopes.
///   5. Restore the (now-extended) LHS as the only FROM item; the redundant LEFT JOIN
///      on `T1` is gone.
///
/// Under the assumption that the join key is truly unique and NOT NULL, this preserves
/// both row cardinality and all observable column values, while removing an unnecessary
/// self-join and its associated scan.
///
/// The function recurses into nested subqueries first to handle this pattern bottom-up.
fn drop_redundant_self_joins(
    stmt: &mut SelectStatement,
    ctx: &DropRedundantJoinContext,
) -> ReadySetResult<bool> {
    let mut any_rewrite = false;

    for t in get_local_from_items_iter_mut!(stmt) {
        if let Some((inner_stmt, _alias)) = as_sub_query_with_alias_mut(t)
            && drop_redundant_self_joins(inner_stmt, ctx)?
        {
            any_rewrite = true;
        }
    }

    if stmt.tables.len() != 1 || stmt.join.is_empty() {
        return Ok(any_rewrite);
    }

    let join_clause = &stmt.join[0];
    match join_clause.operator {
        JoinOperator::Join
        | JoinOperator::LeftJoin
        | JoinOperator::LeftOuterJoin
        | JoinOperator::InnerJoin
        | JoinOperator::CrossJoin
        | JoinOperator::StraightJoin => {}
        JoinOperator::RightJoin | JoinOperator::RightOuterJoin => {
            return Ok(any_rewrite);
        }
    }

    let JoinRightSide::Table(rhs_base_table) = &join_clause.right else {
        return Ok(any_rewrite);
    };

    let lhs_rel = get_from_item_reference_name(&stmt.tables[0])?;
    let rhs_rel = get_from_item_reference_name(rhs_base_table)?;

    let JoinConstraint::On(on_expr) = &join_clause.constraint else {
        return Ok(any_rewrite);
    };

    let mut cross_eq = None;
    if !is_column_eq_column(on_expr, |lhs_col, rhs_col| {
        if is_column_of!(lhs_col, lhs_rel) && is_column_of!(rhs_col, rhs_rel) {
            cross_eq = Some((lhs_col.clone(), rhs_col.clone()));
            return true;
        }
        if is_column_of!(lhs_col, rhs_rel) && is_column_of!(rhs_col, lhs_rel) {
            cross_eq = Some((rhs_col.clone(), lhs_col.clone()));
            return true;
        }
        false
    }) {
        return Ok(any_rewrite);
    }

    let Some((lhs_col, rhs_col)) = &cross_eq else {
        return Ok(any_rewrite);
    };

    if !lhs_is_subset_of_rhs(stmt, &lhs_rel, lhs_col, &rhs_rel, rhs_col, ctx)? {
        return Ok(any_rewrite);
    }

    // Remove both RHS and LHS from `stmt`. We will restore the LHS before exit.
    stmt.join.remove(0);
    let mut stmt_tables = mem::take(&mut stmt.tables);

    let mut rhs_refs = HashSet::new();
    collect_rhs_refs(stmt, &rhs_rel, &mut rhs_refs)?;

    let (lhs_stmt, _) = expect_sub_query_with_alias_mut(&mut stmt_tables[0]);
    let inner_lhs_rel = get_from_item_reference_name(&lhs_stmt.tables[0])?;

    let proj_exprs = rhs_refs
        .into_iter()
        .map(|c_name| {
            Expr::Column(Column {
                name: c_name,
                table: Some(inner_lhs_rel.clone()),
            })
        })
        .sorted()
        .collect::<Vec<_>>();

    let proj_aliases = project_columns_if_not_exist_fix_duplicate_aliases(lhs_stmt, &proj_exprs);

    let col_to_alias = proj_exprs
        .into_iter()
        .zip(proj_aliases)
        .map(|(expr, (_, alias))| (as_column!(expr).name, alias))
        .collect::<HashMap<_, _>>();

    rebind_rhs_refs(stmt, &rhs_rel, &col_to_alias, &lhs_rel)?;

    // Restore the LHS.
    stmt.tables = stmt_tables;

    any_rewrite = true;

    Ok(any_rewrite)
}
