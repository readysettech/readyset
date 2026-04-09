use crate::rewrite_utils::{
    alias_for_expr, as_sub_query_with_alias_mut, deep_columns_visitor, deep_columns_visitor_mut,
    expect_field_as_expr, expect_sub_query_with_alias_mut, get_from_item_reference_name,
    is_column_eq_column, project_columns_if_not_exist_fix_duplicate_aliases,
};
use crate::{
    BaseSchemasContext, as_column, get_local_from_items_iter, get_local_from_items_iter_mut,
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
    fn drop_redundant_join<C: BaseSchemasContext>(&mut self, ctx: C) -> ReadySetResult<&mut Self>;
}

impl DropRedundantSelfJoin for SelectStatement {
    fn drop_redundant_join<C: BaseSchemasContext>(&mut self, ctx: C) -> ReadySetResult<&mut Self> {
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

impl<C: BaseSchemasContext> From<C> for UniqueColumnsSchemaImpl {
    fn from(ctx: C) -> Self {
        let mut unique_cols_schema = HashMap::new();

        for (rel, body) in ctx.base_schemas() {
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
                // Only single-column PK/UNIQUE keys guarantee individual column
                // uniqueness.  A composite key like UNIQUE(a, b) only guarantees
                // the *pair* is unique — neither `a` nor `b` alone is unique.
                unique_cols.extend(
                    keys.iter()
                        .filter_map(|key| {
                            if key.is_primary_key() || key.is_unique_key() {
                                let cols = key.get_columns();
                                if cols.len() == 1 { Some(cols) } else { None }
                            } else {
                                None
                            }
                        })
                        .flat_map(|cols| cols.into_iter().cloned())
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
    let Some(lhs_first_table) = lhs_base_stmt.tables.first() else {
        return Ok(false); // FROM-less inner subquery — cannot prove subset
    };
    if let TableExpr {
        inner: TableExprInner::Table(lhs_base_table),
        ..
    } = lhs_first_table
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
            && table.as_ref() == Some(&get_from_item_reference_name(lhs_first_table)?)
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
                // SAFETY: `collect_rhs_refs` and `rebind_rhs_refs` use the same
                // `deep_columns_visitor` traversal, so every column found here was also
                // found during collection. The `col_to_alias` map covers all collected
                // column names. TODO: refactor visitor callback to return `ReadySetResult`.
                unreachable!(
                    "RHS column '{}' not found in LHS projection (col_to_alias)",
                    column.name
                )
            }
        }
    })
}

/// Eliminate a very specific redundant self-join pattern.
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
///   • The first JOIN is a LEFT JOIN whose RHS is a base table `T1`.
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
        | JoinOperator::CrossJoin => {}
        JoinOperator::RightJoin | JoinOperator::RightOuterJoin | JoinOperator::StraightJoin => {
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
    //
    // IMPORTANT COUPLING: the join is removed BEFORE `collect_rhs_refs` runs. This is safe
    // because `is_column_eq_column` (above) guarantees the ON is a single `col = col`
    // equality — both columns are the join key, already handled by `lhs_col`/`rhs_col`.
    // If `is_column_eq_column` is ever relaxed to accept AND-conjunctions (multi-column
    // keys), additional RHS columns in the ON would be missed by `collect_rhs_refs` since
    // the join is already gone.  Any such extension MUST also update this section to walk
    // the removed ON before collection, or defer the removal until after collection.
    stmt.join.remove(0);
    let mut stmt_tables = mem::take(&mut stmt.tables);

    let mut rhs_refs = HashSet::new();
    collect_rhs_refs(stmt, &rhs_rel, &mut rhs_refs)?;

    // SAFETY: `lhs_is_subset_of_rhs` returned `Ok(true)` only if stmt_tables[0] is a
    // subquery whose first FROM item is the same base table as the RHS.
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

#[cfg(test)]
mod tests {
    use super::*;
    use readyset_sql::ast::{
        ColumnSpecification, CreateTableBody, IndexKeyPart, SqlType, TableKey,
    };

    /// Build a minimal `CreateTableBody` with the given column names, inline constraints,
    /// and table-level keys.
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

    #[test]
    fn single_column_pk_key_is_unique() {
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
        let ctx: UniqueColumnsSchemaImpl = UniqueColumnsSchemaImpl {
            unique_cols_schema: {
                let schema_impl =
                    UniqueColumnsSchemaImpl::from(HashMap::from([(rel.clone(), body)]));
                schema_impl.unique_cols_schema
            },
        };
        let unique = ctx
            .unique_columns_of(&rel)
            .expect("single-column PK table should have unique columns");
        assert!(unique.contains(&mk_col("t", "id")));
    }

    #[test]
    fn composite_pk_columns_are_not_individually_unique() {
        let rel: Relation = "t".into();
        let body = make_body(
            &[
                ("t", "id", vec![]),
                ("t", "tenant_id", vec![]),
                ("t", "name", vec![]),
            ],
            vec![TableKey::PrimaryKey {
                constraint_name: None,
                constraint_timing: None,
                index_name: None,
                columns: vec![
                    IndexKeyPart::Column(mk_col("t", "id")),
                    IndexKeyPart::Column(mk_col("t", "tenant_id")),
                ],
            }],
        );
        let schema_impl = UniqueColumnsSchemaImpl::from(HashMap::from([(rel.clone(), body)]));
        // Neither column individually should be in the unique set.
        let unique = schema_impl.unique_columns_of(&rel);
        assert!(
            unique.is_none() || !unique.as_ref().unwrap().contains(&mk_col("t", "id")),
            "composite PK column 'id' should NOT be individually unique"
        );
        assert!(
            unique.is_none() || !unique.as_ref().unwrap().contains(&mk_col("t", "tenant_id")),
            "composite PK column 'tenant_id' should NOT be individually unique"
        );
    }

    #[test]
    fn composite_unique_key_columns_are_not_individually_unique() {
        let rel: Relation = "t".into();
        let body = make_body(
            &[("t", "a", vec![]), ("t", "b", vec![]), ("t", "c", vec![])],
            vec![TableKey::UniqueKey {
                constraint_name: None,
                constraint_timing: None,
                index_name: None,
                columns: vec![
                    IndexKeyPart::Column(mk_col("t", "a")),
                    IndexKeyPart::Column(mk_col("t", "b")),
                ],
                index_type: None,
                nulls_distinct: None,
            }],
        );
        let schema_impl = UniqueColumnsSchemaImpl::from(HashMap::from([(rel.clone(), body)]));
        let unique = schema_impl.unique_columns_of(&rel);
        assert!(
            unique.is_none() || !unique.as_ref().unwrap().contains(&mk_col("t", "a")),
            "composite UNIQUE column 'a' should NOT be individually unique"
        );
    }

    #[test]
    fn inline_pk_constraint_is_unique() {
        let rel: Relation = "t".into();
        let body = make_body(
            &[
                ("t", "id", vec![ColumnConstraint::PrimaryKey]),
                ("t", "name", vec![]),
            ],
            vec![],
        );
        let schema_impl = UniqueColumnsSchemaImpl::from(HashMap::from([(rel.clone(), body)]));
        let unique = schema_impl
            .unique_columns_of(&rel)
            .expect("inline PK table should have unique columns");
        assert!(unique.contains(&mk_col("t", "id")));
        assert!(!unique.contains(&mk_col("t", "name")));
    }

    #[test]
    fn inline_unique_not_null_is_unique() {
        let rel: Relation = "t".into();
        let body = make_body(
            &[(
                "t",
                "email",
                vec![ColumnConstraint::NotNull, ColumnConstraint::Unique],
            )],
            vec![],
        );
        let schema_impl = UniqueColumnsSchemaImpl::from(HashMap::from([(rel.clone(), body)]));
        let unique = schema_impl
            .unique_columns_of(&rel)
            .expect("inline UNIQUE+NOT NULL table should have unique columns");
        assert!(unique.contains(&mk_col("t", "email")));
    }

    #[test]
    fn mixed_single_and_composite_keys() {
        let rel: Relation = "t".into();
        let body = make_body(
            &[
                ("t", "id", vec![]),
                ("t", "tenant_id", vec![]),
                ("t", "email", vec![]),
            ],
            vec![
                // Single-column PK — id IS individually unique
                TableKey::PrimaryKey {
                    constraint_name: None,
                    constraint_timing: None,
                    index_name: None,
                    columns: vec![IndexKeyPart::Column(mk_col("t", "id"))],
                },
                // Composite UNIQUE — neither column individually unique
                TableKey::UniqueKey {
                    constraint_name: None,
                    constraint_timing: None,
                    index_name: None,
                    columns: vec![
                        IndexKeyPart::Column(mk_col("t", "tenant_id")),
                        IndexKeyPart::Column(mk_col("t", "email")),
                    ],
                    index_type: None,
                    nulls_distinct: None,
                },
            ],
        );
        let schema_impl = UniqueColumnsSchemaImpl::from(HashMap::from([(rel.clone(), body)]));
        let unique = schema_impl
            .unique_columns_of(&rel)
            .expect("mixed-key table should have unique columns from single-column PK");
        assert!(
            unique.contains(&mk_col("t", "id")),
            "single-column PK should be unique"
        );
        assert!(
            !unique.contains(&mk_col("t", "tenant_id")),
            "composite UNIQUE column should NOT be individually unique"
        );
        assert!(
            !unique.contains(&mk_col("t", "email")),
            "composite UNIQUE column should NOT be individually unique"
        );
    }

    #[test]
    fn table_with_no_keys_returns_none() {
        let rel: Relation = "t".into();
        let body = make_body(&[("t", "a", vec![]), ("t", "b", vec![])], vec![]);
        let schema_impl = UniqueColumnsSchemaImpl::from(HashMap::from([(rel.clone(), body)]));
        assert!(
            schema_impl.unique_columns_of(&rel).is_none(),
            "table with no PK/UNIQUE should return None"
        );
    }

    // ── End-to-end rewrite tests ──

    use readyset_sql::Dialect;
    use readyset_sql_parsing::{ParsingPreset, parse_select_with_config};

    /// Mock schema: `t` has a single-column PK on `id`.
    struct TestUniqueSchema;

    impl UniqueColumnsSchema for TestUniqueSchema {
        fn unique_columns_of(&self, rel: &Relation) -> Option<HashSet<Column>> {
            if rel.name == "t" {
                Some(HashSet::from([mk_col("t", "id")]))
            } else {
                None
            }
        }
    }

    fn parse_and_rewrite(sql: &str) -> readyset_sql::ast::SelectStatement {
        let mut stmt =
            parse_select_with_config(ParsingPreset::OnlySqlparser, Dialect::PostgreSQL, sql)
                .unwrap_or_else(|e| panic!("parse failed: {e}\n  sql: {sql}"));
        drop_redundant_self_joins_main(&mut stmt, &TestUniqueSchema)
            .unwrap_or_else(|e| panic!("rewrite failed: {e}\n  sql: {sql}"));
        stmt
    }

    /// Returns true if the rewrite eliminated a join (join count decreased).
    fn did_eliminate(sql: &str) -> bool {
        let mut stmt =
            parse_select_with_config(ParsingPreset::OnlySqlparser, Dialect::PostgreSQL, sql)
                .unwrap_or_else(|e| panic!("parse failed: {e}\n  sql: {sql}"));
        let join_count_before = stmt.join.len();
        drop_redundant_self_joins_main(&mut stmt, &TestUniqueSchema)
            .unwrap_or_else(|e| panic!("rewrite failed: {e}\n  sql: {sql}"));
        stmt.join.len() < join_count_before
    }

    // ── R4: Positive end-to-end test — join eliminated ──

    #[test]
    fn self_join_on_unique_key_is_eliminated() {
        let stmt = parse_and_rewrite(
            r#"SELECT "sq"."id", "t"."name"
               FROM (SELECT "t"."id" FROM "t" WHERE "t"."active" = TRUE) AS "sq"
               LEFT JOIN "t" ON "sq"."id" = "t"."id""#,
        );
        assert!(
            stmt.join.is_empty(),
            "self-join should be eliminated (JOIN removed)"
        );
    }

    // ── R5: Negative tests — bail-out conditions ──

    #[test]
    fn bail_no_unique_key() {
        assert!(
            !did_eliminate(
                r#"SELECT "sq"."x" FROM (SELECT "u"."x" FROM "u") AS "sq"
                   LEFT JOIN "u" ON "sq"."x" = "u"."x""#
            ),
            "should bail: u.x is not a unique key"
        );
    }

    #[test]
    fn bail_group_by_in_inner() {
        assert!(
            !did_eliminate(
                r#"SELECT "sq"."id" FROM (SELECT "t"."id" FROM "t" GROUP BY "t"."id") AS "sq"
                   LEFT JOIN "t" ON "sq"."id" = "t"."id""#
            ),
            "should bail: inner LHS has GROUP BY"
        );
    }

    #[test]
    fn bail_rhs_is_subquery() {
        assert!(
            !did_eliminate(
                r#"SELECT "sq"."id" FROM (SELECT "t"."id" FROM "t") AS "sq"
                   LEFT JOIN (SELECT "t"."id", "t"."name" FROM "t") AS "t2"
                   ON "sq"."id" = "t2"."id""#
            ),
            "should bail: RHS is a subquery, not a base table"
        );
    }

    #[test]
    fn bail_non_equality_on() {
        assert!(
            !did_eliminate(
                r#"SELECT "sq"."id" FROM (SELECT "t"."id" FROM "t") AS "sq"
                   LEFT JOIN "t" ON "sq"."id" > "t"."id""#
            ),
            "should bail: ON is not a column equality"
        );
    }

    #[test]
    fn bail_different_base_table() {
        assert!(
            !did_eliminate(
                r#"SELECT "sq"."id" FROM (SELECT "u"."id" FROM "u") AS "sq"
                   LEFT JOIN "t" ON "sq"."id" = "t"."id""#
            ),
            "should bail: inner selects from u, RHS is t"
        );
    }

    #[test]
    fn bail_from_less_inner_subquery() {
        assert!(
            !did_eliminate(
                r#"SELECT "sq"."x" FROM (SELECT 1 AS "x") AS "sq"
                   LEFT JOIN "t" ON "sq"."x" = "t"."id""#
            ),
            "should bail: inner subquery has no FROM clause"
        );
    }
}
