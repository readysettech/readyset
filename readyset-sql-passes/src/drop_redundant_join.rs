use crate::rewrite_utils::{
    alias_for_expr, as_sub_query_with_alias_mut, deep_columns_visitor, deep_columns_visitor_mut,
    expect_field_as_expr, expect_sub_query_with_alias_mut, get_from_item_reference_name,
    is_column_eq_column, project_columns_if_not_exist_fix_duplicate_aliases, split_expr_mut,
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
    fn drop_redundant_join<U: UniqueColumnsSchema>(
        &mut self,
        unique_cols_schema: &U,
    ) -> ReadySetResult<&mut Self>;
}

impl DropRedundantSelfJoin for SelectStatement {
    fn drop_redundant_join<U: UniqueColumnsSchema>(
        &mut self,
        unique_cols_schema: &U,
    ) -> ReadySetResult<&mut Self> {
        drop_redundant_self_joins_main(self, unique_cols_schema)?;
        Ok(self)
    }
}

pub(crate) trait UniqueColumnsSchema {
    /// Single-column unique keys.  Convenience view over [`unique_keys_of`]
    /// filtered to keys of arity 1; callers that need composite-key coverage
    /// should query [`unique_keys_of`] directly.
    fn unique_columns_of(&self, rel: &Relation) -> Option<HashSet<Column>>;

    /// All unique keys of the relation, including composite keys.  Each key
    /// is a `Vec<Column>` — single-column keys are 1-element vecs.  The
    /// returned set may be filtered by the impl on schema-level facts
    /// (table-level `UNIQUE` requires all member columns proven `NOT NULL`;
    /// `PRIMARY KEY` is unconditional).
    ///
    /// The default impl derives single-column keys from `unique_columns_of`;
    /// impls with native composite-key storage (e.g. [`UniqueColumnsSchemaImpl`])
    /// should override to expose them.
    fn unique_keys_of(&self, rel: &Relation) -> Option<HashSet<Vec<Column>>> {
        self.unique_columns_of(rel)
            .map(|cols| cols.into_iter().map(|c| vec![c]).collect())
    }
}

pub(crate) struct UniqueColumnsSchemaImpl {
    unique_keys: HashMap<Relation, HashSet<Vec<Column>>>,
}

impl UniqueColumnsSchema for UniqueColumnsSchemaImpl {
    fn unique_columns_of(&self, rel: &Relation) -> Option<HashSet<Column>> {
        let single_col: HashSet<Column> = self
            .unique_keys
            .get(rel)?
            .iter()
            .filter(|key| key.len() == 1)
            .map(|key| key[0].clone())
            .collect();
        if single_col.is_empty() {
            None
        } else {
            Some(single_col)
        }
    }

    fn unique_keys_of(&self, rel: &Relation) -> Option<HashSet<Vec<Column>>> {
        self.unique_keys.get(rel).cloned()
    }
}

impl<C: BaseSchemasContext> From<C> for UniqueColumnsSchemaImpl {
    fn from(ctx: C) -> Self {
        let mut unique_keys: HashMap<Relation, HashSet<Vec<Column>>> = HashMap::new();

        for (rel, body) in ctx.base_schemas() {
            // Single walk over body.fields populates two sets:
            //   - `single_unique_cols`: columns individually unique-and-non-null via
            //     inline constraints (NotNull+Unique together, or PrimaryKey).  Each
            //     such column becomes a 1-element unique key.
            //   - `not_null_cols`: columns proven non-null by inline NotNull or
            //     PrimaryKey.  Used below to gate table-level UNIQUE acceptance
            //     (composite or single).
            let mut keys: HashSet<Vec<Column>> = HashSet::new();
            let mut not_null_cols = HashSet::new();
            for col_spec in &body.fields {
                let has_not_null = col_spec.constraints.contains(&ColumnConstraint::NotNull);
                let has_unique = col_spec.constraints.contains(&ColumnConstraint::Unique);
                let has_pk = col_spec.constraints.contains(&ColumnConstraint::PrimaryKey);
                if has_not_null || has_pk {
                    not_null_cols.insert(col_spec.column.clone());
                }
                if (has_not_null && has_unique) || has_pk {
                    keys.insert(vec![col_spec.column.clone()]);
                }
            }

            if let Some(table_keys) = &body.keys {
                // Table-level PRIMARY KEY (any arity) implies NOT NULL on every
                // member column by SQL semantics — unconditionally accepted.
                //
                // Table-level UNIQUE (any arity) is NOT a superkey unless every
                // member column is proven `NOT NULL`: SQL permits multiple rows
                // with NULL in a nullable UNIQUE column (NULLS DISTINCT default),
                // so the constraint does not uniquely identify rows.
                // `NULLS NOT DISTINCT` (Postgres 15+) limits the count of NULL
                // rows but does not change `NULL = NULL` evaluation in join ON
                // predicates — still UNKNOWN.  Gate table-level UNIQUE on
                // proven NOT NULL for every member column.
                for key in table_keys {
                    let cols: Vec<Column> =
                        key.get_columns().iter().map(|c| (*c).clone()).collect();
                    if cols.is_empty() {
                        continue;
                    }
                    let accepted = if key.is_primary_key() {
                        true
                    } else if key.is_unique_key() {
                        cols.iter().all(|c| not_null_cols.contains(c))
                    } else {
                        false
                    };
                    if accepted {
                        keys.insert(cols);
                    }
                }
            }

            if !keys.is_empty() {
                unique_keys.insert((*rel).clone(), keys);
            }
        }

        UniqueColumnsSchemaImpl { unique_keys }
    }
}

struct DropRedundantJoinContext<'a, U: UniqueColumnsSchema> {
    unique_cols_schema: &'a U,
}

impl<U: UniqueColumnsSchema> DropRedundantJoinContext<'_, U> {
    /// Returns `true` when the `names` set covers all columns of some
    /// unique key on `table` — i.e., one of the keys in
    /// `unique_keys_of(table)` is a subset of `names`.  Used to admit
    /// single- and multi-column equi-predicate joins (`a=a` or
    /// `a=a AND b=b ...`) against a `PRIMARY KEY` or NOT-NULL-gated
    /// `UNIQUE`.
    fn covers_unique_key(&self, table: &Relation, names: &HashSet<SqlIdentifier>) -> bool {
        let Some(keys) = self.unique_cols_schema.unique_keys_of(table) else {
            return false;
        };
        keys.iter().any(|key| {
            key.iter()
                .all(|k| k.table.as_ref() == Some(table) && names.contains(&k.name))
        })
    }
}

pub(crate) fn drop_redundant_self_joins_main<U: UniqueColumnsSchema>(
    stmt: &mut SelectStatement,
    unique_cols_schema: &U,
) -> ReadySetResult<()> {
    let ctx = DropRedundantJoinContext { unique_cols_schema };
    drop_redundant_self_joins(stmt, &ctx)?;
    Ok(())
}

/// Walk `on_expr` collecting `lhs_rel.col = rhs_rel.col` equality pairs
/// (in either orientation), descending through AND-conjunctions.  The
/// returned tuples are oriented `(lhs_col, rhs_col)`.  Returns `None`
/// when the ON contains any atom that isn't a table-qualified `col = col`
/// equality between `lhs_rel` and `rhs_rel` — in that case the join's
/// predicate isn't fully expressible as key coverage and removing the
/// join would change semantics.
fn collect_cross_eqs(
    on_expr: &Expr,
    lhs_rel: &Relation,
    rhs_rel: &Relation,
) -> Option<Vec<(Column, Column)>> {
    let mut pairs = Vec::new();
    let mut sink = Vec::new();
    let remainder = split_expr_mut(
        on_expr,
        &mut |atom| {
            let mut found: Option<(Column, Column)> = None;
            is_column_eq_column(atom, |l, r| {
                if is_column_of!(l, *lhs_rel) && is_column_of!(r, *rhs_rel) {
                    found = Some((l.clone(), r.clone()));
                    true
                } else if is_column_of!(l, *rhs_rel) && is_column_of!(r, *lhs_rel) {
                    found = Some((r.clone(), l.clone()));
                    true
                } else {
                    false
                }
            });
            if let Some(p) = found {
                pairs.push(p);
                true
            } else {
                false
            }
        },
        &mut sink,
    );
    if remainder.is_some() {
        return None;
    }
    Some(pairs)
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
fn lhs_is_subset_of_rhs<U: UniqueColumnsSchema>(
    base_stmt: &SelectStatement,
    lhs_rel: &Relation,
    rhs_rel: &Relation,
    pairs: &[(Column, Column)],
    ctx: &DropRedundantJoinContext<U>,
) -> ReadySetResult<bool> {
    if pairs.is_empty() {
        return Ok(false);
    }

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

    // The set of RHS columns in the equi-pairs must cover some unique key on
    // `rhs_base_table` — single-column or composite.  `covers_unique_key`
    // honors the NULLS-DISTINCT gate from `UniqueColumnsSchemaImpl::from`
    // (table-level UNIQUE requires every member NOT NULL; PRIMARY KEY is
    // unconditional).
    let rhs_names: HashSet<SqlIdentifier> = pairs.iter().map(|(_, r)| r.name.clone()).collect();
    if !ctx.covers_unique_key(rhs_base_table, &rhs_names) {
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
    let TableExpr {
        inner: TableExprInner::Table(lhs_base_table),
        ..
    } = lhs_first_table
    else {
        return Ok(false);
    };
    if lhs_base_table != rhs_base_table {
        return Ok(false);
    }
    // Extending the projection of a grouping subquery could change its cardinality;
    // we only support aggregate-free LHS subqueries here.
    if lhs_base_stmt.group_by.is_some() || lhs_base_stmt.having.is_some() {
        return Ok(false);
    }
    let lhs_first_ref = get_from_item_reference_name(lhs_first_table)?;
    // Every (lhs_col, rhs_col) pair must be a direct projection of the same
    // base column from the LHS subquery: the projection aliased as
    // `lhs_col.name` must resolve to `<lhs_first_ref>.rhs_col.name`.  When
    // this holds for all pairs, the LHS's keyset is a subset of the RHS's
    // keyset and the join is redundant.
    for (lhs_col, rhs_col) in pairs {
        let lhs_field = lhs_base_stmt.fields.iter().find_map(|fe| {
            let (f_expr, f_alias) = expect_field_as_expr(fe);
            if alias_for_expr(f_expr, f_alias).eq(&lhs_col.name) {
                Some(f_expr)
            } else {
                None
            }
        });
        let Some(Expr::Column(Column { name, table })) = lhs_field else {
            return Ok(false);
        };
        if name != &rhs_col.name || table.as_ref() != Some(&lhs_first_ref) {
            return Ok(false);
        }
    }
    Ok(true)
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
fn drop_redundant_self_joins<U: UniqueColumnsSchema>(
    stmt: &mut SelectStatement,
    ctx: &DropRedundantJoinContext<U>,
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

    let Some(pairs) = collect_cross_eqs(on_expr, &lhs_rel, &rhs_rel) else {
        return Ok(any_rewrite);
    };
    if pairs.is_empty() {
        return Ok(any_rewrite);
    }

    if !lhs_is_subset_of_rhs(stmt, &lhs_rel, &rhs_rel, &pairs, ctx)? {
        return Ok(any_rewrite);
    }

    // Remove both RHS and LHS from `stmt`. We will restore the LHS before exit.
    //
    // The join is removed BEFORE `collect_rhs_refs` runs.  Every column on
    // either side of the equi-pairs is already handled by `pairs`, so any
    // `rhs.col` reference still scattered through the rest of the stmt
    // (SELECT list, WHERE, etc.) gets collected and rebound to the LHS.
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
        let ctx = UniqueColumnsSchemaImpl::from(HashMap::from([(rel.clone(), body)]));
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

    /// Table-level UNIQUE on a column with inline NOT NULL: the column is a
    /// true superkey and must be recognized.  Covers DDL patterns like
    /// `username VARCHAR(100) NOT NULL, UNIQUE KEY u_username (username)`
    /// (common in Laravel-style migrations and named-constraint schemas).
    #[test]
    fn table_level_unique_on_inline_not_null_column_is_unique() {
        let rel: Relation = "t".into();
        let body = make_body(
            &[("t", "username", vec![ColumnConstraint::NotNull])],
            vec![TableKey::UniqueKey {
                constraint_name: None,
                constraint_timing: None,
                index_name: None,
                columns: vec![IndexKeyPart::Column(mk_col("t", "username"))],
                index_type: None,
                nulls_distinct: None,
            }],
        );
        let schema_impl = UniqueColumnsSchemaImpl::from(HashMap::from([(rel.clone(), body)]));
        let unique = schema_impl
            .unique_columns_of(&rel)
            .expect("table-level UNIQUE on inline-NOT-NULL column should be unique");
        assert!(unique.contains(&mk_col("t", "username")));
    }

    /// Table-level UNIQUE on a column without NOT NULL is NOT a superkey.
    /// SQL allows multiple NULL rows in a nullable UNIQUE column (NULLS
    /// DISTINCT default); the column does not uniquely identify rows, so
    /// treating it as a superkey would cause wrong-results in callers like
    /// `drop_redundant_join` and `is_upstream_item_safe_for_lateral_flatten`
    /// (post-flatten GROUP BY would collapse multiple NULL-key outer rows
    /// into one group).
    #[test]
    fn table_level_unique_on_nullable_column_is_not_unique() {
        let rel: Relation = "t".into();
        let body = make_body(
            &[("t", "filename", vec![])],
            vec![TableKey::UniqueKey {
                constraint_name: None,
                constraint_timing: None,
                index_name: None,
                columns: vec![IndexKeyPart::Column(mk_col("t", "filename"))],
                index_type: None,
                nulls_distinct: None,
            }],
        );
        let schema_impl = UniqueColumnsSchemaImpl::from(HashMap::from([(rel.clone(), body)]));
        let unique = schema_impl.unique_columns_of(&rel);
        assert!(
            unique.is_none() || !unique.as_ref().unwrap().contains(&mk_col("t", "filename")),
            "nullable table-level UNIQUE column should NOT be treated as a superkey"
        );
    }

    /// `NULLS NOT DISTINCT` (Postgres 15+) limits the count of NULL rows but
    /// does not change `NULL = NULL` evaluation in join ON predicates (still
    /// UNKNOWN per SQL three-valued logic).  The column can still hold a
    /// NULL value, so it is not a superkey for join-elimination purposes.
    #[test]
    fn table_level_unique_nulls_not_distinct_without_not_null_is_not_unique() {
        let rel: Relation = "t".into();
        let body = make_body(
            &[("t", "id", vec![])],
            vec![TableKey::UniqueKey {
                constraint_name: None,
                constraint_timing: None,
                index_name: None,
                columns: vec![IndexKeyPart::Column(mk_col("t", "id"))],
                index_type: None,
                nulls_distinct: Some(readyset_sql::ast::NullsDistinct::NotDistinct),
            }],
        );
        let schema_impl = UniqueColumnsSchemaImpl::from(HashMap::from([(rel.clone(), body)]));
        let unique = schema_impl.unique_columns_of(&rel);
        assert!(
            unique.is_none() || !unique.as_ref().unwrap().contains(&mk_col("t", "id")),
            "NULLS NOT DISTINCT without NOT NULL should NOT be treated as a superkey"
        );
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

    // ── DRJ-2 composite-key tests (T1 of REA-6651) ──

    /// Mock schema: `t` has a composite PRIMARY KEY on `(a, b)` — neither
    /// column individually unique.
    struct TestCompositeKeySchema;

    impl UniqueColumnsSchema for TestCompositeKeySchema {
        fn unique_columns_of(&self, _rel: &Relation) -> Option<HashSet<Column>> {
            // Composite key has no single-column unique columns.
            None
        }

        fn unique_keys_of(&self, rel: &Relation) -> Option<HashSet<Vec<Column>>> {
            if rel.name == "t" {
                Some(HashSet::from([vec![mk_col("t", "a"), mk_col("t", "b")]]))
            } else {
                None
            }
        }
    }

    fn did_eliminate_with<S: UniqueColumnsSchema>(sql: &str, schema: &S) -> bool {
        let mut stmt =
            parse_select_with_config(ParsingPreset::OnlySqlparser, Dialect::PostgreSQL, sql)
                .unwrap_or_else(|e| panic!("parse failed: {e}\n  sql: {sql}"));
        let join_count_before = stmt.join.len();
        drop_redundant_self_joins_main(&mut stmt, schema)
            .unwrap_or_else(|e| panic!("rewrite failed: {e}\n  sql: {sql}"));
        stmt.join.len() < join_count_before
    }

    /// AND-conjoined equi-predicates that cover every column of a composite
    /// PRIMARY KEY are enough to prove redundancy.  Pre-T1 the single-pair
    /// matcher rejected ANDs outright; post-T1 `collect_cross_eqs` admits
    /// them and `covers_unique_key` confirms the composite is fully pinned.
    #[test]
    fn composite_key_fully_covered_eliminates_join() {
        assert!(
            did_eliminate_with(
                r#"SELECT "sq"."a", "t"."x"
                   FROM (SELECT "t"."a", "t"."b" FROM "t" WHERE "t"."x" > 0) AS "sq"
                   LEFT JOIN "t" ON "sq"."a" = "t"."a" AND "sq"."b" = "t"."b""#,
                &TestCompositeKeySchema,
            ),
            "composite PK (a, b) fully pinned by AND-conjoined ON should eliminate the join"
        );
    }

    /// Pinning only one column of a composite PRIMARY KEY does NOT prove
    /// the join is redundant — the key isn't covered.  Stays as-is.
    #[test]
    fn composite_key_partially_covered_preserves_join() {
        assert!(
            !did_eliminate_with(
                r#"SELECT "sq"."a", "t"."x"
                   FROM (SELECT "t"."a", "t"."b" FROM "t" WHERE "t"."x" > 0) AS "sq"
                   LEFT JOIN "t" ON "sq"."a" = "t"."a""#,
                &TestCompositeKeySchema,
            ),
            "composite PK (a, b) only partially pinned should bail"
        );
    }

    /// One ON pair references a column outside the composite PK: the set
    /// of RHS columns does not cover the key, so bail.
    #[test]
    fn composite_key_with_extra_non_key_pair_preserves_join() {
        assert!(
            !did_eliminate_with(
                r#"SELECT "sq"."a", "t"."x"
                   FROM (SELECT "t"."a", "t"."x" FROM "t") AS "sq"
                   LEFT JOIN "t" ON "sq"."a" = "t"."a" AND "sq"."x" = "t"."x""#,
                &TestCompositeKeySchema,
            ),
            "ON includes a non-key column (x); pair set covers no unique key — bail"
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
