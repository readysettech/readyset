use std::collections::{HashMap, HashSet};

use itertools::Itertools;
use readyset_errors::ReadySetResult;
use readyset_sql::ast::{
    BinaryOperator, Expr, LimitClause, Literal, Relation, SelectStatement, SqlIdentifier, SqlQuery,
    TableExpr,
};

use crate::BaseSchemasContext;
use crate::drop_redundant_join::{UniqueColumnsSchema, UniqueColumnsSchemaImpl};

pub trait OrderLimitRemoval: Sized {
    /// Remove any LIMIT and ORDER statement belonging to a query that is determined to return at
    /// most one row. Under this condition, the order and limit have no effect on the result.
    ///
    /// This past must be run after the expand_implied_tables() pass, because it requires that each
    /// column have an associated table name.
    fn order_limit_removal<C: BaseSchemasContext>(&mut self, ctx: C) -> ReadySetResult<&mut Self>;
}

/// Resolve a possibly-aliased table reference to its base relation by
/// scanning the statement's FROM list.  Returns the original ref when
/// it is already schema-qualified or already a base name; returns
/// `None` when no FROM item matches.
fn resolve_alias_to_base(rel: &Relation, table_exprs: &[TableExpr]) -> Option<Relation> {
    if rel.schema.is_some() {
        return Some(rel.clone());
    }
    table_exprs.iter().find_map(|te| {
        let alias = te.alias.as_ref()?;
        if alias == &rel.name {
            te.inner.as_table().cloned()
        } else {
            None
        }
    })
}

/// Walk `expr` collecting columns compared against non-NULL literals via
/// `col = lit` (in any orientation), descending through `AND`-conjunctions.
/// Columns are grouped by their owning base relation (aliases resolved).
/// Non-`col=lit` shapes, `col=NULL`, and disjunctions contribute nothing.
fn collect_eq_literal_columns(
    expr: &Expr,
    table_exprs: &[TableExpr],
    out: &mut HashMap<Relation, HashSet<SqlIdentifier>>,
) -> ReadySetResult<()> {
    // TODO(DAN): nested SELECTs in the conditional could also pin at-most-one
    // in some shapes; not currently traversed.
    let Expr::BinaryOp { lhs, op, rhs } = expr else {
        return Ok(());
    };
    match (lhs.as_ref(), op, rhs.as_ref()) {
        (Expr::Literal(lit), BinaryOperator::Equal, Expr::Column(c))
        | (Expr::Column(c), BinaryOperator::Equal, Expr::Literal(lit)) => {
            if !matches!(lit, Literal::Null)
                && let Some(table) = c.table.as_ref()
            {
                let resolved =
                    resolve_alias_to_base(table, table_exprs).unwrap_or_else(|| table.clone());
                out.entry(resolved).or_default().insert(c.name.clone());
            }
        }
        (lhs, BinaryOperator::And, rhs) => {
            collect_eq_literal_columns(lhs, table_exprs, out)?;
            collect_eq_literal_columns(rhs, table_exprs, out)?;
        }
        _ => {}
    }
    Ok(())
}

/// Returns `true` if the WHERE-clause's `col = lit` predicates pin every
/// column of some unique key on some FROM relation — guaranteeing the
/// query returns at most one row, making `ORDER BY` / `LIMIT` no-ops.
///
/// Backed by [`UniqueColumnsSchema`], which already gates table-level
/// `UNIQUE` on proven `NOT NULL` (avoiding the nullable-UNIQUE
/// silent-wrong bug the pre-T1 ad-hoc check had).  Composite keys are
/// satisfied when ALL member columns are pinned within a single AND
/// conjunction; the cross-key OR is implicit (covering any one key
/// suffices).
fn covers_unique_key<S: UniqueColumnsSchema>(
    expr: &Expr,
    schema: &S,
    table_exprs: &[TableExpr],
) -> ReadySetResult<bool> {
    let mut compared: HashMap<Relation, HashSet<SqlIdentifier>> = HashMap::new();
    collect_eq_literal_columns(expr, table_exprs, &mut compared)?;

    for (rel, names) in &compared {
        let Some(keys) = schema.unique_keys_of(rel) else {
            continue;
        };
        for key in &keys {
            if key.iter().all(|k| names.contains(&k.name)) {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

impl OrderLimitRemoval for SelectStatement {
    fn order_limit_removal<C: BaseSchemasContext>(&mut self, ctx: C) -> ReadySetResult<&mut Self> {
        let has_limit = matches!(
            self.limit_clause,
            LimitClause::LimitOffset { limit: Some(_), .. } | LimitClause::OffsetCommaLimit { .. }
        ) && matches!(
            self.limit_clause.offset(), // OFFSET > 0 implies (>= OFFSET) rows to return, hence do not drop
            None | Some(Literal::Integer(0)) | Some(Literal::UnsignedInteger(0))
        ) && !matches!(self.limit_clause.limit(), Some(lit) if lit.is_placeholder());

        // If the WHERE clause's col=literal predicates pin every column of some
        // unique key on the FROM relation, ORDER and LIMIT are no-ops.  The
        // schema-backed check honors the NULLS-DISTINCT rule (table-level
        // UNIQUE requires proven NOT NULL on every member column) and admits
        // composite keys when all member columns are pinned.
        if has_limit
            && let Some(ref expr) = self.where_clause
            && self.join.is_empty()
            && matches!(self.tables.iter().exactly_one(), Ok(tab) if tab.inner.as_table().is_some())
        {
            let schema = UniqueColumnsSchemaImpl::from(ctx);
            if covers_unique_key(expr, &schema, &self.tables)? {
                self.limit_clause = LimitClause::default();
                self.order = None;
            }
        }
        Ok(self)
    }
}

impl OrderLimitRemoval for SqlQuery {
    fn order_limit_removal<C: BaseSchemasContext>(&mut self, ctx: C) -> ReadySetResult<&mut Self> {
        if let SqlQuery::Select(stmt) = self {
            stmt.order_limit_removal(ctx)?;
        }
        Ok(self)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use readyset_sql::Dialect;
    use readyset_sql::ast::{
        Column, ColumnConstraint, ColumnSpecification, CreateTableBody, IndexKeyPart, Relation,
        SqlType, TableKey,
    };
    use readyset_sql_parsing::parse_query;

    use super::*;

    impl BaseSchemasContext for HashMap<Relation, CreateTableBody> {
        fn base_schemas(&self) -> Box<dyn Iterator<Item = (&Relation, &CreateTableBody)> + '_> {
            Box::new(self.iter())
        }

        fn base_schema(&self, relation: &Relation) -> Option<&CreateTableBody> {
            self.get(relation)
        }
    }

    fn generate_base_schemas() -> HashMap<Relation, CreateTableBody> {
        let col1 = ColumnSpecification {
            column: Column {
                name: "c1".into(),
                table: Some("t".into()),
            },
            sql_type: SqlType::Bool,
            generated: None,
            constraints: vec![],
            comment: None,
            invisible: false,
        };
        // c2 is `NOT NULL UNIQUE`: nullable `UNIQUE` does not guarantee
        // at-most-one because nullable-distinct semantics permit multiple
        // NULL rows (see [`UniqueColumnsSchemaImpl::from`] for the gate).
        let col2 = ColumnSpecification {
            column: Column {
                name: "c2".into(),
                table: Some("t".into()),
            },
            sql_type: SqlType::Bool,
            generated: None,
            constraints: vec![ColumnConstraint::NotNull, ColumnConstraint::Unique],
            comment: None,
            invisible: false,
        };
        let col3 = ColumnSpecification {
            column: Column {
                name: "c3".into(),
                table: Some("t".into()),
            },
            sql_type: SqlType::Bool,
            generated: None,
            constraints: vec![],
            comment: None,
            invisible: false,
        };
        // c4 is `NOT NULL`, then named in a table-level `UNIQUE` below.
        // Without NOT NULL the table-level UNIQUE would not pin at-most-one
        // (multiple NULL rows permitted under nullable-distinct semantics).
        let col4 = ColumnSpecification {
            column: Column {
                name: "c4".into(),
                table: Some("t".into()),
            },
            sql_type: SqlType::Bool,
            generated: None,
            constraints: vec![ColumnConstraint::NotNull],
            comment: None,
            invisible: false,
        };
        // c5 is a *nullable* inline UNIQUE — pre-T1 the ad-hoc check would
        // mistakenly accept it; post-fix it must be rejected because nullable
        // UNIQUE does not pin at-most-one.
        let col5 = ColumnSpecification {
            column: Column {
                name: "c5".into(),
                table: Some("t".into()),
            },
            sql_type: SqlType::Bool,
            generated: None,
            constraints: vec![ColumnConstraint::Unique],
            comment: None,
            invisible: false,
        };

        let fields = vec![col1.clone(), col2, col3, col4.clone(), col5];
        let keys = Some(vec![
            TableKey::UniqueKey {
                index_name: None,
                constraint_name: None,
                constraint_timing: None,
                columns: vec![IndexKeyPart::Column(col4.column)],
                index_type: None,
                nulls_distinct: None,
            },
            TableKey::PrimaryKey {
                index_name: None,
                constraint_name: None,
                constraint_timing: None,
                columns: vec![IndexKeyPart::Column(col1.column)],
            },
        ]);

        let mut base_schemas = HashMap::new();
        base_schemas.insert("t".into(), CreateTableBody { fields, keys });
        base_schemas
    }

    fn removes_limit_order(input: &str) {
        let mut q = parse_query(Dialect::MySQL, input).unwrap();
        let base_schemas = generate_base_schemas();
        q.order_limit_removal(&base_schemas).unwrap();
        match q {
            SqlQuery::Select(stmt) => {
                assert!(stmt.order.is_none());
                assert!(matches!(
                    stmt.limit_clause,
                    LimitClause::LimitOffset {
                        limit: None,
                        offset: None
                    }
                ));
            }
            _ => panic!("Invalid query returned: {q:?}"),
        }
    }

    fn does_not_change_limit_order(input: &str) {
        let input_query = parse_query(Dialect::MySQL, input).unwrap();
        let base_schemas = generate_base_schemas();
        let mut revised_query = input_query.clone();
        revised_query.order_limit_removal(&base_schemas).unwrap();
        assert_eq!(input_query, revised_query);
    }

    #[test]
    fn single_primary_key() {
        // condition on primary key
        removes_limit_order("SELECT t.c1 FROM t WHERE t.c1 = 1 ORDER BY c1 ASC LIMIT 10")
    }

    #[test]
    fn single_unique_key() {
        // condition on unique key
        removes_limit_order("SELECT t.c1 FROM t WHERE t.c2 = 1 ORDER BY c1 ASC LIMIT 10")
    }

    #[test]
    fn single_non_unique_key() {
        // condition on non-unique (or primary) key
        does_not_change_limit_order("SELECT t.c1 FROM t WHERE t.c3 = 1 ORDER BY c1 ASC LIMIT 10")
    }

    #[test]
    fn unique_and_non_unique_key1() {
        // condition on non-unique AND unique key
        removes_limit_order(
            "SELECT t.c1 FROM t WHERE t.c3 = 1 AND t.c2 = 1 ORDER BY c1 ASC LIMIT 10",
        )
    }

    #[test]
    fn unique_and_non_unique_key2() {
        // condition on primary AND non-unique key
        removes_limit_order(
            "SELECT t.c1 FROM t WHERE t.c1 = 1 AND t.c3 = 1 ORDER BY c1 ASC LIMIT 10",
        )
    }

    #[test]
    fn non_unique_and_non_unique_key() {
        // condition on non-unique AND (the same) non-unique key
        does_not_change_limit_order(
            "SELECT t.c1 FROM t WHERE t.c3 = 1 AND t.c3 = 1 ORDER BY c1 ASC LIMIT 10",
        )
    }

    #[test]
    fn primary_key_clause() {
        // condition on unique key, when the constraints field is empty
        removes_limit_order("SELECT t.c1 FROM t WHERE t.c4 = 1 ORDER BY c1 ASC LIMIT 10")
    }

    #[test]
    fn primary_key_with_table_alias() {
        // condition on primary key with table alias
        removes_limit_order("SELECT p.c1 FROM t as p where p.c1 = 1 ORDER BY c1 ASC LIMIT 10")
    }

    #[test]
    fn placeholder_limit_not_removed() {
        // A placeholder LIMIT (e.g. LIMIT ?) must not be stripped, because the placeholder could
        // be 0 at runtime (meaning "return nothing"), and removing it would cause a parameter count
        // mismatch between client and server.
        does_not_change_limit_order("SELECT t.c1 FROM t WHERE t.c1 = 1 ORDER BY c1 ASC LIMIT ?")
    }

    #[test]
    fn compound_keys() {
        // condition on primary/unique key with compound primary/unique key
        let mut base_schema = generate_base_schemas();
        let col1 = Column {
            name: "c1".into(),
            table: Some("t".into()),
        };
        let col2 = Column {
            name: "c2".into(),
            table: Some("t".into()),
        };
        let input_query = parse_query(
            Dialect::MySQL,
            "SELECT t.c1 FROM t WHERE t.c1 = 1 ORDER BY c1 ASC LIMIT 10",
        )
        .unwrap();

        let input_query2 = parse_query(
            Dialect::MySQL,
            "SELECT t.c1 FROM t WHERE t.c2 = 1 ORDER BY c1 ASC LIMIT 10",
        )
        .unwrap();
        // compound Primary
        let keys = Some(vec![TableKey::PrimaryKey {
            constraint_name: None,
            constraint_timing: None,
            index_name: None,
            columns: vec![
                IndexKeyPart::Column(col1.clone()),
                IndexKeyPart::Column(col2.clone()),
            ],
        }]);
        base_schema.get_mut(&Relation::from("t")).unwrap().keys = keys;
        let mut revised_query = input_query.clone();
        revised_query.order_limit_removal(&base_schema).unwrap();
        assert_eq!(input_query, revised_query);
        // compound Unique
        let keys = Some(vec![TableKey::UniqueKey {
            constraint_name: None,
            constraint_timing: None,
            index_name: None,
            columns: vec![IndexKeyPart::Column(col1), IndexKeyPart::Column(col2)],
            index_type: None,
            nulls_distinct: None,
        }]);
        base_schema.get_mut(&Relation::from("t")).unwrap().keys = keys;
        let mut revised_query = input_query.clone();
        revised_query.order_limit_removal(&base_schema).unwrap();
        assert_eq!(input_query, revised_query);
        // compound unique but col is separately specified to be unique
        let mut revised_query = input_query2.clone();
        revised_query.order_limit_removal(&base_schema).unwrap();
        match revised_query {
            SqlQuery::Select(stmt) => {
                assert!(stmt.order.is_none());
                assert!(matches!(
                    stmt.limit_clause,
                    LimitClause::LimitOffset { limit: None, .. }
                ));
            }
            _ => panic!("Invalid query returned: {revised_query:?}"),
        }
    }

    /// Nullable inline `UNIQUE` does NOT pin at-most-one — SQL's
    /// nullable-distinct semantics permit multiple NULL rows in a UNIQUE
    /// column.  Pre-T1 the ad-hoc `is_unique_or_primary` accepted any
    /// inline UNIQUE without the NOT-NULL gate; OLR-2 fix routes the check
    /// through `UniqueColumnsSchema`, which correctly rejects this shape.
    /// The fixture's `c5` is nullable inline UNIQUE; filtering on it must
    /// leave LIMIT/ORDER intact.
    #[test]
    fn nullable_inline_unique_does_not_remove_limit() {
        does_not_change_limit_order("SELECT t.c1 FROM t WHERE t.c5 = 1 ORDER BY c1 ASC LIMIT 10");
    }

    /// All columns of a composite PRIMARY KEY pinned via AND-conjoined
    /// equality predicates → at-most-one row, LIMIT/ORDER removable.
    /// Pre-T1 the ad-hoc check `columns.len() == 1` filter rejected this
    /// outright (TODO(DAN): Support compound keys); post-T1 the catalog
    /// exposes composite keys and `covers_unique_key` accepts them when
    /// fully pinned.
    #[test]
    fn composite_pk_fully_covered_removes_limit() {
        let mut base_schema = generate_base_schemas();
        let col1 = Column {
            name: "c1".into(),
            table: Some("t".into()),
        };
        let col2 = Column {
            name: "c2".into(),
            table: Some("t".into()),
        };
        let keys = Some(vec![TableKey::PrimaryKey {
            constraint_name: None,
            constraint_timing: None,
            index_name: None,
            columns: vec![IndexKeyPart::Column(col1), IndexKeyPart::Column(col2)],
        }]);
        base_schema.get_mut(&Relation::from("t")).unwrap().keys = keys;
        let mut q = parse_query(
            Dialect::MySQL,
            "SELECT t.c1 FROM t WHERE t.c1 = 1 AND t.c2 = 2 ORDER BY c1 ASC LIMIT 10",
        )
        .unwrap();
        q.order_limit_removal(&base_schema).unwrap();
        match q {
            SqlQuery::Select(stmt) => {
                assert!(stmt.order.is_none());
                assert!(matches!(
                    stmt.limit_clause,
                    LimitClause::LimitOffset { limit: None, .. }
                ));
            }
            _ => panic!("Unexpected query shape"),
        }
    }

    /// Composite UNIQUE with at least one nullable member column does NOT
    /// pin at-most-one — same NULLS-DISTINCT rationale as inline UNIQUE.
    /// In this fixture, `c3` is nullable; the composite UNIQUE `(c3, c4)`
    /// must be rejected even when both columns are pinned.
    #[test]
    fn composite_unique_with_nullable_member_does_not_remove_limit() {
        let mut base_schema = generate_base_schemas();
        let col3 = Column {
            name: "c3".into(),
            table: Some("t".into()),
        };
        let col4 = Column {
            name: "c4".into(),
            table: Some("t".into()),
        };
        let keys = Some(vec![TableKey::UniqueKey {
            constraint_name: None,
            constraint_timing: None,
            index_name: None,
            columns: vec![IndexKeyPart::Column(col3), IndexKeyPart::Column(col4)],
            index_type: None,
            nulls_distinct: None,
        }]);
        base_schema.get_mut(&Relation::from("t")).unwrap().keys = keys;
        let input = parse_query(
            Dialect::MySQL,
            "SELECT t.c1 FROM t WHERE t.c3 = 1 AND t.c4 = 2 ORDER BY c1 ASC LIMIT 10",
        )
        .unwrap();
        let mut revised = input.clone();
        revised.order_limit_removal(&base_schema).unwrap();
        assert_eq!(
            input, revised,
            "composite UNIQUE with nullable c3 must be rejected"
        );
    }
}
