use std::collections::HashMap;

use nom_sql::{
    BinaryOperator, Column, ColumnConstraint, CreateTableBody, Expr, LimitClause, Relation,
    SelectStatement, SqlQuery, TableExpr, TableKey,
};
use readyset_errors::{internal_err, ReadySetError, ReadySetResult};

pub trait OrderLimitRemoval: Sized {
    /// Remove any LIMIT and ORDER statement belonging to a query that is determined to return at
    /// most one row. Under this condition, the order and limit have no effect on the result.
    ///
    /// This past must be run after the expand_implied_tables() pass, because it requires that each
    /// column have an associated table name.
    fn order_limit_removal(
        self,
        base_schemas: &HashMap<&Relation, &CreateTableBody>,
    ) -> ReadySetResult<Self>;
}

fn is_unique_or_primary(
    col: &Column,
    base_schemas: &HashMap<&Relation, &CreateTableBody>,
    table_exprs: &[TableExpr],
) -> ReadySetResult<bool> {
    // This assumes that we will find exactly one table matching col.table and exactly one col
    // matching col.name. The pass also assumes that col will always have an associated table.
    // The last assumption will only hold true if this pass is run after the
    // expand_implied_tables() pass
    // This pass should also be run after the key_def_coalition pass, because this pass will only
    // search for primary keys in the table.keys filed (and not in column.constraints)
    let table = col.table.as_ref().ok_or_else(|| {
        internal_err!("All columns must have an associated table name at this point")
    })?;

    let table = match base_schemas.get(table) {
        None => {
            // Attempt to resolve alias. Most queries are not likely to do this, so we resolve
            // reactively
            let err = || internal_err!("Table name must match table in base schema");
            let resolved_table = table_exprs
                .iter()
                .find_map(|table_expr| {
                    if let Some(alias) = table_expr.alias.as_ref() {
                        if table.schema.is_none() && table.name == alias {
                            table_expr.inner.as_table()
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                })
                .ok_or_else(err)?;
            base_schemas.get(resolved_table).ok_or_else(err)?
        }
        Some(table) => table,
    };

    // check to see if column is in table.keys (and whether we have a compound primary key)
    let col_in_keys = if let Some(keys) = &table.keys {
        keys.iter().any(|key| match key {
            // TODO(DAN): Support compound keys
            TableKey::PrimaryKey { columns, .. } | TableKey::UniqueKey { columns, .. } => {
                columns.len() == 1 && columns.iter().any(|c| c.name == col.name)
            }
            _ => false,
        })
    } else {
        false
    };

    // check to see if column constraints specify unique or primary
    let col_spec = table
        .fields
        .iter()
        .find(|col_spec| col_spec.column.name == col.name)
        .ok_or_else(|| {
            ReadySetError::Internal(
                "Column name must match column in base schema table".to_string(),
            )
        })?;

    Ok(col_in_keys
        || col_spec
            .constraints
            .iter()
            .any(|c| matches!(c, ColumnConstraint::Unique)))
}

fn compares_unique_key_against_literal(
    expr: &Expr,
    base_schemas: &HashMap<&Relation, &CreateTableBody>,
    table_exprs: &[TableExpr],
) -> ReadySetResult<bool> {
    match expr {
        Expr::BinaryOp {
            lhs: box Expr::Literal(_),
            rhs: box Expr::Column(ref c),
            op: BinaryOperator::Equal | BinaryOperator::Is,
        }
        | Expr::BinaryOp {
            lhs: box Expr::Column(ref c),
            rhs: box Expr::Literal(_),
            op: BinaryOperator::Equal | BinaryOperator::Is,
        } => Ok(is_unique_or_primary(c, base_schemas, table_exprs)?),
        Expr::BinaryOp {
            op: BinaryOperator::And,
            ref lhs,
            ref rhs,
        } => Ok(
            compares_unique_key_against_literal(lhs, base_schemas, table_exprs)?
                || compares_unique_key_against_literal(rhs, base_schemas, table_exprs)?,
        ),
        // TODO(DAN): it may be possible to determine that a query will return a single (or no)
        // result if it has a nested select in the conditional
        _ => Ok(false),
    }
}

impl OrderLimitRemoval for SelectStatement {
    fn order_limit_removal(
        mut self,
        base_schemas: &HashMap<&Relation, &CreateTableBody>,
    ) -> ReadySetResult<Self> {
        let has_limit = matches!(
            self.limit_clause,
            LimitClause::LimitOffset { limit: Some(_), .. } | LimitClause::OffsetCommaLimit { .. }
        );
        // If the query uses an equality filter on a column that has a unique or primary key
        // index, remove order and limit
        if has_limit {
            if let Some(ref expr) = self.where_clause {
                if compares_unique_key_against_literal(expr, base_schemas, &self.tables)? {
                    self.limit_clause = LimitClause::default();
                    self.order = None;
                }
            }
        }
        Ok(self)
    }
}

impl OrderLimitRemoval for SqlQuery {
    fn order_limit_removal(
        self,
        base_schemas: &HashMap<&Relation, &CreateTableBody>,
    ) -> ReadySetResult<Self> {
        match self {
            SqlQuery::Select(stmt) => Ok(SqlQuery::Select(stmt.order_limit_removal(base_schemas)?)),
            _ => Ok(self),
        }
    }
}

#[cfg(test)]
mod tests {
    use nom_sql::{parse_query, ColumnSpecification, CreateTableBody, Dialect, Relation};

    use super::*;

    fn generate_base_schemas() -> HashMap<Relation, CreateTableBody> {
        let col1 = ColumnSpecification {
            column: Column {
                name: "c1".into(),
                table: Some("t".into()),
            },
            sql_type: nom_sql::SqlType::Bool,
            constraints: vec![],
            comment: None,
        };
        let col2 = ColumnSpecification {
            column: Column {
                name: "c2".into(),
                table: Some("t".into()),
            },
            sql_type: nom_sql::SqlType::Bool,
            constraints: vec![ColumnConstraint::Unique],
            comment: None,
        };
        let col3 = ColumnSpecification {
            column: Column {
                name: "c3".into(),
                table: Some("t".into()),
            },
            sql_type: nom_sql::SqlType::Bool,
            constraints: vec![],
            comment: None,
        };
        let col4 = ColumnSpecification {
            column: Column {
                name: "c4".into(),
                table: Some("t".into()),
            },
            sql_type: nom_sql::SqlType::Bool,
            constraints: vec![],
            comment: None,
        };

        let fields = vec![col1.clone(), col2, col3, col4.clone()];
        let keys = Some(vec![
            TableKey::UniqueKey {
                index_name: None,
                constraint_name: None,
                columns: vec![col4.column],
                index_type: None,
            },
            TableKey::PrimaryKey {
                index_name: None,
                constraint_name: None,
                columns: vec![col1.column],
            },
        ]);

        let mut base_schemas = HashMap::new();
        base_schemas.insert("t".into(), CreateTableBody { fields, keys });
        base_schemas
    }

    fn removes_limit_order(input: &str) {
        let input_query = parse_query(Dialect::MySQL, input).unwrap();
        let base_schemas = generate_base_schemas();
        let revised_query = input_query
            .order_limit_removal(&base_schemas.iter().map(|(k, v)| (k, v)).collect())
            .unwrap();
        match revised_query {
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
            _ => panic!("Invalid query returned: {:?}", revised_query),
        }
    }

    fn does_not_change_limit_order(input: &str) {
        let input_query = parse_query(Dialect::MySQL, input).unwrap();
        let base_schemas = generate_base_schemas();
        assert_eq!(
            input_query,
            input_query
                .clone()
                .order_limit_removal(&base_schemas.iter().map(|(k, v)| (k, v)).collect(),)
                .unwrap()
        );
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
            index_name: None,
            columns: vec![col1.clone(), col2.clone()],
        }]);
        base_schema.get_mut(&Relation::from("t")).unwrap().keys = keys;
        assert_eq!(
            input_query,
            input_query
                .clone()
                .order_limit_removal(&base_schema.iter().map(|(k, v)| (k, v)).collect())
                .unwrap()
        );
        // compound Unique
        let keys = Some(vec![TableKey::UniqueKey {
            constraint_name: None,
            index_name: None,
            columns: vec![col1, col2],
            index_type: None,
        }]);
        base_schema.get_mut(&Relation::from("t")).unwrap().keys = keys;
        assert_eq!(
            input_query,
            input_query
                .clone()
                .order_limit_removal(&base_schema.iter().map(|(k, v)| (k, v)).collect())
                .unwrap()
        );
        // compound unique but col is separately specified to be unique
        let revised_query = input_query2
            .order_limit_removal(&base_schema.iter().map(|(k, v)| (k, v)).collect())
            .unwrap();
        match revised_query {
            SqlQuery::Select(stmt) => {
                assert!(stmt.order.is_none());
                assert!(matches!(
                    stmt.limit_clause,
                    LimitClause::LimitOffset { limit: None, .. }
                ));
            }
            _ => panic!("Invalid query returned: {:?}", revised_query),
        }
    }
}
