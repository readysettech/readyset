use std::collections::HashMap;

use nom_sql::{
    BinaryOperator, Column, ColumnConstraint, CreateTableStatement, Expression, SqlIdentifier,
    SqlQuery, Table, TableKey,
};
use noria_errors::{ReadySetError, ReadySetResult};

pub trait OrderLimitRemoval: Sized {
    /// Remove any LIMIT and ORDER statement belonging to a query that is determined to return at
    /// most one row. Under this condition, the order and limit have no effect on the result.
    ///
    /// This past must be run after the expand_implied_tables() pass, because it requires that each
    /// column have an associated table name.
    fn order_limit_removal(
        self,
        base_schemas: &HashMap<SqlIdentifier, CreateTableStatement>,
    ) -> ReadySetResult<Self>;
}

fn is_unique_or_primary(
    col: &Column,
    base_schemas: &HashMap<SqlIdentifier, CreateTableStatement>,
    tables: &[Table],
) -> ReadySetResult<bool> {
    // This assumes that we will find exactly one table matching col.table and exactly one col
    // matching col.name. The pass also assumes that col will always have an associated table.
    // The last assumption will only hold true if this pass is run after the
    // expand_implied_tables() pass
    // This pass should also be run after the key_def_coalition pass, beause this pass will only
    // search for primary keys in the table.keys filed (and not in column.constraints)
    let table_name = col.table.as_ref().ok_or_else(|| {
        ReadySetError::Internal(
            "All columns must have an associated table name at this point".to_string(),
        )
    })?;

    let table = match base_schemas.get(table_name) {
        None => {
            // Attempt to resolve alias. Most queries are not likely to do this, so we resolve
            // reactively
            let err_str = "Table name must match table in base schema".to_string();
            let table_name = tables
                .iter()
                .find_map(|table| {
                    if let Some(_alias) = table.alias.as_ref() {
                        if table_name == _alias {
                            Some(&table.name)
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                })
                .ok_or_else(|| ReadySetError::Internal(err_str.clone()))?;
            base_schemas
                .get(table_name)
                .ok_or(ReadySetError::Internal(err_str))?
        }
        Some(table) => table,
    };

    // check to see if column is in table.keys (and whether we have a compound primary key)
    let col_in_keys = if let Some(ref keys) = table.keys {
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
    expr: &Expression,
    base_schemas: &HashMap<SqlIdentifier, CreateTableStatement>,
    tables: &[Table],
) -> ReadySetResult<bool> {
    match expr {
        Expression::BinaryOp {
            lhs: box Expression::Literal(_),
            rhs: box Expression::Column(ref c),
            op: BinaryOperator::Equal | BinaryOperator::Is,
        }
        | Expression::BinaryOp {
            lhs: box Expression::Column(ref c),
            rhs: box Expression::Literal(_),
            op: BinaryOperator::Equal | BinaryOperator::Is,
        } => Ok(is_unique_or_primary(c, base_schemas, tables)?),
        Expression::BinaryOp {
            op: BinaryOperator::And,
            ref lhs,
            ref rhs,
        } => Ok(
            compares_unique_key_against_literal(lhs, base_schemas, tables)?
                || compares_unique_key_against_literal(rhs, base_schemas, tables)?,
        ),
        // TODO(DAN): it may be possible to determine that a query will return a single (or no)
        // resut if it has a nested select in the conditional
        _ => Ok(false),
    }
}

impl OrderLimitRemoval for SqlQuery {
    fn order_limit_removal(
        mut self,
        base_schemas: &HashMap<SqlIdentifier, CreateTableStatement>,
    ) -> ReadySetResult<Self> {
        // If the query uses an equality filter on a column that has a unique or primary key
        // index, remove order and limit
        if let SqlQuery::Select(stmt) = &mut self {
            if stmt.limit.is_none() {
                return Ok(self);
            }
            if let Some(ref expr) = stmt.where_clause {
                if compares_unique_key_against_literal(expr, base_schemas, &stmt.tables)? {
                    stmt.limit = None;
                    stmt.order = None;
                    return Ok(self);
                }
            }
        }
        Ok(self)
    }
}

#[cfg(test)]
mod tests {
    use nom_sql::{parse_query, ColumnSpecification, Dialect};

    use super::*;

    fn generate_base_schemas() -> HashMap<SqlIdentifier, CreateTableStatement> {
        let table = Table {
            name: "t".into(),
            alias: None,
            schema: None,
        };

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
                name: None,
                columns: vec![col4.column],
                index_type: None,
            },
            TableKey::PrimaryKey {
                name: None,
                columns: vec![col1.column],
            },
        ]);
        let if_not_exists = false;

        let mut base_schemas = HashMap::new();
        base_schemas.insert(
            "t".into(),
            CreateTableStatement {
                table,
                fields,
                keys,
                if_not_exists,
                options: vec![],
            },
        );
        base_schemas
    }

    fn removes_limit_order(input: &str) {
        let input_query = parse_query(Dialect::MySQL, input).unwrap();
        let revised_query = input_query
            .order_limit_removal(&generate_base_schemas())
            .unwrap();
        match revised_query {
            SqlQuery::Select(stmt) => {
                assert!(stmt.order.is_none());
                assert!(stmt.limit.is_none());
            }
            _ => panic!("Invalid query returned: {:?}", revised_query),
        }
    }

    fn does_not_change_limit_order(input: &str) {
        let input_query = parse_query(Dialect::MySQL, input).unwrap();
        assert_eq!(
            input_query,
            input_query
                .clone()
                .order_limit_removal(&generate_base_schemas())
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
            name: None,
            columns: vec![col1.clone(), col2.clone()],
        }]);
        base_schema.get_mut("t").unwrap().keys = keys;
        assert_eq!(
            input_query,
            input_query
                .clone()
                .order_limit_removal(&base_schema)
                .unwrap()
        );
        // compound Unique
        let keys = Some(vec![TableKey::UniqueKey {
            name: None,
            columns: vec![col1, col2],
            index_type: None,
        }]);
        base_schema.get_mut("t").unwrap().keys = keys;
        assert_eq!(
            input_query,
            input_query
                .clone()
                .order_limit_removal(&base_schema)
                .unwrap()
        );
        // compound unique but col is separately specified to be unique
        let revised_query = input_query2.order_limit_removal(&base_schema).unwrap();
        match revised_query {
            SqlQuery::Select(stmt) => {
                assert!(stmt.order.is_none());
                assert!(stmt.limit.is_none());
            }
            _ => panic!("Invalid query returned: {:?}", revised_query),
        }
    }
}
