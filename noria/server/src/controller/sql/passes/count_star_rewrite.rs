use nom_sql::{
    Column, ConditionBase, ConditionExpression, ConditionTree, Expression,
    FieldDefinitionExpression, FunctionExpression, SqlQuery, Table,
};

use crate::errors::{internal_err, ReadySetResult};
use crate::{internal, invariant, unsupported};
use std::collections::{HashMap, HashSet};

pub trait CountStarRewrite {
    fn rewrite_count_star(
        self,
        write_schemas: &HashMap<String, Vec<String>>,
    ) -> ReadySetResult<SqlQuery>;
}

fn extract_condition_columns(ce: &ConditionExpression) -> ReadySetResult<HashSet<Column>> {
    Ok(match *ce {
        ConditionExpression::LogicalOp(ConditionTree {
            ref left,
            ref right,
            ..
        }) => extract_condition_columns(left)?
            .into_iter()
            .chain(extract_condition_columns(right)?.into_iter())
            .collect(),
        ConditionExpression::ComparisonOp(ConditionTree {
            ref left,
            ref right,
            ..
        }) => {
            let mut cols = HashSet::new();
            if let ConditionExpression::Base(ConditionBase::Field(ref f)) = **left {
                cols.insert(f.clone());
            }
            if let ConditionExpression::Base(ConditionBase::Field(ref f)) = **right {
                cols.insert(f.clone());
            }

            cols
        }
        ConditionExpression::NegationOp(ref inner) => extract_condition_columns(inner)?,
        ConditionExpression::Bracketed(ref inner) => extract_condition_columns(inner)?,
        ConditionExpression::Base(_) => internal!(),
        ConditionExpression::Arithmetic(_) => unsupported!(),
        ConditionExpression::ExistsOp(_) => unsupported!(),
        ConditionExpression::Between {
            ref operand,
            ref min,
            ref max,
        } => {
            let mut res = extract_condition_columns(operand)?;
            res.extend(extract_condition_columns(min)?);
            res.extend(extract_condition_columns(max)?);
            res
        }
    })
}

impl CountStarRewrite for SqlQuery {
    fn rewrite_count_star(
        self,
        write_schemas: &HashMap<String, Vec<String>>,
    ) -> ReadySetResult<SqlQuery> {
        use nom_sql::FunctionExpression::*;

        let rewrite_count_star = |f: &mut FunctionExpression,
                                  tables: &Vec<Table>,
                                  avoid_columns: &Vec<Column>|
         -> ReadySetResult<_> {
            invariant!(!tables.is_empty());
            if *f == CountStar {
                let bogo_table = &tables[0];
                let mut schema_iter = write_schemas.get(&bogo_table.name).unwrap().iter();
                let mut bogo_column = schema_iter.next().unwrap();
                while avoid_columns.iter().any(|c| c.name == *bogo_column) {
                    bogo_column = schema_iter.next().ok_or_else(|| {
                        internal_err("ran out of columns trying to pick a bogo column for COUNT(*)")
                    })?;
                }

                *f = Count {
                    expr: Box::new(Expression::Column(Column {
                        name: bogo_column.clone(),
                        table: Some(bogo_table.name.clone()),
                        function: None,
                    })),
                    distinct: false,
                };
            }
            Ok(())
        };

        Ok(match self {
            SqlQuery::Select(mut sq) => {
                // Expand within field list
                let tables = sq.tables.clone();
                let mut avoid_cols = vec![];
                if let Some(ref gbc) = sq.group_by {
                    avoid_cols.extend(gbc.columns.clone());
                }
                if let Some(ref w) = sq.where_clause {
                    avoid_cols.extend(extract_condition_columns(w)?);
                }
                for field in sq.fields.iter_mut() {
                    match *field {
                        FieldDefinitionExpression::All
                        | FieldDefinitionExpression::AllInTable(_) => {
                            internal!("Must apply StarExpansion pass before CountStarRewrite")
                        }
                        FieldDefinitionExpression::Expression {
                            expr: Expression::Call(ref mut f),
                            ..
                        } => rewrite_count_star(f, &tables, &avoid_cols)?,
                        _ => {}
                    }
                }
                // TODO: also expand function columns within WHERE clause
                SqlQuery::Select(sq)
            }
            // nothing to do for other query types, as they cannot have aliases
            x => x,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nom_sql::{Column, FieldDefinitionExpression, SqlQuery};
    use std::collections::HashMap;

    #[test]
    fn it_expands_count_star() {
        use nom_sql::parser::parse_query;
        use nom_sql::FunctionExpression;

        // SELECT COUNT(*) FROM users;
        // -->
        // SELECT COUNT(users.id) FROM users;
        let q = parse_query("SELECT COUNT(*) FROM users;").unwrap();
        let mut schema = HashMap::new();
        schema.insert(
            "users".into(),
            vec!["id".into(), "name".into(), "age".into()],
        );

        let res = q.rewrite_count_star(&schema).unwrap();
        match res {
            SqlQuery::Select(tq) => {
                assert_eq!(
                    tq.fields,
                    vec![FieldDefinitionExpression::from(Expression::Call(
                        FunctionExpression::Count {
                            expr: Box::new(Expression::Column(Column::from("users.id"))),
                            distinct: false,
                        }
                    ))]
                );
            }
            // if we get anything other than a selection query back, something really weird is up
            _ => panic!(),
        }
    }

    #[test]
    fn it_expands_count_star_with_group_by() {
        use nom_sql::parser::parse_query;
        use nom_sql::FunctionExpression;

        // SELECT COUNT(*) FROM users GROUP BY id;
        // -->
        // SELECT COUNT(users.name) FROM users GROUP BY id;
        let q = parse_query("SELECT COUNT(*) FROM users GROUP BY id;").unwrap();
        let mut schema = HashMap::new();
        schema.insert(
            "users".into(),
            vec!["id".into(), "name".into(), "age".into()],
        );

        let res = q.rewrite_count_star(&schema).unwrap();
        match res {
            SqlQuery::Select(tq) => {
                assert_eq!(
                    tq.fields,
                    vec![FieldDefinitionExpression::from(Expression::Call(
                        FunctionExpression::Count {
                            expr: Box::new(Expression::Column(Column::from("users.name"))),
                            distinct: false,
                        }
                    ))]
                );
            }
            // if we get anything other than a selection query back, something really weird is up
            _ => panic!(),
        }
    }
}
