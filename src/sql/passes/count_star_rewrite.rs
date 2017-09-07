use nom_sql::{Column, ConditionBase, ConditionExpression, ConditionTree, FieldExpression,
              SqlQuery, Table};


use std::collections::HashMap;

pub trait CountStarRewrite {
    fn rewrite_count_star(self, write_schemas: &HashMap<String, Vec<String>>) -> SqlQuery;
}

fn extract_condition_columns(ce: &ConditionExpression) -> Vec<Column> {
    match *ce {
        ConditionExpression::LogicalOp(ConditionTree {
            box ref left,
            box ref right,
            ..
        }) => {
            return extract_condition_columns(left)
                .into_iter()
                .chain(extract_condition_columns(right).into_iter())
                .collect();
        }
        ConditionExpression::ComparisonOp(ConditionTree {
            box ref left,
            box ref right,
            ..
        }) => {
            let mut cols = vec![];
            match *left {
                ConditionExpression::Base(ConditionBase::Field(ref f)) => cols.push(f.clone()),
                _ => (),
            }
            match *right {
                ConditionExpression::Base(ConditionBase::Field(ref f)) => cols.push(f.clone()),
                _ => (),
            }

            return cols;
        }
        ConditionExpression::NegationOp(ref inner) => {
            return extract_condition_columns(inner);
        }
        ConditionExpression::Base(_) => unreachable!(),
    }
}


impl CountStarRewrite for SqlQuery {
    fn rewrite_count_star(self, write_schemas: &HashMap<String, Vec<String>>) -> SqlQuery {
        use nom_sql::FunctionExpression::*;

        let rewrite_count_star =
            |c: &mut Column, tables: &Vec<Table>, avoid_columns: &Vec<Column>| {
                assert!(tables.len() > 0);
                if let Some(box CountStar) = c.function {
                    let bogo_table = tables.get(0).unwrap();
                    let mut schema_iter = write_schemas.get(&bogo_table.name).unwrap().iter();
                    let mut bogo_column = schema_iter.next().unwrap();
                    while avoid_columns
                        .iter()
                        .position(|c| c.name == *bogo_column)
                        .is_some()
                    {
                        bogo_column = schema_iter.next().expect(
                            "ran out of columns trying to pick a bogo column for COUNT(*)",
                        );
                    }

                    c.function = Some(Box::new(Count(
                        Column {
                            name: bogo_column.clone(),
                            alias: None,
                            table: Some(bogo_table.name.clone()),
                            function: None,
                        },
                        false,
                    )));
                }
            };

        let err = "Must apply StarExpansion pass before CountStarRewrite"; // for wrapping
        match self {
            SqlQuery::Select(mut sq) => {
                // Expand within field list
                let tables = sq.tables.clone();
                let mut avoid_cols = vec![];
                match sq.group_by {
                    Some(ref gbc) => avoid_cols.extend(gbc.columns.clone()),
                    None => (),
                };
                if let Some(ref w) = sq.where_clause {
                    avoid_cols.extend(extract_condition_columns(w));
                }
                for field in sq.fields.iter_mut() {
                    match field {
                        &mut FieldExpression::All => panic!(err),
                        &mut FieldExpression::AllInTable(_) => panic!(err),
                        &mut FieldExpression::Literal(_) => (),
                        &mut FieldExpression::Col(ref mut c) => {
                            rewrite_count_star(c, &tables, &avoid_cols)
                        }
                    }
                }
                // TODO: also expand function columns within WHERE clause
                SqlQuery::Select(sq)
            }
            // nothing to do for other query types, as they cannot have aliases
            x => x,
        }
    }
}

#[cfg(test)]
mod tests {
    use nom_sql::{Column, FieldExpression, SqlQuery};
    use std::collections::HashMap;
    use super::CountStarRewrite;

    #[test]
    fn it_expands_count_star() {
        use nom_sql::FunctionExpression;
        use nom_sql::parser::parse_query;

        // SELECT COUNT(*) FROM users;
        // -->
        // SELECT COUNT(users.id) FROM users;
        let q = parse_query("SELECT COUNT(*) FROM users;").unwrap();
        let mut schema = HashMap::new();
        schema.insert(
            "users".into(),
            vec!["id".into(), "name".into(), "age".into()],
        );

        let res = q.rewrite_count_star(&schema);
        match res {
            SqlQuery::Select(tq) => {
                assert_eq!(
                    tq.fields,
                    vec![
                        FieldExpression::Col(Column {
                            name: String::from("count(all)"),
                            alias: None,
                            table: None,
                            function: Some(Box::new(
                                FunctionExpression::Count(Column::from("users.id"), false),
                            )),
                        }),
                    ]
                );
            }
            // if we get anything other than a selection query back, something really weird is up
            _ => panic!(),
        }
    }

    #[test]
    fn it_expands_count_star_with_group_by() {
        use nom_sql::FunctionExpression;
        use nom_sql::parser::parse_query;

        // SELECT COUNT(*) FROM users GROUP BY id;
        // -->
        // SELECT COUNT(users.name) FROM users GROUP BY id;
        let q = parse_query("SELECT COUNT(*) FROM users GROUP BY id;").unwrap();
        let mut schema = HashMap::new();
        schema.insert(
            "users".into(),
            vec!["id".into(), "name".into(), "age".into()],
        );

        let res = q.rewrite_count_star(&schema);
        match res {
            SqlQuery::Select(tq) => {
                assert_eq!(
                    tq.fields,
                    vec![
                        FieldExpression::Col(Column {
                            name: String::from("count(all)"),
                            alias: None,
                            table: None,
                            function: Some(Box::new(
                                FunctionExpression::Count(Column::from("users.name"), false),
                            )),
                        }),
                    ]
                );
            }
            // if we get anything other than a selection query back, something really weird is up
            _ => panic!(),
        }
    }
}
