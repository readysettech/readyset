use nom_sql::{Column, FieldExpression, SqlQuery, Table};

use std::collections::HashMap;

pub trait CountStarRewrite {
    fn rewrite_count_star(self, write_schemas: &HashMap<String, Vec<String>>) -> SqlQuery;
}

impl CountStarRewrite for SqlQuery {
    fn rewrite_count_star(self, write_schemas: &HashMap<String, Vec<String>>) -> SqlQuery {
        use nom_sql::FunctionExpression::*;

        let rewrite_count_star = |mut c: Column, tables: &Vec<Table>| -> Column {
            assert!(tables.len() > 0);
            let bogo_table = tables.get(0).unwrap();
            let bogo_column = write_schemas.get(&bogo_table.name).unwrap().last().unwrap();

            let rewrite = |fe: FieldExpression| -> FieldExpression {
                match fe {
                    FieldExpression::All => {
                        FieldExpression::Seq(vec![Column {
                                                      name: bogo_column.clone(),
                                                      alias: None,
                                                      table: Some(bogo_table.name.clone()),
                                                      function: None,
                                                  }])
                    }
                    x => x,
                }
            };
            c.function = match c.function {
                Some(f) => {
                    Some(match f {
                             Avg(fe) => Avg(rewrite(fe)),
                             Count(fe) => Count(rewrite(fe)),
                             Sum(fe) => Sum(rewrite(fe)),
                             Min(fe) => Min(rewrite(fe)),
                             Max(fe) => Max(rewrite(fe)),
                             GroupConcat(fe) => GroupConcat(rewrite(fe)),
                         })
                }
                None => None,
            };
            c
        };

        let err = "Must apply StarExpansion pass before CountStarRewrite"; // for wrapping
        match self {
            SqlQuery::Select(mut sq) => {
                // Expand within field list
                let tables = sq.tables.clone();
                sq.fields = match sq.fields {
                    FieldExpression::All => panic!(err),
                    FieldExpression::Seq(fs) => {
                        FieldExpression::Seq(fs.into_iter()
                                                 .map(|c| rewrite_count_star(c, &tables))
                                                 .collect())
                    }
                };
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
        // SELECT COUNT(users.age) FROM users;
        let q = parse_query("SELECT COUNT(*) FROM users;").unwrap();
        let mut schema = HashMap::new();
        schema.insert("users".into(),
                      vec!["id".into(), "name".into(), "age".into()]);

        let res = q.rewrite_count_star(&schema);
        match res {
            SqlQuery::Select(tq) => {
                assert_eq!(tq.fields,
                           FieldExpression::Seq(vec![Column {
                               name: String::from("anon_fn"),
                               alias: None,
                               table: None,
                               function: Some(FunctionExpression::Count(
                                   FieldExpression::Seq(vec![Column::from("users.age")]))),
                           }]));
            }
            // if we get anything other than a selection query back, something really weird is up
            _ => panic!(),
        }
    }
}
