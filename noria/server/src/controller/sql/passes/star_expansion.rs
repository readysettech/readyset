use nom_sql::{
    Column, CommonTableExpression, Expression, FieldDefinitionExpression, JoinClause,
    JoinRightSide, SelectStatement, SqlQuery,
};

use itertools::Either;
use std::collections::HashMap;
use std::{iter, mem};

pub trait StarExpansion {
    fn expand_stars(self, write_schemas: &HashMap<String, Vec<String>>) -> Self;
}

impl StarExpansion for SelectStatement {
    fn expand_stars(mut self, write_schemas: &HashMap<String, Vec<String>>) -> Self {
        self.ctes = mem::take(&mut self.ctes)
            .into_iter()
            .map(|cte| CommonTableExpression {
                name: cte.name,
                statement: cte.statement.expand_stars(write_schemas),
            })
            .collect();

        self.join = mem::take(&mut self.join)
            .into_iter()
            .map(|join| JoinClause {
                right: match join.right {
                    JoinRightSide::NestedSelect(stmt, name) => JoinRightSide::NestedSelect(
                        Box::new(stmt.expand_stars(write_schemas)),
                        name,
                    ),
                    r => r,
                },
                ..join
            })
            .collect();

        let fields = mem::take(&mut self.fields);
        let subquery_schemas = super::subquery_schemas(&self.ctes, &self.join);

        let expand_table = move |table_name: String| {
            write_schemas
                .get(&table_name)
                .map(|fs| fs.iter().map(String::as_str).collect())
                .or_else(|| subquery_schemas.get(table_name.as_str()).cloned())
                // TODO(eta): check that this panic can never fire
                .unwrap_or_else(|| panic!("table name `{}` does not exist", table_name))
                .into_iter()
                .map(move |f| FieldDefinitionExpression::Expression {
                    expr: Expression::Column(Column::from(
                        format!("{}.{}", table_name, f).as_ref(),
                    )),
                    alias: None,
                })
        };

        self.fields = fields
            .into_iter()
            .flat_map(|field| match field {
                FieldDefinitionExpression::All => Either::Left(
                    self.tables
                        .iter()
                        .map(|t| t.name.clone())
                        .flat_map(&expand_table),
                ),
                FieldDefinitionExpression::AllInTable(t) => {
                    Either::Right(Either::Left(expand_table(t)))
                }
                e @ FieldDefinitionExpression::Expression { .. } => {
                    Either::Right(Either::Right(iter::once(e)))
                }
            })
            .collect();

        self
    }
}

impl StarExpansion for SqlQuery {
    fn expand_stars(self, write_schemas: &HashMap<String, Vec<String>>) -> Self {
        match self {
            SqlQuery::Select(sq) => SqlQuery::Select(sq.expand_stars(write_schemas)),
            _ => self,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::StarExpansion;
    use maplit::hashmap;
    use nom_sql::parse_query;

    macro_rules! expands_stars {
	    ($source: expr, $expected: expr, schema: {$($schema:tt)*}) => {{
            let q = parse_query($source).unwrap();
            let expected = parse_query($expected).unwrap();
            let schema = hashmap!($($schema)*);
            let res = q.expand_stars(&schema);
            assert_eq!(res, expected, "{} != {}", res, expected);
        }};
    }

    #[test]
    fn single_table() {
        expands_stars!(
            "SELECT * FROM PaperTag",
            "SELECT PaperTag.paper_id, PaperTag.tag_id FROM PaperTag",
            schema: {
                "PaperTag".into() => vec!["paper_id".into(), "tag_id".into()]
            }
        );
    }

    #[test]
    fn multiple_tables() {
        expands_stars!(
            "SELECT * FROM PaperTag, Users",
            "SELECT PaperTag.paper_id, PaperTag.tag_id, Users.uid, Users.name FROM PaperTag, Users",
            schema: {
                "PaperTag".into() => vec!["paper_id".into(), "tag_id".into()],
                "Users".into() => vec!["uid".into(), "name".into()],
            }
        );
    }

    #[test]
    fn table_stars() {
        expands_stars!(
            "SELECT Users.*, PaperTag.* FROM PaperTag, Users",
            "SELECT Users.uid, Users.name, PaperTag.paper_id, PaperTag.tag_id FROM PaperTag, Users",
            schema: {
                "PaperTag".into() => vec!["paper_id".into(), "tag_id".into()],
                "Users".into() => vec!["uid".into(), "name".into()],
            }
        );
    }

    #[test]
    fn in_cte() {
        expands_stars!(
            "WITH users AS (SELECT Users.* FROM Users) SELECT uid FROM users",
            "WITH users AS (SELECT Users.uid, Users.name FROM Users) SELECT uid FROM users",
            schema: {
                "Users".into() => vec!["uid".into(), "name".into()]
            }
        );
    }

    #[test]
    fn referencing_cte() {
        expands_stars!(
            "WITH users AS (SELECT Users.* FROM Users) SELECT * FROM users",
            "WITH users AS (SELECT Users.uid, Users.name FROM Users) SELECT users.uid, users.name FROM users",
            schema: {
                "Users".into() => vec!["uid".into(), "name".into()]
            }
        );
    }

    #[test]
    fn in_subquery() {
        expands_stars!(
            "SELECT uid FROM PaperTag JOIN (SELECT Users.* FROM Users) users On paper_id = uid",
            "SELECT uid FROM PaperTag JOIN (SELECT Users.uid, Users.name FROM Users) users On paper_id = uid",
            schema: {
                "PaperTag".into() => vec!["paper_id".into(), "tag_id".into()],
                "Users".into() => vec!["uid".into(), "name".into()]
            }
        );
    }

    #[test]
    fn referencing_subquery() {
        expands_stars!(
            "SELECT users.* FROM PaperTag JOIN (SELECT Users.* FROM Users) users On paper_id = uid",
            "SELECT users.uid, users.name FROM PaperTag JOIN (SELECT Users.uid, Users.name FROM Users) users On paper_id = uid",
            schema: {
                "Users".into() => vec!["uid".into(), "name".into()]
            }
        );
    }
}
