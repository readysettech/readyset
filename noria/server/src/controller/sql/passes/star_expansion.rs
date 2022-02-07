use std::collections::HashMap;
use std::mem;

use nom_sql::{
    Column, CommonTableExpression, Expression, FieldDefinitionExpression, JoinClause,
    JoinRightSide, SelectStatement, SqlQuery, Table,
};
use noria::{ReadySetError, ReadySetResult};

fn extract_tables_from_join(joins: &[JoinClause]) -> impl Iterator<Item = &Table> {
    use itertools::Either;
    joins.iter().flat_map(|j| match j.right {
        // subquery schemas should be handled separately
        JoinRightSide::NestedSelect(..) => Either::Left(Either::Left(std::iter::empty())),
        JoinRightSide::Table(ref t) => Either::Left(Either::Right(std::iter::once(t))),
        JoinRightSide::Tables(ref v) => Either::Right(v.iter()),
    })
}

pub trait StarExpansion: Sized {
    fn expand_stars(self, write_schemas: &HashMap<String, Vec<String>>) -> ReadySetResult<Self>;
}

impl StarExpansion for SelectStatement {
    fn expand_stars(
        mut self,
        write_schemas: &HashMap<String, Vec<String>>,
    ) -> ReadySetResult<Self> {
        self.ctes = mem::take(&mut self.ctes)
            .into_iter()
            .map(|cte| -> ReadySetResult<_> {
                Ok(CommonTableExpression {
                    name: cte.name,
                    statement: cte.statement.expand_stars(write_schemas)?,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        self.join = mem::take(&mut self.join)
            .into_iter()
            .map(|join| -> ReadySetResult<_> {
                Ok(JoinClause {
                    right: match join.right {
                        JoinRightSide::NestedSelect(stmt, name) => JoinRightSide::NestedSelect(
                            Box::new(stmt.expand_stars(write_schemas)?),
                            name,
                        ),
                        r => r,
                    },
                    ..join
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        let fields = mem::take(&mut self.fields);
        let subquery_schemas = super::subquery_schemas(&self.ctes, &self.join);

        let expand_table = move |table_name: String| -> ReadySetResult<_> {
            Ok(write_schemas
                .get(&table_name)
                .map(|fs| fs.iter().map(String::as_str).collect())
                .or_else(|| subquery_schemas.get(table_name.as_str()).cloned())
                .ok_or_else(|| ReadySetError::TableNotFound(table_name.clone()))?
                .into_iter()
                .map(move |f| FieldDefinitionExpression::Expression {
                    expr: Expression::Column(Column::from(
                        format!("{}.{}", table_name, f).as_ref(),
                    )),
                    alias: None,
                }))
        };

        for field in fields {
            match field {
                FieldDefinitionExpression::All => {
                    for table in &self.tables {
                        for field in expand_table(table.name.clone())? {
                            self.fields.push(field);
                        }
                    }
                    for t in extract_tables_from_join(&self.join) {
                        for field in expand_table(t.name.clone())? {
                            self.fields.push(field);
                        }
                    }
                }
                FieldDefinitionExpression::AllInTable(t) => {
                    for field in expand_table(t)? {
                        self.fields.push(field);
                    }
                }
                e @ FieldDefinitionExpression::Expression { .. } => {
                    self.fields.push(e);
                }
            }
        }

        Ok(self)
    }
}

impl StarExpansion for SqlQuery {
    fn expand_stars(self, write_schemas: &HashMap<String, Vec<String>>) -> ReadySetResult<Self> {
        Ok(match self {
            SqlQuery::Select(sq) => SqlQuery::Select(sq.expand_stars(write_schemas)?),
            _ => self,
        })
    }
}

#[cfg(test)]
mod tests {
    use maplit::hashmap;
    use nom_sql::{parse_query, Dialect};

    use super::StarExpansion;

    macro_rules! expands_stars {
	    ($source: expr, $expected: expr, schema: {$($schema:tt)*}) => {{
            let q = parse_query(Dialect::MySQL, $source).unwrap();
            let expected = parse_query(Dialect::MySQL, $expected).unwrap();
            let schema = hashmap!($($schema)*);
            let res = q.expand_stars(&schema).unwrap();
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

    #[test]
    fn simple_join() {
        expands_stars!(
            "SELECT * FROM t1 JOIN t2 on t1.a = t2.b",
            "SELECT t1.a, t2.b FROM t1 JOIN t2 on t1.a = t2.b",
            schema: {
                "t1".into() => vec!["a".into()],
                "t2".into() => vec!["b".into()],
            }
        );
    }
}
