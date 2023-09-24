use std::collections::{HashMap, HashSet};
use std::mem;

use nom_sql::analysis::visit_mut::{self, VisitorMut};
use nom_sql::{
    Column, Expr, FieldDefinitionExpr, NonReplicatedRelation, Relation, SelectStatement,
    SqlIdentifier, SqlQuery,
};
use readyset_errors::{ReadySetError, ReadySetResult};

use crate::{outermost_table_exprs, util};

pub trait StarExpansion: Sized {
    /// Expand all `*` column references in the query given a map from tables to the lists of
    /// columns in those tables
    fn expand_stars(
        self,
        table_columns: &HashMap<Relation, Vec<SqlIdentifier>>,
        non_replicated_relations: &HashSet<NonReplicatedRelation>,
    ) -> ReadySetResult<Self>;
}

struct ExpandStarsVisitor<'schema> {
    table_columns: &'schema HashMap<Relation, Vec<SqlIdentifier>>,
    non_replicated_relations: &'schema HashSet<NonReplicatedRelation>,
}

impl<'ast, 'schema> VisitorMut<'ast> for ExpandStarsVisitor<'schema> {
    type Error = ReadySetError;

    fn visit_select_statement(
        &mut self,
        select_statement: &'ast mut SelectStatement,
    ) -> Result<(), Self::Error> {
        visit_mut::walk_select_statement(self, select_statement)?;

        let fields = mem::take(&mut select_statement.fields);
        let subquery_schemas = util::subquery_schemas(
            &select_statement.tables,
            &select_statement.ctes,
            &select_statement.join,
        );

        let expand_table = |table: Relation, alias: Option<SqlIdentifier>| -> ReadySetResult<_> {
            Ok(if table.schema.is_none() {
                // Can only reference subqueries with tables that don't have a schema
                subquery_schemas.get(&table.name).cloned()
            } else {
                None
            }
            .or_else(|| self.table_columns.get(&table).map(|fs| fs.iter().collect()))
            .ok_or_else(|| {
                let non_replicated_relation = NonReplicatedRelation::new(table.clone());
                if self
                    .non_replicated_relations
                    .contains(&non_replicated_relation)
                {
                    ReadySetError::TableNotReplicated {
                        name: table.name.clone().into(),
                        schema: table.schema.clone().map(Into::into),
                    }
                } else {
                    ReadySetError::TableNotFound {
                        name: table.name.clone().into(),
                        schema: table.schema.clone().map(Into::into),
                    }
                }
            })?
            .into_iter()
            .map(move |f| FieldDefinitionExpr::Expr {
                expr: Expr::Column(Column {
                    table: Some(
                        alias
                            .clone()
                            .map(Relation::from)
                            .unwrap_or_else(|| table.clone()),
                    ),
                    name: f.clone(),
                }),
                alias: None,
            }))
        };

        for field in fields {
            match field {
                FieldDefinitionExpr::All => {
                    for expr in outermost_table_exprs(select_statement)
                        .cloned()
                        .collect::<Vec<_>>()
                    {
                        if let Some(tbl) = expr
                            .inner
                            .as_table()
                            .cloned()
                            .or_else(|| expr.alias.clone().map(Into::into))
                        {
                            for field in expand_table(tbl, expr.alias.clone())? {
                                select_statement.fields.push(field);
                            }
                            continue;
                        }
                    }
                }
                FieldDefinitionExpr::AllInTable(t) => {
                    let alias = if t.schema.is_none() {
                        Some(t.name.clone())
                    } else {
                        None
                    };

                    let tbl = t
                        .schema
                        .is_none()
                        .then(|| {
                            outermost_table_exprs(select_statement).find_map(|expr| {
                                if expr.alias.is_some() && expr.alias == alias {
                                    expr.inner.as_table().cloned()
                                } else {
                                    None
                                }
                            })
                        })
                        .flatten()
                        .unwrap_or(t);

                    for field in expand_table(tbl, alias)? {
                        select_statement.fields.push(field);
                    }
                }
                e @ FieldDefinitionExpr::Expr { .. } => {
                    select_statement.fields.push(e);
                }
            }
        }

        Ok(())
    }
}

impl StarExpansion for SelectStatement {
    fn expand_stars(
        mut self,
        table_columns: &HashMap<Relation, Vec<SqlIdentifier>>,
        non_replicated_relations: &HashSet<NonReplicatedRelation>,
    ) -> ReadySetResult<Self> {
        let mut visitor = ExpandStarsVisitor {
            table_columns,
            non_replicated_relations,
        };
        visitor.visit_select_statement(&mut self)?;
        Ok(self)
    }
}

impl StarExpansion for SqlQuery {
    fn expand_stars(
        self,
        write_schemas: &HashMap<Relation, Vec<SqlIdentifier>>,
        non_replicated_relations: &HashSet<NonReplicatedRelation>,
    ) -> ReadySetResult<Self> {
        Ok(match self {
            SqlQuery::Select(sq) => {
                SqlQuery::Select(sq.expand_stars(write_schemas, non_replicated_relations)?)
            }
            _ => self,
        })
    }
}

#[cfg(test)]
mod tests {
    use nom_sql::{parse_query, Dialect, DialectDisplay};

    use super::*;

    #[track_caller]
    fn expands_stars(source: &str, expected: &str, schema: HashMap<Relation, Vec<SqlIdentifier>>) {
        let q = parse_query(Dialect::MySQL, source).unwrap();
        let expected = parse_query(Dialect::MySQL, expected).unwrap();
        let res = q.expand_stars(&schema, &Default::default()).unwrap();
        assert_eq!(
            res,
            expected,
            "{} != {}",
            res.display(nom_sql::Dialect::MySQL),
            expected.display(nom_sql::Dialect::MySQL)
        );
    }

    #[test]
    fn single_table() {
        expands_stars(
            "SELECT * FROM PaperTag",
            "SELECT PaperTag.paper_id, PaperTag.tag_id FROM PaperTag",
            HashMap::from([("PaperTag".into(), vec!["paper_id".into(), "tag_id".into()])]),
        );
    }

    #[test]
    fn aliased() {
        expands_stars(
            "SELECT * FROM PaperTag AS t",
            "SELECT t.paper_id, t.tag_id FROM PaperTag AS t",
            HashMap::from([("PaperTag".into(), vec!["paper_id".into(), "tag_id".into()])]),
        );
    }

    #[test]
    fn referencing_alias() {
        expands_stars(
            "SELECT t.* FROM PaperTag AS t",
            "SELECT t.paper_id, t.tag_id FROM PaperTag AS t",
            HashMap::from([("PaperTag".into(), vec!["paper_id".into(), "tag_id".into()])]),
        );
    }

    #[test]
    fn multiple_tables() {
        expands_stars(
            "SELECT * FROM PaperTag, Users",
            "SELECT PaperTag.paper_id, PaperTag.tag_id, Users.uid, Users.name FROM PaperTag, Users",
            HashMap::from([
                ("PaperTag".into(), vec!["paper_id".into(), "tag_id".into()]),
                ("Users".into(), vec!["uid".into(), "name".into()]),
            ]),
        );
    }

    #[test]
    fn table_stars() {
        expands_stars(
            "SELECT Users.*, PaperTag.* FROM PaperTag, Users",
            "SELECT Users.uid, Users.name, PaperTag.paper_id, PaperTag.tag_id FROM PaperTag, Users",
            HashMap::from([
                ("PaperTag".into(), vec!["paper_id".into(), "tag_id".into()]),
                ("Users".into(), vec!["uid".into(), "name".into()]),
            ]),
        );
    }

    #[test]
    fn in_cte() {
        expands_stars(
            "WITH users AS (SELECT Users.* FROM Users) SELECT uid FROM users",
            "WITH users AS (SELECT Users.uid, Users.name FROM Users) SELECT uid FROM users",
            HashMap::from([("Users".into(), vec!["uid".into(), "name".into()])]),
        );
    }

    #[test]
    fn referencing_cte() {
        expands_stars(
            "WITH users AS (SELECT Users.* FROM Users) SELECT * FROM users",
            "WITH users AS (SELECT Users.uid, Users.name FROM Users) SELECT users.uid, users.name FROM users",
            HashMap::from([("Users".into(), vec!["uid".into(), "name".into()])])
        );
    }

    #[test]
    fn referencing_cte_shadowing_table() {
        expands_stars(
            "WITH t2 AS (SELECT * FROM t1) SELECT * FROM t2",
            "WITH t2 AS (SELECT t1.a, t1.b FROM t1) SELECT t2.a, t2.b FROM t2",
            HashMap::from([
                ("t1".into(), vec!["a".into(), "b".into()]),
                ("t2".into(), vec!["c".into(), "d".into()]),
            ]),
        )
    }

    #[test]
    fn in_subquery() {
        expands_stars(
            "SELECT uid FROM PaperTag JOIN (SELECT Users.* FROM Users) users On paper_id = uid",
            "SELECT uid FROM PaperTag JOIN (SELECT Users.uid, Users.name FROM Users) users On paper_id = uid",
            HashMap::from([
                ("PaperTag".into(), vec!["paper_id".into(), "tag_id".into()]),
                ("Users".into(), vec!["uid".into(), "name".into()]),
            ])
        );
    }

    #[test]
    fn referencing_subquery() {
        expands_stars(
            "SELECT users.* FROM PaperTag JOIN (SELECT Users.* FROM Users) users On paper_id = uid",
            "SELECT users.uid, users.name FROM PaperTag JOIN (SELECT Users.uid, Users.name FROM Users) users On paper_id = uid",
            HashMap::from([
                ( "Users".into(), vec!["uid".into(), "name".into()] ),
                    ])
        );
    }

    #[test]
    fn star_with_subquery() {
        expands_stars(
            "SELECT * FROM (SELECT t1.x FROM t1) sq",
            "SELECT sq.x FROM (SELECT t1.x FROM t1) sq",
            HashMap::from([("t1".into(), vec!["x".into()])]),
        )
    }

    #[test]
    fn simple_join() {
        expands_stars(
            "SELECT * FROM t1 JOIN t2 on t1.a = t2.b",
            "SELECT t1.a, t2.b FROM t1 JOIN t2 on t1.a = t2.b",
            HashMap::from([
                ("t1".into(), vec!["a".into()]),
                ("t2".into(), vec!["b".into()]),
            ]),
        );
    }
}
