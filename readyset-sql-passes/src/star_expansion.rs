use std::collections::{HashMap, HashSet};
use std::mem;

use readyset_errors::{ReadySetError, ReadySetResult};
use readyset_sql::analysis::visit_mut::{self, VisitorMut};
use readyset_sql::ast::{
    Column, Expr, FieldDefinitionExpr, NonReplicatedRelation, Relation, SelectStatement,
    SqlIdentifier, SqlQuery,
};

use crate::{outermost_table_exprs, util};

pub trait StarExpansionContext {
    /// Map from names of views and tables in the database, to (ordered) lists of the column names
    /// in those views
    fn schema_for_relation(
        &self,
        relation: &Relation,
    ) -> Option<impl IntoIterator<Item = SqlIdentifier>>;
}

impl<S: StarExpansionContext> StarExpansionContext for &S {
    fn schema_for_relation(
        &self,
        relation: &Relation,
    ) -> Option<impl IntoIterator<Item = SqlIdentifier>> {
        (*self).schema_for_relation(relation)
    }
}

pub trait StarExpansion {
    /// Expand all `*` column references in the query given a map from tables to the lists of
    /// columns in those tables
    fn expand_stars<S: StarExpansionContext>(
        &mut self,
        context: S,
        non_replicated_relations: &HashSet<NonReplicatedRelation>,
        dialect: readyset_sql::Dialect,
    ) -> ReadySetResult<&mut Self>;
}

struct ExpandStarsVisitor<'schema, S: StarExpansionContext> {
    context: S,
    non_replicated_relations: &'schema HashSet<NonReplicatedRelation>,
    dialect: readyset_sql::Dialect,
}

impl<'ast, S: StarExpansionContext> VisitorMut<'ast> for ExpandStarsVisitor<'_, S> {
    type Error = ReadySetError;

    fn visit_select_statement(
        &mut self,
        select_statement: &'ast mut SelectStatement,
    ) -> Result<(), Self::Error> {
        visit_mut::walk_select_statement(self, select_statement)?;

        let fields = mem::take(&mut select_statement.fields);

        let subquery_schemas: HashMap<_, Vec<SqlIdentifier>> = util::subquery_schemas(
            &mut select_statement.tables,
            &mut select_statement.ctes,
            &mut select_statement.join,
            self.dialect,
        )?
        .into_iter()
        .map(|(k, v)| (k.clone(), v.into_iter().cloned().collect()))
        .collect();

        let expand_table = |table: Relation, alias: Option<SqlIdentifier>| -> ReadySetResult<_> {
            Ok(if table.schema.is_none() {
                // Can only reference subqueries with tables that don't have a schema
                subquery_schemas.get(&table.name).cloned()
            } else {
                None
            }
            .or_else(|| {
                self.context
                    .schema_for_relation(&table)
                    .map(|fs| fs.into_iter().collect())
            })
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
    fn expand_stars<S: StarExpansionContext>(
        &mut self,
        context: S,
        non_replicated_relations: &HashSet<NonReplicatedRelation>,
        dialect: readyset_sql::Dialect,
    ) -> ReadySetResult<&mut Self> {
        let mut visitor = ExpandStarsVisitor {
            context,
            non_replicated_relations,
            dialect,
        };
        visitor.visit_select_statement(self)?;
        Ok(self)
    }
}

impl StarExpansion for SqlQuery {
    fn expand_stars<S: StarExpansionContext>(
        &mut self,
        context: S,
        non_replicated_relations: &HashSet<NonReplicatedRelation>,
        dialect: readyset_sql::Dialect,
    ) -> ReadySetResult<&mut Self> {
        if let SqlQuery::Select(sq) = self {
            sq.expand_stars(context, non_replicated_relations, dialect)?;
        }
        Ok(self)
    }
}

#[cfg(test)]
mod tests {
    use readyset_sql::{Dialect, DialectDisplay};
    use readyset_sql_parsing::parse_query;

    use super::*;

    struct TestSchemaContext {
        schema: HashMap<Relation, Vec<SqlIdentifier>>,
    }

    impl StarExpansionContext for TestSchemaContext {
        fn schema_for_relation(
            &self,
            table: &Relation,
        ) -> Option<impl IntoIterator<Item = SqlIdentifier>> {
            self.schema.get(table).cloned()
        }
    }

    #[track_caller]
    fn expands_stars(source: &str, expected: &str, schema: HashMap<Relation, Vec<SqlIdentifier>>) {
        let mut q = parse_query(Dialect::MySQL, source).unwrap();
        let expected = parse_query(Dialect::MySQL, expected).unwrap();
        q.expand_stars(
            TestSchemaContext { schema },
            &Default::default(),
            readyset_sql::Dialect::MySQL,
        )
        .unwrap();
        assert_eq!(
            q,
            expected,
            "{} != {}",
            q.display(readyset_sql::Dialect::MySQL),
            expected.display(readyset_sql::Dialect::MySQL)
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
            HashMap::from([("Users".into(), vec!["uid".into(), "name".into()])]),
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
            ]),
        );
    }

    #[test]
    fn referencing_subquery() {
        expands_stars(
            "SELECT users.* FROM PaperTag JOIN (SELECT Users.* FROM Users) users On paper_id = uid",
            "SELECT users.uid, users.name FROM PaperTag JOIN (SELECT Users.uid, Users.name FROM Users) users On paper_id = uid",
            HashMap::from([("Users".into(), vec!["uid".into(), "name".into()])]),
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
    fn star_with_complex_subquery() {
        expands_stars(
            "SELECT * FROM (SELECT t1.x + 1 FROM t1) sq",
            "SELECT `sq`.`x + 1` FROM (SELECT t1.x + 1 AS `x + 1` FROM t1) sq",
            HashMap::from([("t1".into(), vec!["x".into()])]),
        );

        expands_stars(
            "SELECT * FROM (SELECT max(a) FROM t1) sq",
            "SELECT `sq`.`max(a)` FROM (SELECT max(a) AS `max(a)` FROM t1) sq",
            HashMap::from([("t1".into(), vec!["x".into()])]),
        );

        expands_stars(
            "SELECT * FROM (SELECT max(a) AS a FROM t1) sq",
            "SELECT sq.a FROM (SELECT max(a) AS a FROM t1) sq",
            HashMap::from([("t1".into(), vec!["x".into()])]),
        );
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
