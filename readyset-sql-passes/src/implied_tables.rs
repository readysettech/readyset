use std::collections::{HashMap, HashSet};
use std::mem;

use itertools::Itertools;
use readyset_errors::{internal, invalid_query_err, ReadySetError, ReadySetResult};
use readyset_sql::analysis::visit_mut::{
    walk_group_by_clause, walk_order_clause, walk_select_statement, VisitorMut,
};
use readyset_sql::ast::{
    Column, FieldDefinitionExpr, GroupByClause, OrderClause, Relation, SelectStatement,
    SqlIdentifier, SqlQuery, TableExprInner,
};
use tracing::warn;

use crate::{outermost_table_exprs, util};

pub trait ImpliedTableExpansion: Sized {
    fn expand_implied_tables(
        &mut self,
        table_columns: &HashMap<Relation, Vec<SqlIdentifier>>,
        dialect: readyset_sql::Dialect,
    ) -> ReadySetResult<&mut Self>;
}

#[derive(Debug)]
struct ExpandImpliedTablesVisitor<'schema> {
    /// Map from table name to list of fields for all tables in the db
    schema: &'schema HashMap<Relation, Vec<SqlIdentifier>>,
    /// Map from aliases for subqueries that are in scope, to a list of that subquery's projected
    /// fields
    subquery_schemas: HashMap<SqlIdentifier, Vec<SqlIdentifier>>,
    /// All the tables which are currently in scope for the query, represented as a map from the
    /// name of the table to its alias (or name, if unaliased)
    tables: HashMap<Relation, Relation>,
    /// The set of aliases for projected fields that are currently in-scope
    aliases: HashSet<SqlIdentifier>,
    // Are we currently in a position in the query that can reference aliases in the projected
    // field list?
    can_reference_aliases: bool,
    /// SQL dialect to use for expression display
    dialect: readyset_sql::Dialect,
}

impl ExpandImpliedTablesVisitor<'_> {
    fn find_table(&self, column_name: &str) -> Option<Relation> {
        let mut matches = self
            .schema
            .iter()
            .map(|(t, v)| (t.clone(), v))
            .chain(
                self.subquery_schemas
                    .iter()
                    .map(|(n, fs)| (Relation::from(n.clone()), fs)),
            )
            .filter_map(|(t, ws)| self.tables.get(&t).cloned().map(|t| (t, ws)))
            .filter_map(|(t, ws)| {
                let num_matching = ws.iter().filter(|c| **c == column_name).count();
                assert!(num_matching <= 1);
                if num_matching == 1 {
                    Some(t)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        if matches.len() > 1 {
            warn!(
                "Ambiguous column {} exists in tables: {} -- picking a random one",
                column_name,
                matches.iter().map(|t| t.display_unquoted()).join(", ")
            );
            Some(matches.pop().unwrap())
        } else if matches.is_empty() {
            // This might be an alias for a computed column, which has no
            // implied table. So, we allow it to pass and our code should
            // crash in the future if this is not the case.
            None
        } else {
            // exactly one match
            Some(matches.pop().unwrap())
        }
    }
}

impl<'ast> VisitorMut<'ast> for ExpandImpliedTablesVisitor<'_> {
    type Error = ReadySetError;

    fn visit_select_statement(
        &mut self,
        select_statement: &'ast mut SelectStatement,
    ) -> Result<(), Self::Error> {
        let orig_tables = mem::replace(
            &mut self.tables,
            outermost_table_exprs(select_statement)
                .filter_map(|tbl| {
                    Some((
                        match &tbl.inner {
                            TableExprInner::Table(t) => t.clone(),
                            TableExprInner::Subquery(_) => tbl.alias.clone()?.into(),
                        },
                        tbl.alias
                            .clone()
                            .map(Relation::from)
                            .or_else(|| tbl.inner.as_table().cloned())?,
                    ))
                })
                .collect(),
        );
        let orig_subquery_schemas = mem::replace(
            &mut self.subquery_schemas,
            util::subquery_schemas(
                &mut select_statement.tables,
                &mut select_statement.ctes,
                &mut select_statement.join,
                self.dialect,
            )?
            .into_iter()
            .map(|(k, v)| (k.into(), v.into_iter().cloned().collect()))
            .collect(),
        );
        let orig_aliases = mem::replace(
            &mut self.aliases,
            select_statement
                .fields
                .iter()
                .filter_map(|fde| match fde {
                    FieldDefinitionExpr::Expr {
                        alias: Some(alias), ..
                    } => Some(alias.clone()),
                    _ => None,
                })
                .collect(),
        );
        walk_select_statement(self, select_statement)?;

        self.tables = orig_tables;
        self.subquery_schemas = orig_subquery_schemas;
        self.aliases = orig_aliases;

        Ok(())
    }

    fn visit_order_clause(&mut self, order: &'ast mut OrderClause) -> Result<(), Self::Error> {
        self.can_reference_aliases = true;
        walk_order_clause(self, order)?;
        self.can_reference_aliases = false;
        Ok(())
    }

    fn visit_group_by_clause(
        &mut self,
        group_by: &'ast mut GroupByClause,
    ) -> Result<(), Self::Error> {
        self.can_reference_aliases = true;
        walk_group_by_clause(self, group_by)?;
        self.can_reference_aliases = false;
        Ok(())
    }

    fn visit_column(&mut self, column: &'ast mut Column) -> Result<(), Self::Error> {
        if self.can_reference_aliases && self.aliases.contains(&column.name) {
            return Ok(());
        }

        if let Some(table) = &mut column.table {
            if table.schema.is_some() {
                return Ok(());
            }

            let matches = self
                .tables
                .iter()
                .filter(|(t, _alias)| t.name == table.name)
                .map(|(_t, alias)| alias)
                .collect::<Vec<_>>();

            if matches.len() > 1 {
                return Err(invalid_query_err!(
                    "Table reference {} is ambiguous",
                    table.display_unquoted()
                ));
            }

            if let Some(t) = matches.first() {
                table.schema.clone_from(&t.schema);
            }
        } else {
            column.table = self.find_table(&column.name);
        }

        Ok(())
    }
}

fn rewrite_select<'stmt>(
    select_statement: &'stmt mut SelectStatement,
    schema: &HashMap<Relation, Vec<SqlIdentifier>>,
    dialect: readyset_sql::Dialect,
) -> ReadySetResult<&'stmt mut SelectStatement> {
    let mut visitor = ExpandImpliedTablesVisitor {
        schema,
        subquery_schemas: Default::default(),
        tables: Default::default(),
        aliases: Default::default(),
        can_reference_aliases: false,
        dialect,
    };

    visitor.visit_select_statement(select_statement)?;
    Ok(select_statement)
}

impl ImpliedTableExpansion for SelectStatement {
    fn expand_implied_tables(
        &mut self,
        table_columns: &HashMap<Relation, Vec<SqlIdentifier>>,
        dialect: readyset_sql::Dialect,
    ) -> ReadySetResult<&mut Self> {
        rewrite_select(self, table_columns, dialect)
    }
}

impl ImpliedTableExpansion for SqlQuery {
    fn expand_implied_tables(
        &mut self,
        table_columns: &HashMap<Relation, Vec<SqlIdentifier>>,
        dialect: readyset_sql::Dialect,
    ) -> ReadySetResult<&mut SqlQuery> {
        match self {
            SqlQuery::CreateTable(..) => {}
            SqlQuery::CompoundSelect(csq) => {
                for (_op, select) in &mut csq.selects {
                    rewrite_select(select, table_columns, dialect)?;
                }
            }
            SqlQuery::Select(sq) => {
                sq.expand_implied_tables(table_columns, dialect)?;
            }
            SqlQuery::Insert(iq) => {
                let table = iq.table.clone();
                for field in &mut iq.fields {
                    if field.table.is_none() {
                        field.table = Some(table.clone());
                    }
                }
            }
            _ => internal!("Unexpected query type expanding implied tables"),
        }
        Ok(self)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use readyset_sql::ast::{
        BinaryOperator, Column, Expr, FieldDefinitionExpr, SelectStatement, SqlQuery, TableExpr,
    };
    use readyset_sql::{Dialect, DialectDisplay};
    use readyset_sql_parsing::parse_query;

    use super::*;

    #[test]
    fn it_expands_implied_tables_for_select() {
        // SELECT name, title FROM users, articles WHERE users.id = author;
        // -->
        // SELECT users.name, articles.title FROM users, articles WHERE users.id = articles.author;
        let q = SelectStatement {
            tables: vec![
                TableExpr::from(Relation::from("users")),
                TableExpr::from(Relation::from("articles")),
            ],
            fields: vec![
                FieldDefinitionExpr::from(Column::from("name")),
                FieldDefinitionExpr::from(Column::from("title")),
            ],
            where_clause: Some(Expr::BinaryOp {
                lhs: Box::new(Expr::Column(Column::from("users.id"))),
                op: BinaryOperator::Equal,
                rhs: Box::new(Expr::Column(Column::from("author"))),
            }),
            ..Default::default()
        };
        let mut schema = HashMap::new();
        schema.insert(
            "users".into(),
            vec!["id".into(), "name".into(), "age".into()],
        );
        schema.insert(
            "articles".into(),
            vec!["id".into(), "title".into(), "text".into(), "author".into()],
        );

        let mut rewritten = SqlQuery::Select(q);
        rewritten
            .expand_implied_tables(&schema, readyset_sql::Dialect::MySQL)
            .unwrap();
        match rewritten {
            SqlQuery::Select(tq) => {
                assert_eq!(
                    tq.fields,
                    vec![
                        FieldDefinitionExpr::from(Column::from("users.name")),
                        FieldDefinitionExpr::from(Column::from("articles.title")),
                    ]
                );
                assert_eq!(
                    tq.where_clause,
                    Some(Expr::BinaryOp {
                        lhs: Box::new(Expr::Column(Column::from("users.id"))),
                        op: BinaryOperator::Equal,
                        rhs: Box::new(Expr::Column(Column::from("articles.author"))),
                    })
                );
            }
            // if we get anything other than a selection query back, something really weird is up
            _ => panic!(),
        }
    }

    #[test]
    fn doesnt_expand_order_referencing_projected_field() {
        let mut q = parse_query(
            Dialect::MySQL,
            "select value in (2, 3) as value from t1 order by value;",
        )
        .unwrap();
        // `value` here references the *projected field*, not `t1.value`, so we shouldn't qualify it
        let expected = parse_query(
            Dialect::MySQL,
            "select t1.value in (2, 3) as value from t1 order by value;",
        )
        .unwrap();

        let schema = HashMap::from([("t1".into(), vec!["id".into(), "value".into()])]);

        q.expand_implied_tables(&schema, readyset_sql::Dialect::MySQL)
            .unwrap();
        assert_eq!(q, expected);
    }

    #[test]
    fn in_where() {
        let mut q = parse_query(Dialect::MySQL, "SELECT name FROM users WHERE id = ?").unwrap();
        let expected = parse_query(
            Dialect::MySQL,
            "SELECT users.name FROM users WHERE users.id = ?",
        )
        .unwrap();
        let schema = HashMap::from([("users".into(), vec!["id".into(), "name".into()])]);

        q.expand_implied_tables(&schema, readyset_sql::Dialect::MySQL)
            .unwrap();
        assert_eq!(q, expected);
    }

    #[test]
    fn case_when() {
        let mut q = parse_query(
            Dialect::MySQL,
            "SELECT COUNT(CASE WHEN aid = 5 THEN aid END) AS count
             FROM votes GROUP BY votes.userid",
        )
        .unwrap();
        let expected = parse_query(
            Dialect::MySQL,
            "SELECT COUNT(CASE WHEN votes.aid = 5 THEN votes.aid END) AS count
             FROM votes GROUP BY votes.userid",
        )
        .unwrap();
        let schema = HashMap::from([("votes".into(), vec!["aid".into(), "userid".into()])]);

        q.expand_implied_tables(&schema, readyset_sql::Dialect::MySQL)
            .unwrap();
        assert_eq!(q, expected);
    }

    #[test]
    fn in_cte() {
        let mut q = parse_query(
            Dialect::MySQL,
            "With votes AS (SELECT COUNT(id) as id_count, story_id FROM votes GROUP BY story_id )
             SELECT title FROM stories JOIN votes ON stories.id = votes.story_id",
        )
        .unwrap();
        let expected = parse_query(

Dialect::MySQL,
            "With votes AS(SELECT COUNT(votes.id) as id_count, votes.story_id FROM votes GROUP BY votes.story_id )
             SELECT stories.title FROM stories JOIN votes ON stories.id = votes.story_id",
        )
        .unwrap();
        let schema = HashMap::from([
            ("votes".into(), vec!["story_id".into(), "id".into()]),
            ("stories".into(), vec!["id".into(), "title".into()]),
        ]);

        q.expand_implied_tables(&schema, readyset_sql::Dialect::MySQL)
            .unwrap();
        assert_eq!(q, expected);
    }

    #[test]
    fn referencing_cte() {
        let mut q = parse_query(
            Dialect::MySQL,
            "With votes AS (SELECT COUNT(id) as count, story_id FROM votes GROUP BY story_id )
             SELECT count, title FROM stories JOIN votes ON stories.id = votes.story_id",
        )
        .unwrap();
        let expected = parse_query(
            Dialect::MySQL,
            "With votes AS (SELECT COUNT(votes.id) as count, votes.story_id FROM votes GROUP BY votes.story_id )
             SELECT votes.count, stories.title FROM stories JOIN votes ON stories.id = votes.story_id",
        )
        .unwrap();
        let schema = HashMap::from([
            ("votes".into(), vec!["story_id".into(), "id".into()]),
            ("stories".into(), vec!["id".into(), "title".into()]),
        ]);

        q.expand_implied_tables(&schema, readyset_sql::Dialect::MySQL)
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
    fn non_schema_qualified_column() {
        let mut q = parse_query(Dialect::MySQL, "SELECT votes.id from s1.votes").unwrap();
        let expected = parse_query(Dialect::MySQL, "SELECT s1.votes.id FROM s1.votes").unwrap();
        let schema = [(
            Relation {
                schema: Some("s1".into()),
                name: "votes".into(),
            },
            vec!["id".into()],
        )]
        .into();
        q.expand_implied_tables(&schema, readyset_sql::Dialect::MySQL)
            .unwrap();
        assert_eq!(
            q,
            expected,
            "\n{} != {}",
            q.display(readyset_sql::Dialect::MySQL),
            expected.display(readyset_sql::Dialect::MySQL)
        );
    }

    #[test]
    fn ambiguous_non_schema_qualified_table_reference() {
        let mut q = parse_query(Dialect::MySQL, "SELECT t.id from s1.t, s2.t").unwrap();
        let schema = [
            (
                Relation {
                    schema: Some("s1".into()),
                    name: "t".into(),
                },
                vec!["id".into()],
            ),
            (
                Relation {
                    schema: Some("s2".into()),
                    name: "t".into(),
                },
                vec!["id".into()],
            ),
        ]
        .into();
        q.expand_implied_tables(&schema, readyset_sql::Dialect::MySQL)
            .unwrap_err();
    }

    #[test]
    fn votes() {
        let mut q = parse_query(
            Dialect::MySQL,
            "SELECT id, author, title, url, vcount
            FROM stories
            JOIN (SELECT story_id, COUNT(*) AS vcount
                        FROM votes GROUP BY story_id)
            AS VoteCount
            ON VoteCount.story_id = stories.id WHERE stories.id = ?;",
        )
        .unwrap();
        let expected = parse_query(
            Dialect::MySQL,
            "SELECT stories.id, stories.author, stories.title, stories.url, VoteCount.vcount
            FROM stories
            JOIN (SELECT votes.story_id, COUNT(*) AS vcount
                        FROM votes GROUP BY votes.story_id)
            AS VoteCount
            ON VoteCount.story_id = stories.id WHERE stories.id = ?;",
        )
        .unwrap();
        let schema = [
            (
                Relation {
                    schema: None,
                    name: "stories".into(),
                },
                vec!["id".into(), "author".into(), "title".into(), "url".into()],
            ),
            (
                Relation {
                    schema: None,
                    name: "votes".into(),
                },
                vec!["user".into(), "story_id".into()],
            ),
        ]
        .into();
        q.expand_implied_tables(&schema, readyset_sql::Dialect::MySQL)
            .unwrap();
        assert_eq!(
            q,
            expected,
            "\n left: {}\nright: {}",
            q.display(readyset_sql::Dialect::MySQL),
            expected.display(readyset_sql::Dialect::MySQL)
        );
    }

    #[test]
    fn column_referencing_aliased_table() {
        let mut q = parse_query(
            Dialect::MySQL,
            "SELECT bl.time, bl.name, s.ip, s.port
             FROM sb_banlog AS bl
             LEFT JOIN sb_servers AS s
             ON (s.sid = bl.sid)
             WHERE (bid = $1)",
        )
        .unwrap();
        let expected = parse_query(
            Dialect::MySQL,
            "SELECT bl.time, bl.name, s.ip, s.port
             FROM sb_banlog AS bl
             LEFT JOIN sb_servers AS s
             ON (s.sid = bl.sid)
             WHERE (bl.bid = $1)",
        )
        .unwrap();

        let schema = [
            (
                Relation::from("sb_banlog"),
                vec!["sid".into(), "time".into(), "name".into(), "bid".into()],
            ),
            (
                Relation::from("sb_servers"),
                vec![
                    "sid".into(),
                    "ip".into(),
                    "port".into(),
                    "rcon".into(),
                    "modid".into(),
                    "enabled".into(),
                ],
            ),
        ]
        .into();

        q.expand_implied_tables(&schema, readyset_sql::Dialect::MySQL)
            .unwrap();
        assert_eq!(
            q,
            expected,
            "\n left: {}\nright: {}",
            q.display(readyset_sql::Dialect::MySQL),
            expected.display(readyset_sql::Dialect::MySQL)
        );
    }

    #[test]
    fn select_from_subquery() {
        let mut q = parse_query(Dialect::MySQL, "SELECT x FROM (SELECT x FROM t1) sq").unwrap();
        let expected =
            parse_query(Dialect::MySQL, "SELECT sq.x FROM (SELECT t1.x FROM t1) sq").unwrap();
        let schema = [(Relation::from("t1"), vec!["x".into()])].into();

        q.expand_implied_tables(&schema, readyset_sql::Dialect::MySQL)
            .unwrap();
        assert_eq!(
            q,
            expected,
            "\n left: {}\nright: {}",
            q.display(readyset_sql::Dialect::MySQL),
            expected.display(readyset_sql::Dialect::MySQL)
        );
    }
}
