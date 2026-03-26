use std::collections::{HashMap, HashSet};
use std::iter;
use std::mem;

use itertools::{Either, Itertools};
use readyset_errors::{ReadySetError, ReadySetResult, internal, invalid_query};
use readyset_sql::analysis::visit_mut::{VisitorMut, walk_function_expr, walk_select_statement};
use readyset_sql::ast::{
    Column, Expr, FieldDefinitionExpr, FieldReference, FunctionExpr, GroupByClause, JoinConstraint,
    JoinRightSide, OrderClause, Relation, SelectStatement, SqlIdentifier, SqlQuery, TableExprInner,
};
use readyset_sql::{Dialect, DialectDisplay};
use tracing::warn;

use crate::rewrite_utils::get_from_item_reference_name;
use crate::{RewriteDialectContext, get_local_from_items_iter, outermost_table_exprs, util};

pub trait ImpliedTablesContext: RewriteDialectContext {
    /// An exhaustive list of all view and table schemas in the database.
    // TODO(mvzink): Find a better way to do this
    fn all_schemas(&self) -> impl IntoIterator<Item = (Relation, Vec<SqlIdentifier>)>;
}

impl<I: ImpliedTablesContext> ImpliedTablesContext for &I {
    fn all_schemas(&self) -> impl IntoIterator<Item = (Relation, Vec<SqlIdentifier>)> {
        (*self).all_schemas()
    }
}

pub trait ImpliedTableExpansion: Sized {
    fn expand_implied_tables<I: ImpliedTablesContext>(
        &mut self,
        context: I,
    ) -> ReadySetResult<&mut Self>;
}

#[derive(Debug)]
struct ExpandImpliedTablesVisitor<I: ImpliedTablesContext> {
    context: I,
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
    dialect: Dialect,
    /// Tables from outer scopes that are visible inside LATERAL subqueries.
    /// Accumulated when entering a LATERAL subquery; cleared for non-LATERAL subqueries.
    outer_tables: HashMap<Relation, Relation>,
    /// Subquery schemas from outer scopes that are visible inside LATERAL subqueries.
    outer_subquery_schemas: HashMap<SqlIdentifier, Vec<SqlIdentifier>>,
}

impl<I: ImpliedTablesContext> ExpandImpliedTablesVisitor<I> {
    /// Resolve an unqualified column name to its table. Checks the current scope first;
    /// if no match is found and outer-scope tables are visible (LATERAL), checks those.
    fn find_table(&self, column_name: &str) -> Option<Relation> {
        if let Some(t) = self.find_table_in_scope(&self.tables, &self.subquery_schemas, column_name)
        {
            return Some(t);
        }
        // Fall back to outer scope (populated only inside LATERAL subqueries).
        if !self.outer_tables.is_empty() {
            return self.find_table_in_scope(
                &self.outer_tables,
                &self.outer_subquery_schemas,
                column_name,
            );
        }
        None
    }

    fn find_table_in_scope(
        &self,
        tables: &HashMap<Relation, Relation>,
        subquery_schemas: &HashMap<SqlIdentifier, Vec<SqlIdentifier>>,
        column_name: &str,
    ) -> Option<Relation> {
        let mut matches = self
            .context
            .all_schemas()
            .into_iter()
            .chain(
                subquery_schemas
                    .iter()
                    .map(|(n, fs)| (Relation::from(n.clone()), fs.clone())),
            )
            .filter_map(|(t, ws)| tables.get(&t).cloned().map(|t| (t, ws)))
            .filter_map(|(t, ws)| {
                let num_matching = ws.iter().filter(|c| **c == column_name).count();
                // Qualify the column with its table as long as at least one
                // projected name matches.  When there are duplicates
                // (num_matching > 1), qualifying still lets the downstream
                // semantic validator (`validate_no_duplicate_derived_table_columns`)
                // detect and report the ambiguity with a proper user-facing error.
                // Leaving the column unqualified would instead trigger the
                // pipeline-invariants check ("Unresolved column"), which is an
                // internal error that skips the semantic validator entirely.
                if num_matching > 0 { Some(t) } else { None }
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
            None
        } else {
            Some(matches.pop().unwrap())
        }
    }
}

impl<'ast, S: ImpliedTablesContext> VisitorMut<'ast> for ExpandImpliedTablesVisitor<S> {
    type Error = ReadySetError;

    fn visit_select_statement(
        &mut self,
        select_statement: &'ast mut SelectStatement,
    ) -> Result<(), Self::Error> {
        // Reject duplicate effective aliases in FROM — they cause silent wrong
        // column qualification via HashMap key collisions in `tables`.
        {
            let mut seen = HashSet::new();
            for te in get_local_from_items_iter!(select_statement) {
                let name = get_from_item_reference_name(te)?;
                if !seen.insert(name.clone()) {
                    invalid_query!("Not unique table/alias: {}", name.display(self.dialect));
                }
            }
        }

        // For LATERAL subqueries, accumulate the current scope into outer_tables
        // so that correlated column references can resolve against the outer scope.
        // For non-LATERAL subqueries, clear outer scope (they cannot see outer tables).
        let orig_outer_tables;
        let orig_outer_subquery_schemas;
        if select_statement.lateral {
            let mut new_outer_tables: HashMap<Relation, Relation> = self.outer_tables.clone();
            new_outer_tables.extend(self.tables.iter().map(|(k, v)| (k.clone(), v.clone())));
            orig_outer_tables = mem::replace(&mut self.outer_tables, new_outer_tables);

            let mut new_outer_sq: HashMap<SqlIdentifier, Vec<SqlIdentifier>> =
                self.outer_subquery_schemas.clone();
            new_outer_sq.extend(
                self.subquery_schemas
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone())),
            );
            orig_outer_subquery_schemas =
                mem::replace(&mut self.outer_subquery_schemas, new_outer_sq);
        } else {
            orig_outer_tables = mem::take(&mut self.outer_tables);
            orig_outer_subquery_schemas = mem::take(&mut self.outer_subquery_schemas);
        }

        let orig_tables = mem::replace(
            &mut self.tables,
            outermost_table_exprs(select_statement)
                .filter_map(|tbl| {
                    Some((
                        match &tbl.inner {
                            TableExprInner::Table(t) => t.clone(),
                            TableExprInner::Subquery(_) => tbl.alias.clone()?.into(),
                            TableExprInner::Values { .. } => tbl.alias.clone()?.into(),
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
        self.outer_tables = orig_outer_tables;
        self.outer_subquery_schemas = orig_outer_subquery_schemas;

        Ok(())
    }

    fn visit_having_clause(&mut self, expr: &'ast mut Expr) -> Result<(), Self::Error> {
        // If the parser accepted a bare alias reference in HAVING, respect it.
        // MySQL allows this natively; PostgreSQL is stricter, but the parser
        // enforces that — by the time we see the AST, any bare alias that
        // survived parsing is intentional.
        self.can_reference_aliases = true;
        self.visit_expr(expr)?;
        self.can_reference_aliases = false;
        Ok(())
    }

    fn visit_order_clause(&mut self, order: &'ast mut OrderClause) -> Result<(), Self::Error> {
        // Only top-level bare columns may be SELECT-alias references.
        // Columns nested inside expressions (e.g., SUM(col)) must be qualified
        // as table columns, even when the column name matches a SELECT alias.
        for ord_by in order.order_by.iter_mut() {
            self.can_reference_aliases = matches!(
                &ord_by.field,
                FieldReference::Expr(Expr::Column(Column { table: None, .. }))
            );
            self.visit_field_reference(&mut ord_by.field)?;
        }
        self.can_reference_aliases = false;
        Ok(())
    }

    fn visit_group_by_clause(
        &mut self,
        group_by: &'ast mut GroupByClause,
    ) -> Result<(), Self::Error> {
        // Same logic as ORDER BY: only top-level bare columns may reference aliases.
        for field in group_by.fields.iter_mut() {
            self.can_reference_aliases = matches!(
                field,
                FieldReference::Expr(Expr::Column(Column { table: None, .. }))
            );
            self.visit_field_reference(field)?;
        }
        self.can_reference_aliases = false;
        Ok(())
    }

    fn visit_function_expr(
        &mut self,
        function_expr: &'ast mut FunctionExpr,
    ) -> Result<(), Self::Error> {
        // Inside a function call (e.g., SUM(col)), column references are always
        // table columns, never SELECT aliases.  Temporarily disable alias
        // recognition so that columns inside aggregates get properly qualified.
        let saved = self.can_reference_aliases;
        self.can_reference_aliases = false;
        walk_function_expr(self, function_expr)?;
        self.can_reference_aliases = saved;
        Ok(())
    }

    fn visit_join_constraint(
        &mut self,
        join_constraint: &'ast mut JoinConstraint,
    ) -> Result<(), Self::Error> {
        match join_constraint {
            // ON expressions contain normal column references that need qualification.
            JoinConstraint::On(expr) => self.visit_expr(expr),
            // USING columns are bare column *names* (not references) that identify
            // a column present on both sides of the join.  They must NOT be qualified
            // here — `expand_join_on_using` (which runs next) reads only `.name` and
            // builds fresh, correctly-qualified ON predicates.  Qualifying them would
            // trigger a spurious "Ambiguous column" warning in `find_table` (the name
            // exists on both sides by design) and attach an arbitrary table.
            JoinConstraint::Using(_) => Ok(()),
            JoinConstraint::Empty => Ok(()),
        }
    }

    fn visit_column(&mut self, column: &'ast mut Column) -> Result<(), Self::Error> {
        if self.can_reference_aliases && self.aliases.contains(&column.name) {
            return Ok(());
        }

        if let Some(table) = &mut column.table {
            if table.schema.is_some() {
                return Ok(());
            }

            // Check current scope, then fall back to outer scope (LATERAL).
            let matches = self
                .tables
                .iter()
                .filter(|(t, _alias)| t.name == table.name)
                .map(|(_t, alias)| alias)
                .collect::<Vec<_>>();

            if matches.len() > 1 {
                invalid_query!(
                    "Table reference {} is ambiguous",
                    table.display(self.dialect)
                );
            }

            if let Some(t) = matches.first() {
                table.schema.clone_from(&t.schema);
            } else if !self.outer_tables.is_empty() {
                let outer_matches = self
                    .outer_tables
                    .iter()
                    .filter(|(t, _alias)| t.name == table.name)
                    .map(|(_t, alias)| alias)
                    .collect::<Vec<_>>();

                if outer_matches.len() > 1 {
                    invalid_query!(
                        "Table reference {} is ambiguous",
                        table.display(self.dialect)
                    );
                }

                if let Some(t) = outer_matches.first() {
                    table.schema.clone_from(&t.schema);
                }
            }
        } else {
            column.table = self.find_table(&column.name);
        }

        Ok(())
    }
}

fn rewrite_select<S: ImpliedTablesContext>(
    select_statement: &mut SelectStatement,
    context: S,
) -> ReadySetResult<&mut SelectStatement> {
    let dialect = context.dialect().into();
    let mut visitor = ExpandImpliedTablesVisitor {
        context,
        subquery_schemas: Default::default(),
        tables: Default::default(),
        aliases: Default::default(),
        can_reference_aliases: false,
        dialect,
        outer_tables: Default::default(),
        outer_subquery_schemas: Default::default(),
    };

    visitor.visit_select_statement(select_statement)?;
    Ok(select_statement)
}

impl ImpliedTableExpansion for SelectStatement {
    fn expand_implied_tables<S: ImpliedTablesContext>(
        &mut self,
        context: S,
    ) -> ReadySetResult<&mut Self> {
        rewrite_select(self, context)
    }
}

impl ImpliedTableExpansion for SqlQuery {
    fn expand_implied_tables<S: ImpliedTablesContext>(
        &mut self,
        context: S,
    ) -> ReadySetResult<&mut SqlQuery> {
        match self {
            SqlQuery::CreateTable(..) => {}
            SqlQuery::CompoundSelect(csq) => {
                for (_op, select) in &mut csq.selects {
                    rewrite_select(select, &context)?;
                }
            }
            SqlQuery::Select(sq) => {
                sq.expand_implied_tables(context)?;
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
    use readyset_sql_parsing::{ParsingPreset, parse_query, parse_query_with_config};

    use super::*;

    struct TestImpliedTablesContext {
        schema: HashMap<Relation, Vec<SqlIdentifier>>,
        dialect: Dialect,
    }

    impl RewriteDialectContext for TestImpliedTablesContext {
        fn dialect(&self) -> readyset_data::Dialect {
            self.dialect.into()
        }
    }

    impl ImpliedTablesContext for TestImpliedTablesContext {
        fn all_schemas(&self) -> impl IntoIterator<Item = (Relation, Vec<SqlIdentifier>)> {
            self.schema.clone()
        }
    }

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
            .expand_implied_tables(TestImpliedTablesContext {
                schema,
                dialect: Dialect::MySQL,
            })
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

        q.expand_implied_tables(TestImpliedTablesContext {
            schema,
            dialect: Dialect::MySQL,
        })
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

        q.expand_implied_tables(TestImpliedTablesContext {
            schema,
            dialect: Dialect::MySQL,
        })
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

        q.expand_implied_tables(TestImpliedTablesContext {
            schema,
            dialect: Dialect::MySQL,
        })
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

        q.expand_implied_tables(TestImpliedTablesContext {
            schema,
            dialect: Dialect::MySQL,
        })
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

        q.expand_implied_tables(TestImpliedTablesContext {
            schema,
            dialect: Dialect::MySQL,
        })
        .unwrap();
        assert_eq!(
            q,
            expected,
            "{} != {}",
            q.display(Dialect::MySQL),
            expected.display(Dialect::MySQL)
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
        q.expand_implied_tables(TestImpliedTablesContext {
            schema,
            dialect: Dialect::MySQL,
        })
        .unwrap();
        assert_eq!(
            q,
            expected,
            "\n{} != {}",
            q.display(Dialect::MySQL),
            expected.display(Dialect::MySQL)
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
        q.expand_implied_tables(TestImpliedTablesContext {
            schema,
            dialect: Dialect::MySQL,
        })
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
        q.expand_implied_tables(TestImpliedTablesContext {
            schema,
            dialect: Dialect::MySQL,
        })
        .unwrap();
        assert_eq!(
            q,
            expected,
            "\n left: {}\nright: {}",
            q.display(Dialect::MySQL),
            expected.display(Dialect::MySQL)
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

        q.expand_implied_tables(TestImpliedTablesContext {
            schema,
            dialect: Dialect::MySQL,
        })
        .unwrap();
        assert_eq!(
            q,
            expected,
            "\n left: {}\nright: {}",
            q.display(Dialect::MySQL),
            expected.display(Dialect::MySQL)
        );
    }

    #[test]
    fn select_from_subquery() {
        let mut q = parse_query(Dialect::MySQL, "SELECT x FROM (SELECT x FROM t1) sq").unwrap();
        let expected =
            parse_query(Dialect::MySQL, "SELECT sq.x FROM (SELECT t1.x FROM t1) sq").unwrap();
        let schema = [(Relation::from("t1"), vec!["x".into()])].into();

        q.expand_implied_tables(TestImpliedTablesContext {
            schema,
            dialect: Dialect::MySQL,
        })
        .unwrap();
        assert_eq!(
            q,
            expected,
            "\n left: {}\nright: {}",
            q.display(Dialect::MySQL),
            expected.display(Dialect::MySQL)
        );
    }

    #[test]
    fn aggregate_order_by() {
        let mut q = parse_query_with_config(
            ParsingPreset::OnlySqlparser,
            Dialect::PostgreSQL,
            "SELECT array_agg(t1.x ORDER BY x ASC) FROM t1",
        )
        .unwrap();
        let expected = parse_query_with_config(
            ParsingPreset::OnlySqlparser,
            Dialect::PostgreSQL,
            "SELECT array_agg(t1.x ORDER BY t1.x ASC) FROM t1",
        )
        .unwrap();
        let schema = [(Relation::from("t1"), vec!["x".into()])].into();

        q.expand_implied_tables(TestImpliedTablesContext {
            schema,
            dialect: Dialect::PostgreSQL,
        })
        .unwrap();
        assert_eq!(
            q,
            expected,
            "\n left: {}\nright: {}",
            q.display(Dialect::PostgreSQL),
            expected.display(Dialect::PostgreSQL)
        );
    }

    /// MySQL: Outer ORDER BY SUM(test_dec) must qualify test_dec inside the
    /// aggregate to the derived-table column (t.test_dec), not skip it as a
    /// SELECT alias.  Inner HAVING uses a bare alias reference (test_dec > ...)
    /// which MySQL allows — it should stay unqualified.
    #[test]
    fn order_by_agg_over_aliased_column_having_alias_ref() {
        let schema: HashMap<Relation, Vec<SqlIdentifier>> = [(
            Relation {
                schema: Some("qa".into()),
                name: "datatypes".into(),
            },
            vec!["rownum".into(), "test_dec".into()],
        )]
        .into();

        let mut q = parse_query_with_config(
            ParsingPreset::OnlySqlparser,
            Dialect::MySQL,
            r#"SELECT rownum, test_dec
               FROM (SELECT rownum, SUM(test_dec) AS test_dec
                     FROM qa.datatypes
                     GROUP BY rownum
                     HAVING test_dec > 100000.00 AND rownum > 2) t
               ORDER BY SUM(test_dec)"#,
        )
        .unwrap();

        // Inner: rownum and test_dec inside SUM() qualify to qa.datatypes.
        //        HAVING bare test_dec stays unqualified — MySQL allows
        //        SELECT-alias references in HAVING.
        //        rownum in HAVING is NOT a SELECT alias (no explicit AS),
        //        so it qualifies to qa.datatypes.rownum.
        // Outer: rownum and test_dec in SELECT qualify to t.
        //        ORDER BY SUM(test_dec): test_dec inside SUM qualifies to t
        //        (the only FROM source in scope).
        let expected = parse_query_with_config(
            ParsingPreset::OnlySqlparser,
            Dialect::MySQL,
            r#"SELECT `t`.`rownum`, `t`.`test_dec`
               FROM (SELECT `qa`.`datatypes`.`rownum`,
                            SUM(`qa`.`datatypes`.`test_dec`) AS `test_dec`
                     FROM `qa`.`datatypes`
                     GROUP BY `qa`.`datatypes`.`rownum`
                     HAVING `test_dec` > 100000.00
                        AND `qa`.`datatypes`.`rownum` > 2) `t`
               ORDER BY SUM(`t`.`test_dec`)"#,
        )
        .unwrap();

        q.expand_implied_tables(TestImpliedTablesContext {
            schema,
            dialect: Dialect::MySQL,
        })
        .unwrap();
        assert_eq!(
            q,
            expected,
            "\n left: {}\nright: {}",
            q.display(Dialect::MySQL),
            expected.display(Dialect::MySQL)
        );
    }

    /// Same as above but inner HAVING uses the full aggregate expression
    /// SUM(test_dec) instead of the alias.  test_dec inside SUM is a table
    /// column reference (not an alias) — it must be qualified.
    #[test]
    fn order_by_agg_over_aliased_column_having_agg_expr() {
        let schema: HashMap<Relation, Vec<SqlIdentifier>> = [(
            Relation {
                schema: Some("qa".into()),
                name: "datatypes".into(),
            },
            vec!["rownum".into(), "test_dec".into()],
        )]
        .into();

        let mut q = parse_query_with_config(
            ParsingPreset::OnlySqlparser,
            Dialect::PostgreSQL,
            r#"SELECT rownum, test_dec
               FROM (SELECT rownum, SUM(test_dec) AS test_dec
                     FROM qa.datatypes
                     GROUP BY rownum
                     HAVING SUM(test_dec) > 100000.00 AND rownum > 2) t
               ORDER BY SUM(test_dec)"#,
        )
        .unwrap();

        // Inner HAVING SUM(test_dec): test_dec inside the aggregate is a
        // table column reference → qualifies to qa.datatypes.test_dec.
        // rownum in HAVING is NOT a SELECT alias → also qualifies.
        let expected = parse_query_with_config(
            ParsingPreset::OnlySqlparser,
            Dialect::PostgreSQL,
            r#"SELECT "t"."rownum", "t"."test_dec"
               FROM (SELECT "qa"."datatypes"."rownum",
                            SUM("qa"."datatypes"."test_dec") AS "test_dec"
                     FROM "qa"."datatypes"
                     GROUP BY "qa"."datatypes"."rownum"
                     HAVING SUM("qa"."datatypes"."test_dec") > 100000.00
                        AND "qa"."datatypes"."rownum" > 2) "t"
               ORDER BY SUM("t"."test_dec")"#,
        )
        .unwrap();

        q.expand_implied_tables(TestImpliedTablesContext {
            schema,
            dialect: Dialect::PostgreSQL,
        })
        .unwrap();
        assert_eq!(
            q,
            expected,
            "\n left: {}\nright: {}",
            q.display(Dialect::PostgreSQL),
            expected.display(Dialect::PostgreSQL)
        );
    }

    #[test]
    fn unqualified_correlated_column() {
        let schema: HashMap<Relation, Vec<SqlIdentifier>> = [
            (
                Relation {
                    schema: Some("qa".into()),
                    name: "spj".into(),
                },
                vec!["sn".into(), "qty".into()],
            ),
            (
                Relation {
                    schema: Some("qa".into()),
                    name: "s".into(),
                },
                vec!["sn".into(), "status".into()],
            ),
        ]
        .into();

        let mut q = parse_query_with_config(
            ParsingPreset::OnlySqlparser,
            Dialect::PostgreSQL,
            r#"SELECT spj.sn, Tab3.sn, spj.qty FROM qa.spj,
            LATERAL (SELECT status, sn FROM qa.s WHERE status = qty) AS Tab3
            ORDER BY spj.sn, spj.qty;"#,
        )
        .unwrap();

        let expected = parse_query_with_config(
            ParsingPreset::OnlySqlparser,
            Dialect::PostgreSQL,
            r#"SELECT qa.spj.sn, Tab3.sn, qa.spj.qty FROM qa.spj,
            LATERAL (SELECT qa.s.status, qa.s.sn FROM qa.s WHERE qa.s.status = qa.spj.qty) AS Tab3
            ORDER BY qa.spj.sn, qa.spj.qty;"#,
        )
        .unwrap();

        q.expand_implied_tables(TestImpliedTablesContext {
            schema,
            dialect: Dialect::PostgreSQL,
        })
        .unwrap();
        assert_eq!(
            q,
            expected,
            "\n left: {}\nright: {}",
            q.display(Dialect::PostgreSQL),
            expected.display(Dialect::PostgreSQL)
        );
    }
}
