use std::collections::HashMap;
use std::mem;

use itertools::Itertools;
use nom_sql::analysis::visit::{walk_select_statement, Visitor};
use nom_sql::{Column, CommonTableExpression, JoinRightSide, SelectStatement, SqlQuery, Table};

#[derive(Debug, PartialEq)]
pub enum TableAliasRewrite {
    /// An alias to a base table was rewritten
    Table { from: String, to_table: String },

    /// An alias to a view was rewritten
    View {
        from: String,
        to_view: String,
        for_table: String,
    },

    /// An alias to a common table expression was rewritten
    Cte {
        from: String,
        to_view: String,
        for_statement: Box<SelectStatement>, // box for perf
    },
}

pub trait AliasRemoval {
    /// Remove all table aliases, leaving tables unaliased if possible but rewriting the table name
    /// to a new view name derived from 'query_name' when necessary (ie when a single table is
    /// referenced by more than one alias). Return a list of the rewrites performed.
    fn rewrite_table_aliases(&mut self, query_name: &str) -> Vec<TableAliasRewrite>;
}

struct RemoveAliasesVisitor<'a> {
    query_name: &'a str,
    table_remap: HashMap<String, String>,
    col_table_remap: HashMap<String, String>,
    out: Vec<TableAliasRewrite>,
}

impl<'ast, 'a> Visitor<'ast> for RemoveAliasesVisitor<'a> {
    type Error = !;

    fn visit_select_statement(
        &mut self,
        select_statement: &'ast mut SelectStatement,
    ) -> Result<(), Self::Error> {
        // Identify the unique table references for every table appearing in the query FROM and
        // JOIN clauses, and group by table name. Both None (ie unaliased) and Some(alias)
        // reference types are included.
        let table_refs = select_statement
            .tables
            .iter()
            .cloned()
            .chain(select_statement.join.iter().flat_map(|j| match j.right {
                JoinRightSide::Table(ref table) => vec![table.clone()],
                JoinRightSide::Tables(ref ts) => ts.clone(),
                _ => vec![],
            }))
            .map(|t| (t.name, t.alias))
            .unique()
            .into_group_map();

        // Use the map of unique table references to identify any necessary alias rewrites.
        let table_alias_rewrites: Vec<TableAliasRewrite> = table_refs
            .into_iter()
            .flat_map(|(name, aliases)| match aliases[..] {
                [None] => {
                    // The table is never referred to by an alias. No rewrite is needed.
                    vec![]
                }

                [Some(ref alias)] => {
                    // The table is only ever referred to using one specific alias. Rewrite
                    // to remove the alias and refer to the table itself.
                    vec![TableAliasRewrite::Table {
                        from: alias.clone(),
                        to_table: name,
                    }]
                }

                _ => aliases
                    .into_iter()
                    .flatten()
                    .map(|alias| {
                        // The alias is one among multiple distinct references to the
                        // table. Create a globally unique view name, derived from the
                        // query name, and rewrite to remove the alias and refer to this
                        // view.
                        TableAliasRewrite::View {
                            from: alias.clone(),
                            to_view: format!("__{}__{}", self.query_name, alias),
                            for_table: name.clone(),
                        }
                    })
                    .collect(),
            })
            .chain(select_statement.ctes.drain(..).map(
                |CommonTableExpression { name, statement }| TableAliasRewrite::Cte {
                    to_view: format!("__{}__{}", self.query_name, name),
                    from: name,
                    for_statement: Box::new(statement),
                },
            ))
            .collect();

        // Extract remappings for FROM and JOIN table references from the alias rewrites.
        let new_table_remap = self
            .table_remap
            .clone()
            .into_iter()
            .chain(table_alias_rewrites.iter().filter_map(|r| match r {
                TableAliasRewrite::View { from, to_view, .. } => {
                    Some((from.clone(), to_view.clone()))
                }
                _ => None,
            }))
            .collect();
        let orig_table_remap = mem::replace(&mut self.table_remap, new_table_remap);

        // Extract remappings for column tables from the alias rewrites.
        let new_col_table_remap = self
            .col_table_remap
            .clone()
            .into_iter()
            .chain(table_alias_rewrites.iter().map(|r| match r {
                TableAliasRewrite::Table { from, to_table } => (from.clone(), to_table.clone()),
                TableAliasRewrite::View { from, to_view, .. } => (from.clone(), to_view.clone()),
                TableAliasRewrite::Cte { from, to_view, .. } => (from.clone(), to_view.clone()),
            }))
            .collect();
        let orig_col_table_remap = mem::replace(&mut self.col_table_remap, new_col_table_remap);

        walk_select_statement(self, select_statement)?;

        self.table_remap = orig_table_remap;
        self.col_table_remap = orig_col_table_remap;

        self.out.extend(table_alias_rewrites);

        Ok(())
    }

    fn visit_table(&mut self, table: &'ast mut Table) -> Result<(), Self::Error> {
        if let Some(name) = table
            .alias
            .as_ref()
            .and_then(|t| self.table_remap.get(t))
            .cloned()
        {
            table.name = name
        } else if let Some(name) = self.col_table_remap.get(&table.name) {
            table.name = name.clone();
        }
        table.alias = None;

        Ok(())
    }

    fn visit_column(&mut self, column: &'ast mut Column) -> Result<(), Self::Error> {
        if let Some(table) = column
            .table
            .as_ref()
            .and_then(|t| self.col_table_remap.get(t))
            .cloned()
        {
            column.table = Some(table)
        }

        Ok(())
    }
}

impl AliasRemoval for SqlQuery {
    fn rewrite_table_aliases(&mut self, query_name: &str) -> Vec<TableAliasRewrite> {
        if let SqlQuery::Select(ref mut sq) = self {
            let mut visitor = RemoveAliasesVisitor {
                query_name,
                table_remap: Default::default(),
                col_table_remap: Default::default(),
                out: Default::default(),
            };

            let Ok(_) = visitor.visit_select_statement(sq);

            visitor.out
        } else {
            // nothing to do for other query types, as they cannot have aliases
            vec![]
        }
    }
}

#[cfg(test)]
mod tests {
    use std::convert::TryInto;

    use nom_sql::{
        parse_query, parser, BinaryOperator, Column, Dialect, Expression,
        FieldDefinitionExpression, ItemPlaceholder, JoinClause, JoinConstraint, JoinOperator,
        JoinRightSide, Literal, SelectStatement, SqlQuery, Table,
    };

    use super::{AliasRemoval, TableAliasRewrite};

    macro_rules! rewrites_to {
        ($before: expr, $after: expr) => {{
            let mut res = parse_query(Dialect::MySQL, $before).unwrap();
            let expected = parse_query(Dialect::MySQL, $after).unwrap();
            res.rewrite_table_aliases("query");
            assert_eq!(
                res, expected,
                "\n     expected: {} \n\
                 to rewrite to: {} \n\
                       but got: {}",
                $before, expected, res,
            );
        }};
    }

    #[test]
    fn it_removes_aliases() {
        let q = SelectStatement {
            tables: vec![Table {
                name: String::from("PaperTag"),
                alias: Some(String::from("t")),
                schema: None,
            }],
            fields: vec![FieldDefinitionExpression::from(Column::from("t.id"))],
            where_clause: Some(Expression::BinaryOp {
                lhs: Box::new(Expression::Column(Column::from("t.id"))),
                op: BinaryOperator::Equal,
                rhs: Box::new(Expression::Literal(Literal::Placeholder(
                    ItemPlaceholder::QuestionMark,
                ))),
            }),
            ..Default::default()
        };
        let mut res = SqlQuery::Select(q);
        let rewrites = res.rewrite_table_aliases("query");
        // Table alias removed in field list
        match res {
            SqlQuery::Select(tq) => {
                assert_eq!(
                    tq.fields,
                    vec![FieldDefinitionExpression::from(Column::from("PaperTag.id"))]
                );
                assert_eq!(
                    tq.where_clause,
                    Some(Expression::BinaryOp {
                        lhs: Box::new(Expression::Column(Column::from("PaperTag.id"))),
                        op: BinaryOperator::Equal,
                        rhs: Box::new(Expression::Literal(Literal::Placeholder(
                            ItemPlaceholder::QuestionMark
                        ))),
                    })
                );
                assert_eq!(
                    tq.tables,
                    vec![Table {
                        name: String::from("PaperTag"),
                        alias: None,
                        schema: None,
                    }]
                );
            }
            // if we get anything other than a selection query back, something really weird is up
            _ => panic!(),
        }

        assert_eq!(
            rewrites,
            vec![TableAliasRewrite::Table {
                from: String::from("t"),
                to_table: String::from("PaperTag")
            }]
        );
    }

    #[test]
    fn it_removes_nested_aliases() {
        use nom_sql::{BinaryOperator, Expression};

        let col_small = Column {
            name: "count(t.id)".try_into().unwrap(),
            table: None,
        };
        let col_full = Column {
            name: "count(t.id)".try_into().unwrap(),
            table: None,
        };
        let q = SelectStatement {
            tables: vec![Table {
                name: String::from("PaperTag"),
                alias: Some(String::from("t")),
                schema: None,
            }],
            fields: vec![FieldDefinitionExpression::from(col_small.clone())],
            where_clause: Some(Expression::BinaryOp {
                op: BinaryOperator::Equal,
                lhs: Box::new(Expression::Column(col_small)),
                rhs: Box::new(Expression::Literal(Literal::Placeholder(
                    ItemPlaceholder::QuestionMark,
                ))),
            }),
            ..Default::default()
        };
        let mut res = SqlQuery::Select(q);
        let rewrites = res.rewrite_table_aliases("query");
        // Table alias removed in field list
        match res {
            SqlQuery::Select(tq) => {
                assert_eq!(
                    tq.fields,
                    vec![FieldDefinitionExpression::from(col_full.clone())]
                );
                assert_eq!(
                    tq.where_clause,
                    Some(Expression::BinaryOp {
                        op: BinaryOperator::Equal,
                        lhs: Box::new(Expression::Column(col_full)),
                        rhs: Box::new(Expression::Literal(Literal::Placeholder(
                            ItemPlaceholder::QuestionMark
                        ))),
                    })
                );
                assert_eq!(
                    tq.tables,
                    vec![Table {
                        name: String::from("PaperTag"),
                        alias: None,
                        schema: None,
                    }]
                );
            }
            // if we get anything other than a selection query back, something really weird is up
            _ => panic!(),
        }

        assert_eq!(
            rewrites,
            vec![TableAliasRewrite::Table {
                from: String::from("t"),
                to_table: String::from("PaperTag")
            }]
        );
    }

    #[test]
    fn it_rewrites_duplicate_aliases() {
        let mut res = parser::parse_query(
            Dialect::MySQL,
            "SELECT t1.id, t2.name FROM tab t1 JOIN tab t2 ON (t1.other = t2.id)",
        )
        .unwrap();
        let rewrites = res.rewrite_table_aliases("query_name");
        match res {
            SqlQuery::Select(tq) => {
                assert_eq!(
                    tq.fields,
                    vec![
                        FieldDefinitionExpression::from(Column::from("__query_name__t1.id")),
                        FieldDefinitionExpression::from(Column::from("__query_name__t2.name"))
                    ]
                );
                assert_eq!(
                    tq.tables,
                    vec![Table {
                        name: String::from("__query_name__t1"),
                        alias: None,
                        schema: None,
                    }]
                );
                assert_eq!(
                    tq.join,
                    vec![JoinClause {
                        operator: JoinOperator::Join,
                        right: JoinRightSide::Table(Table {
                            name: String::from("__query_name__t2"),
                            alias: None,
                            schema: None,
                        }),
                        constraint: JoinConstraint::On(Expression::BinaryOp {
                            op: BinaryOperator::Equal,
                            lhs: Box::new(Expression::Column(Column::from(
                                "__query_name__t1.other"
                            ))),
                            rhs: Box::new(Expression::Column(Column::from("__query_name__t2.id")))
                        })
                    }]
                );
            }
            // if we get anything other than a selection query back, something really weird is up
            _ => panic!(),
        }

        assert_eq!(
            rewrites,
            vec![
                TableAliasRewrite::View {
                    from: String::from("t1"),
                    to_view: String::from("__query_name__t1"),
                    for_table: String::from("tab")
                },
                TableAliasRewrite::View {
                    from: String::from("t2"),
                    to_view: String::from("__query_name__t2"),
                    for_table: String::from("tab")
                }
            ]
        );
    }

    #[test]
    fn aliases_in_between() {
        rewrites_to!(
            "SELECT id FROM tbl t1 WHERE t1.value BETWEEN 1 AND 6",
            "SELECT id FROM tbl WHERE tbl.value BETWEEN 1 AND 6"
        );
    }

    #[test]
    fn aliases_in_condition_arithmetic() {
        rewrites_to!(
            "SELECT id FROM tbl t1 WHERE t1.x - t1.y > 0",
            "SELECT id FROM tbl WHERE tbl.x - tbl.y > 0"
        );
    }

    #[test]
    fn joined_subquery() {
        rewrites_to!(
            "SELECT
                 u.id, post_count.count
             FROM users u
             JOIN (
                 SELECT p.author_id, count(p.id) AS count
                 FROM posts p
                 GROUP BY p.author_id
             ) post_count
             ON u.id = post_count.author_id",
            "SELECT
                 users.id, post_count.count
             FROM users
             JOIN (
                 SELECT posts.author_id, count(posts.id) AS count
                 FROM posts
                 GROUP BY posts.author_id
             ) post_count
             ON users.id = post_count.author_id"
        )
    }

    #[test]
    fn correlated_subquery() {
        rewrites_to!(
            "SELECT u.id
             FROM users u
             WHERE EXISTS (select p.id from posts p where p.author_id = u.id)",
            "SELECT users.id
             FROM users
             WHERE EXISTS (select posts.id from posts where posts.author_id = users.id)"
        )
    }

    #[test]
    fn cte() {
        let mut res = parse_query(
            Dialect::MySQL,
            "WITH max_val AS (SELECT max(t1.value) as value FROM t1)
             SELECT t2.name FROM t2 JOIN max_val ON max_val.value = t2.value;",
        )
        .unwrap();
        let expected = parse_query(
            Dialect::MySQL,
            "SELECT t2.name FROM t2 JOIN __query__max_val ON __query__max_val.value = t2.value;",
        )
        .unwrap();
        let rewritten = res.rewrite_table_aliases("query");
        assert_eq!(
            rewritten,
            vec![TableAliasRewrite::Cte {
                from: "max_val".to_owned(),
                to_view: "__query__max_val".to_owned(),
                for_statement: match parse_query(
                    Dialect::MySQL,
                    "SELECT max(t1.value) as value FROM t1"
                )
                .unwrap()
                {
                    SqlQuery::Select(stmt) => Box::new(stmt),
                    _ => panic!(),
                }
            }]
        );
        assert_eq!(res, expected, "\n\n   {}\n!= {}", res, expected);
    }
}
