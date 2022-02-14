use std::collections::{HashMap, HashSet};
use std::mem;

use nom_sql::analysis::visit::{
    walk_group_by_clause, walk_order_clause, walk_select_statement, Visitor,
};
use nom_sql::{Column, FieldDefinitionExpression, SelectStatement, SqlQuery, Table};
use noria_errors::{internal, ReadySetError, ReadySetResult};
use tracing::warn;

use crate::outermost_referred_tables;

pub trait ImpliedTableExpansion {
    fn expand_implied_tables(
        self,
        write_schemas: &HashMap<String, Vec<String>>,
    ) -> ReadySetResult<SqlQuery>;
}

// Sets the table for the `Column` in `f`to `table`. This is mostly useful for CREATE TABLE
// and INSERT queries and deliberately leaves function specifications unaffected, since
// they can refer to remote tables and `set_table` should not be used for queries that have
// computed columns.
fn set_table(mut f: Column, table: &Table) -> ReadySetResult<Column> {
    f.table = match f.table {
        None => Some(table.name.clone()),
        Some(x) => Some(x),
    };
    Ok(f)
}

#[derive(Debug)]
struct ExpandImpliedTablesVisitor<'schema> {
    schema: &'schema HashMap<String, Vec<String>>,
    subquery_schemas: HashMap<String, Vec<String>>,
    tables: HashSet<String>,
    aliases: HashSet<String>,
    // Are we currently in a position in the query that can reference aliases in the projected
    // field list?
    can_reference_aliases: bool,
}

impl<'schema> ExpandImpliedTablesVisitor<'schema> {
    fn find_table(&self, column_name: &str) -> Option<String> {
        let mut matches = self
            .schema
            .iter()
            .chain(self.subquery_schemas.iter())
            .filter(|&(t, _)| self.tables.is_empty() || self.tables.contains(t))
            .filter_map(|(t, ws)| {
                let num_matching = ws.iter().filter(|c| **c == column_name).count();
                assert!(num_matching <= 1);
                if num_matching == 1 {
                    Some(t.to_owned())
                } else {
                    None
                }
            })
            .collect::<Vec<String>>();
        if matches.len() > 1 {
            warn!(
                "Ambiguous column {} exists in tables: {} -- picking a random one",
                column_name,
                matches.as_slice().join(", ")
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

impl<'ast, 'schema> Visitor<'ast> for ExpandImpliedTablesVisitor<'schema> {
    type Error = ReadySetError;

    fn visit_select_statement(
        &mut self,
        select_statement: &'ast mut SelectStatement,
    ) -> Result<(), Self::Error> {
        let orig_tables = mem::replace(
            &mut self.tables,
            outermost_referred_tables(select_statement)
                .map(|tbl| tbl.alias.as_ref().unwrap_or(&tbl.name))
                .cloned()
                .collect(),
        );
        let orig_subquery_schemas = mem::replace(
            &mut self.subquery_schemas,
            super::subquery_schemas(&select_statement.ctes, &select_statement.join)
                .into_iter()
                .map(|(k, v)| (k.to_owned(), v.into_iter().map(|s| s.to_owned()).collect()))
                .collect(),
        );
        let orig_aliases = mem::replace(
            &mut self.aliases,
            select_statement
                .fields
                .iter()
                .filter_map(|fde| match fde {
                    FieldDefinitionExpression::Expression {
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

    fn visit_order_clause(
        &mut self,
        order: &'ast mut nom_sql::OrderClause,
    ) -> Result<(), Self::Error> {
        self.can_reference_aliases = true;
        walk_order_clause(self, order)?;
        self.can_reference_aliases = false;
        Ok(())
    }

    fn visit_group_by_clause(
        &mut self,
        group_by: &'ast mut nom_sql::GroupByClause,
    ) -> Result<(), Self::Error> {
        self.can_reference_aliases = true;
        walk_group_by_clause(self, group_by)?;
        self.can_reference_aliases = false;
        Ok(())
    }

    fn visit_column(&mut self, column: &'ast mut Column) -> Result<(), Self::Error> {
        if column.table.is_some() {
            return Ok(());
        }

        if !(self.can_reference_aliases && self.aliases.contains(&column.name)) {
            column.table = self.find_table(&column.name)
        }

        Ok(())
    }
}

fn rewrite_select(
    mut select_statement: SelectStatement,
    schema: &HashMap<String, Vec<String>>,
) -> ReadySetResult<SelectStatement> {
    let mut visitor = ExpandImpliedTablesVisitor {
        schema,
        subquery_schemas: Default::default(),
        tables: Default::default(),
        aliases: Default::default(),
        can_reference_aliases: false,
    };

    visitor.visit_select_statement(&mut select_statement)?;
    Ok(select_statement)
}

impl ImpliedTableExpansion for SqlQuery {
    fn expand_implied_tables(
        self,
        write_schemas: &HashMap<String, Vec<String>>,
    ) -> ReadySetResult<SqlQuery> {
        Ok(match self {
            SqlQuery::CreateTable(..) => self,
            SqlQuery::CompoundSelect(mut csq) => {
                csq.selects = csq
                    .selects
                    .into_iter()
                    .map(|(op, sq)| Ok((op, rewrite_select(sq, write_schemas)?)))
                    .collect::<ReadySetResult<Vec<_>>>()?;
                SqlQuery::CompoundSelect(csq)
            }
            SqlQuery::Select(sq) => SqlQuery::Select(rewrite_select(sq, write_schemas)?),
            SqlQuery::Insert(mut iq) => {
                let table = iq.table.clone();
                // Expand within field list
                iq.fields = iq
                    .fields
                    .map(|fields| fields.into_iter().map(|c| set_table(c, &table)).collect())
                    .transpose()?;
                SqlQuery::Insert(iq)
            }
            _ => internal!(),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use maplit::hashmap;
    use nom_sql::{
        parse_query, Column, Dialect, Expression, FieldDefinitionExpression, SqlQuery, Table,
    };

    use super::*;

    #[test]
    fn it_expands_implied_tables_for_select() {
        use nom_sql::{BinaryOperator, SelectStatement};

        // SELECT name, title FROM users, articles WHERE users.id = author;
        // -->
        // SELECT users.name, articles.title FROM users, articles WHERE users.id = articles.author;
        let q = SelectStatement {
            tables: vec![Table::from("users"), Table::from("articles")],
            fields: vec![
                FieldDefinitionExpression::from(Column::from("name")),
                FieldDefinitionExpression::from(Column::from("title")),
            ],
            where_clause: Some(Expression::BinaryOp {
                lhs: Box::new(Expression::Column(Column::from("users.id"))),
                op: BinaryOperator::Equal,
                rhs: Box::new(Expression::Column(Column::from("author"))),
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

        let res = SqlQuery::Select(q).expand_implied_tables(&schema).unwrap();
        match res {
            SqlQuery::Select(tq) => {
                assert_eq!(
                    tq.fields,
                    vec![
                        FieldDefinitionExpression::from(Column::from("users.name")),
                        FieldDefinitionExpression::from(Column::from("articles.title")),
                    ]
                );
                assert_eq!(
                    tq.where_clause,
                    Some(Expression::BinaryOp {
                        lhs: Box::new(Expression::Column(Column::from("users.id"))),
                        op: BinaryOperator::Equal,
                        rhs: Box::new(Expression::Column(Column::from("articles.author"))),
                    })
                );
            }
            // if we get anything other than a selection query back, something really weird is up
            _ => panic!(),
        }
    }

    #[test]
    fn doesnt_expand_order_referencing_projected_field() {
        let orig = parse_query(
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

        let schema = hashmap! {
            "t1".into() => vec![
                "id".into(),
                "value".into(),
            ]
        };

        let res = orig.expand_implied_tables(&schema).unwrap();
        assert_eq!(res, expected);
    }

    #[test]
    fn in_where() {
        let orig = parse_query(Dialect::MySQL, "SELECT name FROM users WHERE id = ?").unwrap();
        let expected = parse_query(
            Dialect::MySQL,
            "SELECT users.name FROM users WHERE users.id = ?",
        )
        .unwrap();
        let schema = hashmap! {
            "users".into() => vec![
                "id".into(),
                "name".into(),
            ]
        };

        let res = orig.expand_implied_tables(&schema).unwrap();
        assert_eq!(res, expected);
    }

    #[test]
    fn case_when() {
        let orig = parse_query(
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
        let schema = hashmap! {
            "votes".into() => vec![
                "aid".into(),
                "userid".into(),
            ]
        };

        let res = orig.expand_implied_tables(&schema).unwrap();
        assert_eq!(res, expected);
    }

    #[test]
    fn in_cte() {
        let orig = parse_query(
            Dialect::MySQL,
            "With votes AS (SELECT COUNT(id), story_id FROM votes GROUP BY story_id )
             SELECT title FROM stories JOIN votes ON stories.id = votes.story_id",
        )
        .unwrap();
        let expected = parse_query(

Dialect::MySQL,
            "With votes AS(SELECT COUNT(votes.id), votes.story_id FROM votes GROUP BY votes.story_id )
             SELECT stories.title FROM stories JOIN votes ON stories.id = votes.story_id",
        )
        .unwrap();
        let schema = hashmap! {
            "votes".into() => vec![
                "story_id".into(),
                "id".into(),
            ],
            "stories".into() => vec![
                "id".into(),
                "title".into(),
            ]
        };

        let res = orig.expand_implied_tables(&schema).unwrap();
        assert_eq!(res, expected);
    }

    #[test]
    fn referencing_cte() {
        let orig = parse_query(
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
        let schema = hashmap! {
            "votes".into() => vec![
                "story_id".into(),
                "id".into(),
            ],
            "stories".into() => vec![
                "id".into(),
                "title".into(),
            ]
        };

        let res = orig.expand_implied_tables(&schema).unwrap();
        assert_eq!(res, expected, "{} != {}", res, expected);
    }
}
