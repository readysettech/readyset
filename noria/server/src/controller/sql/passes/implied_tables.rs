use std::collections::HashMap;
use std::mem;

use nom_sql::{
    Column, CommonTableExpression, Expression, FieldDefinitionExpression, FunctionExpression,
    InValue, JoinClause, JoinRightSide, SelectStatement, SqlQuery, Table,
};
use noria_errors::{internal, ReadySetResult};
use tracing::warn;

pub trait ImpliedTableExpansion {
    fn expand_implied_tables(
        self,
        write_schemas: &HashMap<String, Vec<String>>,
    ) -> ReadySetResult<SqlQuery>;
}

fn rewrite_expression<F>(expand_columns: &F, expr: &mut Expression)
where
    F: Fn(&mut Column),
{
    match expr {
        Expression::Call(f)
        | Expression::Column(Column {
            function: Some(box f),
            ..
        }) => match f {
            FunctionExpression::CountStar => {}
            FunctionExpression::Avg { box expr, .. }
            | FunctionExpression::Count { box expr, .. }
            | FunctionExpression::Sum { box expr, .. }
            | FunctionExpression::Max(box expr)
            | FunctionExpression::Min(box expr)
            | FunctionExpression::GroupConcat { box expr, .. } => {
                rewrite_expression(expand_columns, expr);
            }
            FunctionExpression::Call { arguments, .. } => {
                for expr in arguments.iter_mut() {
                    rewrite_expression(expand_columns, expr);
                }
            }
        },
        Expression::Literal(_) => {}
        Expression::CaseWhen {
            condition,
            then_expr,
            else_expr,
        } => {
            rewrite_expression(expand_columns, condition);
            rewrite_expression(expand_columns, then_expr);
            if let Some(else_expr) = else_expr {
                rewrite_expression(expand_columns, else_expr);
            }
        }
        Expression::Column(col) => {
            expand_columns(col);
        }
        Expression::BinaryOp { lhs, rhs, .. } => {
            rewrite_expression(expand_columns, lhs);
            rewrite_expression(expand_columns, rhs);
        }
        Expression::UnaryOp { rhs: expr, .. } | Expression::Cast { expr, .. } => {
            rewrite_expression(expand_columns, expr);
        }
        Expression::Between {
            operand, min, max, ..
        } => {
            rewrite_expression(expand_columns, operand);
            rewrite_expression(expand_columns, min);
            rewrite_expression(expand_columns, max);
        }
        Expression::In { lhs, rhs, .. } => {
            rewrite_expression(expand_columns, lhs);
            match rhs {
                InValue::Subquery(_) => {}
                InValue::List(exprs) => {
                    for expr in exprs {
                        rewrite_expression(expand_columns, expr);
                    }
                }
            }
        }
        Expression::Exists(_) => {}
        Expression::NestedSelect(_) => {}
        Expression::Variable(_) => {}
    }
}

// Sets the table for the `Column` in `f`to `table`. This is mostly useful for CREATE TABLE
// and INSERT queries and deliberately leaves function specifications unaffected, since
// they can refer to remote tables and `set_table` should not be used for queries that have
// computed columns.
fn set_table(mut f: Column, table: &Table) -> ReadySetResult<Column> {
    f.table = match f.table {
        None => match f.function {
            Some(ref mut f) => internal!(
                "set_table({}) invoked on computed column {:?}",
                table.name,
                f
            ),
            None => Some(table.name.clone()),
        },
        Some(x) => Some(x),
    };
    Ok(f)
}

fn rewrite_selection(
    mut sq: SelectStatement,
    write_schemas: &HashMap<String, Vec<String>>,
) -> ReadySetResult<SelectStatement> {
    use nom_sql::FunctionExpression::*;

    // Expand within CTEs
    sq.ctes = mem::take(&mut sq.ctes)
        .into_iter()
        .map(|cte| -> ReadySetResult<_> {
            Ok(CommonTableExpression {
                statement: rewrite_selection(cte.statement, write_schemas)?,
                ..cte
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    // Expand within subqueries in joins
    sq.join = mem::take(&mut sq.join)
        .into_iter()
        .map(|join| -> ReadySetResult<_> {
            Ok(JoinClause {
                right: match join.right {
                    JoinRightSide::NestedSelect(stmt, name) => JoinRightSide::NestedSelect(
                        Box::new(rewrite_selection(*stmt, write_schemas)?),
                        name,
                    ),
                    right => right,
                },
                ..join
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    let mut tables: Vec<&str> = sq.tables.iter().map(|t| t.name.as_str()).collect();
    // Keep track of fields that have aliases so we will not assign tables to alias references later
    let mut known_aliases: Vec<&str> = Vec::new();
    // tables mentioned in JOINs are also available for expansion
    for jc in sq.join.iter() {
        match &jc.right {
            JoinRightSide::Table(Table { name, .. }) => tables.push(name),
            JoinRightSide::Tables(join_tables) => {
                tables.extend(join_tables.iter().map(|t| t.name.as_str()))
            }
            JoinRightSide::NestedSelect(_, Some(name)) => tables.push(name),
            _ => {}
        }
    }

    let subquery_schemas = super::subquery_schemas(&sq.ctes, &sq.join);

    // Tries to find a table with a matching column in the `tables_in_query` (information
    // passed as `write_schemas`; this is not something the parser or the expansion pass can
    // know on their own). Panics if no match is found or the match is ambiguous.
    let find_table = |f: &Column| -> Option<String> {
        let mut matches = write_schemas
            .iter()
            .map(|(k, v)| (k.as_str(), v.iter().map(String::as_str).collect()))
            .chain(subquery_schemas.iter().map(|(k, v)| (*k, v.clone())))
            .filter(|&(t, _)| tables.is_empty() || tables.contains(&t))
            .filter_map(|(t, ws)| {
                let num_matching = ws.iter().filter(|c| **c == f.name).count();
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
                f.name,
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
    };

    // Traverses a query and calls `find_table` on any column that has no explicit table set,
    // including computed columns. Should not be used for CREATE TABLE and INSERT queries,
    // which can use the simpler `set_table`.
    let expand_columns = |col: &mut Column| {
        if col.table.is_none() {
            col.table = match col.function {
                Some(ref mut f) => {
                    // There is no implied table (other than "self") for anonymous function
                    // columns, but we have to peek inside the function to expand implied
                    // tables in its specification
                    match **f {
                        Avg {
                            expr: box Expression::Column(ref mut fe),
                            ..
                        }
                        | Count {
                            expr:
                                box Expression::CaseWhen {
                                    then_expr: box Expression::Column(ref mut fe),
                                    ..
                                },
                            ..
                        }
                        | Count {
                            expr: box Expression::Column(ref mut fe),
                            ..
                        }
                        | Sum {
                            expr:
                                box Expression::CaseWhen {
                                    then_expr: box Expression::Column(ref mut fe),
                                    ..
                                },
                            ..
                        }
                        | Sum {
                            expr: box Expression::Column(ref mut fe),
                            ..
                        }
                        | Min(box Expression::Column(ref mut fe))
                        | Max(box Expression::Column(ref mut fe))
                        | GroupConcat {
                            expr: box Expression::Column(ref mut fe),
                            ..
                        } => {
                            if fe.table.is_none() {
                                fe.table = find_table(fe);
                            }
                        }
                        Call {
                            ref mut arguments, ..
                        } => {
                            for arg in arguments.iter_mut() {
                                if let Expression::Column(ref mut fe) = arg {
                                    if fe.table.is_none() {
                                        fe.table = find_table(fe);
                                    }
                                }
                            }
                        }
                        _ => {}
                    }
                    None
                }
                None => find_table(col),
            }
        }
    };

    // Expand within field list
    for field in sq.fields.iter_mut() {
        match *field {
            FieldDefinitionExpression::All | FieldDefinitionExpression::AllInTable(_) => {
                internal!("Must apply StarExpansion pass before ImpliedTableExpansion")
            }
            FieldDefinitionExpression::Expression {
                ref mut expr,
                ref alias,
            } => {
                rewrite_expression(&expand_columns, expr);
                if let Some(alias) = alias.as_deref() {
                    known_aliases.push(alias);
                }
            }
        }
    }

    // Expand within WHERE clause
    if let Some(wc) = &mut sq.where_clause {
        rewrite_expression(&expand_columns, wc);
    }

    // Expand within GROUP BY clause
    if let Some(gbc) = &mut sq.group_by {
        for col in &mut gbc.columns {
            expand_columns(col)
        }
        if let Some(hc) = &mut gbc.having {
            rewrite_expression(&expand_columns, hc)
        }
    }

    // Expand within ORDER BY clause
    if let Some(oc) = &mut sq.order {
        for (col, _) in &mut oc.columns {
            if col.function.is_some() || !known_aliases.contains(&col.name.as_str()) {
                expand_columns(col);
            }
        }
    }

    Ok(sq)
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
                    .map(|(op, sq)| Ok((op, rewrite_selection(sq, write_schemas)?)))
                    .collect::<ReadySetResult<Vec<_>>>()?;
                SqlQuery::CompoundSelect(csq)
            }
            SqlQuery::Select(sq) => SqlQuery::Select(rewrite_selection(sq, write_schemas)?),
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
    use nom_sql::{parse_query, Column, Dialect, FieldDefinitionExpression, SqlQuery, Table};

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
