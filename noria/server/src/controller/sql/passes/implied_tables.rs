use nom_sql::{
    Column, Expression, FieldDefinitionExpression, FunctionExpression, InValue, JoinRightSide,
    SelectStatement, SqlQuery, Table,
};

use crate::errors::ReadySetResult;
use crate::internal;
use std::collections::HashMap;

pub trait ImpliedTableExpansion {
    fn expand_implied_tables(
        self,
        write_schemas: &HashMap<String, Vec<String>>,
    ) -> ReadySetResult<SqlQuery>;
}

fn rewrite_expression<F>(expand_columns: &F, expr: &mut Expression, avail_tables: &[Table])
where
    F: Fn(&mut Column, &[Table]),
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
            | FunctionExpression::GroupConcat { box expr, .. }
            | FunctionExpression::Cast(box expr, _) => {
                rewrite_expression(expand_columns, expr, avail_tables);
            }
            FunctionExpression::Call { arguments, .. } => {
                for expr in arguments.iter_mut() {
                    rewrite_expression(expand_columns, expr, avail_tables);
                }
            }
        },
        Expression::Literal(_) => {}
        Expression::CaseWhen {
            condition,
            then_expr,
            else_expr,
        } => {
            rewrite_expression(expand_columns, condition, avail_tables);
            rewrite_expression(expand_columns, then_expr, avail_tables);
            if let Some(else_expr) = else_expr {
                rewrite_expression(expand_columns, else_expr, avail_tables);
            }
        }
        Expression::Column(col) => {
            expand_columns(col, avail_tables);
        }
        Expression::BinaryOp { lhs, rhs, .. } => {
            rewrite_expression(expand_columns, lhs, avail_tables);
            rewrite_expression(expand_columns, rhs, avail_tables);
        }
        Expression::UnaryOp { rhs, .. } => {
            rewrite_expression(expand_columns, rhs, avail_tables);
        }
        Expression::Between {
            operand, min, max, ..
        } => {
            rewrite_expression(expand_columns, operand, avail_tables);
            rewrite_expression(expand_columns, min, avail_tables);
            rewrite_expression(expand_columns, max, avail_tables);
        }
        Expression::In { lhs, rhs, .. } => {
            rewrite_expression(expand_columns, lhs, avail_tables);
            match rhs {
                InValue::Subquery(_) => {}
                InValue::List(exprs) => {
                    for expr in exprs {
                        rewrite_expression(expand_columns, expr, avail_tables);
                    }
                }
            }
        }
        Expression::Exists(_) => {}
        Expression::NestedSelect(_) => {}
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

    // Tries to find a table with a matching column in the `tables_in_query` (information
    // passed as `write_schemas`; this is not something the parser or the expansion pass can
    // know on their own). Panics if no match is found or the match is ambiguous.
    let find_table = |f: &Column, tables_in_query: &[Table]| -> Option<String> {
        let mut matches = write_schemas
            .iter()
            .filter(|&(t, _)| {
                if !tables_in_query.is_empty() {
                    for qt in tables_in_query {
                        if qt.name == *t {
                            return true;
                        }
                    }
                    false
                } else {
                    // preserve all tables if there are no tables in the query
                    true
                }
            })
            .filter_map(|(t, ws)| {
                let num_matching = ws.iter().filter(|c| **c == f.name).count();
                assert!(num_matching <= 1);
                if num_matching == 1 {
                    Some((*t).clone())
                } else {
                    None
                }
            })
            .collect::<Vec<String>>();
        if matches.len() > 1 {
            println!(
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
    let expand_columns = |col: &mut Column, tables_in_query: &[Table]| {
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
                        | Cast(box Expression::Column(ref mut fe), _)
                        | GroupConcat {
                            expr: box Expression::Column(ref mut fe),
                            ..
                        } => {
                            if fe.table.is_none() {
                                fe.table = find_table(fe, tables_in_query);
                            }
                        }
                        Call {
                            ref mut arguments, ..
                        } => {
                            for arg in arguments.iter_mut() {
                                if let Expression::Column(ref mut fe) = arg {
                                    if fe.table.is_none() {
                                        fe.table = find_table(fe, tables_in_query);
                                    }
                                }
                            }
                        }
                        _ => {}
                    }
                    None
                }
                None => find_table(&col, tables_in_query),
            }
        }
    };

    let mut tables: Vec<Table> = sq.tables.clone();
    // tables mentioned in JOINs are also available for expansion
    for jc in sq.join.iter() {
        match jc.right {
            JoinRightSide::Table(ref join_table) => tables.push(join_table.clone()),
            JoinRightSide::Tables(ref join_tables) => tables.extend(join_tables.clone()),
            _ => unimplemented!(),
        }
    }
    // Expand within field list
    for field in sq.fields.iter_mut() {
        match *field {
            FieldDefinitionExpression::All | FieldDefinitionExpression::AllInTable(_) => {
                internal!("Must apply StarExpansion pass before ImpliedTableExpansion")
            }
            FieldDefinitionExpression::Expression { ref mut expr, .. } => {
                rewrite_expression(&expand_columns, expr, &tables);
            }
        }
    }
    // Expand within WHERE clause
    if let Some(wc) = &mut sq.where_clause {
        rewrite_expression(&expand_columns, wc, &tables);
    }
    // Expand within GROUP BY clause
    if let Some(gbc) = &mut sq.group_by {
        for col in &mut gbc.columns {
            expand_columns(col, &tables)
        }
        if let Some(hc) = &mut gbc.having {
            rewrite_expression(&expand_columns, hc, &tables)
        }
    }

    // Expand within ORDER BY clause
    if let Some(oc) = &mut sq.order {
        for (col, _) in &mut oc.columns {
            expand_columns(col, &tables);
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
    use super::*;
    use maplit::hashmap;
    use nom_sql::{parse_query, Column, FieldDefinitionExpression, SqlQuery, Table};
    use std::collections::HashMap;

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
        let orig = parse_query("SELECT name FROM users WHERE id = ?").unwrap();
        let expected = parse_query("SELECT users.name FROM users WHERE users.id = ?").unwrap();
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
            "SELECT COUNT(CASE WHEN aid = 5 THEN aid END) AS count
             FROM votes GROUP BY votes.userid",
        )
        .unwrap();
        let expected = parse_query(
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
}
