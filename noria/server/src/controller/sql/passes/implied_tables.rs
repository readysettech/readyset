use nom_sql::{
    ArithmeticBase, ArithmeticItem, CaseWhenExpression, Column, ColumnOrLiteral,
    ConditionExpression, ConditionTree, FieldDefinitionExpression, FieldValueExpression,
    FunctionArgument, JoinRightSide, SelectStatement, SqlQuery, Table,
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

fn rewrite_conditional<F>(
    expand_columns: &F,
    ce: ConditionExpression,
    avail_tables: &[Table],
) -> ConditionExpression
where
    F: Fn(Column, &[Table]) -> Column,
{
    use nom_sql::ConditionBase::*;
    use nom_sql::ConditionExpression::*;

    let translate_ct_arm = |bce: Box<ConditionExpression>| -> Box<ConditionExpression> {
        let new_ce = match *bce {
            Base(Field(f)) => Base(Field(expand_columns(f, avail_tables))),
            Base(b) => Base(b),
            x => rewrite_conditional(expand_columns, x, avail_tables),
        };
        Box::new(new_ce)
    };

    match ce {
        ComparisonOp(ct) => {
            let l = translate_ct_arm(ct.left);
            let r = translate_ct_arm(ct.right);
            let rewritten_ct = ConditionTree {
                operator: ct.operator,
                left: l,
                right: r,
            };
            ComparisonOp(rewritten_ct)
        }
        LogicalOp(ConditionTree {
            operator,
            left,
            right,
        }) => LogicalOp(ConditionTree {
            operator,
            left: Box::new(rewrite_conditional(expand_columns, *left, avail_tables)),
            right: Box::new(rewrite_conditional(expand_columns, *right, avail_tables)),
        }),
        Bracketed(ce) => rewrite_conditional(expand_columns, *ce, avail_tables),
        x => x,
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
    use nom_sql::{GroupByClause, OrderClause};

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
    let expand_columns = |mut f: Column, tables_in_query: &[Table]| -> Column {
        f.table = match f.table {
            None => {
                match f.function {
                    Some(ref mut f) => {
                        // There is no implied table (other than "self") for anonymous function
                        // columns, but we have to peek inside the function to expand implied
                        // tables in its specification
                        match **f {
                            Avg(FunctionArgument::Column(ref mut fe), _)
                            | Count(
                                FunctionArgument::Conditional(CaseWhenExpression {
                                    then_expr: ColumnOrLiteral::Column(ref mut fe),
                                    ..
                                }),
                                _,
                            )
                            | Count(FunctionArgument::Column(ref mut fe), _)
                            | Sum(
                                FunctionArgument::Conditional(CaseWhenExpression {
                                    then_expr: ColumnOrLiteral::Column(ref mut fe),
                                    ..
                                }),
                                _,
                            )
                            | Sum(FunctionArgument::Column(ref mut fe), _)
                            | Min(FunctionArgument::Column(ref mut fe))
                            | Max(FunctionArgument::Column(ref mut fe))
                            | Cast(FunctionArgument::Column(ref mut fe), _)
                            | GroupConcat(FunctionArgument::Column(ref mut fe), _) => {
                                if fe.table.is_none() {
                                    fe.table = find_table(fe, tables_in_query);
                                }
                            }
                            _ => {}
                        }
                        None
                    }
                    None => find_table(&f, tables_in_query),
                }
            }
            Some(x) => Some(x),
        };
        f
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
            FieldDefinitionExpression::Value(FieldValueExpression::Literal(_)) => (),
            FieldDefinitionExpression::Value(FieldValueExpression::Arithmetic(ref mut e)) => {
                if let ArithmeticItem::Base(ArithmeticBase::Column(ref mut c)) = e.ari.left {
                    *c = expand_columns(c.clone(), &tables);
                }

                if let ArithmeticItem::Base(ArithmeticBase::Column(ref mut c)) = e.ari.right {
                    *c = expand_columns(c.clone(), &tables);
                }
            }
            FieldDefinitionExpression::Col(ref mut f) => {
                *f = expand_columns(f.clone(), &tables);
                // also need to expand any conditionals in the column, e.g. for filtered aggregations
                match f.function {
                    Some(ref mut f) => match **f {
                        Count(
                            FunctionArgument::Conditional(CaseWhenExpression {
                                ref mut condition,
                                ..
                            }),
                            _,
                        )
                        | Sum(
                            FunctionArgument::Conditional(CaseWhenExpression {
                                ref mut condition,
                                ..
                            }),
                            _,
                        ) => {
                            *condition =
                                rewrite_conditional(&expand_columns, condition.clone(), &tables);
                        }
                        _ => {}
                    },
                    None => {}
                }
            }
        }
    }
    // Expand within WHERE clause
    sq.where_clause = match sq.where_clause {
        None => None,
        Some(wc) => Some(rewrite_conditional(&expand_columns, wc, &tables)),
    };
    // Expand within GROUP BY clause
    sq.group_by = match sq.group_by {
        None => None,
        Some(gbc) => Some(GroupByClause {
            columns: gbc
                .columns
                .into_iter()
                .map(|f| expand_columns(f, &tables))
                .collect(),
            having: match gbc.having {
                None => None,
                Some(hc) => Some(rewrite_conditional(&expand_columns, hc, &tables)),
            },
        }),
    };
    // Expand within ORDER BY clause
    sq.order = match sq.order {
        None => None,
        Some(oc) => Some(OrderClause {
            columns: oc
                .columns
                .into_iter()
                .map(|(f, o)| (expand_columns(f, &tables), o))
                .collect(),
        }),
    };

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
    use super::ImpliedTableExpansion;
    use nom_sql::{Column, FieldDefinitionExpression, SqlQuery, Table};
    use std::collections::HashMap;

    #[test]
    fn it_expands_implied_tables_for_select() {
        use nom_sql::{
            BinaryOperator, ConditionBase, ConditionExpression, ConditionTree, SelectStatement,
        };

        let wrap = |cb| Box::new(ConditionExpression::Base(cb));

        // SELECT name, title FROM users, articles WHERE users.id = author;
        // -->
        // SELECT users.name, articles.title FROM users, articles WHERE users.id = articles.author;
        let q = SelectStatement {
            tables: vec![Table::from("users"), Table::from("articles")],
            fields: vec![
                FieldDefinitionExpression::Col(Column::from("name")),
                FieldDefinitionExpression::Col(Column::from("title")),
            ],
            where_clause: Some(ConditionExpression::ComparisonOp(ConditionTree {
                operator: BinaryOperator::Equal,
                left: wrap(ConditionBase::Field(Column::from("users.id"))),
                right: wrap(ConditionBase::Field(Column::from("author"))),
            })),
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
                        FieldDefinitionExpression::Col(Column::from("users.name")),
                        FieldDefinitionExpression::Col(Column::from("articles.title")),
                    ]
                );
                assert_eq!(
                    tq.where_clause,
                    Some(ConditionExpression::ComparisonOp(ConditionTree {
                        operator: BinaryOperator::Equal,
                        left: wrap(ConditionBase::Field(Column::from("users.id"))),
                        right: wrap(ConditionBase::Field(Column::from("articles.author"))),
                    }))
                );
            }
            // if we get anything other than a selection query back, something really weird is up
            _ => panic!(),
        }
    }
}
