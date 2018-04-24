use nom_sql::{Column, ConditionBase, ConditionExpression, ConditionTree, CreateTableStatement,
              CreateViewStatement, FieldDefinitionExpression, Literal, Operator,
              SelectSpecification, SelectStatement, SqlQuery};

use std::collections::HashMap;
use std::mem;

use schema::Schema;

pub(crate) fn expand_stars(
    mut query: SqlQuery,
    table_schemas: &HashMap<String, Schema>,
) -> SqlQuery {
    let do_expand_select = |sq: &SelectStatement, table_name: &str| {
        sq.fields
            .iter()
            .map(|ref f| match *f {
                FieldDefinitionExpression::Col(ref c) => FieldDefinitionExpression::Col(Column {
                    table: Some(table_name.to_owned()),
                    name: c.alias.as_ref().unwrap_or(&c.name).to_owned(),
                    alias: None,
                    function: None,
                }),
                _ => unimplemented!(),
            })
            .collect::<Vec<_>>()
    };

    let expand_table = |table_name: String| match table_schemas
        .get(&table_name)
        .expect(&format!("table/view named `{}` does not exist", table_name))
    {
        Schema::Table(CreateTableStatement { ref fields, .. }) => fields
            .iter()
            .cloned()
            .map(move |f| {
                FieldDefinitionExpression::Col(Column {
                    table: Some(table_name.to_owned()),
                    name: f.column.name.clone(),
                    alias: None,
                    function: None,
                })
            })
            .collect::<Vec<_>>(),
        Schema::View(CreateViewStatement {
            ref name,
            ref definition,
            ..
        }) => match **definition {
            SelectSpecification::Compound(ref csq) => {
                // use the first select's columns
                do_expand_select(&csq.selects[0].1, name)
            }
            SelectSpecification::Simple(ref sq) => do_expand_select(sq, name),
        },
    };

    if let SqlQuery::Select(ref mut sq) = query {
        let old_fields = mem::replace(&mut sq.fields, vec![]);
        sq.fields = old_fields
            .into_iter()
            .flat_map(|field| match field {
                FieldDefinitionExpression::All => {
                    let v: Vec<_> = sq.tables
                        .iter()
                        .map(|t| t.name.clone())
                        .flat_map(&expand_table)
                        .collect();
                    v.into_iter()
                }
                FieldDefinitionExpression::AllInTable(t) => {
                    let v: Vec<_> = expand_table(t);
                    v.into_iter()
                }
                e @ FieldDefinitionExpression::Value(_) => vec![e].into_iter(),
                FieldDefinitionExpression::Col(c) => {
                    vec![FieldDefinitionExpression::Col(c)].into_iter()
                }
            })
            .collect();
    }
    query
}

fn collapse_where_in_recursive(
    leftmost_param_index: &mut usize,
    expr: &mut ConditionExpression,
    rewrite_literals: bool,
) -> Option<(usize, Vec<Literal>)> {
    match *expr {
        ConditionExpression::Base(ConditionBase::Literal(Literal::Placeholder)) => {
            *leftmost_param_index += 1;
            None
        }
        ConditionExpression::Base(ConditionBase::NestedSelect(ref mut sq)) => {
            if let Some(ref mut w) = sq.where_clause {
                collapse_where_in_recursive(leftmost_param_index, w, rewrite_literals)
            } else {
                None
            }
        }
        ConditionExpression::Base(ConditionBase::LiteralList(ref list)) => {
            *leftmost_param_index += list.iter().filter(|&l| *l == Literal::Placeholder).count();
            None
        }
        ConditionExpression::Base(_) => None,
        ConditionExpression::NegationOp(ref mut ce)
        | ConditionExpression::Bracketed(ref mut ce) => {
            collapse_where_in_recursive(leftmost_param_index, ce, rewrite_literals)
        }
        ConditionExpression::LogicalOp(ref mut ct) => {
            collapse_where_in_recursive(leftmost_param_index, &mut *ct.left, rewrite_literals)
                .or_else(|| {
                    // we can't also try rewriting ct.right, as it'd make it hard to recover
                    // literals: if we rewrote WHERE x IN (a, b) in left and WHERE y IN (1, 2) in
                    // right into WHERE x = ? ... y = ?, then what param values should we use?
                    collapse_where_in_recursive(
                        leftmost_param_index,
                        &mut *ct.right,
                        rewrite_literals,
                    )
                })
        }
        ConditionExpression::ComparisonOp(ref mut ct) if ct.operator != Operator::In => {
            collapse_where_in_recursive(leftmost_param_index, &mut *ct.left, rewrite_literals)
                .or_else(|| {
                    collapse_where_in_recursive(
                        leftmost_param_index,
                        &mut *ct.right,
                        rewrite_literals,
                    )
                })
        }
        ConditionExpression::ComparisonOp(ref mut ct) => {
            let mut do_it = false;
            let literals = if let ConditionExpression::Base(ConditionBase::LiteralList(
                ref mut list,
            )) = *ct.right
            {
                if rewrite_literals || list.iter().all(|l| *l == Literal::Placeholder) {
                    do_it = true;
                    mem::replace(list, Vec::new())
                } else {
                    Vec::new()
                }
            } else {
                Vec::new()
            };

            if !do_it {
                return collapse_where_in_recursive(
                    leftmost_param_index,
                    &mut *ct.left,
                    rewrite_literals,
                ).or_else(|| {
                    collapse_where_in_recursive(
                        leftmost_param_index,
                        &mut *ct.right,
                        rewrite_literals,
                    )
                });
            }

            if let ConditionExpression::Base(ConditionBase::Field(_)) = *ct.left {
            } else {
                unimplemented!();
            }

            let c = mem::replace(
                &mut ct.left,
                Box::new(ConditionExpression::Base(ConditionBase::Literal(
                    Literal::Placeholder,
                ))),
            );

            mem::replace(
                ct,
                ConditionTree {
                    operator: Operator::Equal,
                    left: c,
                    right: Box::new(ConditionExpression::Base(ConditionBase::Literal(
                        Literal::Placeholder,
                    ))),
                },
            );

            if literals.is_empty() {
                eprintln!("spotted empty WHERE IN ()");
            }

            Some((*leftmost_param_index, literals))
        }
    }
}

pub(crate) fn collapse_where_in(
    query: &mut SqlQuery,
    rewrite_literals: bool,
) -> Option<(usize, Vec<Literal>)> {
    if let SqlQuery::Select(ref mut sq) = *query {
        if let Some(ref mut w) = sq.where_clause {
            let mut left_edge = 0;
            return collapse_where_in_recursive(&mut left_edge, w, rewrite_literals);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use nom_sql;

    #[test]
    fn collapsed_where_placeholders() {
        let mut q = nom_sql::parse_query("SELECT * FROM x WHERE x.y IN (?, ?, ?)").unwrap();
        let rewritten = collapse_where_in(&mut q, false).unwrap();
        assert_eq!(rewritten.0, 0);
        assert_eq!(rewritten.1.len(), 3);
        assert_eq!(
            q,
            nom_sql::parse_query("SELECT * FROM x WHERE x.y = ?").unwrap()
        );

        let mut q = nom_sql::parse_query("SELECT * FROM x WHERE y IN (?, ?, ?)").unwrap();
        let rewritten = collapse_where_in(&mut q, false).unwrap();
        assert_eq!(rewritten.0, 0);
        assert_eq!(rewritten.1.len(), 3);
        assert_eq!(
            q,
            nom_sql::parse_query("SELECT * FROM x WHERE y = ?").unwrap()
        );

        let mut q = nom_sql::parse_query("SELECT * FROM x WHERE AVG(y) IN (?, ?, ?)").unwrap();
        let rewritten = collapse_where_in(&mut q, false).unwrap();
        assert_eq!(rewritten.0, 0);
        assert_eq!(rewritten.1.len(), 3);
        assert_eq!(
            q,
            nom_sql::parse_query("SELECT * FROM x WHERE AVG(y) = ?").unwrap()
        );

        let mut q = nom_sql::parse_query("SELECT * FROM t WHERE x = ? AND y IN (?, ?, ?) OR z = ?")
            .unwrap();
        let rewritten = collapse_where_in(&mut q, false).unwrap();
        assert_eq!(rewritten.0, 1);
        assert_eq!(rewritten.1.len(), 3);
        assert_eq!(
            q,
            nom_sql::parse_query("SELECT * FROM t WHERE x = ? AND y = ? OR z = ?").unwrap()
        );

        let mut q = nom_sql::parse_query(
            "SELECT * FROM t WHERE x IN (SELECT * FROM z WHERE a = ?) AND y IN (?, ?) OR z = ?",
        ).unwrap();
        let rewritten = collapse_where_in(&mut q, false).unwrap();
        assert_eq!(rewritten.0, 1);
        assert_eq!(rewritten.1.len(), 2);
        assert_eq!(
            q,
            nom_sql::parse_query(
                "SELECT * FROM t WHERE x IN (SELECT * FROM z WHERE a = ?) AND y = ? OR z = ?"
            ).unwrap()
        );

        let mut q = nom_sql::parse_query(
            "SELECT * FROM t WHERE x IN (SELECT * FROM z WHERE b = ? AND a IN (?, ?)) OR z = ?",
        ).unwrap();
        let rewritten = collapse_where_in(&mut q, false).unwrap();
        assert_eq!(rewritten.0, 1);
        assert_eq!(rewritten.1.len(), 2);
        assert_eq!(
            q,
            nom_sql::parse_query(
                "SELECT * FROM t WHERE x IN (SELECT * FROM z WHERE b = ? AND a = ?) OR z = ?",
            ).unwrap()
        );
    }

    #[test]
    fn collapsed_where_literals() {
        let mut q = nom_sql::parse_query("SELECT * FROM x WHERE x.y IN (1, 2, 3)").unwrap();
        assert_eq!(collapse_where_in(&mut q, false), None);
        assert_eq!(
            q,
            nom_sql::parse_query("SELECT * FROM x WHERE x.y IN (1, 2, 3)").unwrap()
        );

        let mut q = nom_sql::parse_query("SELECT * FROM x WHERE x.y IN (1, 2, 3)").unwrap();
        let rewritten = collapse_where_in(&mut q, true).unwrap();
        assert_eq!(rewritten.0, 0);
        assert_eq!(rewritten.1.len(), 3);
        assert_eq!(
            q,
            nom_sql::parse_query("SELECT * FROM x WHERE x.y = ?").unwrap()
        );

        let mut q = nom_sql::parse_query("SELECT * FROM x WHERE y IN (1, 2, 3)").unwrap();
        let rewritten = collapse_where_in(&mut q, true).unwrap();
        assert_eq!(rewritten.0, 0);
        assert_eq!(rewritten.1.len(), 3);
        assert_eq!(
            q,
            nom_sql::parse_query("SELECT * FROM x WHERE y = ?").unwrap()
        );

        let mut q = nom_sql::parse_query("SELECT * FROM x WHERE AVG(y) IN (1, 2, 3)").unwrap();
        let rewritten = collapse_where_in(&mut q, true).unwrap();
        assert_eq!(rewritten.0, 0);
        assert_eq!(rewritten.1.len(), 3);
        assert_eq!(
            q,
            nom_sql::parse_query("SELECT * FROM x WHERE AVG(y) = ?").unwrap()
        );
    }

    #[test]
    fn noninterference() {
        let mut q = nom_sql::parse_query("SELECT * FROM x WHERE x.y = 'foo'").unwrap();
        assert_eq!(collapse_where_in(&mut q, true), None);
        assert_eq!(
            q,
            nom_sql::parse_query("SELECT * FROM x WHERE x.y = 'foo'").unwrap()
        );
    }
}
