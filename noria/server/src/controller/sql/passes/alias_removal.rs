use nom_sql::{
    Column, ConditionBase, ConditionExpression, ConditionTree, FieldDefinitionExpression,
    JoinConstraint, JoinRightSide, SqlQuery,
};

use std::collections::HashMap;

use dataflow::prelude::DataType;

pub trait AliasRemoval {
    fn expand_table_aliases(self, context: &HashMap<String, DataType>) -> SqlQuery;
}

fn rewrite_conditional(
    table_aliases: &HashMap<String, String>,
    ce: ConditionExpression,
) -> ConditionExpression {
    let translate_column = |f: Column| {
        let new_f = match f.table {
            None => f,
            Some(t) => Column {
                name: f.name,
                alias: f.alias,
                table: if table_aliases.contains_key(&t) {
                    Some(table_aliases[&t].clone())
                } else {
                    Some(t)
                },
                function: None,
            },
        };
        ConditionExpression::Base(ConditionBase::Field(new_f))
    };

    let translate_ct_arm = |bce: Box<ConditionExpression>| -> Box<ConditionExpression> {
        let new_ce = match *bce {
            ConditionExpression::Base(ConditionBase::Field(f)) => translate_column(f),
            ConditionExpression::Base(b) => ConditionExpression::Base(b),
            x => rewrite_conditional(table_aliases, x),
        };
        Box::new(new_ce)
    };

    match ce {
        ConditionExpression::ComparisonOp(ct) => {
            let rewritten_ct = ConditionTree {
                operator: ct.operator,
                left: translate_ct_arm(ct.left),
                right: translate_ct_arm(ct.right),
            };
            ConditionExpression::ComparisonOp(rewritten_ct)
        }
        ConditionExpression::LogicalOp(ConditionTree {
            operator,
            left,
            right,
        }) => {
            let rewritten_ct = ConditionTree {
                operator,
                left: Box::new(rewrite_conditional(table_aliases, *left)),
                right: Box::new(rewrite_conditional(table_aliases, *right)),
            };
            ConditionExpression::LogicalOp(rewritten_ct)
        }
        x => x,
    }
}

impl AliasRemoval for SqlQuery {
    fn expand_table_aliases(self, context: &HashMap<String, DataType>) -> SqlQuery {
        let mut table_aliases = HashMap::new();

        match self {
            SqlQuery::Select(mut sq) => {
                {
                    // Collect table aliases
                    let mut add_alias = |alias: &str, name: &str| {
                        table_aliases.insert(alias.to_string(), name.to_string());
                    };

                    // Add alias for universe context tables
                    if context.get("id").is_some() {
                        let universe_id = context.get("id").unwrap();
                        match context.get("group") {
                            Some(g) => add_alias(
                                "GroupContext",
                                &format!(
                                    "GroupContext_{}_{}",
                                    g.to_string(),
                                    universe_id.to_string()
                                ),
                            ),
                            None => add_alias(
                                "UserContext",
                                &format!("UserContext_{}", universe_id.to_string()),
                            ),
                        }
                    }

                    for t in &mut sq.tables {
                        match t.alias {
                            None => (),
                            Some(ref a) => {
                                add_alias(a, &t.name);
                                t.alias = None;
                            }
                        }
                    }
                    for jc in &sq.join {
                        match jc.right {
                            JoinRightSide::Table(ref t) => match t.alias {
                                None => (),
                                Some(ref a) => add_alias(a, &t.name),
                            },
                            JoinRightSide::Tables(ref ts) => {
                                for t in ts {
                                    match t.alias {
                                        None => (),
                                        Some(ref a) => add_alias(a, &t.name),
                                    }
                                }
                            }
                            JoinRightSide::NestedJoin(_) => unimplemented!(),
                            _ => (),
                        }
                    }
                }
                // Remove them from fields
                sq.fields = sq
                    .fields
                    .into_iter()
                    .map(|field| match field {
                        // WTF rustfmt?
                        FieldDefinitionExpression::Col(mut col) => {
                            if col.table.is_some() {
                                let t = col.table.take().unwrap();
                                col.table = if table_aliases.contains_key(&t) {
                                    Some(table_aliases[&t].clone())
                                } else {
                                    Some(t.clone())
                                };
                                col.function = None;
                            }
                            FieldDefinitionExpression::Col(col)
                        }
                        FieldDefinitionExpression::AllInTable(t) => {
                            if table_aliases.contains_key(&t) {
                                FieldDefinitionExpression::AllInTable(table_aliases[&t].clone())
                            } else {
                                FieldDefinitionExpression::AllInTable(t)
                            }
                        }
                        f => f,
                    })
                    .collect();
                // Remove them from join clauses
                sq.join = sq
                    .join
                    .into_iter()
                    .map(|mut jc| {
                        jc.right = match jc.right {
                            JoinRightSide::Table(t) => {
                                if table_aliases.contains_key(&t.name) {
                                    JoinRightSide::Table(nom_sql::Table::from(
                                        table_aliases[&t.name].as_ref(),
                                    ))
                                } else {
                                    JoinRightSide::Table(t)
                                }
                            }
                            _ => unimplemented!(),
                        };
                        jc.constraint = match jc.constraint {
                            JoinConstraint::On(cond) => {
                                JoinConstraint::On(rewrite_conditional(&table_aliases, cond))
                            }
                            c @ JoinConstraint::Using(..) => c,
                        };
                        jc
                    })
                    .collect();
                // Remove them from conditions
                sq.where_clause = match sq.where_clause {
                    None => None,
                    Some(wc) => Some(rewrite_conditional(&table_aliases, wc)),
                };
                SqlQuery::Select(sq)
            }
            // nothing to do for other query types, as they cannot have aliases
            x => x,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::AliasRemoval;
    use nom_sql::{Column, FieldDefinitionExpression, Literal, SqlQuery, Table};
    use nom_sql::{ItemPlaceholder, SelectStatement};
    use std::collections::HashMap;

    #[test]
    fn it_removes_aliases() {
        use nom_sql::{ConditionBase, ConditionExpression, ConditionTree, Operator};

        let wrap = |cb| Box::new(ConditionExpression::Base(cb));
        let q = SelectStatement {
            tables: vec![Table {
                name: String::from("PaperTag"),
                alias: Some(String::from("t")),
                schema: None,
            }],
            fields: vec![FieldDefinitionExpression::Col(Column::from("t.id"))],
            where_clause: Some(ConditionExpression::ComparisonOp(ConditionTree {
                operator: Operator::Equal,
                left: wrap(ConditionBase::Field(Column::from("t.id"))),
                right: wrap(ConditionBase::Literal(Literal::Placeholder(
                    ItemPlaceholder::QuestionMark,
                ))),
            })),
            ..Default::default()
        };
        let mut context = HashMap::new();
        context.insert(String::from("id"), "global".into());
        let res = SqlQuery::Select(q).expand_table_aliases(&context);
        // Table alias removed in field list
        match res {
            SqlQuery::Select(tq) => {
                assert_eq!(
                    tq.fields,
                    vec![FieldDefinitionExpression::Col(Column::from("PaperTag.id"))]
                );
                assert_eq!(
                    tq.where_clause,
                    Some(ConditionExpression::ComparisonOp(ConditionTree {
                        operator: Operator::Equal,
                        left: wrap(ConditionBase::Field(Column::from("PaperTag.id"))),
                        right: wrap(ConditionBase::Literal(Literal::Placeholder(
                            ItemPlaceholder::QuestionMark
                        ))),
                    }))
                );
            }
            // if we get anything other than a selection query back, something really weird is up
            _ => panic!(),
        }
    }
}
