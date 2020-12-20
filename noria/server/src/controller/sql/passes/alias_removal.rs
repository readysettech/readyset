use nom_sql::{
    CaseWhenExpression, Column, ColumnOrLiteral, ConditionBase, ConditionExpression, ConditionTree,
    FieldDefinitionExpression, FunctionArgument, FunctionArguments, FunctionExpression,
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
        ConditionExpression::Base(ConditionBase::Field(rewrite_column(table_aliases, f)))
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

fn rewrite_column(table_aliases: &HashMap<String, String>, col: Column) -> Column {
    let function = col
        .function
        .map(|f| Box::new(rewrite_function_expression(table_aliases, *f)));
    let table = col.table.map(|t| {
        if table_aliases.contains_key(&t) {
            table_aliases[&t].clone()
        } else {
            t.clone()
        }
    });
    Column {
        name: col.name,
        alias: col.alias,
        table: table,
        function: function,
    }
}

fn rewrite_column_or_literal(
    table_aliases: &HashMap<String, String>,
    col: ColumnOrLiteral,
) -> ColumnOrLiteral {
    match col {
        ColumnOrLiteral::Column(col) => ColumnOrLiteral::Column(rewrite_column(table_aliases, col)),
        ColumnOrLiteral::Literal(lit) => ColumnOrLiteral::Literal(lit),
    }
}

fn rewrite_case_when_expression(
    table_aliases: &HashMap<String, String>,
    cwe: CaseWhenExpression,
) -> CaseWhenExpression {
    CaseWhenExpression {
        condition: rewrite_conditional(table_aliases, cwe.condition),
        then_expr: rewrite_column_or_literal(table_aliases, cwe.then_expr),
        else_expr: cwe
            .else_expr
            .map(|e| rewrite_column_or_literal(table_aliases, e)),
    }
}

fn rewrite_function_argument(
    table_aliases: &HashMap<String, String>,
    arg: FunctionArgument,
) -> FunctionArgument {
    match arg {
        FunctionArgument::Column(col) => {
            FunctionArgument::Column(rewrite_column(table_aliases, col))
        }
        FunctionArgument::Conditional(cwe) => {
            FunctionArgument::Conditional(rewrite_case_when_expression(table_aliases, cwe))
        }
    }
}

fn rewrite_function_expression(
    table_aliases: &HashMap<String, String>,
    function: FunctionExpression,
) -> FunctionExpression {
    match function {
        FunctionExpression::Avg(arg, b) => {
            FunctionExpression::Avg(rewrite_function_argument(table_aliases, arg), b)
        }
        FunctionExpression::Count(arg, b) => {
            FunctionExpression::Count(rewrite_function_argument(table_aliases, arg), b)
        }
        FunctionExpression::CountStar => FunctionExpression::CountStar,
        FunctionExpression::Sum(arg, b) => {
            FunctionExpression::Sum(rewrite_function_argument(table_aliases, arg), b)
        }
        FunctionExpression::Max(arg) => {
            FunctionExpression::Max(rewrite_function_argument(table_aliases, arg))
        }
        FunctionExpression::Min(arg) => {
            FunctionExpression::Min(rewrite_function_argument(table_aliases, arg))
        }
        FunctionExpression::GroupConcat(arg, s) => {
            FunctionExpression::GroupConcat(rewrite_function_argument(table_aliases, arg), s)
        }
        FunctionExpression::Generic(s, args) => FunctionExpression::Generic(
            s,
            FunctionArguments {
                arguments: args
                    .arguments
                    .into_iter()
                    .map(move |arg| rewrite_function_argument(table_aliases, arg))
                    .collect(),
            },
        ),
    }
}

fn rewrite_field(
    table_aliases: &HashMap<String, String>,
    field: FieldDefinitionExpression,
) -> FieldDefinitionExpression {
    match field {
        FieldDefinitionExpression::Col(col) => {
            FieldDefinitionExpression::Col(rewrite_column(table_aliases, col))
        }
        FieldDefinitionExpression::AllInTable(t) => {
            if table_aliases.contains_key(&t) {
                FieldDefinitionExpression::AllInTable(table_aliases[&t].clone())
            } else {
                FieldDefinitionExpression::AllInTable(t)
            }
        }
        f => f,
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
                    .map(|field| rewrite_field(&table_aliases, field))
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
        use nom_sql::{BinaryOperator, ConditionBase, ConditionExpression, ConditionTree};

        let wrap = |cb| Box::new(ConditionExpression::Base(cb));
        let q = SelectStatement {
            tables: vec![Table {
                name: String::from("PaperTag"),
                alias: Some(String::from("t")),
                schema: None,
            }],
            fields: vec![FieldDefinitionExpression::Col(Column::from("t.id"))],
            where_clause: Some(ConditionExpression::ComparisonOp(ConditionTree {
                operator: BinaryOperator::Equal,
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
                        operator: BinaryOperator::Equal,
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

    #[test]
    fn it_removes_nested_aliases() {
        use nom_sql::{
            ConditionBase, ConditionExpression, ConditionTree, FunctionArgument,
            FunctionExpression, Operator,
        };

        let wrap = |cb| Box::new(ConditionExpression::Base(cb));
        let col_small = Column {
            name: "count(t.id)".into(),
            table: None,
            alias: None,
            function: Some(Box::new(FunctionExpression::Count(
                FunctionArgument::Column(Column {
                    name: "id".into(),
                    table: Some("t".into()),
                    alias: None,
                    function: None,
                }),
                true,
            ))),
        };
        let col_full = Column {
            name: "count(t.id)".into(),
            table: None,
            alias: None,
            function: Some(Box::new(FunctionExpression::Count(
                FunctionArgument::Column(Column {
                    name: "id".into(),
                    table: Some("PaperTag".into()),
                    alias: None,
                    function: None,
                }),
                true,
            ))),
        };
        let q = SelectStatement {
            tables: vec![Table {
                name: String::from("PaperTag"),
                alias: Some(String::from("t")),
                schema: None,
            }],
            fields: vec![FieldDefinitionExpression::Col(col_small.clone())],
            where_clause: Some(ConditionExpression::ComparisonOp(ConditionTree {
                operator: Operator::Equal,
                left: wrap(ConditionBase::Field(col_small.clone())),
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
                    vec![FieldDefinitionExpression::Col(col_full.clone())]
                );
                assert_eq!(
                    tq.where_clause,
                    Some(ConditionExpression::ComparisonOp(ConditionTree {
                        operator: Operator::Equal,
                        left: wrap(ConditionBase::Field(col_full.clone())),
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
