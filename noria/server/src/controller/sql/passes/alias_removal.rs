use dataflow::prelude::DataType;
use itertools::Itertools;
use nom_sql::{
    CaseWhenExpression, Column, ColumnOrLiteral, ConditionBase, ConditionExpression, ConditionTree,
    FieldDefinitionExpression, FunctionArgument, FunctionArguments, FunctionExpression,
    JoinConstraint, JoinRightSide, SqlQuery, Table,
};
use std::collections::HashMap;

#[derive(Debug, PartialEq)]
pub enum TableAliasRewrite {
    ToTable {
        from: String,
        to_table: String,
    },
    ToView {
        from: String,
        to_view: String,
        for_table: String,
    },
}

pub trait AliasRemoval {
    // Remove all table aliases, leaving the table unaliased if possible but rewriting the table name
    // to a new view name derived from 'query_name' when necessary (ie when a single table is
    // referenced by more than one alias). Return a list of the rewrites performed.
    fn rewrite_table_aliases(
        &mut self,
        query_name: &str,
        context: &HashMap<String, DataType>,
    ) -> Vec<TableAliasRewrite>;
}

fn rewrite_conditional(
    col_table_remap: &HashMap<String, String>,
    ce: &ConditionExpression,
) -> ConditionExpression {
    let translate_column = |f: &Column| {
        ConditionExpression::Base(ConditionBase::Field(rewrite_column(col_table_remap, f)))
    };

    let translate_ct_arm = |bce: Box<ConditionExpression>| -> Box<ConditionExpression> {
        let new_ce = match *bce {
            ConditionExpression::Base(ConditionBase::Field(ref f)) => translate_column(f),
            ConditionExpression::Base(b) => ConditionExpression::Base(b),
            x => rewrite_conditional(col_table_remap, &x),
        };
        Box::new(new_ce)
    };

    match ce {
        ConditionExpression::ComparisonOp(ct) => {
            let rewritten_ct = ConditionTree {
                operator: ct.operator,
                left: translate_ct_arm(ct.left.clone()),
                right: translate_ct_arm(ct.right.clone()),
            };
            ConditionExpression::ComparisonOp(rewritten_ct)
        }
        ConditionExpression::LogicalOp(ConditionTree {
            operator,
            left,
            right,
        }) => {
            let rewritten_ct = ConditionTree {
                operator: *operator,
                left: Box::new(rewrite_conditional(col_table_remap, left)),
                right: Box::new(rewrite_conditional(col_table_remap, right)),
            };
            ConditionExpression::LogicalOp(rewritten_ct)
        }
        x => x.clone(),
    }
}

fn rewrite_table(table_remap: &HashMap<String, String>, table: &Table) -> Table {
    let name = table
        .alias
        .as_ref()
        .and_then(|t| table_remap.get(t))
        .unwrap_or(&table.name);
    Table {
        name: name.clone(),
        alias: None,
        schema: table.schema.clone(),
    }
}

fn rewrite_column(col_table_remap: &HashMap<String, String>, col: &Column) -> Column {
    let function = col
        .function
        .as_ref()
        .map(|f| Box::new(rewrite_function_expression(col_table_remap, &*f)));
    let table = col.table.as_ref().map(|t| {
        if col_table_remap.contains_key(t) {
            col_table_remap[t].clone()
        } else {
            t.clone()
        }
    });
    Column {
        name: col.name.clone(),
        alias: col.alias.clone(),
        table: table,
        function: function,
    }
}

fn rewrite_column_or_literal(
    col_table_remap: &HashMap<String, String>,
    col: &ColumnOrLiteral,
) -> ColumnOrLiteral {
    match col {
        ColumnOrLiteral::Column(col) => {
            ColumnOrLiteral::Column(rewrite_column(col_table_remap, col))
        }
        ColumnOrLiteral::Literal(lit) => ColumnOrLiteral::Literal(lit.clone()),
    }
}

fn rewrite_case_when_expression(
    col_table_remap: &HashMap<String, String>,
    cwe: &CaseWhenExpression,
) -> CaseWhenExpression {
    CaseWhenExpression {
        condition: rewrite_conditional(col_table_remap, &cwe.condition),
        then_expr: rewrite_column_or_literal(col_table_remap, &cwe.then_expr),
        else_expr: cwe
            .else_expr
            .as_ref()
            .map(|e| rewrite_column_or_literal(col_table_remap, e)),
    }
}

fn rewrite_function_argument(
    col_table_remap: &HashMap<String, String>,
    arg: &FunctionArgument,
) -> FunctionArgument {
    match arg {
        FunctionArgument::Column(col) => {
            FunctionArgument::Column(rewrite_column(col_table_remap, col))
        }
        FunctionArgument::Conditional(cwe) => {
            FunctionArgument::Conditional(rewrite_case_when_expression(col_table_remap, cwe))
        }
    }
}

fn rewrite_function_expression(
    col_table_remap: &HashMap<String, String>,
    function: &FunctionExpression,
) -> FunctionExpression {
    match function {
        FunctionExpression::Avg(arg, b) => {
            FunctionExpression::Avg(rewrite_function_argument(col_table_remap, arg), *b)
        }
        FunctionExpression::Count(arg, b) => {
            FunctionExpression::Count(rewrite_function_argument(col_table_remap, arg), *b)
        }
        FunctionExpression::CountStar => FunctionExpression::CountStar,
        FunctionExpression::Sum(arg, b) => {
            FunctionExpression::Sum(rewrite_function_argument(col_table_remap, arg), *b)
        }
        FunctionExpression::Max(arg) => {
            FunctionExpression::Max(rewrite_function_argument(col_table_remap, arg))
        }
        FunctionExpression::Min(arg) => {
            FunctionExpression::Min(rewrite_function_argument(col_table_remap, arg))
        }
        FunctionExpression::GroupConcat(arg, s) => FunctionExpression::GroupConcat(
            rewrite_function_argument(col_table_remap, arg),
            s.clone(),
        ),
        FunctionExpression::Cast(arg, t) => {
            FunctionExpression::Cast(rewrite_function_argument(col_table_remap, arg), t.clone())
        }
        FunctionExpression::Generic(s, args) => FunctionExpression::Generic(
            s.clone(),
            FunctionArguments {
                arguments: args
                    .arguments
                    .iter()
                    .map(|arg| rewrite_function_argument(col_table_remap, arg))
                    .collect(),
            },
        ),
    }
}

fn rewrite_field(
    col_table_remap: &HashMap<String, String>,
    field: &FieldDefinitionExpression,
) -> FieldDefinitionExpression {
    match field {
        FieldDefinitionExpression::Col(col) => {
            FieldDefinitionExpression::Col(rewrite_column(col_table_remap, col))
        }
        FieldDefinitionExpression::AllInTable(t) => {
            if col_table_remap.contains_key(t) {
                FieldDefinitionExpression::AllInTable(col_table_remap[t].clone())
            } else {
                FieldDefinitionExpression::AllInTable(t.clone())
            }
        }
        f => f.clone(),
    }
}

impl AliasRemoval for SqlQuery {
    fn rewrite_table_aliases(
        &mut self,
        query_name: &str,
        context: &HashMap<String, DataType>,
    ) -> Vec<TableAliasRewrite> {
        if let SqlQuery::Select(ref mut sq) = self {
            // Identify the unique table references for every table appearing in the query FROM and
            // JOIN clauses, and group by table name. Both None (ie unaliased) and Some(alias)
            // reference types are included.
            let table_refs = sq
                .tables
                .iter()
                .cloned()
                .chain(sq.join.iter().flat_map(|j| match j.right {
                    JoinRightSide::Table(ref table) => vec![table.clone()],
                    JoinRightSide::Tables(ref ts) => ts.clone(),
                    JoinRightSide::NestedJoin(_) => unimplemented!(),
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
                        vec![TableAliasRewrite::ToTable {
                            from: alias.clone(),
                            to_table: name,
                        }]
                    }

                    _ => aliases
                        .into_iter()
                        .filter_map(|a| match a {
                            None => {
                                // No rewrite is needed for the unaliased table name.
                                None
                            }

                            Some(alias) => {
                                // The alias is one among multiple distinct references to the
                                // table. Create a globally unique view name, derived from the
                                // query name, and rewrite to remove the alias and refer to this
                                // view.
                                Some(TableAliasRewrite::ToView {
                                    from: alias.clone(),
                                    to_view: format!("__{}__{}", query_name, alias),
                                    for_table: name.clone(),
                                })
                            }
                        })
                        .collect(),
                })
                .collect();

            // Add an alias for any universe context table.
            let universe_rewrite = if let Some(universe_id) = context.get("id") {
                if let Some(g) = context.get("group") {
                    Some((
                        "GroupContext".to_string(),
                        format!("GroupContext_{}_{}", g.to_string(), universe_id.to_string()),
                    ))
                } else {
                    Some((
                        "UserContext".to_string(),
                        format!("UserContext_{}", universe_id.to_string()),
                    ))
                }
            } else {
                None
            };

            // Extract remappings for column tables from the alias rewrites.
            let col_table_remap = table_alias_rewrites
                .iter()
                .map(|r| match r {
                    TableAliasRewrite::ToTable { from, to_table } => {
                        (from.clone(), to_table.clone())
                    }
                    TableAliasRewrite::ToView { from, to_view, .. } => {
                        (from.clone(), to_view.clone())
                    }
                })
                .chain(universe_rewrite.into_iter())
                .collect();

            // Rewrite column table aliases in fields.
            sq.fields = sq
                .fields
                .iter()
                .map(|field| rewrite_field(&col_table_remap, field))
                .collect();

            // Rewrite column table aliases in join constraints.
            sq.join = sq
                .join
                .iter()
                .map(|jc| {
                    let mut jc = jc.clone();
                    jc.constraint = match jc.constraint {
                        JoinConstraint::On(ref cond) => {
                            JoinConstraint::On(rewrite_conditional(&col_table_remap, cond))
                        }
                        c @ JoinConstraint::Using(..) => c,
                    };
                    jc
                })
                .collect();

            // Rewrite column table aliases in conditions.
            sq.where_clause = match sq.where_clause {
                None => None,
                Some(ref wc) => Some(rewrite_conditional(&col_table_remap, wc)),
            };

            // Extract remappings for FROM and JOIN table references from the alias rewrites.
            let table_remap = table_alias_rewrites
                .iter()
                .filter_map(|r| match r {
                    TableAliasRewrite::ToView { from, to_view, .. } => {
                        Some((from.clone(), to_view.clone()))
                    }
                    _ => None,
                })
                .collect();

            // Rewrite tables in FROM clause.
            sq.tables = sq
                .tables
                .iter()
                .map(|t| rewrite_table(&table_remap, t))
                .collect();

            // Rewrite tables in JOIN clauses.
            sq.join = sq
                .join
                .iter()
                .map(|jc| {
                    let mut jc = jc.clone();
                    jc.right = match jc.right {
                        JoinRightSide::Table(ref t) => {
                            JoinRightSide::Table(rewrite_table(&table_remap, t))
                        }
                        JoinRightSide::Tables(ts) => JoinRightSide::Tables(
                            ts.iter()
                                .map(|ref t| rewrite_table(&table_remap, t))
                                .collect(),
                        ),
                        JoinRightSide::NestedJoin(_) => unimplemented!(),
                        r => r,
                    };
                    jc
                })
                .collect();

            table_alias_rewrites
        } else {
            // nothing to do for other query types, as they cannot have aliases
            vec![]
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{AliasRemoval, TableAliasRewrite};
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
        let mut res = SqlQuery::Select(q);
        let rewrites = res.rewrite_table_aliases("query", &context);
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
            vec![TableAliasRewrite::ToTable {
                from: String::from("t"),
                to_table: String::from("PaperTag")
            }]
        );
    }

    #[test]
    fn it_removes_nested_aliases() {
        use nom_sql::{
            BinaryOperator, ConditionBase, ConditionExpression, ConditionTree, FunctionArgument,
            FunctionExpression,
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
                operator: BinaryOperator::Equal,
                left: wrap(ConditionBase::Field(col_small.clone())),
                right: wrap(ConditionBase::Literal(Literal::Placeholder(
                    ItemPlaceholder::QuestionMark,
                ))),
            })),
            ..Default::default()
        };
        let mut context = HashMap::new();
        context.insert(String::from("id"), "global".into());
        let mut res = SqlQuery::Select(q);
        let rewrites = res.rewrite_table_aliases("query", &context);
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
                        operator: BinaryOperator::Equal,
                        left: wrap(ConditionBase::Field(col_full.clone())),
                        right: wrap(ConditionBase::Literal(Literal::Placeholder(
                            ItemPlaceholder::QuestionMark
                        ))),
                    }))
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
            vec![TableAliasRewrite::ToTable {
                from: String::from("t"),
                to_table: String::from("PaperTag")
            }]
        );
    }

    #[test]
    fn it_rewrites_duplicate_aliases() {
        use nom_sql::{
            parser, BinaryOperator, ConditionBase, ConditionExpression, ConditionTree, JoinClause,
            JoinConstraint, JoinOperator, JoinRightSide,
        };

        let wrap = |cb| Box::new(ConditionExpression::Base(cb));
        let mut res = parser::parse_query(
            "SELECT t1.id, t2.name FROM tab t1 JOIN tab t2 ON (t1.other = t2.id)",
        )
        .unwrap();
        let mut context = HashMap::new();
        context.insert(String::from("id"), "global".into());
        let rewrites = res.rewrite_table_aliases("query_name", &context);
        match res {
            SqlQuery::Select(tq) => {
                assert_eq!(
                    tq.fields,
                    vec![
                        FieldDefinitionExpression::Col(Column::from("__query_name__t1.id")),
                        FieldDefinitionExpression::Col(Column::from("__query_name__t2.name"))
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
                        constraint: JoinConstraint::On(ConditionExpression::ComparisonOp(
                            ConditionTree {
                                operator: BinaryOperator::Equal,
                                left: wrap(ConditionBase::Field(Column::from(
                                    "__query_name__t1.other"
                                ))),
                                right: wrap(ConditionBase::Field(Column::from(
                                    "__query_name__t2.id"
                                )))
                            }
                        ))
                    }]
                );
            }
            // if we get anything other than a selection query back, something really weird is up
            _ => panic!(),
        }

        assert_eq!(
            rewrites,
            vec![
                TableAliasRewrite::ToView {
                    from: String::from("t1"),
                    to_view: String::from("__query_name__t1"),
                    for_table: String::from("tab")
                },
                TableAliasRewrite::ToView {
                    from: String::from("t2"),
                    to_view: String::from("__query_name__t2"),
                    for_table: String::from("tab")
                }
            ]
        );
    }
}
