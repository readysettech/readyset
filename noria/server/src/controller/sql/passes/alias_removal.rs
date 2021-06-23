use dataflow::prelude::DataType;
use itertools::Itertools;
use nom_sql::{
    Column, CommonTableExpression, Expression, FieldDefinitionExpression, FunctionExpression,
    InValue, JoinConstraint, JoinRightSide, SelectStatement, SqlQuery, Table,
};
use std::collections::HashMap;

#[derive(Debug, PartialEq)]
pub enum TableAliasRewrite {
    /// An alias to a base table was rewritten
    ToTable { from: String, to_table: String },

    /// An alias to a view was rewritten
    ToView {
        from: String,
        to_view: String,
        for_table: String,
    },

    /// An alias to a common table expression was rewritten
    ToCTE {
        from: String,
        to_view: String,
        for_statement: Box<SelectStatement>, // box for perf
    },
}

pub trait AliasRemoval {
    /// Remove all table aliases, leaving tables unaliased if possible but rewriting the table name
    /// to a new view name derived from 'query_name' when necessary (ie when a single table is
    /// referenced by more than one alias). Return a list of the rewrites performed.
    fn rewrite_table_aliases(
        &mut self,
        query_name: &str,
        context: &HashMap<String, DataType>,
    ) -> Vec<TableAliasRewrite>;
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
        table,
        function,
    }
}

fn rewrite_expression(col_table_remap: &HashMap<String, String>, expr: &Expression) -> Expression {
    match expr {
        Expression::Column(col) => Expression::Column(rewrite_column(col_table_remap, col)),
        Expression::CaseWhen {
            condition,
            then_expr,
            else_expr,
        } => Expression::CaseWhen {
            condition: Box::new(rewrite_expression(col_table_remap, &condition)),
            then_expr: Box::new(rewrite_expression(col_table_remap, &then_expr)),
            else_expr: else_expr
                .as_ref()
                .map(|e| Box::new(rewrite_expression(col_table_remap, e))),
        },
        Expression::Literal(_) => expr.clone(),
        Expression::Call(fun) => {
            Expression::Call(rewrite_function_expression(col_table_remap, fun))
        }
        Expression::BinaryOp { lhs, op, rhs } => Expression::BinaryOp {
            lhs: Box::new(rewrite_expression(col_table_remap, lhs)),
            op: *op,
            rhs: Box::new(rewrite_expression(col_table_remap, rhs)),
        },
        Expression::UnaryOp { op, rhs } => Expression::UnaryOp {
            op: *op,
            rhs: Box::new(rewrite_expression(col_table_remap, rhs)),
        },
        Expression::Exists(_) | Expression::NestedSelect(_) => expr.clone(),
        Expression::Between {
            operand,
            min,
            max,
            negated,
        } => Expression::Between {
            operand: Box::new(rewrite_expression(col_table_remap, operand)),
            min: Box::new(rewrite_expression(col_table_remap, min)),
            max: Box::new(rewrite_expression(col_table_remap, max)),
            negated: *negated,
        },
        Expression::In { lhs, rhs, negated } => Expression::In {
            lhs: Box::new(rewrite_expression(col_table_remap, lhs)),
            rhs: match rhs {
                InValue::Subquery(_) => rhs.clone(),
                InValue::List(exprs) => InValue::List(
                    exprs
                        .iter()
                        .map(|expr| rewrite_expression(col_table_remap, expr))
                        .collect(),
                ),
            },
            negated: *negated,
        },
    }
}

fn rewrite_function_expression(
    col_table_remap: &HashMap<String, String>,
    function: &FunctionExpression,
) -> FunctionExpression {
    match function {
        FunctionExpression::Avg { expr, distinct } => FunctionExpression::Avg {
            expr: Box::new(rewrite_expression(col_table_remap, expr)),
            distinct: *distinct,
        },
        FunctionExpression::Count {
            expr,
            distinct,
            count_nulls,
        } => FunctionExpression::Count {
            expr: Box::new(rewrite_expression(col_table_remap, expr)),
            distinct: *distinct,
            count_nulls: *count_nulls,
        },
        FunctionExpression::CountStar => FunctionExpression::CountStar,
        FunctionExpression::Sum { expr, distinct } => FunctionExpression::Sum {
            expr: Box::new(rewrite_expression(col_table_remap, expr)),
            distinct: *distinct,
        },
        FunctionExpression::Max(arg) => {
            FunctionExpression::Max(Box::new(rewrite_expression(col_table_remap, arg)))
        }
        FunctionExpression::Min(arg) => {
            FunctionExpression::Min(Box::new(rewrite_expression(col_table_remap, arg)))
        }
        FunctionExpression::GroupConcat { expr, separator } => FunctionExpression::GroupConcat {
            expr: Box::new(rewrite_expression(col_table_remap, expr)),
            separator: separator.clone(),
        },
        FunctionExpression::Cast(arg, t) => FunctionExpression::Cast(
            Box::new(rewrite_expression(col_table_remap, arg)),
            t.clone(),
        ),
        FunctionExpression::Call { name, arguments } => FunctionExpression::Call {
            name: name.clone(),
            arguments: arguments
                .iter()
                .map(|arg| rewrite_expression(col_table_remap, arg))
                .collect(),
        },
    }
}

fn rewrite_field(
    col_table_remap: &HashMap<String, String>,
    field: &FieldDefinitionExpression,
) -> FieldDefinitionExpression {
    match field {
        FieldDefinitionExpression::Expression { expr, alias } => {
            FieldDefinitionExpression::Expression {
                expr: rewrite_expression(col_table_remap, expr),
                alias: alias.clone(),
            }
        }
        FieldDefinitionExpression::AllInTable(t) => {
            if col_table_remap.contains_key(t) {
                FieldDefinitionExpression::AllInTable(col_table_remap[t].clone())
            } else {
                FieldDefinitionExpression::AllInTable(t.clone())
            }
        }
        FieldDefinitionExpression::All => FieldDefinitionExpression::All,
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
                        .flatten()
                        .map(|alias| {
                            // The alias is one among multiple distinct references to the
                            // table. Create a globally unique view name, derived from the
                            // query name, and rewrite to remove the alias and refer to this
                            // view.
                            TableAliasRewrite::ToView {
                                from: alias.clone(),
                                to_view: format!("__{}__{}", query_name, alias),
                                for_table: name.clone(),
                            }
                        })
                        .collect(),
                })
                .chain(
                    sq.ctes
                        .drain(..)
                        .map(|CommonTableExpression { name, statement }| {
                            TableAliasRewrite::ToCTE {
                                to_view: format!("__{}__{}", query_name, name),
                                from: name,
                                for_statement: Box::new(statement),
                            }
                        }),
                )
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
                    TableAliasRewrite::ToCTE { from, to_view, .. } => {
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
                    match jc.right {
                        JoinRightSide::Table(Table { ref mut name, .. }) => {
                            if let Some(new_name) = col_table_remap.get(name) {
                                *name = new_name.clone()
                            }
                        }
                        JoinRightSide::Tables(ref mut tables) => {
                            for Table { ref mut name, .. } in tables {
                                if let Some(new_name) = col_table_remap.get(name) {
                                    *name = new_name.clone()
                                }
                            }
                        }
                        JoinRightSide::NestedSelect(_, _) | JoinRightSide::NestedJoin(_) => {}
                    }

                    jc.constraint = match jc.constraint {
                        JoinConstraint::On(ref cond) => {
                            JoinConstraint::On(rewrite_expression(&col_table_remap, cond))
                        }
                        c @ JoinConstraint::Using(..) => c,
                    };
                    jc
                })
                .collect();

            // Rewrite column table aliases in conditions.
            sq.where_clause = sq
                .where_clause
                .as_ref()
                .map(|wc| rewrite_expression(&col_table_remap, wc));

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

            if let Some(group_by) = &mut sq.group_by {
                group_by.columns = group_by
                    .columns
                    .iter()
                    .map(|col| rewrite_column(&col_table_remap, col))
                    .collect();
            }

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
    use maplit::hashmap;
    use nom_sql::{
        parse_query, parser, BinaryOperator, Column, Expression, FieldDefinitionExpression,
        ItemPlaceholder, JoinClause, JoinConstraint, JoinOperator, JoinRightSide, Literal,
        SelectStatement, SqlQuery, Table,
    };
    use std::collections::HashMap;

    macro_rules! rewrites_to {
        ($before: expr, $after: expr) => {{
            let mut res = parse_query($before).unwrap();
            let expected = parse_query($after).unwrap();
            let context = hashmap! {"t1".to_owned() => "global".into()};
            res.rewrite_table_aliases("query", &context);
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
        let mut context = HashMap::new();
        context.insert(String::from("id"), "global".into());
        let mut res = SqlQuery::Select(q);
        let rewrites = res.rewrite_table_aliases("query", &context);
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
            vec![TableAliasRewrite::ToTable {
                from: String::from("t"),
                to_table: String::from("PaperTag")
            }]
        );
    }

    #[test]
    fn it_removes_nested_aliases() {
        use nom_sql::{BinaryOperator, Expression, FunctionExpression};

        let col_small = Column {
            name: "count(t.id)".into(),
            table: None,
            function: Some(Box::new(FunctionExpression::Count {
                expr: Box::new(Expression::Column(Column {
                    name: "id".into(),
                    table: Some("t".into()),
                    function: None,
                })),
                distinct: true,
                count_nulls: false,
            })),
        };
        let col_full = Column {
            name: "count(t.id)".into(),
            table: None,
            function: Some(Box::new(FunctionExpression::Count {
                expr: Box::new(Expression::Column(Column {
                    name: "id".into(),
                    table: Some("PaperTag".into()),
                    function: None,
                })),
                distinct: true,
                count_nulls: false,
            })),
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
        let mut context = HashMap::new();
        context.insert(String::from("id"), "global".into());
        let mut res = SqlQuery::Select(q);
        let rewrites = res.rewrite_table_aliases("query", &context);
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
            vec![TableAliasRewrite::ToTable {
                from: String::from("t"),
                to_table: String::from("PaperTag")
            }]
        );
    }

    #[test]
    fn it_rewrites_duplicate_aliases() {
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
}
