use nom_sql::ConditionExpression::*;
use nom_sql::{Column, ConditionBase, ConditionExpression, JoinRightSide, SqlQuery};

#[derive(Debug, PartialEq)]
pub enum Subquery<'a> {
    InJoin(&'a mut JoinRightSide),
    InComparison(&'a mut ConditionBase),
}

pub trait SubQueries {
    fn extract_subqueries(&mut self) -> Vec<Subquery>;
}

fn extract_subqueries_from_condition(ce: &mut ConditionExpression) -> Vec<Subquery> {
    use nom_sql::ConditionBase::NestedSelect;
    match *ce {
        ComparisonOp(ref mut ct) | LogicalOp(ref mut ct) => {
            let lb = extract_subqueries_from_condition(&mut *ct.left);
            let rb = extract_subqueries_from_condition(&mut *ct.right);

            lb.into_iter().chain(rb.into_iter()).collect()
        }
        NegationOp(ref mut bce) => extract_subqueries_from_condition(&mut *bce),
        Bracketed(ref mut bce) => extract_subqueries_from_condition(&mut *bce),
        Base(ref mut cb) => match *cb {
            NestedSelect(_) => vec![Subquery::InComparison(cb)],
            _ => vec![],
        },
    }
}

pub fn field_with_table_name(name: String, column: Column) -> ConditionBase {
    ConditionBase::Field(Column {
        name: column.name.clone(),
        alias: column.alias.clone(),
        table: Some(name),
        function: column.function.clone(),
    })
}

pub fn query_from_condition_base(cond: &ConditionBase) -> (SqlQuery, Column) {
    use nom_sql::ConditionBase::NestedSelect;
    use nom_sql::FieldDefinitionExpression;
    let (sq, column);
    match *cond {
        NestedSelect(ref bst) => {
            sq = SqlQuery::Select(*bst.clone());
            column = bst
                .fields
                .iter()
                .map(|fe| match *fe {
                    FieldDefinitionExpression::Col(ref c) => c.clone(),
                    _ => unreachable!(),
                })
                .nth(0)
                .unwrap();
        }
        _ => unreachable!(),
    };

    (sq, column)
}

impl SubQueries for SqlQuery {
    fn extract_subqueries(&mut self) -> Vec<Subquery> {
        let mut subqueries = Vec::new();
        if let SqlQuery::Select(ref mut st) = *self {
            for jc in &mut st.join {
                if let JoinRightSide::NestedSelect(_, _) = jc.right {
                    subqueries.push(Subquery::InJoin(&mut jc.right));
                }
            }
            if let Some(ref mut ce) = st.where_clause {
                subqueries.extend(extract_subqueries_from_condition(ce));
            }
        }

        subqueries
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nom_sql::ConditionBase::*;
    use nom_sql::{
        Column, ConditionTree, FieldDefinitionExpression, Operator, SelectStatement, SqlQuery,
        Table,
    };

    fn wrap(cb: ConditionBase) -> Box<ConditionExpression> {
        Box::new(Base(cb))
    }
    #[test]
    fn it_extracts_subqueries() {
        // select userid from role where type=1
        let sq = SelectStatement {
            tables: vec![Table::from("role")],
            fields: vec![FieldDefinitionExpression::Col(Column::from("userid"))],
            where_clause: Some(ComparisonOp(ConditionTree {
                operator: Operator::Equal,
                left: wrap(Field(Column::from("type"))),
                right: wrap(Literal(1.into())),
            })),
            ..Default::default()
        };

        let mut expected = NestedSelect(Box::new(sq.clone()));

        // select pid from post where author in (select userid from role where type=1)
        let st = SelectStatement {
            tables: vec![Table::from("post")],
            fields: vec![FieldDefinitionExpression::Col(Column::from("pid"))],
            where_clause: Some(ComparisonOp(ConditionTree {
                operator: Operator::In,
                left: wrap(Field(Column::from("author"))),
                right: wrap(expected.clone()),
            })),
            ..Default::default()
        };

        let mut q = SqlQuery::Select(st);
        let res = q.extract_subqueries();

        assert_eq!(res, vec![Subquery::InComparison(&mut expected)]);
    }

    #[test]
    fn it_does_nothing_for_flat_queries() {
        // select userid from role where type=1
        let mut q = SqlQuery::Select(SelectStatement {
            tables: vec![Table::from("role")],
            fields: vec![FieldDefinitionExpression::Col(Column::from("userid"))],
            where_clause: Some(ComparisonOp(ConditionTree {
                operator: Operator::Equal,
                left: wrap(Field(Column::from("type"))),
                right: wrap(Literal(1.into())),
            })),
            ..Default::default()
        });

        let res = q.extract_subqueries();
        let expected: Vec<Subquery> = Vec::new();

        assert_eq!(res, expected);
    }

    #[test]
    fn it_works_with_complex_queries() {
        // select users.name, articles.title, votes.uid \
        //          from articles, users, votes
        //          where users.id = articles.author \
        //          and votes.aid = articles.aid;

        let mut q = SqlQuery::Select(SelectStatement {
            tables: vec![
                Table::from("articles"),
                Table::from("users"),
                Table::from("votes"),
            ],
            fields: vec![
                FieldDefinitionExpression::Col(Column::from("users.name")),
                FieldDefinitionExpression::Col(Column::from("articles.title")),
                FieldDefinitionExpression::Col(Column::from("votes.uid")),
            ],
            where_clause: Some(LogicalOp(ConditionTree {
                left: Box::new(ComparisonOp(ConditionTree {
                    left: wrap(Field(Column::from("users.id"))),
                    right: wrap(Field(Column::from("articles.author"))),
                    operator: Operator::Equal,
                })),
                right: Box::new(ComparisonOp(ConditionTree {
                    left: wrap(Field(Column::from("votes.aid"))),
                    right: wrap(Field(Column::from("articles.aid"))),
                    operator: Operator::Equal,
                })),
                operator: Operator::And,
            })),
            ..Default::default()
        });

        let expected: Vec<Subquery> = Vec::new();

        let res = q.extract_subqueries();

        assert_eq!(res, expected);
    }
}
