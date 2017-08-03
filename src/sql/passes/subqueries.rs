use nom_sql::{SqlQuery, ConditionExpression, ConditionBase};
use nom_sql::ConditionExpression::*;
use nom_sql::ConditionBase::NestedSelect;

pub trait SubQueries {
    fn extract_and_replace_subqueries(self) -> (SqlQuery, Vec<(String, SqlQuery)>);
}

fn extract_subqueries_from_condition(ce: ConditionExpression) -> (ConditionExpression, Vec<(String, SqlQuery)>) {
    use nom_sql::FieldExpression;
    use nom_sql::ConditionTree;
    use nom_sql::Column;

    let mut sqs = Vec::new();
    match ce {
        ComparisonOp(ref ct) => {
            let (lfq, lqueries) = extract_subqueries_from_condition(*ct.left.clone());
            let (rfq, rqueries) = extract_subqueries_from_condition(*ct.right.clone());

            sqs.extend(lqueries);
            sqs.extend(rqueries);

            (ComparisonOp(ConditionTree {
                left: Box::new(lfq),
                right: Box::new(rfq),
                operator: ct.operator.clone()
            }),
            sqs)
        },
        LogicalOp(ref ct) => {
            let (lfq, lqueries) = extract_subqueries_from_condition(*ct.left.clone());
            let (rfq, rqueries) = extract_subqueries_from_condition(*ct.right.clone());

            sqs.extend(lqueries);
            sqs.extend(rqueries);

            (LogicalOp(ConditionTree {
                left: Box::new(lfq),
                right: Box::new(rfq),
                operator: ct.operator.clone()
            }),
            sqs)
        },
        NegationOp(bce) => {
            let (expr, queries) = extract_subqueries_from_condition(*bce);

            sqs.extend(queries);

            (NegationOp(Box::new(expr)), sqs)
        },

        Base(NestedSelect(ref bst)) => {
            let sq = SqlQuery::Select(*bst.clone());
            let qname = format!("q_{}", hash_query(&sq));
            sqs.push((qname.clone(), sq));

            let cols: Vec<Column> = bst.fields.iter().map(|fe| {
                match *fe {
                    FieldExpression::Col(ref c) => c.clone(),
                    _ => unimplemented!()
                }
            }).collect();
            assert_eq!(cols.len(), 1);

            let col = cols.first().unwrap();
            let new_col = Column {
                name: col.name.clone(),
                alias: col.alias.clone(),
                table: Some(qname),
                function: col.function.clone(),
            };

            (Base(ConditionBase::Field(new_col.clone())), sqs)
        },
        Base(cb) => (Base(cb), sqs)
    }
}

fn hash_query(q: &SqlQuery) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut h = DefaultHasher::new();
    q.hash(&mut h);
    h.finish()
}

impl SubQueries for SqlQuery {
    fn extract_and_replace_subqueries(self) -> (SqlQuery, Vec<(String, SqlQuery)>) {
        match self {
            SqlQuery::Select(ref st) => {
                if st.where_clause.is_some() {
                    let ce = st.clone().where_clause.unwrap();
                    let (new_ce, queries) = extract_subqueries_from_condition(ce);
                    let mut new_st = st.clone();
                    new_st.where_clause = Some(new_ce);
                    return (SqlQuery::Select(new_st), queries);
                }
            }
            _ => (),
        }

        (self, Vec::new())
    }
}

#[cfg(test)]
mod tests {
    use nom_sql::{ConditionTree, Operator,
        Table, FieldExpression, Column, SelectStatement, SqlQuery};
    use nom_sql::ConditionBase::*;
    use nom_sql::ConditionExpression::*;
    use super::*;

    fn wrap(cb: ConditionBase) -> Box<ConditionExpression>{
        Box::new(Base(cb))
    }
    #[test]
    fn it_extracts_and_replaces_subqueries() {
        // select userid from role where type=1
        let sq = SelectStatement {
            tables: vec![Table::from("role")],
            fields: vec![FieldExpression::Col(Column::from("userid"))],
            where_clause: Some(ComparisonOp(ConditionTree {
                operator: Operator::Equal,
                left: wrap(Field(Column::from("type"))),
                right: wrap(Literal(1.into()))
            })),
            ..Default::default()
        };

        let sqname = format!("q_{}", hash_query(&SqlQuery::Select(sq.clone())));

        // select pid from post where author in (select userid from role where type=1)
        let q = SelectStatement {
            tables: vec![Table::from("post")],
            fields: vec![FieldExpression::Col(Column::from("pid"))],
            where_clause: Some(ComparisonOp(ConditionTree {
                operator: Operator::In,
                left: wrap(Field(Column::from("author"))),
                right: wrap(NestedSelect(Box::new(sq.clone())))
            })),
            ..Default::default()
        };

        let expected = SqlQuery::Select(SelectStatement {
            tables: vec![Table::from("post")],
            fields: vec![FieldExpression::Col(Column::from("pid"))],
            where_clause: Some(ComparisonOp(ConditionTree {
                operator: Operator::In,
                left: wrap(Field(Column::from("author"))),
                right: wrap(Field(Column::from(format!("{}.userid", sqname).as_str())))
            })),
            ..Default::default()
        });

        let res = SqlQuery::Select(q).extract_and_replace_subqueries();

        assert_eq!(res.0, expected);
        assert_eq!(res.1, vec![(sqname, SqlQuery::Select(sq.clone()))]);
    }

    #[test]
    fn it_does_nothing_for_flat_queries() {
        // select userid from role where type=1
        let q = SqlQuery::Select(SelectStatement {
            tables: vec![Table::from("role")],
            fields: vec![FieldExpression::Col(Column::from("userid"))],
            where_clause: Some(ComparisonOp(ConditionTree {
                operator: Operator::Equal,
                left: wrap(Field(Column::from("type"))),
                right: wrap(Literal(1.into()))
            })),
            ..Default::default()
        });

        let expected = q.clone();

        let res = q.extract_and_replace_subqueries();

        assert_eq!(res.0, expected);
        assert_eq!(res.1, vec![]);
    }


    #[test]
    fn it_works_with_complex_queries() {
        // select users.name, articles.title, votes.uid \
        //          from articles, users, votes
        //          where users.id = articles.author \
        //          and votes.aid = articles.aid;

        let q = SqlQuery::Select(SelectStatement {
            tables: vec![Table::from("articles"), Table::from("users"), Table::from("votes")],
            fields: vec![
                FieldExpression::Col(Column::from("users.name")),
                FieldExpression::Col(Column::from("articles.title")),
                FieldExpression::Col(Column::from("votes.uid")),
            ],
            where_clause: Some(LogicalOp(ConditionTree {
                left: Box::new(ComparisonOp(ConditionTree {
                    left: wrap(Field(Column::from("users.id"))),
                    right: wrap(Field(Column::from("articles.author"))),
                    operator: Operator::Equal
                })),
                right: Box::new(ComparisonOp(ConditionTree {
                    left: wrap(Field(Column::from("votes.aid"))),
                    right: wrap(Field(Column::from("articles.aid"))),
                    operator: Operator::Equal
                })),
                operator: Operator::And,
            })),
            ..Default::default()
        });

        let expected = q.clone();

        let res = q.extract_and_replace_subqueries();

        assert_eq!(res.0, expected);
        assert_eq!(res.1, vec![]);
    }
}