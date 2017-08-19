use nom::multispace;
use nom::{IResult, Err, ErrorKind, Needed};
use std::str;

use column::Column;
use common::FieldExpression;
use common::{field_definition_expr, field_list, unsigned_number, statement_terminator, table_list,
             table_reference, column_identifier_no_alias};
use condition::{condition_expr, ConditionExpression};
use join::{join_operator, JoinConstraint, JoinOperator, JoinRightSide};
use table::Table;

#[derive(Clone, Debug, Hash, PartialEq, Serialize, Deserialize)]
pub struct GroupByClause {
    pub columns: Vec<Column>,
    pub having: Option<ConditionExpression>,
}

#[derive(Clone, Debug, Hash, PartialEq, Serialize, Deserialize)]
pub struct JoinClause {
    pub operator: JoinOperator,
    pub right: JoinRightSide,
    pub constraint: JoinConstraint,
}

#[derive(Clone, Debug, Hash, PartialEq, Serialize, Deserialize)]
pub struct LimitClause {
    pub limit: u64,
    pub offset: u64,
}

#[derive(Clone, Debug, Hash, PartialEq, Serialize, Deserialize)]
pub enum OrderType {
    OrderAscending,
    OrderDescending,
}

#[derive(Clone, Debug, Hash, PartialEq, Serialize, Deserialize)]
pub struct OrderClause {
    pub columns: Vec<(Column, OrderType)>, // TODO(malte): can this be an arbitrary expr?
}

#[derive(Clone, Debug, Default, Hash, PartialEq, Serialize, Deserialize)]
pub struct SelectStatement {
    pub tables: Vec<Table>,
    pub distinct: bool,
    pub fields: Vec<FieldExpression>,
    pub join: Vec<JoinClause>,
    pub where_clause: Option<ConditionExpression>,
    pub group_by: Option<GroupByClause>,
    pub order: Option<OrderClause>,
    pub limit: Option<LimitClause>,
}

/// Parse GROUP BY clause
named!(group_by_clause<&[u8], GroupByClause>,
    complete!(chain!(
        multispace? ~
        caseless_tag!("group by") ~
        multispace ~
        group_columns: field_list ~
        having_clause: opt!(
            complete!(chain!(
                multispace? ~
                caseless_tag!("having") ~
                multispace? ~
                ce: condition_expr,
                || { ce }
            ))
        ),
    || {
        GroupByClause {
            columns: group_columns,
            having: having_clause,
        }
    }))
);

/// Parse LIMIT clause
named!(limit_clause<&[u8], LimitClause>,
    complete!(chain!(
        multispace? ~
        caseless_tag!("limit") ~
        multispace ~
        limit_val: unsigned_number ~
        offset_val: opt!(
            complete!(chain!(
                multispace? ~
                caseless_tag!("offset") ~
                multispace ~
                val: unsigned_number,
                || { val }
            ))
        ),
    || {
        LimitClause {
            limit: limit_val,
            offset: match offset_val {
                None => 0,
                Some(v) => v,
            },
        }
    }))
);

/// Parse JOIN clause
named!(join_clause<&[u8], JoinClause>,
    complete!(chain!(
        multispace? ~
        _natural: opt!(caseless_tag!("natural")) ~
        multispace? ~
        op: join_operator ~
        multispace ~
        right: alt_complete!(
              chain!(
                  table: table_reference,
                  || {
                      JoinRightSide::Table(table)
                  }
              )
            | chain!(
                  tables: delimited!(tag!("("), table_list, tag!(")")),
                  || {
                      JoinRightSide::Tables(tables)
                  }
              )
            | chain!(
                  select: delimited!(tag!("("), nested_selection, tag!(")")),
                  || {
                      JoinRightSide::NestedSelect(Box::new(select))
                  }
              )
            | chain!(
                  nested_join: delimited!(tag!("("), join_clause, tag!(")")),
                  || {
                      JoinRightSide::NestedJoin(Box::new(nested_join))
                  }
              )
        ) ~
        multispace ~
        constraint: alt_complete!(
              chain!(
                  caseless_tag!("using") ~
                  multispace ~
                  fields: delimited!(tag!("("), field_list, tag!(")")),
                  || {
                      JoinConstraint::Using(fields)
                  }
              )
            | chain!(
                  caseless_tag!("on") ~
                  multispace ~
                  cond: alt_complete!(delimited!(tag!("("), condition_expr, tag!(")"))
                                      | condition_expr),
                  || {
                      JoinConstraint::On(cond)
                  }
              )
        ),
    || {
        JoinClause {
            operator: op,
            right: right,
            constraint: constraint,
        }
    }))
);

/// Parse ORDER BY clause
named!(order_clause<&[u8], OrderClause>,
    complete!(chain!(
        multispace? ~
        caseless_tag!("order by") ~
        multispace ~
        order_expr: many0!(
            chain!(
                fieldname: column_identifier_no_alias ~
                ordering: opt!(
                    complete!(chain!(
                        multispace? ~
                            ordering: alt_complete!(
                                map!(caseless_tag!("desc"), |_| OrderType::OrderDescending)
                                    | map!(caseless_tag!("asc"), |_| OrderType::OrderAscending)
                            ),
                        || { ordering }
                    ))
                ) ~
                opt!(
                    complete!(chain!(
                        multispace? ~
                            tag!(",") ~
                            multispace?,
                        ||{}
                    ))
                ),
                || { (fieldname, ordering.unwrap_or(OrderType::OrderAscending)) }
            )
        ),
    || {
        OrderClause {
            columns: order_expr,
            // order: match ordering {
            //     None => OrderType::OrderAscending,
            //     Some(ref o) => o.clone(),
            // },
        }
    }))
);

/// Parse WHERE clause of a selection
named!(where_clause<&[u8], ConditionExpression>,
    complete!(chain!(
        multispace? ~
        caseless_tag!("where") ~
        multispace ~
        cond: condition_expr,
        || { cond }
    ))
);

/// Parse rule for a SQL selection query.
named!(pub selection<&[u8], SelectStatement>,
    chain!(
        select: nested_selection ~
        statement_terminator,
        || { select }
    )
);

named!(pub nested_selection<&[u8], SelectStatement>,
    chain!(
        caseless_tag!("select") ~
        multispace ~
        distinct: opt!(caseless_tag!("distinct")) ~
        multispace? ~
        fields: field_definition_expr ~
        delimited!(opt!(multispace), caseless_tag!("from"), opt!(multispace)) ~
        tables: table_list ~
        join: many0!(join_clause) ~
        cond: opt!(where_clause) ~
        group_by: opt!(group_by_clause) ~
        order: opt!(order_clause) ~
        limit: opt!(limit_clause) ~
        || {
            SelectStatement {
                tables: tables,
                distinct: distinct.is_some(),
                fields: fields,
                join: join,
                where_clause: cond,
                group_by: group_by,
                order: order,
                limit: limit,
            }
        }
    )
);

#[cfg(test)]
mod tests {
    use super::*;
    use column::{Column, FunctionExpression};
    use common::{FieldExpression, Operator};
    use condition::ConditionBase::*;
    use condition::ConditionExpression::*;
    use condition::ConditionTree;
    use table::Table;

    fn columns(cols: &[&str]) -> Vec<FieldExpression> {
        cols.iter()
            .map(|c| FieldExpression::Col(Column::from(*c)))
            .collect()
    }

    #[test]
    fn simple_select() {
        let qstring = "SELECT id, name FROM users;";

        let res = selection(qstring.as_bytes());
        assert_eq!(
            res.unwrap().1,
            SelectStatement {
                tables: vec![Table::from("users")],
                fields: columns(&["id", "name"]),
                ..Default::default()
            }
        );
    }

    #[test]
    fn more_involved_select() {
        let qstring = "SELECT users.id, users.name FROM users;";

        let res = selection(qstring.as_bytes());
        assert_eq!(
            res.unwrap().1,
            SelectStatement {
                tables: vec![Table::from("users")],
                fields: columns(&["users.id", "users.name"]),
                ..Default::default()
            }
        );
    }

    #[test]
    fn select_literals() {
        use common::Literal;

        let qstring = "SELECT NULL, 1, \"foo\", CURRENT_TIME FROM users;";

        let res = selection(qstring.as_bytes());
        assert_eq!(
            res.unwrap().1,
            SelectStatement {
                tables: vec![Table::from("users")],
                fields: vec![
                    FieldExpression::Literal(Literal::Null),
                    FieldExpression::Literal(1.into()),
                    FieldExpression::Literal("foo".into()),
                    FieldExpression::Literal(Literal::CurrentTime),
                ],
                ..Default::default()
            }
        );
    }

    #[test]
    fn select_all() {
        let qstring = "SELECT * FROM users;";

        let res = selection(qstring.as_bytes());
        assert_eq!(
            res.unwrap().1,
            SelectStatement {
                tables: vec![Table::from("users")],
                fields: vec![FieldExpression::All],
                ..Default::default()
            }
        );
    }

    #[test]
    fn select_all_in_table() {
        let qstring = "SELECT users.* FROM users, votes;";

        let res = selection(qstring.as_bytes());
        assert_eq!(
            res.unwrap().1,
            SelectStatement {
                tables: vec![Table::from("users"), Table::from("votes")],
                fields: vec![FieldExpression::AllInTable(String::from("users"))],
                ..Default::default()
            }
        );
    }

    #[test]
    fn spaces_optional() {
        let qstring = "SELECT id,name FROM users;";

        let res = selection(qstring.as_bytes());
        assert_eq!(
            res.unwrap().1,
            SelectStatement {
                tables: vec![Table::from("users")],
                fields: columns(&["id", "name"]),
                ..Default::default()
            }
        );
    }

    #[test]
    fn case_sensitivity() {
        let qstring_lc = "select id, name from users;";
        let qstring_uc = "SELECT id, name FROM users;";

        assert_eq!(
            selection(qstring_lc.as_bytes()).unwrap(),
            selection(qstring_uc.as_bytes()).unwrap()
        );
    }

    #[test]
    fn termination() {
        let qstring_sem = "select id, name from users;";
        let qstring_linebreak = "select id, name from users\n";

        let r1 = selection(qstring_sem.as_bytes()).unwrap();
        let r2 = selection(qstring_linebreak.as_bytes()).unwrap();
        assert_eq!(r1, r2);
    }

    #[test]
    fn where_clause() {
        let qstring = "select * from ContactInfo where email=?;";

        let res = selection(qstring.as_bytes());

        let expected_left = Base(Field(Column::from("email")));
        let expected_where_cond = Some(ComparisonOp(ConditionTree {
            left: Box::new(expected_left),
            right: Box::new(Base(Placeholder)),
            operator: Operator::Equal,
        }));
        assert_eq!(
            res.unwrap().1,
            SelectStatement {
                tables: vec![Table::from("ContactInfo")],
                fields: vec![FieldExpression::All],
                where_clause: expected_where_cond,
                ..Default::default()
            }
        );
    }

    #[test]
    fn limit_clause() {
        let qstring1 = "select * from users limit 10\n";
        let qstring2 = "select * from users limit 10 offset 10\n";

        let expected_lim1 = LimitClause {
            limit: 10,
            offset: 0,
        };
        let expected_lim2 = LimitClause {
            limit: 10,
            offset: 10,
        };

        let res1 = selection(qstring1.as_bytes());
        let res2 = selection(qstring2.as_bytes());
        assert_eq!(res1.unwrap().1.limit, Some(expected_lim1));
        assert_eq!(res2.unwrap().1.limit, Some(expected_lim2));
    }

    #[test]
    fn order_clause() {
        let qstring1 = "select * from users order by name desc\n";
        let qstring2 = "select * from users order by name asc, age desc\n";
        let qstring3 = "select * from users order by name\n";

        let expected_ord1 =
            OrderClause { columns: vec![("name".into(), OrderType::OrderDescending)] };
        let expected_ord2 = OrderClause {
            columns: vec![
                ("name".into(), OrderType::OrderAscending),
                ("age".into(), OrderType::OrderDescending),
            ],
        };
        let expected_ord3 =
            OrderClause { columns: vec![("name".into(), OrderType::OrderAscending)] };

        let res1 = selection(qstring1.as_bytes());
        let res2 = selection(qstring2.as_bytes());
        let res3 = selection(qstring3.as_bytes());
        assert_eq!(res1.unwrap().1.order, Some(expected_ord1));
        assert_eq!(res2.unwrap().1.order, Some(expected_ord2));
        assert_eq!(res3.unwrap().1.order, Some(expected_ord3));
    }


    #[test]
    fn table_alias() {
        let qstring1 = "select * from PaperTag as t;";
        // let qstring2 = "select * from PaperTag t;";

        let res1 = selection(qstring1.as_bytes());
        assert_eq!(
            res1.clone().unwrap().1,
            SelectStatement {
                tables: vec![
                    Table {
                        name: String::from("PaperTag"),
                        alias: Some(String::from("t")),
                    },
                ],
                fields: vec![FieldExpression::All],
                ..Default::default()
            }
        );
        // let res2 = selection(qstring2.as_bytes());
        // assert_eq!(res1.unwrap().1, res2.unwrap().1);
    }

    #[test]
    fn column_alias() {
        let qstring1 = "select name as TagName from PaperTag;";
        let qstring2 = "select PaperTag.name as TagName from PaperTag;";

        let res1 = selection(qstring1.as_bytes());
        assert_eq!(
            res1.clone().unwrap().1,
            SelectStatement {
                tables: vec![Table::from("PaperTag")],
                fields: vec![
                    FieldExpression::Col(Column {
                        name: String::from("name"),
                        alias: Some(String::from("TagName")),
                        table: None,
                        function: None,
                    }),
                ],
                ..Default::default()
            }
        );
        let res2 = selection(qstring2.as_bytes());
        assert_eq!(
            res2.clone().unwrap().1,
            SelectStatement {
                tables: vec![Table::from("PaperTag")],
                fields: vec![
                    FieldExpression::Col(Column {
                        name: String::from("name"),
                        alias: Some(String::from("TagName")),
                        table: Some(String::from("PaperTag")),
                        function: None,
                    }),
                ],
                ..Default::default()
            }
        );
    }

    #[test]
    fn column_alias_no_as() {
        let qstring1 = "select name TagName from PaperTag;";
        let qstring2 = "select PaperTag.name TagName from PaperTag;";

        let res1 = selection(qstring1.as_bytes());
        assert_eq!(
            res1.clone().unwrap().1,
            SelectStatement {
                tables: vec![Table::from("PaperTag")],
                fields: vec![
                    FieldExpression::Col(Column {
                        name: String::from("name"),
                        alias: Some(String::from("TagName")),
                        table: None,
                        function: None,
                    }),
                ],
                ..Default::default()
            }
        );
        let res2 = selection(qstring2.as_bytes());
        assert_eq!(
            res2.clone().unwrap().1,
            SelectStatement {
                tables: vec![Table::from("PaperTag")],
                fields: vec![
                    FieldExpression::Col(Column {
                        name: String::from("name"),
                        alias: Some(String::from("TagName")),
                        table: Some(String::from("PaperTag")),
                        function: None,
                    }),
                ],
                ..Default::default()
            }
        );
    }

    #[test]
    fn distinct() {
        let qstring = "select distinct tag from PaperTag where paperId=?;";

        let res = selection(qstring.as_bytes());
        let expected_left = Base(Field(Column::from("paperId")));
        let expected_where_cond = Some(ComparisonOp(ConditionTree {
            left: Box::new(expected_left),
            right: Box::new(Base(Placeholder)),
            operator: Operator::Equal,
        }));
        assert_eq!(
            res.unwrap().1,
            SelectStatement {
                tables: vec![Table::from("PaperTag")],
                distinct: true,
                fields: columns(&["tag"]),
                where_clause: expected_where_cond,
                ..Default::default()
            }
        );
    }

    #[test]
    fn simple_condition_expr() {
        let qstring = "select infoJson from PaperStorage where paperId=? and paperStorageId=?;";

        let res = selection(qstring.as_bytes());

        let left_ct = ConditionTree {
            left: Box::new(Base(Field(Column::from("paperId")))),
            right: Box::new(Base(Placeholder)),
            operator: Operator::Equal,
        };
        let left_comp = Box::new(ComparisonOp(left_ct));
        let right_ct = ConditionTree {
            left: Box::new(Base(Field(Column::from("paperStorageId")))),
            right: Box::new(Base(Placeholder)),
            operator: Operator::Equal,
        };
        let right_comp = Box::new(ComparisonOp(right_ct));
        let expected_where_cond = Some(LogicalOp(ConditionTree {
            left: left_comp,
            right: right_comp,
            operator: Operator::And,
        }));
        assert_eq!(
            res.unwrap().1,
            SelectStatement {
                tables: vec![Table::from("PaperStorage")],
                fields: columns(&["infoJson"]),
                where_clause: expected_where_cond,
                ..Default::default()
            }
        );
    }

    #[test]
    fn where_and_limit_clauses() {
        let qstring = "select * from users where id = ? limit 10\n";
        let res = selection(qstring.as_bytes());

        let expected_lim = Some(LimitClause {
            limit: 10,
            offset: 0,
        });
        let ct = ConditionTree {
            left: Box::new(Base(Field(Column::from("id")))),
            right: Box::new(Base(Placeholder)),
            operator: Operator::Equal,
        };
        let expected_where_cond = Some(ComparisonOp(ct));

        assert_eq!(
            res.unwrap().1,
            SelectStatement {
                tables: vec![Table::from("users")],
                fields: vec![FieldExpression::All],
                where_clause: expected_where_cond,
                limit: expected_lim,
                ..Default::default()
            }
        );
    }

    #[test]
    fn aggregation_column() {
        let qstring = "SELECT max(addr_id) FROM address;";

        let res = selection(qstring.as_bytes());
        let agg_expr = FunctionExpression::Max(Column::from("addr_id"));
        assert_eq!(
            res.unwrap().1,
            SelectStatement {
                tables: vec![Table::from("address")],
                fields: vec![
                    FieldExpression::Col(Column {
                        name: String::from("max(addr_id)"),
                        alias: None,
                        table: None,
                        function: Some(Box::new(agg_expr)),
                    }),
                ],
                ..Default::default()
            }
        );
    }

    #[test]
    fn aggregation_column_with_alias() {
        let qstring = "SELECT max(addr_id) AS max_addr FROM address;";

        let res = selection(qstring.as_bytes());
        let agg_expr = FunctionExpression::Max(Column::from("addr_id"));
        let expected_stmt = SelectStatement {
            tables: vec![Table::from("address")],
            fields: vec![
                FieldExpression::Col(Column {
                    name: String::from("max_addr"),
                    alias: Some(String::from("max_addr")),
                    table: None,
                    function: Some(Box::new(agg_expr)),
                }),
            ],
            ..Default::default()
        };
        assert_eq!(res.unwrap().1, expected_stmt);
    }

    #[test]
    fn count_all() {
        let qstring = "SELECT COUNT(*) FROM votes GROUP BY aid;";

        let res = selection(qstring.as_bytes());
        let agg_expr = FunctionExpression::CountStar;
        let expected_stmt = SelectStatement {
            tables: vec![Table::from("votes")],
            fields: vec![
                FieldExpression::Col(Column {
                    name: String::from("count(all)"),
                    alias: None,
                    table: None,
                    function: Some(Box::new(agg_expr)),
                }),
            ],
            group_by: Some(GroupByClause {
                columns: vec![Column::from("aid")],
                having: None,
            }),
            ..Default::default()
        };
        assert_eq!(res.unwrap().1, expected_stmt);
    }

    #[test]
    fn count_distinct() {
        let qstring = "SELECT COUNT(DISTINCT vote_id) FROM votes GROUP BY aid;";

        let res = selection(qstring.as_bytes());
        let agg_expr = FunctionExpression::Count(Column::from("vote_id"), true);
        let expected_stmt = SelectStatement {
            tables: vec![Table::from("votes")],
            fields: vec![
                FieldExpression::Col(Column {
                    name: String::from("count(distinct vote_id)"),
                    alias: None,
                    table: None,
                    function: Some(Box::new(agg_expr)),
                }),
            ],
            group_by: Some(GroupByClause {
                columns: vec![Column::from("aid")],
                having: None,
            }),
            ..Default::default()
        };
        assert_eq!(res.unwrap().1, expected_stmt);
    }

    #[test]
    fn moderately_complex_selection() {
        let qstring = "SELECT * FROM item, author WHERE item.i_a_id = author.a_id AND \
                       item.i_subject = ? ORDER BY item.i_title limit 50;";

        let res = selection(qstring.as_bytes());
        let expected_where_cond = Some(LogicalOp(ConditionTree {
            left: Box::new(ComparisonOp(ConditionTree {
                left: Box::new(Base(Field(Column::from("item.i_a_id")))),
                right: Box::new(Base(Field(Column::from("author.a_id")))),
                operator: Operator::Equal,
            })),
            right: Box::new(ComparisonOp(ConditionTree {
                left: Box::new(Base(Field(Column::from("item.i_subject")))),
                right: Box::new(Base(Placeholder)),
                operator: Operator::Equal,
            })),
            operator: Operator::And,
        }));
        assert_eq!(
            res.unwrap().1,
            SelectStatement {
                tables: vec![Table::from("item"), Table::from("author")],
                fields: vec![FieldExpression::All],
                where_clause: expected_where_cond,
                order: Some(OrderClause {
                    columns: vec![("item.i_title".into(), OrderType::OrderAscending)],
                }),
                limit: Some(LimitClause {
                    limit: 50,
                    offset: 0,
                }),
                ..Default::default()
            }
        );
    }

    #[test]
    fn simple_joins() {
        let qstring = "select paperId from PaperConflict join PCMember using (contactId);";

        let res = selection(qstring.as_bytes());
        let expected_stmt = SelectStatement {
            tables: vec![Table::from("PaperConflict")],
            fields: columns(&["paperId"]),
            join: vec![
                JoinClause {
                    operator: JoinOperator::Join,
                    right: JoinRightSide::Table(Table::from("PCMember")),
                    constraint: JoinConstraint::Using(vec![Column::from("contactId")]),
                },
            ],
            ..Default::default()
        };
        assert_eq!(res.unwrap().1, expected_stmt);

        // slightly simplified from
        // "select PCMember.contactId, group_concat(reviewType separator '')
        // from PCMember left join PaperReview on (PCMember.contactId=PaperReview.contactId)
        // group by PCMember.contactId"
        let qstring = "select PCMember.contactId \
                       from PCMember \
                       join PaperReview on (PCMember.contactId=PaperReview.contactId) \
                       order by contactId;";

        let res = selection(qstring.as_bytes());
        let ct = ConditionTree {
            left: Box::new(Base(Field(Column::from("PCMember.contactId")))),
            right: Box::new(Base(Field(Column::from("PaperReview.contactId")))),
            operator: Operator::Equal,
        };
        let join_cond = ConditionExpression::ComparisonOp(ct);
        let expected = SelectStatement {
            tables: vec![Table::from("PCMember")],
            fields: columns(&["PCMember.contactId"]),
            join: vec![
                JoinClause {
                    operator: JoinOperator::Join,
                    right: JoinRightSide::Table(Table::from("PaperReview")),
                    constraint: JoinConstraint::On(join_cond),
                },
            ],
            order: Some(OrderClause {
                columns: vec![("contactId".into(), OrderType::OrderAscending)],
            }),
            ..Default::default()
        };
        assert_eq!(res.unwrap().1, expected);

        // Same as above, but no brackets
        let qstring = "select PCMember.contactId \
                       from PCMember \
                       join PaperReview on PCMember.contactId=PaperReview.contactId \
                       order by contactId;";
        let res = selection(qstring.as_bytes());
        assert_eq!(res.unwrap().1, expected);
    }

    #[test]
    fn multi_join() {
        // simplified from
        // "select max(conflictType), PaperReview.contactId as reviewer, PCMember.contactId as
        //  pcMember, ChairAssistant.contactId as assistant, Chair.contactId as chair,
        //  max(PaperReview.reviewNeedsSubmit) as reviewNeedsSubmit from ContactInfo
        //  left join PaperReview using (contactId) left join PaperConflict using (contactId)
        //  left join PCMember using (contactId) left join ChairAssistant using (contactId)
        //  left join Chair using (contactId) where ContactInfo.contactId=?
        //  group by ContactInfo.contactId;";
        let qstring = "select PCMember.contactId, ChairAssistant.contactId, \
                       Chair.contactId from ContactInfo left join PaperReview using (contactId) \
                       left join PaperConflict using (contactId) left join PCMember using \
                       (contactId) left join ChairAssistant using (contactId) left join Chair \
                       using (contactId) where ContactInfo.contactId=?;";

        let res = selection(qstring.as_bytes());
        let ct = ConditionTree {
            left: Box::new(Base(Field(Column::from("ContactInfo.contactId")))),
            right: Box::new(Base(Placeholder)),
            operator: Operator::Equal,
        };
        let expected_where_cond = Some(ComparisonOp(ct));
        let mkjoin = |tbl: &str, col: &str| -> JoinClause {
            JoinClause {
                operator: JoinOperator::LeftJoin,
                right: JoinRightSide::Table(Table::from(tbl)),
                constraint: JoinConstraint::Using(vec![Column::from(col)]),
            }
        };
        assert_eq!(
            res.unwrap().1,
            SelectStatement {
                tables: vec![Table::from("ContactInfo")],
                fields: columns(
                    &[
                        "PCMember.contactId",
                        "ChairAssistant.contactId",
                        "Chair.contactId"
                    ]
                ),
                join: vec![
                    mkjoin("PaperReview", "contactId"),
                    mkjoin("PaperConflict", "contactId"),
                    mkjoin("PCMember", "contactId"),
                    mkjoin("ChairAssistant", "contactId"),
                    mkjoin("Chair", "contactId"),
                ],
                where_clause: expected_where_cond,
                ..Default::default()
            }
        );
    }

    #[test]
    fn nested_select() {
        let qstr = "SELECT ol_i_id FROM orders, order_line WHERE orders.o_c_id \
                    IN (SELECT o_c_id FROM orders, order_line WHERE orders.o_id = order_line.ol_o_id);";

        let res = selection(qstr.as_bytes());
        let inner_where_clause = ComparisonOp(ConditionTree {
            left: Box::new(Base(Field(Column::from("orders.o_id")))),
            right: Box::new(Base(Field(Column::from("order_line.ol_o_id")))),
            operator: Operator::Equal
        });

        let inner_select = SelectStatement {
            tables: vec![Table::from("orders"), Table::from("order_line")],
            fields: columns(&[
                "o_c_id"
            ]),
            where_clause: Some(inner_where_clause),
            ..Default::default()
        };

        let outer_where_clause = ComparisonOp(ConditionTree {
            left: Box::new(Base(Field(Column::from("orders.o_c_id")))),
            right: Box::new(Base(NestedSelect(Box::new(inner_select)))),
            operator: Operator::In,
        });

        let outer_select = SelectStatement {
            tables: vec![Table::from("orders"), Table::from("order_line")],
            fields: columns(&[
                "ol_i_id"
            ]),
            where_clause: Some(outer_where_clause),
            ..Default::default()
        };

        assert_eq!(res.unwrap().1, outer_select);

    }

    #[test]
    fn recursive_nested_select() {
        let qstr = "SELECT ol_i_id FROM orders, order_line WHERE orders.o_c_id \
                    IN (SELECT o_c_id FROM orders, order_line \
                    WHERE orders.o_id = order_line.ol_o_id \
                    AND orders.o_id > (SELECT MAX(o_id) FROM orders));";

        let res = selection(qstr.as_bytes());

        let agg_expr = FunctionExpression::Max(Column::from("o_id"));
        let recursive_select = SelectStatement {
            tables: vec![Table::from("orders")],
            fields: vec![
                FieldExpression::Col(Column {
                    name: String::from("max(o_id)"),
                    alias: None,
                    table: None,
                    function: Some(Box::new(agg_expr)),
                })
            ],
            ..Default::default()
        };

        let cop1 = ComparisonOp(ConditionTree {
            left: Box::new(Base(Field(Column::from("orders.o_id")))),
            right: Box::new(Base(Field(Column::from("order_line.ol_o_id")))),
            operator: Operator::Equal
        });

        let cop2 = ComparisonOp(ConditionTree {
            left: Box::new(Base(Field(Column::from("orders.o_id")))),
            right: Box::new(Base(NestedSelect(Box::new(recursive_select)))),
            operator: Operator::Greater,
        });

        let inner_where_clause = LogicalOp(ConditionTree {
            left: Box::new(cop1),
            right: Box::new(cop2),
            operator: Operator::And,
        });

        let inner_select = SelectStatement {
            tables: vec![Table::from("orders"), Table::from("order_line")],
            fields: columns(&[
                "o_c_id"
            ]),
            where_clause: Some(inner_where_clause),
            ..Default::default()
        };

        let outer_where_clause = ComparisonOp(ConditionTree {
            left: Box::new(Base(Field(Column::from("orders.o_c_id")))),
            right: Box::new(Base(NestedSelect(Box::new(inner_select)))),
            operator: Operator::In,
        });

        let outer_select = SelectStatement {
            tables: vec![Table::from("orders"), Table::from("order_line")],
            fields: columns(&[
                "ol_i_id"
            ]),
            where_clause: Some(outer_where_clause),
            ..Default::default()
        };

        assert_eq!(res.unwrap().1, outer_select);

    }

    #[test]
    fn join_against_nested_select() {
        let qstr = "SELECT o_id, ol_i_id FROM orders JOIN \
                   (SELECT ol_i_id FROM order_line) \
                   ON (orders.o_id = ol_i_id);";

        let res = selection(qstr.as_bytes());

        let inner_select = SelectStatement {
            tables: vec![Table::from("order_line")],
            fields: columns(&[
                "ol_i_id"
            ]),
            ..Default::default()
        };

        let outer_select = SelectStatement {
            tables: vec![Table::from("orders")],
            fields: columns(&[
                "o_id",
                "ol_i_id"
            ]),
            join: vec![JoinClause {
                operator: JoinOperator::Join,
                right: JoinRightSide::NestedSelect(Box::new(inner_select)),
                constraint: JoinConstraint::On(ComparisonOp(ConditionTree {
                    operator: Operator::Equal,
                    left: Box::new(Base(Field(Column::from("orders.o_id")))),
                    right: Box::new(Base(Field(Column::from("ol_i_id")))),
                })),
            }],
            ..Default::default()
        };

        assert_eq!(res.unwrap().1, outer_select);
    }
}
