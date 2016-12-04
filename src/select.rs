use nom::multispace;
use nom::{IResult, Err, ErrorKind, Needed};
use std::str;

use column::Column;
use common::FieldExpression;
use common::{field_expr, field_list, unsigned_number, statement_terminator, table_list};
use parser::ConditionExpression;
use table::Table;

use condition::*;

#[derive(Clone, Debug, PartialEq)]
pub struct GroupByClause {
    columns: Vec<Column>,
    having: Option<ConditionExpression>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct LimitClause {
    limit: u64,
    offset: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub enum OrderType {
    OrderAscending,
    OrderDescending,
}

#[derive(Clone, Debug, PartialEq)]
pub struct OrderClause {
    cols: Vec<Column>, // TODO(malte): can this be an arbitrary expr?
    order: OrderType,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct SelectStatement {
    pub tables: Vec<Table>,
    pub distinct: bool,
    pub fields: FieldExpression,
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

named!(order_clause<&[u8], OrderClause>,
    complete!(chain!(
        multispace? ~
        caseless_tag!("order by") ~
        multispace ~
        order_expr: field_list ~
        ordering: opt!(
            complete!(chain!(
                multispace? ~
                ordering: alt_complete!(
                      map!(caseless_tag!("desc"), |_| OrderType::OrderDescending)
                    | map!(caseless_tag!("asc"), |_| OrderType::OrderAscending)
                ),
                || { ordering }
            ))
        ),
    || {
        OrderClause {
            cols: order_expr,
            order: match ordering {
                None => OrderType::OrderAscending,
                Some(ref o) => o.clone(),
            },
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
/// TODO(malte): support nested queries as selection targets
named!(pub selection<&[u8], SelectStatement>,
    chain!(
        caseless_tag!("select") ~
        multispace ~
        distinct: opt!(caseless_tag!("distinct")) ~
        multispace? ~
        fields: field_expr ~
        delimited!(opt!(multispace), caseless_tag!("from"), opt!(multispace)) ~
        tables: table_list ~
        cond: opt!(where_clause) ~
        group_by: opt!(group_by_clause) ~
        order: opt!(order_clause) ~
        limit: opt!(limit_clause) ~
        statement_terminator,
        || {
            SelectStatement {
                tables: tables,
                distinct: distinct.is_some(),
                fields: fields,
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
    use column::{AggregationExpression, Column};
    use common::{FieldExpression, Operator};
    use parser::{ConditionBase, ConditionExpression, ConditionTree};
    use table::Table;

    fn columns(cols: &[&str]) -> Vec<Column> {
        cols.iter()
            .map(|c| Column::from(*c))
            .collect()
    }

    #[test]
    fn simple_select() {
        let qstring = "SELECT id, name FROM users;";

        let res = selection(qstring.as_bytes());
        assert_eq!(res.unwrap().1,
                   SelectStatement {
                       tables: vec![Table::from("users")],
                       fields: FieldExpression::Seq(columns(&["id", "name"])),
                       ..Default::default()
                   });
    }

    #[test]
    fn select_all() {
        let qstring = "SELECT * FROM users;";

        let res = selection(qstring.as_bytes());
        assert_eq!(res.unwrap().1,
                   SelectStatement {
                       tables: vec![Table::from("users")],
                       fields: FieldExpression::All,
                       ..Default::default()
                   });
    }

    #[test]
    fn spaces_optional() {
        let qstring = "SELECT id,name FROM users;";

        let res = selection(qstring.as_bytes());
        assert_eq!(res.unwrap().1,
                   SelectStatement {
                       tables: vec![Table::from("users")],
                       fields: FieldExpression::Seq(columns(&["id", "name"])),
                       ..Default::default()
                   });
    }

    #[test]
    fn case_sensitivity() {
        let qstring_lc = "select id, name from users;";
        let qstring_uc = "SELECT id, name FROM users;";

        assert_eq!(selection(qstring_lc.as_bytes()).unwrap(),
                   selection(qstring_uc.as_bytes()).unwrap());
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

        let expected_where_cond = Some(ConditionExpression::ComparisonOp(ConditionTree {
            left:
                Some(Box::new(ConditionExpression::Base(ConditionBase::Field(Column::from("email"))))),
            right: Some(Box::new(ConditionExpression::Base(ConditionBase::Placeholder))),
            operator: Operator::Equal,
        }));
        assert_eq!(res.unwrap().1,
                   SelectStatement {
                       tables: vec![Table::from("ContactInfo")],
                       fields: FieldExpression::All,
                       where_clause: expected_where_cond,
                       ..Default::default()
                   });
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
        let qstring2 = "select * from users order by name, age desc\n";
        let qstring3 = "select * from users order by name\n";

        let expected_ord1 = OrderClause {
            cols: columns(&["name"]),
            order: OrderType::OrderDescending,
        };
        let expected_ord2 = OrderClause {
            cols: columns(&["name", "age"]),
            order: OrderType::OrderDescending,
        };
        let expected_ord3 = OrderClause {
            cols: columns(&["name"]),
            order: OrderType::OrderAscending,
        };

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
        assert_eq!(res1.clone().unwrap().1,
                   SelectStatement {
                       tables: vec![Table {
                           name: String::from("PaperTag"),
                           alias: Some(String::from("t")),
                       }],
                       fields: FieldExpression::All,
                       ..Default::default()
                   });
        // let res2 = selection(qstring2.as_bytes());
        // assert_eq!(res1.unwrap().1, res2.unwrap().1);
    }

    #[test]
    fn distinct() {
        let qstring = "select distinct tag from PaperTag where paperId=?;";

        let res = selection(qstring.as_bytes());
        let expected_where_cond = Some(ConditionExpression::ComparisonOp(ConditionTree {
            left:
                Some(Box::new(ConditionExpression::Base(ConditionBase::Field(Column::from("paperId"))))),
            right: Some(Box::new(ConditionExpression::Base(ConditionBase::Placeholder))),
            operator: Operator::Equal,
        }));
        assert_eq!(res.unwrap().1,
                   SelectStatement {
                       tables: vec![Table::from("PaperTag")],
                       distinct: true,
                       fields: FieldExpression::Seq(columns(&["tag"])),
                       where_clause: expected_where_cond,
                       ..Default::default()
                   });
    }

    #[test]
    fn simple_condition_expr() {
        let qstring = "select infoJson from PaperStorage where paperId=? and paperStorageId=?;";

        let res = selection(qstring.as_bytes());

        let left_comp = Some(Box::new(ConditionExpression::ComparisonOp(ConditionTree {
            left:
                Some(Box::new(ConditionExpression::Base(ConditionBase::Field(Column::from("paperId"))))),
            right: Some(Box::new(ConditionExpression::Base(ConditionBase::Placeholder))),
            operator: Operator::Equal,
        })));
        let right_comp = Some(Box::new(ConditionExpression::ComparisonOp(ConditionTree {
            left:
                Some(Box::new(ConditionExpression::Base(ConditionBase::Field(Column::from("paperStorageId"))))),
            right: Some(Box::new(ConditionExpression::Base(ConditionBase::Placeholder))),
            operator: Operator::Equal,
        })));
        let expected_where_cond = Some(ConditionExpression::LogicalOp(ConditionTree {
            left: left_comp,
            right: right_comp,
            operator: Operator::And,
        }));
        assert_eq!(res.unwrap().1,
                   SelectStatement {
                       tables: vec![Table::from("PaperStorage")],
                       fields: FieldExpression::Seq(columns(&["infoJson"])),
                       where_clause: expected_where_cond,
                       ..Default::default()
                   });
    }

    #[test]
    fn where_and_limit_clauses() {
        let qstring = "select * from users where id = ? limit 10\n";
        let res = selection(qstring.as_bytes());

        let expected_lim = Some(LimitClause {
            limit: 10,
            offset: 0,
        });
        let expected_where_cond = Some(ConditionExpression::ComparisonOp(ConditionTree {
            left:
                Some(Box::new(ConditionExpression::Base(ConditionBase::Field(Column::from("id"))))),
            right: Some(Box::new(ConditionExpression::Base(ConditionBase::Placeholder))),
            operator: Operator::Equal,
        }));

        assert_eq!(res.unwrap().1,
                   SelectStatement {
                       tables: vec![Table::from("users")],
                       fields: FieldExpression::All,
                       where_clause: expected_where_cond,
                       limit: expected_lim,
                       ..Default::default()
                   });
    }

    #[test]
    fn aggregation_column() {
        let qstring = "SELECT max(addr_id) FROM address;";

        let res = selection(qstring.as_bytes());
        let agg_expr =
            AggregationExpression::Max(FieldExpression::Seq(vec![Column::from("addr_id")]));
        assert_eq!(res.unwrap().1,
                   SelectStatement {
                       tables: vec![Table::from("address")],
                       fields: FieldExpression::Seq(vec![Column {
                                                             name: String::from("max"),
                                                             table: None,
                                                             aggregation: Some(agg_expr),
                                                         }]),
                       ..Default::default()
                   });
    }
}
