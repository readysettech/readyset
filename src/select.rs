use nom::{alphanumeric, eof, line_ending, multispace, space};
use nom::{IResult, Err, ErrorKind, Needed};
use std::str;

use caseless_tag::*;
use parser::{ConditionBase, ConditionExpression, ConditionTree};
use parser::{binary_comparison_operator, binary_logical_operator, unary_comparison_operator};
use parser::{fieldlist, unsigned_number, statement_terminator};

use condition::*;

#[derive(Clone, Debug, PartialEq)]
pub struct GroupByClause {
    columns: Vec<String>,
    having: String, // XXX(malte): should this be an arbitrary expr?
}

#[derive(Clone, Debug, PartialEq)]
pub struct LimitClause {
    limit: u64,
    offset: u64,
}

#[derive(Clone, Debug, PartialEq)]
enum OrderType {
    OrderAscending,
    OrderDescending,
}

#[derive(Clone, Debug, PartialEq)]
pub struct OrderClause {
    order_type: OrderType,
    order_cols: Vec<String>, // TODO(malte): can this be an arbitrary expr?
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct SelectStatement {
    table: String,
    distinct: bool,
    fields: Vec<String>,
    where_clause: Option<ConditionExpression>,
    group_by: Option<GroupByClause>,
    order: Option<OrderClause>,
    limit: Option<LimitClause>,
}

/// Parse LIMIT clause
named!(limit_clause<&[u8], LimitClause>,
    chain!(
        space ~
        caseless_tag!("limit") ~
        space ~
        limit_val: unsigned_number ~
        space? ~
        offset_val: opt!(chain!(
                caseless_tag!("offset") ~
                space ~
                val: unsigned_number,
                || { val }
            )
        ),
    || {
        LimitClause {
            limit: limit_val,
            offset: match offset_val {
                None => 0,
                Some(v) => v,
            },
        }
    })
);

/// Parse WHERE clause of a selection
named!(where_clause<&[u8], ConditionExpression>,
    dbg_dmp!(chain!(
        space ~
        caseless_tag!("where") ~
        space ~
        cond: condition_expr,
        || { cond }
    ))
);

named!(table_reference<&[u8], &str>,
    chain!(
        table: map_res!(alphanumeric, str::from_utf8) ~
        alias: opt!(
            chain!(
                space ~
                caseless_tag!("as") ~
                space ~
                alias: map_res!(alphanumeric, str::from_utf8),
                || { println!("got alias: {} -> {}", table, alias); alias }
            )
        ),
        || { table }
    )
);

/// Parse rule for a SQL selection query.
/// TODO(malte): support nested queries as selection targets
named!(pub selection<&[u8], SelectStatement>,
    dbg_dmp!(chain!(
        caseless_tag!("select") ~
        space ~
        distinct: opt!(caseless_tag!("distinct")) ~
        space? ~
        fields: fieldlist ~
        delimited!(multispace, caseless_tag!("from"), multispace) ~
        table: table_reference ~
        cond: opt!(where_clause) ~
        limit: opt!(limit_clause) ~
        statement_terminator,
        || {
            SelectStatement {
                table: String::from(table),
                distinct: distinct.is_some(),
                fields: fields.iter().map(|s| String::from(*s)).collect(),
                where_clause: cond,
                group_by: None,
                order: None,
                limit: limit,
            }
        }
    ))
);

mod tests {
    use super::*;
    use parser::{ConditionBase, ConditionExpression, ConditionTree};

    #[test]
    fn simple_select() {
        let qstring = "SELECT id, name FROM users;";

        let res = selection(qstring.as_bytes());
        assert_eq!(res.unwrap().1,
                   SelectStatement {
                       table: String::from("users"),
                       fields: vec!["id".into(), "name".into()],
                       ..Default::default()
                   });
    }

    #[test]
    fn select_all() {
        let qstring = "SELECT * FROM users;";

        let res = selection(qstring.as_bytes());
        assert_eq!(res.unwrap().1,
                   SelectStatement {
                       table: String::from("users"),
                       fields: vec!["ALL".into()],
                       ..Default::default()
                   });
    }

    #[test]
    fn spaces_optional() {
        let qstring = "SELECT id,name FROM users;";

        let res = selection(qstring.as_bytes());
        assert_eq!(res.unwrap().1,
                   SelectStatement {
                       table: String::from("users"),
                       fields: vec!["id".into(), "name".into()],
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

        assert_eq!(selection(qstring_sem.as_bytes()).unwrap(),
                   selection(qstring_linebreak.as_bytes()).unwrap());
    }

    #[test]
    fn where_clause() {
        let qstring = "select * from ContactInfo where email=?;";

        let res = selection(qstring.as_bytes());

        let expected_where_cond = Some(ConditionExpression::ComparisonOp(ConditionTree {
            left:
                Some(Box::new(ConditionExpression::Expr(ConditionBase::Field(String::from("email"))))),
            right: Some(Box::new(ConditionExpression::Expr(ConditionBase::Placeholder))),
            operator: String::from("="),
        }));
        assert_eq!(res.unwrap().1,
                   SelectStatement {
                       table: String::from("ContactInfo"),
                       fields: vec!["ALL".into()],
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
    fn table_alias() {
        let qstring1 = "select * from PaperTag as t;";
        // let qstring2 = "select * from PaperTag t;";

        let res1 = selection(qstring1.as_bytes());
        assert_eq!(res1.clone().unwrap().1,
                   SelectStatement {
                       table: String::from("PaperTag"),
                       fields: vec!["ALL".into()],
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
                Some(Box::new(ConditionExpression::Expr(ConditionBase::Field(String::from("paperId"))))),
            right: Some(Box::new(ConditionExpression::Expr(ConditionBase::Placeholder))),
            operator: String::from("="),
        }));
        assert_eq!(res.unwrap().1,
                   SelectStatement {
                       table: String::from("PaperTag"),
                       distinct: true,
                       fields: vec!["tag".into()],
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
                Some(Box::new(ConditionExpression::Expr(ConditionBase::Field(String::from("paperId"))))),
            right: Some(Box::new(ConditionExpression::Expr(ConditionBase::Placeholder))),
            operator: String::from("="),
        })));
        let right_comp = Some(Box::new(ConditionExpression::ComparisonOp(ConditionTree {
            left:
                Some(Box::new(ConditionExpression::Expr(ConditionBase::Field(String::from("paperStorageId"))))),
            right: Some(Box::new(ConditionExpression::Expr(ConditionBase::Placeholder))),
            operator: String::from("="),
        })));
        let expected_where_cond = Some(ConditionExpression::LogicalOp(ConditionTree {
            left: left_comp,
            right: right_comp,
            operator: String::from("and"),
        }));
        println!("res: {:#?}", res);
        assert_eq!(res.unwrap().1,
                   SelectStatement {
                       table: String::from("PaperStorage"),
                       fields: vec!["infoJson".into()],
                       where_clause: expected_where_cond,
                       ..Default::default()
                   });
    }
}
