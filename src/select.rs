use nom::{alphanumeric, eof, line_ending, space};
use std::str;

use parser::{ConditionTree, fieldlist, unsigned_number};

#[derive(Debug, PartialEq)]
pub struct GroupByClause {
    columns: Vec<String>,
    having: String, // XXX(malte): should this be an arbitrary expr?
}

#[derive(Debug, PartialEq)]
pub struct LimitClause {
    limit: u64,
    offset: u64,
}

#[derive(Debug, PartialEq)]
enum OrderType {
    OrderAscending,
    OrderDescending,
}

#[derive(Debug, PartialEq)]
pub struct OrderClause {
    order_type: OrderType,
    order_cols: Vec<String>, // TODO(malte): can this be an arbitrary expr?
}

#[derive(Debug, Default, PartialEq)]
pub struct SelectStatement {
    table: String,
    distinct: bool,
    fields: Vec<String>,
    where_clause: Option<ConditionTree>,
    group_by: Option<GroupByClause>,
    order: Option<OrderClause>,
    limit: Option<LimitClause>,
}

/// Parse LIMIT clause
named!(limit_clause<&[u8], LimitClause>,
    chain!(
        tag!("limit") ~
        space ~
        limit_val: unsigned_number ~
        space? ~
        offset_val: opt!(chain!(
                tag!("offset") ~
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
named!(where_clause<&[u8], ConditionTree>,
    chain!(
        tag!("where") ~
        space ~
        field: map_res!(alphanumeric, str::from_utf8) ~
        space? ~
        tag!("=") ~
        space? ~
        expr: map_res!(tag_s!(b"?"), str::from_utf8), // XXX(malte): broken!
        || {
            ConditionTree {
                field: String::from(field),
                expr: String::from(expr),
            }
        }
    )
);

/// Parse rule for a SQL selection query.
/// TODO(malte): support nested queries as selection targets
named!(pub selection<&[u8], SelectStatement>,
    chain!(
        tag!("select") ~
        space ~
        fields: fieldlist ~
        space ~
        tag!("from") ~
        space ~
        table: map_res!(alphanumeric, str::from_utf8) ~
        space? ~
        cond: opt!(where_clause) ~
        space? ~
        limit: opt!(limit_clause) ~
        space? ~
        alt!(eof | tag!(";") | line_ending),
        || {
            SelectStatement {
                table: String::from(table),
                distinct: false,
                fields: fields.iter().map(|s| String::from(*s)).collect(),
                where_clause: cond,
                group_by: None,
                order: None,
                limit: limit,
            }
        }
    )
);

mod tests {
    use super::*;
    use parser::ConditionTree;

    #[test]
    fn simple_select() {
        let qstring = "SELECT id, name FROM users;".to_lowercase();

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
        let qstring = "SELECT * FROM users;".to_lowercase();

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
        let qstring = "SELECT id,name FROM users;".to_lowercase();

        let res = selection(qstring.as_bytes());
        assert_eq!(res.unwrap().1,
                   SelectStatement {
                       table: String::from("users"),
                       fields: vec!["id".into(), "name".into()],
                       ..Default::default()
                   });
    }

    #[test]
    // XXX(malte): this test is broken, as we force the qstring to lowercase anyway!
    fn case_sensitivity() {
        let qstring_lc = "select id, name from users;".to_lowercase();
        let qstring_uc = "SELECT id, name FROM users;".to_lowercase();

        assert_eq!(selection(qstring_lc.as_bytes()).unwrap(),
                   selection(qstring_uc.as_bytes()).unwrap());
    }

    #[test]
    fn termination() {
        let qstring_sem = "select id, name from users;".to_lowercase();
        let qstring_linebreak = "select id, name from users\n".to_lowercase();

        assert_eq!(selection(qstring_sem.as_bytes()).unwrap(),
                   selection(qstring_linebreak.as_bytes()).unwrap());
    }

    #[test]
    fn where_clause() {
        let qstring = "select * from ContactInfo where email=?\n".to_lowercase();

        let res = selection(qstring.as_bytes());
        assert_eq!(res.unwrap().1,
                   SelectStatement {
                       table: String::from("ContactInfo").to_lowercase(),
                       fields: vec!["ALL".into()],
                       where_clause: Some(ConditionTree {
                           field: String::from("email"),
                           expr: String::from("?") }),
                       ..Default::default()
                   });
    }

    #[test]
    fn limit_clause() {
        let qstring1 = "select * from users limit 10\n".to_lowercase();
        let qstring2 = "select * from users limit 10 offset 10\n".to_lowercase();

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
}
