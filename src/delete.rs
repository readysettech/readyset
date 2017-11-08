use nom::multispace;
use nom::{Err, ErrorKind, IResult, Needed};
use std::str;

use common::table_reference;
use condition::ConditionExpression;
use table::Table;
use select::where_clause;

#[derive(Clone, Debug, Default, Hash, PartialEq, Serialize, Deserialize)]
pub struct DeleteStatement {
    pub table: Table,
    pub where_clause: Option<ConditionExpression>,
}


named!(pub deletion<&[u8], DeleteStatement>,
    chain!(
        caseless_tag!("delete") ~
        delimited!(opt!(multispace), caseless_tag!("from"), opt!(multispace)) ~
        table: table_reference ~
        cond: opt!(where_clause) ~
        || {
            DeleteStatement {
                table: table,
                where_clause: cond,
            }
        }
    )
);

#[cfg(test)]
mod tests {
    use super::*;
    use column::Column;
    use common::{Literal, Operator};
    use condition::ConditionBase::*;
    use condition::ConditionExpression::*;
    use condition::ConditionTree;
    use table::Table;

    #[test]
    fn simple_delete() {
        let qstring = "DELETE FROM users;";
        let res = deletion(qstring.as_bytes());
        assert_eq!(
            res.unwrap().1,
            DeleteStatement {
                table: Table::from("users"),
                ..Default::default()
            }
        );
    }

    #[test]
    fn delete_with_where_clause() {
        let qstring = "DELETE FROM users WHERE id = 1;";
        let res = deletion(qstring.as_bytes());
        let expected_left = Base(Field(Column::from("id")));
        let expected_where_cond = Some(ComparisonOp(ConditionTree {
            left: Box::new(expected_left),
            right: Box::new(Base(Literal(Literal::Integer(1)))),
            operator: Operator::Equal,
        }));
        assert_eq!(
            res.unwrap().1,
            DeleteStatement {
                table: Table::from("users"),
                where_clause: expected_where_cond,
                ..Default::default()
            }
        );
    }
}
