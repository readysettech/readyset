use nom::character::complete::multispace1;
use serde::{Deserialize, Serialize};
use std::{fmt, str};

use crate::common::{schema_table_reference, statement_terminator};
use crate::select::where_clause;
use crate::table::Table;
use crate::{Dialect, Expression};
use nom::bytes::complete::tag_no_case;
use nom::combinator::opt;
use nom::sequence::{delimited, tuple};
use nom::IResult;

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct DeleteStatement {
    pub table: Table,
    pub where_clause: Option<Expression>,
}

impl fmt::Display for DeleteStatement {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "DELETE FROM `{}`", self.table.name)?;
        if let Some(ref where_clause) = self.where_clause {
            write!(f, " WHERE ")?;
            write!(f, "{}", where_clause)?;
        }
        Ok(())
    }
}

pub fn deletion(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], DeleteStatement> {
    move |i| {
        let (remaining_input, (_, _, table, where_clause, _)) = tuple((
            tag_no_case("delete"),
            delimited(multispace1, tag_no_case("from"), multispace1),
            schema_table_reference(dialect),
            opt(where_clause(dialect)),
            statement_terminator,
        ))(i)?;

        Ok((
            remaining_input,
            DeleteStatement {
                table,
                where_clause,
            },
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column::Column;
    use crate::common::Literal;
    use crate::table::Table;
    use crate::BinaryOperator;

    #[test]
    fn simple_delete() {
        let qstring = "DELETE FROM users;";
        let res = deletion(Dialect::MySQL)(qstring.as_bytes());
        assert_eq!(
            res.unwrap().1,
            DeleteStatement {
                table: Table::from("users"),
                ..Default::default()
            }
        );
    }

    #[test]
    fn simple_delete_schema() {
        let qstring = "DELETE FROM db1.users;";
        let res = deletion(Dialect::MySQL)(qstring.as_bytes());
        assert_eq!(
            res.unwrap().1,
            DeleteStatement {
                table: Table::from(("db1", "users")),
                ..Default::default()
            }
        );
    }

    #[test]
    fn delete_with_where_clause() {
        let qstring = "DELETE FROM users WHERE id = 1;";
        let res = deletion(Dialect::MySQL)(qstring.as_bytes());
        let expected_left = Expression::Column(Column::from("id"));
        let expected_where_cond = Some(Expression::BinaryOp {
            lhs: Box::new(expected_left),
            rhs: Box::new(Expression::Literal(Literal::Integer(1))),
            op: BinaryOperator::Equal,
        });
        assert_eq!(
            res.unwrap().1,
            DeleteStatement {
                table: Table::from("users"),
                where_clause: expected_where_cond,
            }
        );
    }

    #[test]
    fn format_delete() {
        let qstring = "DELETE FROM users WHERE id = 1";
        let expected = "DELETE FROM `users` WHERE (`id` = 1)";
        let res = deletion(Dialect::MySQL)(qstring.as_bytes());
        assert_eq!(format!("{}", res.unwrap().1), expected);
    }
}
