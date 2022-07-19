use std::{fmt, str};

use nom::bytes::complete::tag_no_case;
use nom::combinator::opt;
use nom::sequence::{delimited, tuple};
use nom::IResult;
use serde::{Deserialize, Serialize};

use crate::common::{schema_table_reference, statement_terminator};
use crate::select::where_clause;
use crate::table::Table;
use crate::whitespace::whitespace1;
use crate::{Dialect, Expr};

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct DeleteStatement {
    pub table: Table,
    pub where_clause: Option<Expr>,
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
            delimited(whitespace1, tag_no_case("from"), whitespace1),
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
    use crate::table::Table;
    use crate::{BinaryOperator, Literal};

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
                table: Table {
                    schema: Some("db1".into()),
                    name: "users".into(),
                    alias: None
                },
                ..Default::default()
            }
        );
    }

    #[test]
    fn delete_with_where_clause() {
        let qstring = "DELETE FROM users WHERE id = 1;";
        let res = deletion(Dialect::MySQL)(qstring.as_bytes());
        let expected_left = Expr::Column(Column::from("id"));
        let expected_where_cond = Some(Expr::BinaryOp {
            lhs: Box::new(expected_left),
            rhs: Box::new(Expr::Literal(Literal::Integer(1))),
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
