use nom::character::complete::{multispace0, multispace1};
use serde::{Deserialize, Serialize};
use std::{fmt, str};

use crate::common::{statement_terminator, table_list};
use crate::table::Table;
use crate::Dialect;
use nom::bytes::complete::tag_no_case;
use nom::combinator::opt;
use nom::sequence::{delimited, tuple};
use nom::IResult;

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct DropTableStatement {
    pub tables: Vec<Table>,
    pub if_exists: bool,
}

impl fmt::Display for DropTableStatement {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "DROP TABLE ")?;
        if self.if_exists {
            write!(f, "IF EXISTS ")?;
        }
        let ts = self
            .tables
            .iter()
            .map(|t| format!("`{}`", t.name))
            .collect::<Vec<_>>()
            .join(", ");
        write!(f, "{}", ts)?;
        Ok(())
    }
}

pub fn drop_table(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], DropTableStatement> {
    move |i| {
        let (remaining_input, (_, _, _, opt_if_exists, _, tables, _, _, _, _)) = tuple((
            tag_no_case("drop"),
            multispace1,
            tag_no_case("table"),
            opt(tuple((
                multispace0,
                tag_no_case("if"),
                multispace1,
                tag_no_case("exists"),
                multispace0,
            ))),
            multispace0,
            table_list(dialect),
            multispace0,
            opt(delimited(
                multispace0,
                tag_no_case("restricted"),
                multispace0,
            )),
            opt(delimited(multispace0, tag_no_case("cascade"), multispace0)),
            statement_terminator,
        ))(i)?;

        Ok((
            remaining_input,
            DropTableStatement {
                tables,
                if_exists: opt_if_exists.is_some(),
            },
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table::Table;

    #[test]
    fn simple_drop_table() {
        let qstring = "DROP TABLE users;";
        let res = drop_table(Dialect::MySQL)(qstring.as_bytes());
        assert_eq!(
            res.unwrap().1,
            DropTableStatement {
                tables: vec![Table::from("users")],
                if_exists: false,
            }
        );
    }

    #[test]
    fn format_drop_table() {
        let qstring = "DROP TABLE IF EXISTS users,posts;";
        let expected = "DROP TABLE IF EXISTS `users`, `posts`";
        let res = drop_table(Dialect::MySQL)(qstring.as_bytes());
        assert_eq!(format!("{}", res.unwrap().1), expected);
    }
}
