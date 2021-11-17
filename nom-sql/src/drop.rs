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

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct DropQueryCacheStatement {
    pub name: String,
}

impl fmt::Display for DropQueryCacheStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DROP QUERY CACHE `{}`", self.name)
    }
}

pub fn drop_query_cache(
    dialect: Dialect,
) -> impl Fn(&[u8]) -> IResult<&[u8], DropQueryCacheStatement> {
    move |i| {
        let (i, _) = tag_no_case("drop")(i)?;
        let (i, _) = multispace1(i)?;
        let (i, _) = tag_no_case("query")(i)?;
        let (i, _) = multispace1(i)?;
        let (i, _) = tag_no_case("cache")(i)?;
        let (i, _) = multispace1(i)?;
        let (i, name) = dialect.identifier()(i)?;
        Ok((
            i,
            DropQueryCacheStatement {
                name: name.into_owned(),
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

    #[test]
    fn parse_drop_query_cache() {
        let res = test_parse!(drop_query_cache(Dialect::MySQL), b"DROP QUERY CACHE test");
        assert_eq!(res.name, "test".to_owned());
    }

    #[test]
    fn format_drop_query_cache() {
        let res = DropQueryCacheStatement {
            name: "test".to_owned(),
        }
        .to_string();
        assert_eq!(res, "DROP QUERY CACHE `test`");
    }
}
