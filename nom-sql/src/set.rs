use nom::bytes::complete::{tag, tag_no_case};
use nom::character::complete::{multispace0, multispace1};
use serde::{Deserialize, Serialize};
use std::{fmt, str};

use crate::common::{literal, statement_terminator, Literal};
use crate::Dialect;
use nom::branch::alt;
use nom::combinator::{map, map_res, opt};
use nom::sequence::tuple;
use nom::IResult;

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct SetStatement {
    pub variable: String,
    pub value: Literal,
}

impl fmt::Display for SetStatement {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SET ")?;
        write!(f, "{} = {}", self.variable, self.value.to_string())?;
        Ok(())
    }
}

/// check for one of three ways to specify scope and reformat to a single formatting. Returns none
/// if scope is not specified
pub fn scope(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], Option<String>> {
    move |i| {
        // ignore scope parsing if dialect is not MySQL
        if !matches!(dialect, Dialect::MySQL) {
            return Ok((i, None));
        }
        let (i, scope) = opt(alt((
            map(
                map_res(
                    alt((
                        tag_no_case("session"),
                        tag_no_case("local"),
                        tag_no_case("global"),
                    )),
                    str::from_utf8,
                ),
                |s: &str| format!("@@{}.", s.to_uppercase()),
            ),
            map(
                map_res(
                    alt((
                        tag_no_case("@@session"),
                        tag_no_case("@@local"),
                        tag_no_case("@@global"),
                    )),
                    str::from_utf8,
                ),
                |s: &str| format!("{}.", s.to_uppercase()),
            ),
            // check for @@ last
            map(tag("@@"), |_| "@@SESSION.".into()),
        )))(i)?;

        //scope may be followed by '.' or multispace
        let (i, _) = alt((tag("."), multispace0))(i)?;
        Ok((i, scope))
    }
}

pub fn set(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], SetStatement> {
    move |i| {
        let (remaining_input, (_, _, scope, var, _, _, _, value, _)) = tuple((
            tag_no_case("set"),
            multispace1,
            scope(dialect),
            dialect.identifier(),
            multispace0,
            tag_no_case("="),
            multispace0,
            literal(dialect),
            statement_terminator,
        ))(i)?;

        let variable = if let Some(s) = scope {
            format!("{}{}", s, var)
        } else {
            var.into()
        };

        Ok((remaining_input, SetStatement { variable, value }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_set() {
        let qstring = "SET SQL_AUTO_IS_NULL = 0;";
        let res = set(Dialect::MySQL)(qstring.as_bytes());
        assert_eq!(
            res.unwrap().1,
            SetStatement {
                variable: "SQL_AUTO_IS_NULL".to_owned(),
                value: 0.into(),
            }
        );
    }

    #[test]
    fn user_defined_vars() {
        let qstring = "SET @var = 123;";
        let res = set(Dialect::MySQL)(qstring.as_bytes());
        assert_eq!(
            res.unwrap().1,
            SetStatement {
                variable: "@var".to_owned(),
                value: 123.into(),
            }
        );
    }

    #[test]
    fn format_set() {
        let qstring = "set autocommit=1";
        let expected = "SET autocommit = 1";
        let res = set(Dialect::MySQL)(qstring.as_bytes());
        assert_eq!(format!("{}", res.unwrap().1), expected);
    }

    #[test]
    fn global_set() {
        let qstring1 = "set gloBal var = 2";
        let qstring2 = "set @@gLobal.var = 2";
        let expected = "SET @@GLOBAL.var = 2";
        let res1 = set(Dialect::MySQL)(qstring1.as_bytes());
        let res2 = set(Dialect::MySQL)(qstring2.as_bytes());
        assert_eq!(format!("{}", res1.unwrap().1), expected);
        assert_eq!(format!("{}", res2.unwrap().1), expected);
    }

    #[test]
    fn session_set() {
        let qstring1 = "set @@Session.var = 1";
        let qstring2 = "set @@var = 1";
        let qstring3 = "set SeSsion var = 1";
        let expected = "SET @@SESSION.var = 1";
        let res1 = set(Dialect::MySQL)(qstring1.as_bytes());
        let res2 = set(Dialect::MySQL)(qstring2.as_bytes());
        let res3 = set(Dialect::MySQL)(qstring3.as_bytes());
        assert_eq!(format!("{}", res1.unwrap().1), expected);
        assert_eq!(format!("{}", res2.unwrap().1), expected);
        assert_eq!(format!("{}", res3.unwrap().1), expected);
    }
    #[test]
    fn local_set() {
        let qstring1 = "set lOcal var = 2";
        let qstring2 = "set @@local.var = 2";
        let expected = "SET @@LOCAL.var = 2";
        let res1 = set(Dialect::MySQL)(qstring1.as_bytes());
        let res2 = set(Dialect::MySQL)(qstring2.as_bytes());
        assert_eq!(format!("{}", res1.unwrap().1), expected);
        assert_eq!(format!("{}", res2.unwrap().1), expected);
    }
}
