use std::fmt::Display;
use std::{fmt, str};

use itertools::Itertools;
use nom::bytes::complete::tag_no_case;
use nom::combinator::{map, opt};
use nom::multi::separated_list1;
use nom::sequence::preceded;
use nom::IResult;
use serde::{Deserialize, Serialize};

use crate::common::{statement_terminator, table_list, ws_sep_comma};
use crate::table::Table;
use crate::whitespace::whitespace1;
use crate::{Dialect, SqlIdentifier};

fn if_exists(i: &[u8]) -> IResult<&[u8], bool> {
    map(
        opt(|i| {
            let (i, _) = tag_no_case("if")(i)?;
            let (i, _) = whitespace1(i)?;
            let (i, _) = tag_no_case("exists")(i)?;
            whitespace1(i)
        }),
        |r| r.is_some(),
    )(i)
}

fn restrict_cascade(i: &[u8]) -> IResult<&[u8], (bool, bool)> {
    let (i, restrict) = opt(preceded(whitespace1, tag_no_case("restrict")))(i)?;
    let (i, cascade) = opt(preceded(whitespace1, tag_no_case("cascade")))(i)?;
    Ok((i, (restrict.is_some(), cascade.is_some())))
}

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct DropTableStatement {
    pub tables: Vec<Table>,
    pub if_exists: bool,
}

impl Display for DropTableStatement {
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
        let (i, _) = tag_no_case("drop")(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, _) = tag_no_case("table")(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, if_exists) = if_exists(i)?;
        let (i, tables) = table_list(dialect)(i)?;
        let (i, _) = restrict_cascade(i)?;
        let (i, _) = statement_terminator(i)?;

        Ok((i, DropTableStatement { tables, if_exists }))
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct DropCacheStatement {
    pub name: SqlIdentifier,
}

impl Display for DropCacheStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DROP CACHE `{}`", self.name)
    }
}

pub fn drop_cached_query(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], DropCacheStatement> {
    move |i| {
        let (i, _) = tag_no_case("drop")(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, _) = tag_no_case("cache")(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, name) = dialect.identifier()(i)?;
        let (i, _) = statement_terminator(i)?;
        Ok((i, DropCacheStatement { name }))
    }
}
#[derive(Clone, Debug, Default, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct DropViewStatement {
    pub views: Vec<SqlIdentifier>,
    pub if_exists: bool,
}

impl Display for DropViewStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DROP VIEW ")?;
        if self.if_exists {
            write!(f, "IF EXISTS ")?;
        }
        write!(
            f,
            "{}",
            self.views.iter().map(|v| format!("`{}`", v)).join(", ")
        )
    }
}

pub fn drop_view(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], DropViewStatement> {
    move |i| {
        let (i, _) = tag_no_case("drop")(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, _) = tag_no_case("view")(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, if_exists) = if_exists(i)?;
        let (i, views) = separated_list1(ws_sep_comma, dialect.identifier())(i)?;
        let (i, _) = restrict_cascade(i)?;
        let (i, _) = statement_terminator(i)?;
        Ok((i, DropViewStatement { views, if_exists }))
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
    fn parse_drop_cached_query() {
        let res = test_parse!(drop_cached_query(Dialect::MySQL), b"DROP CACHE test");
        assert_eq!(res.name, "test");
    }

    #[test]
    fn format_drop_cached_query() {
        let res = DropCacheStatement {
            name: "test".into(),
        }
        .to_string();
        assert_eq!(res, "DROP CACHE `test`");
    }

    #[test]
    fn drop_single_view() {
        let res = test_parse!(drop_view(Dialect::MySQL), b"DroP   ViEw  v ;");
        assert_eq!(res.views, vec![SqlIdentifier::from("v")]);
        assert!(!res.if_exists);
    }

    #[test]
    fn drop_view_if_exists() {
        let res = test_parse!(drop_view(Dialect::MySQL), b"DroP   ViEw  if EXISTS v ;");
        assert_eq!(res.views, vec![SqlIdentifier::from("v")]);
        assert!(res.if_exists);
    }

    #[test]
    fn drop_multiple_views() {
        let res = test_parse!(drop_view(Dialect::MySQL), b"DroP   ViEw  v1,   v2, v3 ;");
        assert_eq!(
            res.views,
            vec![
                SqlIdentifier::from("v1"),
                SqlIdentifier::from("v2"),
                SqlIdentifier::from("v3")
            ]
        );
        assert!(!res.if_exists);
    }

    #[test]
    fn format_drop_view() {
        let stmt = DropViewStatement {
            views: vec!["v1".into(), "v2".into()],
            if_exists: true,
        };

        assert_eq!(stmt.to_string(), "DROP VIEW IF EXISTS `v1`, `v2`");
    }
}
