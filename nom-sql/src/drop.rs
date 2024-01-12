use std::fmt::Display;
use std::{fmt, str};

use itertools::Itertools;
use nom::bytes::complete::tag_no_case;
use nom::combinator::{map, opt};
use nom::multi::separated_list1;
use nom::sequence::preceded;
use nom_locate::LocatedSpan;
use readyset_util::fmt::fmt_with;
use serde::{Deserialize, Serialize};
use test_strategy::Arbitrary;

use crate::common::{statement_terminator, ws_sep_comma};
use crate::table::{relation, table_list, Relation};
use crate::whitespace::whitespace1;
use crate::{Dialect, DialectDisplay, NomSqlResult};

fn if_exists(i: LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], bool> {
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

fn restrict_cascade(i: LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], (bool, bool)> {
    let (i, restrict) = opt(preceded(whitespace1, tag_no_case("restrict")))(i)?;
    let (i, cascade) = opt(preceded(whitespace1, tag_no_case("cascade")))(i)?;
    Ok((i, (restrict.is_some(), cascade.is_some())))
}

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub struct DropTableStatement {
    pub tables: Vec<Relation>,
    pub if_exists: bool,
}

impl DialectDisplay for DropTableStatement {
    fn display(&self, dialect: Dialect) -> impl Display + '_ {
        fmt_with(move |f| {
            write!(f, "DROP TABLE ")?;

            if self.if_exists {
                write!(f, "IF EXISTS ")?;
            }

            write!(
                f,
                "{}",
                self.tables
                    .iter()
                    .map(|t| dialect.quote_identifier(&t.name))
                    .join(", ")
            )
        })
    }
}

pub fn drop_table(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], DropTableStatement> {
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

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub struct DropAllProxiedQueriesStatement;

impl DialectDisplay for DropAllProxiedQueriesStatement {
    fn display(&self, _dialect: Dialect) -> impl fmt::Display + '_ {
        fmt_with(move |f| write!(f, "DROP ALL PROXIED QUERIES"))
    }
}

pub fn drop_all_proxied_queries(
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], DropAllProxiedQueriesStatement> {
    move |i| {
        let (i, _) = tag_no_case("drop")(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, _) = tag_no_case("all")(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, _) = tag_no_case("proxied")(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, _) = tag_no_case("queries")(i)?;

        Ok((i, DropAllProxiedQueriesStatement))
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub struct DropCacheStatement {
    pub name: Relation,
}

impl DialectDisplay for DropCacheStatement {
    fn display(&self, dialect: Dialect) -> impl Display + '_ {
        fmt_with(move |f| write!(f, "DROP CACHE {}", self.name.display(dialect)))
    }
}

impl DropCacheStatement {
    pub fn display_unquoted(&self) -> impl Display + Copy + '_ {
        fmt_with(move |f| write!(f, "DROP CACHE {}", self.name.display_unquoted()))
    }
}

pub fn drop_cached_query(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], DropCacheStatement> {
    move |i| {
        let (i, _) = tag_no_case("drop")(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, _) = tag_no_case("cache")(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, name) = relation(dialect)(i)?;
        let (i, _) = statement_terminator(i)?;
        Ok((i, DropCacheStatement { name }))
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub struct DropViewStatement {
    pub views: Vec<Relation>,
    pub if_exists: bool,
}

impl DialectDisplay for DropViewStatement {
    fn display(&self, dialect: Dialect) -> impl Display + '_ {
        fmt_with(move |f| {
            write!(f, "DROP VIEW ")?;
            if self.if_exists {
                write!(f, "IF EXISTS ")?;
            }
            write!(
                f,
                "{}",
                self.views
                    .iter()
                    .map(|view| view.display(dialect))
                    .join(", ")
            )
        })
    }
}

pub fn drop_view(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], DropViewStatement> {
    move |i| {
        let (i, _) = tag_no_case("drop")(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, _) = tag_no_case("view")(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, if_exists) = if_exists(i)?;
        let (i, views) = separated_list1(ws_sep_comma, relation(dialect))(i)?;
        let (i, _) = restrict_cascade(i)?;
        let (i, _) = statement_terminator(i)?;
        Ok((i, DropViewStatement { views, if_exists }))
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub struct DropAllCachesStatement {}

impl Display for DropAllCachesStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DROP ALL CACHES")
    }
}

pub fn drop_all_caches(i: LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], DropAllCachesStatement> {
    let (i, _) = tag_no_case("drop")(i)?;
    let (i, _) = whitespace1(i)?;
    let (i, _) = tag_no_case("all")(i)?;
    let (i, _) = whitespace1(i)?;
    let (i, _) = tag_no_case("caches")(i)?;
    Ok((i, DropAllCachesStatement {}))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table::Relation;

    #[test]
    fn simple_drop_table() {
        let qstring = "DROP TABLE users;";
        let res = drop_table(Dialect::MySQL)(LocatedSpan::new(qstring.as_bytes()));
        assert_eq!(
            res.unwrap().1,
            DropTableStatement {
                tables: vec![Relation::from("users")],
                if_exists: false,
            }
        );
    }

    #[test]
    fn drop_table_qualified() {
        let res = test_parse!(
            drop_table(Dialect::PostgreSQL),
            b"DROP TABLE schema1.t1, schema2.t2"
        );
        assert_eq!(
            res.tables,
            vec![
                Relation {
                    name: "t1".into(),
                    schema: Some("schema1".into())
                },
                Relation {
                    name: "t2".into(),
                    schema: Some("schema2".into())
                }
            ]
        )
    }

    #[test]
    fn parse_drop_cached_query() {
        let res = test_parse!(drop_cached_query(Dialect::MySQL), b"DROP CACHE test");
        assert_eq!(res.name, "test".into());
    }

    #[test]
    fn format_drop_cached_query() {
        let res = DropCacheStatement {
            name: "test".into(),
        }
        .display(Dialect::MySQL)
        .to_string();
        assert_eq!(res, "DROP CACHE `test`");
    }

    #[test]
    fn drop_single_view() {
        let res = test_parse!(drop_view(Dialect::MySQL), b"DroP   ViEw  v ;");
        assert_eq!(res.views, vec![Relation::from("v")]);
        assert!(!res.if_exists);
    }

    #[test]
    fn drop_view_if_exists() {
        let res = test_parse!(drop_view(Dialect::MySQL), b"DroP   ViEw  if EXISTS v ;");
        assert_eq!(res.views, vec![Relation::from("v")]);
        assert!(res.if_exists);
    }

    #[test]
    fn drop_multiple_views() {
        let res = test_parse!(drop_view(Dialect::MySQL), b"DroP   ViEw  v1,   v2, v3 ;");
        assert_eq!(
            res.views,
            vec![
                Relation::from("v1"),
                Relation::from("v2"),
                Relation::from("v3")
            ]
        );
        assert!(!res.if_exists);
    }

    #[test]
    fn parse_drop_all_proxied_queries() {
        test_parse!(
            drop_all_proxied_queries(),
            b"DroP    aLl       PrOXied      querIES"
        );
    }

    mod mysql {
        use super::*;

        #[test]
        fn format_drop_table() {
            let qstring = "DROP TABLE IF EXISTS users,posts;";
            let expected = "DROP TABLE IF EXISTS `users`, `posts`";
            let res = drop_table(Dialect::MySQL)(LocatedSpan::new(qstring.as_bytes()));
            assert_eq!(res.unwrap().1.display(Dialect::MySQL).to_string(), expected);
        }

        #[test]
        fn format_drop_view() {
            let stmt = DropViewStatement {
                views: vec!["v1".into(), "v2".into()],
                if_exists: true,
            };
            assert_eq!(
                stmt.display(Dialect::MySQL).to_string(),
                "DROP VIEW IF EXISTS `v1`, `v2`"
            );
        }
    }

    mod postgres {
        use super::*;

        #[test]
        fn format_drop_table() {
            let qstring = "DROP TABLE IF EXISTS users,posts;";
            let expected = "DROP TABLE IF EXISTS \"users\", \"posts\"";
            let res = drop_table(Dialect::MySQL)(LocatedSpan::new(qstring.as_bytes()));
            assert_eq!(
                res.unwrap().1.display(Dialect::PostgreSQL).to_string(),
                expected
            );
        }

        #[test]
        fn format_drop_view() {
            let stmt = DropViewStatement {
                views: vec!["v1".into(), "v2".into()],
                if_exists: true,
            };
            assert_eq!(
                stmt.display(Dialect::PostgreSQL).to_string(),
                "DROP VIEW IF EXISTS \"v1\", \"v2\""
            );
        }
    }
}
