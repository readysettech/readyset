use std::fmt;

use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::combinator::{map, map_res, opt, value};
use nom::error::{ErrorKind, ParseError};
use nom::sequence::{preceded, tuple};
use nom_locate::LocatedSpan;
use prop::string::string_regex;
use proptest::option;
use proptest::prelude::prop;
use readyset_util::fmt::fmt_with;
use serde::{Deserialize, Serialize};
use test_strategy::Arbitrary;

use crate::expression::expression;
use crate::whitespace::{whitespace0, whitespace1};
use crate::{literal, Dialect, DialectDisplay, Expr, Literal, NomSqlResult};

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Arbitrary)]
pub struct ReadySetTablesOptions {
    pub all: bool,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub enum ShowStatement {
    Events,
    Tables(Tables),
    CachedQueries(Option<String>),
    ProxiedQueries(ProxiedQueriesOptions),
    ReadySetStatus,
    ReadySetStatusAdapter,
    ReadySetMigrationStatus(u64),
    ReadySetVersion,
    ReadySetTables(ReadySetTablesOptions),
    Connections,
    Databases,
}

impl DialectDisplay for ShowStatement {
    fn display(&self, dialect: Dialect) -> impl fmt::Display + '_ {
        fmt_with(move |f| {
            write!(f, "SHOW ")?;
            match self {
                Self::Events => write!(f, "EVENTS"),
                Self::Tables(tables) => write!(f, "{}", tables.display(dialect)),
                Self::CachedQueries(maybe_query_id) => {
                    if let Some(query_id) = maybe_query_id {
                        write!(f, "CACHES WHERE query_id = {}", query_id)
                    } else {
                        write!(f, "CACHES")
                    }
                }
                Self::ProxiedQueries(options) => {
                    write!(f, "PROXIED ")?;
                    if options.only_supported {
                        write!(f, "SUPPORTED ")?;
                    }
                    if let Some(query_id) = &options.query_id {
                        write!(f, "QUERIES WHERE query_id = {}", query_id)
                    } else {
                        write!(f, "QUERIES")
                    }
                }
                Self::ReadySetStatus => write!(f, "READYSET STATUS"),
                Self::ReadySetStatusAdapter => write!(f, "READYSET STATUS ADAPTER"),
                Self::ReadySetMigrationStatus(id) => write!(f, "READYSET MIGRATION STATUS {}", id),
                Self::ReadySetVersion => write!(f, "READYSET VERSION"),
                Self::ReadySetTables(options) => {
                    if options.all {
                        write!(f, "READYSET ALL TABLES")
                    } else {
                        write!(f, "READYSET TABLES")
                    }
                }
                Self::Connections => write!(f, "CONNECTIONS"),
                Self::Databases => write!(f, "DATABASES"),
            }
        })
    }
}

fn where_query_id(dialect: Dialect) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], String> {
    move |i| {
        let (i, _) = tag_no_case("where")(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, _) = tag_no_case("query_id")(i)?;
        let (i, _) = whitespace0(i)?;
        let (i, _) = tag_no_case("=")(i)?;
        let (i, _) = whitespace0(i)?;
        map_res(dialect.string_literal(), String::from_utf8)(i)
    }
}

fn cached_queries(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], ShowStatement> {
    move |i| {
        let (i, _) = tag_no_case("caches")(i)?;
        let (i, q_id) = opt(preceded(whitespace1, where_query_id(dialect)))(i)?;

        Ok((i, ShowStatement::CachedQueries(q_id)))
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub struct ProxiedQueriesOptions {
    #[strategy(option::of(string_regex("q_{a-z}{16}").unwrap()))]
    pub query_id: Option<String>,
    pub only_supported: bool,
    pub limit: Option<u64>,
}
pub fn limit(dialect: Dialect) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], u64> {
    move |i| {
        let (i, _) = tag_no_case("limit")(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, limit) = literal(dialect)(i)?;
        let limit = match &limit {
            Literal::UnsignedInteger(value) => Ok(*value),
            Literal::Integer(value) => Ok(*value as u64),
            _ => Err(nom::Err::Error(ParseError::from_error_kind(
                i,
                ErrorKind::Fail,
            ))),
        }?;
        Ok((i, limit))
    }
}

fn proxied_queries(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], ShowStatement> {
    move |i| {
        let (i, _) = tag_no_case("proxied")(i)?;
        let (i, only_supported) = map(
            opt(tuple((whitespace1, tag_no_case("supported")))),
            |only_supported| only_supported.is_some(),
        )(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, _) = tag_no_case("queries")(i)?;
        let (i, query_id) = opt(preceded(whitespace1, where_query_id(dialect)))(i)?;
        let (i, limit) = opt(preceded(whitespace1, limit(dialect)))(i)?;

        Ok((
            i,
            ShowStatement::ProxiedQueries(ProxiedQueriesOptions {
                query_id,
                only_supported,
                limit,
            }),
        ))
    }
}

/// Parses READYSET STATUS and any READYSET STATUS <COMMAND> statements.
fn readyset_status() -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], ShowStatement> {
    move |i| {
        let (i, _) = tag_no_case("readyset")(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, _) = tag_no_case("status")(i)?;

        // check to see if there's any (optional) command after "status"
        let (i, statement) = opt(value(
            ShowStatement::ReadySetStatusAdapter,
            tuple((whitespace1, tag_no_case("adapter"))),
        ))(i)?;

        match statement {
            Some(s) => Ok((i, s)),
            None => Ok((i, ShowStatement::ReadySetStatus)),
        }
    }
}

/// Parses READYSET MIGRATION STATUS <u64_id>
pub fn readyset_migration_status(i: LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], ShowStatement> {
    let (i, _) = tag_no_case("readyset")(i)?;
    let (i, _) = whitespace1(i)?;
    let (i, _) = tag_no_case("migration")(i)?;
    let (i, _) = whitespace1(i)?;
    let (i, _) = tag_no_case("status")(i)?;
    let (i, _) = whitespace1(i)?;
    let (i, id) = nom::character::complete::u64(i)?;
    Ok((i, ShowStatement::ReadySetMigrationStatus(id)))
}

fn readyset_tables() -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], ShowStatement> {
    move |i| {
        let (i, _) = tag_no_case("readyset")(i)?;
        let (i, all) = map(opt(preceded(whitespace1, tag_no_case("all"))), |all| {
            all.is_some()
        })(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, _) = tag_no_case("tables")(i)?;
        Ok((
            i,
            ShowStatement::ReadySetTables(ReadySetTablesOptions { all }),
        ))
    }
}

pub fn show(dialect: Dialect) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], ShowStatement> {
    move |i| {
        let (i, _) = tag_no_case("show")(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, statement) = alt((
            value(ShowStatement::Databases, tag_no_case("databases")),
            cached_queries(dialect),
            proxied_queries(dialect),
            readyset_migration_status,
            readyset_status(),
            value(
                ShowStatement::ReadySetVersion,
                tuple((tag_no_case("readyset"), whitespace1, tag_no_case("version"))),
            ),
            readyset_tables(),
            map(show_tables(dialect), ShowStatement::Tables),
            value(ShowStatement::Events, tag_no_case("events")),
            value(ShowStatement::Connections, tag_no_case("connections")),
        ))(i)?;
        Ok((i, statement))
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub struct Tables {
    pub full: bool,
    pub from_db: Option<String>,
    pub filter: Option<FilterPredicate>,
}

impl DialectDisplay for Tables {
    fn display(&self, dialect: Dialect) -> impl fmt::Display + '_ {
        fmt_with(move |f| {
            if self.full {
                write!(f, "FULL ")?;
            }
            write!(f, "TABLES")?;
            if let Some(from_db) = self.from_db.as_ref() {
                write!(f, " FROM {}", from_db)?;
            }
            if let Some(filter) = self.filter.as_ref() {
                write!(f, " {}", filter.display(dialect))?;
            }
            Ok(())
        })
    }
}

fn show_tables(dialect: Dialect) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], Tables> {
    move |i| {
        let (i, full) = map(opt(tuple((tag_no_case("full"), whitespace1))), |full| {
            full.is_some()
        })(i)?;
        let (i, _) = tag_no_case("tables")(i)?;
        let (i, from_db) = opt(map(
            tuple((
                whitespace1,
                tag_no_case("from"),
                whitespace1,
                dialect.identifier(),
            )),
            |(_, _, _, from_db)| from_db.to_string(),
        ))(i)?;
        let (i, filter) = opt(filter_predicate(dialect))(i)?;
        Ok((
            i,
            Tables {
                full,
                from_db,
                filter,
            },
        ))
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub enum FilterPredicate {
    Like(String),
    Where(Expr),
}

impl DialectDisplay for FilterPredicate {
    fn display(&self, dialect: Dialect) -> impl fmt::Display + '_ {
        fmt_with(move |f| match self {
            Self::Like(like) => write!(f, "LIKE '{}'", like),
            Self::Where(expr) => write!(f, "WHERE {}", expr.display(dialect)),
        })
    }
}

fn filter_predicate(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], FilterPredicate> {
    move |i| {
        let (i, _) = whitespace1(i)?;
        let (i, predicate) = alt((
            map(
                tuple((tag_no_case("like"), whitespace1, dialect.string_literal())),
                |(_, _, s)| FilterPredicate::Like(String::from_utf8(s).unwrap_or_default()),
            ),
            map(
                tuple((tag_no_case("where"), whitespace1, expression(dialect))),
                |(_, _, expr)| FilterPredicate::Where(expr),
            ),
        ))(i)?;
        Ok((i, predicate))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{BinaryOperator, Column, Literal};

    #[test]
    fn show_tables() {
        let qstring1 = "SHOW TABLES";
        let qstring2 = "SHOW FULL TABLES";
        let qstring3 = "SHOW TABLES FROM db1";
        let qstring4 = "SHOW TABLES LIKE 'm%'";
        let qstring5 = "SHOW TABLES FROM db1 WHERE Tables_in_db1 = 't1'";
        let res1 = show(Dialect::MySQL)(LocatedSpan::new(qstring1.as_bytes()))
            .unwrap()
            .1;
        let res2 = show(Dialect::MySQL)(LocatedSpan::new(qstring2.as_bytes()))
            .unwrap()
            .1;
        let res3 = show(Dialect::MySQL)(LocatedSpan::new(qstring3.as_bytes()))
            .unwrap()
            .1;
        let res4 = show(Dialect::MySQL)(LocatedSpan::new(qstring4.as_bytes()))
            .unwrap()
            .1;
        let res5 = show(Dialect::MySQL)(LocatedSpan::new(qstring5.as_bytes()))
            .unwrap()
            .1;
        assert_eq!(
            res1,
            ShowStatement::Tables(Tables {
                full: false,
                from_db: None,
                filter: None,
            })
        );
        assert_eq!(
            res2,
            ShowStatement::Tables(Tables {
                full: true,
                from_db: None,
                filter: None,
            })
        );
        assert_eq!(
            res3,
            ShowStatement::Tables(Tables {
                full: false,
                from_db: Some("db1".to_string()),
                filter: None,
            })
        );
        assert_eq!(
            res4,
            ShowStatement::Tables(Tables {
                full: false,
                from_db: None,
                filter: Some(FilterPredicate::Like("m%".to_string())),
            })
        );
        assert_eq!(
            res5,
            ShowStatement::Tables(Tables {
                full: false,
                from_db: Some("db1".to_string()),
                filter: Some(FilterPredicate::Where(Expr::BinaryOp {
                    lhs: Box::new(Expr::Column(Column::from("Tables_in_db1"))),
                    op: BinaryOperator::Equal,
                    rhs: Box::new(Expr::Literal(Literal::String("t1".to_string()))),
                })),
            })
        );
    }

    #[test]
    fn show_events() {
        let qstring1 = "SHOW EVENTS";
        let qstring2 = "SHOW\tEVENTS";
        let res1 = show(Dialect::MySQL)(LocatedSpan::new(qstring1.as_bytes()))
            .unwrap()
            .1;
        let res2 = show(Dialect::MySQL)(LocatedSpan::new(qstring2.as_bytes()))
            .unwrap()
            .1;
        assert_eq!(res1, ShowStatement::Events);
        assert_eq!(res2, ShowStatement::Events);
    }

    #[test]
    fn show_caches() {
        let qstring1 = "SHOW CACHES";
        let res1 = show(Dialect::MySQL)(LocatedSpan::new(qstring1.as_bytes()))
            .unwrap()
            .1;
        let qstring2 = "SHOW\tCACHES\t";
        let res2 = show(Dialect::MySQL)(LocatedSpan::new(qstring2.as_bytes()))
            .unwrap()
            .1;
        assert_eq!(res1, ShowStatement::CachedQueries(None));
        assert_eq!(res2, ShowStatement::CachedQueries(None));
    }

    #[test]
    fn show_caches_where() {
        let qstring1 = "SHOW CACHES where query_id = 'test'";
        let res1 = show(Dialect::MySQL)(LocatedSpan::new(qstring1.as_bytes()))
            .unwrap()
            .1;
        assert_eq!(res1, ShowStatement::CachedQueries(Some("test".to_string())));
    }

    #[test]
    fn show_proxied_queries() {
        let proxied_queries_test = |qstring: &str, only_supported: bool, limit: Option<u64>| {
            let res = show(Dialect::MySQL)(LocatedSpan::new(qstring.as_bytes()))
                .unwrap()
                .1;
            assert_eq!(
                res,
                ShowStatement::ProxiedQueries(ProxiedQueriesOptions {
                    query_id: None,
                    only_supported,
                    limit,
                })
            );
        };

        proxied_queries_test("SHOW PROXIED QUERIES", false, None);
        proxied_queries_test("SHOW\tPROXIED\tQUERIES", false, None);
        proxied_queries_test("SHOW PROXIED SUPPORTED QUERIES", true, None);
        proxied_queries_test("SHOW\tPROXIED\tSUPPORTED\tQUERIES", true, None);
        proxied_queries_test("SHOW PROXIED QUERIES LIMIT 10", false, Some(10));
        proxied_queries_test("SHOW\tPROXIED\tSUPPORTED\tQUERIES LIMIT 20", true, Some(20));
    }

    #[test]
    fn show_proxied_queries_where() {
        let proxied_queries_where_test =
            |qstring: &str, query_id: Option<&str>, only_supported: bool, limit: Option<u64>| {
                let res = show(Dialect::MySQL)(LocatedSpan::new(qstring.as_bytes()))
                    .unwrap()
                    .1;
                assert_eq!(
                    res,
                    ShowStatement::ProxiedQueries(ProxiedQueriesOptions {
                        query_id: query_id.map(String::from),
                        only_supported,
                        limit,
                    })
                );
            };

        proxied_queries_where_test(
            "SHOW PROXIED QUERIES where query_id = 'test'",
            Some("test"),
            false,
            None,
        );
        proxied_queries_where_test(
            "SHOW PROXIED SUPPORTED QUERIES where query_id = 'test'",
            Some("test"),
            true,
            None,
        );
        proxied_queries_where_test(
            "SHOW PROXIED QUERIES where query_id = 'other'",
            Some("other"),
            false,
            None,
        );
        proxied_queries_where_test(
            "SHOW PROXIED SUPPORTED QUERIES where query_id = 'other'",
            Some("other"),
            true,
            None,
        );
        proxied_queries_where_test(
            "SHOW PROXIED QUERIES where query_id = 'test' LIMIT 10",
            Some("test"),
            false,
            Some(10),
        );
        proxied_queries_where_test(
            "SHOW PROXIED SUPPORTED QUERIES where query_id = 'test' LIMIT 20",
            Some("test"),
            true,
            Some(20),
        );
    }

    #[test]
    fn show_replication_status() {
        let qstring1 = "SHOW READYSET STATUS";
        let res1 = show(Dialect::MySQL)(LocatedSpan::new(qstring1.as_bytes()))
            .unwrap()
            .1;
        let qstring2 = "SHOW\tREADYSET\tSTATUS";
        let res2 = show(Dialect::MySQL)(LocatedSpan::new(qstring2.as_bytes()))
            .unwrap()
            .1;
        assert_eq!(res1, ShowStatement::ReadySetStatus);
        assert_eq!(res2, ShowStatement::ReadySetStatus);
    }

    #[test]
    fn show_readyset_version() {
        for &dialect in Dialect::ALL {
            let qstring1 = "SHOW READYSET VERSION";
            let res1 = show(dialect)(LocatedSpan::new(qstring1.as_bytes()))
                .unwrap()
                .1;
            let qstring2 = "SHOW\tREADYSET\tVERSION";
            let res2 = show(dialect)(LocatedSpan::new(qstring2.as_bytes()))
                .unwrap()
                .1;
            assert_eq!(res1, ShowStatement::ReadySetVersion);
            assert_eq!(res2, ShowStatement::ReadySetVersion);
        }
    }

    #[test]
    fn show_readyset_tables() {
        let res = test_parse!(show(Dialect::MySQL), b"SHOW READYSET TABLES");
        assert_eq!(
            res,
            ShowStatement::ReadySetTables(ReadySetTablesOptions { all: false })
        );
        let res = test_parse!(show(Dialect::MySQL), b"SHOW READYSET ALL TABLES");
        assert_eq!(
            res,
            ShowStatement::ReadySetTables(ReadySetTablesOptions { all: true })
        );
    }

    #[test]
    fn show_readyset_migration_status() {
        let res = test_parse!(
            show(Dialect::MySQL),
            b"SHOW\t READYSET\t MIGRATION\t STATUS\t 123456"
        );
        assert_eq!(res, ShowStatement::ReadySetMigrationStatus(123456))
    }

    #[test]
    fn show_connections() {
        assert_eq!(
            test_parse!(show(Dialect::MySQL), b"SHOW CONNECTIONS"),
            ShowStatement::Connections
        );
    }
}
