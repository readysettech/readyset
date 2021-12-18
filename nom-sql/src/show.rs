use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::character::complete::multispace1;
use nom::combinator::{map, opt};
use nom::sequence::tuple;
use serde::{Deserialize, Serialize};
use std::fmt;

use crate::expression::expression;
use crate::{Dialect, Expression};
use nom::IResult;

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum ShowStatement {
    Events,
    Tables(Tables),
    QueryCaches,
    ProxiedQueries,
}

impl fmt::Display for ShowStatement {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SHOW ")?;
        match self {
            Self::Events => write!(f, "EVENTS"),
            Self::Tables(tables) => write!(f, "{}", tables),
            Self::QueryCaches => write!(f, "QUERY CACHES"),
            Self::ProxiedQueries => write!(f, "PROXIED QUERIES"),
        }
    }
}

pub fn show(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], ShowStatement> {
    move |i| {
        let (i, _) = tag_no_case("show")(i)?;
        let (i, _) = multispace1(i)?;
        let (i, statement) = alt((
            //ReadySet specific show statement
            map(
                tuple((tag_no_case("query"), multispace1, tag_no_case("caches"))),
                |_| ShowStatement::QueryCaches,
            ),
            map(
                tuple((tag_no_case("proxied"), multispace1, tag_no_case("queries"))),
                |_| ShowStatement::ProxiedQueries,
            ),
            map(show_tables(dialect), ShowStatement::Tables),
            map(tag_no_case("events"), |_| ShowStatement::Events),
        ))(i)?;
        Ok((i, statement))
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct Tables {
    full: bool,
    from_db: Option<String>,
    filter: Option<FilterPredicate>,
}

impl fmt::Display for Tables {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if self.full {
            write!(f, "FULL ")?;
        }
        write!(f, "TABLES")?;
        if let Some(from_db) = self.from_db.as_ref() {
            write!(f, " FROM {}", from_db)?;
        }
        if let Some(filter) = self.filter.as_ref() {
            write!(f, " {}", filter)?;
        }
        Ok(())
    }
}

fn show_tables(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], Tables> {
    move |i| {
        let (i, full) = map(opt(tuple((tag_no_case("full"), multispace1))), |full| {
            full.is_some()
        })(i)?;
        let (i, _) = tag_no_case("tables")(i)?;
        let (i, from_db) = opt(map(
            tuple((
                multispace1,
                tag_no_case("from"),
                multispace1,
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

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum FilterPredicate {
    Like(String),
    Where(Expression),
}

impl fmt::Display for FilterPredicate {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Like(like) => write!(f, "LIKE '{}'", like),
            Self::Where(expr) => write!(f, "WHERE {}", expr),
        }
    }
}

fn filter_predicate(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], FilterPredicate> {
    move |i| {
        let (i, _) = multispace1(i)?;
        let (i, predicate) = alt((
            map(
                tuple((tag_no_case("like"), multispace1, dialect.string_literal())),
                |(_, _, s)| FilterPredicate::Like(String::from_utf8(s).unwrap_or_default()),
            ),
            map(
                tuple((tag_no_case("where"), multispace1, expression(dialect))),
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
        let res1 = show(Dialect::MySQL)(qstring1.as_bytes()).unwrap().1;
        let res2 = show(Dialect::MySQL)(qstring2.as_bytes()).unwrap().1;
        let res3 = show(Dialect::MySQL)(qstring3.as_bytes()).unwrap().1;
        let res4 = show(Dialect::MySQL)(qstring4.as_bytes()).unwrap().1;
        let res5 = show(Dialect::MySQL)(qstring5.as_bytes()).unwrap().1;
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
                filter: Some(FilterPredicate::Where(Expression::BinaryOp {
                    lhs: Box::new(Expression::Column(Column::from("Tables_in_db1"))),
                    op: BinaryOperator::Equal,
                    rhs: Box::new(Expression::Literal(Literal::String("t1".to_string()))),
                })),
            })
        );
    }

    #[test]
    fn show_events() {
        let qstring1 = "SHOW EVENTS";
        let qstring2 = "SHOW\tEVENTS";
        let res1 = show(Dialect::MySQL)(qstring1.as_bytes()).unwrap().1;
        let res2 = show(Dialect::MySQL)(qstring2.as_bytes()).unwrap().1;
        assert_eq!(res1, ShowStatement::Events);
        assert_eq!(res2, ShowStatement::Events);
    }

    #[test]
    fn show_query_caches() {
        let qstring1 = "SHOW QUERY CACHES";
        let res1 = show(Dialect::MySQL)(qstring1.as_bytes()).unwrap().1;
        let qstring2 = "SHOW\tQUERY\tCACHES";
        let res2 = show(Dialect::MySQL)(qstring2.as_bytes()).unwrap().1;
        assert_eq!(res1, ShowStatement::QueryCaches);
        assert_eq!(res2, ShowStatement::QueryCaches);
    }

    #[test]
    fn show_proxied_queries() {
        let qstring1 = "SHOW PROXIED QUERIES";
        let res1 = show(Dialect::MySQL)(qstring1.as_bytes()).unwrap().1;
        let qstring2 = "SHOW\tPROXIED\tQUERIES";
        let res2 = show(Dialect::MySQL)(qstring2.as_bytes()).unwrap().1;
        assert_eq!(res1, ShowStatement::ProxiedQueries);
        assert_eq!(res2, ShowStatement::ProxiedQueries);
    }
}
