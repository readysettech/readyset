use std::{fmt, str};

use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::combinator::opt;
use nom::sequence::tuple;
use nom_locate::LocatedSpan;
use readyset_sql::Dialect;
use readyset_util::fmt::fmt_with;
use serde::{Deserialize, Serialize};
use test_strategy::Arbitrary;

use crate::dialect::DialectParser;
use crate::whitespace::whitespace1;
use crate::NomSqlResult;

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub struct DeallocateStatement {
    pub identifier: StatementIdentifier,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub enum StatementIdentifier {
    SingleStatement(String),
    AllStatements,
}

impl DeallocateStatement {
    pub fn display(&self, dialect: Dialect) -> impl fmt::Display + Copy + '_ {
        fmt_with(move |f| {
            write!(f, "DEALLOCATE ")?;

            // PREPARE is required for MySQL, but optional in PG (although
            // hardly used, so ignore it by convention)
            if dialect == Dialect::MySQL {
                write!(f, "PREPARE ")?;
            }

            match &self.identifier {
                StatementIdentifier::AllStatements => write!(f, "ALL"),
                StatementIdentifier::SingleStatement(id) => write!(f, "{}", id),
            }
        })
    }
}

pub fn deallocate(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], DeallocateStatement> {
    move |i| alt((postgres_deallocate(dialect), mysql_deallocate(dialect)))(i)
}

/// Parse the DEALLOCATE statement for Postgres [0]. It has the following definition:
///
/// DEALLOCATE [ PREPARE ] { name | ALL }
///
/// Interestingly, PG does allow the user to deallocate _all_ prepared statements in one shot.
///
/// [0] https://www.postgresql.org/docs/current/sql-deallocate.html
fn postgres_deallocate(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], DeallocateStatement> {
    move |i| {
        let (i, _) = tag_no_case("deallocate")(i)?;
        let (i, _) = whitespace1(i)?;

        // ignore "PREPARE" if the caller bothered to put it in the string
        let (i, _) = opt(tuple((tag_no_case("prepare"), whitespace1)))(i)?;
        let (i, all) = opt(tag_no_case("all"))(i)?;
        if all.is_some() {
            return Ok((
                i,
                DeallocateStatement {
                    identifier: StatementIdentifier::AllStatements,
                },
            ));
        }

        let (i, id) = dialect.identifier()(i)?;
        Ok((
            i,
            DeallocateStatement {
                identifier: StatementIdentifier::SingleStatement(id.to_string()),
            },
        ))
    }
}

/// Parse the DEALLOCATE statement for Postgres [0]. It has the following definition:
///
/// {DEALLOCATE | DROP} PREPARE stmt_name
///
/// [0] https://dev.mysql.com/doc/refman/8.0/en/deallocate-prepare.html
fn mysql_deallocate(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], DeallocateStatement> {
    move |i| {
        let (i, _) = alt((tag_no_case("deallocate"), tag_no_case("drop")))(i)?;
        let (i, _) = whitespace1(i)?;

        let (i, _) = tag_no_case("prepare")(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, id) = dialect.identifier()(i)?;

        Ok((
            i,
            DeallocateStatement {
                identifier: StatementIdentifier::SingleStatement(id.to_string()),
            },
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pg_dealloc_single() {
        let id = "a42";
        let target = DeallocateStatement {
            identifier: StatementIdentifier::SingleStatement(id.to_string()),
        };

        let qstring1 = "DEALLOCATE a42";
        let res1 = deallocate(Dialect::PostgreSQL)(LocatedSpan::new(qstring1.as_bytes()))
            .unwrap()
            .1;
        let qstring2 = "DEALLOCATE PREPARE a42";
        let res2 = deallocate(Dialect::PostgreSQL)(LocatedSpan::new(qstring2.as_bytes()))
            .unwrap()
            .1;

        assert_eq!(res1, target);
        assert_eq!(res2, target);
    }

    #[test]
    fn pg_dealloc_all() {
        let target = DeallocateStatement {
            identifier: StatementIdentifier::AllStatements,
        };

        let qstring1 = "DEALLOCATE ALL";
        let res1 = deallocate(Dialect::PostgreSQL)(LocatedSpan::new(qstring1.as_bytes()))
            .unwrap()
            .1;
        let qstring2 = "DEALLOCATE PREPARE ALL";
        let res2 = deallocate(Dialect::PostgreSQL)(LocatedSpan::new(qstring2.as_bytes()))
            .unwrap()
            .1;

        assert_eq!(res1, target);
        assert_eq!(res2, target);
    }

    #[test]
    fn mysql_dealloc() {
        let id = "a42";
        let target = DeallocateStatement {
            identifier: StatementIdentifier::SingleStatement(id.to_string()),
        };

        let qstring1 = "DEALLOCATE PREPARE a42";
        let res1 = deallocate(Dialect::MySQL)(LocatedSpan::new(qstring1.as_bytes()))
            .unwrap()
            .1;
        let qstring2 = "DROP PREPARE a42";
        let res2 = deallocate(Dialect::MySQL)(LocatedSpan::new(qstring2.as_bytes()))
            .unwrap()
            .1;

        assert_eq!(res1, target);
        assert_eq!(res2, target);
    }
}
