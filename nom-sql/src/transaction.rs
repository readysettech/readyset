use std::fmt;

use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::combinator::{map, opt};
use nom::sequence::tuple;
use nom_locate::LocatedSpan;
use serde::{Deserialize, Serialize};
use test_strategy::Arbitrary;

use crate::common::statement_terminator;
use crate::whitespace::{whitespace0, whitespace1};
use crate::{Dialect, NomSqlResult};

// TODO(peter): Handle dialect differences.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub enum StartTransactionStatement {
    // TODO(ENG-2992): Implement optional fields for START TRANSACTION
    Start,
    Begin,
}

impl fmt::Display for StartTransactionStatement {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            StartTransactionStatement::Start => write!(f, "START TRANSACTION"),
            StartTransactionStatement::Begin => write!(f, "BEGIN"),
        }
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub struct CommitStatement;

impl fmt::Display for CommitStatement {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "COMMIT")
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub struct RollbackStatement;

impl fmt::Display for RollbackStatement {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ROLLBACK")
    }
}

// Parse rule for a START TRANSACTION query.
// TODO(peter): Handle dialect differences.
pub fn start_transaction(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], StartTransactionStatement> {
    move |i| {
        let (i, _) = whitespace0(i)?;
        let (i, stmt) = alt((
            map(
                tuple((
                    tag_no_case("start"),
                    whitespace1,
                    tag_no_case("transaction"),
                )),
                |_| StartTransactionStatement::Start,
            ),
            begin(dialect),
        ))(i)?;
        let (i, _) = tuple((whitespace0, statement_terminator))(i)?;

        Ok((i, stmt))
    }
}

fn begin(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], StartTransactionStatement> {
    move |i| match dialect {
        Dialect::MySQL => mysql_begin()(i),
        Dialect::PostgreSQL => postgres_begin()(i),
    }
}

fn postgres_begin() -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], StartTransactionStatement>
{
    move |i| {
        map(
            tuple((
                tag_no_case("begin"),
                opt(alt((
                    tuple((whitespace1, tag_no_case("work"))),
                    tuple((whitespace1, tag_no_case("transaction"))),
                ))),
            )),
            |_| StartTransactionStatement::Begin,
        )(i)
    }
}

fn mysql_begin() -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], StartTransactionStatement> {
    move |i| {
        map(
            tuple((
                tag_no_case("begin"),
                opt(tuple((whitespace1, tag_no_case("work")))),
            )),
            |_| StartTransactionStatement::Begin,
        )(i)
    }
}

// Parse rule for a COMMIT query.
// TODO:
// There are optional CHAIN and RELEASE controls for transactions that would require work beyond
// simply parsing but should be supported eventually.
//
// [MySQL](https://dev.mysql.com/doc/refman/5.7/en/commit.html) allows this at the end of COMMIT:
// [AND [NO] CHAIN] [[NO] RELEASE]
// [PostgreSQL](https://www.postgresql.org/docs/current/sql-commit.html) allows:
// [ AND [ NO ] CHAIN ]
pub fn commit(d: Dialect) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], CommitStatement> {
    move |i| {
        let (remaining_input, (_, _)) = match d {
            Dialect::MySQL => tuple((
                whitespace0,
                tuple((
                    tag_no_case("commit"),
                    opt(tuple((whitespace1, tag_no_case("work")))),
                )),
            ))(i)?,
            Dialect::PostgreSQL => tuple((
                whitespace0,
                tuple((
                    alt((tag_no_case("commit"), tag_no_case("end"))),
                    opt(tuple((
                        whitespace1,
                        alt((tag_no_case("work"), tag_no_case("transaction"))),
                    ))),
                )),
            ))(i)?,
        };

        Ok((remaining_input, CommitStatement))
    }
}

// Parse rule for a COMMIT query.
// TODO(peter): Handle dialect differences.
pub fn rollback(
    _: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], RollbackStatement> {
    move |i| {
        let (remaining_input, (_, _)) = tuple((
            whitespace0,
            tuple((
                tag_no_case("rollback"),
                opt(tuple((whitespace1, tag_no_case("work")))),
            )),
        ))(i)?;

        Ok((remaining_input, RollbackStatement))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn start_transaction_dialect_agnostic(dialect: Dialect) {
        let qstring = "    START       TRANSACTION ;  ";
        let res = start_transaction(dialect)(LocatedSpan::new(qstring.as_bytes()));
        assert_eq!(res.unwrap().1, StartTransactionStatement::Start,);
    }

    #[test]
    fn parses_start_transaction() {
        start_transaction_dialect_agnostic(Dialect::MySQL);
        start_transaction_dialect_agnostic(Dialect::PostgreSQL);
    }

    #[test]
    fn begin_transaction() {
        let qstring = " BEGIN  TRANSACTION; ";
        let res = start_transaction(Dialect::MySQL)(LocatedSpan::new(qstring.as_bytes()));
        assert!(res.is_err());
        let res = start_transaction(Dialect::PostgreSQL)(LocatedSpan::new(qstring.as_bytes()));
        assert_eq!(res.unwrap().1, StartTransactionStatement::Begin,);
    }

    fn begin_dialect_agnostic(dialect: Dialect) {
        let qstring = "    BEGIN       WORK;   ";
        let res = start_transaction(dialect)(LocatedSpan::new(qstring.as_bytes()));
        assert_eq!(res.unwrap().1, StartTransactionStatement::Begin,);

        let qstring = "    BEGIN;   ";
        let res = start_transaction(dialect)(LocatedSpan::new(qstring.as_bytes()));
        assert_eq!(res.unwrap().1, StartTransactionStatement::Begin,);
    }

    #[test]
    fn parses_begin() {
        begin_dialect_agnostic(Dialect::MySQL);
        begin_dialect_agnostic(Dialect::PostgreSQL);
    }

    #[test]
    fn commit_complex() {
        let qstring = "    COMMIT";

        let res = commit(Dialect::MySQL)(LocatedSpan::new(qstring.as_bytes()));
        assert_eq!(res.unwrap().1, CommitStatement,);

        let qstring = "    COMMIT       WORK   ";

        let res = commit(Dialect::MySQL)(LocatedSpan::new(qstring.as_bytes()));
        assert_eq!(res.unwrap().1, CommitStatement,);
    }

    #[test]
    fn commit_postgres() {
        let qstring = "    COMMIT";

        let res = commit(Dialect::PostgreSQL)(LocatedSpan::new(qstring.as_bytes()));
        assert_eq!(res.unwrap().1, CommitStatement,);

        let qstring = "    COMMIT       WORK   ";

        let res = commit(Dialect::PostgreSQL)(LocatedSpan::new(qstring.as_bytes()));
        assert_eq!(res.unwrap().1, CommitStatement,);

        let qstring = "    COMMIT       TRANSACTION   ";

        let res = commit(Dialect::PostgreSQL)(LocatedSpan::new(qstring.as_bytes()));
        assert_eq!(res.unwrap().1, CommitStatement,);

        let qstring = "    END";

        let res = commit(Dialect::PostgreSQL)(LocatedSpan::new(qstring.as_bytes()));
        assert_eq!(res.unwrap().1, CommitStatement,);

        let qstring = "    END       WORK   ";

        let res = commit(Dialect::PostgreSQL)(LocatedSpan::new(qstring.as_bytes()));
        assert_eq!(res.unwrap().1, CommitStatement,);

        let qstring = "    END       TRANSACTION   ";

        let res = commit(Dialect::PostgreSQL)(LocatedSpan::new(qstring.as_bytes()));
        assert_eq!(res.unwrap().1, CommitStatement,);
    }

    #[test]
    fn rollback_complex() {
        let qstring = "    ROLLBACK ";

        let res = rollback(Dialect::MySQL)(LocatedSpan::new(qstring.as_bytes()));
        assert_eq!(res.unwrap().1, RollbackStatement,);

        let qstring = "    ROLLBACK       WORK   ";

        let res = rollback(Dialect::MySQL)(LocatedSpan::new(qstring.as_bytes()));
        assert_eq!(res.unwrap().1, RollbackStatement,);
    }
}
