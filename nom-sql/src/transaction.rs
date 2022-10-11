use std::fmt;

use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::combinator::{map, opt};
use nom::sequence::tuple;
use nom_locate::LocatedSpan;
use serde::{Deserialize, Serialize};

use crate::whitespace::{whitespace0, whitespace1};
use crate::{Dialect, NomSqlResult};

// TODO(peter): Handle dialect differences.
#[derive(Clone, Debug, Default, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct StartTransactionStatement;

impl fmt::Display for StartTransactionStatement {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "START TRANSACTION")
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct CommitStatement;

impl fmt::Display for CommitStatement {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "COMMIT")
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct RollbackStatement;

impl fmt::Display for RollbackStatement {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ROLLBACK")
    }
}

// Parse rule for a START TRANSACTION query.
// TODO(peter): Handle dialect differences.
pub fn start_transaction(
    _: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], StartTransactionStatement> {
    move |i| {
        let (remaining_input, (_, _)) = tuple((
            whitespace0,
            alt((
                map(
                    tuple((
                        tag_no_case("start"),
                        whitespace1,
                        tag_no_case("transaction"),
                    )),
                    |_| (),
                ),
                map(
                    tuple((
                        tag_no_case("begin"),
                        opt(tuple((whitespace1, tag_no_case("work")))),
                    )),
                    |_| (),
                ),
            )),
        ))(i)?;

        Ok((remaining_input, StartTransactionStatement))
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

    #[test]
    fn start_transaction_simple() {
        let qstring = "START TRANSACTION";

        let res = start_transaction(Dialect::MySQL)(LocatedSpan::new(qstring.as_bytes()));
        assert_eq!(res.unwrap().1, StartTransactionStatement,);
    }

    #[test]
    fn start_transaction_complex() {
        let qstring = "    START       TRANSACTION   ";

        let res = start_transaction(Dialect::MySQL)(LocatedSpan::new(qstring.as_bytes()));
        assert_eq!(res.unwrap().1, StartTransactionStatement,);

        let qstring = "    BEGIN       WORK   ";

        let res = start_transaction(Dialect::MySQL)(LocatedSpan::new(qstring.as_bytes()));
        assert_eq!(res.unwrap().1, StartTransactionStatement,);
        let qstring = "    BEGIN    ";

        let res = start_transaction(Dialect::MySQL)(LocatedSpan::new(qstring.as_bytes()));
        assert_eq!(res.unwrap().1, StartTransactionStatement,);
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
