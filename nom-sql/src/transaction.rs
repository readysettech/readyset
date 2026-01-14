use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::combinator::{map, opt};
use nom::sequence::tuple;
use nom_locate::LocatedSpan;
use readyset_sql::{ast::*, Dialect};

use crate::common::statement_terminator;
use crate::dialect::DialectParser;
use crate::whitespace::{whitespace0, whitespace1};
use crate::NomSqlResult;

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

// Parse rule for a ROLLBACK query.
// Supports: ROLLBACK [WORK | TRANSACTION] [TO [SAVEPOINT] savepoint_name]
pub fn rollback(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], RollbackStatement> {
    move |i| {
        let (i, _) = whitespace0(i)?;
        let (i, _) = tag_no_case("rollback")(i)?;
        // Optional WORK or TRANSACTION keyword
        let (i, _) = opt(tuple((
            whitespace1,
            alt((tag_no_case("work"), tag_no_case("transaction"))),
        )))(i)?;
        // Optional TO [SAVEPOINT] savepoint_name
        let (i, savepoint) = opt(tuple((
            whitespace1,
            tag_no_case("to"),
            whitespace1,
            opt(tuple((tag_no_case("savepoint"), whitespace1))),
            dialect.identifier(),
        )))(i)?;

        Ok((
            i,
            RollbackStatement {
                savepoint: savepoint.map(|(.., name)| name),
            },
        ))
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
        assert_eq!(res.unwrap().1, RollbackStatement { savepoint: None });

        let qstring = "    ROLLBACK       WORK   ";

        let res = rollback(Dialect::MySQL)(LocatedSpan::new(qstring.as_bytes()));
        assert_eq!(res.unwrap().1, RollbackStatement { savepoint: None });
    }

    #[test]
    fn rollback_to_savepoint() {
        // MySQL syntax: ROLLBACK TO SAVEPOINT sp1
        let qstring = "ROLLBACK TO SAVEPOINT sp1";
        let res = rollback(Dialect::MySQL)(LocatedSpan::new(qstring.as_bytes()));
        assert_eq!(
            res.unwrap().1,
            RollbackStatement {
                savepoint: Some("sp1".into())
            }
        );

        // MySQL syntax without SAVEPOINT keyword: ROLLBACK TO sp1
        let qstring = "ROLLBACK TO sp1";
        let res = rollback(Dialect::MySQL)(LocatedSpan::new(qstring.as_bytes()));
        assert_eq!(
            res.unwrap().1,
            RollbackStatement {
                savepoint: Some("sp1".into())
            }
        );

        // PostgreSQL syntax: ROLLBACK TRANSACTION TO SAVEPOINT sp1
        let qstring = "ROLLBACK TRANSACTION TO SAVEPOINT sp1";
        let res = rollback(Dialect::PostgreSQL)(LocatedSpan::new(qstring.as_bytes()));
        assert_eq!(
            res.unwrap().1,
            RollbackStatement {
                savepoint: Some("sp1".into())
            }
        );

        // PostgreSQL syntax: ROLLBACK WORK TO sp1
        let qstring = "ROLLBACK WORK TO sp1";
        let res = rollback(Dialect::PostgreSQL)(LocatedSpan::new(qstring.as_bytes()));
        assert_eq!(
            res.unwrap().1,
            RollbackStatement {
                savepoint: Some("sp1".into())
            }
        );
    }

    #[test]
    fn rollback_to_savepoint_quoted() {
        // MySQL with backtick-quoted identifier containing special characters
        let qstring = "ROLLBACK TO SAVEPOINT `my-savepoint`";
        let res = rollback(Dialect::MySQL)(LocatedSpan::new(qstring.as_bytes()));
        assert_eq!(
            res.unwrap().1,
            RollbackStatement {
                savepoint: Some("my-savepoint".into())
            }
        );

        // MySQL with backtick-quoted identifier containing spaces
        let qstring = "ROLLBACK TO SAVEPOINT `my savepoint`";
        let res = rollback(Dialect::MySQL)(LocatedSpan::new(qstring.as_bytes()));
        assert_eq!(
            res.unwrap().1,
            RollbackStatement {
                savepoint: Some("my savepoint".into())
            }
        );

        // PostgreSQL with double-quoted identifier containing special characters
        let qstring = r#"ROLLBACK TO SAVEPOINT "My-Savepoint""#;
        let res = rollback(Dialect::PostgreSQL)(LocatedSpan::new(qstring.as_bytes()));
        assert_eq!(
            res.unwrap().1,
            RollbackStatement {
                savepoint: Some("My-Savepoint".into())
            }
        );

        // PostgreSQL with double-quoted identifier preserves case
        let qstring = r#"ROLLBACK TO SAVEPOINT "MyMixedCase""#;
        let res = rollback(Dialect::PostgreSQL)(LocatedSpan::new(qstring.as_bytes()));
        assert_eq!(
            res.unwrap().1,
            RollbackStatement {
                savepoint: Some("MyMixedCase".into())
            }
        );
    }

    #[test]
    fn rollback_to_savepoint_reserved_keywords() {
        // MySQL with reserved keyword as quoted identifier
        let qstring = "ROLLBACK TO SAVEPOINT `select`";
        let res = rollback(Dialect::MySQL)(LocatedSpan::new(qstring.as_bytes()));
        assert_eq!(
            res.unwrap().1,
            RollbackStatement {
                savepoint: Some("select".into())
            }
        );

        // PostgreSQL with reserved keyword as quoted identifier
        let qstring = r#"ROLLBACK TO SAVEPOINT "select""#;
        let res = rollback(Dialect::PostgreSQL)(LocatedSpan::new(qstring.as_bytes()));
        assert_eq!(
            res.unwrap().1,
            RollbackStatement {
                savepoint: Some("select".into())
            }
        );
    }

    #[test]
    fn rollback_to_savepoint_case_sensitivity() {
        // PostgreSQL: unquoted identifiers are lowercased
        let qstring = "ROLLBACK TO SAVEPOINT MyMixedCase";
        let res = rollback(Dialect::PostgreSQL)(LocatedSpan::new(qstring.as_bytes()));
        assert_eq!(
            res.unwrap().1,
            RollbackStatement {
                savepoint: Some("mymixedcase".into())
            }
        );

        // MySQL: unquoted identifiers preserve case
        let qstring = "ROLLBACK TO SAVEPOINT MyMixedCase";
        let res = rollback(Dialect::MySQL)(LocatedSpan::new(qstring.as_bytes()));
        assert_eq!(
            res.unwrap().1,
            RollbackStatement {
                savepoint: Some("MyMixedCase".into())
            }
        );
    }
}
