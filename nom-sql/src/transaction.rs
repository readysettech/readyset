use nom::character::complete::{multispace0, multispace1};
use std::fmt;

use crate::Dialect;
use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::combinator::{map, opt};
use nom::sequence::tuple;
use nom::IResult;

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
) -> impl Fn(&[u8]) -> IResult<&[u8], StartTransactionStatement> {
    move |i| {
        let (remaining_input, (_, _)) = tuple((
            multispace0,
            alt((
                map(
                    tuple((
                        tag_no_case("start"),
                        multispace1,
                        tag_no_case("transaction"),
                    )),
                    |_| (),
                ),
                map(
                    tuple((
                        tag_no_case("begin"),
                        opt(tuple((multispace1, tag_no_case("work")))),
                    )),
                    |_| (),
                ),
            )),
        ))(i)?;

        Ok((remaining_input, StartTransactionStatement))
    }
}

// Parse rule for a COMMIT query.
// TODO(peter): Handle dialect differences.
pub fn commit(_: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], CommitStatement> {
    move |i| {
        let (remaining_input, (_, _)) = tuple((
            multispace0,
            tuple((
                tag_no_case("commit"),
                opt(tuple((multispace1, tag_no_case("work")))),
            )),
        ))(i)?;

        Ok((remaining_input, CommitStatement))
    }
}

// Parse rule for a COMMIT query.
// TODO(peter): Handle dialect differences.
pub fn rollback(_: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], RollbackStatement> {
    move |i| {
        let (remaining_input, (_, _)) = tuple((
            multispace0,
            tuple((
                tag_no_case("rollback"),
                opt(tuple((multispace1, tag_no_case("work")))),
            )),
        ))(i)?;

        Ok((remaining_input, RollbackStatement))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{common::type_identifier, ColumnConstraint, Literal, SqlType};

    #[test]
    fn start_transaction_simple() {
        let qstring = "START TRANSACTION";

        let res = start_transaction(Dialect::MySQL)(qstring.as_bytes());
        assert_eq!(res.unwrap().1, StartTransactionStatement,);
    }

    #[test]
    fn start_transaction_complex() {
        let qstring = "    START       TRANSACTION   ";

        let res = start_transaction(Dialect::MySQL)(qstring.as_bytes());
        assert_eq!(res.unwrap().1, StartTransactionStatement,);

        let qstring = "    BEGIN       WORK   ";

        let res = start_transaction(Dialect::MySQL)(qstring.as_bytes());
        assert_eq!(res.unwrap().1, StartTransactionStatement,);
        let qstring = "    BEGIN    ";

        let res = start_transaction(Dialect::MySQL)(qstring.as_bytes());
        assert_eq!(res.unwrap().1, StartTransactionStatement,);
    }

    #[test]
    fn commit_complex() {
        let qstring = "    COMMIT";

        let res = commit(Dialect::MySQL)(qstring.as_bytes());
        assert_eq!(res.unwrap().1, CommitStatement,);

        let qstring = "    COMMIT       WORK   ";

        let res = commit(Dialect::MySQL)(qstring.as_bytes());
        assert_eq!(res.unwrap().1, CommitStatement,);
    }

    #[test]
    fn rollback_complex() {
        let qstring = "    ROLLBACK ";

        let res = rollback(Dialect::MySQL)(qstring.as_bytes());
        assert_eq!(res.unwrap().1, RollbackStatement,);

        let qstring = "    ROLLBACK       WORK   ";

        let res = rollback(Dialect::MySQL)(qstring.as_bytes());
        assert_eq!(res.unwrap().1, RollbackStatement,);
    }
}
