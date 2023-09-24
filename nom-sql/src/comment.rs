use std::{fmt, str};

use nom::branch::alt;
use nom::bytes::complete::{tag, tag_no_case};
use nom::combinator::map_res;
use nom::sequence::{delimited, terminated};
use nom_locate::LocatedSpan;
use readyset_util::fmt::fmt_with;
use serde::{Deserialize, Serialize};
use test_strategy::Arbitrary;

use crate::common::statement_terminator;
use crate::whitespace::{whitespace0, whitespace1};
use crate::{literal, Dialect, DialectDisplay, NomSqlResult, SqlIdentifier};

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub enum CommentStatement {
    Column {
        column_name: SqlIdentifier,
        table_name: SqlIdentifier,
        comment: String,
    },
    Table {
        table_name: SqlIdentifier,
        comment: String,
    },
}

impl DialectDisplay for CommentStatement {
    fn display(&self, dialect: Dialect) -> impl fmt::Display + Copy + '_ {
        fmt_with(move |f| match self {
            Self::Column {
                column_name,
                table_name,
                comment,
            } => {
                write!(
                    f,
                    "COMMENT ON COLUMN {}.{} IS ",
                    dialect.quote_identifier(&table_name),
                    dialect.quote_identifier(&column_name),
                )?;
                literal::display_string_literal(f, comment)
            }
            Self::Table {
                table_name,
                comment,
            } => {
                write!(
                    f,
                    "COMMENT ON TABLE {} IS ",
                    dialect.quote_identifier(&table_name),
                )?;
                literal::display_string_literal(f, comment)
            }
        })
    }
}

pub fn comment(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], CommentStatement> {
    move |i| alt((table_comment(dialect), column_comment(dialect)))(i)
}

fn table_comment(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], CommentStatement> {
    move |i| {
        let (i, _) = tag_no_case("comment")(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, _) = tag_no_case("on")(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, _) = tag_no_case("table")(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, table_name) = dialect.identifier()(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, _) = tag_no_case("is")(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, comment) = map_res(dialect.string_literal(), String::from_utf8)(i)?;
        let (i, _) = statement_terminator(i)?;

        Ok((
            i,
            CommentStatement::Table {
                table_name,
                comment,
            },
        ))
    }
}

fn column_comment(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], CommentStatement> {
    move |i| {
        let (i, _) = tag_no_case("comment")(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, _) = tag_no_case("on")(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, _) = tag_no_case("column")(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, table_name) = terminated(
            dialect.identifier(),
            delimited(whitespace0, tag("."), whitespace0),
        )(i)?;
        let (i, column_name) = dialect.identifier()(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, _) = tag_no_case("is")(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, comment) = map_res(dialect.string_literal(), String::from_utf8)(i)?;
        let (i, _) = statement_terminator(i)?;

        Ok((
            i,
            CommentStatement::Column {
                table_name,
                column_name,
                comment,
            },
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_comment() {
        let res = test_parse!(
            comment(Dialect::MySQL),
            b"COMMENT ON TABLE test IS 'this is a comment'"
        );

        assert_eq!(
            res,
            CommentStatement::Table {
                table_name: "test".into(),
                comment: "this is a comment".into(),
            }
        )
    }

    #[test]
    fn column_comment() {
        let res = test_parse!(
            comment(Dialect::MySQL),
            b"COMMENT ON COLUMN test_table.test_column IS 'this is a comment'"
        );

        assert_eq!(
            res,
            CommentStatement::Column {
                table_name: "test_table".into(),
                column_name: "test_column".into(),
                comment: "this is a comment".into(),
            }
        )
    }

    #[test]
    fn display_column_comment() {
        let comment = CommentStatement::Column {
            table_name: "test_table".into(),
            column_name: "test_column".into(),
            comment: "this is a comment".into(),
        };

        assert_eq!(
            comment.display(Dialect::PostgreSQL).to_string(),
            "COMMENT ON COLUMN \"test_table\".\"test_column\" IS 'this is a comment'"
        );
    }

    #[test]
    fn display_table_comment() {
        let comment = CommentStatement::Table {
            table_name: "test_table".into(),
            comment: "this is a comment".into(),
        };

        assert_eq!(
            comment.display(Dialect::PostgreSQL).to_string(),
            "COMMENT ON TABLE \"test_table\" IS 'this is a comment'"
        );
    }
}
