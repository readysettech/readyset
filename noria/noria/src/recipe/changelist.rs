//! This module provides the mechanism to transform the [`String`] queries issued to Noria
//! into a list of changes that need to happen (namely, [`ChangeList`]).
//!
//! The [`ChangeList`] structure provides a list of [`Change`]s, which shall be used by
//! the [`Recipe`] to apply them. Those [`Change`]s represent the following SQL statements:
//! - `CREATE TABLE`
//! - `CREATE CACHED QUERY`
//! - `CREATE VIEW`
//! - `DROP CACHED QUERY`
//! - `DROP TABLE`
//! Said list of [`Change`]s are sorted in the same order as the queries came in. This guarantees
//! that within the same request we can have queries like these:
//! ```SQL
//! CREATE TABLE table_1 (id INT);
//! DROP TABLE table_1;
//! CREATE TABLE table_1 (id INT);
//! ```
//! Without this guarantee, we could mistakenly attempt to drop a table before creating it
//! (if we processed removals before additions), or avoid creating a table that shouldn't exist (if
//! we processed additions before removals).
// TODO(fran): Couple of things we need to change/rethink:
//  1. Rethink how we want to bubble up errors. Now we are splitting a bunch of queries in the
//   same string into an array of queries, so that we can report which one failed. Doing a
//   direct  parsing using something like `many1(..)` would be more efficient, but if the
//   parsing fails, we'll get the whole input (instead of just the failing query). For
//   example, if we parse: `CREATE TABLE table (id INT); ILLEGAL SQL; SELECT * FROM table`,
//   the error would be "couldn't parse `ILLEGAL SQL; SELECT * FROM table`", vs "couldn't
//   parse `ILLEGAL SQL;`".
//  2. Rethink how we are parsing queries in `parser.rs`:
//     a. Some queries do not parse the `statement_terminator` at the end, but some do.
//     b. The `statement_terminator` matches whitespaces, semicolons, line ending and eof. For
//    simplicity, it should only match semicolons (or semicolons and eof, at most).
use std::convert::TryFrom;
use std::str::FromStr;

use nom_sql::{
    AlterTableStatement, CacheInner, CreateCacheStatement, CreateTableStatement,
    CreateViewStatement, DropTableStatement, SelectStatement, SqlIdentifier, SqlQuery,
};
use noria_errors::{unsupported, ReadySetError, ReadySetResult};
use serde::{Deserialize, Serialize};

/// The specification for a list of changes that must be made
/// to the MIR and dataflow graphs.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct ChangeList {
    /// The list of changes to be made.
    /// The changes are stored in the order they were issued.
    pub changes: Vec<Change>,
}

impl TryFrom<String> for ChangeList {
    type Error = ReadySetError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.as_str().parse()
    }
}

impl FromStr for ChangeList {
    type Err = ReadySetError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        // We separate the queries first, so that we can parse them one by
        // one and get a correct error message when an individual query fails to be
        // parsed.

        macro_rules! mk_error {
            ($str:expr) => {
                Err(ReadySetError::UnparseableQuery {
                    query: $str.to_string(),
                })
            };
        }

        let queries = match parse::separate_queries(value) {
            Result::Err(nom::Err::Error(e)) => {
                return mk_error!(e.input);
            }
            Result::Err(nom::Err::Failure(e)) => {
                return mk_error!(e.input);
            }
            Result::Err(_) => {
                return mk_error!(value);
            }
            Result::Ok((remainder, parsed)) => {
                if !remainder.is_empty() {
                    return mk_error!(remainder);
                }
                parsed
            }
        };
        let changes = queries.into_iter().fold(
            Ok(Vec::new()),
            |acc: ReadySetResult<Vec<Change>>, query| match parse::query_expr(query) {
                Result::Err(nom::Err::Error(e)) => mk_error!(e.input),
                Result::Err(nom::Err::Failure(e)) => mk_error!(e.input),
                Result::Err(_) => mk_error!(value),
                Result::Ok((remainder, parsed)) => {
                    if !remainder.is_empty() {
                        return mk_error!(remainder);
                    }
                    acc.and_then(|mut changes| {
                        match parsed {
                            SqlQuery::CreateTable(cts) => changes.push(Change::CreateTable(cts)),
                            SqlQuery::CreateView(cvs) => changes.push(Change::CreateView(cvs)),
                            SqlQuery::CreateCache(ccs) => changes.push(Change::CreateCache(ccs)),
                            SqlQuery::AlterTable(ats) => changes.push(Change::AlterTable(ats)),
                            SqlQuery::DropTable(dts) => {
                                let if_exists = dts.if_exists;
                                changes.extend(dts.tables.into_iter().map(|t| Change::Drop {
                                    name: t.name,
                                    if_exists,
                                }))
                            }
                            SqlQuery::DropCache(dcs) => changes.push(Change::Drop {
                                name: dcs.name,
                                if_exists: false,
                            }),
                            _ => unsupported!("Query not supported"),
                        }
                        Ok(changes)
                    })
                }
            },
        )?;

        Ok(ChangeList { changes })
    }
}

impl From<CreateTableStatement> for ChangeList {
    fn from(cts: CreateTableStatement) -> Self {
        ChangeList {
            changes: vec![Change::CreateTable(cts)],
        }
    }
}

impl From<DropTableStatement> for ChangeList {
    fn from(dts: DropTableStatement) -> Self {
        ChangeList {
            changes: dts
                .tables
                .iter()
                .map(|t| Change::Drop {
                    name: t.name.clone(),
                    if_exists: dts.if_exists,
                })
                .collect(),
        }
    }
}

impl From<AlterTableStatement> for ChangeList {
    fn from(ats: AlterTableStatement) -> Self {
        ChangeList {
            changes: vec![Change::AlterTable(ats)],
        }
    }
}

/// Describes a singe change to be made to the MIR and dataflow graphs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Change {
    /// Expression that represents a `CREATE TABLE` statement.
    CreateTable(CreateTableStatement),
    /// Expression that represents a `CREATE VIEW` statement.
    CreateView(CreateViewStatement),
    /// Expression that represents a `CREATE CACHE` statement.
    CreateCache(CreateCacheStatement),
    /// Expression that represents an ALTER TABLE statement.
    AlterTable(AlterTableStatement),
    /// The removal of a [`RecipeExpression`].
    Drop {
        /// The [`SqlIdentifier`] of the query/view to remove.
        name: SqlIdentifier,
        /// If `false`, then an error should be thrown if the query/view is not found.
        if_exists: bool,
    },
}

impl Change {
    /// Creates a new [`Change::CreateCache`] from the given `name` and
    /// [`SelectStatement`].
    pub fn create_cache<N>(name: N, statement: SelectStatement) -> Self
    where
        N: Into<SqlIdentifier>,
    {
        Self::CreateCache(CreateCacheStatement {
            name: Some(name.into()),
            inner: CacheInner::Statement(Box::new(statement)),
        })
    }
}

impl IntoIterator for ChangeList {
    type Item = Change;
    type IntoIter = std::vec::IntoIter<Change>;

    fn into_iter(self) -> Self::IntoIter {
        self.changes.into_iter()
    }
}

mod parse {
    use nom::bytes::complete::{tag, take_until};
    use nom::combinator::recognize;
    use nom::error::ErrorKind;
    use nom::multi::many1;
    use nom::sequence::{delimited, terminated};
    use nom_sql::whitespace::whitespace0;
    use nom_sql::{sql_query, Dialect, SqlQuery};

    /// The canonical SQL dialect used for central Noria server recipes. All direct clients of
    /// noria-server must use this dialect for their SQL recipes, and all adapters and client
    /// libraries must translate into this dialect as part of handling requests from users
    const CANONICAL_DIALECT: Dialect = Dialect::MySQL;

    pub(super) fn query_expr(input: &str) -> nom::IResult<&str, SqlQuery> {
        let (input, _) = whitespace0(input)?;
        let (input, expr) = match sql_query(CANONICAL_DIALECT)(input.as_bytes()) {
            Ok((i, e)) => Ok((std::str::from_utf8(i).unwrap(), e)),
            Err(nom::Err::Incomplete(n)) => Err(nom::Err::Incomplete(n)),
            Err(nom::Err::Error(nom::error::Error { input, code })) => {
                Err(nom::Err::Error(nom::error::Error {
                    input: std::str::from_utf8(input).unwrap(),
                    code,
                }))
            }
            Err(nom::Err::Failure(nom::error::Error { input, code })) => {
                Err(nom::Err::Error(nom::error::Error {
                    input: std::str::from_utf8(input).unwrap(),
                    code,
                }))
            }
        }?;
        Ok((input, expr))
    }

    pub(super) fn separate_queries(queries: &str) -> nom::IResult<&str, Vec<&str>> {
        many1(delimited(
            whitespace0,
            // We only accept SQL queries that end with a semicolon.
            ends_in_semicolon_or_eof,
            whitespace0,
        ))(queries)
    }

    fn ends_in_semicolon_or_eof(input: &str) -> nom::IResult<&str, &str> {
        match recognize(terminated(take_until(";"), tag(";")))(input) {
            Ok(res) => Ok(res),
            Err(nom::Err::Error(nom::error::Error {
                code: ErrorKind::TakeUntil,
                ..
            })) if !input.is_empty() => {
                // We didn't find a semicolon, so the rest of the input is a query.
                Ok(("", input))
            }
            Err(e) => Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_handles_multiple_statements_per_line() {
        let queries =
            "  CREATE CACHE q_0 FROM SELECT a FROM b;     CREATE CACHE q_1 FROM SELECT x FROM y;   ";

        let changelist: ChangeList = queries.parse().unwrap();
        assert_eq!(
            changelist
                .changes
                .iter()
                .filter(|c| matches!(c, Change::CreateCache(_)))
                .count(),
            2
        );
        assert_eq!(
            changelist
                .changes
                .iter()
                .filter(|c| matches!(c, Change::Drop { .. }))
                .count(),
            0
        );
    }

    #[test]
    fn it_handles_spaces() {
        let queries = "  CREATE CACHE q_0 FROM SELECT a FROM b;\
                      CREATE CACHE q_1 FROM SELECT x FROM y;";

        let changelist: ChangeList = queries.parse().unwrap();
        assert_eq!(
            changelist
                .changes
                .iter()
                .filter(|c| matches!(c, Change::CreateCache(_)))
                .count(),
            2
        );
        assert_eq!(
            changelist
                .changes
                .iter()
                .filter(|c| matches!(c, Change::Drop { .. }))
                .count(),
            0
        );
    }

    #[test]
    fn it_handles_missing_semicolon() {
        let queries = "CREATE CACHE q_0 FROM SELECT a FROM b;\nCREATE VIEW q_1 AS SELECT x FROM y";

        let changelist = ChangeList::from_str(queries).unwrap();
        assert_eq!(
            changelist
                .changes
                .iter()
                .filter(|c| matches!(c, Change::CreateCache(_)))
                .count(),
            1
        );
        assert_eq!(
            changelist
                .changes
                .iter()
                .filter(|c| matches!(c, Change::CreateView(_)))
                .count(),
            1
        );
    }
}
