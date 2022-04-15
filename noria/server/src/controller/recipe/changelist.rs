//! This module provides the mechanism to transform the [`String`] queries issued to Noria
//! into a list of changes that need to happen (namely, [`ChangeList`]).
//!
//! The [`ChangeList`] structure provides a list of [`Change`]s (which are SQL queries that must
//! either be added or removed), which shall be used by the [`Recipe`] to apply them.
//! Said list of [`Change`]s are sorted in the same order as the queries came in. This guarantees
//! that within the same request we can have queries like these:
//! ```SQL
//! CREATE TABLE table_1 (id INT);
//! DROP TABLE table_1;
//! CREATE TABLE table_1 (id INT);
//! ```
//! Without this guarantee, we could mistakenly attempt to drop a table before creating it
//! (if we processed removals before additions), or avoid creating a table that should exist (if we
//! processed additions before removals).
use std::fmt;
use std::fmt::Display;
use std::str::FromStr;

use nom_sql::{SqlIdentifier, SqlQuery};
use noria::ParsedRecipeSpec;
use noria_errors::{ReadySetError, ReadySetResult};
use serde::{Deserialize, Serialize};

/// The specification for a list of changes that must be made
/// to the MIR and dataflow graphs.
#[derive(Debug)]
pub struct ChangeList {
    /// The list of changes to be made.
    /// The changes are stored in the order they were issued.
    pub changes: Vec<Change>,
}

/// Describes a singe change to be made to the MIR and dataflow graphs.
// TODO(fran): I believe in the future we can move away from having
//  a [`SqlQuery`] and we can have something more informative and easier
//  to handle (and to understand).
#[derive(Debug)]
pub enum Change {
    /// An [`SqlExpression`] that describes an addition.
    Add(SqlExpression),
    /// An [`SqlExpression`] that describes a removal.
    Remove(SqlExpression),
}

/// A SQL expression, which holds the necessary information about a given
/// SQL query.
#[derive(PartialEq, Eq, Clone, Serialize, Deserialize, Debug)]
pub struct SqlExpression {
    /// Query name, if it has one.
    pub name: Option<SqlIdentifier>,
    /// The SQL query.
    pub query: SqlQuery,
    /// Whether or not the query has a reader.
    // TODO(fran): Is this field really necessary? Can't we get this information
    //  from the graph itself?
    pub is_leaf: bool,
}

impl IntoIterator for ChangeList {
    type Item = Change;
    type IntoIter = std::vec::IntoIter<Change>;

    fn into_iter(self) -> Self::IntoIter {
        self.changes.into_iter()
    }
}

impl Extend<Change> for ChangeList {
    fn extend<T: IntoIterator<Item = Change>>(&mut self, iter: T) {
        self.changes.extend(iter);
    }
}

impl Change {
    /// Retrieves the reference to the associated [`SqlExpression`].
    pub fn expression(&self) -> &SqlExpression {
        match self {
            Change::Add(expr) => expr,
            Change::Remove(expr) => expr,
        }
    }
}

impl Display for ChangeList {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for change in self.changes.iter().map(|c| c.expression()) {
            if change.is_leaf {
                write!(f, "query")?;
            }
            if let Some(ref name) = change.name {
                if change.is_leaf {
                    write!(f, " ")?;
                }
                write!(f, "{}", name)?;
            }
            if change.is_leaf || change.name.is_some() {
                write!(f, ": ")?;
            }
            writeln!(f, "{};", change.query)?;
        }

        Ok(())
    }
}

mod parse {
    //! A separate module to hold all the parsing logic to turn a [`String`]
    //! into a [`ChangeList`].
    use nom_sql::{parser as sql_parser, Dialect, SqlQuery};

    /// The canonical SQL dialect used for central Noria server recipes. All direct clients of
    /// noria-server must use this dialect for their SQL recipes, and all adapters and client
    /// libraries must translate into this dialect as part of handling requests from users
    pub const CANONICAL_DIALECT: Dialect = Dialect::MySQL;

    #[inline]
    fn ident(input: &str) -> nom::IResult<&str, &str> {
        use nom::InputTakeAtPosition;
        input.split_at_position_complete(|chr| {
            !(chr.is_ascii() && (nom::character::is_alphanumeric(chr as u8) || chr == '_'))
        })
    }

    fn query_prefix(input: &str) -> nom::IResult<&str, (bool, Option<&str>)> {
        use nom::branch::alt;
        use nom::bytes::complete::tag_no_case;
        use nom::character::complete::{char, space1};
        use nom::combinator::opt;
        use nom::sequence::{pair, terminated};
        use nom_sql::whitespace::whitespace0;
        let (input, public) = opt(pair(
            alt((tag_no_case("query"), tag_no_case("view"))),
            space1,
        ))(input)?;
        let (input, _) = whitespace0(input)?;
        let (input, name) = opt(terminated(ident, whitespace0))(input)?;
        let (input, _) = whitespace0(input)?;
        let (input, _) = char(':')(input)?;
        let (input, _) = whitespace0(input)?;
        Ok((input, (public.is_some(), name)))
    }

    fn query_expr(input: &str) -> nom::IResult<&str, (bool, Option<&str>, SqlQuery)> {
        use nom::combinator::opt;
        use nom_sql::whitespace::whitespace0;
        let (input, prefix) = opt(query_prefix)(input)?;
        // NOTE: some massaging since nom_sql operates on &[u8], not &str
        let (input, expr) = match sql_parser::sql_query(CANONICAL_DIALECT)(input.as_bytes()) {
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
        let (input, _) = whitespace0(input)?;
        Ok((
            input,
            match prefix {
                None => (false, None, expr),
                Some((public, name)) => (public, name, expr),
            },
        ))
    }

    pub(in crate::controller) fn query_exprs(
        input: &str,
    ) -> nom::IResult<&str, Vec<(bool, Option<&str>, SqlQuery)>> {
        nom::multi::many1(query_expr)(input)
    }

    pub(in crate::controller) fn clean_queries(queries: &str) -> Vec<String> {
        queries
            .lines()
            .map(str::trim)
            .filter(|l| !l.is_empty() && !l.starts_with('#') && !l.starts_with("--"))
            .map(|l| {
                // remove inline comments, too
                match l.find('#') {
                    None => l.trim(),
                    Some(pos) => l[0..pos - 1].trim(),
                }
            })
            .fold(Vec::<String>::new(), |mut acc, l| {
                match acc.last_mut() {
                    Some(s) if !s.ends_with(';') => {
                        s.push(' ');
                        s.push_str(l);
                    }
                    _ => acc.push(l.to_string()),
                }
                acc
            })
    }
}

impl FromStr for ChangeList {
    type Err = ReadySetError;

    fn from_str(raw_queries: &str) -> Result<Self, Self::Err> {
        let query_strings = parse::clean_queries(raw_queries);

        let parsed_queries = query_strings.iter().fold(
            Vec::new(),
            |mut acc: Vec<ReadySetResult<(bool, Option<&str>, SqlQuery)>>, query| {
                match parse::query_exprs(query) {
                    Result::Err(_) => {
                        // we got a parse error
                        acc.push(Err(ReadySetError::UnparseableQuery {
                            query: query.clone(),
                        }));
                    }
                    Result::Ok((remainder, parsed)) => {
                        // should have consumed all input
                        if !remainder.is_empty() {
                            acc.push(Err(ReadySetError::UnparseableQuery {
                                query: query.clone(),
                            }));
                            return acc;
                        }
                        acc.extend(parsed.into_iter().map(|p| Ok(p)).collect::<Vec<_>>());
                    }
                }
                acc
            },
        );
        let changes = parsed_queries
            .into_iter()
            .map(|pr| {
                let (is_leaf, r, q) = pr?;

                let (r, is_leaf) = match (&r, &q) {
                    // Treat views created using CREATE VIEW as leaf views too.
                    (None, SqlQuery::CreateView(query)) => (Some(query.name.clone()), true),
                    // Tables are aliased by their table name.
                    (None, SqlQuery::CreateTable(query)) => {
                        (Some(query.table.name.clone()), is_leaf)
                    }
                    (_, _) => (r.map(|s| s.into()), is_leaf),
                };

                Ok(SqlExpression {
                    name: r,
                    query: q,
                    is_leaf,
                })
            })
            .fold(
                Ok(Vec::new()),
                |acc: ReadySetResult<Vec<Change>>, change| {
                    change.and_then(|c| {
                        acc.map(|mut changes| {
                            match &c.query {
                                SqlQuery::CreateTable(_)
                                | SqlQuery::CreateView(_)
                                | SqlQuery::Select(_)
                                | SqlQuery::CompoundSelect(_) => changes.push(Change::Add(c)),
                                SqlQuery::DropTable(_) | SqlQuery::DropCache(_) => {
                                    changes.push(Change::Remove(c));
                                }
                                // TODO(fran): I've added this as a separate clause, since I'm not
                                // sure if we should  be ignoring
                                // other SqlTypes, or even creating a more distinctions (like
                                // updates, maybe?)
                                _ => changes.push(Change::Add(c)),
                            }
                            changes
                        })
                    })
                },
            )?;

        Ok(ChangeList { changes })
    }
}

impl From<ParsedRecipeSpec> for ChangeList {
    fn from(r: ParsedRecipeSpec) -> Self {
        // Create a leaf for Select, CompoundSelect and CreateView statements. CreateCache
        // statements should already be converted to Select statements at this point.
        let is_leaf = matches!(
            r.query,
            SqlQuery::Select(_) | SqlQuery::CompoundSelect(_) | SqlQuery::CreateView(_)
        );
        Self {
            changes: vec![Change::Add(SqlExpression {
                name: r.name.map(|n| n.into()),
                query: r.query,
                is_leaf,
            })],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_handles_multiple_statements_per_line() {
        let queries = "  QUERY q_0: SELECT a FROM b; QUERY q_1: SELECT x FROM y;";

        let changelist = ChangeList::from_str(queries).unwrap();
        assert_eq!(
            changelist
                .changes
                .iter()
                .filter(|c| matches!(c, Change::Add(_)))
                .count(),
            2
        );
        assert_eq!(
            changelist
                .changes
                .iter()
                .filter(|c| matches!(c, Change::Remove(_)))
                .count(),
            0
        );
    }

    #[test]
    fn it_handles_spaces() {
        let queries = "  QUERY q_0: SELECT a FROM b;\
                      QUERY q_1: SELECT x FROM y;";

        let changelist = ChangeList::from_str(queries).unwrap();
        assert_eq!(
            changelist
                .changes
                .iter()
                .filter(|c| matches!(c, Change::Add(_)))
                .count(),
            2
        );
        assert_eq!(
            changelist
                .changes
                .iter()
                .filter(|c| matches!(c, Change::Remove(_)))
                .count(),
            0
        );
    }

    #[test]
    fn it_handles_missing_semicolon() {
        let queries = "QUERY q_0: SELECT a FROM b;\nVIEW q_1: SELECT x FROM y";

        let changelist = ChangeList::from_str(queries).unwrap();
        assert_eq!(
            changelist
                .changes
                .iter()
                .filter(|c| matches!(c, Change::Add(_)))
                .count(),
            2
        );
        assert_eq!(
            changelist
                .changes
                .iter()
                .filter(|c| matches!(c, Change::Remove(_)))
                .count(),
            0
        );
    }

    #[test]
    fn display_parses() {
        let changelist =
            ChangeList::from_str("CREATE TABLE b (a INT);\nQUERY q_0: SELECT a FROM b;").unwrap();
        let changelist_str = changelist.to_string();
        let res = ChangeList::from_str(&changelist_str).unwrap();
        assert_eq!(
            res.changes
                .iter()
                .filter(|c| matches!(c, Change::Add(_)))
                .count(),
            2
        );
        assert_eq!(
            res.changes
                .iter()
                .filter(|c| matches!(c, Change::Remove(_)))
                .count(),
            0
        );
    }
}
