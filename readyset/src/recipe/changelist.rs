//! This module provides the mechanism to transform the [`String`] queries issued to ReadySet
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

use dataflow_expression::Dialect;
use nom_locate::LocatedSpan;
use nom_sql::{
    AlterTableStatement, CacheInner, CreateCacheStatement, CreateTableStatement,
    CreateViewStatement, DropTableStatement, DropViewStatement, Relation, SelectStatement,
    SqlIdentifier, SqlQuery,
};
use readyset_data::DfType;
use readyset_errors::{unsupported, ReadySetError, ReadySetResult};
use serde::{Deserialize, Serialize};

/// The specification for a list of changes that must be made
/// to the MIR and dataflow graphs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangeList {
    /// The list of changes to be made.
    ///
    /// The changes are stored in the order they were issued.
    pub changes: Vec<Change>,

    /// The schema search path to use to resolve table references within the changelist
    pub schema_search_path: Vec<SqlIdentifier>,

    /// The SQL dialect to use for all types and expressions in queries added by this ChangeList
    pub dialect: Dialect,
}

/// Types that can be converted directly into a list of [`Change`]s. Used to type-overload
/// [`ChangeList::from_changes`]
pub trait IntoChanges {
    /// Convert this value into a list of [`Change`]s
    fn into_changes(self) -> Vec<Change>;
}

impl IntoChanges for CreateTableStatement {
    fn into_changes(self) -> Vec<Change> {
        vec![Change::CreateTable(self)]
    }
}

impl IntoChanges for DropTableStatement {
    fn into_changes(self) -> Vec<Change> {
        self.tables
            .into_iter()
            .map(|name| Change::Drop {
                name,
                if_exists: self.if_exists,
            })
            .collect()
    }
}

impl IntoChanges for DropViewStatement {
    fn into_changes(self) -> Vec<Change> {
        self.views
            .into_iter()
            .map(|name| Change::Drop {
                name,
                if_exists: self.if_exists,
            })
            .collect()
    }
}

impl IntoChanges for AlterTableStatement {
    fn into_changes(self) -> Vec<Change> {
        vec![Change::AlterTable(self)]
    }
}

impl IntoChanges for Vec<Change> {
    fn into_changes(self) -> Vec<Change> {
        self
    }
}

impl IntoIterator for ChangeList {
    type Item = Change;
    type IntoIter = std::vec::IntoIter<Change>;

    fn into_iter(self) -> Self::IntoIter {
        self.changes.into_iter()
    }
}

impl ChangeList {
    /// Construct a new empty `ChangeList` with the given [`Dialect`]
    pub fn new(dialect: Dialect) -> Self {
        Self {
            changes: vec![],
            schema_search_path: vec![],
            dialect,
        }
    }

    /// Parse a `ChangeList` from the given SQL string, formatted using the canonical SQL dialect,
    /// but using the given [`Dialect`] for expression evaluation semantics.
    pub fn from_str<S>(s: S, dialect: Dialect) -> ReadySetResult<Self>
    where
        S: AsRef<str>,
    {
        let value = s.as_ref();

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

        // TODO(alex) Include nom-sql error position info in ReadySetError::UnparseableQuery?
        let queries = match parse::separate_queries(LocatedSpan::new(value.as_bytes())) {
            Result::Err(nom::Err::Error(e)) => {
                return mk_error!(std::str::from_utf8(&e.input).unwrap());
            }
            Result::Err(nom::Err::Failure(e)) => {
                return mk_error!(std::str::from_utf8(&e.input).unwrap());
            }
            Result::Err(_) => {
                return mk_error!(value);
            }
            Result::Ok((remainder, parsed)) => {
                if !remainder.is_empty() {
                    return mk_error!(std::str::from_utf8(&remainder).unwrap());
                }
                parsed
            }
        };
        let changes = queries.into_iter().fold(
            Ok(Vec::new()),
            |acc: ReadySetResult<Vec<Change>>, query| match parse::query_expr(query) {
                Result::Err(nom::Err::Error(e)) => {
                    mk_error!(std::str::from_utf8(&e.input).unwrap())
                }
                Result::Err(nom::Err::Failure(e)) => {
                    mk_error!(std::str::from_utf8(&e.input).unwrap())
                }
                Result::Err(_) => mk_error!(value),
                Result::Ok((remainder, parsed)) => {
                    if !remainder.is_empty() {
                        return mk_error!(std::str::from_utf8(&remainder).unwrap());
                    }
                    acc.and_then(|mut changes| {
                        match parsed {
                            SqlQuery::CreateTable(cts) => changes.push(Change::CreateTable(cts)),
                            SqlQuery::CreateView(cvs) => changes.push(Change::CreateView(cvs)),
                            SqlQuery::CreateCache(ccs) => changes.push(Change::CreateCache(ccs)),
                            SqlQuery::AlterTable(ats) => changes.push(Change::AlterTable(ats)),
                            SqlQuery::DropTable(dts) => {
                                let if_exists = dts.if_exists;
                                changes.extend(
                                    dts.tables
                                        .into_iter()
                                        .map(|name| Change::Drop { name, if_exists }),
                                )
                            }
                            SqlQuery::DropView(dvs) => {
                                changes.extend(dvs.views.into_iter().map(|name| Change::Drop {
                                    name,
                                    if_exists: dvs.if_exists,
                                }))
                            }
                            SqlQuery::DropCache(dcs) => changes.push(Change::Drop {
                                name: dcs.name,
                                if_exists: false,
                            }),
                            _ => unsupported!(
                                "Only DDL statements supported in ChangeList (got {})",
                                parsed.query_type()
                            ),
                        }
                        Ok(changes)
                    })
                }
            },
        )?;

        Ok(ChangeList {
            changes,
            schema_search_path: vec![],
            dialect,
        })
    }

    /// Construct a new `ChangeList` from the given single change and [`Dialect`]
    pub fn from_change(change: Change, dialect: Dialect) -> Self {
        Self::from_changes(vec![change], dialect)
    }

    /// Construct a new `ChangeList` with the given list of changes and [`Dialect`]
    pub fn from_changes<C>(changes: C, dialect: Dialect) -> Self
    where
        C: IntoChanges,
    {
        Self {
            changes: changes.into_changes(),
            schema_search_path: vec![],
            dialect,
        }
    }

    /// Return a reference to the configured schema search path for this `ChangeList`.
    pub fn schema_search_path(&self) -> &[SqlIdentifier] {
        &self.schema_search_path
    }

    /// Construct a new `ChangeList` from `self`, but with the given schema search path
    pub fn with_schema_search_path(self, schema_search_path: Vec<SqlIdentifier>) -> Self {
        Self {
            schema_search_path,
            ..self
        }
    }

    /// Return a mutable reference to the changes in this `ChangeList`
    pub fn changes_mut(&mut self) -> &mut Vec<Change> {
        &mut self.changes
    }

    /// Construct an iterator over references to the changes in this `ChangeList`
    pub fn changes(&self) -> impl Iterator<Item = &Change> + '_ {
        self.changes.iter()
    }
}

/// Describes a single change to make to a custom type
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AlterTypeChange {
    /// Set the variants of this custom type to the given list of variants.
    ///
    /// Currently, if the change does not involve exclusively adding variants to the end of the
    /// existing list of variants, an error will be returned
    SetVariants(Vec<String>),
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
    /// Add a new custom type
    ///
    /// Internally, custom types are just represented as aliases for a [`DfType`].
    CreateType {
        /// The name of the type
        name: Relation,
        /// The definition of the type itself.
        ///
        /// Structurally, this can be any type within ReadySet's internal type system, but in
        /// practice this is currently just enum types (and in the future will be expanded to also
        /// include composite types and range types)
        ty: DfType,
    },
    /// Alter an existing custom type
    AlterType {
        /// The name of the type to change
        name: Relation,
        /// A specification for the change to make to the type
        change: AlterTypeChange,
    },
    /// The removal of a [`RecipeExpr`].
    Drop {
        /// The name of the relation to remove.
        name: Relation,
        /// If `false`, then an error should be thrown if the relation is not found.
        if_exists: bool,
    },
}

impl Change {
    /// Creates a new [`Change::CreateCache`] from the given `name` and
    /// [`SelectStatement`].
    pub fn create_cache<N>(name: N, statement: SelectStatement, always: bool) -> Self
    where
        N: Into<Relation>,
    {
        Self::CreateCache(CreateCacheStatement {
            name: Some(name.into()),
            inner: CacheInner::Statement(Box::new(statement)),
            always,
        })
    }

    /// Return true if this change requires noria to resnapshot the database in order to properly
    /// update the schema
    pub fn requires_resnapshot(&self) -> bool {
        let alter_table = match self {
            Change::AlterTable(a) => a,
            _ => return false,
        };

        // NOTE: This exhaustive list is here so that we remember to think about the behavior of
        // any additional alter table additions we add support for. We may not need to resnapshot
        // for them. As such, this list should not be removed.
        alter_table.definitions.iter().any(|def| match def {
            nom_sql::AlterTableDefinition::AddColumn(_)
            | nom_sql::AlterTableDefinition::AlterColumn { .. }
            | nom_sql::AlterTableDefinition::DropColumn { .. }
            | nom_sql::AlterTableDefinition::ChangeColumn { .. }
            | nom_sql::AlterTableDefinition::RenameColumn { .. }
            | nom_sql::AlterTableDefinition::AddKey(_)
            | nom_sql::AlterTableDefinition::DropConstraint { .. } => true,
        })
    }
}

mod parse {
    use nom::bytes::complete::{tag, take_until};
    use nom::combinator::recognize;
    use nom::error::ErrorKind;
    use nom::multi::many1;
    use nom::sequence::{delimited, terminated};
    use nom::InputTake;
    use nom_locate::LocatedSpan;
    use nom_sql::whitespace::whitespace0;
    use nom_sql::{sql_query, Dialect, NomSqlError, NomSqlResult, SqlQuery};

    /// The canonical SQL dialect used for central ReadySet server recipes. All direct clients of
    /// readyset-server must use this dialect for their SQL recipes, and all adapters and client
    /// libraries must translate into this dialect as part of handling requests from users
    const CANONICAL_DIALECT: Dialect = Dialect::MySQL;

    pub(super) fn query_expr(input: LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], SqlQuery> {
        let (input, _) = whitespace0(input)?;
        sql_query(CANONICAL_DIALECT)(input)
    }

    pub(super) fn separate_queries(
        queries: LocatedSpan<&[u8]>,
    ) -> NomSqlResult<&[u8], Vec<LocatedSpan<&[u8]>>> {
        many1(delimited(
            whitespace0,
            // We only accept SQL queries that end with a semicolon.
            ends_in_semicolon_or_eof,
            whitespace0,
        ))(queries)
    }

    fn ends_in_semicolon_or_eof(
        input: LocatedSpan<&[u8]>,
    ) -> NomSqlResult<&[u8], LocatedSpan<&[u8]>> {
        match recognize(terminated(take_until(";"), tag(";")))(input) {
            Ok((input, output)) => Ok((input, output)),
            Err(nom::Err::Error(NomSqlError {
                kind: ErrorKind::TakeUntil,
                ..
            })) if !input.is_empty() => {
                // We didn't find a semicolon, so the rest of the input is a query.
                let (output, input) = input.take_split(0);
                Ok((input, output))
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

        let changelist = ChangeList::from_str(queries, Dialect::DEFAULT_MYSQL).unwrap();
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

        let changelist = ChangeList::from_str(queries, Dialect::DEFAULT_MYSQL).unwrap();
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

        let changelist = ChangeList::from_str(queries, Dialect::DEFAULT_MYSQL).unwrap();
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
