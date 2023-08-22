//! Data types for implementing snapshot and streaming replication from an upstream database.

pub mod mysql;
pub mod postgres;
pub mod vitess;

use std::borrow::Borrow;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt;
use std::hash::Hash;

use mysql::MySqlPosition;
use nom_sql::Relation;
use postgres::PostgresPosition;
use readyset_errors::{internal_err, ReadySetError, ReadySetResult};
use serde::{Deserialize, Serialize};
use vitess::VStreamPosition;

/// Enum representing whether a base table node was already initialized (and has a replication
/// offset assigned), or if it is still pending initialization.
#[derive(Serialize, Deserialize)]
pub enum ReplicationOffsetState {
    Initialized(Option<ReplicationOffset>),
    Pending,
}

/// A data type representing an offset in a replication log
///
/// Replication offsets are represented by a single global [offset](ReplicationOffset::offset),
/// scoped within a single log, identified by a
/// [`replication_log_name`](ReplicationOffset::replication_log_name). Within a single log, offsets
/// are totally ordered, but outside the scope of a log ordering is not well-defined.
///
/// See [the documentation for PersistentState](::readyset_dataflow::state::persistent_state) for
/// more information about how replication offsets are used and persisted
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum ReplicationOffset {
    MySql(MySqlPosition),
    Postgres(PostgresPosition),
    Vitess(VStreamPosition),
}

impl TryFrom<ReplicationOffset> for MySqlPosition {
    type Error = ReadySetError;

    fn try_from(offset: ReplicationOffset) -> Result<Self, Self::Error> {
        if let ReplicationOffset::MySql(offset) = offset {
            Ok(offset)
        } else {
            Err(internal_err!(
                "cannot extract MySqlPosition from Postgres ReplicationOffset"
            ))
        }
    }
}

impl TryFrom<&ReplicationOffset> for MySqlPosition {
    type Error = ReadySetError;

    fn try_from(offset: &ReplicationOffset) -> Result<Self, Self::Error> {
        offset.clone().try_into()
    }
}

impl TryFrom<ReplicationOffset> for PostgresPosition {
    type Error = ReadySetError;

    fn try_from(offset: ReplicationOffset) -> Result<Self, Self::Error> {
        if let ReplicationOffset::Postgres(offset) = offset {
            Ok(offset)
        } else {
            Err(internal_err!(
                "cannot extract PostgresPosition from MySQL ReplicationOffset"
            ))
        }
    }
}

impl TryFrom<&ReplicationOffset> for PostgresPosition {
    type Error = ReadySetError;

    fn try_from(offset: &ReplicationOffset) -> Result<Self, Self::Error> {
        offset.clone().try_into()
    }
}

impl fmt::Display for ReplicationOffset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MySql(pos) => write!(f, "{pos}"),
            Self::Postgres(pos) => write!(f, "{pos}"),
            Self::Vitess(pos) => write!(f, "{pos}"),
        }
    }
}

impl PartialOrd for ReplicationOffset {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Self::MySql(pos), Self::MySql(other_pos)) => pos.partial_cmp(other_pos),
            (Self::Postgres(pos), Self::Postgres(other_pos)) => pos.partial_cmp(other_pos),
            _ => None,
        }
    }
}

impl ReplicationOffset {
    /// Try to mutate `other` to take the maximum of its offset and the offset of
    /// `self`. If `other` is `None`, will assign it to `Some(self.clone)`.
    ///
    /// If the offsets are from different replication logs, returns an error with
    /// [`ReadySetError::ReplicationOffsetLogDifferent`]
    pub fn try_max_into(&self, other: &mut Option<ReplicationOffset>) -> ReadySetResult<()> {
        if matches!(other, Some(other) if self.try_partial_cmp(other)?.is_gt()) || other.is_none() {
            *other = Some(self.clone());
        }

        Ok(())
    }

    /// This method compares `self` and `other`, returning an [`Ordering`] if the two replication
    /// offsets are comparable and an error otherwise. Whether two replication offsets are
    /// comparable is an implementation detail of the representation of the log position for the
    /// underlying database backend.
    pub fn try_partial_cmp(&self, other: &Self) -> ReadySetResult<Ordering> {
        match (self, other) {
            (Self::MySql(offset), Self::MySql(other_offset)) => {
                offset.try_partial_cmp(other_offset)
            }
            (Self::Postgres(offset), Self::Postgres(other_offset)) => Ok(offset.cmp(other_offset)),
            _ => Err(internal_err!(
                "Cannot compare replication offsets from different database backends"
            )),
        }
    }

    /// Returns the minimum of the two replication offsets if the values are comparable; otherwise,
    /// returns an error. The first argument is returned if the values are equal.
    pub fn try_min<'a>(offset1: &'a Self, offset2: &'a Self) -> ReadySetResult<&'a Self> {
        match offset1.try_partial_cmp(offset2)? {
            Ordering::Less | Ordering::Equal => Ok(offset1),
            Ordering::Greater => Ok(offset2),
        }
    }
}

/// Set of replication offsets for the entire system
#[derive(Default, Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct ReplicationOffsets {
    /// Replication offset for the database schema, set as part of the controller metadata stored
    /// in the authority
    pub schema: Option<ReplicationOffset>,

    /// Replication offset for each individual table, if any.
    ///
    /// A table with [`None`] as its replication offset has not yet been snapshotted successfully
    pub tables: HashMap<Relation, Option<ReplicationOffset>>,
}

impl ReplicationOffsets {
    /// Create a new [`ReplicationOffset`] with the given [`schema`][Self::schema] offset and an
    /// empty map of tables
    pub fn with_schema_offset(schema: Option<ReplicationOffset>) -> Self {
        Self {
            schema,
            tables: HashMap::new(),
        }
    }

    /// Returns `true` if self has an offset for the schema
    pub fn has_schema(&self) -> bool {
        self.schema.is_some()
    }

    /// Returns `true` if self has an offset for the table with the given name
    ///
    /// # Examples
    ///
    /// A completely missing table returns `false`:
    ///
    /// ```rust
    /// use replication_offset::ReplicationOffsets;
    ///
    /// let mut replication_offsets = ReplicationOffsets::default();
    /// assert!(!replication_offsets.has_table(&"table_1".into()));
    /// ```
    ///
    /// A table that is present but set to [`None`] also returns `false`:
    ///
    /// ```rust
    /// use replication_offset::ReplicationOffsets;
    ///
    /// let mut replication_offsets = ReplicationOffsets::default();
    /// replication_offsets.tables.insert("table_1".into(), None);
    /// assert!(!replication_offsets.has_table(&"table_1".into()));
    /// ```
    ///
    /// A table that is present returns `true`:
    ///
    /// ```rust
    /// use replication_offset::mysql::MySqlPosition;
    /// use replication_offset::{ReplicationOffset, ReplicationOffsets};
    ///
    /// let mut replication_offsets = ReplicationOffsets::default();
    /// replication_offsets.tables.insert(
    ///     "table_1".into(),
    ///     Some(
    ///         MySqlPosition::from_file_name_and_position("binlog.00001".into(), 1)
    ///             .unwrap()
    ///             .into(),
    ///     ),
    /// );
    /// assert!(replication_offsets.has_table(&"table_1".into()));
    /// ```
    pub fn has_table<T>(&self, table: &T) -> bool
    where
        T: ?Sized,
        Relation: Borrow<T>,
        T: Hash + Eq,
    {
        self.tables.get(table).iter().any(|o| o.is_some())
    }

    /// If all replication offsets are present (the schema and all tables), returns the minimum of
    /// all replication offsets, from which streaming replication can successfully continue.
    /// Otherwise, returns `Ok(None)`.
    ///
    /// If all replication offsets are present but any have mismatched [`replication_log_name`]s,
    /// returns an error.
    ///
    /// [`replication_log_name`]: ReplicationOffset::replication_log_name
    pub fn max_offset(&self) -> ReadySetResult<Option<&ReplicationOffset>> {
        let mut res = match &self.schema {
            Some(schema_offset) => schema_offset,
            None => return Ok(None),
        };
        for offset in self.tables.values() {
            let offset = match offset {
                Some(offset) => offset,
                None => return Ok(None),
            };

            if offset.try_partial_cmp(res)?.is_gt() {
                res = offset;
            }
        }
        Ok(Some(res))
    }

    /// Returns the minimum offset *from those present* within the set of replication offsets.
    ///
    /// If no offset is present _at all_, returns [`None`] (but note that unlike [`max_offset`][]
    /// this function does *not* return [`None`] if an offset is absent).
    ///
    /// If any offsets have a different [`replication_log_name`], returns an error.
    ///
    /// [`max_offset`]: Self::max_offset
    /// [`replication_log_name`]: ReplicationOffset::replication_log_name
    pub fn min_present_offset(&self) -> ReadySetResult<Option<&ReplicationOffset>> {
        let mut res: Option<&ReplicationOffset> = None;
        for offset in self.schema.iter().chain(self.tables.values().flatten()) {
            match (res, offset) {
                (Some(off1), off2) => {
                    res = Some(ReplicationOffset::try_min(off1, off2)?);
                }
                (None, off) => {
                    res = Some(off);
                }
            }
        }
        Ok(res)
    }

    /// Advance replication offset for the schema and all tables to the given offset.
    /// Replication offsets will not change if they are ahead of the provided offset.
    pub fn advance_offset(&mut self, offset: ReplicationOffset) -> ReadySetResult<()> {
        for table_offset in self.tables.values_mut().flatten() {
            if offset.try_partial_cmp(table_offset)?.is_gt() {
                *table_offset = offset.clone();
            }
        }

        if let Some(ref mut schema_offset) = self.schema {
            if offset.try_partial_cmp(schema_offset)?.is_gt() {
                *schema_offset = offset;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod max_offset {
        use super::*;

        #[test]
        fn all_present_and_matching() {
            let offsets = ReplicationOffsets {
                schema: Some(
                    MySqlPosition::from_file_name_and_position("test.00001".into(), 1)
                        .unwrap()
                        .into(),
                ),
                tables: HashMap::from([
                    (
                        "t1".into(),
                        Some(
                            MySqlPosition::from_file_name_and_position("test.00001".into(), 2)
                                .unwrap()
                                .into(),
                        ),
                    ),
                    (
                        "t2".into(),
                        Some(
                            MySqlPosition::from_file_name_and_position("test.00001".into(), 3)
                                .unwrap()
                                .into(),
                        ),
                    ),
                ]),
            };
            let res: MySqlPosition = offsets.max_offset().unwrap().unwrap().try_into().unwrap();
            assert_eq!(res.binlog_file_name().to_string(), "test.00001");
            assert_eq!(res.position, 3);
        }

        #[test]
        fn all_present_not_matching() {
            let offsets = ReplicationOffsets {
                schema: Some(
                    MySqlPosition::from_file_name_and_position("binlog.00001".into(), 1)
                        .unwrap()
                        .into(),
                ),
                tables: HashMap::from([
                    (
                        "t1".into(),
                        Some(
                            MySqlPosition::from_file_name_and_position("test.00001".into(), 2)
                                .unwrap()
                                .into(),
                        ),
                    ),
                    (
                        "t2".into(),
                        Some(
                            MySqlPosition::from_file_name_and_position("test.00001".into(), 3)
                                .unwrap()
                                .into(),
                        ),
                    ),
                ]),
            };
            let res = offsets.max_offset();
            res.unwrap_err();
        }

        #[test]
        fn schema_missing() {
            let offsets = ReplicationOffsets {
                schema: None,
                tables: HashMap::from([
                    (
                        "t1".into(),
                        Some(
                            MySqlPosition::from_file_name_and_position("test.00001".into(), 2)
                                .unwrap()
                                .into(),
                        ),
                    ),
                    (
                        "t2".into(),
                        Some(
                            MySqlPosition::from_file_name_and_position("test.00001".into(), 3)
                                .unwrap()
                                .into(),
                        ),
                    ),
                ]),
            };
            let res = offsets.max_offset().unwrap();
            assert!(res.is_none());
        }

        #[test]
        fn table_missing() {
            let offsets = ReplicationOffsets {
                schema: Some(
                    MySqlPosition::from_file_name_and_position("test.00001".into(), 1)
                        .unwrap()
                        .into(),
                ),
                tables: HashMap::from([
                    (
                        "t1".into(),
                        Some(
                            MySqlPosition::from_file_name_and_position("test.00001".into(), 2)
                                .unwrap()
                                .into(),
                        ),
                    ),
                    ("t2".into(), None),
                ]),
            };
            let res = offsets.max_offset().unwrap();
            assert!(res.is_none());
        }
    }
}
