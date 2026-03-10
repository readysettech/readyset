//! Data types for implementing snapshot and streaming replication from an upstream database.

pub mod mysql;
pub mod mysql_gtid;
pub mod postgres;

pub use mysql_gtid::{GtidEvent, GtidRange, GtidSet, GtidSource, looks_like_gtid};

use std::borrow::Borrow;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt;
use std::hash::Hash;
use std::str::FromStr;

use mysql::MySqlPosition;
use postgres::PostgresPosition;
use readyset_errors::{ReadySetError, ReadySetResult, internal_err, replication_failed};
use readyset_sql::Dialect;
use readyset_sql::ast::Relation;
use serde::{Deserialize, Serialize};

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
    Gtid(GtidSet),
    Postgres(PostgresPosition),
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

impl TryFrom<ReplicationOffset> for GtidSet {
    type Error = ReadySetError;

    fn try_from(offset: ReplicationOffset) -> Result<Self, Self::Error> {
        if let ReplicationOffset::Gtid(offset) = offset {
            Ok(offset)
        } else {
            Err(internal_err!(
                "cannot extract GtidSet from non-GTID ReplicationOffset"
            ))
        }
    }
}

impl TryFrom<&ReplicationOffset> for GtidSet {
    type Error = ReadySetError;

    fn try_from(offset: &ReplicationOffset) -> Result<Self, Self::Error> {
        offset.clone().try_into()
    }
}

impl fmt::Display for ReplicationOffset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MySql(pos) => write!(f, "{pos}"),
            Self::Gtid(gtid) => write!(f, "{gtid}"),
            Self::Postgres(pos) => write!(f, "{pos}"),
        }
    }
}

impl FromStr for ReplicationOffset {
    type Err = ReadySetError;

    /// Parse a replication position string into a [`ReplicationOffset`].
    ///
    /// The format is auto-detected:
    /// - Contains `'/'` → PostgreSQL LSN (e.g. `"0/16B3748"`)
    /// - Contains a UUID prefix → MySQL GTID (e.g.
    ///   `"3E11FA47-71CA-11E1-9E33-C80AA9429562:1-10"`)
    /// - Otherwise → MySQL binlog position (e.g. `"mysql-bin.000003:154"`)
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.contains('/') {
            let commit_lsn: postgres::CommitLsn = s.parse()?;
            Ok(Self::Postgres(PostgresPosition::commit_end(commit_lsn)))
        } else if looks_like_gtid(s) {
            Ok(Self::Gtid(GtidSet::parse(s)?))
        } else {
            Ok(Self::MySql(s.parse()?))
        }
    }
}

impl PartialOrd for ReplicationOffset {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Self::MySql(pos), Self::MySql(other_pos)) => pos.partial_cmp(other_pos),
            (Self::Gtid(gtid), Self::Gtid(other_gtid)) => gtid.partial_cmp(other_gtid),
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
            (Self::Gtid(offset), Self::Gtid(other_offset)) => offset.try_partial_cmp(other_offset),
            (Self::Postgres(offset), Self::Postgres(other_offset)) => Ok(offset.cmp(other_offset)),
            _ => Err(internal_err!(
                "Cannot compare replication offsets from different database backends"
            )),
        }
    }

    /// Returns the SQL dialect corresponding to this replication offset variant.
    pub fn dialect(&self) -> Dialect {
        match self {
            Self::MySql(_) | Self::Gtid(_) => Dialect::MySQL,
            Self::Postgres(_) => Dialect::PostgreSQL,
        }
    }

    /// Returns true if this is a GTID-based MySQL offset.
    pub fn is_gtid(&self) -> bool {
        matches!(self, Self::Gtid(_))
    }

    /// Get the MySQL binlog position, or error if this is not a file-based MySQL offset.
    pub fn mysql_position(&self) -> ReadySetResult<&MySqlPosition> {
        match self {
            Self::MySql(pos) => Ok(pos),
            _ => replication_failed!("Expected MySQL file-based offset, got {self:?}"),
        }
    }

    /// Get a mutable reference to the MySQL binlog position, or error if not file-based.
    pub fn mysql_position_mut(&mut self) -> ReadySetResult<&mut MySqlPosition> {
        match self {
            Self::MySql(pos) => Ok(pos),
            _ => replication_failed!("Expected MySQL file-based offset"),
        }
    }

    /// Get a mutable reference to the GTID set, if this is a GTID-based offset.
    pub fn gtid_set_mut(&mut self) -> Option<&mut GtidSet> {
        match self {
            Self::Gtid(set) => Some(set),
            _ => None,
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

    /// If all replication offsets are present (the schema and all tables), returns the max of
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

        if let Some(ref mut schema_offset) = self.schema
            && offset.try_partial_cmp(schema_offset)?.is_gt()
        {
            *schema_offset = offset;
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

    mod gtid_offset {
        use super::*;
        use uuid::Uuid;

        fn test_uuid() -> Uuid {
            Uuid::parse_str("3E11FA47-71CA-11E1-9E33-C80AA9429562").unwrap()
        }

        fn key() -> GtidSource {
            GtidSource {
                server_uuid: test_uuid(),
                tag: None,
            }
        }

        /// Create a GTID set with a contiguous range [1, max_sequence]
        fn make_gtid_set_range(max_sequence: u64) -> GtidSet {
            let mut set = GtidSet::new();
            for i in 1..=max_sequence {
                set.advance(key(), i);
            }
            set
        }

        #[test]
        fn from_gtid_set_to_replication_offset() {
            let gtid_set = make_gtid_set_range(10);
            let offset: ReplicationOffset = gtid_set.into();
            assert!(matches!(offset, ReplicationOffset::Gtid(_)));
        }

        #[test]
        fn try_from_gtid_offset_succeeds() {
            let gtid_set = make_gtid_set_range(10);
            let offset: ReplicationOffset = gtid_set.clone().into();
            let extracted: GtidSet = offset.try_into().unwrap();
            assert_eq!(extracted, gtid_set);
        }

        #[test]
        fn try_from_binlog_offset_fails() {
            let offset: ReplicationOffset =
                MySqlPosition::from_file_name_and_position("test.00001".into(), 1)
                    .unwrap()
                    .into();
            let result: Result<GtidSet, _> = offset.try_into();
            assert!(result.is_err());
        }

        #[test]
        fn gtid_offsets_are_comparable() {
            // Create sets where one is a proper subset of the other
            // set1: [1-10], set2: [1-20]
            // set1 < set2 because set1 is a subset of set2
            let offset1: ReplicationOffset = make_gtid_set_range(10).into();
            let offset2: ReplicationOffset = make_gtid_set_range(20).into();

            assert!(offset1.try_partial_cmp(&offset2).unwrap().is_lt());
            assert!(offset2.try_partial_cmp(&offset1).unwrap().is_gt());
        }

        #[test]
        fn gtid_offsets_equal_when_same() {
            let offset1: ReplicationOffset = make_gtid_set_range(10).into();
            let offset2: ReplicationOffset = make_gtid_set_range(10).into();

            assert!(offset1.try_partial_cmp(&offset2).unwrap().is_eq());
        }

        #[test]
        fn gtid_vs_binlog_not_comparable() {
            let gtid_offset: ReplicationOffset = make_gtid_set_range(10).into();
            let binlog_offset: ReplicationOffset =
                MySqlPosition::from_file_name_and_position("test.00001".into(), 1)
                    .unwrap()
                    .into();

            assert!(gtid_offset.try_partial_cmp(&binlog_offset).is_err());
            assert!(binlog_offset.try_partial_cmp(&gtid_offset).is_err());
        }

        #[test]
        fn gtid_offset_display() {
            let gtid_set = make_gtid_set_range(10);
            let offset: ReplicationOffset = gtid_set.into();
            let display = format!("{}", offset);
            assert!(display.contains(&test_uuid().to_string()));
            // Range [1-10] should be displayed
            assert!(display.contains("1-10"));
        }

        #[test]
        fn max_offset_with_gtid() {
            // Create GTID sets where each is a superset of the previous
            // schema: [1-5], t1: [1-10], t2: [1-15]
            let offsets = ReplicationOffsets {
                schema: Some(make_gtid_set_range(5).into()),
                tables: HashMap::from([
                    ("t1".into(), Some(make_gtid_set_range(10).into())),
                    ("t2".into(), Some(make_gtid_set_range(15).into())),
                ]),
            };
            let res: GtidSet = offsets.max_offset().unwrap().unwrap().try_into().unwrap();
            // The max should be [1-15]
            let ranges = res.get(&key()).unwrap();
            assert_eq!(ranges.len(), 1);
            assert_eq!(ranges[0].start, 1);
            assert_eq!(ranges[0].end, 15);
        }
    }

    mod parse_replication_offset {
        use super::*;

        #[test]
        fn parse_mysql_binlog_position() {
            let offset: ReplicationOffset = "mysql-bin.000003:154".parse().unwrap();
            let mysql_pos = MySqlPosition::try_from(offset).unwrap();
            assert_eq!(mysql_pos.binlog_file_suffix, 3);
            assert_eq!(mysql_pos.position, 154);
        }

        #[test]
        fn parse_mysql_binlog_position_invalid() {
            assert!("invalid".parse::<ReplicationOffset>().is_err());
            assert!("".parse::<ReplicationOffset>().is_err());
            assert!(":".parse::<ReplicationOffset>().is_err());
            assert!(":154".parse::<ReplicationOffset>().is_err());
            assert!("file:notanumber".parse::<ReplicationOffset>().is_err());
            assert!(
                "file:99999999999999999999"
                    .parse::<ReplicationOffset>()
                    .is_err()
            );
        }

        #[test]
        fn parse_postgres_lsn() {
            let offset: ReplicationOffset = "0/16B3748".parse().unwrap();
            assert!(matches!(offset, ReplicationOffset::Postgres(_)));
        }

        #[test]
        fn parse_postgres_lsn_invalid() {
            assert!("0/".parse::<ReplicationOffset>().is_err());
            assert!("/16B3748".parse::<ReplicationOffset>().is_err());
        }

        #[test]
        fn parse_gtid_untagged() {
            let offset: ReplicationOffset =
                "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-10".parse().unwrap();
            assert!(matches!(offset, ReplicationOffset::Gtid(_)));
        }

        #[test]
        fn parse_gtid_tagged() {
            let offset: ReplicationOffset = "3E11FA47-71CA-11E1-9E33-C80AA9429562:mytag:1-10"
                .parse()
                .unwrap();
            assert!(matches!(offset, ReplicationOffset::Gtid(_)));
        }

        #[test]
        fn parse_gtid_mixed_tagged_and_untagged() {
            let offset: ReplicationOffset =
                "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-121:readtest:1-3:repltest:1-6"
                    .parse()
                    .unwrap();
            assert!(matches!(offset, ReplicationOffset::Gtid(_)));
        }

        #[test]
        fn parse_gtid_multiple_uuids() {
            let offset: ReplicationOffset = "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-10,\
                 A0B1C2D3-E4F5-6789-ABCD-EF0123456789:1-5"
                .parse()
                .unwrap();
            assert!(matches!(offset, ReplicationOffset::Gtid(_)));
        }

        #[test]
        fn parse_gtid_multiple_uuids_with_tags() {
            let offset: ReplicationOffset = "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-10:mytag:1-3,\
                 A0B1C2D3-E4F5-6789-ABCD-EF0123456789:othertag:1-5"
                .parse()
                .unwrap();
            assert!(matches!(offset, ReplicationOffset::Gtid(_)));
        }

        #[test]
        fn parse_gtid_invalid() {
            // UUID with no ranges
            assert!(
                "3E11FA47-71CA-11E1-9E33-C80AA9429562"
                    .parse::<ReplicationOffset>()
                    .is_err()
            );
            // Malformed UUID
            assert!("not-a-uuid:1-10".parse::<ReplicationOffset>().is_err());
        }

        #[test]
        fn dialect_mysql_binlog() {
            let offset: ReplicationOffset = "mysql-bin.000003:154".parse().unwrap();
            assert_eq!(offset.dialect(), Dialect::MySQL);
        }

        #[test]
        fn dialect_postgres() {
            let offset: ReplicationOffset = "0/16B3748".parse().unwrap();
            assert_eq!(offset.dialect(), Dialect::PostgreSQL);
        }

        #[test]
        fn dialect_gtid() {
            let offset: ReplicationOffset =
                "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-10".parse().unwrap();
            assert_eq!(offset.dialect(), Dialect::MySQL);
        }
    }
}
