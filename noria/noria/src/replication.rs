//! Data types for implementing snapshot and streaming replication from an upstream database.

use std::cmp::{min_by_key, Ordering};
use std::collections::HashMap;

use noria_errors::{ReadySetError, ReadySetResult};
use serde::{Deserialize, Serialize};

/// A data type representing an offset in a replication log
///
/// Replication offsets are represented by a single global [offset](ReplicationOffset::offset),
/// scoped within a single log, identified by a
/// [`replication_log_name`](ReplicationOffset::replication_log_name). Within a single log, offsets
/// are totally ordered, but outside the scope of a log ordering is not well-defined.
///
/// See [the documentation for PersistentState](::noria_dataflow::state::persistent_state) for
/// more information about how replication offsets are used and persisted
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct ReplicationOffset {
    /// The actual offset within the replication log
    pub offset: u128,

    /// The name of the replication log that this offset is within. [`ReplicationOffset`]s with
    /// different log names are not comparable
    pub replication_log_name: String,
}

impl PartialOrd for ReplicationOffset {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if other.replication_log_name != self.replication_log_name {
            None
        } else {
            self.offset.partial_cmp(&other.offset)
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
        if let Some(other) = other {
            if self.replication_log_name != other.replication_log_name {
                return Err(ReadySetError::ReplicationOffsetLogDifferent(
                    self.replication_log_name.clone(),
                    other.replication_log_name.clone(),
                ));
            }

            if self.offset > other.offset {
                other.offset = self.offset
            }
        } else {
            *other = Some(self.clone())
        }

        Ok(())
    }
}

/// Set of replication offsets for the entire system
#[derive(Default, Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct ReplicationOffsets {
    /// Replication offset for the database schema, set as part of the controller metadata stored in
    /// the authority
    pub schema: Option<ReplicationOffset>,

    /// Replication offset for each individual table, if any.
    ///
    /// A table with [`None`] as its replication offset has not yet been snapshotted successfully
    pub tables: HashMap<String, Option<ReplicationOffset>>,
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

    /// If all replication offsets are present (the schema and all tables), returns the maximum of
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
            if res.replication_log_name != offset.replication_log_name {
                return Err(ReadySetError::ReplicationOffsetLogDifferent(
                    res.replication_log_name.clone(),
                    offset.replication_log_name.clone(),
                ));
            }
            if offset.offset > res.offset {
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
                (Some(off1), off2) if off1.replication_log_name != off2.replication_log_name => {
                    return Err(ReadySetError::ReplicationOffsetLogDifferent(
                        off1.replication_log_name.clone(),
                        off2.replication_log_name.clone(),
                    ));
                }
                (Some(off1), off2) => {
                    res = Some(min_by_key(off1, off2, |off| off.offset));
                }
                (None, off) => {
                    res = Some(off);
                }
            }
        }
        Ok(res)
    }

    /// Set the replication offset for the schema and all tables to the given offset
    pub fn set_offset(&mut self, offset: ReplicationOffset) {
        for table_offset in self.tables.values_mut() {
            *table_offset = Some(offset.clone());
        }
        self.schema = Some(offset);
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
                schema: Some(ReplicationOffset {
                    offset: 1,
                    replication_log_name: "test".to_owned(),
                }),
                tables: HashMap::from([
                    (
                        "t1".to_owned(),
                        Some(ReplicationOffset {
                            offset: 2,
                            replication_log_name: "test".to_owned(),
                        }),
                    ),
                    (
                        "t2".to_owned(),
                        Some(ReplicationOffset {
                            offset: 3,
                            replication_log_name: "test".to_owned(),
                        }),
                    ),
                ]),
            };
            let res = offsets.max_offset().unwrap().unwrap();
            assert_eq!(res.replication_log_name, "test");
            assert_eq!(res.offset, 3);
        }

        #[test]
        fn all_present_not_matching() {
            let offsets = ReplicationOffsets {
                schema: Some(ReplicationOffset {
                    offset: 1,
                    replication_log_name: "binlog".to_owned(),
                }),
                tables: HashMap::from([
                    (
                        "t1".to_owned(),
                        Some(ReplicationOffset {
                            offset: 2,
                            replication_log_name: "test".to_owned(),
                        }),
                    ),
                    (
                        "t2".to_owned(),
                        Some(ReplicationOffset {
                            offset: 3,
                            replication_log_name: "test".to_owned(),
                        }),
                    ),
                ]),
            };
            let res = offsets.max_offset();
            assert!(res.is_err());
        }

        #[test]
        fn schema_missing() {
            let offsets = ReplicationOffsets {
                schema: None,
                tables: HashMap::from([
                    (
                        "t1".to_owned(),
                        Some(ReplicationOffset {
                            offset: 2,
                            replication_log_name: "test".to_owned(),
                        }),
                    ),
                    (
                        "t2".to_owned(),
                        Some(ReplicationOffset {
                            offset: 3,
                            replication_log_name: "test".to_owned(),
                        }),
                    ),
                ]),
            };
            let res = offsets.max_offset().unwrap();
            assert!(res.is_none());
        }

        #[test]
        fn table_missing() {
            let offsets = ReplicationOffsets {
                schema: Some(ReplicationOffset {
                    offset: 1,
                    replication_log_name: "test".to_owned(),
                }),
                tables: HashMap::from([
                    (
                        "t1".to_owned(),
                        Some(ReplicationOffset {
                            offset: 2,
                            replication_log_name: "test".to_owned(),
                        }),
                    ),
                    ("t2".to_owned(), None),
                ]),
            };
            let res = offsets.max_offset().unwrap();
            assert!(res.is_none());
        }
    }
}
