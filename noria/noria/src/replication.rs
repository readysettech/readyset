//! Data types for implementing snapshot and streaming replication from an upstream database.

use std::cmp::Ordering;
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
}
