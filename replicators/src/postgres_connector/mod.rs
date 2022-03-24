mod connector;
mod snapshot;
mod wal;
mod wal_reader;

use std::fmt::{self, Display};

pub use connector::PostgresWalConnector;
use noria::replication::ReplicationOffset;
pub use snapshot::PostgresReplicator;

pub(crate) const REPLICATION_SLOT: &str = "readyset";
pub(crate) const PUBLICATION_NAME: &str = "readyset";

#[derive(Debug, PartialEq, PartialOrd, Ord, Eq, Clone, Copy, Default)]
pub struct PostgresPosition {
    /// Postgres Log Sequence Number
    pub lsn: i64,
}

impl From<&PostgresPosition> for ReplicationOffset {
    fn from(value: &PostgresPosition) -> Self {
        ReplicationOffset {
            replication_log_name: String::new(),
            offset: value.lsn as _,
        }
    }
}

impl From<PostgresPosition> for ReplicationOffset {
    fn from(value: PostgresPosition) -> Self {
        (&value).into()
    }
}

impl From<ReplicationOffset> for PostgresPosition {
    fn from(val: ReplicationOffset) -> Self {
        PostgresPosition {
            lsn: val.offset as _,
        }
    }
}

impl From<&ReplicationOffset> for PostgresPosition {
    fn from(val: &ReplicationOffset) -> Self {
        PostgresPosition {
            lsn: val.offset as _,
        }
    }
}

impl From<i64> for PostgresPosition {
    fn from(val: i64) -> Self {
        PostgresPosition { lsn: val }
    }
}

impl Display for PostgresPosition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:x}/{:x}", self.lsn >> 32, self.lsn & 0xffffffff)
    }
}
