mod connector;
mod snapshot;
mod wal;
mod wal_reader;

pub use connector::PostgresWalConnector;
pub use snapshot::PostgresReplicator;

use std::fmt::{self, Display};

pub(crate) const REPLICATION_SLOT: &str = "noria";
pub(crate) const PUBLICATION_NAME: &str = "noria";

#[derive(Debug, PartialEq, PartialOrd, Ord, Eq, Clone, Copy, Default)]
pub struct PostgresPosition {
    /// Postgres Log Sequence Number
    pub lsn: i64,
}

impl From<&PostgresPosition> for noria::ReplicationOffset {
    fn from(value: &PostgresPosition) -> Self {
        noria::ReplicationOffset {
            replication_log_name: String::new(),
            offset: value.lsn as _,
        }
    }
}

impl From<PostgresPosition> for noria::ReplicationOffset {
    fn from(value: PostgresPosition) -> Self {
        (&value).into()
    }
}

impl From<noria::ReplicationOffset> for PostgresPosition {
    fn from(val: noria::ReplicationOffset) -> Self {
        PostgresPosition {
            lsn: val.offset as _,
        }
    }
}

impl From<&noria::ReplicationOffset> for PostgresPosition {
    fn from(val: &noria::ReplicationOffset) -> Self {
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
