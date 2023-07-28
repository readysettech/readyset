mod connector;
mod ddl_replication;
mod lsn;
mod snapshot;
mod wal;
mod wal_reader;

use std::fmt::{self, Display};

pub use connector::{
    drop_publication, drop_readyset_schema, drop_replication_slot, PostgresWalConnector,
};
use readyset_client::replication::ReplicationOffset;
pub use snapshot::PostgresReplicator;

use self::lsn::Lsn;

pub(crate) const REPLICATION_SLOT: &str = "readyset";
pub(crate) const PUBLICATION_NAME: &str = "readyset";

#[derive(Debug, PartialEq, PartialOrd, Ord, Eq, Clone, Copy, Default)]
pub struct PostgresPosition {
    /// Postgres Log Sequence Number
    pub lsn: Lsn,
}

impl From<&PostgresPosition> for ReplicationOffset {
    fn from(value: &PostgresPosition) -> Self {
        ReplicationOffset {
            replication_log_name: String::new(),
            offset: value.lsn.0 as _,
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
            lsn: (val.offset as i64).into(),
        }
    }
}

impl From<&ReplicationOffset> for PostgresPosition {
    fn from(val: &ReplicationOffset) -> Self {
        PostgresPosition {
            lsn: (val.offset as i64).into(),
        }
    }
}

impl From<i64> for PostgresPosition {
    fn from(i: i64) -> Self {
        PostgresPosition { lsn: Lsn(i) }
    }
}

impl From<Lsn> for PostgresPosition {
    fn from(lsn: Lsn) -> Self {
        PostgresPosition { lsn }
    }
}

impl From<&Lsn> for ReplicationOffset {
    fn from(lsn: &Lsn) -> Self {
        ReplicationOffset {
            replication_log_name: String::new(),
            offset: lsn.0 as _,
        }
    }
}

impl From<Lsn> for ReplicationOffset {
    fn from(lsn: Lsn) -> Self {
        (&lsn).into()
    }
}

impl Display for PostgresPosition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.lsn)
    }
}
