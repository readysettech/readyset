use std::fmt::{self, Formatter};

use crate::ReplicationOffset;

/// Represents a position within in the Postgres write-ahead log
#[derive(Debug, PartialEq, PartialOrd, Ord, Eq, Clone, Copy, Default)]
pub struct PostgresPosition {
    /// Postgres Log Sequence Number
    pub lsn: Lsn,
}

impl From<&PostgresPosition> for ReplicationOffset {
    fn from(value: &PostgresPosition) -> Self {
        ReplicationOffset {
            replication_log_name: String::new(),
            offset: value.lsn.0 as u128,
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
            lsn: Lsn(val.offset as i64),
        }
    }
}

impl From<&ReplicationOffset> for PostgresPosition {
    fn from(val: &ReplicationOffset) -> Self {
        PostgresPosition {
            lsn: Lsn(val.offset as i64),
        }
    }
}

impl From<i64> for PostgresPosition {
    fn from(i: i64) -> Self {
        PostgresPosition { lsn: i.into() }
    }
}

impl fmt::Display for PostgresPosition {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.lsn)
    }
}

/// Postgres's "log sequence number"
#[derive(PartialEq, PartialOrd, Ord, Eq, Clone, Copy, Default)]
pub struct Lsn(pub i64);

impl fmt::Display for Lsn {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{:X}/{:X}", self.0 >> 32, self.0 & 0xffffffff)
    }
}

impl fmt::Debug for Lsn {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self)
    }
}

impl From<i64> for Lsn {
    fn from(i: i64) -> Self {
        Self(i)
    }
}

impl From<Lsn> for PostgresPosition {
    fn from(lsn: Lsn) -> Self {
        PostgresPosition { lsn }
    }
}

impl From<Lsn> for ReplicationOffset {
    fn from(lsn: Lsn) -> Self {
        (&lsn).into()
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
