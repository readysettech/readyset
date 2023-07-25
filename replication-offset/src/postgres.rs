use std::fmt::{self, Formatter};

use serde::{Deserialize, Serialize};

use crate::ReplicationOffset;

/// Represents a position within the Postgres write-ahead log
#[derive(Debug, PartialEq, PartialOrd, Ord, Eq, Clone, Copy, Default, Serialize, Deserialize)]
pub struct PostgresPosition {
    /// Postgres Log Sequence Number
    pub lsn: Lsn,
}

impl From<PostgresPosition> for ReplicationOffset {
    fn from(value: PostgresPosition) -> Self {
        Self::Postgres(value)
    }
}

impl From<i64> for PostgresPosition {
    fn from(i: i64) -> Self {
        PostgresPosition { lsn: i.into() }
    }
}

impl From<Lsn> for PostgresPosition {
    fn from(lsn: Lsn) -> Self {
        PostgresPosition { lsn }
    }
}

impl fmt::Display for PostgresPosition {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.lsn)
    }
}

/// Postgres's "log sequence number"
#[derive(PartialEq, PartialOrd, Ord, Eq, Clone, Copy, Default, Serialize, Deserialize)]
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

impl From<Lsn> for ReplicationOffset {
    fn from(lsn: Lsn) -> Self {
        Self::Postgres(PostgresPosition { lsn })
    }
}
