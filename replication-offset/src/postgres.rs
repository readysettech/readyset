use std::fmt::{self, Formatter};

use readyset_errors::ReadySetError;
use serde::{Deserialize, Serialize};

use crate::ReplicationOffset;

/// Represents a position within the Postgres write-ahead log.
///
/// Postgres reorders WAL events so that we receive all of the events in a given transaction at
/// once, which means that there is no guarantee that the LSNs of events we receive will
/// monotonically increase. This means the event LSN on its own is not enough to ensure that we
/// can define a [`ReplicationOffset`] that is well-ordered. However, there *is* a guarantee that
/// the LSNs of **COMMIT** events we receive will monotonically increase. We leverage this by
/// including the LSN of the last COMMIT we saw in our definition of [`PostgresPosition`], which
/// gives us an ordering key of `(last_commit_lsn, lsn)`.
#[derive(Debug, PartialEq, Ord, Eq, Clone, Copy, Default, Serialize, Deserialize)]
pub struct PostgresPosition {
    /// The LSN of the last COMMIT we saw
    pub last_commit_lsn: CommitLsn,
    /// The LSN of the position
    pub lsn: Lsn,
}

impl PartialOrd for PostgresPosition {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        if self.lsn == other.lsn {
            debug_assert!(
                self.last_commit_lsn == other.last_commit_lsn,
                "If two positions have the same lsn, they should have the same last_commit_lsn."
            );

            return Some(std::cmp::Ordering::Equal);
        }

        // If our lsn is greater than other.lsn, we are greater.
        // TODO: This doesn't really make sense since it won't be the same with the arguments
        // reversed, right? Or does it since we are looking at a stream of lsns?
        //
        if self.lsn > other.lsn {
            return Some(std::cmp::Ordering::Greater);
        }

        // Otherwise use the commit first and then the lsn to determine ordering
        (self.last_commit_lsn, self.lsn).partial_cmp(&(other.last_commit_lsn, other.lsn))
    }
}

impl PostgresPosition {
    /// Constructs a [`PostgresPosition`] from a [`CommitLsn`]. This function initializes the
    /// [`Lsn`] of the [`PostgresPosition`] to zero.
    pub fn from_commit_lsn(commit_lsn: impl Into<CommitLsn>) -> Self {
        Self {
            last_commit_lsn: commit_lsn.into(),
            lsn: 0.into(),
        }
    }

    /// Consumes `self`, constructing a new [`PostgresPosition`] with `self`'s [`CommitLsn`] and the
    /// given [`Lsn`].
    pub fn with_lsn(self, lsn: impl Into<Lsn>) -> Self {
        Self {
            last_commit_lsn: self.last_commit_lsn,
            lsn: lsn.into(),
        }
    }
}

impl From<PostgresPosition> for ReplicationOffset {
    fn from(value: PostgresPosition) -> Self {
        Self::Postgres(value)
    }
}

impl fmt::Display for PostgresPosition {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "({}, {})", self.last_commit_lsn, self.lsn)
    }
}

/// Postgres's "log sequence number"
#[derive(PartialEq, PartialOrd, Ord, Eq, Clone, Copy, Default, Serialize, Deserialize)]
pub struct Lsn(pub i64);

impl TryFrom<ReplicationOffset> for Lsn {
    type Error = ReadySetError;

    fn try_from(offset: ReplicationOffset) -> Result<Self, Self::Error> {
        Ok(PostgresPosition::try_from(offset)?.lsn)
    }
}

/// This type specifically represents the Postgres "log sequence number" of COMMITs. It is used to
/// differentiate at compile time between LSNs that can be used to represent any WAL events and LSNs
/// that can only represent COMMITs.
#[derive(PartialEq, PartialOrd, Ord, Eq, Clone, Copy, Default, Serialize, Deserialize)]
pub struct CommitLsn(pub i64);

impl TryFrom<ReplicationOffset> for CommitLsn {
    type Error = ReadySetError;

    fn try_from(offset: ReplicationOffset) -> Result<Self, Self::Error> {
        Ok(PostgresPosition::try_from(offset)?.last_commit_lsn)
    }
}

macro_rules! impl_lsn_traits {
    ($t:ty) => {
        impl fmt::Display for $t {
            fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
                write!(f, "{:X}/{:X}", self.0 >> 32, self.0 & 0xffffffff)
            }
        }

        impl fmt::Debug for $t {
            fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
                write!(f, "{}", self)
            }
        }

        impl From<i64> for $t {
            fn from(i: i64) -> Self {
                Self(i)
            }
        }
    };
}

impl_lsn_traits!(Lsn);
impl_lsn_traits!(CommitLsn);

#[cfg(test)]
mod tests {
    use super::PostgresPosition;

    // The `PartialOrd` derivation on `PostgresPosition` relies upon the ordering of the members
    // in the struct: `last_commit_lsn` *must* be listed before `lsn`. This test will fail if
    // the members of the struct are ever reordered.
    #[test]
    fn test_postgres_position_partial_ord() {
        let pos1 = PostgresPosition {
            last_commit_lsn: 1.into(),
            lsn: 0.into(),
        };
        let pos2 = PostgresPosition {
            last_commit_lsn: 0.into(),
            lsn: 1.into(),
        };

        assert!(pos1 > pos2);
    }
}
