use std::fmt::{self, Formatter};
use std::ops::Add;
use std::str::FromStr;

use bytes::{BufMut, BytesMut};
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
/// defining a [`PostgresPosition`] as `(commit_lsn, lsn)`, where `commit_lsn` is the LSN of the
/// the COMMIT that will end the current transaction.
#[derive(Debug, PartialEq, PartialOrd, Ord, Eq, Clone, Copy, Default, Serialize, Deserialize)]
pub struct PostgresPosition {
    /// The LSN of the COMMIT that ends the current transaction
    pub commit_lsn: CommitLsn,
    /// The LSN of the position
    pub lsn: Lsn,
}

impl PostgresPosition {
    /// Constructs a [`PostgresPosition`] from a [`CommitLsn`] that points to the lowest possible
    /// position in the given commit. In other words, this method constructs a [`PostgresPosition`]
    /// that points to `(commit_lsn, 0)`.
    pub fn commit_start(commit_lsn: CommitLsn) -> Self {
        Self {
            commit_lsn,
            lsn: 0.into(),
        }
    }

    /// Constructs a [`PostgresPosition`] from a [`CommitLsn`] that points to the end position in
    /// the given commit. In other words, this method constructs a [`PostgresPosition`] that points
    /// to `(commit_lsn, commit_lsn)`.
    pub fn commit_end(commit_lsn: CommitLsn) -> Self {
        Self {
            commit_lsn,
            lsn: Lsn(commit_lsn.0),
        }
    }

    /// Consumes `self`, constructing a new [`PostgresPosition`] with `self`'s [`CommitLsn`] and the
    /// given [`Lsn`].
    pub fn with_lsn(self, lsn: impl Into<Lsn>) -> Self {
        Self {
            commit_lsn: self.commit_lsn,
            lsn: lsn.into(),
        }
    }
}

impl PostgresPosition {
    /// Computes the byte lag between `self` (ReadySet's position) and `upstream`.
    ///
    /// Returns the number of WAL bytes `self` is behind `upstream`, using `commit_lsn`
    /// (the last fully committed transaction position).
    /// Returns 0 if `self` is at or ahead of `upstream`.
    pub fn byte_lag_behind(&self, upstream: &PostgresPosition) -> u64 {
        upstream
            .commit_lsn
            .as_i64()
            .saturating_sub(self.commit_lsn.as_i64())
            .max(0) as u64
    }
}

impl From<PostgresPosition> for ReplicationOffset {
    fn from(value: PostgresPosition) -> Self {
        Self::Postgres(value)
    }
}

impl fmt::Display for PostgresPosition {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "({}, {})", self.commit_lsn, self.lsn)
    }
}

/// Postgres's "log sequence number"
#[derive(PartialEq, PartialOrd, Ord, Eq, Clone, Copy, Default, Serialize, Deserialize)]
pub struct Lsn(i64);

impl Lsn {
    /// Puts `self` into the given [`BytesMut`].
    pub fn put_into(&self, bytes: &mut BytesMut) {
        bytes.put_i64(self.0);
    }
}

impl Add<i64> for Lsn {
    type Output = Self;

    fn add(self, rhs: i64) -> Self::Output {
        Self(self.0 + rhs)
    }
}

impl TryFrom<&ReplicationOffset> for Lsn {
    type Error = ReadySetError;

    fn try_from(offset: &ReplicationOffset) -> Result<Self, Self::Error> {
        Ok(PostgresPosition::try_from(offset)?.lsn)
    }
}

/// This type specifically represents the Postgres "log sequence number" of COMMITs. It is used to
/// differentiate at compile time between LSNs that can be used to represent any WAL events and LSNs
/// that can only represent COMMITs.
#[derive(PartialEq, PartialOrd, Ord, Eq, Clone, Copy, Default, Serialize, Deserialize)]
pub struct CommitLsn(i64);

impl TryFrom<ReplicationOffset> for CommitLsn {
    type Error = ReadySetError;

    fn try_from(offset: ReplicationOffset) -> Result<Self, Self::Error> {
        Ok(PostgresPosition::try_from(offset)?.commit_lsn)
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

        impl $t {
            /// Returns the inner byte offset as an `i64`.
            pub fn as_i64(&self) -> i64 {
                self.0
            }
        }

        impl FromStr for $t {
            type Err = ReadySetError;

            fn from_str(lsn: &str) -> Result<Self, ReadySetError> {
                // Internally, an LSN is a 64-bit integer, representing a byte position in the
                // write-ahead log stream. It is printed as two hexadecimal numbers of up to
                // 8 digits each, separated by a slash; for example, 16/B374D848
                let (hi, lo) = lsn.split_once('/').ok_or_else(|| {
                    ReadySetError::ReplicationFailed(format!("Invalid LSN {lsn}"))
                })?;
                let hi = i64::from_str_radix(hi, 16).map_err(|e| {
                    ReadySetError::ReplicationFailed(format!("Invalid LSN {lsn}: {e}"))
                })?;
                let lo = i64::from_str_radix(lo, 16).map_err(|e| {
                    ReadySetError::ReplicationFailed(format!("Invalid LSN {lsn}: {e}"))
                })?;

                Ok(Self(hi << 32 | lo))
            }
        }
    };
}

impl_lsn_traits!(Lsn);
impl_lsn_traits!(CommitLsn);

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::{CommitLsn, Lsn, PostgresPosition};
    use crate::ReplicationOffset;

    // The `PartialOrd` derivation on `PostgresPosition` relies upon the ordering of the members
    // in the struct: `commit_lsn` *must* be listed before `lsn`. This test will fail if
    // the members of the struct are ever reordered.
    #[test]
    fn test_postgres_position_partial_ord() {
        let pos1 = PostgresPosition {
            commit_lsn: 1.into(),
            lsn: 0.into(),
        };
        let pos2 = PostgresPosition {
            commit_lsn: 0.into(),
            lsn: 1.into(),
        };

        assert!(pos1 > pos2);
    }

    #[test]
    fn test_commit_lsn_round_trip() {
        assert_eq!(
            CommitLsn::from_str("16/17DD38B8").unwrap().to_string(),
            "16/17DD38B8"
        );

        let pos = CommitLsn(198237);
        assert_eq!(CommitLsn::from_str(&pos.to_string()).unwrap(), pos);
    }

    #[test]
    fn test_commit_lsn_from_str() {
        assert_eq!(CommitLsn::from_str("16/17DD38B8").unwrap().0, 94889654456);
    }

    #[test]
    fn test_commit_lsn_from_str_leading_zero() {
        assert_eq!(
            CommitLsn::from_str("16/B374D84").unwrap(),
            CommitLsn::from_str("16/0B374D84").unwrap()
        );
    }

    #[test]
    fn test_lsn_round_trip() {
        assert_eq!(
            Lsn::from_str("16/17DD38B8").unwrap().to_string(),
            "16/17DD38B8"
        );

        let pos = Lsn(198237);
        assert_eq!(Lsn::from_str(&pos.to_string()).unwrap(), pos);
    }

    #[test]
    fn test_lsn_from_str() {
        assert_eq!(Lsn::from_str("16/17DD38B8").unwrap().0, 94889654456);
    }

    #[test]
    fn test_lsn_from_str_leading_zero() {
        assert_eq!(
            Lsn::from_str("16/B374D84").unwrap(),
            Lsn::from_str("16/0B374D84").unwrap()
        );
    }

    // NOTE: This test checks that the current structure of `ReplicationOffset` is backwards
    // compatible with the last committed structure. If it is failing, the updated structure
    // needs to be modified to be backwards compatible, or additional serde logic is needed to
    // convert between the two forms, and new tests should be added that check that we are
    // incrementally backwards compatible from the original version.
    #[test]
    /// Serialized data from the current version of the struct, generated with:
    /// ```rust
    /// use crate::postgres::PostgresPosition;
    /// let offset = ReplicationOffset::Postgres(PostgresPosition {
    ///     commit_lsn: 42.into(),
    ///     lsn: 9001.into(),
    /// });
    /// eprintln!("{}", serde_json::ser::to_string(&offset).unwrap());
    /// ```
    fn test_postgres_v1() {
        let serialized_data = r#"{"Postgres":{"commit_lsn":42,"lsn":9001}}"#;
        let deserialized: Result<ReplicationOffset, _> = serde_json::from_str(serialized_data);
        assert!(
            deserialized.is_ok(),
            "ReplicationOffset is not backwards compatible. See note above test."
        );
        assert_eq!(
            deserialized.unwrap(),
            ReplicationOffset::Postgres(PostgresPosition {
                commit_lsn: CommitLsn(42),
                lsn: Lsn(9001)
            })
        );
    }

    fn pg_pos(lsn: i64) -> PostgresPosition {
        PostgresPosition {
            commit_lsn: CommitLsn::from(lsn),
            lsn: Lsn::from(lsn),
        }
    }

    #[test]
    fn test_byte_lag_behind_upstream_ahead() {
        assert_eq!(pg_pos(100).byte_lag_behind(&pg_pos(200)), 100);
    }

    #[test]
    fn test_byte_lag_behind_same_position() {
        assert_eq!(pg_pos(500).byte_lag_behind(&pg_pos(500)), 0);
    }

    #[test]
    fn test_byte_lag_behind_readyset_ahead() {
        assert_eq!(pg_pos(300).byte_lag_behind(&pg_pos(100)), 0);
    }

    #[test]
    fn test_byte_lag_behind_realistic_lsns() {
        let readyset_lsn = Lsn::from_str("16/17DD38B8").unwrap();
        let upstream_lsn = Lsn::from_str("16/27DD38B8").unwrap();
        let readyset = PostgresPosition {
            commit_lsn: CommitLsn::from(readyset_lsn.as_i64()),
            lsn: readyset_lsn,
        };
        let upstream = PostgresPosition {
            commit_lsn: CommitLsn::from(upstream_lsn.as_i64()),
            lsn: upstream_lsn,
        };
        // 0x27DD38B8 - 0x17DD38B8 = 0x10000000 = 268435456
        assert_eq!(readyset.byte_lag_behind(&upstream), 268435456);
    }
}
