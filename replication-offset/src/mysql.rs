use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;

use readyset_errors::{ReadySetError, ReadySetResult};
use readyset_util::fmt::fmt_with;
use serde::{Deserialize, Serialize};

use crate::ReplicationOffset;

/// Represents a position within in the MySQL binlog. The binlog consists of an ordered sequence of
/// files that share a base name, where each file name has a sequence number appended to the end.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct MySqlPosition {
    /// The base name of the binlog file. [`MySqlPosition`]s that have different file base names
    /// cannot be compared as they do not refer to the same replication stream.
    binlog_file_base_name: String,
    /// The suffix of the binlog file name. This suffix is a sequence number that is used to order
    /// the binlog files chronologically. We store this suffx as an integer to implement
    /// [`PartialOrd`] for [`MySqlPosition`].
    pub binlog_file_suffix: u32,
    /// The length of the binlog file name suffix in the original file name. Because we convert the
    /// file name from a string to an integer and because the suffix in that filename is padded
    /// with zeroes, we need to store the length of the suffix in order to reproduce the exact
    /// filename in [`MySqlPosition::binlog_file_name()`].
    binlog_file_suffix_length: usize,
    /// The position within the binlog file represented by this type.
    pub position: u64,
}

impl fmt::Display for MySqlPosition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.binlog_file_name(), self.position)
    }
}

impl MySqlPosition {
    /// Converts a raw binlog file name and a position within that file to a [`MySqlPosition`].
    pub fn from_file_name_and_position(file_name: String, position: u64) -> ReadySetResult<Self> {
        let (binlog_file_base_name, binlog_file_suffix) =
            file_name.rsplit_once('.').ok_or_else(|| {
                ReadySetError::ReplicationFailed(format!("Invalid binlog name {file_name}"))
            })?;
        let binlog_file_suffix_length = binlog_file_suffix.len();
        let binlog_file_suffix = binlog_file_suffix.parse::<u32>().map_err(|e| {
            ReadySetError::ReplicationFailed(format!(
                "Invalid binlog filename suffix {file_name}: {e}"
            ))
        })?;

        Ok(Self {
            binlog_file_base_name: binlog_file_base_name.to_owned(),
            binlog_file_suffix,
            binlog_file_suffix_length,
            position,
        })
    }

    /// Returns the raw binlog file name associated with `self`.
    pub fn binlog_file_name(&self) -> impl fmt::Display + Copy + '_ {
        fmt_with(|f| {
            write!(
                f,
                "{0}.{1:02$}",
                self.binlog_file_base_name, self.binlog_file_suffix, self.binlog_file_suffix_length
            )
        })
    }

    /// Computes the byte lag between `self` (ReadySet's position) and `upstream`.
    ///
    /// For same-file positions, returns the difference in byte offsets. For cross-file
    /// positions, sums the remaining bytes in ReadySet's file, all intermediate file
    /// sizes, and the upstream's offset in its file.
    ///
    /// `binlog_file_sizes` maps binlog file suffix to total file size in bytes
    /// (from `SHOW BINARY LOGS`).
    ///
    /// Returns an error if the positions are from different binlog streams or if a
    /// required file size is missing from the map.
    pub fn try_byte_lag_behind(
        &self,
        upstream: &MySqlPosition,
        binlog_file_sizes: &HashMap<u32, u64>,
    ) -> ReadySetResult<u64> {
        if self.binlog_file_base_name != upstream.binlog_file_base_name {
            return Err(ReadySetError::Internal(
                "Cannot compute lag between positions in different binlog streams".into(),
            ));
        }

        match self.binlog_file_suffix.cmp(&upstream.binlog_file_suffix) {
            Ordering::Equal => Ok(upstream.position.saturating_sub(self.position)),
            Ordering::Less => {
                let readyset_file_size = binlog_file_sizes
                    .get(&self.binlog_file_suffix)
                    .ok_or_else(|| {
                        ReadySetError::Internal(format!(
                            "Missing binlog file size for suffix {}",
                            self.binlog_file_suffix
                        ))
                    })?;
                let mut lag = readyset_file_size.saturating_sub(self.position);

                for suffix in (self.binlog_file_suffix + 1)..upstream.binlog_file_suffix {
                    let file_size = binlog_file_sizes.get(&suffix).ok_or_else(|| {
                        ReadySetError::Internal(format!(
                            "Missing binlog file size for suffix {suffix}"
                        ))
                    })?;
                    lag = lag.saturating_add(*file_size);
                }

                lag = lag.saturating_add(upstream.position);
                Ok(lag)
            }
            // Readyset is in a later binlog file than the upstream position we queried.
            // This can happen transiently if a binlog rotate occurs between querying
            // the upstream and reading our position. Report 0 lag.
            Ordering::Greater => Ok(0),
        }
    }

    /// This method compares `self` and `other`, returning an [`Ordering`] if the two items are
    /// comparable and an error otherwise.
    pub fn try_partial_cmp(&self, other: &Self) -> ReadySetResult<Ordering> {
        self.partial_cmp(other).ok_or_else(|| {
            ReadySetError::Internal(
                "Cannot compare MySQL positions in two different binlogs".into(),
            )
        })
    }
}

impl FromStr for MySqlPosition {
    type Err = ReadySetError;

    /// Parse a MySQL binlog position from `"file:position"` format
    /// (e.g. `"mysql-bin.000003:154"`).
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (file, pos) = s.split_once(':').ok_or_else(|| {
            ReadySetError::ReplicationFailed(format!(
                "Invalid MySQL replication position '{s}': expected format 'file:position'"
            ))
        })?;
        let pos: u64 = pos.parse().map_err(|e| {
            ReadySetError::ReplicationFailed(format!(
                "Invalid MySQL replication position '{s}': {e}"
            ))
        })?;
        Self::from_file_name_and_position(file.to_string(), pos)
    }
}

impl PartialOrd for MySqlPosition {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        // Note that we don't compare the suffix lengths here; suffix length should not affect order
        if self.binlog_file_base_name == other.binlog_file_base_name {
            // If the base file names are the same, we need to compare the sequence numbers of the
            // binlog files, and if the sequence numbers are equal, we need to compare the positions
            // within the binlog files. We can accomplish this by invoking `partial_cmp` on the
            // ordered pairs containing (binlog file sequence number, position)
            (self.binlog_file_suffix, self.position)
                .partial_cmp(&(other.binlog_file_suffix, other.position))
        } else {
            // We can't compare positions in different binlogs
            None
        }
    }
}

impl From<&MySqlPosition> for ReplicationOffset {
    fn from(value: &MySqlPosition) -> Self {
        ReplicationOffset::MySql(value.to_owned())
    }
}

impl From<MySqlPosition> for ReplicationOffset {
    fn from(value: MySqlPosition) -> Self {
        ReplicationOffset::MySql(value)
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use super::MySqlPosition;
    use crate::ReplicationOffset;

    #[test]
    fn test_partial_ord() {
        let file1_pos1 = MySqlPosition {
            binlog_file_base_name: "binlog_file".to_owned(),
            binlog_file_suffix: 1,
            binlog_file_suffix_length: 8,
            position: 1,
        };
        let file1_pos2 = MySqlPosition {
            binlog_file_base_name: "binlog_file".to_owned(),
            binlog_file_suffix: 1,
            binlog_file_suffix_length: 8,
            position: 2,
        };
        let file2_pos1 = MySqlPosition {
            binlog_file_base_name: "binlog_file".to_owned(),
            binlog_file_suffix: 2,
            binlog_file_suffix_length: 8,
            position: 1,
        };
        let file1_with_different_suffix_length = MySqlPosition {
            binlog_file_base_name: "binlog_file".to_owned(),
            binlog_file_suffix: 1,
            binlog_file_suffix_length: 2,
            position: 1,
        };
        let other_file = MySqlPosition {
            binlog_file_base_name: "other_file".to_owned(),
            binlog_file_suffix: 1,
            binlog_file_suffix_length: 8,
            position: 1,
        };

        assert!(file1_pos1 < file1_pos2);
        assert!(file1_pos1 < file2_pos1);
        assert!(file1_pos2 < file2_pos1);

        // Padding in the file sequence number should not affect ordering
        assert!(file1_with_different_suffix_length < file1_pos2);

        // Files with different base names cannot be compared
        assert!(other_file.partial_cmp(&file1_pos1).is_none());
    }

    // NOTE: This test checks that the current structure of `ReplicationOffset` is backwards
    // compatible with the last committed structure. If it is failing, the updated structure
    // needs to be modified to be backwards compatible, or additional serde logic is needed to
    // convert between the two forms, and new tests should be added that check that we are
    // incrementally backwards compatible from the original version.
    #[test]
    /// Serialized data from the current version of the struct, generated with:
    /// ```rust
    /// use crate::mysql::MySqlPosition;
    /// let offset = ReplicationOffset::MySql(
    ///     MySqlPosition::from_file_name_and_position("v1.0".to_string(), 42)
    ///         .expect("failed to create MySqlPosition"),
    /// );
    /// eprintln!("{}", serde_json::ser::to_string(&offset).unwrap());
    /// ```
    fn test_mysql_v1() {
        let serialized_data = r#"{"MySql":{"binlog_file_base_name":"v1","binlog_file_suffix":0,"binlog_file_suffix_length":1,"position":42}}"#;
        let deserialized: Result<ReplicationOffset, _> = serde_json::from_str(serialized_data);
        assert!(
            deserialized.is_ok(),
            "ReplicationOffset is not backwards compatible. See note above test."
        );
        assert_eq!(
            deserialized.unwrap(),
            ReplicationOffset::MySql(MySqlPosition {
                binlog_file_base_name: "v1".to_string(),
                binlog_file_suffix: 0,
                binlog_file_suffix_length: 1,
                position: 42,
            })
        );
    }

    fn pos(suffix: u32, position: u64) -> MySqlPosition {
        MySqlPosition {
            binlog_file_base_name: "binlog".to_owned(),
            binlog_file_suffix: suffix,
            binlog_file_suffix_length: 6,
            position,
        }
    }

    #[test]
    fn test_byte_lag_same_file() {
        let readyset = pos(1, 100);
        let upstream = pos(1, 500);
        let sizes = HashMap::new();
        assert_eq!(
            readyset.try_byte_lag_behind(&upstream, &sizes).unwrap(),
            400
        );
    }

    #[test]
    fn test_byte_lag_same_position() {
        let p = pos(1, 100);
        let sizes = HashMap::new();
        assert_eq!(p.try_byte_lag_behind(&p, &sizes).unwrap(), 0);
    }

    #[test]
    fn test_byte_lag_cross_file_adjacent() {
        let readyset = pos(1, 800);
        let upstream = pos(2, 200);
        // file 1 size = 1000, remaining = 1000 - 800 = 200, + upstream 200 = 400
        let sizes = HashMap::from([(1, 1000)]);
        assert_eq!(
            readyset.try_byte_lag_behind(&upstream, &sizes).unwrap(),
            400
        );
    }

    #[test]
    fn test_byte_lag_cross_file_with_intermediates() {
        let readyset = pos(1, 800);
        let upstream = pos(4, 300);
        // file 1: 1000 - 800 = 200 remaining
        // file 2: 2000 (full intermediate)
        // file 3: 1500 (full intermediate)
        // file 4: 300 (upstream position)
        // total: 200 + 2000 + 1500 + 300 = 4000
        let sizes = HashMap::from([(1, 1000), (2, 2000), (3, 1500)]);
        assert_eq!(
            readyset.try_byte_lag_behind(&upstream, &sizes).unwrap(),
            4000
        );
    }

    #[test]
    fn test_byte_lag_readyset_ahead_returns_zero() {
        // Can happen transiently during a binlog rotate race.
        let readyset = pos(3, 100);
        let upstream = pos(1, 500);
        let sizes = HashMap::new();
        assert_eq!(readyset.try_byte_lag_behind(&upstream, &sizes).unwrap(), 0);
    }

    #[test]
    fn test_byte_lag_different_base_names() {
        let readyset = pos(1, 100);
        let upstream = MySqlPosition {
            binlog_file_base_name: "other".to_owned(),
            binlog_file_suffix: 1,
            binlog_file_suffix_length: 6,
            position: 500,
        };
        let sizes = HashMap::new();
        assert!(readyset.try_byte_lag_behind(&upstream, &sizes).is_err());
    }

    #[test]
    fn test_byte_lag_missing_file_size() {
        let readyset = pos(1, 800);
        let upstream = pos(3, 200);
        // Missing file 2 size
        let sizes = HashMap::from([(1, 1000)]);
        assert!(readyset.try_byte_lag_behind(&upstream, &sizes).is_err());
    }

    #[test]
    fn test_mysql_position_roundtrip() {
        let p = pos(3, 154);
        let s = p.to_string();
        let parsed: MySqlPosition = s.parse().unwrap();
        assert_eq!(parsed, p);
    }
}
