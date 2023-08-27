use std::cmp::Ordering;
use std::fmt;

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
    binlog_file_suffix: u32,
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
                ReadySetError::ReplicationFailed(format!("Invalid binlog name {}", file_name))
            })?;
        let binlog_file_suffix_length = binlog_file_suffix.len();
        let binlog_file_suffix = binlog_file_suffix.parse::<u32>().map_err(|e| {
            ReadySetError::ReplicationFailed(format!(
                "Invalid binlog filename suffix {}: {e}",
                file_name
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
    use super::MySqlPosition;

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
}
