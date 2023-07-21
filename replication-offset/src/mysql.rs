use std::fmt;

use readyset_errors::ReadySetError;

use crate::ReplicationOffset;

/// Represents a position within in the MySQL binlog
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct MySqlPosition {
    pub binlog_file: String,
    pub position: u32,
}

impl fmt::Display for MySqlPosition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.binlog_file, self.position)
    }
}

impl PartialOrd for MySqlPosition {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        // The log files are sequentially numbered using a .NNNNNN suffix. The index file has a
        // suffix of .index. All files share a common basename. The default binary log
        // file-naming basename is "HOSTNAME-bin".
        if self.binlog_file == other.binlog_file {
            return self.position.partial_cmp(&other.position);
        }

        // This implementation assumes proper binlog filename format, and will return None on an
        // invalid format.
        let (basename, suffix) = self.binlog_file.rsplit_once('.')?;
        let (other_basename, other_suffix) = other.binlog_file.rsplit_once('.')?;

        // If basename is different than we are talking about completely different logs
        if basename != other_basename {
            return None;
        }

        // Otherwise we compare the suffix, which if valid must be a valid number
        // probably u32 would suffice
        let suffix = suffix.parse::<u64>().ok()?;
        let other_suffix = other_suffix.parse::<u64>().ok()?;

        suffix.partial_cmp(&other_suffix)
    }
}

impl TryFrom<&MySqlPosition> for ReplicationOffset {
    type Error = ReadySetError;

    /// `ReplicationOffset` is a filename and a u128 offset
    /// We use the binlog basefile name as the filename, and we use the binlog suffix len for
    /// the top 5 bits, which can be as big as 31 digits in theory, but we only allow up to 17
    /// decimal digits, which is more than enough for the binlog spec. This is required to be
    /// able to properly format the integer back to string, including any leading zeroes.
    /// The following 59 bits are used for the numerical value of the suffix, finally the last
    /// 64 bits of the offset are the actual binlog offset.
    fn try_from(value: &MySqlPosition) -> Result<Self, Self::Error> {
        let (basename, suffix) = value.binlog_file.rsplit_once('.').ok_or_else(|| {
            ReadySetError::ReplicationFailed(format!("Invalid binlog name {}", value.binlog_file))
        })?;

        let suffix_len = suffix.len() as u128;

        if suffix_len > 17 {
            // 17 digit decimal number is the most we can fit into 59 bits
            return Err(ReadySetError::ReplicationFailed(format!(
                "Invalid binlog suffix {}",
                value.binlog_file
            )));
        }

        let suffix = suffix.parse::<u128>().map_err(|_| {
            ReadySetError::ReplicationFailed(format!("Invalid binlog suffix {}", value.binlog_file))
        })?;

        Ok(ReplicationOffset {
            offset: (suffix_len << 123) + (suffix << 64) + (value.position as u128),
            replication_log_name: basename.to_string(),
        })
    }
}

impl TryFrom<MySqlPosition> for ReplicationOffset {
    type Error = ReadySetError;

    fn try_from(value: MySqlPosition) -> Result<Self, Self::Error> {
        (&value).try_into()
    }
}

impl From<&ReplicationOffset> for MySqlPosition {
    fn from(val: &ReplicationOffset) -> Self {
        let suffix_len = (val.offset >> 123) as usize;
        let suffix = (val.offset >> 64) as u32;
        let position = val.offset as u32;

        // To get the binlog filename back we use `replication_log_name` as the basename and append
        // the suffix which is encoded in bits 64:123 of `offset`, and format it as a zero
        // padded decimal integer with `suffix_len` characters (encoded in the top 5 bits of
        // the offset)
        MySqlPosition {
            binlog_file: format!("{0}.{1:02$}", val.replication_log_name, suffix, suffix_len),
            position,
        }
    }
}

impl From<ReplicationOffset> for MySqlPosition {
    fn from(val: ReplicationOffset) -> Self {
        (&val).into()
    }
}
