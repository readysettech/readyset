//! System status information that can sent to a noria-client.
//!
//! When introducing new fields to the [`ReadySetStatus`] type, be sure to
//! update support for converting the object to strings:
//!   * `ReadySetStatus::try_from(_: Vec<(String, String)>)`
//!   * `Vec<(String, String)>::from(_: ReadySetStatus)`
//!
//! These two converions are used to convert the [`ReadySetStatus`] structs to a format
//! that can be passed to various SQL clients.
use mysql_common::row::Row;
use noria_errors::{internal, ReadySetError};
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use std::fmt::{self, Display};

// Consts for variable names.
const SNAPSHOT_STATUS_VARIABLE: &str = "Snapshot Status";

/// ReadySetStatus holds information regarding the status of ReadySet, similar to
/// [`SHOW STATUS`](https://dev.mysql.com/doc/refman/8.0/en/show-status.html) in MySQL.
///
/// Returned via the /status RPC and SHOW READYSET STATUS.
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct ReadySetStatus {
    /// The snapshot status of the current leader.
    pub snapshot_status: SnapshotStatus,
    //TODO: Include binlog position and other fields helpful for evaluating a ReadySet cluster.
}

impl TryFrom<Vec<(String, String)>> for ReadySetStatus {
    type Error = ReadySetError;
    fn try_from(vars: Vec<(String, String)>) -> Result<Self, Self::Error> {
        let mut res = ReadySetStatus {
            snapshot_status: SnapshotStatus::InProgress,
        };
        for v in vars {
            match (v.0.as_str(), v.1) {
                (SNAPSHOT_STATUS_VARIABLE, v) => res.snapshot_status = SnapshotStatus::try_from(v)?,
                (_, _) => {
                    internal!("Invalid ReadySetStatus variable")
                }
            }
        }

        Ok(res)
    }
}

impl From<ReadySetStatus> for Vec<(String, String)> {
    fn from(status: ReadySetStatus) -> Vec<(String, String)> {
        vec![(
            SNAPSHOT_STATUS_VARIABLE.to_string(),
            status.snapshot_status.to_string(),
        )]
    }
}

impl TryFrom<Vec<Row>> for ReadySetStatus {
    type Error = ReadySetError;
    /// Convinience wrapper useful for converting a ReadySetStatus returned via a MySQL
    /// query, as a Vec<Row>.
    fn try_from(vars: Vec<Row>) -> Result<Self, Self::Error> {
        let v = vars
            .into_iter()
            .map(|row| {
                if let (Some(l), Some(v)) = (row.get(0), row.get(1)) {
                    Ok((l, v))
                } else {
                    Err(ReadySetError::Internal(
                        "Invalid row structure for ReadySetStatus".to_string(),
                    ))
                }
            })
            .collect::<Result<Vec<(String, String)>, ReadySetError>>()?;

        ReadySetStatus::try_from(v)
    }
}

/// Whether or not snapshotting has completed.
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub enum SnapshotStatus {
    /// Snapshotting has not yet completed.
    InProgress,
    /// Snapshotting has completed.
    Completed,
}

impl Display for SnapshotStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            SnapshotStatus::InProgress => "In Progress",
            SnapshotStatus::Completed => "Completed",
        };
        write!(f, "{}", s)
    }
}

impl TryFrom<String> for SnapshotStatus {
    type Error = ReadySetError;
    fn try_from(val: String) -> Result<Self, Self::Error> {
        Ok(match val.as_str() {
            "In Progress" => SnapshotStatus::InProgress,
            "Completed" => SnapshotStatus::Completed,
            _ => internal!("Invalid snapshot status"),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn readyset_status_round_trip() {
        let original = ReadySetStatus {
            snapshot_status: SnapshotStatus::Completed,
        };
        let intermediate: Vec<(String, String)> = original.clone().into();
        let round_tripped = ReadySetStatus::try_from(intermediate).unwrap();

        assert_eq!(original, round_tripped);
    }
}
