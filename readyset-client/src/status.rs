//! System status information that can sent to a noria-client.
//!
//! When introducing new fields to the [`ReadySetStatus`] type, be sure to
//! update support for converting the object to strings:
//!   * `ReadySetStatus::try_from(_: Vec<(String, String)>)`
//!   * `Vec<(String, String)>::from(_: ReadySetStatus)`
//!
//! These two converions are used to convert the [`ReadySetStatus`] structs to a format
//! that can be passed to various SQL clients.
use std::fmt::{self, Display};

use serde::{Deserialize, Serialize};

// Consts for variable names.
const SNAPSHOT_STATUS_VARIABLE: &str = "Snapshot Status";

/// ReadySetStatus holds information regarding the status of ReadySet, similar to
/// [`SHOW STATUS`](https://dev.mysql.com/doc/refman/8.0/en/show-status.html) in MySQL.
///
/// Returned via the /status RPC and SHOW READYSET STATUS.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct ReadySetStatus {
    /// The snapshot status of the current leader.
    pub snapshot_status: SnapshotStatus,
    //TODO: Include binlog position and other fields helpful for evaluating a ReadySet cluster.
}

impl From<ReadySetStatus> for Vec<(String, String)> {
    fn from(status: ReadySetStatus) -> Vec<(String, String)> {
        vec![(
            SNAPSHOT_STATUS_VARIABLE.to_string(),
            status.snapshot_status.to_string(),
        )]
    }
}

/// Whether or not snapshotting has completed.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
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
