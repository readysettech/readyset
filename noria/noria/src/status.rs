//! System status information that can sent to a noria-client.
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

/// ReadySetStatus holds information regarding the status of ReadySet, similar to
/// [`SHOW STATUS`](https://dev.mysql.com/doc/refman/8.0/en/show-status.html) in MySQL.
///
/// Returned via the /status RPC and SHOW READYSET STATUS.
#[derive(Debug, Serialize, Deserialize)]
pub struct ReadySetStatus {
    /// The snapshot status of the current leader.
    pub snapshot_status: SnapshotStatus,
    //TODO: Include binlog position and other fields helpful for evaluating a ReadySet cluster.
}

/// Whether or not snapshotting has completed.
#[derive(Debug, Serialize, Deserialize)]
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
