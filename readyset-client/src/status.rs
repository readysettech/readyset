//! System status information that can sent to a noria-client.
//!
//! When introducing new fields to the [`ReadySetControllerStatus`] type, be sure to
//! update support for converting the object to strings:
//!   * `ReadySetControllerStatus::try_from(_: Vec<(String, String)>)`
//!   * `Vec<(String, String)>::from(_: ReadySetControllerStatus)`
//!
//! These two conversions are used to convert the [`ReadySetControllerStatus`] structs to a format
//! that can be passed to various SQL clients.
use std::fmt::{self, Display};

use replication_offset::ReplicationOffset;
use serde::{Deserialize, Serialize};

// Consts for variable names.

const STATUS_VARIABLE: &str = "Status";
const MAX_REPLICATION_OFFSET: &str = "Maximum Replication Offset";
const MIN_REPLICATION_OFFSET: &str = "Minimum Replication Offset";

/// ReadySetControllerStatus holds information regarding the controller status of ReadySet, similar
/// to
///
/// [`SHOW STATUS`](https://dev.mysql.com/doc/refman/8.0/en/show-status.html) in MySQL.
///
/// Returned via the /status RPC and SHOW READYSET STATUS.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct ReadySetControllerStatus {
    /// The status of the current leader.
    pub current_status: CurrentStatus,
    /// The current maximum replication offset known by the leader.
    pub max_replication_offset: Option<ReplicationOffset>,
    /// The current minimum replication offset known by the leader.
    pub min_replication_offset: Option<ReplicationOffset>,
}

impl From<ReadySetControllerStatus> for Vec<(String, String)> {
    fn from(status: ReadySetControllerStatus) -> Vec<(String, String)> {
        let mut res = vec![(
            STATUS_VARIABLE.to_string(),
            status.current_status.to_string(),
        )];

        if let Some(replication_offset) = status.max_replication_offset {
            res.push((
                MAX_REPLICATION_OFFSET.to_string(),
                replication_offset.to_string(),
            ))
        }

        if let Some(replication_offset) = status.min_replication_offset {
            res.push((
                MIN_REPLICATION_OFFSET.to_string(),
                replication_offset.to_string(),
            ))
        }

        res
    }
}

/// Whether or not snapshotting has completed.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub enum CurrentStatus {
    /// Snapshotting has not yet completed.
    SnapshotInProgress,
    /// Node is online and ready to serve traffic.
    Online,
    /// Node is in maintenance mode.
    MaintenanceMode,
}

impl Display for CurrentStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            CurrentStatus::SnapshotInProgress => "Snapshot In Progress",
            CurrentStatus::Online => "Online",
            CurrentStatus::MaintenanceMode => "Maintenance Mode",
        };
        write!(f, "{s}")
    }
}

#[derive(Debug, Default)]
pub struct CacheProperties {
    always: bool,
}

impl Display for CacheProperties {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // List the most important properties first.
        let mut properties = vec![];
        if self.always {
            properties.push("always");
        }
        write!(f, "{}", properties.join(", "))
    }
}

impl CacheProperties {
    pub fn set_always(&mut self, always: bool) {
        self.always = always;
    }
}
