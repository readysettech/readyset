//! System status information that can sent to a noria-client.
//!
//! When introducing new fields to the [`ReadySetControllerStatus`] type, be sure to
//! update support for converting the object to strings:
//!   * `ReadySetControllerStatus::try_from(_: Vec<(String, String)>)`
//!   * `Vec<(String, String)>::from(_: ReadySetControllerStatus)`
//!
//! These two conversions are used to convert the [`ReadySetControllerStatus`] structs to a format
//! that can be passed to various SQL clients.
use std::{
    borrow::Cow,
    fmt::{self, Display},
};

use serde::{Deserialize, Serialize};

use readyset_sql::ast::CacheType;
use replication_offset::ReplicationOffset;

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

#[derive(Debug)]
pub struct CacheProperties {
    cache_type: CacheType,
    ttl_ms: Option<u64>,
    refresh_ms: Option<u64>,
    coalesce_ms: Option<u64>,
    always: bool,
}

impl Display for CacheProperties {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // List the most important properties first.
        let mut properties = vec![Cow::Owned(format!("{}", self.cache_type))];
        if self.always {
            properties.push(Cow::Borrowed("always"));
        }
        if let Some(ttl_ms) = self.ttl_ms {
            properties.push(Cow::Owned(format!("ttl {ttl_ms} ms")));
        }
        if let Some(refresh_ms) = self.refresh_ms {
            properties.push(Cow::Owned(format!("refresh {refresh_ms} ms")));
        }
        if let Some(coalesce_ms) = self.coalesce_ms {
            properties.push(Cow::Owned(format!("coalesce {coalesce_ms} ms")));
        }
        write!(f, "{}", properties.join(", "))
    }
}

impl CacheProperties {
    pub fn new(cache_type: CacheType) -> Self {
        Self {
            cache_type,
            ttl_ms: None,
            refresh_ms: None,
            coalesce_ms: None,
            always: false,
        }
    }

    pub fn set_always(&mut self, always: bool) {
        self.always = always;
    }

    pub fn set_ttl_ms(&mut self, ttl_ms: u64) {
        self.ttl_ms = Some(ttl_ms);
    }

    pub fn set_refresh_ms(&mut self, refresh_ms: u64) {
        self.refresh_ms = Some(refresh_ms);
    }

    pub fn set_coalesce_ms(&mut self, coalesce_ms: u64) {
        self.coalesce_ms = Some(coalesce_ms);
    }
}
