/// Histogram: The amount of time in microseconds a snapshot takes to be performed.
pub const REPLICATOR_SNAPSHOT_DURATION: &str = "readyset_replicator.snapshot_duration_us";

/// How the replicator handled a snapshot.
pub enum SnapshotStatusTag {
    /// A snapshot was started by the replicator.
    Started,
    /// A snapshot succeeded at the replicator.
    Successful,
    /// A snapshot failed at the replicator.
    Failed,
}

impl SnapshotStatusTag {
    /// Returns the enum tag as a &str for use in metrics labels.
    pub fn value(&self) -> &str {
        match self {
            SnapshotStatusTag::Started => "started",
            SnapshotStatusTag::Successful => "successful",
            SnapshotStatusTag::Failed => "failed",
        }
    }
}

/// Counter: Number of snapshots started at this node. Incremented by 1 when a snapshot begins.
///
/// | Tag | Description |
/// | --- | ----------- |
/// | status | SnapshotStatusTag |
pub const REPLICATOR_SNAPSHOT_STATUS: &str = "readyset_replicator.snapshot_status";

/// Gauge: The number of tables currently snapshotting
pub const REPLICATOR_TABLES_SNAPSHOTTING: &str = "readyset_replicator.tables_snapshotting";

/// Counter: Number of failures encountered when following the replication log.
pub const REPLICATOR_FAILURE: &str = "readyset_replicator.update_failure";

/// Counter: Number of tables that failed to replicate and are ignored
pub const TABLE_FAILED_TO_REPLICATE: &str = "readyset_replicator.table_failed";

/// Counter: Number of replication actions performed successfully.
pub const REPLICATOR_SUCCESS: &str = "readyset_replicator.update_success";

/// Histogram: Number of table operations per perform_all RPC call.
pub const REPLICATOR_BATCH_SIZE: &str = "readyset_replicator.batch_size";

/// Counter: Total number of perform_all RPC calls made by the replicator.
pub const REPLICATOR_PERFORM_ALL_CALLS: &str = "readyset_replicator.perform_all_calls";

/// Histogram: Duration in microseconds of each perform_all RPC call.
pub const REPLICATOR_PERFORM_ALL_DURATION: &str = "readyset_replicator.perform_all_duration_us";

/// Histogram: Number of committed transactions coalesced per group commit flush. A value of 1 means
/// no coalescing occurred. Higher values indicate effective group commit batching.
pub const REPLICATOR_GROUP_COMMIT_TXNS: &str = "readyset_replicator.group_commit_txns";

/// Histogram: Duration in microseconds of the group commit wait window. Measures elapsed time from
/// the first commit (leader) to the flush.
pub const REPLICATOR_GROUP_COMMIT_DURATION: &str = "readyset_replicator.group_commit_duration_us";

/// Gauge: Replication lag between upstream and Readyset. Unit depends on mode: bytes for
/// postgres/mysql_file, transactions for mysql_gtid.
///
/// | Tag | Description |
/// | --- | ----------- |
/// | mode | postgres, mysql_file, or mysql_gtid |
/// | kind | consume (stream position) or persist (min persisted offset) |
pub const REPLICATOR_REPLICATION_LAG: &str = "readyset_replicator.replication_lag";

/// Counter: Number of failed replication lag poll attempts.
pub const REPLICATOR_LAG_POLL_FAILURE: &str = "readyset_replicator.lag_poll_failure";

/// Gauge: Time-based replication staleness in seconds, measured via pt-heartbeat. Only emitted when
/// `--replication-heartbeat` is enabled.
///
/// | Tag | Description |
/// | --- | ----------- |
/// | mode | postgres, mysql_file, or mysql_gtid |
pub const REPLICATOR_REPLICATION_STALENESS: &str =
    "readyset_replicator.replication_staleness_seconds";
