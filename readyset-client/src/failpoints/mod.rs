//! Failpoints
//!
//! A collection of failpoints that can be enabled if the `failure_injection` feature is set.
//!
//! See **[Failure Injection](../docs/src/failure_injection.md)** for much more details.

/// All requests to the authority will behave as if the Authority is down
///
/// Currently only supports consul.
pub const AUTHORITY: &str = "authority";
/// Injects an error in crate::worker::readers::listen()
pub const READ_QUERY: &str = "read-query";
/// Imitates traffic being dropped from upstream
pub const UPSTREAM: &str = "upstream";
/// Imitates a failure during the `handle_action` in replication
pub const REPLICATION_HANDLE_ACTION: &str = "replication-handle-action";
/// Imitates a failure during `PostgresWalConnector::next_action` in replication
pub const POSTGRES_REPLICATION_NEXT_ACTION: &str = "postgres-replication-next-action";
/// Imitates a failure right before we begin snapshotting against a Postgres upstream
pub const POSTGRES_SNAPSHOT_START: &str = "postgres-snapshot-start";
/// Imitates a failure right before we invoke `START_REPLICATION` during Postgres replication
pub const POSTGRES_START_REPLICATION: &str = "postgres-start-replication";
/// Imitates a failure when we go to pull the next event from the WAL during Postgres replication
pub const POSTGRES_NEXT_WAL_EVENT: &str = "postgres-next-wal-event";
