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
pub const REPLICATION_ACTION: &str = "replication-action";
