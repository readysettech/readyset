#![feature(
    never_type,
    hash_raw_entry,
    drain_filter,
    string_remove_matches,
    iter_intersperse,
    let_chains
)]
pub mod db_util;
pub(crate) mod mysql_connector;
pub(crate) mod noria_adapter;
pub(crate) mod postgres_connector;
pub(crate) mod table_filter;

use std::time::Duration;

pub use noria_adapter::{cleanup, NoriaAdapter};
use readyset_errors::ReadySetError;
pub use replication_offset::mysql::MySqlPosition;
pub use replication_offset::postgres::PostgresPosition;

/// Event notifications sent from the replicator to the controller.
pub enum ReplicatorMessage {
    /// The replicator finished an initial base table snapshot
    SnapshotDone,
    /// The replicator finished startup and entered the main replication loop
    ReplicationStarted,
    /// The replicator encountered an unrecoverable error
    UnrecoverableError(ReadySetError),
    /// The replicator encountered an error that caused it to restart, but the error could be
    /// recoverable. The controller is notified so that it can update status for the user.
    RecoverableError(ReadySetError),
}

/// Provide a simplistic human-readable estimate for how much time remains to complete an operation
pub(crate) fn estimate_remaining_time(elapsed: Duration, progress: f64, total: f64) -> String {
    let estimated_length = elapsed.div_f64(progress).mul_f64(total);
    let remaining = estimated_length.saturating_sub(elapsed);
    let seconds = remaining.as_secs() % 60;
    let minutes = (remaining.as_secs() / 60) % 60;
    let hours = (remaining.as_secs() / 60) / 60;
    format!("{:02}:{:02}:{:02}", hours, minutes, seconds)
}
