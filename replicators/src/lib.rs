#![feature(
    never_type,
    hash_raw_entry,
    extract_if,
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

use metrics::Gauge;
use nom_sql::Relation;
pub use noria_adapter::{cleanup, NoriaAdapter};
use readyset_errors::ReadySetError;
pub use replication_offset::mysql::MySqlPosition;
pub use replication_offset::postgres::PostgresPosition;
use tracing::info;

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

/// Event notification sent from the controller to the replicator
pub enum ControllerMessage {
    /// Drop the specified table and require a new partial snapshot
    ResnapshotTable { table: Relation },
}

/// Provide a simplistic human-readable estimate for how much time remains to complete an operation
pub(crate) fn estimate_remaining_time(elapsed: Duration, progress: i64, total: i64) -> String {
    // Total number of rows can be undefined (0 for PG  or 1 for MySQL) or zero
    if total <= 1 {
        return "n/a".to_owned();
    }
    // If we have progress greater than total (inaccurate estimate for total)
    if progress > total {
        return format!("{:02}:{:02}:{:02}", 0, 0, 0);
    }
    // Do normal calculation
    let estimated_length = elapsed.div_f64(progress as f64).mul_f64(total as f64);
    let remaining = estimated_length.saturating_sub(elapsed);
    let seconds = remaining.as_secs() % 60;
    let minutes = (remaining.as_secs() / 60) % 60;
    let hours = (remaining.as_secs() / 60) / 60;
    format!("{:02}:{:02}:{:02}", hours, minutes, seconds)
}
/// Logs snapshot progress. Handles cases when 'total' is undefined or inaccurate.
/// Log message will never show progress greater than 100%, if total number of rows is
/// unknown (equals to 0 or 1), then estimated time and progress will be set to 'n/a'
/// To provide feedback to a user, number of replicated rows and estimated total rows are logged
/// If total number of rows is unknown progress %% is set to 0.0 (in metric).
pub(crate) fn log_snapshot_progress(elapsed: Duration, cnt: i64, total: i64, metric: &Gauge) {
    let estimate = estimate_remaining_time(elapsed, cnt, total);
    let mut progress_percent: f64;
    progress_percent = if total > 1 {
        progress_percent = (cnt as f64 / total as f64) * 100.;
        if progress_percent > 100.0 {
            99.9
        } else {
            progress_percent
        }
    } else {
        0.
    };
    let progress = if total > 1 {
        format!("{:.2}%", progress_percent)
    } else {
        "n/a".to_owned()
    };
    let rows_total_est = if total > cnt { total } else { cnt };
    info!(rows_replicated = %cnt, rows_total_est = %rows_total_est, %progress, %estimate, "Snapshotting progress");
    metric.set(progress_percent);
}
