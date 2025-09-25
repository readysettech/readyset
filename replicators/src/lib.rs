pub mod db_util;
pub(crate) mod mysql_connector;
pub(crate) mod noria_adapter;
pub(crate) mod postgres_connector;
pub mod table_filter;

use std::time::{Duration, Instant};

use metrics::{gauge, Gauge};
pub use noria_adapter::{cleanup, NoriaAdapter};
use readyset_client::metrics::recorded;
use readyset_client::{TableStatus, TABLE_STATUS_REPORT_INTERVAL};
use readyset_errors::ReadySetError;
use readyset_sql::ast::Relation;
pub use replication_offset::mysql::MySqlPosition;
pub use replication_offset::postgres::PostgresPosition;
use tokio::sync::mpsc::UnboundedSender;
use tracing::{debug, info};

/// Event notification sent to the controller.
#[derive(Debug)]
pub enum ControllerMessage {
    /// The replicator is about to begin a snapshot (initial or re-snapshot)
    SnapshotStarting,
    /// The replicator finished an initial base table snapshot
    SnapshotDone,
    /// The replicator finished startup and entered the main replication loop
    ReplicationStarted,
    /// The replicator encountered an unrecoverable error
    UnrecoverableError(ReadySetError),
    /// The replicator encountered an error that caused it to restart, but the error could be
    /// recoverable. The controller is notified so that it can update status for the user.
    RecoverableError(ReadySetError),
    /// The controller requested that the replicator enter maintenance mode
    EnterMaintenanceMode,
    /// The controller requested that the replicator exit maintenance mode
    ExitMaintenanceMode,
}

/// Event notification sent to the replicator.
pub enum ReplicatorMessage {
    /// Drop the specified table and require a new partial snapshot
    ResnapshotTable { table: Relation },
    /// Add new tables to the replicator filter
    AddTables { tables: Vec<Relation> },
}

/// A handle to the metric we use to track the number of tables currently snapshotting. To use this
/// handle, just keep it in scope while the table is snapshotting; once the handle is dropped, the
/// gauge will be decremented. The type is designed to ensure that we *always* 1) increment the
/// gauge when the handle is created and 2) decrement the gauge when the handle is dropped.
struct TablesSnapshottingGaugeGuard(Gauge);

impl TablesSnapshottingGaugeGuard {
    /// Creates a new handle and increments the gauge by 1. The gauge is automatically decremented
    /// when the handle is dropped.
    fn new() -> Self {
        let gauge = gauge!(recorded::REPLICATOR_TABLES_SNAPSHOTTING);
        gauge.increment(1.0);
        Self(gauge)
    }
}

impl Drop for TablesSnapshottingGaugeGuard {
    fn drop(&mut self) {
        self.0.decrement(1.0);
    }
}

/// Provide a simplistic human-readable estimate for how much time remains to complete an operation
fn estimate_remaining_time(elapsed: Duration, progress: usize, total: usize) -> String {
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
    format!("{hours:02}:{minutes:02}:{seconds:02}")
}

/// Report snapshot progress if the report interval has elapsed.
#[allow(clippy::too_many_arguments)]
pub(crate) fn report_snapshot_progress(
    table: &Relation,
    start: Instant,
    progress: usize,
    total: usize,
    log_interval_s: u16,
    last_log: &mut Instant,
    last_status: &mut Instant,
    table_status_tx: &UnboundedSender<(Relation, TableStatus)>,
) {
    fn percent(progress: usize, total: usize) -> f64 {
        let percent = progress as f64 / total as f64 * 100.0;
        percent.clamp(0.0, 99.99)
    }

    if last_status.elapsed() >= TABLE_STATUS_REPORT_INTERVAL {
        let percent = percent(progress, total);
        if let Err(err) =
            table_status_tx.send((table.clone(), TableStatus::Snapshotting(Some(percent))))
        {
            debug!(
                error = %err,
                table = %table.display_unquoted(),
                "Failed to notify controller of snapshotting progress",
            );
        }
        *last_status = Instant::now();
    }
    if log_interval_s == 0 || last_log.elapsed().as_secs() < log_interval_s as u64 {
        return;
    }
    if total <= 1 {
        info!(rows_replicated = %progress, "Snapshotting progress");
        *last_log = Instant::now();
        return;
    }

    let percent = format!("{:.2}%", percent(progress, total));
    let estimate = estimate_remaining_time(start.elapsed(), progress, total);
    info!(
        rows_replicated = %progress,
        rows_estimated = %total,
        progress = %percent,
        %estimate,
        "Snapshotting progress"
    );
    *last_log = Instant::now();
}
