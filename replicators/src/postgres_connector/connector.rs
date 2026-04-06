use std::collections::HashMap;
use std::error::Error;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use database_utils::UpstreamConfig;
#[cfg(feature = "failure_injection")]
use failpoint_macros::set_failpoint;
use futures::FutureExt;
use metrics::histogram;
use pgsql::SimpleQueryMessage;
use postgres_native_tls::MakeTlsConnector;
use postgres_protocol::escape::escape_literal;
use readyset_client::metrics::recorded;
use readyset_client::{PersistencePoint, ReadySetHandle, TableOperation};
use readyset_errors::{
    invariant, invariant_eq, set_failpoint_return_err, ReadySetError, ReadySetResult,
};
use readyset_sql::ast::{Relation, SqlIdentifier};
use readyset_sql_parsing::{ParsingConfig, ParsingPreset};
#[cfg(feature = "failure_injection")]
use readyset_util::failpoints;
use readyset_util::select;
use replication_offset::postgres::{CommitLsn, Lsn, PostgresPosition};
use replication_offset::ReplicationOffset;
use tokio_postgres as pgsql;
use tokio_postgres::error::{DbError, SqlState};
use tracing::{debug, error, info, trace, warn};

use super::ddl_replication::setup_ddl_replication;
use super::wal_reader::{WalEvent, WalReader};
use super::PUBLICATION_NAME;
use crate::db_util::error_is_slot_not_found;
use crate::noria_adapter::{Connector, ReplicationAction};
use crate::table_filter::TableFilter;

/// Per-connector group commit state. Encapsulates the accumulated pending
/// row actions and the window-tracking fields that decide when to flush.
#[derive(Debug)]
struct GroupCommitState {
    /// Maximum number of row events to accumulate in memory before
    /// flushing a batch. Bounds memory usage for large transactions.
    replication_batch_size: usize,
    /// Accumulated row operations for the current batch being built,
    /// keyed by table relation so operations for the same table coalesce
    /// into a single `ReplicationAction::TableAction` at flush time.
    pending_actions: HashMap<Relation, Vec<TableOperation>>,
    /// Number of row events accumulated in `pending_actions`.
    pending_row_count: usize,
    /// Maximum transactions per group commit batch.
    max_trx: usize,
    /// Duration budget for the group commit window.
    wait: Duration,
    /// Deadline for current group commit window. Set when the first
    /// transaction in a group commits (the "leader").
    deadline: Option<Instant>,
    /// Instant at which the leader transaction of the current group
    /// committed. Stored separately from `deadline` so the metric path
    /// reads it directly instead of computing `deadline - wait`, which
    /// would panic on `Instant - Duration` underflow.
    leader: Option<Instant>,
    /// Number of committed transactions in the current group.
    trx_count: usize,
}

impl GroupCommitState {
    fn new(batch_size: usize, max_trx: usize, wait: Duration) -> Self {
        Self {
            replication_batch_size: batch_size,
            pending_actions: HashMap::new(),
            pending_row_count: 0,
            max_trx,
            wait,
            deadline: None,
            leader: None,
            trx_count: 0,
        }
    }

    /// Accumulate a row operation into the pending batch for the given
    /// table. Row operations for the same table are appended to the same
    /// per-table `Vec`, preserving the order in which they arrived.
    ///
    /// Returns `true` if the batch has reached `replication_batch_size`
    /// and should be flushed.
    fn accumulate_row(
        &mut self,
        relation: Relation,
        ops: impl IntoIterator<Item = TableOperation>,
    ) -> bool {
        let entry = self.pending_actions.entry(relation).or_default();
        let before = entry.len();
        entry.extend(ops);
        self.pending_row_count += entry.len() - before;
        self.pending_row_count >= self.replication_batch_size
    }

    /// Drain the pending action accumulator and return the batch,
    /// constructing one `ReplicationAction::TableAction` per table.
    /// Also emits group commit metrics and resets the window state.
    fn flush(&mut self) -> Vec<ReplicationAction> {
        self.emit_metrics();
        self.pending_row_count = 0;
        self.reset_window();
        self.pending_actions
            .drain()
            .map(|(table, actions)| ReplicationAction::TableAction { table, actions })
            .collect()
    }

    /// Advance group commit state for a newly committed transaction.
    ///
    /// Starts the group commit deadline on the first commit (the
    /// "leader") and increments the transaction counter. Only takes
    /// effect when there are pending row actions to flush.
    fn advance(&mut self) {
        if self.pending_row_count > 0 {
            if self.deadline.is_none() {
                let now = Instant::now();
                self.leader = Some(now);
                self.deadline = Some(now + self.wait);
            }
            self.trx_count += 1;
        }
    }

    /// Check if the group commit batch should be flushed now.
    fn should_flush(&self) -> bool {
        self.wait.is_zero()
            || self.trx_count >= self.max_trx
            || self.pending_row_count >= self.replication_batch_size
    }

    /// Reset the group commit window tracking fields after a flush.
    fn reset_window(&mut self) {
        self.deadline = None;
        self.leader = None;
        self.trx_count = 0;
    }

    /// Emit group commit metrics before flushing.
    fn emit_metrics(&self) {
        if self.trx_count > 0 {
            histogram!(recorded::REPLICATOR_GROUP_COMMIT_TXNS).record(self.trx_count as f64);
            if let Some(leader) = self.leader {
                histogram!(recorded::REPLICATOR_GROUP_COMMIT_DURATION)
                    .record(leader.elapsed().as_micros() as f64);
            }
        }
    }
}

/// A connector that connects to a PostgreSQL server and starts reading WAL from the "noria"
/// replication slot with the "noria" publication.
///
/// The server must be configured with `wal_level` set to `logical`.
///
/// The connector user must have the following permissions:
/// `REPLICATION` - to be able to create a replication slot.
/// `SELECT` - In order to be able to copy the initial table data, the role used for the replication
/// connection must have the `SELECT` privilege on the published tables (or be a superuser).
/// `CREATE` - To create a publication, the user must have the CREATE privilege in the database. To
/// add tables to a publication, the user must have ownership rights on the table. To create a
/// publication that publishes all tables automatically, the user must be a superuser.
pub struct PostgresWalConnector {
    /// This is the underlying (regular) PostgreSQL client
    client: pgsql::Client,
    /// A tokio task that handles the connection, required by `tokio_postgres` to operate
    connection_handle: tokio::task::JoinHandle<Result<(), pgsql::Error>>,
    /// Reader is a decoder for binlog events
    reader: Option<WalReader>,
    /// Stores an event or error that was read but not handled
    peek: Option<ReadySetResult<WalEvent>>,
    /// If we just want to continue reading the log from a previous point
    next_position: Option<Lsn>,
    /// The replication slot if was created for this connector
    pub(crate) replication_slot: Option<CreatedSlot>,
    /// Whether to log statements received by the connector
    enable_statement_logging: bool,
    /// The time at which we last sent a status update to the upstream database reporting our
    /// current WAL position
    time_last_position_reported: Instant,
    /// Whether we are currently in the middle of processing a transaction
    in_transaction: bool,
    /// A handle to the controller
    controller: ReadySetHandle,
    /// The interval on which we should send status updates to the upstream Postgres instance
    status_update_interval: Duration,
    /// Whether or not we just processed a table error and need to allow mismatched lsns for the
    /// next commit
    had_table_error: bool,
    /// Table Filter
    table_filter: TableFilter,
    /// Parsing mode that determines which parser(s) to use and how to handle conflicts
    parsing_config: ParsingConfig,
    /// Group commit state: accumulated pending row actions plus the
    /// window-tracking fields that decide when to flush.
    group_commit: GroupCommitState,
    /// Position of the last fully-processed COMMIT. Used when flushing
    /// pending row actions from a path where `cur_pos` has been clobbered
    /// to `(T_next.final_lsn, 0)` by the BEGIN of a subsequent transaction
    /// (specifically the `WalEvent::Truncate` and `WalEvent::DdlEvent`
    /// flush-pending-first branches), so the flushed rows carry the
    /// position of the last real COMMIT instead of the sentinel.
    last_committed_pos: Option<PostgresPosition>,
    /// Saved `cur_pos` to restore when the next `next_action` call pops
    /// `peek`. Set whenever we stash an event in `peek` *after* the
    /// in-progress transaction's BEGIN has already clobbered `cur_pos`,
    /// so the next iteration's `cur_pos` reflects the new transaction's
    /// `commit_lsn` rather than the (stale) returned flush position.
    peek_cur_pos: Option<PostgresPosition>,
}

/// The decoded response to `IDENTIFY_SYSTEM`
#[derive(Debug)]
#[allow(dead_code)]
pub(crate) struct ServerIdentity {
    /// The unique system identifier identifying the cluster. This can be used to check that the
    /// base backup used to initialize the standby came from the same cluster.
    pub(crate) id: String,
    /// Current timeline ID. Also useful to check that the standby is consistent with the master.
    pub(crate) timeline: i8,
    /// Current WAL flush location. Useful to get a known location in the write-ahead log where
    /// streaming can start.
    pub(crate) xlogpos: Lsn,
    /// Database connected to or null.
    pub(crate) dbname: String,
}

/// The decoded response to `CREATE_REPLICATION_SLOT`
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct CreatedSlot {
    /// The name of the newly-created replication slot
    pub(crate) slot_name: String,
    /// The WAL location at which the slot became consistent. This is the earliest location
    /// from which streaming can start on this replication slot.
    pub(crate) consistent_point: CommitLsn,
    /// The identifier of the snapshot exported by the command. The snapshot is valid until a
    /// new command is executed on this connection or the replication connection is closed.
    /// Null if the created slot is physical.
    pub(crate) snapshot_name: String,
    /// The name of the output plugin used by the newly-created replication slot.
    /// Null if the created slot is physical.
    pub(crate) output_plugin: String,
}

impl PostgresWalConnector {
    /// Connects to postgres and if needed creates a new replication slot for itself with an
    /// exported snapshot.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn connect<S: AsRef<str>>(
        mut pg_config: pgsql::Config,
        dbname: S,
        config: &UpstreamConfig,
        next_position: Option<Lsn>,
        tls_connector: MakeTlsConnector,
        repl_slot_name: &str,
        enable_statement_logging: bool,
        full_resnapshot: bool,
        controller: ReadySetHandle,
        table_filter: TableFilter,
        parsing_preset: ParsingPreset,
    ) -> ReadySetResult<Self> {
        if !config.disable_setup_ddl_replication {
            setup_ddl_replication(pg_config.clone(), tls_connector.clone()).await?;
        }
        pg_config.dbname(dbname.as_ref()).set_replication_database();

        let (client, connection) = pg_config.connect(tls_connector).await.map_err(|e| {
            if let Some(e) = e.source().and_then(|e| e.downcast_ref::<DbError>()) {
                if e.code() == &SqlState::INVALID_AUTHORIZATION_SPECIFICATION {
                    // Can be caused by rds.logical_replication = 0 on RDS.
                    return ReadySetError::ReplicationFailed(format!(
                        "Failed to connect (remember to enable logical replication): {e}"
                    ));
                }
            }
            ReadySetError::ReplicationFailed(format!("Failed to connect: {e}"))
        })?;
        let connection_handle = tokio::spawn(connection);
        let status_update_interval = Duration::from_secs(config.status_update_interval_secs as u64);

        let mut connector = PostgresWalConnector {
            client,
            connection_handle,
            reader: None,
            peek: None,
            next_position,
            replication_slot: None,
            enable_statement_logging,
            // We initialize `time_last_position_reported` to be more than
            // `Self.status_update_interval` seconds ago to ensure that we report our position in
            // the logs the next time we have an opportunity to
            time_last_position_reported: Instant::now()
                - status_update_interval
                - Duration::from_secs(1),
            // We'll never start replicating in the middle of a transaction, since Postgres's
            // logical replication protocol only streams whole transactions to us at a time. This
            // means we know that when we first initiate a replication connection to the upstream
            // database, we'll always start replicating outside of a transaction
            in_transaction: false,
            controller,
            status_update_interval,
            had_table_error: false,
            table_filter,
            parsing_config: parsing_preset.into_config().rate_limit_logging(false),
            group_commit: GroupCommitState::new(
                config.replication_batch_size,
                config.group_commit_max_trx,
                Duration::from_micros(config.group_commit_wait_us),
            ),
            last_committed_pos: None,
            peek_cur_pos: None,
        };

        if full_resnapshot || next_position.is_none() {
            // If we don't have a consistent replication offset to start replicating from or if we
            // need to perform a full resnapshot, drop and recreate our replication slot.
            //
            // Note that later on, this means we'll need to make sure we resnapshot *all* tables!
            connector
                .create_publication_and_slot(repl_slot_name, config)
                .await?;
        }

        Ok(connector)
    }

    async fn create_publication_and_slot(
        &mut self,
        repl_slot_name: &str,
        config: &UpstreamConfig,
    ) -> ReadySetResult<()> {
        let system = self.identify_system().await?;
        debug!(
            id = %system.id,
            timeline = %system.timeline,
            xlogpos = %system.xlogpos,
            dbname = ?system.dbname
        );

        if !config.disable_create_publication {
            match self.create_publication(PUBLICATION_NAME).await {
                Ok(()) => {
                    // Created a new publication, everything is good
                }
                Err(err)
                    if err.to_string().contains("publication")
                        && err.to_string().contains("already exists") =>
                {
                    // This is an existing publication we are going to use
                }
                Err(err) if err.to_string().contains("permission denied") => {
                    error!("Insufficient permissions to create publication FOR ALL TABLES");
                }
                Err(err) => return Err(err),
            }
        }

        // Drop the existing slot if any
        self.drop_replication_slot(repl_slot_name).await?;

        match self.create_replication_slot(repl_slot_name, false).await {
            Ok(slot) => self.replication_slot = Some(slot), /* Created a new slot, */
            // everything is good
            Err(err)
                if err.to_string().contains("replication slot")
                    && err.to_string().contains("already exists") =>
            {
                // This is an existing slot we will be using
            }
            Err(err) => return Err(err),
        };

        Ok(())
    }

    /// Waits and returns the next WAL event, while monitoring the connection
    /// handle for errors.
    async fn next_event(&mut self) -> ReadySetResult<WalEvent> {
        set_failpoint_return_err!(failpoints::POSTGRES_NEXT_WAL_EVENT);

        let PostgresWalConnector {
            reader,
            connection_handle,
            ..
        } = self;

        if let Some(reader) = reader.as_mut() {
            select! {
                ev = reader.next_event().fuse() => ev,
                err = connection_handle.fuse() => match err {
                    Err(e) => Err(ReadySetError::UpstreamConnectionLost(e.to_string())),
                    Ok(Ok(_)) => unreachable!(), // Unreachable because it runs in infinite loop unless errors
                    Ok(Err(err)) => Err(err.into()),
                }
            }
        } else {
            Err(ReadySetError::ReplicationFailed("Not started".to_string()))
        }
    }

    /// Waits for the next WAL event with a timeout. Returns `Ok(None)` if the
    /// timeout expires before an event arrives.
    async fn next_event_with_timeout(
        &mut self,
        read_timeout: Duration,
    ) -> ReadySetResult<Option<WalEvent>> {
        match tokio::time::timeout(read_timeout, self.next_event()).await {
            Ok(result) => result.map(Some),
            Err(_) => Ok(None),
        }
    }

    /// Thin wrapper around [`GroupCommitState::accumulate_row`] that
    /// assembles the `Relation` from the WAL event's schema/table fields.
    fn accumulate_row(
        &mut self,
        schema: SqlIdentifier,
        table: SqlIdentifier,
        ops: impl IntoIterator<Item = TableOperation>,
    ) -> bool {
        let relation = Relation {
            schema: Some(schema),
            name: table,
        };
        self.group_commit.accumulate_row(relation, ops)
    }

    /// Drain pending row actions and append a
    /// [`ReplicationAction::LogPosition`] marker so the caller can
    /// persist the replication offset alongside the flushed rows.
    fn flush_pending_actions_with_log_position(&mut self) -> Vec<ReplicationAction> {
        let mut actions = self.group_commit.flush();
        actions.push(ReplicationAction::LogPosition);
        actions
    }

    /// Requests the server to identify itself. Server replies with a result set of a single row,
    /// containing four fields:
    ///
    /// * `systemid` (text) - The unique system identifier identifying the cluster. This can be used
    ///   to check that the base backup used to initialize the standby came from the same cluster.
    /// * `timeline` (int4) - Current timeline ID. Also useful to check that the standby is
    ///   consistent with the master.
    /// * `xlogpos` (text) - Current WAL flush location. Useful to get a known location in the
    ///   write-ahead log where streaming can start.
    /// * dbname (text) - Database connected to or null.
    async fn identify_system(&mut self) -> ReadySetResult<ServerIdentity> {
        let [id, timeline_str, xlogpos_str, dbname] =
            self.one_row_query::<4>("IDENTIFY_SYSTEM").await?;

        let timeline: i8 = timeline_str.parse().map_err(|_| {
            ReadySetError::ReplicationFailed("Unable to parse identify system".into())
        })?;
        let xlogpos = xlogpos_str.parse()?;

        Ok(ServerIdentity {
            id,
            timeline,
            xlogpos,
            dbname,
        })
    }

    /// Creates a new `PUBLICATION name FOR ALL TABLES`, to be able to receive WAL on that slot.
    /// The user must have superuser privileges for that to work.
    async fn create_publication(&mut self, name: &str) -> ReadySetResult<()> {
        let query = format!("CREATE PUBLICATION {name} FOR ALL TABLES");
        self.simple_query(&query).await.map_err(|e| {
            ReadySetError::ReplicationFailed(format!("Failed to create publication: {e}"))
        })?;
        Ok(())
    }

    /// Creates a new replication slot on the primary.
    /// The command format for PostgreSQL is as follows:
    ///
    /// `CREATE_REPLICATION_SLOT slot_name [ TEMPORARY ] { PHYSICAL [ RESERVE_WAL ] | LOGICAL
    /// output_plugin [ EXPORT_SNAPSHOT | NOEXPORT_SNAPSHOT | USE_SNAPSHOT ] }`
    ///
    /// We use the following options:
    /// `TEMPORARY` - we only use it for a resnapshotting slot, otherwise we want the slot to
    /// persist when connection to primary is down
    /// `LOGICAL` - we are using logical streaming replication
    /// `pgoutput` - the plugin to use for logical decoding, always available from PG > 10
    /// `EXPORT_SNAPSHOT` -  we want the operation to export a snapshot that can be then used for
    /// snapshotting
    pub(crate) async fn create_replication_slot(
        &mut self,
        name: &str,
        temporary: bool,
    ) -> ReadySetResult<CreatedSlot> {
        info!(slot = name, temporary, "Creating replication slot");
        let query = format!(
            "CREATE_REPLICATION_SLOT {name} {} LOGICAL pgoutput EXPORT_SNAPSHOT",
            if temporary { "TEMPORARY" } else { "" }
        );

        let [slot_name, consistent_point_str, snapshot_name, output_plugin] =
            self.one_row_query::<4>(&query).await.map_err(|e| {
                ReadySetError::ReplicationFailed(format!("Failed to create replication slot: {e}"))
            })?;
        let consistent_point = consistent_point_str.parse()?;

        debug!(
            slot_name,
            %consistent_point, snapshot_name, output_plugin, "Created replication slot"
        );

        Ok(CreatedSlot {
            slot_name,
            consistent_point,
            snapshot_name,
            output_plugin,
        })
    }

    /// Begin replication on the `slot` and `publication`. The `publication` must be present on
    /// the server, and can be created using: `CREATE PUBLICATION publication FOR ALL TABLES;`
    pub(crate) async fn start_replication(
        &mut self,
        slot: &str,
        publication: &str,
        version: u32,
    ) -> ReadySetResult<()> {
        set_failpoint_return_err!(failpoints::POSTGRES_START_REPLICATION);

        // Load the confirmed flush LSN for this replication slot so we can log it before starting
        // replication, and get the WAL status so we can make sure our replication slot is healthy
        let [confirmed_flush_lsn, wal_status] = self
                .one_row_query::<2>(&format!(
                    "SELECT confirmed_flush_lsn, wal_status FROM pg_replication_slots WHERE slot_name = {}",
                    escape_literal(slot),
                ))
                .await?;

        tracing::info!(?confirmed_flush_lsn, ?wal_status);

        if wal_status == "unreserved" {
            // If the WAL status is "unreserved," it means Postgres has marked WAL files we need as
            // being ready for deletion. This happens when the number of WAL files exceeds the
            // `max_slot_wal_keep_size` setting on the upstream database.
            warn!(
                "The upstream database has marked WAL files we need as being ready for deletion. \
                   This usually means we aren't able to replicate changes as fast as they are \
                   occurring. If we don't catch up by the next checkpoint on the Postgres server, \
                   we'll be forced to perform a full resnapshot"
            );
        } else if wal_status == "lost" {
            // If the WAL status is "lost," it means Postgres had to purge WAL entries such that it
            // can no longer replay the events we need based on the last log position we
            // reported
            error!(
                "Our replication slot has become invalidated, so a full resnapshot is necessary"
            );

            return Err(ReadySetError::FullResnapshotNeeded);
        }

        let inner_client = self.client.inner();
        let wal_position = self.next_position.unwrap_or_default();
        let messages_support = if version >= 140000 {
            ", \"messages\" 'true'"
        } else {
            ""
        };

        debug!(%wal_position, %slot, postgres_version = %version, %confirmed_flush_lsn, "Starting replication");

        let query = format!(
            "START_REPLICATION SLOT {slot} LOGICAL {wal_position} (
                \"proto_version\" '1',
                \"publication_names\" '{publication}'
                {messages_support}
            )",
        );

        let query = pgsql::simple_query::encode(inner_client, &query).unwrap();

        let mut wal = inner_client.send(pgsql::connection::RequestMessages::Single(
            pgsql::codec::FrontendMessage::Raw(query),
        ))?;

        // On success, server responds with a CopyBothResponse message, and then starts to stream
        // WAL to the frontend. The messages inside the CopyBothResponse messages are of the
        // same format documented for START_REPLICATION ... PHYSICAL, including two
        // CommandComplete messages. The output plugin associated with the selected slot is
        // used to process the output for streaming.
        match wal.next().await? {
            pgsql::Message::CopyBothResponse(_) => {}
            _ => {
                return Err(ReadySetError::ReplicationFailed(
                    "Unexpected result for replication".into(),
                ))
            }
        }

        self.reader = Some(WalReader::new(wal, self.table_filter.clone()));

        Ok(())
    }

    /// Send a status update to the upstream database, reporting our position in
    /// the WAL as the minimum offset across all the base tables up to which data has been
    /// persisted. If none of the base tables contain unpersisted data, we report the position
    /// passed into this method.
    ///
    /// Sending a standby status update to the upstream database involves "ACKing"
    /// the point in the WAL up to which we've successfully persisted data. This lets
    /// the upstream database know that it can remove all the WAL entries up to this
    /// point.
    ///
    /// The LSNs of events sent to us from Postgres do not monotonically increase
    /// with each event (only the LSNs of COMMITs monotonically increase), so it may seem like a bad
    /// idea to report our position in the WAL as the LSN of an arbitrary event we persisted to a
    /// base table. However, because Postgres's logical replication stream reorders events such that
    /// we receive entire transactions at a time, Postgres will know that when we ack a given LSN,
    /// it just means that we've applied all of the COMMITS up until this point and all of the
    /// events in the current transaction up until this point.
    async fn send_standby_status_update(&mut self, cur_pos: Lsn) -> ReadySetResult<()> {
        use bytes::{BufMut, BytesMut};

        // The difference between UNIX and Postgres epoch
        const J2000_EPOCH_GAP: i64 = 946_684_800_000_000;

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros() as i64
            - J2000_EPOCH_GAP;

        let lsn_to_ack = match self.controller.min_persisted_replication_offset().await? {
            // If all of the data written to base tables has been persisted, we can just report our
            // current position upstream
            PersistencePoint::Persisted => cur_pos,
            PersistencePoint::UpTo(offset) => Lsn::try_from(&offset)?,
        };

        debug!(%lsn_to_ack, "sending status update");

        // Can reply with StandbyStatusUpdate or HotStandbyFeedback
        let mut b = BytesMut::with_capacity(39);
        b.put_u8(b'd'); // Copy data
        b.put_i32(38); // Message length (including this field)
        b.put_u8(b'r'); // Status update

        // LSN of the last event written to disk - we report our current position here because our
        // current position is associated with the last event we wrote to a base table
        cur_pos.put_into(&mut b);

        // LSN of the last event *flushed* to disk - this tells the server that it can remove prior
        // WAL entries for this slot. Each base table has an offset up to which data has been
        // persisted to disk, and we report the minimum of those, since this is the maximum position
        // we can report upstream such that Postgres won't purge WAL files with data we haven't yet
        // persisted
        lsn_to_ack.put_into(&mut b);

        // LSN of the last event applied. In our case, an event that has been flushed has
        // necessarily also been applied (since flushing an event happens *after* applying the
        // event to a base table), so the "flush LSN" and the "apply LSN" will always be the same
        lsn_to_ack.put_into(&mut b);

        b.put_i64(now);
        b.put_u8(0);
        self.client
            .inner()
            .send(pgsql::connection::RequestMessages::Single(
                pgsql::codec::FrontendMessage::Raw(b.freeze()),
            ))?;

        Ok(())
    }

    /// Drops a replication slot, freeing any reserved server-side resources.
    /// If the slot is a logical slot that was created in a database other than the database
    /// the walsender is connected to, this command fails.
    /// Not really needed when `TEMPORARY` slot is Used
    pub(crate) async fn drop_replication_slot(&mut self, name: &str) -> ReadySetResult<()> {
        drop_replication_slot(&mut self.client, name).await?;

        Ok(())
    }

    /// Perform a simple query that expects a singe row in response, check that the response is
    /// indeed one row, and contains exactly `n_cols` columns, then return that row
    async fn one_row_query<const N: usize>(&mut self, query: &str) -> ReadySetResult<[String; N]> {
        let mut rows = self.simple_query(query).await?;

        if rows.len() != 2 {
            return Err(ReadySetError::ReplicationFailed(format!(
                "Incorrect response to query {:?} expected 2 rows, got {}",
                query,
                rows.len()
            )));
        }

        match (rows.remove(0), rows.remove(0)) {
            (SimpleQueryMessage::Row(row), SimpleQueryMessage::CommandComplete(_))
                if row.len() == N =>
            {
                Ok(std::array::from_fn(|i| row.get(i).unwrap().into()))
            }
            _ => Err(ReadySetError::ReplicationFailed(format!(
                "Incorrect response to query {query:?}"
            ))),
        }
    }

    /// Perform a simple query and return the resulting rows
    async fn simple_query(&mut self, query: &str) -> ReadySetResult<Vec<SimpleQueryMessage>> {
        Ok(self.client.simple_query(query).await?)
    }
}

/// Drops a replication slot, freeing any reserved server-side resources.
/// If the slot is a logical slot that was created in a database other than the database
/// the walsender is connected to, this command fails.
/// Not really needed when `TEMPORARY` slot is Used
pub async fn drop_replication_slot(client: &mut pgsql::Client, name: &str) -> ReadySetResult<()> {
    info!(slot = name, "Dropping replication slot if exists");
    // SQL command to drop the replication slot over replication connection
    let formatted_command = format!("DROP_REPLICATION_SLOT {name}");

    let res: ReadySetResult<Vec<pgsql::SimpleQueryMessage>> = client
        .simple_query(&formatted_command)
        .await
        .map_err(ReadySetError::from);

    match res {
        Ok(_) => Ok(()),
        Err(err) if error_is_slot_not_found(&err, name) => {
            debug!(
                slot = name,
                "Replication slot to-drop already doesn't exist"
            );
            Ok(())
        }
        Err(err) => Err(err),
    }
}

pub async fn drop_publication(client: &mut pgsql::Client, name: &str) -> ReadySetResult<()> {
    info!(slot = name, "Dropping publication if exists");
    client
        .simple_query(&format!("DROP PUBLICATION IF EXISTS {name}"))
        .await
        .map_err(ReadySetError::from)
        .map(|_| ())
}

pub async fn drop_readyset_schema(client: &mut pgsql::Client) -> ReadySetResult<()> {
    info!("Dropping readyset schema if exists");
    client
        .simple_query("DROP SCHEMA IF EXISTS readyset CASCADE")
        .await
        .map_err(ReadySetError::from)
        .map(|_| ())
}

impl Drop for PostgresWalConnector {
    fn drop(&mut self) {
        self.connection_handle.abort();
    }
}

#[async_trait]
impl Connector for PostgresWalConnector {
    /// Process WAL events and batch them into actions.
    ///
    /// Uses group commit to coalesce multiple consecutive committed
    /// transactions into a single batch, reducing RPC overhead. After the
    /// first transaction in a group commits, a time window is opened to
    /// collect additional committed transactions before flushing.
    async fn next_action(
        &mut self,
        last_pos: &ReplicationOffset,
        until: Option<&ReplicationOffset>,
    ) -> ReadySetResult<(Vec<ReplicationAction>, ReplicationOffset)> {
        set_failpoint_return_err!(failpoints::POSTGRES_REPLICATION_NEXT_ACTION);

        let mut cur_pos = PostgresPosition::try_from(last_pos.clone())?;

        loop {
            // If we have no pending actions, an `until` was passed, and we
            // have reached it, report the log position.
            if self.group_commit.pending_row_count == 0
                && matches!(until, Some(until) if &ReplicationOffset::from(cur_pos) >= until)
            {
                return Ok((vec![ReplicationAction::LogPosition], cur_pos.into()));
            }

            // When a group commit window is active, use it as the read
            // timeout so we flush promptly when no more transactions arrive.
            let read_timeout = if let Some(deadline) = self.group_commit.deadline {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    // Group commit deadline expired — flush immediately
                    let actions = self.flush_pending_actions_with_log_position();
                    return Ok((actions, cur_pos.into()));
                }
                remaining
            } else {
                Duration::from_secs(30)
            };

            // Send a status update to the upstream database if the interval
            // has elapsed, reporting the last successfully applied position.
            if self.time_last_position_reported.elapsed() > self.status_update_interval {
                let lsn: Lsn = last_pos.try_into()?;
                self.send_standby_status_update(lsn).await?;
                self.time_last_position_reported = Instant::now();
            }

            // Get the next buffered event, or read a new one with timeout.
            let event = match self.peek.take() {
                Some(buffered) => {
                    // If the peek was stashed from a flush-pending-first
                    // branch (DDL or Truncate that arrived after BEGIN had
                    // already clobbered `cur_pos`), restore the saved
                    // in-transaction `cur_pos` so the peek'd event is
                    // processed in the correct transaction context.
                    if let Some(saved) = self.peek_cur_pos.take() {
                        cur_pos = saved;
                    }
                    buffered
                }
                None => match self.next_event_with_timeout(read_timeout).await {
                    Ok(Some(ev)) => Ok(ev),
                    Ok(None) => {
                        // Timeout expired
                        if self.group_commit.pending_row_count > 0 {
                            let actions = self.flush_pending_actions_with_log_position();
                            return Ok((actions, cur_pos.into()));
                        }
                        continue;
                    }
                    Err(e) => Err(e),
                },
            };

            // If the next event is an error but we have pending actions,
            // flush the pending actions before propagating the error.
            // We use `cur_pos` rather than `last_committed_pos` because
            // pending rows may belong to the *current* uncommitted
            // transaction (e.g. when two statements share an implicit
            // transaction via simple_query).  In that case
            // `last_committed_pos` still points at a *prior* transaction
            // whose position has already been recorded by the adapter via
            // `handle_log_position`, so flushing at that position would
            // cause the adapter to skip the data as "already applied".
            // `cur_pos` is always at or after the last accumulated row's
            // LSN, so the data will not be skipped.
            if event.is_err() && self.group_commit.pending_row_count > 0 {
                self.peek = Some(event);
                self.peek_cur_pos = Some(cur_pos);
                let actions = self.flush_pending_actions_with_log_position();
                return Ok((actions, cur_pos.into()));
            }

            trace!(?event);
            if self.enable_statement_logging {
                info!(target: "replicator_statement", "{:?}", event);
            }

            if let Err(ReadySetError::TableError { .. }) = event {
                self.had_table_error = true;
            }

            let event = event?;

            match event {
                WalEvent::DdlEvent { ddl_event, lsn } => {
                    if self.group_commit.pending_row_count > 0 {
                        // Flush pending rows first; the DDL is stashed in
                        // peek and processed on the next call. Return
                        // `last_committed_pos` as the flush position —
                        // `cur_pos` has been clobbered to
                        // `(T_ddl.final_lsn, 0)` by the BEGIN of this DDL
                        // transaction, so using it here would tag the
                        // flushed rows with `lsn = 0`. Stash `cur_pos` so
                        // the peek-pop on the next call can restore the
                        // correct in-transaction state before processing
                        // the DDL event.
                        let flush_pos = self
                            .last_committed_pos
                            .expect("pending rows imply at least one prior COMMIT");
                        self.peek = Some(Ok(WalEvent::DdlEvent { ddl_event, lsn }));
                        self.peek_cur_pos = Some(cur_pos);
                        let actions = self.group_commit.flush();
                        return Ok((actions, flush_pos.into()));
                    }
                    return Ok((
                        vec![ReplicationAction::DdlChange {
                            schema: ddl_event.schema().to_string(),
                            changes: vec![ddl_event.try_into_change(self.parsing_config)?],
                        }],
                        cur_pos.with_lsn(lsn).into(),
                    ));
                }

                WalEvent::WantsKeepaliveResponse { end } => {
                    if !self.in_transaction && self.group_commit.pending_row_count == 0 {
                        // No buffered or in-flight actions — safe to report
                        // the keepalive's end LSN as our current position.
                        self.send_standby_status_update(end).await?;
                    } else {
                        // We have uncommitted or unflushed actions — report
                        // the last position we actually persisted.
                        self.send_standby_status_update(last_pos.try_into()?)
                            .await?;
                    }
                    self.time_last_position_reported = Instant::now();
                }

                WalEvent::Begin { final_lsn } => {
                    invariant!(!self.in_transaction);
                    self.in_transaction = true;
                    cur_pos = PostgresPosition::commit_start(final_lsn);
                }

                WalEvent::Commit { lsn, end_lsn } => {
                    if !self.had_table_error {
                        invariant_eq!(lsn, cur_pos.commit_lsn);
                    }
                    self.had_table_error = false;
                    invariant!(self.in_transaction);
                    self.in_transaction = false;

                    cur_pos = cur_pos.with_lsn(end_lsn);
                    // Record this committed position. The DDL and Truncate
                    // flush-pending-first paths return it as their flush
                    // position, because by that point `cur_pos` has been
                    // clobbered to `(T_next.final_lsn, 0)` by the BEGIN
                    // of the next transaction.
                    self.last_committed_pos = Some(cur_pos);

                    // Group commit: decide whether to flush or coalesce.
                    self.group_commit.advance();

                    // During catchup (until is Some), flush on every commit
                    // to stop precisely at the target position.
                    if until.is_some() || self.group_commit.should_flush() {
                        let actions = self.flush_pending_actions_with_log_position();
                        return Ok((actions, cur_pos.into()));
                    }

                    // Not flushing yet — coalesce with next transaction.
                    debug!(
                        group_commit_trx_count = self.group_commit.trx_count,
                        pending_row_count = self.group_commit.pending_row_count,
                        "group commit: coalescing COMMIT"
                    );
                    if self.group_commit.pending_row_count == 0 {
                        // Empty transaction — just report position.
                        return Ok((vec![ReplicationAction::LogPosition], cur_pos.into()));
                    }
                }

                WalEvent::Truncate { mut tables, lsn } => {
                    // Truncate needs its own batch per table so the base
                    // node can see all rows when deleting them. If we have
                    // pending row actions, flush them first. Return
                    // `last_committed_pos` as the flush position —
                    // `cur_pos` has been clobbered to
                    // `(T_trunc.final_lsn, 0)` by the BEGIN of this
                    // truncate transaction, so using it here would tag
                    // the flushed rows with `lsn = 0`. Stash `cur_pos` so
                    // the peek-pop on the next call can restore the
                    // correct in-transaction state before processing the
                    // Truncate event.
                    if self.group_commit.pending_row_count > 0 {
                        let flush_pos = self
                            .last_committed_pos
                            .expect("pending rows imply at least one prior COMMIT");
                        self.peek = Some(Ok(WalEvent::Truncate { tables, lsn }));
                        self.peek_cur_pos = Some(cur_pos);
                        let actions = self.group_commit.flush();
                        return Ok((actions, flush_pos.into()));
                    }

                    let Some((schema, name)) = tables.pop() else {
                        continue;
                    };

                    // Stash remaining tables for the next call
                    if !tables.is_empty() {
                        self.peek = Some(Ok(WalEvent::Truncate { tables, lsn }));
                    }

                    let table = Relation {
                        schema: Some(schema.into()),
                        name: name.into(),
                    };
                    cur_pos = cur_pos.with_lsn(lsn);
                    return Ok((
                        vec![ReplicationAction::TableAction {
                            table,
                            actions: vec![TableOperation::Truncate],
                        }],
                        cur_pos.into(),
                    ));
                }

                WalEvent::Insert {
                    schema,
                    table,
                    tuple,
                    lsn,
                } => {
                    cur_pos = cur_pos.with_lsn(lsn);
                    if self.accumulate_row(
                        schema.into(),
                        table.into(),
                        [TableOperation::Insert(tuple)],
                    ) {
                        let actions = self.group_commit.flush();
                        return Ok((actions, cur_pos.into()));
                    }
                }

                WalEvent::DeleteRow {
                    schema,
                    table,
                    tuple,
                    lsn,
                } => {
                    cur_pos = cur_pos.with_lsn(lsn);
                    if self.accumulate_row(
                        schema.into(),
                        table.into(),
                        [TableOperation::DeleteRow { row: tuple }],
                    ) {
                        let actions = self.group_commit.flush();
                        return Ok((actions, cur_pos.into()));
                    }
                }

                WalEvent::DeleteByKey {
                    schema,
                    table,
                    key,
                    lsn,
                } => {
                    cur_pos = cur_pos.with_lsn(lsn);
                    if self.accumulate_row(
                        schema.into(),
                        table.into(),
                        [TableOperation::DeleteByKey { key }],
                    ) {
                        let actions = self.group_commit.flush();
                        return Ok((actions, cur_pos.into()));
                    }
                }

                WalEvent::UpdateRow {
                    schema,
                    table,
                    old_tuple,
                    new_tuple,
                    lsn,
                } => {
                    cur_pos = cur_pos.with_lsn(lsn);
                    if self.accumulate_row(
                        schema.into(),
                        table.into(),
                        [
                            TableOperation::DeleteRow { row: old_tuple },
                            TableOperation::Insert(new_tuple),
                        ],
                    ) {
                        let actions = self.group_commit.flush();
                        return Ok((actions, cur_pos.into()));
                    }
                }

                WalEvent::UpdateByKey {
                    schema,
                    table,
                    key,
                    set,
                    lsn,
                } => {
                    cur_pos = cur_pos.with_lsn(lsn);
                    if self.accumulate_row(
                        schema.into(),
                        table.into(),
                        [TableOperation::Update { key, update: set }],
                    ) {
                        let actions = self.group_commit.flush();
                        return Ok((actions, cur_pos.into()));
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use readyset_data::DfValue;

    use super::*;

    fn relation(name: &str) -> Relation {
        Relation {
            schema: Some("public".into()),
            name: SqlIdentifier::from(name),
        }
    }

    /// A plain `TableOperation::Insert` of a single integer, used so the
    /// unit tests don't have to care about column types.
    fn int_insert(v: i32) -> TableOperation {
        TableOperation::Insert(vec![DfValue::from(v)])
    }

    /// Extract the inserted integer values from the first `TableAction`
    /// in the given Vec that targets the given table.
    fn insert_values(actions: &[ReplicationAction], table: &Relation) -> Vec<i32> {
        actions
            .iter()
            .find_map(|a| match a {
                ReplicationAction::TableAction { table: t, actions } if t == table => Some(
                    actions
                        .iter()
                        .filter_map(|op| match op {
                            TableOperation::Insert(row) => i32::try_from(row[0].clone()).ok(),
                            _ => None,
                        })
                        .collect(),
                ),
                _ => None,
            })
            .unwrap_or_default()
    }

    /// Construct a `GroupCommitState` with the given limits and default
    /// empty accumulator. The wait duration is large enough that tests
    /// can treat the time-based flush path as "never fires on its own".
    fn new_state(batch_size: usize, max_trx: usize) -> GroupCommitState {
        GroupCommitState::new(batch_size, max_trx, Duration::from_secs(3600))
    }

    // ── accumulate_row ──────────────────────────────────────────────

    #[test]
    fn accumulate_row_creates_new_entry() {
        let mut state = new_state(100, 20);
        let should_flush = state.accumulate_row(relation("users"), [int_insert(1), int_insert(2)]);

        assert!(!should_flush);
        assert_eq!(state.pending_row_count, 2);
        assert_eq!(state.pending_actions.len(), 1);
        assert_eq!(state.pending_actions[&relation("users")].len(), 2);
    }

    #[test]
    fn accumulate_row_appends_to_existing_table() {
        let mut state = new_state(100, 20);
        state.accumulate_row(relation("users"), [int_insert(1), int_insert(2)]);
        state.accumulate_row(relation("users"), [int_insert(3)]);

        assert_eq!(state.pending_row_count, 3);
        assert_eq!(state.pending_actions.len(), 1);
        assert_eq!(state.pending_actions[&relation("users")].len(), 3);
    }

    #[test]
    fn accumulate_row_separate_tables() {
        let mut state = new_state(100, 20);
        state.accumulate_row(relation("users"), [int_insert(1)]);
        state.accumulate_row(relation("posts"), [int_insert(2)]);

        assert_eq!(state.pending_row_count, 2);
        assert_eq!(state.pending_actions.len(), 2);
    }

    #[test]
    fn accumulate_row_preserves_order_within_table() {
        let mut state = new_state(100, 20);
        state.accumulate_row(relation("users"), [int_insert(10), int_insert(20)]);
        state.accumulate_row(relation("users"), [int_insert(30), int_insert(40)]);

        let actions = state.flush();
        assert_eq!(
            insert_values(&actions, &relation("users")),
            vec![10, 20, 30, 40]
        );
    }

    #[test]
    fn accumulate_row_returns_true_at_exact_batch_size() {
        let mut state = new_state(2, 20);
        // 1 row — under the limit
        assert!(!state.accumulate_row(relation("users"), [int_insert(1)]));
        // 2 rows — exactly at the limit, should signal flush
        assert!(state.accumulate_row(relation("users"), [int_insert(2)]));
    }

    #[test]
    fn accumulate_row_returns_true_past_batch_size() {
        let mut state = new_state(2, 20);
        // Inserting 3 in one shot crosses the limit in a single call
        assert!(state.accumulate_row(
            relation("users"),
            [int_insert(1), int_insert(2), int_insert(3)],
        ));
    }

    // ── flush ───────────────────────────────────────────────────────

    #[test]
    fn flush_drains_and_resets_counters() {
        let mut state = new_state(100, 20);
        state.accumulate_row(relation("users"), [int_insert(1)]);
        state.accumulate_row(relation("posts"), [int_insert(2)]);
        state.advance();

        let actions = state.flush();

        assert_eq!(actions.len(), 2);
        assert_eq!(state.pending_row_count, 0);
        assert!(state.pending_actions.is_empty());
        assert_eq!(state.trx_count, 0);
        assert!(state.deadline.is_none());
        assert!(state.leader.is_none());
    }

    #[test]
    fn flush_with_no_pending_returns_empty() {
        let mut state = new_state(100, 20);
        let actions = state.flush();
        assert!(actions.is_empty());
    }

    #[test]
    fn flush_emits_one_table_action_per_relation() {
        let mut state = new_state(100, 20);
        state.accumulate_row(relation("a"), [int_insert(1), int_insert(2)]);
        state.accumulate_row(relation("b"), [int_insert(3)]);
        state.accumulate_row(relation("a"), [int_insert(4)]);

        let mut actions = state.flush();
        // Order across tables isn't guaranteed by HashMap drain; sort for
        // deterministic comparison.
        actions.sort_by(|x, y| match (x, y) {
            (
                ReplicationAction::TableAction { table: t1, .. },
                ReplicationAction::TableAction { table: t2, .. },
            ) => t1.name.cmp(&t2.name),
            _ => std::cmp::Ordering::Equal,
        });

        assert_eq!(actions.len(), 2);
        assert_eq!(insert_values(&actions, &relation("a")), vec![1, 2, 4]);
        assert_eq!(insert_values(&actions, &relation("b")), vec![3]);
    }

    // ── advance / should_flush / reset_window ──────────────────────

    #[test]
    fn advance_noop_when_no_pending_rows() {
        let mut state = new_state(100, 20);
        state.advance();

        assert_eq!(state.trx_count, 0);
        assert!(state.deadline.is_none());
        assert!(state.leader.is_none());
    }

    #[test]
    fn advance_sets_deadline_on_first_commit() {
        let mut state = new_state(100, 20);
        state.accumulate_row(relation("users"), [int_insert(1)]);
        state.advance();

        assert_eq!(state.trx_count, 1);
        assert!(state.deadline.is_some());
        assert!(state.leader.is_some());
    }

    #[test]
    fn advance_preserves_deadline_on_subsequent_commits() {
        let mut state = new_state(100, 20);
        state.accumulate_row(relation("users"), [int_insert(1)]);
        state.advance();
        let deadline_after_first = state.deadline;
        let leader_after_first = state.leader;

        state.accumulate_row(relation("users"), [int_insert(2)]);
        state.advance();

        assert_eq!(state.trx_count, 2);
        assert_eq!(state.deadline, deadline_after_first);
        assert_eq!(state.leader, leader_after_first);
    }

    #[test]
    fn should_flush_false_when_below_all_limits() {
        let mut state = new_state(100, 20);
        state.accumulate_row(relation("users"), [int_insert(1)]);
        state.advance();
        assert!(!state.should_flush());
    }

    #[test]
    fn should_flush_true_when_trx_count_reaches_max() {
        let mut state = new_state(100, 3);
        for i in 0..3 {
            state.accumulate_row(relation("users"), [int_insert(i)]);
            state.advance();
        }
        assert!(state.should_flush());
    }

    #[test]
    fn should_flush_true_when_row_count_reaches_batch_size() {
        let mut state = new_state(2, 20);
        state.accumulate_row(relation("users"), [int_insert(1), int_insert(2)]);
        state.advance();
        assert!(state.should_flush());
    }

    #[test]
    fn should_flush_true_when_wait_is_zero() {
        let mut state = GroupCommitState::new(100, 20, Duration::ZERO);
        state.accumulate_row(relation("users"), [int_insert(1)]);
        state.advance();
        assert!(state.should_flush());
    }

    #[test]
    fn reset_window_clears_deadline_leader_and_trx_count() {
        let mut state = new_state(100, 20);
        state.accumulate_row(relation("users"), [int_insert(1)]);
        state.advance();
        // Sanity check — advance set everything
        assert!(state.deadline.is_some());
        assert!(state.leader.is_some());
        assert_eq!(state.trx_count, 1);

        state.reset_window();

        assert!(state.deadline.is_none());
        assert!(state.leader.is_none());
        assert_eq!(state.trx_count, 0);
        // `reset_window` is intentionally *not* supposed to clear the
        // accumulated rows — those are only drained by `flush`.
        assert_eq!(state.pending_row_count, 1);
    }
}
