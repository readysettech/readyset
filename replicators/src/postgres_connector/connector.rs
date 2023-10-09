use std::time::{Duration, Instant};

use async_trait::async_trait;
use database_utils::UpstreamConfig;
#[cfg(feature = "failure_injection")]
use failpoint_macros::set_failpoint;
use futures::FutureExt;
use nom_sql::Relation;
use pgsql::SimpleQueryMessage;
use postgres_native_tls::MakeTlsConnector;
use postgres_protocol::escape::escape_literal;
#[cfg(feature = "failure_injection")]
use readyset_client::failpoints;
use readyset_client::TableOperation;
use readyset_errors::{
    invariant, invariant_eq, set_failpoint_return_err, ReadySetError, ReadySetResult,
};
use readyset_util::select;
use replication_offset::postgres::{CommitLsn, Lsn, PostgresPosition};
use replication_offset::ReplicationOffset;
use tokio_postgres as pgsql;
use tracing::{debug, error, info, trace, warn};

use super::ddl_replication::setup_ddl_replication;
use super::wal_reader::{WalEvent, WalReader};
use super::PUBLICATION_NAME;
use crate::db_util::error_is_slot_not_found;
use crate::noria_adapter::{Connector, ReplicationAction};

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
    /// The max number of replication actions we buffer before returning them as a batch.
    ///
    /// Calling the ReadySet API is a bit expensive, therefore we try to queue as many actions
    /// as possible before calling into the API. We therefore try to stop batching actions when
    /// we hit this limit, BUT note that we may substantially exceed this limit in cases where
    /// we receive many consecutive events that share the same LSN.
    const MAX_QUEUED_INDEPENDENT_ACTIONS: usize = 100;

    /// The interval at which we will send status updates to the upstream database. This matches the
    /// default value for Postgres's `wal_receiver_status_interval`, which is the interval between
    /// status updates given by Postgres's own WAL receiver during replication.
    const STATUS_UPDATE_INTERVAL: Duration = Duration::from_secs(10);

    /// Connects to postgres and if needed creates a new replication slot for itself with an
    /// exported snapshot.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn connect<S: AsRef<str>>(
        mut pg_config: pgsql::Config,
        dbname: S,
        config: UpstreamConfig,
        next_position: Option<Lsn>,
        tls_connector: MakeTlsConnector,
        repl_slot_name: &str,
        enable_statement_logging: bool,
        full_resnapshot: bool,
    ) -> ReadySetResult<Self> {
        if !config.disable_setup_ddl_replication {
            setup_ddl_replication(pg_config.clone(), tls_connector.clone()).await?;
        }
        pg_config.dbname(dbname.as_ref()).set_replication_database();

        let (client, connection) = pg_config
            .connect(tls_connector)
            .await
            .map_err(|e| ReadySetError::ReplicationFailed(format!("Failed to connect: {e}")))?;
        let connection_handle = tokio::spawn(connection);

        let mut connector = PostgresWalConnector {
            client,
            connection_handle,
            reader: None,
            peek: None,
            next_position,
            replication_slot: None,
            enable_statement_logging,
            // We initialize `time_last_position_reported` to be more than
            // `Self::STATUS_UPDATE_INTERVAL` seconds ago to ensure that we report our position in
            // the logs the next time we have an opportunity to
            time_last_position_reported: Instant::now()
                - Self::STATUS_UPDATE_INTERVAL
                - Duration::from_secs(1),
            // We'll never start replicating in the middle of a transaction, since Postgres's
            // logical replication protocol only streams whole transactions to us at a time. This
            // means we know that when we first initiate a replication connection to the upstream
            // database, we'll always start replicating outside of a transaction
            in_transaction: false,
        };

        if full_resnapshot || next_position.is_none() {
            // If we don't have a consistent replication offset to start replicating from or if we
            // need to perform a full resnapshot, drop and recreate our replication slot.
            //
            // Note that later on, this means we'll need to make sure we resnapshot *all* tables!
            connector
                .create_publication_and_slot(repl_slot_name)
                .await?;
        }

        Ok(connector)
    }

    async fn create_publication_and_slot(&mut self, repl_slot_name: &str) -> ReadySetResult<()> {
        let system = self.identify_system().await?;
        debug!(
            id = %system.id,
            timeline = %system.timeline,
            xlogpos = %system.xlogpos,
            dbname = ?system.dbname
        );

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
                ev = reader.next_event().fuse() => ev.map_err(Into::into),
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
        let query = format!("CREATE PUBLICATION {} FOR ALL TABLES", name);
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

        self.reader = Some(WalReader::new(wal));

        Ok(())
    }

    /// Send a status update to the upstream database, reporting our position in
    /// the WAL as `ack`.
    ///
    /// Sending a standby status update to the upstream database involves "ACKing"
    /// the point in the WAL up to which we've successfully persisted data. This lets
    /// the upstream database know that it can remove all the WAL entries up to this
    /// point.
    ///
    /// The LSNs of events sent to us from Postgres do not monotonically increase
    /// with each event (only the LSNs of COMMITs monotonically increase), so it may seem like a bad
    /// idea to report our position in the WAL as the LSN of the last event we wrote to a base
    /// table. However, because Postgres's logical replication stream reorders events such that we
    /// receive entire transactions at a time, Postgres will know that when we ack a given LSN, it
    /// just means that we've applied all of the COMMITS up until this point and all of the events
    /// in the current transaction up until this point.
    fn send_standby_status_update(&self, ack: Lsn) -> ReadySetResult<()> {
        use bytes::{BufMut, BytesMut};

        // The difference between UNIX and Postgres epoch
        const J2000_EPOCH_GAP: i64 = 946_684_800_000_000;

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros() as i64
            - J2000_EPOCH_GAP;

        // Can reply with StandbyStatusUpdate or HotStandbyFeedback
        let mut b = BytesMut::with_capacity(39);
        b.put_u8(b'd'); // Copy data
        b.put_i32(38); // Message length (including this field)
        b.put_u8(b'r'); // Status update
        ack.put_into(&mut b); // Acked
        ack.put_into(&mut b); // Flushed - this tells the server that it can remove prior WAL entries for this slot
        ack.put_into(&mut b); // Applied
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
                "Incorrect response to query {:?}",
                query
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
    let res: ReadySetResult<Vec<pgsql::SimpleQueryMessage>> = client
        .simple_query(&format!("DROP_REPLICATION_SLOT {}", name))
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
        .simple_query(&format!("DROP PUBLICATION IF EXISTS {}", name))
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
    /// Process WAL events and batch them into actions
    async fn next_action(
        &mut self,
        last_pos: &ReplicationOffset,
        until: Option<&ReplicationOffset>,
    ) -> ReadySetResult<(Vec<ReplicationAction>, ReplicationOffset)> {
        set_failpoint_return_err!(failpoints::POSTGRES_REPLICATION_NEXT_ACTION);

        // If it has been longer than the defined status update interval, send a status update to
        // the upstream database to report our position in the WAL as the LSN of the last event we
        // successfully applied to ReadySet
        if self.time_last_position_reported.elapsed() > Self::STATUS_UPDATE_INTERVAL {
            let lsn: Lsn = last_pos.clone().try_into()?;
            debug!("Reporting our position in the WAL to the upstream database as {lsn}");

            self.send_standby_status_update(lsn)?;
            self.time_last_position_reported = Instant::now();
        }

        let mut cur_table = Relation {
            schema: None,
            name: "".into(),
        };
        let mut cur_pos = PostgresPosition::try_from(last_pos.clone())?;
        let mut actions = Vec::with_capacity(Self::MAX_QUEUED_INDEPENDENT_ACTIONS);

        loop {
            debug_assert!(
                cur_table.schema.is_some() || cur_table.name.is_empty(),
                "We should either have no current table, or the current table should have a schema"
            );

            // If we have no buffered actions, an `until` was passed, and the LSN is >= `until`,
            // we report the log position
            if actions.is_empty()
                && matches!(until, Some(until) if &ReplicationOffset::from(cur_pos) >= until)
            {
                return Ok((vec![ReplicationAction::LogPosition], cur_pos.into()));
            }

            // Get the next buffered event or error, or read a new event from the WAL stream.
            let event = match self.peek.take() {
                Some(buffered) => buffered,
                None => self.next_event().await,
            };

            match &event.as_ref().map(|ev| ev.lsn()) {
                // If our next event is an error but there are buffered actions, we need to flush
                // the buffered actions before handling the error
                Err(_) if !actions.is_empty() => {
                    self.peek = Some(event);
                    return Ok((
                        vec![ReplicationAction::TableAction {
                            table: cur_table,
                            actions,
                            txid: None,
                        }],
                        cur_pos.into(),
                    ));
                }
                // If our next event has an LSN, if our batch size has exceeded the max, and if the
                // LSN of the event does not match the LSN of the prior event, we need to flush the
                // buffered actions. This is to ensure that we apply all of the actions with a given
                // LSN atomically: if the next event has an LSN that is different than the LSN of
                // our current position, we need to stash the event with a different LSN, apply the
                // batch of actions we have buffered, and then come back to the stashed event the
                // next time this method is called.
                //
                // Note that COMMITs and BEGINs are counted as events that don't have LSNs and thus
                // will never result in an early return here, since WalEvent::lsn() will return
                // None.
                //
                // There are two situations in which we will receive a BEGIN: 1) if the last event
                // we've received was a COMMIT and 2) if we are starting replication in the middle
                // of a transaction after a restart. If the last event we received was a COMMIT, we
                // know that we've already flushed all of our actions in the prior invocation of
                // `handle_action`, so there would be no reason to return early here. If we are
                // starting replication in the middle of a transaction after a restart, we will end
                // up throwing out the actions that occur between this BEGIN and `last_pos`, since
                // we've already processed them.
                //
                // A COMMIT always results in the flushing of our buffered events with the
                // replication offset reported to be `(LSN of the COMMIT, LSN of the COMMIT)`. For a
                // COMMIT event with LSN `x`, the `PostgresPosition` of that COMMIT event would be
                // `(x, x)`. Even if the next event were to share an LSN with the COMMIT that came
                // directly before it, the LSN `y` of the COMMIT that ends the next transaction
                // would be greater than the LSN of the prior commit, which means
                // `(x, x) != (y, x)`. In simpler terms, this guarantees that there will never be an
                // event that comes after a COMMIT that shares the same `PostgresPosition`, so we
                // don't need to worry about applying these events atomically to a base table
                // domain.
                Ok(Some(lsn))
                    if actions.len() >= Self::MAX_QUEUED_INDEPENDENT_ACTIONS
                        && *lsn != cur_pos.lsn =>
                {
                    self.peek = Some(event);
                    return Ok((
                        vec![ReplicationAction::TableAction {
                            table: cur_table,
                            actions,
                            txid: None,
                        }],
                        cur_pos.into(),
                    ));
                }
                _ => {}
            }

            trace!(?event);
            // Don't log the statement if we're catching up
            if self.enable_statement_logging {
                info!(target: "replicator_statement", "{:?}", event);
            }

            let mut event = event?;

            // Check if next event is for another table, in which case we have to flush the events
            // accumulated for this table and store the next event in `peek`.
            match &mut event {
                WalEvent::Truncate { tables, lsn } => {
                    let (matching, mut other_tables) =
                        tables.drain(..).partition::<Vec<_>, _>(|(schema, table)| {
                            cur_table.schema.as_deref() == Some(schema.as_str())
                                && cur_table.name == table.as_str()
                        });

                    if actions.is_empty() {
                        invariant!(matching.is_empty());
                        if let Some((schema, name)) = other_tables.pop() {
                            if !other_tables.is_empty() {
                                self.peek = Some(Ok(WalEvent::Truncate {
                                    tables: other_tables,
                                    lsn: *lsn,
                                }));
                            }

                            actions.push(TableOperation::Truncate);
                            return Ok((
                                vec![ReplicationAction::TableAction {
                                    table: Relation {
                                        schema: Some(schema.into()),
                                        name: name.into(),
                                    },
                                    actions,
                                    txid: None,
                                }],
                                cur_pos.with_lsn(*lsn).into(),
                            ));
                        } else {
                            // Empty truncate op
                            continue;
                        }
                    } else {
                        if !other_tables.is_empty() {
                            self.peek = Some(Ok(WalEvent::Truncate {
                                tables: other_tables,
                                lsn: *lsn,
                            }));
                        }

                        if !matching.is_empty() {
                            actions.push(TableOperation::Truncate);
                        }

                        return Ok((
                            vec![ReplicationAction::TableAction {
                                table: cur_table,
                                actions,
                                txid: None,
                            }],
                            cur_pos.into(),
                        ));
                    }
                }
                WalEvent::Insert { schema, table, .. }
                | WalEvent::DeleteRow { schema, table, .. }
                | WalEvent::DeleteByKey { schema, table, .. }
                | WalEvent::UpdateRow { schema, table, .. }
                | WalEvent::UpdateByKey { schema, table, .. }
                    if cur_table.schema.as_deref() != Some(schema.as_str())
                        || cur_table.name != table.as_str() =>
                {
                    if !actions.is_empty() {
                        self.peek = Some(Ok(event));
                        return Ok((
                            vec![ReplicationAction::TableAction {
                                table: cur_table,
                                actions,
                                txid: None,
                            }],
                            cur_pos.into(),
                        ));
                    } else {
                        cur_table = Relation {
                            schema: Some((&*schema).into()),
                            name: (&*table).into(),
                        };
                    }
                }
                _ => {}
            }

            match event {
                WalEvent::DdlEvent { ddl_event, lsn } => {
                    if actions.is_empty() {
                        return Ok((
                            vec![ReplicationAction::DdlChange {
                                schema: ddl_event.schema().to_string(),
                                changes: vec![ddl_event.into_change()],
                            }],
                            cur_pos.with_lsn(lsn).into(),
                        ));
                    } else {
                        self.peek = Some(Ok(WalEvent::DdlEvent { ddl_event, lsn }));
                        return Ok((
                            vec![ReplicationAction::TableAction {
                                table: cur_table,
                                actions,
                                txid: None,
                            }],
                            cur_pos.into(),
                        ));
                    }
                }
                WalEvent::WantsKeepaliveResponse { end } => {
                    if !self.in_transaction && actions.is_empty() {
                        // If the last event we applied to our base tables was a COMMIT and we have
                        // no buffered actions, we can safely report the "end LSN" given to us in
                        // the keepalive request as our current position.
                        self.send_standby_status_update(end)?;
                    } else {
                        // If we have buffered actions, we have to report the position of the *last*
                        // event we applied, since we haven't yet applied the events associated with
                        // `cur_pos`
                        self.send_standby_status_update(last_pos.clone().try_into()?)?;
                    }
                    self.time_last_position_reported = Instant::now();
                }
                WalEvent::Begin { final_lsn } => {
                    // BEGINs should only happen if we aren't already in a transaction
                    invariant!(!self.in_transaction);
                    self.in_transaction = true;

                    // Update our current position to be `(final_lsn, 0)`, which points to the first
                    // position in the transaction that is ended by the COMMIT at `final_lsn`.
                    cur_pos = PostgresPosition::commit_start(final_lsn);
                }
                WalEvent::Commit { lsn, end_lsn } => {
                    // If the `CommitLsn` from the COMMIT does not match the `CommitLsn` from the
                    // BEGIN, something has gone very wrong
                    invariant_eq!(lsn, cur_pos.commit_lsn);

                    // COMMITs should only happen in the context of a transaction
                    invariant!(self.in_transaction);
                    self.in_transaction = false;

                    // On commit we flush, because there is no knowing when the next commit is
                    // coming. We report our current position to reflect the COMMIT we just saw.
                    // If we crash after returning this position but before persisting this new
                    // position in the base tables, we will begin replicating from a COMMIT prior to
                    // this one, guaranteeing that we don't miss any events.
                    let position = cur_pos.with_lsn(end_lsn);

                    if !actions.is_empty() {
                        return Ok((
                            vec![ReplicationAction::TableAction {
                                table: cur_table,
                                actions,
                                txid: None,
                            }],
                            position.into(),
                        ));
                    } else {
                        return Ok((vec![ReplicationAction::LogPosition], position.into()));
                    }
                }
                WalEvent::Insert { tuple, lsn, .. } => {
                    cur_pos = cur_pos.with_lsn(lsn);
                    actions.push(TableOperation::Insert(tuple));
                }
                WalEvent::DeleteRow { tuple, lsn, .. } => {
                    cur_pos = cur_pos.with_lsn(lsn);
                    actions.push(TableOperation::DeleteRow { row: tuple });
                }
                WalEvent::DeleteByKey { key, lsn, .. } => {
                    cur_pos = cur_pos.with_lsn(lsn);
                    actions.push(TableOperation::DeleteByKey { key })
                }
                WalEvent::UpdateRow {
                    old_tuple,
                    new_tuple,
                    lsn,
                    ..
                } => {
                    cur_pos = cur_pos.with_lsn(lsn);
                    actions.push(TableOperation::DeleteRow { row: old_tuple });
                    actions.push(TableOperation::Insert(new_tuple));
                }
                WalEvent::UpdateByKey { key, set, lsn, .. } => {
                    cur_pos = cur_pos.with_lsn(lsn);
                    actions.push(TableOperation::Update { key, update: set })
                }
                WalEvent::Truncate { lsn, .. } => {
                    cur_pos = cur_pos.with_lsn(lsn);
                    actions.push(TableOperation::Truncate)
                }
            }
        }
    }
}
