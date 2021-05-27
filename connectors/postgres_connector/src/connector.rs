use futures::FutureExt;
use noria::{ReadySetError, ReadySetResult, TableOperation};
use std::convert::{TryFrom, TryInto};
use tokio_postgres as pgsql;

use crate::wal_reader::{WalEvent, WalReader};

const REPLICATION_SLOT: &str = "noria";
const PUBLICATION_NAME: &str = "noria";

#[derive(Debug, PartialEq, PartialOrd, Ord, Eq, Clone, Copy, Default)]
pub struct PostgresPosition {
    /// Postgres Log Sequence Number
    pub lsn: i64,
}

pub struct PostgresWalConnector {
    /// This is the underlying (regular) PostgreSQL client
    client: pgsql::Client,
    /// A tokio task that handles the connection, required by `tokio_postgres` to operate
    connection_handle: tokio::task::JoinHandle<Result<(), pgsql::Error>>,
    /// Reader is a decoder for binlog events
    reader: Option<WalReader>,
    /// Stores an event that was read but not handled
    peek: Option<(WalEvent, i64)>,
    /// If we just want to continue reading the binlog from a previous point
    next_position: Option<PostgresPosition>,
    log: slog::Logger,
}

#[derive(Debug)]
pub enum WalAction {
    TableAction {
        table: String,
        actions: Vec<TableOperation>,
        lsn: PostgresPosition,
    },
}

/// The decoded response to `IDENTIFY_SYSTEM`
#[derive(Debug)]
pub struct ServerIdentity {
    /// The unique system identifier identifying the cluster. This can be used to check that the base
    /// backup used to initialize the standby came from the same cluster.
    id: String,
    /// Current timeline ID. Also useful to check that the standby is consistent with the master.
    timeline: i8,
    /// Current WAL flush location. Useful to get a known location in the write-ahead log where streaming can start.
    xlogpos: String,
    /// Database connected to or null.
    dbname: Option<String>,
}

/// The decoded response to `CREATE_REPLICATION_SLOT`
#[derive(Debug)]
pub struct CreatedSlot {
    slot_name: String,
    consistent_point: String,
    snapshot_name: Option<String>,
    output_plugin: Option<String>,
}

impl TryFrom<&PostgresPosition> for noria::ReplicationOffset {
    type Error = ReadySetError;

    fn try_from(value: &PostgresPosition) -> Result<Self, Self::Error> {
        Ok(noria::ReplicationOffset {
            replication_log_name: String::new(),
            offset: value.lsn as _,
        })
    }
}

impl TryFrom<PostgresPosition> for noria::ReplicationOffset {
    type Error = ReadySetError;

    fn try_from(value: PostgresPosition) -> Result<Self, Self::Error> {
        (&value).try_into()
    }
}

impl From<noria::ReplicationOffset> for PostgresPosition {
    fn from(val: noria::ReplicationOffset) -> Self {
        PostgresPosition {
            lsn: val.offset as _,
        }
    }
}

impl From<i64> for PostgresPosition {
    fn from(val: i64) -> Self {
        PostgresPosition { lsn: val }
    }
}

impl std::fmt::Display for PostgresPosition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:x}/{:x}", self.lsn >> 32, self.lsn & 0xffffffff)
    }
}

impl PostgresWalConnector {
    pub async fn connect<S: AsRef<str>>(
        mut config: pgsql::Config,
        dbname: S,
        next_position: Option<PostgresPosition>,
        logger: slog::Logger,
    ) -> ReadySetResult<Self> {
        let connector = native_tls::TlsConnector::builder().build().unwrap(); // Never returns an error
        let connector = postgres_native_tls::MakeTlsConnector::new(connector);

        config.dbname(dbname.as_ref()).set_replication_database();

        let (client, connection) = config.connect(connector).await?;

        let connection_handle = tokio::spawn(connection);

        let mut connector = PostgresWalConnector {
            client,
            connection_handle,
            reader: None,
            peek: None,
            next_position,
            log: logger,
        };

        // TODO: what do we do with this information?
        let _system = connector.identify_system().await?;

        match connector.create_publication(PUBLICATION_NAME).await {
            Ok(()) => {
                // When a new publication is created we have to remove the existing slot if any
                let _ = connector.drop_replication_slot(REPLICATION_SLOT).await;
            } // Created a new publication, everything is good
            Err(err)
                if err.to_string().contains("publication")
                    && err.to_string().contains("already exists") => {} // This is an existing slot, also good
            Err(err) => return Err(err),
        }

        match connector.create_replication_slot(REPLICATION_SLOT).await {
            Ok(_slot) => {} // Created a new slot, everything is good
            Err(err)
                if err.to_string().contains("replication slot")
                    && err.to_string().contains("already exists") => {} // This is an existing slot, also good
            Err(err) => return Err(err),
        };

        connector
            .start_replication(REPLICATION_SLOT, PUBLICATION_NAME)
            .await?;
        Ok(connector)
    }

    /// Waits and returns the next WAL event, while monotoring the connection
    /// handle for errors.
    async fn next_event(&mut self) -> ReadySetResult<(WalEvent, i64)> {
        let PostgresWalConnector {
            reader,
            connection_handle,
            ..
        } = self;

        if let Some(reader) = reader.as_mut() {
            futures::select! {
                ev = reader.next_event().fuse() => ev,
                err = connection_handle.fuse() => match err.unwrap() { // This unwrap is ok, because it is on the handle
                    Ok(_) => unreachable!(), // Unrechable because it runs in infinite loop unless errors
                    Err(err) => Err(err.into()),
                }
            }
        } else {
            Err(ReadySetError::ReplicationFailed("Not started".to_string()))
        }
    }

    /// Process WAL events and batch them into actions
    pub async fn next_action(&mut self, acked_lsn: PostgresPosition) -> ReadySetResult<WalAction> {
        // Calling the Noria API is a bit expensive, therefore we try to queue as many
        // actions as possible before calling into the API.
        const MAX_QUEUED_ACTIONS: usize = 100;
        let mut cur_table = String::new();
        let mut cur_lsn = 0;
        let mut actions = Vec::with_capacity(MAX_QUEUED_ACTIONS);

        loop {
            // Don't accumulate too many actions between calls
            if actions.len() > MAX_QUEUED_ACTIONS {
                return Ok(WalAction::TableAction {
                    table: cur_table,
                    actions,
                    lsn: cur_lsn.into(),
                });
            }

            let (event, lsn) = match self.peek.take() {
                Some(event) => event,
                None => self.next_event().await?,
            };

            // Check if next event is for another table, in which case we have to flush the events accumulated for this table
            // and store the next event in `peek`.
            match &event {
                WalEvent::Insert { table, .. }
                | WalEvent::DeleteRow { table, .. }
                | WalEvent::DeleteByKey { table, .. }
                | WalEvent::UpdateRow { table, .. }
                | WalEvent::UpdateByKey { table, .. }
                    if table.as_str() != cur_table =>
                {
                    if !actions.is_empty() {
                        self.peek = Some((event, lsn));
                        return Ok(WalAction::TableAction {
                            table: cur_table,
                            actions,
                            lsn: cur_lsn.into(),
                        });
                    } else {
                        cur_table = table.clone();
                    }
                }
                _ => {}
            }

            cur_lsn = lsn;

            match event {
                WalEvent::WantsKeepaliveResponse => {
                    self.send_standy_status_update(acked_lsn)?;
                }
                WalEvent::Commit => {
                    if !actions.is_empty() {
                        // On commit we flush, because there is no knowing when the next commit is comming
                        return Ok(WalAction::TableAction {
                            table: cur_table,
                            actions,
                            lsn: lsn.into(),
                        });
                    }
                }
                WalEvent::Insert { tuple, .. } => actions.push(TableOperation::Insert(tuple)),
                WalEvent::DeleteRow { tuple, .. } => {
                    actions.push(TableOperation::DeleteRow { row: tuple })
                }
                WalEvent::DeleteByKey { key, .. } => {
                    actions.push(TableOperation::DeleteByKey { key })
                }
                WalEvent::UpdateRow {
                    old_tuple,
                    new_tuple,
                    ..
                } => {
                    actions.push(TableOperation::DeleteRow { row: old_tuple });
                    actions.push(TableOperation::Insert(new_tuple));
                }
                WalEvent::UpdateByKey { key, set, .. } => {
                    actions.push(TableOperation::Update { key, set })
                }
            }
        }
    }

    /// Requests the server to identify itself. Server replies with a result set of a single row, containing four fields:
    /// systemid (text) - The unique system identifier identifying the cluster. This can be used to check that the base
    ///                   backup used to initialize the standby came from the same cluster.
    /// timeline (int4) - Current timeline ID. Also useful to check that the standby is consistent with the master.
    /// xlogpos (text) - Current WAL flush location. Useful to get a known location in the write-ahead log where streaming can start.
    /// dbname (text) - Database connected to or null.
    async fn identify_system(&mut self) -> ReadySetResult<ServerIdentity> {
        let row = self.one_row_query("IDENTIFY_SYSTEM", 4).await?;
        // We know we have 4 valid columns because `one_row_query` checks that, so can unwrap here
        let id = row.get(0).unwrap().to_string();
        let timeline: i8 = row.get(1).unwrap().parse().map_err(|_| {
            ReadySetError::ReplicationFailed("Unable to parse identify system".into())
        })?;
        let xlogpos = row.get(2).unwrap().to_string();
        let dbname = row.get(3).map(Into::into);

        Ok(ServerIdentity {
            id,
            timeline,
            xlogpos,
            dbname,
        })
    }

    /// Creates a new `PUBLICATION name FOR ALL TABLES`, to be able to recieve WAL on that slot.
    async fn create_publication(&mut self, name: &str) -> ReadySetResult<()> {
        let query = format!("CREATE PUBLICATION {} FOR ALL TABLES", name);
        self.simple_query(&query).await?;
        Ok(())
    }

    /// Creates a new replication slot on the primary.
    async fn create_replication_slot(&mut self, name: &str) -> ReadySetResult<CreatedSlot> {
        // Command format:
        // CREATE_REPLICATION_SLOT slot_name [ TEMPORARY ] { PHYSICAL [ RESERVE_WAL ] | LOGICAL output_plugin [ EXPORT_SNAPSHOT | NOEXPORT_SNAPSHOT | USE_SNAPSHOT ] }
        //
        // TEMPORARY - Specify that this replication slot is a temporary one. Temporary slots are not saved to disk and are
        // automatically dropped on error or when the session has finished.
        // NOEXPORT_SNAPSHOT - Decides what to do with the snapshot created during logical slot initialization. NOEXPORT_SNAPSHOT
        // will just use the snapshot for logical decoding as normal but won't do anything else with it.
        // pgoutput - the plugin to use for logical decoding, always available from PG > 10
        let query = format!(
            "CREATE_REPLICATION_SLOT {} LOGICAL pgoutput NOEXPORT_SNAPSHOT",
            name
        );

        let row = self.one_row_query(&query, 4).await?;

        // In response to this command, the server will send a one-row result set containing the following fields:
        // slot_name (text) - The name of the newly-created replication slot.
        // consistent_point (text) - The WAL location at which the slot became consistent. This is the earliest location
        //                           from which streaming can start on this replication slot.
        // snapshot_name (text) - The identifier of the snapshot exported by the command. The snapshot is valid until a
        //                        new command is executed on this connection or the replication connection is closed.
        //                        Null if the created slot is physical.
        // output_plugin (text) - The name of the output plugin used by the newly-created replication slot.
        //                        Null if the created slot is physical.
        let slot_name = row.get(0).unwrap().to_string(); // Can unwrap because checked by `one_row_query`
        let consistent_point = row.get(1).unwrap().to_string();
        let snapshot_name = row.get(2).map(Into::into);
        let output_plugin = row.get(3).map(Into::into);

        Ok(CreatedSlot {
            slot_name,
            consistent_point,
            snapshot_name,
            output_plugin,
        })
    }

    /// Begin replication on the `slot` and `publication`. The `publication` must be present on
    /// the server, and can be created using: `CREATE PUBLICATION publication FOR ALL TABLES;`
    pub async fn start_replication(&mut self, slot: &str, publication: &str) -> ReadySetResult<()> {
        let inner_client = self.client.inner();

        let query = format!(
            "START_REPLICATION SLOT {} LOGICAL {} (\"proto_version\" '1', \"publication_names\" '{}')",
            slot, self.next_position.unwrap_or_default(), publication
        );

        let query = pgsql::simple_query::encode(inner_client, &query).unwrap();

        let mut wal = inner_client.send(pgsql::connection::RequestMessages::Single(
            pgsql::codec::FrontendMessage::Raw(query),
        ))?;

        // On success, server responds with a CopyBothResponse message, and then starts to stream WAL to the frontend.
        // The messages inside the CopyBothResponse messages are of the same format documented for START_REPLICATION ... PHYSICAL,
        // including two CommandComplete messages.
        // The output plugin associated with the selected slot is used to process the output for streaming.
        match wal.next().await? {
            pgsql::Message::CopyBothResponse(_) => {}
            _ => {
                return Err(ReadySetError::ReplicationFailed(
                    "Unexpected result for replication".into(),
                ))
            }
        }

        self.reader = Some(WalReader::new(wal, self.log.clone()));

        Ok(())
    }

    fn send_standy_status_update(&self, ack: PostgresPosition) -> ReadySetResult<()> {
        use bytes::{BufMut, BytesMut};

        // The difference between UNIX and Postgres epoch
        const J2000_EPOCH_GAP: u64 = 946_684_800_000_000;

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64
            - J2000_EPOCH_GAP;

        let pos = ack.lsn + 1;

        // Can reply with StandbyStatusUpdate or HotStandbyFeedback
        let mut b = BytesMut::with_capacity(39);
        b.put_u8(b'd'); // Copy data
        b.put_i32(38); // Message length (including this field)
        b.put_u8(b'r'); // Status update
        b.put_i64(pos); // Acked
        b.put_i64(pos); // Flushed
        b.put_i64(pos); // Applied - this tells the server that it can remove prior WAL entries for this slot
        b.put_u64(now.to_be());
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
    async fn drop_replication_slot(&mut self, name: &str) -> ReadySetResult<()> {
        self.simple_query(&format!("DROP_REPLICATION_SLOT {}", name))
            .await
            .map(|_| ())
    }

    /// Perform a simple query that expects a singe row in response, check that the response is indeed
    /// one row, and contains exatly `n_cols` columns, then return that row
    async fn one_row_query(
        &mut self,
        query: &str,
        n_cols: usize,
    ) -> ReadySetResult<pgsql::SimpleQueryRow> {
        let mut rows = self.simple_query(query).await?;

        if rows.len() != 2 {
            return Err(ReadySetError::ReplicationFailed(format!(
                "Incorrect response to query {:?} expected 2 rows, got {}",
                query,
                rows.len()
            )));
        }

        match (rows.remove(0), rows.remove(0)) {
            (
                pgsql::SimpleQueryMessage::Row(row),
                pgsql::SimpleQueryMessage::CommandComplete(_),
            ) if row.len() == n_cols => Ok(row),
            _ => Err(ReadySetError::ReplicationFailed(format!(
                "Incorrect response to query {:?}",
                query
            ))),
        }
    }

    /// Perform a simple query and return the resulting rows
    async fn simple_query(
        &mut self,
        query: &str,
    ) -> ReadySetResult<Vec<pgsql::SimpleQueryMessage>> {
        Ok(self.client.simple_query(query).await?)
    }
}
