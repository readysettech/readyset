use async_trait::async_trait;
use database_utils::UpstreamConfig;
#[cfg(feature = "failure_injection")]
use failpoint_macros::set_failpoint;
use futures::FutureExt;
use nom_sql::Relation;
use postgres_native_tls::MakeTlsConnector;
#[cfg(feature = "failure_injection")]
use readyset_client::failpoints;
use readyset_client::replication::ReplicationOffset;
use readyset_client::{ReadySetError, ReadySetResult, TableOperation};
use readyset_errors::{invariant, set_failpoint_return_err};
use readyset_tracing::{debug, error, info, trace, warn};
use readyset_util::select;
use tokio_postgres as pgsql;

use super::ddl_replication::setup_ddl_replication;
use super::lsn::Lsn;
use super::wal_reader::{WalEvent, WalReader};
use super::{PostgresPosition, PUBLICATION_NAME, REPLICATION_SLOT};
use crate::db_util::error_is_slot_not_found;
use crate::noria_adapter::{Connector, ReplicationAction};
use crate::postgres_connector::wal::{TableErrorKind, WalError};

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
    /// Stores an event that was read but not handled
    peek: Option<(WalEvent, Lsn)>,
    /// If we just want to continue reading the log from a previous point
    next_position: Option<PostgresPosition>,
    /// The replication slot if was created for this connector
    pub(crate) replication_slot: Option<CreatedSlot>,
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
    pub(crate) xlogpos: i64,
    /// Database connected to or null.
    pub(crate) dbname: Option<String>,
}

/// The decoded response to `CREATE_REPLICATION_SLOT`
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct CreatedSlot {
    /// The name of the newly-created replication slot
    pub(crate) slot_name: String,
    /// The WAL location at which the slot became consistent. This is the earliest location
    /// from which streaming can start on this replication slot.
    pub(crate) consistent_point: i64,
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
    pub(crate) async fn connect<S: AsRef<str>>(
        mut pg_config: pgsql::Config,
        dbname: S,
        config: UpstreamConfig,
        next_position: Option<PostgresPosition>,
        tls_connector: MakeTlsConnector,
    ) -> ReadySetResult<Self> {
        if !config.disable_setup_ddl_replication {
            setup_ddl_replication(pg_config.clone(), tls_connector.clone()).await?;
        }
        pg_config.dbname(dbname.as_ref()).set_replication_database();

        let (client, connection) = pg_config.connect(tls_connector).await?;
        let connection_handle = tokio::spawn(connection);

        let mut connector = PostgresWalConnector {
            client,
            connection_handle,
            reader: None,
            peek: None,
            next_position,
            replication_slot: None,
        };

        if next_position.is_none() {
            connector.create_publication_and_slot().await?;
        }

        Ok(connector)
    }

    async fn create_publication_and_slot(&mut self) -> ReadySetResult<()> {
        let system = self.identify_system().await?;
        debug!(?system);

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
        self.drop_replication_slot(REPLICATION_SLOT).await?;

        match self.create_replication_slot(REPLICATION_SLOT, false).await {
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
    async fn next_event(&mut self) -> Result<(WalEvent, Lsn), WalError> {
        let PostgresWalConnector {
            reader,
            connection_handle,
            ..
        } = self;

        if let Some(reader) = reader.as_mut() {
            select! {
                ev = reader.next_event().fuse() => ev,
                err = connection_handle.fuse() => match err.unwrap() { // This unwrap is ok, because it is on the handle
                    Ok(_) => unreachable!(), // Unreachable because it runs in infinite loop unless errors
                    Err(err) => Err(WalError::ReadySetError(err.into())),
                }
            }
        } else {
            Err(WalError::ReadySetError(ReadySetError::ReplicationFailed(
                "Not started".to_string(),
            )))
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
        let row = self.one_row_query("IDENTIFY_SYSTEM", 4).await?;
        // We know we have 4 valid columns because `one_row_query` checks that, so can unwrap here
        let id = row.get(0).unwrap().to_string();
        let timeline: i8 = row.get(1).unwrap().parse().map_err(|_| {
            ReadySetError::ReplicationFailed("Unable to parse identify system".into())
        })?;
        let xlogpos = parse_wal(row.get(2).unwrap())?;
        let dbname = row.get(3).map(Into::into);

        Ok(ServerIdentity {
            id,
            timeline,
            xlogpos,
            dbname,
        })
    }

    /// Creates a new `PUBLICATION name FOR ALL TABLES`, to be able to recieve WAL on that slot.
    /// The user must have superuser privileges for that to work.
    async fn create_publication(&mut self, name: &str) -> ReadySetResult<()> {
        let query = format!("CREATE PUBLICATION {} FOR ALL TABLES", name);
        self.simple_query(&query).await?;
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

        let row = self.one_row_query(&query, 4).await?;

        let slot_name = row.get(0).unwrap().to_string(); // Can unwrap all because checked by `one_row_query`
        let consistent_point = parse_wal(row.get(1).unwrap())?;
        let snapshot_name = row.get(2).map(Into::into).unwrap();
        let output_plugin = row.get(3).map(Into::into).unwrap();
        debug!(
            slot_name,
            consistent_point, snapshot_name, output_plugin, "Created replication slot"
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
        let inner_client = self.client.inner();
        let wal_position = self.next_position.unwrap_or_default();
        let messages_support = if version >= 140000 {
            ", \"messages\" 'true'"
        } else {
            ""
        };

        debug!("Postgres version {version}");

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

    fn send_standy_status_update(&self, ack: PostgresPosition) -> ReadySetResult<()> {
        use bytes::{BufMut, BytesMut};

        // The difference between UNIX and Postgres epoch
        const J2000_EPOCH_GAP: u64 = 946_684_800_000_000;

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64
            - J2000_EPOCH_GAP;

        let pos = ack.lsn.0 + 1;

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
    pub(crate) async fn drop_replication_slot(&mut self, name: &str) -> ReadySetResult<()> {
        info!(slot = name, "Dropping replication slot if exists");
        let res = self
            .simple_query(&format!("DROP_REPLICATION_SLOT {}", name))
            .await;

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

    /// Perform a simple query that expects a singe row in response, check that the response is
    /// indeed one row, and contains exactly `n_cols` columns, then return that row
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

fn parse_wal(wal: &str) -> ReadySetResult<i64> {
    // Internally, an LSN is a 64-bit integer, representing a byte position in the write-ahead log
    // stream. It is printed as two hexadecimal numbers of up to 8 digits each, separated by a
    // slash; for example, 16/B374D848
    let (hi, lo) = wal
        .split_once('/')
        .ok_or_else(|| ReadySetError::ReplicationFailed(format!("Invalid wal {wal}")))?;
    let hi = i64::from_str_radix(hi, 16)
        .map_err(|e| ReadySetError::ReplicationFailed(format!("Invalid wal {e:?}")))?;
    let lo = i64::from_str_radix(lo, 16)
        .map_err(|e| ReadySetError::ReplicationFailed(format!("Invalid wal {e:?}")))?;
    Ok(hi << 32 | lo)
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
    ) -> ReadySetResult<(ReplicationAction, ReplicationOffset)> {
        set_failpoint_return_err!(failpoints::POSTGRES_REPLICATION_NEXT_ACTION);

        // Calling the ReadySet API is a bit expensive, therefore we try to queue as many actions
        // as possible before calling into the API. We therefore try to stop batching actions when
        // we hit this limit, BUT note that we may substantially exceed this limit in cases where
        // we receive many consecutive events that share the same LSN:
        const MAX_QUEUED_INDEPENDENT_ACTIONS: usize = 100;
        let mut cur_table = Relation {
            schema: None,
            name: "".into(),
        };
        let mut cur_lsn: PostgresPosition = 0.into();
        let mut actions = Vec::with_capacity(MAX_QUEUED_INDEPENDENT_ACTIONS);

        loop {
            debug_assert!(
                cur_table.schema.is_some() || cur_table.name.is_empty(),
                "We should either have no current table, or the current table should have a schema"
            );

            let (mut event, lsn) = match self.peek.take() {
                Some(event) => event,
                None => match self.next_event().await {
                    Ok(ev) => ev,
                    Err(WalError::TableError {
                        kind: TableErrorKind::UnsupportedTypeConversion { type_oid },
                        schema,
                        table,
                    }) => {
                        warn!(
                            type_oid,
                            schema = schema,
                            table = table,
                            "Ignoring write with value of unsupported type"
                        );
                        // ReadySet will skip replicate tables with unsupported types (e.g.
                        // Postgres's user defined types). RS will leverage the fallback instead.
                        continue;
                    }
                    Err(err) => {
                        return Err(err.into());
                    }
                },
            };

            // Try not to accumulate too many actions between calls, but if we're collecting a
            // series of events that reuse the same LSN we have to make sure they all end up in the
            // same batch:
            //
            // If we have no actions but our offset exceeds the given 'until', report our
            // position in the logs.
            if actions.len() >= MAX_QUEUED_INDEPENDENT_ACTIONS && lsn != cur_lsn.lsn {
                self.peek = Some((event, lsn));
                return Ok((
                    ReplicationAction::TableAction {
                        table: cur_table,
                        actions,
                        txid: None,
                    },
                    cur_lsn.into(),
                ));
            } else if let Some(until) = until && actions.is_empty() && ReplicationOffset::from(cur_lsn) >= *until {
                return Ok((ReplicationAction::LogPosition, cur_lsn.into()));
            }

            trace!(?event);

            // Check if next event is for another table, in which case we have to flush the events
            // accumulated for this table and store the next event in `peek`.
            match &mut event {
                WalEvent::Truncate { tables } => {
                    let (matching, mut other_tables) =
                        tables.drain(..).partition::<Vec<_>, _>(|(schema, table)| {
                            cur_table.schema.as_deref() == Some(schema.as_str())
                                && cur_table.name == table.as_str()
                        });

                    if actions.is_empty() {
                        invariant!(matching.is_empty());
                        if let Some((schema, name)) = other_tables.pop() {
                            if !other_tables.is_empty() {
                                self.peek = Some((
                                    WalEvent::Truncate {
                                        tables: other_tables,
                                    },
                                    lsn,
                                ));
                            }

                            actions.push(TableOperation::Truncate);
                            return Ok((
                                ReplicationAction::TableAction {
                                    table: Relation {
                                        schema: Some(schema.into()),
                                        name: name.into(),
                                    },
                                    actions,
                                    txid: None,
                                },
                                PostgresPosition::from(lsn).into(),
                            ));
                        } else {
                            // Empty truncate op
                            continue;
                        }
                    } else {
                        if !other_tables.is_empty() {
                            self.peek = Some((
                                WalEvent::Truncate {
                                    tables: other_tables,
                                },
                                lsn,
                            ));
                        }

                        if !matching.is_empty() {
                            actions.push(TableOperation::Truncate);
                        }

                        return Ok((
                            ReplicationAction::TableAction {
                                table: cur_table,
                                actions,
                                txid: None,
                            },
                            cur_lsn.into(),
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
                        self.peek = Some((event, lsn));
                        return Ok((
                            ReplicationAction::TableAction {
                                table: cur_table,
                                actions,
                                txid: None,
                            },
                            cur_lsn.into(),
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

            cur_lsn = lsn.into();

            match event {
                WalEvent::DdlEvent { ddl_event } => {
                    if actions.is_empty() {
                        return Ok((
                            ReplicationAction::DdlChange {
                                schema: ddl_event.schema().to_string(),
                                changes: vec![ddl_event.into_change()],
                            },
                            PostgresPosition::from(lsn).into(),
                        ));
                    } else {
                        self.peek = Some((WalEvent::DdlEvent { ddl_event }, lsn));
                        return Ok((
                            ReplicationAction::TableAction {
                                table: cur_table,
                                actions,
                                txid: None,
                            },
                            cur_lsn.into(),
                        ));
                    }
                }
                WalEvent::WantsKeepaliveResponse => {
                    self.send_standy_status_update(last_pos.into())?;
                }
                WalEvent::Commit => {
                    if !actions.is_empty() {
                        // On commit we flush, because there is no knowing when the next commit is
                        // coming
                        return Ok((
                            ReplicationAction::TableAction {
                                table: cur_table,
                                actions,
                                txid: None,
                            },
                            cur_lsn.into(),
                        ));
                    } else {
                        return Ok((ReplicationAction::LogPosition, cur_lsn.into()));
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
                    actions.push(TableOperation::Update { key, update: set })
                }
                WalEvent::Truncate { .. } => actions.push(TableOperation::Truncate),
            }
        }
    }
}
