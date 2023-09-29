use std::convert::{TryFrom, TryInto};
use std::io;

use async_trait::async_trait;
use binlog::consts::{BinlogChecksumAlg, EventType};
use metrics::counter;
use mysql::binlog::events::StatusVarVal;
use mysql::binlog::jsonb::{self, JsonbToJsonError};
use mysql::prelude::Queryable;
use mysql_async as mysql;
use mysql_common::binlog;
use mysql_common::binlog::row::BinlogRow;
use mysql_common::binlog::value::BinlogValue;
use nom_sql::Relation;
use readyset_client::metrics::recorded;
use readyset_client::recipe::ChangeList;
use readyset_data::{DfValue, Dialect};
use readyset_errors::{internal, internal_err, ReadySetError, ReadySetResult};
use replication_offset::mysql::MySqlPosition;
use replication_offset::ReplicationOffset;
use tracing::{error, info, warn};

use crate::noria_adapter::{Connector, ReplicationAction};

const CHECKSUM_QUERY: &str = "SET @master_binlog_checksum='CRC32'";
const DEFAULT_SERVER_ID: u32 = u32::MAX - 55;

/// A connector that connects to a MySQL server and starts reading binlogs from a given position.
///
/// The server must be configured with `binlog_format` set to `row` and `binlog_row_image` set to
/// `full`.
///
/// The connector user may optionally have the following permissions:
/// * `BACKUP_ADMIN` - (optional) to perform LOCK INSTANCE FOR BACKUP, not available on RDS
/// * `SELECT` - to be able to perform a snapshot
/// * `LOCK TABLES` - this permission is required for table level locks
/// * `SHOW DATABASES` - to see databases for a snapshot
/// * `REPLICATION SLAVE` - to be able to connect and read the binlog
/// * `REPLICATION CLIENT` - to use SHOW MASTER STATUS, SHOW SLAVE STATUS, and SHOW BINARY LOGS;
///
/// The connector must also be assigned a unique `server_id` value
pub(crate) struct MySqlBinlogConnector {
    /// This is the underlying (regular) MySQL connection
    connection: mysql::Conn,
    /// Reader is a decoder for binlog events
    reader: binlog::EventStreamReader,
    /// The binlog "slave" must be assigned a unique `server_id` in the replica topology
    /// if one is not assigned we will use (u32::MAX - 55)
    server_id: Option<u32>,
    /// If we just want to continue reading the binlog from a previous point
    next_position: MySqlPosition,
    /// The GTID of the current transaction. Table modification events will have
    /// the current GTID attached if enabled in mysql.
    current_gtid: Option<u64>,
    /// Whether to log statements received by the connector
    enable_statement_logging: bool,
}

impl MySqlBinlogConnector {
    /// The binlog replica must be assigned a unique `server_id` in the replica topology
    /// if one is not assigned we will use (u32::MAX - 55)
    fn server_id(&self) -> u32 {
        self.server_id.unwrap_or(DEFAULT_SERVER_ID)
    }

    /// In order to request a binlog, we must first register as a replica, and let the primary
    /// know what type of checksum we support (NONE and CRC32 are the options), NONE seems to work
    /// but others use CRC32 🤷‍♂️
    async fn register_as_replica(&mut self) -> mysql::Result<()> {
        self.connection.query_drop(CHECKSUM_QUERY).await?;

        let cmd = mysql_common::packets::ComRegisterSlave::new(self.server_id());
        self.connection.write_command(&cmd).await?;
        // Server will respond with OK.
        self.connection.read_packet().await?;
        Ok(())
    }

    /// After we have registered as a replica, we can request the binlog
    async fn request_binlog(&mut self) -> mysql::Result<()> {
        info!(next_position = %self.next_position, "Starting binlog replication");
        let filename = self.next_position.binlog_file_name().to_string();

        // If the next position is greater than u32::MAX, we need to re-snapshot
        if self.next_position.position > u64::from(u32::MAX) {
            Err(mysql_async::Error::Other(Box::new(
                ReadySetError::FullResnapshotNeeded,
            )))?;
        }
        let cmd = mysql_common::packets::ComBinlogDump::new(self.server_id())
            .with_pos(
                self.next_position
                    .position
                    .try_into()
                    .expect("Impossible binlog start position. Please re-snapshot."),
            )
            .with_filename(filename.as_bytes());

        self.connection.write_command(&cmd).await?;
        self.connection.read_packet().await?;
        Ok(())
    }

    /// Compute the checksum of the event and compare to the supplied checksum
    fn validate_event_checksum(event: &binlog::events::Event) -> bool {
        if let Ok(Some(BinlogChecksumAlg::BINLOG_CHECKSUM_ALG_CRC32)) =
            event.footer().get_checksum_alg()
        {
            if let Some(checksum) = event.checksum() {
                return u32::from_le_bytes(checksum)
                    == event.calc_checksum(BinlogChecksumAlg::BINLOG_CHECKSUM_ALG_CRC32);
            }
            return false;
        }

        true
    }

    /// Connect to a given MySQL database and subscribe to the binlog
    pub(crate) async fn connect<O: Into<mysql::Opts>>(
        mysql_opts: O,
        next_position: MySqlPosition,
        server_id: Option<u32>,
        enable_statement_logging: bool,
    ) -> ReadySetResult<Self> {
        let mut connector = MySqlBinlogConnector {
            connection: mysql::Conn::new(mysql_opts).await?,
            reader: binlog::EventStreamReader::new(binlog::consts::BinlogVersion::Version4),
            server_id,
            next_position,
            current_gtid: None,
            enable_statement_logging,
        };

        connector.register_as_replica().await?;
        let binlog_request = connector.request_binlog().await;
        match binlog_request {
            Ok(()) => (),
            Err(mysql_async::Error::Server(ref err))
                if mysql_srv::ErrorKind::from(err.code)
                    == mysql_srv::ErrorKind::ER_MASTER_FATAL_ERROR_READING_BINLOG =>
            {
                // Requested binlog is not available, we need to re-snapshot
                error!(error = %err, "Failed to request binlog");
                return Err(ReadySetError::FullResnapshotNeeded);
            }
            Err(mysql_async::Error::Other(ref err))
                if err.downcast_ref::<ReadySetError>()
                    == Some(&ReadySetError::FullResnapshotNeeded) =>
            {
                error!(error = %err, "Failed to request binlog");
                return Err(ReadySetError::FullResnapshotNeeded);
            }
            Err(err) => {
                internal!("Failed to request binlog: {err}")
            }
        }
        Ok(connector)
    }

    /// Get the next raw binlog event
    async fn next_event(&mut self) -> mysql::Result<binlog::events::Event> {
        let packet = self.connection.read_packet().await?;
        if let Some(first_byte) = packet.first() {
            // We should only see EOF/(254) if the mysql upstream has gone away or the
            // NON_BLOCKING SQL flag is set,
            // otherwise the first byte should always be 0
            if *first_byte != 0 && *first_byte != 254 {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    format!("Received invalid first byte ({first_byte}) from MySQL server"),
                )
                .into());
            } else if *first_byte == 254 {
                return Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    format!("Received EOF ({first_byte}) from MySQL server"),
                )
                .into());
            }
        }
        let event = self.reader.read(&packet[1..])?;
        assert!(Self::validate_event_checksum(&event)); // TODO: definitely should never fail a CRC check, but what to do if we do?
        Ok(event)
    }

    /// Process binlog events until an actionable event occurs.
    ///
    /// # Arguments
    ///
    /// * `until` - an optional position in the binlog to stop at, even if no actionable
    /// occurred. In that case the action [`ReplicationAction::LogPosition`] is returned.
    pub(crate) async fn next_action_inner(
        &mut self,
        until: Option<&ReplicationOffset>,
    ) -> mysql::Result<(ReplicationAction, &MySqlPosition)> {
        use mysql_common::binlog::events;

        loop {
            let binlog_event = self.next_event().await?;

            if u64::from(binlog_event.header().log_pos()) < self.next_position.position
                && self.next_position.position + u64::from(binlog_event.header().event_size())
                    > u64::from(u32::MAX)
            {
                self.next_position.position =
                    u64::from(u32::MAX) + 1 + u64::from(binlog_event.header().log_pos());
            } else {
                self.next_position.position = u64::from(binlog_event.header().log_pos());
            }

            match binlog_event.header().event_type().map_err(|ev| {
                mysql_async::Error::Other(Box::new(internal_err!(
                    "Unknown binlog event type {}",
                    ev
                )))
            })? {
                EventType::ROTATE_EVENT => {
                    // Written when mysqld switches to a new binary log file.
                    // This occurs when someone issues a FLUSH LOGS statement or the current binary
                    // log file becomes too large. The maximum size is
                    // determined by max_binlog_size.
                    let ev: events::RotateEvent = binlog_event.read_event()?;
                    if self.enable_statement_logging {
                        info!(target: "replicator_statement", "{:?}", ev);
                    }

                    self.next_position = MySqlPosition::from_file_name_and_position(
                        ev.name().to_string(),
                        ev.position(),
                    )
                    .map_err(|e| {
                        mysql_async::Error::Other(Box::new(internal_err!(
                            "Failed to create MySqlPosition: {e}"
                        )))
                    })?;

                    return Ok((ReplicationAction::LogPosition, &self.next_position));
                }

                EventType::QUERY_EVENT => {
                    // Written when an updating statement is done.
                    let ev: events::QueryEvent = binlog_event.read_event()?;
                    if self.enable_statement_logging {
                        info!(target: "replicator_statement", "{:?}", ev);
                    }

                    let schema = match ev
                        .status_vars()
                        .get_status_var(binlog::consts::StatusVarKey::UpdatedDbNames)
                        .as_ref()
                        .and_then(|v| v.get_value().ok())
                    {
                        Some(StatusVarVal::UpdatedDbNames(names)) if !names.is_empty() => {
                            // IMPORTANT: For some statements there can be more than one update db,
                            // for example `DROP TABLE db1.tbl, db2.table;` Will have `db1` and
                            // `db2` listed, however we only need the schema to filter out
                            // `CREATE TABLE` and `ALTER TABLE` and those always change only one DB.
                            names.first().unwrap().as_str().to_string()
                        }
                        // If the query does not affect the schema, just keep going
                        // TODO: Transactions begin with the `BEGIN` queries, but we do not
                        // currently support those
                        _ => continue,
                    };

                    let changes = match ChangeList::from_str(&ev.query(), Dialect::DEFAULT_MYSQL) {
                        Ok(changelist) => changelist.changes,
                        Err(error) => {
                            warn!(%error, "Error extending recipe, DDL statement will not be used");
                            counter!(recorded::REPLICATOR_FAILURE, 1u64);
                            continue;
                        }
                    };

                    return Ok((
                        ReplicationAction::DdlChange { schema, changes },
                        &self.next_position,
                    ));
                }

                ev @ EventType::TABLE_MAP_EVENT => {
                    // Used for row-based binary logging. This event precedes each row operation
                    // event. It maps a table definition to a number, where the
                    // table definition consists of database and table names and
                    // column definitions. The purpose of this event is to
                    // enable replication when a table has different definitions on the master and
                    // slave. Row operation events that belong to the same
                    // transaction may be grouped into sequences, in which case
                    // each such sequence of events begins with a sequence of
                    // TABLE_MAP_EVENT events: one per table used by events in the sequence.
                    // Those events are implicitly handled by our lord and saviour
                    // `binlog::EventStreamReader`
                    if self.enable_statement_logging {
                        info!(target: "replicator_statement", "unhandled event: {:?}", ev);
                    }
                }

                EventType::WRITE_ROWS_EVENT => {
                    // This is the event we get on `INSERT INTO`
                    let ev: events::WriteRowsEvent = binlog_event.read_event()?;
                    if self.enable_statement_logging {
                        info!(target: "replicator_statement", "{:?}", ev);
                    }
                    // Retrieve the corresponding TABLE_MAP_EVENT
                    let tme = self.reader.get_tme(ev.table_id()).ok_or_else(|| {
                        mysql_async::Error::Other(Box::new(internal_err!(
                            "TME not found for WRITE_ROWS_EVENT"
                        )))
                    })?;

                    let mut inserted_rows = Vec::new();

                    for row in ev.rows(tme) {
                        // For each row in the event we produce a vector of ReadySet types that
                        // represent that row
                        inserted_rows.push(readyset_client::TableOperation::Insert(
                            binlog_row_to_noria_row(
                                &row?.1.ok_or_else(|| {
                                    mysql_async::Error::Other(Box::new(internal_err!(
                                        "Missing data in WRITE_ROWS_EVENT"
                                    )))
                                })?,
                                tme,
                            )?,
                        ));
                    }

                    return Ok((
                        ReplicationAction::TableAction {
                            table: Relation {
                                schema: Some(tme.database_name().into()),
                                name: tme.table_name().into(),
                            },
                            actions: inserted_rows,
                            txid: self.current_gtid,
                        },
                        &self.next_position,
                    ));
                }

                EventType::UPDATE_ROWS_EVENT => {
                    // This is the event we get on `UPDATE`
                    let ev: events::UpdateRowsEvent = binlog_event.read_event()?;
                    if self.enable_statement_logging {
                        info!(target: "replicator_statement", "{:?}", ev);
                    }
                    // Retrieve the corresponding TABLE_MAP_EVENT
                    let tme = self.reader.get_tme(ev.table_id()).ok_or_else(|| {
                        mysql_async::Error::Other(Box::new(internal_err!(
                            "TME not found for UPDATE_ROWS_EVENT {:?}",
                            ev
                        )))
                    })?;

                    let mut updated_rows = Vec::new();

                    for row in ev.rows(tme) {
                        // For each row in the event we produce a pair of ReadySet table operations
                        // to delete the previous entry and insert the new
                        // one
                        let row = &row?;
                        updated_rows.push(readyset_client::TableOperation::DeleteRow {
                            row: binlog_row_to_noria_row(
                                row.0.as_ref().ok_or_else(|| {
                                    mysql_async::Error::Other(Box::new(internal_err!(
                                        "Missing before rows in UPDATE_ROWS_EVENT {:?}",
                                        row
                                    )))
                                })?,
                                tme,
                            )?,
                        });

                        updated_rows.push(readyset_client::TableOperation::Insert(
                            binlog_row_to_noria_row(
                                row.1.as_ref().ok_or_else(|| {
                                    mysql_async::Error::Other(Box::new(internal_err!(
                                        "Missing after rows in UPDATE_ROWS_EVENT {:?}",
                                        row
                                    )))
                                })?,
                                tme,
                            )?,
                        ));
                    }

                    return Ok((
                        ReplicationAction::TableAction {
                            table: Relation {
                                schema: Some(tme.database_name().into()),
                                name: tme.table_name().into(),
                            },
                            actions: updated_rows,
                            txid: self.current_gtid,
                        },
                        &self.next_position,
                    ));
                }

                EventType::DELETE_ROWS_EVENT => {
                    // This is the event we get on `ALTER TABLE`
                    let ev: events::DeleteRowsEvent = binlog_event.read_event()?;
                    if self.enable_statement_logging {
                        info!(target: "replicator_statement", "{:?}", ev);
                    }
                    // Retrieve the corresponding TABLE_MAP_EVENT
                    let tme = self.reader.get_tme(ev.table_id()).ok_or_else(|| {
                        mysql_async::Error::Other(Box::new(internal_err!(
                            "TME not found for UPDATE_ROWS_EVENT {:?}",
                            ev
                        )))
                    })?;

                    let mut deleted_rows = Vec::new();

                    for row in ev.rows(tme) {
                        // For each row in the event we produce a vector of ReadySet types that
                        // represent that row
                        deleted_rows.push(readyset_client::TableOperation::DeleteRow {
                            row: binlog_row_to_noria_row(
                                &row?.0.ok_or_else(|| {
                                    mysql_async::Error::Other(Box::new(internal_err!(
                                        "Missing data in DELETE_ROWS_EVENT"
                                    )))
                                })?,
                                tme,
                            )?,
                        });
                    }

                    return Ok((
                        ReplicationAction::TableAction {
                            table: Relation {
                                schema: Some(tme.database_name().into()),
                                name: tme.table_name().into(),
                            },
                            actions: deleted_rows,
                            txid: self.current_gtid,
                        },
                        &self.next_position,
                    ));
                }

                EventType::WRITE_ROWS_EVENT_V1 => unimplemented!(), /* The V1 event numbers are */
                // used from 5.1.16 until
                // mysql-5.6.
                EventType::UPDATE_ROWS_EVENT_V1 => unimplemented!(), /* The V1 event numbers are */
                // used from 5.1.16 until
                // mysql-5.6.
                EventType::DELETE_ROWS_EVENT_V1 => unimplemented!(), /* The V1 event numbers are */
                // used from 5.1.16 until
                // mysql-5.6.
                EventType::GTID_EVENT => {
                    // GTID stands for Global Transaction Identifier It is composed of two parts:
                    // SID for Source Identifier, and GNO for Group Number. The basic idea is to
                    // Associate an identifier, the Global Transaction Identifier or GTID, to every
                    // transaction. When a transaction is copied to a slave,
                    // re-executed on the slave, and written to the
                    // slave's binary log, the GTID is preserved.  When a slave connects to a
                    // master, the slave uses GTIDs instead of (file, offset)
                    // See also https://dev.mysql.com/doc/refman/8.0/en/replication-mode-change-online-concepts.html
                    let ev: events::GtidEvent = binlog_event.read_event()?;
                    if self.enable_statement_logging {
                        info!(target: "replicator_statement", "{:?}", ev);
                    }
                    self.current_gtid = Some(ev.gno());
                }

                /*

                EventType::ANONYMOUS_GTID_EVENT => {}

                EventType::XID_EVENT => {
                    // Generated for a commit of a transaction that modifies one or more tables of an XA-capable
                    // storage engine. Normal transactions are implemented by sending a QUERY_EVENT containing a
                    // BEGIN statement and a QUERY_EVENT containing a COMMIT statement
                    // (or a ROLLBACK statement if the transaction is rolled back).
                }

                EventType::START_EVENT_V3 // Old version of FORMAT_DESCRIPTION_EVENT
                | EventType::FORMAT_DESCRIPTION_EVENT // A descriptor event that is written to the beginning of each binary log file. This event is used as of MySQL 5.0; it supersedes START_EVENT_V3.
                | EventType::STOP_EVENT // Written when mysqld stops
                | EventType::INCIDENT_EVENT // The event is used to inform the slave that something out of the ordinary happened on the master that might cause the database to be in an inconsistent state.
                | EventType::HEARTBEAT_EVENT => {} // The event is originated by master's dump thread and sent straight to slave without being logged. Slave itself does not store it in relay log but rather uses a data for immediate checks and throws away the event.

                EventType::UNKNOWN_EVENT | EventType::SLAVE_EVENT => {} // Ignored events

                EventType::INTVAR_EVENT => {} // Written every time a statement uses an AUTO_INCREMENT column or the LAST_INSERT_ID() function; precedes other events for the statement. This is written only before a QUERY_EVENT and is not used with row-based logging.
                EventType::LOAD_EVENT => {} // Used for LOAD DATA INFILE statements in MySQL 3.23.
                EventType::CREATE_FILE_EVENT => {} // Used for LOAD DATA INFILE statements in MySQL 4.0 and 4.1.
                EventType::APPEND_BLOCK_EVENT => {} // Used for LOAD DATA INFILE statements as of MySQL 4.0.
                EventType::EXEC_LOAD_EVENT => {} // Used for LOAD DATA INFILE statements in 4.0 and 4.1.
                EventType::DELETE_FILE_EVENT => {} // Used for LOAD DATA INFILE statements as of MySQL 4.0.
                EventType::NEW_LOAD_EVENT => {} // Used for LOAD DATA INFILE statements in MySQL 4.0 and 4.1.
                EventType::RAND_EVENT => {} // Written every time a statement uses the RAND() function; precedes other events for the statement. Indicates the seed values to use for generating a random number with RAND() in the next statement. This is written only before a QUERY_EVENT and is not used with row-based logging.
                EventType::USER_VAR_EVENT => {} // Written every time a statement uses a user variable; precedes other events for the statement. Indicates the value to use for the user variable in the next statement. This is written only before a QUERY_EVENT and is not used with row-based logging.

                EventType::BEGIN_LOAD_QUERY_EVENT => {} // Used for LOAD DATA INFILE statements as of MySQL 5.0.
                EventType::EXECUTE_LOAD_QUERY_EVENT => {} // Used for LOAD DATA INFILE statements as of MySQL 5.0.

                EventType::PRE_GA_WRITE_ROWS_EVENT => {} // Obsolete version of WRITE_ROWS_EVENT.
                EventType::PRE_GA_UPDATE_ROWS_EVENT => {} // Obsolete version of UPDATE_ROWS_EVENT.
                EventType::PRE_GA_DELETE_ROWS_EVENT => {} // Obsolete version of DELETE_ROWS_EVENT.

                EventType::IGNORABLE_EVENT => {} // In some situations, it is necessary to send over ignorable data to the slave: data that a slave can handle in case there is code for handling it, but which can be ignored if it is not recognized.
                EventType::ROWS_QUERY_EVENT => {} // Query that caused the following ROWS_EVENT

                EventType::PREVIOUS_GTIDS_EVENT => {}
                EventType::TRANSACTION_CONTEXT_EVENT => {}
                EventType::VIEW_CHANGE_EVENT => {}
                EventType::XA_PREPARE_LOG_EVENT => {}
                EventType::PARTIAL_UPDATE_ROWS_EVENT => {}
                EventType::ENUM_END_EVENT => {}
                */
                ev => {
                    if self.enable_statement_logging {
                        info!(target: "replicator_statement", "unhandled event: {:?}", ev);
                    }
                }
            }

            // We didn't get an actionable event, but we still need to check that we haven't reached
            // the until limit
            if let Some(limit) = until {
                let limit = MySqlPosition::try_from(limit).expect("Valid binlog limit");
                if self.next_position >= limit {
                    return Ok((ReplicationAction::LogPosition, &self.next_position));
                }
            }
        }
    }
}

fn binlog_val_to_noria_val(
    val: &mysql_common::value::Value,
    col_kind: mysql_common::constants::ColumnType,
    meta: &[u8],
) -> mysql::Result<DfValue> {
    // Not all values are coerced to the value expected by ReadySet directly

    use mysql_common::constants::ColumnType;

    let buf = match val {
        mysql_common::value::Value::Bytes(b) => b,
        _ => {
            return val.try_into().map_err(|e| {
                mysql_async::Error::Other(Box::new(internal_err!("Unable to coerce value {}", e)))
            })
        }
    };

    match (col_kind, meta) {
        (ColumnType::MYSQL_TYPE_TIMESTAMP2, &[0]) => {
            //https://github.com/blackbeam/rust_mysql_common/blob/408effed435c059d80a9e708bcfa5d974527f476/src/binlog/value.rs#L144
            // When meta is 0, `mysql_common` encodes this value as number of seconds (since UNIX
            // EPOCH)
            let epoch = String::from_utf8_lossy(buf).parse::<i64>().unwrap(); // Can unwrap because we know the format is integer
            if epoch == 0 {
                // The 0 epoch is reserved for the '0000-00-00 00:00:00' timestamp, which we
                // currently set to None
                return Ok(DfValue::None);
            }
            let time = chrono::naive::NaiveDateTime::from_timestamp(epoch, 0);
            // Can unwrap because we know it maps directly to [`DfValue`]
            Ok(time.try_into().unwrap())
        }
        (ColumnType::MYSQL_TYPE_TIMESTAMP2, _) => {
            // When meta is anything else, `mysql_common` encodes this value as number of
            // seconds.microseconds (since UNIX EPOCH)
            let s = String::from_utf8_lossy(buf);
            let (secs, usecs) = s.split_once('.').unwrap(); // safe to unwrap because format is fixed
            let secs = secs.parse::<i64>().unwrap();
            let usecs = usecs.parse::<u32>().unwrap();
            let time = chrono::naive::NaiveDateTime::from_timestamp(secs, usecs * 32);
            // Can wrap because we know this maps directly to [`DfValue`]
            Ok(time.try_into().unwrap())
        }
        _ => Ok(val.try_into().map_err(|e| {
            mysql_async::Error::Other(Box::new(internal_err!("Unable to coerce value {}", e)))
        })?),
    }
}

fn binlog_row_to_noria_row(
    binlog_row: &BinlogRow,
    tme: &binlog::events::TableMapEvent<'static>,
) -> mysql::Result<Vec<DfValue>> {
    (0..binlog_row.len())
        .map(|idx| {
            match binlog_row.as_ref(idx).unwrap() {
                BinlogValue::Value(val) => {
                    let (kind, meta) = (
                        tme.get_column_type(idx)
                            .map_err(|e| {
                                mysql_async::Error::Other(Box::new(internal_err!(
                                    "Unable to get column type {}",
                                    e
                                )))
                            })?
                            .unwrap(),
                        tme.get_column_metadata(idx).unwrap(),
                    );
                    binlog_val_to_noria_val(val, kind, meta)
                }
                BinlogValue::Jsonb(val) => {
                    let json: Result<serde_json::Value, _> = val.clone().try_into(); // urgh no TryFrom impl
                    match json {
                        Ok(val) => Ok(DfValue::from(val.to_string())),
                        Err(JsonbToJsonError::Opaque) => match val {
                            jsonb::Value::Opaque(opaque_val) => {
                                // As far as I can *tell* Opaque is just a raw JSON string, which we
                                // can just translate into a DfValue as JSON directly without going
                                // through serde_json::Value first.
                                Ok(DfValue::from(opaque_val.data().as_ref()))
                            }
                            _ => {
                                #[allow(clippy::unreachable)] // actually unreachable
                                {
                                    unreachable!("Opaque error only returned for opaque values")
                                }
                            }
                        },
                        Err(JsonbToJsonError::InvalidUtf8(err)) => {
                            Err(mysql_async::Error::Other(Box::new(internal_err!("{err}"))))
                        }
                        Err(JsonbToJsonError::InvalidJsonb(e)) => Err(e.into()),
                    }
                }
                _ => Err(mysql_async::Error::Other(Box::new(internal_err!(
                    "Expected a value in WRITE_ROWS_EVENT",
                )))),
            }
        })
        .collect()
}

#[async_trait]
impl Connector for MySqlBinlogConnector {
    async fn next_action(
        &mut self,
        _: &ReplicationOffset,
        until: Option<&ReplicationOffset>,
    ) -> ReadySetResult<(ReplicationAction, ReplicationOffset)> {
        let (action, pos) = self.next_action_inner(until).await?;
        Ok((action, pos.into()))
    }
}
