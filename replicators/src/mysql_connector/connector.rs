use async_trait::async_trait;
use binlog::consts::{BinlogChecksumAlg, EventType};
use mysql::prelude::Queryable;
use mysql_async as mysql;
use mysql_common::binlog;
use mysql_common::proto::MySerialize;
use noria::ReplicationOffset;
use noria::{ReadySetError, ReadySetResult};
use std::convert::{TryFrom, TryInto};

use crate::noria_adapter::Connector;
use crate::noria_adapter::ReplicationAction;

use super::BinlogPosition;

const CHECKSUM_QUERY: &str = "SET @master_binlog_checksum='CRC32'";
const DEFAULT_SERVER_ID: u32 = u32::MAX - 55;

/// A connector that connects to a MySQL server and starts reading binlogs from a given position.
///
/// The server must be configured with `binlog_format` set to `row` and `binlog_row_image` set to `full`.
///
/// The connector user may optionally have the following permissions:
/// `SELECT` - to be able to perform a snapshot
/// `RELOAD` - to be able to flush tables and acquire locks for a snapshot (otherwise table level locks will be used)
/// `LOCK TABLES` - if unable to acquire a global lock, this permission is required for table level locks
/// `SHOW DATABASES` - to see databases for a snapshot
/// `REPLICATION SLAVE` - to be able to connect and read the binlog
/// `REPLICATION CLIENT` - to use SHOW MASTER STATUS, SHOW SLAVE STATUS, and SHOW BINARY LOGS;
///
/// The connector must also be assigned a unique `server_id` value
pub struct MySqlBinlogConnector {
    /// This is the underlying (regular) MySQL connection
    connection: mysql::Conn,
    /// Reader is a decoder for binlog events
    reader: binlog::EventStreamReader,
    /// The binlog "slave" must be assigned a unique `server_id` in the replica topology
    /// if one is not assigned we will use (u32::MAX - 55)
    server_id: Option<u32>,
    /// If we just want to continue reading the binlog from a previous point
    next_position: BinlogPosition,
    /// The list of schemas we are interested in, others will be filtered out
    schemas: Vec<String>,
    /// The GTID of the current transaction. Table modification events will have
    /// the current GTID attached if enabled in mysql.
    current_gtid: Option<u64>,
}

impl PartialOrd for BinlogPosition {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        // The log files are sequentially numbered using a .NNNNNN suffix. The index file has a suffix of .index.
        // All files share a common basename. The default binary log file-naming basename is "HOSTNAME-bin".
        if self.binlog_file == other.binlog_file {
            return self.position.partial_cmp(&other.position);
        }

        // This implementation assumes proper binlog filename format, and will return None on an invalid format.
        let (basename, suffix) = self.binlog_file.rsplit_once('.')?;
        let (other_basename, other_suffix) = other.binlog_file.rsplit_once('.')?;

        // If basename is different than we are talking about completely different logs
        if basename != other_basename {
            return None;
        }

        // Otherwise we compare the suffix, which if valid must be a valid number
        // probably u32 would suffice
        let suffix = suffix.parse::<u64>().ok()?;
        let other_suffix = other_suffix.parse::<u64>().ok()?;

        suffix.partial_cmp(&other_suffix)
    }
}

impl TryFrom<&BinlogPosition> for noria::ReplicationOffset {
    type Error = ReadySetError;

    /// `ReplicationOffset` is a filename and a u128 offset   
    /// We use the binlog basefile name as the filename, and we use the binlog suffix len for
    /// the top 5 bits, which can be as big as 31 digits in theory, but we only allow up to 17
    /// decimal digits, which is more than enough for the binlog spec. This is required to be
    /// able to properly format the integer back to string, including any leading zeroes.
    /// The following 59 bits are used for the numerical value of the suffix, finally the last
    /// 64 bits of the offset are the actual binlog offset.
    fn try_from(value: &BinlogPosition) -> Result<Self, Self::Error> {
        let (basename, suffix) = value.binlog_file.rsplit_once('.').ok_or_else(|| {
            ReadySetError::ReplicationFailed(format!("Invalid binlog name {}", value.binlog_file))
        })?;

        let suffix_len = suffix.len() as u128;

        if suffix_len > 17 {
            // 17 digit decimal number is the most we can fit into 59 bits
            return Err(ReadySetError::ReplicationFailed(format!(
                "Invalid binlog suffix {}",
                value.binlog_file
            )));
        }

        let suffix = suffix.parse::<u128>().map_err(|_| {
            ReadySetError::ReplicationFailed(format!("Invalid binlog suffix {}", value.binlog_file))
        })?;

        Ok(noria::ReplicationOffset {
            offset: (suffix_len << 123) + (suffix << 64) + (value.position as u128),
            replication_log_name: basename.to_string(),
        })
    }
}

impl TryFrom<BinlogPosition> for noria::ReplicationOffset {
    type Error = ReadySetError;

    fn try_from(value: BinlogPosition) -> Result<Self, Self::Error> {
        (&value).try_into()
    }
}

impl From<noria::ReplicationOffset> for BinlogPosition {
    fn from(val: noria::ReplicationOffset) -> Self {
        let suffix_len = (val.offset >> 123) as usize;
        let suffix = (val.offset >> 64) as u32;
        let position = val.offset as u32;

        // To get the binlog filename back we use `replication_log_name` as the basename and append the suffix
        // which is encoded in bits 64:123 of `offset`, and format it as a zero padded decimal integer with
        // `suffix_len` characters (encoded in the top 5 bits of the offset)
        BinlogPosition {
            binlog_file: format!("{0}.{1:02$}", val.replication_log_name, suffix, suffix_len),
            position,
        }
    }
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

        let mut buf = Vec::new();
        cmd.serialize(&mut buf);
        self.connection.write_command_raw(buf).await?;
        // Server will respond with OK.
        self.connection.read_packet().await?;
        Ok(())
    }

    /// After we have registered as a replica, we can request the binlog
    async fn request_binlog(&mut self) -> mysql::Result<()> {
        let cmd = mysql_common::packets::ComBinlogDump::new(self.server_id())
            .with_pos(self.next_position.position)
            .with_filename(self.next_position.binlog_file.as_bytes());

        let mut buf = Vec::new();
        cmd.serialize(&mut buf);
        self.connection.write_command_raw(buf).await?;
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
    pub async fn connect<S: Into<String>, O: Into<mysql::Opts>>(
        mysql_opts: O,
        schemas: Vec<S>,
        next_position: BinlogPosition,
        server_id: Option<u32>,
    ) -> ReadySetResult<Self> {
        let mut connector = MySqlBinlogConnector {
            connection: mysql::Conn::new(mysql_opts).await?,
            reader: binlog::EventStreamReader::new(binlog::consts::BinlogVersion::Version4),
            server_id,
            next_position,
            schemas: schemas.into_iter().map(|e| e.into()).collect(),
            current_gtid: None,
        };

        connector.register_as_replica().await?;
        connector.request_binlog().await?;

        Ok(connector)
    }

    /// Get the next raw binlog event
    async fn next_event(&mut self) -> mysql::Result<binlog::events::Event> {
        let packet = self.connection.read_packet().await?;
        // TODO: byte 0 of packet should be zero, unless EOF is reached, however we should never get one
        // without the NON_BLOCKING SQL flag set
        assert_eq!(packet.get(0), Some(&0));
        let event = self.reader.read(&packet[1..])?;
        assert!(Self::validate_event_checksum(&event)); // TODO: definitely should never fail a CRC check, but what to do if we do?
        Ok(event)
    }

    /// Check if the provided schema is in the list of schemas we are monitoring
    fn interested_in_schema(&self, schema: &str) -> bool {
        self.schemas.iter().any(|s| *s == schema)
    }

    /// Process binlog events until an actionable event occurs
    pub(crate) async fn next_action_inner(
        &mut self,
    ) -> mysql::Result<(ReplicationAction, &BinlogPosition)> {
        use mysql_common::binlog::events;

        loop {
            let binlog_event = self.next_event().await?;

            self.next_position.position = binlog_event.header().log_pos();

            match binlog_event
                .header()
                .event_type()
                .map_err(|ev| format!("Unknown binlog event type {}", ev))?
            {
                EventType::ROTATE_EVENT => {
                    // Written when mysqld switches to a new binary log file.
                    // This occurs when someone issues a FLUSH LOGS statement or the current binary log file becomes too large.
                    // The maximum size is determined by max_binlog_size.
                    let ev: events::RotateEvent = binlog_event.read_event()?;

                    self.next_position = BinlogPosition {
                        binlog_file: ev.name().to_string(),
                        // This should never happen, but better to panic than to get the wrong position
                        position: u32::try_from(ev.position()).unwrap(),
                    };
                }

                EventType::QUERY_EVENT => {
                    // Written when an updating statement is done.
                    let ev: events::QueryEvent = binlog_event.read_event()?;
                    if !self.interested_in_schema(ev.schema().as_ref()) {
                        continue;
                    }

                    if ev
                        .status_vars()
                        .get_status_var(binlog::consts::StatusVarKey::UpdatedDbNames)
                        .is_none()
                    {
                        // If the query does not affect the schema, just keep going
                        // TODO: Transactions begin with the `BEGIN` queries, but we do not currently support those
                        continue;
                    }

                    return Ok((
                        ReplicationAction::SchemaChange {
                            ddl: ev.query().to_string(),
                        },
                        &self.next_position,
                    ));
                }

                EventType::TABLE_MAP_EVENT => {
                    // Used for row-based binary logging. This event precedes each row operation event.
                    // It maps a table definition to a number, where the table definition consists of
                    // database and table names and column definitions. The purpose of this event is to
                    // enable replication when a table has different definitions on the master and slave.
                    // Row operation events that belong to the same transaction may be grouped into
                    // sequences, in which case each such sequence of events begins with a sequence of
                    // TABLE_MAP_EVENT events: one per table used by events in the sequence.
                    // Those events are implicitly handled by our lord and saviour `binlog::EventStreamReader`
                }

                EventType::WRITE_ROWS_EVENT => {
                    // This is the event we get on `INSERT INTO`
                    let ev: events::WriteRowsEvent = binlog_event.read_event()?;
                    // Retrieve the corresponding TABLE_MAP_EVENT
                    let tme = self
                        .reader
                        .get_tme(ev.table_id())
                        .ok_or("TME not found for WRITE_ROWS_EVENT")?;

                    if !self.interested_in_schema(tme.database_name().as_ref()) {
                        continue;
                    }

                    let mut inserted_rows = Vec::new();

                    for row in ev.rows(&tme) {
                        // For each row in the event we produce a vector of Noria types that represent that row
                        inserted_rows.push(noria::TableOperation::Insert(binlog_row_to_noria_row(
                            &row?.1.ok_or("Missing data in WRITE_ROWS_EVENT")?,
                            tme,
                        )?));
                    }

                    return Ok((
                        ReplicationAction::TableAction {
                            table: tme.table_name().to_string(),
                            actions: inserted_rows,
                            txid: self.current_gtid,
                        },
                        &self.next_position,
                    ));
                }

                EventType::UPDATE_ROWS_EVENT => {
                    // This is the event we get on `UPDATE`
                    let ev: events::UpdateRowsEvent = binlog_event.read_event()?;
                    // Retrieve the corresponding TABLE_MAP_EVENT
                    let tme = self
                        .reader
                        .get_tme(ev.table_id())
                        .ok_or(format!("TME not found for UPDATE_ROWS_EVENT {:?}", ev))?;

                    if !self.interested_in_schema(tme.database_name().as_ref()) {
                        continue;
                    }

                    let mut updated_rows = Vec::new();

                    for row in ev.rows(&tme) {
                        // For each row in the event we produce a pair of Noria table operations to
                        // delete the previous entry and insert the new one
                        let row = &row?;
                        updated_rows.push(noria::TableOperation::DeleteRow {
                            row: binlog_row_to_noria_row(
                                row.0.as_ref().ok_or(format!(
                                    "Missing before rows in UPDATE_ROWS_EVENT {:?}",
                                    row
                                ))?,
                                tme,
                            )?,
                        });

                        updated_rows.push(noria::TableOperation::Insert(binlog_row_to_noria_row(
                            row.1.as_ref().ok_or(format!(
                                "Missing after rows in UPDATE_ROWS_EVENT {:?}",
                                row
                            ))?,
                            tme,
                        )?));
                    }

                    return Ok((
                        ReplicationAction::TableAction {
                            table: tme.table_name().to_string(),
                            actions: updated_rows,
                            txid: self.current_gtid,
                        },
                        &self.next_position,
                    ));
                }

                EventType::DELETE_ROWS_EVENT => {
                    // This is the event we get on `ALTER TABLE`
                    let ev: events::DeleteRowsEvent = binlog_event.read_event()?;
                    // Retrieve the corresponding TABLE_MAP_EVENT
                    let tme = self
                        .reader
                        .get_tme(ev.table_id())
                        .ok_or(format!("TME not found for UPDATE_ROWS_EVENT {:?}", ev))?;

                    if !self.interested_in_schema(tme.database_name().as_ref()) {
                        continue;
                    }

                    let mut deleted_rows = Vec::new();

                    for row in ev.rows(&tme) {
                        // For each row in the event we produce a vector of Noria types that represent that row
                        deleted_rows.push(noria::TableOperation::DeleteRow {
                            row: binlog_row_to_noria_row(
                                &row?.0.ok_or("Missing data in DELETE_ROWS_EVENT")?,
                                tme,
                            )?,
                        });
                    }

                    return Ok((
                        ReplicationAction::TableAction {
                            table: tme.table_name().to_string(),
                            actions: deleted_rows,
                            txid: self.current_gtid,
                        },
                        &self.next_position,
                    ));
                }

                EventType::WRITE_ROWS_EVENT_V1 => unimplemented!(), // The V1 event numbers are used from 5.1.16 until mysql-5.6.
                EventType::UPDATE_ROWS_EVENT_V1 => unimplemented!(), // The V1 event numbers are used from 5.1.16 until mysql-5.6.
                EventType::DELETE_ROWS_EVENT_V1 => unimplemented!(), // The V1 event numbers are used from 5.1.16 until mysql-5.6.

                EventType::GTID_EVENT => {
                    // GTID stands for Global Transaction IDentifier It is composed of two parts:
                    // SID for Source Identifier, and GNO for Group Number. The basic idea is to
                    // Associate an identifier, the Global Transaction IDentifier or GTID, to every transaction.
                    // When a transaction is copied to a slave, re-executed on the slave, and written to the
                    // slave's binary log, the GTID is preserved.  When a slave connects to a master, the slave
                    // uses GTIDs instead of (file, offset)
                    // See also https://dev.mysql.com/doc/refman/8.0/en/replication-mode-change-online-concepts.html
                    let ev: events::GtidEvent = binlog_event.read_event()?;
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
                _ => {}
            }
        }
    }
}

fn binlog_val_to_noria_val(
    val: &mysql_common::value::Value,
    col_kind: mysql_common::constants::ColumnType,
    meta: &[u8],
) -> mysql::Result<noria::DataType> {
    // Not all values are coereced to the value expected by Noria directly

    use mysql_common::constants::ColumnType;

    let buf = match val {
        mysql_common::value::Value::Bytes(b) => b,
        _ => {
            return Ok(val
                .try_into()
                .map_err(|e| format!("Unable to coerce value {}", e))?)
        }
    };

    match (col_kind, meta) {
        (ColumnType::MYSQL_TYPE_TIMESTAMP2, &[0]) => {
            //https://github.com/blackbeam/rust_mysql_common/blob/408effed435c059d80a9e708bcfa5d974527f476/src/binlog/value.rs#L144
            // When meta is 0, `mysql_common` encodes this value as number of seconds (since UNIX EPOCH)
            let epoch = String::from_utf8_lossy(buf).parse::<i64>().unwrap(); // Can unwrap because we know the format is integer
            let time = chrono::naive::NaiveDateTime::from_timestamp(epoch, 0);
            Ok(time.try_into().unwrap()) // Can unwarp because we know maps derectly to noria type
        }
        (ColumnType::MYSQL_TYPE_TIMESTAMP2, _) => {
            // When meta is anything else, `mysql_common` encodes this value as number of seconds.microseconds (since UNIX EPOCH)
            let s = String::from_utf8_lossy(buf);
            let (secs, usecs) = s.split_once(".").unwrap(); // safe to unwrap because format is fixed
            let secs = secs.parse::<i64>().unwrap();
            let usecs = usecs.parse::<u32>().unwrap();
            let time = chrono::naive::NaiveDateTime::from_timestamp(secs, usecs * 32);
            Ok(time.try_into().unwrap()) // Can unwarp because we know maps derectly to noria type
        }
        _ => Ok(val
            .try_into()
            .map_err(|e| format!("Unable to coerce value {}", e))?),
    }
}

fn binlog_row_to_noria_row(
    binlog_row: &binlog::row::BinlogRow,
    tme: &binlog::events::TableMapEvent<'static>,
) -> mysql::Result<Vec<noria::DataType>> {
    let mut noria_row = Vec::with_capacity(binlog_row.len());

    for idx in 0..binlog_row.len() {
        let val = match binlog_row.as_ref(idx).unwrap() {
            binlog::value::BinlogValue::Value(val) => val,
            _ => {
                return Err(format!("Expected a value in WRITE_ROWS_EVENT {:?}", binlog_row).into())
            }
        };

        let (kind, meta) = (
            tme.get_column_type(idx)
                .map_err(|e| format!("Unable to get column type {}", e))?
                .unwrap(),
            tme.get_column_metadata(idx).unwrap(),
        );

        noria_row.push(binlog_val_to_noria_val(val, kind, meta)?);
    }
    Ok(noria_row)
}

#[async_trait]
impl Connector for MySqlBinlogConnector {
    async fn next_action(
        &mut self,
        _: ReplicationOffset,
    ) -> ReadySetResult<(ReplicationAction, ReplicationOffset)> {
        let (action, pos) = self.next_action_inner().await?;
        Ok((action, pos.try_into()?))
    }
}
