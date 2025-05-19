use core::str;
use std::collections::HashMap;
use std::convert::{TryFrom, TryInto};
use std::sync::Arc;

use async_trait::async_trait;
use atoi::atoi;
use binlog::consts::{BinlogChecksumAlg, EventType};
use metrics::counter;
use mysql::prelude::Queryable;
use mysql_async as mysql;
use mysql_common::binlog::events::{OptionalMetaExtractor, StatusVarVal, TableMapEvent};
use mysql_common::binlog::jsonb::{self, JsonbToJsonError};
use mysql_common::binlog::jsonb::{
    Array, ComplexValue, Large, Object, OpaqueValue, Small, StorageFormat,
};
use mysql_common::binlog::row::BinlogRow;
use mysql_common::binlog::value::BinlogValue;
use mysql_common::collations::{Collation, CollationId};
use mysql_common::constants::ColumnType;
use mysql_common::{binlog, Value};
use mysql_srv::ColumnFlags;
use readyset_data::encoding::{mysql_character_set_name_to_collation_id, Encoding};
use rust_decimal::Decimal;
use serde_json::Map;
use tracing::{error, info, warn};

use readyset_client::metrics::recorded;
use readyset_client::recipe::changelist::Change;
use readyset_client::recipe::ChangeList;
use readyset_client::{Modification, ReadySetHandle, TableOperation};
use readyset_data::{Collation as RsCollation, DfValue, Dialect, TimestampTz};
use readyset_errors::{internal, internal_err, unsupported_err, ReadySetError, ReadySetResult};
use readyset_sql::ast::{
    AlterTableStatement, CollationName, CreateTableBody, CreateTableOption, NonReplicatedRelation,
    NotReplicatedReason, Relation, SqlIdentifier, SqlQuery,
};
use replication_offset::mysql::MySqlPosition;
use replication_offset::ReplicationOffset;

use crate::mysql_connector::utils::{mysql_pad_binary_column, mysql_pad_char_column};
use crate::noria_adapter::{Connector, ReplicationAction};
use crate::table_filter::TableFilter;

use super::utils::get_mysql_version;

const DEFAULT_SERVER_ID: u32 = u32::MAX - 55;
const MAX_POSITION_TIME: u64 = 10;

macro_rules! binlog_err {
    ($connector:expr, $inner:expr) => {
        binlog_err!($connector, None::<&TableMapEvent>, $inner)
    };
    ($connector:expr, $table:expr, $inner:expr) => {{
        let table: Option<&TableMapEvent> = $table;
        let (table_name_msg, tme_msg) = if let Some(tme) = table {
            (
                format!(" for table {}.{}", tme.database_name(), tme.table_name()),
                format!("; {tme:?}"),
            )
        } else {
            ("".to_string(), "".to_string())
        };
        let gtid_message = if let Some(gtid) = $connector.current_gtid {
            format!(" at GTID {}", gtid)
        } else {
            "".to_string()
        };
        ReadySetError::ReplicationFailed(format!(
            "Binlog error before position {}{}{} at {}:{}:{}: {}{}",
            $connector.next_position,
            gtid_message,
            table_name_msg,
            std::file!(),
            std::line!(),
            std::column!(),
            $inner,
            tme_msg
        ))
    }};
}

/// Wraps the provided operation with error handling using `binlog_err!` macro.
///
/// This convenience macro executes an operation and wraps any generated error with context
/// information about the current binlog position, and returns the error. It should replace usage of
/// the `?` operator in this file.
macro_rules! handle_err {
    ($connector:expr, $op:expr) => {
        match $op {
            Ok(val) => val,
            Err(e) => return Err(binlog_err!($connector, None, e)),
        }
    };
    ($connector:expr, $table:expr, $op:expr) => {
        match $op {
            Ok(val) => val,
            Err(e) => return Err(binlog_err!($connector, Some($table), e)),
        }
    };
}

type TableMetadata = (Vec<Option<u16>>, Vec<Option<bool>>);

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
    /// A handle to the Readyset instance; used for retrieving schemas in MySQL 5.7. See [`MySqlBinlogConnector::table_schemas`].
    noria: ReadySetHandle,
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
    /// Timestamp of the last reported position. This is use to ensure we keep the distance
    /// between min/max position as short as possible.
    last_reported_pos_ts: std::time::Instant,
    /// Table filter
    table_filter: TableFilter,
    /// A cache of `CREATE TABLE` statements retrieved from the controller. This is only populated
    /// and referenced if it is not otherwise possible to determine the character set for a text
    /// column, which happens in MySQL 5.7.
    table_schemas: HashMap<Relation, CreateTableBody>,
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
        let query = match get_mysql_version(&mut self.connection).await {
            Ok(version) => {
                if version >= 80400 {
                    // MySQL 8.4.0 and above
                    "SET @source_binlog_checksum='CRC32'"
                } else {
                    // MySQL 8.3.0 and below
                    "SET @master_binlog_checksum='CRC32'"
                }
            }
            Err(err) => {
                return Err(err);
            }
        };
        self.connection.query_drop(query).await?;

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
        noria: ReadySetHandle,
        mysql_opts: O,
        next_position: MySqlPosition,
        server_id: Option<u32>,
        enable_statement_logging: bool,
        table_filter: TableFilter,
    ) -> ReadySetResult<Self> {
        let mut connector = MySqlBinlogConnector {
            noria,
            connection: mysql::Conn::new(mysql_opts).await?,
            reader: binlog::EventStreamReader::new(binlog::consts::BinlogVersion::Version4),
            server_id,
            next_position,
            current_gtid: None,
            enable_statement_logging,
            last_reported_pos_ts: std::time::Instant::now()
                - std::time::Duration::from_secs(MAX_POSITION_TIME),
            table_filter,
            table_schemas: Default::default(),
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
    async fn next_event(&mut self) -> ReadySetResult<binlog::events::Event> {
        let packet = handle_err!(self, self.connection.read_packet().await);
        if let Some(first_byte) = packet.first() {
            // We should only see EOF/(254) if the mysql upstream has gone away or the
            // NON_BLOCKING SQL flag is set,
            // otherwise the first byte should always be 0
            if *first_byte != 0 && *first_byte != 254 {
                return Err(binlog_err!(
                    self,
                    None,
                    format!("Received invalid first byte ({first_byte}) from MySQL server")
                ));
            } else if *first_byte == 254 {
                return Err(binlog_err!(
                    self,
                    None,
                    format!("Received EOF ({first_byte}) from MySQL server")
                ));
            }
        }
        let event = handle_err!(self, self.reader.read(&packet[1..])).unwrap();
        // TODO: definitely should never fail a CRC check, but what to do if we do?
        assert!(Self::validate_event_checksum(&event));
        Ok(event)
    }

    /// Maps partial binlog row values to their correct positions in a full row vector.
    ///
    /// When binlog row is partial (minimal row), we receive a binlog row with only
    /// the columns that have been set, like `(col1, col2, col3, col4)`, might come as
    /// `col[@2,@4] / val [x, y]`. This function maps the values to their correct positions
    /// in the row and leave a [`DfValue::Default`] for the rest. The replacement of the
    /// default values is done at [`DfValue::maybe_apply_default`].
    ///
    /// # Arguments
    /// * `row_data` - The binlog row data containing partial column information
    /// * `tme` - Table map event containing table metadata
    ///
    /// # Returns
    /// A vector of DfValues with values mapped to their correct positions and defaults elsewhere
    fn map_partial_binlog_row_insert(
        &self,
        row_data: &BinlogRow,
        tme: &binlog::events::TableMapEvent<'static>,
        collation_vec: &[Option<u16>],
        signedness_vec: &[Option<bool>],
    ) -> ReadySetResult<Vec<DfValue>> {
        let mut row = vec![DfValue::Default; tme.columns_count() as usize];
        let mut binlog_iter = handle_err!(
            self,
            tme,
            binlog_row_to_noria_row(row_data, tme, collation_vec, signedness_vec)
        )
        .into_iter();

        for col in row_data.columns_ref() {
            if let Some(idx) = parse_column_index(&col.name_str()) {
                if let Some(value) = binlog_iter.next() {
                    row[idx] = value;
                }
            } else {
                warn!(
                    "Unable to parse column index from column {}",
                    col.name_str()
                );
            }
        }

        assert!(
            binlog_iter.next().is_none(),
            "binlog row has more columns than the table"
        );
        Ok(row)
    }

    /// Process a single binlog `ROTATE_EVENT`.
    /// This occurs when someone issues a `FLUSH LOGS` statement or the current binary
    /// log file becomes too large. The maximum size is
    /// determined by `max_binlog_size`.
    /// # Arguments
    ///
    /// * `rotate_event` - the rotate event to process
    async fn process_event_rotate(
        &mut self,
        rotate_event: binlog::events::RotateEvent<'_>,
    ) -> ReadySetResult<ReplicationAction> {
        if self.enable_statement_logging {
            info!(target: "replicator_statement", "{:?}", rotate_event);
        }

        let rotate_position = handle_err!(
            self,
            MySqlPosition::from_file_name_and_position(
                rotate_event.name().to_string(),
                rotate_event.position(),
            )
        );

        // We are on this binlog already, no need to do anything
        if self.next_position.binlog_file_suffix == rotate_position.binlog_file_suffix
            && self.next_position.position == 0
        {
            return Ok(ReplicationAction::Empty);
        }

        self.next_position = rotate_position;

        Ok(ReplicationAction::LogPosition)
    }

    /// Process a single binlog WRITE_ROWS_EVENT.
    /// This occurs when someone issues a INSERT INTO statement.
    ///
    /// # Arguments
    ///
    /// * `wr_event` - the write rows event to process
    async fn process_event_write_rows(
        &mut self,
        wr_event: binlog::events::WriteRowsEvent<'_>,
    ) -> ReadySetResult<ReplicationAction> {
        if self.enable_statement_logging {
            info!(target: "replicator_statement", "{:?}", wr_event);
        }
        // Retrieve the corresponding TABLE_MAP_EVENT, charset and signedness metadata
        let table_id = wr_event.table_id();
        let tme = handle_err!(
            self,
            self.reader
                .get_tme(table_id)
                .ok_or_else(|| format!("TME not found for table ID {table_id}"))
        );

        if !self
            .table_filter
            .should_be_processed(tme.database_name().as_ref(), tme.table_name().as_ref())
        {
            return Ok(ReplicationAction::Empty);
        }

        let (collation_vec, signedness_vec) = handle_err!(
            self,
            Self::get_table_metadata(&mut self.noria, &mut self.table_schemas, tme).await
        );

        let mut inserted_rows = Vec::new();
        for row in wr_event.rows(tme) {
            let row = handle_err!(self, tme, &row);
            let row_data = handle_err!(
                self,
                tme,
                row.1.as_ref().ok_or("Missing data in WRITE_ROWS_EVENT")
            );
            inserted_rows.push(TableOperation::Insert(self.map_partial_binlog_row_insert(
                row_data,
                tme,
                &collation_vec,
                &signedness_vec,
            )?));
        }

        if inserted_rows.is_empty() {
            inserted_rows.push(TableOperation::Insert(vec![
                DfValue::Default;
                tme.columns_count() as usize
            ]));
        }

        let table = Relation {
            schema: Some(tme.database_name().into()),
            name: tme.table_name().into(),
        };

        Ok(ReplicationAction::TableAction {
            table,
            actions: inserted_rows,
        })
    }

    /// Process a single row update, handling both full and partial row updates
    ///
    /// # Arguments
    /// * `tme` - Table map event containing table metadata and column information
    /// * `row` - A tuple containing optional before and after row images
    ///
    /// # Returns
    /// * `mysql::Result<Vec<TableOperation>>` - A vector of table operations (Delete+Insert for full updates, Update for partial)
    fn process_update_row(
        &self,
        tme: &binlog::events::TableMapEvent<'static>,
        row: &(Option<BinlogRow>, Option<BinlogRow>),
        collation_vec: &[Option<u16>],
        signedness_vec: &[Option<bool>],
    ) -> ReadySetResult<Vec<TableOperation>> {
        let (before_image, after_image) =
            self.get_before_after_images(tme, row, collation_vec, signedness_vec)?;

        if before_image.len() == tme.columns_count() as usize {
            let after_image = if after_image.len() == tme.columns_count() as usize {
                after_image
            } else {
                let update = self.create_partial_update(tme, row, after_image)?;
                // iterate over update, each row that has a modification::set, replace the value in the before_image with the new value
                before_image
                    .iter()
                    .zip(update.iter())
                    .map(|(before, update)| {
                        if let Modification::Set(value) = update {
                            value
                        } else {
                            before
                        }
                    })
                    .cloned()
                    .collect()
            };
            // Full row update - convert to delete + insert
            Ok(vec![
                TableOperation::DeleteRow { row: before_image },
                TableOperation::Insert(after_image),
            ])
        } else {
            // Partial row update
            let update = self.create_partial_update(tme, row, after_image)?;
            Ok(vec![TableOperation::Update {
                key: before_image,
                update,
            }])
        }
    }

    /// Extract before and after images from a row update
    ///
    /// # Arguments
    /// * `tme` - Table map event containing table metadata
    /// * `row` - A tuple containing optional before and after row images
    ///
    /// # Returns
    /// * `mysql::Result<(Vec<DfValue>, Vec<DfValue>)>` - A tuple of (before_image, after_image) as DfValue vectors
    ///
    /// # Errors
    /// Returns an error if either the before or after image is missing or cannot be converted
    fn get_before_after_images(
        &self,
        tme: &binlog::events::TableMapEvent<'static>,
        row: &(Option<BinlogRow>, Option<BinlogRow>),
        collation_vec: &[Option<u16>],
        signedness_vec: &[Option<bool>],
    ) -> ReadySetResult<(Vec<DfValue>, Vec<DfValue>)> {
        let before_row = handle_err!(
            self,
            tme,
            row.0
                .as_ref()
                .ok_or("Missing UPDATE_ROWS_EVENT before image")
        );
        let before_image = handle_err!(
            self,
            tme,
            binlog_row_to_noria_row(before_row, tme, collation_vec, signedness_vec)
        );

        let after_row = handle_err!(
            self,
            tme,
            row.1
                .as_ref()
                .ok_or("Missing UPDATE_ROWS_EVENT after image")
        );
        let after_image = handle_err!(
            self,
            tme,
            binlog_row_to_noria_row(after_row, tme, collation_vec, signedness_vec)
        );

        Ok((before_image, after_image))
    }

    /// Create a partial update modification vector
    ///
    /// # Arguments
    /// * `tme` - Table map event containing table metadata
    /// * `row` - A tuple containing optional before and after row images
    /// * `after_image` - Vector of DfValues representing the new values
    ///
    /// # Returns
    /// * `mysql::Result<Vec<Modification>>` - A vector of modifications to be applied
    ///
    /// # Errors
    /// Returns an error if the after image is missing from the row
    fn create_partial_update(
        &self,
        tme: &binlog::events::TableMapEvent<'static>,
        row: &(Option<BinlogRow>, Option<BinlogRow>),
        after_image: Vec<DfValue>,
    ) -> ReadySetResult<Vec<Modification>> {
        let row_after = handle_err!(
            self,
            tme,
            row.1
                .as_ref()
                .ok_or("Missing data in UPDATE_ROWS_EVENT after image in partial update")
        );

        let mut update = vec![Modification::None; tme.columns_count() as usize];
        let mut taken = 0;

        // Process each column in the row
        for col in row_after.columns_ref() {
            if let Some(idx) = parse_column_index(&col.name_str()) {
                if taken < after_image.len() {
                    update[idx] = Modification::Set(after_image[taken].clone());
                    taken += 1;
                }
            }
        }

        Ok(update)
    }

    /// Process a single binlog UPDATE_ROWS_EVENT
    ///
    /// # Arguments
    /// * `ur_event` - The update rows event to process
    ///
    /// # Returns
    /// * `mysql::Result<ReplicationAction>` - The resulting replication action
    ///   - `ReplicationAction::Empty` if the table should not be processed
    ///   - `ReplicationAction::TableAction` containing the table operations otherwise
    ///
    /// # Errors
    /// Returns an error if:
    /// - The table map event is not found
    /// - Row processing fails
    /// - Data conversion fails
    async fn process_event_update_rows(
        &mut self,
        ur_event: binlog::events::UpdateRowsEvent<'_>,
    ) -> ReadySetResult<ReplicationAction> {
        if self.enable_statement_logging {
            info!(target: "replicator_statement", "{:?}", ur_event);
        }

        // Retrieve the corresponding TABLE_MAP_EVENT, charset and signedness metadata
        let table_id = ur_event.table_id();
        let tme = handle_err!(
            self,
            self.reader
                .get_tme(table_id)
                .ok_or_else(|| format!("TME not found for table ID {table_id}"))
        );

        if !self
            .table_filter
            .should_be_processed(tme.database_name().as_ref(), tme.table_name().as_ref())
        {
            return Ok(ReplicationAction::Empty);
        }

        let (collation_vec, signedness_vec) = handle_err!(
            self,
            Self::get_table_metadata(&mut self.noria, &mut self.table_schemas, tme).await
        );

        let mut updated_rows = Vec::with_capacity(ur_event.rows(tme).count());
        // Process each row update
        for row in ur_event.rows(tme) {
            let row = handle_err!(self, tme, &row);
            let row_ops = self.process_update_row(tme, row, &collation_vec, &signedness_vec)?;
            updated_rows.extend(row_ops);
        }

        let table = Relation {
            schema: Some(tme.database_name().into()),
            name: tme.table_name().into(),
        };

        Ok(ReplicationAction::TableAction {
            table,
            actions: updated_rows,
        })
    }

    /// Process a single binlog DELETE_ROWS_EVENT.
    /// This occurs when someone issues a `DELETE` statement.
    ///
    /// # Arguments
    ///
    /// * `dr_event` - the delete rows event to process
    async fn process_event_delete_rows(
        &mut self,
        dr_event: binlog::events::DeleteRowsEvent<'_>,
    ) -> ReadySetResult<ReplicationAction> {
        if self.enable_statement_logging {
            info!(target: "replicator_statement", "{:?}", dr_event);
        }
        // Retrieve the corresponding TABLE_MAP_EVENT, charset and signedness metadata
        let table_id = dr_event.table_id();
        let tme = handle_err!(
            self,
            self.reader
                .get_tme(table_id)
                .ok_or_else(|| format!("TME not found for table ID {table_id}"))
        );

        if !self
            .table_filter
            .should_be_processed(tme.database_name().as_ref(), tme.table_name().as_ref())
        {
            return Ok(ReplicationAction::Empty);
        }

        let (collation_vec, signedness_vec) = handle_err!(
            self,
            Self::get_table_metadata(&mut self.noria, &mut self.table_schemas, tme).await
        );

        let mut deleted_rows = Vec::new();
        for row in dr_event.rows(tme) {
            // For each row in the event we produce a vector of ReadySet types that
            // represent that row
            let row = handle_err!(self, tme, row);
            let before_row =
                handle_err!(self, tme, row.0.ok_or("Missing data in DELETE_ROWS_EVENT"));
            let before_row_image = handle_err!(
                self,
                tme,
                binlog_row_to_noria_row(&before_row, tme, &collation_vec, &signedness_vec)
            );

            // Partial Row, Before row image is the PKE
            if tme.columns_count() != before_row_image.len() as u64 {
                deleted_rows.push(TableOperation::DeleteByKey {
                    key: before_row_image,
                });
            } else {
                deleted_rows.push(TableOperation::DeleteRow {
                    row: before_row_image,
                });
            }
        }

        let table = Relation {
            schema: Some(tme.database_name().into()),
            name: tme.table_name().into(),
        };

        Ok(ReplicationAction::TableAction {
            table,
            actions: deleted_rows,
        })
    }

    /// Add a table to the non-replicated list
    /// # Arguments
    /// * `table` - the table to add to the non-replicated list
    /// * `current_schema` - the current schema from the binlog
    ///
    /// # Returns
    /// This function returns a vector of changes to be applied to the schema.
    async fn drop_and_add_non_replicated_table(
        &mut self,
        mut table: Relation,
        current_schema: &String,
    ) -> Vec<Change> {
        if table.schema.is_none() {
            table.schema = Some(SqlIdentifier::from(current_schema));
        }
        vec![
            Change::Drop {
                name: table.clone(),
                if_exists: true,
            },
            Change::AddNonReplicatedRelation(NonReplicatedRelation {
                name: table,
                reason: NotReplicatedReason::OtherError(format!(
                    "Event received as binlog_format=STATEMENT. File: {:} - Pos: {:}",
                    self.next_position.binlog_file_name(),
                    self.next_position.position
                )),
            }),
        ]
    }
    /// Process a single binlog QUERY_EVENT.
    /// This occurs when someone issues a DDL statement or Query using binlog_format = STATEMENT.
    ///
    /// # Arguments
    ///
    /// * `q_event` - the query event to process.
    /// * `is_last` - a boolean indicating if this is the last event during catchup.
    ///
    /// # Returns
    /// This function might return an error of type `ReadySetError::SkipEvent` if the query does not
    /// affect the schema. This event should be skipped in the parent function.
    async fn process_event_query(
        &mut self,
        q_event: binlog::events::QueryEvent<'_>,
        is_last: bool,
    ) -> ReadySetResult<ReplicationAction> {
        // Written when an updating statement is done.
        if self.enable_statement_logging {
            info!(target: "replicator_statement", "{:?}", q_event);
        }

        let schema = match q_event
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
            // Even if the query does not affect the schema, it may still require a table action.
            _ => return self.try_non_ddl_action_from_query(q_event, is_last),
        };

        let changes = match ChangeList::from_strings(vec![q_event.query()], Dialect::DEFAULT_MYSQL)
        {
            Ok(mut changelist) => {
                // During replication of DDL, we don't necessarily have the default charset/collation as part of the
                // query event.
                let charset = q_event
                    .status_vars()
                    .get_status_var(binlog::consts::StatusVarKey::Charset);

                if charset.is_some() {
                    let default_server_charset = match charset.unwrap().get_value().unwrap() {
                        StatusVarVal::Charset {
                            charset_client: _,
                            collation_connection: _,
                            collation_server,
                        } => collation_server,
                        _ => unreachable!(),
                    };

                    for change in changelist.changes_mut() {
                        match change {
                            Change::CreateTable { statement, .. } => {
                                let default_table_collation = statement.get_collation();
                                if default_table_collation.is_none() {
                                    let collation = Collation::resolve(CollationId::from(
                                        default_server_charset,
                                    ));
                                    let options = match statement.options.as_mut() {
                                        Ok(opts) => opts,
                                        Err(_) => &mut Vec::new(),
                                    };
                                    options.push(CreateTableOption::Collate(
                                        CollationName::Quoted(SqlIdentifier::from(
                                            collation.collation(),
                                        )),
                                    ));
                                }
                                statement.propagate_default_charset(readyset_sql::Dialect::MySQL);
                                if let Ok(body) = &statement.body {
                                    self.table_schemas
                                        .insert(statement.table.clone(), body.clone());
                                }
                            }
                            Change::Drop { name, .. } => {
                                self.table_schemas.remove(name);
                            }
                            Change::AlterTable(AlterTableStatement { table, .. }) => {
                                self.table_schemas.remove(table);
                            }
                            _ => {}
                        }
                    }
                }
                changelist.changes
            }
            Err(error) => {
                match readyset_sql_parsing::parse_query(
                    readyset_sql::Dialect::MySQL,
                    q_event.query(),
                ) {
                    Ok(SqlQuery::Insert(insert)) => {
                        self.drop_and_add_non_replicated_table(insert.table, &schema)
                            .await
                    }
                    Ok(SqlQuery::Update(update)) => {
                        self.drop_and_add_non_replicated_table(update.table, &schema)
                            .await
                    }
                    Ok(SqlQuery::Delete(delete)) => {
                        self.drop_and_add_non_replicated_table(delete.table, &schema)
                            .await
                    }
                    Ok(SqlQuery::StartTransaction(_)) => {
                        return Err(ReadySetError::SkipEvent);
                    }
                    _ => {
                        warn!(%error, "Error extending recipe, DDL statement will not be used");
                        counter!(recorded::REPLICATOR_FAILURE).increment(1u64);
                        return Err(ReadySetError::SkipEvent);
                    }
                }
            }
        };

        Ok(ReplicationAction::DdlChange { schema, changes })
    }

    /// Attempt to produce a non-DDL [`ReplicationAction`] from the give query.
    ///
    /// `COMMIT` queries are issued for writes on non-transactional storage engines such as MyISAM.
    /// We report the position after the `COMMIT` query if necessary.
    ///
    /// TRUNCATE statements are also parsed and handled here.
    ///
    /// TODO: Transactions begin with `BEGIN` queries, but we do not currently support those.
    fn try_non_ddl_action_from_query(
        &mut self,
        q_event: binlog::events::QueryEvent<'_>,
        is_last: bool,
    ) -> ReadySetResult<ReplicationAction> {
        use readyset_sql::Dialect;
        use readyset_sql_parsing::parse_query;

        match parse_query(Dialect::MySQL, q_event.query()) {
            Ok(SqlQuery::Commit(_)) if self.report_position_elapsed() || is_last => {
                Ok(ReplicationAction::LogPosition)
            }
            Ok(SqlQuery::Truncate(truncate)) if truncate.tables.len() == 1 => {
                // MySQL only allows one table in the statement, or we would be in trouble.
                let mut relation = truncate.tables[0].relation.clone();
                if relation.schema.is_none() {
                    relation.schema = Some(SqlIdentifier::from(q_event.schema()))
                }
                Ok(ReplicationAction::TableAction {
                    table: relation,
                    actions: vec![TableOperation::Truncate],
                })
            }
            _ => Err(ReadySetError::SkipEvent),
        }
    }

    /// Merge table actions into a hashmap of actions.
    /// If the table already exists in the hashmap, the actions are merged.
    /// If the table does not exist in the hashmap, a new entry is created.
    ///
    /// # Arguments
    /// * `map` - the hashmap to merge the actions into
    /// * `action` - the action to merge
    ///
    /// # Returns
    /// This function does not return anything, it modifies the hashmap in place.
    async fn merge_table_actions(
        &mut self,
        map: &mut HashMap<Relation, ReplicationAction>,
        action: ReplicationAction,
    ) {
        match action {
            ReplicationAction::TableAction {
                table,
                actions: incoming_actions,
            } => {
                map.entry(table.clone())
                    .and_modify(|e| {
                        if let ReplicationAction::TableAction { actions, .. } = e {
                            actions.extend(incoming_actions.clone());
                        }
                    })
                    .or_insert(ReplicationAction::TableAction {
                        table,
                        actions: incoming_actions,
                    });
            }
            ReplicationAction::Empty => {}
            ReplicationAction::DdlChange { .. } | ReplicationAction::LogPosition => {
                warn!("Unexpected action in merge_table_actions: {:?}", action);
            }
        }
    }

    /// Process inner events from a TRANSACTION_PAYLOAD_EVENT.
    /// This occurs when binlog_transaction_compression is enabled.
    /// This function returns a vector of all actionable inner events
    /// # Arguments
    ///
    /// * `payload_event` - the payload event to process
    /// * `is_last` - a boolean indicating if this is the last event during catchup.
    ///
    /// # Returns
    /// This function returns a vector of all actionable inner events
    async fn process_event_transaction_payload(
        &mut self,
        payload_event: binlog::events::TransactionPayloadEvent<'_>,
        is_last: bool,
    ) -> ReadySetResult<Vec<ReplicationAction>> {
        let mut hash_actions: HashMap<Relation, ReplicationAction> = HashMap::new();
        if self.enable_statement_logging {
            info!(target: "replicator_statement", "{:?}", payload_event);
        }
        let mut buff = handle_err!(self, payload_event.decompressed());
        while let Some(binlog_ev) = handle_err!(self, self.reader.read_decompressed(&mut buff)) {
            match handle_err!(self, binlog_ev.header().event_type()) {
                EventType::QUERY_EVENT => {
                    // We only accept query events in the transaction payload that do not affect the
                    // schema. Those are `BEGIN` and `COMMIT`. `BEGIN` will return a
                    // `ReadySetError::SkipEvent` and `COMMIT` will return a
                    // `ReplicationAction::LogPosition` if necessary. We skip
                    // `ReplicationAction::LogPosition` here because we will report the position
                    // only once at the end.
                    let event = handle_err!(self, binlog_ev.read_event());
                    match self.process_event_query(event, is_last).await {
                        Err(ReadySetError::SkipEvent) => {
                            continue;
                        }
                        Err(err) => {
                            return Err(binlog_err!(
                                self,
                                format!("Could not process query inside transaction: {err}")
                            ))
                        }
                        Ok(action) => match action {
                            ReplicationAction::LogPosition => {
                                continue;
                            }
                            _ => {
                                return Err(binlog_err!(
                                    self,
                                    format!(
                                        "Unexpected query event in transaction payload {:?}",
                                        action
                                    )
                                ));
                            }
                        },
                    };
                }
                EventType::WRITE_ROWS_EVENT => {
                    let event = handle_err!(self, binlog_ev.read_event());
                    let binlog_action =
                        handle_err!(self, self.process_event_write_rows(event).await);
                    self.merge_table_actions(&mut hash_actions, binlog_action)
                        .await;
                }

                EventType::UPDATE_ROWS_EVENT => {
                    let event = handle_err!(self, binlog_ev.read_event());

                    let binlog_action =
                        handle_err!(self, self.process_event_update_rows(event).await);
                    self.merge_table_actions(&mut hash_actions, binlog_action)
                        .await;
                }

                EventType::DELETE_ROWS_EVENT => {
                    let event = handle_err!(self, binlog_ev.read_event());
                    let binlog_action =
                        handle_err!(self, self.process_event_delete_rows(event).await);
                    self.merge_table_actions(&mut hash_actions, binlog_action)
                        .await;
                }
                ev => {
                    if self.enable_statement_logging {
                        info!(target: "replicator_statement", "unhandled event: {:?}", ev);
                    }
                }
            }
        }
        // We will always have received at least one COMMIT from either COM_QUERY or XID_EVENT.
        // To avoid reporting multiple times the same position we only report it once here if
        // necessary.
        if !hash_actions.is_empty() && (self.report_position_elapsed() || is_last) {
            hash_actions.insert(
                Relation {
                    schema: None,
                    name: SqlIdentifier::from(""),
                },
                ReplicationAction::LogPosition,
            );
        }
        Ok(hash_actions.into_values().collect())
    }

    /// Check whatever we need to report the current position
    /// If last_reported_pos_ts has elapsed, update it with the current timestamp.
    ///
    /// # Returns
    /// This function returns a boolean indicating if we need to report the current position
    fn report_position_elapsed(&mut self) -> bool {
        if self.last_reported_pos_ts.elapsed().as_secs() > MAX_POSITION_TIME {
            self.last_reported_pos_ts = std::time::Instant::now();
            return true;
        }
        false
    }

    /// Process binlog events until an actionable event occurs.
    ///
    /// # Arguments
    /// * `until` - an optional position in the binlog to stop at, even if no actionable
    ///
    /// occurred. In that case the action [`ReplicationAction::LogPosition`] is returned.
    pub(crate) async fn next_action_inner(
        &mut self,
        until: Option<&ReplicationOffset>,
    ) -> ReadySetResult<(Vec<ReplicationAction>, &MySqlPosition)> {
        use mysql_common::binlog::events;

        loop {
            let binlog_event = handle_err!(self, self.next_event().await);

            if u64::from(binlog_event.header().log_pos()) < self.next_position.position
                && self.next_position.position + u64::from(binlog_event.header().event_size())
                    > u64::from(u32::MAX)
            {
                self.next_position.position =
                    u64::from(u32::MAX) + 1 + u64::from(binlog_event.header().log_pos());
            } else {
                self.next_position.position = u64::from(binlog_event.header().log_pos());
            }

            let is_last = match until {
                Some(limit) => {
                    let limit = MySqlPosition::try_from(limit).expect("Valid binlog limit");
                    self.next_position >= limit
                }
                None => false,
            };
            match handle_err!(self, binlog_event.header().event_type()) {
                EventType::ROTATE_EVENT => {
                    let event = handle_err!(self, binlog_event.read_event());
                    let action = handle_err!(self, self.process_event_rotate(event).await);
                    return Ok((vec![action], &self.next_position));
                }

                EventType::QUERY_EVENT => {
                    let event = handle_err!(self, binlog_event.read_event());
                    let action = match self.process_event_query(event, is_last).await {
                        Ok(action) => action,
                        Err(ReadySetError::SkipEvent) => {
                            continue;
                        }
                        Err(err) => return Err(binlog_err!(self, None, err)),
                    };
                    return Ok((vec![action], &self.next_position));
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
                    let event = handle_err!(self, binlog_event.read_event());
                    let action = self.process_event_write_rows(event).await?;
                    return Ok((vec![action], &self.next_position));
                }

                EventType::UPDATE_ROWS_EVENT => {
                    let event = handle_err!(self, binlog_event.read_event());
                    let action = self.process_event_update_rows(event).await?;
                    return Ok((vec![action], &self.next_position));
                }

                EventType::DELETE_ROWS_EVENT => {
                    let event = handle_err!(self, binlog_event.read_event());
                    let action = self.process_event_delete_rows(event).await?;
                    return Ok((vec![action], &self.next_position));
                }

                EventType::TRANSACTION_PAYLOAD_EVENT => {
                    let event = handle_err!(self, binlog_event.read_event());
                    let actions = handle_err!(
                        self,
                        self.process_event_transaction_payload(event, is_last).await
                    );
                    return Ok((actions, &self.next_position));
                }

                EventType::XID_EVENT => {
                    // Generated for a commit of a transaction that modifies one or more tables of
                    // an XA-capable storage engine (InnoDB).
                    if self.report_position_elapsed() || is_last {
                        return Ok((vec![ReplicationAction::LogPosition], &self.next_position));
                    }
                    continue;
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
                    let ev: events::GtidEvent = handle_err!(self, binlog_event.read_event());
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
            if is_last {
                return Ok((vec![ReplicationAction::LogPosition], &self.next_position));
            }
        }
    }

    /// Get the table map event, charset and signedness metadata for a table map event.
    ///
    /// If there is no charset metadata for a text column, we attempt to retrieve it from DDL we have seen via
    /// [`MySqlBinlogConnector::get_table_schema`].
    ///
    /// Note that this is a static method on the type because we otherwise can't partially borrow
    /// these fields as mutable in calling methods which are also borrowing `self` as immutable.
    async fn get_table_metadata(
        noria: &mut ReadySetHandle,
        table_schemas: &mut HashMap<Relation, CreateTableBody>,
        tme: &TableMapEvent<'static>,
    ) -> ReadySetResult<TableMetadata> {
        /*
         * TME iterators are only populated for the types that make sense.
         * For example, for a table with col1 CHAR(1) collate utf8mb4_0900_ai_ci, col2 int, col3 unsigned int, col4 char(1) collate binary
         * the iterators will be:
         * charset_iter: [255, 63]
         * enum_and_set_charset_iter: []
         * signedness_iter: [true, false]
         *
         * For MRBR - where we receive the row event with only the list of columns that are set
         * we need to advance the iterators even for columns that are not present in the event. In the event above, if we do an UPDATE table SET col4 = 'A'
         * the row event will come as {@3: Value(Bytes("A"))}, making it difficult to retrive the charset 63 for col4.
         */
        let opt_meta_extractor = OptionalMetaExtractor::new(tme.iter_optional_meta()).unwrap();

        let mut missing_col_charsets = vec![];
        let mut collation_vec = vec![None; tme.columns_count() as usize];
        let mut signedness_vec = vec![None; tme.columns_count() as usize];
        {
            // This is in a block to capture these iterators, which are not `Send`, so that we can
            // `.await` later and not get a "this value may be used later" error.
            let mut charset_iter = opt_meta_extractor.iter_charset();
            let mut enum_and_set_charset_iter = opt_meta_extractor.iter_enum_and_set_charset();
            let mut signedness_iter = opt_meta_extractor.iter_signedness();
            for id in 0..tme.columns_count() as usize {
                let kind = tme
                    .get_column_type(id)
                    .map_err(|e| {
                        mysql_async::Error::Other(Box::new(internal_err!(
                            "Unable to get column type {}",
                            e
                        )))
                    })?
                    .unwrap();
                if kind.is_character_type() {
                    collation_vec[id] = match charset_iter.next().transpose()? {
                        None => {
                            missing_col_charsets.push(id);
                            None
                        }
                        charset => charset,
                    };
                } else if kind.is_enum_or_set_type() {
                    collation_vec[id] = Some(
                        enum_and_set_charset_iter
                            .next()
                            .transpose()?
                            .unwrap_or_default(),
                    );
                } else if kind.is_numeric_type() {
                    signedness_vec[id] = Some(signedness_iter.next().unwrap_or(false));
                }
            }
        }

        // These character column did not have a charset available in the column metadata,
        // presumably because this is coming from MySQL 5.7 which does not send charsets with table
        // map events; so we need to fallback to the table schema definition.
        for col_id in missing_col_charsets {
            collation_vec[col_id] =
                Self::get_charset_for_column(noria, table_schemas, tme, col_id).await?;
        }

        Ok((collation_vec, signedness_vec))
    }

    /// Retrieves the charset for the given column based on the DDL schema of the table.
    ///
    /// If the table was created during streaming replication, we should have already stored its
    /// schema in [`MySqlBinlogConnector::process_query_event`]. Otherwise, if it was encountered
    /// during snapshotting or during a previous run of the readyset binary, we consult the
    /// controller's `Recipe`. This will slow us down because we have to communicate with the
    /// server, but we do retain it until/unless it becomes invalidated in
    /// [`MySqlBinlogConnector::process_event_query`].
    ///
    /// Note that this relies on already having called
    /// [`readyset_sql::ast::CreateTableStatement::propagate_default_charset`] on the table when it
    /// was snapshotted or replicated.
    async fn get_charset_for_column(
        noria: &mut ReadySetHandle,
        table_schemas: &mut HashMap<Relation, CreateTableBody>,
        tme: &TableMapEvent<'static>,
        column_id: usize,
    ) -> ReadySetResult<Option<u16>> {
        let table = Relation {
            schema: Some(tme.database_name().into()),
            name: tme.table_name().into(),
        };
        let exists = table_schemas.contains_key(&table);
        if !exists {
            if let Some(schema) = noria.table(table.clone()).await?.schema().cloned() {
                table_schemas.insert(table.clone(), schema);
            }
        }

        if let Some(column) = table_schemas
            .get(&table)
            .and_then(|schema| schema.fields.get(column_id))
        {
            if let Some(charset) = column.get_charset() {
                Ok(Some(mysql_character_set_name_to_collation_id(charset)))
            } else if let Some(collation) = column.get_collation() {
                Ok(Some(CollationId::from(collation) as u16))
            } else if column.sql_type.is_any_binary() {
                Ok(Some(CollationId::BINARY as u16))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }
}

fn binlog_val_to_noria_val(
    val: &Value,
    col_kind: ColumnType,
    meta: &[u8],
    collation: u16,
    unsigned: bool,
    binary: bool,
) -> ReadySetResult<DfValue> {
    // Not all values are coerced to the value expected by ReadySet directly

    use mysql_common::constants::ColumnType;
    if let Value::NULL = val {
        return Ok(DfValue::None);
    }
    match (col_kind, meta) {
        (ColumnType::MYSQL_TYPE_TIMESTAMP2, &[0]) => {
            let buf = match val {
                Value::Bytes(b) => b,
                _ => internal!("Expected a byte array for timestamp"),
            };
            // https://github.com/blackbeam/rust_mysql_common/blob/408effed435c059d80a9e708bcfa5d974527f476/src/binlog/value.rs#L144
            // When meta is 0, `mysql_common` encodes this value as number of seconds (since UNIX
            // EPOCH)
            let epoch = String::from_utf8_lossy(buf).parse::<i64>().unwrap(); // Can unwrap because we know the format is integer
            if epoch == 0 {
                // The 0 epoch is reserved for the '0000-00-00 00:00:00' timestamp
                return Ok(DfValue::TimestampTz(TimestampTz::zero()));
            }
            let time = chrono::DateTime::from_timestamp(epoch, 0)
                .unwrap()
                .naive_utc();
            // Can unwrap because we know it maps directly to [`DfValue`]
            Ok(time.into())
        }
        (ColumnType::MYSQL_TYPE_TIMESTAMP2, meta) => {
            let buf = match val {
                Value::Bytes(b) => b,
                _ => internal!("Expected a byte array for timestamp"),
            };
            // When meta is anything else, `mysql_common` encodes this value as number of
            // seconds.microseconds (since UNIX EPOCH)
            let s = String::from_utf8_lossy(buf);
            let (secs, usecs) = s.split_once('.').unwrap_or((&s, "0"));
            let secs = secs.parse::<i64>().unwrap();
            let usecs = usecs.parse::<u32>().unwrap();
            let time = chrono::DateTime::from_timestamp(secs, usecs * 1000)
                .unwrap()
                .naive_utc();
            let mut ts: TimestampTz = time.into();
            // The meta[0] is the fractional seconds precision
            ts.set_subsecond_digits(meta[0]);
            Ok(DfValue::TimestampTz(ts))
        }
        (ColumnType::MYSQL_TYPE_DATETIME2, meta) => {
            //meta[0] is the fractional seconds precision
            val.try_into().and_then(|val| match val {
                DfValue::TimestampTz(mut ts) => {
                    ts.set_subsecond_digits(meta[0]);
                    Ok(DfValue::TimestampTz(ts))
                }
                DfValue::None => Ok(DfValue::None), // NULL
                _ => internal!("Expected a timestamp"),
            })
        }
        (ColumnType::MYSQL_TYPE_DATE, _) | (ColumnType::MYSQL_TYPE_NEWDATE, _) => {
            let df_val: DfValue = val.try_into().and_then(|val| match val {
                DfValue::TimestampTz(mut ts) => {
                    ts.set_date_only();
                    Ok(DfValue::TimestampTz(ts))
                }
                DfValue::None => Ok(DfValue::None), // NULL
                _ => internal!("Expected a timestamp"),
            })?;
            Ok(df_val)
        }
        (ColumnType::MYSQL_TYPE_STRING, meta) => {
            let buf = match val {
                Value::Bytes(b) => b,
                _ => internal!("Expected a byte array for string"),
            };
            // Check for special encoding when length is greater than 255 (as happens with multibyte
            // encodings such as utf8mb4). cf https://bugs.mysql.com/bug.php?id=37426
            let length = if meta[0] & 0x30 != 0x30 {
                meta[1] as usize | (((meta[0] as usize & 0x30) ^ 0x30) << 4)
            } else {
                meta[1] as usize
            };
            if binary {
                mysql_pad_binary_column(buf.to_owned(), length)
            } else {
                mysql_pad_char_column(buf, collation, length, false)
            }
        }
        (ColumnType::MYSQL_TYPE_BLOB, _)
        | (ColumnType::MYSQL_TYPE_TINY_BLOB, _)
        | (ColumnType::MYSQL_TYPE_MEDIUM_BLOB, _)
        | (ColumnType::MYSQL_TYPE_LONG_BLOB, _)
        | (ColumnType::MYSQL_TYPE_VAR_STRING, _)
        | (ColumnType::MYSQL_TYPE_VARCHAR, _)
            if binary =>
        {
            let bytes = match val {
                Value::Bytes(b) => b.to_vec(),
                _ => internal!("Expected a byte array for blob or binary string column"),
            };
            Ok(DfValue::from(bytes))
        }
        (ColumnType::MYSQL_TYPE_BLOB, _)
        | (ColumnType::MYSQL_TYPE_TINY_BLOB, _)
        | (ColumnType::MYSQL_TYPE_MEDIUM_BLOB, _)
        | (ColumnType::MYSQL_TYPE_LONG_BLOB, _)
        | (ColumnType::MYSQL_TYPE_VAR_STRING, _)
        | (ColumnType::MYSQL_TYPE_VARCHAR, _)
            if !binary =>
        {
            let bytes = match val {
                Value::Bytes(b) => b.as_ref(),
                _ => internal!("Expected a byte array for string"),
            };

            let encoding = Encoding::from_mysql_collation_id(collation);

            let utf8_string = encoding.decode(bytes)?;

            let my_collation = Collation::resolve(CollationId::from(collation));
            let rs_collation =
                RsCollation::from_mysql_collation(my_collation.collation()).unwrap_or_default();
            Ok(DfValue::from_str_and_collation(&utf8_string, rs_collation))
        }
        (ColumnType::MYSQL_TYPE_DECIMAL, _) | (ColumnType::MYSQL_TYPE_NEWDECIMAL, _) => {
            if let Value::Bytes(b) = val {
                str::from_utf8(b)
                    .ok()
                    .and_then(|s| Decimal::from_str_exact(s).ok())
                    .map(|d| DfValue::Numeric(Arc::new(d)))
                    .ok_or_else(|| internal_err!("Failed to parse decimal value"))
            } else {
                internal!("Expected a bytes value for decimal column")
            }
        }
        (ColumnType::MYSQL_TYPE_INT24, _) => {
            match val {
                Value::Int(x) => {
                    // MySQL can send signed int values even for unsigned columns, so we have to
                    // check whether it's actually signed; if so, we need to sign-extend from 24
                    // bits to 64 bits.
                    if unsigned {
                        Ok(DfValue::UnsignedInt((*x).try_into().map_err(|e| {
                            internal_err!(
                                "Could not convert signed to unsigned mediumint column: {e}"
                            )
                        })?))
                    } else {
                        let missing = size_of::<i64>() as u32 * 8 - 24;
                        Ok(DfValue::Int(x.wrapping_shl(missing).wrapping_shr(missing)))
                    }
                }
                Value::UInt(x) => Ok(DfValue::UnsignedInt(*x)),
                _ => internal!("Expected an integer value for mediumint column"),
            }
        }
        (ColumnType::MYSQL_TYPE_GEOMETRY, _) => match val {
            Value::Bytes(b) => Ok(DfValue::ByteArray(Arc::new(b.to_vec()))),
            _ => internal!("Expected a byte array for geometry column"),
        },
        (_, _) => Ok(val.try_into()?),
    }
}

fn binlog_to_serde_object<T>(v: &ComplexValue<T, Object>) -> ReadySetResult<serde_json::Value>
where
    T: StorageFormat,
{
    let mut serde_map: Map<String, serde_json::value::Value> =
        Map::with_capacity(v.element_count() as usize);
    for e in v.iter() {
        let (key, val) = e?;
        serde_map.insert(key.value().into(), binlog_to_serde_jsonb_value(&val)?);
    }
    Ok(serde_json::Value::Object(serde_map))
}

fn binlog_to_serde_array<T>(v: &ComplexValue<T, Array>) -> ReadySetResult<serde_json::Value>
where
    T: StorageFormat,
{
    let mut serde_array: Vec<serde_json::value::Value> =
        Vec::with_capacity(v.element_count() as usize);
    for e in v.iter() {
        serde_array.push(binlog_to_serde_jsonb_value(&e?)?);
    }
    Ok(serde_json::Value::Array(serde_array))
}

fn string_to_serde_value(s: &str) -> ReadySetResult<serde_json::Value> {
    serde_json::from_str::<serde_json::Value>(s).map_err(|e| internal_err!("{e}"))
}

fn slice_to_8bytes_array(data: &[u8]) -> [u8; 8] {
    let len = data.len();
    if len == 8 {
        data.try_into().unwrap()
    } else {
        let mut bytes = [0u8; 8];
        bytes[..len].copy_from_slice(&data[..len]);
        bytes
    }
}

/*
 *  This method reads binary opaque value of a temporal type into a packed i64,
 *  which should be unpacked with appropriate binlog::misc::* function into
 *  Value::{Date, Time}.
 *
 *  Note, even there are functions: binlog::misc::{
 *      my_datetime_packed_from_binary,
 *      my_timestamp_from_binary,
 *      my_time_packed_from_binary
 *  }, we can not use them, b/c actual MySQL code which reads binary
 *  opaque values for temporal types, DOES NOT MATCH the above functions implementation.
 *
 *  The binlog::mics::* functions ALWAYS read the input as BigEndian, no matter what platform is,
 *  as two blocks of 5 bytes and the residual upto 3 bytes respectively,
 *  combining the result into i64 value.
 *
 *  The MySQL implementation, ALWAYS reads 8 bytes, based on the platform endianness, without
 *  any post-read modifications. MySQL reads it equally for all temporal data types.
 *
 *  TO BE INVESTIGATED:
 *  Logically, the opaque bytes must be read with the same endianness it were written with,
 *  and not just according to the reading platform endianness. Though, the MySQL source code
 *  seems to read it just according to the run-time platform's endianness.
 *
 *  MySQL source code github references:
 *
 *  Main function "json_binary_to_dom_template", #689:
 *  https://github.com/mysql/mysql-server/blob/mysql-8.0.39/sql-common/json_dom.cc#L712
 *
 *  Function, which reads from binary for all temporal types, #1242:
 *  https://github.com/mysql/mysql-server/blob/mysql-8.0.39/sql-common/json_dom.cc#L1242
 *
 *  void Json_datetime::from_packed(const char *from, enum_field_types ft, MYSQL_TIME *to) {
 *       TIME_from_longlong_packed(to, ft, sint8korr(from));
 *  }
 *
 *  This function is called for all temporal types, and it always reads full 8 bytes via
 *  "sint8korr" function.  The further browsing shows, the "sint8korr" function calls either
 *  BigEndian, or  LittleEndian implementation, based  on the platform config:
 *  https://github.com/mysql/mysql-server/blob/596f0d238489a9cf9f43ce1ff905984f58d227b6/include/my_byteorder.h#L167
 */
fn temporal_packed_from_binary(data: &[u8]) -> i64 {
    if cfg!(target_endian = "little") {
        u64::from_le_bytes(slice_to_8bytes_array(data)) as i64
    } else if cfg!(target_endian = "big") {
        u64::from_be_bytes(slice_to_8bytes_array(data)) as i64
    } else {
        panic!("Unknown endianness.");
    }
}

fn mysql_common_value_to_json_string(val: &Value) -> String {
    match *val {
        Value::Date(y, m, d, 0, 0, 0, 0) => format!("\"{y:04}-{m:02}-{d:02}\""),
        Value::Date(year, month, day, hour, minute, second, micros) => format!(
            "\"{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}.{micros:06}\""
        ),
        Value::Time(neg, d, h, i, s, u) => {
            assert_eq!(d, 0);
            if neg {
                format!("\"-{:02}:{:02}:{:02}.{:06}\"", u32::from(h), i, s, u)
            } else {
                format!("\"{:02}:{:02}:{:02}.{:06}\"", u32::from(h), i, s, u)
            }
        }
        _ => val.as_sql(true),
    }
}

fn binary_temporal_to_serde_value(
    data: &[u8],
    temporal_from_packed_func: fn(i64) -> Value,
) -> ReadySetResult<serde_json::Value> {
    let packed = temporal_packed_from_binary(data);
    let val = temporal_from_packed_func(packed);
    string_to_serde_value(&mysql_common_value_to_json_string(&val)[..])
}

fn binlog_opaque_to_serde_value(v: &OpaqueValue) -> ReadySetResult<serde_json::Value> {
    match v.value_type() {
        ColumnType::MYSQL_TYPE_NEWDECIMAL => {
            let data = v.data_raw();
            let decimal = binlog::decimal::Decimal::read_bin(
                &data[2..],
                data[0] as usize,
                data[1] as usize,
                false,
            )?;
            string_to_serde_value(&decimal.to_string())
        }

        ColumnType::MYSQL_TYPE_TIME => {
            binary_temporal_to_serde_value(v.data_raw(), binlog::misc::time_from_packed)
        }

        ColumnType::MYSQL_TYPE_DATETIME
        | ColumnType::MYSQL_TYPE_TIMESTAMP
        | ColumnType::MYSQL_TYPE_DATE => {
            binary_temporal_to_serde_value(v.data_raw(), binlog::misc::datetime_from_packed)
        }

        _ => Err(unsupported_err!(
            "Can not handle opaque value for column type {:?}",
            v.value_type()
        )),
    }
}

fn binlog_to_serde_jsonb_value(binlog_val: &jsonb::Value) -> ReadySetResult<serde_json::Value> {
    match binlog_val {
        jsonb::Value::Null => Ok(serde_json::Value::Null),
        jsonb::Value::Bool(x) => Ok(serde_json::Value::Bool(*x)),
        jsonb::Value::I16(x) => Ok((*x).into()),
        jsonb::Value::U16(x) => Ok((*x).into()),
        jsonb::Value::I32(x) => Ok((*x).into()),
        jsonb::Value::U32(x) => Ok((*x).into()),
        jsonb::Value::I64(x) => Ok((*x).into()),
        jsonb::Value::U64(x) => Ok((*x).into()),
        jsonb::Value::F64(x) => Ok((*x).into()),
        jsonb::Value::String(x) => Ok(serde_json::Value::String(x.str().into())),
        jsonb::Value::SmallArray(x) => binlog_to_serde_array::<Small>(x),
        jsonb::Value::LargeArray(x) => binlog_to_serde_array::<Large>(x),
        jsonb::Value::SmallObject(x) => binlog_to_serde_object::<Small>(x),
        jsonb::Value::LargeObject(x) => binlog_to_serde_object::<Large>(x),
        jsonb::Value::Opaque(x) => binlog_opaque_to_serde_value(x),
    }
}

/// Parse column index from column name by skipping the first character
///
/// # Arguments
/// * `col_name` - Column name string, expected to be in format "@{index}"
///
/// # Returns
/// * `Option<usize>` - The parsed index if valid, None otherwise
#[inline]
fn parse_column_index(col_name: &str) -> Option<usize> {
    let bytes = col_name.as_bytes();
    if bytes.first() == Some(&b'@') {
        atoi::<usize>(&bytes[1..])
    } else {
        None
    }
}

fn binlog_row_to_noria_row(
    binlog_row: &BinlogRow,
    tme: &binlog::events::TableMapEvent<'static>,
    collation_vec: &[Option<u16>],
    signedness_vec: &[Option<bool>],
) -> ReadySetResult<Vec<DfValue>> {
    binlog_row
        .columns()
        .iter()
        .enumerate()
        .map(|(idx, col)| {
            let tme_idx = parse_column_index(&col.name_str()).unwrap();
            match binlog_row.as_ref(idx).unwrap() {
                BinlogValue::Value(val) => {
                    let (kind, meta) = (
                        tme.get_column_type(tme_idx)
                            .map_err(|e| internal_err!("Unable to get column type {}", e))?
                            .unwrap(),
                        tme.get_column_metadata(tme_idx).unwrap(),
                    );
                    debug_assert!(
                        !kind.is_character_type() || collation_vec[tme_idx].is_some(),
                        "Unexpected column type {kind:?} has no collation; val={val:?}, meta={meta:?}, col={col:?}",
                    );
                    let collation = collation_vec[tme_idx].unwrap_or(0);
                    let unsigned = signedness_vec[tme_idx].unwrap_or(false);
                    // `BLOB` columns have `BINARY_FLAG` set in snapshot, but not here. However, the
                    // collation should be correctly set to binary here, though it's not over there.
                    let binary = col.flags().contains(ColumnFlags::BINARY_FLAG)
                        || collation == CollationId::BINARY as u16
                        || col.character_set() == CollationId::BINARY as u16;
                    binlog_val_to_noria_val(val, kind, meta, collation, unsigned, binary)
                }
                BinlogValue::Jsonb(val) => {
                    let json: Result<serde_json::Value, _> = val.clone().try_into(); // urgh no TryFrom impl
                    match json {
                        Ok(val) => Ok(DfValue::from(&val)),
                        Err(JsonbToJsonError::Opaque) => {
                            Ok(DfValue::from(&binlog_to_serde_jsonb_value(val)?))
                        }
                        Err(JsonbToJsonError::InvalidUtf8(e)) => Err(e.into()),
                        Err(JsonbToJsonError::InvalidJsonb(e)) => Err(e.into()),
                    }
                }
                _ => Err(internal_err!("Expected a value in WRITE_ROWS_EVENT")),
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
    ) -> ReadySetResult<(Vec<ReplicationAction>, ReplicationOffset)> {
        let (actions, pos) = self.next_action_inner(until).await?;
        Ok((actions, pos.into()))
    }
}
