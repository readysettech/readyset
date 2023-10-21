use std::collections::VecDeque;

use async_trait::async_trait;
use nom_sql::Relation;
use readyset_client::recipe::ChangeList;
use readyset_client::TableOperation;
use readyset_data::{DfValue, Dialect};
use readyset_errors::{invariant, ReadySetError, ReadySetResult};
use readyset_vitess_data::SchemaCache;
use replication_offset::vitess::VStreamPosition;
use replication_offset::ReplicationOffset;
use tokio::sync::mpsc;
use tonic::Streaming;
use tracing::{debug, error, info, trace, warn};
use vitess_grpc::binlogdata::{Filter, Rule, ShardGtid, VEvent, VEventType, VGtid};
use vitess_grpc::query::Row;
use vitess_grpc::topodata::TabletType;
use vitess_grpc::vtgate::{VStreamFlags, VStreamRequest, VStreamResponse};
use vitess_grpc::vtgateservice::vitess_client::VitessClient;

use crate::noria_adapter::{Connector, ReplicationAction};

/// A connector that connects to a Vitess cluster following its VStream from a given position.
pub(crate) struct VitessConnector {
    // Channel with separate VStream API response events
    vstream_events: mpsc::Receiver<VEvent>,

    /// The GTID of the current transaction, we only receive and change it on VGTID events,
    /// which are delivered at the end of a transaction. So, if we ever need to recover,
    /// we'll do that from the end of the last transaction.
    current_position: Option<VStreamPosition>,

    // Schema cache for all the tables we have seen so far
    schema_cache: SchemaCache,

    // Whether to log statements received by the connector
    enable_statement_logging: bool,

    // Event buffer for all ROW events in the current transaction
    //
    // We need to return ReplicationAction with the correct position, but the position is
    // delivered to us in the VGTID event at the very end of the transaction.
    // So, we need to buffer all the ROW events until we receive the VGTID event
    // (or, rather, until COMMIT).
    //
    // Note: The buffer can contain rows from multiple tables, so we cannot combine all events into
    // a single set of table actions. For now we process one event at a time, but we could
    // potentially process all events for a single table at once.
    row_buffer: VecDeque<VEvent>,
}

impl VitessConnector {
    pub(crate) async fn connect(
        vitess_config: database_utils::VitessConfig,
        table: Option<&Relation>,
        initial_position: Option<&VStreamPosition>,
        enable_statement_logging: bool,
    ) -> ReadySetResult<Self> {
        // Connect to Vitess
        let mut client = VitessClient::connect(vitess_config.grpc_url())
            .await
            .map_err(|err| {
                error!("Could not connect to Vitess: {}", err);
                readyset_errors::ReadySetError::InvalidUpstreamDatabase
            })?;
        info!("Connected to Vitess");

        // Configure the details of VStream
        let vstream_flags = VStreamFlags {
            stop_on_reshard: true,
            heartbeat_interval: 60,
            ..Default::default()
        };

        // Start from the beginning (run copy, then follow the changes) or use the latest position
        let initial_vgtid = match initial_position {
            Some(pos) => pos.into(),
            None => VGtid {
                shard_gtids: vec![ShardGtid {
                    keyspace: vitess_config.keyspace(),
                    ..Default::default()
                }],
            },
        };
        info!("Starting VStream from: {:?}", initial_vgtid);

        // Set up the table filter if needed
        let table_filter = match table {
            Some(table) => {
                let table_name = table.name.to_string();
                let table_rule = Rule {
                    r#match: table_name,
                    ..Default::default()
                };

                Some(Filter {
                    rules: vec![table_rule],
                    ..Default::default()
                })
            }
            None => None,
        };
        debug!("Table filter: {:?}", table_filter);

        // Make the VStream API request to start streaming changes from the cluster
        let request = VStreamRequest {
            vgtid: Some(initial_vgtid),
            tablet_type: TabletType::Primary.into(),
            flags: Some(vstream_flags),
            filter: table_filter,
            ..Default::default()
        };

        let vstream = client.v_stream(request).await.map_err(|err| {
            error!("Could not start VStream: {}", err);
            readyset_errors::ReadySetError::InvalidUpstreamDatabase
        })?;

        // If we were given an initial position, we should use it as the current position
        // since no VGTID events will be sent to us until the end of the first transaction
        let current_position = initial_position.cloned();

        // Run the VStream API request in a separate task, getting events via a channel
        let (tx, rx) = mpsc::channel(1);
        tokio::spawn(Self::run_v_stream(vstream.into_inner(), tx));

        let connector = VitessConnector {
            vstream_events: rx,
            current_position,
            schema_cache: SchemaCache::new(vitess_config.keyspace().as_ref()),
            enable_statement_logging,
            row_buffer: VecDeque::with_capacity(10),
        };

        Ok(connector)
    }

    pub(crate) fn disconnect(&mut self) {
        self.vstream_events.close();
    }

    // A separate task that receives VStream events and sends them to a channel
    async fn run_v_stream(
        mut vstream: Streaming<VStreamResponse>,
        tx: mpsc::Sender<VEvent>,
    ) -> Result<(), readyset_errors::ReadySetError> {
        while !tx.is_closed() {
            // Wait for the next message from VStream
            let response = vstream.message().await;

            // If the channel is closed, we should exit and can ignore the result from the VStream
            if tx.is_closed() {
                break;
            }

            let response = response.map_err(|err| {
                error!("Could not receive VStream event: {}", err);
                readyset_errors::ReadySetError::InvalidUpstreamDatabase
            })?;

            match response {
                Some(response) => {
                    for message in response.events {
                        if tx.is_closed() {
                            break;
                        }

                        tx.send(message).await.map_err(|err| {
                            error!("Could not send VStream event to channel: {}", err);
                            readyset_errors::ReadySetError::InvalidUpstreamDatabase
                        })?;
                    }
                }
                None => {
                    debug!("Vitess stream closed, exiting");
                    break;
                }
            }
        }

        debug!("Vitess stream or the event consumer closed, exiting the VStream task");
        return Ok(());
    }

    pub(crate) fn lookup_table(
        &self,
        table_name: &str,
        keyspace: &str,
    ) -> ReadySetResult<&readyset_vitess_data::Table> {
        self.schema_cache.tables.get(table_name).ok_or_else(|| {
            ReadySetError::ReplicationFailed(format!(
                "Unknown table '{}' in keyspace '{}'",
                table_name, keyspace
            ))
        })
    }

    fn current_offset(&self) -> Option<ReplicationOffset> {
        self.current_position
            .as_ref()
            .map(|pos| ReplicationOffset::Vitess(pos.clone()))
    }

    // Process a VStream ROW event and return a ReplicationAction object to be used by Noria
    fn process_row_event(
        &self,
        event: &VEvent,
    ) -> ReadySetResult<(ReplicationAction, ReplicationOffset)> {
        let row_event = event.row_event.as_ref().unwrap();
        let keyspace = &row_event.keyspace;
        let fully_qualified_table_name = &row_event.table_name;
        let table_name = fully_qualified_table_name
            .split('.')
            .last()
            .ok_or_else(|| {
                ReadySetError::ReplicationFailed(format!(
                    "Could not extract table name from fully qualified name: {}",
                    fully_qualified_table_name
                ))
            })?;

        if self.schema_cache.keyspace != *keyspace {
            return Err(ReadySetError::ReplicationFailed(format!(
                "Unexpected keyspace '{}' encountered in a ROW event while following the '{}' keyspace",
                keyspace, self.schema_cache.keyspace
            )));
        }

        let table = self.lookup_table(table_name, &row_event.keyspace)?;

        // Process all row changes for the table
        info!(
            "Received a ROW event for table '{}' with {} changes",
            table_name,
            row_event.row_changes.len()
        );
        let mut table_ops = Vec::with_capacity(row_event.row_changes.len());
        for row_change in row_event.row_changes.iter() {
            let row_operation = readyset_vitess_data::row_operation(row_change);

            // Generate a table operation for the row
            match row_operation {
                readyset_vitess_data::RowOperation::Insert => {
                    table_ops.push(TableOperation::Insert(self.row_change_to_noria_row(
                        &table,
                        &row_change.after.as_ref().unwrap(),
                    )?));
                }

                readyset_vitess_data::RowOperation::Delete => {
                    table_ops.push(TableOperation::DeleteRow {
                        row: self.row_change_to_noria_row(
                            &table,
                            &row_change.before.as_ref().unwrap(),
                        )?,
                    });
                }

                readyset_vitess_data::RowOperation::Update => {
                    table_ops.push(TableOperation::DeleteRow {
                        row: self.row_change_to_noria_row(
                            &table,
                            &row_change.before.as_ref().unwrap(),
                        )?,
                    });

                    table_ops.push(TableOperation::Insert(self.row_change_to_noria_row(
                        &table,
                        &row_change.after.as_ref().unwrap(),
                    )?));
                }
            }
        }

        let action = ReplicationAction::TableAction {
            table: Relation {
                schema: Some(keyspace.into()),
                name: table_name.into(),
            },
            actions: table_ops,
            txid: None, // VStream does not provide transaction IDs
        };

        invariant!(
            self.current_position.is_some(),
            "We haven't seen a VGTID event yet trying to process a ROW. No current position information can be found!"
        );

        let pos = self.current_offset().unwrap();

        Ok((action, pos))
    }

    pub(crate) fn row_change_to_noria_row(
        &self,
        table: &readyset_vitess_data::Table,
        row_change: &Row,
    ) -> ReadySetResult<Vec<DfValue>> {
        table.vstream_row_to_noria_row(row_change).map_err(|e| {
            ReadySetError::ReplicationFailed(format!(
                "Could not convert VStream row to Noria row: {}",
                e
            ))
        })
    }

    // Waits for the next actionable VStream event (handles heartbeats, fields, etc internally)
    pub(crate) async fn next_event(&mut self) -> Option<VEvent> {
        while let Some(event) = self.vstream_events.recv().await {
            if self.enable_statement_logging {
                info!("Received VStream event: {:?}", &event);
            }

            if event.r#type() == VEventType::Heartbeat {
                info!("Received VStream heartbeat");
                continue;
            }

            // We receive this right before the first time we see a new table within the stream
            if event.r#type() == VEventType::Field {
                let field_event = event.field_event.unwrap();
                info!(
                    "Received VStream FIELD event for table: {}",
                    &field_event.table_name
                );
                self.schema_cache.process_field_event(&field_event).unwrap();
                continue;
            }

            return Some(event);
        }

        // VStream closed
        None
    }
}

#[async_trait]
impl Connector for VitessConnector {
    /// Process VStream events until an actionable event occurs.
    async fn next_action(
        &mut self,
        _last_pos: &ReplicationOffset,
        until: Option<&ReplicationOffset>,
    ) -> ReadySetResult<(ReplicationAction, ReplicationOffset)> {
        // First, check if we have any buffered events from the previous iteration
        if let Some(row_event) = self.row_buffer.pop_front() {
            return self.process_row_event(&row_event);
        }

        // This loop runs until we have something to return to the caller or we reach the until
        // position (which is used in a catch-up scenario after a snapshot)
        while let Some(event) = self.next_event().await {
            match event.r#type() {
                VEventType::Heartbeat => unreachable!("Heartbeats are handled by next_event()"),
                VEventType::Field => unreachable!("Field events are handled by next_event()"),

                VEventType::Vgtid => {
                    info!("Received VStream VGTID");
                    invariant!(
                        event.vgtid.is_some(),
                        "Received a VGTID event without a VGTID"
                    );

                    let vstream_pos: VStreamPosition =
                        event.vgtid.unwrap().try_into().map_err(|e| {
                            ReadySetError::ReplicationFailed(format!(
                                "Could not convert VGTID to VStream position: {}",
                                e
                            ))
                        })?;
                    self.current_position = Some(vstream_pos.clone());

                    // Now that we have our new position, we can return any buffered events
                    // Tbe first event is returned here, the rest are handled in the next call of
                    // this function.
                    if let Some(row_event) = self.row_buffer.pop_front() {
                        return self.process_row_event(&row_event);
                    }

                    // Check the row buffer is empty and we have reached the catch-up position
                    if let Some(limit) = until {
                        let current_offset = ReplicationOffset::Vitess(vstream_pos);
                        if current_offset >= *limit {
                            return Ok((ReplicationAction::LogPosition, current_offset));
                        }
                    }
                }

                VEventType::Begin => {
                    info!("Received VStream begin");
                    if !self.row_buffer.is_empty() {
                        warn!(
                            "There were {} buffered event(s) from the previous transaction! Dropping them.",
                            self.row_buffer.len()
                        );
                        self.row_buffer.clear();
                    }
                }

                VEventType::Commit => {
                    info!("Received VStream commit");
                    if let Some(pos) = &self.current_position {
                        info!("Returning VStream position: {}", &pos);
                        return Ok((
                            ReplicationAction::LogPosition,
                            ReplicationOffset::Vitess(pos.clone()),
                        ));
                    }
                }

                VEventType::Row => {
                    trace!(
                        "Received VStream ROW event, buffering until the end of the transaction"
                    );
                    self.row_buffer.push_back(event);
                }

                VEventType::Ddl => {
                    info!("Received VStream DDL");
                    invariant!(
                        !event.statement.is_empty(),
                        "Received a DDL event without a statement"
                    );

                    // DDLs should automatically commit any open transactions
                    invariant!(
                        self.row_buffer.is_empty(),
                        "Received a DDL event while there are still buffered ROW events"
                    );

                    // There should be a VGTID event before the DDL
                    invariant!(
                        self.current_position.is_some(),
                        "We haven't seen a VGTID event yet trying to process a ROW. No current position information can be found!"
                    );

                    let changes = match ChangeList::from_str(
                        &event.statement,
                        Dialect::DEFAULT_MYSQL,
                    ) {
                        Ok(change_list) => change_list.changes,
                        Err(error) => {
                            warn!(%error, "Error extending recipe, DDL statement will not be used");
                            continue;
                        }
                    };

                    return Ok((
                        ReplicationAction::DdlChange {
                            schema: event.keyspace,
                            changes,
                        },
                        self.current_offset().unwrap(),
                    ));
                }

                VEventType::CopyCompleted => {
                    error!("COPY_COMPLETED received from VStream, but we should not be in a snapshot mode!");
                    return Err(ReadySetError::ReplicationFailed(
                        "Unexpected COPY_COMPLETED received from VStream".to_string(),
                    ));
                }

                // Probably safe to ignore
                VEventType::Unknown
                | VEventType::Insert
                | VEventType::Replace
                | VEventType::Update
                | VEventType::Delete
                | VEventType::Set
                | VEventType::Other
                | VEventType::Rollback
                | VEventType::Journal
                | VEventType::Version
                | VEventType::Lastpk
                | VEventType::Gtid
                | VEventType::Savepoint => {
                    warn!("Received unsupported VStream event: {:?}", &event);
                }
            }
        }

        info!("Vitess stream closed, no more events coming");
        // Not sure if there is a better return value here
        Err(ReadySetError::ReplicationFailed(
            "Vitess stream closed".to_string(),
        ))
    }
}
