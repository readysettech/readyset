use async_trait::async_trait;
use nom_sql::Relation;
use readyset_client::replication::ReplicationOffset;
use readyset_errors::{ReadySetError, ReadySetResult};
use readyset_vitess_data::{SchemaCache, VStreamPosition};
use tokio::sync::mpsc;
use tonic::Streaming;
use tracing::{error, info, warn};
use vitess_grpc::binlogdata::{ShardGtid, VEvent, VEventType, VGtid};
use vitess_grpc::topodata::TabletType;
use vitess_grpc::vtgate::{VStreamFlags, VStreamRequest, VStreamResponse};
use vitess_grpc::vtgateservice::vitess_client::VitessClient;

use crate::noria_adapter::{Connector, ReplicationAction};

/// A connector that connects to a Vitess cluster following its VStream from a given position.
pub struct VitessConnector {
    // Channel with separate VStream API response events
    vstream_events: mpsc::Receiver<VEvent>,

    /// Reader is a decoder for binlog events
    // reader: binlog::EventStreamReader,
    /// The binlog "slave" must be assigned a unique `server_id` in the replica topology
    /// if one is not assigned we will use (u32::MAX - 55)
    // server_id: Option<u32>,
    /// If we just want to continue reading the binlog from a previous point
    // next_position: BinlogPosition,

    /// The GTID of the current transaction. Table modification events will have
    /// the current GTID attached if enabled in mysql.
    current_position: Option<VStreamPosition>,

    // Schema cache for all the tables we have seen so far
    schema_cache: SchemaCache,

    // / Whether to log statements received by the connector
    enable_statement_logging: bool,
}

impl VitessConnector {
    pub(crate) async fn connect(
        vitess_config: database_utils::VitessConfig,
        _config: database_utils::UpstreamConfig,
        enable_statement_logging: bool,
    ) -> ReadySetResult<Self> {
        // Connect to Vitess
        let mut client = VitessClient::connect(vitess_config.grpc_url())
            .await
            .map_err(|_| readyset_errors::ReadySetError::InvalidUpstreamDatabase)?;
        info!("Connected to Vitess");

        // Configure the details of VStream
        let vstream_flags = VStreamFlags {
            stop_on_reshard: true,
            heartbeat_interval: 5,
            ..Default::default()
        };

        // Start from the beginning (run copy, then follow the changes)
        let initial_position = VGtid {
            shard_gtids: vec![ShardGtid {
                keyspace: vitess_config.keyspace(),
                ..Default::default()
            }],
        };

        // Make the VStream API request to start streaming changes from the cluster
        let request = VStreamRequest {
            vgtid: Some(initial_position),
            tablet_type: TabletType::Primary.into(),
            flags: Some(vstream_flags),
            ..Default::default()
        };

        let vstream = client
            .v_stream(request)
            .await
            .map_err(|_| readyset_errors::ReadySetError::InvalidUpstreamDatabase)?
            .into_inner();

        // Run the VStream API request in a separate task, getting events via a channel
        let (tx, rx) = mpsc::channel(1);
        tokio::spawn(Self::run_v_stream(vstream, tx));

        let connector = VitessConnector {
            vstream_events: rx,
            current_position: None,
            schema_cache: SchemaCache::new(vitess_config.keyspace().as_ref()),
            enable_statement_logging,
        };

        Ok(connector)
    }

    async fn run_v_stream(
        mut vstream: Streaming<VStreamResponse>,
        tx: mpsc::Sender<VEvent>,
    ) -> Result<(), readyset_errors::ReadySetError> {
        loop {
            let response = vstream.message().await.map_err(|err| {
                error!("Could not receive VStream event: {}", err);
                readyset_errors::ReadySetError::InvalidUpstreamDatabase
            })?;

            match response {
                Some(response) => {
                    for message in response.events {
                        tx.send(message).await.map_err(|err| {
                            error!("Could not send VStream event to channel: {}", err);
                            readyset_errors::ReadySetError::InvalidUpstreamDatabase
                        })?;
                    }
                }
                None => {
                    warn!("Vitess stream closed, exiting");
                    drop(tx);
                    return Ok(());
                }
            }
        }
    }
}

#[async_trait]
impl Connector for VitessConnector {
    /// Process VStream events until an actionable event occurs.
    async fn next_action(
        &mut self,
        _: &ReplicationOffset,
        _until: Option<&ReplicationOffset>,
    ) -> ReadySetResult<(ReplicationAction, ReplicationOffset)> {
        // This loop runs until we have something to return to the caller
        loop {
            let event = self.vstream_events.recv().await;
            if event.is_none() {
                info!("Vitess stream closed, no more events coming");
                // TODO: Return the position action?
                return Err(ReadySetError::ReplicationFailed(
                    "Vitess stream closed".to_string(),
                ));
            }

            let event = event.unwrap();
            if self.enable_statement_logging {
                info!("Received VStream event: {:?}", &event);
            }

            match event.r#type() {
                VEventType::Heartbeat => info!("Received VStream heartbeat"),
                VEventType::Vgtid => {
                    info!("Received VStream VGTID");
                    self.current_position = Some(event.vgtid.unwrap().into());
                }

                // TODO: When adding support for event buffering, we may want to flush on commit
                VEventType::Begin => info!("Received VStream begin"),
                VEventType::Commit => info!("Received VStream commit"),

                VEventType::Field => {
                    let field_event = event.field_event.unwrap();
                    info!(
                        "Received VStream FIELD event for table: {}",
                        &field_event.table_name
                    );
                    self.schema_cache.process_field_event(&field_event);
                }

                // This assumes that:
                // 1. We are following a single keyspace
                // 2. Each ROW event will be converted to a single ReplicationAction
                // 3. Each generated ReplicationAction will have a single action inside (performance
                // issues may be caused by too granular rpc calls to Noria; ,ay want to buffer
                // events like the Postgres connector does)
                VEventType::Row => return process_row_event(&event, &self.schema_cache),

                // TODO: Maybe handle these?
                VEventType::Ddl => info!("Received VStream DDL"),

                // TODO: Handle this specially as a part of snapshot implementation
                VEventType::CopyCompleted => info!("Received VStream copy completed"),

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
                    continue;
                }
            }

            // TODO: Check until position and potentially return:
            // Ok((ReplicationAction::LogPosition, &self.next_position));
        }
    }
}

fn process_row_event(
    event: &VEvent,
    schema_cache: &SchemaCache,
) -> ReadySetResult<(ReplicationAction, ReplicationOffset)> {
    let row_event = event.row_event.as_ref().unwrap();
    let keyspace = &row_event.keyspace;
    let table_name = &row_event.table_name;

    if schema_cache.keyspace != *keyspace {
        return Err(ReadySetError::ReplicationFailed(format!(
            "Unexpected keyspace '{}' encountered in a ROW event while following the '{}' keyspace",
            keyspace, schema_cache.keyspace
        )));
    }

    let table = schema_cache.tables.get(table_name).ok_or_else(|| {
        ReadySetError::ReplicationFailed(format!(
            "Unknown table '{}' in keyspace '{}'",
            table_name, row_event.keyspace
        ))
    })?;

    // TODO: figure out the type of the event and process it accordingly
    // 1. Determine operation type
    // 2. Generate a table operation for the row

    let table_ops = vec![];
    let action = ReplicationAction::TableAction {
        table: Relation {
            schema: Some(keyspace.into()),
            name: table_name.into(),
        },
        actions: table_ops,
        txid: None, // VStream does not provide transaction IDs
    };

    let pos = ReplicationOffset {
        replication_log_name: String::new(),
        offset: 0,
    };

    Ok((action, pos))
}
