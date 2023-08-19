use async_trait::async_trait;
use readyset_client::replication::ReplicationOffset;
use readyset_errors::{ReadySetError, ReadySetResult};
use readyset_vitess_data::VStreamPosition;
use tokio::sync::mpsc;
use tonic::transport::Channel;
use tonic::Streaming;
use tracing::info;
use vitess_grpc::binlogdata::{ShardGtid, VEvent, VGtid};
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
                keyspace: vitess_config.keyspace,
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
            enable_statement_logging,
        };

        Ok(connector)
    }

    async fn run_v_stream(
        mut vstream: Streaming<VStreamResponse>,
        tx: mpsc::Sender<VEvent>,
    ) -> Result<(), readyset_errors::ReadySetError> {
        loop {
            let response = vstream
                .message()
                .await
                .map_err(|_| readyset_errors::ReadySetError::InvalidUpstreamDatabase)?;

            match response {
                Some(response) => {
                    for message in response.events {
                        tx.send(message).await.unwrap();
                    }
                }
                None => {
                    info!("Vitess stream closed");
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
        let event = self.vstream_events.recv().await;

        match event {
            Some(event) => {
                info!("Received VStream event: {:?}", &event);

                let pos = ReplicationOffset {
                    replication_log_name: String::new(),
                    offset: 0,
                };

                Ok((ReplicationAction::LogPosition, pos))
            }
            None => {
                info!("Vitess stream closed");
                Err(ReadySetError::ReplicationFailed(
                    "Vitess stream closed".to_string(),
                ))
            }
        }
    }
}
