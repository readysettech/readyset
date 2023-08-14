use readyset_vitess_data::VStreamPosition;
use tonic::transport::Channel;
use vitess_grpc::vtgateservice::vitess_client::VitessClient;

/// A connector that connects to a Vitess cluster following its VStream from a given position.
pub struct VitessConnector {
    /// This is the underlying (regular) MySQL connection
    client: VitessClient<Channel>,

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
