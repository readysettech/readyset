mod connector;
mod snapshot;

pub use connector::MySqlBinlogConnector;
pub use snapshot::MySqlReplicator;

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct BinlogPosition {
    pub binlog_file: String,
    pub position: u32,
}
