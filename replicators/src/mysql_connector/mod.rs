mod connector;
mod snapshot;

pub(crate) use connector::MySqlBinlogConnector;
pub(crate) use snapshot::MySqlReplicator;

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct BinlogPosition {
    pub binlog_file: String,
    pub position: u32,
}
