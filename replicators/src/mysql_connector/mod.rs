mod connector;
mod snapshot;

use std::fmt::{self, Display};

pub(crate) use connector::MySqlBinlogConnector;
pub(crate) use snapshot::MySqlReplicator;

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct BinlogPosition {
    pub binlog_file: String,
    pub position: u32,
}

impl Display for BinlogPosition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.binlog_file, self.position)
    }
}
