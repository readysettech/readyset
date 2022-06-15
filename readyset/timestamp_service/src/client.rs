use anyhow::{anyhow, Error};
use readyset::consistency::Timestamp;
use readyset::internal::LocalNodeIndex;

/// Unique identifier for a client write discernable at both the
/// debezium connector and the ReadySet client.
pub enum WriteId {
    // MySQL global transaction identifier in form: <server-id>:<gtid>
    MySqlGtid(String),
}

/// A key identifying the objects we are maintaining read-your-write
/// consistency over. This may be table names, shards, rows in tables.
pub enum WriteKey {
    // String representation of the table name, i.e. "employees"
    TableName(String),
    // Index of the table, to be used directly in timestamps.
    // The `node` field within a table handle from readyset/readyset
    TableIndex(LocalNodeIndex),
}

/// The timestamp client provides users the ability to synchronize
/// read-your-write state at the ReadySet client and debezium connector.
// TODO: Integrate with server compnoent of timestamp service.
// TODO: TimestampClient only works with MySQL.
#[allow(dead_code)]
#[derive(Clone)]
pub struct TimestampClient {}

impl TimestampClient {
    /// Creates a timestamp client with no connection information.
    pub fn default() -> Self {
        TimestampClient {}
    }

    /// Sends a set of write keys, `keys` for a single write, identifier by `write_id` to
    /// the timestamp server. Returns the updated timestamp for the write.
    // TODO(andrew): Currently only parsing MySql GTID's is supported.
    pub fn append_write(&self, write_id: WriteId, keys: Vec<WriteKey>) -> Result<Timestamp, Error> {
        // TODO: implement to interact with true service
        let timestamp_val: u64 = match write_id {
            WriteId::MySqlGtid(id) => {
                // Expecting form: <server-id>:<id>
                let mut gtid_tokens = id.split(':');
                let _server_id = gtid_tokens.next();
                // TODO: Proper error handling.
                let txid = gtid_tokens.next().ok_or_else(|| {
                    anyhow!("GTID Parising Failure: GTID does not have a valid sequence number")
                })?;
                txid.parse()
                    .map_err(|_| anyhow!("GTID Parse Failure: GTID sequence number not a number"))?
            }
        };

        let mut timestamp = Timestamp::default();
        for key in keys {
            match key {
                WriteKey::TableIndex(i) => {
                    timestamp.map.insert(i, timestamp_val);
                }
                _ => unimplemented!(
                    "Only table index write keys are supported in local timestamp service"
                ),
            }
        }
        Ok(timestamp)
    }

    /// Gets the timestamp associated with a specific write id, `id`.
    /// TODO: this method not used until timestamp service is added
    pub fn get_timestamp(&self, _id: WriteId) -> Timestamp {
        Timestamp::default()
    }
}
