use noria::consistency::Timestamp;

/// Unique identifier for a client write discernable at both the
/// debezium connector and the noria client.
pub enum WriteId {
    TransactionId(u64),
}

/// A key identifying the objects we are maintaining read-your-write
/// consistency over. This may be table names, shards, rows in tables.
pub enum WriteKey {
    TableName(String),
}

/// The timestamp client provides users the ability to synchronize
/// read-your-write state at the noria client and debezium connector.
// TODO(justin): Integrate with server compnoent of timestamp service.
#[allow(dead_code)]
pub struct TimestampClient {}

impl TimestampClient {
    /// Creates a timestamp client with no connection information.
    pub fn default() -> Self {
        TimestampClient {}
    }

    /// Sends a set of write keys, `keys` for a single write, identifier by `id` to
    /// the timestamp server. Returns the updated timestamp for the write.
    pub fn append_write(&self, _id: WriteId, _keys: Vec<WriteKey>) -> Timestamp {
        Timestamp::default()
    }

    /// Gets the timestamp associated with a specific write id, `id`.
    pub fn get_timestamp(&self, _id: WriteId) -> Timestamp {
        Timestamp::default()
    }
}
