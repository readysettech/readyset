//! This module implements 100% of the current Postgres end: (), time: (), reply: ()  end: (), time:
//! (), reply: ()  WAL spec as defined in [Streaming Replication Protocol](https://www.postgresql.org/docs/current/protocol-replication.html)
//! and [Logical Replication Message Formats](https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html)
//! every struct here maps precisely to the definitions in the spec, and tries to avoid
//! abstractions that don't map exactly. The parsing is therefore very much straightforward.

use std::convert::{TryFrom, TryInto};

use bytes::Bytes;
use nom_sql::Relation;
use readyset_errors::ReadySetError;
use replication_offset::postgres::{CommitLsn, Lsn};

/// An parse error
#[derive(Debug)]
pub enum WalError {
    /// Got empty record
    Empty,
    /// Incorrect length for record, hold record type
    IncorrectLen(u8),
    UnterminatedString,
    CorruptTuple,
    UnknownTuple,
    CorruptRelation,
    CorruptUpdate,
    CorruptType,
    CorruptInsert,
    CorruptDelete,
    CorruptTruncate,
    CorruptMessage,
    TryFromSliceError,
    ReadySetError(ReadySetError),
    ConnectionLost(String),

    /// An error specific to one of the tables
    TableError {
        kind: TableErrorKind,
        table: String,
        schema: String,
    },
}

/// The kinds of table-specific errors that can arise during replication
#[derive(Debug)]
pub enum TableErrorKind {
    FloatParseError,
    IntParseError,
    BoolParseError,
    CStrParseError,
    JsonParseError(String),
    ByteArraySyntaxError,
    ByteArrayHexParseError,
    TimestampParseError,
    TimestampTzParseError,
    DateParseError,
    TimeParseError(mysql_time::ConvertError),
    NumericParseError(rust_decimal::Error),
    BitVectorParseError(String),
    InvalidMapping(String),
    UnsupportedTypeConversion { type_oid: u32 },
    UnknownEnumVariant(Bytes),
    UnexpectedUnchangedEntry { reason: &'static str },
}

impl From<std::array::TryFromSliceError> for WalError {
    fn from(_: std::array::TryFromSliceError) -> Self {
        WalError::TryFromSliceError
    }
}

impl From<WalError> for ReadySetError {
    fn from(err: WalError) -> Self {
        match err {
            WalError::TableError {
                kind,
                table,
                schema,
            } => {
                let table = Relation {
                    schema: Some(schema.into()),
                    name: table.into(),
                };

                ReadySetError::TableError {
                    table,
                    source: Box::new(ReadySetError::ReplicationFailed(format!(
                        "WAL error: {:?}",
                        kind
                    ))),
                }
            }
            _ => ReadySetError::ReplicationFailed(format!("WAL error: {:?}", err)),
        }
    }
}

impl From<ReadySetError> for WalError {
    fn from(err: ReadySetError) -> Self {
        WalError::ReadySetError(err)
    }
}

/// `WalData` represents a single [WAL message](https://www.postgresql.org/docs/current/protocol-replication.html)
#[derive(Debug, PartialEq, Eq)]
pub enum WalData {
    XLogData {
        /// The starting point of the WAL data in this message.
        start: Lsn,
        /// The current end of WAL on the server.
        end: Lsn,
        /// The server's system clock at the time of transmission, as microseconds since midnight
        /// on 2000-01-01.
        time: i64,
        /// A section of the WAL data stream.
        /// A single WAL record is never split across two XLogData messages. When a WAL record
        /// crosses a WAL page boundary, and is therefore already split using continuation
        /// records, it can be split at the page boundary. In other words, the first main
        /// WAL record and its continuation records can be sent in different XLogData messages.
        data: WalRecord,
    },
    /// Primary keepalive message
    Keepalive {
        /// The current end of WAL on the server.
        end: Lsn,
        /// The server's system clock at the time of transmission, as microseconds since midnight
        /// on 2000-01-01.
        time: i64,
        /// 1 means that the client should reply to this message as soon as possible, to avoid a
        /// timeout disconnect. 0 otherwise. The receiving process can send replies back to
        /// the sender at any time, using one of the following message formats (also in the
        /// payload of a CopyData message):
        reply: u8,
    },
    StandbyStatusUpdate {
        /// The location of the last WAL byte + 1 received and written to disk in the standby.
        ack: Lsn,
        /// The location of the last WAL byte + 1 flushed to disk in the standby.
        flushed: Lsn,
        /// The location of the last WAL byte + 1 applied in the standby.
        applied: Lsn,
        /// The client's system clock at the time of transmission, as microseconds since midnight
        /// on 2000-01-01.
        time: i64,
        /// If 1, the client requests the server to reply to this message immediately. This can be
        /// used to ping the server, to test if the connection is still healthy.
        reply: u8,
    },
    HotStandbyFeedback {
        /// The client's system clock at the time of transmission, as microseconds since midnight
        /// on 2000-01-01.
        time: i64,
        /// The standby's current global xmin, excluding the catalog_xmin from any replication
        /// slots. If both this value and the following catalog_xmin are 0 this is treated
        /// as a notification that Hot Standby feedback will no longer be sent on this
        /// connection. Later non-zero messages may reinitiate the feedback mechanism.
        xmin: i32,
        /// The epoch of the global xmin xid on the standby.
        epoch: i32,
        /// The lowest catalog_xmin of any replication slots on the standby. Set to 0 if no
        /// catalog_xmin exists on the standby or if hot standby feedback is being disabled.
        catalog_xmin: i32,
        /// The epoch of the catalog_xmin xid on the standby.
        epoch_catalog_xmin: i32,
    },
    Unknown(Bytes),
}

#[derive(Debug, PartialEq, Eq)]
pub struct RelationMapping {
    /// ID of the relation.
    pub(crate) id: i32,
    /// Schema (aka namespace). This is an empty string for pg_catalog.
    pub(crate) schema: Bytes,
    /// Relation name.
    pub(crate) name: Bytes,
    /// Replica identity setting for the relation (same as relreplident in pg_class).
    /// Columns used to form "replica identity" for rows:
    /// d = default (primary key, if any)
    /// n = nothing
    /// f = all columns
    /// i = index with indisreplident set (same as nothing if the index used has been dropped)
    pub(crate) relreplident: i8,
    /// Number of columns.
    pub(crate) n_cols: i16,
    /// Next, the following message part appears for each column (except generated columns):
    pub(crate) cols: Vec<ColumnSpec>,
}

impl RelationMapping {
    /// Gets the name of the schema through a lossy UTF-8 interpretation of the raw `Bytes`.
    pub(crate) fn schema_name_lossy(&self) -> String {
        String::from_utf8_lossy(&self.schema).to_string()
    }

    /// Gets the name of the relation through a lossy UTF-8 interpretation of the raw `Bytes`.
    pub(crate) fn relation_name_lossy(&self) -> String {
        String::from_utf8_lossy(&self.name).to_string()
    }
}

/// `WalRecord` represents a [record](https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html)
/// in `WalData::XLogData`
#[derive(Debug, PartialEq, Eq)]
#[allow(dead_code)]
pub enum WalRecord {
    /// Sent to indicate a transaction block
    Begin {
        /// The final LSN of the transaction. This corresponds to the `lsn` field in
        /// `WalRecord::Commit`.
        final_lsn: CommitLsn,
        /// Commit timestamp of the transaction. The value is in number of microseconds since
        /// PostgreSQL epoch (2000-01-01).
        timestamp: i64,
        /// Xid of the transaction.
        xid: i32,
    },
    /// Sent to indicate a transaction block is finished
    Commit {
        /// Flags; currently unused (must be 0).
        flags: u8,
        /// The LSN of the commit. This value matches `final_lsn` in `WalData::Begin`.
        lsn: CommitLsn,
        /// The end LSN of the transaction.
        end_lsn: Lsn,
        /// Commit timestamp of the transaction. The value is in number of microseconds since
        /// PostgreSQL epoch (2000-01-01).
        timestamp: i64,
    },
    Origin {
        /// The LSN of the commit on the origin server.
        lsn: Lsn,
        /// Name of the origin.
        /// Note that there can be multiple Origin messages inside a single transaction.
        name: Bytes,
    },
    /// This message is similar to Table Map Event in MySQL binlog
    Relation(RelationMapping),
    /// Sent when [`CREATE TYPE`](https://www.postgresql.org/docs/current/sql-createtype.html) is used
    Type {
        /// ID of the data type.
        id: i32,
        /// Schema (aka namespace). This is an empty string for pg_catalog.
        schema: Bytes,
        /// Name of the data type.
        name: Bytes,
    },
    /// Sent when [`INSERT INTO`](https://www.postgresql.org/docs/current/sql-insert.html) is used
    Insert {
        /// ID of the relation corresponding to the ID in the relation message.
        relation_id: i32,
        /// The tuple to insert into the table
        new_tuple: TupleData,
    },
    /// Sent when [`UPDATE`](https://www.postgresql.org/docs/current/sql-update.html) is used
    /// By default `UPDATE` will have the `key_tuple` to identify the modified row if a key
    /// is defined for the table.
    /// For unkeyed tables the operation is only valid if `REPLICA IDENTITY FULL` is set for the
    /// table, in which case `old_tuple` will be present.
    Update {
        /// ID of the relation corresponding to the ID in the relation message.
        relation_id: i32,
        /// This field is optional and is only present if the update changed data in any of the
        /// column(s) that are part of the REPLICA IDENTITY index.
        key_tuple: Option<TupleData>,
        /// This field is optional and is only present if table in which the update happened has
        /// REPLICA IDENTITY set to FULL.
        old_tuple: Option<TupleData>,
        new_tuple: TupleData,
    },
    /// Sent when [`DELETE`](https://www.postgresql.org/docs/current/sql-delete.html) is used
    /// Same constraint as `UPDATE` apply.
    Delete {
        /// ID of the relation corresponding to the ID in the relation message.
        relation_id: i32,
        /// This field is present if the table in which the delete has happened uses an index as
        /// REPLICA IDENTITY.
        key_tuple: Option<TupleData>,
        /// This field is present if the table in which the delete happened has REPLICA IDENTITY
        /// set to FULL.
        old_tuple: Option<TupleData>,
    },
    /// Sent when [`TRUNCATE`](https://www.postgresql.org/docs/current/sql-truncate.html) is used
    Truncate {
        /// Number of relations
        n_relations: i32,
        /// Option bits for TRUNCATE: 1 for CASCADE, 2 for RESTART IDENTITY
        options: i8,
        /// ID of the relation corresponding to the ID in the relation message. Identifies the
        /// tables to truncate.
        relation_ids: Vec<i32>,
    },
    /// Sent using the [`pg_logical_emit_message`](https://www.postgresql.org/docs/current/functions-admin.html) function
    Message {
        /// Flags; Either 0 for no flags or 1 if the logical decoding message is transactional.
        transactional: bool,
        /// The LSN of the logical decoding message.
        lsn: Lsn,
        /// The prefix of the logical decoding message.
        prefix: Bytes,
        /// Length of the content.
        len: i32,
        /// The content of the logical decoding message.
        payload: Bytes,
    },
    Unknown(Bytes),
}

/// Defines the type and metadata for a single column in a `Relation` mapping
#[derive(Debug, PartialEq, Eq)]
pub struct ColumnSpec {
    /// Flags for the column. Currently can be either 0 for no flags or 1 which marks the column as
    /// part of the key.
    pub(crate) flags: i8,
    /// Name of the column.
    pub(crate) name: Bytes,
    /// Postgres `oid` of the column's data type.
    pub(crate) type_oid: u32,
    /// Type modifier of the column (atttypmod)
    pub(crate) type_modifier: i32,
}

/// Stores tuple information for `INSERT`, `UPDATE` and `DELETE`.
/// Tuple is the WAL term for a row, or partial row
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TupleData {
    pub(crate) n_cols: i16,
    pub(crate) cols: Vec<TupleEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TupleEntry {
    Null,        // This is a null value
    Unchanged,   // Keep using the previous value
    Text(Bytes), // A new value as a string
}

impl TryFrom<Bytes> for WalData {
    type Error = WalError;

    // The kind of `WalData` is identified by the value of the first byte
    fn try_from(b: Bytes) -> Result<Self, Self::Error> {
        match *b.first().ok_or(WalError::Empty)? {
            b'k' => WalData::keepalive(b),
            b'w' => WalData::xlog_data(b),
            b'r' => WalData::standby_update(b),
            b'h' => WalData::hot_standby_feedback(b),
            _ => Ok(WalData::Unknown(b)),
        }
    }
}

impl TryFrom<Bytes> for WalRecord {
    type Error = WalError;

    // The kind of `WalRecord` is identified by the value of the first byte
    fn try_from(b: Bytes) -> Result<Self, Self::Error> {
        match *b.first().ok_or(WalError::Empty)? {
            b'B' => WalRecord::begin(b),
            b'C' => WalRecord::commit(b),
            b'R' => WalRecord::relation(b),
            b'Y' => WalRecord::type_(b),
            b'U' => WalRecord::update(b),
            b'I' => WalRecord::insert(b),
            b'D' => WalRecord::delete(b),
            b'T' => WalRecord::truncate(b),
            b'M' => WalRecord::message(b),
            _ => Ok(WalRecord::Unknown(b)),
        }
    }
}

impl WalData {
    /// Parse as `Keepalive`, assumes b[0] == 'k'
    fn keepalive(b: Bytes) -> Result<Self, WalError> {
        if b.len() != 18 {
            return Err(WalError::IncorrectLen(b[0]));
        }

        let end = i64::from_be_bytes(b[1..9].try_into()?).into();
        let time = i64::from_be_bytes(b[9..17].try_into()?);
        let reply = b[17];
        Ok(WalData::Keepalive { end, time, reply })
    }

    /// Parse as `XLogData`, assumes b[0] == 'w'
    fn xlog_data(mut b: Bytes) -> Result<Self, WalError> {
        if b.len() < 25 {
            return Err(WalError::IncorrectLen(b[0]));
        }

        let start = i64::from_be_bytes(b[1..9].try_into()?).into();
        let end = i64::from_be_bytes(b[9..17].try_into()?).into();
        let time = i64::from_be_bytes(b[17..25].try_into()?);
        let data = b.split_off(25).try_into()?;

        Ok(WalData::XLogData {
            start,
            end,
            time,
            data,
        })
    }

    /// Parse as `StandbyStatusUpdate`, assumes b[0] == 'r'
    fn standby_update(b: Bytes) -> Result<Self, WalError> {
        if b.len() != 34 {
            return Err(WalError::IncorrectLen(b[0]));
        }

        let ack = i64::from_be_bytes(b[1..9].try_into()?).into();
        let flushed = i64::from_be_bytes(b[9..17].try_into()?).into();
        let applied = i64::from_be_bytes(b[17..25].try_into()?).into();
        let time = i64::from_be_bytes(b[25..33].try_into()?);
        let reply = b[33];

        Ok(WalData::StandbyStatusUpdate {
            ack,
            flushed,
            applied,
            time,
            reply,
        })
    }

    /// Parse as `HotStandbyFeedback`, assumes b[0] == 'h'
    fn hot_standby_feedback(b: Bytes) -> Result<Self, WalError> {
        if b.len() != 25 {
            return Err(WalError::IncorrectLen(b[0]));
        }

        let time = i64::from_be_bytes(b[1..9].try_into()?);
        let xmin = i32::from_be_bytes(b[9..13].try_into()?);
        let epoch = i32::from_be_bytes(b[13..17].try_into()?);
        let catalog_xmin = i32::from_be_bytes(b[17..21].try_into()?);
        let epoch_catalog_xmin = i32::from_be_bytes(b[21..25].try_into()?);

        Ok(WalData::HotStandbyFeedback {
            time,
            xmin,
            epoch,
            catalog_xmin,
            epoch_catalog_xmin,
        })
    }
}

impl WalRecord {
    /// Parse as `Begin`, assumes b[0] == 'B'
    fn begin(b: Bytes) -> Result<Self, WalError> {
        if b.len() != 21 {
            return Err(WalError::IncorrectLen(b[0]));
        }

        let final_lsn = i64::from_be_bytes(b[1..9].try_into()?).into();
        let timestamp = i64::from_be_bytes(b[9..17].try_into()?);
        let xid = i32::from_be_bytes(b[17..21].try_into()?);

        Ok(WalRecord::Begin {
            final_lsn,
            timestamp,
            xid,
        })
    }

    /// Parse as `Commit`, assumes b[0] == 'C'
    fn commit(b: Bytes) -> Result<Self, WalError> {
        if b.len() != 26 {
            return Err(WalError::IncorrectLen(b[0]));
        }

        let flags = b[1];
        let lsn = i64::from_be_bytes(b[2..10].try_into()?).into();
        let end_lsn = i64::from_be_bytes(b[10..18].try_into()?).into();
        let timestamp = i64::from_be_bytes(b[18..26].try_into()?);

        Ok(WalRecord::Commit {
            flags,
            lsn,
            end_lsn,
            timestamp,
        })
    }

    /// Finds the first occurrence of a null, and splits the buffer at that position
    /// the returned value contains all the bytes up to the null, and the input buffer
    /// references all the bytes past the null
    fn consume_string(b: &mut Bytes) -> Result<Bytes, WalError> {
        let null = b
            .iter()
            .position(|e| *e == 0)
            .ok_or(WalError::UnterminatedString)?;

        let ret = b.split_to(null); // Take the string up to the null
        let _ = b.split_to(1); // Remove the null from the input buffer
        Ok(ret)
    }

    fn consume_tuple(b: &mut Bytes) -> Result<TupleData, WalError> {
        if b.len() < 2 {
            return Err(WalError::CorruptTuple);
        }

        let n_cols = i16::from_be_bytes([b[0], b[1]]);
        let _ = b.split_to(2);

        let mut cols = Vec::with_capacity(n_cols as usize);

        for _ in 0..n_cols as usize {
            match *b.first().ok_or(WalError::CorruptTuple)? {
                // Identifies the data as NULL value.
                b'n' => {
                    let _ = b.split_to(1);
                    cols.push(TupleEntry::Null);
                }
                // Identifies unchanged TOASTed value (the actual value is not sent).
                b'u' => {
                    let _ = b.split_to(1);
                    cols.push(TupleEntry::Unchanged);
                }
                b't' => {
                    if b.len() < 5 {
                        return Err(WalError::CorruptTuple);
                    }

                    let len = i32::from_be_bytes(b[1..5].try_into()?) as usize;

                    if b.len() < len + 5 {
                        return Err(WalError::CorruptTuple);
                    }

                    let val = b.slice(5..5 + len);
                    let _ = b.split_to(5 + len);
                    cols.push(TupleEntry::Text(val));
                }
                _ => return Err(WalError::UnknownTuple),
            }
        }

        Ok(TupleData { n_cols, cols })
    }

    /// Parse as Relation, assumes b[0] == 'R'
    fn relation(mut b: Bytes) -> Result<Self, WalError> {
        if b.len() < 5 {
            return Err(WalError::CorruptRelation);
        }

        let id = i32::from_be_bytes(b[1..5].try_into()?);
        let _ = b.split_to(5);

        let schema = Self::consume_string(&mut b)?;
        let name = Self::consume_string(&mut b)?;

        if b.len() < 3 {
            return Err(WalError::CorruptRelation);
        }

        let relreplident = b[0] as i8;
        let n_cols = i16::from_be_bytes([b[1], b[2]]);
        let _ = b.split_to(3);

        // Each column spec consists of flags, name, data type and type modifier
        // those are stored sequentially for the number of columns specified
        let mut cols = Vec::with_capacity(n_cols as usize);
        for _ in 0..n_cols {
            let flags = *b.first().ok_or(WalError::CorruptRelation)? as i8;
            let _ = b.split_to(1);
            let name = Self::consume_string(&mut b)?;

            if b.len() < 8 {
                return Err(WalError::CorruptRelation);
            }

            let type_oid = u32::from_be_bytes(b[0..4].try_into()?);
            let type_modifier = i32::from_be_bytes(b[4..8].try_into()?);
            let _ = b.split_to(8);

            cols.push(ColumnSpec {
                flags,
                name,
                type_oid,
                type_modifier,
            })
        }

        if !b.is_empty() {
            // Should consume the entire record
            return Err(WalError::CorruptRelation);
        }

        Ok(WalRecord::Relation(RelationMapping {
            id,
            schema,
            name,
            relreplident,
            n_cols,
            cols,
        }))
    }

    /// Parse as `Type`, assumes `b[0] == 'Y`
    fn type_(mut b: Bytes) -> Result<Self, WalError> {
        if b.len() < 10 {
            return Err(WalError::CorruptType);
        }

        let id = i32::from_be_bytes(b[1..5].try_into()?);
        let _ = b.split_to(5);
        let schema = Self::consume_string(&mut b)?;
        let name = Self::consume_string(&mut b)?;

        Ok(WalRecord::Type { id, schema, name })
    }

    /// Parse as Insert, assumes b[0] == 'I'
    fn insert(mut b: Bytes) -> Result<Self, WalError> {
        if b.len() < 6 || b[5] != b'N' {
            return Err(WalError::CorruptInsert);
        }

        let relation_id = i32::from_be_bytes(b[1..5].try_into()?);
        let _ = b.split_to(6);

        let new_tuple = Self::consume_tuple(&mut b)?;
        if !b.is_empty() {
            // Should consume the entire record
            return Err(WalError::CorruptInsert);
        }

        Ok(WalRecord::Insert {
            relation_id,
            new_tuple,
        })
    }

    /// Parse as Update, assumes b[0] == 'U'
    fn update(mut b: Bytes) -> Result<Self, WalError> {
        if b.len() < 6 {
            return Err(WalError::CorruptUpdate);
        }

        let relation_id = i32::from_be_bytes(b[1..5].try_into()?);
        let _ = b.split_to(5);
        let mut key_tuple = None;
        let mut old_tuple = None;

        if b[0] == b'K' {
            let _ = b.split_to(1);
            key_tuple = Some(Self::consume_tuple(&mut b)?);
        } else if b[0] == b'O' {
            let _ = b.split_to(1);
            old_tuple = Some(Self::consume_tuple(&mut b)?);
        }

        if *b.first().ok_or(WalError::CorruptUpdate)? != b'N' {
            return Err(WalError::CorruptUpdate);
        }

        let _ = b.split_to(1);
        let new_tuple = Self::consume_tuple(&mut b)?;

        if !b.is_empty() {
            // Should consume the entire record
            return Err(WalError::CorruptUpdate);
        }

        Ok(WalRecord::Update {
            relation_id,
            key_tuple,
            old_tuple,
            new_tuple,
        })
    }

    /// Parse as Delete, assumes b[0] == 'D'
    fn delete(mut b: Bytes) -> Result<Self, WalError> {
        if b.len() < 6 {
            return Err(WalError::CorruptDelete);
        }

        let relation_id = i32::from_be_bytes(b[1..5].try_into()?);
        let mut key_tuple = None;
        let mut old_tuple = None;

        if b[5] == b'K' {
            let _ = b.split_to(6);
            key_tuple = Some(Self::consume_tuple(&mut b)?);
        } else if b[5] == b'O' {
            let _ = b.split_to(6);
            old_tuple = Some(Self::consume_tuple(&mut b)?);
        } else {
            return Err(WalError::CorruptDelete);
        }

        if !b.is_empty() {
            // Should consume the entire record
            return Err(WalError::CorruptDelete);
        }

        Ok(WalRecord::Delete {
            relation_id,
            key_tuple,
            old_tuple,
        })
    }

    /// Parse as Truncate, assumes b[0] == 'T'
    fn truncate(mut b: Bytes) -> Result<Self, WalError> {
        if b.len() < 6 {
            return Err(WalError::CorruptTruncate);
        }

        let n_relations = i32::from_be_bytes(b[1..5].try_into()?);
        let options = b[5] as i8;
        let _ = b.split_to(6);

        let mut relation_ids = Vec::with_capacity(n_relations as usize);

        for _ in 0..n_relations as usize {
            if b.len() < 4 {
                return Err(WalError::CorruptTruncate);
            }
            relation_ids.push(i32::from_be_bytes(b[0..4].try_into()?));
            let _ = b.split_to(4);
        }

        Ok(WalRecord::Truncate {
            n_relations,
            options,
            relation_ids,
        })
    }

    /// Parse as Message, assumes b[0] == 'M'
    fn message(mut b: Bytes) -> Result<Self, WalError> {
        if b.len() < 10 {
            return Err(WalError::CorruptMessage);
        }
        let transactional = b[1] == 1;
        let lsn = i64::from_be_bytes(b[2..10].try_into()?).into();
        let _ = b.split_to(10);
        let prefix = Self::consume_string(&mut b)?;
        if b.len() < 4 {
            return Err(WalError::CorruptMessage);
        }

        let len = i32::from_be_bytes(b[0..4].try_into()?);
        let _ = b.split_to(4);

        if b.len() < len as usize {
            return Err(WalError::CorruptMessage);
        }

        Ok(WalRecord::Message {
            transactional,
            lsn,
            prefix,
            len,
            payload: b,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::convert::TryInto;

    use bytes::Bytes;
    use postgres_types::Type;

    use super::*;

    #[test]
    fn wal_parse_keepalive() {
        let wal: WalData = bytes::Bytes::copy_from_slice(b"k\0\0\0\0\x01j\x8b(\0\x02g?s\\\xbb}\0")
            .try_into()
            .unwrap();

        assert_eq!(
            wal,
            WalData::Keepalive {
                end: 23759656.into(),
                time: 676472169479037,
                reply: 0
            }
        );
    }

    #[test]
    fn wal_parse_relation_mapping() {
        let wal: WalData = Bytes::copy_from_slice(
            b"w\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x02g?\x9e\xc7yKR\0\0@/public\0employees\0d\0\x04\x01emp_no\0\0\0\0\x17\xff\xff\xff\xff\0first_name\0\0\0\x04\x13\0\0\0\x12\0last_name\0\0\0\x04\x13\0\0\0\x14\0status\0\0\0\x04\x13\0\0\0\x05")
            .try_into()
            .unwrap();

        assert_eq!(
            wal,
            WalData::XLogData {
                start: 0.into(),
                end: 0.into(),
                time: 676472897894731,
                data: WalRecord::Relation(RelationMapping {
                    id: 16431,
                    schema: Bytes::copy_from_slice(b"public"),
                    name: Bytes::copy_from_slice(b"employees"),
                    relreplident: 100,
                    n_cols: 4,
                    cols: vec![
                        ColumnSpec {
                            flags: 1,
                            name: Bytes::copy_from_slice(b"emp_no"),
                            type_oid: Type::INT4.oid(),
                            type_modifier: -1
                        },
                        ColumnSpec {
                            flags: 0,
                            name: Bytes::copy_from_slice(b"first_name"),
                            type_oid: Type::VARCHAR.oid(),
                            type_modifier: 18
                        },
                        ColumnSpec {
                            flags: 0,
                            name: Bytes::copy_from_slice(b"last_name"),
                            type_oid: Type::VARCHAR.oid(),
                            type_modifier: 20
                        },
                        ColumnSpec {
                            flags: 0,
                            name: Bytes::copy_from_slice(b"status"),
                            type_oid: Type::VARCHAR.oid(),
                            type_modifier: 5
                        }
                    ]
                })
            }
        );
    }

    #[test]
    fn wal_parse_insert() {
        let wal: WalData = Bytes::copy_from_slice(
            b"w\0\0\0\0\x01l\xafx\0\0\0\0\x01l\xafx\0\x02g?\x9e\xc7y\xbcI\0\0@/N\0\x04t\0\0\0\x0210t\0\0\0\x04Dropt\0\0\0\x06Tablest\0\0\0\x01a")
            .try_into()
            .unwrap();

        assert_eq!(
            wal,
            WalData::XLogData {
                start: 23900024.into(),
                end: 23900024.into(),
                time: 676472897894844,
                data: WalRecord::Insert {
                    relation_id: 16431,
                    new_tuple: TupleData {
                        n_cols: 4,
                        cols: vec![
                            TupleEntry::Text(Bytes::copy_from_slice(b"10")),
                            TupleEntry::Text(Bytes::copy_from_slice(b"Drop")),
                            TupleEntry::Text(Bytes::copy_from_slice(b"Tables")),
                            TupleEntry::Text(Bytes::copy_from_slice(b"a")),
                        ]
                    }
                }
            }
        );
    }

    #[test]
    fn wal_parse_type() {
        let wal: WalData = Bytes::copy_from_slice(
            b"w\0\0\0\0\x01l\xafx\0\0\0\0\x01l\xafx\0\x02g?\x9e\xc7y\xbcY\0\x01@\xf1public\0abc\0",
        )
        .try_into()
        .unwrap();

        assert_eq!(
            wal,
            WalData::XLogData {
                start: 23900024.into(),
                end: 23900024.into(),
                time: 676472897894844,
                data: WalRecord::Type {
                    id: 82161,
                    schema: "public".into(),
                    name: "abc".into()
                }
            }
        );
    }
}
