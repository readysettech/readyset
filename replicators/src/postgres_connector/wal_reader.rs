use std::collections::HashMap;
use std::convert::TryInto;
use std::sync::Arc;

use bit_vec::BitVec;
use mysql_time::MysqlTime;
use nom_sql::SqlType;
use noria::{ReadySetError, ReadySetResult};
use postgres_types::Kind;
use readyset_data::{Array, DataType};
use readyset_errors::unsupported;
use rust_decimal::prelude::FromStr;
use rust_decimal::Decimal;
use tokio_postgres as pgsql;
use tracing::{debug, error, trace};

use super::ddl_replication::DdlEvent;
use super::wal::{self, RelationMapping, WalData, WalError, WalRecord};

struct Relation {
    namespace: String,
    table: String,
    mapping: RelationMapping,
}

pub struct WalReader {
    /// The handle to the log stream itself
    wal: pgsql::client::Responses,
    /// Keeps track of the relation mappings that we had
    relations: HashMap<i32, Relation>,
}

#[derive(Debug)]
pub(crate) enum WalEvent {
    WantsKeepaliveResponse,
    Commit,
    Insert {
        namespace: String,
        table: String,
        tuple: Vec<DataType>,
    },
    DeleteRow {
        namespace: String,
        table: String,
        tuple: Vec<DataType>,
    },
    DeleteByKey {
        namespace: String,
        table: String,
        key: Vec<DataType>,
    },
    UpdateRow {
        namespace: String,
        table: String,
        old_tuple: Vec<DataType>,
        new_tuple: Vec<DataType>,
    },
    UpdateByKey {
        namespace: String,
        table: String,
        key: Vec<DataType>,
        set: Vec<noria::Modification>,
    },
    DdlEvent {
        ddl_event: Box<DdlEvent>,
    },
}

impl WalReader {
    pub(crate) fn new(wal: pgsql::client::Responses) -> Self {
        WalReader {
            relations: Default::default(),
            wal,
        }
    }

    pub(crate) async fn next_event(&mut self) -> ReadySetResult<(WalEvent, i64)> {
        let WalReader { wal, relations } = self;

        loop {
            let data: WalData = match wal.next().await? {
                pgsql::Message::CopyData(body) => body.into_bytes().try_into()?,
                _ => {
                    return Err(ReadySetError::ReplicationFailed(
                        "Unexpected message during WAL replication".to_string(),
                    ))
                }
            };

            let (end, record) = match data {
                WalData::Keepalive { end, reply, .. } if reply == 1 => {
                    return Ok((WalEvent::WantsKeepaliveResponse, end))
                }
                WalData::XLogData { end, data, .. } => (end, data),
                msg => {
                    trace!(?msg, "Unhandled message");
                    // For any other message, just keep going
                    continue;
                }
            };

            trace!(?record);

            match record {
                WalRecord::Commit { .. } => return Ok((WalEvent::Commit, end)),
                WalRecord::Relation(mapping) => {
                    // Store the relation in the hash map for future use
                    let id = mapping.id;
                    let namespace = String::from_utf8(mapping.namespace.to_vec()).map_err(|v| {
                        ReadySetError::ReplicationFailed(format!(
                            "Non UTF8 name {:?}",
                            v.as_bytes()
                        ))
                    })?;
                    let table = String::from_utf8(mapping.name.to_vec()).map_err(|v| {
                        ReadySetError::ReplicationFailed(format!(
                            "Non UTF8 name {:?}",
                            v.as_bytes()
                        ))
                    })?;
                    relations.insert(
                        id,
                        Relation {
                            namespace,
                            table,
                            mapping,
                        },
                    );
                }
                WalRecord::Insert {
                    relation_id,
                    new_tuple,
                } => {
                    if let Some(Relation {
                        namespace,
                        table,
                        mapping,
                    }) = relations.get(&relation_id)
                    {
                        return Ok((
                            WalEvent::Insert {
                                namespace: namespace.clone(),
                                table: table.clone(),
                                tuple: new_tuple.into_noria_vec(mapping, false)?,
                            },
                            end,
                        ));
                    } else {
                        debug!(
                            relation_id,
                            "Ignoring WAL insert event for unknown relation"
                        );
                    }
                }
                WalRecord::Update {
                    relation_id,
                    key_tuple,
                    old_tuple,
                    new_tuple,
                } => {
                    if let Some(Relation {
                        namespace,
                        table,
                        mapping,
                    }) = relations.get(&relation_id)
                    {
                        // We only ever going to have a `key_tuple` *OR* `old_tuple` *OR* neither
                        if let Some(old_tuple) = old_tuple {
                            // This happens when there is no key defined for the table and `REPLICA
                            // IDENTITY` is set to `FULL`
                            return Ok((
                                WalEvent::UpdateRow {
                                    namespace: namespace.clone(),
                                    table: table.clone(),
                                    old_tuple: old_tuple.into_noria_vec(mapping, false)?,
                                    new_tuple: new_tuple.into_noria_vec(mapping, false)?,
                                },
                                end,
                            ));
                        } else if let Some(key_tuple) = key_tuple {
                            // This happens when the update is modifying the key column
                            return Ok((
                                WalEvent::UpdateByKey {
                                    namespace: namespace.clone(),
                                    table: table.clone(),
                                    key: key_tuple.into_noria_vec(mapping, true)?,
                                    set: new_tuple
                                        .into_noria_vec(mapping, false)?
                                        .into_iter()
                                        .map(noria::Modification::Set)
                                        .collect(),
                                },
                                end,
                            ));
                        } else {
                            // This happens when the update is not modifying the key column and
                            // therefore it is possible to extract the
                            // key value from the tuple as is
                            return Ok((
                                WalEvent::UpdateByKey {
                                    namespace: namespace.clone(),
                                    table: table.clone(),
                                    key: new_tuple.clone().into_noria_vec(mapping, true)?,
                                    set: new_tuple
                                        .into_noria_vec(mapping, false)?
                                        .into_iter()
                                        .map(noria::Modification::Set)
                                        .collect(),
                                },
                                end,
                            ));
                        }
                    }
                }
                WalRecord::Delete {
                    relation_id,
                    key_tuple,
                    old_tuple,
                } => {
                    if let Some(Relation {
                        namespace,
                        table,
                        mapping,
                    }) = relations.get(&relation_id)
                    {
                        // We only ever going to have a `key_tuple` *OR* `old_tuple`
                        if let Some(old_tuple) = old_tuple {
                            // This happens when there is no key defined for the table and `REPLICA
                            // IDENTITY` is set to `FULL`
                            return Ok((
                                WalEvent::DeleteRow {
                                    namespace: namespace.clone(),
                                    table: table.clone(),
                                    tuple: old_tuple.into_noria_vec(mapping, false)?,
                                },
                                end,
                            ));
                        } else if let Some(key_tuple) = key_tuple {
                            return Ok((
                                WalEvent::DeleteByKey {
                                    namespace: namespace.clone(),
                                    table: table.clone(),
                                    key: key_tuple.into_noria_vec(mapping, true)?,
                                },
                                end,
                            ));
                        }
                    }
                }
                WalRecord::Begin { .. } => {}
                WalRecord::Message {
                    prefix,
                    payload,
                    lsn,
                    ..
                } if prefix == b"readyset".as_slice() => {
                    let ddl_event = match serde_json::from_slice(&payload) {
                        Err(err) => {
                            error!(
                                ?err,
                                "Error parsing DDL event, table or view will not be used"
                            );
                            continue;
                        }
                        Ok(ddl_event) => ddl_event,
                    };
                    return Ok((WalEvent::DdlEvent { ddl_event }, lsn));
                }
                WalRecord::Message { prefix, .. } => {
                    debug!("Message with ignored prefix {prefix:?}")
                }
                msg @ WalRecord::Type { .. } => {
                    // This happens when a `NEW TYPE` is used, unsupported yet
                    error!(?msg, "Unhandled message");
                }
                msg @ WalRecord::Truncate { .. } => {
                    // This happens when `TRUNCATE table` is used, unsupported yet
                    error!(?msg, "Unhandled message");
                }
                WalRecord::Origin { .. } => {
                    // Just tells where the transaction originated
                }
                WalRecord::Unknown(payload) => {
                    error!(?payload, "Unknown message");
                }
            }
        }
    }
}

impl wal::TupleData {
    pub(crate) fn into_noria_vec(
        self,
        relation: &RelationMapping,
        is_key: bool,
    ) -> Result<Vec<DataType>, WalError> {
        use postgres_types::Type as PGType;

        if self.n_cols != relation.n_cols {
            return Err(WalError::InvalidMapping(format!(
                "Relation and tuple must have 1:1 mapping; {:?}; {:?}",
                self, relation
            )));
        }

        let mut ret = Vec::with_capacity(self.n_cols as usize);

        for (data, spec) in self.cols.into_iter().zip(relation.cols.iter()) {
            if is_key && spec.flags != 1 {
                // We only want key columns, and this ain't the key
                continue;
            }

            match data {
                wal::TupleEntry::Null => ret.push(DataType::None),
                wal::TupleEntry::Unchanged => return Err(WalError::ToastNotSupported),
                wal::TupleEntry::Text(text) => {
                    // WAL delivers all entries as text, and it is up to us to parse to the proper
                    // Noria type
                    let str = String::from_utf8_lossy(&text);

                    let val = match spec.data_type.kind() {
                        Kind::Array(member_type) => {
                            let target_sql_type = SqlType::Array(Box::new(match *member_type {
                                PGType::BOOL => SqlType::Bool,
                                PGType::CHAR => SqlType::Char(None),
                                PGType::VARCHAR => SqlType::Varchar(None),
                                PGType::INT4 => SqlType::Int(None),
                                PGType::INT8 => SqlType::Bigint(None),
                                PGType::INT2 => SqlType::Smallint(None),
                                PGType::FLOAT4 => SqlType::Real,
                                PGType::FLOAT8 => SqlType::Double,
                                PGType::TEXT => SqlType::Text,
                                PGType::TIMESTAMP => SqlType::Timestamp,
                                PGType::TIMESTAMPTZ => SqlType::TimestampTz,
                                PGType::JSON => SqlType::Json,
                                PGType::JSONB => SqlType::Jsonb,
                                PGType::DATE => SqlType::Date,
                                PGType::TIME => SqlType::Time,
                                PGType::NUMERIC => SqlType::Numeric(None),
                                PGType::BYTEA => SqlType::ByteArray,
                                PGType::MACADDR => SqlType::MacAddr,
                                PGType::INET => SqlType::Inet,
                                PGType::UUID => SqlType::Uuid,
                                PGType::BIT => SqlType::Bit(None),
                                PGType::VARBIT => SqlType::Varbit(None),
                                _ => unsupported!("unsupported type {}", spec.data_type),
                            }));

                            DataType::from(str.parse::<Array>()?).coerce_to(&target_sql_type)?
                        }
                        _ => match spec.data_type {
                            PGType::BOOL => DataType::UnsignedInt(match str.as_ref() {
                                "t" => true as _,
                                "f" => false as _,
                                _ => return Err(WalError::BoolParseError),
                            }),
                            PGType::INT2 | PGType::INT4 | PGType::INT8 => {
                                DataType::Int(str.parse()?)
                            }
                            PGType::OID => DataType::UnsignedInt(str.parse()?),
                            PGType::FLOAT4 => str.parse::<f32>()?.try_into()?,
                            PGType::FLOAT8 => str.parse::<f64>()?.try_into()?,
                            PGType::NUMERIC => Decimal::from_str(str.as_ref())
                                .map_err(|_| WalError::NumericParseError)
                                .map(|d| DataType::Numeric(Arc::new(d)))?,
                            PGType::TEXT
                            | PGType::JSON
                            | PGType::VARCHAR
                            | PGType::CHAR
                            | PGType::MACADDR
                            | PGType::INET
                            | PGType::UUID
                            | PGType::NAME => DataType::from(str.as_ref()),
                            // JSONB might rearrange the json value (like the order of the keys in
                            // an object for example), vs JSON that
                            // keeps the text as-is. So, in order to get
                            // the same values, we parse the json into a
                            // serde_json::Value and then convert it
                            // back to String. ♪ ┏(・o･)┛ ♪
                            PGType::JSONB => {
                                serde_json::from_str::<serde_json::Value>(str.as_ref())
                                    .map_err(|e| WalError::JsonParseError(e.to_string()))
                                    .map(|v| DataType::from(v.to_string()))?
                            }
                            PGType::TIMESTAMP => DataType::TimestampTz(
                                str.parse().map_err(|_| WalError::TimestampParseError)?,
                            ),
                            PGType::TIMESTAMPTZ => DataType::TimestampTz(
                                str.parse().map_err(|_| WalError::TimestampTzParseError)?,
                            ),
                            PGType::BYTEA => hex::decode(str.strip_prefix("\\x").unwrap_or(&str))
                                .map_err(|_| WalError::ByteArrayHexParseError)
                                .map(|bytes| DataType::ByteArray(Arc::new(bytes)))?,
                            PGType::DATE => DataType::TimestampTz(
                                str.parse().map_err(|_| WalError::DateParseError)?,
                            ),
                            PGType::TIME => DataType::Time(MysqlTime::from_str(&str)?),
                            PGType::BIT | PGType::VARBIT => {
                                let mut bits = BitVec::with_capacity(str.len());
                                for c in str.chars() {
                                    match c {
                                        '0' => bits.push(false),
                                        '1' => bits.push(true),
                                        _ => {
                                            return Err(WalError::BitVectorParseError(format!(
                                                "\"{}\" is not a valid binary digit",
                                                c
                                            )))
                                        }
                                    }
                                }
                                DataType::from(bits)
                            }
                            ref t => {
                                unsupported!("Conversion not implemented for type {:?}", t);
                            }
                        },
                    };

                    ret.push(val);
                }
            }
        }

        Ok(ret)
    }
}
