use std::collections::{HashMap, HashSet};
use std::convert::TryInto;
use std::sync::Arc;

use bit_vec::BitVec;
use mysql_time::MySqlTime;
use postgres_types::Kind;
use readyset::ReadySetError;
use readyset_data::{Array, Collation, DfType, DfValue, Dialect};
use readyset_errors::unsupported;
use rust_decimal::prelude::FromStr;
use rust_decimal::Decimal;
use tokio_postgres as pgsql;
use tracing::{debug, error, trace};

use super::ddl_replication::DdlEvent;
use super::wal::{self, RelationMapping, WalData, WalError, WalRecord};
use crate::postgres_connector::wal::TupleEntry;

/// The names of the schema table that DDL replication logs will be written to
pub(crate) const DDL_REPLICATION_LOG_SCHEMA: &str = "readyset";
pub(crate) const DDL_REPLICATION_LOG_TABLE: &str = "ddl_replication_log";

struct Relation {
    schema: String,
    table: String,
    mapping: RelationMapping,
}

pub struct WalReader {
    /// The handle to the log stream itself
    wal: pgsql::client::Responses,
    /// Keeps track of the relation mappings that we had
    relations: HashMap<i32, Relation>,
    /// Keeps track of the OIDs of all custom types we've seen
    custom_types: HashSet<u32>,
}

#[derive(Debug)]
pub(crate) enum WalEvent {
    WantsKeepaliveResponse,
    Commit,
    Insert {
        schema: String,
        table: String,
        tuple: Vec<DfValue>,
    },
    DeleteRow {
        schema: String,
        table: String,
        tuple: Vec<DfValue>,
    },
    DeleteByKey {
        schema: String,
        table: String,
        key: Vec<DfValue>,
    },
    UpdateRow {
        schema: String,
        table: String,
        old_tuple: Vec<DfValue>,
        new_tuple: Vec<DfValue>,
    },
    UpdateByKey {
        schema: String,
        table: String,
        key: Vec<DfValue>,
        set: Vec<readyset::Modification>,
    },
    Truncate {
        tables: Vec<(String, String)>,
    },
    DdlEvent {
        ddl_event: Box<DdlEvent>,
    },
}

impl WalReader {
    pub(crate) fn new(wal: pgsql::client::Responses) -> Self {
        WalReader {
            relations: Default::default(),
            custom_types: Default::default(),
            wal,
        }
    }

    pub(crate) async fn next_event(&mut self) -> Result<(WalEvent, i64), WalError> {
        let WalReader {
            wal,
            relations,
            custom_types,
        } = self;

        loop {
            let data: WalData = match wal
                .next()
                .await
                .map_err(|e| WalError::ReadySetError(e.into()))?
            {
                pgsql::Message::CopyData(body) => body.into_bytes().try_into()?,
                _ => {
                    return Err(WalError::ReadySetError(ReadySetError::ReplicationFailed(
                        "Unexpected message during WAL replication".to_string(),
                    )))
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
                    let schema = String::from_utf8(mapping.schema.to_vec()).map_err(|v| {
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
                            schema,
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
                        schema,
                        table,
                        mapping,
                    }) = relations.get(&relation_id)
                    {
                        return Ok((
                            WalEvent::Insert {
                                schema: schema.clone(),
                                table: table.clone(),
                                tuple: new_tuple.into_noria_vec(mapping, custom_types, false)?,
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
                    let Relation {
                        schema,
                        table,
                        mapping,
                    } = match relations.get(&relation_id) {
                        None => continue,
                        Some(relation) => relation,
                    };

                    if schema == DDL_REPLICATION_LOG_SCHEMA && table == DDL_REPLICATION_LOG_TABLE {
                        // This is a special update message for the DDL replication table, convert
                        // that to the same format as if it were a message record
                        let ddl_data = match new_tuple.cols.get(0) {
                            Some(TupleEntry::Text(data)) => data,
                            _ => {
                                error!("Error fetching DDL event from update record");
                                continue;
                            }
                        };

                        let ddl_event: Box<DdlEvent> = match serde_json::from_slice(ddl_data) {
                            Err(err) => {
                                error!(
                                    ?err,
                                    "Error parsing DDL event, table or view will not be used"
                                );
                                continue;
                            }
                            Ok(ddl_event) => ddl_event,
                        };

                        return Ok((WalEvent::DdlEvent { ddl_event }, end));
                    }
                    // We only ever going to have a `key_tuple` *OR* `old_tuple` *OR* neither
                    if let Some(old_tuple) = old_tuple {
                        // This happens when there is no key defined for the table and `REPLICA
                        // IDENTITY` is set to `FULL`
                        return Ok((
                            WalEvent::UpdateRow {
                                schema: schema.clone(),
                                table: table.clone(),
                                old_tuple: old_tuple.into_noria_vec(
                                    mapping,
                                    custom_types,
                                    false,
                                )?,
                                new_tuple: new_tuple.into_noria_vec(
                                    mapping,
                                    custom_types,
                                    false,
                                )?,
                            },
                            end,
                        ));
                    } else if let Some(key_tuple) = key_tuple {
                        // This happens when the update is modifying the key column
                        return Ok((
                            WalEvent::UpdateByKey {
                                schema: schema.clone(),
                                table: table.clone(),
                                key: key_tuple.into_noria_vec(mapping, custom_types, true)?,
                                set: new_tuple
                                    .into_noria_vec(mapping, custom_types, false)?
                                    .into_iter()
                                    .map(readyset::Modification::Set)
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
                                schema: schema.clone(),
                                table: table.clone(),
                                key: new_tuple.clone().into_noria_vec(
                                    mapping,
                                    custom_types,
                                    true,
                                )?,
                                set: new_tuple
                                    .into_noria_vec(mapping, custom_types, false)?
                                    .into_iter()
                                    .map(readyset::Modification::Set)
                                    .collect(),
                            },
                            end,
                        ));
                    }
                }
                WalRecord::Delete {
                    relation_id,
                    key_tuple,
                    old_tuple,
                } => {
                    if let Some(Relation {
                        schema,
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
                                    schema: schema.clone(),
                                    table: table.clone(),
                                    tuple: old_tuple.into_noria_vec(
                                        mapping,
                                        custom_types,
                                        false,
                                    )?,
                                },
                                end,
                            ));
                        } else if let Some(key_tuple) = key_tuple {
                            return Ok((
                                WalEvent::DeleteByKey {
                                    schema: schema.clone(),
                                    table: table.clone(),
                                    key: key_tuple.into_noria_vec(mapping, custom_types, true)?,
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
                WalRecord::Type { id, .. } => {
                    custom_types.insert(id as _);
                }
                WalRecord::Truncate {
                    n_relations,
                    relation_ids,
                    ..
                } => {
                    let mut tables = Vec::with_capacity(n_relations as _);
                    for relation_id in relation_ids {
                        if let Some(Relation { schema, table, .. }) = relations.get(&relation_id) {
                            tables.push((schema.clone(), table.clone()))
                        } else {
                            debug!(%relation_id, "Ignoring WAL event for unknown relation");
                        }
                    }

                    return Ok((WalEvent::Truncate { tables }, end));
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
        custom_types: &HashSet<u32>,
        is_key: bool,
    ) -> Result<Vec<DfValue>, WalError> {
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
                wal::TupleEntry::Null => ret.push(DfValue::None),
                wal::TupleEntry::Unchanged => {
                    return Err(WalError::ToastNotSupported {
                        schema: relation.schema.clone(),
                        table: relation.name.clone(),
                    })
                }
                wal::TupleEntry::Text(text) => {
                    // WAL delivers all entries as text, and it is up to us to parse to the proper
                    // ReadySet type
                    let str = String::from_utf8_lossy(&text);

                    let unsupported_type_err = || WalError::UnsupportedTypeConversion {
                        type_oid: spec.type_oid,
                        schema: relation.schema.clone(),
                        table: relation.name.clone(),
                    };

                    let val = if custom_types.contains(&spec.type_oid) {
                        // For custom types (or arrays of custom types), just leave the value as
                        // text - we don't have enough information here to actually coerce to the
                        // correct type, but the table will do that for us (albeit this is slightly
                        // less efficient)
                        DfValue::from(&*text)
                    } else {
                        let pg_type =
                            PGType::from_oid(spec.type_oid).ok_or_else(unsupported_type_err)?;

                        match pg_type.kind() {
                            Kind::Array(member_type) => {
                                let dialect = Dialect::DEFAULT_POSTGRESQL;
                                let subsecond_digits = dialect.default_subsecond_digits();

                                let target_type = DfType::Array(Box::new(match *member_type {
                                    PGType::BOOL => DfType::Bool,
                                    PGType::CHAR => DfType::Char(1, Collation::default(), dialect),
                                    PGType::TEXT | PGType::VARCHAR => DfType::DEFAULT_TEXT,
                                    PGType::INT2 => DfType::SmallInt,
                                    PGType::INT4 => DfType::Int,
                                    PGType::INT8 => DfType::BigInt,
                                    PGType::FLOAT4 => DfType::Float,
                                    PGType::FLOAT8 => DfType::Double,
                                    PGType::TIMESTAMP => DfType::Timestamp { subsecond_digits },
                                    PGType::TIMESTAMPTZ => DfType::TimestampTz { subsecond_digits },
                                    PGType::JSON => DfType::Json,
                                    PGType::JSONB => DfType::Jsonb,
                                    PGType::DATE => DfType::Date,
                                    PGType::TIME => DfType::Time { subsecond_digits },
                                    PGType::NUMERIC => DfType::DEFAULT_NUMERIC,
                                    PGType::BYTEA => DfType::Blob,
                                    PGType::MACADDR => DfType::MacAddr,
                                    PGType::INET => DfType::Inet,
                                    PGType::UUID => DfType::Uuid,
                                    PGType::BIT => DfType::DEFAULT_BIT,
                                    PGType::VARBIT => DfType::VarBit(None),
                                    ref ty => unsupported!("Unsupported type: {ty}"),
                                }));

                                DfValue::from(str.parse::<Array>()?)
                                    .coerce_to(&target_type, &DfType::Unknown)?
                            }
                            Kind::Enum(variants) => DfValue::from(
                                variants
                                    .iter()
                                    .position(|v| v.as_bytes() == text)
                                    .ok_or(WalError::UnknownEnumVariant(text))?
                                    // To be compatible with mysql enums, we always represent enum
                                    // values as *1-indexed* (since mysql needs 0 to represent
                                    // invalid values)
                                    + 1,
                            ),
                            _ => match pg_type {
                                PGType::BOOL => DfValue::UnsignedInt(match str.as_ref() {
                                    "t" => true as _,
                                    "f" => false as _,
                                    _ => return Err(WalError::BoolParseError),
                                }),
                                PGType::INT2 | PGType::INT4 | PGType::INT8 => {
                                    DfValue::Int(str.parse()?)
                                }
                                PGType::OID => DfValue::UnsignedInt(str.parse()?),
                                PGType::FLOAT4 => str.parse::<f32>()?.try_into()?,
                                PGType::FLOAT8 => str.parse::<f64>()?.try_into()?,
                                PGType::NUMERIC => Decimal::from_str(str.as_ref())
                                    .map_err(|_| WalError::NumericParseError)
                                    .map(|d| DfValue::Numeric(Arc::new(d)))?,
                                PGType::TEXT
                                | PGType::JSON
                                | PGType::VARCHAR
                                | PGType::CHAR
                                | PGType::BPCHAR
                                | PGType::MACADDR
                                | PGType::INET
                                | PGType::UUID
                                | PGType::NAME => DfValue::from(str.as_ref()),
                                // JSONB might rearrange the json value (like the order of the keys
                                // in an object for example), vs
                                // JSON that keeps the text as-is.
                                // So, in order to get
                                // the same values, we parse the json into a
                                // serde_json::Value and then convert it
                                // back to String. ♪ ┏(・o･)┛ ♪
                                PGType::JSONB => {
                                    serde_json::from_str::<serde_json::Value>(str.as_ref())
                                        .map_err(|e| WalError::JsonParseError(e.to_string()))
                                        .map(|v| DfValue::from(v.to_string()))?
                                }
                                PGType::TIMESTAMP => DfValue::TimestampTz(
                                    str.parse().map_err(|_| WalError::TimestampParseError)?,
                                ),
                                PGType::TIMESTAMPTZ => DfValue::TimestampTz(
                                    str.parse().map_err(|_| WalError::TimestampTzParseError)?,
                                ),
                                PGType::BYTEA => {
                                    hex::decode(str.strip_prefix("\\x").unwrap_or(&str))
                                        .map_err(|_| WalError::ByteArrayHexParseError)
                                        .map(|bytes| DfValue::ByteArray(Arc::new(bytes)))?
                                }
                                PGType::DATE => DfValue::TimestampTz(
                                    str.parse().map_err(|_| WalError::DateParseError)?,
                                ),
                                PGType::TIME => DfValue::Time(MySqlTime::from_str(&str)?),
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
                                    DfValue::from(bits)
                                }
                                _ => return Err(unsupported_type_err()),
                            },
                        }
                    };

                    ret.push(val);
                }
            }
        }

        Ok(ret)
    }
}
