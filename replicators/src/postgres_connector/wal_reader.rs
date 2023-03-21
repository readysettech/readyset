use std::collections::{HashMap, HashSet};
use std::convert::TryInto;
use std::sync::Arc;

use bit_vec::BitVec;
use mysql_time::MySqlTime;
use postgres_types::Kind;
use readyset_data::{Array, Collation, DfType, DfValue, Dialect};
use readyset_errors::{unsupported, ReadySetError};
use rust_decimal::prelude::FromStr;
use rust_decimal::Decimal;
use tokio_postgres as pgsql;
use tracing::{debug, error, trace};

use super::ddl_replication::DdlEvent;
use super::lsn::Lsn;
use super::wal::{self, RelationMapping, WalData, WalError, WalRecord};
use crate::postgres_connector::wal::{TableErrorKind, TupleEntry};

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
        set: Vec<readyset_client::Modification>,
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

    pub(crate) async fn next_event(&mut self) -> Result<(WalEvent, Lsn), WalError> {
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
                                tuple: new_tuple
                                    .into_noria_vec(mapping, custom_types, false)?
                                    .into_iter()
                                    .collect::<Option<Vec<_>>>()
                                    // Insert records should never have "unchanged" fields... unchanged from what?
                                    .ok_or_else(|| WalError::TableError {
                                        kind: TableErrorKind::UnexpectedUnchangedEntry {
                                            reason: "WalRecord::Insert::new_tuple should never contain TupleEntry::Unchanged",
                                        },
                                        schema: schema.clone(),
                                        table: table.clone(),
                                    })?,
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

                        // Replace TupleEntry::Unchanged in new_tuple by the corresponding value in
                        // old_tuple
                        let mut new_tuple = new_tuple;
                        while let Some(pos) = new_tuple
                            .cols
                            .iter()
                            .position(|x| *x == TupleEntry::Unchanged)
                        {
                            new_tuple.cols[pos] = old_tuple.cols[pos].clone();
                        }

                        return Ok((
                            WalEvent::UpdateRow {
                                schema: schema.clone(),
                                table: table.clone(),
                                old_tuple: old_tuple
                                    .into_noria_vec(mapping, custom_types, false)?
                                    .into_iter()
                                    .collect::<Option<Vec<_>>>()
                                    // The old row must always be complete, or we won't be able to delete (a copy of) it
                                    .ok_or_else(|| WalError::TableError {
                                        kind: TableErrorKind::UnexpectedUnchangedEntry {
                                            reason: "WalRecord::Update::old_tuple should never contain TupleEntry::Unchanged"
                                        },
                                        schema: schema.clone(),
                                        table: table.clone(),
                                    })?,
                                new_tuple: new_tuple
                                    .into_noria_vec(mapping, custom_types, false)?
                                    .into_iter()
                                    .collect::<Option<Vec<_>>>()
                                    // We should have filled in any "unchanged" entries in the new row above
                                    .ok_or_else(|| WalError::TableError {
                                        kind: TableErrorKind::UnexpectedUnchangedEntry {
                                            reason: "All instances of TupleEntry::Unchanged in WalRecord::Update::new_tuple should have been replaced"
                                        },
                                        schema: schema.clone(),
                                        table: table.clone(),
                                    })?,
                            },
                            end,
                        ));
                    } else if let Some(key_tuple) = key_tuple {
                        // This happens when the update is modifying the key column
                        return Ok((
                            WalEvent::UpdateByKey {
                                schema: schema.clone(),
                                table: table.clone(),
                                key: key_tuple
                                    .into_noria_vec(mapping, custom_types, true)?
                                    .into_iter()
                                    .collect::<Option<Vec<_>>>()
                                    // The key must always be complete, or we won't be able to look up the row
                                    .ok_or_else(|| WalError::TableError {
                                        kind: TableErrorKind::UnexpectedUnchangedEntry {
                                            reason: "WalRecord::Update::key_tuple should never contain TupleEntry::Unchanged",
                                        },
                                        schema: schema.clone(),
                                        table: table.clone(),
                                    })?,
                                set: new_tuple
                                    .into_noria_vec(mapping, custom_types, false)?
                                    .into_iter()
                                    .map(Into::into)
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
                                key: new_tuple
                                    .clone()
                                    .into_noria_vec(mapping, custom_types, true)?
                                    .into_iter()
                                    .collect::<Option<Vec<_>>>()
                                    // The key must always be complete, or we won't be able to look up the row
                                    .ok_or_else(|| WalError::TableError {
                                        kind: TableErrorKind::UnexpectedUnchangedEntry {
                                            reason: "When key_tuple is not present, the key columns in WalRecord::Update::new_tuple should never contain TupleEntry::Unchanged",
                                        },
                                        schema: schema.clone(),
                                        table: table.clone(),
                                    })?,
                                set: new_tuple
                                    .into_noria_vec(mapping, custom_types, false)?
                                    .into_iter()
                                    .map(Into::into)
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
                                    tuple: old_tuple
                                        .into_noria_vec(mapping, custom_types, false)?
                                        .into_iter()
                                        .collect::<Option<Vec<_>>>()
                                        // The old row must always be complete, or we won't be able to delete (a copy of) it
                                        .ok_or_else(|| WalError::TableError {
                                            kind: TableErrorKind::UnexpectedUnchangedEntry {
                                                reason: "WalRecord::Delete::old_tuple should never contain TupleEntry::Unchanged",
                                            },
                                            schema: schema.clone(),
                                            table: table.clone(),
                                        })?,
                                },
                                end,
                            ));
                        } else if let Some(key_tuple) = key_tuple {
                            return Ok((
                                WalEvent::DeleteByKey {
                                    schema: schema.clone(),
                                    table: table.clone(),
                                    key: key_tuple
                                        .into_noria_vec(mapping, custom_types, true)?
                                        .into_iter()
                                        .collect::<Option<Vec<_>>>()
                                        // The key must always be complete, or we won't be able to look up the row to delete it
                                        .ok_or_else(|| WalError::TableError {
                                            kind: TableErrorKind::UnexpectedUnchangedEntry {
                                                reason: "WalRecord::Delete::key_tuple should never contain TupleEntry::Unchanged",
                                            },
                                            schema: schema.clone(),
                                            table: table.clone(),
                                        })?,
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
    /// Converts a WAL tuple into a row of *maybe* DfValues.
    /// WAL tuple entries for update records can be "unchanged", which we represent here as None so
    /// that we don't have to add a DfValue variant that gets used nowhere else.
    pub(crate) fn into_noria_vec(
        self,
        relation: &RelationMapping,
        custom_types: &HashSet<u32>,
        is_key: bool,
    ) -> Result<Vec<Option<DfValue>>, WalError> {
        use postgres_types::Type as PGType;

        if self.n_cols != relation.n_cols {
            return Err(WalError::TableError {
                kind: TableErrorKind::InvalidMapping(format!(
                    "Relation and tuple must have 1:1 mapping; {:?}; {:?}",
                    self, relation
                )),
                table: relation.relation_name_lossy(),
                schema: relation.schema_name_lossy(),
            });
        }

        let mut ret = Vec::with_capacity(self.n_cols as usize);

        for (data, spec) in self.cols.into_iter().zip(relation.cols.iter()) {
            if is_key && spec.flags != 1 {
                // We only want key columns, and this ain't the key
                continue;
            }

            match data {
                wal::TupleEntry::Null => ret.push(Some(DfValue::None)),
                // This can only occur within an update record, specifically when there is an update
                // for a row containing one or more TOAST values, and at least one of the TOAST
                // values was unmodified
                wal::TupleEntry::Unchanged => ret.push(None),
                wal::TupleEntry::Text(text) => {
                    // WAL delivers all entries as text, and it is up to us to parse to the proper
                    // ReadySet type
                    let str = String::from_utf8_lossy(&text);

                    let unsupported_type_err = || WalError::TableError {
                        kind: TableErrorKind::UnsupportedTypeConversion {
                            type_oid: spec.type_oid,
                        },
                        schema: relation.schema_name_lossy(),
                        table: relation.relation_name_lossy(),
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
                                    PGType::CHAR => DfType::Char(1, Collation::default()),
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
                                    .ok_or(WalError::TableError {
                                        kind: TableErrorKind::UnknownEnumVariant(text),
                                        schema: relation.schema_name_lossy(),
                                        table: relation.relation_name_lossy(),
                                    })?
                                    // To be compatible with mysql enums, we always represent enum
                                    // values as *1-indexed* (since mysql needs 0 to represent
                                    // invalid values)
                                    + 1,
                            ),
                            _ => match pg_type {
                                PGType::BOOL => DfValue::UnsignedInt(match str.as_ref() {
                                    "t" => true as _,
                                    "f" => false as _,
                                    _ => {
                                        return Err(WalError::TableError {
                                            kind: TableErrorKind::BoolParseError,
                                            table: relation.relation_name_lossy(),
                                            schema: relation.schema_name_lossy(),
                                        })
                                    }
                                }),
                                PGType::INT2 | PGType::INT4 | PGType::INT8 => {
                                    let result = str.parse().map_err(|_| WalError::TableError {
                                        kind: TableErrorKind::IntParseError,
                                        table: relation.relation_name_lossy(),
                                        schema: relation.schema_name_lossy(),
                                    });

                                    DfValue::Int(result?)
                                }
                                PGType::OID => {
                                    let result = str.parse().map_err(|_| WalError::TableError {
                                        kind: TableErrorKind::IntParseError,
                                        table: relation.relation_name_lossy(),
                                        schema: relation.schema_name_lossy(),
                                    });

                                    DfValue::UnsignedInt(result?)
                                }
                                PGType::FLOAT4 => str
                                    .parse::<f32>()
                                    .map_err(|_| WalError::TableError {
                                        kind: TableErrorKind::FloatParseError,
                                        table: relation.relation_name_lossy(),
                                        schema: relation.schema_name_lossy(),
                                    })?
                                    .try_into()?,
                                PGType::FLOAT8 => str
                                    .parse::<f64>()
                                    .map_err(|_| WalError::TableError {
                                        kind: TableErrorKind::FloatParseError,
                                        table: relation.relation_name_lossy(),
                                        schema: relation.schema_name_lossy(),
                                    })?
                                    .try_into()?,
                                PGType::NUMERIC => Decimal::from_str_exact(str.as_ref())
                                    .map_err(|e| WalError::TableError {
                                        kind: TableErrorKind::NumericParseError(e),
                                        table: relation.relation_name_lossy(),
                                        schema: relation.schema_name_lossy(),
                                    })
                                    .map(DfValue::from)?,
                                PGType::CHAR => match text.as_ref() {
                                    [] => DfValue::None,
                                    [c] => DfValue::Int(i8::from_ne_bytes([*c]).into()),
                                    [b'\\', _, _, _] => {
                                        // The input in this case is in the bytea escaped input
                                        // representation. See https://www.postgresql.org/docs/current/datatype-binary.html#DATATYPE-BINARY-SQLESC
                                        // for details

                                        // Decode the octet string representation of the byte
                                        let byte =
                                            u8::from_str_radix(&str[1..], 8).map_err(|_| {
                                                WalError::TableError {
                                                    kind: TableErrorKind::IntParseError,
                                                    table: relation.relation_name_lossy(),
                                                    schema: relation.schema_name_lossy(),
                                                }
                                            })?;
                                        // Create the i8 from the byte
                                        let ch = i8::from_ne_bytes([byte]);
                                        DfValue::Int(ch.into())
                                    }
                                    _ => {
                                        return Err(WalError::TableError {
                                            kind: TableErrorKind::IntParseError,
                                            table: relation.relation_name_lossy(),
                                            schema: relation.schema_name_lossy(),
                                        })
                                    }
                                },
                                PGType::TEXT
                                | PGType::JSON
                                | PGType::VARCHAR
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
                                        .map_err(|e| WalError::TableError {
                                            kind: TableErrorKind::JsonParseError(e.to_string()),
                                            schema: relation.schema_name_lossy(),
                                            table: relation.relation_name_lossy(),
                                        })
                                        .map(|v| DfValue::from(v.to_string()))?
                                }
                                PGType::TIMESTAMP => {
                                    DfValue::TimestampTz(str.parse().map_err(|_| {
                                        WalError::TableError {
                                            kind: TableErrorKind::TimestampParseError,
                                            schema: relation.schema_name_lossy(),
                                            table: relation.relation_name_lossy(),
                                        }
                                    })?)
                                }
                                PGType::TIMESTAMPTZ => {
                                    DfValue::TimestampTz(str.parse().map_err(|_| {
                                        WalError::TableError {
                                            kind: TableErrorKind::TimestampTzParseError,
                                            schema: relation.schema_name_lossy(),
                                            table: relation.relation_name_lossy(),
                                        }
                                    })?)
                                }
                                PGType::BYTEA => {
                                    hex::decode(str.strip_prefix("\\x").unwrap_or(&str))
                                        .map_err(|_| WalError::TableError {
                                            kind: TableErrorKind::ByteArrayHexParseError,
                                            schema: relation.schema_name_lossy(),
                                            table: relation.relation_name_lossy(),
                                        })
                                        .map(|bytes| DfValue::ByteArray(Arc::new(bytes)))?
                                }
                                PGType::DATE => {
                                    DfValue::TimestampTz(str.parse().map_err(|_| {
                                        WalError::TableError {
                                            kind: TableErrorKind::DateParseError,
                                            schema: relation.schema_name_lossy(),
                                            table: relation.relation_name_lossy(),
                                        }
                                    })?)
                                }
                                PGType::TIME => {
                                    let result = MySqlTime::from_str(&str).map_err(|e| {
                                        WalError::TableError {
                                            kind: TableErrorKind::TimeParseError(e),
                                            table: relation.relation_name_lossy(),
                                            schema: relation.schema_name_lossy(),
                                        }
                                    });

                                    DfValue::Time(result?)
                                }
                                PGType::BIT | PGType::VARBIT => {
                                    let mut bits = BitVec::with_capacity(str.len());
                                    for c in str.chars() {
                                        match c {
                                            '0' => bits.push(false),
                                            '1' => bits.push(true),
                                            _ => {
                                                return Err(WalError::TableError {
                                                    kind: TableErrorKind::BitVectorParseError(
                                                        format!(
                                                            "\"{}\" is not a valid binary digit",
                                                            c
                                                        ),
                                                    ),
                                                    schema: relation.schema_name_lossy(),
                                                    table: relation.relation_name_lossy(),
                                                })
                                            }
                                        }
                                    }
                                    DfValue::from(bits)
                                }
                                _ => return Err(unsupported_type_err()),
                            },
                        }
                    };

                    ret.push(Some(val));
                }
            }
        }

        Ok(ret)
    }
}
