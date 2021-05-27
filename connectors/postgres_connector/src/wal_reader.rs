use crate::wal::{RelationMapping, WalData, WalError, WalRecord};
use noria::{ReadySetError, ReadySetResult};
use slog::{debug, error};
use std::{collections::HashMap, convert::TryInto};
use tokio_postgres as pgsql;

pub struct WalReader {
    /// The handle to the log stream itself
    wal: pgsql::client::Responses,
    /// Keeps track of the relation mappings that we had
    relations: HashMap<i32, (String, RelationMapping)>,
    log: slog::Logger,
}

#[derive(Debug)]
pub enum WalEvent {
    WantsKeepaliveResponse,
    Commit,
    Insert {
        table: String,
        tuple: Vec<noria::DataType>,
    },
    DeleteRow {
        table: String,
        tuple: Vec<noria::DataType>,
    },
    DeleteByKey {
        table: String,
        key: Vec<noria::DataType>,
    },
    UpdateRow {
        table: String,
        old_tuple: Vec<noria::DataType>,
        new_tuple: Vec<noria::DataType>,
    },
    UpdateByKey {
        table: String,
        key: Vec<noria::DataType>,
        set: Vec<noria::Modification>,
    },
}

impl WalReader {
    pub(crate) fn new(wal: pgsql::client::Responses, log: slog::Logger) -> Self {
        WalReader {
            relations: Default::default(),
            wal,
            log,
        }
    }

    pub(crate) async fn next_event(&mut self) -> ReadySetResult<(WalEvent, i64)> {
        let WalReader {
            wal,
            relations,
            log,
        } = self;

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
                    debug!(log, "Unhandled message: {:?}", msg);
                    // For any other message, just keep going
                    continue;
                }
            };

            match record {
                WalRecord::Commit { .. } => return Ok((WalEvent::Commit, end)),
                WalRecord::Relation(mapping) => {
                    // Store the relation in the hash map for future use
                    let id = mapping.id;
                    let name = String::from_utf8(mapping.name.to_vec()).map_err(|v| {
                        ReadySetError::ReplicationFailed(format!(
                            "Non UTF8 name {:?}",
                            v.as_bytes()
                        ))
                    })?;
                    relations.insert(id, (name, mapping));
                }
                WalRecord::Insert {
                    relation_id,
                    new_tuple,
                } => {
                    if let Some((name, mapping)) = relations.get(&relation_id) {
                        return Ok((
                            WalEvent::Insert {
                                table: name.clone(),
                                tuple: new_tuple.into_noria_vec(mapping, false)?,
                            },
                            end,
                        ));
                    }
                }
                WalRecord::Update {
                    relation_id,
                    key_tuple,
                    old_tuple,
                    new_tuple,
                } => {
                    if let Some((name, mapping)) = relations.get(&relation_id) {
                        // We only ever going to have a `key_tuple` *OR* `old_tuple` *OR* neither
                        if let Some(old_tuple) = old_tuple {
                            // This happens when there is no key defined for the table and `REPLICA IDENTITY`
                            // is set to `FULL`
                            return Ok((
                                WalEvent::UpdateRow {
                                    table: name.clone(),
                                    old_tuple: old_tuple.into_noria_vec(mapping, false)?,
                                    new_tuple: new_tuple.into_noria_vec(mapping, false)?,
                                },
                                end,
                            ));
                        } else if let Some(key_tuple) = key_tuple {
                            // This happens when the update is modifying the key column
                            return Ok((
                                WalEvent::UpdateByKey {
                                    table: name.clone(),
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
                            // This happens when the update is not modifying the key column and therefore
                            // it is possible to extract the key value from the tuple as is
                            return Ok((
                                WalEvent::UpdateByKey {
                                    table: name.clone(),
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
                    if let Some((name, mapping)) = relations.get(&relation_id) {
                        // We only ever going to have a `key_tuple` *OR* `old_tuple`
                        if let Some(old_tuple) = old_tuple {
                            // This happens when there is no key defined for the table and `REPLICA IDENTITY`
                            // is set to `FULL`
                            return Ok((
                                WalEvent::DeleteRow {
                                    table: name.clone(),
                                    tuple: old_tuple.into_noria_vec(mapping, false)?,
                                },
                                end,
                            ));
                        } else if let Some(key_tuple) = key_tuple {
                            return Ok((
                                WalEvent::DeleteByKey {
                                    table: name.clone(),
                                    key: key_tuple.into_noria_vec(mapping, true)?,
                                },
                                end,
                            ));
                        }
                    }
                }
                WalRecord::Begin { .. } => {}
                msg @ WalRecord::Type { .. } => {
                    // This happens when a `NEW TYPE` is used, unsupported yet
                    error!(log, "Unhandled message: {:?}", msg);
                }
                msg @ WalRecord::Truncate { .. } => {
                    // This happens when `TRUNCATE table` is used, unsupported yet
                    error!(log, "Unhandled message: {:?}", msg);
                }
                WalRecord::Origin { .. } => {
                    // Just tells where the transaction originated
                }
                WalRecord::Unknown(payload) => {
                    error!(log, "Unknown message: {:?}", payload);
                }
            }
        }
    }
}

use noria::DataType;
use postgres_types::Type as PGType;

impl crate::wal::TupleData {
    pub(crate) fn into_noria_vec(
        self,
        relation: &RelationMapping,
        is_key: bool,
    ) -> Result<Vec<noria::DataType>, WalError> {
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
                crate::wal::TupleEntry::Null => ret.push(DataType::None),
                crate::wal::TupleEntry::Unchanged => return Err(WalError::ToastNotSupported),
                crate::wal::TupleEntry::Text(text) => {
                    // WAL delivers all entries as text, and it is up to us to parse to the proper Noria type
                    let str = String::from_utf8_lossy(&text);

                    let val = match spec.data_type {
                        PGType::BOOL => DataType::UnsignedInt(str.parse::<bool>()? as _),
                        PGType::INT2 | PGType::INT4 => DataType::Int(str.parse()?),
                        PGType::OID => DataType::UnsignedInt(str.parse()?),
                        PGType::INT8 => DataType::BigInt(str.parse()?),
                        PGType::FLOAT4 => str.parse::<f32>()?.try_into()?,
                        PGType::FLOAT8 => str.parse::<f64>()?.try_into()?,
                        PGType::JSON | PGType::TEXT | PGType::VARCHAR | PGType::CHAR => {
                            DataType::Text(str.as_ref().try_into()?)
                        }
                        PGType::TIMESTAMP => DataType::Timestamp(
                            chrono::NaiveDateTime::parse_from_str(&str, noria::TIMESTAMP_FORMAT)?,
                        ),
                        PGType::DATE => DataType::Timestamp(chrono::NaiveDateTime::parse_from_str(
                            &str,
                            noria::DATE_FORMAT,
                        )?),
                        PGType::TIME => DataType::Timestamp(chrono::NaiveDateTime::parse_from_str(
                            &str,
                            noria::TIME_FORMAT,
                        )?),
                        ref t => {
                            unimplemented!(
                                "Conversion not implemented for type {:?}; value {:?}",
                                t,
                                str
                            );
                        }
                    };

                    ret.push(val);
                }
            }
        }

        Ok(ret)
    }
}
