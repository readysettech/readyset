use crate::channel::Channel;
use crate::error::Error;
use crate::message::{
    BackendMessage::{self, *},
    CommandCompleteTag, ErrorSeverity, FieldDescription,
    FrontendMessage::{self, *},
    SqlState,
    StatementName::*,
    TransferFormat::{self, *},
};
use crate::r#type::{ColType, Type};
use crate::value::Value;
use crate::{Backend, PrepareResponse, QueryResponse::*, Schema};
use std::borrow::Borrow;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};

const ATTTYPMOD_NONE: i32 = -1;
const TYPLEN_1: i16 = 1;
const TYPLEN_2: i16 = 2;
const TYPLEN_4: i16 = 4;
const TYPLEN_8: i16 = 8;
const TYPLEN_VARLENA: i16 = -1;
const UNKNOWN_COLUMN: i16 = 0;
const UNKNOWN_TABLE: i32 = 0;

pub struct Protocol {
    is_starting_up: bool,
    prepared_statements: HashMap<String, PreparedStatementData>,
    portals: HashMap<String, PortalData>,
}

struct PreparedStatementData {
    prepared_statement_id: u32,
    param_schema: Schema,
    row_schema: Schema,
}

struct PortalData {
    prepared_statement_id: u32,
    prepared_statement_name: String,
    params: Vec<Value>,
    result_transfer_formats: Arc<Vec<TransferFormat>>,
}

impl Protocol {
    pub fn new() -> Protocol {
        Protocol {
            is_starting_up: true,
            prepared_statements: HashMap::new(),
            portals: HashMap::new(),
        }
    }

    pub async fn handle_request<B: Backend, C: AsyncRead + AsyncWrite + Unpin>(
        &mut self,
        message: FrontendMessage,
        backend: &mut B,
        channel: &mut Channel<C, B::Row>,
    ) -> Result<(), Error> {
        if self.is_starting_up {
            match message {
                SSLRequest { .. } => channel.feed(BackendMessage::ssl_response_n()).await?,

                StartupMessage { database, .. } => {
                    let database = database
                        .ok_or_else(|| Error::Unsupported("database is required".to_string()))?;

                    backend.on_init(database.borrow()).await?;

                    self.is_starting_up = false;
                    channel.set_start_up_complete();

                    channel.feed(AuthenticationOk).await?;
                    channel.feed(BackendMessage::ready_for_query_idle()).await?;
                }

                m => Err(Error::UnsupportedMessage(m))?,
            };

            channel.flush().await?;
            return Ok(());
        }

        match message {
            Bind {
                prepared_statement_name,
                portal_name,
                params,
                result_transfer_formats,
            } => {
                let PreparedStatementData {
                    prepared_statement_id,
                    row_schema,
                    ..
                } = self
                    .prepared_statements
                    .get(prepared_statement_name.borrow() as &str)
                    .ok_or_else(|| {
                        Error::MissingPreparedStatement(prepared_statement_name.to_string())
                    })?;

                let n_cols = row_schema.len();
                let result_transfer_formats = match result_transfer_formats[..] {
                    [] => vec![Text; n_cols],
                    [f] => vec![f; n_cols],
                    _ => {
                        if result_transfer_formats.len() == n_cols {
                            result_transfer_formats
                        } else {
                            Err(Error::IncorrectFormatCount(n_cols))?
                        }
                    }
                };

                self.portals.insert(
                    portal_name.to_string(),
                    PortalData {
                        prepared_statement_id: *prepared_statement_id,
                        prepared_statement_name: prepared_statement_name.to_string(),
                        params,
                        result_transfer_formats: Arc::new(result_transfer_formats),
                    },
                );

                channel.feed(BindComplete).await?;
            }

            Close { name } => {
                match name {
                    Portal(name) => {
                        self.portals.remove(name.borrow() as &str);
                    }

                    PreparedStatement(name) => {
                        if let Some(id) = self
                            .prepared_statements
                            .get(name.borrow() as &str)
                            .map(|d| d.prepared_statement_id)
                        {
                            backend.on_close(id).await?;
                            channel.clear_statement_param_types(name.borrow() as &str);
                            self.prepared_statements.remove(name.borrow() as &str);
                            // TODO Remove all portals referencing this prepared statement.
                        }
                    }
                };
                channel.feed(CloseComplete).await?;
            }

            Describe { name } => match name {
                Portal(name) => {
                    let PortalData {
                        prepared_statement_name,
                        result_transfer_formats,
                        ..
                    } = self
                        .portals
                        .get(name.borrow() as &str)
                        .ok_or_else(|| Error::MissingPortal(name.to_string()))?;

                    let PreparedStatementData { row_schema, .. } = self
                        .prepared_statements
                        .get(prepared_statement_name)
                        .ok_or_else(|| {
                            Error::InternalError("missing prepared statement".to_string())
                        })?;

                    debug_assert_eq!(row_schema.len(), result_transfer_formats.len());
                    channel
                        .feed(RowDescription {
                            field_descriptions: row_schema
                                .iter()
                                .zip(result_transfer_formats.iter())
                                .map(|(i, f)| make_field_description(i, *f))
                                .collect::<Result<Vec<FieldDescription>, Error>>()?,
                        })
                        .await?;
                }

                PreparedStatement(name) => {
                    let PreparedStatementData {
                        param_schema,
                        row_schema,
                        ..
                    } = self
                        .prepared_statements
                        .get(name.borrow() as &str)
                        .ok_or_else(|| Error::MissingPreparedStatement(name.to_string()))?;

                    channel
                        .feed(ParameterDescription {
                            parameter_data_types: param_schema
                                .iter()
                                .map(|(_, t)| to_type(t))
                                .collect::<Result<Vec<Type>, Error>>()?,
                        })
                        .await?;
                    channel
                        .feed(RowDescription {
                            field_descriptions: row_schema
                                .iter()
                                .map(|i| make_field_description(i, Text))
                                .collect::<Result<Vec<FieldDescription>, Error>>()?,
                        })
                        .await?;
                }
            },

            Execute { portal_name, .. } => {
                let PortalData {
                    prepared_statement_id,
                    params,
                    result_transfer_formats,
                    ..
                } = self
                    .portals
                    .get(portal_name.borrow() as &str)
                    .ok_or_else(|| Error::MissingPreparedStatement(portal_name.to_string()))?;

                let response = backend.on_execute(*prepared_statement_id, params).await?;

                if let Select { resultset, .. } = response {
                    let mut n_rows = 0;
                    for r in resultset {
                        channel
                            .feed(DataRow {
                                values: r,
                                explicit_transfer_formats: Some(result_transfer_formats.clone()),
                            })
                            .await?;
                        n_rows += 1;
                    }
                    channel
                        .feed(CommandComplete {
                            tag: CommandCompleteTag::Select(n_rows),
                        })
                        .await?;
                } else {
                    let tag = match response {
                        Insert(n) => CommandCompleteTag::Insert(n),
                        Update(n) => CommandCompleteTag::Update(n),
                        Delete(n) => CommandCompleteTag::Delete(n),
                        Command => CommandCompleteTag::Empty,
                        Select { .. } => unreachable!(),
                    };
                    channel.feed(CommandComplete { tag }).await?;
                }
            }

            Query { query } => {
                let response = backend.on_query(query.borrow()).await?;

                if let Select { schema, resultset } = response {
                    channel
                        .feed(RowDescription {
                            field_descriptions: schema
                                .iter()
                                .map(|i| make_field_description(i, Text))
                                .collect::<Result<Vec<FieldDescription>, Error>>()?,
                        })
                        .await?;
                    let mut n_rows = 0;
                    for r in resultset {
                        channel
                            .feed(DataRow {
                                values: r,
                                explicit_transfer_formats: None,
                            })
                            .await?;
                        n_rows += 1;
                    }
                    channel
                        .feed(CommandComplete {
                            tag: CommandCompleteTag::Select(n_rows),
                        })
                        .await?;
                    channel.feed(BackendMessage::ready_for_query_idle()).await?;
                } else {
                    let tag = match response {
                        Insert(n) => CommandCompleteTag::Insert(n),
                        Update(n) => CommandCompleteTag::Update(n),
                        Delete(n) => CommandCompleteTag::Delete(n),
                        Command => CommandCompleteTag::Empty,
                        Select { .. } => unreachable!(),
                    };
                    channel.feed(CommandComplete { tag }).await?;
                    channel.feed(BackendMessage::ready_for_query_idle()).await?;
                }
            }

            Parse {
                prepared_statement_name,
                query,
                ..
            } => {
                let PrepareResponse {
                    prepared_statement_id,
                    param_schema,
                    row_schema,
                } = backend.on_prepare(query.borrow()).await?;

                channel.set_statement_param_types(
                    prepared_statement_name.borrow() as &str,
                    param_schema
                        .iter()
                        .map(|(_, t)| to_type(t))
                        .collect::<Result<Vec<Type>, Error>>()?,
                );
                self.prepared_statements.insert(
                    prepared_statement_name.to_string(),
                    PreparedStatementData {
                        prepared_statement_id,
                        param_schema,
                        row_schema,
                    },
                );

                channel.feed(ParseComplete).await?;
            }

            Sync => channel.feed(BackendMessage::ready_for_query_idle()).await?,

            Terminate => {
                // no response is sent
            }

            m => Err(Error::UnsupportedMessage(m))?,
        }

        channel.flush().await?;
        Ok(())
    }

    pub async fn handle_error<B: Backend, C: AsyncRead + AsyncWrite + Unpin>(
        &mut self,
        error: Error,
        channel: &mut Channel<C, B::Row>,
    ) -> Result<(), Error> {
        if self.is_starting_up {
            channel.feed(make_error_response(error)).await?;
        } else {
            channel.feed(make_error_response(error)).await?;
            channel.feed(BackendMessage::ready_for_query_idle()).await?;
        }
        channel.flush().await?;
        Ok(())
    }
}

fn make_error_response<R>(error: Error) -> BackendMessage<R> {
    let sqlstate = match error {
        Error::DecodeError(_) => SqlState::IO_ERROR,
        Error::EncodeError(_) => SqlState::IO_ERROR,
        Error::IncorrectFormatCount(_) => SqlState::IO_ERROR,
        Error::InternalError(_) => SqlState::INTERNAL_ERROR,
        Error::InvalidInteger(_) => SqlState::DATATYPE_MISMATCH,
        Error::IoError(_) => SqlState::IO_ERROR,
        Error::MissingPortal(_) => SqlState::UNDEFINED_PSTATEMENT,
        Error::MissingPreparedStatement(_) => SqlState::UNDEFINED_PSTATEMENT,
        Error::ParseError(_) => SqlState::INVALID_PSTATEMENT_DEFINITION,
        Error::Unimplemented(_) => SqlState::FEATURE_NOT_SUPPORTED,
        Error::Unknown(_) => SqlState::INTERNAL_ERROR,
        Error::Unsupported(_) => SqlState::FEATURE_NOT_SUPPORTED,
        Error::UnsupportedMessage(_) => SqlState::FEATURE_NOT_SUPPORTED,
        Error::UnsupportedType(_) => SqlState::FEATURE_NOT_SUPPORTED,
    };
    ErrorResponse {
        severity: ErrorSeverity::Error,
        sqlstate,
        message: error.to_string(),
    }
}

fn make_field_description(
    (name, col_type): &(String, ColType),
    transfer_format: TransferFormat,
) -> Result<FieldDescription, Error> {
    let data_type = to_type(col_type)?;
    let (data_type_size, type_modifier) = match *col_type {
        ColType::Bool => (TYPLEN_1, ATTTYPMOD_NONE),
        ColType::Char(v) => (TYPLEN_1, i32::from(v)),
        ColType::Varchar(v) => (TYPLEN_VARLENA, i32::from(v)),
        ColType::UnsignedInt(_) => Err(Error::UnsupportedType(col_type.clone()))?,
        ColType::Int(_) => (TYPLEN_4, ATTTYPMOD_NONE),
        ColType::Bigint(_) => (TYPLEN_8, ATTTYPMOD_NONE),
        ColType::UnsignedBigint(_) => Err(Error::UnsupportedType(col_type.clone()))?,
        ColType::Tinyint(_) => Err(Error::UnsupportedType(col_type.clone()))?,
        ColType::UnsignedTinyint(_) => Err(Error::UnsupportedType(col_type.clone()))?,
        ColType::Smallint(_) => (TYPLEN_2, ATTTYPMOD_NONE),
        ColType::UnsignedSmallint(_) => Err(Error::UnsupportedType(col_type.clone()))?,
        ColType::Blob => Err(Error::UnsupportedType(col_type.clone()))?,
        ColType::Longblob => Err(Error::UnsupportedType(col_type.clone()))?,
        ColType::Mediumblob => Err(Error::UnsupportedType(col_type.clone()))?,
        ColType::Tinyblob => Err(Error::UnsupportedType(col_type.clone()))?,
        ColType::Double => (TYPLEN_8, ATTTYPMOD_NONE),
        ColType::Float => Err(Error::UnsupportedType(col_type.clone()))?,
        ColType::Real => (TYPLEN_4, ATTTYPMOD_NONE),
        ColType::Tinytext => Err(Error::UnsupportedType(col_type.clone()))?,
        ColType::Mediumtext => Err(Error::UnsupportedType(col_type.clone()))?,
        ColType::Longtext => Err(Error::UnsupportedType(col_type.clone()))?,
        ColType::Text => (TYPLEN_VARLENA, ATTTYPMOD_NONE),
        ColType::Date => Err(Error::UnsupportedType(col_type.clone()))?,
        ColType::DateTime(_) => Err(Error::UnsupportedType(col_type.clone()))?,
        ColType::Time => Err(Error::UnsupportedType(col_type.clone()))?,
        ColType::Timestamp => (TYPLEN_8, ATTTYPMOD_NONE),
        ColType::Binary(_) => Err(Error::UnsupportedType(col_type.clone()))?,
        ColType::Varbinary(_) => Err(Error::UnsupportedType(col_type.clone()))?,
        ColType::Enum(_) => Err(Error::UnsupportedType(col_type.clone()))?,
        ColType::Decimal(_, _) => Err(Error::UnsupportedType(col_type.clone()))?,
    };
    Ok(FieldDescription {
        field_name: name.clone(),
        table_id: UNKNOWN_TABLE,
        col_id: UNKNOWN_COLUMN,
        data_type,
        data_type_size,
        type_modifier,
        transfer_format: transfer_format,
    })
}

fn to_type(col_type: &ColType) -> Result<Type, Error> {
    match *col_type {
        ColType::Bool => Ok(Type::BOOL),
        ColType::Char(_) => Ok(Type::CHAR),
        ColType::Varchar(_) => Ok(Type::VARCHAR),
        ColType::Int(_) => Ok(Type::INT4),
        ColType::UnsignedInt(_) => Err(Error::UnsupportedType(col_type.clone()))?,
        ColType::Bigint(_) => Ok(Type::INT8),
        ColType::UnsignedBigint(_) => Err(Error::UnsupportedType(col_type.clone()))?,
        ColType::Tinyint(_) => Err(Error::UnsupportedType(col_type.clone()))?,
        ColType::UnsignedTinyint(_) => Err(Error::UnsupportedType(col_type.clone()))?,
        ColType::Smallint(_) => Ok(Type::INT2),
        ColType::UnsignedSmallint(_) => Err(Error::UnsupportedType(col_type.clone()))?,
        ColType::Blob => Err(Error::UnsupportedType(col_type.clone()))?,
        ColType::Longblob => Err(Error::UnsupportedType(col_type.clone()))?,
        ColType::Mediumblob => Err(Error::UnsupportedType(col_type.clone()))?,
        ColType::Tinyblob => Err(Error::UnsupportedType(col_type.clone()))?,
        ColType::Double => Ok(Type::FLOAT8),
        ColType::Float => Err(Error::UnsupportedType(col_type.clone()))?,
        ColType::Real => Ok(Type::FLOAT4),
        ColType::Tinytext => Err(Error::UnsupportedType(col_type.clone()))?,
        ColType::Mediumtext => Err(Error::UnsupportedType(col_type.clone()))?,
        ColType::Longtext => Err(Error::UnsupportedType(col_type.clone()))?,
        ColType::Text => Ok(Type::TEXT),
        ColType::Date => Err(Error::UnsupportedType(col_type.clone()))?,
        ColType::DateTime(_) => Err(Error::UnsupportedType(col_type.clone()))?,
        ColType::Time => Err(Error::UnsupportedType(col_type.clone()))?,
        ColType::Timestamp => Ok(Type::TIMESTAMP),
        ColType::Binary(_) => Err(Error::UnsupportedType(col_type.clone()))?,
        ColType::Varbinary(_) => Err(Error::UnsupportedType(col_type.clone()))?,
        ColType::Enum(_) => Err(Error::UnsupportedType(col_type.clone()))?,
        ColType::Decimal(_, _) => Err(Error::UnsupportedType(col_type.clone()))?,
    }
}
