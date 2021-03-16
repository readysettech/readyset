use crate::channel::Channel;
use crate::error::Error;
use crate::message::{
    BackendMessage::{self, *},
    CommandCompleteTag, ErrorSeverity, FieldDescription,
    FrontendMessage::{self, *},
    SqlState,
    StatementName::*,
    TransferFormat::*,
};
use crate::r#type::{ColType, Type};
use crate::value::Value;
use crate::{Backend, PrepareResponse, QueryResponse::*, Schema};
use std::borrow::Borrow;
use std::collections::HashMap;
use tokio::io::{AsyncRead, AsyncWrite};

const ATTTYPMOD_NONE: i32 = -1;
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
                SSLRequest { .. } => channel.send(BackendMessage::ssl_response_n()).await?,

                StartupMessage { database, .. } => {
                    let database = database
                        .ok_or_else(|| Error::Unsupported("database is required".to_string()))?;

                    backend.on_init(database.borrow()).await?;

                    self.is_starting_up = false;
                    channel.set_start_up_complete();

                    channel.feed(AuthenticationOk).await?;
                    channel.send(BackendMessage::ready_for_query_idle()).await?;
                }

                m => Err(Error::UnsupportedMessage(m))?,
            };

            return Ok(());
        }

        match message {
            Bind {
                prepared_statement_name,
                portal_name,
                params,
                ..
            } => {
                let PreparedStatementData {
                    prepared_statement_id,
                    ..
                } = self
                    .prepared_statements
                    .get(prepared_statement_name.borrow() as &str)
                    .ok_or_else(|| {
                        Error::MissingPreparedStatement(prepared_statement_name.to_string())
                    })?;

                self.portals.insert(
                    portal_name.to_string(),
                    PortalData {
                        prepared_statement_id: *prepared_statement_id,
                        prepared_statement_name: prepared_statement_name.to_string(),
                        params,
                    },
                );

                channel.send(BindComplete).await?;
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
                channel.send(CloseComplete).await?;
            }

            Describe { name } => match name {
                Portal(name) => {
                    let PortalData {
                        prepared_statement_name,
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

                    channel
                        .send(RowDescription {
                            field_descriptions: row_schema
                                .iter()
                                .map(make_field_description)
                                .collect(),
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
                                .collect(),
                        })
                        .await?;
                    channel
                        .send(RowDescription {
                            field_descriptions: row_schema
                                .iter()
                                .map(make_field_description)
                                .collect(),
                        })
                        .await?;
                }
            },

            Execute { portal_name, .. } => {
                let PortalData {
                    prepared_statement_id,
                    params,
                    ..
                } = self
                    .portals
                    .get(portal_name.borrow() as &str)
                    .ok_or_else(|| Error::MissingPreparedStatement(portal_name.to_string()))?;

                let response = backend.on_execute(*prepared_statement_id, params).await?;

                if let Select { rows, .. } = response {
                    let mut n_rows = 0;
                    for r in rows {
                        channel.feed(DataRow { values: r }).await?;
                        n_rows += 1;
                    }
                    channel
                        .send(CommandComplete {
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
                    channel.send(CommandComplete { tag }).await?;
                }
            }

            Query { query } => {
                let response = backend.on_query(query.borrow()).await?;

                if let Select { schema, rows } = response {
                    channel
                        .feed(RowDescription {
                            field_descriptions: schema.iter().map(make_field_description).collect(),
                        })
                        .await?;
                    let mut n_rows = 0;
                    for r in rows {
                        channel.feed(DataRow { values: r }).await?;
                        n_rows += 1;
                    }
                    channel
                        .feed(CommandComplete {
                            tag: CommandCompleteTag::Select(n_rows),
                        })
                        .await?;
                    channel.send(BackendMessage::ready_for_query_idle()).await?;
                } else {
                    let tag = match response {
                        Insert(n) => CommandCompleteTag::Insert(n),
                        Update(n) => CommandCompleteTag::Update(n),
                        Delete(n) => CommandCompleteTag::Delete(n),
                        Command => CommandCompleteTag::Empty,
                        Select { .. } => unreachable!(),
                    };
                    channel.feed(CommandComplete { tag }).await?;
                    channel.send(BackendMessage::ready_for_query_idle()).await?;
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
                    param_schema.iter().map(|(_, t)| to_type(t)).collect(),
                );
                self.prepared_statements.insert(
                    prepared_statement_name.to_string(),
                    PreparedStatementData {
                        prepared_statement_id,
                        param_schema,
                        row_schema,
                    },
                );

                channel.send(ParseComplete).await?;
            }

            Sync => channel.send(BackendMessage::ready_for_query_idle()).await?,

            Terminate => {
                // no response is sent
            }

            m => Err(Error::UnsupportedMessage(m))?,
        }

        Ok(())
    }

    pub async fn handle_error<B: Backend, C: AsyncRead + AsyncWrite + Unpin>(
        &mut self,
        error: Error,
        channel: &mut Channel<C, B::Row>,
    ) -> Result<(), Error> {
        if self.is_starting_up {
            channel.send(make_error_response(error)).await?;
        } else {
            channel.feed(make_error_response(error)).await?;
            channel.send(BackendMessage::ready_for_query_idle()).await?;
        }
        Ok(())
    }
}

fn make_error_response<R>(error: Error) -> BackendMessage<R> {
    let sqlstate = match error {
        Error::DecodeError(_) => SqlState::IO_ERROR,
        Error::EncodeError(_) => SqlState::IO_ERROR,
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
    };
    ErrorResponse {
        severity: ErrorSeverity::Error,
        sqlstate,
        message: error.to_string(),
    }
}

fn make_field_description((name, col_type): &(String, ColType)) -> FieldDescription {
    let data_type = to_type(col_type);
    let (data_type_size, type_modifier) = match *col_type {
        ColType::Varchar(v) => (TYPLEN_VARLENA, i32::from(v)),
        ColType::Int(_) => (TYPLEN_4, ATTTYPMOD_NONE),
        ColType::Bigint(_) => (TYPLEN_8, ATTTYPMOD_NONE),
        ColType::Double => (TYPLEN_8, ATTTYPMOD_NONE),
        ColType::Text => (TYPLEN_VARLENA, ATTTYPMOD_NONE),
        ColType::Timestamp => (TYPLEN_8, ATTTYPMOD_NONE),
        _ => unimplemented!(),
    };
    FieldDescription {
        field_name: name.clone(),
        table_id: UNKNOWN_TABLE,
        col_id: UNKNOWN_COLUMN,
        data_type,
        data_type_size,
        type_modifier,
        format: Binary,
    }
}

fn to_type(col_type: &ColType) -> Type {
    match *col_type {
        ColType::Varchar(_) => Type::VARCHAR,
        ColType::Int(_) => Type::INT4,
        ColType::Bigint(_) => Type::INT8,
        ColType::Double => Type::FLOAT8,
        ColType::Text => Type::TEXT,
        ColType::Timestamp => Type::TIMESTAMP,
        _ => unimplemented!(),
    }
}
