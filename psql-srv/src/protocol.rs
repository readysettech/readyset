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
use crate::response::Response;
use crate::value::Value;
use crate::{Backend, PrepareResponse, QueryResponse::*, Schema};
use std::borrow::Borrow;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};

const ATTTYPMOD_NONE: i32 = -1;
const TRANSFER_FORMAT_PLACEHOLDER: TransferFormat = TransferFormat::Text;
const TYPLEN_1: i16 = 1;
const TYPLEN_2: i16 = 2;
const TYPLEN_4: i16 = 4;
const TYPLEN_8: i16 = 8;
const TYPLEN_VARLENA: i16 = -1;
const UNKNOWN_COLUMN: i16 = 0;
const UNKNOWN_TABLE: i32 = 0;

/// A struct to maintain state for an implementation of the backend side of the PostgreSQL
/// frontend/backend protocol.
pub struct Protocol {
    /// Indicator of whether the connection to the frontend is starting up. When starting up,
    /// start-up messages are sent by the frontend rather than regular mode messages.
    is_starting_up: bool,

    /// A prepared statement allows a frontend to specify the general form of a SQL statement while
    /// leaving some values absent, but parameterized so that they can be provided later. This
    /// `HashMap` contains metadata about prepared statements that the frontend has requested, keyed
    /// by the prepared statement's name.
    prepared_statements: HashMap<String, PreparedStatementData>,

    /// A portal is a combination of a prepared statement and a list of values provided by the
    /// frontend for the prepared statement's parameters. This `HashMap` contains these parameter
    /// values as well as metadata about the portal, and is keyed by the portal's name.
    portals: HashMap<String, PortalData>,
}

/// A prepared statement allows a frontend to specify the general form of a SQL statement while
/// leaving some values absent, but parameterized so that they can be provided later. This struct
/// contains metadata about a prepared statement that the frontend has requested.
#[derive(Debug, PartialEq)]
struct PreparedStatementData {
    prepared_statement_id: u32,
    param_schema: Schema,
    row_schema: Schema,
}

/// A portal is a combination of a prepared statement and a list of values provided by the frontend
/// for the prepared statement's parameters. This struct contains these parameter values as well as
/// metadata about the portal.
#[derive(Debug, PartialEq)]
struct PortalData {
    prepared_statement_id: u32,
    prepared_statement_name: String,
    params: Vec<Value>,
    result_transfer_formats: Arc<Vec<TransferFormat>>,
}

/// An implementation of the backend side of the PostgreSQL frontend/backend protocol. See
/// `on_request` for the primary entry point.
impl Protocol {
    pub fn new() -> Protocol {
        Protocol {
            is_starting_up: true,
            prepared_statements: HashMap::new(),
            portals: HashMap::new(),
        }
    }

    /// The core implementation of the backend side of the PostgreSQL frontend/backend protocol.
    /// This implementation processes a message received from the frontend, forwards suitable
    /// requests to a `Backend`, and returns appropriate responses as a `Result`.
    ///
    /// * `message` - The message that has been received from the frontend.
    /// * `backend` - A `Backend` that handles the SQL statements supplied by the frontend. The
    ///     `Protocol`'s job is to extract SQL statements from frontend messages, supply these SQL
    ///     statements to the backend, and forward the backend's responses back to the frontend
    ///     using appropriate messages.
    /// * `channel` - A `Channel` representing a connection to the frontend. The channel is not
    ///     read from or written to within this function, but `channel` is provided so that its
    ///     codec state can be updated when the protocol state changes. (The codec state must be
    ///     synchronized with the frontend/backend protocol state in order to parse some types of
    ///     frontend messages.)
    /// * returns - A `Response` representing a sequence of `BackendMessage`s to return to the
    ///     frontend, otherwise an `Error` if a failure occurs.
    pub async fn on_request<B: Backend, C: AsyncRead + AsyncWrite + Unpin>(
        &mut self,
        message: FrontendMessage,
        backend: &mut B,
        channel: &mut Channel<C, B::Row>,
    ) -> Result<Response<B::Row, B::Resultset>, Error> {
        if self.is_starting_up {
            return match message {
                // A request for an SSL connection.
                SSLRequest { .. } => {
                    // Deny the SSL connection. The frontend may choose to proceed without SSL.
                    Ok(Response::Message(BackendMessage::ssl_response_n()))
                }

                // A request to start up a connection, with some metadata provided.
                StartupMessage { database, .. } => {
                    let database = database
                        .ok_or_else(|| Error::Unsupported("database is required".to_string()))?;
                    backend.on_init(database.borrow()).await?;
                    self.is_starting_up = false;
                    channel.set_start_up_complete();
                    Ok(Response::Message2(
                        AuthenticationOk,
                        BackendMessage::ready_for_query_idle(),
                    ))
                }

                m => Err(Error::UnsupportedMessage(m)),
            };
        }

        match message {
            // A request to bind parameters to a prepared statement, creating a portal.
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
                    // If no format codes are provided, use the default format (`Text`).
                    [] => vec![Text; n_cols],
                    // If only one format code is provided, apply it to all columns.
                    [f] => vec![f; n_cols],
                    // Otherwise use the format codes that have been provided, as is.
                    _ => {
                        if result_transfer_formats.len() == n_cols {
                            result_transfer_formats
                        } else {
                            return Err(Error::IncorrectFormatCount(n_cols));
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
                Ok(Response::Message(BindComplete))
            }

            // A request to close (deallocate) either a prepared statement or a portal.
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
                Ok(Response::Message(CloseComplete))
            }

            // A request to describe either a prepared statement or a portal.
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
                    Ok(Response::Message(RowDescription {
                        field_descriptions: row_schema
                            .iter()
                            .zip(result_transfer_formats.iter())
                            .map(|(i, f)| make_field_description(i, *f))
                            .collect::<Result<Vec<FieldDescription>, Error>>()?,
                    }))
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
                    Ok(Response::Message2(
                        ParameterDescription {
                            parameter_data_types: param_schema
                                .iter()
                                .map(|(_, t)| to_type(t))
                                .collect::<Result<Vec<Type>, Error>>()?,
                        },
                        RowDescription {
                            field_descriptions: row_schema
                                .iter()
                                .map(|i| make_field_description(i, TRANSFER_FORMAT_PLACEHOLDER))
                                .collect::<Result<Vec<FieldDescription>, Error>>()?,
                        },
                    ))
                }
            },

            // A request to execute a portal (a combination of a prepared statement with
            // parameter values).
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
                    Ok(Response::Select {
                        header: None,
                        resultset,
                        result_transfer_formats: Some(result_transfer_formats.clone()),
                        trailer: None,
                    })
                } else {
                    let tag = match response {
                        Insert(n) => CommandCompleteTag::Insert(n),
                        Update(n) => CommandCompleteTag::Update(n),
                        Delete(n) => CommandCompleteTag::Delete(n),
                        Command => CommandCompleteTag::Empty,
                        Select { .. } => unreachable!("Select is handled as a special case above."),
                    };
                    Ok(Response::Message(CommandComplete { tag }))
                }
            }

            // A request to directly execute a complete SQL statement, without creating a prepared
            // statement.
            Query { query } => {
                let response = backend.on_query(query.borrow()).await?;
                if let Select { schema, resultset } = response {
                    Ok(Response::Select {
                        header: Some(RowDescription {
                            field_descriptions: schema
                                .iter()
                                .map(|i| make_field_description(i, Text))
                                .collect::<Result<Vec<FieldDescription>, Error>>()?,
                        }),
                        resultset,
                        result_transfer_formats: None,
                        trailer: Some(BackendMessage::ready_for_query_idle()),
                    })
                } else {
                    let tag = match response {
                        Insert(n) => CommandCompleteTag::Insert(n),
                        Update(n) => CommandCompleteTag::Update(n),
                        Delete(n) => CommandCompleteTag::Delete(n),
                        Command => CommandCompleteTag::Empty,
                        Select { .. } => unreachable!("Select is handled as a special case above."),
                    };
                    Ok(Response::Message2(
                        CommandComplete { tag },
                        BackendMessage::ready_for_query_idle(),
                    ))
                }
            }

            // A request to create a prepared statement.
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
                Ok(Response::Message(ParseComplete))
            }

            // A request to synchronize state. Generally sent by the frontend after a query
            // sequence, or after an error has occurred.
            Sync => Ok(Response::Message(BackendMessage::ready_for_query_idle())),

            // A request to terminate the connection.
            Terminate => Ok(Response::Empty),

            m => Err(Error::UnsupportedMessage(m)),
        }
    }

    /// An error handler producing an `ErrorResponse` message.
    ///
    /// * `error` - an `Error` that has occurred while communicating with the frontend or handling
    ///     one of the frontend's requests.
    /// * returns - A `Response` containing an `ErrorResponse` message to send to the frontend.
    pub async fn on_error<B: Backend>(
        &mut self,
        error: Error,
    ) -> Result<Response<B::Row, B::Resultset>, Error> {
        if self.is_starting_up {
            Ok(Response::Message(make_error_response(error)))
        } else {
            Ok(Response::Message2(
                make_error_response(error),
                BackendMessage::ready_for_query_idle(),
            ))
        }
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
        ColType::UnsignedInt(_) => return Err(Error::UnsupportedType(col_type.clone())),
        ColType::Int(_) => (TYPLEN_4, ATTTYPMOD_NONE),
        ColType::Bigint(_) => (TYPLEN_8, ATTTYPMOD_NONE),
        ColType::UnsignedBigint(_) => return Err(Error::UnsupportedType(col_type.clone())),
        ColType::Tinyint(_) => return Err(Error::UnsupportedType(col_type.clone())),
        ColType::UnsignedTinyint(_) => return Err(Error::UnsupportedType(col_type.clone())),
        ColType::Smallint(_) => (TYPLEN_2, ATTTYPMOD_NONE),
        ColType::UnsignedSmallint(_) => return Err(Error::UnsupportedType(col_type.clone())),
        ColType::Blob => return Err(Error::UnsupportedType(col_type.clone())),
        ColType::Longblob => return Err(Error::UnsupportedType(col_type.clone())),
        ColType::Mediumblob => return Err(Error::UnsupportedType(col_type.clone())),
        ColType::Tinyblob => return Err(Error::UnsupportedType(col_type.clone())),
        ColType::Double => (TYPLEN_8, ATTTYPMOD_NONE),
        ColType::Float => return Err(Error::UnsupportedType(col_type.clone())),
        ColType::Real => (TYPLEN_4, ATTTYPMOD_NONE),
        ColType::Tinytext => return Err(Error::UnsupportedType(col_type.clone())),
        ColType::Mediumtext => return Err(Error::UnsupportedType(col_type.clone())),
        ColType::Longtext => return Err(Error::UnsupportedType(col_type.clone())),
        ColType::Text => (TYPLEN_VARLENA, ATTTYPMOD_NONE),
        ColType::Date => return Err(Error::UnsupportedType(col_type.clone())),
        ColType::DateTime(_) => return Err(Error::UnsupportedType(col_type.clone())),
        ColType::Time => return Err(Error::UnsupportedType(col_type.clone())),
        ColType::Timestamp => (TYPLEN_8, ATTTYPMOD_NONE),
        ColType::Binary(_) => return Err(Error::UnsupportedType(col_type.clone())),
        ColType::Varbinary(_) => return Err(Error::UnsupportedType(col_type.clone())),
        ColType::Enum(_) => return Err(Error::UnsupportedType(col_type.clone())),
        ColType::Decimal(_, _) => return Err(Error::UnsupportedType(col_type.clone())),
    };
    Ok(FieldDescription {
        field_name: name.clone(),
        table_id: UNKNOWN_TABLE,
        col_id: UNKNOWN_COLUMN,
        data_type,
        data_type_size,
        type_modifier,
        transfer_format,
    })
}

fn to_type(col_type: &ColType) -> Result<Type, Error> {
    match *col_type {
        ColType::Bool => Ok(Type::BOOL),
        ColType::Char(_) => Ok(Type::CHAR),
        ColType::Varchar(_) => Ok(Type::VARCHAR),
        ColType::Int(_) => Ok(Type::INT4),
        ColType::UnsignedInt(_) => Err(Error::UnsupportedType(col_type.clone())),
        ColType::Bigint(_) => Ok(Type::INT8),
        ColType::UnsignedBigint(_) => Err(Error::UnsupportedType(col_type.clone())),
        ColType::Tinyint(_) => Err(Error::UnsupportedType(col_type.clone())),
        ColType::UnsignedTinyint(_) => Err(Error::UnsupportedType(col_type.clone())),
        ColType::Smallint(_) => Ok(Type::INT2),
        ColType::UnsignedSmallint(_) => Err(Error::UnsupportedType(col_type.clone())),
        ColType::Blob => Err(Error::UnsupportedType(col_type.clone())),
        ColType::Longblob => Err(Error::UnsupportedType(col_type.clone())),
        ColType::Mediumblob => Err(Error::UnsupportedType(col_type.clone())),
        ColType::Tinyblob => Err(Error::UnsupportedType(col_type.clone())),
        ColType::Double => Ok(Type::FLOAT8),
        ColType::Float => Err(Error::UnsupportedType(col_type.clone())),
        ColType::Real => Ok(Type::FLOAT4),
        ColType::Tinytext => Err(Error::UnsupportedType(col_type.clone())),
        ColType::Mediumtext => Err(Error::UnsupportedType(col_type.clone())),
        ColType::Longtext => Err(Error::UnsupportedType(col_type.clone())),
        ColType::Text => Ok(Type::TEXT),
        ColType::Date => Err(Error::UnsupportedType(col_type.clone())),
        ColType::DateTime(_) => Err(Error::UnsupportedType(col_type.clone())),
        ColType::Time => Err(Error::UnsupportedType(col_type.clone())),
        ColType::Timestamp => Ok(Type::TIMESTAMP),
        ColType::Binary(_) => Err(Error::UnsupportedType(col_type.clone())),
        ColType::Varbinary(_) => Err(Error::UnsupportedType(col_type.clone())),
        ColType::Enum(_) => Err(Error::UnsupportedType(col_type.clone())),
        ColType::Decimal(_, _) => Err(Error::UnsupportedType(col_type.clone())),
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::bytes::BytesStr;
    use crate::value::Value as DataValue;
    use crate::{PrepareResponse, QueryResponse};
    use async_trait::async_trait;
    use bytes::BytesMut;
    use futures::task::Context;
    use std::convert::TryFrom;
    use std::io;
    use std::pin::Pin;
    use std::task::Poll;
    use tokio::io::ReadBuf;
    use tokio_test::block_on;

    fn bytes_str(s: &str) -> BytesStr {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(s.as_bytes());
        BytesStr::try_from(buf.freeze()).unwrap()
    }

    #[derive(Debug, PartialEq)]
    struct Value(DataValue);

    impl TryFrom<Value> for DataValue {
        type Error = Error;

        fn try_from(v: Value) -> Result<Self, Self::Error> {
            Ok(v.0)
        }
    }

    // A dummy `Backend` that records the values passed to it and can return a few hard-coded
    // responses.
    struct Backend {
        is_query_err: bool,
        is_query_read: bool,

        is_prepare_err: bool,

        database: Option<String>,
        last_query: Option<String>,
        last_prepare: Option<String>,
        last_close: Option<u32>,
        last_execute_id: Option<u32>,
        last_execute_params: Option<Vec<DataValue>>,
    }

    impl Backend {
        fn new() -> Backend {
            Backend {
                is_query_err: false,
                is_query_read: true,
                is_prepare_err: false,
                database: None,
                last_query: None,
                last_prepare: None,
                last_close: None,
                last_execute_id: None,
                last_execute_params: None,
            }
        }
    }

    #[async_trait]
    impl crate::Backend for Backend {
        type Value = Value;
        type Row = Vec<Self::Value>;
        type Resultset = Vec<Self::Row>;

        async fn on_init(&mut self, database: &str) -> Result<(), Error> {
            self.database = Some(database.to_string());
            Ok(())
        }

        async fn on_query(&mut self, query: &str) -> Result<QueryResponse<Self::Resultset>, Error> {
            self.last_query = Some(query.to_string());
            if self.is_query_err {
                Err(Error::InternalError("error requested".to_string()))
            } else if self.is_query_read {
                Ok(QueryResponse::Select {
                    schema: vec![
                        ("col1".to_string(), ColType::Int(4)),
                        ("col2".to_string(), ColType::Double),
                    ],
                    resultset: vec![
                        vec![Value(DataValue::Int(88)), Value(DataValue::Double(0.123))],
                        vec![Value(DataValue::Int(22)), Value(DataValue::Double(0.456))],
                    ],
                })
            } else {
                Ok(QueryResponse::Delete(5))
            }
        }

        async fn on_prepare(&mut self, query: &str) -> Result<PrepareResponse, Error> {
            self.last_prepare = Some(query.to_string());
            if self.is_prepare_err {
                Err(Error::InternalError("error requested".to_string()))
            } else {
                Ok(PrepareResponse {
                    prepared_statement_id: 1,
                    param_schema: vec![
                        ("x".to_string(), ColType::Double),
                        ("y".to_string(), ColType::Int(4)),
                    ],
                    row_schema: vec![
                        ("col1".to_string(), ColType::Int(4)),
                        ("col2".to_string(), ColType::Double),
                    ],
                })
            }
        }

        async fn on_execute(
            &mut self,
            statement_id: u32,
            params: &[DataValue],
        ) -> Result<QueryResponse<Self::Resultset>, Error> {
            self.last_execute_id = Some(statement_id);
            self.last_execute_params = Some(params.to_vec());
            if self.is_query_err {
                Err(Error::InternalError("error requested".to_string()))
            } else if self.is_query_read {
                Ok(QueryResponse::Select {
                    schema: vec![
                        ("col1".to_string(), ColType::Int(4)),
                        ("col2".to_string(), ColType::Double),
                    ],
                    resultset: vec![
                        vec![Value(DataValue::Int(88)), Value(DataValue::Double(0.123))],
                        vec![Value(DataValue::Int(22)), Value(DataValue::Double(0.456))],
                    ],
                })
            } else {
                Ok(QueryResponse::Delete(5))
            }
        }

        async fn on_close(&mut self, statement_id: u32) -> Result<(), Error> {
            self.last_close = Some(statement_id);
            Ok(())
        }
    }

    // A dummy `AsyncRead + AsyncWrite` that does not read or write any data.
    struct NullBytestream;

    impl AsyncRead for NullBytestream {
        fn poll_read(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    impl AsyncWrite for NullBytestream {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[test]
    fn ssl_request() {
        let mut protocol = Protocol::new();
        let request = FrontendMessage::SSLRequest;
        let mut backend = Backend::new();
        let mut channel = Channel::<NullBytestream, Vec<Value>>::new(NullBytestream);
        // SSLRequest is denied.
        assert_eq!(
            block_on(protocol.on_request(request, &mut backend, &mut channel)).unwrap(),
            Response::Message(BackendMessage::ssl_response_n())
        );
    }

    #[test]
    fn startup_message() {
        let mut protocol = Protocol::new();
        assert!(protocol.is_starting_up);
        let request = FrontendMessage::StartupMessage {
            protocol_version: 12345,
            user: Some(bytes_str("user_name")),
            database: Some(bytes_str("database_name")),
        };
        let mut backend = Backend::new();
        let mut channel = Channel::<NullBytestream, Vec<Value>>::new(NullBytestream);
        // A StartupMessage with a database specified is accepted.
        assert_eq!(
            block_on(protocol.on_request(request, &mut backend, &mut channel)).unwrap(),
            Response::Message2(
                BackendMessage::AuthenticationOk,
                BackendMessage::ready_for_query_idle()
            )
        );
        // The database has been set on the backend.
        assert_eq!(backend.database.unwrap(), "database_name");
        // The protocol is no longer "starting up".
        assert!(!protocol.is_starting_up);
    }

    #[test]
    fn startup_message_without_database() {
        let mut protocol = Protocol::new();
        let request = FrontendMessage::StartupMessage {
            protocol_version: 12345,
            user: Some(bytes_str("user_name")),
            database: None,
        };
        let mut backend = Backend::new();
        let mut channel = Channel::<NullBytestream, Vec<Value>>::new(NullBytestream);
        // A StartupMessage with no database specified triggers an error.
        assert!(block_on(protocol.on_request(request, &mut backend, &mut channel)).is_err(),);
    }

    #[test]
    fn regular_mode_message_without_startup() {
        let mut protocol = Protocol::new();
        let request = FrontendMessage::Sync;
        let mut backend = Backend::new();
        let mut channel = Channel::<NullBytestream, Vec<Value>>::new(NullBytestream);
        // A Sync message cannot be sent until after a StartupMessage has been sent.
        assert!(block_on(protocol.on_request(request, &mut backend, &mut channel)).is_err(),);
    }

    #[test]
    fn startup_message_repeated() {
        let mut protocol = Protocol::new();
        let mut backend = Backend::new();
        let mut channel = Channel::<NullBytestream, Vec<Value>>::new(NullBytestream);

        let startup_request = FrontendMessage::StartupMessage {
            protocol_version: 12345,
            user: Some(bytes_str("user_name")),
            database: Some(bytes_str("database_name")),
        };
        block_on(protocol.on_request(startup_request, &mut backend, &mut channel)).unwrap();

        // StartupMessage cannot be handled after the connection has already started.
        let request = FrontendMessage::StartupMessage {
            protocol_version: 12345,
            user: Some(bytes_str("user_name")),
            database: Some(bytes_str("database_name")),
        };
        assert!(block_on(protocol.on_request(request, &mut backend, &mut channel)).is_err(),);
    }

    #[test]
    fn sync() {
        let mut protocol = Protocol::new();
        let mut backend = Backend::new();
        let mut channel = Channel::<NullBytestream, Vec<Value>>::new(NullBytestream);

        let startup_request = FrontendMessage::StartupMessage {
            protocol_version: 12345,
            user: Some(bytes_str("user_name")),
            database: Some(bytes_str("database_name")),
        };
        block_on(protocol.on_request(startup_request, &mut backend, &mut channel)).unwrap();

        // A Sync message is accepted (after connection start up completes).
        let request = FrontendMessage::Sync;
        assert_eq!(
            block_on(protocol.on_request(request, &mut backend, &mut channel)).unwrap(),
            Response::Message(BackendMessage::ready_for_query_idle())
        );
    }

    #[test]
    fn terminate() {
        let mut protocol = Protocol::new();
        let mut backend = Backend::new();
        let mut channel = Channel::<NullBytestream, Vec<Value>>::new(NullBytestream);

        let startup_request = FrontendMessage::StartupMessage {
            protocol_version: 12345,
            user: Some(bytes_str("user_name")),
            database: Some(bytes_str("database_name")),
        };
        block_on(protocol.on_request(startup_request, &mut backend, &mut channel)).unwrap();

        // A Terminate message is accepted (no response message is returned).
        let request = FrontendMessage::Terminate;
        assert_eq!(
            block_on(protocol.on_request(request, &mut backend, &mut channel)).unwrap(),
            Response::Empty
        );
    }

    #[test]
    fn query_read() {
        let mut protocol = Protocol::new();
        let mut backend = Backend::new();
        let mut channel = Channel::<NullBytestream, Vec<Value>>::new(NullBytestream);

        let startup_request = FrontendMessage::StartupMessage {
            protocol_version: 12345,
            user: Some(bytes_str("user_name")),
            database: Some(bytes_str("database_name")),
        };
        block_on(protocol.on_request(startup_request, &mut backend, &mut channel)).unwrap();

        // A read query is passed to the backend correctly and a suitable result is returned.
        let request = FrontendMessage::Query {
            query: bytes_str("SELECT * FROM test;"),
        };
        assert_eq!(
            block_on(protocol.on_request(request, &mut backend, &mut channel)).unwrap(),
            Response::Select {
                header: Some(RowDescription {
                    field_descriptions: vec![
                        FieldDescription {
                            field_name: "col1".to_string(),
                            table_id: UNKNOWN_TABLE,
                            col_id: UNKNOWN_COLUMN,
                            data_type: Type::INT4,
                            data_type_size: TYPLEN_4,
                            type_modifier: ATTTYPMOD_NONE,
                            transfer_format: TransferFormat::Text
                        },
                        FieldDescription {
                            field_name: "col2".to_string(),
                            table_id: UNKNOWN_TABLE,
                            col_id: UNKNOWN_COLUMN,
                            data_type: Type::FLOAT8,
                            data_type_size: TYPLEN_8,
                            type_modifier: ATTTYPMOD_NONE,
                            transfer_format: TransferFormat::Text
                        },
                    ],
                }),
                resultset: vec![
                    vec![Value(DataValue::Int(88)), Value(DataValue::Double(0.123))],
                    vec![Value(DataValue::Int(22)), Value(DataValue::Double(0.456))]
                ],
                result_transfer_formats: None,
                trailer: Some(BackendMessage::ready_for_query_idle())
            }
        );
        assert_eq!(backend.last_query.unwrap(), "SELECT * FROM test;");
    }

    #[test]
    fn query_error() {
        let mut protocol = Protocol::new();
        let mut backend = Backend::new();
        backend.is_query_err = true;
        let mut channel = Channel::<NullBytestream, Vec<Value>>::new(NullBytestream);

        let startup_request = FrontendMessage::StartupMessage {
            protocol_version: 12345,
            user: Some(bytes_str("user_name")),
            database: Some(bytes_str("database_name")),
        };
        block_on(protocol.on_request(startup_request, &mut backend, &mut channel)).unwrap();

        // An `Error` is returned when the backend returns an error.
        let request = FrontendMessage::Query {
            query: bytes_str("SELECT * FROM test;"),
        };
        assert!(block_on(protocol.on_request(request, &mut backend, &mut channel)).is_err());
    }

    #[test]
    fn query_write() {
        let mut protocol = Protocol::new();
        let mut backend = Backend::new();
        backend.is_query_read = false;
        let mut channel = Channel::<NullBytestream, Vec<Value>>::new(NullBytestream);

        let startup_request = FrontendMessage::StartupMessage {
            protocol_version: 12345,
            user: Some(bytes_str("user_name")),
            database: Some(bytes_str("database_name")),
        };
        block_on(protocol.on_request(startup_request, &mut backend, &mut channel)).unwrap();

        // A write query is passed to the backend correctly and a suitable result is returned.
        let request = FrontendMessage::Query {
            query: bytes_str("DELETE * FROM test;"),
        };
        assert_eq!(
            block_on(protocol.on_request(request, &mut backend, &mut channel)).unwrap(),
            Response::Message2(
                CommandComplete {
                    tag: CommandCompleteTag::Delete(5)
                },
                BackendMessage::ready_for_query_idle()
            )
        );
        assert_eq!(backend.last_query.unwrap(), "DELETE * FROM test;");
    }

    #[test]
    fn parse() {
        let mut protocol = Protocol::new();
        let mut backend = Backend::new();
        let mut channel = Channel::<NullBytestream, Vec<Value>>::new(NullBytestream);

        let startup_request = FrontendMessage::StartupMessage {
            protocol_version: 12345,
            user: Some(bytes_str("user_name")),
            database: Some(bytes_str("database_name")),
        };
        block_on(protocol.on_request(startup_request, &mut backend, &mut channel)).unwrap();

        // A parse message generates a correct prepared statement with the backend, correctly
        // updates Protocol prepared statement state, and produces a suitable response.
        let request = FrontendMessage::Parse {
            prepared_statement_name: bytes_str("prepared1"),
            query: bytes_str("SELECT * FROM test WHERE x = $1 AND y = $2;"),
            parameter_data_types: vec![],
        };
        assert_eq!(
            block_on(protocol.on_request(request, &mut backend, &mut channel)).unwrap(),
            Response::Message(ParseComplete)
        );
        assert_eq!(
            backend.last_prepare.unwrap(),
            "SELECT * FROM test WHERE x = $1 AND y = $2;"
        );
        assert_eq!(
            *protocol.prepared_statements.get("prepared1").unwrap(),
            PreparedStatementData {
                prepared_statement_id: 1,
                param_schema: vec![
                    ("x".to_string(), ColType::Double),
                    ("y".to_string(), ColType::Int(4))
                ],
                row_schema: vec![
                    ("col1".to_string(), ColType::Int(4)),
                    ("col2".to_string(), ColType::Double),
                ],
            }
        );
    }

    #[test]
    fn parse_error() {
        let mut protocol = Protocol::new();
        let mut backend = Backend::new();
        backend.is_prepare_err = true;
        let mut channel = Channel::<NullBytestream, Vec<Value>>::new(NullBytestream);

        let startup_request = FrontendMessage::StartupMessage {
            protocol_version: 12345,
            user: Some(bytes_str("user_name")),
            database: Some(bytes_str("database_name")),
        };
        block_on(protocol.on_request(startup_request, &mut backend, &mut channel)).unwrap();

        // An `Error` is returned when the backend returns an error.
        let request = FrontendMessage::Parse {
            prepared_statement_name: bytes_str("prepared1"),
            query: bytes_str("SELECT * FROM test WHERE x = $1 AND y = $2;"),
            parameter_data_types: vec![Type::FLOAT8, Type::INT4],
        };
        assert!(block_on(protocol.on_request(request, &mut backend, &mut channel)).is_err());
    }

    #[test]
    fn bind() {
        let mut protocol = Protocol::new();
        let mut backend = Backend::new();
        let mut channel = Channel::<NullBytestream, Vec<Value>>::new(NullBytestream);

        let startup_request = FrontendMessage::StartupMessage {
            protocol_version: 12345,
            user: Some(bytes_str("user_name")),
            database: Some(bytes_str("database_name")),
        };
        block_on(protocol.on_request(startup_request, &mut backend, &mut channel)).unwrap();

        let parse_request = FrontendMessage::Parse {
            prepared_statement_name: bytes_str("prepared1"),
            query: bytes_str("SELECT * FROM test WHERE x = $1 AND y = $2;"),
            parameter_data_types: vec![],
        };
        block_on(protocol.on_request(parse_request, &mut backend, &mut channel)).unwrap();

        // A bind message generates correctly updates Protocol portal state and produces a suitable
        // response.
        let request = FrontendMessage::Bind {
            prepared_statement_name: bytes_str("prepared1"),
            portal_name: bytes_str("portal1"),
            params: vec![DataValue::Double(0.8887), DataValue::Int(45678)],
            result_transfer_formats: vec![TransferFormat::Text, TransferFormat::Binary],
        };
        assert_eq!(
            block_on(protocol.on_request(request, &mut backend, &mut channel)).unwrap(),
            Response::Message(BindComplete)
        );
        assert_eq!(
            *protocol.portals.get("portal1").unwrap(),
            PortalData {
                prepared_statement_id: 1,
                prepared_statement_name: "prepared1".to_string(),
                params: vec![DataValue::Double(0.8887), DataValue::Int(45678)],
                result_transfer_formats: Arc::new(vec![
                    TransferFormat::Text,
                    TransferFormat::Binary
                ])
            }
        );
    }

    #[test]
    fn bind_no_result_transfer_formats() {
        let mut protocol = Protocol::new();
        let mut backend = Backend::new();
        let mut channel = Channel::<NullBytestream, Vec<Value>>::new(NullBytestream);

        let startup_request = FrontendMessage::StartupMessage {
            protocol_version: 12345,
            user: Some(bytes_str("user_name")),
            database: Some(bytes_str("database_name")),
        };
        block_on(protocol.on_request(startup_request, &mut backend, &mut channel)).unwrap();

        let parse_request = FrontendMessage::Parse {
            prepared_statement_name: bytes_str("prepared1"),
            query: bytes_str("SELECT * FROM test WHERE x = $1 AND y = $2;"),
            parameter_data_types: vec![],
        };
        block_on(protocol.on_request(parse_request, &mut backend, &mut channel)).unwrap();

        // A bind message generates correctly updates Protocol portal state and produces a suitable
        // response.
        let request = FrontendMessage::Bind {
            prepared_statement_name: bytes_str("prepared1"),
            portal_name: bytes_str("portal1"),
            params: vec![DataValue::Double(0.8887), DataValue::Int(45678)],
            result_transfer_formats: vec![],
        };
        assert_eq!(
            block_on(protocol.on_request(request, &mut backend, &mut channel)).unwrap(),
            Response::Message(BindComplete)
        );
        assert_eq!(
            *protocol.portals.get("portal1").unwrap(),
            PortalData {
                prepared_statement_id: 1,
                prepared_statement_name: "prepared1".to_string(),
                params: vec![DataValue::Double(0.8887), DataValue::Int(45678)],
                // The transfer formats are set to the default value (Text).
                result_transfer_formats: Arc::new(vec![TransferFormat::Text, TransferFormat::Text])
            }
        );
    }

    #[test]
    fn bind_single_result_transfer_format() {
        let mut protocol = Protocol::new();
        let mut backend = Backend::new();
        let mut channel = Channel::<NullBytestream, Vec<Value>>::new(NullBytestream);

        let startup_request = FrontendMessage::StartupMessage {
            protocol_version: 12345,
            user: Some(bytes_str("user_name")),
            database: Some(bytes_str("database_name")),
        };
        block_on(protocol.on_request(startup_request, &mut backend, &mut channel)).unwrap();

        let parse_request = FrontendMessage::Parse {
            prepared_statement_name: bytes_str("prepared1"),
            query: bytes_str("SELECT * FROM test WHERE x = $1 AND y = $2;"),
            parameter_data_types: vec![],
        };
        block_on(protocol.on_request(parse_request, &mut backend, &mut channel)).unwrap();

        // A bind message generates correctly updates Protocol portal state and produces a suitable
        // response.
        let request = FrontendMessage::Bind {
            prepared_statement_name: bytes_str("prepared1"),
            portal_name: bytes_str("portal1"),
            params: vec![DataValue::Double(0.8887), DataValue::Int(45678)],
            result_transfer_formats: vec![TransferFormat::Binary],
        };
        assert_eq!(
            block_on(protocol.on_request(request, &mut backend, &mut channel)).unwrap(),
            Response::Message(BindComplete)
        );
        assert_eq!(
            *protocol.portals.get("portal1").unwrap(),
            PortalData {
                prepared_statement_id: 1,
                prepared_statement_name: "prepared1".to_string(),
                params: vec![DataValue::Double(0.8887), DataValue::Int(45678)],
                // The single transfer format is applied to both fields.
                result_transfer_formats: Arc::new(vec![
                    TransferFormat::Binary,
                    TransferFormat::Binary
                ])
            }
        );
    }

    #[test]
    fn bind_invalid_result_transfer_formats() {
        let mut protocol = Protocol::new();
        let mut backend = Backend::new();
        let mut channel = Channel::<NullBytestream, Vec<Value>>::new(NullBytestream);

        let startup_request = FrontendMessage::StartupMessage {
            protocol_version: 12345,
            user: Some(bytes_str("user_name")),
            database: Some(bytes_str("database_name")),
        };
        block_on(protocol.on_request(startup_request, &mut backend, &mut channel)).unwrap();

        let parse_request = FrontendMessage::Parse {
            prepared_statement_name: bytes_str("prepared1"),
            query: bytes_str("SELECT * FROM test WHERE x = $1 AND y = $2;"),
            parameter_data_types: vec![],
        };
        block_on(protocol.on_request(parse_request, &mut backend, &mut channel)).unwrap();

        // An unsupported number of data transfer formats triggers an error.
        let request = FrontendMessage::Bind {
            prepared_statement_name: bytes_str("prepared1"),
            portal_name: bytes_str("portal1"),
            params: vec![DataValue::Double(0.8887), DataValue::Int(45678)],
            result_transfer_formats: vec![
                TransferFormat::Binary,
                TransferFormat::Binary,
                TransferFormat::Binary,
            ],
        };
        assert!(block_on(protocol.on_request(request, &mut backend, &mut channel)).is_err());
    }

    #[test]
    fn bind_missing_prepared_statement() {
        let mut protocol = Protocol::new();
        let mut backend = Backend::new();
        let mut channel = Channel::<NullBytestream, Vec<Value>>::new(NullBytestream);

        let startup_request = FrontendMessage::StartupMessage {
            protocol_version: 12345,
            user: Some(bytes_str("user_name")),
            database: Some(bytes_str("database_name")),
        };
        block_on(protocol.on_request(startup_request, &mut backend, &mut channel)).unwrap();

        let parse_request = FrontendMessage::Parse {
            prepared_statement_name: bytes_str("prepared1"),
            query: bytes_str("SELECT * FROM test WHERE x = $1 AND y = $2;"),
            parameter_data_types: vec![],
        };
        block_on(protocol.on_request(parse_request, &mut backend, &mut channel)).unwrap();

        // An attempt to bind a prepared statement that does not exist triggers an error.
        let request = FrontendMessage::Bind {
            prepared_statement_name: bytes_str("prepared_invalid name"),
            portal_name: bytes_str("portal1"),
            params: vec![DataValue::Double(0.8887), DataValue::Int(45678)],
            result_transfer_formats: vec![],
        };
        assert!(block_on(protocol.on_request(request, &mut backend, &mut channel)).is_err());
    }

    #[test]
    fn close_prepared_statement() {
        let mut protocol = Protocol::new();
        let mut backend = Backend::new();
        let mut channel = Channel::<NullBytestream, Vec<Value>>::new(NullBytestream);

        let startup_request = FrontendMessage::StartupMessage {
            protocol_version: 12345,
            user: Some(bytes_str("user_name")),
            database: Some(bytes_str("database_name")),
        };
        block_on(protocol.on_request(startup_request, &mut backend, &mut channel)).unwrap();

        let parse_request = FrontendMessage::Parse {
            prepared_statement_name: bytes_str("prepared1"),
            query: bytes_str("SELECT * FROM test WHERE x = $1 AND y = $2;"),
            parameter_data_types: vec![],
        };
        block_on(protocol.on_request(parse_request, &mut backend, &mut channel)).unwrap();
        assert!(protocol.prepared_statements.get("prepared1").is_some());

        // A prepared statement close request calls close on the backend and removes Protocol state
        // for the prepared statement.
        let request = FrontendMessage::Close {
            name: PreparedStatement(bytes_str("prepared1")),
        };
        assert_eq!(
            block_on(protocol.on_request(request, &mut backend, &mut channel)).unwrap(),
            Response::Message(CloseComplete)
        );
        assert_eq!(backend.last_close.unwrap(), 1);
        assert!(protocol.prepared_statements.get("prepared1").is_none());
    }

    #[test]
    fn close_missing_prepared_statement() {
        let mut protocol = Protocol::new();
        let mut backend = Backend::new();
        let mut channel = Channel::<NullBytestream, Vec<Value>>::new(NullBytestream);

        let startup_request = FrontendMessage::StartupMessage {
            protocol_version: 12345,
            user: Some(bytes_str("user_name")),
            database: Some(bytes_str("database_name")),
        };
        block_on(protocol.on_request(startup_request, &mut backend, &mut channel)).unwrap();

        // An attempt to close a missing prepared statement triggers a normal response (no error).
        let request = FrontendMessage::Close {
            name: PreparedStatement(bytes_str("prepared1")),
        };
        assert_eq!(
            block_on(protocol.on_request(request, &mut backend, &mut channel)).unwrap(),
            Response::Message(CloseComplete)
        );
    }

    #[test]
    fn close_portal() {
        let mut protocol = Protocol::new();
        let mut backend = Backend::new();
        let mut channel = Channel::<NullBytestream, Vec<Value>>::new(NullBytestream);

        let startup_request = FrontendMessage::StartupMessage {
            protocol_version: 12345,
            user: Some(bytes_str("user_name")),
            database: Some(bytes_str("database_name")),
        };
        block_on(protocol.on_request(startup_request, &mut backend, &mut channel)).unwrap();

        let parse_request = FrontendMessage::Parse {
            prepared_statement_name: bytes_str("prepared1"),
            query: bytes_str("SELECT * FROM test WHERE x = $1 AND y = $2;"),
            parameter_data_types: vec![],
        };
        block_on(protocol.on_request(parse_request, &mut backend, &mut channel)).unwrap();

        let bind_request = FrontendMessage::Bind {
            prepared_statement_name: bytes_str("prepared1"),
            portal_name: bytes_str("portal1"),
            params: vec![DataValue::Double(0.8887), DataValue::Int(45678)],
            result_transfer_formats: vec![TransferFormat::Text, TransferFormat::Binary],
        };
        assert_eq!(
            block_on(protocol.on_request(bind_request, &mut backend, &mut channel)).unwrap(),
            Response::Message(BindComplete)
        );
        assert!(protocol.portals.get("portal1").is_some());

        // A portal close request removes Protocol state for the portal.
        let request = FrontendMessage::Close {
            name: Portal(bytes_str("portal1")),
        };
        assert_eq!(
            block_on(protocol.on_request(request, &mut backend, &mut channel)).unwrap(),
            Response::Message(CloseComplete)
        );
        assert!(protocol.portals.get("protal1").is_none());
    }

    #[test]
    fn close_missing_portal() {
        let mut protocol = Protocol::new();
        let mut backend = Backend::new();
        let mut channel = Channel::<NullBytestream, Vec<Value>>::new(NullBytestream);

        let startup_request = FrontendMessage::StartupMessage {
            protocol_version: 12345,
            user: Some(bytes_str("user_name")),
            database: Some(bytes_str("database_name")),
        };
        block_on(protocol.on_request(startup_request, &mut backend, &mut channel)).unwrap();

        // An attempt to close a missing portal triggers a normal response (no error).
        let request = FrontendMessage::Close {
            name: Portal(bytes_str("portal1")),
        };
        assert_eq!(
            block_on(protocol.on_request(request, &mut backend, &mut channel)).unwrap(),
            Response::Message(CloseComplete)
        );
    }

    #[test]
    fn describe_prepared_statement() {
        let mut protocol = Protocol::new();
        let mut backend = Backend::new();
        let mut channel = Channel::<NullBytestream, Vec<Value>>::new(NullBytestream);

        let startup_request = FrontendMessage::StartupMessage {
            protocol_version: 12345,
            user: Some(bytes_str("user_name")),
            database: Some(bytes_str("database_name")),
        };
        block_on(protocol.on_request(startup_request, &mut backend, &mut channel)).unwrap();

        let parse_request = FrontendMessage::Parse {
            prepared_statement_name: bytes_str("prepared1"),
            query: bytes_str("SELECT * FROM test WHERE x = $1 AND y = $2;"),
            parameter_data_types: vec![],
        };
        block_on(protocol.on_request(parse_request, &mut backend, &mut channel)).unwrap();
        assert!(protocol.prepared_statements.get("prepared1").is_some());

        // A prepared statement describe request generates a suitable description.
        let request = FrontendMessage::Describe {
            name: PreparedStatement(bytes_str("prepared1")),
        };
        assert_eq!(
            block_on(protocol.on_request(request, &mut backend, &mut channel)).unwrap(),
            Response::Message2(
                ParameterDescription {
                    parameter_data_types: vec![Type::FLOAT8, Type::INT4]
                },
                RowDescription {
                    field_descriptions: vec![
                        FieldDescription {
                            field_name: "col1".to_string(),
                            table_id: UNKNOWN_TABLE,
                            col_id: UNKNOWN_COLUMN,
                            data_type: Type::INT4,
                            data_type_size: TYPLEN_4,
                            type_modifier: ATTTYPMOD_NONE,
                            transfer_format: TransferFormat::Text
                        },
                        FieldDescription {
                            field_name: "col2".to_string(),
                            table_id: UNKNOWN_TABLE,
                            col_id: UNKNOWN_COLUMN,
                            data_type: Type::FLOAT8,
                            data_type_size: TYPLEN_8,
                            type_modifier: ATTTYPMOD_NONE,
                            transfer_format: TransferFormat::Text
                        },
                    ],
                }
            )
        );
    }

    #[test]
    fn describe_missing_prepared_statement() {
        let mut protocol = Protocol::new();
        let mut backend = Backend::new();
        let mut channel = Channel::<NullBytestream, Vec<Value>>::new(NullBytestream);

        let startup_request = FrontendMessage::StartupMessage {
            protocol_version: 12345,
            user: Some(bytes_str("user_name")),
            database: Some(bytes_str("database_name")),
        };
        block_on(protocol.on_request(startup_request, &mut backend, &mut channel)).unwrap();

        // An attempt to describe a missing prepared statement triggers an error.
        let request = FrontendMessage::Describe {
            name: PreparedStatement(bytes_str("prepared_name_does_not_exist")),
        };
        assert!(block_on(protocol.on_request(request, &mut backend, &mut channel)).is_err());
    }

    #[test]
    fn describe_portal() {
        let mut protocol = Protocol::new();
        let mut backend = Backend::new();
        let mut channel = Channel::<NullBytestream, Vec<Value>>::new(NullBytestream);

        let startup_request = FrontendMessage::StartupMessage {
            protocol_version: 12345,
            user: Some(bytes_str("user_name")),
            database: Some(bytes_str("database_name")),
        };
        block_on(protocol.on_request(startup_request, &mut backend, &mut channel)).unwrap();

        let parse_request = FrontendMessage::Parse {
            prepared_statement_name: bytes_str("prepared1"),
            query: bytes_str("SELECT * FROM test WHERE x = $1 AND y = $2;"),
            parameter_data_types: vec![],
        };
        block_on(protocol.on_request(parse_request, &mut backend, &mut channel)).unwrap();
        assert!(protocol.prepared_statements.get("prepared1").is_some());

        let bind_request = FrontendMessage::Bind {
            prepared_statement_name: bytes_str("prepared1"),
            portal_name: bytes_str("portal1"),
            params: vec![DataValue::Double(0.8887), DataValue::Int(45678)],
            result_transfer_formats: vec![TransferFormat::Text, TransferFormat::Binary],
        };
        assert_eq!(
            block_on(protocol.on_request(bind_request, &mut backend, &mut channel)).unwrap(),
            Response::Message(BindComplete)
        );

        // A portal describe request generates a suitable description.
        let request = FrontendMessage::Describe {
            name: Portal(bytes_str("portal1")),
        };
        assert_eq!(
            block_on(protocol.on_request(request, &mut backend, &mut channel)).unwrap(),
            Response::Message(RowDescription {
                field_descriptions: vec![
                    FieldDescription {
                        field_name: "col1".to_string(),
                        table_id: UNKNOWN_TABLE,
                        col_id: UNKNOWN_COLUMN,
                        data_type: Type::INT4,
                        data_type_size: TYPLEN_4,
                        type_modifier: ATTTYPMOD_NONE,
                        transfer_format: TransferFormat::Text
                    },
                    FieldDescription {
                        field_name: "col2".to_string(),
                        table_id: UNKNOWN_TABLE,
                        col_id: UNKNOWN_COLUMN,
                        data_type: Type::FLOAT8,
                        data_type_size: TYPLEN_8,
                        type_modifier: ATTTYPMOD_NONE,
                        transfer_format: TransferFormat::Binary
                    },
                ],
            })
        );
    }

    #[test]
    fn describe_missing_portal() {
        let mut protocol = Protocol::new();
        let mut backend = Backend::new();
        let mut channel = Channel::<NullBytestream, Vec<Value>>::new(NullBytestream);

        let startup_request = FrontendMessage::StartupMessage {
            protocol_version: 12345,
            user: Some(bytes_str("user_name")),
            database: Some(bytes_str("database_name")),
        };
        block_on(protocol.on_request(startup_request, &mut backend, &mut channel)).unwrap();

        // An attempt to describe a missing portal triggers an error.
        let request = FrontendMessage::Describe {
            name: Portal(bytes_str("portal_name_does_not_exist")),
        };
        assert!(block_on(protocol.on_request(request, &mut backend, &mut channel)).is_err());
    }

    #[test]
    fn execute_read() {
        let mut protocol = Protocol::new();
        let mut backend = Backend::new();
        let mut channel = Channel::<NullBytestream, Vec<Value>>::new(NullBytestream);

        let startup_request = FrontendMessage::StartupMessage {
            protocol_version: 12345,
            user: Some(bytes_str("user_name")),
            database: Some(bytes_str("database_name")),
        };
        block_on(protocol.on_request(startup_request, &mut backend, &mut channel)).unwrap();

        let parse_request = FrontendMessage::Parse {
            prepared_statement_name: bytes_str("prepared1"),
            query: bytes_str("SELECT * FROM test WHERE x = $1 AND y = $2;"),
            parameter_data_types: vec![],
        };
        block_on(protocol.on_request(parse_request, &mut backend, &mut channel)).unwrap();
        assert!(protocol.prepared_statements.get("prepared1").is_some());

        let bind_request = FrontendMessage::Bind {
            prepared_statement_name: bytes_str("prepared1"),
            portal_name: bytes_str("portal1"),
            params: vec![DataValue::Double(0.8887), DataValue::Int(45678)],
            result_transfer_formats: vec![TransferFormat::Text, TransferFormat::Binary],
        };
        assert_eq!(
            block_on(protocol.on_request(bind_request, &mut backend, &mut channel)).unwrap(),
            Response::Message(BindComplete)
        );

        // A portal execute request returns the correct results from the backend.
        let request = FrontendMessage::Execute {
            portal_name: bytes_str("portal1"),
            limit: 0,
        };
        assert_eq!(
            block_on(protocol.on_request(request, &mut backend, &mut channel)).unwrap(),
            Response::Select {
                header: None,
                resultset: vec![
                    vec![Value(DataValue::Int(88)), Value(DataValue::Double(0.123))],
                    vec![Value(DataValue::Int(22)), Value(DataValue::Double(0.456))]
                ],
                result_transfer_formats: Some(Arc::new(vec![
                    TransferFormat::Text,
                    TransferFormat::Binary
                ])),
                trailer: None
            }
        );
        assert_eq!(backend.last_execute_id.unwrap(), 1);
        assert_eq!(
            backend.last_execute_params.unwrap(),
            vec![DataValue::Double(0.8887), DataValue::Int(45678)]
        );
    }

    #[test]
    fn execute_error() {
        let mut protocol = Protocol::new();
        let mut backend = Backend::new();
        backend.is_query_err = true;
        let mut channel = Channel::<NullBytestream, Vec<Value>>::new(NullBytestream);

        let startup_request = FrontendMessage::StartupMessage {
            protocol_version: 12345,
            user: Some(bytes_str("user_name")),
            database: Some(bytes_str("database_name")),
        };
        block_on(protocol.on_request(startup_request, &mut backend, &mut channel)).unwrap();

        let parse_request = FrontendMessage::Parse {
            prepared_statement_name: bytes_str("prepared1"),
            query: bytes_str("SELECT * FROM test WHERE x = $1 AND y = $2;"),
            parameter_data_types: vec![],
        };
        block_on(protocol.on_request(parse_request, &mut backend, &mut channel)).unwrap();
        assert!(protocol.prepared_statements.get("prepared1").is_some());

        let bind_request = FrontendMessage::Bind {
            prepared_statement_name: bytes_str("prepared1"),
            portal_name: bytes_str("portal1"),
            params: vec![DataValue::Double(0.8887), DataValue::Int(45678)],
            result_transfer_formats: vec![TransferFormat::Text, TransferFormat::Binary],
        };
        assert_eq!(
            block_on(protocol.on_request(bind_request, &mut backend, &mut channel)).unwrap(),
            Response::Message(BindComplete)
        );

        // An `Error` is returned when the backend returns an error.
        let request = FrontendMessage::Execute {
            portal_name: bytes_str("portal1"),
            limit: 0,
        };
        assert!(block_on(protocol.on_request(request, &mut backend, &mut channel)).is_err());
    }

    #[test]
    fn execute_write() {
        let mut protocol = Protocol::new();
        let mut backend = Backend::new();
        backend.is_query_read = false;
        let mut channel = Channel::<NullBytestream, Vec<Value>>::new(NullBytestream);

        let startup_request = FrontendMessage::StartupMessage {
            protocol_version: 12345,
            user: Some(bytes_str("user_name")),
            database: Some(bytes_str("database_name")),
        };
        block_on(protocol.on_request(startup_request, &mut backend, &mut channel)).unwrap();

        let parse_request = FrontendMessage::Parse {
            prepared_statement_name: bytes_str("prepared1"),
            query: bytes_str("SELECT * FROM test WHERE x = $1 AND y = $2;"),
            parameter_data_types: vec![],
        };
        block_on(protocol.on_request(parse_request, &mut backend, &mut channel)).unwrap();
        assert!(protocol.prepared_statements.get("prepared1").is_some());

        let bind_request = FrontendMessage::Bind {
            prepared_statement_name: bytes_str("prepared1"),
            portal_name: bytes_str("portal1"),
            params: vec![DataValue::Double(0.8887), DataValue::Int(45678)],
            result_transfer_formats: vec![TransferFormat::Text, TransferFormat::Binary],
        };
        assert_eq!(
            block_on(protocol.on_request(bind_request, &mut backend, &mut channel)).unwrap(),
            Response::Message(BindComplete)
        );

        // A write portal is passed to the backend correctly and a suitable result is returned.
        let request = FrontendMessage::Execute {
            portal_name: bytes_str("portal1"),
            limit: 0,
        };
        assert_eq!(
            block_on(protocol.on_request(request, &mut backend, &mut channel)).unwrap(),
            Response::Message(CommandComplete {
                tag: CommandCompleteTag::Delete(5)
            })
        );
        assert_eq!(backend.last_execute_id.unwrap(), 1);
        assert_eq!(
            backend.last_execute_params.unwrap(),
            vec![DataValue::Double(0.8887), DataValue::Int(45678)]
        );
    }

    #[test]
    fn on_error_starting_up() {
        let mut protocol = Protocol::new();
        assert_eq!(
            block_on(
                protocol.on_error::<Backend>(Error::InternalError("error requested".to_string()))
            )
            .unwrap(),
            Response::Message(ErrorResponse {
                severity: ErrorSeverity::Error,
                sqlstate: SqlState::INTERNAL_ERROR,
                message: "internal error: error requested".to_string()
            })
        );
    }

    #[test]
    fn on_error_after_starting_up() {
        let mut protocol = Protocol::new();
        protocol.is_starting_up = false;
        assert_eq!(
            block_on(
                protocol.on_error::<Backend>(Error::InternalError("error requested".to_string()))
            )
            .unwrap(),
            Response::Message2(
                ErrorResponse {
                    severity: ErrorSeverity::Error,
                    sqlstate: SqlState::INTERNAL_ERROR,
                    message: "internal error: error requested".to_string()
                },
                BackendMessage::ready_for_query_idle()
            )
        );
    }
}
