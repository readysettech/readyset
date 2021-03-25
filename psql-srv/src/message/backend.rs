use crate::error::Error;
use crate::message::TransferFormat;
use crate::r#type::Type;
use crate::value::Value;
use std::convert::TryInto;
use std::sync::Arc;

pub use postgres::error::SqlState;

const READY_FOR_QUERY_IDLE: u8 = b'I';
const SSL_RESPONSE_N: u8 = b'N';

/// A message to be sent by a Postgresql backend (server). The different types of backend messages,
/// and the fields they contain, are described in the
/// [Postgresql frontend/backend protocol documentation][documentation].
///
/// [documentation]: https://www.postgresql.org/docs/current/protocol-message-formats.html
///
/// # Type Parameters
///
/// * `R` - Represents a row of data values. `BackendMessage` implementations are provided wherein
///         a value of type `R` will, upon iteration, emit values that are convertable into type
///         `Value`, which can be serialized along with the rest of the `BackendMessage`.
#[derive(Debug)]
pub enum BackendMessage<R> {
    AuthenticationOk,
    BindComplete,
    CloseComplete,
    CommandComplete {
        tag: CommandCompleteTag,
    },
    DataRow {
        values: R,
        explicit_transfer_formats: Option<Arc<Vec<TransferFormat>>>,
    },
    ErrorResponse {
        severity: ErrorSeverity,
        sqlstate: SqlState,
        message: String,
    },
    ParameterDescription {
        parameter_data_types: Vec<Type>,
    },
    ParseComplete,
    ReadyForQuery {
        status: u8,
    },
    RowDescription {
        field_descriptions: Vec<FieldDescription>,
    },
    SSLResponse {
        byte: u8,
    },
}

impl<R: IntoIterator<Item: TryInto<Value, Error = Error>>> BackendMessage<R> {
    pub fn ready_for_query_idle() -> BackendMessage<R> {
        BackendMessage::ReadyForQuery {
            status: READY_FOR_QUERY_IDLE,
        }
    }

    pub fn ssl_response_n() -> BackendMessage<R> {
        BackendMessage::SSLResponse {
            byte: SSL_RESPONSE_N,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub enum CommandCompleteTag {
    Delete(u64),
    Empty,
    Insert(u64),
    Select(u64),
    Update(u64),
}

#[allow(dead_code)]
#[derive(Clone, Copy, Debug)]
pub enum ErrorSeverity {
    Error,
    Fatal,
    Panic,
}

#[derive(Debug)]
pub struct FieldDescription {
    pub field_name: String,
    pub table_id: i32,
    pub col_id: i16,
    pub data_type: Type,
    pub data_type_size: i16,
    pub type_modifier: i32,
    pub transfer_format: TransferFormat,
}
