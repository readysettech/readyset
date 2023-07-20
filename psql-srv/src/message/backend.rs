use std::sync::Arc;

use bytes::Bytes;
use postgres::error::ErrorPosition;
pub use postgres::error::SqlState;
use postgres::SimpleQueryRow;
use postgres_types::Type;
use tokio_postgres::OwnedField;

use crate::message::TransferFormat;
use crate::value::PsqlValue;

const READY_FOR_QUERY_IDLE: u8 = b'I';
const SSL_RESPONSE_UNWILLING: u8 = b'N';
const SSL_RESPONSE_WILLING: u8 = b'S';

/// A message to be sent by a Postgresql backend (server). The different types of backend messages,
/// and the fields they contain, are described in the
/// [Postgresql frontend/backend protocol documentation][documentation].
///
/// [documentation]: https://www.postgresql.org/docs/current/protocol-message-formats.html
///
/// # Type Parameters
///
/// * `R` - Represents a row of data values. `BackendMessage` implementations are provided wherein a
///   value of type `R` will, upon iteration, emit values that are convertible into type
///   `PsqlValue`, which can be serialized along with the rest of the `BackendMessage`.
#[derive(Debug, PartialEq)]
#[allow(clippy::large_enum_variant)] // TODO: benchmark if this matters
pub enum BackendMessage {
    AuthenticationCleartextPassword,
    AuthenticationSasl {
        allow_channel_binding: bool,
    },
    AuthenticationSaslContinue {
        sasl_data: Bytes,
    },
    AuthenticationSaslFinal {
        sasl_data: Bytes,
    },
    AuthenticationOk,
    BindComplete,
    CloseComplete,
    CommandComplete {
        tag: CommandCompleteTag,
    },
    PassThroughCommandComplete(Bytes),
    DataRow {
        values: Vec<PsqlValue>,
        explicit_transfer_formats: Option<Arc<Vec<TransferFormat>>>,
    },
    ErrorResponse {
        severity: ErrorSeverity,
        sqlstate: SqlState,
        message: String,
        detail: Option<String>,
        hint: Option<String>,
        position: Option<ErrorPosition>,
        where_: Option<String>,
        schema: Option<String>,
        table: Option<String>,
        column: Option<String>,
        datatype: Option<String>,
        constraint: Option<String>,
        file: Option<String>,
        line: Option<u32>,
        routine: Option<String>,
    },
    ParameterDescription {
        parameter_data_types: Vec<Type>,
    },
    ParameterStatus {
        parameter_name: String,
        parameter_value: String,
    },
    ParseComplete,
    ReadyForQuery {
        status: u8,
    },
    RowDescription {
        field_descriptions: Vec<FieldDescription>,
    },
    PassThroughRowDescription(Vec<OwnedField>),
    PassThroughDataRow(SimpleQueryRow),
    SSLResponse {
        byte: u8,
    },
}

impl BackendMessage {
    pub fn ready_for_query_idle() -> BackendMessage {
        BackendMessage::ReadyForQuery {
            status: READY_FOR_QUERY_IDLE,
        }
    }

    pub fn ssl_response_unwilling() -> BackendMessage {
        BackendMessage::SSLResponse {
            byte: SSL_RESPONSE_UNWILLING,
        }
    }

    pub fn ssl_response_willing() -> BackendMessage {
        BackendMessage::SSLResponse {
            byte: SSL_RESPONSE_WILLING,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CommandCompleteTag {
    Delete(u64),
    Empty,
    Insert(u64),
    Select(u64),
    Update(u64),
}

#[allow(dead_code)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErrorSeverity {
    Error,
    Fatal,
    Panic,
}

#[derive(Debug, PartialEq, Eq)]
pub struct FieldDescription {
    pub field_name: String,
    pub table_id: i32,
    pub col_id: i16,
    pub data_type: Type,
    pub data_type_size: i16,
    pub type_modifier: i32,
    pub transfer_format: TransferFormat,
}
