use std::sync::Arc;

use bytes::Bytes;
use nom_sql::SqlIdentifier;
use postgres::error::ErrorPosition;
pub use postgres::error::SqlState;
use postgres::{Row, SimpleQueryRow};
use postgres_types::Type;
use tokio_postgres::{OwnedField, SimpleQueryMessage};

use crate::message::TransferFormat;
use crate::value::PsqlValue;

/// Idle (not in a transaction block)
pub(crate) const READY_FOR_QUERY_IDLE: u8 = b'I';
/// In a transaction block
pub(crate) const READY_FOR_QUERY_TX: u8 = b'T';
/// In a failed transaction block (queries will be rejected until block is ended)
pub(crate) const READY_FOR_QUERY_FAILED_TX: u8 = b'E';

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
#[derive(Debug)]
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
    NoData,
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
    PassThroughSimpleRow(SimpleQueryRow),
    PassThroughDataRow(Row),
    SSLResponse {
        byte: u8,
    },
}

/// The state of connection wrt a transaction, as will be reported in the
/// ReadyForQuery backend message [0].
///
/// [0] https://www.postgresql.org/docs/current/protocol-message-formats.html
pub(crate) enum TransactionState {
    NotInTransaction,
    InTransactionOk,
    InTransactionError,
}

impl BackendMessage {
    pub fn ready_for_query(state: TransactionState) -> BackendMessage {
        let status = match state {
            TransactionState::NotInTransaction => READY_FOR_QUERY_IDLE,
            TransactionState::InTransactionOk => READY_FOR_QUERY_TX,
            TransactionState::InTransactionError => READY_FOR_QUERY_FAILED_TX,
        };

        BackendMessage::ReadyForQuery { status }
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
pub enum DeallocationType {
    Single,
    All,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CommandCompleteTag {
    Delete(u64),
    Insert(u64),
    Select(u64),
    Update(u64),
    /// The bool field indicates if all prepared statements were deallocated.
    Deallocate(DeallocationType),
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
    pub field_name: SqlIdentifier,
    pub table_id: u32,
    pub col_id: i16,
    pub data_type: Type,
    pub data_type_size: i16,
    pub type_modifier: i32,
    pub transfer_format: TransferFormat,
}

#[derive(Debug)]
pub enum PsqlSrvRow {
    SimpleQueryMessage(SimpleQueryMessage),
    RawRow(Row),
    ValueVec(Vec<PsqlValue>),
}

impl From<Vec<PsqlValue>> for PsqlSrvRow {
    fn from(value: Vec<PsqlValue>) -> Self {
        Self::ValueVec(value)
    }
}

impl From<Row> for PsqlSrvRow {
    fn from(value: Row) -> Self {
        Self::RawRow(value)
    }
}
impl From<SimpleQueryRow> for PsqlSrvRow {
    fn from(value: SimpleQueryRow) -> Self {
        Self::SimpleQueryMessage(SimpleQueryMessage::Row(value))
    }
}
