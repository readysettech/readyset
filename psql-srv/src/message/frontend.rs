use postgres_types::Type;

use crate::bytes::BytesStr;
use crate::message::TransferFormat;
use crate::value::Value;
use std::fmt;

/// A message received from a Postgresql frontend (client). The different types of frontend
/// messages, and the fields they contain, are described in the
/// [Postgresql frontend/backend protocol documentation][documentation].
///
/// [documentation]: https://www.postgresql.org/docs/current/protocol-message-formats.html
///
/// # Lifecycle
///
/// A `FrontendMessage` may contain [`Bytes`] views of binary data that has been read from a binary
/// channel into a data buffer. `FrontendMessage` values should be dropped eagerly (once no longer
/// needed) to facilitate reclamation of this data buffer for subsequent reads.
///
/// [`Bytes`]: https://docs.rs/bytes/0.5.6/bytes/struct.Bytes.html
#[derive(Debug, PartialEq)]
pub enum FrontendMessage {
    Bind {
        portal_name: BytesStr,
        prepared_statement_name: BytesStr,
        params: Vec<Value>,
        result_transfer_formats: Vec<TransferFormat>,
    },
    Close {
        name: StatementName,
    },
    Describe {
        name: StatementName,
    },
    Execute {
        portal_name: BytesStr,
        limit: i32,
    },
    Parse {
        prepared_statement_name: BytesStr,
        query: BytesStr,
        parameter_data_types: Vec<Type>,
    },
    Query {
        query: BytesStr,
    },
    SSLRequest,
    StartupMessage {
        protocol_version: i32,
        user: Option<BytesStr>,
        database: Option<BytesStr>,
    },
    Sync,
    Terminate,
}

#[derive(Debug, PartialEq)]
pub enum StatementName {
    Portal(BytesStr),
    PreparedStatement(BytesStr),
}

impl fmt::Display for FrontendMessage {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Bind { .. } => write!(f, "Bind"),
            Self::Close { .. } => write!(f, "Close"),
            Self::Describe { .. } => write!(f, "Describe"),
            Self::Execute { .. } => write!(f, "Execute"),
            Self::Parse { .. } => write!(f, "Parse"),
            Self::Query { .. } => write!(f, "Query"),
            Self::SSLRequest => write!(f, "SSLRequest"),
            Self::StartupMessage { .. } => write!(f, "StartupMessage"),
            Self::Sync => write!(f, "Sync"),
            Self::Terminate => write!(f, "Terminate"),
        }
    }
}
