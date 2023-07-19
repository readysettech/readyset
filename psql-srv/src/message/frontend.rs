use std::fmt;

use bytes::Bytes;
use postgres_types::Type;

use crate::bytes::BytesStr;
use crate::message::TransferFormat;
use crate::value::PsqlValue;

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
    Authenticate {
        /// The message itself doesn't give us enough context to know which *kind* of authenticate
        /// response this is - we have to interpret it according to the context
        body: Bytes,
    },
    Bind {
        portal_name: BytesStr,
        prepared_statement_name: BytesStr,
        params: Vec<PsqlValue>,
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
    SaslResponse {
        scram_data: Bytes,
    },
    Sync,
    Flush,
    Terminate,
}

#[derive(Debug, PartialEq, Eq)]
pub enum StatementName {
    Portal(BytesStr),
    PreparedStatement(BytesStr),
}

impl fmt::Display for FrontendMessage {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Authenticate { .. } => write!(f, "Authenticate"),
            Self::Bind { .. } => write!(f, "Bind"),
            Self::Close { .. } => write!(f, "Close"),
            Self::Describe { .. } => write!(f, "Describe"),
            Self::Execute { .. } => write!(f, "Execute"),
            Self::Parse { .. } => write!(f, "Parse"),
            Self::Query { .. } => write!(f, "Query"),
            Self::SSLRequest => write!(f, "SSLRequest"),
            Self::StartupMessage { .. } => write!(f, "StartupMessage"),
            Self::SaslResponse { .. } => write!(f, "SASLResponse"),
            Self::Sync => write!(f, "Sync"),
            Self::Flush => write!(f, "Flush"),
            Self::Terminate => write!(f, "Terminate"),
        }
    }
}

impl FrontendMessage {
    /// Indicates if this message must be flushed to the socket after processing.
    /// By not immediately flushing, callers can optimize when flush is invoked;
    /// that is, they can defer flush until a later signal.
    ///
    /// For example, when processing an extended query protocol interaction, only
    /// `Sync` needs to actually flush, earlier messages in the chain can be buffered.
    pub(crate) fn requires_flush(&self) -> bool {
        !matches!(
            self,
            FrontendMessage::Parse { .. }
                | FrontendMessage::Bind { .. }
                | FrontendMessage::Describe { .. }
                | FrontendMessage::Close { .. }
                | FrontendMessage::Execute { .. }
        )
    }
}

/// Parsed body for the SASLInitialResponse message
///
/// This is its own type because it can't be parsed during regular message parsing without context
/// about which kind of authentication we're doing.
pub struct SaslInitialResponse {
    pub authentication_mechanism: BytesStr,
    pub scram_data: Bytes,
}
