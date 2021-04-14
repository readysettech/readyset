//! Error handling, definitions, and utilities

use crate::channel::tcp::SendError;
use crate::ValueCoerceError;
use petgraph::graph::NodeIndex;
use std::error::Error;
use thiserror::Error;

/// Wraps a boxed `std::error::Error` to make it implement, um, `std::error::Error`.
/// Yes, I'm as disappointed as you are.
#[repr(transparent)]
pub struct BoxedErrorWrapper(pub Box<dyn std::error::Error + Send + Sync + 'static>);

impl std::fmt::Display for BoxedErrorWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

impl std::fmt::Debug for BoxedErrorWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&self.0, f)
    }
}

impl std::error::Error for BoxedErrorWrapper {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0.source()
    }
}

/// Wrap a boxed `std::error::Error` in a nice warm `anyhow::Error` blanket.
pub fn wrap_boxed_error(
    boxed: Box<dyn std::error::Error + Send + Sync + 'static>,
) -> anyhow::Error {
    anyhow::Error::new(BoxedErrorWrapper(boxed))
}

/// General error type to be used across all of the ReadySet codebase.
#[derive(Serialize, Deserialize, Error, Debug)]
pub enum ReadySetError {
    /// An intra-ReadySet RPC call failed.
    #[error("Error during RPC ({during}): {source}")]
    RpcFailed {
        /// A textual description of the nature of the RPC call that failed.
        during: String,
        /// The error returned by the failed call.
        source: Box<ReadySetError>,
    },

    /// A SQL SELECT query couldn't be created.
    #[error("SQL SELECT query '{qname}' couldn't be added: {source}")]
    SelectQueryCreationFailed {
        /// The query name (identifier) of the query that couldn't be added.
        qname: String,
        /// The error encountered while adding the query.
        source: Box<ReadySetError>,
    },

    /// A MIR node couldn't be created.
    #[error("MIR node '{name}' (vers {from_version}) couldn't be made: {source}")]
    MirNodeCreationFailed {
        /// The name of the MIR node that couldn't be made.
        name: String,
        /// The recipe version the MIR node is from.
        from_version: usize,
        /// The error encountered while adding the MIR node.
        source: Box<ReadySetError>,
    },

    /// A connection to Apache ZooKeeper failed.
    #[error("Failed to connect to ZooKeeper at '{connect_string}': {reason}")]
    ZookeeperConnectionFailed {
        /// The connection string used to connect to ZooKeeper.
        connect_string: String,
        /// A textual reason why the connection failed.
        reason: String,
    },

    /// An error occurred while sending on a TCP socket.
    #[error("TCP send error: {0}")]
    TcpSendError(String),

    /// Serializing/deserializing the result of an intra-ReadySet RPC call failed.
    ///
    /// This is created by the [`From`] impl on [`serde_json::error::Error`].
    #[error("Failed to (de)serialize: {0}")]
    SerializationFailed(String),

    /// The wrong number of columns was given when inserting a row.
    #[error("wrong number of columns specified: expected {0}, got {1}")]
    WrongColumnCount(usize, usize),

    /// The wrong number of key columns was given when modifying a row.
    #[error("wrong number of key columns used: expected {0}, got {1}")]
    WrongKeyColumnCount(usize, usize),

    /// A table operation was passed an incorrect packet data type.
    #[error("wrong packet data type")]
    WrongPacketDataType,

    /// A NOT NULL column was set to [`DataType::None`].
    #[error("Attempted to set NOT NULL column '{col}' to DataType::None")]
    NonNullable {
        /// The column in question.
        col: String,
    },

    /// A column is declared NOT NULL, but was not provided (and has no default).
    #[error("Column '{col}' is declared NOT NULL, has no default, and was not provided")]
    ColumnRequired {
        /// The column in question.
        col: String,
    },

    /// A table couldn't be found.
    ///
    /// FIXME(eta): this is currently slightly overloaded in meaning.
    #[error("Could not find table '{0}'")]
    TableNotFound(String),

    /// A view is not yet available.
    #[error("view not yet available")]
    ViewNotYetAvailable,

    /// A view couldn't be found.
    #[error("Could not find view '{0}'")]
    ViewNotFound(String),

    /// The query specified an empty lookup key.
    #[error("the query specified an empty lookup key")]
    EmptyKey,

    /// A prepared statement is missing.
    #[error("a prepared statement is missing")]
    PreparedStatementMissing,

    /// An internal invariant has been violated.
    ///
    /// This is produced by the [`internal!`] and [`invariant!`] macros, as an alternative to
    /// panicking the whole database.
    /// It should **not** be used for errors we're expecting to be able to handle; this is
    /// a worst-case scenario.
    #[error("Internal invariant violated: {0}")]
    Internal(String),

    /// The user fed ReadySet bad input (and there's no more specific error).
    #[error("Bad request: {0}")]
    BadRequest(String),

    /// An operation isn't supported by ReadySet yet, but might be in the future.
    ///
    /// This is produced by the [`unsupported!`] macro.
    #[error("Operation unsupported: {0}")]
    Unsupported(String),

    /// The query provided by the user could not be parsed by `nom-sql`.
    ///
    /// TODO(eta): extend nom-sql to be able to provide more granular parse failure information.
    #[error("Query failed to parse: {query}")]
    UnparseableQuery {
        /// The SQL of the query.
        query: String,
    },

    /// Manipulating a base table failed.
    #[error("Failed to manipulate table {name}: {source}")]
    TableError {
        /// The name of the base table being manipulated.
        name: String,
        /// The underlying error that occurred while manipulating the table.
        source: Box<ReadySetError>,
    },

    /// Manipulating a view failed.
    #[error("Failed to manipulate view at {:?}: {source}")]
    ViewError {
        /// The index of the view being manipulated.
        idx: NodeIndex,
        /// The underlying error that occurred while manipulating the view.
        source: Box<ReadySetError>,
    },

    /// A `ValueCoerceError` that really shouldn't be a separate type.
    ///
    /// FIXME(eta): make this not be a separate type.
    #[error(transparent)]
    ValueCoerce(ValueCoerceError),

    /// Multiple `AUTO_INCREMENT` columns were provided, which isn't allowed.
    #[error("Multiple auto incrementing columns are not permitted")]
    MultipleAutoIncrement,

    /// A column couldn't be found.
    #[error("Column {0} not found in table or view")]
    NoSuchColumn(String),
}

impl ReadySetError {
    /// Returns `true` if the error is an [`UnparseableQuery`].
    pub fn is_unparseable_query(&self) -> bool {
        matches!(self, Self::UnparseableQuery { .. })
    }

    /// Returns true if the error either *is* [`UnparseableQuery`], or was *caused by*
    /// [`UnparseableQuery`]
    pub fn caused_by_unparseable_query(&self) -> bool {
        // TODO(grfn): Once https://github.com/rust-lang/rust/issues/58520 stabilizes, this can be
        // rewritten to use that
        self.is_unparseable_query()
            || self
                .source()
                .and_then(|e| e.downcast_ref::<Box<ReadySetError>>())
                .iter()
                .any(|e| e.is_unparseable_query())
    }
}

/// Make a new [`ReadySetError::Internal`] with the provided string-able argument.
pub fn internal_err<T: Into<String>>(err: T) -> ReadySetError {
    ReadySetError::Internal(err.into())
}

/// Make a new [`ReadySetError::Unsupported`] with the provided string-able argument.
pub fn unsupported_err<T: Into<String>>(err: T) -> ReadySetError {
    ReadySetError::Unsupported(err.into())
}

/// Make a new [`ReadySetError::BadRequest`] with the provided string-able argument.
pub fn bad_request_err<T: Into<String>>(err: T) -> ReadySetError {
    ReadySetError::BadRequest(err.into())
}

/// Renders information about the current source location *if* building in debug mode, for use in
/// error-generating macros
#[doc(hidden)]
#[macro_export]
macro_rules! __location_info {
    () => {
        $crate::__location_info!(" (in {})")
    };
    ($fstr: literal) => {
        if cfg!(debug) {
            format!(
                $fstr,
                format!("{}:{}:{}", std::file!(), std::line!(), std::column!(),)
            )
        } else {
            "".to_owned()
        }
    };
}

/// Return a [`ReadySetError::Internal`] from the current function.
///
/// Usage is like [`panic!`], in that you can pass a format string and arguments.  When building in
/// debug mode, the returned error also captures file, line, and column information for further
/// debugging purposes.
///
/// When called with no arguments, generates an internal error with the text
/// "entered unreachable code".
#[macro_export]
macro_rules! internal {
    () => {
        internal!("entered unreachable code")
    };
    ($($tt:tt)*) => {
        return Err($crate::errors::internal_err(format!(
            "{}{}",
            $crate::__location_info!("in {}: "),
            format_args!($($tt)*)
        )).into());
    };
}

/// Return a [`ReadySetError::Unsupported`] from the current function.
///
/// Usage is like [`panic!`], in that you can pass a format string and arguments.
/// When building in debug mode, the returned error also captures file, line, and column information
/// for further debugging purposes.
///
/// When called with no arguments, generates an internal error with the text
/// "operation not implemented yet".
#[macro_export]
macro_rules! unsupported {
    () => {
        unsupported!("operation not implemented yet")
    };
    ($($tt:tt)*) => {
        return Err($crate::errors::unsupported_err(format!(
            "{}{}",
            format_args!($($tt)*),
            $crate::__location_info!()
        )).into());
    };
}

/// Return a [`ReadySetError::Internal`] from the current function, if and only if
/// the argument evaluates to false.
///
/// This is intended to be used wherever [`assert!`] was used previously.
#[macro_export]
macro_rules! invariant {
    ($expr:expr, $($tt:tt)*) => {
        if !$expr {
            $crate::internal!($($tt)*);
        }
    };
    ($expr:expr) => {
        if !$expr {
            $crate::internal!("assertion failed: {}", std::stringify!($expr));
        }
    };
}

/// Return a [`ReadySetError::Internal`] from the current function, if and only if
/// the two arguments aren't equal.
///
/// This is intended to be used wherever [`assert_eq!`] was used previously.
#[macro_export]
macro_rules! invariant_eq {
    ($expr:expr, $expr2:expr, $($tt:tt)*) => {
        if $expr != $expr2 {
            $crate::internal!(
                "assertion failed: {} == {} ({});\nleft = {:?};\nright = {:?}",
                std::stringify!($expr),
                std::stringify!($expr2),
                format_args!($($tt)*),
                $expr,
                $expr2
            )
        }
    };
    ($expr:expr, $expr2:expr) => {
        if $expr != $expr2 {
            $crate::internal!(
                "assertion failed: {} == {};\nleft = {:?};\nright = {:?}",
                std::stringify!($expr),
                std::stringify!($expr2),
                $expr,
                $expr2
            )
        }
    };
}

/// Standard issue [`Result`] alias.
pub type ReadySetResult<T> = ::std::result::Result<T, ReadySetError>;

/// Make a new [`ReadySetError::RpcFailed`] with the provided string-able `during` value
/// and the provided `err` as cause.
pub fn rpc_err_no_downcast<T: Into<String>>(during: T, err: ReadySetError) -> ReadySetError {
    ReadySetError::RpcFailed {
        during: during.into(),
        source: Box::new(err),
    }
}

/// Make a new [`ReadySetError::RpcFailed`] with the provided string-able `during` value
/// and the provided `err` as cause.
///
/// This attempts to downcast the `err` into a `Box<ReadySetError>`. If the downcasting
/// fails, the error is formatted as a [`ReadySetError::Internal`].
pub fn rpc_err<T: Into<String>>(during: T, err: Box<dyn std::error::Error>) -> ReadySetError {
    // TODO(eta): this downcast WILL always fail, because I haven't really had a chance to
    // unravel the complete madness that is `tokio_tower` yet.
    let rse: Box<ReadySetError> = err
        .downcast()
        .unwrap_or_else(|e| Box::new(internal_err(format!("failed to downcast: {}", e))));
    ReadySetError::RpcFailed {
        during: during.into(),
        source: rse,
    }
}

/// Make a new [`ReadySetError::ViewError`] with the provided `idx` and `err` values.
pub fn view_err<A: Into<NodeIndex>, B: Into<ReadySetError>>(idx: A, err: B) -> ReadySetError {
    ReadySetError::ViewError {
        idx: idx.into(),
        source: Box::new(err.into()),
    }
}

/// Make a new `ReadySetError::TableError` with the provided `name` and `err` values.
pub fn table_err<A: Into<String>, B: Into<ReadySetError>>(name: A, err: B) -> ReadySetError {
    ReadySetError::TableError {
        name: name.into(),
        source: Box::new(err.into()),
    }
}

/// Generates a closure, suitable as an argument to `.map_err()`, that maps the provided error
/// argument into a [`ReadySetError::RpcFailed`] with the given `during` argument (anything
/// that implements `Display`).
///
/// When building in debug mode, the `during` argument generated also captures file, line, and
/// column information for further debugging purposes.
///
/// # Example
///
/// ```ignore
/// let rpc_result = do_rpc_call()
///     .map_err(rpc_err!("do_rpc_call"));
/// ```
#[macro_export]
macro_rules! rpc_err {
    ($during:expr) => {
        |e| $crate::errors::rpc_err(format!("{}{}", $during, $crate::__location_info!()), e)
    };
}

/// HACK(eta): this From impl just stringifies the error, so that `ReadySetError` can be serialized
/// and deserialized.
impl From<serde_json::error::Error> for ReadySetError {
    fn from(e: serde_json::error::Error) -> ReadySetError {
        ReadySetError::SerializationFailed(e.to_string())
    }
}

/// HACK(eta): this From impl just stringifies the error, so that `ReadySetError` can be serialized
/// and deserialized.
impl From<SendError> for ReadySetError {
    fn from(e: SendError) -> ReadySetError {
        ReadySetError::TcpSendError(e.to_string())
    }
}
