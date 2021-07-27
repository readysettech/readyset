//! Error handling, definitions, and utilities

use crate::channel::tcp::SendError;
use crate::consensus::Epoch;
use crate::internal::LocalNodeIndex;
use derive_more::Display;
use petgraph::graph::NodeIndex;
use std::error::Error;
use std::io;
use thiserror::Error;
use url::Url;
use vec1::Size0Error;

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

/// An (inexhaustive, currently) enumeration of the types of Node in the graph, for use in
/// [`ReadySetError::InvalidNodeType`]
#[derive(Serialize, Deserialize, Debug, Display)]
pub enum NodeType {
    /// Egress nodes
    Egress,
    /// Reader nodes
    Reader,
    /// Sharder nodes
    Sharder,
}

/// General error type to be used across all of the ReadySet codebase.
#[derive(Serialize, Deserialize, Error, Debug)]
pub enum ReadySetError {
    /// Query fallback failed because noria_connector is not present. This error
    /// would indicate a bug if seen.
    #[error("Fallback failed because noria_connector is not present")]
    FallbackNoConnector,

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

    /// A migration failed during the planning stage. Nothing was touched.
    #[error("Failed to plan migration: {source}")]
    MigrationPlanFailed {
        /// The error encountered while planning the migration.
        source: Box<ReadySetError>,
    },

    /// A migration failed to apply. The dataflow graph may be in an inconsistent state.
    #[error("Failed to apply migration: {source}")]
    MigrationApplyFailed {
        /// The error encountered while applying the migration.
        source: Box<ReadySetError>,
    },

    /// A domain couldn't be booted on the remote worker.
    #[error("Failed to boot domain {domain_index}.{shard} on worker '{worker_uri}': {source}")]
    DomainCreationFailed {
        /// The index of the domain.
        domain_index: usize,
        /// The shard of the domain.
        shard: usize,
        /// The URI of the worker where the domain was to be placed.
        worker_uri: Url,
        /// The error encountered while trying to boot the domain.
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

    /// An expression appears in either the select list, HAVING condition, or ORDER BY list that is
    /// not either named in the GROUP BY clause or is functionally dependent on (uniquely determined
    /// by) GROUP BY columns.
    ///
    /// cf <https://dev.mysql.com/doc/refman/8.0/en/sql-mode.html#sqlmode_only_full_group_by>
    #[error(
        "Expression `{expression}` appears in {position} but is not functionally dependent on \
             columns in the GROUP BY clause"
    )]
    ExpressionNotInGroupBy {
        /// A string representation (via [`.to_string()`](ToString::to_string)) of the expression in
        /// question
        expression: String,
        /// A name for where the expression appears in the query (eg "ORDER BY")
        position: String,
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

    /// A view couldn't be found in the given pool of worker.
    #[error("Could not find view '{name}' in workers '{workers:?}'")]
    ViewNotFoundInWorkers {
        /// The name of the view that could not be found.
        name: String,
        /// The pool of workers where the view was attempted to be found.
        workers: Vec<Url>,
    },

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

    /// A user-provided SQL query referenced a function that does not exist
    #[error("Function {0} does not exist")]
    NoSuchFunction(String),

    /// A user-provided SQL query used the wrong number of arguments in a call to a built-in
    /// function
    #[error("Incorrect parameter count in the call to native function '{0}'")]
    ArityError(String),

    /// Multiple `AUTO_INCREMENT` columns were provided, which isn't allowed.
    #[error("Multiple auto incrementing columns are not permitted")]
    MultipleAutoIncrement,

    /// A column couldn't be found.
    #[error("Column {0} not found in table or view")]
    NoSuchColumn(String),

    /// Conversion to or from a [`DataType`](crate::DataType) failed.
    #[error("DataType conversion error: Failed to convert {val} of type {src_type} to the type {target_type}: {details}")]
    DataTypeConversionError {
        /// Source value.
        val: String,
        /// Source type.
        src_type: String,
        /// Target type.
        target_type: String,
        /// More details about the nature of the error.
        details: String,
    },

    /// Invalid index when evaluating a project expression.
    #[error("Column index out-of-bounds while evaluating project expression: index was {0}")]
    ProjectExpressionInvalidColumnIndex(usize),

    /// Error in built-in function of expression projection
    #[error("Error in project expression built-in function: {function}: {message}")]
    ProjectExpressionBuiltInFunctionError {
        /// The built-in function the error occured in.
        function: String,
        /// Details about the specific error.
        message: String,
    },

    /// Error parsing a string into a NativeDateTime.
    #[error("Error parsing a NativeDateTime from the string: {0}")]
    NaiveDateTimeParseError(String),

    /// Primary key is not on a primitive field.
    #[error("Primary key must be on a primitive field")]
    InvalidPrimaryKeyField,

    /// A worker operation couldn't be completed because the worker doesn't know where the
    /// controller is yet, or has lost track of it.
    #[error("Worker cannot find its controller")]
    LostController,

    /// An RPC request was made to a noria-server instance that isn't the leader.
    #[error("This instance is not the leader")]
    NotLeader,

    /// An RPC request was made to a controller that doesn't have quorum.
    #[error("A quorum of workers is not yet available")]
    NoQuorum,

    /// A request was made to an API endpoint not known to the controller.
    #[error("API endpoint not found")]
    UnknownEndpoint,

    /// A node index passed to the controller was invalid.
    #[error("Node {index} not found in controller")]
    NodeNotFound {
        /// The erroneous node index.
        index: usize,
    },

    /// An RPC operation couldn't be completed because the message epoch didn't match.
    #[error("Epoch mismatch: supplied {supplied:?}, but current is {current:?}")]
    EpochMismatch {
        /// The epoch supplied in the RPC message.
        supplied: Option<Epoch>,
        /// What the recipient thinks the current epoch is.
        current: Option<Epoch>,
    },

    /// A worker tried to check in with a heartbeat payload, but the controller is unaware of it.
    #[error("Unknown worker at {unknown_uri} tried to check in with heartbeat")]
    UnknownWorker {
        /// The URI of the worker that the controller didn't recognize.
        unknown_uri: Url,
    },

    /// A request for reader replication into a worker failed, because the worker URI provided
    /// could not be found in the list of registered workers.
    #[error("Could not find worker at {unknown_uri} for reader replication")]
    ReplicationUnknownWorker {
        /// The URI of the worker that could not be found.
        unknown_uri: Url,
    },

    /// An RPC request was attempted against a worker that has failed.
    #[error("Worker at {uri} failed")]
    WorkerFailed {
        /// The failed worker's URI.
        uri: Url,
    },

    /// Making a HTTP request failed.
    #[error("HTTP request failed: {0}")]
    HttpRequestFailed(String),

    /// A domain request was sent to a worker that doesn't have that domain.
    #[error("Could not find domain {domain_index}.{shard} on worker")]
    NoSuchDomain {
        /// The index of the domain.
        domain_index: usize,
        /// The shard.
        shard: usize,
    },

    /// A request referencing a node was sent to a domain not responsible for that node.
    #[error("Node {0:?} not found in domain")]
    NoSuchNode(LocalNodeIndex),

    /// An operation that is valid on only one type of node was made on a node that is not that type
    #[error("Node {node_index:?} is not of type {expected_type}")]
    InvalidNodeType {
        /// The index of the node in question
        node_index: LocalNodeIndex,
        /// The type of node that the operation is supported on
        expected_type: NodeType,
    },

    /// A request was sent to a domain with a nonexistent or unknown replay path
    #[error("Replay path identified by Tag({0}) not found")]
    NoSuchReplayPath(u32),

    /// Some of the controller's internal structures are missing state (like read addresses) for
    /// a given domain.
    #[error("Internal state is missing for domain {domain_index}")]
    UnmappableDomain {
        /// The index of the domain.
        domain_index: usize,
    },

    /// A `DomainHandle` was passed a `workers` hashmap that couldn't spit out a `Worker`
    /// for a given `WorkerIdentifier`.
    #[error("Could not find RPC handle for worker identifier {ident}")]
    UnmappableWorkerIdentifier {
        /// The worker identifier we don't have a `Worker` RPC handle for.
        ident: String,
    },

    /// A migration tried to reference a domain that doesn't exist.
    #[error("Migration tried to reference domain {domain_index}.{shard:?}")]
    MigrationUnknownDomain {
        /// The index of the domain.
        domain_index: usize,
        /// The shard, if there is one.
        shard: Option<usize>,
    },

    /// The remote end isn't ready to handle requests yet, or has fallen over.
    #[error("Service unavailable")]
    ServiceUnavailable,

    /// An error was encountered when working with URLs.
    #[error("URL parse failed: {0}")]
    UrlParseFailed(String),

    /// An attempt was made to compare replication offsets from different logs.
    ///
    /// See the documentation for [`ReplicationOffset`](noria::ReplicationOffset) for why this might
    /// happen
    #[error(
        "Cannot compare replication offsets from different logs: expected {0}, but got {1} \
             (did the replication log name change?)"
    )]
    ReplicationOffsetLogDifferent(String, String),

    /// An error that was encountered in the mysql_async crate during snapshot/binlog replication proccess
    #[error("MySQL Error during replication: {0}")]
    ReplicationFailed(String),

    /// There are no available Workers to assign domains to.
    #[error("Could not find healthy worker to place domain {domain_index}.{shard}")]
    NoAvailableWorkers {
        /// The index of the domain.
        domain_index: usize,
        /// The shard.
        shard: usize,
    },

    /// A `GroupedOperation` has insufficient state to produce the next result, and must have all
    /// of its state replayed through it to continue.
    #[error("Grouped operation lost state")]
    GroupedStateLost,

    /// A dataflow ingredient received a record of the wrong length
    #[error("Record of invalid length received")]
    InvalidRecordLength,

    /// Wrapper for [`io::Error`]
    #[error("{0}")]
    IOError(String),

    /// A `Vec1` was constructed from a 0-length vector.
    #[error("Vector length was unexpectedly zero")]
    Size0Error,

    /// Expected NodeType to be `Internal` but it's not.
    #[error("Not an internal node")]
    NonInternalNode,

    /// Node has already been taken, so we can't execute whatever action we need to.
    #[error("Node already taken")]
    NodeAlreadyTaken,

    /// Attempted to fill a key that has already been filled.
    #[error("attempted to fill already-filled key")]
    KeyAlreadyFilled,
}

impl ReadySetError {
    fn any_cause<F>(&self, f: F) -> bool
    where
        F: Fn(&Self) -> bool,
    {
        // TODO(grfn): Once https://github.com/rust-lang/rust/issues/58520 stabilizes, this can be
        // rewritten to use that
        f(self)
            || self
                .source()
                .and_then(|e| e.downcast_ref::<Box<ReadySetError>>())
                .iter()
                .any(|e| f(*e))
    }

    /// Returns `true` if the error is an [`UnparseableQuery`].
    pub fn is_unparseable_query(&self) -> bool {
        matches!(self, Self::UnparseableQuery { .. })
    }

    /// Returns true if the error either *is* [`UnparseableQuery`], or was *caused by*
    /// [`UnparseableQuery`]
    pub fn caused_by_unparseable_query(&self) -> bool {
        self.any_cause(|e| e.is_unparseable_query())
    }

    /// Returns `true` if the ready_set_error is [`Unsupported`].
    pub fn is_unsupported(&self) -> bool {
        matches!(self, Self::Unsupported(..))
    }

    /// Returns true if the error either *is* [`Unsupported`], or was *caused by*
    /// [`Unsupported`]
    pub fn caused_by_unsupported(&self) -> bool {
        self.any_cause(|e| e.is_unsupported())
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
        if cfg!(debug_assertions) {
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

/// Return a [`ReadySetError::Internal`] from the current function, if and only if
/// the two arguments are equal.
///
/// This is intended to be used wherever [`assert_ne!`] was used previously.
#[macro_export]
macro_rules! invariant_ne {
    ($expr:expr, $expr2:expr, $($tt:tt)*) => {
        if $expr == $expr2 {
            $crate::internal!(
                "assertion failed: {} != {} ({});\nleft = {:?};\nright = {:?}",
                std::stringify!($expr),
                std::stringify!($expr2),
                format_args!($($tt)*),
                $expr,
                $expr2
            )
        }
    };
    ($expr:expr, $expr2:expr) => {
        if $expr == $expr2 {
            $crate::internal!(
                "assertion failed: {} != {};\nleft = {:?};\nright = {:?}",
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
impl From<bincode::Error> for ReadySetError {
    fn from(e: bincode::Error) -> ReadySetError {
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

/// HACK(eta): this From impl just stringifies the error, so that `ReadySetError` can be serialized
/// and deserialized.
impl From<url::ParseError> for ReadySetError {
    fn from(e: url::ParseError) -> ReadySetError {
        ReadySetError::UrlParseFailed(e.to_string())
    }
}

/// HACK(eta): this From impl just stringifies the error, so that `ReadySetError` can be serialized
/// and deserialized.
impl From<mysql_async::Error> for ReadySetError {
    fn from(e: mysql_async::Error) -> ReadySetError {
        ReadySetError::ReplicationFailed(e.to_string())
    }
}

/// HACK(eta): this From impl just stringifies the error, so that `ReadySetError` can be serialized
/// and deserialized.
impl From<tokio_postgres::Error> for ReadySetError {
    fn from(e: tokio_postgres::Error) -> ReadySetError {
        ReadySetError::ReplicationFailed(e.to_string())
    }
}

/// HACK(eta): this From impl just stringifies the error, so that `ReadySetError` can be serialized
/// and deserialized.
impl From<io::Error> for ReadySetError {
    fn from(e: io::Error) -> ReadySetError {
        ReadySetError::IOError(e.to_string())
    }
}

impl From<Size0Error> for ReadySetError {
    fn from(_: Size0Error) -> Self {
        ReadySetError::Size0Error
    }
}

#[cfg(test)]
mod test {
    use crate::{internal, ReadySetResult};

    #[test]
    #[should_panic(expected = "errors.rs")]
    fn it_reports_location_info() {
        fn example() -> ReadySetResult<()> {
            internal!("honk")
        }
        example().unwrap();
    }
}
