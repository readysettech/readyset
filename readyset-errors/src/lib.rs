//! Error handling, definitions, and utilities

use std::error::Error;
use std::io;

use derive_more::Display;
use nom_sql::Relation;
use petgraph::graph::NodeIndex;
use readyset_util::redacted::Sensitive;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use url::Url;
use vec1::{Size0Error, Vec1};

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
#[derive(Eq, PartialEq, Serialize, Deserialize, Debug, Display, Clone)]
pub enum NodeType {
    /// Base table nodes
    Base,
    /// Egress nodes
    Egress,
    /// Reader nodes
    Reader,
    /// Sharder nodes
    Sharder,
}

/// General error type to be used across all of the ReadySet codebase.
#[derive(Eq, PartialEq, Serialize, Deserialize, Error, Debug, Clone)]
pub enum ReadySetError {
    /// Additional context provided to another [`ReadySetError`] variant
    #[error("{error} ({context})")]
    Context {
        /// Additional context provided to the error.
        context: String,
        /// The original error
        #[source]
        error: Box<ReadySetError>,
    },

    /// The query is invalid
    #[error("The provided query is invalid: {0}")]
    InvalidQuery(String),

    /// The adapter received a query id in CREATE CACHE that does not correspond to a known
    /// query
    #[error("No query known by id {id}")]
    NoQueryForId { id: String },

    /// The adapter will return this error on any set statement that is not
    /// explicitly allowed.
    #[error("Set statement disallowed: {}", Sensitive(statement))]
    SetDisallowed {
        /// The set statement passed to the mysql adapter
        statement: String,
    },

    /// Could not connect to the upstream database provided
    #[error("Could not connect to the upstream database provided")]
    InvalidUpstreamDatabase,

    /// An intra-ReadySet RPC call failed.
    #[error("Error during RPC ({during}): {source}")]
    RpcFailed {
        /// A textual description of the nature of the RPC call that failed.
        during: String,
        /// The error returned by the failed call.
        #[source]
        source: Box<ReadySetError>,
    },

    /// A SQL SELECT query couldn't be created.
    #[error("SQL SELECT query '{qname}' couldn't be added: {source}")]
    SelectQueryCreationFailed {
        /// The query name (identifier) of the query that couldn't be added.
        qname: String,
        /// The error encountered while adding the query.
        #[source]
        source: Box<ReadySetError>,
    },

    /// A migration failed during the planning stage. Nothing was touched.
    #[error("Failed to plan migration: {source}")]
    MigrationPlanFailed {
        /// The error encountered while planning the migration.
        #[source]
        source: Box<ReadySetError>,
    },

    /// A migration failed to apply. The dataflow graph may be in an inconsistent state.
    #[error("Failed to apply migration: {source}")]
    MigrationApplyFailed {
        /// The error encountered while applying the migration.
        #[source]
        source: Box<ReadySetError>,
    },

    /// Failures during recipe creation which may indicate ReadySet is in an invalid state.
    #[error("Unable to create recipe from received DDL: {}", Sensitive(.0))]
    RecipeInvariantViolated(String),

    /// A domain couldn't be booted on the remote worker.
    #[error(
        "Failed to boot domain {domain_index}.{shard}.{replica} on worker '{worker_uri}': {source}"
    )]
    DomainCreationFailed {
        /// The index of the domain.
        domain_index: usize,
        /// The shard of the domain.
        shard: usize,
        /// The replica of the domain.
        replica: usize,
        /// The URI of the worker where the domain was to be placed.
        worker_uri: Url,
        /// The error encountered while trying to boot the domain.
        #[source]
        source: Box<ReadySetError>,
    },

    #[error("MIR node '{index}' couldn't be made: {source}")]
    MirNodeToDataflowFailed {
        /// The index (within the MirGraph) of the MIR node that couldn't
        /// be made.
        index: usize,
        /// The error encountered while adding the MIR node.
        #[source]
        source: Box<ReadySetError>,
    },

    /// An error occurred while sending on a TCP socket.
    #[error("TCP send error: {0}")]
    TcpSendError(String),

    /// Serializing/deserializing the result of an intra-ReadySet RPC call failed.
    ///
    /// This is created by the [`From`] impl on [`serde_json::error::Error`].
    #[error("Failed to (de)serialize: {0}")]
    SerializationFailed(String),

    /// The wrong number of columns was given when inserting a row or preparing
    /// a statement.
    #[error("wrong number of columns specified: expected {0}, got {1}")]
    WrongColumnCount(usize, usize),

    /// The wrong column type was returned from noria when preparing a statement.
    #[error("wrong column type returned from noria: expected {0}, got {1}")]
    WrongColumnType(String, String),

    /// The wrong number of key columns was given when modifying a row.
    #[error("wrong number of key columns used: expected {0}, got {1}")]
    WrongKeyColumnCount(usize, usize),

    /// A table operation was passed an incorrect packet data type.
    #[error("wrong packet data type")]
    WrongPacketDataType,

    /// A NOT NULL column was set to [`DfValue::None`].
    #[error("Attempted to set NOT NULL column '{col}' to DfValue::None")]
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
    /// not either named in the GROUP BY clause or is functionally dependent on (uniquely
    /// determined by) GROUP BY columns.
    ///
    /// cf <https://dev.mysql.com/doc/refman/8.0/en/sql-mode.html#sqlmode_only_full_group_by>
    #[error(
        "Expression `{}` appears in {position} but is not functionally dependent on \
             columns in the GROUP BY clause",
        Sensitive(expression)
    )]
    ExprNotInGroupBy {
        /// A string representation (via [`.to_string()`](ToString::to_string)) of the expression
        /// in question
        expression: String,
        /// A name for where the expression appears in the query (eg "ORDER BY")
        position: String,
    },

    /// A table couldn't be found.
    ///
    /// FIXME(eta): this is currently slightly overloaded in meaning.
    #[error(
        "Could not find table '{}{}'",
        schema.as_ref().map(|s| format!("{}.", s)).unwrap_or_default(),
        name
    )]
    TableNotFound {
        name: String,
        schema: Option<String>,
    },

    /// A query was made referencing a table that exists in the upstream database, but is not being
    /// replicated
    #[error(
        "Table '{}{}' is not being replicated by ReadySet",
        schema.as_ref().map(|s| format!("{}.", s)).unwrap_or_default(),
        name
    )]
    TableNotReplicated {
        name: String,
        schema: Option<String>,
    },

    /// A view is not yet available.
    #[error("view not yet available")]
    ViewNotYetAvailable,

    /// A request was made to a destroyed view
    #[error("view destroyed")]
    ViewDestroyed,

    /// A view couldn't be found.
    #[error("Could not find view {0}")]
    ViewNotFound(String),

    /// A view couldn't be found in the given pool of worker.
    #[error("Could not find view '{name}' in workers '{workers:?}'")]
    ViewNotFoundInWorkers {
        /// The name of the view that could not be found.
        name: String,
        /// The pool of workers where the view was attempted to be found.
        workers: Vec<Url>,
    },

    /// A request to create a view that already exists.
    #[error("View '{0}' already exists")]
    ViewAlreadyExists(String),

    /// No cache found for the given query parameters.
    ///
    /// This error may occur when attempting to find an inlined cache to satisfy a parametrized
    /// query. This error should not reach the client when an upstream database is present.
    #[error("Existing caches do not satisfy the given query parameters.")]
    NoCacheForQuery,

    /// A reader could not be found at the given worker.
    #[error("Reader not found")]
    ReaderNotFound,

    /// The request cannot be serviced because the server is shutting down.
    #[error("Server is shutting down")]
    ServerShuttingDown,

    /// Upquery timeout reached.
    #[error("Upquery timeout")]
    UpqueryTimeout,

    /// The query specified an empty lookup key.
    #[error("the query specified an empty lookup key")]
    EmptyKey,

    /// The queries lookup key is not found at the reader - a cache miss.
    #[error("the queries lookup key is not found at the reader")]
    ReaderMissingKey,

    /// A prepared statement is missing.
    #[error("Prepared statement with ID {statement_id} not found")]
    PreparedStatementMissing {
        /// The prepared statement ID supplied by the user
        statement_id: u32,
    },

    /// An internal error has occurred
    ///
    /// This is produced by the [`internal!`] and [`invariant!`] macros, as an alternative to
    /// panicking.
    /// It should **not** be used for errors we're expecting to be able to handle; this is
    /// a worst-case scenario.
    #[error("Internal error: {0}")]
    Internal(String),

    /// An error has occurred during an HTTP request
    #[error("{0}")]
    HttpError(String),

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
    #[error("Query failed to parse: {}", Sensitive(query))]
    UnparseableQuery {
        /// The SQL of the query.
        query: String,
    },

    /// Manipulating a base table failed.
    #[error("Failed to manipulate table {}: {source}", table.display_unquoted())]
    TableError {
        /// The base table being manipulated.
        table: Relation,
        /// The underlying error that occurred while manipulating the table.
        #[source]
        source: Box<ReadySetError>,
    },

    /// Manipulating a view failed.
    #[error("Failed to manipulate view at {idx:?}: {source}")]
    ViewError {
        /// The index of the view being manipulated.
        idx: NodeIndex,
        /// The underlying error that occurred while manipulating the view.
        #[source]
        source: Box<ReadySetError>,
    },

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

    /// Conversion to or from a [`DfValue`](crate::DfValue) failed.
    #[error("DfValue conversion error: Failed to convert value of type {src_type} to the type {target_type}: {details}")]
    DfValueConversionError {
        /// Source type.
        src_type: String,
        /// Target type.
        target_type: String,
        /// More details about the nature of the error.
        details: String,
    },

    /// Invalid index when evaluating a project expression.
    #[error("Column index out-of-bounds while evaluating project expression: index was {0}")]
    ProjectExprInvalidColumnIndex(usize),

    /// Error in built-in function of expression projection
    #[error(
        "Error in project expression built-in function: {}: {message}",
        Sensitive(function)
    )]
    ProjectExprBuiltInFunctionError {
        /// The built-in function the error occurred in.
        function: String,
        /// Details about the specific error.
        message: String,
    },

    /// Error parsing a string into a NativeDateTime.
    #[error("Error parsing a NaiveDateTime from the string: {}", Sensitive(.0))]
    NaiveDateTimeParseError(String),

    /// Primary key is not on a primitive field.
    #[error("Primary key must be on a primitive field")]
    InvalidPrimaryKeyField,

    /// A worker operation couldn't be completed because the worker doesn't know where the
    /// controller is yet, or has lost track of it.
    #[error("Worker cannot find its controller")]
    LostController,

    /// An RPC request was made to a readyset-server instance that isn't the leader.
    #[error("This instance is not the leader")]
    NotLeader,

    /// An RPC request was made to the leader, but it's not ready. This is likely because
    /// snapshotting is currently ongoing.
    #[error(
        "The leader is not ready. Either it has not finished initializing, or there is an ongoing snapshotting operation."
    )]
    LeaderNotReady,

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

    /// A lookup was performed on a node with a nonexistent index
    #[error("Node with local index {node} does not have an index for {columns:?}")]
    IndexNotFound {
        /// The node that the lookup was performed into
        node: usize,
        /// The set of columns for the index
        columns: Vec<usize>,
    },

    /// A worker tried to check in with a heartbeat payload, but the controller is unaware of it.
    #[error("Unknown worker at {unknown_uri} tried to check in with heartbeat")]
    UnknownWorker {
        /// The URI of the worker that the controller didn't recognize.
        unknown_uri: Url,
    },

    /// An RPC request was attempted against a worker that has failed.
    #[error("Worker at {uri} failed")]
    WorkerFailed {
        /// The failed worker's URI.
        uri: Url,
    },

    /// Making a HTTP request failed.
    #[error("HTTP request {request} failed: {message}")]
    HttpRequestFailed { request: String, message: String },

    /// A shard index was used for a domain that doesn't have that many shards
    #[error("Shard {shard} out of bounds for domain {domain_index} with {num_shards} shards")]
    ShardIndexOutOfBounds {
        shard: usize,
        domain_index: usize,
        num_shards: usize,
    },

    /// A replica index was used for a domain that doesn't have that many shards
    #[error("Replica {replica} out of bounds for view {view_name} with {num_replicas} replicas")]
    ViewReplicaOutOfBounds {
        replica: usize,
        view_name: String,
        num_replicas: usize,
    },

    /// A view was attempted to be built for a reader whose domain is not running
    #[error("Domain at replica {replica} for reader node {node:?} is not running")]
    ReaderReplicaNotRunning { replica: usize, node: NodeIndex },

    /// A request for a domain replica was sent to a worker that doesn't have that domain replica.
    #[error("Could not find domain {domain_index}.{shard}.{replica} on worker")]
    NoSuchReplica {
        /// The index of the domain.
        domain_index: usize,
        /// The shard.
        shard: usize,
        /// The replica.
        replica: usize,
    },

    /// A request referencing a node was sent to a domain not responsible for that node.
    #[error("Node {0} not found in domain")]
    NoSuchNode(usize),

    /// An operation that is valid on only one type of node was made on a node that is not that
    /// type
    #[error("Node {node_index} is not of type {expected_type}")]
    InvalidNodeType {
        /// The index of the node in question
        node_index: usize,
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

    /// An unknown domain was requested
    #[error("Unknown domain {domain_index}")]
    UnknownDomain {
        /// The index of the domain.
        domain_index: usize,
    },

    /// The remote end isn't ready to handle requests yet, or has fallen over.
    #[error("Service unavailable")]
    ServiceUnavailable,

    /// An error was encountered when working with URLs.
    #[error("URL parse failed: {0}")]
    UrlParseFailed(String),

    /// An error was encountered when trying to parse an unknown sql mode.
    #[error("SqlMode parse failed: {0}")]
    SqlModeParseFailed(String),

    /// An attempt was made to compare replication offsets from different logs.
    ///
    /// See the documentation for [`ReplicationOffset`](readyset_client::ReplicationOffset) for why
    /// this might happen
    #[error(
        "Cannot compare replication offsets from different logs: expected {0}, but got {1} \
             (did the replication log name change?)"
    )]
    ReplicationOffsetLogDifferent(String, String),

    /// An error that was encountered during snapshot/binlog/wal replication process
    #[error("Error during replication: {0}")]
    ReplicationFailed(String),

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

    /// Attempted to fill a range, part of which has already been filled.
    #[error("attempted to fill at least partially filled range")]
    RangeAlreadyFilled,

    /// Tried to look up non-existent column.
    #[error("Could not look up non-existent column {column} in {node}")]
    NonExistentColumn {
        /// The column that was attempted to be looked up.
        column: String,
        /// The node the column was looked up in.
        node: String,
    },

    /// Error when calling a [`jemalloc_ctl`] API
    #[error("Error from jemalloc_ctl: {0}")]
    JemallocCtlError(String),

    /// Error when parsing a string as a literal for an array value
    #[error("Malformed array literal '{}': {}", Sensitive(&input), message)]
    ArrayParseError { input: String, message: String },

    #[error("Change in DDL requires partial resnapshot")]
    ResnapshotNeeded,

    #[error("Root certificate must be a valid DER or PEM encoded certificate")]
    InvalidRootCertificate,

    /// Error when a node couldn't be found in MIR.
    #[error("Could not find MIR node {index}")]
    MirNodeNotFound { index: usize },

    /// Error when a relation couldn't be found in MIR.
    #[error("Could not find MIR node for relation '{relation}'")]
    RelationNotFound { relation: String },

    /// Error applying ops to base node
    #[error("Failed to apply some operations to base node for {}", table.display_unquoted())]
    FailedBaseOps {
        /// The base table being manipulated.
        table: Relation,
    },

    /// Error when a MIR node does not have dataflow node assigned, in contexts
    /// where it should had one.
    #[error("MIR node should have a dataflow node assigned: {mir_node_index}")]
    MirNodeMustHaveDfNodeAssigned { mir_node_index: usize },

    /// Error that the upstream database is using an unsupported server version.
    #[error("Upstream server version {major}-{minor} is too low. Minimum supported major version is {min}")]
    UnsupportedServerVersion { major: u16, minor: String, min: u16 },

    /// Error that the upstream database reports a server version the ReadySet could not parse.
    #[error("Upstream server version could not be parsed")]
    UnparseableServerVersion,

    /// Error that we lost the connection to the upstream database
    #[error("Connection to the upstream database was lost: {0}")]
    UpstreamConnectionLost(String),

    /// An unknown pending migration was referenced
    #[error("Unknown migration: {0}")]
    UnknownMigration(u64),

    /// Error interacting with a Consul server
    #[error("Consul error: {0}")]
    ConsulError(String),

    /// A query contains placeholders in positions that are unsupported by ReadySet.
    #[error("Query contains placeholders in unsupported positions")]
    UnsupportedPlaceholders {
        /// The placeholders that were unsupported. This is an internal field, and not displayed to
        /// the user.
        ///
        /// The query must have had all placeholders converted to DollarNumber placeholders. The
        /// values in this field correspond to the post-rewrite placeholders, which may not be what
        /// was given by the client. As such, these values may not make sense to a user.
        placeholders: Vec1<u32>,
    },

    /// Error interacting with native_tls
    #[error("TLS error: {0}")]
    NativeTlsError(String),

    /// Incorrect Auxiliary node state was found
    #[error("Incorrect Auxiliary Node State for {0} Node")]
    IncorrectNodeState(String),

    /// Failed to reach the Controller
    #[error("The ReadySet Controller could not be reached")]
    ControllerUnavailable,
}

impl ReadySetError {
    /// Add additional context to this error
    pub fn context<S>(self, context: S) -> Self
    where
        String: From<S>,
    {
        ReadySetError::Context {
            context: context.into(),
            error: Box::new(self),
        }
    }

    fn any_cause<F>(&self, f: F) -> bool
    where
        F: Fn(&Self) -> bool + Clone,
    {
        // TODO(grfn): Once https://github.com/rust-lang/rust/issues/58520 stabilizes, this can be
        // rewritten to use that
        f(self)
            || self
                .source()
                .and_then(|e| e.downcast_ref::<Box<ReadySetError>>())
                .iter()
                .any(move |e| e.any_cause(f.clone()))
    }

    fn find_map_cause<'a, F, R: 'a>(&'a self, f: F) -> Option<R>
    where
        F: Fn(&'a Self) -> Option<R>,
    {
        f(self).or_else(|| {
            self.source()
                .and_then(|e| e.downcast_ref::<Box<ReadySetError>>())
                .and_then(|e| e.find_map_cause(f))
        })
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

    /// Returns `true` if the error is [`Unsupported`].
    pub fn is_unsupported(&self) -> bool {
        matches!(self, Self::Unsupported(..))
    }

    /// Returns true if the error either *is* [`Unsupported`], or was *caused by*
    /// [`Unsupported`]
    pub fn caused_by_unsupported(&self) -> bool {
        self.any_cause(|e| e.is_unsupported())
    }

    /// Returns `true` if self is ['ViewNotFound'].
    pub fn is_view_not_found(&self) -> bool {
        matches!(self, Self::ViewNotFound(..))
    }

    /// Returns `true` if self either *is* [`ViewNotFound`], or was *caused by* [`ViewNotFound`].
    pub fn caused_by_view_not_found(&self) -> bool {
        self.any_cause(|e| e.is_view_not_found())
    }

    /// Returns `true` if self is [`TableNotFound`].
    pub fn is_table_not_found(&self) -> bool {
        matches!(self, Self::TableNotFound { .. })
    }

    /// Returns `true` if self either *is* [`TableNotFound`], or was *caused by* [`TableNotFound`].
    pub fn caused_by_table_not_found(&self) -> bool {
        self.any_cause(|e| e.is_table_not_found())
    }

    /// If `self` either *is* [`TableNotFound`] or was *caused by* [`TableNotFound`], returns the
    /// fields of the table not found error as a tuple of `(name, schema)`. Otherwise, returns
    /// `None`
    pub fn table_not_found_cause(&self) -> Option<(&str, Option<&str>)> {
        self.find_map_cause(|e| match e {
            Self::TableNotFound { name, schema } => Some((name.as_str(), schema.as_deref())),
            _ => None,
        })
    }

    /// Returns `true` if self is [`TableNotReplicated`].
    pub fn is_table_not_replicated(&self) -> bool {
        matches!(self, Self::TableNotReplicated { .. })
    }

    /// Returns `true` if self either *is* [`TableNotReplicated`], or was *caused by*
    /// [`TableNotReplicated`].
    pub fn caused_by_table_not_replicated(&self) -> bool {
        self.any_cause(|e| e.is_table_not_replicated())
    }

    /// Returns `true` if the error could have been caused by a networking problem.
    pub fn is_networking_related(&self) -> bool {
        self.any_cause(|e| {
            matches!(
                e,
                Self::RpcFailed { .. }
                    | Self::IOError(..)
                    | Self::TcpSendError(..)
                    | Self::ServiceUnavailable
            )
        })
    }

    /// Returns true if the error either *is* [`DfValueConversionError`], or was *caused by*
    /// [`DfValueConversionError`]
    pub fn caused_by_data_type_conversion(&self) -> bool {
        self.any_cause(|e| matches!(e, Self::DfValueConversionError { .. }))
    }

    /// Returns true if the error either *is* [`ViewDestroyed`], or was *caused by*
    /// [`ViewDestroyed`]
    pub fn caused_by_view_destroyed(&self) -> bool {
        self.any_cause(|e| matches!(e, Self::ViewDestroyed))
    }

    /// Returns true if the error either *is* [`SerializationFailed`], or was *caused by*
    /// [`SerializationFailed`]
    pub fn caused_by_serialization_failed(&self) -> bool {
        self.any_cause(|e| matches!(e, Self::SerializationFailed(_)))
    }

    /// Returns `true` if the error is [`InvalidQuery`].
    pub fn is_invalid_query(&self) -> bool {
        matches!(self, Self::InvalidQuery(..))
    }

    /// If `self` either *is* [`UnsupportedPlaceholders`] or was *caused by*
    /// [`UnsupportedPlaceholders`], returns the placeholders that were not supported in the query.
    /// Otherwise, returns `None`
    pub fn unsupported_placeholders_cause(&self) -> Option<Vec1<u32>> {
        self.find_map_cause(|e| match e {
            Self::UnsupportedPlaceholders { placeholders } => Some(placeholders.clone()),
            _ => None,
        })
    }
}

/// Make a new [`ReadySetError::Internal`] with the provided format arguments.
///
/// When building in debug mode, the returned error also captures file, line, and column information
/// for further debugging purposes
///
/// When called with no arguments, generates an internal error with the text
/// "entered unreachable code".
///
/// # Examples
///
/// ```
/// use readyset_errors::internal_err;
///
/// let x = 4;
/// let my_err = internal_err!("{x} things went wrong!");
/// assert!(my_err.to_string().contains("4 things went wrong!"));
/// ```
#[macro_export]
macro_rules! internal_err {
    () => {
        $crate::internal_err!("entered unreachable code")
    };
    ($($format_args:tt)*) => {
        $crate::ReadySetError::Internal(format!(
            "{}{}",
            $crate::__location_info!("in {}: "),
            format_args!($($format_args)*)
        ))
    }
}

/// Make a new [`ReadySetError::InvalidQuery`] with the provided format arguments.
///
/// When building in debug mode, the returned error also captures file, line, and column information
/// for further debugging purposes
///
/// # Examples
///
/// ```
/// use readyset_errors::invalid_query_err;
///
/// let x = 4;
/// let my_err = invalid_query_err!("{x} things were wrong about your query!");
/// assert!(my_err
///     .to_string()
///     .contains("4 things were wrong about your query!"));
/// ```
#[macro_export]
macro_rules! invalid_query_err {
    ($($format_args:tt)*) => {
        $crate::ReadySetError::InvalidQuery(format!(
            "{}{}",
            $crate::__location_info!("in {}: "),
            format_args!($($format_args)*)
        ))
    }
}

/// Make a new [`ReadySetError::Unsupported`] with the provided format arguments.
///
/// When building in debug mode, the returned error also captures file, line, and column information
/// for further debugging purposes
///
/// When called with no arguments, generates an unsupported error with the text "operation not
/// implemented yet".
///
/// # Examples
///
/// ```
/// use readyset_errors::unsupported_err;
///
/// let x = 4;
/// let my_err = unsupported_err!("We can't do {x} of those things yet");
/// assert!(my_err
///     .to_string()
///     .contains("We can't do 4 of those things yet"));
/// ```
#[macro_export]
macro_rules! unsupported_err {
    () => {
        $crate::unsupported_err!("operation not implemented yet")
    };
    ($($format_args:tt)*) => {
        $crate::ReadySetError::Unsupported(format!(
            "{}{}",
            $crate::__location_info!("in {}: "),
            format_args!($($format_args)*)
        ))
    }
}

/// Make a new [`ReadySetError::BadRequest`] with the provided string-able argument.
pub fn bad_request_err<T: Into<String>>(err: T) -> ReadySetError {
    ReadySetError::BadRequest(err.into())
}

/// Make a new [`ReadySetError::Internal`] for a column with no associated table. An internal error
/// is used, because the implied table expansion should guarantee that this should not happen.
pub fn no_table_for_col() -> ReadySetError {
    ReadySetError::Internal(
        "Implied table expansion should guarantee all columns reference a table".to_string(),
    )
}

/// Renders information about the current source location *if* building in debug mode, for use in
/// error-generating macros
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
    ($($format_args:tt)*) => {
        return Err($crate::internal_err!($($format_args)*).into())
    };
}

/// Return a [`ReadySetError::InvalidQuery`] from the current function.
///
/// Usage is like [`panic!`], in that you can pass a format string and arguments.
/// When building in debug mode, the returned error also captures file, line, and column information
/// for further debugging purposes.
#[macro_export]
macro_rules! invalid_query {
    ($($format_args:tt)*) => {
        return Err($crate::invalid_query_err!($($format_args)*).into())
    };
}

/// Return a [`ReadySetError::Unsupported`] from the current function.
///
/// Usage is like [`panic!`], in that you can pass a format string and arguments.
/// When building in debug mode, the returned error also captures file, line, and column information
/// for further debugging purposes.
///
/// When called with no arguments, generates an unsupported error with the text "operation not
/// implemented yet".
#[macro_export]
macro_rules! unsupported {
    ($($format_args:tt)*) => {
        return Err($crate::unsupported_err!($($format_args)*).into())
    };
}

/// Return a [`ReadySetError::Internal`] from the current function, if and only if the argument
/// evaluates to false.
///
/// This is intended to be used wherever [`assert!`] would otherwise be used.
#[macro_export]
macro_rules! invariant {
    ($expr:expr, $($tt:tt)*) => {
        if !$expr {
            $crate::internal!($($tt)*);
        }
    };
    ($expr:expr) => {
        if !$expr {
            $crate::internal!("invariant failed: {}", std::stringify!($expr));
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
                "invariant failed: {} == {} ({});\nleft = {:?};\nright = {:?}",
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
                "invariant failed: {} == {};\nleft = {:?};\nright = {:?}",
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
                "invariant failed: {} != {} ({});\nleft = {:?};\nright = {:?}",
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
                "invariant failed: {} != {};\nleft = {:?};\nright = {:?}",
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
        .unwrap_or_else(|e| Box::new(internal_err!("failed to downcast: {}", e)));
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
pub fn table_err<E: Into<ReadySetError>>(table: Relation, err: E) -> ReadySetError {
    ReadySetError::TableError {
        table,
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
        |e| $crate::rpc_err(format!("{}{}", $during, $crate::__location_info!()), e)
    };
}

/// Generate a `From` impl for the given error type and variant that just stringifies the error, so
/// that `ReadySetError` can be serialized and deserialized
macro_rules! impl_from_to_string {
    ($err_type:ty, $err_variant:ident) => {
        impl From<$err_type> for ReadySetError {
            fn from(e: $err_type) -> Self {
                Self::$err_variant(e.to_string())
            }
        }
    };
}

impl_from_to_string!(serde_json::error::Error, SerializationFailed);
impl_from_to_string!(bincode::Error, SerializationFailed);
impl_from_to_string!(rmp_serde::encode::Error, SerializationFailed);
impl_from_to_string!(rmp_serde::decode::Error, SerializationFailed);
impl_from_to_string!(url::ParseError, UrlParseFailed);
impl_from_to_string!(mysql_async::Error, ReplicationFailed);
impl_from_to_string!(tokio_postgres::Error, ReplicationFailed);
impl_from_to_string!(deadpool_postgres::PoolError, ReplicationFailed);
impl_from_to_string!(deadpool_postgres::CreatePoolError, ReplicationFailed);
impl_from_to_string!(deadpool_postgres::BuildError, ReplicationFailed);
impl_from_to_string!(io::Error, IOError);
impl_from_to_string!(tikv_jemalloc_ctl::Error, JemallocCtlError);
impl_from_to_string!(consulrs::error::ClientError, ConsulError);
impl_from_to_string!(tokio_native_tls::native_tls::Error, NativeTlsError);
impl_from_to_string!(hyper::Error, HttpError);

impl From<Size0Error> for ReadySetError {
    fn from(_: Size0Error) -> Self {
        ReadySetError::Size0Error
    }
}

/// Sets a failpoint with the provided
/// To trigger a generic ReadysetError::Internal error, enable the failpoint with "return".
/// A custom error can be provided with "return(error)" where error is a ReadySetError that has been
/// serialized using serde_json.
/// The failpoint can also be used as a non-return ("pause", "panic", etc.) as well
#[macro_export]
macro_rules! set_failpoint_return_err {
    ($name:expr) => {{
        #[cfg(feature = "failure_injection")]
        set_failpoint!($name, |r| r.map_or(
            Err(ReadySetError::Internal(
                "unspecified failpoint injected".to_string()
            )),
            |e| {
                let err: ReadySetError =
                    serde_json::from_str(&e).expect("failed to parse injected error");
                tracing::info!(%err, "injecting error");
                Err(err)
            }
        ));
    }};
}

#[cfg(test)]
mod test {
    use crate::{internal, ReadySetError, ReadySetResult};

    #[test]
    #[should_panic(expected = "errors/src/lib.rs")]
    fn it_reports_location_info() {
        fn example() -> ReadySetResult<()> {
            internal!("honk")
        }
        example().unwrap();
    }

    #[test]
    fn caused_by_unsupported_two_deep() {
        let err = ReadySetError::RpcFailed {
            during: "test".to_owned(),
            source: Box::new(ReadySetError::MigrationPlanFailed {
                source: Box::new(ReadySetError::Unsupported("Test".to_owned())),
            }),
        };
        assert!(err.caused_by_unsupported());
    }

    #[test]
    fn context_caused_by_table_not_replicated() {
        let err = ReadySetError::TableNotReplicated {
            name: "t1".into(),
            schema: Some("s1".into()),
        }
        .context("some context");
        assert!(err.caused_by_table_not_replicated());
    }

    #[test]
    fn select_query_caused_by_table_not_found() {
        let err = ReadySetError::SelectQueryCreationFailed {
            qname: "my_query".into(),
            source: Box::new(ReadySetError::TableNotFound {
                name: "t1".into(),
                schema: None,
            }),
        };
        assert!(err.caused_by_table_not_found());
    }

    #[test]
    fn table_not_found_cause_two_deep() {
        let err = ReadySetError::RpcFailed {
            during: "extend_recipe (in readyset-client/src/controller/rpc.rs:50:26)".into(),
            source: Box::new(ReadySetError::SelectQueryCreationFailed {
                qname: "`public`.`t2_view_q`".into(),
                source: Box::new(ReadySetError::TableNotFound {
                    name: "t2_view".into(),
                    schema: Some("public".into()),
                }),
            }),
        };
        assert_eq!(
            err.table_not_found_cause().unwrap(),
            ("t2_view", Some("public"))
        )
    }
}
