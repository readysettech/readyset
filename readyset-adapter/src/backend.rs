//!
//! [`Backend`] handles the execution of queries and prepared statements. Queries and
//! statements can be executed either on ReadySet itself, or on the upstream when applicable.
//! In general if an upstream (fallback) connection is available queries and statements
//! will execute as follows:
//!
//! * `INSERT`, `DELETE`, `UPDATE` - on upstream
//! * Anything inside a transaction - on upstream
//! * Cached statements created with "always" - on ReadySet
//! * `SELECT` - on ReadySet
//! * Anything that failed on ReadySet, or while a migration is ongoing - on upstream
//!
//! # The execution flow
//!
//! ## Prepare
//!
//! When an upstream is available we will only try to prepare `SELECT` statements on ReadySet and
//! forward all other prepare requests to the upstream. For `SELECT` statements we will attempt
//! to prepare on both ReadySet and the upstream. The if ReadySet select fails we will perform a
//! fallback execution on the upstream (`execute_cascade`).
//!
//! ## Queries
//!
//! Queries are handled in a similar way to prepare statements. with the exception that additional
//! overhead is required to parse and rewrite them prior to their execution.
//!
//! ## Migrations
//!
//! When a prepared statement is not immediately available for execution on ReadySet, we will
//! perform a migration, migrations can happen in one of three ways:
//!
//! * Explicit migrations: only `CREATE CACHE` and `CREATE VIEW` will cause migrations. A `CREATE
//!   PREPARED STATEMENT` will not cause a migration, and queries will go to upstream fallback.
//!   Enabled with the `--query-caching=explicit` argument. However if a migration already happened,
//!   we will use it.
//! * Async migration: prepared statements will be put in a [`QueryStatusCache`] and another thread
//!   will perform migrations in the background. Once a statement finished migration it will execute
//!   on ReadySet, while it is waiting for a migration to happen it will execute on fallback.
//!   Enabled with the `--query-caching=async` flag.
//! * In request path: migrations will happen when either `CREATE CACHE` or `CREATE PREPARED
//!   STATEMENT` are called. It is also the only available option when a upstream fallback is not
//!   available.
//!
//! ## Caching
//!
//! Since we don't want to pay a penalty every time we execute a prepared statement, either
//! on ReadySet or on the upstream fallback, we aggressively cache all the information required
//! for immediate execution. This way a statement can be immediately forwarded to either ReadySet
//! or upstream with no additional overhead.
//!
//! ## Handling unsupported queries
//!
//! Queries are marked with MigrationState::Unsupported when they fail to prepare on ReadySet
//! with an Unsupported ReadySetError. These queries should not be tried again against ReadySet,
//! however, if a fallback database exists, may be executed against the fallback.
//!
//! ## Handling component outage
//!
//! In a distributed deployment, a component (such as a readyset-server instance) may go down,
//! causing some queries that rely on that server instance to fail. To help direct all affected
//! queries immediately to fallback when this happens, you can configure the
//! --query-max-failure-seconds flag to provide a maximum time in seconds that any given query may
//! continuously fail for before entering into a fallback only recovery period. You can configure
//! the --fallback-recovery-seconds flag to configure how long you would like this recovery period
//! to be enabled for, before allowing affected queries to be retried against noria.
//!
//! The metadata for this feature is tracked in the QueryStatusCache for each query. We currently
//! only trigger on networking related errors specifically to try to prevent this feature from
//! being too heavy handed.

use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt::{self, Debug};
use std::marker::PhantomData;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use clap::ValueEnum;
use crossbeam_skiplist::SkipSet;
use futures::future::{self, OptionFuture};
use lru::LruCache;
use mysql_common::row::convert::{FromRow, FromRowError};
use readyset_adapter_types::{DeallocateId, ParsedCommand, PreparedStatementType};
use readyset_client::consensus::{Authority, AuthorityControl, CacheDDLRequest};
use readyset_client::query::*;
use readyset_client::recipe::CacheExpr;
use readyset_client::results::Results;
use readyset_client::{ColumnSchema, PlaceholderIdx, ViewCreateRequest};
pub use readyset_client_metrics::QueryDestination;
use readyset_client_metrics::{
    recorded, EventType, QueryExecutionEvent, QueryIdWrapper, QueryLogMode, ReadysetExecutionEvent,
    SqlQueryType,
};
use readyset_data::{DfType, DfValue};
use readyset_errors::ReadySetError::{self, PreparedStatementMissing};
use readyset_errors::{internal, internal_err, unsupported, unsupported_err, ReadySetResult};
use readyset_sql::ast::{
    self, AlterReadysetStatement, CacheInner, CreateCacheStatement, DeallocateStatement,
    DropCacheStatement, ExplainStatement, Relation, SelectStatement, SetStatement, ShowStatement,
    SqlIdentifier, SqlQuery, StatementIdentifier, UseStatement,
};
use readyset_sql::{Dialect, DialectDisplay};
use readyset_sql_parsing::ParsingPreset;
use readyset_sql_passes::adapter_rewrites::{self, ProcessedQueryParams};
use readyset_telemetry_reporter::{TelemetryBuilder, TelemetryEvent, TelemetrySender};
use readyset_util::redacted::{RedactedString, Sensitive};
use readyset_util::retry_with_exponential_backoff;
use readyset_version::READYSET_VERSION;
use slab::Slab;
use tokio::sync::mpsc::UnboundedSender;
use tracing::{debug, error, trace, warn};
use vec1::Vec1;

use crate::backend::noria_connector::ExecuteSelectContext;
use crate::metrics_handle::{MetricsHandle, MetricsSummary};
use crate::query_handler::SetBehavior;
use crate::query_status_cache::QueryStatusCache;
use crate::status_reporter::ReadySetStatusReporter;
pub use crate::upstream_database::UpstreamPrepare;
use crate::utils::{create_dummy_column, time_or_null};
use crate::{create_dummy_schema, QueryHandler, UpstreamDatabase, UpstreamDestination};

pub mod noria_connector;

pub use self::noria_connector::NoriaConnector;
use self::noria_connector::{MetaVariable, PreparedSelectTypes};

/// Reserved program/application name used by ReadySet components to identify internal connections
pub const READYSET_QUERY_SAMPLER: &str = "READYSET_QUERY_SAMPLER";

const UNSUPPORTED_CACHE_DDL_MSG: &str = "This instance has been provisioned through Readyset Cloud. Please use the Readyset Cloud UI to manage caches. You may continue to use the SQL interface to run other 'read' commands.";

/// Unique identifier for a prepared statement, local to a single [`Backend`].
pub type StatementId = u32;

/// Query metadata used to plan query prepare
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
enum PrepareMeta {
    /// Query was received in a state that should unconditionally proxy upstream
    Proxy,
    /// Query could not be parsed
    FailedToParse,
    /// Query could not be rewritten for processing in noria
    FailedToRewrite(ReadySetError),
    /// ReadySet does not implement this prepared statement. The statement may also be invalid SQL
    Unimplemented(ReadySetError),
    /// A write query (Insert, Update, Delete)
    Write { stmt: SqlQuery },
    /// A read (Select; may be extended in the future)
    Select(PrepareSelectMeta),
    /// A transaction boundary (Start, Commit, Rollback)
    Transaction { stmt: SqlQuery },
    /// A set command
    Set { stmt: SetStatement },
}

#[derive(Debug)]
struct PrepareSelectMeta {
    stmt: SelectStatement,
    rewritten: SelectStatement,
    must_migrate: bool,
    should_do_noria: bool,
    always: bool,
}

/// How to behave when receiving unsupported `SET` statements
#[derive(Copy, Clone, Debug, PartialEq, Eq, ValueEnum)]
pub enum UnsupportedSetMode {
    /// Return an error to the client (the default)
    Error,
    /// Proxy all subsequent statements to the upstream
    Proxy,
    /// Allow all unsupported set statements
    Allow,
}

/// A state machine representing how statements are proxied upstream for a particular instance of a
/// backend.
///
/// The possible transitions of the state machine are modeled by the following graph:
///
/// ```dot
/// digraph ProxyState {
///     Never -> Never;
///
///     Upstream -> InTransaction;
///     InTransaction -> Upstream;
///     Upstream -> ProxyAlways;
///     InTransaction -> ProxyAlways;
/// }
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ProxyState {
    /// Never proxy statements upstream. This is the behavior used when no upstream database is
    /// configured for a backend
    Never,

    /// Proxy writes upstream, and proxy reads upstream only after they fail when executed against
    /// ReadySet.
    ///
    /// This is the initial behavior used when an upstream database is configured for a backend
    Fallback,

    /// We are inside an explicit transaction (received a BEGIN or START TRANSACTION packet), so
    /// proxy all statements upstream, but return to [`ProxyState::Fallback`] when the transaction
    /// is finished. This state does not apply to transactions formed by `SET autocommit=0`.
    InTransaction,

    /// We are inside of an implicit transaction due to autocommit being turned off. This means
    /// that every time we get COMMIT or ROLLBACK, we instantly start a new transaction. All
    /// statements are proxied upstream unless we receive a `SET autocommit=1` statement, which
    /// would turn autocommit back on.
    AutocommitOff,

    /// Unconditionally proxy all statements upstream, and do not leave this state when leaving
    /// transactions. The backend enters this state when it receives an unsupported SQL `SET`
    /// statement and the [`unsupported_set_mode`] is set to [`Proxy`]
    ///
    /// [`unsupported_set_mode`]: Backend::unsupported_set_mode
    /// [`Proxy`]: UnsupportedSetMode::Proxy
    ProxyAlways,
}

impl ProxyState {
    /// Returns true if a query should be proxied upstream in most cases per this [`ProxyState`].
    /// The case in which we should not proxy a query upstream, is if the query in question has
    /// been manually migrated with the optional `ALWAYS` flag, such as `CREATE CACHE ALWAYS`.
    fn should_proxy(&self) -> bool {
        matches!(
            self,
            Self::AutocommitOff | Self::InTransaction | Self::ProxyAlways
        )
    }

    /// Perform the appropriate state transition for this proxy state to begin a new transaction.
    fn start_transaction(&mut self) {
        if self.is_fallback() {
            *self = ProxyState::InTransaction;
        }
    }

    /// Perform the appropriate state transition for this proxy state to end a transaction
    fn end_transaction(&mut self) {
        if !matches!(self, Self::Never | Self::ProxyAlways | Self::AutocommitOff) {
            *self = ProxyState::Fallback;
        }
    }

    fn in_transaction(&self) -> bool {
        *self == ProxyState::InTransaction
    }

    /// Sets the autocommit state accordingly. If turning autocommit on, will set ProxyState to
    /// Fallback as long as current state is AutocommitOff.
    ///
    /// If turning autocommit off, will set state to AutocommitOff as long as state is not
    /// currently ProxyAlways or Never, as these states should not be overwritten.
    fn set_autocommit(&mut self, on: bool) {
        if on {
            if matches!(self, Self::AutocommitOff) {
                *self = ProxyState::Fallback;
            }
        } else if !matches!(self, Self::ProxyAlways | Self::Never) {
            *self = ProxyState::AutocommitOff;
        }
    }

    /// Returns `true` if the proxy state is [`Fallback`].
    ///
    /// [`Fallback`]: ProxyState::Fallback
    #[must_use]
    fn is_fallback(&self) -> bool {
        matches!(self, Self::Fallback)
    }
}

/// Builder for a [`Backend`]
#[must_use]
#[derive(Clone)]
pub struct BackendBuilder {
    client_addr: SocketAddr,
    slowlog: bool,
    dialect: Dialect,
    parsing_preset: ParsingPreset,
    users: HashMap<String, String>,
    require_authentication: bool,
    query_log_sender: Option<UnboundedSender<QueryExecutionEvent>>,
    query_log_mode: Option<QueryLogMode>,
    unsupported_set_mode: UnsupportedSetMode,
    migration_mode: MigrationMode,
    query_max_failure_seconds: u64,
    fallback_recovery_seconds: u64,
    telemetry_sender: Option<TelemetrySender>,
    placeholder_inlining: bool,
    metrics_handle: Option<MetricsHandle>,
    connections: Option<Arc<SkipSet<SocketAddr>>>,
    allow_cache_ddl: bool,
    sampler_tx: Option<tokio::sync::mpsc::Sender<(QueryExecutionEvent, String)>>,
}

impl Default for BackendBuilder {
    fn default() -> Self {
        BackendBuilder {
            client_addr: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), 0)),
            slowlog: false,
            dialect: Dialect::MySQL,
            parsing_preset: ParsingPreset::for_prod(),
            users: Default::default(),
            require_authentication: true,
            query_log_sender: None,
            query_log_mode: None,
            unsupported_set_mode: UnsupportedSetMode::Error,
            migration_mode: MigrationMode::InRequestPath,
            query_max_failure_seconds: (i64::MAX / 1000) as u64,
            fallback_recovery_seconds: 0,
            telemetry_sender: None,
            placeholder_inlining: false,
            metrics_handle: None,
            connections: None,
            allow_cache_ddl: true,
            sampler_tx: None,
        }
    }
}

impl BackendBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn build<DB: UpstreamDatabase, Handler>(
        self,
        noria: NoriaConnector,
        upstream: Option<DB>,
        query_status_cache: &'static QueryStatusCache,
        authority: Arc<Authority>,
        status_reporter: ReadySetStatusReporter<DB>,
        adapter_start_time: SystemTime,
    ) -> Backend<DB, Handler> {
        metrics::gauge!(recorded::CONNECTED_CLIENTS).increment(1.0);
        metrics::counter!(recorded::CLIENT_CONNECTIONS_OPENED).increment(1);

        let proxy_state = if upstream.is_some() {
            ProxyState::Fallback
        } else {
            ProxyState::Never
        };

        if let Some(connections) = &self.connections {
            connections.insert(self.client_addr);
        }

        Backend {
            client_addr: self.client_addr,
            noria,
            upstream,
            users: self.users,
            query_log_sender: self.query_log_sender,
            query_log_mode: self.query_log_mode,
            last_query: None,
            state: BackendState {
                proxy_state,
                parsed_query_cache: LruCache::new(10_000.try_into().expect("10000 is not 0")),
                prepared_statements: Default::default(),
                unnamed_prepared_statements: Default::default(),
                query_status_cache,
            },
            settings: BackendSettings {
                slowlog: self.slowlog,
                dialect: self.dialect,
                parsing_preset: self.parsing_preset,
                require_authentication: self.require_authentication,
                unsupported_set_mode: self.unsupported_set_mode,
                migration_mode: self.migration_mode,
                query_max_failure_duration: Duration::new(self.query_max_failure_seconds, 0),
                fallback_recovery_duration: Duration::new(self.fallback_recovery_seconds, 0),
                placeholder_inlining: self.placeholder_inlining,
            },
            telemetry_sender: self.telemetry_sender,
            authority,
            metrics_handle: self.metrics_handle,
            connections: self.connections,
            status_reporter,
            allow_cache_ddl: self.allow_cache_ddl,
            adapter_start_time,
            sampler_tx: self.sampler_tx,
            is_internal_connection: false,
            _query_handler: PhantomData,
        }
    }

    pub fn client_addr(mut self, client_addr: SocketAddr) -> Self {
        self.client_addr = client_addr;
        self
    }

    pub fn slowlog(mut self, slowlog: bool) -> Self {
        self.slowlog = slowlog;
        self
    }

    pub fn dialect(mut self, dialect: Dialect) -> Self {
        self.dialect = dialect;
        self
    }

    pub fn parsing_preset(mut self, parsing_preset: ParsingPreset) -> Self {
        self.parsing_preset = parsing_preset;
        self
    }

    pub fn query_log_sender(
        mut self,
        query_log_sender: Option<UnboundedSender<QueryExecutionEvent>>,
    ) -> Self {
        self.query_log_sender = query_log_sender;
        self
    }

    pub fn query_log_mode(mut self, query_log_mode: Option<QueryLogMode>) -> Self {
        self.query_log_mode = query_log_mode;
        self
    }

    pub fn users(mut self, users: HashMap<String, String>) -> Self {
        self.users = users;
        self
    }

    pub fn require_authentication(mut self, require_authentication: bool) -> Self {
        self.require_authentication = require_authentication;
        self
    }

    /// Whether or not to allow cache ddl statements to be executed. If false, cache ddl statements
    /// received will instead return an error prompting the user to use ReadySet cloud to manage
    /// their caches.
    pub fn allow_cache_ddl(mut self, allow_cache_ddl: bool) -> Self {
        self.allow_cache_ddl = allow_cache_ddl;
        self
    }

    pub fn unsupported_set_mode(mut self, unsupported_set_mode: UnsupportedSetMode) -> Self {
        self.unsupported_set_mode = unsupported_set_mode;
        self
    }

    pub fn migration_mode(mut self, q: MigrationMode) -> Self {
        self.migration_mode = q;
        self
    }

    pub fn query_max_failure_seconds(mut self, secs: u64) -> Self {
        self.query_max_failure_seconds = secs;
        self
    }

    pub fn fallback_recovery_seconds(mut self, secs: u64) -> Self {
        self.fallback_recovery_seconds = secs;
        self
    }

    pub fn telemetry_sender(mut self, telemetry_sender: TelemetrySender) -> Self {
        self.telemetry_sender = Some(telemetry_sender);
        self
    }

    pub fn set_placeholder_inlining(mut self, placeholder_inlining: bool) -> Self {
        self.placeholder_inlining = placeholder_inlining;
        self
    }

    pub fn connections(mut self, connections: Arc<SkipSet<SocketAddr>>) -> Self {
        self.connections = Some(connections);
        self
    }

    pub fn metrics_handle(mut self, metrics_handle: Option<MetricsHandle>) -> Self {
        self.metrics_handle = metrics_handle;
        self
    }

    /// Set the sender used to enqueue original queries for background sampling/verification
    pub fn sampler_tx(
        mut self,
        tx: Option<tokio::sync::mpsc::Sender<(QueryExecutionEvent, String)>>,
    ) -> Self {
        self.sampler_tx = tx;
        self
    }
}

/// A [`PreparedStatement`] stores the data needed for an immediate execution of a prepared
/// statement on either noria or the upstream connection.
struct PreparedStatement<DB>
where
    DB: UpstreamDatabase,
{
    /// Indicates if the statement was prepared for ReadySet, Fallback, or Both
    prep: PrepareResult<DB>,
    /// The current ReadySet migration state
    migration_state: MigrationState,
    /// Indicates whether the prepared statement was already migrated manually with the optional
    /// ALWAYS flag, such as a CREATE CACHE ALWAYS FROM command.
    /// This is imperfect, but leans on performance over correctness. It requires a user to
    /// re-prepare queries if they decide to change between ALWAYS and not ALWAYS.
    always: bool,
    /// Holds information about if executes have been succeeding, or failing, along with a state
    /// transition timestamp. None if prepared statement has never been executed.
    execution_info: Option<ExecutionInfo>,
    /// If query was successfully parsed, will store the parsed query
    parsed_query: Option<Arc<SqlQuery>>,
    /// If was able to hash the query, will store the generated hash
    query_id: Option<QueryId>,
    /// If statement was successfully rewritten, will store all information necessary to install
    /// the view in readyset
    view_request: Option<ViewCreateRequest>,
}

impl<DB> PreparedStatement<DB>
where
    DB: UpstreamDatabase,
{
    /// Returns whether we are currently in fallback recovery mode for the given prepared statement
    /// we are attempting to execute.
    /// WARNING: This will also mutate execution info timestamp if we have exceeded the supplied
    /// recovery period.
    pub(crate) fn in_fallback_recovery(
        &mut self,
        query_max_failure_duration: Duration,
        fallback_recovery_duration: Duration,
    ) -> bool {
        if let Some(info) = self.execution_info.as_mut() {
            info.reset_if_exceeded_recovery(query_max_failure_duration, fallback_recovery_duration);
            info.execute_network_failure_exceeded(query_max_failure_duration)
        } else {
            false
        }
    }

    pub(crate) fn is_unsupported_execute(&self) -> bool {
        if let Some(info) = self.execution_info.as_ref() {
            matches!(info.state, ExecutionState::Unsupported)
        } else {
            false
        }
    }

    /// Get a reference to the `ViewRequest` or return an error
    fn as_view_request(&self) -> ReadySetResult<&ViewCreateRequest> {
        self.view_request
            .as_ref()
            .ok_or_else(|| internal_err!("Expected ViewRequest for CachedPreparedStatement"))
    }
}

pub struct Backend<DB, Handler>
where
    DB: UpstreamDatabase,
{
    /// Remote socket address of a connected client
    client_addr: SocketAddr,
    /// ReadySet connector used for reads, and writes when no upstream DB is present
    pub noria: NoriaConnector,
    /// Optional connector to the upstream DB. Used for fallback reads and all writes if it exists
    upstream: Option<DB>,
    /// Map from username to password for all users allowed to connect to the db
    pub users: HashMap<String, String>,

    query_log_sender: Option<UnboundedSender<QueryExecutionEvent>>,
    query_log_mode: Option<QueryLogMode>,

    /// Information regarding the last query sent over this connection. If None, then no queries
    /// have been handled using this connection (Backend) yet.
    last_query: Option<QueryInfo>,

    /// Encapsulates the inner state of this [`Backend`]
    state: BackendState<DB>,
    /// The settings with which the [`Backend`] was started
    settings: BackendSettings,

    /// Provides the ability to send [`TelemetryEvent`]s to Segment
    telemetry_sender: Option<TelemetrySender>,

    /// Handle to the Authority. A handle is also stored in Self::noria where it is used to find
    /// the Controller.
    authority: Arc<Authority>,

    /// Handle to the [`metrics_exporter_prometheus::PrometheusRecorder`] that runs in the adapter.
    metrics_handle: Option<MetricsHandle>,

    /// Set of active connections to this adapter
    connections: Option<Arc<SkipSet<SocketAddr>>>,

    status_reporter: ReadySetStatusReporter<DB>,

    /// Whether or not to allow cache ddl statements to be executed. If false, cache ddl statements
    /// received will instead return an error prompting the user to use ReadySet cloud to manage
    /// their caches.
    allow_cache_ddl: bool,

    /// The time at which the adapter started.
    adapter_start_time: SystemTime,

    /// Optional sender to enqueue original queries for background sampling/verification
    sampler_tx: Option<tokio::sync::mpsc::Sender<(QueryExecutionEvent, String)>>,

    /// Boolean to indicate if the backend connection is an internal connection (eg.: From Query Sampler)
    is_internal_connection: bool,

    _query_handler: PhantomData<Handler>,
}

/// Variables that keep track of the [`Backend`] state
struct BackendState<DB>
where
    DB: UpstreamDatabase,
{
    proxy_state: ProxyState,
    /// A cache of queries that we've seen, and their current state, used for processing
    query_status_cache: &'static QueryStatusCache,
    // a cache of all previously parsed queries
    parsed_query_cache: LruCache<String, SqlQuery>,
    // all queries previously prepared on noria or upstream. The position in the slab is the
    // id to retrieve the prepared statement.
    prepared_statements: Slab<PreparedStatement<DB>>,
    /// For unnamed prepared statements, we need to prepare the statement on the upstream in
    /// order to get the types for the query. We store that metadata in `prepared_statements`,
    /// just like regular prepared statements. The difference is that the clients are not
    /// "reusing" that prepared statement, so we only have the query string to identify
    /// any metadata we've previously prepared and cached. Thus this map is a link from the query
    /// to the index of the prepared statement in `prepared_statements`.
    unnamed_prepared_statements: HashMap<String, usize>,
}

/// Settings that have no state and are constant for a given [`Backend`]
struct BackendSettings {
    /// SQL dialect to use when parsing queries from clients
    dialect: Dialect,
    /// Parsing mode that determines which parser(s) to use and how to handle conflicts
    parsing_preset: ParsingPreset,
    slowlog: bool,
    require_authentication: bool,
    /// How to behave when receiving unsupported `SET` statements
    unsupported_set_mode: UnsupportedSetMode,
    /// How this backend handles migrations, See MigrationMode.
    migration_mode: MigrationMode,
    /// The maximum duration that a query can continuously fail for before we enter into a recovery
    /// period.
    query_max_failure_duration: Duration,
    /// The recovery period that we enter into for a given query, when that query has
    /// repeatedly failed for query_max_failure_duration.
    fallback_recovery_duration: Duration,
    /// Whether to automatically create inlined migrations for queries with unsupported
    /// placeholders.
    placeholder_inlining: bool,
}

/// QueryInfo holds information regarding the last query that was sent along this connection
/// (Backend).
#[derive(Debug, Default)]
pub struct QueryInfo {
    pub destination: QueryDestination,
    pub noria_error: String,
}

impl FromRow for QueryInfo {
    fn from_row_opt(row: mysql_common::row::Row) -> Result<Self, FromRowError> {
        let mut res = QueryInfo::default();

        // Parse each column into it's respective QueryInfo field.
        for (i, c) in row.columns_ref().iter().enumerate() {
            if let mysql_common::value::Value::Bytes(d) = row.as_ref(i).unwrap() {
                let dest = std::str::from_utf8(d).map_err(|_| FromRowError(row.clone()))?;

                if c.name_str() == "Query_destination" {
                    res.destination =
                        QueryDestination::try_from(dest).map_err(|_| FromRowError(row.clone()))?;
                } else if c.name_str() == "Readyset_error" {
                    res.noria_error = std::str::from_utf8(d)
                        .map_err(|_| FromRowError(row.clone()))?
                        .to_string();
                } else {
                    return Err(FromRowError(row.clone()));
                }
            }
        }

        Ok(res)
    }
}

/// How to handle a migration in the adapter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MigrationMode {
    /// Handle migrations as part of the query process, if a query has not been
    /// successfully migrated when we are processing the query, attempt to
    /// perform the migration as part of the query.
    InRequestPath,
    /// Never perform migrations in the query path. If a query has not been
    /// migrated yet, send it to fallback if fallback exists, otherwise reject
    /// the query.
    ///
    /// This mode is used when some other operation is performing the
    /// migrations and updating a query's migration status. Either
    /// --query-caching=async which runs migrations in a separate thread,
    /// or --query-caching=explicit which enables special syntax to perform
    /// migrations "CREATE CACHE ..." may be used.
    OutOfBand,
}

#[derive(Debug, Clone)]
pub struct SelectSchema<'a> {
    pub schema: Cow<'a, [ColumnSchema]>,
    pub columns: Cow<'a, [SqlIdentifier]>,
}

impl SelectSchema<'_> {
    pub fn into_owned(self) -> SelectSchema<'static> {
        SelectSchema {
            schema: Cow::Owned(self.schema.into_owned()),
            columns: Cow::Owned(self.columns.into_owned()),
        }
    }
}

/// Adapter clients need only one of the prepare results returned from prepare().
/// PrepareResult provides noria_biased() and upstream_biased() to get
/// the single relevant prepare result from `PrepareResult` which may return
/// PrepareResult::Both.
pub enum SinglePrepareResult<'a, DB: UpstreamDatabase> {
    Noria(&'a noria_connector::PrepareResult),
    Upstream(&'a UpstreamPrepare<DB>),
}

pub enum PrepareResultInner<DB: UpstreamDatabase> {
    Noria(noria_connector::PrepareResult),
    Upstream(UpstreamPrepare<DB>),
    Both(noria_connector::PrepareResult, UpstreamPrepare<DB>),
}

impl<DB: UpstreamDatabase> Debug for PrepareResultInner<DB> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Noria(r) => f.debug_tuple("Noria").field(r).finish(),
            Self::Upstream(r) => f.debug_tuple("Upstream").field(r).finish(),
            Self::Both(nr, ur) => f.debug_tuple("Both").field(nr).field(ur).finish(),
        }
    }
}

/// The type returned when a query is prepared by `Backend` through the `prepare` function.
#[derive(Debug)]
pub struct PrepareResult<DB: UpstreamDatabase> {
    pub statement_id: StatementId,
    inner: PrepareResultInner<DB>,
}

impl<DB: UpstreamDatabase> PrepareResult<DB> {
    pub fn new(statement_id: StatementId, inner: PrepareResultInner<DB>) -> Self {
        Self {
            statement_id,
            inner,
        }
    }

    pub fn noria_biased(&self) -> SinglePrepareResult<'_, DB> {
        match &self.inner {
            PrepareResultInner::Noria(res) | PrepareResultInner::Both(res, _) => {
                SinglePrepareResult::Noria(res)
            }
            PrepareResultInner::Upstream(res) => SinglePrepareResult::Upstream(res),
        }
    }

    pub fn upstream_biased(&self) -> SinglePrepareResult<'_, DB> {
        match &self.inner {
            PrepareResultInner::Upstream(res) | PrepareResultInner::Both(_, res) => {
                SinglePrepareResult::Upstream(res)
            }
            PrepareResultInner::Noria(res) => SinglePrepareResult::Noria(res),
        }
    }

    pub fn into_upstream(self) -> Option<UpstreamPrepare<DB>> {
        match self.inner {
            PrepareResultInner::Upstream(ur) | PrepareResultInner::Both(_, ur) => Some(ur),
            _ => None,
        }
    }

    /// If this [`PrepareResult`] is a [`PrepareResult::Both`], convert it into only a
    /// [`PrepareResult::Upstream`]
    pub fn make_upstream_only(&mut self) {
        match &mut self.inner {
            PrepareResultInner::Noria(_) | PrepareResultInner::Upstream(_) => {}
            PrepareResultInner::Both(_, u) => self.inner = PrepareResultInner::Upstream(u.clone()),
        }
    }
}

/// The type returned when a query is carried out by `Backend`, through either the `query` or
/// `execute` functions.
#[allow(clippy::large_enum_variant)]
pub enum QueryResult<'a, DB>
where
    DB: UpstreamDatabase + 'a,
{
    /// Results from noria
    Noria(noria_connector::QueryResult<'a>),
    /// Results from upstream
    Upstream(DB::QueryResult<'a>),
    /// Results from upstream that are explicitly buffered in a Vec (from postgres' Simple Query
    /// Protocol)
    UpstreamBufferedInMemory(DB::QueryResult<'a>),
    /// Results from parsing a SQL statement and determining that it's a command that should
    /// be handled at an outer layer.
    Parser(ParsedCommand),
}

impl<'a, DB: UpstreamDatabase> From<noria_connector::QueryResult<'a>> for QueryResult<'a, DB> {
    fn from(r: noria_connector::QueryResult<'a>) -> Self {
        Self::Noria(r)
    }
}

impl<DB> Debug for QueryResult<'_, DB>
where
    DB: UpstreamDatabase,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Noria(r) => f.debug_tuple("Noria").field(r).finish(),
            Self::Upstream(r) => f.debug_tuple("Upstream").field(r).finish(),
            Self::UpstreamBufferedInMemory(r) => {
                f.debug_tuple("UpstreamBufferedInMemory").field(r).finish()
            }
            Self::Parser(r) => f.debug_tuple("Parser").field(r).finish(),
        }
    }
}

/// TODO: The ideal approach for query handling is as follows:
/// 1. If we know we can't support a query, send it to fallback.
/// 2. If we think we can support a query, try to send it to ReadySet. If that hits an error that
///    should be retried, retry.    If not, try fallback without dropping the connection inbetween.
/// 3. If that fails and we got a MySQL error code, send that back to the client and keep the
///    connection open. This is a real correctness bug. 4. If we got another kind of error that is
///    retryable from fallback, retry. 5. If we got a non-retry related error that's not a MySQL
///    error code already, convert it to the most appropriate MySQL error code and write    that
///    back to the caller without dropping the connection.
impl<DB, Handler> Backend<DB, Handler>
where
    DB: 'static + UpstreamDatabase,
    Handler: 'static + QueryHandler,
{
    pub fn version(&self) -> String {
        self.upstream
            .as_ref()
            .map(|upstream| upstream.version())
            .unwrap_or_else(|| DB::DEFAULT_DB_VERSION.to_string())
    }

    /// Send ping on the upstream connection, if it exists
    pub async fn ping(&mut self) -> Result<(), DB::Error> {
        if let Some(upstream) = &mut self.upstream {
            upstream.ping().await
        } else {
            Ok(())
        }
    }
    /// Reset the current upstream connection
    pub async fn reset(&mut self) -> Result<(), DB::Error> {
        if let Some(upstream) = &mut self.upstream {
            upstream.reset().await
        } else {
            Ok(())
        }
    }

    /// Switch the active database for this backend to the given named database.
    ///
    /// Internally, this will set the schema search path to a single-element vector with the
    /// database, and send a `USE` command to the upstream, if any.
    pub async fn set_database(&mut self, db: &str) -> Result<(), DB::Error> {
        if let Some(upstream) = &mut self.upstream {
            upstream
                .query(
                    &UseStatement {
                        database: db.into(),
                    }
                    .to_string(),
                )
                .await?;
        }
        self.noria.set_schema_search_path(vec![db.into()]);
        Ok(())
    }

    /// Change the user for the upstream connection, if it exists
    ///
    /// This is called when the client authenticates to the server.
    pub async fn set_user(
        &mut self,
        user: &str,
        password: RedactedString,
    ) -> Result<(), DB::Error> {
        if let Some(upstream) = &mut self.upstream {
            let _ = upstream.set_user(user, password).await;
        }
        Ok(())
    }
    pub async fn change_user(
        &mut self,
        user: &str,
        password: &str,
        database: &str,
    ) -> Result<(), DB::Error> {
        if let Some(upstream) = &mut self.upstream {
            upstream.change_user(user, password, database).await?;
        }
        Ok(())
    }

    /// Executes query on the upstream database, for when it cannot be parsed or executed by noria.
    /// Returns the query result, or an error if fallback is not configured
    pub async fn query_fallback<'a>(
        upstream: Option<&'a mut DB>,
        query: &'a str,
        event: &mut QueryExecutionEvent,
    ) -> Result<QueryResult<'a, DB>, DB::Error> {
        let upstream = upstream.ok_or_else(|| {
            ReadySetError::Internal("This case requires an upstream connector".to_string())
        })?;
        let _t = event.start_upstream_timer();
        let result = upstream.query(query).await;
        drop(_t);
        event.destination = Some(match &result {
            Ok(qr) => qr.destination(),
            Err(_) => QueryDestination::Upstream,
        });
        result.map(QueryResult::Upstream)
    }

    /// Executes query on the upstream database using the "simple query" protocol, which buffers
    /// results in memory before returning. Note that this only applies to PostgreSQL backends, and
    /// for MySQL will return an error.
    pub async fn simple_query_upstream<'a>(
        &'a mut self,
        query: &'a str,
    ) -> Result<QueryResult<'a, DB>, DB::Error> {
        let upstream = self.upstream.as_mut().ok_or_else(|| {
            ReadySetError::Internal("This case requires an upstream connector".to_string())
        })?;
        let result = upstream.simple_query(query).await;
        result.map(QueryResult::UpstreamBufferedInMemory)
    }

    /// Prepares query on the upstream database, if present, when it cannot be parsed or prepared by
    /// noria.
    pub async fn prepare_fallback(
        &mut self,
        query: &str,
        data: DB::PrepareData<'_>,
        statement_type: PreparedStatementType,
    ) -> Result<UpstreamPrepare<DB>, DB::Error> {
        let upstream = self.upstream.as_mut().ok_or_else(|| {
            ReadySetError::Internal("This case requires an upstream connector".to_string())
        })?;
        upstream.prepare(query, data, statement_type).await
    }

    /// Prepares query against ReadySet. If an upstream database exists, the prepare is mirrored to
    /// the upstream database.
    ///
    /// This function may perform a migration, and update a queries migration state, if
    /// InRequestPath mode is enabled or of not upstream is set
    async fn mirror_prepare(
        &mut self,
        select_meta: &PrepareSelectMeta,
        query: &str,
        data: DB::PrepareData<'_>,
        statement_type: PreparedStatementType,
        event: &mut QueryExecutionEvent,
    ) -> Result<PrepareResultInner<DB>, DB::Error> {
        let do_noria = select_meta.should_do_noria;
        let do_migrate = select_meta.must_migrate;

        let up_prep: OptionFuture<_> = self
            .upstream
            .as_mut()
            .map(|u| u.prepare(query, data, statement_type))
            .into();
        let noria_prep: OptionFuture<_> = do_noria
            .then_some(
                self.noria
                    .prepare_select(select_meta.stmt.clone(), do_migrate, None),
            )
            .into();

        let (upstream_res, noria_res) = future::join(up_prep, noria_prep).await;

        let destination = match (upstream_res.is_some(), noria_res.is_some()) {
            (true, true) => Some(QueryDestination::Both),
            (false, true) => Some(QueryDestination::Readyset),
            (true, false) => Some(QueryDestination::Upstream),
            (false, false) => None,
        };

        self.last_query = destination.map(|d| QueryInfo {
            destination: d,
            noria_error: String::new(),
        });

        // Update noria migration state for query
        match &noria_res {
            Some(Ok(noria_connector::PrepareResult::Select { .. })) => {
                self.state.query_status_cache.update_query_migration_state(
                    &ViewCreateRequest::new(
                        select_meta.rewritten.clone(),
                        self.noria.schema_search_path().to_owned(),
                    ),
                    MigrationState::Successful,
                );
            }
            Some(Err(e)) => {
                if e.caused_by_view_not_found() {
                    debug!(error = %e, "View not found during mirror_prepare()");
                    self.state.query_status_cache.view_not_found_for_query(
                        &ViewCreateRequest::new(
                            select_meta.rewritten.clone(),
                            self.noria.schema_search_path().to_owned(),
                        ),
                    );
                } else if e.caused_by_unsupported() {
                    self.state.query_status_cache.update_query_migration_state(
                        &ViewCreateRequest::new(
                            select_meta.rewritten.clone(),
                            self.noria.schema_search_path().to_owned(),
                        ),
                        MigrationState::Unsupported(e.unsupported_cause().unwrap_or_default()),
                    );
                } else {
                    error!(
                        error = %e,
                        "Error received from noria during mirror_prepare()"
                    );
                }
                event.set_noria_error(e);
            }
            None => {}
            _ => internal!("Can only return SELECT result or error"),
        }

        let prep_result = match (upstream_res, noria_res) {
            (Some(upstream_res), Some(Ok(noria_res))) => {
                PrepareResultInner::Both(noria_res, upstream_res?)
            }
            (None, Some(Ok(noria_res))) => {
                if matches!(
                    noria_res,
                    noria_connector::PrepareResult::Select {
                        types: PreparedSelectTypes::NoSchema,
                        ..
                    }
                ) {
                    // We fail when attempting to borrow a cache without an upstream here in case
                    // the connection to the upstream is temporarily down.
                    internal!(
                        "Cannot create PrepareResult for borrowed cache without an upstream result"
                    );
                }
                PrepareResultInner::Noria(noria_res)
            }
            (None, Some(Err(noria_err))) => return Err(noria_err.into()),
            (Some(upstream_res), _) => PrepareResultInner::Upstream(upstream_res?),
            (None, None) => return Err(ReadySetError::Unsupported(query.to_string()).into()),
        };

        Ok(prep_result)
    }

    /// Prepares Insert, Delete, and Update statements
    async fn prepare_write(
        &mut self,
        query: &str,
        stmt: &SqlQuery,
        data: DB::PrepareData<'_>,
        statement_type: PreparedStatementType,
        event: &mut QueryExecutionEvent,
    ) -> Result<PrepareResultInner<DB>, DB::Error> {
        event.sql_type = SqlQueryType::Write;
        if let Some(ref mut upstream) = self.upstream {
            let _t = event.start_upstream_timer();
            let res = upstream
                .prepare(query, data, statement_type)
                .await
                .map(PrepareResultInner::Upstream);
            self.last_query = Some(QueryInfo {
                destination: QueryDestination::Upstream,
                noria_error: String::new(),
            });
            res
        } else {
            let start = Instant::now();
            let res = match stmt {
                SqlQuery::Insert(stmt) => self.noria.prepare_insert(stmt.clone()).await?,
                SqlQuery::Delete(stmt) => self.noria.prepare_delete(stmt.clone()).await?,
                SqlQuery::Update(stmt) => self.noria.prepare_update(stmt.clone()).await?,
                // prepare_write does not support other statements
                _ => internal!(),
            };
            self.last_query = Some(QueryInfo {
                destination: QueryDestination::Readyset,
                noria_error: String::new(),
            });

            event.readyset_event = Some(ReadysetExecutionEvent::Other {
                duration: start.elapsed(),
            });

            Ok(PrepareResultInner::Noria(res))
        }
    }

    /// Ensure we are allowed to handle the SET statement.
    async fn prepare_set(
        &mut self,
        stmt: &SetStatement,
        query: &str,
        data: DB::PrepareData<'_>,
        statement_type: PreparedStatementType,
        event: &mut QueryExecutionEvent,
    ) -> Result<PrepareResultInner<DB>, DB::Error> {
        // if `handle_set()` returns an error, we aren't supposed to process
        // the SET anyway, so propagating the error is expected.
        // Then we need to determine if we're actually going to proxy to the upstream.
        Self::handle_set(
            &mut self.noria,
            self.upstream.is_some(),
            &self.settings,
            &mut self.state,
            query,
            stmt,
            event,
        )?;
        let res = if self.state.proxy_state.should_proxy() && self.upstream.is_some() {
            let upstream = self.upstream.as_mut().unwrap(); // just checked
            let prep = upstream.prepare(query, data, statement_type).await?;
            PrepareResultInner::Upstream(prep)
        } else {
            PrepareResultInner::Noria(noria_connector::PrepareResult::Set {
                statement: stmt.clone(),
            })
        };

        Ok(res)
    }

    /// Provides metadata required to prepare a select query
    fn plan_prepare_select(&mut self, stmt: SelectStatement) -> PrepareMeta {
        let mut rewritten = stmt.clone();
        let _ = adapter_rewrites::process_query(&mut rewritten, self.noria.rewrite_params())
            .map_err(|e| {
                warn!(
                    statement = %Sensitive(&stmt.display(self.settings.dialect)),
                    "This statement could not be rewritten by Readyset"
                );
                PrepareMeta::FailedToRewrite(e)
            });

        let status = self
            .state
            .query_status_cache
            .query_status(&ViewCreateRequest::new(
                rewritten.clone(),
                self.noria.schema_search_path().to_owned(),
            ));
        if self.state.proxy_state == ProxyState::ProxyAlways && !status.always {
            PrepareMeta::Proxy
        } else {
            let should_do_readyset =
                !matches!(status.migration_state, MigrationState::Unsupported(_));
            PrepareMeta::Select(PrepareSelectMeta {
                stmt,
                rewritten,
                should_do_noria: should_do_readyset,
                // For select statements only InRequestPath should trigger migrations
                // synchronously, or if no upstream is present.
                must_migrate: self.settings.migration_mode == MigrationMode::InRequestPath
                    || !self.has_fallback(),
                always: status.always,
            })
        }
    }

    /// Provides metadata required to prepare a query
    async fn plan_prepare(&mut self, query: &str, event: &mut QueryExecutionEvent) -> PrepareMeta {
        if self.state.proxy_state == ProxyState::ProxyAlways {
            return PrepareMeta::Proxy;
        }

        let parse_result = match self.state.parsed_query_cache.get(query) {
            Some(cached_query) => Ok(cached_query.clone()),
            None => {
                let res = {
                    let _t = event.start_parse_timer();
                    self.parse_query(query)
                };
                if res.is_ok() {
                    self.state
                        .parsed_query_cache
                        .put(query.to_string(), res.clone().unwrap());
                }
                res
            }
        };

        match parse_result {
            Ok(SqlQuery::Select(stmt)) => self.plan_prepare_select(stmt),
            Ok(
                query @ SqlQuery::Insert(_)
                | query @ SqlQuery::Update(_)
                | query @ SqlQuery::Delete(_),
            ) => PrepareMeta::Write { stmt: query },
            Ok(
                query @ SqlQuery::StartTransaction(_)
                | query @ SqlQuery::Commit(_)
                | query @ SqlQuery::Rollback(_),
            ) => PrepareMeta::Transaction { stmt: query },
            Ok(SqlQuery::Set(s)) => PrepareMeta::Set { stmt: s },
            Ok(pq) => {
                debug!(
                    statement = %pq.display(self.settings.dialect),
                    "Statement cannot be prepared by Readyset"
                );
                PrepareMeta::Unimplemented(unsupported_err!(
                    "{} not supported without an upstream",
                    pq.query_type()
                ))
            }
            Err(_) => {
                let mode = if self.state.proxy_state == ProxyState::Never {
                    PrepareMeta::FailedToParse
                } else {
                    PrepareMeta::Proxy
                };
                debug!(query = %Sensitive(&query), plan = ?mode, "Readyset failed to parse query");
                mode
            }
        }
    }

    /// Prepares a query on noria and upstream based on the provided PrepareMeta
    async fn do_prepare(
        &mut self,
        meta: &PrepareMeta,
        query: &str,
        data: DB::PrepareData<'_>,
        statement_type: PreparedStatementType,
        event: &mut QueryExecutionEvent,
    ) -> Result<PrepareResultInner<DB>, DB::Error> {
        match meta {
            PrepareMeta::Select(select_meta) => {
                self.mirror_prepare(select_meta, query, data, statement_type, event)
                    .await
            }
            PrepareMeta::Write { stmt } => {
                self.prepare_write(query, stmt, data, statement_type, event)
                    .await
            }
            PrepareMeta::Set { stmt } => {
                self.prepare_set(stmt, query, data, statement_type, event)
                    .await
            }
            PrepareMeta::Proxy
            | PrepareMeta::FailedToParse
            | PrepareMeta::FailedToRewrite(_)
            | PrepareMeta::Unimplemented(_)
            | PrepareMeta::Transaction { .. }
                if self.upstream.is_some() =>
            {
                let _t = event.start_upstream_timer();
                let res = self
                    .prepare_fallback(query, data, statement_type)
                    .await
                    .map(PrepareResultInner::Upstream);

                self.last_query = Some(QueryInfo {
                    destination: QueryDestination::Upstream,
                    noria_error: String::new(),
                });

                res
            }
            PrepareMeta::Proxy => unsupported!("No upstream, so query cannot be proxied"),
            PrepareMeta::Transaction { .. } => {
                unsupported!("No upstream, transactions not supported")
            }
            PrepareMeta::FailedToParse => unsupported!("Query failed to parse"),
            PrepareMeta::FailedToRewrite(e) | PrepareMeta::Unimplemented(e) => {
                Err(e.clone().into())
            }
        }
    }

    #[inline]
    fn create_prepared_statement(
        &mut self,
        prepare_meta: PrepareMeta,
        prep: PrepareResultInner<DB>,
        statement_id: StatementId,
    ) -> PreparedStatement<DB> {
        match prepare_meta {
            PrepareMeta::Write { stmt } | PrepareMeta::Transaction { stmt } => PreparedStatement {
                query_id: None,
                prep: PrepareResult::new(statement_id, prep),
                migration_state: MigrationState::Successful,
                execution_info: None,
                parsed_query: Some(Arc::new(stmt)),
                view_request: None,
                always: false,
            },
            PrepareMeta::Set { stmt } => PreparedStatement {
                query_id: None,
                prep: PrepareResult::new(statement_id, prep),
                migration_state: MigrationState::Successful,
                execution_info: None,
                parsed_query: Some(Arc::new(SqlQuery::Set(stmt))),
                view_request: None,
                always: false,
            },
            PrepareMeta::Select(PrepareSelectMeta {
                stmt,
                rewritten,
                always,
                ..
            }) => {
                let request =
                    ViewCreateRequest::new(rewritten, self.noria.schema_search_path().to_owned());
                let migration_state = self
                    .state
                    .query_status_cache
                    .query_migration_state(&request);
                PreparedStatement {
                    query_id: Some(migration_state.0),
                    prep: PrepareResult::new(statement_id, prep),
                    migration_state: migration_state.1,
                    execution_info: None,
                    parsed_query: Some(Arc::new(SqlQuery::Select(stmt))),
                    view_request: Some(request),
                    always,
                }
            }
            PrepareMeta::Proxy
            | PrepareMeta::FailedToParse
            | PrepareMeta::FailedToRewrite(..)
            | PrepareMeta::Unimplemented(..) => PreparedStatement {
                query_id: None,
                prep: PrepareResult::new(statement_id, prep),
                migration_state: MigrationState::Successful,
                execution_info: None,
                parsed_query: None,
                view_request: None,
                always: false,
            },
        }
    }

    /// Prepares `query` to be executed later using the reader/writer belonging
    /// to the calling `Backend` struct and adds the prepared query
    /// to the calling struct's map of prepared queries with a unique id.
    pub async fn prepare(
        &mut self,
        query: &str,
        data: DB::PrepareData<'_>,
        statement_type: PreparedStatementType,
    ) -> Result<&PrepareResult<DB>, DB::Error> {
        // early return if we're preparing an unnamed statement that we already have the metadata for.
        // Also, don't bother to record an event for this query as it's not really useful data.
        if matches!(statement_type, PreparedStatementType::Unnamed)
            && self.state.unnamed_prepared_statements.contains_key(query)
        {
            let id = self.state.unnamed_prepared_statements[query];
            return Ok(&self.state.prepared_statements[id].prep);
        }

        let mut query_event = QueryExecutionEvent::new(EventType::Prepare);
        let meta = self.plan_prepare(query, &mut query_event).await;
        let prep = self
            .do_prepare(&meta, query, data, statement_type, &mut query_event)
            .await?;

        let next_id = self
            .state
            .prepared_statements
            .vacant_key()
            .try_into()
            .expect("Cannot prepare more than u32::MAX statements with a single connection");
        let prepared_statement = self.create_prepared_statement(meta, prep, next_id);
        let statement_id = self.state.prepared_statements.insert(prepared_statement);
        assert_eq!(next_id, statement_id as u32);

        if matches!(statement_type, PreparedStatementType::Unnamed) {
            // For unnamed prepared statements, store the query string mapping
            self.state
                .unnamed_prepared_statements
                .insert(query.to_string(), statement_id);
        }

        // we've already put the prepared statement in the cache, but dig it a reference
        // for both query logging and returning to the caller.
        let prepared_statement = &self.state.prepared_statements[statement_id];

        if let Some(QueryLogMode::Verbose) = self.query_log_mode {
            // We only use the full query in verbose mode, so avoid cloning if we don't need to
            if let Some(parsed) = &prepared_statement.parsed_query {
                query_event.query = Some(parsed.clone());
            }
        }

        query_event.query_id = prepared_statement.query_id.into();
        let query_log_sender = self.query_log_sender.clone();
        let slowlog = self.settings.slowlog;
        log_query(
            query_log_sender.as_ref(),
            query_event,
            slowlog,
            self.settings.dialect,
        );

        Ok(&prepared_statement.prep)
    }

    /// Executes a prepared statement on ReadySet
    async fn execute_noria<'a>(
        noria: &'a mut NoriaConnector,
        prep: &noria_connector::PrepareResult,
        params: &[DfValue],
        event: &mut QueryExecutionEvent,
    ) -> ReadySetResult<QueryResult<'a, DB>> {
        use noria_connector::PrepareResult::*;

        event.destination = Some(QueryDestination::Readyset);

        let res = match prep {
            Select { statement, .. } => {
                let ctx = ExecuteSelectContext::Prepared {
                    ps: statement,
                    params,
                };
                noria.execute_select(ctx, event).await
            }
            Insert { statement, .. } => noria.execute_prepared_insert(statement, params).await,
            Update { statement, .. } => noria.execute_prepared_update(statement, params).await,
            Delete { statement, .. } => noria.execute_prepared_delete(statement, params).await,
            // we do not (yet) handle SET commands internal to readyset.
            Set { .. } => Ok(noria_connector::QueryResult::Empty),
        }
        .map(Into::into);

        if let Err(e) = &res {
            event.set_noria_error(e);
        }

        res
    }

    /// Execute a prepared statement on ReadySet
    async fn execute_upstream<'a>(
        upstream: &'a mut Option<DB>,
        prep: &UpstreamPrepare<DB>,
        params: &[DfValue],
        exec_meta: DB::ExecMeta<'_>,
        event: &mut QueryExecutionEvent,
        is_fallback: bool,
    ) -> Result<QueryResult<'a, DB>, DB::Error> {
        let upstream = upstream.as_mut().ok_or_else(|| {
            ReadySetError::Internal("This condition requires an upstream connector".to_string())
        })?;

        if is_fallback {
            event.destination = Some(QueryDestination::ReadysetThenUpstream);
        } else {
            event.destination = Some(QueryDestination::Upstream);
        }

        let _t = event.start_upstream_timer();

        upstream
            .execute(&prep.statement_id, params, exec_meta)
            .await
            .map(|r| QueryResult::Upstream(r))
    }

    /// Execute on ReadySet, and if fails execute on upstream
    #[allow(clippy::too_many_arguments)] // meh.
    async fn execute_cascade<'a>(
        noria: &'a mut NoriaConnector,
        upstream: &'a mut Option<DB>,
        noria_prep: &noria_connector::PrepareResult,
        upstream_prep: &UpstreamPrepare<DB>,
        params: &[DfValue],
        exec_meta: DB::ExecMeta<'_>,
        ex_info: Option<&mut ExecutionInfo>,
        event: &mut QueryExecutionEvent,
    ) -> Result<QueryResult<'a, DB>, DB::Error> {
        let noria_res = Self::execute_noria(noria, noria_prep, params, event).await;
        match noria_res {
            Ok(noria_ok) => {
                if let Some(info) = ex_info {
                    info.execute_succeeded();
                }
                Ok(noria_ok)
            }
            Err(noria_err) => {
                if let Some(info) = ex_info {
                    if noria_err.is_networking_related() {
                        info.execute_network_failure();
                    } else if noria_err.caused_by_data_type_conversion() {
                        // Consider queries that fail due to data type conversion errors as
                        // unsupported. These queries will likely fail on each query to noria,
                        // introducing increased latency.
                        info.execute_unsupported();
                    }
                }
                if !noria_err.any_cause(|e| {
                    matches!(
                        e,
                        ReadySetError::ReaderMissingKey
                            | ReadySetError::NoCacheForQuery
                            | ReadySetError::UnparseableQuery { .. }
                    )
                }) {
                    warn!(error = %noria_err,
                          "Error received from noria, sending query to fallback");
                }

                Self::execute_upstream(upstream, upstream_prep, params, exec_meta, event, true)
                    .await
            }
        }
    }

    /// Attempts to migrate a query on noria, after
    /// - the query was marked as `MigrationState::Successful` in the cache -or-
    /// - the epoch stored in `MigrationState::Inlined` advanced but the query is not yet prepared
    ///   on noria.
    ///
    /// If the migration is successful, the prepare result is updated with the noria result. If the
    /// state was previously `MigrationState::Pending`, it is updated to
    /// `MigrationState::Successful`.
    ///
    /// Returns an error if the statement is already prepared on noria.
    ///
    /// # Panics
    ///
    /// If the query is not in the `MigrationState::Pending` or `MigrationState::Inlined` state
    async fn update_noria_prepare(
        noria: &mut NoriaConnector,
        cached_entry: &mut PreparedStatement<DB>,
    ) -> ReadySetResult<()> {
        debug_assert!(
            cached_entry.migration_state.is_pending() || cached_entry.migration_state.is_inlined()
        );

        let upstream_prep: UpstreamPrepare<DB> = match &cached_entry.prep.inner {
            PrepareResultInner::Upstream(prep) => prep.clone(),
            _ => internal!("Update may only be called for Upstream prepares"),
        };

        let parsed_statement = cached_entry
            .parsed_query
            .as_ref()
            .expect("Cached entry for pending state");

        let noria_prep = match &**parsed_statement {
            SqlQuery::Select(stmt) => {
                noria
                    .prepare_select(
                        stmt.clone(),
                        false,
                        cached_entry
                            .view_request
                            .as_ref()
                            .map(|pr| pr.schema_search_path.clone()),
                    )
                    .await?
            }
            _ => internal!("Only SELECT statements can be pending migration"),
        };

        // At this point we got a successful noria prepare, so we want to replace the Upstream
        // result with a Both result
        cached_entry.prep = PrepareResult::new(
            cached_entry.prep.statement_id,
            PrepareResultInner::Both(noria_prep, upstream_prep),
        );
        // If the query was previously `Pending`, update to `Successful`. If it was inlined, we do
        // not update the migration state.
        if cached_entry.migration_state == MigrationState::Pending {
            cached_entry.migration_state = MigrationState::Successful;
        }

        Ok(())
    }

    /// Iterate over the cache of the prepared statements, and invalidate those that are
    /// equal to the one provided
    fn invalidate_prepared_statements_cache(&mut self, stmt: &ViewCreateRequest) {
        // Linear scan, but we shouldn't be doing it often, right?
        self.state
            .prepared_statements
            .iter_mut()
            .filter_map(
                |(
                    _,
                    PreparedStatement {
                        prep,
                        migration_state,
                        view_request,
                        ..
                    },
                )| {
                    if *migration_state == MigrationState::Successful
                        && view_request.as_ref() == Some(stmt)
                    {
                        *migration_state = MigrationState::Pending;
                        Some(prep)
                    } else {
                        None
                    }
                },
            )
            .for_each(|ps| ps.make_upstream_only());
    }

    /// Executes a prepared statement identified by `id` with parameters specified by the client
    /// `params`.
    /// A [`QueryExecutionEvent`], is used to track metrics and behavior scoped to the
    /// execute operation.
    #[inline]
    pub async fn execute(
        &mut self,
        id: u32,
        params: &[DfValue],
        exec_meta: DB::ExecMeta<'_>,
    ) -> Result<QueryResult<'_, DB>, DB::Error> {
        self.last_query = None;
        let cached_statement = self
            .state
            .prepared_statements
            .get_mut(id as _)
            .ok_or(PreparedStatementMissing { statement_id: id })?;

        let mut event = QueryExecutionEvent::new(EventType::Execute);
        event.query.clone_from(&cached_statement.parsed_query);
        event.query_id = cached_statement.query_id.into();

        let upstream = &mut self.upstream;
        let noria = &mut self.noria;

        // If the query is pending, check the query status cache to see if it is now successful.
        //
        // If the query is inlined, we have to check the epoch of the current state in the query
        // status cache to see if we should prepare the statement again.
        if cached_statement.migration_state.is_pending()
            || cached_statement.migration_state.is_inlined()
        {
            // We got a statement with a pending migration, we want to check if migration is
            // finished by now
            let new_migration_state = self
                .state
                .query_status_cache
                .query_migration_state(cached_statement.as_view_request()?)
                .1;

            if new_migration_state == MigrationState::Successful {
                // Attempt to prepare on ReadySet
                let _ = Self::update_noria_prepare(noria, cached_statement).await;
            } else if let MigrationState::Inlined(new_state) = new_migration_state {
                if let MigrationState::Inlined(ref old_state) = cached_statement.migration_state {
                    // if the epoch has advanced, then we've made changes to the inlined caches so
                    // we should refresh the view cache and prepare if necessary.
                    if new_state.epoch > old_state.epoch {
                        let view_request = cached_statement.as_view_request()?;
                        // Request a new view from ReadySet.
                        let updated_view_cache = noria
                            .update_view_cache(
                                &view_request.statement,
                                Some(view_request.schema_search_path.clone()),
                                false, // create_if_not_exists
                                true,  // is_prepared
                            )
                            .await
                            .is_ok();
                        // If we got a new view from ReadySet and we have only prepared against
                        // upstream, prepare the statement against ReadySet.
                        //
                        // Update the migration state if we updated the view_cache and, if
                        // necessary, the PrepareResult.
                        if updated_view_cache
                            && matches!(
                                cached_statement.prep.inner,
                                PrepareResultInner::Upstream(_)
                            )
                        {
                            if Self::update_noria_prepare(noria, cached_statement)
                                .await
                                .is_ok()
                            {
                                cached_statement.migration_state =
                                    MigrationState::Inlined(new_state);
                            }
                        } else if updated_view_cache {
                            cached_statement.migration_state = MigrationState::Inlined(new_state);
                        }
                    }
                }
            }
        }

        let should_fallback = {
            if cached_statement.always {
                false
            } else {
                let is_recovering = cached_statement.in_fallback_recovery(
                    self.settings.query_max_failure_duration,
                    self.settings.fallback_recovery_duration,
                );

                let always_readyset = cached_statement
                    .view_request
                    .as_ref()
                    .map(|stmt| self.state.query_status_cache.query_status(stmt).always)
                    .unwrap_or(false);

                if cached_statement.is_unsupported_execute() {
                    true
                } else if always_readyset {
                    false
                } else {
                    is_recovering || self.state.proxy_state.should_proxy()
                }
            }
        };

        let result = match &cached_statement.prep.inner {
            PrepareResultInner::Noria(prep) => Self::execute_noria(noria, prep, params, &mut event)
                .await
                .map_err(Into::into),
            PrepareResultInner::Upstream(prep) => {
                // No inlined caches for this query exist if we are only prepared on upstream.
                if cached_statement.migration_state.is_inlined() {
                    self.state
                        .query_status_cache
                        .inlined_cache_miss(cached_statement.as_view_request()?, params.to_vec())
                }
                Self::execute_upstream(upstream, prep, params, exec_meta, &mut event, false).await
            }
            PrepareResultInner::Both(.., uprep) if should_fallback => {
                Self::execute_upstream(upstream, uprep, params, exec_meta, &mut event, false).await
            }
            PrepareResultInner::Both(nprep, uprep) => {
                if cached_statement.execution_info.is_none() {
                    cached_statement.execution_info = Some(ExecutionInfo {
                        state: ExecutionState::Failed,
                        last_transition_time: Instant::now(),
                    });
                }
                Self::execute_cascade(
                    noria,
                    upstream,
                    nprep,
                    uprep,
                    params,
                    exec_meta,
                    cached_statement.execution_info.as_mut(),
                    &mut event,
                )
                .await
            }
        };

        if let Some(q) = &cached_statement.parsed_query {
            Self::update_transaction_boundaries(&mut self.state.proxy_state, q.as_ref());
        }

        if let Some(e) = event.noria_error.as_ref() {
            if e.caused_by_view_not_found() {
                // This can happen during cascade execution if the noria query was removed from
                // another connection
                cached_statement.prep.make_upstream_only();
            } else if e.caused_by_unsupported() {
                // On an unsupported execute we update the query migration state to be unsupported.
                self.state.query_status_cache.update_query_migration_state(
                    cached_statement.as_view_request()?,
                    MigrationState::Unsupported(e.unsupported_cause().unwrap_or_default()),
                );
            } else if matches!(e, ReadySetError::NoCacheForQuery) {
                self.state
                    .query_status_cache
                    .inlined_cache_miss(cached_statement.as_view_request()?, params.to_vec())
            }
        };

        self.last_query = event.destination.map(|d| QueryInfo {
            destination: d,
            noria_error: event
                .noria_error
                .as_ref()
                .map(|e| e.to_string())
                .unwrap_or_default(),
        });
        log_query(
            self.query_log_sender.as_ref(),
            event,
            self.settings.slowlog,
            self.settings.dialect,
        );

        result
    }

    pub async fn remove_statement(&mut self, deallocate_id: DeallocateId) -> Result<(), DB::Error> {
        // in all cases, we need to call upstream.remove_statement(), but in the case
        // of a Numeric id and it's in self.state.prepared_statements, we need to use
        // that id instead when we call upstream.remove_statement().
        let mut dealloc_id = deallocate_id.clone();
        match deallocate_id {
            DeallocateId::Numeric(id) => {
                if let Some(statement) = self.state.prepared_statements.try_remove(id as usize) {
                    if let Some(ur) = statement.prep.into_upstream() {
                        dealloc_id = DeallocateId::Numeric(ur.statement_id);
                    } else {
                        // this is the case where a prepared statement was created for readyset
                        // use, and not prepared/executed on the upstream.
                        return Ok(());
                    }
                }
            }
            DeallocateId::All => {
                self.state.prepared_statements.clear();
            }
            DeallocateId::Named(_) => {}
        }

        if let Some(upstream) = &mut self.upstream {
            upstream.remove_statement(dealloc_id).await?;
        }
        Ok(())
    }

    /// Should only be called with a SqlQuery that is of type StartTransaction, Commit, or
    /// Rollback. Used to handle transaction boundary queries.
    fn update_transaction_boundaries(proxy_state: &mut ProxyState, query: &SqlQuery) {
        match query {
            SqlQuery::StartTransaction(_) => {
                proxy_state.start_transaction();
            }
            SqlQuery::Commit(_) => {
                proxy_state.end_transaction();
            }
            SqlQuery::Rollback(_) => {
                proxy_state.end_transaction();
            }
            _ => (),
        }
    }

    /// Should only be called with a SqlQuery that is of type StartTransaction, Commit, or
    /// Rollback. Used to handle transaction boundary queries.
    async fn handle_transaction_boundaries<'a>(
        upstream: Option<&'a mut DB>,
        proxy_state: &mut ProxyState,
        query: &SqlQuery,
    ) -> Result<QueryResult<'a, DB>, DB::Error> {
        let upstream = upstream.ok_or_else(|| {
            ReadySetError::Internal("This case requires an upstream connector".to_string())
        })?;

        match query {
            SqlQuery::StartTransaction(inner) => {
                let result = QueryResult::Upstream(upstream.start_tx(inner).await?);
                proxy_state.start_transaction();
                Ok(result)
            }
            SqlQuery::Commit(_) => {
                let result = QueryResult::Upstream(upstream.commit().await?);
                proxy_state.end_transaction();
                Ok(result)
            }
            SqlQuery::Rollback(_) => {
                let result = QueryResult::Upstream(upstream.rollback().await?);
                proxy_state.end_transaction();
                Ok(result)
            }
            _ => {
                error!(
                    "handle_transaction_boundary was called with a SqlQuery that was not of type \
                     StartTransaction, Commit, or Rollback"
                );
                internal!(
                    "handle_transaction_boundary was called with a SqlQuery that was not of type \
                     StartTransaction, Commit, or Rollback"
                );
            }
        }
    }

    /// Generates response to the `EXPLAIN LAST STATEMENT` query
    fn explain_last_statement(&self) -> ReadySetResult<noria_connector::QueryResult<'static>> {
        let (destination, error) = self
            .last_query
            .as_ref()
            .map(|info| {
                (
                    info.destination.to_string(),
                    match &info.noria_error {
                        s if s.is_empty() => "ok".to_string(),
                        s => s.clone(),
                    },
                )
            })
            .unwrap_or_else(|| ("unknown".to_string(), "ok".to_string()));

        Ok(noria_connector::QueryResult::Meta(vec![
            ("Query_destination", destination).into(),
            ("Readyset_error", error).into(),
        ]))
    }

    /// Forwards a `CREATE CACHE` request to ReadySet
    async fn create_cached_query(
        &mut self,
        name: &mut Option<Relation>,
        mut stmt: SelectStatement,
        override_schema_search_path: Option<Vec<SqlIdentifier>>,
        always: bool,
        concurrently: bool,
    ) -> ReadySetResult<noria_connector::QueryResult<'static>> {
        adapter_rewrites::process_query(&mut stmt, self.noria.rewrite_params())?;
        let query_id = QueryId::from_select(&stmt, self.noria.schema_search_path());
        let requested_name = name.clone();
        let name = match name {
            Some(name) => &*name,
            None => {
                *name = Some(query_id.into());
                name.as_ref().unwrap()
            }
        };

        // If we have existing caches with the same query_id or name, drop them first.
        for CacheExpr {
            name,
            statement,
            query_id,
            ..
        } in self
            .noria
            .verbose_views(Some(query_id), requested_name.as_ref())
            .await?
        {
            warn!(
                query_id = %query_id,
                name = %name.display(DB::SQL_DIALECT),
                statement = %Sensitive(&statement.display(self.settings.dialect)),
                "Dropping previously cached query",
            );
            self.drop_cached_query(&name).await?;
        }

        // Now migrate the new query
        let migration_state = match self
            .noria
            .handle_create_cached_query(
                Some(name),
                &stmt,
                override_schema_search_path,
                always,
                concurrently,
            )
            .await
        {
            Ok(None) => MigrationState::Successful,
            Ok(Some(id)) => {
                return Ok(noria_connector::QueryResult::Meta(vec![(
                    "Migration Id".to_string(),
                    id.to_string(),
                )
                    .into()]))
            }
            // If the query fails because it contains unsupported placeholders, then mark it as an
            // inlined query in the query status cache.
            Err(e) => {
                if let Some(placeholders) = e.unsupported_placeholders_cause() {
                    let placeholders = Vec1::try_from(
                        placeholders
                            .into_iter()
                            .map(|p| p as PlaceholderIdx)
                            .collect::<Vec<_>>(),
                    )
                    .unwrap();
                    if self.settings.placeholder_inlining {
                        MigrationState::Inlined(InlinedState::from_placeholders(placeholders))
                    } else {
                        return Err(e);
                    }
                } else {
                    return Err(e);
                }
            }
        };
        self.state.query_status_cache.update_query_migration_state(
            &ViewCreateRequest::new(stmt.clone(), self.noria.schema_search_path().to_owned()),
            migration_state,
        );
        self.state.query_status_cache.always_attempt_readyset(
            &ViewCreateRequest::new(stmt.clone(), self.noria.schema_search_path().to_owned()),
            always,
        );
        Ok(noria_connector::QueryResult::Empty)
    }

    /// Forwards an `EXPLAIN CREATE CACHE` request to ReadySet. Where possible, this method performs
    /// the dry run in the request path so we can return a result to the client immediately. If we
    /// encounter an error we think might be transient or if the query is unsupported and we might
    /// be able to inline some of its parameters (and parameter inlining is enabled), we will set
    /// the status of the migration to "pending"/"inlined" and allow the migration to proceed in
    /// the background. In these cases, it is the responsibility of the client to poll for the
    /// final status of the query.
    async fn explain_create_cache(
        &mut self,
        id: QueryId,
        req: ViewCreateRequest,
        migration_state: Option<MigrationState>,
    ) -> ReadySetResult<noria_connector::QueryResult<'static>> {
        let (supported, migration_state) = match migration_state {
            Some(m @ MigrationState::Unsupported(_)) => ("no", m),
            // If the migration state is "Inlined", we need to let the migration handler process
            // the inlined migrations in the background until we can report whether the query is
            // supported with certainty
            Some(m @ MigrationState::Inlined(_)) | Some(m @ MigrationState::Pending) => {
                ("pending", m)
            }
            Some(m @ MigrationState::Successful) => ("cached", m),
            Some(m @ MigrationState::DryRunSucceeded) => ("yes", m),
            // If we don't already have a migration state for the query, we do a dry run
            None => {
                match self.noria.handle_dry_run(id, &req).await {
                    Ok(_) => ("yes", MigrationState::DryRunSucceeded),
                    // If the root cause of the error is that the query is unsupported, we can
                    // just convey that to the client up front
                    Err(e) if e.caused_by_unsupported() => (
                        "no",
                        MigrationState::Unsupported(e.unsupported_cause().unwrap_or_default()),
                    ),
                    Err(e) => return Err(e),
                }
            }
        };

        let results = vec![
            MetaVariable {
                name: "query id".into(),
                value: id.to_string(),
            },
            MetaVariable {
                name: "query".into(),
                value: req.statement.display(self.settings.dialect).to_string(),
            },
            MetaVariable {
                name: "readyset supported".into(),
                value: supported.into(),
            },
        ];

        self.state
            .query_status_cache
            .update_query_migration_state(&req, migration_state);

        Ok(noria_connector::QueryResult::Meta(results))
    }

    /// Forwards a `DROP CACHE` request to noria
    async fn drop_cached_query(
        &mut self,
        name: &Relation,
    ) -> ReadySetResult<noria_connector::QueryResult<'static>> {
        let maybe_view_request = self.noria.view_create_request_from_name(name).await;
        let result = self.noria.drop_view(name).await?;
        if let Some(view_request) = maybe_view_request {
            self.state
                .query_status_cache
                .update_query_migration_state(&view_request, MigrationState::Pending);
            self.state
                .query_status_cache
                .always_attempt_readyset(&view_request, false);
            self.invalidate_prepared_statements_cache(&view_request);
        }
        Ok(noria_connector::QueryResult::Delete {
            num_rows_deleted: result,
        })
    }

    /// Forwards a `DROP ALL CACHES` request to noria
    async fn drop_all_caches(&mut self) -> ReadySetResult<noria_connector::QueryResult<'static>> {
        self.authority.remove_all_cache_ddl_requests().await?;
        self.noria.drop_all_caches().await?;
        self.state.query_status_cache.clear();
        self.state.prepared_statements.iter_mut().for_each(
            |(
                _,
                PreparedStatement {
                    prep,
                    migration_state,
                    ..
                },
            )| {
                if *migration_state == MigrationState::Successful {
                    *migration_state = MigrationState::Pending;
                }
                prep.make_upstream_only();
            },
        );
        Ok(noria_connector::QueryResult::Empty)
    }

    /// Handles a `DROP ALL PROXIED QUERIES` request
    async fn drop_all_proxied_queries(
        &mut self,
    ) -> ReadySetResult<noria_connector::QueryResult<'static>> {
        self.state.query_status_cache.clear_proxied_queries();
        Ok(noria_connector::QueryResult::Empty)
    }

    /// Responds to a `SHOW PROXIED QUERIES` query
    async fn show_proxied_queries(
        &mut self,
        query_id: &Option<String>,
        only_supported: bool,
        limit: Option<u64>,
    ) -> ReadySetResult<noria_connector::QueryResult<'static>> {
        let mut queries = self.state.query_status_cache.deny_list();
        if let Some(q_id) = query_id {
            queries.retain(|q| &q.id.to_string() == q_id);
        }

        if only_supported {
            queries.retain(|q| q.status.migration_state.is_supported());
        }

        let select_schema = if let Some(handle) = self.metrics_handle.as_mut() {
            // Must snapshot to get the latest metrics
            handle.snapshot_counters(readyset_client_metrics::DatabaseType::Upstream);

            let mut select_schema =
                create_dummy_schema!("query id", "proxied query", "readyset supported");

            // Add count separately with a different type (UnsignedInt)
            let count_schema = ColumnSchema {
                column: ast::Column {
                    name: "count".into(),
                    table: None,
                },
                column_type: DfType::UnsignedInt,
                base: None,
            };
            select_schema.schema.to_mut().push(count_schema);
            select_schema.columns.to_mut().push("count".into());

            select_schema
        } else {
            create_dummy_schema!("query id", "proxied query", "readyset supported")
        };

        let mut data = queries
            .into_iter()
            .map(|DeniedQuery { id, query, status }| {
                let s = match status.migration_state {
                    MigrationState::DryRunSucceeded | MigrationState::Successful => {
                        "yes".to_string()
                    }
                    MigrationState::Pending | MigrationState::Inlined(_) => "pending".to_string(),
                    MigrationState::Unsupported(reason) if reason.is_empty() => {
                        "unsupported: unknown reason".to_string()
                    }
                    MigrationState::Unsupported(reason) => format!("unsupported: {reason}"),
                };

                let mut row = vec![
                    DfValue::from(id.to_string()),
                    DfValue::from(Self::format_query_text(
                        query.display(DB::SQL_DIALECT).to_string(),
                    )),
                    DfValue::from(s),
                ];

                // Append metrics if we have them
                if let Some(handle) = self.metrics_handle.as_ref() {
                    let MetricsSummary { sample_count } =
                        handle.metrics_summary(id.to_string()).unwrap_or_default();
                    row.push(DfValue::UnsignedInt(sample_count));
                }

                row
            })
            .collect::<Vec<_>>();

        data.sort_by(|a, b| {
            let status_order = |s: &str| match s {
                "yes" => 0,
                // we sometimes provide the reason for unsupported queries
                // like so "unsupported: xyz"
                unsupported if unsupported.starts_with("unsupported") => 1,
                "pending" => 2,
                _ => 3,
            };

            let a_status = status_order(&a[2].to_string());
            let b_status = status_order(&b[2].to_string());

            // If we don't have counts from metrics, give them all the same count for sorting
            // purposes
            let a_count = match a.get(3) {
                Some(DfValue::UnsignedInt(val)) => *val,
                _ => 0,
            };

            let b_count = match b.get(3) {
                Some(DfValue::UnsignedInt(val)) => *val,
                _ => 0,
            };

            // Reverse for descending order
            match a_status.cmp(&b_status) {
                std::cmp::Ordering::Equal => b_count.cmp(&a_count),
                other => other,
            }
        });

        if let Some(limit) = limit {
            data.truncate(limit as usize);
        }

        Ok(noria_connector::QueryResult::from_owned(
            select_schema,
            vec![Results::new(data)],
        ))
    }

    /// Responds to a `SHOW CACHES` query
    async fn show_caches(
        &mut self,
        query_id: Option<&str>,
    ) -> ReadySetResult<noria_connector::QueryResult<'static>> {
        let query_id = match query_id {
            // Bail if query_id is specified and invalid.
            Some(query_id) => Some(query_id.parse()?),
            None => None,
        };

        let views = self.noria.verbose_views(query_id, None).await?;

        let select_schema = if let Some(handle) = self.metrics_handle.as_mut() {
            // Must snapshot histograms to get the latest metrics
            handle.snapshot_counters(readyset_client_metrics::DatabaseType::ReadySet);
            create_dummy_schema!(
                "query id",
                "cache name",
                "query text",
                "fallback behavior",
                "count"
            )
        } else {
            create_dummy_schema!("query id", "cache name", "query text", "fallback behavior")
        };

        // Get the cache name for each query from the view cache
        let mut results: Vec<Vec<DfValue>> = vec![];
        for view in views {
            let mut row: Vec<DfValue> = vec![
                view.query_id.to_string().into(),
                view.name.display_unquoted().to_string().into(),
                Self::format_query_text(view.statement.display(DB::SQL_DIALECT).to_string()).into(),
                if view.always {
                    "no fallback".into()
                } else {
                    "fallback allowed".into()
                },
            ];

            // Append metrics if we have them
            if let Some(handle) = self.metrics_handle.as_ref() {
                let MetricsSummary { sample_count } = handle
                    .metrics_summary(view.query_id.to_string())
                    .unwrap_or_default();
                row.push(DfValue::from(format!("{sample_count}")));
            }

            results.push(row);
        }

        Ok(noria_connector::QueryResult::from_owned(
            select_schema,
            vec![Results::new(results)],
        ))
    }

    fn readyset_adapter_status(&self) -> ReadySetResult<noria_connector::QueryResult<'static>> {
        let mut statuses = match self.metrics_handle.as_ref() {
            Some(handle) => handle.readyset_status(),
            None => vec![],
        };
        let time_ms = self
            .adapter_start_time
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        statuses.push((
            "Process start time".to_string(),
            time_or_null(Some(time_ms)),
        ));

        Ok(noria_connector::QueryResult::MetaVariables(
            statuses.into_iter().map(MetaVariable::from).collect(),
        ))
    }

    async fn query_readyset_extensions<'a>(
        &'a mut self,
        query: &'a SqlQuery,
        event: &mut QueryExecutionEvent,
    ) -> ReadySetResult<noria_connector::QueryResult<'static>> {
        event.sql_type = SqlQueryType::Other;
        event.destination = Some(QueryDestination::Readyset);

        let start = Instant::now();

        let res = match query {
            SqlQuery::Explain(ExplainStatement::LastStatement) => self.explain_last_statement(),
            SqlQuery::Explain(ExplainStatement::Graphviz {
                simplified: _,
                for_cache,
            }) => self.noria.graphviz(for_cache.clone()).await,
            SqlQuery::Explain(ExplainStatement::Domains) => self.noria.explain_domains().await,
            SqlQuery::Explain(ExplainStatement::Caches) => self.explain_caches().await,
            SqlQuery::Explain(ExplainStatement::Materializations) => {
                self.noria.explain_materializations().await
            }
            SqlQuery::Explain(ExplainStatement::CreateCache { inner, .. }) => {
                match inner {
                    Ok(inner) => {
                        let view_request = match inner {
                            CacheInner::Statement(stmt) => {
                                let mut stmt = *stmt.clone();
                                adapter_rewrites::process_query(
                                    &mut stmt,
                                    self.noria.rewrite_params(),
                                )?;

                                ViewCreateRequest::new(
                                    stmt.clone(),
                                    self.noria.schema_search_path().to_owned(),
                                )
                            }
                            CacheInner::Id(id) => {
                                match self.state.query_status_cache.query(id.as_str()) {
                                    Some(q) => match q {
                                        Query::Parsed(view_request) => (*view_request).clone(),
                                        Query::ParseFailed(_, err) => {
                                            return Err(ReadySetError::UnparseableQuery(err));
                                        }
                                    },
                                    None => {
                                        return Err(ReadySetError::NoQueryForId {
                                            id: id.to_string(),
                                        })
                                    }
                                }
                            }
                        };

                        let (id, mut migration_state) = self
                            .state
                            .query_status_cache
                            .try_query_migration_state(&view_request);

                        // If the QSC didn't have this query, check with the controller to see if a
                        // view already exists there
                        if migration_state.is_none()
                            && self
                                .noria
                                .get_view_name(view_request.clone())
                                .await?
                                .is_some()
                        {
                            migration_state = Some(MigrationState::Successful);
                        }

                        self.explain_create_cache(id, view_request, migration_state)
                            .await
                    }
                    Err(err) => Err(ReadySetError::UnparseableQuery(err.clone())),
                }
            }
            SqlQuery::CreateCache(CreateCacheStatement {
                name,
                inner,
                always,
                concurrently,
                unparsed_create_cache_statement,
            }) => {
                if !self.allow_cache_ddl {
                    unsupported!("{}", UNSUPPORTED_CACHE_DDL_MSG);
                }
                let (stmt, search_path) = match inner {
                    Ok(CacheInner::Statement(st)) => Ok((*st.clone(), None)),
                    Ok(CacheInner::Id(id)) => {
                        match self.state.query_status_cache.query(id.as_str()) {
                            Some(q) => match q {
                                Query::Parsed(view_request) => Ok((
                                    view_request.statement.clone(),
                                    Some(view_request.schema_search_path.clone()),
                                )),
                                Query::ParseFailed(_, err) => {
                                    Err(ReadySetError::UnparseableQuery(err))
                                }
                            },
                            None => Err(ReadySetError::NoQueryForId { id: id.to_string() }),
                        }
                    }
                    Err(err) => Err(ReadySetError::UnparseableQuery(err.clone())),
                }?;

                // Log a telemetry event
                if let Some(ref telemetry_sender) = self.telemetry_sender {
                    if let Err(e) = telemetry_sender.send_event(TelemetryEvent::CreateCache) {
                        warn!(error = %e, "Failed to send CREATE CACHE metric");
                    }
                } else {
                    trace!("No telemetry sender. not sending metric for CREATE CACHE");
                }

                let ddl_req = if let Some(unparsed_create_cache_statement) =
                    unparsed_create_cache_statement
                {
                    let ddl_req = CacheDDLRequest {
                        unparsed_stmt: unparsed_create_cache_statement.clone(),
                        schema_search_path: self.noria.schema_search_path().to_owned(),
                        dialect: self.settings.dialect.into(),
                    };

                    self.authority
                        .add_cache_ddl_request(ddl_req.clone())
                        .await?;
                    Some(ddl_req)
                } else {
                    None
                };

                let mut name = name.clone();
                let res = self
                    .create_cached_query(&mut name, stmt, search_path, *always, *concurrently)
                    .await;
                // The extend_recipe may have failed, in which case we should remove our intention
                // to create this cache. Extend recipe waits a bit and then returns an
                // Ok(ExtendRecipeResult::Pending) if it is still creating a cache in the
                // background, so we don't remove the ddl request for timeouts.
                if let Err(e) = &res {
                    if let Some(ddl_req) = ddl_req {
                        let remove_res = retry_with_exponential_backoff!(
                            || async {
                                let ddl_req = ddl_req.clone();
                                self.authority.remove_cache_ddl_request(ddl_req).await
                            },
                            retries: 5,
                            delay: 1,
                            backoff: 2,
                        );
                        if remove_res.is_err() {
                            error!("Failed to remove stored 'create cache' request. It will be re-run if there is a backwards incompatible upgrade.");
                        }
                    }
                    error!(
                        name = %name.unwrap_or("".into()).display_unquoted(),
                        "Failed to create cache: {}",
                        e
                    );
                }
                res
            }
            SqlQuery::DropCache(drop_cache) => {
                if !self.allow_cache_ddl {
                    unsupported!("{}", UNSUPPORTED_CACHE_DDL_MSG)
                }
                let ddl_req = CacheDDLRequest {
                    unparsed_stmt: drop_cache.display_unquoted().to_string(),
                    // drop cache statements explicitly don't use a search path, as the only schema
                    // we need to resolve is the cache name.
                    schema_search_path: vec![],
                    dialect: self.settings.dialect.into(),
                };
                self.authority
                    .add_cache_ddl_request(ddl_req.clone())
                    .await?;
                let DropCacheStatement { name } = drop_cache;
                let res = self.drop_cached_query(name).await;
                // `drop_cached_query` may return an Err, but if the cache fails to be dropped for
                // certain reasons, we can also see an Ok(Delete) here with num_rows_deleted set to
                // 0.
                if res.is_err()
                    || matches!(
                        res,
                        Ok(noria_connector::QueryResult::Delete { num_rows_deleted }) if num_rows_deleted < 1
                    )
                {
                    let remove_res = retry_with_exponential_backoff!(
                        || async {
                            let ddl_req = ddl_req.clone();
                            self.authority.remove_cache_ddl_request(ddl_req).await
                        },
                        retries: 5,
                        delay: 1,
                        backoff: 2,
                    );
                    if remove_res.is_err() {
                        error!("Failed to remove stored 'drop cache' request. It will be re-run if there is a backwards incompatible upgrade");
                    }
                }
                res
            }
            SqlQuery::DropAllCaches(_) => {
                if !self.allow_cache_ddl {
                    unsupported!("{}", UNSUPPORTED_CACHE_DDL_MSG);
                }
                self.drop_all_caches().await
            }
            SqlQuery::DropAllProxiedQueries(_) => {
                if !self.allow_cache_ddl {
                    unsupported!("{}", UNSUPPORTED_CACHE_DDL_MSG);
                }
                self.drop_all_proxied_queries().await
            }
            SqlQuery::Show(ShowStatement::CachedQueries(query_id)) => {
                // Log a telemetry event
                if let Some(ref telemetry_sender) = self.telemetry_sender {
                    if let Err(e) = telemetry_sender.send_event(TelemetryEvent::ShowCaches) {
                        warn!(error = %e, "Failed to send SHOW CACHES metric");
                    }
                } else {
                    trace!("No telemetry sender. not sending metric for SHOW CACHES");
                }

                self.show_caches(query_id.as_deref()).await
            }
            SqlQuery::Show(ShowStatement::ReadySetStatus) => Ok(self
                .status_reporter
                .report_status()
                .await
                .into_query_result()),
            SqlQuery::Show(ShowStatement::ReadySetStatusAdapter) => self.readyset_adapter_status(),
            SqlQuery::Show(ShowStatement::ReadySetMigrationStatus(id)) => {
                self.noria.migration_status(*id).await
            }
            SqlQuery::Show(ShowStatement::ReadySetVersion) => readyset_version(),
            SqlQuery::Show(ShowStatement::ReadySetTables(options)) => {
                self.noria.table_statuses(options.all).await
            }
            SqlQuery::Show(ShowStatement::Connections) => self.show_connections(),
            SqlQuery::Show(ShowStatement::ProxiedQueries(proxied_queries_options)) => {
                // Log a telemetry event
                if let Some(ref telemetry_sender) = self.telemetry_sender {
                    if let Err(e) = telemetry_sender.send_event(TelemetryEvent::ShowProxiedQueries)
                    {
                        warn!(error = %e, "Failed to send SHOW PROXIED QUERIES metric");
                    }
                } else {
                    trace!("No telemetry sender. not sending metric for SHOW PROXIED QUERIES");
                }

                self.show_proxied_queries(
                    &proxied_queries_options.query_id,
                    proxied_queries_options.only_supported,
                    proxied_queries_options.limit,
                )
                .await
            }
            SqlQuery::Show(ShowStatement::Rls(_maybe_table)) => {
                unsupported!("SHOW RLS statement is not yet supported")
            }
            SqlQuery::AlterReadySet(AlterReadysetStatement::ResnapshotTable(stmt)) => {
                let mut table = stmt.table.clone();
                self.noria.resnapshot_table(&mut table).await
            }
            SqlQuery::AlterReadySet(AlterReadysetStatement::AddTables(stmt)) => {
                let mut tables = stmt.tables.clone();
                self.noria.add_filter_tables(&mut tables).await
            }
            SqlQuery::AlterReadySet(AlterReadysetStatement::EnterMaintenanceMode) => {
                self.noria.enter_maintenance_mode().await
            }
            SqlQuery::AlterReadySet(AlterReadysetStatement::ExitMaintenanceMode) => {
                self.noria.exit_maintenance_mode().await
            }
            SqlQuery::AlterReadySet(AlterReadysetStatement::SetLogLevel(directives)) => {
                match readyset_tracing::set_log_level(directives) {
                    Ok(()) => Ok(noria_connector::QueryResult::Empty),
                    Err(e) => Err(internal_err!("Failed to set log level: {e}")),
                }
            }
            SqlQuery::CreateRls(_create_rls) => {
                unsupported!("CREATE RLS statement is not yet supported")
            }
            SqlQuery::DropRls(_drop_rls) => {
                unsupported!("DROP RLS statement is not yet supported")
            }
            _ => Err(internal_err!("Provided query is not a Readyset extension")),
        };

        event.readyset_event = Some(ReadysetExecutionEvent::Other {
            duration: start.elapsed(),
        });

        res
    }

    #[allow(clippy::too_many_arguments)]
    async fn query_adhoc_select<'a>(
        noria: &'a mut NoriaConnector,
        upstream: Option<&'a mut DB>,
        settings: &BackendSettings,
        state: &mut BackendState<DB>,
        original_query: &'a str,
        view_request: &ViewCreateRequest,
        mut status: QueryStatus,
        event: &mut QueryExecutionEvent,
        processed_query_params: ProcessedQueryParams,
        sampler_tx: Option<&tokio::sync::mpsc::Sender<(QueryExecutionEvent, String)>>,
    ) -> Result<QueryResult<'a, DB>, DB::Error> {
        let original_status = status.clone();
        let did_work = if let Some(ref mut i) = status.execution_info {
            i.reset_if_exceeded_recovery(
                settings.query_max_failure_duration,
                settings.fallback_recovery_duration,
            )
        } else {
            false
        };

        // Test several conditions to see if we should proxy
        let upstream_exists = upstream.is_some();
        let proxy_out_of_band = settings.migration_mode != MigrationMode::InRequestPath
            && status.migration_state != MigrationState::Successful;
        let unsupported = matches!(&status.migration_state, MigrationState::Unsupported(_));
        let exceeded_network_failure = status
            .execution_info
            .as_mut()
            .map(|i| i.execute_network_failure_exceeded(settings.query_max_failure_duration))
            .unwrap_or(false);

        if !status.always
            && (upstream_exists && (proxy_out_of_band || unsupported || exceeded_network_failure))
        {
            if did_work {
                state.query_status_cache.update_transition_time(
                    view_request,
                    &status.execution_info.unwrap().last_transition_time,
                );
            }
            return Self::query_fallback(upstream, original_query, event).await;
        }

        event.destination = Some(QueryDestination::Readyset);
        let ctx = ExecuteSelectContext::AdHoc {
            statement: &view_request.statement,
            create_if_missing: settings.migration_mode == MigrationMode::InRequestPath,
            processed_query_params,
        };
        let res = noria.execute_select(ctx, event).await;
        if status.execution_info.is_none() {
            status.execution_info = Some(ExecutionInfo {
                state: ExecutionState::Failed,
                last_transition_time: Instant::now(),
            });
        }

        match res {
            Ok(noria_ok) => {
                // We managed to select on ReadySet, good for us
                status.migration_state = MigrationState::Successful;
                if let Some(i) = status.execution_info.as_mut() {
                    i.execute_succeeded()
                }
                if status != original_status {
                    state
                        .query_status_cache
                        .update_query_status(view_request, status);
                }
                // Enqueue the original query for background sampling if enabled.
                if let Some(tx) = sampler_tx {
                    let _ = tx.try_send((event.clone(), original_query.to_string()));
                }
                Ok(noria_ok.into())
            }
            Err(noria_err) => {
                event.set_noria_error(&noria_err);

                if let Some(i) = status.execution_info.as_mut() {
                    if noria_err.is_networking_related() {
                        i.execute_network_failure();
                    } else if noria_err.caused_by_view_destroyed() {
                        i.execute_dropped();
                    }
                }

                if noria_err.caused_by_view_not_found() {
                    status.migration_state = MigrationState::Pending;
                } else if noria_err.caused_by_unsupported() {
                    status.migration_state = MigrationState::Unsupported(
                        noria_err.unsupported_cause().unwrap_or_default(),
                    );
                };

                let always = status.always;

                if status != original_status {
                    state
                        .query_status_cache
                        .update_query_status(view_request, status);
                }

                // Try to execute on fallback if present, as long as query is not an `always`
                // query.
                match (always, upstream) {
                    (true, _) | (_, None) => {
                        // Enqueue the original query for background sampling if enabled.
                        if let Some(tx) = sampler_tx {
                            let _ = tx.try_send((event.clone(), original_query.to_string()));
                        }
                        Err(noria_err.into())
                    }
                    (false, Some(fallback)) => {
                        event.destination = Some(QueryDestination::ReadysetThenUpstream);
                        let _t = event.start_upstream_timer();
                        fallback
                            .query(original_query)
                            .await
                            .map(QueryResult::Upstream)
                    }
                }
            }
        }
    }

    /// Checks if noria should try to execute a given select and in the process mutates the
    /// supplied select statement by rewriting it.
    /// Returns whether noria should try the select, along with the query status if it was obtained
    /// during processing.
    fn noria_should_try_select(
        &self,
        q: &mut ViewCreateRequest,
    ) -> (bool, Option<QueryStatus>, Option<ProcessedQueryParams>) {
        match adapter_rewrites::process_query(&mut q.statement, self.noria.rewrite_params()) {
            Ok(processed_query_params) => {
                let s = self.state.query_status_cache.query_status(q);
                let should_try = if self.state.proxy_state.should_proxy() {
                    s.always
                } else {
                    true
                };
                (should_try, Some(s), Some(processed_query_params))
            }
            Err(e) => {
                warn!(
                    statement = %Sensitive(&q.statement.display(self.settings.dialect)),
                    error = e.to_string(),
                    "This statement could not be rewritten by Readyset",
                );
                (false, None, None)
            }
        }
    }

    /// Handles a parsed set statement by deferring to `Handler::handle_set_statement` and
    /// respecting `BackendSettings::unsupported_set_mode`. When the search path is changed
    /// (SetBehavior::SetSearchPath) or other sets need to be handled (certain variables being
    /// changed), the `noria` instance gets updated accordingly.
    ///
    /// - If upstream exists, valid set statements are forwarded to it.
    /// - If no upstream is present, statements are typically ignored.
    /// - Disallowed set statements always produce an error.
    fn handle_set(
        noria: &mut NoriaConnector,
        has_upstream: bool,
        settings: &BackendSettings,
        state: &mut BackendState<DB>,
        query: &str,
        set: &SetStatement,
        event: &mut QueryExecutionEvent,
    ) -> Result<(), DB::Error> {
        let SetBehavior {
            unsupported,
            proxy: _, // Basically ignored, caller will proxy unless we return an error
            set_autocommit,
            set_search_path,
            set_results_encoding,
        } = Handler::handle_set_statement(set);

        if unsupported {
            match settings.unsupported_set_mode {
                UnsupportedSetMode::Error => {
                    let e = ReadySetError::SetDisallowed {
                        statement: query.to_string(),
                    };
                    if has_upstream {
                        event.set_noria_error(&e);
                    }
                    error!(
                        set = %set.display(settings.dialect),
                        "received unsupported SET statement."
                    );
                    return Err(e.into());
                }
                UnsupportedSetMode::Proxy => {
                    warn!(
                        set = %set.display(settings.dialect),
                        "received unsupported SET statement."
                    );
                    state.proxy_state = ProxyState::ProxyAlways;
                }
                UnsupportedSetMode::Allow => {}
            }
        }
        if let Some(enabled) = set_autocommit {
            if !enabled {
                warn!(
                    set = %set.display(settings.dialect),
                    "Disabling autocommit is an anti-pattern for use with Readyset, as all queries would then be proxied upstream."
                );
            }

            match settings.unsupported_set_mode {
                UnsupportedSetMode::Error => {
                    if enabled {
                        state.proxy_state.set_autocommit(enabled);
                    } else {
                        let e = ReadySetError::SetDisallowed {
                            statement: query.to_string(),
                        };
                        if has_upstream {
                            event.set_noria_error(&e);
                        }
                        return Err(e.into());
                    }
                }
                UnsupportedSetMode::Proxy => {
                    state.proxy_state.set_autocommit(enabled);
                }
                // TODO: I'm not sure this is correct ....
                UnsupportedSetMode::Allow => {}
            }
        }
        if let Some(search_path) = set_search_path {
            trace!(?search_path, "Setting search_path");
            noria.set_schema_search_path(search_path);
        }
        if let Some(encoding) = set_results_encoding {
            trace!(?encoding, "Setting results_encoding");
            noria.set_results_encoding(encoding);
        }

        Ok(())
    }

    async fn query_adhoc_non_select<'a>(
        noria: &'a mut NoriaConnector,
        upstream: Option<&'a mut DB>,
        raw_query: &'a str,
        event: &mut QueryExecutionEvent,
        query: SqlQuery,
        settings: &BackendSettings,
        state: &mut BackendState<DB>,
    ) -> Result<QueryResult<'a, DB>, DB::Error> {
        match &query {
            SqlQuery::Set(s) => Self::handle_set(
                noria,
                upstream.is_some(),
                settings,
                state,
                raw_query,
                s,
                event,
            )?,
            SqlQuery::Use(UseStatement { database }) => {
                noria.set_schema_search_path(vec![database.clone()])
            }
            _ => (),
        }

        let res = {
            // Upstream reads are tried when noria reads produce an error. Upstream writes are done
            // by default when the upstream connector is present.
            if let Some(upstream) = upstream {
                match query {
                    SqlQuery::Select(_) => unreachable!("read path returns prior"),
                    SqlQuery::Insert(_) | SqlQuery::Update(_) | SqlQuery::Delete(_) => {
                        event.sql_type = SqlQueryType::Write;
                        event.destination = Some(QueryDestination::Upstream);
                        let _t = event.start_upstream_timer();

                        let query_result = upstream.query(raw_query).await;
                        query_result.map(QueryResult::Upstream)
                    }

                    SqlQuery::CreateDatabase(_)
                    | SqlQuery::CreateView(_)
                    | SqlQuery::CreateTable(_)
                    | SqlQuery::DropTable(_)
                    | SqlQuery::DropView(_)
                    | SqlQuery::AlterTable(_)
                    | SqlQuery::Truncate(_)
                    | SqlQuery::Use(_)
                    | SqlQuery::CreateIndex(_) => {
                        event.sql_type = SqlQueryType::Other;
                        upstream.query(raw_query).await.map(QueryResult::Upstream)
                    }
                    SqlQuery::RenameTable(_) => {
                        unsupported!("{} not yet supported", query.query_type());
                    }
                    SqlQuery::Set(_)
                    | SqlQuery::CompoundSelect(_)
                    | SqlQuery::Show(_)
                    | SqlQuery::Comment(_) => {
                        event.sql_type = SqlQueryType::Other;
                        upstream.query(raw_query).await.map(QueryResult::Upstream)
                    }

                    SqlQuery::StartTransaction(_) | SqlQuery::Commit(_) | SqlQuery::Rollback(_) => {
                        Self::handle_transaction_boundaries(
                            Some(upstream),
                            &mut state.proxy_state,
                            &query,
                        )
                        .await
                    }

                    SqlQuery::CreateCache(_)
                    | SqlQuery::Deallocate(_)
                    | SqlQuery::DropCache(_)
                    | SqlQuery::DropAllCaches(_)
                    | SqlQuery::DropAllProxiedQueries(_)
                    | SqlQuery::AlterReadySet(_)
                    | SqlQuery::Explain(_)
                    | SqlQuery::CreateRls(_)
                    | SqlQuery::DropRls(_) => {
                        unreachable!("path returns prior")
                    }
                }
            } else {
                event.destination = Some(QueryDestination::Readyset);
                let start = Instant::now();

                let res = match &query {
                    SqlQuery::Select(_) => unreachable!("read path returns prior"),
                    // CREATE VIEW will still trigger migrations with explicit-migrations enabled
                    SqlQuery::CreateView(q) => noria.handle_create_view(q).await,
                    SqlQuery::CreateTable(q) => noria.handle_table_operation(q.clone()).await,
                    SqlQuery::AlterTable(q) => noria.handle_table_operation(q.clone()).await,
                    SqlQuery::DropTable(q) => noria.handle_table_operation(q.clone()).await,
                    SqlQuery::DropView(q) => noria.handle_table_operation(q.clone()).await,
                    SqlQuery::Insert(q) => noria.handle_insert(q).await,
                    SqlQuery::Update(q) => noria.handle_update(q).await,
                    SqlQuery::Delete(q) => noria.handle_delete(q).await,
                    SqlQuery::Truncate(q) => noria.handle_truncate(q).await,
                    SqlQuery::Deallocate(_) => unreachable!("deallocate path returns prior"),

                    // Return an empty result as we are allowing unsupported set statements. Commit
                    // messages are dropped - we do not support transactions in noria standalone.
                    // We return an empty result set instead of an error to support test
                    // applications.
                    SqlQuery::Set(_)
                    | SqlQuery::Commit(_)
                    | SqlQuery::Use(_)
                    | SqlQuery::Comment(_) => Ok(noria_connector::QueryResult::Empty),
                    q => {
                        error!(query = ?q, "unsupported query");
                        unsupported!("query type unsupported: {q:?}");
                    }
                };

                event.readyset_event = Some(ReadysetExecutionEvent::Other {
                    duration: start.elapsed(),
                });
                event.noria_error = res.as_ref().err().cloned();
                Ok(QueryResult::Noria(res?))
            }
        };

        res
    }

    fn handle_deallocate_statement<'a>(stmt: DeallocateStatement) -> QueryResult<'a, DB> {
        let dealloc_id = match stmt.identifier {
            StatementIdentifier::SingleStatement(name) => DeallocateId::from(name.clone()),
            StatementIdentifier::AllStatements => DeallocateId::All,
        };
        QueryResult::Parser(ParsedCommand::Deallocate(dealloc_id))
    }

    /// Executes `query` using the reader/writer belonging to the calling `Backend` struct.
    #[inline]
    pub async fn query<'a>(&'a mut self, query: &'a str) -> Result<QueryResult<'a, DB>, DB::Error> {
        let mut event = QueryExecutionEvent::new(EventType::Query);
        let query_log_sender = self.query_log_sender.clone();
        let slowlog = self.settings.slowlog;

        let parse_result = {
            let _t = event.start_parse_timer();
            self.parse_query(query)
        };

        let result = match parse_result {
            // Parse error, but no fallback exists
            Err(e) if !self.has_fallback() => {
                error!("{}", e);
                event.set_noria_error(&e);
                Err(e.into())
            }
            // Parse error, send to fallback
            Err(e) => {
                if !matches!(
                    e,
                    ReadySetError::ReaderMissingKey
                        | ReadySetError::NoCacheForQuery
                        | ReadySetError::UnparseableQuery { .. }
                ) {
                    warn!(error = %e, "Error received from noria, sending query to fallback");
                    event.set_noria_error(&e);
                }
                let fallback_res =
                    Self::query_fallback(self.upstream.as_mut(), query, &mut event).await;
                if fallback_res.is_ok() {
                    let (id, _) = self
                        .state
                        .query_status_cache
                        .insert(Query::ParseFailed(query.to_string().into(), e.to_string()));
                    if let Some(ref telemetry_sender) = self.telemetry_sender {
                        if let Err(e) = telemetry_sender.send_event_with_payload(
                            TelemetryEvent::QueryParseFailed,
                            TelemetryBuilder::new()
                                .server_version(
                                    option_env!("CARGO_PKG_VERSION").unwrap_or_default(),
                                )
                                .query_id(id.to_string())
                                .build(),
                        ) {
                            warn!(error = %e, "Failed to send parse failed metric");
                        }
                    } else {
                        trace!("No telemetry sender. not sending metric for {query}");
                    }
                }
                fallback_res
            }
            // Check for COMMIT+ROLLBACK before we check whether we should proxy, since we need to
            // know when a COMMIT or ROLLBACK happens so we can leave `ProxyState::InTransaction`
            Ok(parsed_query @ (SqlQuery::Commit(_) | SqlQuery::Rollback(_))) => {
                Self::query_adhoc_non_select(
                    &mut self.noria,
                    self.upstream.as_mut(),
                    query,
                    &mut event,
                    parsed_query,
                    &self.settings,
                    &mut self.state,
                )
                .await
            }
            Ok(ref parsed_query) if parsed_query.is_readyset_extension() => self
                .query_readyset_extensions(parsed_query, &mut event)
                .await
                .map(Into::into)
                .map_err(Into::into),
            // SET autocommit=1 needs to be handled explicitly or it will end up getting proxied in
            // most cases.
            Ok(SqlQuery::Set(s))
                if Handler::handle_set_statement(&s).set_autocommit == Some(true) =>
            {
                Self::query_adhoc_non_select(
                    &mut self.noria,
                    self.upstream.as_mut(),
                    query,
                    &mut event,
                    SqlQuery::Set(s),
                    &self.settings,
                    &mut self.state,
                )
                .await
            }
            Ok(ref parsed_query) if Handler::requires_fallback(parsed_query) => {
                if !Handler::return_default_response(parsed_query) && self.has_fallback() {
                    if let SqlQuery::Select(stmt) = parsed_query {
                        event.sql_type = SqlQueryType::Read;
                        event.query = Some(Arc::new(parsed_query.clone()));
                        event.query_id = QueryIdWrapper::Calculated(QueryId::from_select(
                            stmt,
                            self.noria.schema_search_path(),
                        ));
                    }

                    // Query requires a fallback and we can send it to fallback
                    Self::query_fallback(self.upstream.as_mut(), query, &mut event).await
                } else {
                    // Query should return a default response or requires a fallback, but none is
                    // available
                    Handler::default_response(parsed_query)
                        .map(QueryResult::Noria)
                        .map_err(Into::into)
                }
            }
            Ok(SqlQuery::Select(stmt)) => {
                let mut view_request =
                    ViewCreateRequest::new(stmt, self.noria.schema_search_path().to_owned());

                event.sql_type = SqlQueryType::Read;

                if let Some(QueryLogMode::Verbose) = self.query_log_mode {
                    event.query = Some(Arc::new(SqlQuery::Select(view_request.statement.clone())));
                }

                // force the QueryLogger to recalculate the query_id, instead of doing it here
                // on the hot path as it will execute a rewrite pass over the query.
                event.query_id =
                    QueryIdWrapper::Uncalculated(self.noria.schema_search_path().into());

                // if should_try is true then status and params MUST be Some
                let (noria_should_try, status, processed_query_params) =
                    self.noria_should_try_select(&mut view_request);
                let sampler_tx = if !self.is_internal_connection {
                    self.sampler_tx.as_ref()
                } else {
                    None
                };
                if noria_should_try {
                    Self::query_adhoc_select(
                        &mut self.noria,
                        self.upstream.as_mut(),
                        &self.settings,
                        &mut self.state,
                        query,
                        &view_request,
                        status.unwrap(),
                        &mut event,
                        processed_query_params.unwrap(),
                        sampler_tx,
                    )
                    .await
                } else {
                    Self::query_fallback(self.upstream.as_mut(), query, &mut event).await
                }
            }
            Ok(SqlQuery::Deallocate(stmt)) => Ok(Self::handle_deallocate_statement(stmt)),
            Ok(_) if self.state.proxy_state.should_proxy() => {
                Self::query_fallback(self.upstream.as_mut(), query, &mut event).await
            }
            Ok(parsed_query) => {
                let result = Self::query_adhoc_non_select(
                    &mut self.noria,
                    self.upstream.as_mut(),
                    query,
                    &mut event,
                    parsed_query.clone(),
                    &self.settings,
                    &mut self.state,
                )
                .await;

                if let SqlQuery::DropTable(drop_stmt) = &parsed_query {
                    if result.is_ok() {
                        self.state
                            .query_status_cache
                            .invalidate_queries_referencing_tables(&drop_stmt.tables);
                    }
                }

                result
            }
        };

        self.last_query = event.destination.map(|d| QueryInfo {
            destination: d,
            noria_error: event
                .noria_error
                .as_ref()
                .map(|e| e.to_string())
                .unwrap_or_default(),
        });

        log_query(
            query_log_sender.as_ref(),
            event,
            slowlog,
            self.settings.dialect,
        );

        result
    }

    /// Whether or not we have fallback enabled.
    pub fn has_fallback(&self) -> bool {
        self.upstream.is_some()
    }

    /// Mark or unmark this backend connection as an internal ReadySet connection
    pub fn set_internal_connection(&mut self, is_internal: bool) {
        self.is_internal_connection = is_internal;
    }

    /// If we are using fallback, this will return the database that was in the original connection
    /// string, if it exists, otherwise it will return None. If we are not using fallback this will
    /// always return None.
    pub fn database(&self) -> Option<&str> {
        match &self.upstream {
            Some(db) => db.database(),
            None => None,
        }
    }

    fn parse_query(&mut self, query: &str) -> ReadySetResult<SqlQuery> {
        trace!(%query, "Parsing query");
        readyset_sql_parsing::parse_query_with_config(
            self.settings
                .parsing_preset
                .into_config()
                .log_only_selects(true),
            self.settings.dialect,
            query,
        )
        .map_err(Into::into)
    }

    pub fn does_require_authentication(&self) -> bool {
        self.settings.require_authentication
    }

    /// Gets a list of all `CREATE CACHE ...` statements
    async fn explain_caches(&mut self) -> ReadySetResult<noria_connector::QueryResult<'static>> {
        let results: Vec<Vec<DfValue>> = self
            .noria
            .list_create_cache_stmts()
            .await?
            .into_iter()
            .map(|s| vec![DfValue::from(s)])
            .collect();

        let select_schema = create_dummy_schema!("query text");

        Ok(noria_connector::QueryResult::from_owned(
            select_schema,
            vec![Results::new(results)],
        ))
    }

    /// Prettify queries above an arbitrary length.
    /// Don't do it for MySQL because the terminal client doesn't handle newlines.
    fn format_query_text(query: String) -> String {
        if DB::SQL_DIALECT != readyset_sql::Dialect::MySQL && query.len() > 40 {
            sqlformat::format(&query, &Default::default(), &Default::default())
        } else {
            query
        }
    }

    fn show_connections(&self) -> Result<noria_connector::QueryResult<'static>, ReadySetError> {
        let schema = SelectSchema {
            schema: Cow::Owned(vec![ColumnSchema {
                column: ast::Column {
                    name: "remote_addr".into(),
                    table: None,
                },
                column_type: DfType::DEFAULT_TEXT,
                base: None,
            }]),
            columns: Cow::Owned(vec!["remote_addr".into()]),
        };

        let data = self
            .connections
            .iter()
            .flat_map(|c| c.iter())
            .map(|conn| vec![conn.to_string().into()])
            .collect::<Vec<_>>();

        Ok(noria_connector::QueryResult::from_owned(
            schema,
            vec![Results::new(data)],
        ))
    }

    pub fn in_transaction(&self) -> bool {
        self.state.proxy_state.in_transaction()
    }
}

impl<DB, Handler> Drop for Backend<DB, Handler>
where
    DB: UpstreamDatabase,
{
    fn drop(&mut self) {
        if let Some(connections) = &self.connections {
            connections.remove(&self.client_addr);
        }
        metrics::gauge!(recorded::CONNECTED_CLIENTS).decrement(1.0);
        metrics::counter!(recorded::CLIENT_CONNECTIONS_CLOSED).increment(1);
    }
}

/// Offloads recording query metrics to a separate thread. Sends a
/// message over a mpsc channel.
fn log_query(
    sender: Option<&UnboundedSender<QueryExecutionEvent>>,
    event: QueryExecutionEvent,
    slowlog: bool,
    dialect: Dialect,
) {
    const SLOW_DURATION: std::time::Duration = std::time::Duration::from_millis(5);

    let readyset_duration = event
        .readyset_event
        .as_ref()
        .map(|e| e.duration())
        .unwrap_or_default();

    if slowlog
        && (event.upstream_duration.unwrap_or_default() > SLOW_DURATION
            || readyset_duration > SLOW_DURATION)
    {
        if let Some(query) = &event.query {
            warn!(
                query = %Sensitive(&query.display(dialect)),
                readyset_time = ?readyset_duration,
                upstream_time = ?event.upstream_duration,
                "slow query"
            );
        }
    }

    if let Some(sender) = sender {
        // Drop the error if something goes wrong with query logging.
        if let Err(e) = sender.send(event) {
            warn!("Error logging query with query logging enabled: {}", e);
        }
    }
}

fn readyset_version() -> ReadySetResult<noria_connector::QueryResult<'static>> {
    Ok(noria_connector::QueryResult::MetaWithHeader(
        <Vec<(String, String)>>::from(READYSET_VERSION.clone())
            .into_iter()
            .map(MetaVariable::from)
            .collect(),
    ))
}
