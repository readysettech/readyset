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
//! * Explicit migrations: only `CREATE CACHE` and `CREATE VIEW` will cause migrations.
//! A `CREATE PREPARED STATEMENT` will not cause a migration, and queries will go to upstream
//! fallback. Enabled with the `--query-caching=explicit` argument. However if a migration already
//! happened, we will use it.
//! * Async migration: prepared statements will be put in a [`QueryStatusCache`] and another
//! thread will perform migrations in the background. Once a statement finished migration it
//! will execute on ReadySet, while it is waiting for a migration to happen it will execute on
//! fallback. Enabled with the `--query-caching=async` flag.
//! * In request path: migrations will happen when either `CREATE CACHE` or
//! `CREATE PREPARED STATEMENT` are called. It is also the only available option when a
//! upstream fallback is not available.
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
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::convert::{TryFrom, TryInto};
use std::fmt::{self, Debug};
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::future::{self, OptionFuture};
use mysql_common::row::convert::{FromRow, FromRowError};
use nom_sql::{
    CacheInner, CreateCacheStatement, DeleteStatement, Dialect, DropCacheStatement,
    InsertStatement, Relation, SelectStatement, SetStatement, ShowStatement, SqlIdentifier,
    SqlQuery, UpdateStatement, UseStatement,
};
use readyset_client::consistency::Timestamp;
use readyset_client::query::*;
use readyset_client::results::Results;
use readyset_client::{ColumnSchema, ViewCreateRequest};
pub use readyset_client_metrics::QueryDestination;
use readyset_client_metrics::{recorded, EventType, QueryExecutionEvent, SqlQueryType};
use readyset_data::{DfType, DfValue};
use readyset_errors::ReadySetError::{self, PreparedStatementMissing};
use readyset_errors::{internal, internal_err, unsupported, ReadySetResult};
use readyset_telemetry_reporter::{TelemetryBuilder, TelemetryEvent, TelemetrySender};
use readyset_tracing::{error, instrument_root, trace, warn};
use readyset_util::redacted::Sensitive;
use readyset_version::READYSET_VERSION;
use timestamp_service::client::{TimestampClient, WriteId, WriteKey};
use tokio::sync::mpsc::UnboundedSender;
use tracing::instrument;

use crate::backend::noria_connector::ExecuteSelectContext;
use crate::query_handler::SetBehavior;
use crate::query_status_cache::QueryStatusCache;
use crate::upstream_database::NoriaCompare;
pub use crate::upstream_database::UpstreamPrepare;
use crate::{rewrite, QueryHandler, UpstreamDatabase, UpstreamDestination};

pub mod noria_connector;

pub use self::noria_connector::NoriaConnector;
use self::noria_connector::{MetaVariable, SelectPrepareResult, SelectPrepareResultInner};

/// Query metadata used to plan query prepare
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
enum PrepareMeta {
    /// Query was received in a state that should unconditionally proxy upstream
    Proxy,
    /// Query could not be parsed
    FailedToParse,
    /// Query could not be rewritten for processing in noria
    FailedToRewrite,
    /// ReadySet does not implement this prepared statement. The statement may also be invalid SQL
    Unimplemented,
    /// A write query (Insert, Update, Delete)
    Write { stmt: SqlQuery },
    /// A read (Select; may be extended in the future)
    Select(PrepareSelectMeta),
}

#[derive(Debug)]
struct PrepareSelectMeta {
    stmt: nom_sql::SelectStatement,
    rewritten: nom_sql::SelectStatement,
    must_migrate: bool,
    should_do_noria: bool,
    always: bool,
}

/// How to behave when receiving unsupported `SET` statements
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
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

    /// Sets the autocommit state accordingly. If turning autcommit on, will set ProxyState to
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
    slowlog: bool,
    dialect: Dialect,
    users: HashMap<String, String>,
    require_authentication: bool,
    ticket: Option<Timestamp>,
    timestamp_client: Option<TimestampClient>,
    query_log_sender: Option<UnboundedSender<QueryExecutionEvent>>,
    query_log_ad_hoc_queries: bool,
    validate_queries: bool,
    fail_invalidated_queries: bool,
    unsupported_set_mode: UnsupportedSetMode,
    migration_mode: MigrationMode,
    query_max_failure_seconds: u64,
    fallback_recovery_seconds: u64,
    telemetry_sender: Option<TelemetrySender>,
}

impl Default for BackendBuilder {
    fn default() -> Self {
        BackendBuilder {
            slowlog: false,
            dialect: Dialect::MySQL,
            users: Default::default(),
            require_authentication: true,
            ticket: None,
            timestamp_client: None,
            query_log_sender: None,
            query_log_ad_hoc_queries: false,
            validate_queries: false,
            fail_invalidated_queries: false,
            unsupported_set_mode: UnsupportedSetMode::Error,
            migration_mode: MigrationMode::InRequestPath,
            query_max_failure_seconds: (i64::MAX / 1000) as u64,
            fallback_recovery_seconds: 0,
            telemetry_sender: None,
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
    ) -> Backend<DB, Handler> {
        metrics::increment_gauge!(recorded::CONNECTED_CLIENTS, 1.0);

        let proxy_state = if upstream.is_some() {
            ProxyState::Fallback
        } else {
            ProxyState::Never
        };

        Backend {
            noria,
            upstream,
            users: self.users,
            query_log_sender: self.query_log_sender,
            last_query: None,
            state: BackendState {
                proxy_state,
                parsed_query_cache: HashMap::new(),
                prepared_statements: Vec::new(),
                query_status_cache,
                ticket: self.ticket,
                timestamp_client: self.timestamp_client,
            },
            settings: BackendSettings {
                slowlog: self.slowlog,
                dialect: self.dialect,
                require_authentication: self.require_authentication,
                validate_queries: self.validate_queries,
                fail_invalidated_queries: self.fail_invalidated_queries,
                unsupported_set_mode: self.unsupported_set_mode,
                migration_mode: self.migration_mode,
                query_max_failure_duration: Duration::new(self.query_max_failure_seconds, 0),
                query_log_ad_hoc_queries: self.query_log_ad_hoc_queries,
                fallback_recovery_duration: Duration::new(self.fallback_recovery_seconds, 0),
            },
            telemetry_sender: self.telemetry_sender,
            _query_handler: PhantomData,
        }
    }

    pub fn slowlog(mut self, slowlog: bool) -> Self {
        self.slowlog = slowlog;
        self
    }

    pub fn dialect(mut self, dialect: Dialect) -> Self {
        self.dialect = dialect;
        self
    }

    pub fn query_log(
        mut self,
        query_log_sender: Option<UnboundedSender<QueryExecutionEvent>>,
        ad_hoc_queries: bool,
    ) -> Self {
        self.query_log_sender = query_log_sender;
        self.query_log_ad_hoc_queries = ad_hoc_queries;
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

    /// Specifies whether RYW consistency should be enabled. If true, RYW consistency
    /// constraints will be enforced on all reads.
    pub fn enable_ryw(mut self, enable_ryw: bool) -> Self {
        if enable_ryw {
            // initialize with an empty timestamp, which will be satisfied by any data version
            self.ticket = Some(Timestamp::default());
            self.timestamp_client = Some(TimestampClient::default())
        }
        self
    }

    pub fn validate_queries(
        mut self,
        validate_queries: bool,
        fail_invalidated_queries: bool,
    ) -> Self {
        self.validate_queries = validate_queries;
        self.fail_invalidated_queries = fail_invalidated_queries;
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
}

/// A [`CachedPreparedStatement`] stores the data needed for an immediate
/// execution of a prepared statement on either noria or the upstream
/// connection.
struct CachedPreparedStatement<DB>
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

impl<DB> CachedPreparedStatement<DB>
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
}

pub struct Backend<DB, Handler>
where
    DB: UpstreamDatabase,
{
    /// ReadySet connector used for reads, and writes when no upstream DB is present
    noria: NoriaConnector,
    /// Optional connector to the upstream DB. Used for fallback reads and all writes if it exists
    upstream: Option<DB>,
    /// Map from username to password for all users allowed to connect to the db
    pub users: HashMap<String, String>,

    query_log_sender: Option<UnboundedSender<QueryExecutionEvent>>,

    /// Information regarding the last query sent over this connection. If None, then no queries
    /// have been handled using this connection (Backend) yet.
    last_query: Option<QueryInfo>,

    /// Encapsulates the inner state of this [`Backend`]
    state: BackendState<DB>,
    /// The settings with which the [`Backend`] was started
    settings: BackendSettings,

    /// Provides the ability to send [`TelemetryEvent`]s to Segment
    telemetry_sender: Option<TelemetrySender>,

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
    parsed_query_cache: HashMap<String, SqlQuery>,
    // all queries previously prepared on noria or upstream, mapped by their ID.
    prepared_statements: Vec<CachedPreparedStatement<DB>>,
    /// Current RYW ticket. `None` if RYW is not enabled. This `ticket` will
    /// be updated as the client makes writes so as to be an accurate low watermark timestamp
    /// required to make RYW-consistent reads. On reads, the client will pass in this ticket to be
    /// checked by noria view nodes.
    ticket: Option<Timestamp>,
    /// `timestamp_client` is the Backends connection to the TimestampService. The TimestampService
    /// is responsible for creating accurate RYW timestamps/tickets based on writes made by the
    /// Backend client.
    timestamp_client: Option<TimestampClient>,
}

/// Settings that have no state and are constant for a given [`Backend`]
struct BackendSettings {
    /// SQL dialect to use when parsing queries from clients
    dialect: Dialect,
    slowlog: bool,
    require_authentication: bool,
    /// Whether to log ad-hoc queries by full query text in the query logger.
    query_log_ad_hoc_queries: bool,
    /// Run select statements with query validation.
    validate_queries: bool,
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
    fail_invalidated_queries: bool,
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
                } else if c.name_str() == "ReadySet_error" {
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
    pub use_bogo: bool,
    pub schema: Cow<'a, [ColumnSchema]>,
    pub columns: Cow<'a, [SqlIdentifier]>,
}

impl<'a> SelectSchema<'a> {
    pub fn into_owned(self) -> SelectSchema<'static> {
        SelectSchema {
            use_bogo: self.use_bogo,
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

/// The type returned when a query is prepared by `Backend` through the `prepare` function.
#[derive(Debug)]
pub enum PrepareResult<DB: UpstreamDatabase> {
    Noria(noria_connector::PrepareResult),
    Upstream(UpstreamPrepare<DB>),
    Both(noria_connector::PrepareResult, UpstreamPrepare<DB>),
}

// Sadly rustc is very confused when trying to derive Clone for UpstreamPrepare, so have to do it
// manually
impl<DB: UpstreamDatabase> Clone for PrepareResult<DB> {
    fn clone(&self) -> Self {
        match self {
            PrepareResult::Noria(n) => PrepareResult::Noria(n.clone()),
            PrepareResult::Upstream(u) => PrepareResult::Upstream(u.clone()),
            PrepareResult::Both(n, u) => PrepareResult::Both(n.clone(), u.clone()),
        }
    }
}

impl<DB: UpstreamDatabase> PrepareResult<DB> {
    pub fn noria_biased(&self) -> SinglePrepareResult<'_, DB> {
        match self {
            Self::Noria(res) | Self::Both(res, _) => SinglePrepareResult::Noria(res),
            Self::Upstream(res) => SinglePrepareResult::Upstream(res),
        }
    }

    pub fn upstream_biased(&self) -> SinglePrepareResult<'_, DB> {
        match self {
            Self::Upstream(res) | Self::Both(_, res) => SinglePrepareResult::Upstream(res),
            Self::Noria(res) => SinglePrepareResult::Noria(res),
        }
    }

    /// If this [`PrepareResult`] is a [`PrepareResult::Both`], convert it into only a
    /// [`PrepareResult::Upstream`]
    pub fn make_upstream_only(&mut self) {
        match self {
            Self::Noria(_) => {}
            Self::Upstream(_) => {}
            Self::Both(_, u) => *self = Self::Upstream(u.clone()),
        }
    }
}

/// The type returned when a query is carried out by `Backend`, through either the `query` or
/// `execute` functions.
pub enum QueryResult<'a, DB: UpstreamDatabase>
where
    DB: 'a,
{
    /// Results from noria
    Noria(noria_connector::QueryResult<'a>),
    /// Results from upstream
    Upstream(DB::QueryResult<'a>),
}

impl<'a, DB: UpstreamDatabase> From<noria_connector::QueryResult<'a>> for QueryResult<'a, DB> {
    fn from(r: noria_connector::QueryResult<'a>) -> Self {
        Self::Noria(r)
    }
}

impl<'a, DB> Debug for QueryResult<'a, DB>
where
    DB: UpstreamDatabase,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Noria(r) => f.debug_tuple("Noria").field(r).finish(),
            Self::Upstream(r) => f.debug_tuple("Upstream").field(r).finish(),
        }
    }
}

/// TODO: The ideal approach for query handling is as follows:
/// 1. If we know we can't support a query, send it to fallback.
/// 2. If we think we can support a query, try to send it to ReadySet. If that hits an error that
/// should be retried, retry.    If not, try fallback without dropping the connection inbetween.
/// 3. If that fails and we got a MySQL error code, send that back to the client and keep the
/// connection open. This is a real correctness bug. 4. If we got another kind of error that is
/// retryable from fallback, retry. 5. If we got a non-retry related error that's not a MySQL error
/// code already, convert it to the most appropriate MySQL error code and write    that back to the
/// caller without dropping the connection.
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

    /// The identifier of the last prepared statement (which is always the last in the vector)
    pub fn last_prepared_id(&self) -> u32 {
        (self.state.prepared_statements.len() - 1)
            .try_into()
            .expect("Too many prepared statements")
    }

    /// The identifier we can reserve for the next prepared statement
    pub fn next_prepared_id(&self) -> u32 {
        (self.state.prepared_statements.len())
            .try_into()
            .expect("Too many prepared statements")
    }

    /// Switch the active database for this backend to the given named database.
    ///
    /// Internally, this will set the schema search path to a single-element vector with the
    /// database, and send a `USE` command to the upstream, if any.
    pub async fn set_database(&mut self, db: &str) -> Result<(), DB::Error> {
        if let Some(upstream) = &mut self.upstream {
            upstream
                .query(
                    UseStatement {
                        database: db.into(),
                    }
                    .to_string(),
                )
                .await?;
        }
        self.noria.set_schema_search_path(vec![db.into()]);
        Ok(())
    }

    /// Executes query on the upstream database, for when it cannot be parsed or executed by noria.
    /// Returns the query result, or an error if fallback is not configured
    #[instrument_root(level = "info")]
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

    /// Prepares query on the mysql_backend, if present, when it cannot be parsed or prepared by
    /// noria.
    pub async fn prepare_fallback(
        &mut self,
        query: &str,
    ) -> Result<UpstreamPrepare<DB>, DB::Error> {
        let upstream = self.upstream.as_mut().ok_or_else(|| {
            ReadySetError::Internal("This case requires an upstream connector".to_string())
        })?;
        upstream.prepare(query).await
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
        event: &mut QueryExecutionEvent,
    ) -> Result<PrepareResult<DB>, DB::Error> {
        let prep_idx = self.next_prepared_id();

        let do_noria = select_meta.should_do_noria;
        let do_migrate = select_meta.must_migrate;

        let up_prep: OptionFuture<_> = self.upstream.as_mut().map(|u| u.prepare(query)).into();
        let noria_prep: OptionFuture<_> = do_noria
            .then_some(self.noria.prepare_select(
                select_meta.stmt.clone(),
                prep_idx,
                do_migrate,
                None,
            ))
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
            Some(Ok(noria_connector::PrepareResult::Select(res))) => {
                let mut state = MigrationState::Successful;

                let (schema, params) = match res {
                    SelectPrepareResult::Schema(SelectPrepareResultInner {
                        schema,
                        params,
                        ..
                    }) => (Some(schema), Some(params)),
                    _ => (None, None),
                };

                if let Some(Ok(upstream_res)) = &upstream_res {
                    // If we are using `validate_queries`, a query that was successfully
                    // migrated may actually be unsupported if the schema or columns do not
                    // match up.
                    if self.settings.validate_queries {
                        if let (Some(schema), Some(params)) = (schema, params) {
                            if let Err(e) = upstream_res.meta.compare(schema, params) {
                                if self.settings.fail_invalidated_queries {
                                    internal!("Query comparison failed to validate: {}", e);
                                }
                                warn!(error = %e,
                                    query = %Sensitive(&select_meta.stmt),
                                    "Query compare failed");
                                state = MigrationState::Unsupported;
                            }
                        } else if self.settings.fail_invalidated_queries {
                            internal!("Cannot compare schema for borrowed query");
                        } else {
                            warn!("Cannot compare schema for borrowed query");
                            state = MigrationState::Unsupported;
                        }
                    }
                }

                self.state.query_status_cache.update_query_migration_state(
                    &ViewCreateRequest::new(
                        select_meta.rewritten.clone(),
                        self.noria.schema_search_path().to_owned(),
                    ),
                    state,
                );
            }
            Some(Err(e)) => {
                if e.caused_by_view_not_found() {
                    warn!(error = %e, "View not found during mirror_prepare()");
                    self.state.query_status_cache.update_query_migration_state(
                        &ViewCreateRequest::new(
                            select_meta.rewritten.clone(),
                            self.noria.schema_search_path().to_owned(),
                        ),
                        MigrationState::Pending,
                    );
                } else if e.caused_by_unsupported() {
                    self.state.query_status_cache.update_query_migration_state(
                        &ViewCreateRequest::new(
                            select_meta.rewritten.clone(),
                            self.noria.schema_search_path().to_owned(),
                        ),
                        MigrationState::Unsupported,
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
                PrepareResult::Both(noria_res, upstream_res?)
            }
            (None, Some(Ok(noria_res))) => {
                if let noria_connector::PrepareResult::Select(SelectPrepareResult::NoSchema(_)) =
                    noria_res
                {
                    // We fail when attempting to borrow a cache without an upstream here in case
                    // the connection to the upstream is temporarily down.
                    internal!(
                        "Cannot create PrepareResult for borrowed cache without an upstream result"
                    );
                }
                PrepareResult::Noria(noria_res)
            }
            (None, Some(Err(noria_err))) => return Err(noria_err.into()),
            (Some(upstream_res), _) => PrepareResult::Upstream(upstream_res?),
            (None, None) => return Err(ReadySetError::Unsupported(query.to_string()).into()),
        };

        Ok(prep_result)
    }

    /// Prepares Insert, Delete, and Update statements
    async fn prepare_write(
        &mut self,
        query: &str,
        stmt: &SqlQuery,
        event: &mut QueryExecutionEvent,
    ) -> Result<PrepareResult<DB>, DB::Error> {
        let prep_idx = self.next_prepared_id();
        event.sql_type = SqlQueryType::Write;
        if let Some(ref mut upstream) = self.upstream {
            let _t = event.start_upstream_timer();
            let res = upstream.prepare(query).await.map(PrepareResult::Upstream);
            self.last_query = Some(QueryInfo {
                destination: QueryDestination::Upstream,
                noria_error: String::new(),
            });
            res
        } else {
            let _t = event.start_noria_timer();
            let res = match stmt {
                SqlQuery::Insert(stmt) => self.noria.prepare_insert(stmt.clone(), prep_idx).await?,
                SqlQuery::Delete(stmt) => self.noria.prepare_delete(stmt.clone(), prep_idx).await?,
                SqlQuery::Update(stmt) => self.noria.prepare_update(stmt.clone(), prep_idx).await?,
                // prepare_write does not support other statements
                _ => internal!(),
            };
            self.last_query = Some(QueryInfo {
                destination: QueryDestination::Readyset,
                noria_error: String::new(),
            });
            Ok(PrepareResult::Noria(res))
        }
    }

    /// Provides metadata required to prepare a select query
    fn plan_prepare_select(&mut self, stmt: nom_sql::SelectStatement) -> PrepareMeta {
        match self.rewrite_select_and_check_noria(&stmt) {
            Some((rewritten, should_do_noria)) => {
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
                    PrepareMeta::Select(PrepareSelectMeta {
                        stmt,
                        rewritten,
                        should_do_noria,
                        // For select statements only InRequestPath should trigger migrations
                        // synchronously, or if no upstream is present.
                        must_migrate: self.settings.migration_mode == MigrationMode::InRequestPath
                            || !self.has_fallback(),
                        always: status.always,
                    })
                }
            }
            None => {
                warn!(statement = %Sensitive(&stmt),
                      "This statement could not be rewritten by ReadySet");
                PrepareMeta::FailedToRewrite
            }
        }
    }

    /// Rewrites the provided select, and checks if the select statement should be
    /// handled by noria. If so, the second tuple member will be true. If the select should be
    /// handled by upstream, the second tuple member will be false.
    ///
    /// If the rewrite fails, the option will be None.
    fn rewrite_select_and_check_noria(
        &mut self,
        stmt: &nom_sql::SelectStatement,
    ) -> Option<(nom_sql::SelectStatement, bool)> {
        let mut rewritten = stmt.clone();
        if rewrite::process_query(&mut rewritten, self.noria.server_supports_pagination()).is_err()
        {
            None
        } else {
            let should_do_noria = self
                .state
                .query_status_cache
                .query_migration_state(&ViewCreateRequest::new(
                    rewritten.clone(),
                    self.noria.schema_search_path().to_owned(),
                ))
                .1
                != MigrationState::Unsupported;
            Some((rewritten, should_do_noria))
        }
    }

    /// Provides metadata required to prepare a query
    async fn plan_prepare(&mut self, query: &str) -> PrepareMeta {
        if self.state.proxy_state == ProxyState::ProxyAlways {
            return PrepareMeta::Proxy;
        }

        match self.parse_query(query) {
            Ok(SqlQuery::Select(stmt)) => self.plan_prepare_select(stmt),
            Ok(
                query @ SqlQuery::Insert(_)
                | query @ SqlQuery::Update(_)
                | query @ SqlQuery::Delete(_),
            ) => PrepareMeta::Write { stmt: query },
            Ok(pq) => {
                warn!(statement = %Sensitive(&pq), "Statement cannot be prepared by ReadySet");
                PrepareMeta::Unimplemented
            }
            Err(_) => {
                let mode = if self.state.proxy_state == ProxyState::Never {
                    PrepareMeta::FailedToParse
                } else {
                    PrepareMeta::Proxy
                };
                warn!(query = %Sensitive(&query), plan = ?mode, "ReadySet failed to parse query");
                mode
            }
        }
    }

    /// Prepares a query on noria and upstream based on the provided PrepareMeta
    async fn do_prepare(
        &mut self,
        meta: &PrepareMeta,
        query: &str,
        event: &mut QueryExecutionEvent,
    ) -> Result<PrepareResult<DB>, DB::Error> {
        match meta {
            PrepareMeta::Proxy
            | PrepareMeta::FailedToParse
            | PrepareMeta::FailedToRewrite
            | PrepareMeta::Unimplemented
                if self.upstream.is_some() =>
            {
                let _t = event.start_upstream_timer();
                let res = self
                    .prepare_fallback(query)
                    .await
                    .map(PrepareResult::Upstream);

                self.last_query = Some(QueryInfo {
                    destination: QueryDestination::Upstream,
                    noria_error: String::new(),
                });

                res
            }
            PrepareMeta::Write { stmt } => self.prepare_write(query, stmt, event).await,
            PrepareMeta::Select(select_meta) => {
                self.mirror_prepare(select_meta, query, event).await
            }
            _ => unsupported!(),
        }
    }

    /// Prepares `query` to be executed later using the reader/writer belonging
    /// to the calling `Backend` struct and adds the prepared query
    /// to the calling struct's map of prepared queries with a unique id.
    #[instrument_root(level = "info")]
    pub async fn prepare(&mut self, query: &str) -> Result<&PrepareResult<DB>, DB::Error> {
        self.last_query = None;
        let mut query_event = QueryExecutionEvent::new(EventType::Prepare);

        let meta = self.plan_prepare(query).await;
        let res = self.do_prepare(&meta, query, &mut query_event).await?;

        let (id, parsed_query, migration_state, view_request, always) = match meta {
            PrepareMeta::Write { stmt } => (
                None,
                Some(Arc::new(stmt)),
                MigrationState::Successful,
                None,
                false,
            ),
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
                (
                    Some(migration_state.0),
                    Some(Arc::new(SqlQuery::Select(stmt))),
                    migration_state.1,
                    Some(request),
                    always,
                )
            }
            _ => (None, None, MigrationState::Successful, None, false),
        };

        if let Some(parsed) = &parsed_query {
            query_event.query = Some(parsed.clone());
        }
        query_event.query_id = id;

        let cache_entry = CachedPreparedStatement {
            query_id: id,
            prep: res,
            migration_state,
            execution_info: None,
            parsed_query,
            view_request,
            always,
        };

        self.state.prepared_statements.push(cache_entry);

        Ok(&self.state.prepared_statements.last().unwrap().prep)
    }

    /// Executes a prepared statement on ReadySet
    async fn execute_noria<'a>(
        noria: &'a mut NoriaConnector,
        prep: &noria_connector::PrepareResult,
        params: &[DfValue],
        ticket: Option<Timestamp>,
        event: &mut QueryExecutionEvent,
    ) -> ReadySetResult<QueryResult<'a, DB>> {
        use noria_connector::PrepareResult::*;

        event.destination = Some(QueryDestination::Readyset);
        let start = Instant::now();

        let res = match prep {
            Select(_) => {
                let ctx = ExecuteSelectContext::Prepared {
                    q_id: prep.statement_id(),
                    params,
                };
                noria.execute_select(ctx, ticket, event).await
            }
            Insert {
                statement_id: id, ..
            } => noria.execute_prepared_insert(*id, params).await,
            Update {
                statement_id: id, ..
            } => noria.execute_prepared_update(*id, params).await,
            Delete {
                statement_id: id, ..
            } => noria.execute_prepared_delete(*id, params).await,
        }
        .map(Into::into);

        if let Err(e) = &res {
            event.set_noria_error(e);
        }

        event.readyset_duration = Some(start.elapsed());

        res
    }

    /// Execute a prepared statement on ReadySet
    async fn execute_upstream<'a>(
        upstream: &'a mut Option<DB>,
        prep: &UpstreamPrepare<DB>,
        params: &[DfValue],
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
            .execute(prep.statement_id, params)
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
        ex_info: Option<&mut ExecutionInfo>,
        ticket: Option<Timestamp>,
        event: &mut QueryExecutionEvent,
    ) -> Result<QueryResult<'a, DB>, DB::Error> {
        let noria_res = Self::execute_noria(noria, noria_prep, params, ticket, event).await;
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
                if !matches!(
                    noria_err,
                    ReadySetError::ReaderMissingKey | ReadySetError::NoCacheForQuery
                ) {
                    warn!(error = %noria_err,
                          "Error received from noria, sending query to fallback");
                }

                Self::execute_upstream(upstream, upstream_prep, params, event, true).await
            }
        }
    }

    /// Attempts to migrate a query on noria, after it was marked as Successful in the cache. If the
    /// migration is successful, the cached entry is marked as such and will attempt to resolve
    /// noria first in the future
    ///
    /// # Panics
    ///
    /// If the cached entry is not of kind `PrepareResult::Upstream` or is not in the
    /// `MigrationState::Pending` state
    async fn update_noria_prepare(
        noria: &mut NoriaConnector,
        cached_entry: &mut CachedPreparedStatement<DB>,
        id: u32,
    ) -> ReadySetResult<()> {
        debug_assert!(cached_entry.migration_state.is_pending());

        let upstream_prep: UpstreamPrepare<DB> = match &cached_entry.prep {
            PrepareResult::Upstream(UpstreamPrepare { statement_id, meta }) => UpstreamPrepare {
                statement_id: *statement_id,
                meta: meta.clone(),
            },
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
                        id,
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
        cached_entry.prep = PrepareResult::Both(noria_prep, upstream_prep);
        cached_entry.migration_state = MigrationState::Successful;

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
                |CachedPreparedStatement {
                     prep,
                     migration_state,
                     view_request,
                     ..
                 }| {
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
    // TODO(andrew, justin): add RYW support for executing prepared queries
    #[instrument_root(level = "info", fields(id))]
    #[inline]
    pub async fn execute(
        &mut self,
        id: u32,
        params: &[DfValue],
    ) -> Result<QueryResult<'_, DB>, DB::Error> {
        self.last_query = None;
        let cached_statement = self
            .state
            .prepared_statements
            .get_mut(id as usize)
            .ok_or(PreparedStatementMissing { statement_id: id })?;

        let mut event = QueryExecutionEvent::new(EventType::Execute);
        event.query = cached_statement.parsed_query.clone();
        event.query_id = cached_statement.query_id;

        let upstream = &mut self.upstream;
        let noria = &mut self.noria;
        let ticket = self.state.ticket.clone();

        if cached_statement.migration_state.is_pending() {
            // We got a statement with a pending migration, we want to check if migration is
            // finished by now
            let new_migration_state = self
                .state
                .query_status_cache
                .query_migration_state(
                    cached_statement
                        .view_request
                        .as_ref()
                        .expect("Pending must have view_request set"),
                )
                .1;

            if new_migration_state == MigrationState::Successful {
                // Attempt to prepare on ReadySet
                let _ = Self::update_noria_prepare(noria, cached_statement, id).await;
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

        let result = match &cached_statement.prep {
            PrepareResult::Noria(prep) => {
                Self::execute_noria(noria, prep, params, ticket, &mut event)
                    .await
                    .map_err(Into::into)
            }
            PrepareResult::Upstream(prep) => {
                Self::execute_upstream(upstream, prep, params, &mut event, false).await
            }
            PrepareResult::Both(.., uprep) if should_fallback => {
                Self::execute_upstream(upstream, uprep, params, &mut event, false).await
            }
            PrepareResult::Both(nprep, uprep) => {
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
                    cached_statement.execution_info.as_mut(),
                    ticket,
                    &mut event,
                )
                .await
            }
        };

        if let Some(e) = event.noria_error.as_ref() {
            if e.caused_by_view_not_found() {
                // This can happen during cascade execution if the noria query was removed from
                // another connection
                cached_statement.prep.make_upstream_only();
            } else if e.caused_by_unsupported() {
                // On an unsupported execute we update the query migration state to be unsupported.
                //
                // Must exist or we would not have executed the query against ReadySet.
                #[allow(clippy::unwrap_used)]
                self.state.query_status_cache.update_query_migration_state(
                    cached_statement.view_request.as_ref().unwrap(),
                    MigrationState::Unsupported,
                );
            }
        }

        self.last_query = event.destination.map(|d| QueryInfo {
            destination: d,
            noria_error: event
                .noria_error
                .as_ref()
                .map(|e| e.to_string())
                .unwrap_or_default(),
        });
        log_query(self.query_log_sender.as_ref(), event, self.settings.slowlog);

        result
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
            SqlQuery::StartTransaction(_) => {
                let result = QueryResult::Upstream(upstream.start_tx().await?);
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
            ("ReadySet_error", error).into(),
        ]))
    }

    /// Forwards a `CREATE CACHE` request to noria
    async fn create_cached_query(
        &mut self,
        name: Option<&Relation>,
        mut stmt: SelectStatement,
        override_schema_search_path: Option<Vec<SqlIdentifier>>,
        always: bool,
    ) -> ReadySetResult<noria_connector::QueryResult<'static>> {
        // If we have another query with the same name, drop that query first
        if let Some(name) = name {
            if let Some(view_request) = self.noria.view_create_request_from_name(name) {
                warn!(
                    statement = %Sensitive(&view_request.statement),
                    %name,
                    "Dropping previously cached query",
                );
                self.drop_cached_query(name).await?;
            }
        }
        // Now migrate the new query
        rewrite::process_query(&mut stmt, self.noria.server_supports_pagination())?;
        self.noria
            .handle_create_cached_query(name, &stmt, override_schema_search_path, always)
            .await?;
        self.state.query_status_cache.update_query_migration_state(
            &ViewCreateRequest::new(stmt.clone(), self.noria.schema_search_path().to_owned()),
            MigrationState::Successful,
        );
        self.state.query_status_cache.always_attempt_readyset(
            &ViewCreateRequest::new(stmt.clone(), self.noria.schema_search_path().to_owned()),
            always,
        );
        Ok(noria_connector::QueryResult::Empty)
    }

    /// Forwards a `DROP CACHE` request to noria
    async fn drop_cached_query(
        &mut self,
        name: &Relation,
    ) -> ReadySetResult<noria_connector::QueryResult<'static>> {
        let maybe_view_request = self.noria.view_create_request_from_name(name);
        self.noria.drop_view(name).await?;
        if let Some(view_request) = maybe_view_request {
            self.state
                .query_status_cache
                .update_query_migration_state(&view_request, MigrationState::Pending);
            self.state
                .query_status_cache
                .always_attempt_readyset(&view_request, false);
            self.invalidate_prepared_statements_cache(&view_request);
        }
        Ok(noria_connector::QueryResult::Empty)
    }

    /// Forwards a `DROP ALL CACHES` request to noria
    async fn drop_all_caches(&mut self) -> ReadySetResult<noria_connector::QueryResult<'static>> {
        self.noria.drop_all_caches().await?;
        self.state.query_status_cache.clear();
        self.state.prepared_statements.iter_mut().for_each(
            |CachedPreparedStatement {
                 prep,
                 migration_state,
                 ..
             }| {
                if *migration_state == MigrationState::Successful {
                    *migration_state = MigrationState::Pending;
                }
                prep.make_upstream_only();
            },
        );
        Ok(noria_connector::QueryResult::Empty)
    }

    /// Responds to a `SHOW PROXIED QUERIES` query
    async fn show_proxied_queries(
        &mut self,
        query_id: &Option<String>,
    ) -> ReadySetResult<noria_connector::QueryResult<'static>> {
        let create_dummy_column = |n: &str| ColumnSchema {
            column: nom_sql::Column {
                name: n.into(),
                table: None,
            },
            column_type: DfType::DEFAULT_TEXT,
            base: None,
        };

        let mut queries = self.state.query_status_cache.deny_list();
        if let Some(q_id) = query_id {
            queries.retain(|q| &q.id.to_string() == q_id);
        }
        let select_schema = SelectSchema {
            use_bogo: false,
            schema: Cow::Owned(vec![
                create_dummy_column("query id"),
                create_dummy_column("proxied query"),
                create_dummy_column("readyset supported"),
            ]),

            columns: Cow::Owned(vec![
                "query id".into(),
                "proxied query".into(),
                "readyset supported".into(),
            ]),
        };

        let data = queries
            .into_iter()
            .map(|DeniedQuery { id, query, status }| {
                let s = match status.migration_state {
                    MigrationState::DryRunSucceeded | MigrationState::Successful => "yes",
                    MigrationState::Pending => "pending",
                    MigrationState::Unsupported => "unsupported",
                }
                .to_string();

                vec![
                    DfValue::from(id.to_string()),
                    DfValue::from(query.to_string()),
                    DfValue::from(s),
                ]
            })
            .collect::<Vec<_>>();
        Ok(noria_connector::QueryResult::from_owned(
            select_schema,
            vec![Results::new(data)],
        ))
    }

    async fn query_noria_extensions<'a>(
        &'a mut self,
        query: &'a SqlQuery,
        event: &mut QueryExecutionEvent,
    ) -> Option<ReadySetResult<noria_connector::QueryResult<'static>>> {
        // Those will get cleared if it was not destined to noria
        event.sql_type = SqlQueryType::Other;
        event.destination = Some(QueryDestination::Readyset);

        let _t = event.start_noria_timer();

        let res = match query {
            SqlQuery::Explain(nom_sql::ExplainStatement::LastStatement) => {
                self.explain_last_statement()
            }
            SqlQuery::Explain(nom_sql::ExplainStatement::Graphviz { simplified }) => {
                self.noria.graphviz(*simplified).await
            }
            SqlQuery::CreateCache(CreateCacheStatement {
                name,
                inner,
                always,
            }) => {
                let (stmt, search_path) = match inner {
                    CacheInner::Statement(st) => (*st.clone(), None),
                    CacheInner::Id(id) => match self.state.query_status_cache.query(id.as_str()) {
                        Some(q) => match q {
                            Query::Parsed(view_request) => (
                                view_request.statement.clone(),
                                Some(view_request.schema_search_path.clone()),
                            ),
                            Query::ParseFailed(q) => {
                                return Some(Err(ReadySetError::UnparseableQuery {
                                    query: (*q).clone(),
                                }))
                            }
                        },
                        None => {
                            return Some(Err(ReadySetError::NoQueryForId { id: id.to_string() }))
                        }
                    },
                };

                // Log a telemetry event
                if let Some(ref telemetry_sender) = self.telemetry_sender {
                    if let Err(e) = telemetry_sender.send_event(TelemetryEvent::CreateCache) {
                        warn!(error = %e, "Failed to send CREATE CACHE metric");
                    }
                } else {
                    trace!("No telemetry sender. not sending metric for CREATE CACHE");
                }

                self.create_cached_query(name.as_ref(), stmt, search_path, *always)
                    .await
            }
            SqlQuery::DropCache(DropCacheStatement { name }) => self.drop_cached_query(name).await,
            SqlQuery::DropAllCaches(_) => self.drop_all_caches().await,
            SqlQuery::Show(ShowStatement::CachedQueries(query_id)) => {
                // Log a telemetry event
                if let Some(ref telemetry_sender) = self.telemetry_sender {
                    if let Err(e) = telemetry_sender.send_event(TelemetryEvent::ShowCaches) {
                        warn!(error = %e, "Failed to send SHOW CACHES metric");
                    }
                } else {
                    trace!("No telemetry sender. not sending metric for SHOW CACHES");
                }

                self.noria.verbose_views(query_id).await
            }
            SqlQuery::Show(ShowStatement::ReadySetStatus) => self.noria.readyset_status().await,
            SqlQuery::Show(ShowStatement::ReadySetVersion) => readyset_version(),
            SqlQuery::Show(ShowStatement::ReadySetTables) => self.noria.table_statuses().await,
            SqlQuery::Show(ShowStatement::ProxiedQueries(q_id)) => {
                // Log a telemetry event
                if let Some(ref telemetry_sender) = self.telemetry_sender {
                    if let Err(e) = telemetry_sender.send_event(TelemetryEvent::ShowProxiedQueries)
                    {
                        warn!(error = %e, "Failed to send SHOW PROXIED QUERIES metric");
                    }
                } else {
                    trace!("No telemetry sender. not sending metric for SHOW PROXIED QUERIES");
                }

                self.show_proxied_queries(q_id).await
            }
            _ => {
                drop(_t);
                // Clear readyset timer, since it was not a readyset request
                event.readyset_duration.take();
                return None;
            }
        };

        Some(res)
    }

    #[allow(clippy::too_many_arguments)]
    async fn query_adhoc_select<'a>(
        noria: &'a mut NoriaConnector,
        upstream: Option<&'a mut DB>,
        settings: &BackendSettings,
        state: &mut BackendState<DB>,
        original_query: &'a str,
        original_stmt: SelectStatement,
        view_request: &ViewCreateRequest,
        status: Option<QueryStatus>,
        event: &mut QueryExecutionEvent,
    ) -> Result<QueryResult<'a, DB>, DB::Error> {
        let mut status = status.unwrap_or(QueryStatus {
            migration_state: MigrationState::Unsupported,
            execution_info: None,
            always: false,
        });
        let original_status = status.clone();
        let did_work = if let Some(ref mut i) = status.execution_info {
            i.reset_if_exceeded_recovery(
                settings.query_max_failure_duration,
                settings.fallback_recovery_duration,
            )
        } else {
            false
        };

        if !status.always
            && (upstream.is_some()
                && (settings.migration_mode != MigrationMode::InRequestPath
                    && status.migration_state != MigrationState::Successful)
                || (status.migration_state == MigrationState::Unsupported)
                || (status
                    .execution_info
                    .as_mut()
                    .map(|i| {
                        i.execute_network_failure_exceeded(settings.query_max_failure_duration)
                    })
                    .unwrap_or(false)))
        {
            if did_work {
                #[allow(clippy::unwrap_used)] // Validated by did_work.
                state.query_status_cache.update_transition_time(
                    view_request,
                    &status.execution_info.unwrap().last_transition_time,
                );
            }
            return Self::query_fallback(upstream, original_query, event).await;
        }

        let noria_res = {
            event.destination = Some(QueryDestination::Readyset);
            let start = Instant::now();
            let ctx = ExecuteSelectContext::AdHoc {
                statement: original_stmt,
                query: original_query,
                create_if_missing: settings.migration_mode == MigrationMode::InRequestPath,
            };
            let res = noria.execute_select(ctx, state.ticket.clone(), event).await;
            event.readyset_duration = Some(start.elapsed());
            res
        };

        if status.execution_info.is_none() {
            status.execution_info = Some(ExecutionInfo {
                state: ExecutionState::Failed,
                last_transition_time: Instant::now(),
            });
        }
        match noria_res {
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
                    status.migration_state = MigrationState::Unsupported;
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
                    (true, _) | (_, None) => Err(noria_err.into()),
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
    fn noria_should_try_select(&self, q: &mut ViewCreateRequest) -> (bool, Option<QueryStatus>) {
        let mut status = None;
        let should_try =
            if rewrite::process_query(&mut q.statement, self.noria.server_supports_pagination())
                .is_ok()
            {
                let s = self.state.query_status_cache.query_status(q);
                let should_try = if self.state.proxy_state.should_proxy() {
                    s.always
                } else {
                    true
                };
                status = Some(s);
                should_try
            } else {
                warn!(statement = %Sensitive(&q.statement),
                  "This statement could not be rewritten by ReadySet");
                matches!(
                    self.state.proxy_state,
                    ProxyState::Never | ProxyState::Fallback
                )
            };

        (should_try, status)
    }

    /// Handles a parsed set statement.
    ///
    /// If we have an upstream then we will pass valid set statements across to that upstream.
    /// If no upstream is present we will ignore the statement
    /// Disallowed set statements always produce an error
    fn handle_set(
        noria: &mut NoriaConnector,
        upstream: Option<&mut &mut DB>,
        settings: &BackendSettings,
        state: &mut BackendState<DB>,
        query: &str,
        set: &SetStatement,
        event: &mut QueryExecutionEvent,
    ) -> Result<(), DB::Error> {
        match Handler::handle_set_statement(set) {
            SetBehavior::Unsupported => {
                warn!(%set, "received unsupported SET statement");
                match settings.unsupported_set_mode {
                    UnsupportedSetMode::Error => {
                        let e = ReadySetError::SetDisallowed {
                            statement: query.to_string(),
                        };
                        if upstream.is_some() {
                            event.set_noria_error(&e);
                        }
                        return Err(e.into());
                    }
                    UnsupportedSetMode::Proxy => {
                        state.proxy_state = ProxyState::ProxyAlways;
                    }
                    UnsupportedSetMode::Allow => {}
                }
            }
            SetBehavior::Proxy => { /* Do nothing (the caller will proxy for us) */ }
            SetBehavior::SetAutocommit(on) => {
                warn!(%set, "received unsupported SET statement");
                match settings.unsupported_set_mode {
                    UnsupportedSetMode::Error if !on => {
                        let e = ReadySetError::SetDisallowed {
                            statement: query.to_string(),
                        };
                        if upstream.is_some() {
                            event.set_noria_error(&e);
                        }
                        return Err(e.into());
                    }
                    UnsupportedSetMode::Proxy => {
                        state.proxy_state.set_autocommit(on);
                    }
                    _ => {}
                }
            }
            SetBehavior::SetSearchPath(search_path) => {
                trace!(?search_path, "Setting search_path");
                noria.set_schema_search_path(search_path);
            }
        }

        Ok(())
    }

    #[instrument(level = "trace", name = "query", skip_all)]
    async fn query_adhoc_non_select<'a>(
        noria: &'a mut NoriaConnector,
        mut upstream: Option<&'a mut DB>,
        raw_query: &'a str,
        event: &mut QueryExecutionEvent,
        query: SqlQuery,
        settings: &BackendSettings,
        state: &mut BackendState<DB>,
    ) -> Result<QueryResult<'a, DB>, DB::Error> {
        match &query {
            SqlQuery::Set(s) => Self::handle_set(
                noria,
                upstream.as_mut(),
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
                    SqlQuery::Insert(InsertStatement { table: t, .. })
                    | SqlQuery::Update(UpdateStatement { table: t, .. })
                    | SqlQuery::Delete(DeleteStatement { table: t, .. }) => {
                        event.sql_type = SqlQueryType::Write;
                        let _t = event.start_upstream_timer();

                        // Update ticket if RYW enabled
                        let query_result = if cfg!(feature = "ryw") {
                            if let Some(timestamp_service) = &mut state.timestamp_client {
                                let (query_result, identifier) =
                                    upstream.handle_ryw_write(raw_query).await?;

                                // TODO(andrew): Move table name to table index conversion to
                                // timestamp service https://app.clubhouse.io/readysettech/story/331
                                let index = noria.node_index_of(t.name.as_str()).await?;
                                let affected_tables = vec![WriteKey::TableIndex(index)];

                                let new_timestamp = timestamp_service
                                    .append_write(WriteId::MySqlGtid(identifier), affected_tables)
                                    .map_err(|e| internal_err!("{e}"))?;

                                // TODO(andrew, justin): solidify error handling in client
                                // https://app.clubhouse.io/readysettech/story/366
                                let current_ticket = state.ticket.as_ref().ok_or_else(|| {
                                    internal_err!("RYW enabled backends must have a current ticket")
                                })?;

                                state.ticket =
                                    Some(Timestamp::join(current_ticket, &new_timestamp));
                                Ok(query_result)
                            } else {
                                upstream.query(raw_query).await
                            }
                        } else {
                            upstream.query(raw_query).await
                        };

                        query_result.map(QueryResult::Upstream)
                    }

                    // Table Create / Drop (RYW not supported)
                    // TODO(andrew, justin): how are these types of writes handled w.r.t RYW?
                    SqlQuery::CreateView(_)
                    | SqlQuery::CreateTable(_)
                    | SqlQuery::DropTable(_)
                    | SqlQuery::DropView(_)
                    | SqlQuery::AlterTable(_)
                    | SqlQuery::Use(_) => {
                        event.sql_type = SqlQueryType::Other;
                        upstream.query(raw_query).await.map(QueryResult::Upstream)
                    }
                    SqlQuery::RenameTable(_) => {
                        unsupported!("{} not yet supported", query.query_type());
                    }
                    SqlQuery::Set(_) | SqlQuery::CompoundSelect(_) | SqlQuery::Show(_) => {
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
                    | SqlQuery::DropCache(_)
                    | SqlQuery::DropAllCaches(_)
                    | SqlQuery::Explain(_) => {
                        unreachable!("path returns prior")
                    }
                }
            } else {
                // Interacting directly with ReadySet writer (No RYW support)
                //
                // TODO(andrew, justin): Do we want RYW support with the NoriaConnector?
                // Currently, no. TODO: Implement event execution metrics for
                // ReadySet without upstream.
                event.destination = Some(QueryDestination::Readyset);
                let start = Instant::now();

                let res = match &query {
                    SqlQuery::Select(_) => unreachable!("read path returns prior"),
                    // CREATE VIEW will still trigger migrations with epxlicit-migrations enabled
                    SqlQuery::CreateView(q) => noria.handle_create_view(q).await,
                    SqlQuery::CreateTable(q) => noria.handle_table_operation(q.clone()).await,
                    SqlQuery::AlterTable(q) => noria.handle_table_operation(q.clone()).await,
                    SqlQuery::DropTable(q) => noria.handle_table_operation(q.clone()).await,
                    SqlQuery::DropView(q) => noria.handle_table_operation(q.clone()).await,
                    SqlQuery::Insert(q) => noria.handle_insert(q).await,
                    SqlQuery::Update(q) => noria.handle_update(q).await,
                    SqlQuery::Delete(q) => noria.handle_delete(q).await,
                    // Return a empty result we are allowing unsupported set statements. Commit
                    // messages are dropped - we do not support transactions in noria standalone.
                    // We return an empty result set instead of an error to support test
                    // applications.
                    SqlQuery::Set(_) | SqlQuery::Commit(_) | SqlQuery::Use(_) => {
                        Ok(noria_connector::QueryResult::Empty)
                    }
                    _ => {
                        error!("unsupported query");
                        unsupported!("query type unsupported");
                    }
                };

                event.readyset_duration = Some(start.elapsed());
                event.noria_error = res.as_ref().err().cloned();
                Ok(QueryResult::Noria(res?))
            }
        };

        res
    }

    /// Executes `query` using the reader/writer belonging to the calling `Backend` struct.
    #[instrument_root(level = "info")]
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
                Err(e.into())
            }
            // Parse error, send to fallback
            Err(e) => {
                if !matches!(e, ReadySetError::ReaderMissingKey) {
                    warn!(error = %e, "Error received from noria, sending query to fallback");
                }
                let fallback_res =
                    Self::query_fallback(self.upstream.as_mut(), query, &mut event).await;
                if fallback_res.is_ok() {
                    self.state.query_status_cache.insert(query);

                    let (id, _) = self.state.query_status_cache.insert(query);
                    if let Some(ref telemetry_sender) = self.telemetry_sender {
                        if let Err(e) = telemetry_sender
                            .send_event_with_payload(
                                TelemetryEvent::QueryParseFailed,
                                TelemetryBuilder::new()
                                    .server_version(option_env!("CARGO_PKG_VERSION").unwrap_or_default())
                                    .query_id(id.to_string())
                                    .build(),
                            )
                        {
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
            // ReadySet extensions should never be proxied.
            Ok(ref parsed_query) if let Some(noria_extension) = self.query_noria_extensions(parsed_query, &mut event).await => {
                noria_extension.map(Into::into).map_err(Into::into)
            }
            // SET autocommit=1 needs to be handled explicitly or it will end up getting proxied in
            // most cases.
            Ok(SqlQuery::Set(s))
                if Handler::handle_set_statement(&s) == SetBehavior::SetAutocommit(true) =>
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
                if self.has_fallback() {
                    // Query requires a fallback and we can send it to fallback
                    Self::query_fallback(self.upstream.as_mut(), query, &mut event).await
                } else {
                    // Query requires a fallback, but none is available
                    Handler::default_response(parsed_query)
                        .map(QueryResult::Noria)
                        .map_err(Into::into)
                }
            }
            Ok(SqlQuery::Select(stmt)) => {
                let mut view_request = ViewCreateRequest::new(
                    stmt.clone(),
                    self.noria.schema_search_path().to_owned(),
                );
                let (noria_should_try, status) = self.noria_should_try_select(&mut view_request);
                if noria_should_try {
                    event.sql_type = SqlQueryType::Read;
                    if self.settings.query_log_ad_hoc_queries {
                        event.query = Some(Arc::new(SqlQuery::Select(stmt.clone())));
                    }
                    Self::query_adhoc_select(
                        &mut self.noria,
                        self.upstream.as_mut(),
                        &self.settings,
                        &mut self.state,
                        query,
                        stmt,
                        &view_request,
                        status,
                        &mut event,
                    )
                    .await
                } else {
                    Self::query_fallback(self.upstream.as_mut(), query, &mut event).await
                }
            }
            Ok(_) if self.state.proxy_state.should_proxy() => {
                Self::query_fallback(self.upstream.as_mut(), query, &mut event).await
            }
            Ok(parsed_query) => {
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
        };

        self.last_query = event.destination.map(|d| QueryInfo {
            destination: d,
            noria_error: event
                .noria_error
                .as_ref()
                .map(|e| e.to_string())
                .unwrap_or_default(),
        });

        log_query(query_log_sender.as_ref(), event, slowlog);

        result
    }

    /// Whether or not we have fallback enabled.
    pub fn has_fallback(&self) -> bool {
        self.upstream.is_some()
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

    // For debugging purposes
    pub fn ticket(&self) -> &Option<Timestamp> {
        &self.state.ticket
    }

    fn parse_query(&mut self, query: &str) -> ReadySetResult<SqlQuery> {
        match self.state.parsed_query_cache.entry(query.to_owned()) {
            Entry::Occupied(entry) => Ok(entry.get().clone()),
            Entry::Vacant(entry) => {
                trace!(%query, "Parsing query");
                match nom_sql::parse_query(self.settings.dialect, query) {
                    Ok(parsed_query) => Ok(entry.insert(parsed_query).clone()),
                    Err(_) => Err(ReadySetError::UnparseableQuery {
                        query: query.to_string(),
                    }),
                }
            }
        }
    }

    pub fn does_require_authentication(&self) -> bool {
        self.settings.require_authentication
    }
}

impl<DB, Handler> Drop for Backend<DB, Handler>
where
    DB: UpstreamDatabase,
{
    fn drop(&mut self) {
        metrics::decrement_gauge!(recorded::CONNECTED_CLIENTS, 1.0);
    }
}

/// Offloads recording query metrics to a separate thread. Sends a
/// message over a mpsc channel.
fn log_query(
    sender: Option<&UnboundedSender<QueryExecutionEvent>>,
    event: QueryExecutionEvent,
    slowlog: bool,
) {
    const SLOW_DURATION: std::time::Duration = std::time::Duration::from_millis(5);

    if slowlog
        && (event.upstream_duration.unwrap_or_default() > SLOW_DURATION
            || event.readyset_duration.unwrap_or_default() > SLOW_DURATION)
    {
        if let Some(query) = &event.query {
            warn!(query = %Sensitive(&query), readyset_time = ?event.readyset_duration, upstream_time = ?event.upstream_duration, "slow query");
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
