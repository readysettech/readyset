//!
//! [`Backend`] handles the execution of queries and prepared statements. Queries and
//! statements can be executed either on Noria itself, or on the upstream when applicable.
//! In general if an upstream (fallback) connection is available queries and statements
//! will execute as follows:
//!
//! * `INSERT`, `DELETE`, `UPDATE` - on upstream
//! * Anything inside a transaction - on upstream
//! * `SELECT` - on Noria
//! * Anything that failed on Noria, or while a migration is ongoing - on upstream
//!
//! # The execution flow
//!
//! ## Prepare
//!
//! When an upstream is available we will only try to prepare `SELECT` statements on Noria and
//! forward all other prepare requests to the upstream. For `SELECT` statements we will attempt
//! to prepare on both Noria and the upstream. The if Noria select fails we will perform a
//! fallback execution on the upstream (`execute_cascade`).
//!
//! ## Queries
//!
//! Queries are handled in a similar way to prepare statements. with the exception that additional
//! overhead is required to parse and rewrite them prior to their executuion.
//!
//! ## Migrations
//!
//! When a prepared statement is not immediately available for execution on Noria, we will
//! perform a migration, migrations can happen in one of three ways:
//!
//! * Explicit migrations: only `CREATE CACHE` and `CREATE VIEW` will cause migrations.
//! A `CREATE PREPARED STATEMENT` will not cause a migration, and queries will go to upstream
//! fallback. Enabled with the `--explicit-migrations` flag. However if a migration already
//! happened, we will use it.
//! * Async migration: prepared statements will be put in a [`QueryStatusCache`] and another
//! thread will perform migrations in the background. Once a statement finished migration it
//! will execute on Noria, while it is waiting for a migration to happen it will execute on
//! fallback. Enabled with the `--async-migrations` flag.
//! * In request path: migrations will happen when either `CREATE CACHE` or
//! `CREATE PREPARED STATEMENT` are called. It is also the only available option when a
//! upstream fallback is not availbale.
//!
//! ## Caching
//!
//! Since we don't want to pay a penalty every time we execute a prepared statement, either
//! on Noria or on the upstream fallback, we aggresively cache all the information required
//! for immediate execution. This way a statement can be immediately forwarded to either Noria
//! or upstream with no additional overhead.
//!
//! ## Handling unsupported queries
//!
//! Queries are marked with MigrationState::Unsupported when they fail to prepare on Noria
//! with an Unsupported ReadySetError. These queries should not be tried again against Noria,
//! however, if a fallback database exists, may be executed against the fallback.
//!
//! ## Handling component outage
//!
//! In a distributed deployment, a component (such as a noria-server instance) may go down, causing
//! some queries that rely on that server instance to fail. To help direct all affected queries
//! immediately to fallback when this happens, you can configure the --query-max-failure-seconds
//! flag to provide a maximum time in seconds that any given query may continuously fail for before
//! entering into a fallback only recovery period. You can configure the
//! --fallback-recovery-seconds flag to configure how long you would like this recovery period to
//! be enabled for, before allowing affected queries to be retried against noria.
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
use launchpad::redacted::Sensitive;
use mysql_common::row::convert::{FromRow, FromRowError};
use nom_sql::{
    CacheInner, CreateCacheStatement, DeleteStatement, Dialect, DropCacheStatement,
    InsertStatement, SelectStatement, ShowStatement, SqlIdentifier, SqlQuery, UpdateStatement,
};
use noria::consistency::Timestamp;
use noria::results::Results;
use noria::ColumnSchema;
use noria_client_metrics::{
    recorded, EventType, QueryDestination, QueryExecutionEvent, SqlQueryType,
};
use noria_data::DataType;
use noria_errors::ReadySetError::{self, PreparedStatementMissing};
use noria_errors::{internal, internal_err, unsupported, ReadySetResult};
use readyset_tracing::instrument_root;
use timestamp_service::client::{TimestampClient, WriteId, WriteKey};
use tokio::sync::mpsc::UnboundedSender;
use tracing::{error, instrument, trace, warn};

use crate::query_status_cache::{
    DeniedQuery, ExecutionInfo, ExecutionState, MigrationState, QueryStatus, QueryStatusCache,
};
use crate::upstream_database::NoriaCompare;
pub use crate::upstream_database::UpstreamPrepare;
use crate::{rewrite, QueryHandler, UpstreamDatabase};

pub mod noria_connector;

pub use self::noria_connector::NoriaConnector;

/// Query metadata used to plan query prepare
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
enum PrepareMeta {
    /// Query was received in a transaction
    InTransaction,
    /// Query could not be parsed
    FailedToParse,
    /// Query could not be rewritten for processing in noria
    FailedToRewrite,
    /// Noria does not implement this prepared statement. The statement may also be invalid SQL
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
}

/// Builder for a [`Backend`]
#[must_use]
#[derive(Clone)]
pub struct BackendBuilder {
    slowlog: bool,
    dialect: Dialect,
    mirror_ddl: bool,
    users: HashMap<String, String>,
    require_authentication: bool,
    ticket: Option<Timestamp>,
    timestamp_client: Option<TimestampClient>,
    query_log_sender: Option<UnboundedSender<QueryExecutionEvent>>,
    query_log_ad_hoc_queries: bool,
    validate_queries: bool,
    fail_invalidated_queries: bool,
    allow_unsupported_set: bool,
    migration_mode: MigrationMode,
    query_max_failure_seconds: u64,
    fallback_recovery_seconds: u64,
}

impl Default for BackendBuilder {
    fn default() -> Self {
        BackendBuilder {
            slowlog: false,
            dialect: Dialect::MySQL,
            mirror_ddl: false,
            users: Default::default(),
            require_authentication: true,
            ticket: None,
            timestamp_client: None,
            query_log_sender: None,
            query_log_ad_hoc_queries: false,
            validate_queries: false,
            fail_invalidated_queries: false,
            allow_unsupported_set: false,
            migration_mode: MigrationMode::InRequestPath,
            query_max_failure_seconds: (i64::MAX / 1000) as u64,
            fallback_recovery_seconds: 0,
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
        Backend {
            parsed_query_cache: HashMap::new(),
            prepared_statements: Vec::new(),
            noria,
            upstream,
            slowlog: self.slowlog,
            dialect: self.dialect,
            mirror_ddl: self.mirror_ddl,
            users: self.users,
            require_authentication: self.require_authentication,
            ticket: self.ticket,
            timestamp_client: self.timestamp_client,
            query_log_sender: self.query_log_sender,
            query_log_ad_hoc_queries: self.query_log_ad_hoc_queries,
            query_status_cache,
            validate_queries: self.validate_queries,
            fail_invalidated_queries: self.fail_invalidated_queries,
            allow_unsupported_set: self.allow_unsupported_set,
            migration_mode: self.migration_mode,
            last_query: None,
            query_max_failure_duration: Duration::new(self.query_max_failure_seconds, 0),
            fallback_recovery_duration: Duration::new(self.fallback_recovery_seconds, 0),
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

    pub fn mirror_ddl(mut self, mirror_ddl: bool) -> Self {
        self.mirror_ddl = mirror_ddl;
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

    pub fn allow_unsupported_set(mut self, allow_unsupported_set: bool) -> Self {
        self.allow_unsupported_set = allow_unsupported_set;
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
}

/// A [`CachedPreparedStatement`] stores the data needed for an immediate
/// execution of a prepared statement on either noria or the upstream
/// connection.
struct CachedPreparedStatement<DB>
where
    DB: UpstreamDatabase,
{
    /// Indicates if the statment was prepared for Noria, Fallback, or Both
    prep: PrepareResult<DB>,
    /// The current Noria migration state
    migration_state: MigrationState,
    /// Holds information about if executes have been succeeding, or failing, along with a state
    /// transition timestamp. None if prepared statement has never been executed.
    execution_info: Option<ExecutionInfo>,
    /// If query was succesfully parsed, will store the parsed query
    parsed_query: Option<Arc<SqlQuery>>,
    /// If statement was succesfully rewritten, will store the rewritten statement
    rewritten: Option<SelectStatement>,
}

impl<DB> CachedPreparedStatement<DB>
where
    DB: UpstreamDatabase,
{
    /// Returns whether we are currently in fallback recovery mode for the given prepared statement
    /// we are attempting to execute.
    /// WARNING: This will also mutate execution info timestamp if we have exceeded the supplied
    /// recovery period.
    pub fn in_fallback_recovery(
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

    pub fn is_unsupported_execute(&self) -> bool {
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
    // a cache of all previously parsed queries
    parsed_query_cache: HashMap<String, SqlQuery>,
    // all queries previously prepared on noria or upstream, mapped by their ID.
    prepared_statements: Vec<CachedPreparedStatement<DB>>,
    /// Noria connector used for reads, and writes when no upstream DB is present
    noria: NoriaConnector,
    /// Optional connector to the upstream DB. Used for fallback reads and all writes if it exists
    upstream: Option<DB>,
    slowlog: bool,
    /// SQL dialect to use when parsing queries from clients
    dialect: Dialect,
    /// Map from username to password for all users allowed to connect to the db
    pub users: HashMap<String, String>,
    pub require_authentication: bool,
    /// Current RYW ticket. `None` if RYW is not enabled. This `ticket` will
    /// be updated as the client makes writes so as to be an accurate low watermark timestamp
    /// required to make RYW-consistent reads. On reads, the client will pass in this ticket to be
    /// checked by noria view nodes.
    ticket: Option<Timestamp>,
    /// `timestamp_client` is the Backends connection to the TimestampService. The TimestampService
    /// is responsible for creating accurate RYW timestamps/tickets based on writes made by the
    /// Backend client.
    timestamp_client: Option<TimestampClient>,

    /// If set to `true`, all DDL changes will be mirrored to both the upstream db (if present) and
    /// noria. Otherwise, DDL changes will only go to the upstream if configured, or noria
    /// otherwise
    mirror_ddl: bool,

    query_log_sender: Option<UnboundedSender<QueryExecutionEvent>>,

    /// Whether to log ad-hoc queries by full query text in the query logger.
    query_log_ad_hoc_queries: bool,

    /// A cache of queries that we've seen, and their current state, used for processing
    query_status_cache: &'static QueryStatusCache,

    /// Run select statements with query validation.
    validate_queries: bool,
    fail_invalidated_queries: bool,

    /// Allow, but ignore, unsupported SQL `SET` statements.
    allow_unsupported_set: bool,

    /// How this backend handles migrations, See MigrationMode.
    migration_mode: MigrationMode,
    /// Information regarding the last query sent over this connection. If None, then no queries
    /// have been handled using this connection (Backend) yet.
    last_query: Option<QueryInfo>,

    /// The maximum duration that a query can continuously fail for before we enter into a recovery
    /// period.
    query_max_failure_duration: Duration,
    /// The recovery period that we enter into for a given query, when that query has
    /// repeatedly failed for query_max_failure_duration.
    fallback_recovery_duration: Duration,

    _query_handler: PhantomData<Handler>,
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
#[derive(Clone, Copy, PartialEq)]
pub enum MigrationMode {
    /// Handle migrations as part of the query process, if a query has not been
    /// successfully migrated when we are processing the query, attempt to
    /// perform the migration as part of the query.
    InRequestPath,
    /// Never perform migrations in the query path. If a query has not been
    /// migrated yet, send it to fallback if fallback exists, otherwise reject
    /// the query.
    ///
    /// This mode is used when something other operation is performing the
    /// migrations and updating a queries migration status. Either
    /// --async-migrations which runs migrations in a separate thread,
    /// or --explicit-migrations which enables special syntax to perform
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
}

/// The type returned when a query is carried out by `Backend`, through either the `query` or
/// `execute` functions.
pub enum QueryResult<'a, DB: UpstreamDatabase> {
    /// Results from noria
    Noria(noria_connector::QueryResult<'a>),
    /// Results from upstream
    Upstream(DB::QueryResult),
}

impl<'a, DB: UpstreamDatabase> From<noria_connector::QueryResult<'a>> for QueryResult<'a, DB> {
    fn from(r: noria_connector::QueryResult<'a>) -> Self {
        Self::Noria(r)
    }
}

// Because js_client needs an owned version to pass we implement a manual
// `into_owned`, the things we do for performance
impl<'a, DB: UpstreamDatabase> QueryResult<'a, DB> {
    pub fn into_owned(self) -> QueryResult<'static, DB> {
        match self {
            QueryResult::Noria(n) => QueryResult::Noria(n.into_owned()),
            QueryResult::Upstream(u) => QueryResult::Upstream(u),
        }
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
/// 2. If we think we can support a query, try to send it to Noria. If that hits an error that
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
    /// The identifier of the last prepared statement (which is always the last in the vector)
    pub fn last_prepared_id(&self) -> u32 {
        (self.prepared_statements.len() - 1)
            .try_into()
            .expect("Too many prepared statements")
    }

    /// The identifier we can reserve for the next prepared statement
    pub fn next_prepared_id(&self) -> u32 {
        (self.prepared_statements.len())
            .try_into()
            .expect("Too many prepared statements")
    }

    // Returns whether we are in a transaction currently or not. Transactions are only supported
    // over fallback, so if we have no fallback connector we return false.
    fn is_in_tx(&self) -> bool {
        if let Some(db) = self.upstream.as_ref() {
            db.is_in_tx()
        } else {
            false
        }
    }

    /// Check whether the set statement is explicitly allowed. All other set
    /// statements should return an error
    pub fn is_allowed_set_statement(&self, set: &nom_sql::SetStatement) -> bool {
        self.allow_unsupported_set || Handler::is_set_allowed(set)
    }

    /// Executes query on the upstream database, for when it cannot be parsed or executed by noria.
    /// Returns the query result, or an error if fallback is not configured
    #[instrument_root(level = "info")]
    pub async fn query_fallback(
        &mut self,
        query: &str,
        event: &mut QueryExecutionEvent,
    ) -> Result<QueryResult<'static, DB>, DB::Error> {
        let upstream = self.upstream.as_mut().ok_or_else(|| {
            ReadySetError::Internal("This case requires an upstream connector".to_string())
        })?;
        event.destination = Some(QueryDestination::Fallback);
        let _t = event.start_upstream_timer();
        upstream.query(query).await.map(QueryResult::Upstream)
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

    /// Prepares query against Noria. If an upstream database exists, the prepare is mirrored to
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
            .then_some(
                self.noria
                    .prepare_select(select_meta.stmt.clone(), prep_idx, do_migrate),
            )
            .into();

        let (upstream_res, noria_res) = future::join(up_prep, noria_prep).await;

        let destination = match (upstream_res.is_some(), noria_res.is_some()) {
            (true, true) => Some(QueryDestination::Both),
            (false, true) => Some(QueryDestination::Noria),
            (true, false) => Some(QueryDestination::Fallback),
            (false, false) => None,
        };

        self.last_query = destination.map(|d| QueryInfo {
            destination: d,
            noria_error: String::new(),
        });

        // Update noria migration state for query
        match &noria_res {
            Some(Ok(noria_connector::PrepareResult::Select { schema, params, .. })) => {
                let mut state = MigrationState::Successful;

                if let Some(Ok(upstream_res)) = &upstream_res {
                    // If we are using `validate_queries`, a query that was successfully
                    // migrated may actually be unsupported if the schema or columns do not
                    // match up.
                    if self.validate_queries {
                        if let Err(e) = upstream_res.meta.compare(schema, params) {
                            if self.fail_invalidated_queries {
                                internal!("Query comparison failed to validate: {}", e);
                            }
                            warn!(error = %e, query = %Sensitive(&select_meta.stmt), "Query compare failed");
                            state = MigrationState::Unsupported;
                        }
                    }
                }

                self.query_status_cache
                    .update_query_migration_state(&select_meta.rewritten, state);
            }
            Some(Err(e)) => {
                if e.caused_by_view_not_found() {
                    warn!(error = %e, "View not found during mirror_prepare()");
                    self.query_status_cache.update_query_migration_state(
                        &select_meta.rewritten,
                        MigrationState::Pending,
                    );
                } else if e.caused_by_unsupported() {
                    self.query_status_cache.update_query_migration_state(
                        &select_meta.rewritten,
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
            (None, Some(Ok(noria_res))) => PrepareResult::Noria(noria_res),
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
                destination: QueryDestination::Fallback,
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
                destination: QueryDestination::Noria,
                noria_error: String::new(),
            });
            Ok(PrepareResult::Noria(res))
        }
    }

    /// Provides metadata required to prepare a select query
    fn plan_prepare_select(&mut self, stmt: nom_sql::SelectStatement) -> PrepareMeta {
        let mut rewritten = stmt.clone();
        if rewrite::process_query(&mut rewritten).is_err() {
            warn!(statement = %Sensitive(&stmt), "This statement could not be rewritten by ReadySet");
            PrepareMeta::FailedToRewrite
        } else {
            // For select statements we will always try to check with noria if it already migrated,
            // unless it is explicitely known to be not supported.
            let should_do_noria = self.query_status_cache.query_migration_state(&rewritten)
                != MigrationState::Unsupported;
            // For select statements only InRequestPath should trigger migrations synchornously,
            // or if no upstream is present
            let must_migrate =
                self.migration_mode == MigrationMode::InRequestPath || !self.has_fallback();

            PrepareMeta::Select(PrepareSelectMeta {
                stmt,
                rewritten,
                should_do_noria,
                must_migrate,
            })
        }
    }

    /// Provides metadata required to prepare a query
    async fn plan_prepare(&mut self, query: &str) -> PrepareMeta {
        if self.is_in_tx() {
            return PrepareMeta::InTransaction;
        }

        let parsed_query = match self.parse_query(query) {
            Ok(pq) => pq,
            Err(_) => {
                warn!(query = %Sensitive(&query), "ReadySet failed to parse query");
                return PrepareMeta::FailedToParse;
            }
        };

        match parsed_query {
            SqlQuery::Select(stmt) => self.plan_prepare_select(stmt),
            SqlQuery::Insert(_) | SqlQuery::Update(_) | SqlQuery::Delete(_) => {
                PrepareMeta::Write { stmt: parsed_query }
            }
            // Invalid prepare statement
            SqlQuery::CreateTable(..)
            | SqlQuery::CreateView(..)
            | SqlQuery::Set(..)
            | SqlQuery::StartTransaction(..)
            | SqlQuery::Commit(..)
            | SqlQuery::Rollback(..)
            | SqlQuery::Use(..)
            | SqlQuery::Show(..)
            | SqlQuery::CompoundSelect(..)
            | SqlQuery::DropTable(..)
            | SqlQuery::AlterTable(..)
            | SqlQuery::RenameTable(..)
            | SqlQuery::CreateCache(..)
            | SqlQuery::DropCache(..)
            | SqlQuery::Explain(_) => {
                warn!(statement = %Sensitive(&parsed_query), "Statement cannot be prepared by ReadySet");
                PrepareMeta::Unimplemented
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
            PrepareMeta::InTransaction
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
                    destination: QueryDestination::Fallback,
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

        let (parsed_query, migration_state, rewritten) = match meta {
            PrepareMeta::Write { stmt } => (Some(Arc::new(stmt)), MigrationState::Successful, None),
            PrepareMeta::Select(PrepareSelectMeta {
                stmt, rewritten, ..
            }) => (
                Some(Arc::new(SqlQuery::Select(stmt))),
                self.query_status_cache.query_migration_state(&rewritten),
                Some(rewritten),
            ),
            _ => (None, MigrationState::Successful, None),
        };

        let cache_entry = CachedPreparedStatement {
            prep: res,
            migration_state,
            execution_info: None,
            parsed_query,
            rewritten,
        };

        self.prepared_statements.push(cache_entry);

        Ok(&self.prepared_statements.last().unwrap().prep)
    }

    /// Executes a prepared statement on Noria
    async fn execute_noria<'a>(
        noria: &'a mut NoriaConnector,
        prep: &noria_connector::PrepareResult,
        params: &[DataType],
        ticket: Option<Timestamp>,
        event: &mut QueryExecutionEvent,
    ) -> ReadySetResult<QueryResult<'a, DB>> {
        use noria_connector::PrepareResult::*;

        event.destination = Some(QueryDestination::Noria);
        let start = Instant::now();

        let res = match prep {
            Select {
                statement_id: id, ..
            } => {
                noria
                    .execute_prepared_select(*id, params, ticket, event)
                    .await
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

        event.noria_duration = Some(start.elapsed());

        res
    }

    /// Execute a prepared statement on Noria
    async fn execute_upstream<'a>(
        upstream: &'a mut Option<DB>,
        prep: &UpstreamPrepare<DB>,
        params: &[DataType],
        event: &mut QueryExecutionEvent,
        is_fallback: bool,
    ) -> Result<QueryResult<'a, DB>, DB::Error> {
        let upstream = upstream.as_mut().ok_or_else(|| {
            ReadySetError::Internal("This condition requires an upstream connector".to_string())
        })?;

        if is_fallback {
            event.destination = Some(QueryDestination::NoriaThenFallback);
        } else {
            event.destination = Some(QueryDestination::Fallback);
        }

        let _t = event.start_upstream_timer();

        upstream
            .execute(prep.statement_id, params)
            .await
            .map(|r| QueryResult::Upstream(r))
    }

    /// Execute on Noria, and if fails execute on upstream
    #[allow(clippy::too_many_arguments)] // meh.
    async fn execute_cascade<'a>(
        noria: &'a mut NoriaConnector,
        upstream: &'a mut Option<DB>,
        noria_prep: &noria_connector::PrepareResult,
        upstream_prep: &UpstreamPrepare<DB>,
        params: &[DataType],
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
                if !matches!(noria_err, ReadySetError::ReaderMissingKey) {
                    warn!(error = %noria_err, "Error received from noria, sending query to fallback");
                }

                Self::execute_upstream(upstream, upstream_prep, params, event, true).await
            }
        }
    }

    /// Attempts to migrate a query on noria, after it was marked as Succesful in the cache. If the
    /// migration is succesful, the cached entry is marked as such and will attempt to resolve
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
            SqlQuery::Select(stmt) => noria.prepare_select(stmt.clone(), id, false).await?,
            _ => internal!("Only SELECT statements can be pending migration"),
        };

        // At this point we got a succesful noria prepare, so we want to replace the Upstream result
        // with a Both result
        cached_entry.prep = PrepareResult::Both(noria_prep, upstream_prep);
        cached_entry.migration_state = MigrationState::Successful;

        Ok(())
    }

    fn invalidate_prepared_cache_entry(prep: &mut PrepareResult<DB>) {
        match prep {
            //We can't invalidate the Noria entry if there is no fallback
            PrepareResult::Noria(_) => {}
            //Nothing to see here
            PrepareResult::Upstream(_) => {}
            PrepareResult::Both(_, u) => *prep = PrepareResult::Upstream(u.clone()),
        }
    }

    /// Iterate over the cache of the prepared statements, and invalidate those that are
    /// equal to the one provided
    fn invalidate_prepared_statments_cache(&mut self, stmt: &SelectStatement) {
        // Linear scan, but we shouldn't be doing it often, right?
        self.prepared_statements
            .iter_mut()
            .filter_map(
                |CachedPreparedStatement {
                     prep,
                     migration_state,
                     rewritten,
                     ..
                 }| {
                    if *migration_state == MigrationState::Successful
                        && rewritten.as_ref() == Some(stmt)
                    {
                        *migration_state = MigrationState::Pending;
                        Some(prep)
                    } else {
                        None
                    }
                },
            )
            .for_each(Self::invalidate_prepared_cache_entry);
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
        params: &[DataType],
    ) -> Result<QueryResult<'_, DB>, DB::Error> {
        self.last_query = None;
        let cached_statement = self
            .prepared_statements
            .get_mut(id as usize)
            .ok_or(PreparedStatementMissing { statement_id: id })?;

        let mut event = QueryExecutionEvent::new(EventType::Execute);
        event.query = cached_statement.parsed_query.clone();

        let upstream = &mut self.upstream;
        let noria = &mut self.noria;
        let ticket = self.ticket.clone();

        if cached_statement.migration_state.is_pending() {
            // We got a statement with a pending migration, we want to check if migration is
            // finished by now
            let new_migration_state = self.query_status_cache.query_migration_state(
                cached_statement
                    .rewritten
                    .as_ref()
                    .expect("Pending must have rewritten set"),
            );

            if new_migration_state == MigrationState::Successful {
                // Attempt to prepare on Noria
                let _ = Self::update_noria_prepare(noria, cached_statement, id).await;
            }
        }

        let should_fallback = cached_statement.in_fallback_recovery(
            self.query_max_failure_duration,
            self.fallback_recovery_duration,
        ) || cached_statement.is_unsupported_execute();

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
                Self::invalidate_prepared_cache_entry(&mut cached_statement.prep);
            } else if e.caused_by_unsupported() {
                // On an unsupported execute we update the query migration state to be unsupported.
                //
                // Must exist or we would not have executed the query against Noria.
                #[allow(clippy::unwrap_used)]
                self.query_status_cache.update_query_migration_state(
                    cached_statement.rewritten.as_ref().unwrap(),
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
        log_query(self.query_log_sender.as_ref(), event, self.slowlog);

        result
    }

    /// Should only be called with a SqlQuery that is of type StartTransaction, Commit, or
    /// Rollback. Used to handle transaction boundary queries.
    pub async fn handle_transaction_boundaries(
        &mut self,
        query: &SqlQuery,
    ) -> Result<QueryResult<'static, DB>, DB::Error> {
        let upstream = self.upstream.as_mut().ok_or_else(|| {
            ReadySetError::Internal("This case requires an upstream connector".to_string())
        })?;

        match query {
            SqlQuery::StartTransaction(_) => upstream.start_tx().await.map(QueryResult::Upstream),
            SqlQuery::Commit(_) => upstream.commit().await.map(QueryResult::Upstream),
            SqlQuery::Rollback(_) => upstream.rollback().await.map(QueryResult::Upstream),
            _ => {
                error!("handle_transaction_boundary was called with a SqlQuery that was not of type StartTransaction, Commit, or Rollback");
                internal!("handle_transaction_boundary was called with a SqlQuery that was not of type StartTransaction, Commit, or Rollback");
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
        name: Option<&str>,
        mut stmt: SelectStatement,
    ) -> ReadySetResult<noria_connector::QueryResult<'static>> {
        // If we have another query with the same name, drop that query first
        if let Some(name) = name {
            if let Some(stmt) = self.noria.select_statement_from_name(name) {
                warn!(
                    "Dropping query previously cached as {name}, stmt={}",
                    Sensitive(&stmt)
                );
                self.drop_cached_query(name).await?;
            }
        }
        // Now migrate the new query
        rewrite::process_query(&mut stmt)?;
        self.noria.handle_create_cached_query(name, &stmt).await?;
        self.query_status_cache
            .update_query_migration_state(&stmt, MigrationState::Successful);
        Ok(noria_connector::QueryResult::Empty)
    }

    /// Forwards a `DROP CACHE` request to noria
    async fn drop_cached_query(
        &mut self,
        name: &str,
    ) -> ReadySetResult<noria_connector::QueryResult<'static>> {
        let maybe_select_statement = self.noria.select_statement_from_name(name);
        self.noria.drop_view(name).await?;
        if let Some(stmt) = maybe_select_statement {
            self.query_status_cache
                .update_query_migration_state(&stmt, MigrationState::Pending);
            self.invalidate_prepared_statments_cache(&stmt);
        }
        Ok(noria_connector::QueryResult::Empty)
    }

    /// Responds to a `SHOW PROXIED QUERIES` query
    async fn show_proxied_queries(
        &mut self,
    ) -> ReadySetResult<noria_connector::QueryResult<'static>> {
        let create_dummy_column = |n: &str| ColumnSchema {
            spec: nom_sql::ColumnSpecification {
                column: nom_sql::Column {
                    name: n.into(),
                    table: None,
                },
                sql_type: nom_sql::SqlType::Text,
                constraints: vec![],
                comment: None,
            },
            base: None,
        };

        let queries = self.query_status_cache.deny_list();
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
            .map(
                |DeniedQuery {
                     id,
                     mut query,
                     status,
                 }| {
                    let s = match status.migration_state {
                        MigrationState::DryRunSucceeded | MigrationState::Successful => "yes",
                        MigrationState::Pending => "pending",
                        MigrationState::Unsupported => "unsupported",
                    }
                    .to_string();
                    rewrite::anonymize_literals(&mut query);
                    vec![
                        DataType::from(id),
                        DataType::from(query.to_string()),
                        DataType::from(s),
                    ]
                },
            )
            .collect::<Vec<_>>();
        let data = vec![Results::new(
            data,
            Arc::new([
                "query id".into(),
                "proxied query".into(),
                "readyset supported".into(),
            ]),
        )];
        Ok(noria_connector::QueryResult::Select {
            data,
            select_schema,
        })
    }

    async fn query_noria_extensions<'a>(
        &'a mut self,
        query: &'a SqlQuery,
        event: &mut QueryExecutionEvent,
    ) -> Option<ReadySetResult<noria_connector::QueryResult<'static>>> {
        event.sql_type = SqlQueryType::Other; // Those will get cleared if it was not destined to noria
        event.destination = Some(QueryDestination::Noria);

        let _t = event.start_noria_timer();

        let res = match query {
            SqlQuery::Explain(nom_sql::ExplainStatement::LastStatement) => {
                self.explain_last_statement()
            }
            SqlQuery::Explain(nom_sql::ExplainStatement::Graphviz { simplified }) => {
                self.noria.graphviz(*simplified).await
            }
            SqlQuery::CreateCache(CreateCacheStatement { name, inner }) => {
                let st = match inner {
                    CacheInner::Statement(st) => *st.clone(),
                    CacheInner::Id(id) => match self.query_status_cache.query(id.as_str()) {
                        Some(st) => st,
                        None => {
                            return Some(Err(ReadySetError::NoQueryForId { id: id.to_string() }))
                        }
                    },
                };
                self.create_cached_query(name.as_deref(), st).await
            }
            SqlQuery::DropCache(DropCacheStatement { name }) => {
                self.drop_cached_query(name.as_str()).await
            }
            SqlQuery::Show(ShowStatement::CachedQueries) => self.noria.verbose_outputs().await,
            SqlQuery::Show(ShowStatement::ReadySetStatus) => self.noria.readyset_status().await,
            SqlQuery::Show(ShowStatement::ProxiedQueries) => self.show_proxied_queries().await,
            _ => {
                drop(_t);
                event.noria_duration.take(); // Clear noria timer, since it was not a noria request
                return None;
            }
        };

        Some(res)
    }

    async fn query_adhoc_select(
        &mut self,
        original_query: &str,
        stmt: &SelectStatement,
        event: &mut QueryExecutionEvent,
    ) -> Result<QueryResult<'_, DB>, DB::Error> {
        event.sql_type = SqlQueryType::Read;
        if self.query_log_ad_hoc_queries {
            event.query = Some(Arc::new(SqlQuery::Select(stmt.clone())));
        }

        // TODO(vlad): don't rewrite multiple times, it is wasteful
        let mut rewritten = stmt.clone();
        let mut status = if rewrite::process_query(&mut rewritten).is_ok() {
            self.query_status_cache.query_status(&rewritten)
        } else {
            QueryStatus {
                migration_state: MigrationState::Unsupported,
                execution_info: None,
            }
        };
        let original_status = status.clone();
        let did_work = if let Some(ref mut i) = status.execution_info {
            i.reset_if_exceeded_recovery(
                self.query_max_failure_duration,
                self.fallback_recovery_duration,
            )
        } else {
            false
        };

        if self.has_fallback()
            && (self.migration_mode != MigrationMode::InRequestPath
                && status.migration_state != MigrationState::Successful)
            || (status.migration_state == MigrationState::Unsupported)
            || (self.has_fallback()
                && status
                    .execution_info
                    .as_mut()
                    .map(|i| i.execute_network_failure_exceeded(self.query_max_failure_duration))
                    .unwrap_or(false))
        {
            if did_work {
                #[allow(clippy::unwrap_used)] // Validated by did_work.
                self.query_status_cache.update_transition_time(
                    &rewritten,
                    &status.execution_info.unwrap().last_transition_time,
                );
            }
            return self.query_fallback(original_query, event).await;
        }

        let noria_res = {
            event.destination = Some(QueryDestination::Noria);
            let start = Instant::now();
            let res = self
                .noria
                .handle_select(
                    stmt.clone(),
                    self.ticket.clone(),
                    self.migration_mode == MigrationMode::InRequestPath,
                    event,
                )
                .await;
            event.noria_duration = Some(start.elapsed());
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
                // We managed to select on Noria, good for us
                status.migration_state = MigrationState::Successful;
                if let Some(i) = status.execution_info.as_mut() {
                    i.execute_succeeded()
                }
                if status != original_status {
                    self.query_status_cache
                        .update_query_status(&rewritten, status);
                }
                Ok(noria_ok.into())
            }
            Err(noria_err) => {
                event.set_noria_error(&noria_err);

                if let Some(i) = status.execution_info.as_mut() {
                    if noria_err.is_networking_related() {
                        i.execute_network_failure();
                    }
                }

                if noria_err.caused_by_view_not_found() {
                    status.migration_state = MigrationState::Pending;
                } else if noria_err.caused_by_unsupported() {
                    status.migration_state = MigrationState::Unsupported;
                };

                if status != original_status {
                    self.query_status_cache
                        .update_query_status(&rewritten, status);
                }

                // Try to execute on fallback if present
                if let Some(fallback) = self.upstream.as_mut() {
                    event.destination = Some(QueryDestination::NoriaThenFallback);
                    let _t = event.start_upstream_timer();
                    fallback
                        .query(&original_query)
                        .await
                        .map(QueryResult::Upstream)
                } else {
                    Err(noria_err.into())
                }
            }
        }
    }

    /// Executes `query` using the reader/writer belonging to the calling `Backend` struct.
    #[instrument_root(level = "info")]
    #[inline]
    pub async fn query(&mut self, query: &str) -> Result<QueryResult<'_, DB>, DB::Error> {
        let mut event = QueryExecutionEvent::new(EventType::Query);
        let query_log_sender = self.query_log_sender.clone();
        let slowlog = self.slowlog;

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
                self.query_fallback(query, &mut event).await
            }
            // Parsed but is in transaction so send to fallback
            Ok(_) if self.is_in_tx() => self.query_fallback(query, &mut event).await,
            Ok(ref parsed_query) if Handler::requires_fallback(parsed_query) => {
                if self.has_fallback() {
                    // Query requires a fallback and we can send it to fallback
                    self.query_fallback(query, &mut event).await
                } else {
                    // Query requires a fallback, but none is availbale
                    Handler::default_response(parsed_query)
                        .map(QueryResult::Noria)
                        .map_err(Into::into)
                }
            }
            Ok(ref parsed_query) if let Some(noria_extension) = self.query_noria_extensions(parsed_query, &mut event).await => {
                noria_extension.map(Into::into).map_err(Into::into)
            }
            Ok(SqlQuery::Select(ref stmt)) => self.query_adhoc_select(query, stmt, &mut event).await,
            Ok(parsed_query) => self.query_adhoc_non_select(query, &mut event, parsed_query).await,
        }
        .map(|r| r.into_owned());

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
        &self.ticket
    }

    fn parse_query(&mut self, query: &str) -> ReadySetResult<SqlQuery> {
        match self.parsed_query_cache.entry(query.to_owned()) {
            Entry::Occupied(entry) => Ok(entry.get().clone()),
            Entry::Vacant(entry) => {
                trace!("Parsing query");
                match nom_sql::parse_query(self.dialect, query) {
                    Ok(parsed_query) => Ok(entry.insert(parsed_query).clone()),
                    Err(_) => {
                        // error is useless anyway
                        error!("query can't be parsed: \"{}\"", Sensitive(&query));
                        Err(ReadySetError::UnparseableQuery {
                            query: query.to_string(),
                        })
                    }
                }
            }
        }
    }

    #[instrument(level = "trace", name = "query", skip_all)]
    async fn query_adhoc_non_select(
        &mut self,
        query: &str,
        event: &mut QueryExecutionEvent,
        parse_result: SqlQuery,
    ) -> Result<QueryResult<'_, DB>, DB::Error> {
        let parsed_query = &parse_result;

        // If we have an upstream then we will pass valid set statements across to that upstream.
        // If no upstream is present we will ignore the statement
        // Disallowed set statements always produce an error
        if let SqlQuery::Set(s) = parsed_query {
            if !self.is_allowed_set_statement(s) {
                warn!(%s, "received unsupported SET statement");
                let e = ReadySetError::SetDisallowed {
                    statement: parsed_query.to_string(),
                };
                if self.has_fallback() {
                    event.set_noria_error(&e);
                }
                return Err(e.into());
            }
        }

        macro_rules! handle_ddl {
            ($noria_method: ident ($stmt: expr)) => {
                if let Some(upstream) = &mut self.upstream {
                    if self.mirror_ddl {
                        if let Err(e) = self.noria.$noria_method($stmt).await {
                            event.set_noria_error(&e);
                        }
                        self.last_query = Some(QueryInfo {
                            destination: QueryDestination::Both,
                            noria_error: String::new(),
                        });
                    } else {
                        self.last_query = Some(QueryInfo {
                            destination: QueryDestination::Fallback,
                            noria_error: String::new(),
                        });
                    }
                    let upstream_res = upstream.query(query).await;
                    Ok(QueryResult::Upstream(upstream_res?))
                } else {
                    self.last_query = Some(QueryInfo {
                        destination: QueryDestination::Noria,
                        noria_error: String::new(),
                    });
                    Ok(QueryResult::Noria(self.noria.$noria_method($stmt).await?))
                }
            };
        }

        let res = {
            // Upstream reads are tried when noria reads produce an error. Upstream writes are done
            // by default when the upstream connector is present.
            if let Some(ref mut upstream) = self.upstream {
                match parsed_query {
                    SqlQuery::Select(_) => unreachable!("read path returns prior"),
                    SqlQuery::Insert(InsertStatement { table: t, .. })
                    | SqlQuery::Update(UpdateStatement { table: t, .. })
                    | SqlQuery::Delete(DeleteStatement { table: t, .. }) => {
                        event.sql_type = SqlQueryType::Write;
                        event.destination = Some(QueryDestination::Fallback);

                        let _t = event.start_upstream_timer();
                        // Update ticket if RYW enabled
                        let query_result = if cfg!(feature = "ryw") {
                            if let Some(timestamp_service) = &mut self.timestamp_client {
                                let (query_result, identifier) =
                                    upstream.handle_ryw_write(query).await?;

                                // TODO(andrew): Move table name to table index conversion to
                                // timestamp service https://app.clubhouse.io/readysettech/story/331
                                let index = self.noria.node_index_of(t.name.as_str()).await?;
                                let affected_tables = vec![WriteKey::TableIndex(index)];

                                let new_timestamp = timestamp_service
                                    .append_write(WriteId::MySqlGtid(identifier), affected_tables)
                                    .map_err(|e| internal_err(e.to_string()))?;

                                // TODO(andrew, justin): solidify error handling in client
                                // https://app.clubhouse.io/readysettech/story/366
                                let current_ticket = &self.ticket.as_ref().ok_or_else(|| {
                                    internal_err("RYW enabled backends must have a current ticket")
                                })?;

                                self.ticket = Some(Timestamp::join(current_ticket, &new_timestamp));
                                Ok(query_result)
                            } else {
                                upstream.query(query).await
                            }
                        } else {
                            upstream.query(query).await
                        };

                        self.last_query = Some(QueryInfo {
                            destination: QueryDestination::Fallback,
                            noria_error: String::new(),
                        });
                        Ok(QueryResult::Upstream(query_result?))
                    }

                    // Table Create / Drop (RYW not supported)
                    // TODO(andrew, justin): how are these types of writes handled w.r.t RYW?
                    // CREATE VIEW will still trigger migrations with explicit-migrations enabled
                    SqlQuery::CreateView(stmt) => {
                        handle_ddl!(handle_create_view(stmt))
                    }
                    SqlQuery::CreateTable(stmt) => {
                        handle_ddl!(handle_table_operation(stmt.clone()))
                    }
                    SqlQuery::DropTable(stmt) => {
                        handle_ddl!(handle_table_operation(stmt.clone()))
                    }
                    SqlQuery::AlterTable(stmt) => {
                        handle_ddl!(handle_table_operation(stmt.clone()))
                    }
                    SqlQuery::RenameTable(_) => {
                        unsupported!("{} not yet supported", parsed_query.query_type());
                    }
                    SqlQuery::Set(_) | SqlQuery::CompoundSelect(_) | SqlQuery::Show(_) => {
                        let res = upstream.query(query).await.map(QueryResult::Upstream);
                        self.last_query = Some(QueryInfo {
                            destination: QueryDestination::Fallback,
                            noria_error: String::new(),
                        });
                        res
                    }
                    SqlQuery::StartTransaction(_) | SqlQuery::Commit(_) | SqlQuery::Rollback(_) => {
                        let res = self.handle_transaction_boundaries(parsed_query).await;
                        self.last_query = Some(QueryInfo {
                            destination: QueryDestination::Fallback,
                            noria_error: String::new(),
                        });
                        res
                    }
                    SqlQuery::Use(stmt) => {
                        match self.upstream.as_ref().and_then(|u| u.database()) {
                            Some(db) if stmt.database == db => {
                                Ok(QueryResult::Noria(noria_connector::QueryResult::Empty))
                            }
                            _ => {
                                error!("USE statement attempted to change the database");
                                unsupported!("USE");
                            }
                        }
                    }
                    SqlQuery::CreateCache(_) | SqlQuery::DropCache(_) | SqlQuery::Explain(_) => {
                        unreachable!("path returns prior")
                    }
                }
            } else {
                // Interacting directly with Noria writer (No RYW support)
                //
                // TODO(andrew, justin): Do we want RYW support with the NoriaConnector? Currently,
                // no. TODO: Implement event execution metrics for Noria without
                // upstream.
                event.destination = Some(QueryDestination::Noria);
                let start = Instant::now();

                let res = match parsed_query {
                    // CREATE VIEW will still trigger migrations with epxlicit-migrations enabled
                    SqlQuery::CreateView(q) => self.noria.handle_create_view(q).await,
                    SqlQuery::CreateTable(q) => self.noria.handle_table_operation(q.clone()).await,
                    SqlQuery::AlterTable(q) => self.noria.handle_table_operation(q.clone()).await,
                    SqlQuery::DropTable(q) => self.noria.handle_table_operation(q.clone()).await,
                    SqlQuery::Select(q) => {
                        let res = self
                            .noria
                            .handle_select(q.clone(), self.ticket.clone(), true, event)
                            .await;
                        res
                    }
                    SqlQuery::Insert(q) => self.noria.handle_insert(q).await,
                    SqlQuery::Update(q) => self.noria.handle_update(q).await,
                    SqlQuery::Delete(q) => self.noria.handle_delete(q).await,
                    // Return a empty result we are allowing unsupported set statements. Commit
                    // messages are dropped - we do not support transactions in noria standalone.
                    // We return an empty result set instead of an error to support test
                    // applications.
                    SqlQuery::Set(_) | SqlQuery::Commit(_) => {
                        Ok(noria_connector::QueryResult::Empty)
                    }
                    _ => {
                        error!("unsupported query");
                        unsupported!("query type unsupported");
                    }
                }?;
                event.noria_duration = Some(start.elapsed());

                Ok(QueryResult::Noria(res))
            }
        };

        res
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
            || event.noria_duration.unwrap_or_default() > SLOW_DURATION)
    {
        if let Some(query) = &event.query {
            warn!(query = %Sensitive(&query), noria_time = ?event.noria_duration, upstream_time = ?event.upstream_duration, "slow query");
        }
    }

    if let Some(sender) = sender {
        // Drop the error if something goes wrong with query logging.
        if let Err(e) = sender.send(event) {
            warn!("Error logging query with query logging enabled: {}", e);
        }
    }
}
