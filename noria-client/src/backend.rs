//! # The query read path (SELECT queries)
//! Before a query can be executed against Noria, a migration, the creation of all
//! the dataflow state, must be performed. When this is done depends on:
//!   - The migration mode of the backend: Migrations may be done in the request path with
//!     InRequestPath, or outside the request path with MigrationMode::OutOfBand, via
//!     the CREATE_QUERY_CACHE query, with --explicit-migrations, or an async thread with
//!     --async-migrations.
//!   - The current migration status of a query in the query status cache. We do not try to perform
//!     migrations for queries that have already been successfully migrated, or have been marked
//!     unsupported.
//!
//! ## Handling unsupported queries
//! Queries are marked with MigrationState::Unsupported when they fail to prepare after many
//! attempts or explicitely return an Unsupported ReadySetError. These queries should not be tried
//! again against Noria, however, if a fallback database exists, may be executed against the
//! fallback db.
use std::borrow::Cow;
use std::fmt;
use std::fmt::Debug;
use std::sync::Arc;
use std::time;
use std::{collections::HashMap, str::FromStr};
use std::{
    collections::{hash_map::Entry, HashSet},
    time::Instant,
};

use nom_sql::{CreateQueryCacheStatement, Dialect, DropQueryCacheStatement};
use tokio::sync::mpsc::UnboundedSender;
use tracing::{error, instrument, span, trace, warn, Level};

use nom_sql::{
    DeleteStatement, Expression, InsertStatement, Literal, ShowStatement, SqlQuery, UpdateStatement,
};
use noria::consistency::Timestamp;
use noria::{ColumnSchema, DataType};
use noria_client_metrics::{EventType, QueryExecutionEvent, SqlQueryType};
use noria_errors::{
    internal, internal_err, unsupported,
    ReadySetError::{self, PreparedStatementMissing},
    ReadySetResult,
};
use timestamp_service::client::{TimestampClient, WriteId, WriteKey};

use crate::query_status_cache::{MigrationState, QueryStatusCache};
use crate::rewrite;
use crate::upstream_database::NoriaCompare;
pub use crate::upstream_database::UpstreamPrepare;
use crate::{QueryHandler, UpstreamDatabase};

pub mod noria_connector;

pub use self::noria_connector::NoriaConnector;
use std::marker::PhantomData;

const ALLOWED_SQL_MODES: [SqlMode; 7] = [
    SqlMode::OnlyFullGroupBy,
    SqlMode::StrictTransTables,
    SqlMode::NoZeroInDate,
    SqlMode::NoZeroDate,
    SqlMode::ErrorForDivisionByZero,
    SqlMode::NoAutoCreateUser,
    SqlMode::NoEngineSubstitution,
];

// SqlMode holds the current list of known sql modes that we care to deal with.
// TODO(peter): expand this later to include ALL sql modes.
#[derive(PartialEq, Eq, Hash)]
enum SqlMode {
    OnlyFullGroupBy,
    StrictTransTables,
    NoZeroInDate,
    NoZeroDate,
    ErrorForDivisionByZero,
    NoAutoCreateUser,
    NoEngineSubstitution,
}

impl FromStr for SqlMode {
    type Err = ReadySetError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let res = match &s.trim().to_ascii_lowercase()[..] {
            "only_full_group_by" => SqlMode::OnlyFullGroupBy,
            "strict_trans_tables" => SqlMode::StrictTransTables,
            "no_zero_in_date" => SqlMode::NoZeroInDate,
            "no_zero_date" => SqlMode::NoZeroDate,
            "error_for_division_by_zero" => SqlMode::ErrorForDivisionByZero,
            "no_auto_create_user" => SqlMode::NoAutoCreateUser,
            "no_engine_substitution" => SqlMode::NoEngineSubstitution,
            _ => {
                return Err(ReadySetError::SqlModeParseFailed(s.to_string()));
            }
        };
        Ok(res)
    }
}

fn raw_sql_modes_to_list(sql_modes: &str) -> Result<Vec<SqlMode>, ReadySetError> {
    sql_modes
        .split(',')
        .into_iter()
        .map(SqlMode::from_str)
        .collect::<Result<Vec<SqlMode>, ReadySetError>>()
}

pub fn warn_on_slow_query(start: &time::Instant, query: &str) {
    let took = start.elapsed();
    if took.as_secs_f32() > time::Duration::from_millis(5).as_secs_f32() {
        warn!(
            %query,
            time = ?took,
            "slow query",
        );
    }
}

#[derive(Clone, Debug)]
pub struct PreparedStatement {
    noria: Option<u32>,
    upstream: Option<u32>,
}

/// Builder for a [`Backend`]
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
        }
    }
}

impl BackendBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn build<DB, Handler>(
        self,
        noria: NoriaConnector,
        upstream: Option<DB>,
        qsc: Arc<QueryStatusCache>,
    ) -> Backend<DB, Handler> {
        Backend {
            parsed_query_cache: HashMap::new(),
            prepared_queries: HashMap::new(),
            prepared_count: 0,
            noria,
            upstream,
            slowlog: self.slowlog,
            dialect: self.dialect,
            mirror_ddl: self.mirror_ddl,
            users: self.users,
            require_authentication: self.require_authentication,
            ticket: self.ticket,
            timestamp_client: self.timestamp_client,
            prepared_statements: Default::default(),
            query_log_sender: self.query_log_sender,
            query_log_ad_hoc_queries: self.query_log_ad_hoc_queries,
            query_status_cache: qsc,
            validate_queries: self.validate_queries,
            fail_invalidated_queries: self.fail_invalidated_queries,
            allow_unsupported_set: self.allow_unsupported_set,
            migration_mode: self.migration_mode,
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
}

pub struct Backend<DB, Handler> {
    // a cache of all previously parsed queries
    parsed_query_cache: HashMap<String, SqlQuery>,
    // all queries previously prepared on noria or upstream, mapped by their ID.
    prepared_queries: HashMap<u32, Arc<SqlQuery>>,
    prepared_count: u32,
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
    /// prepared_statements is used to map prepared statement ids from the user to prepared
    /// statements stored in noria and the underlying database. The id may map to a new value to
    /// avoid conflicts between noria and the underlying db.
    prepared_statements: HashMap<u32, PreparedStatement>,

    /// If set to `true`, all DDL changes will be mirrored to both the upstream db (if present) and
    /// noria. Otherwise, DDL changes will only go to the upstream if configured, or noria otherwise
    mirror_ddl: bool,

    query_log_sender: Option<UnboundedSender<QueryExecutionEvent>>,

    /// Whether to log ad-hoc queries by full query text in the query logger.
    query_log_ad_hoc_queries: bool,

    /// A cache of queries that we've seen, and their current state, used for processing
    query_status_cache: Arc<QueryStatusCache>,

    /// Run select statements with query validation.
    validate_queries: bool,
    fail_invalidated_queries: bool,

    /// Allow, but ignore, unsupported SQL `SET` statements.
    allow_unsupported_set: bool,

    /// How this backend handles migrations, See MigrationMode.
    migration_mode: MigrationMode,

    _query_handler: PhantomData<Handler>,
}

impl<DB, Handler> Backend<DB, Handler> {
    fn unsupported_err_if_no_upstream(&self) -> ReadySetResult<()> {
        if self.upstream.is_none() {
            // TODO: Make a more appropriate error type.
            return Err(ReadySetError::Unsupported(
                "Query may not be executed against Noria".to_string(),
            ));
        }

        Ok(())
    }
}

/// How to handle a migration in the adapter.
#[derive(Clone, PartialEq)]
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
    /// migrations "CREATE QUERY CACHE ..." may be used.
    OutOfBand,
}

#[derive(Debug, Clone)]
pub struct SelectSchema<'a> {
    pub use_bogo: bool,
    pub schema: Cow<'a, [ColumnSchema]>,
    pub columns: Cow<'a, [String]>,
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
pub enum SinglePrepareResult<DB: UpstreamDatabase> {
    Noria(noria_connector::PrepareResult),
    Upstream(UpstreamPrepare<DB>),
}

/// The type returned when a query is prepared by `Backend` through the `prepare` function.
#[derive(Debug)]
pub enum PrepareResult<DB: UpstreamDatabase> {
    Noria(noria_connector::PrepareResult),
    Upstream(UpstreamPrepare<DB>),
    Both(noria_connector::PrepareResult, UpstreamPrepare<DB>),
}

impl<DB: UpstreamDatabase> PrepareResult<DB> {
    pub fn noria_biased(self) -> SinglePrepareResult<DB> {
        match self {
            Self::Noria(res) | Self::Both(res, _) => SinglePrepareResult::Noria(res),
            Self::Upstream(res) => SinglePrepareResult::Upstream(res),
        }
    }

    pub fn upstream_biased(self) -> SinglePrepareResult<DB> {
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
/// 2. If we think we can support a query, try to send it to Noria. If that
/// hits an error that should be retried, retry. If not try fallback without dropping the
/// connection inbetween.
/// 3. If that fails and we got a MySQL error code, send that back to the client and keep the connection open. This is a real correctness bug.
/// 4. If we got another kind of error that is retryable from fallback, retry.
/// 5. If we got a non-retry related error that's not a MySQL error code already, convert it to the
///    most appropriate MySQL error code and write that back to the caller without dropping the
///    connection.
impl<DB, Handler> Backend<DB, Handler>
where
    DB: 'static + UpstreamDatabase,
    Handler: 'static + QueryHandler,
{
    pub fn prepared_count(&self) -> u32 {
        self.prepared_count
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
    pub fn is_allowed_set(&self, set: &nom_sql::SetStatement) -> bool {
        if self.allow_unsupported_set {
            return true;
        }

        match set {
            nom_sql::SetStatement::Variable(set) => set.variables.iter().all(|(variable, value)| {
                match variable.as_non_user_var() {
                    Some("time_zone") => {
                        matches!(value, Expression::Literal(Literal::String(ref s)) if s == "+00:00")
                    }
                    Some("autocommit") => {
                        matches!(value, Expression::Literal(Literal::Integer(i)) if *i == 1)
                    }
                    Some("sql_mode") => {
                        if let Expression::Literal(Literal::String(ref s)) = value {
                            match raw_sql_modes_to_list(&s[..]) {
                                Ok(sql_modes) => {
                                    let allowed = HashSet::from(ALLOWED_SQL_MODES);
                                    sql_modes.iter().all(|sql_mode| allowed.contains(sql_mode))
                                }
                                Err(e) => {
                                    warn!(
                                        %e,
                                        "unknown sql modes in set"
                                    );
                                    false
                                }
                            }
                        } else {
                            false
                        }
                    }
                    Some("names") => {
                        if let Expression::Literal(Literal::String(ref s)) = value {
                            matches!(&s[..], "latin1" | "utf8" | "utf8mb4")
                        } else {
                            false
                        }
                    }
                    Some("foreign_key_checks") => true,
                    _ => false,
                }
            }),
            nom_sql::SetStatement::Names(names) => {
                names.collation.is_none() && matches!(&names.charset[..], "latin1" | "utf8" | "utf8mb4")
            }
        }
    }

    /// Executes query on the upstream database, for when it cannot be parsed or executed by noria.
    /// Returns the query result, or an error if fallback is not configured
    pub async fn query_fallback(
        &mut self,
        query: &str,
    ) -> Result<QueryResult<'static, DB>, DB::Error> {
        let upstream = self.upstream.as_mut().ok_or_else(|| {
            ReadySetError::Internal("This case requires an upstream connector".to_string())
        })?;
        upstream.query(query).await.map(QueryResult::Upstream)
    }

    /// Should only be called with a nom_sql::SqlQuery that is of type StartTransaction, Commit, or
    /// Rollback. Used to handle transaction boundary queries.
    pub async fn handle_transaction_boundaries(
        &mut self,
        query: &nom_sql::SqlQuery,
    ) -> Result<QueryResult<'static, DB>, DB::Error> {
        let upstream = self.upstream.as_mut().ok_or_else(|| {
            ReadySetError::Internal("This case requires an upstream connector".to_string())
        })?;

        match query {
            nom_sql::SqlQuery::StartTransaction(_) => {
                upstream.start_tx().await.map(QueryResult::Upstream)
            }
            nom_sql::SqlQuery::Commit(_) => upstream.commit().await.map(QueryResult::Upstream),
            nom_sql::SqlQuery::Rollback(_) => upstream.rollback().await.map(QueryResult::Upstream),
            _ => {
                error!("handle_transaction_boundary was called with a SqlQuery that was not of type StartTransaction, Commit, or Rollback");
                internal!("handle_transaction_boundary was called with a SqlQuery that was not of type StartTransaction, Commit, or Rollback");
            }
        }
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

    /// Stores the prepared query id in a table. If the statement already
    /// exists, update that statement's ids in the table.
    fn store_prep_statement(&mut self, id: u32, prepare: &PrepareResult<DB>) {
        use noria_connector::PrepareResult::*;

        let statement = self
            .prepared_statements
            .entry(id)
            .or_insert(PreparedStatement {
                noria: None,
                upstream: None,
            });

        match prepare {
            PrepareResult::Noria(
                Select { statement_id, .. }
                | Insert { statement_id, .. }
                | Update { statement_id, .. }
                | Delete { statement_id, .. },
            ) => {
                statement.noria = Some(*statement_id);
            }
            PrepareResult::Upstream(UpstreamPrepare { statement_id, .. }) => {
                statement.upstream = Some(*statement_id);
            }
            PrepareResult::Both(
                Select { statement_id, .. }
                | Insert { statement_id, .. }
                | Update { statement_id, .. }
                | Delete { statement_id, .. },
                UpstreamPrepare {
                    statement_id: upstream_id,
                    ..
                },
            ) => {
                statement.noria = Some(*statement_id);
                statement.upstream = Some(*upstream_id);
            }
        }
    }

    /// Prepares query against Noria. If an upstream database exists, the prepare is mirrored to
    /// the upstream database.
    ///
    /// This function may perform a migration, and update a queries migration state,
    /// if `create_view_in_noria` is set.
    async fn mirror_prepare(
        &mut self,
        q: nom_sql::SelectStatement,
        rewritten: &nom_sql::SelectStatement,
        query: &str,
        event: &mut QueryExecutionEvent,
        create_view_in_noria: bool,
    ) -> Result<PrepareResult<DB>, DB::Error> {
        match self.upstream.as_mut() {
            // If we have an upstream try to prepare against both.
            Some(upstream) => {
                // TODO(): This timer is not entirely accurate as it is set to the
                // greater of the two times.
                let handle = event.start_timer();
                let (noria_res, upstream_res) = tokio::join!(
                    self.noria
                        .prepare_select(q.clone(), self.prepared_count, create_view_in_noria),
                    upstream.prepare(query),
                );
                handle.set_noria_duration();
                let upstream_res = upstream_res?;

                // Make updates to the query status cache if neccessary. Make changes to the
                // migration status if the query is successfully migrated, unsupported in Noria, or
                // the view is not found (the query is definitely not migrated).
                if create_view_in_noria {
                    match noria_res {
                        Ok(noria_connector::PrepareResult::Select {
                            ref schema,
                            ref params,
                            ..
                        }) => {
                            // If we are using `validate_queries`, a query that was successfully
                            // migrated may actually be unsupported if the schema or columns do not
                            // match up.
                            let valid = if self.validate_queries {
                                if let Err(e) = upstream_res.meta.compare(schema, params) {
                                    if self.fail_invalidated_queries {
                                        internal!("Query comparison failed to validate: {}", e);
                                    }
                                    warn!(error = %e, query = %q, "Query compare failed");
                                    false
                                } else {
                                    true
                                }
                            } else {
                                true
                            };

                            self.query_status_cache
                                .update_query_migration_state(
                                    rewritten,
                                    if valid {
                                        MigrationState::Successful
                                    } else {
                                        MigrationState::Unsupported
                                    },
                                )
                                .await;
                        }
                        Err(ref e) if e.caused_by_unsupported() => {
                            error!(error = %e,
                                   query = %q,
                                   "Select query is unsupported in ReadySet");
                            self.query_status_cache
                                .update_query_migration_state(
                                    rewritten,
                                    MigrationState::Unsupported,
                                )
                                .await;
                        }
                        _ => {}
                    }
                }

                // Return the correct PrepareResult based on the Noria response. If Noria failed
                // we only return the upstream's prepare result.
                match noria_res {
                    Ok(noria) => Ok(PrepareResult::Both(noria, upstream_res)),
                    Err(e) => {
                        if e.caused_by_view_not_found() {
                            self.query_status_cache
                                .update_query_migration_state(rewritten, MigrationState::Pending)
                                .await;
                        }

                        event.set_noria_error(&e);
                        error!(error = %e, "Error received from noria during mirror_prepare()");
                        Ok(PrepareResult::Upstream(upstream_res))
                    }
                }
            }
            // Otherwise only prepare against Noria.
            None => {
                let t = event.start_timer();
                let res = self
                    .noria
                    .prepare_select(q.clone(), self.prepared_count, true)
                    .await;
                t.set_noria_duration();

                // Make updates to the query status cache if neccessary. Make changes to the
                // migration status if the query is successfully migrated, unsupported in Noria, or
                // the view is not found (the query is definitely not migrated).
                match res {
                    Ok(r) => {
                        if create_view_in_noria {
                            self.query_status_cache
                                .update_query_migration_state(rewritten, MigrationState::Successful)
                                .await;
                        }
                        Ok(PrepareResult::Noria(r))
                    }
                    Err(e) if e.caused_by_unsupported() => {
                        error!(error = %e,
                        query = %q,
                        "Select query is unsupported in ReadySet");
                        self.query_status_cache
                            .update_query_migration_state(rewritten, MigrationState::Unsupported)
                            .await;
                        Err(e.into())
                    }
                    Err(e) if e.caused_by_view_not_found() => {
                        self.query_status_cache
                            .update_query_migration_state(rewritten, MigrationState::Pending)
                            .await;
                        Err(e.into())
                    }
                    // Errors that were not caused by unsupported may be transient, do nothing
                    // so we may retry the migration on this query.
                    Err(e) => {
                        warn!(error = %e,
                      query = %q,
                      "Select query may have transiently failed");
                        Err(e.into())
                    }
                }
            }
        }
    }

    /// Handles executing against only upstream.
    async fn execute_upstream(
        &mut self,
        upstream_statement_id: u32,
        params: &[DataType],
        event: &mut QueryExecutionEvent,
    ) -> Result<QueryResult<'static, DB>, DB::Error> {
        let upstream = self.upstream.as_mut().ok_or_else(|| {
            ReadySetError::Internal("This condition requires an upstream connector".to_string())
        })?;
        let handle = event.start_timer();
        upstream
            .execute(upstream_statement_id, params)
            .await
            .map(|r| {
                handle.set_upstream_duration();
                QueryResult::Upstream(r)
            })
    }

    /// Handles executing against only noria.
    #[allow(clippy::needless_lifetimes)] // Because Clippy says that 'noria can be elided, but that does not appear to be the case
    async fn execute_noria<'noria>(
        noria: &'noria mut NoriaConnector,
        noria_statement_id: u32,
        params: &[DataType],
        statement: &SqlQuery,
        ticket: Option<Timestamp>,
    ) -> ReadySetResult<QueryResult<'noria, DB>> {
        let res = match statement {
            SqlQuery::Select(_) => {
                let try_read = noria
                    .execute_prepared_select(noria_statement_id, params, ticket)
                    .await;
                match try_read {
                    Ok(read) => Ok(QueryResult::Noria(read)),
                    Err(e) => {
                        error!(error = %e);
                        Err(e)
                    }
                }
            }
            SqlQuery::Insert(ref _q) => Ok(QueryResult::Noria(
                noria
                    .execute_prepared_insert(noria_statement_id, params)
                    .await?,
            )),
            SqlQuery::Update(ref _q) => Ok(QueryResult::Noria(
                noria
                    .execute_prepared_update(noria_statement_id, params)
                    .await?,
            )),
            SqlQuery::Delete(..) => Ok(QueryResult::Noria(
                noria
                    .execute_prepared_delete(noria_statement_id, params)
                    .await?,
            )),
            _ => internal!(),
        };

        res
    }

    /// Executes the given read against noria, and on failure sends the read to fallback instead.
    /// If there is no fallback setup, then an error in Noria will be returned to the caller.
    /// If fallback is setup, cascade_read will only return an error if it occurred during fallback,
    /// in which case the caller is responsible for writing an appropriate MySQL error back to
    /// the client.
    pub async fn cascade_read(
        &mut self,
        q: nom_sql::SelectStatement,
        rewritten_stmt: &nom_sql::SelectStatement,
        query_str: &str,
        ticket: Option<Timestamp>,
        event: &mut QueryExecutionEvent,
        create_if_not_exists: bool,
    ) -> Result<QueryResult<'_, DB>, DB::Error> {
        let t = event.start_timer();
        let noria_result = self
            .noria
            .handle_select(q.clone(), ticket, create_if_not_exists)
            .await;
        t.set_noria_duration();

        match noria_result {
            Ok(r) => {
                if create_if_not_exists {
                    self.query_status_cache
                        .update_query_migration_state(rewritten_stmt, MigrationState::Successful)
                        .await;
                }
                Ok(QueryResult::Noria(r))
            }
            Err(e) => {
                if e.caused_by_unsupported() {
                    error!(error = %e,
                          query = %q,
                           "Select query is unsupported in ReadySet");
                    self.query_status_cache
                        .update_query_migration_state(rewritten_stmt, MigrationState::Unsupported)
                        .await;
                } else if e.caused_by_view_not_found() {
                    self.query_status_cache
                        .update_query_migration_state(rewritten_stmt, MigrationState::Pending)
                        .await;
                }
                event.set_noria_error(&e);
                // Check if we have fallback setup. If not, we need to return this error,
                // otherwise, we transition to fallback.
                match self.upstream {
                    Some(ref mut connector) => {
                        error!(error = %e, "Error received from noria, sending query to fallback");

                        let t = event.start_timer();
                        let res = connector.query(query_str).await.map(QueryResult::Upstream);
                        t.set_upstream_duration();
                        res
                    }
                    None => {
                        error!("{}", e);
                        Err(e.into())
                    }
                }
            }
        }
    }

    /// Prepares Insert, Delete, and Update statements
    async fn prepare_write(
        &mut self,
        query: &str,
        stmt: &nom_sql::SqlQuery,
        event: &mut QueryExecutionEvent,
    ) -> Result<PrepareResult<DB>, DB::Error> {
        event.sql_type = SqlQueryType::Write;
        if let Some(ref mut upstream) = self.upstream {
            let handle = event.start_timer();
            let res = upstream.prepare(query).await.map(PrepareResult::Upstream);
            handle.set_upstream_duration();
            res
        } else {
            let handle = event.start_timer();
            let res = match stmt {
                nom_sql::SqlQuery::Insert(ref stmt) => {
                    self.noria
                        .prepare_insert(stmt.clone(), self.prepared_count)
                        .await?
                }
                nom_sql::SqlQuery::Delete(ref stmt) => {
                    self.noria
                        .prepare_delete(stmt.clone(), self.prepared_count)
                        .await?
                }
                nom_sql::SqlQuery::Update(ref stmt) => {
                    self.noria
                        .prepare_update(stmt.clone(), self.prepared_count)
                        .await?
                }
                // prepare_write does not support other statements
                _ => internal!(),
            };
            handle.set_noria_duration();
            Ok(PrepareResult::Noria(res))
        }
    }

    /// Prepares `query` to be executed later using the reader/writer belonging
    /// to the calling `Backend` struct and adds the prepared query
    /// to the calling struct's map of prepared queries with a unique id.
    #[instrument(level = "debug", name = "prepare", skip(self, event))]
    async fn prepare_inner(
        &mut self,
        query: &str,
        event: &mut QueryExecutionEvent,
    ) -> Result<PrepareResult<DB>, DB::Error> {
        //the updated count will serve as the id for the prepared statement
        self.prepared_count += 1;

        let mut handle = event.start_timer();

        if self.is_in_tx() {
            warn!("In transaction, sending query to fallback");
            let res = self
                .prepare_fallback(query)
                .await
                .map(PrepareResult::Upstream);
            if let Ok(ref result) = res {
                self.store_prep_statement(self.prepared_count, result);
                handle.set_upstream_duration();
            }
            return res;
        }

        let parsed_query = match self.parse_query(query) {
            Ok(parsed_query) => parsed_query,
            Err(e) => {
                drop(handle);
                event.set_noria_error(&e);
                if self.upstream.is_some() {
                    error!(error = %e, "Error received from noria, sending query to fallback");
                    handle = event.start_timer();

                    let res = self
                        .prepare_fallback(query)
                        .await
                        .map(PrepareResult::Upstream);
                    if let Ok(ref result) = res {
                        self.store_prep_statement(self.prepared_count, result);
                        handle.set_upstream_duration();
                    }
                    return res;
                } else {
                    error!("{}", e);
                    return Err(e.into());
                }
            }
        };

        let res = match parsed_query {
            nom_sql::SqlQuery::Select(ref stmt) => {
                event.sql_type = SqlQueryType::Read;

                // If we cannot rewrite the query, we treat the query as unsupported as
                // it will fail to execute in Noria.
                let mut rewritten_stmt = stmt.clone();
                let migration_state = if rewrite::process_query(&mut rewritten_stmt).is_ok() {
                    self.query_status_cache
                        .query_migration_state(&rewritten_stmt)
                        .await
                } else {
                    warn!("Failed to rewrite SELECT query, marking query as unsupported");
                    MigrationState::Unsupported
                };

                match self.migration_mode {
                    MigrationMode::InRequestPath => {
                        match migration_state {
                            MigrationState::Unsupported => {
                                self.unsupported_err_if_no_upstream()?;

                                // Never send unsupported queries to Noria.
                                let handle = event.start_timer();
                                let res = self
                                    .prepare_fallback(query)
                                    .await
                                    .map(PrepareResult::Upstream);

                                handle.set_upstream_duration();

                                res
                            }
                            s => {
                                // For queries we might support, try to create the view if we
                                // haven't successfully migrated.
                                self.mirror_prepare(
                                    stmt.clone(),
                                    &rewritten_stmt,
                                    query,
                                    event,
                                    s != MigrationState::Successful,
                                )
                                .await
                            }
                        }
                    }
                    MigrationMode::OutOfBand => {
                        match migration_state {
                            MigrationState::Successful => {
                                self.mirror_prepare(
                                    stmt.clone(),
                                    &rewritten_stmt,
                                    query,
                                    event,
                                    false,
                                )
                                .await
                            }
                            _ => {
                                self.unsupported_err_if_no_upstream()?;

                                // Never send unsupported queries to Noria.
                                let handle = event.start_timer();
                                let res = self
                                    .prepare_fallback(query)
                                    .await
                                    .map(PrepareResult::Upstream);

                                handle.set_upstream_duration();

                                res
                            }
                        }
                    }
                }
            }
            nom_sql::SqlQuery::Insert(_)
            | nom_sql::SqlQuery::Update(_)
            | nom_sql::SqlQuery::Delete(_) => self.prepare_write(query, &parsed_query, event).await,
            nom_sql::SqlQuery::CreateTable(..)
            | nom_sql::SqlQuery::CreateView(..)
            | nom_sql::SqlQuery::Set(..)
            | nom_sql::SqlQuery::StartTransaction(..)
            | nom_sql::SqlQuery::Commit(..)
            | nom_sql::SqlQuery::Rollback(..)
            | nom_sql::SqlQuery::Use(..)
            | nom_sql::SqlQuery::Show(..)
            | nom_sql::SqlQuery::CompoundSelect(..) => {
                if let Some(ref mut upstream) = self.upstream {
                    upstream.prepare(query).await.map(|r| {
                        handle.set_upstream_duration();
                        PrepareResult::Upstream(r)
                    })
                } else {
                    // For now we only support prepare deletes over fallback.
                    error!("unsupported query");
                    unsupported!("query type unsupported");
                }
            }
            nom_sql::SqlQuery::DropTable(..)
            | nom_sql::SqlQuery::AlterTable(..)
            | nom_sql::SqlQuery::RenameTable(..)
            | nom_sql::SqlQuery::CreateQueryCache(..)
            | nom_sql::SqlQuery::DropQueryCache(..)
            | nom_sql::SqlQuery::Explain(_) => {
                error!("unsupported query");
                unsupported!("query type unsupported");
            }
        };

        if let Ok(ref result) = res {
            self.prepared_queries
                .insert(self.prepared_count, Arc::new(parsed_query));
            self.store_prep_statement(self.prepared_count, result);
        }
        res
    }

    /// Prepares `query` to be executed later using the reader/writer belonging
    /// to the calling `Backend` struct and adds the prepared query
    /// to the calling struct's map of prepared queries with a unique id.
    pub async fn prepare(&mut self, query: &str) -> Result<PrepareResult<DB>, DB::Error> {
        let mut query_event = QueryExecutionEvent::new(EventType::Prepare);
        let res = self.prepare_inner(query, &mut query_event).await;
        res
    }

    async fn cascade_execute(
        &mut self,
        id: u32,
        params: &[DataType],
        prep: &SqlQuery,
        event: &mut QueryExecutionEvent,
    ) -> Result<QueryResult<'_, DB>, DB::Error> {
        let prepared_statement = self
            .prepared_statements
            .get(&id)
            .ok_or(PreparedStatementMissing { statement_id: id })?;

        let handle = event.start_timer();
        if let Some(id) = prepared_statement.noria {
            let res =
                Self::execute_noria(&mut self.noria, id, params, prep, self.ticket.clone()).await;

            match res {
                Ok(res) => {
                    handle.set_noria_duration();
                    return Ok(res);
                }
                Err(e) => {
                    if self.upstream.is_none() {
                        return Err(e.into());
                    }

                    error!(error = %e, "Error received from noria during execute, sending query to fallback");
                }
            }
        }

        // Returns above if upstream is none.
        #[allow(clippy::unwrap_used)]
        let upstream = self.upstream.as_mut().unwrap();

        let statement_id = if let Some(statement_id) = prepared_statement.upstream {
            statement_id
        } else {
            // Prepare the statement if we have not seen this
            // prepared statement yet.
            let UpstreamPrepare { statement_id, .. } = upstream.prepare(&prep.to_string()).await?;
            // The prepared statement must exist if we are executing on it.
            self.prepared_statements.get_mut(&id).unwrap().upstream = Some(statement_id);
            statement_id
        };

        let handle = event.start_timer();
        let res = upstream
            .execute(statement_id, params)
            .await
            .map(QueryResult::Upstream);
        handle.set_upstream_duration();
        res
    }

    async fn execute_inner(
        &mut self,
        id: u32,
        params: &[DataType],
        event: &mut QueryExecutionEvent,
    ) -> Result<QueryResult<'_, DB>, DB::Error> {
        let span = span!(Level::TRACE, "execute", id);
        let _g = span.enter();

        let prepared_statement = self
            .prepared_statements
            .get(&id)
            .ok_or(PreparedStatementMissing { statement_id: id })?;

        if self.is_in_tx() {
            // transactions are only supported on upstream
            match prepared_statement.upstream {
                Some(upstream_id) => {
                    return self.execute_upstream(upstream_id, params, event).await;
                }
                None => {
                    error!("Missing upstream statement ID for query in transactoin");
                    return Err(PreparedStatementMissing { statement_id: id }.into());
                }
            }
        }

        match self.prepared_queries.get(&id).cloned() {
            // If we have a `prepared_statement` but not a `SqlQuery` associated with it,
            // this is an unparseable prepared statement, issue this directly to
            // upstream.
            None => match prepared_statement.upstream {
                Some(upstream_id) => self.execute_upstream(upstream_id, params, event).await,
                None => {
                    error!("Missing upstream statement ID for unparseable query (adapter statement ID {})", id);
                    Err(PreparedStatementMissing { statement_id: id }.into())
                }
            },
            Some(prep) => {
                match prep.as_ref() {
                    SqlQuery::Select(ref stmt) => {
                        // If we cannot rewrite the query, we treat the query as unsupported as
                        // it will fail to execute in Noria.
                        let mut rewritten_stmt = stmt.clone();
                        let migration_state = if rewrite::process_query(&mut rewritten_stmt).is_ok()
                        {
                            self.query_status_cache
                                .query_migration_state(&rewritten_stmt)
                                .await
                        } else {
                            warn!("Failed to rewrite SELECT query, marking query as unsupported");
                            MigrationState::Unsupported
                        };

                        let stmt = stmt.clone();

                        match migration_state {
                            MigrationState::Unsupported => {
                                if let (Some(statement_id), Some(upstream)) =
                                    (prepared_statement.upstream, &mut self.upstream)
                                {
                                    let handle = event.start_timer();
                                    let res = upstream
                                        .execute(statement_id, params)
                                        .await
                                        .map(QueryResult::Upstream);
                                    handle.set_upstream_duration();
                                    res
                                } else {
                                    Err(PreparedStatementMissing { statement_id: id })
                                        .map_err(|e| e.into())
                                }
                            }

                            s => {
                                // If the query is allowed and we have not yet prepared it noria - then
                                // during our prepare we either encountered a transient noria
                                // failure or the migration was pending. Prepare it now as we
                                // should be able to succeed.
                                let should_prepare = match self.migration_mode {
                                    MigrationMode::InRequestPath => {
                                        prepared_statement.noria.is_none()
                                    }
                                    MigrationMode::OutOfBand => {
                                        prepared_statement.noria.is_none()
                                            && s == MigrationState::Successful
                                    }
                                };
                                if should_prepare {
                                    // TODO(justin): Refactor prepared statement cache to wrap preparing and storing
                                    // prepared statements in a thread-local cache.
                                    let res = self
                                        .noria
                                        .prepare_select(
                                            stmt.clone(),
                                            id,
                                            s != MigrationState::Successful,
                                        )
                                        .await
                                        .map(PrepareResult::Noria);

                                    if let Ok(ref result) = res {
                                        self.store_prep_statement(id, result);
                                        // This circumvents executing against upstream and validate
                                        // queries.
                                        if s != MigrationState::Successful {
                                            self.query_status_cache
                                                .update_query_migration_state(
                                                    &rewritten_stmt,
                                                    MigrationState::Successful,
                                                )
                                                .await
                                        }
                                    }
                                }

                                self.cascade_execute(id, params, &prep, event).await
                            }
                        }
                    }
                    SqlQuery::Insert(_) | SqlQuery::Update(_) | SqlQuery::Delete(_) => {
                        event.sql_type = SqlQueryType::Write;
                        if self.upstream.is_some() {
                            match prepared_statement.upstream {
                                Some(upstream_id) => {
                                    self.execute_upstream(upstream_id, params, event).await
                                }
                                None => {
                                    error!("Missing upstream statement ID for write query (adapter statement ID {})", id);
                                    Err(PreparedStatementMissing { statement_id: id }.into())
                                }
                            }
                        } else {
                            Self::execute_noria(
                                &mut self.noria,
                                id,
                                params,
                                &prep,
                                self.ticket.clone(),
                            )
                            .await
                            .map_err(|e| e.into())
                        }
                    }
                    _ => internal!(),
                }
            }
        }
    }

    /// Executes the already-prepared query with id `id` and parameters `params` using the reader/writer
    /// belonging to the calling `Backend` struct.
    // TODO(andrew, justin): add RYW support for executing prepared queries
    pub async fn execute(
        &mut self,
        id: u32,
        params: &[DataType],
    ) -> Result<QueryResult<'_, DB>, DB::Error> {
        // Requires clone as no references are allowed after self.execute_inner
        // due to borrow checker rules.
        let query_logger = self.query_log_sender.clone();
        let mut query_event = QueryExecutionEvent::new(EventType::Execute);
        query_event.query = self.prepared_queries.get(&id).cloned();
        let res = self.execute_inner(id, params, &mut query_event).await?;

        log_query(query_logger, query_event);
        Ok(res)
    }

    #[instrument(level = "trace", name = "query", skip(self, event))]
    async fn query_inner(
        &mut self,
        query: &str,
        event: &mut QueryExecutionEvent,
    ) -> Result<QueryResult<'_, DB>, DB::Error> {
        let start = Instant::now();
        let slowlog = self.slowlog;
        let handle = event.start_timer();

        if self.is_in_tx() {
            let res = self.query_fallback(query).await;
            if slowlog {
                warn_on_slow_query(&start, query);
            }
            handle.set_upstream_duration();
            return res;
        }

        let parse_result = self.parse_query(query);
        handle.set_parse_duration();

        // fallback to upstream database on query parse failure
        let parsed_query = match parse_result {
            Ok(parsed_tuple) => Arc::new(parsed_tuple),
            Err(e) => {
                // Do not fall back if the set is not allowed
                if matches!(e, ReadySetError::SetDisallowed { statement: _ }) {
                    return Err(e.into());
                }
                // TODO(Dan): Implement RYW for query_fallback
                if self.upstream.is_some() {
                    error!(error = %e, "Error received from noria, sending query to fallback");
                    let handle = event.start_timer();
                    let res = self.query_fallback(query).await;
                    if slowlog {
                        warn_on_slow_query(&start, query);
                    }
                    handle.set_upstream_duration();
                    return res;
                } else {
                    error!("{}", e);
                    return Err(e.into());
                }
            }
        };

        if Handler::requires_fallback(&parsed_query) {
            // Noria can't handle this query according to the handler.
            return if self.upstream.is_some() {
                // Fallback is enabled, so route this query to the underlying
                // database.
                //
                let handle = event.start_timer();
                let res = self.query_fallback(query).await;
                if slowlog {
                    warn_on_slow_query(&start, query);
                }
                handle.set_upstream_duration();
                res
            } else {
                // Fallback is not enabled, so let the handler return a default result or
                // throw an error.
                Ok(QueryResult::Noria(Handler::default_response(
                    &parsed_query,
                )?))
            };
        }

        // If we have an upstream then we will pass valid set statements across to that upstream.
        // If no upstream is present we will ignore the statement
        // Disallowed set statements always produce an error
        if let nom_sql::SqlQuery::Set(s) = parsed_query.as_ref() {
            if !self.is_allowed_set(s) {
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
                    }
                    let upstream_res = upstream.query(query).await;
                    Ok(QueryResult::Upstream(upstream_res?))
                } else {
                    Ok(QueryResult::Noria(self.noria.$noria_method($stmt).await?))
                }
            };
        }

        let res = if let nom_sql::SqlQuery::Select(ref stmt) = parsed_query.as_ref() {
            // Handle the read path separately as it handles whether upstream exists
            // or not inline.
            event.sql_type = SqlQueryType::Read;
            if self.query_log_ad_hoc_queries {
                event.query = Some(parsed_query.clone());
            }

            // If we cannot rewrite the query, we treat the query as unsupported as
            // it will fail to execute in Noria.
            let mut rewritten_stmt = stmt.clone();
            let migration_state = if rewrite::process_query(&mut rewritten_stmt).is_ok() {
                self.query_status_cache
                    .query_migration_state(&rewritten_stmt)
                    .await
            } else {
                warn!("Failed to rewrite SELECT query, marking query as unsupported");
                MigrationState::Unsupported
            };

            let unsupported_err_if_no_upstream = self.unsupported_err_if_no_upstream();
            match self.migration_mode {
                MigrationMode::InRequestPath => {
                    match migration_state {
                        MigrationState::Unsupported => {
                            // Never send unsupported queries to Noria.
                            unsupported_err_if_no_upstream?;

                            let handle = event.start_timer();
                            // Guaranteed by unsupported_err_if_no_upstream.
                            #[allow(clippy::unwrap_used)]
                            let res = self
                                .upstream
                                .as_mut()
                                .unwrap()
                                .query(&query)
                                .await
                                .map(QueryResult::Upstream);
                            handle.set_upstream_duration();

                            res
                        }
                        s => {
                            // If we are validating the schema, explicitely compare the
                            // prepare results for this select before we proceed.
                            if self.validate_queries {
                                self.prepared_count += 1;
                                self.mirror_prepare(
                                    stmt.clone(),
                                    &rewritten_stmt,
                                    query,
                                    event,
                                    true,
                                )
                                .await?;
                            }
                            // retry extend_recipe if pending for non-explicit migrations
                            self.cascade_read(
                                stmt.clone(),
                                &rewritten_stmt,
                                query,
                                self.ticket.clone(),
                                event,
                                s != MigrationState::Successful,
                            )
                            .await
                        }
                    }
                }
                MigrationMode::OutOfBand => {
                    match migration_state {
                        MigrationState::Successful => {
                            self.cascade_read(
                                stmt.clone(),
                                &rewritten_stmt,
                                query,
                                self.ticket.clone(),
                                event,
                                false,
                            )
                            .await
                        }
                        _ => {
                            unsupported_err_if_no_upstream?;
                            let handle = event.start_timer();
                            // Guaranteed by unsupported_err_if_no_upstream.
                            #[allow(clippy::unwrap_used)]
                            let res = self
                                .upstream
                                .as_mut()
                                .unwrap()
                                .query(&query)
                                .await
                                .map(QueryResult::Upstream);
                            handle.set_upstream_duration();

                            res
                        }
                    }
                }
            }
        } else {
            // Upstream reads are tried when noria reads produce an error. Upstream writes are done by
            // default when the upstream connector is present.
            let handle = event.start_timer();
            if let Some(ref mut upstream) = self.upstream {
                match parsed_query.as_ref() {
                    nom_sql::SqlQuery::Select(_) => {
                        unreachable!("read path returns prior")
                    }
                    nom_sql::SqlQuery::Insert(InsertStatement { table: t, .. })
                    | nom_sql::SqlQuery::Update(UpdateStatement { table: t, .. })
                    | nom_sql::SqlQuery::Delete(DeleteStatement { table: t, .. }) => {
                        event.sql_type = SqlQueryType::Write;
                        let handle = event.start_timer();
                        // Update ticket if RYW enabled
                        let query_result = if cfg!(feature = "ryw") {
                            if let Some(timestamp_service) = &mut self.timestamp_client {
                                let (query_result, identifier) =
                                    upstream.handle_ryw_write(query).await?;

                                // TODO(andrew): Move table name to table index conversion to timestamp service
                                // https://app.clubhouse.io/readysettech/story/331
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
                        handle.set_upstream_duration();
                        Ok(QueryResult::Upstream(query_result?))
                    }

                    SqlQuery::Explain(nom_sql::ExplainStatement::Graphviz { simplified }) => self
                        .noria
                        .graphviz(*simplified)
                        .await
                        .map(QueryResult::Noria)
                        .map_err(|e| e.into()),

                    SqlQuery::CreateQueryCache(CreateQueryCacheStatement {
                        ref name,
                        ref statement,
                    }) => {
                        let mut statement = statement.clone();
                        rewrite::process_query(&mut statement)?;
                        self.noria
                            .handle_create_query_cache(
                                name.as_ref().map(|s| s.as_str()),
                                &statement,
                            )
                            .await?;
                        self.query_status_cache
                            .update_query_migration_state(&statement, MigrationState::Successful)
                            .await;

                        Ok(QueryResult::Noria(noria_connector::QueryResult::Empty))
                    }

                    SqlQuery::DropQueryCache(DropQueryCacheStatement { ref name }) => {
                        self.noria.drop_view(name).await?;
                        Ok(QueryResult::Noria(noria_connector::QueryResult::Empty))
                    }

                    // Table Create / Drop (RYW not supported)
                    // TODO(andrew, justin): how are these types of writes handled w.r.t RYW?
                    // CREATE VIEW will still trigger migrations with explicit-migrations enabled
                    nom_sql::SqlQuery::CreateView(stmt) => handle_ddl!(handle_create_view(stmt)),
                    nom_sql::SqlQuery::CreateTable(stmt) => handle_ddl!(handle_create_table(stmt)),
                    nom_sql::SqlQuery::DropTable(_)
                    | nom_sql::SqlQuery::AlterTable(_)
                    | nom_sql::SqlQuery::RenameTable(_) => {
                        unsupported!("{} not yet supported", parsed_query.query_type());
                    }
                    nom_sql::SqlQuery::Show(ShowStatement::Queries) => {
                        Ok(QueryResult::Noria(self.noria.verbose_outputs().await?))
                    }
                    nom_sql::SqlQuery::Set(_)
                    | nom_sql::SqlQuery::CompoundSelect(_)
                    | nom_sql::SqlQuery::Show(_) => {
                        let res = upstream.query(query).await.map(QueryResult::Upstream);
                        handle.set_upstream_duration();
                        res
                    }
                    nom_sql::SqlQuery::StartTransaction(_)
                    | nom_sql::SqlQuery::Commit(_)
                    | nom_sql::SqlQuery::Rollback(_) => {
                        let res = self.handle_transaction_boundaries(&parsed_query).await;
                        handle.set_upstream_duration();
                        res
                    }
                    nom_sql::SqlQuery::Use(stmt) => {
                        match self.upstream.as_ref().map(|u| u.database()).flatten() {
                            Some(db) if db == stmt.database => {
                                Ok(QueryResult::Noria(noria_connector::QueryResult::Empty))
                            }
                            _ => {
                                error!("USE statement attempted to change the database");
                                unsupported!("USE");
                            }
                        }
                    }
                }
            } else {
                // Interacting directly with Noria writer (No RYW support)
                //
                // TODO(andrew, justin): Do we want RYW support with the NoriaConnector? Currently, no.
                // TODO: Implement event execution metrics for Noria without upstream.
                let res = match parsed_query.as_ref() {
                    // CREATE VIEW will still trigger migrations with epxlicit-migrations enabled
                    SqlQuery::CreateView(q) => self.noria.handle_create_view(q).await,
                    SqlQuery::CreateTable(q) => self.noria.handle_create_table(q).await,

                    SqlQuery::Select(q) => {
                        // TODO: Handle --async-migrations --explicit-migrations
                        let res = self
                            .noria
                            .handle_select(q.clone(), self.ticket.clone(), true)
                            .await;
                        res
                    }
                    SqlQuery::Insert(q) => self.noria.handle_insert(q).await,
                    SqlQuery::Update(q) => self.noria.handle_update(q).await,
                    SqlQuery::Delete(q) => self.noria.handle_delete(q).await,
                    SqlQuery::CreateQueryCache(CreateQueryCacheStatement { name, statement }) => {
                        let mut statement = statement.clone();
                        rewrite::process_query(&mut statement)?;
                        self.noria
                            .handle_create_query_cache(
                                name.as_ref().map(|s| s.as_str()),
                                &statement,
                            )
                            .await?;
                        self.query_status_cache
                            .update_query_migration_state(&statement, MigrationState::Successful)
                            .await;
                        Ok(noria_connector::QueryResult::Empty)
                    }
                    SqlQuery::DropQueryCache(DropQueryCacheStatement { ref name }) => {
                        self.noria.drop_view(name).await?;
                        Ok(noria_connector::QueryResult::Empty)
                    }
                    SqlQuery::Explain(nom_sql::ExplainStatement::Graphviz { simplified }) => {
                        self.noria.graphviz(*simplified).await
                    }
                    SqlQuery::Show(ShowStatement::Queries) => self.noria.verbose_outputs().await,
                    _ => {
                        error!("unsupported query");
                        unsupported!("query type unsupported");
                    }
                }?;

                Ok(QueryResult::Noria(res))
            }
        };

        if slowlog {
            warn_on_slow_query(&start, query);
        }

        res
    }

    /// Executes `query` using the reader/writer belonging to the calling `Backend` struct.
    pub async fn query(&mut self, query: &str) -> Result<QueryResult<'_, DB>, DB::Error> {
        let mut query_event = QueryExecutionEvent::new(EventType::Query);
        let query_logger = self.query_log_sender.clone();
        let res = self.query_inner(query, &mut query_event).await;

        log_query(query_logger, query_event);
        res
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
                        error!(%query, "query can't be parsed: \"{}\"", query);
                        Err(ReadySetError::UnparseableQuery {
                            query: query.to_string(),
                        })
                    }
                }
            }
        }
    }
}

/// Offloads recording query metrics to a separate thread. Sends a
/// message over a mpsc channel.
fn log_query(sender: Option<UnboundedSender<QueryExecutionEvent>>, event: QueryExecutionEvent) {
    if let Some(sender) = sender {
        // Drop the error if something goes wrong with query logging.
        if let Err(e) = sender.send(event) {
            warn!("Error logging query with query logging enabled: {}", e);
        }
    }
}
