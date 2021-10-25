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

use metrics::histogram;
use nom_sql::Dialect;
use tokio::sync::mpsc::UnboundedSender;
use tracing::{error, instrument, span, trace, warn, Level};

use nom_sql::{DeleteStatement, InsertStatement, Literal, SqlQuery, UpdateStatement};
use noria::consistency::Timestamp;
use noria::errors::internal_err;
use noria::errors::ReadySetError::PreparedStatementMissing;
use noria::{internal, unsupported, ColumnSchema, DataType, ReadySetError, ReadySetResult};
use noria_client_metrics::recorded::SqlQueryType;
use noria_client_metrics::{EventType, QueryExecutionEvent};
use timestamp_service::client::{TimestampClient, WriteId, WriteKey};

use crate::coverage::QueryCoverageInfoRef;
use crate::query_status_cache::{QueryState, QueryStatusCache};
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

/// Check whether the set statement is explicitly allowed. All other set
/// statements should return an error
pub fn is_allowed_set(set: &nom_sql::SetStatement) -> bool {
    match set {
        nom_sql::SetStatement::Variable(var) => match &var.variable.to_ascii_lowercase()[..] {
            "time_zone" | "@@global.time_zone" | "@@local.time_zone" | "@@session.time_zone" => {
                matches!(&var.value, Literal::String(s) if s == "+00:00")
            }
            "autocommit" => {
                matches!(&var.value, Literal::Integer(i) if *i == 1)
            }
            "@@session.sql_mode" | "@@global.sql_mode" | "sql_mode" => {
                if let Literal::String(s) = &var.value {
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
            "names" => {
                if let Literal::String(s) = &var.value {
                    matches!(&s[..], "latin1" | "utf8" | "utf8mb4")
                } else {
                    false
                }
            }
            "foreign_key_checks" => true,
            _ => false,
        },
        nom_sql::SetStatement::Names(names) => {
            names.collation.is_none() && matches!(&names.charset[..], "latin1" | "utf8" | "utf8mb4")
        }
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
    mirror_reads: bool,
    mirror_ddl: bool,
    users: HashMap<String, String>,
    require_authentication: bool,
    ticket: Option<Timestamp>,
    timestamp_client: Option<TimestampClient>,
    query_coverage_info: Option<QueryCoverageInfoRef>,
    query_log_sender: Option<UnboundedSender<QueryExecutionEvent>>,
    // TODO(ENG-685): Remove option when live_qca flag is removed in main.
    query_status_cache: Option<Arc<QueryStatusCache>>,
    live_qca: bool,
}

impl Default for BackendBuilder {
    fn default() -> Self {
        BackendBuilder {
            slowlog: false,
            dialect: Dialect::MySQL,
            mirror_reads: false,
            mirror_ddl: false,
            users: Default::default(),
            require_authentication: true,
            ticket: None,
            timestamp_client: None,
            query_coverage_info: None,
            query_log_sender: None,
            query_status_cache: None,
            live_qca: false,
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
    ) -> Backend<DB, Handler> {
        let parsed_query_cache = HashMap::new();
        let prepared_queries = HashMap::new();
        let prepared_count = 0;
        Backend {
            parsed_query_cache,
            prepared_queries,
            prepared_count,
            noria,
            upstream,
            slowlog: self.slowlog,
            dialect: self.dialect,
            mirror_reads: self.mirror_reads,
            mirror_ddl: self.mirror_ddl,
            users: self.users,
            require_authentication: self.require_authentication,
            ticket: self.ticket,
            timestamp_client: self.timestamp_client,
            prepared_statements: Default::default(),
            query_coverage_info: self.query_coverage_info,
            query_log_sender: self.query_log_sender,
            query_status_cache: self.query_status_cache,
            live_qca: self.live_qca,
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

    pub fn mirror_reads(mut self, mirror_reads: bool) -> Self {
        self.mirror_reads = mirror_reads;
        self
    }

    pub fn mirror_ddl(mut self, mirror_ddl: bool) -> Self {
        self.mirror_ddl = mirror_ddl;
        self
    }

    pub fn query_log(
        mut self,
        query_log_sender: Option<UnboundedSender<QueryExecutionEvent>>,
    ) -> Self {
        self.query_log_sender = query_log_sender;
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

    pub fn query_coverage_info(
        mut self,
        query_coverage_info: Option<QueryCoverageInfoRef>,
    ) -> Self {
        self.query_coverage_info = query_coverage_info;
        self
    }

    pub fn query_status_cache(mut self, query_status_cache: Option<Arc<QueryStatusCache>>) -> Self {
        self.query_status_cache = query_status_cache;
        self
    }

    pub fn live_qca(mut self, live_qca: bool) -> Self {
        self.live_qca = live_qca;
        self
    }
}

pub struct Backend<DB, Handler> {
    // a cache of all previously parsed queries
    parsed_query_cache: HashMap<String, SqlQuery>,
    // all queries previously prepared on noria or upstream, mapped by their ID.
    prepared_queries: HashMap<u32, SqlQuery>,
    prepared_count: u32,
    /// Noria connector used for reads, and writes when no upstream DB is present
    noria: NoriaConnector,
    /// Optional connector to the upstream DB. Used for fallback reads and all writes if it exists
    upstream: Option<DB>,
    slowlog: bool,
    /// SQL dialect to use when parsing queries from clients
    dialect: Dialect,
    /// If set to true and a backend is configured for fallback, all reads will be performed
    /// in both Noria and against upstream, with the upstream result always being returned.
    mirror_reads: bool,
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

    /// Shared reference to information about the queries that have been executed during the runtime
    /// of this adapter.
    ///
    /// If None, query coverage analysis is disabled
    #[allow(dead_code)] // TODO: Remove once this is used
    query_coverage_info: Option<QueryCoverageInfoRef>,

    query_log_sender: Option<UnboundedSender<QueryExecutionEvent>>,

    /// A cache of queries that we've seen, and their current state, used for processing
    query_status_cache: Option<Arc<QueryStatusCache>>,

    /// Run with query coverage analysis enabled in the serving path.
    live_qca: bool,

    _query_handler: PhantomData<Handler>,
}

impl<DB, Handler> Backend<DB, Handler> {
    fn live_qca_enabled(&self) -> bool {
        self.live_qca
    }
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
        query: nom_sql::SqlQuery,
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

    /// Executes the given read against both noria and the upstream database, returning
    /// the result from the upstream database regardless of outcome.
    ///
    /// If fallback is not configured, returns an error.
    async fn mirror_read(
        &mut self,
        q: nom_sql::SelectStatement,
        query_str: String,
        ticket: Option<Timestamp>,
        event: &mut QueryExecutionEvent,
    ) -> Result<QueryResult<'_, DB>, DB::Error> {
        let connector = self
            .upstream
            .as_mut()
            .ok_or_else(|| internal_err("mirror_read called without fallback configured"))?;

        let mut handle = event.start_timer();
        // TODO: Record upstream query duration on success.
        let upstream_res = connector.query(query_str).await.map(|r| {
            handle.set_upstream_duration();
            QueryResult::Upstream(r)
        });
        handle = event.start_timer();
        match self.noria.handle_select(q, ticket).await {
            Ok(_) => {
                handle.set_noria_duration();
            }
            Err(e) => {
                event.set_noria_error(&e);
                error!(error = %e, "Error received from noria during mirror_read()");
            }
        };

        upstream_res
    }

    /// Used to link the statement ids of two successful prepare results
    fn link_prepare_results(
        &mut self,
        upstream_result: &UpstreamPrepare<DB>,
        noria_result: &noria_connector::PrepareResult,
    ) {
        let query_coverage_info = match self.query_coverage_info {
            Some(qci) => qci,
            None => return,
        };

        let UpstreamPrepare {
            statement_id: upstream_statement_id,
            ..
        } = upstream_result;

        match noria_result {
            noria_connector::PrepareResult::Select { statement_id, .. }
            | noria_connector::PrepareResult::Insert { statement_id, .. }
            | noria_connector::PrepareResult::Update { statement_id, .. }
            | noria_connector::PrepareResult::Delete { statement_id, .. } => {
                query_coverage_info.link_statement_ids(*upstream_statement_id, *statement_id)
            }
        }
    }

    /// Executes the given prepare select against both noria, and the upstream database, returning
    /// the result from the upstream database regardless of outcome.
    ///
    /// If fallback is not configured, returns an error.
    async fn mirror_prepare(
        &mut self,
        q: nom_sql::SelectStatement,
        query: &str,
        event: &mut QueryExecutionEvent,
    ) -> Result<PrepareResult<DB>, DB::Error> {
        let connector = self.upstream.as_mut().ok_or_else(|| {
            ReadySetError::Internal("This case requires an upstream connector".to_string())
        })?;

        // TODO(): This timer is not entirely accurate as it is set to the
        // greater of the two times.
        let handle = event.start_timer();
        let (noria_res, upstream_res) = tokio::join!(
            self.noria.prepare_select(q, self.prepared_count),
            connector.prepare(query),
        );
        handle.set_noria_duration();
        let upstream_res = upstream_res?;

        // If noria and the upstream prepares both succeed we return a PrepareResult =
        // with both. If the upstream failed, we error propagate.
        match noria_res {
            Ok(noria) => {
                if self.mirror_reads {
                    self.link_prepare_results(&upstream_res, &noria);
                }

                Ok(PrepareResult::Both(noria, upstream_res))
            }
            Err(e) => {
                event.set_noria_error(&e);
                error!(error = %e, "Error received from noria during mirror_prepare()");
                Ok(PrepareResult::Upstream(upstream_res))
            }
        }
    }

    /// Handles executing against only upstream.
    async fn execute_upstream(
        &mut self,
        upstream_statement_id: u32,
        params: Vec<DataType>,
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
    async fn execute_noria(
        noria: &mut NoriaConnector,
        noria_statement_id: u32,
        params: Vec<DataType>,
        statement: SqlQuery,
        ticket: Option<Timestamp>,
    ) -> ReadySetResult<QueryResult<'_, DB>> {
        let res = match statement {
            SqlQuery::Select(_) => {
                let try_read = noria
                    .execute_prepared_select(noria_statement_id, params.clone(), ticket)
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

    /// Issues an execute against both upstream and noria.
    ///
    /// If fallback is not configured, returns an error.
    ///
    /// # Panics
    ///
    /// Panics if query coverage info is not enabled.
    #[allow(clippy::unwrap_used)] // Should not be called unless QCA is enabled.
    async fn mirror_execute(
        &mut self,
        upstream_statement_id: u32,
        params: Vec<DataType>,
        event: &mut QueryExecutionEvent,
    ) -> Result<QueryResult<'static, DB>, DB::Error> {
        let upstream_res = self
            .execute_upstream(upstream_statement_id, params.clone(), event)
            .await;

        if let Some(noria_statement_id) = self
            .query_coverage_info
            .unwrap()
            .noria_statement_id(upstream_statement_id)
        {
            let statement: SqlQuery =
                if let Some(q_t) = self.prepared_queries.get(&noria_statement_id).cloned() {
                    q_t
                } else {
                    let e = PreparedStatementMissing {
                        statement_id: noria_statement_id,
                    };
                    event.set_noria_error(&e);
                    error!(error = %e, "Error received from noria during mirror_execute()");
                    return upstream_res;
                };

            let handle = event.start_timer();
            match Self::execute_noria(
                &mut self.noria,
                noria_statement_id,
                params,
                statement,
                self.ticket.clone(),
            )
            .await
            {
                Ok(_) => {
                    handle.set_noria_duration();
                }
                Err(e) => {
                    event.set_noria_error(&e);
                    error!(error = %e, "Error received from noria during mirror_execute()");
                }
            }
        }

        upstream_res
    }

    /// Executes the given read against noria, and on failure sends the read to fallback instead.
    /// If there is no fallback setup, then an error in Noria will be returned to the caller.
    /// If fallback is setup, cascade_read will only return an error if it occurred during fallback,
    /// in which case the caller is responsible for writing an appropriate MySQL error back to
    /// the client.
    pub async fn cascade_read(
        &mut self,
        q: nom_sql::SelectStatement,
        query_str: &str,
        ticket: Option<Timestamp>,
        event: &mut QueryExecutionEvent,
    ) -> Result<QueryResult<'_, DB>, DB::Error> {
        let t = event.start_timer();
        let noria_result = self.noria.handle_select(q, ticket).await;
        t.set_noria_duration();

        match noria_result {
            Ok(r) => Ok(QueryResult::Noria(r)),
            Err(e) => {
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
                if self.upstream.is_some() {
                    if self.live_qca_enabled() {
                        // The query status cache is guaranteed to exist if `live_qca` is enabled.
                        #[allow(clippy::unwrap_used)]
                        let cache = self.query_status_cache.as_ref().unwrap().clone();

                        let result = match cache.query_state(stmt).await {
                            // If this is the first time this adapter has seen the query, we should
                            // set the queries status in the cache and attempt to validate column
                            // headers to fast-track a query to the allow list.
                            None => {
                                cache.register_query(stmt).await;
                                let handle = event.start_timer();
                                let result = self
                                    .prepare_fallback(query)
                                    .await
                                    .map(PrepareResult::Upstream);
                                handle.set_upstream_duration();
                                result
                            }
                            Some(QueryState::NeedsProcessing) => {
                                // Any query whose state is still being processed should be sent
                                // directly to fallback.
                                let handle = event.start_timer();
                                let result = self
                                    .prepare_fallback(query)
                                    .await
                                    .map(PrepareResult::Upstream);
                                handle.set_upstream_duration();
                                result
                            }
                            Some(QueryState::Allow) => {
                                // Prepare allowed queries on both noria and fallback so that
                                // if there are errors during execute we can still fallback
                                // correctly.
                                self.mirror_prepare(stmt.clone(), query, event).await
                            }
                        };

                        result
                    } else {
                        self.mirror_prepare(stmt.clone(), query, event).await
                    }
                } else {
                    self.noria
                        .prepare_select(stmt.clone(), self.prepared_count)
                        .await
                        .map(PrepareResult::Noria)
                        .map_err(|e| e.into())
                }
            }
            nom_sql::SqlQuery::Insert(_) => {
                if let Some(ref mut upstream) = self.upstream {
                    let res = upstream.prepare(query).await.map(PrepareResult::Upstream);
                    handle.set_upstream_duration();
                    res
                } else {
                    Ok(PrepareResult::Noria(
                        self.noria
                            .prepare_insert(parsed_query.clone(), self.prepared_count)
                            .await?,
                    ))
                }
            }
            nom_sql::SqlQuery::Update(_) => {
                if let Some(ref mut upstream) = self.upstream {
                    upstream.prepare(query).await.map(PrepareResult::Upstream)
                } else {
                    Ok(PrepareResult::Noria(
                        self.noria
                            .prepare_update(parsed_query.clone(), self.prepared_count)
                            .await?,
                    ))
                }
            }
            nom_sql::SqlQuery::Delete(ref stmt) => {
                if let Some(ref mut upstream) = self.upstream {
                    upstream.prepare(query).await.map(PrepareResult::Upstream)
                } else {
                    Ok(PrepareResult::Noria(
                        self.noria.prepare_delete(stmt, self.prepared_count).await?,
                    ))
                }
            }
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
            | nom_sql::SqlQuery::RenameTable(..) => {
                match self.upstream {
                    Some(ref mut upstream) if self.mirror_reads => {
                        // Mirror reads is enabled. That means that we can simply prepare against
                        // upstream. All read results will always be returned from upstream in this
                        // mode anyways.
                        upstream.prepare(query).await.map(|r| {
                            handle.set_upstream_duration();
                            PrepareResult::Upstream(r)
                        })
                    }
                    _ => {
                        error!("unsupported query");
                        unsupported!("query type unsupported");
                    }
                }
            }
        };

        if let Ok(ref result) = res {
            self.prepared_queries
                .insert(self.prepared_count, parsed_query.to_owned());
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
        if let (Some(info), e) = (self.query_coverage_info, query_event) {
            info.query_prepared(query.to_string(), e, self.prepared_count);
        }
        res
    }

    async fn cascade_execute(
        &mut self,
        id: u32,
        prepared_statement: &PreparedStatement,
        params: Vec<DataType>,
        prep: SqlQuery,
        event: &mut QueryExecutionEvent,
    ) -> Result<QueryResult<'_, DB>, DB::Error> {
        let handle = event.start_timer();
        if let Some(id) = prepared_statement.noria {
            let res = Self::execute_noria(
                &mut self.noria,
                id,
                params.clone(),
                prep.clone(),
                self.ticket.clone(),
            )
            .await;

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
        params: Vec<DataType>,
        prepared_statement: &PreparedStatement,
        event: &mut QueryExecutionEvent,
    ) -> Result<QueryResult<'_, DB>, DB::Error> {
        let span = span!(Level::TRACE, "execute", id);
        let _g = span.enter();

        if self.mirror_reads {
            match prepared_statement.upstream {
                Some(upstream_id) => {
                    return self.mirror_execute(upstream_id, params, event).await;
                }
                None => {
                    // No idea how we got here. We'll fall through so we can still handle this.
                    error!("Mirror reads were enabled, and a client gave us a statement id for a noria prepared statement. This shouldn't be possible");
                }
            }
        }

        match self.prepared_queries.get(&id).cloned() {
            // If we have a `prepared_statement` but not a `SqlQuery` associated with it,
            // this is an unparseable prepared statement, issue this directly to
            // upstream.
            None => self.execute_upstream(id, params, event).await,
            Some(prep) => {
                match prep {
                    SqlQuery::Select(ref stmt) => {
                        if self.live_qca_enabled() {
                            // The query status cache is guaranteed to exist if `live_qca` is enabled.
                            #[allow(clippy::unwrap_used)]
                            let cache = self.query_status_cache.as_ref().unwrap().clone();
                            let result = cache.query_state(stmt).await;
                            match result {
                                // If prepare was called for this query prior, it should be in the
                                // query status cache.
                                None => Err(PreparedStatementMissing { statement_id: id })
                                    .map_err(|e| e.into()),
                                Some(QueryState::Allow) => {
                                    // If the query is allowed and we have not yet prepared it noria - then
                                    // during our prepare we either encountered a transient noria
                                    // failure or the migration was pending. Prepare it now as we
                                    // should be able to succeed.
                                    if prepared_statement.noria.is_none() {
                                        // TODO(justin): Refactor prepared statement cache to wrap preparing and storing
                                        // prepared statements in a thread-local cache.
                                        let res = self
                                            .noria
                                            .prepare_select(stmt.clone(), id)
                                            .await
                                            .map(PrepareResult::Noria);

                                        // If we cannot prepare, then set that this query needs
                                        // reprocessing.
                                        if let Ok(ref result) = res {
                                            self.store_prep_statement(id, result);
                                        } else {
                                            cache.set_needs_processing(stmt).await;
                                        }
                                    }

                                    let prepared_statement = self
                                        .prepared_statements
                                        .get(&id)
                                        .cloned()
                                        .ok_or(PreparedStatementMissing { statement_id: id })?;

                                    self.cascade_execute(
                                        id,
                                        &prepared_statement,
                                        params,
                                        prep,
                                        event,
                                    )
                                    .await
                                }
                                Some(QueryState::NeedsProcessing) => {
                                    // Prepare the statement if we have not seen this
                                    // prepared statement yet.
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
                            }
                        } else {
                            // If there is no upstream or live qca is disabled we just cascade
                            // execute.
                            self.cascade_execute(id, prepared_statement, params, prep, event)
                                .await
                        }
                    }
                    SqlQuery::Insert(_) | SqlQuery::Update(_) | SqlQuery::Delete(_) => {
                        if self.upstream.is_some() {
                            self.execute_upstream(id, params, event).await
                        } else {
                            Self::execute_noria(
                                &mut self.noria,
                                id,
                                params,
                                prep,
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
        params: Vec<DataType>,
    ) -> Result<QueryResult<'_, DB>, DB::Error> {
        let query_coverage_info = self.query_coverage_info;
        // Requires clone as no references are allowed after self.execute_inner
        // due to borrow checker rules.
        let query_logger = self.query_log_sender.clone();
        let prepared_statement = self
            .prepared_statements
            .get(&id)
            .cloned()
            .ok_or(PreparedStatementMissing { statement_id: id })?;

        let mut query_event = QueryExecutionEvent::new(EventType::Execute);
        let prepared_query = self.prepared_queries.get(&id).cloned();
        query_event.query = prepared_query;

        let res = self
            .execute_inner(id, params, &prepared_statement, &mut query_event)
            .await;

        if let Some(info) = query_coverage_info {
            info.prepare_executed(id, query_event.clone());
        }

        log_query(query_logger, query_event);
        res
    }

    #[instrument(level = "trace", name = "query", skip(self, event))]
    async fn query_inner(
        &mut self,
        query: &str,
        event: &mut QueryExecutionEvent,
    ) -> Result<QueryResult<'_, DB>, DB::Error> {
        let start = time::Instant::now();
        let slowlog = self.slowlog;
        let mut handle = event.start_timer();

        if self.is_in_tx() {
            let res = self.query_fallback(query).await;
            if slowlog {
                warn_on_slow_query(&start, query);
            }
            handle.set_upstream_duration();
            return res;
        }

        let parse_result = self.parse_query(query);
        let parse_time = start.elapsed().as_micros();

        // fallback to upstream database on query parse failure
        let parsed_query = match parse_result {
            Ok(parsed_tuple) => parsed_tuple,
            Err(e) => {
                // Do not fall back if the set is not allowed
                if matches!(e, ReadySetError::SetDisallowed { statement: _ }) {
                    return Err(e.into());
                }
                // TODO(Dan): Implement RYW for query_fallback
                if self.upstream.is_some() {
                    error!(error = %e, "Error received from noria, sending query to fallback");
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
        if let nom_sql::SqlQuery::Set(s) = &parsed_query {
            if !is_allowed_set(s) {
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
                            drop(handle);
                            event.set_noria_error(&e);
                        } else {
                            handle.set_noria_duration();
                        }
                        handle = event.start_timer();
                    }
                    let upstream_res = upstream.query(query).await;
                    // TODO(peter): Implement logic to detect if results differed.
                    handle.set_upstream_duration();

                    Ok(QueryResult::Upstream(upstream_res?))
                } else {
                    Ok(QueryResult::Noria(self.noria.$noria_method($stmt).await?))
                }
            };
        }

        // Upstream reads are tried when noria reads produce an error. Upstream writes are done by
        // default when the upstream connector is present.
        let live_qca_enabled = self.live_qca_enabled();
        let res = if let Some(ref mut upstream) = self.upstream {
            match parsed_query {
                nom_sql::SqlQuery::Select(ref stmt) => {
                    let execution_timer = Instant::now();
                    let res = if self.mirror_reads {
                        self.mirror_read(stmt.clone(), query.to_owned(), self.ticket.clone(), event)
                            .await
                    } else if live_qca_enabled {
                        // The query status cache is guarenteed to exist if `live_qca` is enabled.
                        #[allow(clippy::unwrap_used)]
                        let cache = self.query_status_cache.as_ref().unwrap().clone();

                        match cache.query_state(stmt).await {
                            // If this is the first time this adapter has seen the query, we should
                            // set the queries status in the cache and send it to fallback.
                            None => {
                                cache.register_query(stmt).await;
                                upstream.query(&query).await.map(QueryResult::Upstream)
                            }
                            Some(QueryState::NeedsProcessing) => {
                                upstream.query(&query).await.map(QueryResult::Upstream)
                            }
                            Some(QueryState::Allow) => {
                                let res = self
                                    .cascade_read(stmt.clone(), query, self.ticket.clone(), event)
                                    .await;
                                if res.is_err() {
                                    cache.set_needs_processing(stmt).await;
                                }
                                res
                            }
                        }
                    } else {
                        self.cascade_read(stmt.clone(), query, self.ticket.clone(), event)
                            .await
                    };
                    //TODO(Dan): Implement fallback execution timing
                    let execution_time = execution_timer.elapsed().as_micros();
                    measure_parse_and_execution_time(
                        parse_time,
                        execution_time,
                        SqlQueryType::Read,
                    );
                    res
                }
                nom_sql::SqlQuery::Insert(InsertStatement { table: t, .. })
                | nom_sql::SqlQuery::Update(UpdateStatement { table: t, .. })
                | nom_sql::SqlQuery::Delete(DeleteStatement { table: t, .. }) => {
                    let execution_timer = Instant::now();

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

                    measure_parse_and_execution_time(
                        parse_time,
                        execution_timer.elapsed().as_micros(),
                        SqlQueryType::Write,
                    );

                    Ok(QueryResult::Upstream(query_result?))
                }

                // Table Create / Drop (RYW not supported)
                // TODO(andrew, justin): how are these types of writes handled w.r.t RYW?
                nom_sql::SqlQuery::CreateView(stmt) => handle_ddl!(handle_create_view(stmt)),
                nom_sql::SqlQuery::CreateTable(stmt) => handle_ddl!(handle_create_table(stmt)),
                nom_sql::SqlQuery::DropTable(_)
                | nom_sql::SqlQuery::AlterTable(_)
                | nom_sql::SqlQuery::RenameTable(_)
                | nom_sql::SqlQuery::Show(_) => {
                    if self.mirror_reads {
                        let res = upstream.query(query).await;
                        handle.set_upstream_duration();
                        Ok(QueryResult::Upstream(res?))
                    } else {
                        unsupported!("{} not yet supported", parsed_query.query_type());
                    }
                }
                nom_sql::SqlQuery::Set(_) => {
                    let res = upstream
                        .query(&parsed_query.to_string())
                        .await
                        .map(QueryResult::Upstream);
                    handle.set_upstream_duration();
                    res
                }
                nom_sql::SqlQuery::StartTransaction(_)
                | nom_sql::SqlQuery::Commit(_)
                | nom_sql::SqlQuery::Rollback(_) => {
                    let res = self.handle_transaction_boundaries(parsed_query).await;
                    handle.set_upstream_duration();
                    res
                }
                nom_sql::SqlQuery::CompoundSelect(_) => {
                    let res = self.query_fallback(query).await;
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
            let mut execution_timer = None;

            let res = match parsed_query {
                SqlQuery::CreateView(q) => self.noria.handle_create_view(q).await,
                SqlQuery::CreateTable(q) => self.noria.handle_create_table(q).await,

                SqlQuery::Select(q) => {
                    execution_timer = Some((Instant::now(), SqlQueryType::Read));
                    self.noria.handle_select(q, self.ticket.clone()).await
                }
                SqlQuery::Insert(q) => {
                    execution_timer = Some((Instant::now(), SqlQueryType::Write));
                    self.noria.handle_insert(q).await
                }
                SqlQuery::Update(q) => {
                    execution_timer = Some((Instant::now(), SqlQueryType::Write));
                    self.noria.handle_update(q).await
                }
                SqlQuery::Delete(q) => {
                    execution_timer = Some((Instant::now(), SqlQueryType::Write));
                    self.noria.handle_delete(q).await
                }
                _ => {
                    error!("unsupported query");
                    unsupported!("query type unsupported");
                }
            }?;

            if let Some((timer, kind)) = execution_timer {
                measure_parse_and_execution_time(parse_time, timer.elapsed().as_micros(), kind);
            }

            Ok(QueryResult::Noria(res))
        };

        if slowlog {
            warn_on_slow_query(&start, query);
        }

        res
    }

    /// Executes `query` using the reader/writer belonging to the calling `Backend` struct.
    pub async fn query(&mut self, query: &str) -> Result<QueryResult<'_, DB>, DB::Error> {
        let mut query_event = QueryExecutionEvent::new(EventType::Execute);
        let query_coverage_info = self.query_coverage_info;
        let query_logger = self.query_log_sender.clone();
        let res = self.query_inner(query, &mut query_event).await;
        if let Some(info) = query_coverage_info {
            info.query_executed(query.to_string(), query_event.clone());
        }

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

fn measure_parse_and_execution_time(
    parse_time: u128,
    execution_time: u128,
    sql_query_type: SqlQueryType,
) {
    histogram!(
        noria_client_metrics::recorded::QUERY_PARSING_TIME,
        parse_time as f64,
        "query_type" => sql_query_type
    );
    histogram!(
        noria_client_metrics::recorded::QUERY_EXECUTION_TIME,
        execution_time as f64,
        "query_type" => sql_query_type
    );
}
