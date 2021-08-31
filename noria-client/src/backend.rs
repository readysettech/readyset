use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt;
use std::fmt::Debug;
use std::time;

use futures::FutureExt;
use metrics::histogram;
use nom_sql::Dialect;
use tokio::sync::mpsc;
use tracing::Level;

use nom_sql::{DeleteStatement, InsertStatement, Literal, SqlQuery, UpdateStatement};
use noria::consensus::Authority;
use noria::consistency::Timestamp;
use noria::errors::internal_err;
use noria::errors::ReadySetError::PreparedStatementMissing;
use noria::{internal, unsupported, ColumnSchema, DataType, ReadySetError};
use noria_client_metrics::recorded::SqlQueryType;
use timestamp_service::client::{TimestampClient, WriteId, WriteKey};

use crate::backend::error::Error;
pub use crate::upstream_database::UpstreamPrepare;
use crate::{rewrite, UpstreamDatabase};

pub mod error;
pub mod noria_connector;

pub use self::noria_connector::NoriaConnector;

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
    // Only allow set time_zone = '+00:00' at the moment
    if (&set.variable[..] == "time_zone")
        || (&set.variable == "@@GLOBAL.time_zone")
        || (&set.variable == "@@LOCAL.time_zone")
        || (&set.variable == "@@SESSION.time_zone")
    {
        matches!(&set.value, Literal::String(s) if s == "+00:00")
    } else {
        false
    }
}

//TODO(DAN): Remove in favor of determining whether a query was read or write based on the presence
//of columns in the resultset
/// Check whether the given query is a read or write. This method is not comprehensive
fn is_read(query: &str) -> bool {
    use nom::branch::alt;
    use nom::bytes::complete::tag_no_case;
    use nom::character::complete::multispace0;
    use nom::sequence::tuple;

    let q = query.as_bytes();

    tuple::<_, _, (_, nom::error::ErrorKind), _>((
        multispace0,
        alt((
            tag_no_case("select"),
            tag_no_case("show"),
            tag_no_case("describe"),
        )),
    ))(q)
    .is_ok()
}

/// A database connection for performing writes
///
/// Noria-client can be either configured to pass-through writes to an [upstream
/// database](Self::Upstream) or write [directly to noria](Self::Noria)
#[allow(clippy::large_enum_variant)]
pub enum Writer<A: 'static + Authority, DB> {
    Upstream(DB),
    Noria(NoriaConnector<A>),
}

/// A database connection for performing reads
///
/// A reader consists of a connection to noria, plus optionally a connection to an upstream database
/// to use for query fallback
pub struct Reader<A: 'static + Authority, DB> {
    pub upstream: Option<DB>,
    pub noria_connector: NoriaConnector<A>,
}

#[derive(Clone, Debug)]
pub enum PreparedStatement {
    NoriaPrepStatement(u32),
    UpstreamPrepWrite(u32),
    UpstreamPrepRead(u32),
}

/// Builder for a [`Backend`]
#[derive(Clone)]
pub struct BackendBuilder {
    slowlog: bool,
    dialect: Dialect,
    race_reads: bool,
    users: HashMap<String, String>,
    require_authentication: bool,
    ticket: Option<Timestamp>,
    timestamp_client: Option<TimestampClient>,
}

impl Default for BackendBuilder {
    fn default() -> Self {
        BackendBuilder {
            slowlog: false,
            dialect: Dialect::MySQL,
            race_reads: false,
            users: Default::default(),
            require_authentication: true,
            ticket: None,
            timestamp_client: None,
        }
    }
}

impl BackendBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn build<A: 'static + Authority, DB>(
        self,
        writer: Writer<A, DB>,
        reader: Reader<A, DB>,
    ) -> Backend<A, DB> {
        let parsed_query_cache = HashMap::new();
        let prepared_queries = HashMap::new();
        let prepared_count = 0;
        Backend {
            parsed_query_cache,
            prepared_queries,
            prepared_count,
            writer,
            reader,
            slowlog: self.slowlog,
            dialect: self.dialect,
            race_reads: self.race_reads,
            users: self.users,
            require_authentication: self.require_authentication,
            ticket: self.ticket,
            timestamp_client: self.timestamp_client,
            prepared_statements: Default::default(),
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

    pub fn race_reads(mut self, race_reads: bool) -> Self {
        self.race_reads = race_reads;
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
}

pub struct Backend<A: 'static + Authority, DB> {
    // a cache of all previously parsed queries
    parsed_query_cache: HashMap<String, (SqlQuery, Vec<nom_sql::Literal>)>,
    // all queries previously prepared, mapped by their ID
    prepared_queries: HashMap<u32, SqlQuery>,
    prepared_count: u32,
    writer: Writer<A, DB>,
    reader: Reader<A, DB>,
    slowlog: bool,
    /// SQL dialect to use when parsing queries from clients
    dialect: Dialect,
    /// If set to true and a MySQL backend is configured for fallback, all reads will be performed
    /// simultaneously in Noria and MySQL, with the first successful result being returned.
    race_reads: bool,
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
    /// statements stored in noria or the underlying database. The id may map to a new value to
    /// avoid conflicts between noria and the underlying db.
    prepared_statements: HashMap<u32, PreparedStatement>,
}

#[derive(Debug)]
pub struct SelectSchema {
    pub use_bogo: bool,
    pub schema: Vec<ColumnSchema>,
    pub columns: Vec<String>,
}

/// The type returned when a query is prepared by `Backend` through the `prepare` function.
#[derive(Debug)]
pub enum PrepareResult<Col> {
    Noria(noria_connector::PrepareResult),
    Upstream(UpstreamPrepare<Col>),
}

/// The type returned when a query is carried out by `Backend`, through either the `query` or
/// `execute` functions.
pub enum QueryResult<DB: UpstreamDatabase> {
    /// Results from noria
    Noria(noria_connector::QueryResult),
    /// Results from upstream
    Upstream(DB::QueryResult),
}

impl<DB> Debug for QueryResult<DB>
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
impl<A, DB> Backend<A, DB>
where
    A: 'static + Authority,
    DB: 'static + UpstreamDatabase,
{
    pub fn prepared_count(&self) -> u32 {
        self.prepared_count
    }

    /// Executes query on the upstream database, for when it cannot be parsed or executed by noria.
    /// Returns the query result, or an error if fallback is not configured
    pub async fn query_fallback(&mut self, query: &str) -> Result<QueryResult<DB>, Error> {
        let is_read = is_read(query);
        let upstream = self
            .reader
            .upstream
            .as_mut()
            .ok_or(Error::ReadySet(ReadySetError::FallbackNoConnector))?;

        if is_read {
            upstream.handle_read(query).await.map(QueryResult::Upstream)
        } else {
            upstream
                .handle_write(query)
                .await
                .map(QueryResult::Upstream)
        }
    }

    /// Prepares query on the mysql_backend, if present, when it cannot be parsed or prepared by
    /// noria.
    pub async fn prepare_fallback(
        &mut self,
        query: &str,
    ) -> Result<UpstreamPrepare<DB::Column>, Error> {
        let q = query.to_string().trim_start().to_lowercase();
        if is_read(&q) {
            match self.reader.upstream {
                Some(ref mut connector) => connector.prepare(query).await,
                None => Err(ReadySetError::FallbackNoConnector.into()),
            }
        } else {
            match self.writer {
                Writer::Upstream(ref mut connector) => connector.prepare(query).await,
                _ => Err(ReadySetError::FallbackNoConnector.into()),
            }
        }
    }

    /// Stores the prepared query id in a table
    fn store_prep_statement(&mut self, prepare: &PrepareResult<DB::Column>) {
        use noria_connector::PrepareResult::*;

        match prepare {
            PrepareResult::Noria(Select { statement_id, .. })
            | PrepareResult::Noria(Insert { statement_id, .. }) => {
                self.prepared_statements.insert(
                    self.prepared_count,
                    PreparedStatement::NoriaPrepStatement(*statement_id),
                );
            }
            PrepareResult::Noria(Update { statement_id, .. }) => {
                self.prepared_statements.insert(
                    self.prepared_count,
                    PreparedStatement::NoriaPrepStatement(*statement_id as u32),
                );
            }
            PrepareResult::Upstream(UpstreamPrepare {
                statement_id,
                is_read,
                ..
            }) => {
                self.prepared_statements.insert(self.prepared_count, {
                    if *is_read {
                        PreparedStatement::UpstreamPrepRead(*statement_id)
                    } else {
                        PreparedStatement::UpstreamPrepWrite(*statement_id)
                    }
                });
            }
        }
    }

    /// Executes the given read against both noria and the upstream database in simultaneous racing
    /// tasks, returning the result of the first query that completes successfully, or the error
    /// from the upstream database if both fail.
    ///
    /// If fallback is not configured, returns an error
    pub async fn race_read(
        &mut self,
        q: nom_sql::SelectStatement,
        query_str: String,
        use_params: Vec<Literal>,
        ticket: Option<Timestamp>,
    ) -> Result<QueryResult<DB>, Error> {
        let url = self
            .reader
            .upstream
            .as_ref()
            .ok_or_else(|| internal_err("race_read called without fallback configured"))?
            .url()
            .to_owned();
        let mut upstream = DB::connect(url).await?;
        let mut noria = self.reader.noria_connector.clone();

        macro_rules! grab_err {
            ($sender: expr) => {
                |result| async move {
                    match result {
                        Ok(res) => Ok(res),
                        Err(e) => {
                            // TODO(grfn): Also log the error, especially if it came from noria
                            $sender.send(e).await.unwrap();
                            Err(())
                        }
                    }
                }
            };
        }

        let (noria_err_sender, mut noria_err) = mpsc::channel(1);
        let noria_read = tokio::spawn(async move {
            noria
                .handle_select(q, use_params, ticket)
                .then(grab_err!(noria_err_sender))
                .await
        });
        let (upstream_err_sender, mut upstream_err) = mpsc::channel(1);
        let upstream_read = tokio::spawn(async move {
            upstream
                .handle_read(&query_str)
                .then(grab_err!(upstream_err_sender))
                .await
        });
        let errs = tokio::spawn(async move { tokio::join!(noria_err.recv(), upstream_err.recv()) });

        tokio::select! {
            Ok(Ok(noria_res)) = noria_read => Ok(QueryResult::Noria(noria_res)),
            Ok(Ok(upstream_res)) = upstream_read => Ok(QueryResult::Upstream(upstream_res)),
            Ok((_, Some(e))) = errs => Err(e)
        }
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
        use_params: Vec<Literal>,
        ticket: Option<Timestamp>,
    ) -> Result<QueryResult<DB>, Error> {
        match self
            .reader
            .noria_connector
            .handle_select(q, use_params, ticket)
            .await
        {
            Ok(r) => Ok(QueryResult::Noria(r)),
            Err(e) => {
                // Check if we have fallback setup. If not, we need to return this error,
                // otherwise, we transition to fallback.
                match self.reader.upstream {
                    Some(ref mut connector) => connector
                        .handle_read(query_str)
                        .await
                        .map(QueryResult::Upstream),
                    None => Err(e),
                }
            }
        }
    }

    /// Executes the given prepare select against noria, and on failure sends the prepare to
    /// fallback. cascape_prepare will return a result or a mysql_async error (which could be a
    /// mysql server error) if fallback is configured.
    pub async fn cascade_prepare(
        &mut self,
        q: nom_sql::SelectStatement,
        query: &str,
    ) -> Result<PrepareResult<DB::Column>, Error> {
        match self
            .reader
            .noria_connector
            .prepare_select(nom_sql::SqlQuery::Select(q), self.prepared_count)
            .await
        {
            Ok(res) => Ok(PrepareResult::Noria(res)),
            Err(e) => match self.reader.upstream {
                Some(_) => self
                    .prepare_fallback(query)
                    .await
                    .map(PrepareResult::Upstream),
                None => Err(e),
            },
        }
    }

    /// Prepares `query` to be executed later using the reader/writer belonging
    /// to the calling `Backend` struct and adds the prepared query
    /// to the calling struct's map of prepared queries with a unique id.
    pub async fn prepare(&mut self, query: &str) -> Result<PrepareResult<DB::Column>, Error> {
        //the updated count will serve as the id for the prepared statement
        self.prepared_count += 1;

        let span = span!(Level::DEBUG, "prepare", query);
        let _g = span.enter();

        let res = self.parse_query(query, false);
        let parsed_query = match res {
            Ok((parsed_query, _)) => parsed_query,
            Err(e) => {
                if self.reader.upstream.is_some() {
                    // Fallback prepare will always go to the reader connector
                    let res = self
                        .prepare_fallback(query)
                        .await
                        .map(PrepareResult::Upstream);
                    if let Ok(ref result) = res {
                        self.store_prep_statement(result);
                    }
                    return res;
                } else {
                    return Err(e);
                }
            }
        };

        let res = match parsed_query {
            nom_sql::SqlQuery::Select(ref stmt) => self.cascade_prepare(stmt.clone(), query).await,
            nom_sql::SqlQuery::Insert(_) => match &mut self.writer {
                Writer::Noria(connector) => connector
                    .prepare_insert(parsed_query.clone(), self.prepared_count)
                    .await
                    .map(PrepareResult::Noria),
                Writer::Upstream(connector) => {
                    connector.prepare(query).await.map(PrepareResult::Upstream)
                }
            },
            nom_sql::SqlQuery::Update(_) => match &mut self.writer {
                Writer::Noria(connector) => connector
                    .prepare_update(parsed_query.clone(), self.prepared_count)
                    .await
                    .map(PrepareResult::Noria),
                Writer::Upstream(connector) => {
                    connector.prepare(query).await.map(PrepareResult::Upstream)
                }
            },
            _ => {
                error!("unsupported query");
                unsupported!("query type unsupported");
            }
        };

        if matches!(res, Ok(PrepareResult::Noria(_))) {
            self.prepared_queries
                .insert(self.prepared_count, parsed_query.to_owned());
        }

        if let Ok(ref result) = res {
            self.store_prep_statement(result);
        }
        res
    }

    /// Executes the already-prepared query with id `id` and parameters `params` using the reader/writer
    /// belonging to the calling `Backend` struct.
    // TODO(andrew, justin): add RYW support for executing prepared queries
    pub async fn execute(
        &mut self,
        id: u32,
        params: Vec<DataType>,
    ) -> Result<QueryResult<DB>, Error> {
        let span = span!(Level::TRACE, "execute", id);
        let _g = span.enter();

        let start = time::Instant::now();

        let prepared_statement = self
            .prepared_statements
            .get(&id)
            .cloned()
            .ok_or(PreparedStatementMissing { statement_id: id })?;

        match prepared_statement {
            PreparedStatement::UpstreamPrepRead(id) => {
                let connector = self
                    .reader
                    .upstream
                    .as_mut()
                    .ok_or(Error::ReadySet(ReadySetError::FallbackNoConnector))?;
                return connector
                    .execute_read(id, params)
                    .await
                    .map(QueryResult::Upstream);
            }
            PreparedStatement::UpstreamPrepWrite(id) => {
                let connector = match &mut self.writer {
                    Writer::Upstream(conn) => conn,
                    _ => return Err(ReadySetError::FallbackNoConnector.into()),
                };
                return connector
                    .execute_write(id, params)
                    .await
                    .map(QueryResult::Upstream);
            }
            PreparedStatement::NoriaPrepStatement(statement_id) => {
                let prep: SqlQuery = self
                    .prepared_queries
                    .get(&statement_id)
                    .cloned()
                    .ok_or(PreparedStatementMissing { statement_id })?;
                let res = match prep {
                    SqlQuery::Select(_) => {
                        let try_read = self
                            .reader
                            .noria_connector
                            .execute_prepared_select(
                                statement_id,
                                params.clone(),
                                self.ticket.clone(),
                            )
                            .await;
                        // fallback on failure, however we must extract the original query because
                        // it was not prepared on the underlying db
                        match try_read {
                            Ok(read) => Ok(QueryResult::Noria(read)),
                            Err(e) => match &mut self.reader.upstream {
                                Some(conn) => {
                                    // TODO(DAN): The prepared statement id should be returned to
                                    // the backend so that it can be stored
                                    let UpstreamPrepare { statement_id, .. } =
                                        conn.prepare(&prep.to_string()).await?;
                                    conn.execute_read(statement_id, params)
                                        .await
                                        .map(QueryResult::Upstream)
                                }
                                None => Err(e),
                            },
                        }
                    }
                    SqlQuery::Insert(ref _q) => match &mut self.writer {
                        Writer::Noria(connector) => connector
                            .execute_prepared_insert(statement_id, params)
                            .await
                            .map(QueryResult::Noria),
                        Writer::Upstream(connector) => connector
                            .execute_write(statement_id, params)
                            .await
                            .map(QueryResult::Upstream),
                    },
                    SqlQuery::Update(ref _q) => match &mut self.writer {
                        Writer::Noria(connector) => connector
                            .execute_prepared_update(statement_id, params)
                            .await
                            .map(QueryResult::Noria),
                        Writer::Upstream(connector) => connector
                            .execute_write(statement_id, params)
                            .await
                            .map(QueryResult::Upstream),
                    },
                    _ => internal!(),
                };
                if self.slowlog {
                    let took = start.elapsed();
                    if took.as_secs() > 0 || took.subsec_nanos() > 5_000_000 {
                        let query: &dyn std::fmt::Display = match prep {
                            SqlQuery::Select(ref q) => q,
                            SqlQuery::Insert(ref q) => q,
                            SqlQuery::Update(ref q) => q,
                            _ => internal!(),
                        };
                        warn!(
                            %query,
                            time = ?took,
                            "slow query",
                        );
                    }
                }

                res
            }
        }
    }

    /// Executes `query` using the reader/writer belonging to the calling `Backend` struct.
    pub async fn query(&mut self, query: &str) -> Result<QueryResult<DB>, Error> {
        let span = span!(Level::TRACE, "query", query);
        let _g = span.enter();

        let start = time::Instant::now();

        let parse_result = self.parse_query(query, true);
        let parse_time = start.elapsed().as_micros();

        // fallback to upstream database on query parse failure
        let (parsed_query, use_params) = match parse_result {
            Ok(parsed_tuple) => parsed_tuple,
            Err(e) => {
                // Do not fall back if the set is not allowed
                if matches!(
                    e,
                    Error::ReadySet(ReadySetError::SetDisallowed { statement: _ })
                ) {
                    return Err(e);
                }
                // TODO(Dan): Implement RYW for query_fallback
                match self.reader.upstream {
                    Some(_) => {
                        let res = self.query_fallback(query).await?;
                        if self.slowlog {
                            warn_on_slow_query(&start, query);
                        }
                        return Ok(res);
                    }
                    None => {
                        return Err(e);
                    }
                }
            }
        };

        // If we have an upstream then we will pass valid set statements across to that upstream.
        // If no upstream is present we will ignore the statement
        // Disallowed set statements always produce an error
        if let nom_sql::SqlQuery::Set(s) = &parsed_query {
            if !is_allowed_set(s) {
                return Err(ReadySetError::SetDisallowed {
                    statement: parsed_query.to_string(),
                }
                .into());
            }
        }

        let res = match &mut self.writer {
            // Interacting directly with Noria writer (No RYW support)
            //
            // This is relatively unintuitive and could use a re-write. We only have a single
            // writer, and potentially multiple readers. If our writer is an upstream database, then
            // we have fallback setup, and can assume we have two readers, noria and an upstream
            // database. Otherwise, if our single writer is noria, we assume that there's only a
            // single reader setup - noria.
            // TODO(andrew, justin): Do we want RYW support with the NoriaConnector? Currently, no.
            Writer::Noria(connector) => match parsed_query {
                nom_sql::SqlQuery::Select(q) => {
                    let execution_timer = std::time::Instant::now();
                    let res = self
                        .reader
                        .noria_connector
                        .handle_select(q, use_params, self.ticket.clone())
                        .await;
                    let execution_time = execution_timer.elapsed().as_micros();

                    measure_parse_and_execution_time(
                        parse_time,
                        execution_time,
                        SqlQueryType::Read,
                    );
                    res
                }
                nom_sql::SqlQuery::CreateView(q) => {
                    self.reader.noria_connector.handle_create_view(q).await
                }

                nom_sql::SqlQuery::CreateTable(q) => connector.handle_create_table(q).await,
                nom_sql::SqlQuery::Insert(q) => {
                    let execution_timer = std::time::Instant::now();
                    let res = connector.handle_insert(q).await;
                    let execution_time = execution_timer.elapsed().as_micros();

                    measure_parse_and_execution_time(
                        parse_time,
                        execution_time,
                        SqlQueryType::Write,
                    );
                    res
                }
                nom_sql::SqlQuery::Update(q) => {
                    let execution_timer = std::time::Instant::now();
                    let res = connector.handle_update(q).await;
                    let execution_time = execution_timer.elapsed().as_micros();

                    measure_parse_and_execution_time(
                        parse_time,
                        execution_time,
                        SqlQueryType::Write,
                    );
                    res
                }
                nom_sql::SqlQuery::Delete(q) => {
                    let execution_timer = std::time::Instant::now();
                    let res = connector.handle_delete(q).await;
                    let execution_time = execution_timer.elapsed().as_micros();

                    measure_parse_and_execution_time(
                        parse_time,
                        execution_time,
                        SqlQueryType::Write,
                    );
                    res
                }
                _ => {
                    error!("unsupported query");
                    unsupported!("query type unsupported");
                }
            }
            .map(QueryResult::Noria),
            Writer::Upstream(connector) => {
                match parsed_query {
                    nom_sql::SqlQuery::Select(q) => {
                        let execution_timer = std::time::Instant::now();
                        let res = if self.race_reads {
                            self.race_read(q, query.to_owned(), use_params, self.ticket.clone())
                                .await
                        } else {
                            self.cascade_read(q, query, use_params, self.ticket.clone())
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
                        let execution_timer = std::time::Instant::now();

                        // Update ticket if RYW enabled
                        let query_result = if cfg!(feature = "ryw") {
                            if let Some(timestamp_service) = &mut self.timestamp_client {
                                let (query_result, identifier) =
                                    connector.handle_ryw_write(query).await?;

                                // TODO(andrew): Move table name to table index conversion to timestamp service
                                // https://app.clubhouse.io/readysettech/story/331
                                let index = self
                                    .reader
                                    .noria_connector
                                    .node_index_of(t.name.as_str())
                                    .await?;
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
                                query_result
                            } else {
                                connector.handle_write(query).await?
                            }
                        } else {
                            connector.handle_write(query).await?
                        };
                        let execution_time = execution_timer.elapsed().as_micros();

                        measure_parse_and_execution_time(
                            parse_time,
                            execution_time,
                            SqlQueryType::Write,
                        );

                        Ok(QueryResult::Upstream(query_result))
                    }

                    // Table Create / Drop (RYW not supported)
                    // TODO(andrew, justin): how are these types of writes handled w.r.t RYW?
                    nom_sql::SqlQuery::CreateView(_)
                    | nom_sql::SqlQuery::CreateTable(_)
                    | nom_sql::SqlQuery::DropTable(_)
                    | nom_sql::SqlQuery::Set(_) => connector
                        .handle_write(&parsed_query.to_string())
                        .await
                        .map(QueryResult::Upstream),
                    _ => self.query_fallback(query).await,
                }
            }
        };

        if self.slowlog {
            warn_on_slow_query(&start, query);
        }

        res
    }

    // For debugging purposes
    pub fn ticket(&self) -> &Option<Timestamp> {
        &self.ticket
    }

    fn parse_query(
        &mut self,
        query: &str,
        collapse_where_ins: bool,
    ) -> Result<(SqlQuery, Vec<Literal>), Error> {
        match self.parsed_query_cache.entry(query.to_owned()) {
            Entry::Occupied(entry) => Ok(entry.get().clone()),
            Entry::Vacant(entry) => {
                trace!("Parsing query");
                match nom_sql::parse_query(self.dialect, query) {
                    Ok(mut parsed_query) => {
                        trace!("collapsing where-in clauses");
                        let mut use_params = Vec::new();
                        if collapse_where_ins {
                            if let Some((_, p)) =
                                rewrite::collapse_where_in(&mut parsed_query, true)?
                            {
                                use_params = p;
                            }
                        }
                        Ok(entry.insert((parsed_query, use_params)).clone())
                    }
                    Err(_) => {
                        // error is useless anyway
                        error!(%query, "query can't be parsed: \"{}\"", query);
                        Err(ReadySetError::UnparseableQuery {
                            query: query.to_string(),
                        }
                        .into())
                    }
                }
            }
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
