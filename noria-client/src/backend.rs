use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::io;
use std::time;

use async_trait::async_trait;
use derive_more::From;
use metrics::histogram;
use tokio::io::AsyncWrite;
use tracing::Level;

use msql_srv::{self, *};
use mysql_connector::MySqlConnector;
use nom_sql::{self, DeleteStatement, InsertStatement, Literal, SqlQuery, UpdateStatement};
use noria::consensus::Authority;
use noria::consistency::Timestamp;
use noria::errors::internal_err;
use noria::errors::ReadySetError::PreparedStatementMissing;
use noria::results::Results;
use noria::{internal, unsupported, DataType, ReadySetError};
use noria_client_metrics::recorded::SqlQueryType;
use noria_connector::NoriaConnector;
use timestamp_service::client::{TimestampClient, WriteId, WriteKey};

use crate::backend::error::Error;
use crate::convert::ToDataType;
use crate::rewrite;
use crate::utils;

pub mod mysql_connector;
pub mod noria_connector;

pub mod error;

async fn write_column<W: AsyncWrite + Unpin>(
    rw: &mut RowWriter<'_, W>,
    c: &DataType,
    cs: &msql_srv::Column,
) -> Result<(), Error> {
    let written = match *c {
        DataType::None => rw.write_col(None::<i32>).await,
        // NOTE(malte): the code repetition here is unfortunate, but it's hard to factor
        // this out into a helper since i has a different time depending on the DataType
        // variant.
        DataType::Int(i) => {
            if cs.colflags.contains(ColumnFlags::UNSIGNED_FLAG) {
                rw.write_col(i as usize).await
            } else {
                rw.write_col(i as isize).await
            }
        }
        DataType::BigInt(i) => {
            if cs.colflags.contains(ColumnFlags::UNSIGNED_FLAG) {
                rw.write_col(i as usize).await
            } else {
                rw.write_col(i as isize).await
            }
        }
        DataType::UnsignedInt(i) => {
            if cs.colflags.contains(ColumnFlags::UNSIGNED_FLAG) {
                rw.write_col(i as usize).await
            } else {
                rw.write_col(i as isize).await
            }
        }
        DataType::UnsignedBigInt(i) => {
            if cs.colflags.contains(ColumnFlags::UNSIGNED_FLAG) {
                rw.write_col(i as usize).await
            } else {
                rw.write_col(i as isize).await
            }
        }
        DataType::Text(ref t) => rw.write_col(t.to_str().unwrap()).await,
        ref dt @ DataType::TinyText(_) => rw.write_col::<&str>(<&str>::try_from(dt)?).await,
        ref dt @ DataType::Real(_, _) => match cs.coltype {
            msql_srv::ColumnType::MYSQL_TYPE_DECIMAL => {
                let f = dt.to_string();
                rw.write_col(f).await
            }
            msql_srv::ColumnType::MYSQL_TYPE_DOUBLE => {
                let f: f64 = <f64>::try_from(dt)?;
                rw.write_col(f).await
            }
            msql_srv::ColumnType::MYSQL_TYPE_FLOAT => {
                let f: f32 = <f64>::try_from(dt)? as f32;
                rw.write_col(f).await
            }
            _ => {
                internal!()
            }
        },
        DataType::Timestamp(ts) => rw.write_col(ts).await,
        DataType::Time(ref t) => rw.write_col(t.as_ref()).await,
    };
    Ok(written?)
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

#[derive(From)]
pub enum Writer<A: 'static + Authority> {
    MySqlConnector(MySqlConnector),
    NoriaConnector(NoriaConnector<A>),
}

pub struct Reader<A: 'static + Authority> {
    pub mysql_connector: Option<MySqlConnector>,
    pub noria_connector: NoriaConnector<A>,
}

/// Builder for a [`Backend`]
pub struct BackendBuilder<A: 'static + Authority> {
    sanitize: bool,
    static_responses: bool,
    writer: Option<Writer<A>>,
    reader: Option<Reader<A>>,
    slowlog: bool,
    permissive: bool,
    users: HashMap<String, String>,
    require_authentication: bool,
    ticket: Option<Timestamp>,
    timestamp_client: Option<TimestampClient>,
}

impl<A: 'static + Authority> Default for BackendBuilder<A> {
    fn default() -> Self {
        BackendBuilder {
            sanitize: true,
            static_responses: true,
            writer: None,
            reader: None,
            slowlog: false,
            permissive: false,
            users: Default::default(),
            require_authentication: true,
            ticket: None,
            timestamp_client: None,
        }
    }
}

impl<A: 'static + Authority> BackendBuilder<A> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn build(self) -> Backend<A> {
        let parsed_query_cache = HashMap::new();
        let prepared_queries = HashMap::new();
        let prepared_count = 0;
        Backend {
            sanitize: self.sanitize,
            parsed_query_cache,
            prepared_queries,
            prepared_count,
            static_responses: self.static_responses,
            writer: self.writer.expect("BackendBuilder must be passed a writer"),
            reader: self.reader.expect("BackendBuilder must be passed a reader"),
            slowlog: self.slowlog,
            permissive: self.permissive,
            users: self.users,
            require_authentication: self.require_authentication,
            ticket: self.ticket,
            timestamp_client: self.timestamp_client,
        }
    }

    pub fn sanitize(mut self, sanitize: bool) -> Self {
        self.sanitize = sanitize;
        self
    }

    pub fn static_responses(mut self, static_responses: bool) -> Self {
        self.static_responses = static_responses;
        self
    }

    pub fn writer<W: Into<Writer<A>>>(mut self, writer: W) -> Self {
        self.writer = Some(writer.into());
        self
    }

    pub fn reader(mut self, reader: Reader<A>) -> Self {
        self.reader = Some(reader);
        self
    }

    pub fn slowlog(mut self, slowlog: bool) -> Self {
        self.slowlog = slowlog;
        self
    }

    pub fn permissive(mut self, permissive: bool) -> Self {
        self.permissive = permissive;
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

pub struct Backend<A: 'static + Authority> {
    //if false, queries are not sanitized which improves latency.
    sanitize: bool,
    // a cache of all previously parsed queries
    parsed_query_cache: HashMap<String, (SqlQuery, Vec<nom_sql::Literal>)>,
    // all queries previously prepared, mapped by their ID
    prepared_queries: HashMap<u32, SqlQuery>,
    prepared_count: u32,
    static_responses: bool,
    writer: Writer<A>,
    reader: Reader<A>,
    slowlog: bool,
    permissive: bool,
    /// Map from username to password for all users allowed to connect to the db
    users: HashMap<String, String>,
    require_authentication: bool,
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

#[derive(Debug)]
pub struct SelectSchema {
    pub use_bogo: bool,
    pub schema: Vec<Column>,
    pub columns: Vec<String>,
}

/// The type returned when a query is prepared by `Backend`
/// through the `prepare` function.
/// Variants prefixed with `Noria` come from a `NoriaConnector` writer or reader.
/// Variants prefixed with `MySQL` come from a `MySqlConnector` writer
#[derive(Debug)]
pub enum PrepareResult {
    NoriaPrepareSelect {
        statement_id: u32,
        params: Vec<msql_srv::Column>,
        schema: Vec<Column>,
    },
    NoriaPrepareInsert {
        statement_id: u32,
        params: Vec<msql_srv::Column>,
        schema: Vec<Column>,
    },
    NoriaPrepareUpdate {
        statement_id: u64,
        params: Vec<Column>,
    },
    MySqlPrepareWrite {
        statement_id: u32,
    },
}

/// The type returned when a query is carried out by `Backend`,
/// through either the `query` or `execute` functions.
/// Variants prefixed with `Noria` come from a `NoriaConnector` writer or reader.
/// Variants prefixed with `MySQL` come from a `MySqlConnector` writer or reader.
#[derive(Debug)]
pub enum QueryResult {
    NoriaCreateTable,
    NoriaCreateView,
    NoriaInsert {
        num_rows_inserted: u64,
        first_inserted_id: u64,
    },
    NoriaSelect {
        data: Vec<Results>,
        select_schema: SelectSchema,
    },
    NoriaUpdate {
        num_rows_updated: u64,
        last_inserted_id: u64,
    },
    NoriaDelete {
        num_rows_deleted: u64,
    },
    /// `last_inserted_id` is 0 if not applicable
    MySqlWrite {
        num_rows_affected: u64,
        last_inserted_id: u64,
    },
    MySqlSelect {
        data: Vec<mysql_async::Row>,
    },
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
impl<A: 'static + Authority> Backend<A> {
    /// Executes query on mysql_backend, if present, when it cannot be parsed_query
    /// or executed by noria. Returns the query result and RYW ticket.
    pub async fn query_fallback(
        &mut self,
        query: &str,
    ) -> Result<(QueryResult, Option<String>), Error> {
        let is_read = self.is_read(query);
        match self.reader.mysql_connector {
            Some(ref mut connector) => {
                let (res, id) = if is_read {
                    (connector.handle_select(query).await?, None)
                } else {
                    connector.handle_write(query, false).await?
                };
                Ok((res, id))
            }
            None => Err(Error::ReadySet(ReadySetError::FallbackNoConnector)),
        }
    }

    fn is_read(&self, query: &str) -> bool {
        let q = query.to_string().trim_start().to_lowercase();
        q.starts_with("select") || q.starts_with("show") || q.starts_with("describe")
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
    ) -> Result<QueryResult, Error> {
        match self
            .reader
            .noria_connector
            .handle_select(q, use_params, ticket)
            .await
        {
            Ok(r) => Ok(r),
            Err(e) => {
                // Check if we have fallback setup. If not, we need to return this error,
                // otherwise, we transition to fallback.
                match self.reader.mysql_connector {
                    Some(ref mut connector) => connector.handle_select(query_str).await,
                    None => Err(e),
                }
            }
        }
    }

    /// Prepares `query` to be executed later using the reader/writer belonging
    /// to the calling `Backend` struct and adds the prepared query
    /// to the calling struct's map of prepared queries with a unique id.
    pub async fn prepare(&mut self, query: &str) -> Result<PrepareResult, Error> {
        //the updated count will serve as the id for the prepared statement
        self.prepared_count += 1;

        let span = span!(Level::DEBUG, "prepare", query);
        let _g = span.enter();

        let query = self.sanitize_query(query);
        let (parsed_query, _) = self.parse_query(&query, false)?;

        let res = match parsed_query {
            nom_sql::SqlQuery::Select(_) => {
                //todo : also prepare in writer if it is mysql
                let (statement_id, params, schema) = self
                    .reader
                    .noria_connector
                    .prepare_select(parsed_query.clone(), self.prepared_count)
                    .await?;
                Ok(PrepareResult::NoriaPrepareSelect {
                    statement_id,
                    params,
                    schema,
                })
            }
            nom_sql::SqlQuery::Insert(_) => {
                match &mut self.writer {
                    Writer::NoriaConnector(connector) => {
                        let (statement_id, params, schema) = connector
                            .prepare_insert(parsed_query.clone(), self.prepared_count)
                            .await?;
                        Ok(PrepareResult::NoriaPrepareInsert {
                            statement_id,
                            params,
                            schema,
                        })
                    }
                    Writer::MySqlConnector(connector) => {
                        // todo : handle params correctly. dont just leave them blank.
                        Ok(PrepareResult::MySqlPrepareWrite {
                            statement_id: connector
                                .on_prepare(&parsed_query.to_string(), self.prepared_count)
                                .await?,
                        })
                    }
                }
            }
            nom_sql::SqlQuery::Update(_) => {
                match &mut self.writer {
                    Writer::NoriaConnector(connector) => {
                        let (statement_id, params) = connector
                            .prepare_update(parsed_query.clone(), self.prepared_count)
                            .await?;
                        Ok(PrepareResult::NoriaPrepareUpdate {
                            statement_id,
                            params,
                        })
                    }
                    Writer::MySqlConnector(connector) => {
                        // todo : handle params correctly. dont just leave them blank.
                        Ok(PrepareResult::MySqlPrepareWrite {
                            statement_id: connector
                                .on_prepare(&parsed_query.to_string(), self.prepared_count)
                                .await?,
                        })
                    }
                }
            }
            _ => {
                error!("unsupported query");
                unsupported!("query type unsupported");
            }
        };

        self.prepared_queries
            .insert(self.prepared_count, parsed_query.to_owned());

        res
    }

    /// Executes the already-prepared query with id `id` and parameters `params` using the reader/writer
    /// belonging to the calling `Backend` struct.
    // TODO(andrew, justin): add RYW support for executing prepared queries
    pub async fn execute(&mut self, id: u32, params: Vec<DataType>) -> Result<QueryResult, Error> {
        let span = span!(Level::TRACE, "execute", id);
        let _g = span.enter();

        let start = time::Instant::now();

        let prep: SqlQuery = self
            .prepared_queries
            .get(&id)
            .cloned()
            .ok_or(PreparedStatementMissing)?;

        let res = match prep {
            SqlQuery::Select(_) => {
                self.reader
                    .noria_connector
                    .execute_prepared_select(id, params, self.ticket.clone())
                    .await
            }
            SqlQuery::Insert(ref _q) => match &mut self.writer {
                Writer::NoriaConnector(connector) => {
                    connector.execute_prepared_insert(id, params).await
                }
                Writer::MySqlConnector(connector) => {
                    let (num_rows_affected, last_inserted_id) =
                        connector.on_execute(id, params).await?;
                    Ok(QueryResult::MySqlWrite {
                        num_rows_affected,
                        last_inserted_id,
                    })
                }
            },
            SqlQuery::Update(ref _q) => match &mut self.writer {
                Writer::NoriaConnector(connector) => {
                    connector.execute_prepared_update(id, params).await
                }
                Writer::MySqlConnector(connector) => {
                    let (num_rows_affected, last_inserted_id) =
                        connector.on_execute(id, params).await?;
                    Ok(QueryResult::MySqlWrite {
                        num_rows_affected,
                        last_inserted_id,
                    })
                }
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

    /// Executes `query` using the reader/writer belonging to the calling `Backend` struct.
    pub async fn query(&mut self, query: &str) -> Result<QueryResult, Error> {
        let span = span!(Level::TRACE, "query", query);
        let _g = span.enter();

        let start = time::Instant::now();
        let query = self.sanitize_query(query);

        let parse_result = self.parse_query(&query, true);
        let parse_time = start.elapsed().as_micros();

        // fallback to mysql database on query parse failure
        let (parsed_query, use_params) = match parse_result {
            Ok(parsed_tuple) => parsed_tuple,
            Err(e) => {
                // TODO(Dan): Implement RYW for query_fallback
                match self.reader.mysql_connector {
                    Some(_) => {
                        let (res, _) = self.query_fallback(&query).await?;
                        if self.slowlog {
                            warn_on_slow_query(&start, &query);
                        }
                        return Ok(res);
                    }
                    None => {
                        return Err(e);
                    }
                }
            }
        };

        let res = match &mut self.writer {
            // Interacting directly with Noria writer (No RYW support)
            //
            // This is relatively unintuitive and could use a re-write. We only have a single
            // writer, and potentially multiple readers. If our writer is MySQL, then we have
            // fallback setup, and can assume we have two readers, noria and MySQL. Otherwise, if
            // our single writer is noria, we assume that there's only a single reader setup -
            // noria.
            // TODO(andrew, justin): Do we want RYW support with the NoriaConnector? Currently, no.
            Writer::NoriaConnector(connector) => match parsed_query {
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

                nom_sql::SqlQuery::DropTable(_) => {
                    warn!("Ignoring drop table because using Noria writer not Mysql writer.");
                    if self.permissive {
                        warn!("Permissive flag enabled, so returning successful drop table despite failure.");
                        Ok(QueryResult::MySqlWrite {
                            num_rows_affected: 0,
                            last_inserted_id: 0,
                        })
                    } else {
                        unimplemented!()
                    }
                }
                _ => {
                    error!("unsupported query");
                    unsupported!("query type unsupported");
                }
            },
            Writer::MySqlConnector(connector) => {
                match parsed_query {
                    nom_sql::SqlQuery::Select(q) => {
                        let execution_timer = std::time::Instant::now();
                        let res = self
                            .cascade_read(q, &query, use_params, self.ticket.clone())
                            .await;
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
                        let (query_result, identifier) = connector
                            .handle_write(&query, self.timestamp_client.is_some())
                            .await?;

                        // Update ticket if RYW enabled
                        if let Some(timestamp_service) = &mut self.timestamp_client {
                            let identifier = identifier.ok_or_else(|| internal_err("RYW enabled writes should always produce a transaction identifier"))?;

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
                        }
                        let execution_time = execution_timer.elapsed().as_micros();

                        measure_parse_and_execution_time(
                            parse_time,
                            execution_time,
                            SqlQueryType::Write,
                        );

                        Ok(query_result)
                    }

                    // Table Create / Drop (RYW not supported)
                    // TODO(andrew, justin): how are these types of writes handled w.r.t RYW?
                    nom_sql::SqlQuery::CreateView(_)
                    | nom_sql::SqlQuery::CreateTable(_)
                    | nom_sql::SqlQuery::DropTable(_) => {
                        let (query_result, _) = connector
                            .handle_write(&parsed_query.to_string(), false)
                            .await?;
                        Ok(query_result)
                    }
                    _ => {
                        let (query_result, _) = self.query_fallback(&query).await?;
                        Ok(query_result)
                    }
                }
            }
        };

        if self.slowlog {
            warn_on_slow_query(&start, &query);
        }

        res
    }

    // For debugging purposes
    pub fn ticket(&self) -> &Option<Timestamp> {
        &self.ticket
    }

    fn sanitize_query(&mut self, query: &str) -> String {
        trace!("sanitize");
        if self.sanitize {
            let q = utils::sanitize_query(query);
            trace!(%q, "sanitized");
            q
        } else {
            query.to_owned()
        }
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
                match nom_sql::parse_query(query) {
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

    async fn write_query_results<W: AsyncWrite + Unpin>(
        r: Result<(u64, u64), Error>,
        results: QueryResultWriter<'_, W>,
    ) -> io::Result<()> {
        match r {
            Ok((row_count, last_insert)) => results.completed(row_count, last_insert).await,
            Err(e) => {
                results
                    .error(e.error_kind(), e.to_string().as_bytes())
                    .await
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

#[async_trait]
impl<W, A> MysqlShim<W> for Backend<A>
where
    W: AsyncWrite + Unpin + Send + 'static,
    A: 'static + Authority,
{
    type Error = Error;

    async fn on_prepare(
        &mut self,
        query: &str,
        info: StatementMetaWriter<'_, W>,
    ) -> std::result::Result<(), Error> {
        trace!("delegate");
        let res = match self.prepare(query).await {
            Ok(PrepareResult::NoriaPrepareSelect {
                statement_id,
                params,
                schema,
            }) => {
                info.reply(statement_id, params.as_slice(), schema.as_slice())
                    .await
            }
            Ok(PrepareResult::NoriaPrepareInsert {
                statement_id,
                params,
                schema,
            }) => {
                info.reply(statement_id, params.as_slice(), schema.as_slice())
                    .await
            }
            Ok(PrepareResult::NoriaPrepareUpdate {
                statement_id: _,
                params,
            }) => info.reply(self.prepared_count, &params[..], &[]).await,
            Ok(PrepareResult::MySqlPrepareWrite { statement_id: _ }) => {
                info.reply(self.prepared_count, &[], &[]).await
            } // TODO : handle params correctly. dont just leave them blank.
            Err(e) => info.error(e.error_kind(), e.to_string().as_bytes()).await,
        };

        Ok(res?)
    }

    async fn on_execute(
        &mut self,
        id: u32,
        params: ParamParser<'_>,
        results: QueryResultWriter<'_, W>,
    ) -> std::result::Result<(), Error> {
        let mut datatype_params = Vec::new();
        for p in params {
            datatype_params.push(p?.value.into_datatype()?);
        }
        let res = match self.execute(id, datatype_params).await {
            Ok(QueryResult::NoriaSelect {
                data,
                select_schema,
            }) => {
                let mut rw = results.start(&select_schema.schema).await?;
                for resultsets in data {
                    for r in resultsets {
                        for c in &select_schema.schema {
                            let coli = select_schema
                                .columns
                                .iter()
                                .position(|f| f == &c.column)
                                .ok_or_else(|| {
                                    internal_err(format!(
                                        "tried to emit column {:?} not in getter with schema {:?}",
                                        c.column, select_schema.columns
                                    ))
                                })?;
                            write_column(&mut rw, &r[coli], c).await?;
                        }
                        rw.end_row().await?;
                    }
                }
                rw.finish().await
            }
            Ok(QueryResult::NoriaInsert {
                num_rows_inserted,
                first_inserted_id,
            }) => Backend::<A>::write_query_results(Ok((num_rows_inserted, first_inserted_id)), results).await,
            Ok(QueryResult::NoriaUpdate {
                num_rows_updated,
                last_inserted_id
            }) => Backend::<A>::write_query_results(Ok((num_rows_updated, last_inserted_id)), results).await,
            Ok(QueryResult::MySqlWrite {
                num_rows_affected,
                last_inserted_id,
            }) => {
                Backend::<A>::write_query_results(
                    Ok((num_rows_affected, last_inserted_id)),
                    results,
                )
                .await
            }
            e @ Err(Error::ReadySet(ReadySetError::PreparedStatementMissing)) => {
                return results
                    .error(
                        e.unwrap_err().error_kind(),
                        "non-existent statement".as_bytes(),
                    )
                    .await
                    .map_err(Error::from)
            }
            Err(e) => return Err(e),
            _ => internal!("Matched a QueryResult that is not supported by on_prepare/on_execute in on_execute."),
        };

        Ok(res?)
    }

    async fn on_close(&mut self, _: u32) {}

    async fn on_query(
        &mut self,
        query: &str,
        results: QueryResultWriter<'_, W>,
    ) -> std::result::Result<(), Error> {
        if self.static_responses {
            for &(ref pattern, ref columns) in &*utils::HARD_CODED_REPLIES {
                if pattern.is_match(query) {
                    trace!("sending static response");
                    let cols: Vec<_> = columns
                        .iter()
                        .map(|c| Column {
                            table: String::from(""),
                            column: String::from(c.0),
                            coltype: ColumnType::MYSQL_TYPE_STRING,
                            colflags: ColumnFlags::empty(),
                        })
                        .collect();
                    let mut writer = results.start(&cols[..]).await?;
                    for &(_, r) in columns {
                        writer.write_col(String::from(r)).await?;
                    }
                    return Ok(writer.end_row().await?);
                }
            }
        }

        let res = match self.query(query).await {
            Ok(QueryResult::NoriaCreateTable) => results.completed(0, 0).await,
            Ok(QueryResult::NoriaCreateView) => results.completed(0, 0).await,
            Ok(QueryResult::NoriaInsert {
                num_rows_inserted,
                first_inserted_id,
            }) => {
                Backend::<A>::write_query_results(
                    Ok((num_rows_inserted, first_inserted_id)),
                    results,
                )
                .await
            }
            Ok(QueryResult::NoriaSelect {
                data,
                select_schema,
            }) => {
                let mut rw = results.start(&select_schema.schema).await?;
                for resultsets in data {
                    for r in resultsets {
                        for c in &select_schema.schema {
                            let coli = select_schema
                                .columns
                                .iter()
                                .position(|f| f == &c.column)
                                .ok_or_else(|| {
                                    internal_err(format!(
                                        "tried to emit column {:?} not in getter with schema {:?}",
                                        c.column, select_schema.columns
                                    ))
                                })?;
                            write_column(&mut rw, &r[coli], c).await?;
                        }
                        rw.end_row().await?;
                    }
                }
                rw.finish().await
            }
            Ok(QueryResult::NoriaUpdate {
                num_rows_updated,
                last_inserted_id,
            }) => {
                Backend::<A>::write_query_results(Ok((num_rows_updated, last_inserted_id)), results)
                    .await
            }
            Ok(QueryResult::NoriaDelete { num_rows_deleted }) => {
                results.completed(num_rows_deleted, 0).await
            }
            Ok(QueryResult::MySqlWrite {
                num_rows_affected,
                last_inserted_id,
            }) => {
                Backend::<A>::write_query_results(
                    Ok((num_rows_affected, last_inserted_id)),
                    results,
                )
                .await
            }
            Ok(QueryResult::MySqlSelect { data }) => {
                if let Some(cols) = data.get(0).cloned() {
                    let cols = cols.columns_ref();
                    let formatted_cols = cols
                        .iter()
                        .map(|c| Column {
                            table: c.table_str().to_string(),
                            column: c.name_str().to_string(),
                            coltype: c.column_type(),
                            colflags: c.flags(),
                        })
                        .collect::<Vec<Column>>();
                    let mut rw = results.start(&formatted_cols).await?;
                    for r in data {
                        for (coli, _) in formatted_cols.iter().enumerate() {
                            rw.write_col(&r[coli]).await?;
                        }
                        rw.end_row().await?
                    }
                    rw.finish().await
                } else {
                    // TODO: (Dan) msql-srv will respond Query Ok in this case instead of "Empty
                    // set". Passing a dummy column to result.start() will trigger an empty set
                    // response. A better solution could be to modify msql-srv.
                    let rw = results.start(&[]).await?;
                    rw.finish().await
                }
            }

            Err(e) => {
                results
                    .error(e.error_kind(), e.to_string().as_bytes())
                    .await
            }
        };

        Ok(res?)
    }

    fn password_for_username(&self, username: &[u8]) -> Option<Vec<u8>> {
        String::from_utf8(username.to_vec())
            .ok()
            .and_then(|un| self.users.get(&un))
            .cloned()
            .map(String::into_bytes)
    }

    fn require_authentication(&self) -> bool {
        self.require_authentication
    }
}
