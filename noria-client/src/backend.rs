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
        ref dt @ DataType::Real(_, _, _, _) => match cs.coltype {
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

#[derive(From)]
pub enum Writer<A: 'static + Authority> {
    MySqlConnector(MySqlConnector),
    NoriaConnector(NoriaConnector<A>),
}

/// Builder for a [`Backend`]
pub struct BackendBuilder<A: 'static + Authority> {
    sanitize: bool,
    static_responses: bool,
    writer: Option<Writer<A>>,
    reader: Option<NoriaConnector<A>>,
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

    pub fn reader(mut self, reader: NoriaConnector<A>) -> Self {
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
    reader: NoriaConnector<A>,
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
/// Variants prefixed with `MySQL` come from a `MySqlConnector` writer
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
}

impl<A: 'static + Authority> Backend<A> {
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
                let (data, select_schema) = self
                    .reader
                    .execute_prepared_select(id, params, self.ticket.clone())
                    .await?;
                Ok(QueryResult::NoriaSelect {
                    data,
                    select_schema,
                })
            }
            SqlQuery::Insert(ref _q) => match &mut self.writer {
                Writer::NoriaConnector(connector) => {
                    let (num_rows_inserted, first_inserted_id) =
                        connector.execute_prepared_insert(id, params).await?;
                    Ok(QueryResult::NoriaInsert {
                        num_rows_inserted,
                        first_inserted_id,
                    })
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
                    let (num_rows_updated, last_inserted_id) =
                        connector.execute_prepared_update(id, params).await?;
                    Ok(QueryResult::NoriaUpdate {
                        num_rows_updated,
                        last_inserted_id,
                    })
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

        return res;
    }

    /// Executes `query` using the reader/writer belonging to the calling `Backend` struct.
    pub async fn query(&mut self, query: &str) -> Result<QueryResult, Error> {
        let span = span!(Level::TRACE, "query", query);
        let _g = span.enter();
        let start = time::Instant::now();

        let query = self.sanitize_query(query);
        let (parsed_query, use_params) = self.parse_query(&query, true)?;
        let parse_time = start.elapsed().as_micros();

        let res = match &mut self.writer {
            // Interacting directly with Noria writer (No RYW support)
            // TODO(andrew, justin): Do we want RYW support with the NoriaConnector? Currently, no.
            Writer::NoriaConnector(connector) => match parsed_query {
                nom_sql::SqlQuery::Select(q) => {
                    let execution_timer = std::time::Instant::now();
                    let (data, select_schema) = self
                        .reader
                        .handle_select(q, use_params, self.ticket.clone())
                        .await?;
                    let execution_time = execution_timer.elapsed().as_micros();

                    measure_parse_and_execution_time(
                        parse_time,
                        execution_time,
                        SqlQueryType::Read,
                    );
                    Ok(QueryResult::NoriaSelect {
                        data,
                        select_schema,
                    })
                }
                nom_sql::SqlQuery::CreateView(q) => {
                    self.reader.handle_create_view(q).await?;
                    Ok(QueryResult::NoriaCreateView)
                }
                nom_sql::SqlQuery::CreateTable(q) => {
                    connector.handle_create_table(q).await?;
                    Ok(QueryResult::NoriaCreateTable)
                }
                nom_sql::SqlQuery::Insert(q) => {
                    let execution_timer = std::time::Instant::now();
                    let (num_rows_inserted, first_inserted_id) = connector.handle_insert(q).await?;
                    let execution_time = execution_timer.elapsed().as_micros();

                    measure_parse_and_execution_time(
                        parse_time,
                        execution_time,
                        SqlQueryType::Write,
                    );
                    Ok(QueryResult::NoriaInsert {
                        num_rows_inserted,
                        first_inserted_id,
                    })
                }
                nom_sql::SqlQuery::Update(q) => {
                    let execution_timer = std::time::Instant::now();
                    let (num_rows_updated, last_inserted_id) = connector.handle_update(q).await?;
                    let execution_time = execution_timer.elapsed().as_micros();

                    measure_parse_and_execution_time(
                        parse_time,
                        execution_time,
                        SqlQueryType::Write,
                    );
                    Ok(QueryResult::NoriaUpdate {
                        num_rows_updated,
                        last_inserted_id,
                    })
                }
                nom_sql::SqlQuery::Delete(q) => {
                    let execution_timer = std::time::Instant::now();
                    let num_rows_deleted = connector.handle_delete(q).await?;
                    let execution_time = execution_timer.elapsed().as_micros();

                    measure_parse_and_execution_time(
                        parse_time,
                        execution_time,
                        SqlQueryType::Write,
                    );
                    Ok(QueryResult::NoriaDelete { num_rows_deleted })
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
                        let (data, select_schema) = self
                            .reader
                            .handle_select(q, use_params, self.ticket.clone())
                            .await?;
                        let execution_time = execution_timer.elapsed().as_micros();
                        measure_parse_and_execution_time(
                            parse_time,
                            execution_time,
                            SqlQueryType::Read,
                        );
                        Ok(QueryResult::NoriaSelect {
                            data,
                            select_schema,
                        })
                    }
                    nom_sql::SqlQuery::Insert(InsertStatement { table: t, .. })
                    | nom_sql::SqlQuery::Update(UpdateStatement { table: t, .. })
                    | nom_sql::SqlQuery::Delete(DeleteStatement { table: t, .. }) => {
                        let execution_timer = std::time::Instant::now();
                        let (num_rows_affected, last_inserted_id, identifier) = connector
                            .on_query(&query, self.timestamp_client.is_some())
                            .await?;

                        // Update ticket if RYW enabled
                        if let Some(timestamp_service) = &mut self.timestamp_client {
                            let identifier = identifier.ok_or_else(|| internal_err("RYW enabled writes should always produce a transaction identifier"))?;

                            // TODO(andrew): Move table name to table index conversion to timestamp service
                            // https://app.clubhouse.io/readysettech/story/331
                            let index = self.reader.node_index_of(t.name.as_str()).await?;
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

                        Ok(QueryResult::MySqlWrite {
                            num_rows_affected,
                            last_inserted_id,
                        })
                    }

                    // Table Create / Drop (RYW not supported)
                    // TODO(andrew, justin): how are these types of writes handled w.r.t RYW?
                    nom_sql::SqlQuery::CreateView(_)
                    | nom_sql::SqlQuery::CreateTable(_)
                    | nom_sql::SqlQuery::DropTable(_) => {
                        let (num_rows_affected, last_inserted_id, _) =
                            connector.on_query(&parsed_query.to_string(), false).await?;
                        Ok(QueryResult::MySqlWrite {
                            num_rows_affected,
                            last_inserted_id,
                        })
                    }
                    // Other queries that we are not expecting, just pass straight to MySQL
                    _ => {
                        let (num_rows_affected, last_inserted_id, _) =
                            connector.on_query(&parsed_query.to_string(), false).await?;
                        Ok(QueryResult::MySqlWrite {
                            num_rows_affected,
                            last_inserted_id,
                        })
                    }
                }
            }
        };

        if self.slowlog {
            let took = start.elapsed();
            if took.as_secs() > 0 || took.subsec_nanos() > 5_000_000 {
                warn!(
                    %query,
                    time = ?took,
                    "slow query",
                );
            }
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

    async fn handle_failure<W: AsyncWrite + Unpin>(
        &mut self,
        q: &str,
        results: QueryResultWriter<'_, W>,
        e: String,
    ) -> io::Result<()> {
        if let Writer::MySqlConnector(connector) = &mut self.writer {
            let res = connector.on_query(q, false).await;
            let res = res.map(|(row_count, last_insert, _)| (row_count, last_insert));
            Backend::<A>::write_query_results(res, results).await
        } else {
            results
                .error(msql_srv::ErrorKind::ER_PARSE_ERROR, e.as_bytes())
                .await?;
            Ok(())
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
        let datatype_params: Vec<DataType> = params
            .into_iter()
            .map(|p| p.value.into_datatype())
            .collect::<Result<Vec<_>, _>>()?;

        let res = match self.execute(id, datatype_params).await {
            Ok(QueryResult::NoriaSelect {
                data,
                select_schema,
            }) => {
                let mut rw = results.start(&select_schema.schema).await?;
                for resultsets in data {
                    for r in resultsets {
                        let mut r: Vec<_> = r.into();
                        if select_schema.use_bogo {
                            // drop bogokey
                            r.pop();
                        }

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
                            write_column(&mut rw, &r[coli], &c).await?;
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
                if pattern.is_match(&query) {
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
                    for &(_, ref r) in columns {
                        writer.write_col(String::from(*r)).await?;
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
                        let mut r: Vec<_> = r.into();
                        if select_schema.use_bogo {
                            // drop bogokey
                            r.pop();
                        }

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
                            write_column(&mut rw, &r[coli], &c).await?;
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
            e @ Err(Error::ReadySet(ReadySetError::UnparseableQuery { .. })) => {
                if self.permissive {
                    warn!(
                        "permissive flag enabled, so returning success despite query parse failure"
                    );
                    return Ok(results.completed(0, 0).await?);
                } else {
                    return self
                        .handle_failure(&query, results, e.unwrap_err().to_string())
                        .await
                        .map_err(Error::Io);
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
