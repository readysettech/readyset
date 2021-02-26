use noria::results::Results;
use noria::DataType;

use derive_more::From;
use msql_srv::{self, *};
use nom_sql::{self, Literal, SqlQuery};

use async_trait::async_trait;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::io;
use std::time;
use tokio::io::AsyncWrite;
use tracing::Level;

use crate::rewrite;
use crate::utils;

pub mod mysql_connector;
pub mod noria_connector;

pub mod error;

use crate::backend::error::Error;
use mysql_connector::MySqlConnector;
use noria_connector::NoriaConnector;

async fn write_column<W: AsyncWrite + Unpin>(
    rw: &mut RowWriter<'_, W>,
    c: &DataType,
    cs: &msql_srv::Column,
) {
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
        ref dt @ DataType::TinyText(_) => rw.write_col::<&str>(dt.into()).await,
        ref dt @ DataType::Real(_, _) => match cs.coltype {
            msql_srv::ColumnType::MYSQL_TYPE_DECIMAL => {
                let f = dt.to_string();
                rw.write_col(f).await
            }
            msql_srv::ColumnType::MYSQL_TYPE_DOUBLE => {
                let f: f64 = dt.into();
                rw.write_col(f).await
            }
            _ => unreachable!(),
        },
        DataType::Timestamp(ts) => rw.write_col(ts).await,
    };
    match written {
        Ok(_) => (),
        Err(e) => panic!("failed to write column: {:?}", e),
    }
}

#[derive(From)]
pub enum Writer {
    MySqlConnector(MySqlConnector),
    NoriaConnector(NoriaConnector),
}

/// Builder for a [`Backend`]
pub struct BackendBuilder {
    sanitize: bool,
    static_responses: bool,
    writer: Option<Writer>,
    reader: Option<NoriaConnector>,
    slowlog: bool,
    permissive: bool,
    users: HashMap<String, String>,
    require_authentication: bool,
}

impl Default for BackendBuilder {
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
        }
    }
}

impl BackendBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn build(self) -> Backend {
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

    pub fn writer<W: Into<Writer>>(mut self, writer: W) -> Self {
        self.writer = Some(writer.into());
        self
    }

    pub fn reader(mut self, reader: NoriaConnector) -> Self {
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
}

pub struct Backend {
    //if false, queries are not sanitized which improves latency.
    sanitize: bool,
    // a cache of all previously parsed queries
    parsed_query_cache: HashMap<String, (SqlQuery, Vec<nom_sql::Literal>)>,
    // all queries previously prepared, mapped by their ID
    prepared_queries: HashMap<u32, SqlQuery>,
    prepared_count: u32,
    static_responses: bool,
    writer: Writer,
    reader: NoriaConnector,
    slowlog: bool,
    permissive: bool,
    /// Map from username to password for all users allowed to connect to the db
    users: HashMap<String, String>,
    require_authentication: bool,
}

#[derive(Debug)]
pub struct SelectSchema {
    use_bogo: bool,
    schema: Vec<Column>,
    columns: Vec<String>,
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

impl Backend {
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
                Err(Error::UnsupportedError("Unsupported query".to_string()))
            }
        };

        self.prepared_queries
            .insert(self.prepared_count, parsed_query.to_owned());

        res
    }

    /// Executes the already-prepared query with id `id` and parameters `params` using the reader/writer
    /// belonging to the calling `Backend` struct.
    pub async fn execute(
        &mut self,
        id: u32,
        params: ParamParser<'_>,
    ) -> Result<QueryResult, Error> {
        let span = span!(Level::TRACE, "execute", id);
        let _g = span.enter();

        let start = time::Instant::now();

        let prep: SqlQuery = self
            .prepared_queries
            .get(&id)
            .cloned()
            .ok_or(Error::MissingPreparedStatement)?;

        let res = match prep {
            SqlQuery::Select(_) => {
                let (data, select_schema) = self.reader.execute_prepared_select(id, params).await?;
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
            _ => unreachable!(),
        };

        if self.slowlog {
            let took = start.elapsed();
            if took.as_secs() > 0 || took.subsec_nanos() > 5_000_000 {
                let query: &dyn std::fmt::Display = match prep {
                    SqlQuery::Select(ref q) => q,
                    SqlQuery::Insert(ref q) => q,
                    SqlQuery::Update(ref q) => q,
                    _ => unreachable!(),
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

        let res = match parsed_query {
            nom_sql::SqlQuery::CreateTable(q) => match &mut self.writer {
                Writer::NoriaConnector(connector) => {
                    connector.handle_create_table(q).await?;
                    Ok(QueryResult::NoriaCreateTable)
                }
                Writer::MySqlConnector(connector) => {
                    let (num_rows_affected, last_inserted_id) =
                        connector.on_query(&q.to_string()).await?;
                    Ok(QueryResult::MySqlWrite {
                        num_rows_affected,
                        last_inserted_id,
                    })
                }
            },
            nom_sql::SqlQuery::CreateView(q) => {
                self.reader.handle_create_view(q).await?;
                Ok(QueryResult::NoriaCreateView)
            }
            nom_sql::SqlQuery::Insert(q) => match &mut self.writer {
                Writer::NoriaConnector(connector) => {
                    let (num_rows_inserted, first_inserted_id) = connector.handle_insert(q).await?;
                    Ok(QueryResult::NoriaInsert {
                        num_rows_inserted,
                        first_inserted_id,
                    })
                }
                Writer::MySqlConnector(connector) => {
                    let (num_rows_affected, last_inserted_id) =
                        connector.on_query(&q.to_string()).await?;
                    Ok(QueryResult::MySqlWrite {
                        num_rows_affected,
                        last_inserted_id,
                    })
                }
            },
            nom_sql::SqlQuery::Select(q) => {
                let (data, select_schema) = self.reader.handle_select(q, use_params).await?;
                Ok(QueryResult::NoriaSelect {
                    data,
                    select_schema,
                })
            }
            nom_sql::SqlQuery::Update(q) => match &mut self.writer {
                Writer::NoriaConnector(connector) => {
                    let (num_rows_updated, last_inserted_id) = connector.handle_update(q).await?;
                    Ok(QueryResult::NoriaUpdate {
                        num_rows_updated,
                        last_inserted_id,
                    })
                }
                Writer::MySqlConnector(connector) => {
                    let (num_rows_affected, last_inserted_id) =
                        connector.on_query(&q.to_string()).await?;
                    Ok(QueryResult::MySqlWrite {
                        num_rows_affected,
                        last_inserted_id,
                    })
                }
            },
            nom_sql::SqlQuery::Delete(q) => match &mut self.writer {
                Writer::NoriaConnector(connector) => {
                    let num_rows_deleted = connector.handle_delete(q).await?;
                    Ok(QueryResult::NoriaDelete { num_rows_deleted })
                }
                Writer::MySqlConnector(connector) => {
                    let (num_rows_affected, last_inserted_id) =
                        connector.on_query(&q.to_string()).await?;
                    Ok(QueryResult::MySqlWrite {
                        num_rows_affected,
                        last_inserted_id,
                    })
                }
            },
            nom_sql::SqlQuery::DropTable(q) => match &mut self.writer {
                Writer::MySqlConnector(connector) => {
                    let (num_rows_affected, last_inserted_id) =
                        connector.on_query(&q.to_string()).await?;
                    Ok(QueryResult::MySqlWrite {
                        num_rows_affected,
                        last_inserted_id,
                    })
                }
                _ => {
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
            },
            _ => match &mut self.writer {
                Writer::MySqlConnector(connector) => {
                    let (num_rows_affected, last_inserted_id) =
                        connector.on_query(&parsed_query.to_string()).await?;
                    Ok(QueryResult::MySqlWrite {
                        num_rows_affected,
                        last_inserted_id,
                    })
                }
                _ => {
                    error!("unsupported query");
                    Err(Error::UnsupportedError("Unsupported query".to_string()))
                }
            },
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
                                rewrite::collapse_where_in(&mut parsed_query, true)
                            {
                                use_params = p;
                            }
                        }
                        Ok(entry.insert((parsed_query, use_params)).clone())
                    }
                    Err(e) => {
                        error!(%query, "query can't be parsed: \"{}\"", query);
                        Err(Error::ParseError(e.to_string()))
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
            Backend::write_query_results(connector.on_query(q).await, results).await
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
            Err(e) => results.error(e.error_kind(), e.message().as_bytes()).await,
        }
    }
}

#[async_trait]
impl<W: AsyncWrite + Unpin + Send + 'static> MysqlShim<W> for Backend {
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
            Err(e) => info.error(e.error_kind(), e.message().as_bytes()).await,
        };

        Ok(res?)
    }

    async fn on_execute(
        &mut self,
        id: u32,
        params: ParamParser<'_>,
        results: QueryResultWriter<'_, W>,
    ) -> std::result::Result<(), Error> {
        let res = match self.execute(id, params).await {
            Ok(QueryResult::NoriaSelect {
                data,
                select_schema,
            }) => {
                let mut rw = results.start(&select_schema.schema).await.unwrap();
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
                                .unwrap_or_else(|| {
                                    panic!(
                                        "tried to emit column {:?} not in getter with schema {:?}",
                                        c.column, select_schema.columns
                                    );
                                });
                            write_column(&mut rw, &r[coli], &c).await;
                        }
                        rw.end_row().await?;
                    }
                }
                rw.finish().await
            }
            Ok(QueryResult::NoriaInsert {
                num_rows_inserted,
                first_inserted_id,
            }) => Backend::write_query_results(Ok((num_rows_inserted, first_inserted_id)), results).await,
            Ok(QueryResult::NoriaUpdate {
                num_rows_updated,
                last_inserted_id
            }) => Backend::write_query_results(Ok((num_rows_updated, last_inserted_id)), results).await,
            Ok(QueryResult::MySqlWrite {
                num_rows_affected,
                last_inserted_id,
            }) => {
                Backend::write_query_results(
                    Ok((num_rows_affected, last_inserted_id)),
                    results,
                )
                .await
            }
            Err(Error::MissingPreparedStatement) => {
                return results
                    .error(
                        Error::MissingPreparedStatement.error_kind(),
                        "non-existent statement".as_bytes(),
                    )
                    .await
                    .map_err(|e| Error::IOError(e));
            }
            Err(e) => return Err(e),
            _ => unreachable!("Matched a QueryResult that is not supported by on_prepare/on_execute in on_execute."),
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
                Backend::write_query_results(Ok((num_rows_inserted, first_inserted_id)), results)
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
                                .unwrap_or_else(|| {
                                    panic!(
                                        "tried to emit column {:?} not in getter with schema {:?}",
                                        c.column, &select_schema.columns
                                    );
                                });
                            write_column(&mut rw, &r[coli], &c).await;
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
                Backend::write_query_results(Ok((num_rows_updated, last_inserted_id)), results)
                    .await
            }
            Ok(QueryResult::NoriaDelete { num_rows_deleted }) => {
                results.completed(num_rows_deleted, 0).await
            }
            Ok(QueryResult::MySqlWrite {
                num_rows_affected,
                last_inserted_id,
            }) => {
                Backend::write_query_results(Ok((num_rows_affected, last_inserted_id)), results)
                    .await
            }
            Err(Error::ParseError(e)) => {
                if self.permissive {
                    warn!(
                        "permissive flag enabled, so returning success despite query parse failure"
                    );
                    return Ok(results.completed(0, 0).await?);
                } else {
                    return self
                        .handle_failure(&query, results, e.to_string())
                        .await
                        .map_err(|e| Error::IOError(e));
                }
            }
            Err(e) => results.error(e.error_kind(), e.message().as_bytes()).await,
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
