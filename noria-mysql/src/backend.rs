use noria::DataType;

use msql_srv::{self, *};
use nom_sql::{self, SqlQuery};

use std::collections::HashMap;
use std::io;
use std::time;
use tracing::Level;

use crate::rewrite;
use crate::utils;

pub mod mysql_connector;
pub mod noria_connector;

pub(crate) mod error;

use crate::backend::error::Error;
use crate::backend::error::Error::{IOError, ParseError};
use mysql_connector::MySqlConnector;
use noria_connector::NoriaConnector;

pub enum Writer {
    MySqlConnector(MySqlConnector),
    NoriaConnector(NoriaConnector),
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
}

pub struct SelectSchema {
    use_bogo: bool,
    schema: Vec<Column>,
    columns: Vec<String>,
}
impl Backend {
    pub fn new(
        sanitize: bool,
        static_responses: bool,
        writer: Writer,
        reader: NoriaConnector,
        slowlog: bool,
        permissive: bool,
        users: HashMap<String, String>,
    ) -> Self {
        let parsed_query_cache = HashMap::new();
        let prepared_queries = HashMap::new();
        let prepared_count = 0;
        Backend {
            sanitize,
            parsed_query_cache,
            prepared_queries,
            prepared_count,
            static_responses,
            reader,
            writer,
            slowlog,
            permissive,
            users,
        }
    }

    fn handle_failure<W: io::Write>(
        &mut self,
        q: &str,
        results: QueryResultWriter<W>,
    ) -> io::Result<()> {
        if let Writer::MySqlConnector(connector) = &mut self.writer {
            Backend::write_query_results(connector.on_query(q), results)
        } else {
            Ok(())
        }
    }

    fn write_query_results<W: io::Write>(
        r: Result<(u64, u64), Error>,
        results: QueryResultWriter<W>,
    ) -> io::Result<()> {
        match r {
            Ok((row_count, last_insert)) => results.completed(row_count, last_insert),
            Err(e) => results.error(e.error_kind(), e.message().as_bytes()),
        }
    }
}

impl<W: io::Write> MysqlShim<W> for Backend {
    type Error = Error;

    fn on_prepare(
        &mut self,
        query: &str,
        info: StatementMetaWriter<W>,
    ) -> std::result::Result<(), Error> {
        //the updated count will serve as the id for the prepared statement
        self.prepared_count += 1;

        let span = span!(Level::DEBUG, "prepare", query);
        let _g = span.enter();

        trace!("sanitize");
        let query = if self.sanitize {
            utils::sanitize_query(query)
        } else {
            query.to_owned()
        };

        if !self.parsed_query_cache.contains_key(&query) {
            //if we dont already have the parsed query, try to parse it
            trace!("Parsing query");
            match nom_sql::parse_query(&query) {
                Ok(q) => {
                    self.parsed_query_cache
                        .insert(query.to_owned(), (q.clone(), vec![]));
                }

                Err(_e) => {
                    // if nom-sql rejects the query, there is no chance Noria will like it
                    let e_string = format!("query can't be parsed: \"{}\"", query);
                    error!(%query, "query can't be parsed: \"{}\"", query);
                    let error = ParseError(e_string);
                    info.error(error.error_kind(), error.message().as_bytes())?;
                    return Err(error);
                }
            }
        }

        let sql_q = match self.parsed_query_cache.get(&query) {
            Some((q, _)) => q.clone(),
            None => unreachable!(),
        };

        trace!("delegate");
        let res = match sql_q {
            nom_sql::SqlQuery::Select(_) => {
                //todo : also prepare in writer if it is mysql
                let (id, params, schema) = self
                    .reader
                    .prepare_select(sql_q.clone(), self.prepared_count)?;
                info.reply(id, params.as_slice(), schema.as_slice())
            }
            nom_sql::SqlQuery::Insert(_) => {
                match &mut self.writer {
                    Writer::NoriaConnector(connector) => {
                        match connector.prepare_insert(sql_q.clone(), self.prepared_count) {
                            Ok((id, params, schema)) => {
                                info.reply(id, params.as_slice(), schema.as_slice())
                            }
                            Err(e) => info.error(e.error_kind(), e.message().as_bytes()),
                        }
                    }
                    Writer::MySqlConnector(connector) => {
                        // todo : handle params correctly. dont just leave them blank.
                        connector.on_prepare(&sql_q.to_string(), self.prepared_count)?;
                        info.reply(self.prepared_count, &[], &[])
                    }
                }
            }
            nom_sql::SqlQuery::Update(_) => {
                match &mut self.writer {
                    Writer::NoriaConnector(connector) => {
                        let (_, params) =
                            connector.prepare_update(sql_q.clone(), self.prepared_count)?;
                        info.reply(self.prepared_count, &params[..], &[])
                    }
                    Writer::MySqlConnector(connector) => {
                        // todo : handle params correctly. dont just leave them blank.
                        connector.on_prepare(&sql_q.to_string(), self.prepared_count)?;
                        info.reply(self.prepared_count, &[], &[])
                    }
                }
            }
            _ => unimplemented!(),
        };
        self.prepared_queries
            .insert(self.prepared_count, sql_q.to_owned());
        Ok(res?)
    }

    fn on_execute(
        &mut self,
        id: u32,
        params: ParamParser,
        results: QueryResultWriter<W>,
    ) -> std::result::Result<(), Error> {
        let span = span!(Level::TRACE, "execute", id);
        let _g = span.enter();

        let start = time::Instant::now();

        // TODO(malte): unfortunate clone here, but we can't call execute_select(&mut self) if we
        // have self.prepared borrowed
        let prep: SqlQuery = {
            match self.prepared_queries.get(&id) {
                Some(e) => e.clone(),
                None => {
                    let e = Error::MissingPreparedStatement;
                    return results
                        .error(e.error_kind(), "non-existent statement".as_bytes())
                        .map_err(|e| IOError(e));
                }
            }
        };

        trace!("delegate");
        let res = match prep {
            SqlQuery::Select(_) => {
                let write_column = |rw: &mut RowWriter<W>, c: &DataType, cs: &msql_srv::Column| {
                    let written = match *c {
                        DataType::None => rw.write_col(None::<i32>),
                        // NOTE(malte): the code repetition here is unfortunate, but it's hard to factor
                        // this out into a helper since i has a different time depending on the DataType
                        // variant.
                        DataType::Int(i) => {
                            if cs.colflags.contains(ColumnFlags::UNSIGNED_FLAG) {
                                rw.write_col(i as usize)
                            } else {
                                rw.write_col(i as isize)
                            }
                        }
                        DataType::BigInt(i) => {
                            if cs.colflags.contains(ColumnFlags::UNSIGNED_FLAG) {
                                rw.write_col(i as usize)
                            } else {
                                rw.write_col(i as isize)
                            }
                        }
                        DataType::UnsignedInt(i) => {
                            if cs.colflags.contains(ColumnFlags::UNSIGNED_FLAG) {
                                rw.write_col(i as usize)
                            } else {
                                rw.write_col(i as isize)
                            }
                        }
                        DataType::UnsignedBigInt(i) => {
                            if cs.colflags.contains(ColumnFlags::UNSIGNED_FLAG) {
                                rw.write_col(i as usize)
                            } else {
                                rw.write_col(i as isize)
                            }
                        }
                        DataType::Text(ref t) => rw.write_col(t.to_str().unwrap()),
                        ref dt @ DataType::TinyText(_) => rw.write_col::<&str>(dt.into()),
                        ref dt @ DataType::Real(_, _) => match cs.coltype {
                            msql_srv::ColumnType::MYSQL_TYPE_DECIMAL => {
                                let f = dt.to_string();
                                rw.write_col(f)
                            }
                            msql_srv::ColumnType::MYSQL_TYPE_DOUBLE => {
                                let f: f64 = dt.into();
                                rw.write_col(f)
                            }
                            _ => unreachable!(),
                        },
                        DataType::Timestamp(ts) => rw.write_col(ts),
                    };
                    match written {
                        Ok(_) => (),
                        Err(e) => panic!("failed to write column: {:?}", e),
                    }
                };
                let (data, select_schema) = self.reader.execute_prepared_select(id, params)?;
                let mut rw = results.start(&select_schema.schema).unwrap();
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
                            write_column(&mut rw, &r[coli], &c);
                        }
                        rw.end_row()?;
                    }
                }
                rw.finish()
            }
            SqlQuery::Insert(ref _q) => match &mut self.writer {
                Writer::NoriaConnector(connector) => Backend::write_query_results(
                    connector.execute_prepared_insert(id, params),
                    results,
                ),
                Writer::MySqlConnector(connector) => {
                    Backend::write_query_results(connector.on_execute(id, params), results)
                }
            },
            SqlQuery::Update(ref _q) => match &mut self.writer {
                Writer::NoriaConnector(connector) => Backend::write_query_results(
                    connector.execute_prepared_update(id, params),
                    results,
                ),
                Writer::MySqlConnector(connector) => {
                    Backend::write_query_results(connector.on_execute(id, params), results)
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
        Ok(res?)
    }

    fn on_close(&mut self, _: u32) {}

    fn on_query(
        &mut self,
        query: &str,
        results: QueryResultWriter<W>,
    ) -> std::result::Result<(), Error> {
        let span = span!(Level::TRACE, "query", query);
        let _g = span.enter();

        let start = time::Instant::now();
        trace!("sanitize");
        let query = if self.sanitize {
            let q = utils::sanitize_query(query);
            trace!(%q, "sanitized");
            q
        } else {
            query.to_owned()
        };

        let query_lc = query.to_lowercase();

        if query_lc.starts_with("begin")
            || query_lc.starts_with("start transaction")
            || query_lc.starts_with("commit")
        {
            trace!("ignoring transaction bits");
            return Ok(results.completed(0, 0)?);
        }

        if query_lc.starts_with("show databases")
            || query_lc.starts_with("rollback")
            || query_lc.starts_with("alter table")
            || query_lc.starts_with("create index")
            || query_lc.starts_with("create unique index")
            || query_lc.starts_with("create fulltext index")
        {
            warn!("unsupported query; returning empty results");
            return Ok(results.completed(0, 0)?);
        }

        if query_lc.starts_with("show tables") {
            let cols = [Column {
                table: String::from(""),
                column: String::from("Tables"),
                coltype: ColumnType::MYSQL_TYPE_STRING,
                colflags: ColumnFlags::empty(),
            }];
            // TODO(malte): we could find out which tables exist via RPC to Soup and return them
            let writer = results.start(&cols)?;
            trace!("mocking show tables");
            return Ok(writer.finish()?);
        }

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
                    let mut writer = results.start(&cols[..])?;
                    for &(_, ref r) in columns {
                        writer.write_col(String::from(*r))?;
                    }
                    return Ok(writer.end_row()?);
                }
            }
        }

        if !self.parsed_query_cache.contains_key(&query) {
            //if we dont already have the parsed query, try to parse it
            trace!("Parsing query");
            match nom_sql::parse_query(&query) {
                Ok(mut q) => {
                    trace!("checking endpoint availability");
                    trace!("collapsing where-in clauses");
                    let mut use_params = Vec::new();
                    if let Some((_, p)) = rewrite::collapse_where_in(&mut q, true) {
                        use_params = p;
                    }
                    self.parsed_query_cache
                        .insert(query.to_owned(), (q.clone(), use_params.clone()));
                }

                Err(_e) => {
                    // if nom-sql rejects the query, there is no chance Noria will like it
                    error!(%query, "query can't be parsed: \"{}\"", query);
                    if self.permissive {
                        warn!("permissive flag enabled, so returning success despite query parse failure");
                        return Ok(results.completed(0, 0)?);
                    } else {
                        return self.handle_failure(&query, results).map_err(|e| IOError(e));
                    }
                }
            }
        }

        let (q, use_params) = match self.parsed_query_cache.get(&query) {
            Some((q, use_params)) => (q.clone(), use_params.clone()),
            None => unreachable!(),
        };

        trace!("delegating");
        let res = match q {
            nom_sql::SqlQuery::CreateTable(q) => match &mut self.writer {
                Writer::NoriaConnector(connector) => match connector.handle_create_table(q) {
                    Ok(()) => results.completed(0, 0),
                    Err(e) => results.error(e.error_kind(), e.message().as_bytes()),
                },
                Writer::MySqlConnector(connector) => {
                    Backend::write_query_results(connector.on_query(&q.to_string()), results)
                }
            },
            nom_sql::SqlQuery::CreateView(q) => match self.reader.handle_create_view(q) {
                Ok(()) => results.completed(0, 0),
                Err(e) => results.error(e.error_kind(), e.message().as_bytes()),
            },
            nom_sql::SqlQuery::Insert(q) => match &mut self.writer {
                Writer::NoriaConnector(connector) => {
                    Backend::write_query_results(connector.handle_insert(q), results)
                }
                Writer::MySqlConnector(connector) => {
                    Backend::write_query_results(connector.on_query(&q.to_string()), results)
                }
            },
            nom_sql::SqlQuery::Select(q) => {
                let (d, select_result) = self.reader.handle_select(q, use_params)?;
                let write_column = |rw: &mut RowWriter<W>, c: &DataType, cs: &msql_srv::Column| {
                    let written = match *c {
                        DataType::None => rw.write_col(None::<i32>),
                        // NOTE(malte): the code repetition here is unfortunate, but it's hard to factor
                        // this out into a helper since i has a different time depending on the DataType
                        // variant.
                        DataType::Int(i) => {
                            if cs.colflags.contains(ColumnFlags::UNSIGNED_FLAG) {
                                rw.write_col(i as usize)
                            } else {
                                rw.write_col(i as isize)
                            }
                        }
                        DataType::BigInt(i) => {
                            if cs.colflags.contains(ColumnFlags::UNSIGNED_FLAG) {
                                rw.write_col(i as usize)
                            } else {
                                rw.write_col(i as isize)
                            }
                        }
                        DataType::UnsignedInt(i) => {
                            if cs.colflags.contains(ColumnFlags::UNSIGNED_FLAG) {
                                rw.write_col(i as usize)
                            } else {
                                rw.write_col(i as isize)
                            }
                        }
                        DataType::UnsignedBigInt(i) => {
                            if cs.colflags.contains(ColumnFlags::UNSIGNED_FLAG) {
                                rw.write_col(i as usize)
                            } else {
                                rw.write_col(i as isize)
                            }
                        }
                        DataType::Text(ref t) => rw.write_col(t.to_str().unwrap()),
                        ref dt @ DataType::TinyText(_) => rw.write_col::<&str>(dt.into()),
                        ref dt @ DataType::Real(_, _) => match cs.coltype {
                            msql_srv::ColumnType::MYSQL_TYPE_DECIMAL => {
                                let f = dt.to_string();
                                rw.write_col(f)
                            }
                            msql_srv::ColumnType::MYSQL_TYPE_DOUBLE => {
                                let f: f64 = dt.into();
                                rw.write_col(f)
                            }
                            _ => unreachable!(),
                        },
                        DataType::Timestamp(ts) => rw.write_col(ts),
                    };
                    match written {
                        Ok(_) => (),
                        Err(e) => panic!("failed to write column: {:?}", e),
                    }
                };
                let mut rw = results.start(&select_result.schema)?;
                for resultsets in d {
                    for r in resultsets {
                        let mut r: Vec<_> = r.into();
                        if select_result.use_bogo {
                            // drop bogokey
                            r.pop();
                        }

                        for c in &select_result.schema {
                            let coli = select_result
                                .columns
                                .iter()
                                .position(|f| f == &c.column)
                                .unwrap_or_else(|| {
                                    panic!(
                                        "tried to emit column {:?} not in getter with schema {:?}",
                                        c.column, &select_result.columns
                                    );
                                });
                            write_column(&mut rw, &r[coli], &c);
                        }
                        rw.end_row()?;
                    }
                }
                rw.finish()
            }
            nom_sql::SqlQuery::Set(_) => results.error(
                msql_srv::ErrorKind::ER_NOT_SUPPORTED_YET,
                "unsupported set query".as_bytes(),
            ),
            nom_sql::SqlQuery::Update(q) => match &mut self.writer {
                Writer::NoriaConnector(connector) => {
                    Backend::write_query_results(connector.handle_update(q), results)
                }
                Writer::MySqlConnector(connector) => {
                    Backend::write_query_results(connector.on_query(&q.to_string()), results)
                }
            },
            nom_sql::SqlQuery::Delete(q) => match &mut self.writer {
                Writer::NoriaConnector(connector) => match connector.handle_delete(q) {
                    Ok(row_count) => results.completed(row_count, 0),
                    Err(e) => results.error(e.error_kind(), e.message().as_bytes()),
                },
                Writer::MySqlConnector(connector) => {
                    Backend::write_query_results(connector.on_query(&q.to_string()), results)
                }
            },
            nom_sql::SqlQuery::DropTable(q) => match &mut self.writer {
                Writer::MySqlConnector(connector) => {
                    Backend::write_query_results(connector.on_query(&q.to_string()), results)
                }
                _ => {
                    warn!("ignoring drop table");
                    unimplemented!()
                }
            },
            _ => match &mut self.writer {
                Writer::MySqlConnector(connector) => {
                    Backend::write_query_results(connector.on_query(&q.to_string()), results)
                }
                _ => {
                    error!("unsupported query");
                    results.error(
                        msql_srv::ErrorKind::ER_NOT_SUPPORTED_YET,
                        "unsupported query".as_bytes(),
                    )
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
        Ok(res?)
    }

    fn password_for_username(&self, username: &[u8]) -> Option<Vec<u8>> {
        String::from_utf8(username.to_vec())
            .ok()
            .and_then(|un| self.users.get(&un))
            .cloned()
            .map(String::into_bytes)
    }
}
