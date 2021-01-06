use noria::DataType;

use msql_srv::{self, *};
use nom_sql::{self, SqlQuery};

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::io;
use std::time;
use tracing::Level;

use crate::convert::ToDataType;
use crate::rewrite;
use crate::utils;

pub mod noria_connector;
mod reader;
mod writer;

use reader::Reader;
use writer::Writer;

#[derive(Clone)]
pub enum PreparedStatement {
    /// Query name, Query, result schema, optional parameter rewrite map
    /// TODO(eta): make these into actual struct-y things?
    Select(
        String,
        nom_sql::SelectStatement,
        Vec<msql_srv::Column>,
        Option<(usize, usize)>,
    ),
    Insert(nom_sql::InsertStatement),
    Update(nom_sql::UpdateStatement),
}

impl fmt::Debug for PreparedStatement {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match *self {
            PreparedStatement::Select(ref qname, ref s, _, _) => write!(f, "{}: {}", qname, s),
            PreparedStatement::Insert(ref s) => write!(f, "{}", s),
            PreparedStatement::Update(ref s) => write!(f, "{}", s),
        }
    }
}

pub struct Backend<W> {
    //if false, queries are not sanitized which improves latency.
    sanitize: bool,
    // a cache of all previously parsed queries
    parsed_query_cache: HashMap<String, (SqlQuery, Vec<nom_sql::Literal>)>,
    // all queries previously prepared, mapped by their ID
    prepared_queries: HashMap<u32, PreparedStatement>,
    prepared_count: u32,
    static_responses: bool,
    writer: Box<dyn Writer<W>>,
    reader: Box<dyn Reader<W>>,
    slowlog: bool,
    permissive: bool,
}

impl<W: io::Write> Backend<W> {
    pub fn new(
        sanitize: bool,
        static_responses: bool,
        reader: Box<dyn Reader<W>>,
        writer: Box<dyn Writer<W>>,
        slowlog: bool,
        permissive: bool,
    ) -> Self {
        let parsed_query_cache = HashMap::new();
        let prepared_queries = HashMap::new();
        let prepared_count = 0;
        Backend {
            sanitize: sanitize,
            parsed_query_cache: parsed_query_cache,
            prepared_queries: prepared_queries,
            prepared_count: prepared_count,
            static_responses: static_responses,
            reader: reader,
            writer: writer,
            slowlog,
            permissive,
        }
    }
}

impl<W: io::Write> MysqlShim<W> for Backend<W> {
    type Error = io::Error;

    fn on_prepare(&mut self, query: &str, info: StatementMetaWriter<W>) -> io::Result<()> {
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

        trace!("parse");
        let sql_q = match self.parsed_query_cache.get(&query) {
            None => match nom_sql::parse_query(&query) {
                Ok(sql_q) => {
                    self.parsed_query_cache
                        .insert(query.to_owned(), (sql_q.clone(), vec![]));

                    sql_q
                }
                Err(e) => {
                    // if nom-sql rejects the query, there is no chance Noria will like it
                    error!(%query, "query can't be parsed: \"{}\"", query);
                    return info.error(msql_srv::ErrorKind::ER_PARSE_ERROR, e.as_bytes());
                }
            },
            Some((q, _)) => q.clone(),
        };

        trace!("delegate");
        let prepared_statement = match sql_q {
            nom_sql::SqlQuery::Select(_) => {
                self.reader.prepare_select(sql_q, info, self.prepared_count)
            }
            nom_sql::SqlQuery::Insert(_) => {
                self.writer.prepare_insert(sql_q, info, self.prepared_count)
            }
            nom_sql::SqlQuery::Update(_) => {
                self.writer.prepare_update(sql_q, info, self.prepared_count)
            }
            _ => {
                // Noria only supports prepared SELECT statements at the moment
                error!(%query, "unsupported query for prepared statement");
                return info.error(
                    msql_srv::ErrorKind::ER_NOT_SUPPORTED_YET,
                    "unsupported query".as_bytes(),
                );
            }
        };

        // register a new prepared statement
        self.prepared_queries
            .insert(self.prepared_count, prepared_statement.unwrap());
        Ok(())
    }

    fn on_execute(
        &mut self,
        id: u32,
        params: ParamParser,
        results: QueryResultWriter<W>,
    ) -> io::Result<()> {
        let span = span!(Level::TRACE, "execute", id);
        let _g = span.enter();

        let start = time::Instant::now();

        // TODO(malte): unfortunate clone here, but we can't call execute_select(&mut self) if we
        // have self.prepared borrowed
        let prep: PreparedStatement = {
            match self.prepared_queries.get(&id) {
                Some(e) => e.clone(),
                None => {
                    return results.error(
                        msql_srv::ErrorKind::ER_UNKNOWN_ERROR,
                        "non-existent statement".as_bytes(),
                    )
                }
            }
        };

        trace!("delegate");
        let res = match prep {
            PreparedStatement::Select(ref qname, ref q, ref schema, ref rewritten) => {
                trace!("apply where-in rewrites");
                let key = match rewritten {
                    Some((first_rewritten, nrewritten)) => {
                        // this is a little tricky
                        // the user is giving us some params [a, b, c, d]
                        // for the query WHERE x = ? AND y IN (?, ?) AND z = ?
                        // that we rewrote to WHERE x = ? AND y = ? AND z = ?
                        // so we need to turn that into the keys:
                        // [[a, b, d], [a, c, d]]
                        let params: Vec<_> = params
                            .into_iter()
                            .map(|pv| pv.value.to_datatype())
                            .collect::<Vec<_>>();
                        (0..*nrewritten)
                            .map(|poffset| {
                                params
                                    .iter()
                                    .take(*first_rewritten)
                                    .chain(params.iter().skip(first_rewritten + poffset).take(1))
                                    .chain(params.iter().skip(first_rewritten + nrewritten))
                                    .cloned()
                                    .collect()
                            })
                            .collect()
                    }
                    None => vec![params
                        .into_iter()
                        .map(|pv| pv.value.to_datatype())
                        .collect::<Vec<_>>()],
                };

                self.reader.execute_select(&qname, q, key, schema, results)
            }
            PreparedStatement::Insert(ref q) => {
                let values: Vec<DataType> = params
                    .into_iter()
                    .map(|pv| pv.value.to_datatype())
                    .collect();

                self.writer.execute_insert(&q, values, results)
            }
            PreparedStatement::Update(ref q) => self.writer.execute_update(&q, params, results),
        };

        if self.slowlog {
            let took = start.elapsed();
            if took.as_secs() > 0 || took.subsec_nanos() > 5_000_000 {
                let query: &dyn std::fmt::Display = match prep {
                    PreparedStatement::Select(_, ref q, ..) => q,
                    PreparedStatement::Insert(ref q) => q,
                    PreparedStatement::Update(ref q) => q,
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

    fn on_close(&mut self, _: u32) {}

    fn on_query(&mut self, query: &str, results: QueryResultWriter<W>) -> io::Result<()> {
        let span = span!(Level::TRACE, "query", query);
        let _g = span.enter();

        let start = time::Instant::now();
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
            return results.completed(0, 0);
        }

        if query_lc.starts_with("show databases")
            || query_lc.starts_with("rollback")
            || query_lc.starts_with("alter table")
            || query_lc.starts_with("create index")
            || query_lc.starts_with("create unique index")
            || query_lc.starts_with("create fulltext index")
        {
            warn!("unsupported query; returning empty results");
            return results.completed(0, 0);
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
            return writer.finish();
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
                    return writer.end_row();
                }
            }
        }

        trace!("analyzing query");
        let (q, use_params) = match self.parsed_query_cache.get(&query) {
            None => {
                trace!("parsing query");
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

                        (q, use_params)
                    }
                    Err(e) => {
                        // if nom-sql rejects the query, there is no chance Noria will like it
                        error!(%query, "query can't be parsed: \"{}\"", query);
                        if self.permissive {
                            warn!("permissive flag enabled, so returning success despite query parse failure");
                            return results.completed(0, 0);
                        } else {
                            return results
                                .error(msql_srv::ErrorKind::ER_PARSE_ERROR, e.as_bytes());
                        }
                    }
                }
            }
            Some((q, use_params)) => (q.clone(), use_params.clone()),
        };

        trace!("delegate");
        let res = match q {
            nom_sql::SqlQuery::CreateTable(q) => self.writer.handle_create_table(q, results),
            nom_sql::SqlQuery::CreateView(q) => self.writer.handle_create_view(q, results),
            nom_sql::SqlQuery::Insert(q) => self.writer.handle_insert(q, results),
            nom_sql::SqlQuery::Select(q) => self.reader.handle_select(q, use_params, results),
            nom_sql::SqlQuery::Set(q) => self.writer.handle_set(q, results),
            nom_sql::SqlQuery::Update(q) => self.writer.handle_update(q, results),
            nom_sql::SqlQuery::Delete(q) => self.writer.handle_delete(q, results),
            nom_sql::SqlQuery::DropTable(_) => {
                warn!("ignoring drop table");
                return results.completed(0, 0);
            }
            _ => {
                error!("unsupported query");
                return results.error(
                    msql_srv::ErrorKind::ER_NOT_SUPPORTED_YET,
                    "unsupported query".as_bytes(),
                );
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
}
