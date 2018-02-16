extern crate distributary;
extern crate msql_srv;
extern crate nom_sql;
#[macro_use]
extern crate slog;

use distributary::{ControllerHandle, Mutator, RemoteGetter, RpcError, ZookeeperAuthority};
use msql_srv::*;
use std::{io, net};
use std::collections::BTreeMap;

pub struct SoupBackend {
    soup: ControllerHandle<ZookeeperAuthority>,
    log: slog::Logger,

    recipe: String,
    inputs: BTreeMap<String, Mutator>,
    outputs: BTreeMap<String, RemoteGetter>,
}

impl SoupBackend {
    fn new() -> Self {
        let log = distributary::logger_pls();

        let zk_auth = ZookeeperAuthority::new("127.0.0.1:2181");

        debug!(log, "Connecting to Soup...");
        let mut ch = ControllerHandle::new(zk_auth);

        debug!(log, "Connected!");

        let inputs = ch.inputs()
            .into_iter()
            .map(|(n, i)| (n, ch.get_mutator(i).unwrap()))
            .collect::<BTreeMap<String, Mutator>>();
        let outputs = ch.outputs()
            .into_iter()
            .map(|(n, o)| (n, ch.get_getter(o).unwrap()))
            .collect::<BTreeMap<String, RemoteGetter>>();

        SoupBackend {
            soup: ch,
            log: log,

            recipe: String::new(),
            inputs: inputs,
            outputs: outputs,
        }
    }

    fn handle_create_table<W: io::Write>(
        &mut self,
        q: &str,
        results: QueryResultWriter<W>,
    ) -> io::Result<()> {
        match self.soup.install_recipe(format!("{}", q)) {
            Ok(_) => {
                // no rows to return
                results.completed(0, 0)
            }
            Err(e) => {
                // XXX(malte): implement Error for RpcError
                let msg = match e {
                    RpcError::Other(msg) => msg,
                };
                Err(io::Error::new(io::ErrorKind::Other, msg))
            }
        }
    }

    fn handle_insert(&mut self, q: nom_sql::InsertStatement) -> io::Result<()> {
        unimplemented!()
    }

    fn handle_select(&mut self, q: nom_sql::SelectStatement) -> io::Result<()> {
        unimplemented!()
    }

    fn handle_set<W: io::Write>(
        &mut self,
        q: nom_sql::SetStatement,
        results: QueryResultWriter<W>,
    ) -> io::Result<()> {
        // ignore
        results.completed(0, 0)
    }
}

impl<W: io::Write> MysqlShim<W> for SoupBackend {
    fn on_prepare(&mut self, query: &str, info: StatementMetaWriter<W>) -> io::Result<()> {
        warn!(self.log, "prepare: {}", query);
        info.reply(42, &[], &[])
    }

    fn on_execute(
        &mut self,
        id: u32,
        _: ParamParser,
        results: QueryResultWriter<W>,
    ) -> io::Result<()> {
        warn!(self.log, "exec: {}", id);
        results.completed(0, 0)
    }

    fn on_close(&mut self, _: u32) {}

    fn on_query(&mut self, query: &str, results: QueryResultWriter<W>) -> io::Result<()> {
        debug!(self.log, "query: {}", query);

        if query.to_lowercase().contains("show tables") || query.to_lowercase().contains("rollback")
        {
            return results.completed(0, 0);
        }

        match nom_sql::parse_query(query) {
            Ok(q) => match q {
                nom_sql::SqlQuery::CreateTable(q) => self.handle_create_table(query, results),
                nom_sql::SqlQuery::Insert(q) => self.handle_insert(q),
                nom_sql::SqlQuery::Select(q) => self.handle_select(q),
                nom_sql::SqlQuery::Set(q) => self.handle_set(q, results),
                _ => {
                    return results.error(
                        msql_srv::ErrorKind::ER_NOT_SUPPORTED_YET,
                        "unsupported query".as_bytes(),
                    )
                }
            },
            Err(e) => {
                // if nom-sql rejects the query, there is no chance Soup will like it
                crit!(self.log, "query can't be parsed: \"{}\"", query);
                return results.error(msql_srv::ErrorKind::ER_PARSE_ERROR, e.as_bytes());
            }
        }
    }
}

fn main() {
    let listener = net::TcpListener::bind("127.0.0.1:3306").unwrap();
    let port = listener.local_addr().unwrap().port();
    println!("listening on port {}", port);

    loop {
        if let Ok((s, _)) = listener.accept() {
            MysqlIntermediary::run_on_tcp(SoupBackend::new(), s).unwrap();
        }
    }
}
