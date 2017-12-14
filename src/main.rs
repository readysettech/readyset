extern crate distributary;
extern crate msql_srv;
#[macro_use]
extern crate slog;

use distributary::{ControllerHandle, Mutator, RemoteGetter, ZookeeperAuthority};
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
}

impl<W: io::Write> MysqlShim<W> for SoupBackend {
    fn param_info(&self, stmt: u32) -> &[Column] {
        &[]
    }

    fn on_prepare(&mut self, query: &str, info: StatementMetaWriter<W>) -> io::Result<()> {
        println!("prepare: {}", query);
        info.reply(42, &[], &[])
    }

    fn on_execute(
        &mut self,
        id: u32,
        _: Vec<Value>,
        results: QueryResultWriter<W>,
    ) -> io::Result<()> {
        println!("exec: {}", id);
        results.completed(0, 0)
    }

    fn on_close(&mut self, _: u32) {}

    fn on_query(&mut self, query: &str, results: QueryResultWriter<W>) -> io::Result<()> {
        debug!(self.log, "query: {}", query);

        self.soup.install_recipe(format!("{};", query));

        results.completed(0, 0)
    }
}

fn main() {
    let listener = net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    println!("listening on port {}", port);

    if let Ok((s, _)) = listener.accept() {
        MysqlIntermediary::run_on_tcp(SoupBackend::new(), s).unwrap();
    }
}
