#![feature(box_syntax, box_patterns)]
#![feature(nll)]
#![feature(allow_fail)]

extern crate arccstr;
extern crate chrono;
#[macro_use]
extern crate clap;
extern crate noria;
#[macro_use]
extern crate failure;
extern crate msql_srv;
extern crate nom_sql;

#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate slog;
extern crate regex;
extern crate slog_term;
extern crate tokio;

mod backend;
mod convert;
mod referred_tables;
mod rewrite;
mod schema;
mod utils;

use msql_srv::MysqlIntermediary;
use nom_sql::SelectStatement;
use noria::{SyncControllerHandle, ZookeeperAuthority};
use std::collections::HashMap;
use std::io::{self, BufReader, BufWriter};
use std::net;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, RwLock};
use std::thread;
use tokio::prelude::*;

use backend::NoriaBackend;

// Just give me a damn terminal logger
// Duplicated from distributary, as the API subcrate doesn't export it.
pub fn logger_pls() -> slog::Logger {
    use slog::Drain;
    use slog::Logger;
    use slog_term::term_full;
    use std::sync::Mutex;
    Logger::root(Mutex::new(term_full()).fuse(), o!())
}

fn main() {
    use clap::{App, Arg};

    let matches = App::new("distributary-mysql")
        .version("0.0.1")
        .about("MySQL shim for Noria.")
        .arg(
            Arg::with_name("deployment")
                .long("deployment")
                .takes_value(true)
                .required(true)
                .help("Noria deployment ID to attach to."),
        )
        .arg(
            Arg::with_name("zk_addr")
                .long("zookeeper-address")
                .short("z")
                .default_value("127.0.0.1:2181")
                .help("IP:PORT for Zookeeper."),
        )
        .arg(
            Arg::with_name("port")
                .long("port")
                .short("p")
                .default_value("3306")
                .takes_value(true)
                .help("Port to listen on."),
        )
        .arg(
            Arg::with_name("slowlog")
                .long("log-slow")
                .help("Log slow queries (> 5ms)"),
        )
        .arg(
            Arg::with_name("no-static-responses")
                .long("no-static-responses")
                .takes_value(false)
                .help("Disable checking for queries requiring static responses. Improves latency."),
        )
        .arg(
            Arg::with_name("no-sanitize")
                .long("no-sanitize")
                .takes_value(false)
                .help("Disable query sanitization. Improves latency."),
        )
        .arg(Arg::with_name("verbose").long("verbose").short("v"))
        .get_matches();

    let deployment = matches.value_of("deployment").unwrap().to_owned();
    assert!(!deployment.contains("-"));

    let port = value_t_or_exit!(matches, "port", u16);
    let slowlog = matches.is_present("slowlog");
    let zk_addr = matches.value_of("zk_addr").unwrap().to_owned();
    let sanitize = !matches.is_present("no-sanitize");
    let static_responses = !matches.is_present("no-static-responses");

    let listener = net::TcpListener::bind(format!("127.0.0.1:{}", port)).unwrap();

    let log = logger_pls();

    info!(log, "listening on port {}", port);

    let auto_increments: Arc<RwLock<HashMap<String, AtomicUsize>>> = Arc::default();
    let query_cache: Arc<RwLock<HashMap<SelectStatement, String>>> = Arc::default();

    let mut zk_auth = ZookeeperAuthority::new(&format!("{}/{}", zk_addr, deployment)).unwrap();
    zk_auth.log_with(log.clone());

    debug!(log, "Connecting to Noria...",);
    let rt = tokio::runtime::Runtime::new().unwrap();
    let ch = SyncControllerHandle::new(zk_auth, rt.executor()).unwrap();
    debug!(log, "Connected!");

    let mut threads = Vec::new();
    let mut i = 0;
    while let Ok((s, _)) = listener.accept() {
        s.set_nodelay(true).unwrap();

        let builder = thread::Builder::new().name(format!("conn-{}", i));

        let (auto_increments, query_cache, log) =
            (auto_increments.clone(), query_cache.clone(), log.clone());

        let ch = ch.clone();

        let jh = builder
            .spawn(move || {
                let b = NoriaBackend::new(
                    ch,
                    auto_increments,
                    query_cache,
                    slowlog,
                    static_responses,
                    sanitize,
                    log,
                );
                let rs = s.try_clone().unwrap();
                if let Err(e) = MysqlIntermediary::run_on(b, BufReader::new(rs), BufWriter::new(s))
                {
                    match e.kind() {
                        io::ErrorKind::ConnectionReset | io::ErrorKind::BrokenPipe => {}
                        _ => {
                            panic!("{:?}", e);
                        }
                    }
                }
            })
            .unwrap();
        threads.push(jh);
        i += 1;
    }

    for t in threads.drain(..) {
        t.join()
            .map_err(|e| e.downcast::<io::Error>().unwrap())
            .unwrap();
    }

    rt.shutdown_on_idle().wait().unwrap();
}
