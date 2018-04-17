#![feature(box_syntax, box_patterns)]
#![feature(nll)]
#![feature(try_from)]

extern crate arccstr;
extern crate chrono;
#[macro_use]
extern crate clap;
extern crate distributary;
extern crate msql_srv;
extern crate nom_sql;

#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate slog;

extern crate regex;

mod convert;
mod rewrite;
mod schema;
mod soup_backend;
mod utils;

use msql_srv::MysqlIntermediary;
use nom_sql::{CreateTableStatement, SelectStatement};
use std::collections::HashMap;
use std::io::{BufReader, BufWriter};
use std::net;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, RwLock};
use std::thread;

use soup_backend::SoupBackend;

fn main() {
    use clap::{App, Arg};

    let matches = App::new("distributary-mysql")
        .version("0.0.1")
        .about("MySQL shim for Soup.")
        .arg(
            Arg::with_name("deployment")
                .long("deployment")
                .takes_value(true)
                .required(true)
                .help("Soup deployment ID to attach to."),
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
    let port = value_t_or_exit!(matches, "port", u16);
    let slowlog = matches.is_present("slowlog");
    let zk_addr = matches.value_of("zk_addr").unwrap().to_owned();
    let sanitize = !matches.is_present("no-sanitize");
    let static_responses = !matches.is_present("no-static-responses");

    let listener = net::TcpListener::bind(format!("127.0.0.1:{}", port)).unwrap();

    let log = distributary::logger_pls();

    info!(log, "listening on port {}", port);

    let query_counter = Arc::new(AtomicUsize::new(0));
    let schemas: Arc<RwLock<HashMap<String, CreateTableStatement>>> = Arc::default();
    let auto_increments: Arc<RwLock<HashMap<String, AtomicUsize>>> = Arc::default();
    let query_cache: Arc<RwLock<HashMap<SelectStatement, String>>> = Arc::default();

    let mut threads = Vec::new();
    let mut i = 0;
    while let Ok((s, _)) = listener.accept() {
        s.set_nodelay(true).unwrap();

        let builder = thread::Builder::new().name(format!("handler{}", i));

        let (schemas, auto_increments, query_cache, query_counter, log) = (
            schemas.clone(),
            auto_increments.clone(),
            query_cache.clone(),
            query_counter.clone(),
            log.clone(),
        );

        let zk_addr = zk_addr.clone();
        let deployment = deployment.clone();

        let jh = builder
            .spawn(move || {
                let soup = SoupBackend::new(
                    &zk_addr,
                    &deployment,
                    schemas,
                    auto_increments,
                    query_cache,
                    query_counter,
                    slowlog,
                    static_responses,
                    sanitize,
                    log,
                );
                let rs = s.try_clone().unwrap();
                MysqlIntermediary::run_on(soup, BufReader::new(rs), BufWriter::new(s)).unwrap();
            })
            .unwrap();
        threads.push(jh);
        i += 1;
    }

    for t in threads.drain(..) {
        t.join().unwrap();
    }
}
