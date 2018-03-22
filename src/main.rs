#![feature(box_syntax, box_patterns)]

#[macro_use]
extern crate clap;
extern crate distributary;
extern crate msql_srv;
extern crate nom_sql;

#[macro_use]
extern crate slog;
#[macro_use]
extern crate lazy_static;

extern crate regex;

mod soup_backend;
mod utils;

use msql_srv::MysqlIntermediary;
use nom_sql::ColumnSpecification;
use std::collections::HashMap;
use std::net;
use std::sync::{Arc, Mutex};
use std::sync::atomic::AtomicUsize;

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
        .arg(Arg::with_name("verbose").long("verbose").short("v"))
        .get_matches();

    let deployment = matches.value_of("deployment").unwrap();
    let port = value_t_or_exit!(matches, "port", u16);
    let zk_addr = matches.value_of("zk_addr").unwrap();

    let listener = net::TcpListener::bind(format!("127.0.0.1:{}", port)).unwrap();

    let log = distributary::logger_pls();

    info!(log, "listening on port {}", port);

    let query_counter = Arc::new(AtomicUsize::new(0));
    let schemas: Arc<Mutex<HashMap<String, Vec<ColumnSpecification>>>> =
        Arc::new(Mutex::new(HashMap::default()));
    let auto_increments: Arc<Mutex<HashMap<String, u64>>> =
        Arc::new(Mutex::new(HashMap::default()));

    while let Ok((s, _)) = listener.accept() {
        let soup = SoupBackend::new(
            zk_addr,
            deployment,
            schemas.clone(),
            auto_increments.clone(),
            query_counter.clone(),
            log.clone(),
        );
        MysqlIntermediary::run_on_tcp(soup, s).unwrap();
    }
}
