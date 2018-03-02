#[macro_use]
extern crate clap;
extern crate distributary;
extern crate msql_srv;
extern crate nom_sql;
extern crate regex;

#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate slog;

mod soup_backend;

use msql_srv::MysqlIntermediary;
use std::net;
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

    while let Ok((s, _)) = listener.accept() {
        // XXX: shouldn't be making a new soup backend for every connection
        let soup = SoupBackend::new(zk_addr, deployment, log.clone());
        MysqlIntermediary::run_on_tcp(soup, s).unwrap();
    }
}
