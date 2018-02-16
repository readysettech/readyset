extern crate distributary;
extern crate msql_srv;
extern crate nom_sql;
#[macro_use]
extern crate slog;

mod soup_backend;

use msql_srv::MysqlIntermediary;
use std::net;
use soup_backend::SoupBackend;

fn main() {
    let listener = net::TcpListener::bind("127.0.0.1:3306").unwrap();
    let port = listener.local_addr().unwrap().port();

    let log = distributary::logger_pls();

    info!(log, "listening on port {}", port);

    while let Ok((s, _)) = listener.accept() {
        // XXX: shouldn't be making a new soup backend for every connection
        let soup = SoupBackend::new(log.clone());
        MysqlIntermediary::run_on_tcp(soup, s).unwrap();
    }
}
