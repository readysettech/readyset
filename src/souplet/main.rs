#[macro_use]
extern crate clap;
extern crate consensus;
extern crate distributary;
extern crate hostname;
extern crate serde_json;
#[macro_use]
extern crate slog;
extern crate slog_term;

use consensus::ZookeeperAuthority;
use distributary::Souplet;

use clap::{App, Arg};
use slog::Drain;
use slog::Logger;
use slog_term::term_full;
use std::sync::Mutex;
use std::time::Duration;

fn main() {
    let matches = App::new("gulaschkanone")
        .version("0.0.1")
        .about("Delivers scalable Soup distribution.")
        .arg(
            Arg::with_name("zookeeper")
                .short("z")
                .long("zookeeper")
                .takes_value(true)
                .default_value("127.0.0.1:2181")
                .help("Zookeeper connection info."),
        )
        .arg(
            Arg::with_name("workers")
                .short("w")
                .long("workers")
                .takes_value(true)
                .default_value("1")
                .help("Number of worker threads to spin up"),
        )
        .arg(
            Arg::with_name("readers")
                .short("r")
                .long("readers")
                .takes_value(true)
                .default_value("1")
                .help("Number of reader threads to spin up"),
        )
        .arg(
            Arg::with_name("heartbeat_frequency")
                .long("heartbeat-frequency")
                .takes_value(true)
                .value_name("N")
                .default_value("1000")
                .help("Heartbeat every N milliseconds"),
        )
        .arg(
            Arg::with_name("listen_addr")
                .short("l")
                .long("listen")
                .default_value("127.0.0.1")
                .value_name("HOST-OR-IP")
                .help("Address to listen on."),
        )
        .get_matches();

    let log = Logger::root(Mutex::new(term_full()).fuse(), o!());
    let workers = value_t_or_exit!(matches, "workers", usize);
    let readers = value_t_or_exit!(matches, "readers", usize);
    let listen_addr = matches.value_of("listen_addr").unwrap().parse().unwrap();
    let heartbeat_freq = value_t_or_exit!(matches, "heartbeat_frequency", u64);
    let zookeeper_addr = matches.value_of("zookeeper").unwrap();

    Souplet::new(
        ZookeeperAuthority::new(&zookeeper_addr),
        listen_addr,
        Duration::from_millis(heartbeat_freq),
        workers,
        readers,
        log,
    ).run();
}
