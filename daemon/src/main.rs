extern crate bincode;
extern crate channel;
#[macro_use]
extern crate clap;
extern crate distributary;
extern crate hostname;
extern crate mio;
#[macro_use]
extern crate rustful;
#[macro_use]
extern crate serde_json;
extern crate serde;
#[macro_use]
extern crate slog;
extern crate slog_term;

mod api;
mod controller;
mod worker;

use slog::Logger;
use std::thread;
use std::time::Duration;

struct Config {
    hostname: String,
    addr: String,
    port: u16,
    controller: Option<String>,
    heartbeat_freq: u64,
    healthcheck_freq: u64,
}

fn logger_pls() -> slog::Logger {
    use slog::Drain;
    use slog::Logger;
    use slog_term::term_full;
    use std::sync::Mutex;
    Logger::root(Mutex::new(term_full()).fuse(), o!())
}

fn parse_args(log: &Logger) -> Config {
    use clap::{Arg, App};

    let matches = App::new("gulaschkanone")
        .version("0.0.1")
        .about("Delivers scalable Soup distribution.")
        .arg(
            Arg::with_name("controller")
                .short("c")
                .long("controller")
                .required_if("mode", "worker")
                .takes_value(true)
                .value_name("HOST-OR-IP:PORT")
                .help("Network location of the controller to connect to."),
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
            Arg::with_name("healthcheck_frequency")
                .long("healthcheck-frequency")
                .takes_value(true)
                .value_name("N")
                .default_value("10000")
                .help("Check worker health every N milliseconds"),
        )
        .arg(
            Arg::with_name("listen_addr")
                .short("l")
                .long("listen")
                .default_value("0.0.0.0")
                .value_name("HOST-OR-IP")
                .help("Address to listen on."),
        )
        .arg(
            Arg::with_name("mode")
                .short("m")
                .long("mode")
                .required(true)
                .possible_values(&["controller", "worker"])
                .default_value("worker")
                .value_name("MODE")
                .help("Operational mode for this instance."),
        )
        .arg(
            Arg::with_name("port")
                .short("p")
                .long("port")
                .default_value("9999")
                .value_name("PORT")
                .help("Port to listen on."),
        )
        .get_matches();

    Config {
        hostname: match hostname::get_hostname() {
            Some(hn) => hn,
            None => "unknown".to_string(),
        },
        addr: String::from(matches.value_of("listen_addr").unwrap()),
        port: value_t_or_exit!(matches, "port", u16),
        controller: match matches.value_of("mode") {
            Some("controller") => None,
            Some("worker") => Some(String::from(matches.value_of("controller").unwrap())),
            _ => unreachable!(),
        },
        heartbeat_freq: value_t_or_exit!(matches, "heartbeat_frequency", u64),
        healthcheck_freq: value_t_or_exit!(matches, "healthcheck_frequency", u64),
    }
}

fn main() {
    let log = logger_pls();
    let config = parse_args(&log);

    let mode = if config.controller.is_some() {
        "worker"
    } else {
        "controller"
    };
    info!(
        log,
        "{} starting on {}:{}",
        mode,
        config.hostname,
        config.port
    );

    match config.controller {
        None => {
            let mut controller = controller::Controller::new(
                &config.addr,
                config.port,
                Duration::from_millis(config.heartbeat_freq),
                Duration::from_millis(config.healthcheck_freq),
                log,
            );

            controller.listen()
        }
        Some(c) => {
            let mut worker = worker::Worker::new(
                &c,
                &config.addr,
                config.port,
                Duration::from_millis(config.heartbeat_freq),
                log.clone(),
            );
            loop {
                match worker.connect() {
                    Ok(_) => {
                        // enter worker loop, wait for instructions
                        worker.handle()
                    }
                    Err(e) => error!(log, "failed to connect to controller: {:?}", e),
                }

                // wait for a second in between connection attempts
                thread::sleep(Duration::from_millis(1000));
            }
        }
    }
}
