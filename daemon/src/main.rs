extern crate bincode;
extern crate channel;
#[macro_use]
extern crate clap;
extern crate hostname;
extern crate mio;
#[macro_use]
extern crate serde_derive;
extern crate serde;
#[macro_use]
extern crate slog;
extern crate slog_term;

mod controller;
mod protocol;
mod worker;

use std::thread;
use std::time::Duration;

struct Config {
    hostname: String,
    addr: String,
    port: u16,
    controller: Option<String>,
}

fn logger_pls() -> slog::Logger {
    use slog::Drain;
    use slog::Logger;
    use slog_term::term_full;
    use std::sync::Mutex;
    Logger::root(Mutex::new(term_full()).fuse(), o!())
}

fn parse_args() -> Config {
    use clap::{Arg, App};

    let matches = App::new("gulaschkanone")
        .version("0.0.1")
        .about("Delivers scalable Soup distribution.")
        .arg(
            Arg::with_name("listen_addr")
                .short("l")
                .default_value("0.0.0.0")
                .help("Address to listen on."),
        )
        .arg(
            Arg::with_name("port")
                .short("p")
                .default_value("9999")
                .help("Port to listen on."),
        )
        .arg(
            Arg::with_name("mode")
                .short("m")
                .required(true)
                .possible_values(&["controller", "worker"])
                .default_value("worker")
                .help("Operational mode for this instance."),
        )
        .arg(
            Arg::with_name("controller")
                .short("c")
                .required_if("mode", "worker")
                .takes_value(true)
                .help("Network location of the controller to connect to."),
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
    }
}

fn main() {
    let log = logger_pls();
    let config = parse_args();

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
            let mut controller = controller::Controller::new(&config.addr, config.port, log);
            controller.listen()
        }
        Some(c) => {
            let mut worker = worker::Worker::new(&c, &config.addr, config.port, log.clone());
            loop {
                match worker.connect() {
                    Ok(_) => {
                        // enter worker loop, wait for instructions
                        worker.handle()
                    }
                    Err(e) => error!(log, "failed to connect to controller: {:?}", e),
                }

                thread::sleep(Duration::from_millis(1000));
            }
        }
    }
}
