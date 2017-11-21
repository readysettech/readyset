extern crate clap;
extern crate distributary;
extern crate consensus;

use std::thread;
use std::time::Duration;

use consensus::Connection;
use distributary::Controller;

fn main() {
    use clap::{App, Arg};
    let matches = App::new("controller")
        .version("0.0.1")
        .about("Delivers scalable Soup distribution.")
        .arg(
            Arg::with_name("address")
                .short("a")
                .long("address")
                .takes_value(true)
                .default_value("127.0.0.1")
                .help("IP address to listen on"),
        )
        .arg(
            Arg::with_name("zookeeper")
                .short("z")
                .long("zookeeper")
                .takes_value(true)
                .default_value("127.0.0.1:2181")
                .help("Zookeeper connection info."),
        )
        .get_matches();

    let listen_addr = matches.value_of("address").unwrap().parse().unwrap();
    let zookeeper_addr = matches.value_of("zookeeper").unwrap();

    let connection = Connection::new(&zookeeper_addr);
    let _handle = Controller::start(listen_addr, connection);

    loop {
        thread::sleep(Duration::from_secs(5));
    }
}
