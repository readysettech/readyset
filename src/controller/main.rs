#[macro_use]
extern crate clap;
extern crate consensus;
extern crate distributary;

use consensus::ZookeeperAuthority;
use distributary::ControllerBuilder;

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
        .arg(
            Arg::with_name("local_workers")
                .long("local-workers")
                .takes_value(true)
                .value_name("COUNT")
                .conflicts_with("remote_workers")
                .required_unless("remote_workers")
                .help("Number of local workers."),
        )
        .arg(
            Arg::with_name("remote_workers")
                .short("w")
                .long("remote-workers")
                .takes_value(true)
                .value_name("COUNT")
                .conflicts_with("local_workers")
                .required_unless("local_workers")
                .help("Number of workers we expect to connect."),
        )
        .get_matches();

    let listen_addr = matches.value_of("address").unwrap().parse().unwrap();
    let zookeeper_addr = matches.value_of("zookeeper").unwrap();
    let local_workers = value_t!(matches, "local_workers", usize).unwrap_or(0);
    let remote_workers = value_t!(matches, "remote_workers", usize).unwrap_or(0);

    let authority = ZookeeperAuthority::new(&zookeeper_addr);
    let mut builder = ControllerBuilder::default();
    builder.set_listen_addr(listen_addr);
    builder.set_local_workers(local_workers);
    builder.set_nworkers(remote_workers);

    builder.build(authority).wait();
}
