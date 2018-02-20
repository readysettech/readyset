#[macro_use]
extern crate clap;
extern crate consensus;
extern crate distributary;
#[macro_use]
extern crate slog;
extern crate uuid;

use consensus::ZookeeperAuthority;
use distributary::ControllerBuilder;
use std::sync::Arc;
use std::time::Duration;
use uuid::{Uuid, UuidVersion};

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
            Arg::with_name("deployment")
                .long("deployment")
                .takes_value(true)
                .help("Soup deployment ID."),
        )
        .arg(
            Arg::with_name("ephemeral")
                .long("ephemeral")
                .takes_value(false)
                .help("Do not permanently store base logs."),
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
            Arg::with_name("shards")
                .long("shards")
                .takes_value(true)
                .default_value("0")
                .help("Shard the graph this many ways (0 = disable sharding)."),
        )
        .arg(
            Arg::with_name("verbose")
                .short("v")
                .long("verbose")
                .takes_value(false)
                .help("Verbose log output."),
        )
        .get_matches();

    let log = distributary::logger_pls();

    let ephemeral = matches.is_present("ephemeral");
    let listen_addr = matches.value_of("address").unwrap().parse().unwrap();
    let zookeeper_addr = matches.value_of("zookeeper").unwrap();
    let workers = value_t_or_exit!(matches, "workers", usize);
    let readers = value_t_or_exit!(matches, "readers", usize);
    let sharding = match value_t_or_exit!(matches, "shards", usize) {
        0 => None,
        x => Some(x),
    };
    let verbose = matches.is_present("verbose");

    let deployment_name = match matches.value_of("deployment") {
        Some(d) => String::from(d),
        None => {
            let id = Uuid::new(UuidVersion::Random).unwrap();
            if verbose {
                info!(log, "starting new Soup deployment '{}'", id.simple());
            }
            format!("{}", id.simple())
        }
    };

    let mut authority = ZookeeperAuthority::new(&format!("{}/{}", zookeeper_addr, deployment_name));
    let mut builder = ControllerBuilder::default();
    builder.set_listen_addr(listen_addr);
    builder.set_nworkers(workers);
    builder.set_local_read_threads(readers);
    builder.set_sharding(sharding);

    let persistence_params = distributary::PersistenceParameters::new(
        if ephemeral {
            distributary::DurabilityMode::DeleteOnExit
        } else {
            distributary::DurabilityMode::Permanent
        },
        512,
        Duration::from_millis(1),
        Some(deployment_name),
    );
    builder.set_persistence(persistence_params);

    if verbose {
        authority.log_with(log.clone());
        builder.log_with(log);
    }

    builder.build(Arc::new(authority)).wait();
}
