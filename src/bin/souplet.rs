#[macro_use]
extern crate clap;
extern crate consensus;
extern crate distributary;
extern crate slog;

use consensus::ZookeeperAuthority;
use distributary::{ControllerBuilder, ReuseConfigType};
use std::sync::Arc;
use std::time::Duration;

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
                .required(true)
                .takes_value(true)
                .help("Soup deployment ID."),
        )
        .arg(
            Arg::with_name("durability")
                .long("durability")
                .takes_value(true)
                .possible_values(&["persistent", "ephemeral", "memory"])
                .default_value("persistent")
                .help("How to maintain base logs."),
        )
        .arg(
            Arg::with_name("persistence-threads")
                .long("persistence-threads")
                .takes_value(true)
                .default_value("1")
                .help("Number of background threads used by RocksDB."),
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
                .help("Number of worker threads to run on this souplet."),
        )
        .arg(
            Arg::with_name("memory")
                .short("m")
                .long("memory")
                .takes_value(true)
                .default_value("0")
                .help("Memory, in bytes, available for materialized state [0 = unlimited]."),
        )
        .arg(
            Arg::with_name("noreuse")
                .long("no-reuse")
                .help("Disable reuse"),
        )
        .arg(
            Arg::with_name("nopartial")
                .long("no-partial")
                .help("Disable partial"),
        )
        .arg(
            Arg::with_name("readers")
                .short("r")
                .long("readers")
                .takes_value(true)
                .default_value("1")
                .help("Number of reader threads to run on this souplet."),
        )
        .arg(
            Arg::with_name("quorum")
                .short("q")
                .long("quorum")
                .takes_value(true)
                .default_value("1")
                .help("Number of souplets to wait for before starting (including this one)."),
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

    let durability = matches.value_of("durability").unwrap();
    let listen_addr = matches.value_of("address").unwrap().parse().unwrap();
    let zookeeper_addr = matches.value_of("zookeeper").unwrap();
    let workers = value_t_or_exit!(matches, "workers", usize);
    let memory = value_t_or_exit!(matches, "memory", usize);
    let readers = value_t_or_exit!(matches, "readers", usize);
    let quorum = value_t_or_exit!(matches, "quorum", usize);
    let persistence_threads = value_t_or_exit!(matches, "persistence-threads", i32);
    let sharding = match value_t_or_exit!(matches, "shards", usize) {
        0 => None,
        x => Some(x),
    };
    let verbose = matches.is_present("verbose");
    let deployment_name = matches.value_of("deployment").unwrap();

    let mut authority = ZookeeperAuthority::new(&format!("{}/{}", zookeeper_addr, deployment_name));
    let mut builder = ControllerBuilder::default();
    builder.set_listen_addr(listen_addr);
    builder.set_worker_threads(workers);
    if memory > 0 {
        builder.set_memory_limit(memory);
    }
    builder.set_read_threads(readers);
    builder.set_sharding(sharding);
    builder.set_quorum(quorum);
    if matches.is_present("nopartial") {
        builder.disable_partial();
    }
    if matches.is_present("noreuse") {
        builder.set_reuse(ReuseConfigType::NoReuse);
    }

    let persistence_params = distributary::PersistenceParameters::new(
        match durability {
            "persistent" => distributary::DurabilityMode::Permanent,
            "ephemeral" => distributary::DurabilityMode::DeleteOnExit,
            "memory" => distributary::DurabilityMode::MemoryOnly,
            _ => unreachable!(),
        },
        512,
        Duration::new(0, 100_000),
        Some(deployment_name.to_string()),
        persistence_threads,
    );
    builder.set_persistence(persistence_params);

    if verbose {
        authority.log_with(log.clone());
        builder.log_with(log);
    }

    builder.build(Arc::new(authority)).wait();
}
