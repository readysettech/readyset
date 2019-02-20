#[macro_use]
extern crate clap;
extern crate noria_server;
extern crate slog;

use noria_server::{Builder, ReuseConfigType, ZookeeperAuthority};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

fn main() {
    use clap::{App, Arg};
    let matches = App::new("noria-server")
        .version("0.0.1")
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
                .help("Noria deployment ID."),
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
            Arg::with_name("flush-timeout")
                .long("flush-timeout")
                .takes_value(true)
                .default_value("100000")
                .help("Time to wait before processing a merged packet, in nanoseconds."),
        )
        .arg(
            Arg::with_name("log-dir")
                .long("log-dir")
                .takes_value(true)
                .help("Absolute path to the directory where the log files will be written."),
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
            Arg::with_name("memory")
                .short("m")
                .long("memory")
                .takes_value(true)
                .default_value("0")
                .help("Memory, in bytes, available for partially materialized state [0 = unlimited]."),
        )
        .arg(
            Arg::with_name("memory_check_freq")
                .long("memory-check-every")
                .takes_value(true)
                .default_value("10")
                .requires("memory")
                .help("Frequency at which to check the state size against the memory limit [in milliseconds]."),
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
            Arg::with_name("quorum")
                .short("q")
                .long("quorum")
                .takes_value(true)
                .default_value("1")
                .help("Number of workers to wait for before starting (including this one)."),
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

    let log = noria_server::logger_pls();

    let durability = matches.value_of("durability").unwrap();
    let listen_addr = matches.value_of("address").unwrap().parse().unwrap();
    let zookeeper_addr = matches.value_of("zookeeper").unwrap();
    let memory = value_t_or_exit!(matches, "memory", usize);
    let memory_check_freq = value_t_or_exit!(matches, "memory_check_freq", u64);
    let quorum = value_t_or_exit!(matches, "quorum", usize);
    let persistence_threads = value_t_or_exit!(matches, "persistence-threads", i32);
    let flush_ns = value_t_or_exit!(matches, "flush-timeout", u32);
    let sharding = match value_t_or_exit!(matches, "shards", usize) {
        0 => None,
        x => Some(x),
    };
    let verbose = matches.is_present("verbose");
    let deployment_name = matches.value_of("deployment").unwrap();

    let mut authority =
        ZookeeperAuthority::new(&format!("{}/{}", zookeeper_addr, deployment_name)).unwrap();
    let mut builder = Builder::default();
    builder.set_listen_addr(listen_addr);
    if memory > 0 {
        builder.set_memory_limit(memory, Duration::from_millis(memory_check_freq));
    }
    builder.set_sharding(sharding);
    builder.set_quorum(quorum);
    if matches.is_present("nopartial") {
        builder.disable_partial();
    }
    if matches.is_present("noreuse") {
        builder.set_reuse(ReuseConfigType::NoReuse);
    }

    let mut persistence_params = noria_server::PersistenceParameters::new(
        match durability {
            "persistent" => noria_server::DurabilityMode::Permanent,
            "ephemeral" => noria_server::DurabilityMode::DeleteOnExit,
            "memory" => noria_server::DurabilityMode::MemoryOnly,
            _ => unreachable!(),
        },
        Duration::new(0, flush_ns),
        Some(deployment_name.to_string()),
        persistence_threads,
    );
    persistence_params.log_dir = matches
        .value_of("log-dir")
        .and_then(|p| Some(PathBuf::from(p)));
    builder.set_persistence(persistence_params);

    if verbose {
        authority.log_with(log.clone());
        builder.log_with(log);
    }

    let mut rt = tokio::runtime::Builder::new();
    rt.name_prefix("worker-");
    if let Some(threads) = None {
        rt.core_threads(threads);
    }
    rt.build()
        .unwrap()
        .block_on_all(builder.start(Arc::new(authority)))
        .unwrap();
}
