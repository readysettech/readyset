#![warn(clippy::dbg_macro)]

use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use std::path::PathBuf;
use std::process;
use std::sync::Arc;
use std::time::Duration;

use clap::value_t_or_exit;
use futures_util::future::{self, Either};
use metrics_exporter_prometheus::PrometheusBuilder;

use noria_server::metrics::{
    install_global_recorder, BufferedRecorder, CompositeMetricsRecorder, MetricsRecorder,
};
use noria_server::{Builder, NoriaMetricsRecorder, ReuseConfigType, ZookeeperAuthority};

const PRIVATE_IP_ENDPOINT: &str = "http://169.254.169.254/latest/meta-data/local-ipv4";

/// Obtain the private ipv4 address of the AWS instance that the current program is running on using
/// the AWS metadata service
pub async fn get_aws_private_ip() -> anyhow::Result<IpAddr> {
    Ok(reqwest::get(PRIVATE_IP_ENDPOINT)
        .await?
        .text()
        .await?
        .parse()?)
}

pub fn resolve_addr(addr: &str) -> IpAddr {
    return [addr, ":0"]
        .concat()
        .to_socket_addrs()
        .unwrap()
        .next()
        .unwrap()
        .ip();
}

fn main() -> anyhow::Result<()> {
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
            Arg::with_name("external_address")
                .long("external-address")
                .takes_value(true)
                .help("IP address to advertise to other Noria instances running in the same deployment.
If not specified, defaults to the value of `address`")
        )
        .arg(
            Arg::with_name("external_port")
            .long("external-port")
            .short("p")
            .takes_value(true)
            .default_value("6033")
            .help("Port to advertise to other Noria instances running in the same deployment.")
        )
        .arg(
            Arg::with_name("use_aws_external_address")
                .long("use-aws-external-address")
        .help("Use the AWS EC2 metadata service to determine the external address of this noria instance.
If specified, overrides the value of --external-address"))
        .arg(
            Arg::with_name("deployment")
                .long("deployment")
                .required(true)
                .takes_value(true)
                .env("NORIA_DEPLOYMENT")
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
                .env("ZOOKEEPER_URL")
                .help("Zookeeper connection info."),
        )
        .arg(
            Arg::with_name("memory")
                .short("m")
                .long("memory")
                .takes_value(true)
                .default_value("0")
                .env("NORIA_MEMORY_BYTES")
                .help("Memory, in bytes, available for partially materialized state [0 = unlimited]."),
        )
        .arg(
            Arg::with_name("memory_check_freq")
                .long("memory-check-every")
                .takes_value(true)
                .default_value("1")
                .requires("memory")
                .help("Frequency at which to check the state size against the memory limit [in seconds]."),
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
                .env("NORIA_QUORUM")
                .help("Number of workers to wait for before starting (including this one)."),
        )
        .arg(
            Arg::with_name("shards")
                .long("shards")
                .takes_value(true)
                .default_value("0")
                .env("NORIA_SHARDS")
                .help("Shard the graph this many ways (0 = disable sharding)."),
        )
        .arg(
            Arg::with_name("metrics-queue-len")
                .long("metrics-queue-len")
                .takes_value(true)
                .default_value("1024")
                .help("Metrics queue length (number of metrics updates before a flush is needed).")
        )
        .arg(
            Arg::with_name("verbose")
                .short("v")
                .long("verbose")
                .takes_value(false)
                .help("Verbose log output."),
        )
        .arg(
            Arg::with_name("primary-region")
                .long("primary-region")
                .takes_value(true)
                .env("NORIA_PRIMARY_REGION")
                .help("The region where the controller is hosted in."),
        )
        .arg(
            Arg::with_name("region")
                .long("region")
            .default_value("")
            .env("NORIA_REGION")
            .help("The region the worker is hosted in. Required to route view requests to specific regions."),
        ).arg(
            Arg::with_name("replication-url")
                .long("replication-url")
                .takes_value(true)
                .required(false)
                .help("A URL identifying a MySQL or PostgreSQL primary server to replicater from. Should include username and password if necessary."),
        )
        .arg(
            Arg::with_name("reader-only")
                .long("reader-only")
                .takes_value(false)
                .help("Whether this server should only run reader domains or not.")
        )
        .arg(
            Arg::with_name("prometheus-metrics")
                .long("prometheus-metrics")
                .takes_value(false)
                .help("Output prometheus metrics."),
        )
        .arg(
            Arg::with_name("noria-metrics")
                .long("noria-metrics")
                .takes_value(false)
                .help("Output noria metrics."),
        )
        .get_matches();

    let log = noria_server::logger_pls();

    let durability = matches.value_of("durability").unwrap();
    let listen_addr: IpAddr = resolve_addr(matches.value_of("address").unwrap());
    let external_addr = if matches.is_present("use_aws_external_address") {
        Either::Left(get_aws_private_ip())
    } else {
        Either::Right(future::ok(
            matches
                .value_of("external_address")
                .map_or(listen_addr.clone(), |addr| resolve_addr(addr)),
        ))
    };
    let external_port = value_t_or_exit!(matches, "external_port", u16);
    let zookeeper_addr = matches.value_of("zookeeper").unwrap();
    let memory = value_t_or_exit!(matches, "memory", usize);
    let memory_check_freq = value_t_or_exit!(matches, "memory_check_freq", u64);
    let metrics_queue_len = value_t_or_exit!(matches, "metrics-queue-len", usize);
    let quorum = value_t_or_exit!(matches, "quorum", usize);
    let persistence_threads = value_t_or_exit!(matches, "persistence-threads", i32);
    let flush_ns = value_t_or_exit!(matches, "flush-timeout", u32);
    let sharding = match value_t_or_exit!(matches, "shards", usize) {
        0 => None,
        x => Some(x),
    };
    let verbose = matches.is_present("verbose");
    let use_noria_metrics = matches.is_present("noria-metrics");
    let use_prometheus_metrics = matches.is_present("prometheus-metrics");
    let deployment_name = matches.value_of("deployment").unwrap();

    // SAFETY: we haven't initialized threads that might call the recorder yet
    unsafe {
        let rec = CompositeMetricsRecorder::new();
        if use_noria_metrics {
            rec.add(MetricsRecorder::Noria(NoriaMetricsRecorder::new()));
        }
        if use_prometheus_metrics {
            rec.add(MetricsRecorder::Prometheus(
                PrometheusBuilder::new().build(),
            ));
        }
        let bufrec = BufferedRecorder::new(rec, metrics_queue_len);
        install_global_recorder(bufrec).unwrap();
    }

    let mut authority =
        ZookeeperAuthority::new(&format!("{}/{}", zookeeper_addr, deployment_name)).unwrap();
    let mut builder = Builder::default();
    builder.set_listen_addr(listen_addr);
    if memory > 0 {
        builder.set_memory_limit(memory, Duration::from_secs(memory_check_freq));
    }
    builder.set_sharding(sharding);
    builder.set_quorum(quorum);
    if matches.is_present("nopartial") {
        builder.disable_partial();
    }
    if matches.is_present("noreuse") {
        builder.set_reuse(ReuseConfigType::NoReuse);
    }

    if let Some(r) = matches.value_of("region") {
        builder.set_region(r.into());
    }

    if let Some(pr) = matches.value_of("primary-region") {
        builder.set_primary_region(pr.into());
    }

    if matches.is_present("reader-only") {
        builder.as_reader_only()
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

    if let Some(url) = matches.value_of("replication-url") {
        builder.set_replicator_url(url.into());
    }

    if verbose {
        authority.log_with(log.clone());
        builder.log_with(log);
    }

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("worker")
        .build()?;
    let mut handle = rt.block_on(async move {
        let external_addr = external_addr.await.unwrap_or_else(|err| {
            eprintln!("Error obtaining external IP address: {}", err);
            process::exit(1)
        });
        builder.set_external_addr(SocketAddr::from((external_addr, external_port)));
        builder.start(Arc::new(authority)).await
    })?;
    rt.block_on(handle.wait_done());
    drop(rt);
    Ok(())
}
