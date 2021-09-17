#![warn(clippy::dbg_macro)]

use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use std::path::PathBuf;
use std::process;
use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use clap::Clap;
use futures_util::future::{self, Either};
use metrics_exporter_prometheus::PrometheusBuilder;

use noria_server::metrics::{
    install_global_recorder, BufferedRecorder, CompositeMetricsRecorder, MetricsRecorder,
};
use noria_server::{
    Authority, Builder, DurabilityMode, NoriaMetricsRecorder, ReuseConfigType, VolumeId,
    ZookeeperAuthority,
};

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

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

pub fn resolve_addr(addr: &str) -> anyhow::Result<IpAddr> {
    Ok([addr, ":0"]
        .concat()
        .to_socket_addrs()?
        .next()
        .ok_or_else(|| anyhow!("Could not resolve address: {}", addr))?
        .ip())
}

#[derive(Clap)]
#[clap(name = "noria-server")]
struct Opts {
    /// IP address to listen on
    #[clap(
        long,
        short = 'a',
        env = "LISTEN_ADDRESS",
        default_value = "127.0.0.1",
        parse(try_from_str = resolve_addr)
    )]
    address: IpAddr,

    /// IP address to advertise to other noria instances running in the same deployment.
    ///
    /// If not specified, defaults to the value of `address`
    #[clap(long, env = "EXTERNAL_ADDRESS", parse(try_from_str=resolve_addr))]
    external_address: Option<IpAddr>,

    /// Port to advertise to other Noria instances running in the same deployment.
    #[clap(long, short = 'p', default_value = "6033", parse(try_from_str))]
    external_port: u16,

    /// Use the AWS EC2 metadata service to determine the external address of this noria instance.
    ///
    /// If specified, overrides the value of --external-address
    #[clap(long)]
    use_aws_external_address: bool,

    /// Noria deployment ID.
    #[clap(long, env = "NORIA_DEPLOYMENT")]
    deployment: String,

    /// How to maintain base table state
    #[clap(long, default_value = "persistent", parse(try_from_str))]
    durability: DurabilityMode,

    /// Number of background threads used by RocksDB
    #[clap(long, default_value = "1")]
    persistence_threads: i32,

    /// Time to wait before processing a merged packet, in nanoseconds
    #[clap(long, default_value = "100000")]
    flush_timeout: u32,

    /// Absolute path to the directory where the log files will be written
    #[clap(long)]
    log_dir: Option<PathBuf>,

    /// Zookeeper connection info
    #[clap(
        long,
        short = 'z',
        env = "ZOOKEEPER_ADDRESS",
        default_value = "127.0.0.1:2181"
    )]
    zookeeper: String,

    /// Memory, in bytes, available for partially materialized state (0 = unlimited)
    #[clap(long, short = 'm', default_value = "0", env = "NORIA_MEMORY_BYTES")]
    memory: usize,

    /// Frequency at which to check the state size against the memory limit (in seconds)
    #[clap(long = "memory-check-every", default_value = "1")]
    memory_check_freq: u64,

    /// Enable query graph node reuse
    #[clap(long)]
    enable_reuse: bool,

    /// Disable partial
    #[clap(long = "nopartial")]
    no_partial: bool,

    /// Number of workers to wait for before starting (including this one)
    #[clap(long, short = 'q', default_value = "1", env = "NORIA_QUORUM")]
    quorum: usize,

    /// Shard the graph this many ways (0 = disable sharding)
    #[clap(long, default_value = "0", env = "NORIA_SHARDS")]
    shards: usize,

    /// Metrics queue length (number of metrics updates before a flush is needed).
    #[clap(long, default_value = "1024")]
    metrics_queue_len: usize,

    /// The region where the controller is hosted
    #[clap(long, env = "NORIA_PRIMARY_REGION")]
    primary_region: Option<String>,

    /// The region the worker is hosted in. Required to route view requests to specific regions.
    #[clap(long, env = "NORIA_REGION")]
    region: Option<String>,

    /// A URL identifying a MySQL or PostgreSQL primary server to replicate from. Should include
    /// username and password if necessary.
    #[clap(long, env = "REPLICATION_URL")]
    replication_url: Option<String>,

    /// Whether this server should only run reader domains
    #[clap(long)]
    reader_only: bool,

    /// Output prometheus metrics
    #[clap(long)]
    prometheus_metrics: bool,

    /// Output noria metrics
    #[clap(long)]
    noria_metrics: bool,

    /// Volume associated with the server.
    #[clap(long)]
    volume_id: Option<VolumeId>,

    #[clap(flatten)]
    logging: readyset_logging::Options,
}

fn main() -> anyhow::Result<()> {
    let opts: Opts = Opts::parse();
    opts.logging.init()?;

    let external_addr = if opts.use_aws_external_address {
        Either::Left(get_aws_private_ip())
    } else {
        Either::Right(future::ok(opts.external_address.unwrap_or(opts.address)))
    };
    let sharding = match opts.shards {
        0 => None,
        x => Some(x),
    };

    // SAFETY: we haven't initialized threads that might call the recorder yet
    unsafe {
        let rec = CompositeMetricsRecorder::new();
        if opts.noria_metrics {
            rec.add(MetricsRecorder::Noria(NoriaMetricsRecorder::new()));
        }
        if opts.prometheus_metrics {
            rec.add(MetricsRecorder::Prometheus(
                PrometheusBuilder::new()
                    .add_global_label("deployment", &opts.deployment)
                    .build(),
            ));
        }
        let bufrec = BufferedRecorder::new(rec, opts.metrics_queue_len);
        install_global_recorder(bufrec).unwrap();
    }

    let mut builder = Builder::default();
    builder.set_listen_addr(opts.address);
    if opts.memory > 0 {
        builder.set_memory_limit(opts.memory, Duration::from_secs(opts.memory_check_freq));
    }
    builder.set_sharding(sharding);
    builder.set_quorum(opts.quorum);
    if opts.no_partial {
        builder.disable_partial();
    }

    if opts.enable_reuse {
        builder.set_reuse(Some(ReuseConfigType::Finkelstein));
    } else {
        builder.set_reuse(None)
    }

    if let Some(r) = opts.region {
        builder.set_region(r);
    }

    if let Some(v) = opts.volume_id {
        builder.set_volume_id(v);
    }

    if let Some(pr) = opts.primary_region {
        builder.set_primary_region(pr);
    }

    if opts.reader_only {
        builder.as_reader_only()
    }

    let mut persistence_params = noria_server::PersistenceParameters::new(
        opts.durability,
        Duration::new(0, opts.flush_timeout),
        Some(opts.deployment.clone()),
        opts.persistence_threads,
    );
    persistence_params.log_dir = opts.log_dir;
    builder.set_persistence(persistence_params);

    if let Some(url) = opts.replication_url {
        builder.set_replicator_url(url);
    }

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("worker")
        .build()?;

    let zookeeper_addr = opts.zookeeper;
    let deployment = opts.deployment;
    let external_port = opts.external_port;
    let mut handle = rt.block_on(async move {
        let authority = Authority::from(
            ZookeeperAuthority::new(&format!("{}/{}", zookeeper_addr, deployment))
                .await
                .unwrap(),
        );

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
