use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use std::path::PathBuf;
use std::process;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::anyhow;
use clap::{ArgEnum, Parser};
use futures_util::future::{self, Either};
use metrics_exporter_prometheus::PrometheusBuilder;
use noria::metrics::recorded;
use noria_server::consensus::AuthorityType;
use noria_server::metrics::{install_global_recorder, CompositeMetricsRecorder, MetricsRecorder};
use noria_server::{
    Builder, DurabilityMode, NoriaMetricsRecorder, ReplicationOptions as DomainReplicationOptions,
    VolumeId,
};
use tracing::{error, info};

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

const AWS_PRIVATE_IP_ENDPOINT: &str = "http://169.254.169.254/latest/meta-data/local-ipv4";
const AWS_METADATA_TOKEN_ENDPOINT: &str = "http://169.254.169.254/latest/api/token";

/// Obtain the private ipv4 address of the AWS instance that the current program is running on using
/// the AWS metadata service
pub async fn get_aws_private_ip() -> anyhow::Result<IpAddr> {
    let client = reqwest::Client::builder().build()?;
    let token: String = client
        .put(AWS_METADATA_TOKEN_ENDPOINT)
        .header("X-aws-ec2-metadata-token-ttl-seconds", "21600")
        .send()
        .await?
        .text()
        .await?
        .parse()?;

    Ok(client
        .get(AWS_PRIVATE_IP_ENDPOINT)
        .header("X-aws-ec2-metadata-token", &token)
        .send()
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

#[derive(Parser, Debug)]
#[clap(name = "noria-server", version)]
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
    #[clap(long, env = "NORIA_DEPLOYMENT", forbid_empty_values = true)]
    deployment: String,

    /// How to maintain base table state
    #[clap(long, default_value = "persistent", parse(try_from_str))]
    durability: DurabilityMode,

    /// Number of background threads used by RocksDB
    #[clap(long, default_value = "6")]
    persistence_threads: i32,

    /// Authority connection string.
    // TODO(justin): The default address should depend on the authority
    // value.
    #[clap(long, env = "AUTHORITY_ADDRESS", default_value = "127.0.0.1:8500")]
    authority_address: String,

    /// The authority to use. Possible values: zookeeper, consul.
    #[clap(long, env = "AUTHORITY", default_value = "consul", possible_values = &["consul", "zookeeper"])]
    authority: AuthorityType,

    /// Memory, in bytes, available for partially materialized state (0 = unlimited)
    #[clap(long, short = 'm', default_value = "0", env = "NORIA_MEMORY_BYTES")]
    memory: usize,

    /// Frequency at which to check the state size against the memory limit (in seconds)
    #[clap(
        long = "memory-check-every",
        default_value = "1",
        env = "MEMORY_CHECK_EVERY"
    )]
    memory_check_freq: u64,

    /// The strategy to use when memory is freed from reader nodes
    #[clap(long = "eviction-policy", arg_enum, default_value_t = dataflow::EvictionKind::Random)]
    eviction_kind: dataflow::EvictionKind,

    /// Disable partial
    #[clap(long = "nopartial")]
    no_partial: bool,

    /// Forbid the creation of fully materialized nodes
    #[clap(long, env = "FORBID_FULL_MATERIALIZATION")]
    forbid_full_materialization: bool,

    /// Enable packet filters in egresses before readers
    #[clap(long)]
    enable_packet_filters: bool,

    /// Number of workers to wait for before starting (including this one)
    #[clap(long, short = 'q', default_value = "1", env = "NORIA_QUORUM")]
    quorum: usize,

    /// Shard the graph this many ways (<= 1 : disable sharding)
    #[clap(long, default_value = "0", env = "NORIA_SHARDS")]
    shards: usize,

    /// Metrics queue length (number of metrics updates before a flush is needed).
    #[clap(long, default_value = "1024")]
    metrics_queue_len: usize,

    /// Whether this server should only run reader domains
    #[clap(long, conflicts_with = "no-readers")]
    reader_only: bool,

    /// If set, this server will never run domains containing reader nodes
    #[clap(long, conflicts_with = "reader-only")]
    no_readers: bool,

    /// Prevent this instance from ever being elected as the leader
    #[clap(long)]
    cannot_become_leader: bool,

    /// Output prometheus metrics
    #[clap(long, env = "PROMETHEUS_METRICS")]
    prometheus_metrics: bool,

    /// Output noria metrics
    #[clap(long)]
    noria_metrics: bool,

    /// Volume associated with the server.
    #[clap(long, env = "VOLUME_ID")]
    volume_id: Option<VolumeId>,

    /// Enable experimental support for TopK in dataflow
    #[clap(long, env = "EXPERIMENTAL_TOPK_SUPPORT", hide = true)]
    enable_experimental_topk_support: bool,

    /// Enable experimental support for Paginate in dataflow
    #[clap(long, env = "EXPERIMENTAL_PAGINATE_SUPPORT", hide = true)]
    enable_experimental_paginate_support: bool,

    /// Enable experimental support for mixing equality and inequality comparisons on query
    /// parameters
    #[clap(long, env = "EXPERIMENTAL_MIXED_COMPARISONS_SUPPORT", hide = true)]
    enable_experimental_mixed_comparisons: bool,

    #[clap(flatten)]
    tracing: readyset_tracing::Options,

    /// Sets the number of concurrent replay requests in a noria-server.
    #[clap(long, hide = true)]
    max_concurrent_replays: Option<usize>,

    /// Directory in which to store replicated table data. If not specified, defaults to the
    /// current working directory.
    #[clap(long, env = "DB_DIR")]
    db_dir: Option<PathBuf>,

    #[clap(flatten)]
    domain_replication_options: DomainReplicationOptions,

    #[clap(flatten)]
    replicator_config: replicators::Config,
}

fn main() -> anyhow::Result<()> {
    let opts: Opts = Opts::parse();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("worker")
        .build()?;

    rt.block_on(async {
        if let Err(error) = opts.tracing.init("noria") {
            error!(%error, "Error initializing tracing");
            process::exit(1)
        }
    });
    info!(?opts, "Starting ReadySet server");

    info!(commit_hash = %env!("CARGO_PKG_VERSION", "version not set"));

    let external_addr = if opts.use_aws_external_address {
        Either::Left(get_aws_private_ip())
    } else {
        Either::Right(future::ok(opts.external_address.unwrap_or(opts.address)))
    };
    let sharding = match opts.shards {
        0 | 1 => None,
        x => Some(x),
    };

    // SAFETY: we haven't initialized threads that might call the recorder yet
    unsafe {
        let mut recs = Vec::new();
        if opts.noria_metrics {
            recs.push(MetricsRecorder::Noria(NoriaMetricsRecorder::new()));
        }
        if opts.prometheus_metrics {
            recs.push(MetricsRecorder::Prometheus(
                PrometheusBuilder::new()
                    .add_global_label("deployment", &opts.deployment)
                    .build_recorder(),
            ));
        }
        install_global_recorder(CompositeMetricsRecorder::with_recorders(recs)).unwrap();
    }

    metrics::counter!(
        recorded::NORIA_STARTUP_TIMESTAMP,
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    );

    let mut builder = Builder::default();
    builder.set_listen_addr(opts.address);
    if opts.memory > 0 {
        builder.set_memory_limit(opts.memory, Duration::from_secs(opts.memory_check_freq));
    }
    builder.set_eviction_kind(opts.eviction_kind);

    builder.set_sharding(sharding);
    builder.set_quorum(opts.quorum);
    if opts.no_partial {
        builder.disable_partial();
    }
    if opts.forbid_full_materialization {
        builder.forbid_full_materialization();
    }
    if opts.enable_packet_filters {
        builder.enable_packet_filters();
    }

    // TODO(fran): Reuse will be disabled until we refactor MIR to make it serializable.
    // See `noria/server/src/controller/sql/serde.rs` for details.
    builder.set_reuse(None);

    builder.set_allow_topk(opts.enable_experimental_topk_support);
    builder.set_allow_paginate(opts.enable_experimental_paginate_support);
    builder.set_allow_mixed_comparisons(opts.enable_experimental_mixed_comparisons);

    builder.set_replication_strategy(opts.domain_replication_options.into());

    if let Some(volume_id) = opts.volume_id {
        info!(%volume_id);
        builder.set_volume_id(volume_id);
    }

    if opts.cannot_become_leader {
        builder.cannot_become_leader();
    }

    if let Some(r) = opts.max_concurrent_replays {
        builder.set_max_concurrent_replay(r)
    }

    if opts.reader_only {
        builder.as_reader_only()
    }
    if opts.no_readers {
        builder.no_readers()
    }

    let persistence_params = noria_server::PersistenceParameters::new(
        opts.durability,
        Some(opts.deployment.clone()),
        opts.persistence_threads,
        opts.db_dir,
    );
    builder.set_persistence(persistence_params);

    builder.set_replicator_config(opts.replicator_config);

    let authority = opts.authority;
    let authority_addr = opts.authority_address;
    let deployment = opts.deployment;
    let external_port = opts.external_port;
    let mut handle = rt.block_on(async move {
        let authority = authority.to_authority(&authority_addr, &deployment).await;

        let external_addr = external_addr.await.unwrap_or_else(|error| {
            error!(%error, "Error obtaining external IP address");
            process::exit(1)
        });
        builder.set_external_addr(SocketAddr::from((external_addr, external_port)));
        builder.start(Arc::new(authority)).await
    })?;
    rt.block_on(handle.wait_done());
    drop(rt);
    Ok(())
}
