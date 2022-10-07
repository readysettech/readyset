use std::net::{IpAddr, SocketAddr};
use std::process;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use clap::Parser;
use futures_util::future::{self, Either};
use metrics_exporter_prometheus::PrometheusBuilder;
use readyset::metrics::recorded;
use readyset_server::consensus::AuthorityType;
use readyset_server::metrics::{
    install_global_recorder, CompositeMetricsRecorder, MetricsRecorder,
};
use readyset_server::{resolve_addr, Builder, NoriaMetricsRecorder, WorkerOptions};
use readyset_telemetry_reporter::{TelemetryEvent, TelemetryInitializer};
use readyset_version::*;
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

#[derive(Parser, Debug)]
#[clap(version = VERSION_STR_PRETTY)]
struct Options {
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

    /// Port to advertise to other ReadySet instances running in the same deployment.
    #[clap(long, short = 'p', default_value = "6033", parse(try_from_str))]
    external_port: u16,

    /// Use the AWS EC2 metadata service to determine the external address of this noria instance.
    ///
    /// If specified, overrides the value of --external-address
    #[clap(long)]
    use_aws_external_address: bool,

    /// ReadySet deployment ID.
    #[clap(long, env = "NORIA_DEPLOYMENT", forbid_empty_values = true)]
    deployment: String,

    /// Authority connection string.
    // TODO(justin): The default address should depend on the authority
    // value.
    #[clap(long, env = "AUTHORITY_ADDRESS", default_value = "127.0.0.1:8500")]
    authority_address: String,

    /// The authority to use. Possible values: zookeeper, consul.
    #[clap(long, env = "AUTHORITY", default_value = "consul", possible_values = &["consul", "zookeeper"])]
    authority: AuthorityType,

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
    pub noria_metrics: bool,

    #[clap(flatten)]
    tracing: readyset_tracing::Options,

    #[clap(flatten)]
    worker_options: WorkerOptions,

    /// Whether to disable telemetry reporting. Defaults to false.
    #[clap(long, env = "DISABLE_TELEMETRY")]
    disable_telemetry: bool,

    /// Whether we should wait for a failpoint request to the servers http router, which may
    /// impact startup.
    #[clap(long, hide = true)]
    wait_for_failpoint: bool,
}

fn main() -> anyhow::Result<()> {
    let opts: Options = Options::parse();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("worker")
        .build()?;

    rt.block_on(async {
        if let Err(error) = opts.tracing.init("readyset") {
            error!(%error, "Error initializing tracing");
            process::exit(1)
        }
    });
    info!(?opts, "Starting ReadySet server");

    info!(version = %VERSION_STR_ONELINE);

    let telemetry_sender = rt.block_on(TelemetryInitializer::init(
        opts.disable_telemetry,
        std::env::var("RS_API_KEY").ok(),
        vec![],
    ));

    let external_addr = if opts.use_aws_external_address {
        Either::Left(get_aws_private_ip())
    } else {
        Either::Right(future::ok(opts.external_address.unwrap_or(opts.address)))
    };

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

    metrics::counter!(
        recorded::NORIA_STARTUP_TIMESTAMP,
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    );

    if let Some(volume_id) = &opts.worker_options.volume_id {
        info!(%volume_id);
    }

    let mut builder = Builder::from_worker_options(opts.worker_options, &opts.deployment);
    builder.set_listen_addr(opts.address);
    builder.set_telemetry_sender(telemetry_sender.clone());
    builder.set_wait_for_failpoint(opts.wait_for_failpoint);

    if opts.cannot_become_leader {
        builder.cannot_become_leader();
    }

    if opts.reader_only {
        builder.as_reader_only()
    }
    if opts.no_readers {
        builder.no_readers()
    }

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

    // Wait for a shutdown condition, being one of:
    // - CTRL-C
    // - SIGTERM
    // - The controller shut down
    let ctrl_c = tokio::signal::ctrl_c();
    let mut sigterm = {
        let _guard = rt.enter();
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).unwrap()
    };
    rt.block_on(async {
        tokio::select! {
            biased;
            _ = ctrl_c => {
                info!("ctrl-c received, shutting down");
            },
            _ = sigterm.recv() => {
                info!("SIGTERM received, shutting down");
            }
            _ = handle.wait_done() => (),
        }
    });

    // Attempt a graceful shutdown of the telemetry reporting system
    rt.block_on(async move {
        let _ = telemetry_sender.send_event(TelemetryEvent::ServerStop);

        let shutdown_timeout = std::time::Duration::from_secs(5);

        match telemetry_sender.graceful_shutdown(shutdown_timeout).await {
            Ok(_) => info!("TelemetrySender shutdown gracefully"),
            Err(e) => info!(error=%e, "TelemetrySender did not shut down gracefully"),
        }
    });

    drop(rt);
    Ok(())
}
