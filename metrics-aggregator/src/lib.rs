// This design should be re-assessed when we have enough time to do so.
// Relevant ticket: https://readysettech.atlassian.net/browse/ENG-719
#![warn(clippy::dbg_macro)]
#![deny(macro_use_extern_crate)]

use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Arc;

use anyhow::anyhow;
use clap::Parser;
use noria::consensus::ConsulAuthority;
use prometheus_http_query::{Client, Scheme};
use stream_cancel::Valve;
use tokio::sync::Mutex;
use tracing::info;

use crate::http_router::MetricsAggregatorHttpRouter;

pub mod cache;
pub mod http_router;
pub mod metrics_reconciler;
use cache::QueryMetricsCache;
use metrics_reconciler::MetricsReconciler;

#[derive(Parser)]
pub struct Options {
    /// IP:PORT to listen on.
    #[clap(
        long,
        short = 'a',
        env = "LISTEN_ADDRESS",
        parse(try_from_str),
        default_value = "0.0.0.0:6035"
    )]
    address: SocketAddr,

    /// Consul connection string.
    #[clap(long, short = 'c', env = "AUTHORITY_ADDRESS")]
    consul_address: String,

    /// ReadySet deployment ID to filter by when aggregating metrics.
    #[clap(long, env = "NORIA_DEPLOYMENT", forbid_empty_values = true)]
    deployment: String,

    /// Prometheus connection string.
    #[clap(long, short = 'p', env = "PROMETHEUS_ADDRESS", parse(try_from_str))]
    prom_address: String,

    /// A flag that if set will communicate with prometheus over http rather than https.
    #[clap(long)]
    unsecured_prometheus: bool,

    /// A flag that if set will communicate with adapters over http rather than https.
    #[clap(long)]
    unsecured_adapters: bool,

    /// A flag that if set will ensure that the metrics aggregator shows latencies from upstream.
    #[clap(long)]
    show_upstream_latencies: bool,

    /// Sets the query reconciler's loop interval in seconds.
    #[clap(long, env = "RECONCILE_INTERVAL", default_value = "20")]
    reconciler_loop_interval: u64,

    #[clap(flatten)]
    logging: readyset_logging::Options,
}

pub fn run(options: Options) -> anyhow::Result<()> {
    options.logging.init()?;

    let prom_address_res: Vec<SocketAddr> = options.prom_address.to_socket_addrs()?.collect();
    if prom_address_res.is_empty() {
        return Err(anyhow!(
            "supplied prometheus address did not parse correctly"
        ));
    }

    let rt = tokio::runtime::Runtime::new()?;

    let mut sigterm = {
        let _guard = rt.enter();
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?
    };
    let mut sigint = {
        let _guard = rt.enter();
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?
    };

    let (shutdown_sender, shutdown_recv) = tokio::sync::broadcast::channel(1);
    #[allow(clippy::indexing_slicing)] // Validated as safe with early bail above.
    let (prom_ip, prom_port) = (prom_address_res[0].ip(), prom_address_res[0].port());
    let prom_client = if options.unsecured_prometheus {
        Client::new(Scheme::Http, &prom_ip.to_string(), prom_port)
    } else {
        Client::new(Scheme::Https, &prom_ip.to_string(), prom_port)
    };
    let consul = ConsulAuthority::new(&format!(
        "http://{}/{}",
        &options.consul_address, &options.deployment
    ))
    .unwrap();

    let query_metrics_cache = {
        let cache = Arc::new(Mutex::new(QueryMetricsCache::new()));
        let loop_interval = options.reconciler_loop_interval;
        let cache_for_reconciler = cache.clone();
        let deployment = options.deployment.clone();
        let unsecured_adapters = options.unsecured_adapters;
        let show_upstream_latencies = options.show_upstream_latencies;
        let fut = async move {
            let mut reconciler = MetricsReconciler::new(
                consul,
                prom_client,
                deployment,
                cache_for_reconciler,
                std::time::Duration::from_secs(loop_interval),
                shutdown_recv,
                unsecured_adapters,
                show_upstream_latencies,
            );
            reconciler.run().await
        };

        rt.handle().spawn(fut);

        cache
    };

    // Deploy http router if an address was supplied to retrieve QCA data.
    let router_handle = {
        let (handle, valve) = Valve::new();
        let http_server = MetricsAggregatorHttpRouter {
            listen_addr: options.address,
            query_metrics_cache,
            valve,
        };
        let fut = async move {
            let http_listener = http_server.create_listener().await.unwrap();
            MetricsAggregatorHttpRouter::route_requests(http_server, http_listener).await
        };

        rt.handle().spawn(fut);

        handle
    };

    // Wait on sigterm or sigint.
    rt.block_on(async move {
        tokio::select! {
            _ = sigterm.recv() => {
                info!("received sigterm");
            },
            _ = sigint.recv() => {
                info!("received sigint");
            },
        }
    });

    info!("shutting down all async threads");

    // Dropping the sender acts as a shutdown signal.
    drop(shutdown_sender);

    // Shut down all tcp streams started by the adapters http router.
    drop(router_handle);

    // TODO(peter): Uncomment once we pull adapter endpoints from authority.
    // // Drop authority channel to close conn with authority.
    // drop(ch);

    // We use `shutdown_timeout` instead of `shutdown_background` in case any
    // blocking IO is ongoing.
    info!("Waiting up to 20s for tasks to complete shutdown");
    rt.shutdown_timeout(std::time::Duration::from_secs(20));

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arg_parsing() {
        // Certain clap things, like `requires`, only ever throw an error at runtime, not at
        // compile-time - this tests that none of those happen
        let opts = Options::parse_from(vec![
            "metrics-aggregator",
            "--deployment",
            "test",
            "--address",
            "0.0.0.0:8090",
            "-c",
            "0.0.0.0:8500",
            "-p",
            "8.8.8.8:9090",
        ]);

        assert_eq!(opts.deployment, "test");
    }
}
