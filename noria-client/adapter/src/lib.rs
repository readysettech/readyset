#![warn(clippy::dbg_macro)]
extern crate anyhow;
#[macro_use]
extern crate tracing;

use std::collections::HashMap;
use std::io;
use std::marker::Send;
use std::net::SocketAddr;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, RwLock};

use anyhow::anyhow;
use async_trait::async_trait;
use clap::Clap;
use futures_util::future::FutureExt;
use futures_util::stream::StreamExt;
use maplit::hashmap;
use metrics_exporter_prometheus::PrometheusBuilder;
use tokio::net;
use tokio_stream::wrappers::TcpListenerStream;
use tracing::Level;

use nom_sql::{Dialect, SelectStatement};
use noria::{ControllerHandle, ZookeeperAuthority};
use noria_client::backend::{
    mysql_connector::MySqlConnector, noria_connector::NoriaConnector, Backend, BackendBuilder,
    Reader, Writer,
};

#[async_trait]
pub trait ConnectionHandler {
    async fn process_connection(
        &mut self,
        stream: net::TcpStream,
        backend: Backend<ZookeeperAuthority>,
    );
}

/// Represents which database interface is being adapted to
/// communicate with Noria.
#[derive(Copy, Clone)]
pub enum DatabaseType {
    /// MySQL database.
    Mysql,
    /// PostgreSQL database.
    Psql,
}

pub struct NoriaAdapter<H> {
    pub name: &'static str,
    pub version: &'static str,
    pub description: &'static str,
    pub default_address: SocketAddr,
    pub connection_handler: H,
    pub database_type: DatabaseType,
    /// SQL dialect to use when parsing queries
    pub dialect: Dialect,
}

#[derive(Clap)]
pub struct Options {
    /// IP:PORT to listen on
    #[clap(long, short = 'a', parse(try_from_str))]
    address: Option<SocketAddr>,

    /// ReadySet deployment ID to attach to
    #[clap(long, env = "NORIA_DEPLOYMENT")]
    deployment: String,

    /// IP:PORT for Zookeeper
    #[clap(
        long,
        short = 'z',
        env = "ZOOKEEPER_ADDRESS",
        default_value = "127.0.0.1:2181"
    )]
    zookeeper_address: String,

    /// Log slow queries (> 5ms)
    #[clap(long)]
    log_slow: bool,

    /// Instead of logging trace events, time them and output metrics on exit
    #[clap(long)]
    time: bool,

    /// Disable checking for queries requiring static responses. Improves latency.
    #[clap(long)]
    no_static_responses: bool,

    /// Don't require authentication for any client connections
    #[clap(long)]
    no_require_authentication: bool,

    /// Allow database connections authenticated as this user. Ignored if
    /// --no-require-authentication is passed
    #[clap(long, short = 'u')]
    username: Option<String>,

    /// Password to authenticate database connections with. Ignored if --no-require-authentication
    /// is passed
    #[clap(long, short = 'p')]
    password: Option<String>,

    /// URL for MySQL connection. Should include username and password if necessary
    #[clap(long, env = "MYSQL_URL")]
    mysql_url: Option<String>,

    /// The region the worker is hosted in. Required to route view requests to specific regions.
    #[clap(long, env = "NORIA_REGION")]
    region: Option<String>,

    /// Enable recording and exposing Prometheus metrics
    #[clap(long)]
    prometheus_metrics: bool,
}

impl<H: ConnectionHandler + Clone + Send + Sync + 'static> NoriaAdapter<H> {
    pub fn run(&mut self, options: Options) -> anyhow::Result<()> {
        let users: &'static HashMap<String, String> = Box::leak(Box::new(
            if !options.no_require_authentication {
                hashmap! {
                    options.username.ok_or_else(|| {
                        anyhow!("Must specify --username/-u unless --no-require-authentication is passed")
                    })? => options.password.ok_or_else(|| {
                        anyhow!("Must specify --password/-p unless --no-require-authentication is passed")
                    })?
                }
            } else {
                HashMap::new()
            },
        ));

        use tracing_subscriber::Layer;
        let filter = tracing_subscriber::EnvFilter::from_default_env();
        let registry = tracing_subscriber::Registry::default();
        let tracer = if options.time {
            use tracing_timing::{Builder, Histogram};
            let s = Builder::default()
                .layer(|| Histogram::new_with_bounds(1_000, 100_000_000, 3).unwrap());
            tracing::Dispatch::new(filter.and_then(s).with_subscriber(registry))
        } else {
            use tracing_subscriber::fmt;
            let s = fmt::format::Format::default().with_timer(fmt::time::Uptime::default());
            let s = fmt::Layer::default().event_format(s);
            tracing::Dispatch::new(filter.and_then(s).with_subscriber(registry))
        };
        tracing::dispatcher::set_global_default(tracer.clone()).unwrap();
        let rt = tracing::dispatcher::with_default(&tracer, tokio::runtime::Runtime::new).unwrap();
        let listen_address = options.address.unwrap_or(self.default_address);
        let listener = rt
            .block_on(tokio::net::TcpListener::bind(&listen_address))
            .unwrap();

        let log = logger_pls();
        slog::info!(log, "listening on address {}", listen_address);

        let auto_increments: Arc<RwLock<HashMap<String, AtomicUsize>>> = Arc::default();
        let query_cache: Arc<RwLock<HashMap<SelectStatement, String>>> = Arc::default();

        let mut zk_auth = ZookeeperAuthority::new(&format!(
            "{}/{}",
            options.zookeeper_address, options.deployment
        ))?;
        zk_auth.log_with(log.clone());

        slog::debug!(log, "Connecting to Noria...",);
        let ch = rt.block_on(ControllerHandle::new(zk_auth));
        slog::debug!(log, "Connected!");

        let ctrlc = tokio::signal::ctrl_c();
        let mut listener = Box::pin(futures_util::stream::select(
            TcpListenerStream::new(listener),
            ctrlc
                .map(|r| {
                    if let Err(e) = r {
                        Err(e)
                    } else {
                        Err(io::Error::new(io::ErrorKind::Interrupted, "got ctrl-c"))
                    }
                })
                .into_stream(),
        ));

        if options.prometheus_metrics {
            let _guard = rt.enter();
            let database_label: noria_client_metrics::recorded::DatabaseType =
                self.database_type.into();
            PrometheusBuilder::new()
                .add_global_label("database_type", database_label)
                .add_global_label("deployment", &options.deployment)
                .install()
                .unwrap();
        }

        while let Some(Ok(s)) = rt.block_on(listener.next()) {
            // bunch of stuff to move into the async block below
            let ch = ch.clone();
            let (auto_increments, query_cache) = (auto_increments.clone(), query_cache.clone());
            let mut connection_handler = self.connection_handler.clone();
            let region = options.region.clone();
            let mysql_url = options.mysql_url.clone();
            let backend_builder = BackendBuilder::new()
                .static_responses(!options.no_static_responses)
                .slowlog(options.log_slow)
                .users(users.clone())
                .require_authentication(!options.no_require_authentication)
                .dialect(self.dialect);
            let fut = async move {
                let connection = span!(Level::DEBUG, "connection", addr = ?s.peer_addr().unwrap());
                connection.in_scope(|| debug!("accepted"));

                // initially, our instinct was that constructing this twice (once for reader and
                // once for writer) did not make sense and we should instead call clone() or use an
                // Arc<RwLock<NoriaConnector>> as both the reader and writer. however, after more
                // thought, there is no benefit to sharing any implentation between the two. the
                // only potential shared state is the query_cache, however, the set of queries
                // handles by a reader and writer are disjoint
                let noria_conn = NoriaConnector::new(
                    ch.clone(),
                    auto_increments.clone(),
                    query_cache.clone(),
                    region.clone(),
                )
                .await;

                let reader = Reader {
                    noria_connector: noria_conn,
                    mysql_connector: if let Some(mysql_url) = &mysql_url {
                        Some(MySqlConnector::new(mysql_url.clone()).await)
                    } else {
                        None
                    },
                };

                let _g = connection.enter();

                let writer: Writer<ZookeeperAuthority> = if let Some(url) = mysql_url {
                    let writer = MySqlConnector::new(url).await;

                    Writer::MySqlConnector(writer)
                } else {
                    let writer =
                        NoriaConnector::new(ch, auto_increments, query_cache, region.clone()).await;
                    Writer::NoriaConnector(writer)
                };

                let backend = backend_builder.clone().build(writer, reader);

                connection_handler.process_connection(s, backend).await;

                debug!("disconnected");
            };
            rt.handle().spawn(fut);
        }

        drop(ch);
        slog::info!(log, "Exiting...");

        drop(rt);

        if let Some(timing) = tracer.downcast_ref::<tracing_timing::TimingLayer>() {
            timing.force_synchronize();
            timing.with_histograms(|hs| {
                for (&span_group, hs) in hs {
                    if span_group == "connection" {
                        // we don't care about the event timings relative to the connection context
                        continue;
                    }

                    println!("==> {}", span_group);
                    for (event_group, h) in hs {
                        // make sure we see the latest samples:
                        h.refresh();
                        // compute the "Coefficient of Variation"
                        // < 1 means "low variance", > 1 means "high variance"
                        if h.stdev() / h.mean() < 1.0 {
                            // low variance -- print the median:
                            println!(
                                ".. {:?} (median)",
                                std::time::Duration::from_nanos(h.value_at_quantile(0.5)),
                            )
                        } else {
                            // high variance -- show more stats
                            println!(
                                "mean: {:?}, p50: {:?}, p90: {:?}, p99: {:?}, p999: {:?}, max: {:?}",
                                std::time::Duration::from_nanos(h.mean() as u64),
                                std::time::Duration::from_nanos(h.value_at_quantile(0.5)),
                                std::time::Duration::from_nanos(h.value_at_quantile(0.9)),
                                std::time::Duration::from_nanos(h.value_at_quantile(0.99)),
                                std::time::Duration::from_nanos(h.value_at_quantile(0.999)),
                                std::time::Duration::from_nanos(h.max()),
                            );

                            let p95 = h.value_at_quantile(0.95);
                            let mut scale = p95 / 5;
                            // set all but highest digit to 0
                            let mut shift = 0;
                            while scale > 10 {
                                scale /= 10;
                                shift += 1;
                            }
                            for _ in 0..shift {
                                scale *= 10;
                            }

                            for v in break_once(
                                h.iter_linear(scale).skip_while(|v| v.quantile() < 0.01),
                                |v| v.quantile() > 0.95,
                            ) {
                                println!(
                                    "{:6?} | {:40} | {:4.1}th %-ile",
                                    std::time::Duration::from_nanos(v.value_iterated_to() + 1),
                                    "*".repeat(
                                        (v.count_since_last_iteration() as f64 * 40.0 / h.len() as f64)
                                            .ceil() as usize
                                    ),
                                    v.percentile(),
                                );
                            }
                        }
                        println!(" -> {}", event_group);
                    }
                }
            });
        }

        Ok(())
    }
}

// Just give me a damn terminal logger
// Duplicated from noria-server, as the API subcrate doesn't export it.
pub fn logger_pls() -> slog::Logger {
    use slog::Drain;
    use slog::Logger;
    use slog_term::term_full;
    use std::sync::Mutex;
    Logger::root(Mutex::new(term_full()).fuse(), slog::o!())
}

// until we have https://github.com/rust-lang/rust/issues/62208
fn break_once<I, F>(it: I, mut f: F) -> impl Iterator<Item = I::Item>
where
    I: IntoIterator,
    F: FnMut(&I::Item) -> bool,
{
    let mut got_true = false;
    it.into_iter().take_while(move |i| {
        if got_true {
            // we've already yielded when f was true
            return false;
        }
        if f(i) {
            // this must be the first time f returns true
            // we should yield i, and then no more
            got_true = true;
        }
        // f returned false, so we should keep yielding
        true
    })
}

impl From<DatabaseType> for noria_client_metrics::recorded::DatabaseType {
    fn from(database_type: DatabaseType) -> Self {
        match database_type {
            DatabaseType::Mysql => noria_client_metrics::recorded::DatabaseType::Mysql,
            DatabaseType::Psql => noria_client_metrics::recorded::DatabaseType::Psql,
        }
    }
}
