#![warn(clippy::dbg_macro)]
#[macro_use]
extern crate clap;
extern crate anyhow;
#[macro_use]
extern crate tracing;

use async_trait::async_trait;
use futures_util::future::FutureExt;
use futures_util::stream::StreamExt;
use maplit::hashmap;
use nom_sql::SelectStatement;
use noria::{ControllerHandle, ZookeeperAuthority};
use noria_client::backend::{
    mysql_connector::MySqlConnector, noria_connector::NoriaConnector, Backend, BackendBuilder,
    Writer,
};
use std::collections::HashMap;
use std::io;
use std::marker::Send;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, RwLock};
use tokio::net;
use tokio_stream::wrappers::TcpListenerStream;
use tracing::Level;

#[async_trait]
pub trait ConnectionHandler {
    async fn process_connection(
        &mut self,
        stream: net::TcpStream,
        backend: Backend<ZookeeperAuthority>,
    );
}

pub struct NoriaAdapter<H> {
    pub name: &'static str,
    pub version: &'static str,
    pub description: &'static str,
    pub default_address: &'static str,
    pub connection_handler: H,
}

impl<H: ConnectionHandler + Clone + Send + Sync + 'static> NoriaAdapter<H> {
    pub fn run(&mut self) {
        use clap::{App, Arg};

        let matches = App::new(self.name)
            .version(self.version)
            .about(self.description)
            .arg(
                Arg::with_name("address")
                    .short("a")
                    .long("address")
                    .takes_value(true)
                    .default_value(self.default_address)
                    .help("IP:PORT to listen on"),
            )
            .arg(
                Arg::with_name("deployment")
                    .long("deployment")
                    .takes_value(true)
                    .required(true)
                    .env("NORIA_DEPLOYMENT")
                    .help("Noria deployment ID to attach to."),
            )
            .arg(
                Arg::with_name("zk_addr")
                    .long("zookeeper-address")
                    .short("z")
                    .default_value("127.0.0.1:2181")
                    .env("ZOOKEEPER_URL")
                    .help("IP:PORT for Zookeeper."),
            )
            .arg(
                Arg::with_name("slowlog")
                    .long("log-slow")
                    .help("Log slow queries (> 5ms)"),
            )
            .arg(
                Arg::with_name("trace")
                    .long("trace")
                    .takes_value(true)
                    .help("Trace client-side execution of every Nth operation"),
            )
            .arg(
                Arg::with_name("permissive")
                    .long("permissive")
                    .takes_value(false)
                    .help("Be permissive in queries that the adapter accepts (rather than rejecting parse errors)"),
            )
            .arg(
                Arg::with_name("time")
                    .long("time")
                    .help("Instead of logging trace events, time them and output metrics on exit"),
            )
            .arg(
                Arg::with_name("no-static-responses")
                    .long("no-static-responses")
                    .takes_value(false)
                    .help("Disable checking for queries requiring static responses. Improves latency."),
            )
            .arg(
                Arg::with_name("no-sanitize")
                    .long("no-sanitize")
                    .takes_value(false)
                    .help("Disable query sanitization. Improves latency."),
            )
            .arg(
                Arg::with_name("no-require-authentication")
                    .long("no-require-authentication")
                    .takes_value(false)
                    .help("Don't require authentication for any client connections")
            )
            .arg(
                Arg::with_name("username")
                    .long("username")
                    .short("u")
                    .takes_value(true)
                    .default_value("root")
                    .help("Allow database connections authenticated as this user. Ignored if --no-require-authentication is passed")
            )
            .arg(
                Arg::with_name("password")
                    .long("password")
                    .short("p")
                    .takes_value(true)
                    .empty_values(false)
                    .help("Password to authenticate database connections with. Ignored if --no-require-authentication is passed")
            )
            .arg(Arg::with_name("verbose").long("verbose").short("v"))
            .arg(
                Arg::with_name("mysql-url")
                    .long("mysql-url")
                    .takes_value(true)
                    .required(false)
                    .env("MYSQL_URL")
                    .help("Host for mysql connection. Should include username and password if nececssary."),
            )
            .arg(
                Arg::with_name("region")
                .default_value("")
                .env("NORIA_REGION")
                .help("The region the worker is hosted in. Required to route view requests to specific regions."),
            )
            .get_matches();

        let listen_addr = value_t_or_exit!(matches, "address", String);
        let deployment = matches.value_of("deployment").unwrap().to_owned();
        assert!(!deployment.contains('-'));

        let histograms = matches.is_present("time");
        let slowlog = matches.is_present("slowlog");
        let zk_addr = matches.value_of("zk_addr").unwrap().to_owned();
        let sanitize = !matches.is_present("no-sanitize");
        let permissive = matches.is_present("permissive");
        let static_responses = !matches.is_present("no-static-responses");
        let mysql_url = matches.value_of("mysql-url").map(|s| s.to_owned());
        let require_authentication = !matches.is_present("no-require-authentication");
        let region = matches.value_of("region").map(|s| s.to_owned());

        let users: &'static HashMap<String, String> =
            Box::leak(Box::new(if require_authentication {
                hashmap! {
                    matches.value_of("username").unwrap().to_owned() =>
                        matches.value_of("password").unwrap().to_owned()
                }
            } else {
                HashMap::new()
            }));

        use tracing_subscriber::Layer;
        let filter = tracing_subscriber::EnvFilter::from_default_env();
        let registry = tracing_subscriber::Registry::default();
        let tracer = if histograms {
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
        let listen_socket: std::net::SocketAddr = listen_addr.parse().unwrap();

        let listener = rt
            .block_on(tokio::net::TcpListener::bind(&listen_socket))
            .unwrap();

        let log = logger_pls();
        slog::info!(log, "listening on address {}", listen_addr);

        let auto_increments: Arc<RwLock<HashMap<String, AtomicUsize>>> = Arc::default();
        let query_cache: Arc<RwLock<HashMap<SelectStatement, String>>> = Arc::default();

        let mut zk_auth = ZookeeperAuthority::new(&format!("{}/{}", zk_addr, deployment)).unwrap();
        zk_auth.log_with(log.clone());

        slog::debug!(log, "Connecting to Noria...",);
        let ch = rt.block_on(ControllerHandle::new(zk_auth)).unwrap();
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

        while let Some(Ok(s)) = rt.block_on(listener.next()) {
            let ch = ch.clone();
            let (auto_increments, query_cache) = (auto_increments.clone(), query_cache.clone());
            let mysql_url = mysql_url.clone();
            let mut connection_handler = self.connection_handler.clone();
            let r = region.clone();
            let fut = async move {
                let connection = span!(Level::DEBUG, "connection", addr = ?s.peer_addr().unwrap());
                connection.in_scope(|| debug!("accepted"));

                // initially, our instinct was that constructing this twice (once for reader and
                // once for writer) did not make sense and we should instead call clone() or use an
                // Arc<RwLock<NoriaConnector>> as both the reader and writer. however, after more
                // thought, there is no benefit to sharing any implentation between the two. the
                // only potential shared state is the query_cache, however, the set of queries
                // handles by a reader and writer are disjoint
                let reader = NoriaConnector::new(
                    ch.clone(),
                    auto_increments.clone(),
                    query_cache.clone(),
                    r.clone(),
                )
                .await;

                let _g = connection.enter();

                let writer: Writer<ZookeeperAuthority> = if mysql_url.is_some() {
                    let url = mysql_url.unwrap();
                    let writer = MySqlConnector::new(url).await;

                    Writer::MySqlConnector(writer)
                } else {
                    let writer =
                        NoriaConnector::new(ch, auto_increments, query_cache, r.clone()).await;
                    Writer::NoriaConnector(writer)
                };

                let b = BackendBuilder::new()
                    .sanitize(sanitize)
                    .static_responses(static_responses)
                    .writer(writer)
                    .reader(reader)
                    .slowlog(slowlog)
                    .permissive(permissive)
                    .users(users.clone())
                    .require_authentication(require_authentication)
                    .build();

                connection_handler.process_connection(s, b).await;

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
