#![feature(box_syntax, box_patterns)]
#![feature(nll)]
#![feature(allow_fail)]
#![feature(drain_filter)]

#[macro_use]
extern crate clap;
extern crate anyhow;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate tracing;

mod backend;
mod convert;
mod referred_tables;
mod rewrite;
mod schema;
mod utils;

use crate::backend::mysql_connector::MySqlConnector;
use crate::backend::noria_connector::NoriaConnector;
use futures_util::future::FutureExt;
use futures_util::stream::StreamExt;
use maplit::hashmap;
use msql_srv::MysqlIntermediary;
use nom_sql::SelectStatement;
use noria::{ControllerHandle, ZookeeperAuthority};
use std::collections::HashMap;
use std::io::{self, BufReader, BufWriter};
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, RwLock};
use std::thread;
use tracing::Level;

use crate::backend::Writer;
use backend::Backend;

// Just give me a damn terminal logger
// Duplicated from distributary, as the API subcrate doesn't export it.
pub fn logger_pls() -> slog::Logger {
    use slog::Drain;
    use slog::Logger;
    use slog_term::term_full;
    use std::sync::Mutex;
    Logger::root(Mutex::new(term_full()).fuse(), slog::o!())
}

fn main() {
    use clap::{App, Arg};

    let matches = App::new("distributary-mysql")
        .version("0.0.1")
        .about("MySQL shim for Noria.")
        .arg(
            Arg::with_name("address")
                .short("a")
                .long("address")
                .takes_value(true)
                .default_value("127.0.0.1:3306")
                .help("IP:PORT to listen on"),
        )
        .arg(
            Arg::with_name("deployment")
                .long("deployment")
                .takes_value(true)
                .required(true)
                .help("Noria deployment ID to attach to."),
        )
        .arg(
            Arg::with_name("zk_addr")
                .long("zookeeper-address")
                .short("z")
                .default_value("127.0.0.1:2181")
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
                .help("Be permissive in queries that the mysql adapter accepts (rather than rejecting parse errors)"),
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
            Arg::with_name("username")
                .long("username")
                .short("u")
                .takes_value(true)
                .default_value("root")
                .help("Allow database connections authenticated as this user")
        )
        .arg(
            Arg::with_name("password")
                .long("password")
                .short("p")
                .takes_value(true)
                .required(true)
                .empty_values(false)
                .help("Password to authenticate database connections with")
        )
        .arg(Arg::with_name("verbose").long("verbose").short("v"))
        .arg(
            Arg::with_name("mysql-url")
                .long("mysql-url")
                .takes_value(true)
                .required(false)
                .help("Host for mysql connection. Should include username and password if nececssary."),
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

    let users: &'static HashMap<String, String> = Box::leak(Box::new(hashmap! {
        matches.value_of("username").unwrap().to_owned() =>
            matches.value_of("password").unwrap().to_owned()
    }));

    use tracing_subscriber::Layer;
    let filter = tracing_subscriber::EnvFilter::from_default_env();
    let registry = tracing_subscriber::Registry::default();
    let tracer = if histograms {
        use tracing_timing::{Builder, Histogram};
        let s =
            Builder::default().layer(|| Histogram::new_with_bounds(1_000, 100_000_000, 3).unwrap());
        tracing::Dispatch::new(filter.and_then(s).with_subscriber(registry))
    } else {
        use tracing_subscriber::fmt;
        let s = fmt::format::Format::default().with_timer(fmt::time::Uptime::default());
        let s = fmt::Layer::default().event_format(s);
        tracing::Dispatch::new(filter.and_then(s).with_subscriber(registry))
    };
    tracing::dispatcher::set_global_default(tracer.clone()).unwrap();
    let mut rt = tracing::dispatcher::with_default(&tracer, tokio::runtime::Runtime::new).unwrap();
    let listen_socket: std::net::SocketAddr = listen_addr.parse().unwrap();

    let mut listener = rt
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
        listener.incoming(),
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

    let mut threads = Vec::new();
    let mut i = 0;
    while let Some(Ok(s)) = rt.block_on(listener.next()) {
        // one day, when msql-srv is async, this won't be necessary
        let s = {
            use std::os::unix::io::AsRawFd;
            use std::os::unix::io::FromRawFd;
            let s2 = unsafe { std::net::TcpStream::from_raw_fd(s.as_raw_fd()) };
            std::mem::forget(s); // don't drop, which would close
            s2.set_nonblocking(false).unwrap();
            s2
        };
        s.set_nodelay(true).unwrap();

        let connection = span!(Level::DEBUG, "connection", addr = ?s.peer_addr().unwrap());
        connection.in_scope(|| debug!("accepted"));

        let builder = thread::Builder::new().name(format!("conn-{}", i));

        let (auto_increments, query_cache) = (auto_increments.clone(), query_cache.clone());

        let ex = rt.handle().clone();
        let ch = ch.clone();
        let mysql_url = mysql_url.clone();

        let jh = builder
            .spawn(move || {
                // initially, our instinct was that constructing this twice (once for reader and once for writer) did not make sense and we should instead
                // call clone() or use an Arc<RwLock<NoriaConnector>> as both the reader and writer.
                // however, after more thought, there is no benefit to sharing any implentation between the two.
                // the only potential shared state is the query_cache, however, the set of queries handles by a reader and writer are
                // disjoint
                let (reader_sender, reader_reciever) = tokio::sync::oneshot::channel();
                let reader = NoriaConnector::new(
                    ex.clone(),
                    ch.clone(),
                    auto_increments.clone(),
                    query_cache.clone(),
                );
                ex.spawn(async move {
                    reader_sender
                        .send(reader.await)
                        .unwrap_or_else(|_| panic!("Could not send reader"));
                });

                let reader = futures_executor::block_on(reader_reciever).unwrap();
                let _g = connection.enter();

                // there is a lot of duplication between these two arms. It isn't ideal. however, the
                // alternative was implementing Future for the writer on the backend.
                let b: Backend = if let Some(url) = mysql_url {
                    let (writer_sender, writer_reciever) = tokio::sync::oneshot::channel();
                    let connector = MySqlConnector::new(url);
                    ex.spawn(async move {
                        writer_sender
                            .send(connector.await)
                            .unwrap_or_else(|_| panic!("Could not send writer"));
                    });

                    let writer = futures_executor::block_on(writer_reciever).unwrap();

                    Backend::new(
                        sanitize,
                        static_responses,
                        Writer::MySqlConnector(writer),
                        reader,
                        slowlog,
                        permissive,
                        users.clone(),
                    )
                } else {
                    let (writer_sender, writer_reciever) = tokio::sync::oneshot::channel();
                    let connector =
                        NoriaConnector::new(ex.clone(), ch, auto_increments, query_cache);
                    ex.spawn(async move {
                        writer_sender
                            .send(connector.await)
                            .unwrap_or_else(|_| panic!("Could not send writer"));
                    });

                    let writer = futures_executor::block_on(writer_reciever).unwrap();

                    Backend::new(
                        sanitize,
                        static_responses,
                        Writer::NoriaConnector(writer),
                        reader,
                        slowlog,
                        permissive,
                        users.clone(),
                    )
                };

                let rs = s.try_clone().unwrap();
                if let Err(backend::error::Error::IOError(e)) =
                    MysqlIntermediary::run_on(b, BufReader::new(rs), BufWriter::new(s))
                {
                    match e.kind() {
                        io::ErrorKind::ConnectionReset | io::ErrorKind::BrokenPipe => {}
                        _ => {
                            error!(err = ?e, "connection lost");
                            return;
                        }
                    }
                }

                debug!("disconnected");
            })
            .unwrap();
        threads.push(jh);
        i += 1;
    }

    drop(ch);
    slog::info!(log, "Exiting...");

    for t in threads.drain(..) {
        t.join()
            .map_err(|e| e.downcast::<io::Error>().unwrap())
            .unwrap();
    }

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
