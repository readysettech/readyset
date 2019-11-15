#![feature(box_syntax, box_patterns)]
#![feature(nll)]
#![feature(allow_fail)]

#[macro_use]
extern crate clap;
#[macro_use]
extern crate failure;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate slog;

mod backend;
mod convert;
mod referred_tables;
mod rewrite;
mod schema;
mod utils;

use crate::backend::NoriaBackend;
use futures_util::stream::StreamExt;
use msql_srv::MysqlIntermediary;
use nom_sql::SelectStatement;
use noria::{ControllerHandle, ZookeeperAuthority};
use std::collections::HashMap;
use std::io::{self, BufReader, BufWriter};
use std::sync::atomic::{self, AtomicUsize};
use std::sync::{Arc, RwLock};
use std::thread;

// Just give me a damn terminal logger
// Duplicated from distributary, as the API subcrate doesn't export it.
pub fn logger_pls() -> slog::Logger {
    use slog::Drain;
    use slog::Logger;
    use slog_term::term_full;
    use std::sync::Mutex;
    Logger::root(Mutex::new(term_full()).fuse(), o!())
}

fn main() {
    use clap::{App, Arg};

    let matches = App::new("distributary-mysql")
        .version("0.0.1")
        .about("MySQL shim for Noria.")
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
            Arg::with_name("port")
                .long("port")
                .short("p")
                .default_value("3306")
                .takes_value(true)
                .help("Port to listen on."),
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
        .arg(Arg::with_name("verbose").long("verbose").short("v"))
        .get_matches();

    let deployment = matches.value_of("deployment").unwrap().to_owned();
    assert!(!deployment.contains("-"));

    let port = value_t_or_exit!(matches, "port", u16);
    let trace_every = if matches.is_present("trace") {
        Some(value_t_or_exit!(matches, "trace", usize))
    } else {
        None
    };
    let slowlog = matches.is_present("slowlog");
    let zk_addr = matches.value_of("zk_addr").unwrap().to_owned();
    let sanitize = !matches.is_present("no-sanitize");
    let static_responses = !matches.is_present("no-static-responses");

    let s = tracing_subscriber::fmt::format::Format::default()
        .with_timer(tracing_subscriber::fmt::time::Uptime::default());
    let s = tracing_subscriber::FmtSubscriber::builder()
        .on_event(s)
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .finish();
    let tracer = tracing::Dispatch::new(s);
    let rt = tracing::dispatcher::with_default(&tracer, tokio::runtime::Runtime::new).unwrap();

    let listener = rt
        .block_on(tokio::net::tcp::TcpListener::bind(
            &std::net::SocketAddr::new(std::net::Ipv4Addr::LOCALHOST.into(), port),
        ))
        .unwrap();

    let log = logger_pls();
    info!(log, "listening on port {}", port);

    let auto_increments: Arc<RwLock<HashMap<String, AtomicUsize>>> = Arc::default();
    let query_cache: Arc<RwLock<HashMap<SelectStatement, String>>> = Arc::default();

    let mut zk_auth = ZookeeperAuthority::new(&format!("{}/{}", zk_addr, deployment)).unwrap();
    zk_auth.log_with(log.clone());

    debug!(log, "Connecting to Noria...",);
    let ch = rt.block_on(ControllerHandle::new(zk_auth)).unwrap();
    debug!(log, "Connected!");

    let ctrlc = rt.block_on(async { tokio::net::signal::ctrl_c() }).unwrap();
    let mut listener = futures_util::stream::select(
        listener.incoming(),
        ctrlc.map(|()| Err(io::Error::new(io::ErrorKind::Interrupted, "got ctrl-c"))),
    );
    let primed = Arc::new(atomic::AtomicBool::new(false));
    let ops = Arc::new(atomic::AtomicUsize::new(0));

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

        let builder = thread::Builder::new().name(format!("conn-{}", i));

        let (auto_increments, query_cache, log, primed) = (
            auto_increments.clone(),
            query_cache.clone(),
            log.clone(),
            primed.clone(),
        );

        let ex = rt.executor();
        let ch = ch.clone();
        let ops = ops.clone();

        let jh = builder
            .spawn(move || {
                let (tx, rx) = tokio::sync::oneshot::channel();
                let b = NoriaBackend::new(
                    ex.clone(),
                    ch,
                    auto_increments,
                    query_cache,
                    (ops, trace_every),
                    primed,
                    slowlog,
                    static_responses,
                    sanitize,
                    log,
                );
                ex.spawn(async move {
                    let _ = tx.send(b.await);
                });
                let mut b = futures_executor::block_on(rx).unwrap();

                let rs = s.try_clone().unwrap();
                if let Err(e) =
                    MysqlIntermediary::run_on(&mut b, BufReader::new(rs), BufWriter::new(s))
                {
                    match e.kind() {
                        io::ErrorKind::ConnectionReset | io::ErrorKind::BrokenPipe => {}
                        _ => {
                            panic!("{:?}", e);
                        }
                    }
                }
            })
            .unwrap();
        threads.push(jh);
        i += 1;
    }

    drop(ch);
    info!(log, "Exiting...");

    for t in threads.drain(..) {
        t.join()
            .map_err(|e| e.downcast::<io::Error>().unwrap())
            .unwrap();
    }

    rt.shutdown_on_idle();
}
