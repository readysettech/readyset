use std::collections::HashMap;
use std::env;
use std::net::TcpListener;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, Barrier, RwLock};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use msql_srv::MysqlIntermediary;
use nom_sql::SelectStatement;
use noria_client::backend::mysql_connector::MySqlConnector;
use noria_client::backend::noria_connector::NoriaConnector;
use noria_client::backend::{BackendBuilder, Reader};
use noria_server::{Builder, ControllerHandle, ZookeeperAuthority};
use slog::{debug, o};
use zookeeper::{WatchedEvent, ZooKeeper, ZooKeeperExt};

// Appends a unique ID to deployment strings, to avoid collisions between tests.
pub struct Deployment {
    name: String,
}

impl Deployment {
    pub fn new(prefix: &str) -> Self {
        let current_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        let name = format!(
            "{}.{}.{}",
            prefix,
            current_time.as_secs(),
            current_time.subsec_nanos()
        );

        Self { name }
    }
}

impl Drop for Deployment {
    fn drop(&mut self) {
        // Remove the ZK data if we created any:
        let zk = ZooKeeper::connect(
            "127.0.0.1:2181",
            Duration::from_secs(3),
            |_: WatchedEvent| {},
        );

        if let Ok(z) = zk {
            let _ = z.delete_recursive(&format!("/{}", self.name));
        }
    }
}

pub fn sleep() {
    thread::sleep(Duration::from_millis(200));
}

pub fn zk_addr() -> String {
    format!(
        "{}:{}",
        env::var("ZOOKEEPER_HOST").unwrap_or_else(|_| "127.0.0.1".into()),
        env::var("ZOOKEEPER_PORT").unwrap_or_else(|_| "2181".into()),
    )
}

pub fn mysql_url() -> String {
    format!(
        "mysql://root:noria@{}:{}/noria",
        env::var("MYSQL_HOST").unwrap_or_else(|_| "127.0.0.1".into()),
        env::var("MYSQL_TCP_PORT").unwrap_or_else(|_| "3306".into()),
    )
}

pub fn setup(
    backend_builder: BackendBuilder,
    deployment: &Deployment,
    fallback: bool,
    partial: bool,
) -> mysql::Opts {
    // Run with VERBOSE=1 for log output.
    let verbose = env::var("VERBOSE")
        .ok()
        .and_then(|v| v.parse().ok())
        .iter()
        .any(|i| i == 1);

    let logger = if verbose {
        noria_server::logger_pls()
    } else {
        slog::Logger::root(slog::Discard, o!())
    };

    let barrier = Arc::new(Barrier::new(2));

    let l = logger.clone();
    let n = deployment.name.clone();
    let b = barrier.clone();
    thread::spawn(move || {
        let mut authority = ZookeeperAuthority::new(&format!("{}/{}", zk_addr(), n)).unwrap();
        let mut builder = Builder::default();
        if !partial {
            builder.disable_partial();
        }
        authority.log_with(l.clone());
        builder.log_with(l);
        if fallback {
            builder.set_replicator_url(mysql_url());
        }
        let rt = tokio::runtime::Runtime::new().unwrap();
        // NOTE(malte): important to assign to a variable here, since otherwise the handle gets
        // dropped immediately and the Noria instance quits.
        let _handle = rt.block_on(builder.start(Arc::new(authority))).unwrap();
        b.wait();
        loop {
            thread::sleep(Duration::from_millis(1000));
        }
    });

    barrier.wait();

    let auto_increments: Arc<RwLock<HashMap<String, AtomicUsize>>> = Arc::default();
    let query_cache: Arc<RwLock<HashMap<SelectStatement, String>>> = Arc::default();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    let mut zk_auth =
        ZookeeperAuthority::new(&format!("{}/{}", zk_addr(), deployment.name)).unwrap();
    zk_auth.log_with(logger.clone());

    debug!(logger, "Connecting to Noria...",);
    let rt = tokio::runtime::Runtime::new().unwrap();
    let ch = rt.block_on(ControllerHandle::new(zk_auth));
    debug!(logger, "Connected!");

    // no need for a barrier here since accept() acts as one
    thread::spawn(move || {
        let (s, _) = listener.accept().unwrap();
        let s = {
            let _guard = rt.handle().enter();
            tokio::net::TcpStream::from_std(s).unwrap()
        };

        let writer = NoriaConnector::new(
            ch.clone(),
            auto_increments.clone(),
            query_cache.clone(),
            None,
        );

        let noria_connector = NoriaConnector::new(ch, auto_increments, query_cache, None);
        let mysql_connector = if fallback {
            Some(rt.block_on(MySqlConnector::new(mysql_url())))
        } else {
            None
        };
        let reader = Reader {
            mysql_connector,
            noria_connector: rt.block_on(noria_connector),
        };

        let backend = backend_builder.build(rt.block_on(writer).into(), reader);

        rt.block_on(MysqlIntermediary::run_on_tcp(backend, s))
            .unwrap();
        drop(rt);
    });

    mysql::OptsBuilder::default().tcp_port(addr.port()).into()
}
