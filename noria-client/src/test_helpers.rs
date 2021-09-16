//! Helpers for writing integration tests against adapters that use noria-client

use std::collections::HashMap;
use std::env;
use std::net::TcpListener;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, Barrier, RwLock};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use nom_sql::SelectStatement;
use noria::consensus::Authority;
use noria_server::{Builder, ControllerHandle, ZookeeperAuthority};
use tokio::net::TcpStream;
use zookeeper::{WatchedEvent, ZooKeeper, ZooKeeperExt};

use crate::backend::noria_connector::NoriaConnector;
use crate::backend::BackendBuilder;
use crate::{Backend, QueryHandler, UpstreamDatabase};

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

#[async_trait]
pub trait Adapter: Send {
    type ConnectionOpts;
    type Upstream: UpstreamDatabase;
    type Handler: QueryHandler;

    const DIALECT: nom_sql::Dialect;

    const MIRROR_DDL: bool = false;

    fn connection_opts_with_port(port: u16) -> Self::ConnectionOpts;
    fn url() -> String;

    async fn make_upstream() -> Self::Upstream {
        Self::Upstream::connect(Self::url()).await.unwrap()
    }

    fn recreate_database();

    async fn run_backend(backend: Backend<Self::Upstream, Self::Handler>, s: TcpStream);
}

pub fn setup<A>(
    backend_builder: BackendBuilder,
    deployment: &Deployment,
    fallback: bool,
    partial: bool,
) -> A::ConnectionOpts
where
    A: Adapter,
{
    // Run with VERBOSE=1 for log output.
    if env::var("VERBOSE").is_ok() {
        readyset_logging::init_test_logging();
    }

    if fallback {
        A::recreate_database();
    }

    let barrier = Arc::new(Barrier::new(2));

    let n = deployment.name.clone();
    let b = barrier.clone();
    thread::spawn(move || {
        let mut builder = Builder::for_tests();
        if !partial {
            builder.disable_partial();
        }
        if fallback {
            builder.set_replicator_url(A::url());
        }
        let rt = tokio::runtime::Runtime::new().unwrap();
        // NOTE(malte): important to assign to a variable here, since otherwise the handle gets
        // dropped immediately and the Noria instance quits.
        let _handle = rt.block_on(async {
            let authority = Authority::from(
                ZookeeperAuthority::new(&format!("{}/{}", zk_addr(), n))
                    .await
                    .unwrap(),
            );

            builder.start(Arc::new(authority)).await.unwrap()
        });
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

    let rt = tokio::runtime::Runtime::new().unwrap();
    let ch = rt.block_on(async {
        let zk_auth = Authority::from(
            ZookeeperAuthority::new(&format!("{}/{}", zk_addr(), deployment.name))
                .await
                .unwrap(),
        );

        ControllerHandle::new(zk_auth).await
    });

    // no need for a barrier here since accept() acts as one
    thread::spawn(move || {
        let (s, _) = listener.accept().unwrap();
        let s = {
            let _guard = rt.handle().enter();
            TcpStream::from_std(s).unwrap()
        };

        let noria = rt.block_on(NoriaConnector::new(ch, auto_increments, query_cache, None));

        // backend either has upstream or noria writer
        let upstream = if fallback {
            Some(rt.block_on(A::make_upstream()))
        } else {
            None
        };

        let backend = backend_builder
            .dialect(A::DIALECT)
            .mirror_ddl(A::MIRROR_DDL)
            .build(noria, upstream);

        rt.block_on(A::run_backend(backend, s));
        drop(rt);
    });

    A::connection_opts_with_port(addr.port())
}
