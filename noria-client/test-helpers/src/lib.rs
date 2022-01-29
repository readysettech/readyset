//! Helpers for writing integration tests against adapters that use noria-client

use std::collections::HashMap;
use std::env;
use std::net::TcpListener;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use async_trait::async_trait;
use nom_sql::SelectStatement;
use noria::consensus::Authority;
use noria::consensus::LocalAuthorityStore;
use noria_client::backend::noria_connector::NoriaConnector;
use noria_client::backend::{BackendBuilder, MigrationMode};
use noria_client::query_status_cache::QueryStatusCache;
use noria_client::{Backend, QueryHandler, UpstreamDatabase};
use noria_server::{Builder, ControllerHandle, LocalAuthority};
use tokio::net::TcpStream;

#[cfg(feature = "mysql")]
pub mod mysql_helpers;
#[cfg(feature = "postgres")]
pub mod psql_helpers;

pub async fn sleep() {
    tokio::time::sleep(Duration::from_millis(200)).await;
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

    async fn recreate_database();

    async fn run_backend(backend: Backend<Self::Upstream, Self::Handler>, s: TcpStream);
}

pub async fn setup<A>(
    backend_builder: BackendBuilder,
    fallback: bool,
    partial: bool,
    wait_for_backend: bool,
) -> A::ConnectionOpts
where
    A: Adapter + 'static,
{
    let query_status_cache = Arc::new(QueryStatusCache::new());
    setup_inner::<A>(
        backend_builder,
        fallback,
        partial,
        wait_for_backend,
        query_status_cache,
        MigrationMode::InRequestPath,
    )
    .await
}

pub async fn setup_inner<A>(
    backend_builder: BackendBuilder,
    fallback: bool,
    partial: bool,
    wait_for_backend: bool,
    query_status_cache: Arc<QueryStatusCache>,
    mode: MigrationMode,
) -> A::ConnectionOpts
where
    A: Adapter + 'static,
{
    // Run with VERBOSE=1 for log output.
    if env::var("VERBOSE").is_ok() {
        readyset_logging::init_test_logging();
    }

    if fallback {
        A::recreate_database().await;
    }

    let authority = Arc::new(Authority::from(LocalAuthority::new_with_store(Arc::new(
        LocalAuthorityStore::new(),
    ))));

    let mut noria_handle = {
        let authority = Arc::clone(&authority);
        let mut builder = Builder::for_tests();
        builder.set_allow_topk(true);
        if !partial {
            builder.disable_partial();
        }
        if fallback {
            builder.set_replicator_url(A::url());
        }
        let mut handle = builder.start(authority).await.unwrap();
        if wait_for_backend {
            handle.backend_ready().await;
        }
        handle
    };

    let auto_increments: Arc<RwLock<HashMap<String, AtomicUsize>>> = Arc::default();
    let query_cache: Arc<RwLock<HashMap<SelectStatement, String>>> = Arc::default();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        let (s, _) = listener.accept().unwrap();

        let fut = async move {
            let s = TcpStream::from_std(s).unwrap();

            let ch = ControllerHandle::new(authority).await;
            let noria = NoriaConnector::new(ch, auto_increments, query_cache, None).await;
            // backend either has upstream or noria writer
            let upstream = if fallback {
                Some(A::make_upstream().await)
            } else {
                None
            };

            let backend = backend_builder
                .dialect(A::DIALECT)
                .mirror_ddl(A::MIRROR_DDL)
                .migration_mode(mode)
                .build(noria, upstream, query_status_cache);

            A::run_backend(backend, s).await;
            noria_handle.shutdown();
            noria_handle.wait_done().await;
        };
        tokio::spawn(fut);
    });

    A::connection_opts_with_port(addr.port())
}
