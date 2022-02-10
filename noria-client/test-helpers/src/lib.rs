//! Helpers for writing integration tests against adapters that use noria-client

use std::collections::HashMap;
use std::env;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use async_trait::async_trait;
use nom_sql::SelectStatement;
use noria::consensus::{Authority, LocalAuthorityStore};
use noria_client::backend::noria_connector::NoriaConnector;
use noria_client::backend::{BackendBuilder, MigrationMode};
use noria_client::query_status_cache::QueryStatusCache;
use noria_client::{Backend, QueryHandler, UpstreamDatabase};
use noria_server::{Builder, ControllerHandle, Handle, LocalAuthority};
use tokio::net::{TcpListener, TcpStream};

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

    async fn make_upstream(addr: String) -> Self::Upstream {
        Self::Upstream::connect(addr).await.unwrap()
    }

    async fn recreate_database();

    async fn run_backend(backend: Backend<Self::Upstream, Self::Handler>, s: TcpStream);
}

/// Runs noria-server and noria-mysql within the process. If `fallback` is true, both will use the
/// `A::url()` as the connection string. This behavior is typically used for tests.
pub async fn setup<A>(
    backend_builder: BackendBuilder,
    fallback: bool,
    partial: bool,
    wait_for_backend: bool,
) -> (A::ConnectionOpts, Handle)
where
    A: Adapter + 'static,
{
    let query_status_cache = Arc::new(QueryStatusCache::new());
    setup_inner::<A>(
        backend_builder,
        if fallback { Some(A::url()) } else { None },
        partial,
        wait_for_backend,
        query_status_cache,
        MigrationMode::InRequestPath,
        true,
        false,
    )
    .await
}

/// Runs noria-server and noria-mysql within the process. The configuration used is meant to most
/// similarly match production. Useful for use in tools like benchmarks to get a better picture of
/// expected prod performance.
///
/// This uses `OutOfBand` migrations and as a result requires `CREATE CACHED QUERY` statements to
/// migrate queries in Noria, using this in tests is usually not preferred.
///
/// `setup_like_prod_with_handle` returns a [`ConnectionOpts`] which may be used to connect with
/// the adapter associated with this setup via parameters for the adapter type `A`'s respective
/// client. i.e. with a mysql_async client for the MySQLAdapter adapter type.
/// ```ignore
/// let (opts, _handle) = setup_like_prod_with_handle::<MySQLAdapter>(
///     backend_builder,
///     Some("mysql://root:password@noria:3306/db"),
///     true,
///     true
/// );
/// let client = mysql_async::Conn::new(opts).await.unwrap();
/// ```
pub async fn setup_like_prod_with_handle<A>(
    backend_builder: BackendBuilder,
    fallback: Option<String>,
    wait_for_backend: bool,
    recreate_database: bool,
) -> (A::ConnectionOpts, Handle)
where
    A: Adapter + 'static,
{
    let query_status_cache = Arc::new(QueryStatusCache::new());
    setup_inner::<A>(
        backend_builder,
        fallback,
        true, // partial
        wait_for_backend,
        query_status_cache,
        MigrationMode::OutOfBand, // Must use CREATE CACHED QUERY to migrate queries.
        recreate_database,
        true, // Allow unsupported set for testing.
    )
    .await
}

/// Run noria-server and noria-mysql within the process. If using a out of process fallback
/// database, `fallback` should be passed the connection string.
#[allow(clippy::too_many_arguments)]
pub async fn setup_inner<A>(
    backend_builder: BackendBuilder,
    fallback: Option<String>,
    partial: bool,
    wait_for_backend: bool,
    query_status_cache: Arc<QueryStatusCache>,
    mode: MigrationMode,
    recreate_database: bool,
    allow_unsupported_set: bool,
) -> (A::ConnectionOpts, Handle)
where
    A: Adapter + 'static,
{
    // Run with VERBOSE=1 for log output.
    if env::var("VERBOSE").is_ok() {
        readyset_logging::init_test_logging();
    }

    if fallback.is_some() && recreate_database {
        A::recreate_database().await;
    }

    let authority = Arc::new(Authority::from(LocalAuthority::new_with_store(Arc::new(
        LocalAuthorityStore::new(),
    ))));

    let mut builder = Builder::for_tests();
    builder.set_allow_topk(true);
    builder.set_allow_paginate(true);
    if !partial {
        builder.disable_partial();
    }

    if let Some(f) = fallback.as_ref() {
        builder.set_replicator_url(f.clone());
    }
    let mut handle = builder.start(authority.clone()).await.unwrap();
    if wait_for_backend {
        handle.backend_ready().await;
    }

    let auto_increments: Arc<RwLock<HashMap<String, AtomicUsize>>> = Arc::default();
    let query_cache: Arc<RwLock<HashMap<SelectStatement, String>>> = Arc::default();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        loop {
            let (s, _) = listener.accept().await.unwrap();
            let query_cache = query_cache.clone();
            let backend_builder = backend_builder.clone();
            let query_status_cache = query_status_cache.clone();
            let auto_increments = auto_increments.clone();
            let authority = authority.clone();

            let ch = ControllerHandle::new(authority).await;
            let noria = NoriaConnector::new(ch, auto_increments, query_cache, None).await;
            // backend either has upstream or noria writer
            let upstream = if let Some(f) = fallback.as_ref() {
                Some(A::make_upstream(f.clone()).await)
            } else {
                None
            };

            let backend = backend_builder
                .dialect(A::DIALECT)
                .mirror_ddl(A::MIRROR_DDL)
                .migration_mode(mode)
                .allow_unsupported_set(allow_unsupported_set)
                .build(noria, upstream, query_status_cache);

            tokio::spawn(A::run_backend(backend, s));
        }
    });

    (A::connection_opts_with_port(addr.port()), handle)
}
