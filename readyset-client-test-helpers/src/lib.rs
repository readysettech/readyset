//! Helpers for writing integration tests against adapters that use noria-client

use std::collections::HashMap;
use std::env;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use async_trait::async_trait;
use nom_sql::Relation;
use readyset_adapter::backend::noria_connector::{NoriaConnector, ReadBehavior};
use readyset_adapter::backend::{BackendBuilder, MigrationMode};
use readyset_adapter::query_status_cache::QueryStatusCache;
use readyset_adapter::{Backend, QueryHandler, UpstreamConfig, UpstreamDatabase};
use readyset_client::consensus::{Authority, LocalAuthorityStore};
use readyset_client::ViewCreateRequest;
use readyset_server::{Builder, Handle, LocalAuthority, ReadySetHandle};
use readyset_util::shutdown::ShutdownSender;
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

    const EXPR_DIALECT: readyset_data::Dialect;

    fn connection_opts_with_port(port: u16) -> Self::ConnectionOpts;
    fn url() -> String;

    async fn make_upstream(addr: String) -> Self::Upstream {
        Self::Upstream::connect(UpstreamConfig::from_url(addr), None)
            .await
            .unwrap()
    }

    async fn recreate_database();

    async fn run_backend(backend: Backend<Self::Upstream, Self::Handler>, s: TcpStream);
}

/// A builder for an adapter integration test case.
///
/// Use this struct to configure the creation of an in-process readyset-server instance and (either
/// MySQL or PostgreSQL) adapter for use in integration tests.
pub struct TestBuilder {
    backend_builder: BackendBuilder,
    fallback: bool,
    fallback_url: Option<String>,
    partial: bool,
    wait_for_backend: bool,
    read_behavior: ReadBehavior,
    migration_mode: MigrationMode,
    recreate_database: bool,
    query_status_cache: Option<&'static QueryStatusCache>,
}

impl Default for TestBuilder {
    fn default() -> Self {
        Self::new(BackendBuilder::default().require_authentication(false))
    }
}

impl TestBuilder {
    pub fn new(backend_builder: BackendBuilder) -> Self {
        Self {
            backend_builder,
            fallback: false,
            fallback_url: None,
            partial: true,
            wait_for_backend: true,
            read_behavior: ReadBehavior::Blocking,
            migration_mode: MigrationMode::InRequestPath,
            recreate_database: true,
            query_status_cache: None,
        }
    }

    pub fn fallback(mut self, fallback: bool) -> Self {
        self.fallback = fallback;
        self
    }

    pub fn fallback_url(mut self, fallback_url: String) -> Self {
        self.fallback = true;
        self.fallback_url = Some(fallback_url);
        self
    }

    pub fn partial(mut self, partial: bool) -> Self {
        self.partial = partial;
        self
    }

    pub fn read_behavior(mut self, read_behavior: ReadBehavior) -> Self {
        self.read_behavior = read_behavior;
        self
    }

    pub fn migration_mode(mut self, migration_mode: MigrationMode) -> Self {
        self.migration_mode = migration_mode;
        self
    }

    pub fn recreate_database(mut self, recreate_database: bool) -> Self {
        self.recreate_database = recreate_database;
        self
    }

    pub fn query_status_cache(mut self, query_status_cache: &'static QueryStatusCache) -> Self {
        self.query_status_cache = Some(query_status_cache);
        self
    }

    pub async fn build<A>(self) -> (A::ConnectionOpts, Handle, ShutdownSender)
    where
        A: Adapter + 'static,
    {
        // Run with VERBOSE=1 for log output.
        if env::var("VERBOSE").is_ok() {
            readyset_tracing::init_test_logging();
        }

        let query_status_cache = self
            .query_status_cache
            .unwrap_or_else(|| Box::leak(Box::new(QueryStatusCache::new())));

        let fallback_url = self
            .fallback
            .then(|| self.fallback_url.unwrap_or_else(A::url));

        if self.fallback && self.recreate_database {
            A::recreate_database().await;
        }

        let authority = Arc::new(Authority::from(LocalAuthority::new_with_store(Arc::new(
            LocalAuthorityStore::new(),
        ))));

        let mut builder = Builder::for_tests();
        builder.set_allow_topk(true);
        builder.set_allow_paginate(true);
        if !self.partial {
            builder.disable_partial();
        }

        if let Some(f) = &fallback_url {
            builder.set_replication_url(f.clone());
        }
        let (mut handle, shutdown_tx) = builder.start(authority.clone()).await.unwrap();
        if self.wait_for_backend {
            handle.backend_ready().await;
        }

        let auto_increments: Arc<RwLock<HashMap<Relation, AtomicUsize>>> = Arc::default();
        let query_cache: Arc<RwLock<HashMap<ViewCreateRequest, Relation>>> = Arc::default();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let mut backend_shutdown_rx = shutdown_tx.subscribe();
        tokio::spawn(async move {
            let backend_shutdown_rx_connection = backend_shutdown_rx.clone();
            let connection_fut = async move {
                loop {
                    let (s, _) = listener.accept().await.unwrap();
                    let query_cache = query_cache.clone();
                    let backend_builder = self.backend_builder.clone();
                    let auto_increments = auto_increments.clone();
                    let authority = authority.clone();

                    // backend either has upstream or noria writer
                    let mut upstream = if let Some(f) = &fallback_url {
                        Some(A::make_upstream(f.clone()).await)
                    } else {
                        None
                    };

                    let schema_search_path = if let Some(upstream) = &mut upstream {
                        upstream.schema_search_path().await.unwrap()
                    } else {
                        Default::default()
                    };

                    let mut rh = ReadySetHandle::new(authority).await;
                    let server_supports_pagination = rh.supports_pagination().await.unwrap();
                    let noria = NoriaConnector::new(
                        rh,
                        auto_increments,
                        query_cache,
                        self.read_behavior,
                        A::EXPR_DIALECT,
                        schema_search_path,
                        server_supports_pagination,
                    )
                    .await;

                    let backend = backend_builder
                        .dialect(A::DIALECT)
                        .migration_mode(self.migration_mode)
                        .build(noria, upstream, query_status_cache);

                    let mut backend_shutdown_rx_clone = backend_shutdown_rx_connection.clone();
                    tokio::spawn(async move {
                        tokio::select! {
                            _ = A::run_backend(backend, s) => {},
                            _ = backend_shutdown_rx_clone.recv() => {},
                        }
                    });
                }
            };

            tokio::select! {
                _ = connection_fut => {},
                _ = backend_shutdown_rx.recv() => {},
            }
        });

        (
            A::connection_opts_with_port(addr.port()),
            handle,
            shutdown_tx,
        )
    }
}
