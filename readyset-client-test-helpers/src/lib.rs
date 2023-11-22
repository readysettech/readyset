//! Helpers for writing integration tests against adapters that use noria-client

use std::collections::HashMap;
use std::env;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use database_utils::DatabaseURL;
use nom_sql::Relation;
use readyset_adapter::backend::noria_connector::{NoriaConnector, ReadBehavior};
use readyset_adapter::backend::{BackendBuilder, MigrationMode};
use readyset_adapter::query_status_cache::QueryStatusCache;
use readyset_adapter::{
    Backend, QueryHandler, ReadySetStatusReporter, UpstreamConfig, UpstreamDatabase,
};
use readyset_client::consensus::{Authority, LocalAuthorityStore};
use readyset_server::{Builder, DurabilityMode, Handle, LocalAuthority, ReadySetHandle};
use readyset_util::shared_cache::SharedCache;
use readyset_util::shutdown::ShutdownSender;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;

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

    fn connection_opts_with_port(db_name: Option<&str>, port: u16) -> Self::ConnectionOpts;

    /// Return the URL for connecting to the upstream database for this adapter with the given DB
    /// name.
    fn upstream_url(_db_name: &str) -> String;

    async fn make_upstream(addr: String) -> Self::Upstream {
        Self::Upstream::connect(UpstreamConfig::from_url(addr))
            .await
            .unwrap()
    }

    async fn recreate_database(db_name: &str);

    async fn run_backend(backend: Backend<Self::Upstream, Self::Handler>, s: TcpStream);
}

#[derive(Debug, Clone, Default)]
enum Fallback {
    #[default]
    None,
    Url(String),
    DB(String),
    DefaultURL,
}

/// A builder for an adapter integration test case.
///
/// Use this struct to configure the creation of an in-process readyset-server instance and (either
/// MySQL or PostgreSQL) adapter for use in integration tests.
pub struct TestBuilder {
    backend_builder: BackendBuilder,
    fallback: Fallback,
    partial: bool,
    wait_for_backend: bool,
    read_behavior: ReadBehavior,
    migration_mode: MigrationMode,
    recreate_database: bool,
    query_status_cache: Option<&'static QueryStatusCache>,
    durability_mode: DurabilityMode,
    storage_dir_path: Option<PathBuf>,
    authority: Option<Arc<Authority>>,
    replication_server_id: Option<u32>,
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
            fallback: Default::default(),
            partial: true,
            wait_for_backend: true,
            read_behavior: ReadBehavior::Blocking,
            migration_mode: MigrationMode::InRequestPath,
            recreate_database: true,
            query_status_cache: None,
            durability_mode: DurabilityMode::MemoryOnly,
            storage_dir_path: None,
            authority: None,
            replication_server_id: None,
        }
    }

    pub fn fallback(mut self, fallback: bool) -> Self {
        self.fallback = if fallback {
            Fallback::DefaultURL
        } else {
            Fallback::None
        };
        self
    }

    pub fn fallback_url(mut self, fallback_url: String) -> Self {
        self.fallback = Fallback::Url(fallback_url);
        self
    }

    pub fn fallback_db(mut self, db_name: String) -> Self {
        self.fallback = Fallback::DB(db_name);
        self
    }

    pub fn partial(mut self, partial: bool) -> Self {
        self.partial = partial;
        self
    }

    pub fn durability_mode(mut self, mode: DurabilityMode) -> Self {
        self.durability_mode = mode;
        self
    }

    pub fn storage_dir_path(mut self, path: PathBuf) -> Self {
        self.storage_dir_path = Some(path);
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

    pub fn authority(mut self, authority: Arc<Authority>) -> Self {
        self.authority = Some(authority);
        self
    }

    pub fn replication_server_id(mut self, replication_server_id: u32) -> Self {
        self.replication_server_id = Some(replication_server_id);
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

        let fallback_url_and_db_name = match self.fallback {
            Fallback::None => None,
            Fallback::Url(url) => {
                let db_name = DatabaseURL::from_str(&url)
                    .unwrap()
                    .db_name()
                    .unwrap()
                    .to_owned();
                Some((url, db_name))
            }
            Fallback::DB(db) => Some((A::upstream_url(&db), db.to_owned())),
            Fallback::DefaultURL => {
                let db_name = "noria";
                Some((A::upstream_url(db_name), db_name.to_owned()))
            }
        };

        if self.recreate_database {
            if let Some((_, db_name)) = &fallback_url_and_db_name {
                A::recreate_database(db_name).await;
            }
        }

        let authority = self.authority.unwrap_or_else(|| {
            Arc::new(Authority::from(LocalAuthority::new_with_store(Arc::new(
                LocalAuthorityStore::new(),
            ))))
        });

        let mut builder = Builder::for_tests();
        let persistence = readyset_server::PersistenceParameters {
            mode: self.durability_mode,
            storage_dir: self.storage_dir_path,
            ..Default::default()
        };
        builder.set_persistence(persistence);
        builder.set_allow_topk(true);
        builder.set_allow_paginate(true);
        builder.set_allow_mixed_comparisons(true);
        if !self.partial {
            builder.disable_partial();
        }

        if let Some((f, _)) = &fallback_url_and_db_name {
            builder.set_replication_url(f.clone());
        }

        if let Some(id) = self.replication_server_id {
            builder.set_replicator_server_id(id);
        }

        let (mut handle, shutdown_tx) = builder.start(authority.clone()).await.unwrap();
        if self.wait_for_backend {
            handle.backend_ready().await;
        }

        let auto_increments: Arc<RwLock<HashMap<Relation, AtomicUsize>>> = Arc::default();
        let view_name_cache = SharedCache::new();
        let view_cache = SharedCache::new();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let mut backend_shutdown_rx = shutdown_tx.subscribe();
        let fallback_url = fallback_url_and_db_name.as_ref().map(|(f, _)| f.clone());
        tokio::spawn(async move {
            let backend_shutdown_rx_connection = backend_shutdown_rx.clone();
            let connection_fut = async move {
                loop {
                    let (s, _) = listener.accept().await.unwrap();
                    let backend_builder = self.backend_builder.clone();
                    let auto_increments = auto_increments.clone();

                    // backend either has upstream or noria writer
                    let mut upstream_config = UpstreamConfig::default();
                    let mut upstream = if let Some(f) = &fallback_url {
                        upstream_config = UpstreamConfig::from_url(f);
                        Some(A::make_upstream(f.clone()).await)
                    } else {
                        None
                    };

                    let schema_search_path = if let Some(upstream) = &mut upstream {
                        upstream.schema_search_path().await.unwrap()
                    } else {
                        Default::default()
                    };

                    let mut rh = ReadySetHandle::new(authority.clone()).await;
                    let server_supports_pagination = rh.supports_pagination().await.unwrap();
                    let noria = NoriaConnector::new(
                        rh.clone(),
                        auto_increments,
                        view_name_cache.new_local(),
                        view_cache.new_local(),
                        self.read_behavior,
                        A::EXPR_DIALECT,
                        A::DIALECT,
                        schema_search_path,
                        server_supports_pagination,
                    )
                    .await;

                    let status_reporter = ReadySetStatusReporter::new(
                        upstream_config,
                        Some(rh),
                        Default::default(),
                        authority.clone(),
                    );
                    let backend = backend_builder
                        .dialect(A::DIALECT)
                        .migration_mode(self.migration_mode)
                        .build(
                            noria,
                            upstream,
                            query_status_cache,
                            authority.clone(),
                            status_reporter,
                        );

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
            A::connection_opts_with_port(
                fallback_url_and_db_name.as_ref().map(|(_, db)| db.as_str()),
                addr.port(),
            ),
            handle,
            shutdown_tx,
        )
    }
}
