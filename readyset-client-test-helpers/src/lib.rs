//! Helpers for writing integration tests against adapters that use noria-client

use std::collections::HashMap;
use std::env;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use database_utils::{DatabaseConnection, DatabaseURL, QueryableConnection, ReplicationServerId};
use readyset_adapter::backend::noria_connector::{NoriaConnector, ReadBehavior};
use readyset_adapter::backend::{BackendBuilder, MigrationMode, QueryDestination, QueryInfo};
use readyset_adapter::query_status_cache::{MigrationStyle, QueryStatusCache};
use readyset_adapter::{
    Backend, QueryHandler, ReadySetStatusReporter, UpstreamConfig, UpstreamDatabase,
    ViewsSynchronizer,
};
use readyset_client::consensus::{Authority, LocalAuthorityStore};
use readyset_data::upstream_system_props::{
    init_system_props, UpstreamSystemProperties, DEFAULT_TIMEZONE_NAME,
};
use readyset_data::Dialect;
use readyset_server::{Builder, DurabilityMode, Handle, LocalAuthority, ReadySetHandle};
use readyset_shallow::CacheManager;
use readyset_sql::ast::Relation;
use readyset_sql_parsing::ParsingPreset;
use readyset_util::shared_cache::SharedCache;
use readyset_util::shutdown::ShutdownSender;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;

pub mod mysql_helpers;
pub mod psql_helpers;

pub use readyset_server::sleep;

static UNIQUE_SERVER_ID_COUNTER: AtomicU32 = AtomicU32::new(0);

/// Generate a unique server ID for this replica to connect to the upstream with. It should be
/// "unique" in the sense that running tests in parallel will not cause conflicts.
///
/// For MySQL, the server ID must be parseable as a u32, and should be unique across all servers
/// in the replication topology. Luckily, 32 bits should be enough to ensure uniqueness in a
/// reasonable test run, exploiting a few facts:
///
/// 1. The theoretical max process ID on Linux is 22 bits.
/// 2. `cargo nextest` runs each test in a separate process.
///
/// Based on (2), we can pretty much just use the PID as the server ID. But to support running
/// under `cargo test`, which runs tests from a given test binary (i.e. crate or test module
/// within a crate) in parallel within a single process.
///
/// 3. We are probably okay with running no more than 2^(32-22)=1024 concurrent tests within a
///    single crate/test module.
///
/// So to give `cargo test` runs a good chance, we set the high bits to the current PID, and
/// have an in-process atomic counter which wraps at 1024 to fill in the low bits.
///
/// For Postgres, we have 43 bytes to play with, but given this scheme, I don't think we need it.
fn unique_server_id() -> ReplicationServerId {
    let pid = std::process::id();
    let counter = UNIQUE_SERVER_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
    let server_id = (pid << 10) | (counter & 0x3FF);
    ReplicationServerId(server_id.to_string())
}

#[async_trait]
pub trait Adapter: Send {
    type ConnectionOpts;
    type Upstream: UpstreamDatabase;
    type Handler: QueryHandler;

    const DIALECT: readyset_sql::Dialect;

    const EXPR_DIALECT: readyset_data::Dialect;

    fn connection_opts_with_port(db_name: Option<&str>, port: u16) -> Self::ConnectionOpts;

    /// Return the URL for connecting to the upstream database for this adapter with the given DB
    /// name.
    fn upstream_url(_db_name: &str) -> String;

    async fn make_upstream(addr: String) -> Self::Upstream {
        Self::Upstream::connect(UpstreamConfig::from_url(addr), None, None)
            .await
            .unwrap()
    }

    async fn recreate_database(db_name: &str);

    async fn run_backend(backend: Backend<Self::Upstream, Self::Handler>, s: TcpStream);
}

#[derive(Debug, Clone, Default)]
enum ReplicationBehavior {
    None,
    Url(String),
    DB(String),
    #[default]
    DefaultURL,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
enum FallbackBehavior {
    #[default]
    NoFallback,
    UseReplicationUpstream,
}

/// A builder for an adapter integration test case.
///
/// Use this struct to configure the creation of an in-process readyset-server instance and (either
/// MySQL or PostgreSQL) adapter for use in integration tests.
pub struct TestBuilder {
    backend_builder: BackendBuilder,
    replicate: ReplicationBehavior,
    fallback: FallbackBehavior,
    partial: bool,
    wait_for_backend: bool,
    read_behavior: ReadBehavior,
    migration_mode: MigrationMode,
    migration_style: MigrationStyle,
    recreate_database: bool,
    query_status_cache: Option<&'static QueryStatusCache>,
    durability_mode: DurabilityMode,
    storage_dir_path: Option<PathBuf>,
    authority: Option<Arc<Authority>>,
    replication_server_id: Option<ReplicationServerId>,
    mixed_comparisons: bool,
    topk: bool,
    parsing_preset: ParsingPreset,
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
            replicate: Default::default(),
            fallback: Default::default(),
            partial: true,
            wait_for_backend: true,
            read_behavior: ReadBehavior::Blocking,
            migration_mode: MigrationMode::InRequestPath,
            migration_style: MigrationStyle::InRequestPath,
            recreate_database: true,
            query_status_cache: None,
            durability_mode: DurabilityMode::DeleteOnExit,
            storage_dir_path: None,
            authority: None,
            replication_server_id: None,
            mixed_comparisons: true,
            topk: false,
            parsing_preset: ParsingPreset::for_tests(),
        }
    }

    pub fn replicate(mut self, default: bool) -> Self {
        self.replicate = if default {
            ReplicationBehavior::DefaultURL
        } else {
            ReplicationBehavior::None
        };
        self
    }

    pub fn replicate_url(mut self, fallback_url: String) -> Self {
        self.replicate = ReplicationBehavior::Url(fallback_url);
        self
    }

    pub fn replicate_db(mut self, db_name: String) -> Self {
        self.replicate = ReplicationBehavior::DB(db_name);
        self
    }

    pub fn fallback(mut self, fallback: bool) -> Self {
        self.fallback = if fallback {
            FallbackBehavior::UseReplicationUpstream
        } else {
            FallbackBehavior::NoFallback
        };
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

    pub fn migration_style(mut self, migration_style: MigrationStyle) -> Self {
        self.migration_style = migration_style;
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

    pub fn replication_server_id(mut self, replication_server_id: String) -> Self {
        self.replication_server_id = Some(ReplicationServerId(replication_server_id));
        self
    }

    pub fn set_mixed_comparisons(mut self, mixed_comparisons: bool) -> Self {
        self.mixed_comparisons = mixed_comparisons;
        self
    }

    pub fn set_topk(mut self, topk: bool) -> Self {
        self.topk = topk;
        self
    }

    pub fn parsing_preset(mut self, parsing_preset: ParsingPreset) -> Self {
        self.parsing_preset = parsing_preset;
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

        let cdc_url_and_db_name = match self.replicate {
            ReplicationBehavior::None => None,
            ReplicationBehavior::Url(url) => {
                let db_name = DatabaseURL::from_str(&url)
                    .unwrap()
                    .db_name()
                    .unwrap()
                    .to_owned();
                Some((url, db_name))
            }
            ReplicationBehavior::DB(db) => Some((A::upstream_url(&db), db.to_owned())),
            ReplicationBehavior::DefaultURL => {
                let db_name = "noria";
                Some((A::upstream_url(db_name), db_name.to_owned()))
            }
        };

        if self.recreate_database {
            if let Some((_, db_name)) = &cdc_url_and_db_name {
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
        builder.set_topk(self.topk);
        builder.set_pagination(false);
        builder.set_mixed_comparisons(self.mixed_comparisons);
        builder.set_parsing_preset(self.parsing_preset);
        builder.set_dialect(A::DIALECT);

        if !self.partial {
            builder.disable_partial();
        }

        if let Some((url, _)) = &cdc_url_and_db_name {
            builder.set_cdc_db_url(url);
            if self.fallback == FallbackBehavior::UseReplicationUpstream {
                builder.set_upstream_db_url(url);
            }
        } else if self.fallback == FallbackBehavior::UseReplicationUpstream {
            panic!("Replication must be configured when using fallback");
        }

        if let Some(id) = self.replication_server_id {
            builder.set_replicator_server_id(id);
        } else {
            builder.set_replicator_server_id(unique_server_id());
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

        let query_status_cache = self.query_status_cache.unwrap_or_else(|| {
            Box::leak(Box::new(
                QueryStatusCache::new().style(self.migration_style),
            ))
        });

        if matches!(self.migration_style, MigrationStyle::Explicit) {
            let rh = handle.clone();
            let expr_dialect = Dialect::DEFAULT_POSTGRESQL;
            let shutdown_rx = shutdown_tx.subscribe();
            let view_name_cache = view_name_cache.clone();
            tokio::spawn(async move {
                let mut views_synchronizer = ViewsSynchronizer::new(
                    rh,
                    query_status_cache,
                    std::time::Duration::from_secs(1),
                    expr_dialect,
                    view_name_cache.new_local(),
                );
                views_synchronizer.run(shutdown_rx).await
            });
        }

        let adapter_start_time = SystemTime::now();

        let mut backend_shutdown_rx = shutdown_tx.subscribe();

        // backend either has upstream or noria writer
        let cdc_url = cdc_url_and_db_name.as_ref().map(|(f, _)| f.clone());
        let cdc_upstream_config = if let Some(f) = &cdc_url {
            UpstreamConfig::from_url(f)
        } else {
            UpstreamConfig::default()
        };
        let shallow = Arc::new(CacheManager::new());
        let shallow_refresh_sender = if cdc_url.is_some() {
            Some(
                Backend::<A::Upstream, A::Handler>::start_shallow_refresh_workers(
                    &tokio::runtime::Handle::current(),
                    &cdc_upstream_config,
                ),
            )
        } else {
            None
        };
        tokio::spawn(async move {
            let backend_shutdown_rx_connection = backend_shutdown_rx.clone();
            let connection_fut = async move {
                loop {
                    let (s, _) = listener.accept().await.unwrap();
                    let backend_builder = self.backend_builder.clone();
                    let auto_increments = auto_increments.clone();

                    let mut cdc_upstream = if let Some(url) = &cdc_url {
                        Some(A::make_upstream(url.clone()).await)
                    } else {
                        None
                    };
                    let mut sys_props = if let Some(cdc_upstream) = &mut cdc_upstream {
                        UpstreamSystemProperties {
                            search_path: cdc_upstream.schema_search_path().await.unwrap(),
                            timezone_name: cdc_upstream.timezone_name().await.unwrap(),
                            lower_case_database_names: cdc_upstream
                                .lower_case_database_names()
                                .await
                                .unwrap(),
                            lower_case_table_names: cdc_upstream
                                .lower_case_table_names()
                                .await
                                .unwrap(),
                            db_version: cdc_upstream.version(),
                        }
                    } else {
                        UpstreamSystemProperties {
                            search_path: UpstreamConfig::default().default_schema_search_path(),
                            timezone_name: UpstreamConfig::default().default_timezone_name(),
                            ..Default::default()
                        }
                    };

                    let fallback_upstream = match self.fallback {
                        FallbackBehavior::NoFallback => None,
                        FallbackBehavior::UseReplicationUpstream => cdc_upstream,
                    };

                    if init_system_props(&sys_props).is_err() {
                        sys_props.timezone_name = DEFAULT_TIMEZONE_NAME.into();
                        init_system_props(&sys_props).expect("Should not fail");
                    }

                    let mut rh = ReadySetHandle::new(authority.clone()).await;
                    let adapter_rewrite_params = rh.adapter_rewrite_params().await.unwrap();
                    let noria = NoriaConnector::new(
                        rh.clone(),
                        auto_increments,
                        view_name_cache.new_local(),
                        view_cache.new_local(),
                        self.read_behavior,
                        A::EXPR_DIALECT,
                        A::DIALECT,
                        sys_props.search_path,
                        adapter_rewrite_params,
                    )
                    .await;

                    let status_reporter = ReadySetStatusReporter::new(
                        cdc_upstream_config.clone(),
                        Some(rh),
                        Default::default(),
                        authority.clone(),
                        Vec::new(),
                    );
                    let backend = backend_builder
                        .dialect(A::DIALECT)
                        .migration_mode(self.migration_mode)
                        .parsing_preset(self.parsing_preset)
                        .build(
                            noria,
                            fallback_upstream,
                            query_status_cache,
                            authority.clone(),
                            status_reporter,
                            adapter_start_time,
                            shallow.clone(),
                            shallow_refresh_sender.clone(),
                        );

                    let mut backend_shutdown_rx_clone = backend_shutdown_rx_connection.clone();
                    tokio::spawn(async move {
                        tokio::select! {
                            biased;
                            _ = backend_shutdown_rx_clone.recv() => {},
                            _ = A::run_backend(backend, s) => {},
                        }
                    });
                }
            };

            tokio::select! {
                biased;
                _ = backend_shutdown_rx.recv() => {},
                _ = connection_fut => {},
            }
        });

        (
            A::connection_opts_with_port(
                cdc_url_and_db_name.as_ref().map(|(_, db)| db.as_str()),
                addr.port(),
            ),
            handle,
            shutdown_tx,
        )
    }
}

#[derive(Debug)]
pub struct ExplainCreateCacheResult {
    pub rewritten_query: String,
    pub supported: String,
}

pub async fn explain_create_cache(
    query: &str,
    conn: &mut DatabaseConnection,
) -> ExplainCreateCacheResult {
    let row = conn
        .simple_query(&format!("EXPLAIN CREATE CACHE FROM {query}"))
        .await
        .unwrap()
        .into_iter()
        .get_single_row()
        .unwrap();

    ExplainCreateCacheResult {
        rewritten_query: row.get(1).unwrap(),
        supported: row.get(2).unwrap(),
    }
}

/// Retrieves where the query executed by parsing the row returned by EXPLAIN LAST STATEMENT.
pub async fn explain_last_statement(conn: &mut DatabaseConnection) -> QueryInfo {
    let row = conn
        .simple_query("EXPLAIN LAST STATEMENT")
        .await
        .unwrap()
        .into_iter()
        .get_single_row()
        .unwrap();

    let destination = QueryDestination::try_from(row.get(0).unwrap().as_str()).unwrap();

    QueryInfo {
        destination,
        noria_error: row.get(1).unwrap(),
    }
}
