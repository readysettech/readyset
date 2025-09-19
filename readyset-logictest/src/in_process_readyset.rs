use std::{
    collections::HashMap,
    sync::{atomic::AtomicUsize, Arc},
    time::{Duration, SystemTime},
};

use anyhow::anyhow;
use database_utils::{
    DatabaseConnection, DatabaseType, DatabaseURL, QueryableConnection as _, TlsMode,
    UpstreamConfig,
};
use itertools::Itertools as _;
use mysql_srv::MySqlIntermediary;
use readyset_adapter::{
    backend::{noria_connector::ReadBehavior, MigrationMode, NoriaConnector},
    query_status_cache::QueryStatusCache,
    shallow_refresh_pool::ShallowRefreshPool,
    upstream_database::LazyUpstream,
    BackendBuilder, ReadySetStatusReporter, UpstreamDatabase,
};
use readyset_client::ReadySetHandle;
use readyset_data::{
    upstream_system_props::{init_system_props, UpstreamSystemProperties, DEFAULT_TIMEZONE_NAME},
    DfValue,
};
use readyset_mysql::{MySqlQueryHandler, MySqlUpstream};
use readyset_psql::{PostgreSqlQueryHandler, PostgreSqlUpstream};
use readyset_server::{Builder, ReuseConfigType};
use readyset_shallow::CacheManager;
use readyset_sql::{ast::Relation, Dialect};
use readyset_util::{
    shared_cache::SharedCache,
    shutdown::{self, ShutdownSender},
};
use schema_catalog::SchemaCatalogSynchronizer;
use tokio::sync::RwLock;

use crate::runner::RunOptions;

async fn start_noria_server(
    run_opts: &RunOptions,
    authority: Arc<readyset_server::Authority>,
) -> (readyset_server::Handle, ShutdownSender) {
    let mut retry: usize = 0;
    loop {
        retry += 1;

        let mut builder = Builder::for_tests();
        builder.set_mixed_comparisons(true);
        builder.set_straddled_joins(true);
        builder.set_post_lookup(true);
        builder.set_topk(true);
        builder.set_parsing_preset(run_opts.parsing_preset);
        builder.set_dialect(run_opts.database_type.into());

        if run_opts.enable_reuse {
            builder.set_reuse(Some(ReuseConfigType::Finkelstein))
        }

        if let Some(replication_url) = &run_opts.replication_url {
            builder.set_cdc_db_url(replication_url);
            builder.set_upstream_db_url(replication_url);
        }

        let persistence = readyset_server::PersistenceParameters {
            mode: readyset_server::DurabilityMode::DeleteOnExit,
            ..Default::default()
        };

        builder.set_persistence(persistence);

        let (mut noria, shutdown_tx) = match builder.start(Arc::clone(&authority)).await {
            Ok(builder) => builder,
            Err(err) => {
                // This can error out if there are too many open files, but if we wait a bit
                // they will get closed (macOS problem)
                if retry > 100 {
                    panic!("{err:?}")
                }
                tokio::time::sleep(Duration::from_millis(1000)).await;
                continue;
            }
        };
        noria.backend_ready().await;
        return (noria, shutdown_tx);
    }
}

async fn setup_adapter(
    run_opts: &RunOptions,
    authority: Arc<readyset_server::Authority>,
    migration_mode: MigrationMode,
) -> (tokio::task::JoinHandle<()>, ShutdownSender, DatabaseURL) {
    let database_type = run_opts.database_type;
    let replication_url = run_opts.replication_url.clone();
    let auto_increments: Arc<RwLock<HashMap<Relation, AtomicUsize>>> = Arc::default();
    let view_name_cache = SharedCache::new();
    let view_cache = SharedCache::new();
    let mut retry: usize = 0;
    let listener = loop {
        retry += 1;
        match tokio::net::TcpListener::bind("127.0.0.1:0").await {
            Ok(listener) => break listener,
            Err(err) => {
                if retry > 100 {
                    panic!("{err:?}")
                }
                tokio::time::sleep(Duration::from_millis(1000)).await
            }
        }
    };
    let addr = listener.local_addr().unwrap();

    let mut rh = ReadySetHandle::new(authority.clone()).await;

    let (schema_catalog_synchronizer, schema_catalog) =
        SchemaCatalogSynchronizer::new(rh.clone(), Duration::from_millis(100));
    let (shutdown_tx, shutdown_rx) = shutdown::channel();
    tokio::spawn(schema_catalog_synchronizer.run(shutdown_rx));

    let adapter_rewrite_params = rh.adapter_rewrite_params().await.unwrap();
    let adapter_start_time = SystemTime::now();
    let parsing_preset = run_opts.parsing_preset;

    let task = tokio::spawn(async move {
        let (s, _) = listener.accept().await.unwrap();

        let noria = NoriaConnector::new(
            rh.clone(),
            auto_increments,
            view_name_cache.new_local(),
            view_cache.new_local(),
            ReadBehavior::Blocking,
            match database_type {
                DatabaseType::MySQL => readyset_data::Dialect::DEFAULT_MYSQL,
                DatabaseType::PostgreSQL => readyset_data::Dialect::DEFAULT_POSTGRESQL,
            },
            match database_type {
                DatabaseType::MySQL => readyset_sql::Dialect::MySQL,
                DatabaseType::PostgreSQL => readyset_sql::Dialect::PostgreSQL,
            },
            match database_type {
                DatabaseType::MySQL if replication_url.is_some() => vec!["noria".into()],
                DatabaseType::PostgreSQL if replication_url.is_some() => {
                    vec!["noria".into(), "public".into()]
                }
                _ => Default::default(),
            },
            adapter_rewrite_params,
        )
        .await;
        let query_status_cache: &'static _ = Box::leak(Box::new(QueryStatusCache::new()));

        macro_rules! make_backend {
            ($upstream:ty, $handler:ty, $dialect:expr $(,)?) => {{
                // cannot use .await inside map
                #[allow(clippy::manual_map)]
                let upstream = match &replication_url {
                    Some(url) => Some(
                        <LazyUpstream<$upstream> as UpstreamDatabase>::connect(
                            UpstreamConfig::from_url(url),
                            None,
                            None,
                        )
                        .await
                        .unwrap(),
                    ),
                    None => None,
                };

                let status_reporter = ReadySetStatusReporter::new(
                    replication_url
                        .as_ref()
                        .map(|url| UpstreamConfig::from_url(url))
                        .unwrap_or_default(),
                    Some(rh),
                    Default::default(),
                    authority.clone(),
                    Vec::new(),
                );
                let shallow = Arc::new(CacheManager::new(None));
                let shallow_refresh_pool =
                    if let Some(config) = replication_url.as_ref().map(UpstreamConfig::from_url) {
                        Some(ShallowRefreshPool::<LazyUpstream<$upstream>>::new(
                            &tokio::runtime::Handle::current(),
                            &config,
                            100,
                        ))
                    } else {
                        None
                    };
                BackendBuilder::new()
                    .require_authentication(false)
                    .dialect($dialect)
                    .parsing_preset(parsing_preset)
                    .migration_mode(migration_mode)
                    .build::<_, $handler>(
                        noria,
                        upstream,
                        authority,
                        query_status_cache,
                        schema_catalog,
                        status_reporter,
                        adapter_start_time,
                        shallow,
                        shallow_refresh_pool,
                    )
            }};
        }

        match database_type {
            DatabaseType::MySQL => MySqlIntermediary::run_on_tcp(
                readyset_mysql::Backend {
                    noria: make_backend!(MySqlUpstream, MySqlQueryHandler, Dialect::MySQL,),
                    enable_statement_logging: false,
                },
                s,
                false,
                None,
                TlsMode::Optional,
            )
            .await
            .unwrap(),
            DatabaseType::PostgreSQL => {
                psql_srv::run_backend(
                    readyset_psql::Backend::new(make_backend!(
                        PostgreSqlUpstream,
                        PostgreSqlQueryHandler,
                        Dialect::PostgreSQL,
                    )),
                    s,
                    false,
                    None,
                    TlsMode::Optional,
                )
                .await
            }
        }
    });

    (
        task,
        shutdown_tx,
        match database_type {
            DatabaseType::MySQL => mysql_async::OptsBuilder::default()
                .tcp_port(addr.port())
                .prefer_socket(false)
                .into(),
            DatabaseType::PostgreSQL => {
                let mut config = tokio_postgres::Config::default();
                config.host("localhost");
                config.port(addr.port());
                config.dbname("noria");
                config.into()
            }
        },
    )
}

pub(crate) async fn start_readyset(
    run_opts: &RunOptions,
    out_of_band_migration: bool,
) -> (
    readyset_server::Handle,
    ShutdownSender,
    ShutdownSender,
    tokio::task::JoinHandle<()>,
    DatabaseURL,
) {
    // LocalAuthority::new() is infallible, so unwrap is safe here
    let authority = Arc::new(
        readyset_client::consensus::AuthorityType::Local
            .to_authority("", "logictest")
            .expect("LocalAuthority creation cannot fail"),
    );
    let (noria_handle, server_shutdown_tx) = start_noria_server(run_opts, authority.clone()).await;
    let migration_mode = if out_of_band_migration {
        MigrationMode::OutOfBand
    } else {
        MigrationMode::InRequestPath
    };
    let (adapter_task, adapter_shutdown_tx, db_url) =
        setup_adapter(run_opts, authority, migration_mode).await;
    (
        noria_handle,
        server_shutdown_tx,
        adapter_shutdown_tx,
        adapter_task,
        db_url,
    )
}

pub(crate) async fn update_system_timezone(conn: &mut DatabaseConnection) -> anyhow::Result<()> {
    let timezone_name = if matches!(conn, DatabaseConnection::PostgreSQL(..)) {
        let res: Vec<Vec<DfValue>> = conn.simple_query("show timezone").await?.try_into()?;
        if let Some(row) = res.into_iter().at_most_one()? {
            let val = row.into_iter().at_most_one()?;
            match &val {
                Some(v) if v.is_string() => v.as_str().unwrap(),
                _ => DEFAULT_TIMEZONE_NAME,
            }
            .into()
        } else {
            DEFAULT_TIMEZONE_NAME.into()
        }
    } else {
        // Have yet to implement system timezone support for MySQL
        DEFAULT_TIMEZONE_NAME.into()
    };
    init_system_props(&UpstreamSystemProperties {
        timezone_name,
        ..Default::default()
    })
    .map_err(|e| anyhow!(e))
}

pub(crate) fn might_be_timezone_changing_statement(
    conn: &mut DatabaseConnection,
    stmt: &str,
) -> bool {
    let stmt = stmt.to_lowercase();
    stmt.contains("set ")
        && if matches!(conn, DatabaseConnection::PostgreSQL(..)) {
            stmt.contains("timezone")
        } else {
            stmt.contains("time_zone")
        }
}
