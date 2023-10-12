#![deny(macro_use_extern_crate)]
#![feature(let_chains)]

pub mod mysql;
pub mod psql;
mod query_logger;

use std::collections::HashMap;
use std::fs::remove_dir_all;
use std::io;
use std::marker::Send;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, ToSocketAddrs};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, bail};
use async_trait::async_trait;
use clap::builder::NonEmptyStringValueParser;
use clap::{ArgGroup, Parser, ValueEnum};
use crossbeam_skiplist::SkipSet;
use database_utils::{DatabaseType, DatabaseURL, UpstreamConfig};
use failpoint_macros::set_failpoint;
use futures_util::future::FutureExt;
use futures_util::stream::StreamExt;
use health_reporter::{HealthReporter as AdapterHealthReporter, State as AdapterState};
use metrics_exporter_prometheus::PrometheusBuilder;
use nom_sql::{Relation, SqlIdentifier};
use readyset_adapter::backend::noria_connector::{NoriaConnector, ReadBehavior};
use readyset_adapter::backend::MigrationMode;
use readyset_adapter::http_router::NoriaAdapterHttpRouter;
use readyset_adapter::metrics_handle::MetricsHandle;
use readyset_adapter::migration_handler::MigrationHandler;
use readyset_adapter::proxied_queries_reporter::ProxiedQueriesReporter;
use readyset_adapter::query_status_cache::{MigrationStyle, QueryStatusCache};
use readyset_adapter::views_synchronizer::ViewsSynchronizer;
use readyset_adapter::{Backend, BackendBuilder, QueryHandler, UpstreamDatabase};
use readyset_alloc::{StdThreadBuildWrapper, ThreadBuildWrapper};
use readyset_alloc_metrics::report_allocator_metrics;
use readyset_client::consensus::AuthorityType;
#[cfg(feature = "failure_injection")]
use readyset_client::failpoints;
use readyset_client::metrics::recorded;
use readyset_client::ReadySetHandle;
use readyset_client_metrics::QueryLogMode;
use readyset_common::ulimit::maybe_increase_nofile_limit;
use readyset_dataflow::Readers;
use readyset_errors::{internal_err, ReadySetError};
use readyset_server::metrics::{CompositeMetricsRecorder, MetricsRecorder};
use readyset_server::worker::readers::{retry_misses, Ack, BlockingRead, ReadRequestHandler};
use readyset_telemetry_reporter::{TelemetryBuilder, TelemetryEvent, TelemetryInitializer};
use readyset_util::futures::abort_on_panic;
use readyset_util::redacted::RedactedString;
use readyset_util::shared_cache::SharedCache;
use readyset_util::shutdown;
use readyset_version::*;
use tokio::net;
use tokio::sync::RwLock;
use tokio::time::{sleep, timeout};
use tokio_stream::wrappers::TcpListenerStream;
use tracing::{debug, debug_span, error, info, info_span, span, warn, Level};
use tracing_futures::Instrument;

// readyset_alloc initializes the global allocator
extern crate readyset_alloc;

/// Timeout to use when connecting to the upstream database
const UPSTREAM_CONNECTION_TIMEOUT: Duration = Duration::from_secs(5);

/// Retry interval to use when attempting to connect to the upstream database
const UPSTREAM_CONNECTION_RETRY_INTERVAL: Duration = Duration::from_secs(1);

#[async_trait]
pub trait ConnectionHandler {
    type UpstreamDatabase: UpstreamDatabase;
    type Handler: QueryHandler;

    async fn process_connection(
        &mut self,
        stream: net::TcpStream,
        backend: Backend<Self::UpstreamDatabase, Self::Handler>,
    );

    /// Return an immediate error to a newly-established connection, then immediately disconnect
    async fn immediate_error(self, stream: net::TcpStream, error_message: String);
}

/// How to behave when receiving unsupported `SET` statements.
///
/// Corresponds to the variants of [`noria_client::backend::UnsupportedSetMode`] that are exposed to
/// the user.
#[derive(Copy, Clone, Debug, PartialEq, Eq, ValueEnum)]
pub enum UnsupportedSetMode {
    /// Return an error to the client (the default)
    Error,
    /// Proxy all subsequent statements to the upstream
    Proxy,
}

impl Default for UnsupportedSetMode {
    fn default() -> Self {
        Self::Error
    }
}

impl FromStr for UnsupportedSetMode {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "error" => Ok(Self::Error),
            "proxy" => Ok(Self::Proxy),
            _ => bail!(
                "Invalid value for unsupoported_set_mode; expected one of \"error\" or \"proxy\""
            ),
        }
    }
}

impl From<UnsupportedSetMode> for readyset_adapter::backend::UnsupportedSetMode {
    fn from(mode: UnsupportedSetMode) -> Self {
        match mode {
            UnsupportedSetMode::Error => Self::Error,
            UnsupportedSetMode::Proxy => Self::Proxy,
        }
    }
}

/// Parse and normalize the given string as an [`IpAddr`]
pub fn resolve_addr(addr: &str) -> anyhow::Result<IpAddr> {
    Ok(format!("{addr}:0")
        .to_socket_addrs()?
        .next()
        .ok_or_else(|| anyhow!("Could not resolve address: {}", addr))?
        .ip())
}

pub struct NoriaAdapter<H>
where
    H: ConnectionHandler,
{
    pub description: &'static str,
    pub default_address: SocketAddr,
    /// Address used to listen for incoming connections
    pub connection_handler: H,
    pub database_type: DatabaseType,
    /// SQL dialect to use when parsing queries
    pub parse_dialect: nom_sql::Dialect,
    /// Expression evaluation dialect to pass to ReadySet for all migration requests
    pub expr_dialect: readyset_data::Dialect,
}

#[derive(Parser, Debug)]
#[clap(group(
    ArgGroup::new("metrics")
        .multiple(true)
        .args(&["prometheus_metrics", "noria_metrics"]),
), version = VERSION_STR_PRETTY)]
#[group(skip)]
pub struct Options {
    /// IP:PORT to listen on
    #[clap(long, short = 'a', env = "LISTEN_ADDRESS")]
    address: Option<SocketAddr>,

    /// ReadySet deployment ID. All nodes in a deployment must have the same deployment ID.
    #[clap(long, env = "DEPLOYMENT", default_value = "tmp-readyset", value_parser = NonEmptyStringValueParser::new(), hide = true)]
    deployment: String,

    /// Database engine protocol to emulate. If omitted, will be inferred from the
    /// `upstream-db-url`
    #[clap(
        long,
        env = "DATABASE_TYPE",
        value_enum,
        required_unless_present("upstream_db_url"),
        hide = true
    )]
    pub database_type: Option<DatabaseType>,

    /// Run ReadySet in standalone mode, running a readyset-server instance within this adapter.
    #[clap(long, env = "STANDALONE", conflicts_with = "embedded_readers")]
    standalone: bool,

    /// The authority to use
    // NOTE: hidden because the value can be derived from `--standalone`
    #[clap(
        long,
        env = "AUTHORITY",
        default_value_if("standalone", "true", Some("standalone")),
        default_value = "consul",
        hide = true
    )]
    authority: AuthorityType,

    /// Authority uri
    // NOTE: `authority_address` should come after `authority` for clap to set default values
    // properly
    #[clap(
        long,
        env = "AUTHORITY_ADDRESS",
        default_value_if("authority", "standalone", Some(".")),
        default_value_if("authority", "consul", Some("127.0.0.1:8500")),
        required = false,
        hide = true
    )]
    authority_address: String,

    /// Log slow queries (> 5ms)
    #[clap(long, hide = true)]
    log_slow: bool,

    /// Don't require authentication for any client connections
    #[clap(long, env = "ALLOW_UNAUTHENTICATED_CONNECTIONS", hide = true)]
    allow_unauthenticated_connections: bool,

    /// Specify the migration mode for ReadySet to use. The default "explicit" mode is the only
    /// non-experimental mode.
    #[clap(long, env = "QUERY_CACHING", default_value = "explicit", hide = true)]
    query_caching: MigrationStyle,

    /// Sets the maximum time in minutes that we will retry migrations for in the
    /// migration handler. If this time is reached, the query will be exclusively
    /// sent to the upstream database.
    ///
    /// Defaults to 15 minutes.
    #[clap(
        long,
        env = "MAX_PROCESSING_MINUTES",
        default_value = "15",
        hide = true
    )]
    max_processing_minutes: u64,

    /// Sets the migration handlers's loop interval in milliseconds.
    #[clap(
        long,
        env = "MIGRATION_TASK_INTERVAL",
        default_value = "20000",
        hide = true
    )]
    migration_task_interval: u64,

    /// IP:PORT to host endpoint for scraping metrics from the adapter.
    #[clap(long, env = "METRICS_ADDRESS", default_value = "0.0.0.0:6034")]
    metrics_address: SocketAddr,

    /// Allow database connections authenticated as this user. Defaults to the username in
    /// --upstream-db-url if not set. Ignored if --allow-unauthenticated-connections is passed
    #[clap(long, env = "ALLOWED_USERNAME", short = 'u', hide = true)]
    username: Option<String>,

    /// Password to authenticate database connections with. Defaults to the password in
    /// --upstream-db-url if not set. Ignored if --allow-unauthenticated-connections is passed
    #[clap(long, env = "ALLOWED_PASSWORD", short = 'p', hide = true)]
    password: Option<RedactedString>,

    /// Enable recording and exposing Prometheus metrics
    #[clap(long, env = "PROMETHEUS_METRICS")]
    prometheus_metrics: bool,

    #[clap(long, hide = true)]
    noria_metrics: bool,

    /// Enable logging queries and execution metrics. This creates a histogram per unique query.
    /// Enabled by default if prometheus-metrics is enabled.
    #[clap(
        long,
        env = "QUERY_LOG_MODE",
        requires = "metrics",
        default_value = "all-queries",
        default_value_if("prometheus_metrics", "true", Some("all-queries")),
        hide = true
    )]
    query_log_mode: QueryLogMode,

    /// IP address to advertise to other ReadySet instances running in the same deployment.
    ///
    /// If not specified, defaults to the value of `address`
    #[clap(long, env = "EXTERNAL_ADDRESS", value_parser = resolve_addr, hide = true)]
    external_address: Option<IpAddr>,

    #[clap(flatten)]
    pub tracing: readyset_tracing::Options,

    /// readyset-psql-specific options
    #[clap(flatten)]
    pub psql_options: psql::Options,

    /// Allow executing, but ignore, unsupported `SET` statements.
    ///
    /// Takes precedence over any value passed to `--unsupported-set-mode`
    #[clap(long, hide = true, env = "ALLOW_UNSUPPORTED_SET", hide = true)]
    allow_unsupported_set: bool,

    /// Configure how ReadySet behaves when receiving unsupported SET statements.
    ///
    /// The possible values are:
    ///
    /// * "error" (default) - return an error to the client
    /// * "proxy" - proxy all subsequent statements
    // NOTE: In order to keep `allow_unsupported_set` hidden, we're keeping these two flags separate
    // and *not* marking them as conflicting with each other.
    #[clap(
        long,
        env = "UNSUPPORTED_SET_MODE",
        default_value = "error",
        hide = true
    )]
    unsupported_set_mode: UnsupportedSetMode,

    // TODO(DAN): require explicit migrations
    /// Specifies the polling interval in seconds for requesting views from the Leader.
    #[clap(long, env = "VIEWS_POLLING_INTERVAL", default_value = "5", hide = true)]
    views_polling_interval: u64,

    /// The time to wait before canceling a migration request. Defaults to 30 minutes.
    #[clap(
        long,
        hide = true,
        env = "MIGRATION_REQUEST_TIMEOUT",
        default_value = "1800000"
    )]
    migration_request_timeout_ms: u64,

    /// The time to wait before canceling a controller request. Defaults to 5 seconds.
    #[clap(long, hide = true, env = "CONTROLLER_TIMEOUT", default_value = "5000")]
    controller_request_timeout_ms: u64,

    /// Specifies the maximum continuous failure time for any given query, in seconds, before
    /// entering into a fallback recovery mode.
    #[clap(
        long,
        hide = true,
        env = "QUERY_MAX_FAILURE_SECONDS",
        default_value = "9223372036854775"
    )]
    query_max_failure_seconds: u64,

    /// Specifies the recovery period in seconds that we enter if a given query fails for the
    /// period of time designated by the query_max_failure_seconds flag.
    #[clap(
        long,
        hide = true,
        env = "FALLBACK_RECOVERY_SECONDS",
        default_value = "0"
    )]
    fallback_recovery_seconds: u64,

    /// Whether to use non-blocking or blocking reads against the cache.
    #[clap(long, env = "NON_BLOCKING_READS", hide = true)]
    non_blocking_reads: bool,

    /// Run ReadySet in embedded readers mode, running reader replicas (and only reader replicas)
    /// in the same process as the adapter
    ///
    /// Should be combined with passing `--no-readers` and `--reader-replicas` with the number of
    /// adapter instances to each server process.
    #[clap(
        long,
        env = "EMBEDDED_READERS",
        conflicts_with = "standalone",
        hide = true
    )]
    embedded_readers: bool,

    #[clap(flatten)]
    server_worker_options: readyset_server::WorkerOptions,

    /// Whether to disable telemetry reporting. Defaults to false.
    #[clap(long, env = "DISABLE_TELEMETRY")]
    disable_telemetry: bool,

    /// Whether we should wait for a failpoint request to the adapters http router, which may
    /// impact startup.
    #[clap(long, hide = true)]
    wait_for_failpoint: bool,

    /// Whether to allow ReadySet to automatically create inlined caches when we receive a CREATE
    /// CACHE command for a query with unsupported placeholders.
    ///
    /// If set, we will create a cache with literals inlined in the unsupported placeholder
    /// positions every time the statement is executed with a new set of parameters.
    #[clap(long, env = "EXPERIMENTAL_PLACEHOLDER_INLINING", hide = true)]
    experimental_placeholder_inlining: bool,

    /// Don't make connections to the upstream aatabase for new client connections.
    ///
    /// If this flag is set queries will never be proxied upstream - even if they are unsupported,
    /// fail to execute, or are run in a transaction.
    #[clap(long, env = "NO_UPSTREAM_CONNECTIONS", hide = true)]
    no_upstream_connections: bool,

    /// If supplied we will clean up assets for the supplied deployment. If an upstream url is
    /// supplied, we will also clean up various assets related to upstream (replication slot, etc.)
    #[clap(long)]
    cleanup: bool,

    /// In standalone or embedded-readers mode, the IP address on which the ReadySet controller
    /// will listen.
    #[clap(long, env = "CONTROLLER_ADDRESS", hide = true)]
    controller_address: Option<IpAddr>,

    /// The number of queries that will be retained and eligible to be returned by `show caches`
    /// and `show proxied queries`.
    #[clap(
        long,
        env = "QUERY_STATUS_CAPACITY",
        default_value = "100000",
        hide = true
    )]
    query_status_capacity: usize,
}

impl Options {
    /// Return the configured database type, either explicitly set by the user or inferred from the
    /// upstream DB URL
    pub fn database_type(&self) -> anyhow::Result<DatabaseType> {
        let infer_from_db_url = |db_url: &str| Ok(db_url.parse::<DatabaseURL>()?.database_type());

        match (
            self.database_type,
            &self.server_worker_options.replicator_config.upstream_db_url,
        ) {
            (None, None) => bail!("One of either --database-type or --upstream-db-url is required"),
            (None, Some(url)) => infer_from_db_url(url),
            (Some(dt), None) => Ok(dt),
            (Some(dt), Some(url)) => {
                let inferred = infer_from_db_url(url)?;
                if dt != inferred {
                    bail!(
                        "Provided --database-type {dt} does not match database type {inferred} for \
                         --upstream-db-url"
                    );
                }
                Ok(dt)
            }
        }
    }
}

async fn connect_upstream<U>(
    upstream_config: UpstreamConfig,
    no_upstream_connections: bool,
) -> Result<Option<U>, U::Error>
where
    U: UpstreamDatabase,
{
    if upstream_config.upstream_db_url.is_some() && !no_upstream_connections {
        set_failpoint!(failpoints::UPSTREAM);
        timeout(UPSTREAM_CONNECTION_TIMEOUT, U::connect(upstream_config))
            .instrument(debug_span!("Connecting to upstream database"))
            .await
            .map_err(|_| internal_err!("Connection timed out").into())
            .and_then(|r| r)
            .map(Some)
    } else {
        Ok(None)
    }
}

/// Spawn a task to query the upstream for its currently-configured schema search path in a loop
/// until it succeeds, returning a lock that will contain the result when it finishes
///
/// NOTE: when we start tracking all configuration parameters, this should be folded into whatever
/// loads those initially
async fn load_schema_search_path<U>(
    upstream_config: UpstreamConfig,
    no_upstream_connections: bool,
) -> Arc<RwLock<Result<Vec<SqlIdentifier>, U::Error>>>
where
    U: UpstreamDatabase,
{
    let try_load = move |upstream_config: UpstreamConfig| async move {
        let upstream =
            connect_upstream::<U>(upstream_config.clone(), no_upstream_connections).await?;

        match upstream {
            Some(mut upstream) => upstream.schema_search_path().await,
            None => Ok(Default::default()),
        }
    };

    // First, try to load once outside the loop
    let e = match try_load(upstream_config.clone()).await {
        Ok(res) => return Arc::new(RwLock::new(Ok(res))),
        Err(error) => {
            warn!(%error, "Loading initial schema search path failed, spawning retry loop");
            error
        }
    };

    // If that fails, spawn a task to keep retrying
    let out = Arc::new(RwLock::new(Err(e)));
    tokio::spawn({
        let out = Arc::clone(&out);
        async move {
            let mut first_loop = true;
            loop {
                if !first_loop {
                    sleep(UPSTREAM_CONNECTION_RETRY_INTERVAL).await;
                }
                first_loop = false;

                let res = try_load(upstream_config.clone()).await;

                if let Ok(ssp) = &res {
                    debug!(?ssp, "Successfully loaded schema search path from upstream");
                }

                let was_ok = res.is_ok();
                *out.write().await = res;
                if was_ok {
                    break;
                }
            }
        }
    });

    out
}

impl<H> NoriaAdapter<H>
where
    H: ConnectionHandler + Clone + Send + Sync + 'static,
{
    pub fn run(&mut self, options: Options) -> anyhow::Result<()> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .with_sys_hooks()
            .enable_all()
            .thread_name("Adapter Runtime")
            .build()?;

        rt.block_on(async { options.tracing.init("adapter", options.deployment.as_ref()) })?;
        info!(?options, "Starting ReadySet adapter");

        if options.standalone {
            maybe_increase_nofile_limit(
                options
                    .server_worker_options
                    .replicator_config
                    .ignore_ulimit_check,
            )?;
        }

        let deployment_dir = options
            .server_worker_options
            .storage_dir
            .clone()
            .unwrap_or_else(|| PathBuf::from("."))
            .join(&options.deployment);

        let upstream_config = options.server_worker_options.replicator_config.clone();

        if options.cleanup {
            info!(?options, "Cleaning up deployment");

            return rt.block_on(async { self.cleanup(upstream_config, deployment_dir).await });
        }

        let users: &'static HashMap<String, String> = Box::leak(Box::new(
            if !options.allow_unauthenticated_connections {
                HashMap::from([{
                    let upstream_url = upstream_config
                        .upstream_db_url
                        .as_ref()
                        .and_then(|s| s.parse::<DatabaseURL>().ok());

                    match (
                        (options.username, options.password),
                        (
                            upstream_url.as_ref().and_then(|url| url.user()),
                            upstream_url.as_ref().and_then(|url| url.password()),
                        ),
                    ) {
                        // --username and --password
                        ((Some(user), Some(pass)), _) => (user, pass.0),
                        // --password, username from url
                        ((None, Some(pass)), (Some(user), _)) => (user.to_owned(), pass.0),
                        // username and password from url
                        (_, (Some(user), Some(pass))) => (user.to_owned(), pass.to_owned()),
                        _ => {
                            if upstream_url.is_some() {
                                bail!(
                                    "Failed to infer ReadySet username and password from \
                                    upstream DB URL. Please ensure they are present and \
                                    correctly formatted as follows: \
                                    <protocol>://<username>:<password>@<address>[:<port>][/<database>] \
                                    You can also configure ReadySet to accept credentials \
                                    different from those of your upstream database via \
                                    --username/-u and --password/-p, or use \
                                    --allow-unauthenticated-connections."
                                )
                            } else {
                                bail!(
                                    "Must specify --username/-u and --password/-p if one of \
                                    --allow-unauthenticated-connections or --upstream-db-url is not \
                                    passed"
                                )
                            }
                        }
                    }
                }])
            } else {
                HashMap::new()
            },
        ));

        info!(version = %VERSION_STR_ONELINE);

        if options.allow_unsupported_set {
            warn!(
                "Running with --allow-unsupported-set can cause certain queries to return \
                 incorrect results"
            )
        }

        let listen_address = options.address.unwrap_or(self.default_address);
        let listener = rt.block_on(tokio::net::TcpListener::bind(&listen_address))?;

        info!(%listen_address, "Listening for new connections");

        let auto_increments: Arc<RwLock<HashMap<Relation, AtomicUsize>>> = Arc::default();
        let view_name_cache = SharedCache::new();
        let view_cache = SharedCache::new();
        let connections: Arc<SkipSet<SocketAddr>> = Arc::default();
        let mut health_reporter = AdapterHealthReporter::new();

        let rs_connect = span!(Level::INFO, "Connecting to RS server");
        rs_connect.in_scope(|| info!(%options.authority_address, %options.deployment));

        let authority_type = options.authority.clone();
        let authority_address = match authority_type {
            AuthorityType::Standalone => deployment_dir
                .clone()
                .into_os_string()
                .into_string()
                .unwrap_or_else(|_| options.authority_address.clone()),
            _ => options.authority_address.clone(),
        };
        let deployment = options.deployment.clone();
        let adapter_authority =
            Arc::new(authority_type.to_authority(&authority_address, &deployment));

        let migration_request_timeout = options.migration_request_timeout_ms;
        let controller_request_timeout = options.controller_request_timeout_ms;
        let server_supports_pagination = options
            .server_worker_options
            .enable_experimental_topk_support
            && options
                .server_worker_options
                .enable_experimental_paginate_support;
        let no_upstream_connections = options.no_upstream_connections;

        let rh = rt.block_on(async {
            Ok::<ReadySetHandle, ReadySetError>(
                ReadySetHandle::with_timeouts(
                    adapter_authority.clone(),
                    Some(Duration::from_millis(controller_request_timeout)),
                    Some(Duration::from_millis(migration_request_timeout)),
                )
                .instrument(rs_connect.clone())
                .await,
            )
        })?;

        rs_connect.in_scope(|| info!("ReadySetHandle created"));

        let ctrlc = tokio::signal::ctrl_c();
        let mut sigterm = {
            let _guard = rt.enter();
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).unwrap()
        };
        let mut listener = Box::pin(futures_util::stream::select(
            TcpListenerStream::new(listener),
            futures_util::stream::select(
                ctrlc
                    .map(|r| {
                        r?;
                        Err(io::Error::new(io::ErrorKind::Interrupted, "got ctrl-c"))
                    })
                    .into_stream(),
                sigterm
                    .recv()
                    .map(futures_util::stream::iter)
                    .into_stream()
                    .flatten()
                    .map(|_| Err(io::Error::new(io::ErrorKind::Interrupted, "got SIGTERM"))),
            ),
        ));
        rs_connect.in_scope(|| info!("Now capturing ctrl-c and SIGTERM events"));

        let mut recorders = Vec::new();
        let prometheus_handle = if options.prometheus_metrics {
            let _guard = rt.enter();
            let database_label = match self.database_type {
                DatabaseType::MySQL => readyset_client_metrics::DatabaseType::MySql,
                DatabaseType::PostgreSQL => readyset_client_metrics::DatabaseType::Psql,
            };

            let recorder = PrometheusBuilder::new()
                .add_global_label("upstream_db_type", database_label)
                .add_global_label("deployment", &options.deployment)
                .build_recorder();

            let handle = recorder.handle();
            recorders.push(MetricsRecorder::Prometheus(recorder));
            Some(handle)
        } else {
            None
        };

        if options.noria_metrics {
            recorders.push(MetricsRecorder::Noria(
                readyset_server::NoriaMetricsRecorder::new(),
            ));
        }

        if !recorders.is_empty() {
            readyset_server::metrics::install_global_recorder(
                CompositeMetricsRecorder::with_recorders(recorders),
            )?;
        }

        rs_connect.in_scope(|| info!("PrometheusHandle created"));

        metrics::gauge!(
            recorded::READYSET_ADAPTER_VERSION,
            1.0,
            &[
                ("release_version", READYSET_VERSION.release_version),
                ("commit_id", READYSET_VERSION.commit_id),
                ("platform", READYSET_VERSION.platform),
                ("rustc_version", READYSET_VERSION.rustc_version),
                ("profile", READYSET_VERSION.profile),
                ("profile", READYSET_VERSION.profile),
                ("opt_level", READYSET_VERSION.opt_level),
            ]
        );
        metrics::counter!(
            recorded::NORIA_STARTUP_TIMESTAMP,
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64
        );

        let (shutdown_tx, shutdown_rx) = shutdown::channel();

        // if we're running in standalone mode, server will already
        // spawn it's own allocator metrics reporter.
        if prometheus_handle.is_some() && !options.standalone {
            let alloc_shutdown = shutdown_rx.clone();
            rt.handle().spawn(report_allocator_metrics(alloc_shutdown));
        }

        // Gate query log code path on the log flag existing.
        let qlog_sender = if options.query_log_mode.is_enabled() {
            rs_connect.in_scope(|| info!("Query logs are enabled. Spawning query logger"));
            let (qlog_sender, qlog_receiver) = tokio::sync::mpsc::unbounded_channel();

            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .max_blocking_threads(1)
                .build()
                .unwrap();

            let shutdown_rx = shutdown_rx.clone();
            // Spawn the actual thread to run the logger
            let query_log_mode = options.query_log_mode;
            std::thread::Builder::new()
                .name("Query logger".to_string())
                .stack_size(2 * 1024 * 1024) // Use the same value tokio is using
                .spawn_wrapper(move || {
                    runtime.block_on(query_logger::QueryLogger::run(
                        qlog_receiver,
                        shutdown_rx,
                        query_log_mode,
                    ));
                    runtime.shutdown_background();
                })?;

            Some(qlog_sender)
        } else {
            rs_connect.in_scope(|| info!("Query logs are disabled"));
            None
        };

        let noria_read_behavior = if options.non_blocking_reads {
            rs_connect.in_scope(|| info!("Will perform NonBlocking Reads"));
            ReadBehavior::NonBlocking
        } else {
            rs_connect.in_scope(|| info!("Will perform Blocking Reads"));
            ReadBehavior::Blocking
        };

        let migration_style = options.query_caching;

        rs_connect.in_scope(|| info!(?migration_style));

        let query_status_cache: &'static _ = Box::leak(Box::new(
            QueryStatusCache::with_capacity(options.query_status_capacity)
                .style(migration_style)
                .enable_experimental_placeholder_inlining(
                    options.experimental_placeholder_inlining,
                ),
        ));

        let telemetry_sender = rt.block_on(async {
            let proxied_queries_reporter =
                Arc::new(ProxiedQueriesReporter::new(query_status_cache));
            TelemetryInitializer::init(
                options.disable_telemetry,
                std::env::var("RS_API_KEY").ok(),
                vec![proxied_queries_reporter],
                options.deployment.clone(),
            )
            .await
        });

        let _ = telemetry_sender
            .send_event_with_payload(
                TelemetryEvent::AdapterStart,
                TelemetryBuilder::new()
                    .adapter_version(option_env!("CARGO_PKG_VERSION").unwrap_or_default())
                    .db_backend(format!("{:?}", &self.database_type).to_lowercase())
                    .build(),
            )
            .map_err(|error| warn!(%error, "Failed to initialize telemetry sender"));

        let migration_mode = match migration_style {
            MigrationStyle::Async | MigrationStyle::Explicit => MigrationMode::OutOfBand,
            MigrationStyle::InRequestPath => MigrationMode::InRequestPath,
        };

        rs_connect.in_scope(|| info!(?migration_mode));

        // Spawn a task for handling this adapter's HTTP request server.
        // This step is done as the last thing before accepting connections because it is used as
        // the health check for the service.
        rs_connect.in_scope(|| info!("Spawning HTTP request server task"));
        let (tx, rx) = if options.wait_for_failpoint {
            let (tx, rx) = tokio::sync::mpsc::channel(1);
            (Some(Arc::new(tx)), Some(rx))
        } else {
            (None, None)
        };
        let http_server = NoriaAdapterHttpRouter {
            listen_addr: options.metrics_address,
            prometheus_handle: prometheus_handle.clone(),
            health_reporter: health_reporter.clone(),
            failpoint_channel: tx,
            metrics: Default::default(),
        };

        let router_shutdown_rx = shutdown_rx.clone();
        let fut = async move {
            let http_listener = http_server.create_listener().await.unwrap();
            NoriaAdapterHttpRouter::route_requests(http_server, http_listener, router_shutdown_rx)
                .await
        };

        rt.handle().spawn(fut);

        // If we previously setup a failpoint channel because wait_for_failpoint was enabled,
        // then we should wait to hear from the http router that a failpoint request was
        // handled.
        if let Some(mut rx) = rx {
            let fut = async move {
                let _ = rx.recv().await;
            };
            rt.block_on(fut);
        }

        let schema_search_path = rt.block_on(load_schema_search_path::<H::UpstreamDatabase>(
            upstream_config.clone(),
            no_upstream_connections,
        ));

        if let MigrationMode::OutOfBand = migration_mode {
            set_failpoint!("adapter-out-of-band");
            let rh = rh.clone();
            let auto_increments = auto_increments.clone();
            let view_name_cache = view_name_cache.clone();
            let view_cache = view_cache.clone();
            let mut shutdown_rx = shutdown_rx.clone();
            let loop_interval = options.migration_task_interval;
            let max_retry = options.max_processing_minutes;
            let dry_run = matches!(migration_style, MigrationStyle::Explicit);
            let expr_dialect = self.expr_dialect;
            let parse_dialect = self.parse_dialect;
            let schema_search_path = Arc::clone(&schema_search_path);

            rs_connect.in_scope(|| info!("Spawning migration handler task"));
            let fut = async move {
                let connection = span!(Level::INFO, "migration task upstream database connection");
                let ssp_retry_loop = async {
                    loop {
                        if let Ok(ssp) = &*schema_search_path.read().await {
                            break ssp.clone();
                        }
                        sleep(UPSTREAM_CONNECTION_RETRY_INTERVAL).await
                    }
                };

                let schema_search_path = tokio::select! {
                    schema_search_path = ssp_retry_loop => schema_search_path,
                    _ = shutdown_rx.recv() => return Ok(()),
                };

                let noria =
                    NoriaConnector::new(
                        rh.clone(),
                        auto_increments,
                        view_name_cache.new_local(),
                        view_cache.new_local(),
                        noria_read_behavior,
                        expr_dialect,
                        parse_dialect,
                        schema_search_path,
                        server_supports_pagination,
                    )
                    .instrument(connection.in_scope(|| {
                        span!(Level::DEBUG, "Building migration task noria connector")
                    }))
                    .await;

                let controller_handle = dry_run.then(|| rh.clone());
                let mut migration_handler = MigrationHandler::new(
                    noria,
                    controller_handle,
                    query_status_cache,
                    expr_dialect,
                    std::time::Duration::from_millis(loop_interval),
                    std::time::Duration::from_secs(max_retry * 60),
                    shutdown_rx.clone(),
                );

                migration_handler.run().await.map_err(move |e| {
                    error!(error = %e, "Migration Handler failed, aborting the process due to service entering a degraded state");
                    std::process::abort()
                })
            };

            rt.handle().spawn(abort_on_panic(fut));
        }

        if matches!(migration_style, MigrationStyle::Explicit) {
            rs_connect.in_scope(|| info!("Spawning explicit migrations task"));
            let rh = rh.clone();
            let loop_interval = options.views_polling_interval;
            let expr_dialect = self.expr_dialect;
            let shutdown_rx = shutdown_rx.clone();
            let fut = async move {
                let mut views_synchronizer = ViewsSynchronizer::new(
                    rh,
                    query_status_cache,
                    std::time::Duration::from_secs(loop_interval),
                    expr_dialect,
                    shutdown_rx,
                );
                views_synchronizer.run().await
            };
            rt.handle().spawn(abort_on_panic(fut));
        }

        // Create a set of readers on this adapter. This will allow servicing queries directly
        // from readers on the adapter rather than across a network hop.
        let readers: Readers = Arc::new(Mutex::new(Default::default()));

        // Run a readyset-server instance within this adapter.
        let internal_server_handle = if options.standalone || options.embedded_readers {
            let authority = options.authority.clone();
            let deployment = options.deployment.clone();
            let mut builder = readyset_server::Builder::from_worker_options(
                options.server_worker_options,
                &options.deployment,
                deployment_dir,
            );
            let r = readers.clone();

            if options.embedded_readers {
                builder.as_reader_only();
                builder.cannot_become_leader();
            }

            builder.set_replicator_statement_logging(options.tracing.statement_logging);

            builder.set_telemetry_sender(telemetry_sender.clone());

            if let Some(addr) = options.controller_address {
                builder.set_listen_addr(addr);
            }

            if let Some(external_addr) = options.external_address.or(options.controller_address) {
                builder.set_external_addr(SocketAddr::new(external_addr, 0));
            }

            let server_handle = rt.block_on(async move {
                let authority = Arc::new(authority.to_authority(&authority_address, &deployment));

                builder
                    .start_with_readers(
                        authority,
                        r,
                        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 4000),
                    )
                    .await
            })?;

            Some(server_handle)
        } else {
            None
        };

        health_reporter.set_state(AdapterState::Healthy);

        rs_connect.in_scope(|| info!(supported = %server_supports_pagination));

        let expr_dialect = self.expr_dialect;
        let parse_dialect = self.parse_dialect;
        while let Some(Ok(s)) = rt.block_on(listener.next()) {
            let client_addr = s.peer_addr()?;
            let connection = info_span!("connection", addr = %client_addr);
            connection.in_scope(|| debug!("Accepted new connection"));
            s.set_nodelay(true)?;

            // bunch of stuff to move into the async block below
            let rh = rh.clone();
            let adapter_authority = adapter_authority.clone();
            let auto_increments = auto_increments.clone();
            let view_name_cache = view_name_cache.clone();
            let view_cache = view_cache.clone();
            let mut connection_handler = self.connection_handler.clone();
            let backend_builder = BackendBuilder::new()
                .client_addr(client_addr)
                .slowlog(options.log_slow)
                .users(users.clone())
                .require_authentication(!options.allow_unauthenticated_connections)
                .dialect(self.parse_dialect)
                .query_log(qlog_sender.clone(), options.query_log_mode)
                .unsupported_set_mode(if options.allow_unsupported_set {
                    readyset_adapter::backend::UnsupportedSetMode::Allow
                } else {
                    options.unsupported_set_mode.into()
                })
                .migration_mode(migration_mode)
                .query_max_failure_seconds(options.query_max_failure_seconds)
                .telemetry_sender(telemetry_sender.clone())
                .fallback_recovery_seconds(options.fallback_recovery_seconds)
                .enable_experimental_placeholder_inlining(options.experimental_placeholder_inlining)
                .connections(connections.clone())
                .metrics_handle(prometheus_handle.clone().map(MetricsHandle::new));
            let telemetry_sender = telemetry_sender.clone();

            // Initialize the reader layer for the adapter.
            let r = (options.standalone || options.embedded_readers).then(|| {
                // Create a task that repeatedly polls BlockingRead's every `RETRY_TIMEOUT`.
                // When the `BlockingRead` completes, tell the future to resolve with ack.
                let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<(BlockingRead, Ack)>();
                rt.handle().spawn(retry_misses(rx));
                ReadRequestHandler::new(readers.clone(), tx, Duration::from_secs(5))
            });

            let upstream_config = upstream_config.clone();
            let schema_search_path = Arc::clone(&schema_search_path);
            let fut = async move {
                let upstream_res = connect_upstream::<H::UpstreamDatabase>(
                    upstream_config,
                    no_upstream_connections,
                )
                .await
                .map_err(|e| format!("Error connecting to upstream database: {}", e));

                match upstream_res {
                    Ok(upstream) => {
                        if let Err(e) =
                            telemetry_sender.send_event(TelemetryEvent::UpstreamConnected)
                        {
                            warn!(error = %e, "Failed to send upstream connected metric");
                        }

                        match &*schema_search_path.read().await {
                            Ok(ssp) => {
                                let noria = NoriaConnector::new_with_local_reads(
                                    rh.clone(),
                                    auto_increments,
                                    view_name_cache.new_local(),
                                    view_cache.new_local(),
                                    noria_read_behavior,
                                    r,
                                    expr_dialect,
                                    parse_dialect,
                                    ssp.clone(),
                                    server_supports_pagination,
                                )
                                .instrument(debug_span!("Building noria connector"))
                                .await;

                                let backend = backend_builder.clone().build(
                                    noria,
                                    upstream,
                                    query_status_cache,
                                    adapter_authority.clone(),
                                );
                                connection_handler.process_connection(s, backend).await;
                            }
                            Err(error) => {
                                error!(
                                    %error,
                                    "Error loading initial schema search path from upstream"
                                );
                                connection_handler
                                    .immediate_error(
                                        s,
                                        format!(
                                            "Error loading initial schema search path from \
                                             upstream: {error}"
                                        ),
                                    )
                                    .await;
                            }
                        }
                    }
                    Err(error) => {
                        error!(%error, "Error during initial connection establishment");
                        connection_handler.immediate_error(s, error).await;
                    }
                }

                debug!("disconnected");
            }
            .instrument(connection);

            rt.handle().spawn(fut);
        }

        let rs_shutdown = span!(Level::INFO, "RS server Shutting down");
        health_reporter.set_state(AdapterState::ShuttingDown);

        // We need to drop the last remaining `ShutdownReceiver` before sending the shutdown
        // signal. If we didn't, `ShutdownSender::shutdown` would hang forever, since it
        // specifically waits for every associated `ShutdownReceiver` to be dropped.
        drop(shutdown_rx);

        // Shut down all of our background tasks
        rs_shutdown.in_scope(|| {
            info!("Waiting up to 20 seconds for all background tasks to shut down");
        });
        rt.block_on(shutdown_tx.shutdown_timeout(Duration::from_secs(20)));

        if let Some((_, server_shutdown_tx)) = internal_server_handle {
            rs_shutdown.in_scope(|| info!("Shutting down embedded server task"));
            rt.block_on(server_shutdown_tx.shutdown_timeout(Duration::from_secs(20)));

            // Send server shutdown telemetry event
            let _ = telemetry_sender.send_event(TelemetryEvent::ServerStop);
        }

        // Send adapter shutdown telemetry event
        let _ = telemetry_sender.send_event(TelemetryEvent::AdapterStop);
        rs_shutdown.in_scope(|| {
            info!("Waiting up to 5s for telemetry reporter to drain in-flight metrics")
        });
        rt.block_on(async move {
            match telemetry_sender
                .shutdown(std::time::Duration::from_secs(5))
                .await
            {
                Ok(_) => info!("TelemetrySender shutdown gracefully"),
                Err(e) => info!(error=%e, "TelemetrySender did not shut down gracefully"),
            }
        });

        // We use `shutdown_timeout` instead of `shutdown_background` in case any
        // blocking IO is ongoing.
        rs_shutdown.in_scope(|| info!("Waiting up to 20s for tasks to complete shutdown"));
        rt.shutdown_timeout(std::time::Duration::from_secs(20));
        rs_shutdown.in_scope(|| info!("Shutdown completed successfully"));

        Ok(())
    }

    // TODO: Figure out a way to not require as many flags when --cleanup flag is supplied.
    /// Cleans up the provided deployment, and if an upstream url was provided, will clean up
    /// replication slot and other deployment related assets in the upstream.
    async fn cleanup(
        &mut self,
        upstream_config: UpstreamConfig,
        deployment_dir: PathBuf,
    ) -> anyhow::Result<()> {
        replicators::cleanup(upstream_config).await?;

        if deployment_dir.exists() {
            remove_dir_all(deployment_dir)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Certain clap things, like `requires`, only ever throw an error at runtime, not at
    // compile-time - this tests that none of those happen
    #[test]
    fn arg_parsing_noria_standalone() {
        let opts = Options::parse_from(vec![
            "readyset",
            "--database-type",
            "mysql",
            "--deployment",
            "test",
            "--address",
            "0.0.0.0:3306",
            "--authority-address",
            "consul:8500",
            "--allow-unauthenticated-connections",
        ]);

        assert_eq!(opts.deployment, "test");
    }

    #[test]
    fn arg_parsing_with_upstream() {
        let opts = Options::parse_from(vec![
            "readyset",
            "--database-type",
            "mysql",
            "--deployment",
            "test",
            "--address",
            "0.0.0.0:3306",
            "--authority-address",
            "consul:8500",
            "--allow-unauthenticated-connections",
            "--upstream-db-url",
            "mysql://root:password@mysql:3306/readyset",
        ]);

        assert_eq!(opts.deployment, "test");
    }

    #[test]
    fn async_migrations_param_defaults() {
        let opts = Options::parse_from(vec![
            "readyset",
            "--database-type",
            "mysql",
            "--deployment",
            "test",
            "--address",
            "0.0.0.0:3306",
            "--authority-address",
            "consul:8500",
            "--allow-unauthenticated-connections",
            "--upstream-db-url",
            "mysql://root:password@mysql:3306/readyset",
            "--query-caching=async",
        ]);

        assert_eq!(opts.max_processing_minutes, 15);
        assert_eq!(opts.migration_task_interval, 20000);
    }

    #[test]
    fn infer_database_type() {
        let opts = Options::parse_from(vec![
            "readyset",
            "--deployment",
            "test",
            "--address",
            "0.0.0.0:3306",
            "--upstream-db-url",
            "mysql://root:password@mysql:3306/readyset",
        ]);
        assert_eq!(opts.database_type().unwrap(), DatabaseType::MySQL);

        let opts = Options::parse_from(vec![
            "readyset",
            "--deployment",
            "test",
            "--address",
            "0.0.0.0:3306",
            "--upstream-db-url",
            "postgresql://root:password@db/readyset",
        ]);
        assert_eq!(opts.database_type().unwrap(), DatabaseType::PostgreSQL);

        let opts = Options::parse_from(vec![
            "readyset",
            "--deployment",
            "test",
            "--address",
            "0.0.0.0:3306",
            "--upstream-db-url",
            "postgresql://root:password@db/readyset",
            "--database-type",
            "postgresql",
        ]);
        assert_eq!(opts.database_type().unwrap(), DatabaseType::PostgreSQL);

        let opts = Options::parse_from(vec![
            "readyset",
            "--deployment",
            "test",
            "--address",
            "0.0.0.0:3306",
            "--upstream-db-url",
            "postgresql://root:password@db/readyset",
            "--database-type",
            "mysql",
        ]);
        opts.database_type().unwrap_err();
    }
}
