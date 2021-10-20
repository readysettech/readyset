#![warn(clippy::dbg_macro)]
#![deny(macro_use_extern_crate)]

use std::marker::Send;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, RwLock};
use std::{collections::HashMap, time::Duration};
use std::{io, net::IpAddr};

use anyhow::anyhow;
use async_compression::tokio::write::GzipEncoder;
use async_trait::async_trait;
use clap::Clap;
use futures_util::future::FutureExt;
use futures_util::stream::StreamExt;
use maplit::hashmap;
use metrics_exporter_prometheus::PrometheusBuilder;
use noria_client::query_reconciler::QueryReconciler;
use noria_client::query_status_cache::QueryStatusCache;
use noria_client::{coverage::QueryCoverageInfoRef, http_router::NoriaAdapterHttpRouter};
use noria_client::{QueryHandler, UpstreamDatabase};
use noria_client_metrics::QueryExecutionEvent;
use stream_cancel::Valve;
use tokio::io::AsyncWriteExt;
use tokio::net;
use tokio::select;
use tokio::sync::broadcast;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::{fs::OpenOptions, net::UdpSocket};
use tokio_stream::wrappers::TcpListenerStream;
use tracing::{debug, error, info, info_span, span, Level};
use tracing_futures::Instrument;

use nom_sql::{Dialect, SelectStatement, SqlQuery};
use noria::consensus::{AuthorityControl, AuthorityType, ConsulAuthority};
use noria::{ControllerHandle, ReadySetError};
use noria_client::backend::noria_connector::NoriaConnector;
use noria_client::rewrite::anonymize_literals;
use noria_client::{Backend, BackendBuilder};

const REGISTER_HTTP_INTERVAL: Duration = Duration::from_secs(20);

#[async_trait]
pub trait ConnectionHandler {
    type UpstreamDatabase: UpstreamDatabase;
    type Handler: QueryHandler;
    async fn process_connection(
        &mut self,
        stream: net::TcpStream,
        backend: Backend<Self::UpstreamDatabase, Self::Handler>,
    );
}

/// Represents which database interface is being adapted to
/// communicate with Noria.
#[derive(Copy, Clone)]
pub enum DatabaseType {
    /// MySQL database.
    Mysql,
    /// PostgreSQL database.
    Psql,
}

pub struct NoriaAdapter<H> {
    pub name: &'static str,
    pub version: &'static str,
    pub description: &'static str,
    pub default_address: SocketAddr,
    pub connection_handler: H,
    pub database_type: DatabaseType,
    /// SQL dialect to use when parsing queries
    pub dialect: Dialect,
    /// Whether to mirror DDL changes to both upstream or noria, or just send them upstream
    pub mirror_ddl: bool,
}

#[derive(Clap)]
pub struct Options {
    /// IP:PORT to listen on
    #[clap(long, short = 'a', env = "LISTEN_ADDRESS", parse(try_from_str))]
    address: Option<SocketAddr>,

    /// ReadySet deployment ID to attach to
    #[clap(long, env = "NORIA_DEPLOYMENT", forbid_empty_values = true)]
    deployment: String,

    /// Authority connection string.
    // TODO(justin): The default_value should depend on the value of authority.
    #[clap(
        long,
        short = 'z',
        env = "AUTHORITY_ADDRESS",
        default_value = "127.0.0.1:2181"
    )]
    authority_address: String,

    /// The authority to use. Possible values: zookeeper, consul.
    #[clap(long, env = "AUTHORITY", default_value = "zookeeper", possible_values = &["consul", "zookeeper"])]
    authority: AuthorityType,

    /// Log slow queries (> 5ms)
    #[clap(long)]
    log_slow: bool,

    /// Don't require authentication for any client connections
    #[clap(long, env = "ALLOW_UNAUTHENTICATED_CONNECTIONS")]
    allow_unauthenticated_connections: bool,

    /// Run with query coverage analysis enabled in the serving path.
    #[clap(long, env = "LIVE_QCA", requires("upstream-db-url"))]
    live_qca: bool,

    /// IP:PORT to host endpoint for scraping metrics from the adapter.
    #[clap(
        long,
        env = "METRICS_ADDRESS",
        default_value = "0.0.0.0:6034",
        parse(try_from_str),
        requires("upstream-db-url")
    )]
    metrics_address: SocketAddr,

    /// Make all reads run against Noria and upstream, returning the upstream result.
    #[clap(long, requires("upstream-db-url"))]
    mirror_reads: bool,

    /// Analyze coverage of queries during execution, and save a report of coverage on exit.
    ///
    /// Implies --race-reads
    #[clap(long, env = "COVERAGE_ANALYSIS", requires("upstream-db-url"))]
    coverage_analysis: Option<PathBuf>,

    /// Set max processing time in minutes for any query within the system before it's deemed to be
    /// unsupported by ReadySet, and will be sent exclusively to fallback.
    ///
    /// Defaults to 5 minutes.
    #[clap(
        long,
        env = "MAX_PROCESSING_MINUTES",
        default_value = "15",
        requires("upstream-db-url")
    )]
    max_processing_minutes: i64,

    /// Sets the query reconciler's loop interval in milliseconds.
    #[clap(
        long,
        env = "RECONCILE_INTERVAL",
        default_value = "1000",
        requires("upstream-db-url")
    )]
    reconciler_loop_interval: u64,

    /// Allow database connections authenticated as this user. Ignored if
    /// --allow-unauthenticated-connections is passed
    #[clap(long, env = "ALLOWED_USERNAME", short = 'u')]
    username: Option<String>,

    /// Password to authenticate database connections with. Ignored if
    /// --allow-unauthenticated-connections is passed
    #[clap(long, env = "ALLOWED_PASSWORD", short = 'p')]
    password: Option<String>,

    /// URL for the upstream database to connect to. Should include username and password if
    /// necessary
    #[clap(long, env = "UPSTREAM_DB_URL")]
    upstream_db_url: Option<String>,

    /// The region the worker is hosted in. Required to route view requests to specific regions.
    #[clap(long, env = "NORIA_REGION")]
    region: Option<String>,

    /// Enable recording and exposing Prometheus metrics
    #[clap(long)]
    prometheus_metrics: bool,

    /// Enable logging queries and execution metrics in prometheus. This creates a
    /// histogram per unique query.
    #[clap(long, requires = "prometheus-metrics")]
    query_log: bool,

    /// Enables logging ad-hoc queries in the query log. Useful for testing.
    #[clap(long, hidden = true, requires = "query-log")]
    query_log_ad_hoc: bool,

    #[clap(flatten)]
    logging: readyset_logging::Options,
}

impl<H> NoriaAdapter<H>
where
    H: ConnectionHandler + Clone + Send + Sync + 'static,
{
    pub fn run(&mut self, options: Options) -> anyhow::Result<()> {
        let users: &'static HashMap<String, String> = Box::leak(Box::new(
            if !options.allow_unauthenticated_connections {
                hashmap! {
                    options.username.ok_or_else(|| {
                        anyhow!("Must specify --username/-u unless --allow-unauthenticated-connections is passed")
                    })? => options.password.ok_or_else(|| {
                        anyhow!("Must specify --password/-p unless --allow-unauthenticated-connections is passed")
                    })?
                }
            } else {
                HashMap::new()
            },
        ));
        options.logging.init()?;
        let rt = tokio::runtime::Runtime::new()?;
        let listen_address = options.address.unwrap_or(self.default_address);
        let listener = rt.block_on(tokio::net::TcpListener::bind(&listen_address))?;

        info!(%listen_address, "Listening for new connections");

        let auto_increments: Arc<RwLock<HashMap<String, AtomicUsize>>> = Arc::default();
        let query_cache: Arc<RwLock<HashMap<SelectStatement, String>>> = Arc::default();

        let rs_connect = span!(
            Level::INFO,
            "Connecting to ReadySet server",
            %options.authority_address,
            %options.deployment
        );

        let authority = options.authority.clone();
        let authority_address = options.authority_address.clone();
        let deployment = options.deployment.clone();
        let ch = rt.block_on(async {
            let authority = authority
                .to_authority(&authority_address, &deployment)
                .await;

            Ok::<ControllerHandle, ReadySetError>(
                ControllerHandle::new(authority)
                    .instrument(rs_connect.clone())
                    .await,
            )
        })?;
        rs_connect.in_scope(|| info!("Connected"));

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
                        if let Err(e) = r {
                            Err(e)
                        } else {
                            Err(io::Error::new(io::ErrorKind::Interrupted, "got ctrl-c"))
                        }
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

        let prometheus_handle = if options.prometheus_metrics {
            let _guard = rt.enter();
            let database_label: noria_client_metrics::recorded::DatabaseType =
                self.database_type.into();

            let recorder = PrometheusBuilder::new()
                .add_global_label("database_type", database_label)
                .add_global_label("deployment", &options.deployment)
                .build();

            let handle = recorder.handle();
            metrics::set_boxed_recorder(Box::new(recorder))?;
            Some(handle)
        } else {
            None
        };

        let query_coverage_info = options
            .coverage_analysis
            .is_some()
            .then(QueryCoverageInfoRef::default);

        let (shutdown_sender, shutdown_recv) = tokio::sync::broadcast::channel(1);

        // Gate query log code path on the log flag existing.
        let qlog_sender = if options.query_log {
            let (qlog_sender, qlog_receiver) = tokio::sync::mpsc::unbounded_channel();
            rt.spawn(query_logger(
                qlog_receiver,
                self.database_type,
                shutdown_recv,
            ));
            Some(qlog_sender)
        } else {
            None
        };

        let query_status_cache = if options.live_qca {
            let cache = Arc::new(QueryStatusCache::new(chrono::Duration::minutes(
                options.max_processing_minutes,
            )));

            // Only start the live qca task if there is an upstream db.
            if let Some(upstream_db_url) = options.upstream_db_url.clone() {
                let ch = ch.clone();
                let (auto_increments, query_cache) = (auto_increments.clone(), query_cache.clone());
                let shutdown_recv = shutdown_sender.subscribe();
                let loop_interval = options.reconciler_loop_interval;
                let cache = cache.clone();
                let fut = async move {
                    let connection = span!(Level::INFO, "live-qca upstream database connection");
                    let upstream = H::UpstreamDatabase::connect(upstream_db_url.clone())
                        .instrument(
                            connection
                                .in_scope(|| span!(Level::INFO, "Connecting to upstream database")),
                        )
                        .await
                        .unwrap();

                    let noria = NoriaConnector::new(
                        ch.clone(),
                        auto_increments.clone(),
                        query_cache.clone(),
                        None,
                    )
                    .instrument(
                        connection
                            .in_scope(|| span!(Level::DEBUG, "Building live-qca noria connector")),
                    )
                    .await;

                    let mut reconciler = QueryReconciler::new(
                        noria,
                        upstream,
                        cache,
                        std::time::Duration::from_millis(loop_interval),
                        shutdown_recv,
                    );
                    reconciler.run().await
                };

                rt.handle().spawn(fut);
            }

            Some(cache)
        } else {
            None
        };

        // Deploy http router if an address was supplied to retrieve QCA data.
        let router_handle = {
            let (handle, valve) = Valve::new();
            let query_cache = query_status_cache.as_ref().cloned();
            let http_server = NoriaAdapterHttpRouter {
                listen_addr: options.metrics_address,
                query_cache,
                valve,
                prometheus_handle,
            };
            let fut = async move {
                let http_listener = http_server.create_listener().await.unwrap();
                NoriaAdapterHttpRouter::route_requests(http_server, http_listener).await
            };

            rt.handle().spawn(fut);

            handle
        };

        // Spin up async thread that is in charge of creating a session with the authority,
        // regularly updating the heartbeat to keep the session live, and registering the adapters
        // http endpoint.
        // For now we only support registering adapters over consul.
        if let AuthorityType::Consul = options.authority {
            let fut = reconcile_endpoint_registration(
                authority_address,
                deployment,
                options.metrics_address.port(),
            );
            rt.handle().spawn(fut);
        }

        while let Some(Ok(s)) = rt.block_on(listener.next()) {
            // bunch of stuff to move into the async block below
            let ch = ch.clone();
            let (auto_increments, query_cache) = (auto_increments.clone(), query_cache.clone());
            let mut connection_handler = self.connection_handler.clone();
            let region = options.region.clone();
            let upstream_db_url = options.upstream_db_url.clone();
            let backend_builder = BackendBuilder::new()
                .slowlog(options.log_slow)
                .mirror_reads(options.mirror_reads || options.coverage_analysis.is_some())
                .users(users.clone())
                .require_authentication(!options.allow_unauthenticated_connections)
                .dialect(self.dialect)
                .mirror_ddl(self.mirror_ddl)
                .query_log(qlog_sender.clone(), options.query_log_ad_hoc)
                .query_coverage_info(query_coverage_info)
                .live_qca(options.live_qca)
                .query_status_cache(query_status_cache.clone());
            let fut = async move {
                let connection = span!(Level::INFO, "connection", addr = ?s.peer_addr().unwrap());
                connection.in_scope(|| info!("Accepted new connection"));

                let noria = NoriaConnector::new(
                    ch.clone(),
                    auto_increments.clone(),
                    query_cache.clone(),
                    region.clone(),
                )
                .instrument(connection.in_scope(|| span!(Level::DEBUG, "Building noria connector")))
                .await;

                let upstream =
                    if let Some(upstream_db_url) = &upstream_db_url {
                        Some(
                            H::UpstreamDatabase::connect(upstream_db_url.clone())
                                .instrument(connection.in_scope(|| {
                                    span!(Level::INFO, "Connecting to upstream database")
                                }))
                                .await
                                .unwrap(),
                        )
                    } else {
                        None
                    };

                let backend = backend_builder.clone().build(noria, upstream);
                connection_handler
                    .process_connection(s, backend)
                    .instrument(connection.clone())
                    .await;
                connection.in_scope(|| debug!("disconnected"));
            };
            rt.handle().spawn(fut);
        }

        // Dropping the sender acts as a shutdown signal.
        drop(shutdown_sender);

        // Shut down all tcp streams started by the adapters http router.
        drop(router_handle);

        if let Some(path) = options.coverage_analysis.as_ref() {
            let _guard = rt.enter();
            let file = rt.block_on(
                OpenOptions::new()
                    .read(false)
                    .write(true)
                    .append(false)
                    .truncate(true)
                    .create(true)
                    .open(path),
            )?;
            let mut tar = tokio_tar::Builder::new(GzipEncoder::new(file));

            if let Some(url) = options.upstream_db_url {
                let result: Result<(), anyhow::Error> = rt.block_on(async {
                    let mut upstream = H::UpstreamDatabase::connect(url).await?;
                    let schema = upstream.schema_dump().await?;
                    let mut header = tokio_tar::Header::new_gnu();
                    tar.append_data(&mut header, "schema.sql", schema.as_slice())
                        .await?;
                    Ok(())
                });
                if let Err(e) = result {
                    error!("Failed to dump database schema:  {}", e);
                }
            }

            let qci = query_coverage_info.unwrap().serialize()?;
            let mut header = tokio_tar::Header::new_gnu();
            let result: Result<(), anyhow::Error> = rt.block_on(async {
                tar.append_data(&mut header, "query-info.json", qci.as_slice())
                    .await?;
                let mut gz = tar.into_inner().await?;
                gz.shutdown().await?;
                let mut file = gz.into_inner();
                file.flush().await?;
                file.sync_all().await?;
                Ok(())
            });
            if let Err(e) = result {
                error!("Failed to write query info: {}", e);
            }
        }

        drop(ch);
        // We use `shutdown_timeout` instead of `shutdown_background` in case any
        // blocking IO is ongoing.
        info!("Waiting up to 20s for tasks to complete shutdown");
        rt.shutdown_timeout(std::time::Duration::from_secs(20));

        Ok(())
    }
}

async fn my_ip(destination: &str) -> Option<IpAddr> {
    let socket = match UdpSocket::bind("0.0.0.0:0").await {
        Ok(s) => s,
        Err(_) => return None,
    };

    match socket.connect(destination).await {
        Ok(()) => (),
        Err(_) => return None,
    };

    match socket.local_addr() {
        Ok(addr) => Some(addr.ip()),
        Err(_) => None,
    }
}

/// Facilitates continuously updating consul with this adapters externally accessibly http
/// endpoint.
async fn reconcile_endpoint_registration(authority_address: String, deployment: String, port: u16) {
    let authority =
        ConsulAuthority::new(&format!("http://{}/{}", &authority_address, &deployment)).unwrap();

    let mut interval = tokio::time::interval(REGISTER_HTTP_INTERVAL);
    let mut session_id = None;

    async fn needs_refresh(id: &Option<String>, consul: &ConsulAuthority) -> bool {
        if let Some(id) = id {
            consul.worker_heartbeat(id.to_owned()).await.is_err()
        } else {
            true
        }
    }

    loop {
        interval.tick().await;

        if needs_refresh(&session_id, &authority).await {
            // If we fail this heartbeat, we assume we need to create a new session.
            if let Err(e) = authority.init().await {
                error!(%e, "encountered error while trying to initialize authority in readyset-adapter");
                // Try again on next tick.
                continue;
            }
        }

        // We try to update our http endpoint every iteration regardless because it may
        // have changed.
        let ip = match my_ip(&authority_address).await {
            Some(ip) => ip,
            None => {
                // Failed to retrieve our IP. We'll try again on next tick iteration.
                continue;
            }
        };
        let http_endpoint = SocketAddr::new(ip, port);

        match authority.register_adapter(http_endpoint).await {
            Ok(id) => session_id = id,
            Err(e) => {
                error!(%e, "encountered error while trying to register adapter endpoint in authority")
            }
        }
    }
}

/// Async task that logs query stats.
async fn query_logger(
    mut receiver: UnboundedReceiver<QueryExecutionEvent>,
    db_type: DatabaseType,
    mut shutdown_recv: broadcast::Receiver<()>,
) {
    let _span = info_span!("query-logger");

    let database_label: noria_client_metrics::recorded::DatabaseType = db_type.into();
    let database_label = String::from(database_label);

    loop {
        select! {
            event = receiver.recv() => {
                if let Some(mut event) = event {
                    let query = match &mut event.query {
                        Some(SqlQuery::Select(stmt)) => {
                            anonymize_literals(stmt);
                            stmt.to_string()
                        }
                        _ => "".to_string(),
                    };

                    if let Some(noria) = event.noria_duration {
                        metrics::histogram!(
                            noria_client_metrics::recorded::QUERY_LOG_EXECUTION_TIME,
                            noria,
                            "query" => query.clone(),
                            "database_type" => String::from(noria_client_metrics::recorded::DatabaseType::Noria)
                        );
                    }

                    if let Some(upstream) = event.upstream_duration {
                        metrics::histogram!(
                            noria_client_metrics::recorded::QUERY_LOG_EXECUTION_TIME,
                            upstream,
                            "query" => query.clone(),
                            "database_type" => database_label.clone()
                        );
                    }
                } else {
                info!("Metrics thread shutting down after request handle dropped.");
                }
            }
            _ = shutdown_recv.recv() => {
                info!("Metrics thread shutting down after signal received.");
                break;
            }
        }
    }
}

impl From<DatabaseType> for noria_client_metrics::recorded::DatabaseType {
    fn from(database_type: DatabaseType) -> Self {
        match database_type {
            DatabaseType::Mysql => noria_client_metrics::recorded::DatabaseType::Mysql,
            DatabaseType::Psql => noria_client_metrics::recorded::DatabaseType::Psql,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arg_parsing() {
        // Certain clap things, like `requires`, only ever throw an error at runtime, not at
        // compile-time - this tests that none of those happen
        let opts = Options::parse_from(vec![
            "noria-mysql",
            "--deployment",
            "test",
            "--address",
            "0.0.0.0:3306",
            "-z",
            "zookeeper:2181",
            "--allow-unauthenticated-connections",
            "--upstream-db-url",
            "mysql://root:password@mysql:3306/readyset",
        ]);

        assert_eq!(opts.deployment, "test");
    }
}
