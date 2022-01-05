#![warn(clippy::dbg_macro)]
#![deny(macro_use_extern_crate)]

use std::marker::Send;
use std::net::SocketAddr;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, RwLock};
use std::{collections::HashMap, time::Duration};
use std::{io, net::IpAddr};

use anyhow::anyhow;
use async_trait::async_trait;
use clap::Parser;
use futures_util::future::FutureExt;
use futures_util::stream::StreamExt;
use maplit::hashmap;
use metrics::SharedString;
use metrics_exporter_prometheus::PrometheusBuilder;
use noria_client::http_router::NoriaAdapterHttpRouter;
use noria_client::migration_handler::MigrationHandler;
use noria_client::outputs_synchronizer::OutputsSynchronizer;
use noria_client::query_status_cache::{MigrationStyle, QueryStatusCache};
use noria_client::{QueryHandler, UpstreamDatabase};
use noria_client_metrics::QueryExecutionEvent;
use stream_cancel::Valve;
use tokio::net;
use tokio::net::UdpSocket;
use tokio::select;
use tokio::sync::broadcast;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio_stream::wrappers::TcpListenerStream;
use tracing::{debug, error, info, info_span, span, Level};
use tracing_futures::Instrument;

use nom_sql::{Dialect, SelectStatement, SqlQuery};
use noria::consensus::{AuthorityControl, AuthorityType, ConsulAuthority};
use noria::{ControllerHandle, ReadySetError};
use noria_client::backend::noria_connector::NoriaConnector;
use noria_client::backend::MigrationMode;
use noria_client::rewrite::anonymize_literals;
use noria_client::{Backend, BackendBuilder};

const REGISTER_HTTP_INTERVAL: Duration = Duration::from_secs(20);
const AWS_PRIVATE_IP_ENDPOINT: &str = "http://169.254.169.254/latest/meta-data/local-ipv4";
const AWS_METADATA_TOKEN_ENDPOINT: &str = "http://169.254.169.254/latest/api/token";

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
    pub description: &'static str,
    pub default_address: SocketAddr,
    pub connection_handler: H,
    pub database_type: DatabaseType,
    /// SQL dialect to use when parsing queries
    pub dialect: Dialect,
    /// Whether to mirror DDL changes to both upstream or noria, or just send them upstream
    pub mirror_ddl: bool,
}

#[derive(Parser)]
pub struct Options {
    /// IP:PORT to listen on
    #[clap(long, short = 'a', env = "LISTEN_ADDRESS", parse(try_from_str))]
    address: Option<SocketAddr>,

    /// ReadySet deployment ID to attach to
    #[clap(long, env = "NORIA_DEPLOYMENT", forbid_empty_values = true)]
    deployment: String,

    /// Authority connection string.
    // TODO(justin): The default_value should depend on the value of authority.
    #[clap(long, env = "AUTHORITY_ADDRESS", default_value = "127.0.0.1:8500")]
    authority_address: String,

    /// The authority to use. Possible values: zookeeper, consul.
    #[clap(long, env = "AUTHORITY", default_value = "consul", possible_values = &["consul", "zookeeper"])]
    authority: AuthorityType,

    /// Log slow queries (> 5ms)
    #[clap(long)]
    log_slow: bool,

    /// Don't require authentication for any client connections
    #[clap(long, env = "ALLOW_UNAUTHENTICATED_CONNECTIONS")]
    allow_unauthenticated_connections: bool,

    /// Run migrations in a separate thread off of the serving path.
    #[clap(long, env = "ASYNC_MIGRATIONS", requires("upstream-db-url"))]
    async_migrations: bool,

    /// Sets the maximum time in minutes that we will retry migrations for in the
    /// migration handler. If this time is reached, the query will be exclusively
    /// sent to fallback.
    ///
    /// Defaults to 15 minutes.
    #[clap(long, env = "MAX_PROCESSING_MINUTES", default_value = "15")]
    max_processing_minutes: u64,

    /// Sets the migration handlers's loop interval in milliseconds.
    #[clap(long, env = "MIGRATION_TASK_INTERVAL", default_value = "20000")]
    migration_task_interval: u64,

    /// Validate queries executing against noria with the upstream db.
    #[clap(long, env = "VALIDATE_QUERIES", requires("upstream-db-url"))]
    validate_queries: bool,

    /// IP:PORT to host endpoint for scraping metrics from the adapter.
    #[clap(
        long,
        env = "METRICS_ADDRESS",
        default_value = "0.0.0.0:6034",
        parse(try_from_str)
    )]
    metrics_address: SocketAddr,

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

    /// Use the AWS EC2 metadata service to determine the external address of this noria adapter's
    /// http endpoint.
    #[clap(long)]
    use_aws_external_address: bool,

    #[clap(flatten)]
    logging: readyset_logging::Options,

    /// Test feature to fail invalidated queries in the serving path instead of going
    /// to fallback.
    #[clap(long, hidden = true)]
    fail_invalidated_queries: bool,

    /// Allow executing, but ignore, unsupported `SET` statements
    #[clap(long, hidden = true, env = "ALLOW_UNSUPPORTED_SET")]
    allow_unsupported_set: bool,

    /// Only run migrations through CREATE CACHED QUERY statements. Async migrations are not
    /// supported in this case.
    #[clap(long, env = "EXPLICIT_MIGRATIONS", conflicts_with = "async-migrations")]
    explicit_migrations: bool,

    // TODO(DAN): require explicit migrations
    /// Specifies the polling interval in seconds for requesting outputs from the Leader.
    #[clap(long, env = "OUTPUTS_POLLING_INTERVAL", default_value = "300")]
    outputs_polling_interval: u64,

    /// Provides support for the EXPLAIN LAST STATEMENT command, which returns metadata about the
    /// last statement issued along the current connection.
    #[clap(long, hidden = true, env = "EXPLAIN_LAST_STATEMENT")]
    explain_last_statement: bool,
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
        info!(commit_hash = %env!("CARGO_PKG_VERSION", "version not set"));

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
            let database_label: noria_client_metrics::DatabaseType = self.database_type.into();

            let recorder = PrometheusBuilder::new()
                .add_global_label("upstream_db_type", database_label)
                .add_global_label("deployment", &options.deployment)
                .build();

            let handle = recorder.handle();
            metrics::set_boxed_recorder(Box::new(recorder))?;
            Some(handle)
        } else {
            None
        };

        let (shutdown_sender, shutdown_recv) = tokio::sync::broadcast::channel(1);

        // Gate query log code path on the log flag existing.
        let qlog_sender = if options.query_log {
            let (qlog_sender, qlog_receiver) = tokio::sync::mpsc::unbounded_channel();
            rt.spawn(query_logger(qlog_receiver, shutdown_recv));
            Some(qlog_sender)
        } else {
            None
        };

        let migration_style = if options.async_migrations {
            MigrationStyle::Async
        } else if options.explicit_migrations {
            MigrationStyle::Explicit
        } else {
            MigrationStyle::InRequestPath
        };
        let query_status_cache = Arc::new(QueryStatusCache::with_style(migration_style));

        if options.async_migrations {
            #[allow(clippy::unwrap_used)] // async_migrations requires upstream_db_url
            let upstream_db_url = options.upstream_db_url.as_ref().unwrap().clone();
            let ch = ch.clone();
            let (auto_increments, query_cache) = (auto_increments.clone(), query_cache.clone());
            let shutdown_recv = shutdown_sender.subscribe();
            let loop_interval = options.migration_task_interval;
            let max_retry = options.max_processing_minutes;
            let validate_queries = options.validate_queries;
            let cache = query_status_cache.clone();

            let fut = async move {
                let connection = span!(Level::INFO, "migration task upstream database connection");
                let upstream = H::UpstreamDatabase::connect(upstream_db_url.clone())
                    .instrument(
                        connection
                            .in_scope(|| span!(Level::INFO, "Connecting to upstream database")),
                    )
                    .await
                    .unwrap();

                //TODO(DAN): allow compatibility with async and explicit migrations
                let noria =
                    NoriaConnector::new(
                        ch.clone(),
                        auto_increments.clone(),
                        query_cache.clone(),
                        None,
                    )
                    .instrument(connection.in_scope(|| {
                        span!(Level::DEBUG, "Building migration task noria connector")
                    }))
                    .await;

                let mut migration_handler = MigrationHandler::new(
                    noria,
                    upstream,
                    cache,
                    validate_queries,
                    std::time::Duration::from_millis(loop_interval),
                    std::time::Duration::from_secs(max_retry * 60),
                    shutdown_recv,
                );
                migration_handler.run().await
            };

            rt.handle().spawn(fut);
        }

        if options.explicit_migrations {
            let ch = ch.clone();
            let loop_interval = options.outputs_polling_interval;
            let query_status_cache = query_status_cache.clone();
            let shutdown_recv = shutdown_sender.subscribe();
            let fut = async move {
                let mut outputs_synchronizer = OutputsSynchronizer::new(
                    ch,
                    query_status_cache,
                    std::time::Duration::from_secs(loop_interval),
                    shutdown_recv,
                );
                outputs_synchronizer.run().await
            };
            rt.handle().spawn(fut);
        }

        // Spawn a thread for handling this adapter's HTTP request server.
        let router_handle = {
            let (handle, valve) = Valve::new();
            let query_cache = query_status_cache.clone();
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

        let migration_mode = if options.async_migrations || options.explicit_migrations {
            MigrationMode::OutOfBand
        } else {
            MigrationMode::InRequestPath
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
                options.use_aws_external_address,
            );
            rt.handle().spawn(fut);
        }

        while let Some(Ok(s)) = rt.block_on(listener.next()) {
            // bunch of stuff to move into the async block below
            let ch = ch.clone();
            let (auto_increments, query_cache) = (auto_increments.clone(), query_cache.clone());
            let migration_mode = migration_mode.clone();
            let mut connection_handler = self.connection_handler.clone();
            let region = options.region.clone();
            let upstream_db_url = options.upstream_db_url.clone();
            let backend_builder = BackendBuilder::new()
                .slowlog(options.log_slow)
                .users(users.clone())
                .require_authentication(!options.allow_unauthenticated_connections)
                .dialect(self.dialect)
                .mirror_ddl(self.mirror_ddl)
                .query_log(qlog_sender.clone(), options.query_log_ad_hoc)
                .validate_queries(options.validate_queries, options.fail_invalidated_queries)
                .allow_unsupported_set(options.allow_unsupported_set)
                .migration_mode(migration_mode)
                .explain_last_statement(options.explain_last_statement);

            // can't move query_status_cache into the async move block
            let query_status_cache = query_status_cache.clone();
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

                let backend =
                    backend_builder
                        .clone()
                        .build(noria, upstream, query_status_cache.clone());
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

        drop(ch);
        // We use `shutdown_timeout` instead of `shutdown_background` in case any
        // blocking IO is ongoing.
        info!("Waiting up to 20s for tasks to complete shutdown");
        rt.shutdown_timeout(std::time::Duration::from_secs(20));

        Ok(())
    }
}

async fn my_ip(destination: &str, use_aws_external: bool) -> Option<IpAddr> {
    if use_aws_external {
        return my_aws_ip().await.ok();
    }

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

// TODO(peter): Pull this out to a shared util between noria-server and noria-adapter
async fn my_aws_ip() -> anyhow::Result<IpAddr> {
    let client = reqwest::Client::builder().build()?;
    let token: String = client
        .put(AWS_METADATA_TOKEN_ENDPOINT)
        .header("X-aws-ec2-metadata-token-ttl-seconds", "21600")
        .send()
        .await?
        .text()
        .await?
        .parse()?;

    Ok(client
        .get(AWS_PRIVATE_IP_ENDPOINT)
        .header("X-aws-ec2-metadata-token", &token)
        .send()
        .await?
        .text()
        .await?
        .parse()?)
}

/// Facilitates continuously updating consul with this adapters externally accessibly http
/// endpoint.
async fn reconcile_endpoint_registration(
    authority_address: String,
    deployment: String,
    port: u16,
    use_aws_external: bool,
) {
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
        let ip = match my_ip(&authority_address, use_aws_external).await {
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
    mut shutdown_recv: broadcast::Receiver<()>,
) {
    let _span = info_span!("query-logger");

    loop {
        select! {
            event = receiver.recv() => {
                if let Some(event) = event {
                    let query = match event.query {
                        Some(s) => match s.as_ref() {
                            SqlQuery::Select(stmt) => {
                                let mut stmt = stmt.clone();
                                anonymize_literals(&mut stmt);
                                stmt.to_string()
                            },
                            _ => "".to_string()
                        },
                        _ => "".to_string()
                    };

                    if let Some(parse) = event.parse_duration {
                        metrics::histogram!(
                            noria_client_metrics::recorded::QUERY_LOG_PARSE_TIME,
                            parse,
                            "query" => query.clone(),
                            "event_type" => SharedString::from(event.event),
                            "query_type" => SharedString::from(event.sql_type)
                        );
                    }

                    if let Some(noria) = event.noria_duration {
                        metrics::histogram!(
                            noria_client_metrics::recorded::QUERY_LOG_EXECUTION_TIME,
                            noria.as_secs_f64(),
                            "query" => query.clone(),
                            "database_type" => String::from(noria_client_metrics::DatabaseType::Noria),
                            "event_type" => SharedString::from(event.event),
                            "query_type" => SharedString::from(event.sql_type)
                        );
                    }

                    if let Some(upstream) = event.upstream_duration {
                        metrics::histogram!(
                            noria_client_metrics::recorded::QUERY_LOG_EXECUTION_TIME,
                            upstream.as_secs_f64(),
                            "query" => query.clone(),
                            "database_type" => String::from(noria_client_metrics::DatabaseType::Mysql),
                            "event_type" => SharedString::from(event.event),
                            "query_type" => SharedString::from(event.sql_type)
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

impl From<DatabaseType> for noria_client_metrics::DatabaseType {
    fn from(database_type: DatabaseType) -> Self {
        match database_type {
            DatabaseType::Mysql => noria_client_metrics::DatabaseType::Mysql,
            DatabaseType::Psql => noria_client_metrics::DatabaseType::Psql,
        }
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
            "noria-mysql",
            "--deployment",
            "test",
            "--address",
            "0.0.0.0:3306",
            "--authority-address",
            "zookeeper:2181",
            "--allow-unauthenticated-connections",
        ]);

        assert_eq!(opts.deployment, "test");
    }

    #[test]
    fn arg_parsing_with_upstream() {
        let opts = Options::parse_from(vec![
            "noria-mysql",
            "--deployment",
            "test",
            "--address",
            "0.0.0.0:3306",
            "--authority-address",
            "zookeeper:2181",
            "--allow-unauthenticated-connections",
            "--upstream-db-url",
            "mysql://root:password@mysql:3306/readyset",
        ]);

        assert_eq!(opts.deployment, "test");
    }

    #[test]
    fn async_migrations_param_defaults() {
        let opts = Options::parse_from(vec![
            "noria-mysql",
            "--deployment",
            "test",
            "--address",
            "0.0.0.0:3306",
            "--authority-address",
            "zookeeper:2181",
            "--allow-unauthenticated-connections",
            "--upstream-db-url",
            "mysql://root:password@mysql:3306/readyset",
            "--async-migrations",
        ]);

        assert_eq!(opts.max_processing_minutes, 15);
        assert_eq!(opts.migration_task_interval, 20000);
    }
}
