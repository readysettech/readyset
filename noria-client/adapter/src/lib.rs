#![warn(clippy::dbg_macro)]
#![deny(macro_use_extern_crate)]

use std::collections::HashMap;
use std::io;
use std::marker::Send;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, RwLock};

use anyhow::anyhow;
use async_compression::tokio::write::GzipEncoder;
use async_trait::async_trait;
use clap::Clap;
use futures_util::future::FutureExt;
use futures_util::stream::StreamExt;
use maplit::hashmap;
use metrics_exporter_prometheus::PrometheusBuilder;
use noria_client::coverage::QueryCoverageInfoRef;
use noria_client::{QueryHandler, UpstreamDatabase};
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;
use tokio::net;
use tokio_stream::wrappers::TcpListenerStream;
use tracing::{debug, error, info, span, Level};
use tracing_futures::Instrument;

use nom_sql::{Dialect, SelectStatement};
use noria::consensus::AuthorityType;
use noria::{ControllerHandle, ReadySetError};
use noria_client::backend::noria_connector::NoriaConnector;
use noria_client::{Backend, BackendBuilder};

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

    /// Make all reads run against Noria and upstream, returning the upstream result.
    #[clap(long, requires("upstream-db-url"))]
    mirror_reads: bool,

    /// Analyze coverage of queries during execution, and save a report of coverage on exit.
    ///
    /// Implies --race-reads
    #[clap(long, env = "COVERAGE_ANALYSIS", requires("upstream-db-url"))]
    coverage_analysis: Option<PathBuf>,

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

        if options.prometheus_metrics {
            let _guard = rt.enter();
            let database_label: noria_client_metrics::recorded::DatabaseType =
                self.database_type.into();
            PrometheusBuilder::new()
                .add_global_label("database_type", database_label)
                .add_global_label("deployment", &options.deployment)
                .install()
                .unwrap();
        }

        let query_coverage_info = options
            .coverage_analysis
            .is_some()
            .then(QueryCoverageInfoRef::default);

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
                .query_coverage_info(query_coverage_info);
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
        info!("Exiting");
        drop(rt);

        Ok(())
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
