#![feature(never_type)]

use std::fmt::{self, Display};
use std::num::ParseIntError;
use std::path::PathBuf;
use std::str::{self, FromStr};
use std::time::Duration;

use clap::{Parser, ValueEnum};
use connection::DatabaseConnectionPoolBuilder;
use derive_more::From;
use error::DatabaseTypeParseError;
use mysql_async::OptsBuilder;
use native_tls::TlsConnectorBuilder;
use nom_sql::Dialect;
use readyset_errors::{ReadySetError, ReadySetResult};
use readyset_util::redacted::RedactedString;
use serde::{Deserialize, Serialize};
use {mysql_async as mysql, tokio_postgres as pgsql};

use crate::error::DatabaseURLParseError;

mod connection;
pub mod error;

pub use connection::{
    DatabaseConnection, DatabaseConnectionPool, DatabaseStatement, QueryResults,
    QueryableConnection, SimpleQueryResults, Transaction,
};
pub use error::DatabaseError;

#[allow(missing_docs)] // If we add docs they get added into --help binary text which is confusing
#[derive(Debug, Clone, Parser, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpstreamConfig {
    /// URL for the upstream database to connect to. Should include username and password if
    /// necessary
    #[clap(long, env = "UPSTREAM_DB_URL")]
    #[serde(default)]
    pub upstream_db_url: Option<RedactedString>,

    /// Disable verification of SSL certificates supplied by the upstream database (postgres
    /// only, ignored for mysql). Ignored if `--upstream-db-url` is not passed.
    ///
    /// # Warning
    ///
    /// You should think very carefully before using this flag. If invalid certificates are
    /// trusted, any certificate for any site will be trusted for use, including expired
    /// certificates. This introduces significant vulnerabilities, and should only be used as a
    /// last resort.
    #[clap(long, env = "DISABLE_UPSTREAM_SSL_VERIFICATION", hide = true)]
    #[serde(default)]
    pub disable_upstream_ssl_verification: bool,

    /// A path to a pem or der certificate of the root that the upstream connection will trust.
    #[clap(long, env = "SSL_ROOT_CERT")]
    #[serde(default)]
    pub ssl_root_cert: Option<PathBuf>,

    /// Disable running DDL Streaming Replication Setup for PostgreSQL. If this flag is set
    /// the DDL Streaming Replication Setup SQL queries will need to be manually run on the
    /// primary server before streaming replication will start.
    #[clap(long, env = "DISABLE_SETUP_DDL_REPLICATION", hide = true)]
    #[serde(default)]
    pub disable_setup_ddl_replication: bool,

    /// Server ID to use when registering as a replication follower with the upstream db
    ///
    /// This can be used to differentiate different ReadySet deployments connected to the same
    /// upstream DB.
    #[clap(long, env = "REPLICATION_SERVER_ID", hide = true)]
    #[serde(default)]
    pub replication_server_id: Option<u32>,

    /// The time to wait before restarting the replicator in seconds.
    #[clap(long, hide = true, default_value = "1", value_parser = duration_from_seconds)]
    #[serde(default = "default_replicator_restart_timeout")]
    pub replicator_restart_timeout: Duration,

    /// By default, ReadySet attempts to snapshot and replicate all tables in the database
    /// specified in --upstream-db-url. However, if the queries you want to cache in ReadySet
    /// access only a subset of tables in the database, you can use this option to limit the tables
    /// ReadySet snapshots and replicates. Filtering out tables that will not be used in caches
    /// will speed up the snapshotting process.
    ///
    /// This option accepts a comma-separated list of `<schema>.<table>` (specific table in a
    /// schema) or `<schema>.*` (all tables in a schema) for Postgres and `<database>.<table>`
    /// for MySQL. Only tables specified in the list will be eligible to be used by caches.
    #[clap(long, env = "REPLICATION_TABLES")]
    #[serde(default)]
    pub replication_tables: Option<RedactedString>,

    /// By default, ReadySet attempts to snapshot and replicate all tables in the database
    /// specified in --upstream-db-url. However, if you know the queries you want to cache in
    /// ReadySet won't access a subset of tables in the database, you can use this option to
    /// limit the tables ReadySet snapshots and replicates. Filtering out tables that will not be
    /// used in caches will speed up the snapshotting process.
    ///
    /// This option accepts a comma-separated list of `<schema>.<table>` (specific table in a
    /// schema) or `<schema>.*` (all tables in a schema) for Postgres and `<database>.<table>`
    /// for MySQL.
    ///
    /// Tables specified in the list will not be eligible to be used by caches.
    #[clap(long, env = "REPLICATION_TABLES_IGNORE")]
    #[serde(default)]
    pub replication_tables_ignore: Option<RedactedString>,

    /// Sets the time (in seconds) between reports of progress snapshotting the database. A value
    /// of 0 disables reporting.
    #[clap(long, default_value = "30", hide = true)]
    #[serde(default = "default_snapshot_report_interval_secs")]
    pub snapshot_report_interval_secs: u16,

    /// Sets the connection count for the pool that is used for replication and snapshotting.
    #[clap(long, default_value = "50", hide = true)]
    #[serde(default)]
    pub replication_pool_size: usize,

    /// Allow ReadySet to start even if the file descriptor limit (ulimit -n) is below our minimum
    /// requirement.
    ///
    /// If set, ReadySet still raises the soft limit to min(our requirement, hard limit). It just
    /// doesn't treat (our requirement > hard limit) as a fatal error.
    #[clap(long, env = "IGNORE_ULIMIT_CHECK")]
    #[serde(default)]
    pub ignore_ulimit_check: bool,

    /// Sets the time (in seconds) between status updates sent to the upstream database. This
    /// setting also the controls the interval on which each base table will flush and sync the
    /// RocksDB WAL to disk. If set to 0, the base tables will flush and sync to disk with every
    /// write, which substantially worsens write latency.
    #[clap(
        long,
        default_value = "10",
        hide = true,
        env = "STATUS_UPDATE_INTERVAL_SECS"
    )]
    #[serde(default = "default_status_update_interval_secs")]
    pub status_update_interval_secs: u16,
}

impl UpstreamConfig {
    /// Read the certificate at [`Self::ssl_root_cert`] path and try to parse it as either PEM or
    /// DER encoded certificate
    pub async fn get_root_cert(&self) -> Option<ReadySetResult<native_tls::Certificate>> {
        match self.ssl_root_cert.as_ref() {
            Some(path) => Some({
                tokio::fs::read(path)
                    .await
                    .map_err(ReadySetError::from)
                    .and_then(|cert| {
                        native_tls::Certificate::from_pem(&cert)
                            .or_else(|_| native_tls::Certificate::from_der(&cert))
                            .map_err(|_| ReadySetError::InvalidRootCertificate)
                    })
            }),
            None => None,
        }
    }

    pub fn from_url<S: AsRef<str>>(url: S) -> Self {
        UpstreamConfig {
            upstream_db_url: Some(url.as_ref().to_string().into()),
            ..Default::default()
        }
    }
}

fn default_replicator_restart_timeout() -> Duration {
    UpstreamConfig::default().replicator_restart_timeout
}

fn default_snapshot_report_interval_secs() -> u16 {
    UpstreamConfig::default().snapshot_report_interval_secs
}

fn default_status_update_interval_secs() -> u16 {
    UpstreamConfig::default().status_update_interval_secs
}

fn duration_from_seconds(i: &str) -> Result<Duration, ParseIntError> {
    i.parse::<u64>().map(Duration::from_secs)
}

impl Default for UpstreamConfig {
    fn default() -> Self {
        Self {
            upstream_db_url: Default::default(),
            disable_upstream_ssl_verification: false,
            disable_setup_ddl_replication: false,
            replication_server_id: Default::default(),
            replicator_restart_timeout: Duration::from_secs(1),
            replication_tables: Default::default(),
            replication_tables_ignore: Default::default(),
            snapshot_report_interval_secs: 30,
            ssl_root_cert: None,
            replication_pool_size: 50,
            ignore_ulimit_check: false,
            status_update_interval_secs: 10,
        }
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, ValueEnum, Serialize, Deserialize)]
pub enum DatabaseType {
    #[value(name = "mysql")]
    MySQL,

    #[value(name = "postgresql")]
    #[default]
    PostgreSQL,
}

/// Parses the strings `"mysql"` and `"postgresql"`, case-insensitively
///
/// # Examples
///
/// ```rust
/// use database_utils::DatabaseType;
///
/// let pg: DatabaseType = "postgresql".parse().unwrap();
/// assert_eq!(pg, DatabaseType::PostgreSQL);
///
/// // "postgres" is supported as an alias for DatabaseType::PostgreSQL
/// let pg: DatabaseType = "postgres".parse().unwrap();
/// assert_eq!(pg, DatabaseType::PostgreSQL);
///
/// let my: DatabaseType = "mysql".parse().unwrap();
/// assert_eq!(my, DatabaseType::MySQL);
///
/// assert!("pgsql".parse::<DatabaseType>().is_err());
/// ```
impl FromStr for DatabaseType {
    type Err = DatabaseTypeParseError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "mysql" => Ok(Self::MySQL),
            "postgresql" | "postgres" => Ok(Self::PostgreSQL),
            _ => Err(DatabaseTypeParseError {
                value: s.to_owned(),
            }),
        }
    }
}

/// Displays as either `MySQL` or PostgreSQL
///
/// # Examples
///
/// ```
/// use database_utils::DatabaseType;
///
/// assert_eq!(DatabaseType::PostgreSQL.to_string(), "postgresql");
/// ```
impl Display for DatabaseType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DatabaseType::MySQL => f.write_str("mysql"),
            DatabaseType::PostgreSQL => f.write_str("postgresql"),
        }
    }
}

/// URL for an upstream database.
///
/// [`DatabaseURL`]s can be constructed directly via the [`From`] implementations, or parsed from a
/// database URL using the [`FromStr`] implementation. A [`DatabaseConnection`] can be built from a
/// [`DatabaseURL`] via the [`connect` method](Self::connect).
#[derive(Debug, Clone, From)]
#[allow(clippy::large_enum_variant)]
pub enum DatabaseURL {
    MySQL(mysql_async::Opts),
    PostgreSQL(pgsql::Config),
}

/// Parses URLs starting with either `"mysql://"` or `"postgresql://"`.
///
/// # Examples
///
/// ```
/// use database_utils::{DatabaseType, DatabaseURL};
///
/// let mysql_url: DatabaseURL = "mysql://root:password@localhost:3306/mysql"
///     .parse()
///     .unwrap();
/// assert_eq!(mysql_url.database_type(), DatabaseType::MySQL);
/// ```
///
/// ```
/// use database_utils::{DatabaseType, DatabaseURL};
///
/// let pgsql_url: DatabaseURL = "postgresql://postgres:postgres@localhost:5432/postgres"
///     .parse()
///     .unwrap();
/// assert_eq!(pgsql_url.database_type(), DatabaseType::PostgreSQL);
/// ```
impl FromStr for DatabaseURL {
    type Err = DatabaseURLParseError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.starts_with("mysql://") {
            Ok(Self::MySQL(mysql::Opts::from_url(s)?))
        } else if s.starts_with("postgresql://") || s.starts_with("postgres://") {
            let result = Self::PostgreSQL(pgsql::Config::from_str(s)?);
            // Require that Postgres URLs specify a database
            match result.db_name() {
                Some(_) => Ok(result),
                None => Err(DatabaseURLParseError::MissingPostgresDbName),
            }
        } else {
            Err(DatabaseURLParseError::InvalidFormat)
        }
    }
}

impl From<mysql_async::OptsBuilder> for DatabaseURL {
    fn from(ob: mysql_async::OptsBuilder) -> Self {
        Self::MySQL(ob.into())
    }
}

impl DatabaseURL {
    /// Create a new [`DatabaseConnection`] by connecting to the database at this database URL. For
    /// postgres connections, optionally provide the `TlsConnectorBuilder` for Postgres.
    pub async fn connect(
        &self,
        tls_connector_builder: Option<TlsConnectorBuilder>,
    ) -> Result<DatabaseConnection, DatabaseError> {
        match self {
            DatabaseURL::MySQL(opts) => {
                if tls_connector_builder.is_some() {
                    Err(DatabaseError::TlsUnsupported)
                } else {
                    Ok(DatabaseConnection::MySQL(
                        mysql::Conn::new(opts.clone()).await?,
                    ))
                }
            }
            DatabaseURL::PostgreSQL(config) => {
                let connector = tls_connector_builder
                    .unwrap_or_else(native_tls::TlsConnector::builder)
                    .build()?;
                let tls = postgres_native_tls::MakeTlsConnector::new(connector);
                let (client, connection) = config.connect(tls).await?;
                let connection_handle =
                    tokio::spawn(async move { connection.await.map_err(Into::into) });
                Ok(DatabaseConnection::PostgreSQL(client, connection_handle))
            }
        }
    }

    /// Construct a new [`DatabaseConnectionPoolBuilder`] which can be used to build a connection
    /// pool for this database URL
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use database_utils::{DatabaseURL, DatabaseError, QueryableConnection};
    /// # use std::str::FromStr;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), DatabaseError> {
    /// let mut url = DatabaseURL::from_str("mysql://root:noria@localhost/test").unwrap();
    /// let pool = url.pool_builder(None)?.max_connections(16).build()?;
    /// let mut conn = pool.get_conn().await?;
    /// conn.query_drop("SHOW TABLES").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn pool_builder(
        self,
        tls_connector_builder: Option<TlsConnectorBuilder>,
    ) -> Result<DatabaseConnectionPoolBuilder, DatabaseError> {
        match self {
            DatabaseURL::MySQL(opts) => Ok(DatabaseConnectionPoolBuilder::MySQL(
                mysql_async::OptsBuilder::from_opts(opts),
                mysql_async::PoolOpts::default(),
            )),
            DatabaseURL::PostgreSQL(opts) => Ok(DatabaseConnectionPoolBuilder::PostgreSQL(
                deadpool_postgres::Pool::builder(deadpool_postgres::Manager::from_config(
                    opts,
                    postgres_native_tls::MakeTlsConnector::new(
                        tls_connector_builder
                            .unwrap_or_else(native_tls::TlsConnector::builder)
                            .build()?,
                    ),
                    deadpool_postgres::ManagerConfig {
                        recycling_method: deadpool_postgres::RecyclingMethod::Fast,
                    },
                )),
            )),
        }
    }

    /// Does test initialization by dropping the schema/database and recreating it before connecting
    #[cfg(test)]
    pub async fn init_and_connect(&self) -> Result<DatabaseConnection, DatabaseError> {
        use mysql_async::prelude::Queryable;

        match self {
            DatabaseURL::MySQL(opts) => {
                {
                    let test_db_name = opts.db_name().unwrap();
                    let no_db_opts =
                        mysql_async::OptsBuilder::from_opts(opts.clone()).db_name::<String>(None);
                    // First, connect without a db to do setup
                    let mut client = mysql_async::Conn::new(no_db_opts).await?;

                    // Then drop and recreate the test db
                    client
                        .query_drop(format!("DROP SCHEMA IF EXISTS {test_db_name};"))
                        .await?;
                    client
                        .query_drop(format!("CREATE SCHEMA {test_db_name};"))
                        .await?;

                    // Then switch to the test db
                    client.query_drop(format!("USE {test_db_name};")).await?;
                }

                Ok(DatabaseConnection::MySQL(
                    mysql::Conn::new(opts.clone()).await?,
                ))
            }
            DatabaseURL::PostgreSQL(config) => {
                let connector = native_tls::TlsConnector::builder().build()?;
                let tls = postgres_native_tls::MakeTlsConnector::new(connector);
                // Drop and recreate the test db
                {
                    let test_db_name = config.get_dbname().unwrap();
                    let mut no_db_config = config.clone();
                    no_db_config.dbname("postgres");
                    let (no_db_client, conn) = no_db_config.connect(tokio_postgres::NoTls).await?;
                    let jh = tokio::spawn(conn);
                    no_db_client
                        .simple_query(&format!("DROP SCHEMA IF EXISTS {test_db_name} CASCADE"))
                        .await?;
                    no_db_client
                        .simple_query(&format!("CREATE SCHEMA {test_db_name}"))
                        .await?;
                    jh.abort();
                    let _ = jh.await;
                }

                let (client, connection) = config.connect(tls).await?;
                let connection_handle =
                    tokio::spawn(async move { connection.await.map_err(Into::into) });
                Ok(DatabaseConnection::PostgreSQL(client, connection_handle))
            }
        }
    }

    /// Returns the underlying database type, either [`DatabaseType::MySQL`] or
    /// [`DatabaseType::PostgreSQL].
    pub fn database_type(&self) -> DatabaseType {
        match self {
            DatabaseURL::MySQL(_) => DatabaseType::MySQL,
            DatabaseURL::PostgreSQL(_) => DatabaseType::PostgreSQL,
        }
    }

    /// Returns the user name for this database URL
    pub fn user(&self) -> Option<&str> {
        match self {
            DatabaseURL::MySQL(opts) => opts.user(),
            DatabaseURL::PostgreSQL(config) => config.get_user(),
        }
    }

    /// Returns the password for this database URL
    ///
    /// # Panics
    ///
    /// * Panics if a postgresql URL is configured with a non-utf8 password
    pub fn password(&self) -> Option<&str> {
        match self {
            DatabaseURL::MySQL(opts) => opts.pass(),
            DatabaseURL::PostgreSQL(opts) => opts.get_password().map(|p| -> &str {
                str::from_utf8(p).expect("PostgreSQL URL configured with non-utf8 password")
            }),
        }
    }

    /// Returns the host name for this database URL
    ///
    /// # Panics
    ///
    /// * Panics if the hostname in the URL is not valid UTF8
    /// * Panics if a postgresql URL has no hostname set
    pub fn host(&self) -> &str {
        match self {
            DatabaseURL::MySQL(opts) => opts.ip_or_hostname(),
            DatabaseURL::PostgreSQL(config) => {
                let host = config
                    .get_hosts()
                    .first()
                    .expect("PostgreSQL URL has no hostname set");
                match host {
                    pgsql::config::Host::Tcp(tcp) => tcp.as_str(),
                    pgsql::config::Host::Unix(p) => p.to_str().expect("Invalid UTF-8 in host"),
                }
            }
        }
    }

    /// Returns the underlying database name.
    pub fn db_name(&self) -> Option<&str> {
        match self {
            DatabaseURL::MySQL(opts) => opts.db_name(),
            DatabaseURL::PostgreSQL(config) => config.get_dbname(),
        }
    }

    /// Sets the underlying database nname.
    pub fn set_db_name(&mut self, db_name: String) {
        match self {
            DatabaseURL::MySQL(opts) => {
                *opts = OptsBuilder::from_opts(opts.clone())
                    .db_name(Some(db_name))
                    .into();
            }
            DatabaseURL::PostgreSQL(config) => {
                config.dbname(&db_name);
            }
        }
    }

    /// Returns `true` if this url is a [`MySQL`] URL.
    ///
    /// [`MySQL`]: DatabaseURL::MySQL
    #[must_use]
    pub fn is_mysql(&self) -> bool {
        matches!(self, Self::MySQL(..))
    }

    /// Returns the SQL dialect for the URL.
    pub fn dialect(&self) -> Dialect {
        match self {
            Self::PostgreSQL(_) => Dialect::PostgreSQL,
            Self::MySQL(_) => Dialect::MySQL,
        }
    }
}
