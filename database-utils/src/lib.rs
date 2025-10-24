use std::cmp::max;
use std::collections::HashMap;
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
use pem::Pem;
use serde::{Deserialize, Serialize};

use readyset_errors::{ReadySetError, ReadySetResult};
use readyset_sql::{Dialect, ast::SqlIdentifier};
use readyset_util::redacted::RedactedString;
use {mysql_async as mysql, tokio_postgres as pgsql};

use crate::error::DatabaseURLParseError;
use crate::tls::{ServerCertVerification, get_mysql_tls_config, get_tls_connector};

mod connection;
pub mod error;
pub mod tls;

pub use connection::{
    DatabaseConnection, DatabaseConnectionPool, DatabaseStatement, QueryResults,
    QueryableConnection, SimpleQueryResults, Transaction,
};
pub use error::DatabaseError;

const DEFAULT_TIMEZONE_NAME: &str = "Etc/UTC";

#[allow(missing_docs)] // If we add docs they get added into --help binary text which is confusing
#[derive(Debug, Clone, Parser, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpstreamConfig {
    /// URL for the upstream database to connect to. Should include username and password if
    /// necessary. This is used to proxy queries to the upstream database.
    #[arg(long, env = "UPSTREAM_DB_URL")]
    #[serde(default)]
    pub upstream_db_url: Option<RedactedString>,

    /// URL for the CDC database to connect to. Should include username and password if
    /// necessary. This is used for replication and snapshotting. Defaults to the same as
    /// `--upstream-db-url`
    #[arg(long, env = "CDC_DB_URL")]
    #[serde(default)]
    pub cdc_db_url: Option<RedactedString>,

    /// Disable verification of SSL certificates supplied by the upstream database (postgres
    /// only, ignored for mysql). Ignored if `--upstream-db-url` is not passed.
    ///
    /// # Warning
    ///
    /// You should think very carefully before using this flag. If invalid certificates are
    /// trusted, any certificate for any site will be trusted for use, including expired
    /// certificates. This introduces significant vulnerabilities, and should only be used as a
    /// last resort.
    #[arg(long, env = "DISABLE_UPSTREAM_SSL_VERIFICATION")]
    #[serde(default)]
    pub disable_upstream_ssl_verification: bool,

    /// A path to a pem root certificate file that the upstream connection will trust.
    #[arg(long, env = "SSL_ROOT_CERT")]
    #[serde(default)]
    pub ssl_root_cert: Option<PathBuf>,

    /// Disable running DDL Streaming Replication Setup for PostgreSQL. If this flag is set the DDL
    /// Streaming Replication Setup SQL queries will need to be manually run on the primary server
    /// before streaming replication will start.
    #[arg(long, env = "DISABLE_SETUP_DDL_REPLICATION")]
    #[serde(default)]
    pub disable_setup_ddl_replication: bool,

    /// Disable running CREATE PUBLICATION query for PostgreSQL. If this flag is set a publication
    /// named readyset should be added for all tables manually on the primary server before
    /// streaming replication will start.
    #[arg(long, env = "DISABLE_CREATE_PUBLICATION")]
    #[serde(default)]
    pub disable_create_publication: bool,

    /// Server ID to use when registering as a replication follower with the upstream db
    ///
    /// This can be used to differentiate different Readyset deployments connected to the same
    /// upstream DB.
    ///
    /// For Postgres, this ends up being a suffix of the replication slot or resnapshot replication
    /// slot, which have prefixes of 'readyset_' and 'readyset_resnapshot_', respectively. Since a
    /// replication slot is limited by postgres to a length of 63 bytes, that means this server id
    /// must be 43 bytes or fewer.
    ///
    /// For MySQL, this must be parseable as a u32 that is unique across the replication topology.
    #[arg(long, env = "REPLICATION_SERVER_ID", value_parser = parse_repl_server_id)]
    #[serde(default)]
    pub replication_server_id: Option<ReplicationServerId>,

    /// Hostname to report when registering as a replica with the upstream database (MySQL only).
    /// If not set, no hostname will be reported.
    #[arg(long = "report-host", env = "REPORT_HOST")]
    #[serde(default)]
    pub replica_report_host: Option<String>,

    /// Port to report when registering as a replica with the upstream database (MySQL only).
    /// If not set, no port will be reported.
    #[arg(long = "report-port", env = "REPORT_PORT")]
    #[serde(default)]
    pub replica_report_port: Option<u16>,

    /// Username to report when registering as a replica with the upstream database (MySQL only).
    /// If not set, no username will be reported.
    #[arg(long = "report-user", env = "REPORT_USER")]
    #[serde(default)]
    pub replica_report_user: Option<String>,

    /// Password to report when registering as a replica with the upstream database (MySQL only).
    /// If not set, no password will be reported.
    #[arg(long = "report-password", env = "REPORT_PASSWORD")]
    #[serde(default)]
    pub replica_report_password: Option<RedactedString>,

    /// The time to wait before restarting the replicator in seconds.
    #[arg(long, hide = true, default_value = "1", value_parser = duration_from_seconds)]
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
    #[arg(long, env = "REPLICATION_TABLES")]
    #[serde(default)]
    pub replication_tables: Option<String>,

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
    /// You can also specify "*.*" to indicate that no tables should be replicated at all.
    ///
    /// Tables specified in the list will not be eligible to be used by caches.
    #[arg(long, env = "REPLICATION_TABLES_IGNORE")]
    #[serde(default)]
    pub replication_tables_ignore: Option<String>,

    /// Sets the time (in seconds) between reports of progress snapshotting the database. A value
    /// of 0 disables reporting.
    #[arg(long, default_value = "30", hide = true)]
    #[serde(default = "default_snapshot_report_interval_secs")]
    pub snapshot_report_interval_secs: u16,

    /// The maximum number of relations that will be snapshotted in parallel from the upstream. Defaults to the number of logical cores - 1.
    #[arg(long, env = "MAX_PARALLEL_SNAPSHOT_TABLES")]
    #[serde(default = "default_max_parallel_snapshot_tables")]
    pub max_parallel_snapshot_tables: Option<usize>,

    /// Sets the connection count for the pool that is used for replication and snapshotting.
    #[arg(long, default_value = "50", hide = true)]
    #[serde(default)]
    pub replication_pool_size: usize,

    /// Allow ReadySet to start even if the file descriptor limit (ulimit -n) is below our minimum
    /// requirement.
    ///
    /// If set, ReadySet still raises the soft limit to min(our requirement, hard limit). It just
    /// doesn't treat (our requirement > hard limit) as a fatal error.
    #[arg(long, env = "IGNORE_ULIMIT_CHECK", default_value = "true")]
    #[serde(default)]
    pub ignore_ulimit_check: bool,

    /// Sets the time (in seconds) between status updates sent to the upstream database. This
    /// setting also the controls the interval on which each base table will flush and sync the
    /// RocksDB WAL to disk. If set to 0, the base tables will flush and sync to disk with every
    /// write, which substantially worsens write latency.
    #[arg(
        long,
        default_value = "10",
        hide = true,
        env = "STATUS_UPDATE_INTERVAL_SECS"
    )]
    #[serde(default = "default_status_update_interval_secs")]
    pub status_update_interval_secs: u16,

    /// A string to be included as a comment in snapshot queries for MySQL.
    /// The provided value is automatically enclosed within the delimiters
    /// /* and */. The caller must supply only the raw comment text, without
    /// any comment markers.
    #[arg(long, env = "SNAPSHOT_QUERY_COMMENT", default_value = "")]
    #[serde(default)]
    pub snapshot_query_comment: Option<String>,
}

impl UpstreamConfig {
    /// Read the root certificates.
    pub async fn get_root_certs(&self) -> ReadySetResult<Option<Vec<Pem>>> {
        let path = match &self.ssl_root_cert {
            Some(path) => path,
            None => return Ok(None),
        };
        let contents = tokio::fs::read(path).await?;
        let certs = pem::parse_many(&contents)?;
        Ok(Some(certs))
    }

    /// get the cdc db url if it is set, otherwise return the upstream db url
    ///
    /// # Output
    ///
    /// - A `Result<DatabaseURL>` representing the cdc db url if it is set, otherwise the
    ///   upstream db url; returns an error if the url is invalid.
    pub fn get_cdc_db_url(&self) -> ReadySetResult<DatabaseURL> {
        let (arg, url) = if let Some(url) = &self.cdc_db_url {
            ("--cdc-db-url", url)
        } else if let Some(url) = &self.upstream_db_url {
            ("--upstream-db-url", url)
        } else {
            return Err(ReadySetError::UrlParseFailed(
                "Please provide --cdc-db-url or --upstream-db-url".to_string(),
            ));
        };

        url.parse().map_err(|e| {
            ReadySetError::UrlParseFailed(format!("Invalid URL supplied to {arg}: {e}"))
        })
    }

    pub fn from_url<S: AsRef<str>>(url: S) -> Self {
        UpstreamConfig {
            upstream_db_url: Some(url.as_ref().to_string().into()),
            cdc_db_url: Some(url.as_ref().to_string().into()),
            ..Default::default()
        }
    }

    pub fn max_parallel_snapshot_tables(&self) -> usize {
        self.max_parallel_snapshot_tables
            .unwrap_or_else(|| default_max_parallel_snapshot_tables().expect("always Some"))
    }

    pub fn default_schema_search_path(&self) -> Vec<SqlIdentifier> {
        if self.upstream_db_url.is_some()
            && let Ok(ref db_url) = self
                .upstream_db_url
                .as_ref()
                .unwrap()
                .parse::<DatabaseURL>()
        {
            return db_url.default_schema_search_path();
        }
        vec![]
    }

    pub fn default_timezone_name(&self) -> SqlIdentifier {
        match self
            .upstream_db_url
            .as_ref()
            .map(|url| url.parse::<DatabaseURL>())
        {
            Some(Ok(db_url)) => db_url.default_timezone_name(),
            _ => DEFAULT_TIMEZONE_NAME.into(),
        }
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub struct ReplicationServerId(pub String);

impl Display for ReplicationServerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0[..])
    }
}

impl From<&std::ffi::OsStr> for ReplicationServerId {
    fn from(os_s: &std::ffi::OsStr) -> Self {
        let mut s = os_s.to_string_lossy().to_string();
        s.truncate(43);
        Self(s)
    }
}

fn parse_repl_server_id(s: &str) -> Result<ReplicationServerId, String> {
    let s = s.trim();
    // Postgres restricts identifiers to 63 bytes or fewer, and we add a prefix of at most 20 bytes
    // ("readyset_resnapshot_")
    if s.len() > 43 {
        return Err("Replication server id must be 43 characters or fewer".to_string());
    }

    // Replication slot names may only contain lower case letters, numbers, and the underscore
    // character.
    if s.chars()
        .any(|c| !(c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_'))
    {
        return Err("Replication server id may only contain lower case letters, numbers, and the underscore character".to_string());
    }

    Ok(ReplicationServerId(s.to_string()))
}

fn default_replicator_restart_timeout() -> Duration {
    UpstreamConfig::default().replicator_restart_timeout
}

fn default_snapshot_report_interval_secs() -> u16 {
    UpstreamConfig::default().snapshot_report_interval_secs
}

fn default_max_parallel_snapshot_tables() -> Option<usize> {
    // Default to leaving one logical core free for background tasks, proxying, etc.
    Some(max(1, num_cpus::get() - 1))
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
            cdc_db_url: Default::default(),
            disable_upstream_ssl_verification: false,
            disable_setup_ddl_replication: false,
            disable_create_publication: false,
            replication_server_id: Default::default(),
            replica_report_host: Default::default(),
            replica_report_port: Default::default(),
            replica_report_user: Default::default(),
            replica_report_password: Default::default(),
            replicator_restart_timeout: Duration::from_secs(1),
            replication_tables: Default::default(),
            replication_tables_ignore: Default::default(),
            snapshot_report_interval_secs: 30,
            ssl_root_cert: None,
            replication_pool_size: 50,
            ignore_ulimit_check: false,
            status_update_interval_secs: 10,
            max_parallel_snapshot_tables: default_max_parallel_snapshot_tables(),
            snapshot_query_comment: Default::default(),
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

impl From<DatabaseType> for readyset_sql::Dialect {
    fn from(value: DatabaseType) -> Self {
        match value {
            DatabaseType::MySQL => Dialect::MySQL,
            DatabaseType::PostgreSQL => Dialect::PostgreSQL,
        }
    }
}

impl From<readyset_sql::Dialect> for DatabaseType {
    fn from(value: readyset_sql::Dialect) -> Self {
        match value {
            Dialect::MySQL => DatabaseType::MySQL,
            Dialect::PostgreSQL => DatabaseType::PostgreSQL,
        }
    }
}

/// URL for an upstream database.
///
/// [`DatabaseURL`]s can be constructed directly via the [`From`] implementations, or parsed from a
/// database URL using the [`FromStr`] implementation. A [`DatabaseConnection`] can be built from a
/// [`DatabaseURL`] via the [`connect` method](Self::connect).
#[derive(Eq, PartialEq, Debug, Clone, From)]
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
            let builder = OptsBuilder::from_opts(mysql::Opts::from_url(s)?).prefer_socket(false);
            Ok(Self::MySQL(builder.into()))
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
    /// Create a new [`DatabaseConnection`] by connecting to the database at this database URL.
    pub async fn connect(
        &self,
        verification: &ServerCertVerification,
    ) -> Result<DatabaseConnection, DatabaseError> {
        match self {
            DatabaseURL::MySQL(opts) => {
                let opts = if let Some(ssl_opts) = get_mysql_tls_config(verification) {
                    OptsBuilder::from_opts(opts.clone())
                        .ssl_opts(ssl_opts)
                        .into()
                } else {
                    opts.clone()
                };
                Ok(DatabaseConnection::MySQL(mysql::Conn::new(opts).await?))
            }
            DatabaseURL::PostgreSQL(config) => {
                let connector = get_tls_connector(verification)?;
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
    /// # use database_utils::tls::ServerCertVerification;
    /// # use database_utils::{DatabaseURL, DatabaseError, QueryableConnection};
    /// # use std::str::FromStr;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), DatabaseError> {
    /// let mut url = DatabaseURL::from_str("mysql://root:noria@localhost/test").unwrap();
    /// let pool = url.pool_builder(&ServerCertVerification::Default)?.max_connections(16).build()?;
    /// let mut conn = pool.get_conn().await?;
    /// conn.query_drop("SHOW TABLES").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn pool_builder(
        self,
        verification: &ServerCertVerification,
    ) -> Result<DatabaseConnectionPoolBuilder, DatabaseError> {
        match self {
            DatabaseURL::MySQL(opts) => {
                let mut builder = mysql_async::OptsBuilder::from_opts(opts);
                if let Some(ssl_opts) = get_mysql_tls_config(verification) {
                    builder = builder.ssl_opts(ssl_opts);
                }
                Ok(DatabaseConnectionPoolBuilder::MySQL(
                    builder,
                    mysql_async::PoolOpts::default(),
                ))
            }
            DatabaseURL::PostgreSQL(opts) => Ok(DatabaseConnectionPoolBuilder::PostgreSQL(
                deadpool_postgres::Pool::builder(deadpool_postgres::Manager::from_config(
                    opts,
                    postgres_native_tls::MakeTlsConnector::new(get_tls_connector(verification)?),
                    deadpool_postgres::ManagerConfig {
                        recycling_method: deadpool_postgres::RecyclingMethod::Fast,
                    },
                )),
            )),
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

    /// Returns the username for this database URL.
    pub fn user(&self) -> Option<&str> {
        match self {
            DatabaseURL::MySQL(opts) => opts.user(),
            DatabaseURL::PostgreSQL(config) => config.get_user(),
        }
    }

    /// Sets the username for this database URL.
    pub fn set_user(&mut self, user: &str) {
        match self {
            DatabaseURL::MySQL(opts) => {
                *opts = OptsBuilder::from_opts(opts.clone()).user(Some(user)).into();
            }
            DatabaseURL::PostgreSQL(config) => {
                config.user(user);
            }
        }
    }

    /// Returns the password for this database URL.
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

    /// Sets the password for this database URL.
    pub fn set_password(&mut self, pass: &str) {
        match self {
            DatabaseURL::MySQL(opts) => {
                *opts = OptsBuilder::from_opts(opts.clone()).pass(Some(pass)).into();
            }
            DatabaseURL::PostgreSQL(config) => {
                config.password(pass);
            }
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

    /// Returns `true` if this url is a [`DatabaseURL::MySQL`] URL.
    #[must_use]
    pub fn is_mysql(&self) -> bool {
        matches!(self, Self::MySQL(..))
    }

    /// Returns `true` if this url is a [`DatabaseURL::PostgreSQL`] URL.
    #[must_use]
    pub fn is_postgres(&self) -> bool {
        matches!(self, Self::PostgreSQL(..))
    }

    /// Returns the SQL dialect for the URL.
    pub fn dialect(&self) -> Dialect {
        match self {
            Self::PostgreSQL(_) => Dialect::PostgreSQL,
            Self::MySQL(_) => Dialect::MySQL,
        }
    }

    pub fn default_schema_search_path(&self) -> Vec<SqlIdentifier> {
        let mut paths = vec![];

        match self {
            Self::MySQL(_) => {
                if let Some(db) = self.db_name() {
                    paths.push(SqlIdentifier::from(db));
                }
            }
            Self::PostgreSQL(_) => {
                if let Some(u) = self.user() {
                    paths.push(SqlIdentifier::from(u));
                }
                paths.push(SqlIdentifier::from("public"));
            }
        }

        paths
    }

    pub fn default_timezone_name(&self) -> SqlIdentifier {
        DEFAULT_TIMEZONE_NAME.into()
    }

    pub fn set_port(&mut self, port: u16) {
        match self {
            Self::MySQL(opts) => {
                *opts = OptsBuilder::from_opts(opts.clone()).tcp_port(port).into();
            }
            Self::PostgreSQL(config) => {
                config.port(port);
            }
        }
    }

    pub fn set_host(&mut self, host: String) {
        match self {
            Self::MySQL(opts) => {
                *opts = OptsBuilder::from_opts(opts.clone())
                    .ip_or_hostname(host.as_str())
                    .into();
            }
            Self::PostgreSQL(config) => {
                config.host(host);
            }
        }
    }

    pub fn set_program_or_application_name(&mut self, program_name: String) {
        match self {
            Self::MySQL(opts) => {
                let attrs: HashMap<String, String> =
                    vec![("_program_name".to_string(), program_name.clone())]
                        .into_iter()
                        .collect();
                *opts = OptsBuilder::from_opts(opts.clone())
                    .connect_attributes(attrs)
                    .into();
            }
            Self::PostgreSQL(config) => {
                config.application_name(program_name);
            }
        }
    }
}

/// Indicates which types of client connections are allowed
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, ValueEnum)]
pub enum TlsMode {
    /// client can use either plain or TLS connections
    #[default]
    Optional,
    /// TLS connections are disabled
    Disabled,
    /// Only TLS connections are allowed
    Required,
}
