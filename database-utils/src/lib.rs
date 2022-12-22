#![feature(never_type)]

use std::convert::TryFrom;
use std::fmt::{self, Display};
use std::marker::{Send, Sync};
use std::num::ParseIntError;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

use clap::Parser;
use derive_more::From;
use error::{ConnectionType, DatabaseTypeParseError};
use futures::{StreamExt, TryStreamExt};
use mysql_async::prelude::Queryable;
use mysql_async::OptsBuilder;
use readyset_client::{ReadySetError, ReadySetResult};
use readyset_util::redacted::RedactedString;
use serde::{Deserialize, Serialize};
use {mysql_async as mysql, tokio_postgres as pgsql};

use crate::error::{DatabaseError, DatabaseURLParseError};

pub mod error;

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
    #[clap(long, env = "DISABLE_UPSTREAM_SSL_VERIFICATION")]
    #[serde(default)]
    pub disable_upstream_ssl_verification: bool,

    /// A path to a pem or der certificate of the root that the upstream connection will trust.
    #[clap(long, env = "SSL_ROOT_CERT")]
    #[serde(default)]
    pub ssl_root_cert: Option<PathBuf>,

    /// Disable running DDL Streaming Replication Setup for PostgreSQL. If this flag is set
    /// the DDL Streaming Replication Setup SQL queries will need to be manually run on the
    /// primary server before streaming replication will start.
    #[clap(long, env = "DISABLE_SETUP_DDL_REPLICATION")]
    #[serde(default)]
    pub disable_setup_ddl_replication: bool,

    /// Sets the server id when acquiring a binlog replication slot.
    #[clap(long, hide = true)]
    #[serde(default)]
    pub replication_server_id: Option<u32>,

    /// The time to wait before restarting the replicator in seconds.
    #[clap(long, hide = true, default_value = "30", parse(try_from_str = duration_from_seconds))]
    #[serde(default = "default_replicator_restart_timeout")]
    pub replicator_restart_timeout: Duration,

    #[clap(long, env = "REPLICATION_TABLES")]
    #[serde(default)]
    pub replication_tables: Option<RedactedString>,

    /// Sets the time (in seconds) between reports of progress snapshotting the database. A value
    /// of 0 disables reporting.
    #[clap(long, default_value = "30")]
    #[serde(default = "default_snapshot_report_interval_secs")]
    pub snapshot_report_interval_secs: u16,

    /// Sets the connection count for the pool that is used for replication and snapshotting.
    #[clap(long, default_value = "50")]
    #[serde(default)]
    pub replication_pool_size: usize,
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
            replicator_restart_timeout: Duration::from_secs(30),
            replication_tables: Default::default(),
            snapshot_report_interval_secs: 30,
            ssl_root_cert: None,
            replication_pool_size: 50,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatabaseType {
    MySQL,
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
            "postgresql" => Ok(Self::PostgreSQL),
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
            Ok(Self::PostgreSQL(pgsql::Config::from_str(s)?))
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
    /// Create a new [`DatabaseConnection`] by connecting to the database at this database URL
    pub async fn connect(&self) -> Result<DatabaseConnection, DatabaseError<!>> {
        match self {
            DatabaseURL::MySQL(opts) => Ok(DatabaseConnection::MySQL(
                mysql::Conn::new(opts.clone()).await?,
            )),
            DatabaseURL::PostgreSQL(config) => {
                let connector = native_tls::TlsConnector::builder().build().unwrap(); // Never returns an error
                let tls = postgres_native_tls::MakeTlsConnector::new(connector);
                let (client, connection) = config.connect(tls).await?;
                let connection_handle =
                    tokio::spawn(async move { connection.await.map_err(Into::into) });
                Ok(DatabaseConnection::PostgreSQL(client, connection_handle))
            }
        }
    }

    /// Does test initialization by dropping the schema/database and recreating it before connecting
    #[cfg(test)]
    pub async fn init_and_connect(&self) -> Result<DatabaseConnection, DatabaseError<!>> {
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
                let connector = native_tls::TlsConnector::builder().build().unwrap(); // Never returns an error
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
}

/// An enum wrapper around either a MySQL or PostgreSQL connection.
pub enum DatabaseConnection {
    /// A MySQL database connection.
    MySQL(mysql_async::Conn),
    /// A PostgreSQL database connection.
    PostgreSQL(
        tokio_postgres::Client,
        tokio::task::JoinHandle<Result<(), ReadySetError>>,
    ),
}

async fn convert_mysql_results<'a, 't, P, V>(
    mut results: mysql::QueryResult<'a, 't, P>,
) -> Result<Vec<Vec<V>>, DatabaseError<V::Error>>
where
    P: mysql::prelude::Protocol,
    V: TryFrom<mysql::Value>,
    <V as TryFrom<mysql::Value>>::Error: std::error::Error,
{
    results
        .map(|mut r| {
            (0..r.columns().len())
                .map(|c| {
                    V::try_from(r.take::<mysql::Value, _>(c).unwrap())
                        .map_err(DatabaseError::ValueConversion)
                })
                .collect::<Result<Vec<V>, _>>()
        })
        .await?
        .into_iter()
        .collect::<Result<Vec<Vec<V>>, _>>()
}

async fn convert_pgsql_results<V>(results: pgsql::RowStream) -> Result<Vec<Vec<V>>, pgsql::Error>
where
    for<'a> V: pgsql::types::FromSql<'a>,
{
    results
        .map(|r| {
            let r = r?;
            (0..r.len())
                .map(|c| r.try_get(c))
                .collect::<Result<Vec<_>, _>>()
        })
        .try_collect()
        .await
}

impl DatabaseConnection {
    /// Executes query_drop for either mysql or postgres, whichever is the underlying
    /// DatabaseConnection variant. Since we aren't returning any results, we can't get a
    /// ValueError type as the result of TryFrom<mysql::Value>, so the error type is annotated with
    /// the never type.
    pub async fn query_drop<Q>(&mut self, stmt: Q) -> Result<(), DatabaseError<!>>
    where
        Q: AsRef<str> + Send + Sync,
    {
        match self {
            DatabaseConnection::MySQL(conn) => Ok(conn.query_drop(stmt).await?),
            DatabaseConnection::PostgreSQL(client, _jh) => {
                client.simple_query(stmt.as_ref()).await?;
                Ok(())
            }
        }
    }

    /// Executes query for either mysql or postgres, whichever is the underlying
    /// DatabaseConnection variant.
    pub async fn query<Q, V>(&mut self, query: Q) -> Result<Vec<Vec<V>>, DatabaseError<V::Error>>
    where
        Q: AsRef<str> + Send + Sync,
        V: TryFrom<mysql::Value>,
        <V as TryFrom<mysql::Value>>::Error: std::error::Error + Send + Sync + 'static,
        for<'a> V: pgsql::types::FromSql<'a>,
    {
        match self {
            DatabaseConnection::MySQL(conn) => {
                convert_mysql_results(conn.query_iter(query).await?).await
            }
            DatabaseConnection::PostgreSQL(client, _jh) => {
                // TODO: We should use simple_query here instead, because query_raw will still
                // prepare. simple_query returns a different result type, so may take some work to
                // get it work properly here.
                convert_pgsql_results(client.query_raw(query.as_ref(), Vec::<i8>::new()).await?)
                    .await
                    .map_err(DatabaseError::PostgreSQL)
            }
        }
    }

    /// Executes prepare for either mysql or postgres, whichever is the underlying
    /// DatabaseConnection variant.
    pub async fn prepare<Q>(&mut self, query: Q) -> Result<DatabaseStatement, DatabaseError<!>>
    where
        Q: AsRef<str> + Send + Sync,
    {
        match self {
            DatabaseConnection::MySQL(conn) => Ok(conn.prep(query).await?.into()),
            DatabaseConnection::PostgreSQL(client, _jh) => {
                Ok(client.prepare(query.as_ref()).await?.into())
            }
        }
    }

    /// Executes a prepared statement for either mysql or postgres, whichever is the underlying
    /// DatabaseConnection variant. Will also optionally prepare and execute a query string, if
    /// supplied instead.
    pub async fn execute<P, V>(
        &mut self,
        stmt: impl Into<DatabaseStatement>,
        params: P,
    ) -> Result<Vec<Vec<V>>, DatabaseError<V::Error>>
    where
        P: Into<mysql_async::Params> + IntoIterator + Send,
        P::IntoIter: ExactSizeIterator,
        P::Item: pgsql::types::BorrowToSql,
        V: TryFrom<mysql::Value>,
        <V as TryFrom<mysql::Value>>::Error: std::error::Error + Send + Sync + 'static,
        for<'a> V: pgsql::types::FromSql<'a>,
    {
        match stmt.into() {
            DatabaseStatement::MySql(s) => {
                convert_mysql_results(
                    self.as_mysql_conn()
                        .ok_or(DatabaseError::WrongConnection(ConnectionType::MySQL))?
                        .exec_iter(s, params)
                        .await?,
                )
                .await
            }
            DatabaseStatement::Postgres(s) => convert_pgsql_results(
                self.as_postgres_conn()
                    .ok_or(DatabaseError::WrongConnection(ConnectionType::PostgreSQL))?
                    .query_raw(&s, params)
                    .await?,
            )
            .await
            .map_err(DatabaseError::PostgreSQL),
            DatabaseStatement::Str(s) => self.execute_str(s.as_ref(), params).await,
        }
    }

    /// Executes a simple query string, and can potentially return an underlying database error.
    /// That might be an underlying mysql or postgres error, depending on the underlying connection
    /// type, or it may be a value conversion error in the case that the caller is using a custom
    /// value type to convert results into.
    pub async fn execute_str<P, V>(
        &mut self,
        stmt: &str,
        params: P,
    ) -> Result<Vec<Vec<V>>, DatabaseError<V::Error>>
    where
        P: Into<mysql_async::Params> + IntoIterator + Send,
        P::IntoIter: ExactSizeIterator,
        P::Item: pgsql::types::BorrowToSql,
        V: TryFrom<mysql::Value>,
        <V as TryFrom<mysql::Value>>::Error: std::error::Error + Send + Sync + 'static,
        for<'a> V: pgsql::types::FromSql<'a>,
    {
        match self {
            DatabaseConnection::MySQL(conn) => {
                convert_mysql_results(conn.exec_iter(stmt, params).await?).await
            }
            DatabaseConnection::PostgreSQL(client, _jh) => {
                convert_pgsql_results(client.query_raw(stmt, params).await?)
                    .await
                    .map_err(DatabaseError::PostgreSQL)
            }
        }
    }

    pub fn as_mysql_conn(&mut self) -> Option<&mut mysql_async::Conn> {
        if let DatabaseConnection::MySQL(c) = self {
            Some(c)
        } else {
            None
        }
    }

    pub fn as_postgres_conn(&mut self) -> Option<&mut tokio_postgres::Client> {
        if let DatabaseConnection::PostgreSQL(c, _jh) = self {
            Some(c)
        } else {
            None
        }
    }
}

/// An enum wrapper around various prepared statement types. Either a mysql_async prepared
/// statement, a tokio_postgres prepared statement, or a plain query string that we would like to
/// both prepare and execute.
#[derive(From, Clone)]
pub enum DatabaseStatement {
    /// A MySQL prepared statement returned from a prepare call in `mysql_async`.
    MySql(mysql_async::Statement),
    /// A PostgreSQL prepared statement returned from a prepare call in `tokio_postgres`.
    Postgres(tokio_postgres::Statement),
    /// A simple query string that a user would like to be both prepared and executed.
    Str(String),
}

impl From<&str> for DatabaseStatement {
    fn from(s: &str) -> DatabaseStatement {
        DatabaseStatement::Str(s.to_owned())
    }
}

impl From<&String> for DatabaseStatement {
    fn from(s: &String) -> DatabaseStatement {
        DatabaseStatement::Str(s.to_owned())
    }
}
