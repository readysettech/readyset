#![feature(never_type, hash_raw_entry)]

pub(crate) mod mysql_connector;
pub(crate) mod noria_adapter;
pub(crate) mod postgres_connector;

use clap::Parser;
use mysql_async as mysql;
use noria::consensus::AuthorityType;
use noria_adapter::{AdapterOpts, NoriaAdapter};
use tokio_postgres as pgsql;

/// A replication connector from an existing database to Noria
#[derive(Parser)]
#[clap(version = "1.0")]
struct Opts {
    /// Noria deployment ID to attach to.
    #[clap(short, long, env("NORIA_DEPLOYMENT"), forbid_empty_values = true)]
    deployment: String,
    /// IP:PORT for Zookeeper.
    #[clap(short, long, env("AUTHORITY_ADDRESS"), default_value("127.0.0.1:2181"))]
    authority_address: String,
    #[clap(long, env("AUTHORITY"), default_value("zookeeper"), possible_values = &["consul", "zookeeper"])]
    authority: AuthorityType,
    #[clap(subcommand)]
    subcmd: DbOpts,

    #[clap(flatten)]
    logging: readyset_logging::Options,
}

#[derive(Parser, Debug)]
enum DbOpts {
    Url(UrlOpts),
    Mysql(MySqlOpts),
    Postgres(PostgresOpts),
}

/// Use a connection URL
#[derive(Parser, Debug)]
struct UrlOpts {
    #[clap(parse(try_from_str))]
    url: AdapterOpts,
}

/// Use MySQL for replication
#[derive(Parser, Debug)]
struct MySqlOpts {
    /// IP or Hostname for MySQL.
    #[clap(short, long, env("MYSQL_SERVER"), default_value("127.0.0.1"))]
    address: String,
    /// Port number for MySQL.
    #[clap(short, long, env("MYSQL_PORT"), default_value("3306"))]
    port: u16,
    /// User name for MySQL.
    #[clap(short, long, env("MYSQL_USER"))]
    user: Option<String>,
    /// User password for MySQL.
    #[clap(long, env("MYSQL_PASSWORD"))]
    password: Option<String>,
    /// The database name to replicate.
    #[clap(long, env("DB_NAME"))]
    db_name: String,
}

/// Use PostgreSQL for replication
#[derive(Parser, Debug)]
struct PostgresOpts {
    /// IP or Hostname for PostgreSQL.
    #[clap(short, long, env("PGHOST"), default_value("127.0.0.1"))]
    address: String,
    /// Port number for PostgreSQL.
    #[clap(short, long, env("PGPORT"), default_value("5432"))]
    port: u16,
    /// User name for PostgreSQL.
    #[clap(short, long, env("PGUSER"))]
    user: Option<String>,
    /// User password for PostgreSQL.
    #[clap(long, env("PGPASSWORD"))]
    password: Option<String>,
    /// The database name to replicate.
    #[clap(long, env("PGDATABASE"))]
    db_name: String,
}

impl From<MySqlOpts> for AdapterOpts {
    fn from(opts: MySqlOpts) -> Self {
        let MySqlOpts {
            address,
            port,
            user,
            password,
            db_name,
        } = opts;

        AdapterOpts::MySql(
            mysql::OptsBuilder::default()
                .ip_or_hostname(address)
                .tcp_port(port)
                .user(user)
                .pass(password)
                .db_name(Some(db_name))
                .into(),
        )
    }
}

impl From<PostgresOpts> for AdapterOpts {
    fn from(opts: PostgresOpts) -> Self {
        let PostgresOpts {
            address,
            port,
            user,
            password,
            db_name,
        } = opts;

        let mut conf = pgsql::Config::new();
        conf.host(&address).port(port).dbname(&db_name);
        if let Some(user) = user {
            conf.user(&user);
        }

        if let Some(password) = password {
            conf.password(&password);
        }

        AdapterOpts::Postgres(conf)
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<!> {
    let opts: Opts = Opts::parse();

    opts.logging.init()?;

    let options = match opts.subcmd {
        DbOpts::Url(opts) => opts.url,
        DbOpts::Mysql(opts) => opts.into(),
        DbOpts::Postgres(opts) => opts.into(),
    };

    let authority = opts
        .authority
        .to_authority(&opts.authority_address, &opts.deployment)
        .await;

    NoriaAdapter::start_with_authority(authority, options).await?
}
