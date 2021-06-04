mod connector;
mod noria_adapter;
mod snapshot;
mod wal;
mod wal_reader;

use clap::{App, Arg};
use slog::*;

pub use connector::{PostgresPosition, PostgresWalConnector, WalAction};
pub use noria_adapter::Builder;
pub use snapshot::PostgresReplicator;

#[tokio::main]
async fn main() -> noria::ReadySetResult<()> {
    let matches = App::new("noria-postgres-connector")
        .version("0.0.1")
        .arg(
            Arg::with_name("postgres-address")
                .short("a")
                .long("postgres-address")
                .takes_value(true)
                .required(true)
                .default_value("127.0.0.1")
                .env("POSTGRES_SERVER")
                .help("IP or Hostname for PostgreSQL."),
        )
        .arg(
            Arg::with_name("postgres-port")
                .short("p")
                .long("postgres-port")
                .takes_value(true)
                .required(true)
                .default_value("5432")
                .env("POSTGRES_PORT")
                .help("Port number for PostgreSQL."),
        )
        .arg(
            Arg::with_name("postgres-user")
                .short("u")
                .long("postgres-user")
                .takes_value(true)
                .required(false)
                .env("POSTGRES_USER")
                .help("User name for PostgreSQL."),
        )
        .arg(
            Arg::with_name("postgres-password")
                .long("postgres-password")
                .takes_value(true)
                .required(false)
                .env("POSTGRES_PASSWORD")
                .help("User password for PostgreSQL."),
        )
        .arg(
            Arg::with_name("zookeeper-address")
                .long("zookeeper-address")
                .takes_value(true)
                .required(true)
                .default_value("127.0.0.1:2181")
                .env("ZOOKEEPER_URL")
                .help("IP:PORT for Zookeeper."),
        )
        .arg(
            Arg::with_name("deployment")
                .long("deployment")
                .takes_value(true)
                .required(true)
                .env("NORIA_DEPLOYMENT")
                .help("Noria deployment ID to attach to."),
        )
        .arg(
            Arg::with_name("db-name")
                .long("db-name")
                .takes_value(true)
                .required(true)
                .env("DB_NAME")
                .help("The db name."),
        )
        .get_matches();

    let addr = matches.value_of("postgres-address").unwrap();
    let port = clap::value_t!(matches.value_of("postgres-port"), u16).unwrap();
    let user_name = matches.value_of("postgres-user");
    let user_password = matches.value_of("postgres-password");
    let zookeeper_address = matches.value_of("zookeeper-address").unwrap();
    let deployment = matches.value_of("deployment").unwrap();
    let schema = matches.value_of("db-name").unwrap();

    let plain = slog_term::PlainSyncDecorator::new(std::io::stdout());
    let log = Logger::root(slog_term::FullFormat::new(plain).build().fuse(), o!());

    let builder = noria_adapter::Builder::new(addr, port)
        .with_user(user_name)
        .with_password(user_password)
        .with_zookeeper_addr(Some(zookeeper_address))
        .with_deployment(Some(deployment))
        .with_database(Some(schema))
        .with_logger(log);

    builder.start().await
}
