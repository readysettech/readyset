use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use clap::Parser;
use database_utils::DatabaseType;
use readyset::mysql::MySqlHandler;
use readyset::psql::PsqlHandler;
use readyset::{NoriaAdapter, Options};
use readyset_server::check_disk_space;

fn main() -> anyhow::Result<()> {
    let options = Options::parse();
    if !options.no_disk_space_check {
        check_disk_space()?;
    }
    match options.database_type()? {
        DatabaseType::MySQL => NoriaAdapter {
            description: "MySQL adapter for ReadySet.",
            default_address: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 3307),
            connection_handler: MySqlHandler {
                enable_statement_logging: options.tracing.statement_logging,
            },
            database_type: DatabaseType::MySQL,
            parse_dialect: nom_sql::Dialect::MySQL,
            expr_dialect: readyset_data::Dialect::DEFAULT_MYSQL,
        }
        .run(options),
        DatabaseType::PostgreSQL => NoriaAdapter {
            description: "PostgreSQL adapter for ReadySet.",
            default_address: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 5433),
            connection_handler: PsqlHandler::new(readyset::psql::Config {
                options: options.psql_options.clone(),
                enable_statement_logging: options.tracing.statement_logging,
            })?,
            database_type: DatabaseType::PostgreSQL,
            parse_dialect: nom_sql::Dialect::PostgreSQL,
            expr_dialect: readyset_data::Dialect::DEFAULT_POSTGRESQL,
        }
        .run(options),
    }
}
