use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use clap::Parser;
use nom_sql::Dialect;
use readyset_client_adapter::{DatabaseType, NoriaAdapter};
use readyset_mysql::MysqlHandler;

mod backend;
mod constants;
mod error;
mod schema;
mod upstream;
mod value;

use error::Error;
use readyset_mysql::MySqlQueryHandler;

const COMMIT_ID: &str = match option_env!("BUILDKITE_COMMIT") {
    Some(x) => x,
    None => "unknown commit ID",
};

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[derive(Parser)]
#[clap(version = COMMIT_ID)]
struct Options {
    #[clap(flatten)]
    adapter_options: readyset_client_adapter::Options,
}

fn main() -> anyhow::Result<()> {
    let options = Options::parse();

    let mut adapter = NoriaAdapter {
        description: "MySQL adapter for ReadySet.",
        default_address: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 3306),
        connection_handler: MysqlHandler,
        database_type: DatabaseType::Mysql,
        dialect: Dialect::MySQL,
        upstream_config: (),
    };

    adapter.run(options.adapter_options)
}
