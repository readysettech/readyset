#![feature(generic_associated_types)]

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use clap::Parser;
use nom_sql::Dialect;
use readyset_client_adapter::{DatabaseType, NoriaAdapter};
use readyset_mysql::MySqlHandler;
use readyset_version::*;

mod backend;
mod constants;
mod error;
mod schema;
mod upstream;
mod value;

use error::Error;
use readyset_mysql::MySqlQueryHandler;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[derive(Parser)]
#[clap(version = VERSION_STR_PRETTY)]
struct Options {
    #[clap(flatten)]
    adapter_options: readyset_client_adapter::Options,
}

fn main() -> anyhow::Result<()> {
    let options = Options::parse();

    let mut adapter = NoriaAdapter {
        description: "MySQL adapter for ReadySet.",
        default_address: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 3306),
        connection_handler: MySqlHandler,
        database_type: DatabaseType::MySql,
        dialect: Dialect::MySQL,
    };

    adapter.run(options.adapter_options)
}
