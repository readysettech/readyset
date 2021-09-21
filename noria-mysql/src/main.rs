#![warn(clippy::dbg_macro)]

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use async_trait::async_trait;
use clap::Clap;
use tokio::net;
use tracing::error;

use msql_srv::MysqlIntermediary;
use nom_sql::Dialect;
use noria_client_adapter::{ConnectionHandler, DatabaseType, NoriaAdapter};

mod backend;
mod error;
mod schema;
mod upstream;
mod value;

use backend::Backend;
pub use error::Error;
use noria_mysql::MySqlQueryHandler;
use upstream::MySqlUpstream;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

#[derive(Clone, Copy)]
struct MysqlHandler;

#[async_trait]
impl ConnectionHandler for MysqlHandler {
    type UpstreamDatabase = MySqlUpstream;
    type Handler = MySqlQueryHandler;

    async fn process_connection(
        &mut self,
        stream: net::TcpStream,
        backend: noria_client::Backend<MySqlUpstream, MySqlQueryHandler>,
    ) {
        if let Err(e) = MysqlIntermediary::run_on_tcp(Backend(backend), stream).await {
            error!(err = %e, "connection lost");
        }
    }
}

#[derive(Clap)]
struct Options {
    #[clap(flatten)]
    adapter_options: noria_client_adapter::Options,
}

fn main() -> anyhow::Result<()> {
    let options = Options::parse();

    let mut adapter = NoriaAdapter {
        name: "noria-mysql",
        version: "0.0.1",
        description: "MySQL adapter for Noria.",
        default_address: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 3306),
        connection_handler: MysqlHandler,
        database_type: DatabaseType::Mysql,
        dialect: Dialect::MySQL,
        mirror_ddl: false,
    };

    adapter.run(options.adapter_options)
}
