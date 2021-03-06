use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use async_trait::async_trait;
use clap::Parser;
use mysql_srv::MysqlIntermediary;
use nom_sql::Dialect;
use readyset_client_adapter::{ConnectionHandler, DatabaseType, NoriaAdapter};
use tokio::net;
use tracing::{error, instrument};

mod backend;
mod error;
mod schema;
mod upstream;
mod value;

use backend::Backend;
pub use error::Error;
use readyset_mysql::MySqlQueryHandler;
use upstream::MySqlUpstream;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[derive(Clone, Copy)]
struct MysqlHandler;

#[async_trait]
impl ConnectionHandler for MysqlHandler {
    type UpstreamDatabase = MySqlUpstream;
    type Handler = MySqlQueryHandler;

    #[instrument(level = "debug", "connection", skip_all, fields(addr = ?stream.peer_addr().unwrap()))]
    async fn process_connection(
        &mut self,
        stream: net::TcpStream,
        backend: readyset_client::Backend<MySqlUpstream, MySqlQueryHandler>,
    ) {
        if let Err(e) = MysqlIntermediary::run_on_tcp(Backend::new(backend), stream).await {
            error!(err = %e, "connection lost");
        }
    }

    async fn immediate_error(self, stream: net::TcpStream, error_message: String) {
        if let Err(error) = mysql_srv::send_immediate_err(
            stream,
            mysql_srv::ErrorKind::ER_UNKNOWN_ERROR,
            error_message.as_bytes(),
        )
        .await
        {
            error!(%error, "Could not send immediate error packet")
        }
    }
}

#[derive(Parser)]
#[clap(name = "readyset-mysql", version)]
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
    };

    adapter.run(options.adapter_options)
}
