#![warn(clippy::dbg_macro)]
#[macro_use]
extern crate tracing;

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use async_trait::async_trait;
use clap::Clap;
use tokio::net;

use msql_srv::MysqlIntermediary;
use nom_sql::Dialect;
use noria_client::backend::mysql_connector::MySqlConnector;
use noria_client::Backend;
use noria_client_adapter::{ConnectionHandler, DatabaseType, NoriaAdapter};

#[derive(Clone, Copy)]
struct MysqlHandler;

#[async_trait]
impl ConnectionHandler for MysqlHandler {
    type UpstreamDatabase = MySqlConnector;

    async fn process_connection(
        &mut self,
        stream: net::TcpStream,
        backend: Backend<noria::ZookeeperAuthority, MySqlConnector>,
    ) {
        if let Err(e) = MysqlIntermediary::run_on_tcp(backend, stream).await {
            match e {
                noria_client::Error::Io(e) => {
                    error!(err = ?e, "connection lost");
                }
                _ => {
                    error!(err = ?e)
                }
            }
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
    };

    adapter.run(options.adapter_options)
}
