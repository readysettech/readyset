#![warn(clippy::dbg_macro)]

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use async_trait::async_trait;
use clap::Clap;
use nom_sql::Dialect;
use tokio::net;

use noria_client::backend as cl;
use noria_client_adapter::{ConnectionHandler, DatabaseType, NoriaAdapter};
use noria_psql::backend::Backend;
use psql_srv::run_backend;

#[derive(Clone, Copy)]
struct PsqlHandler;

#[async_trait]
impl ConnectionHandler for PsqlHandler {
    async fn process_connection(
        &mut self,
        stream: net::TcpStream,
        backend: cl::Backend<noria::ZookeeperAuthority>,
    ) {
        let backend = Backend(backend);
        run_backend(backend, stream).await;
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
        name: "noria-psql",
        version: "0.1.0",
        description: "PostgreSQL adapter for Noria.",
        default_address: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 3306),
        connection_handler: PsqlHandler,
        database_type: DatabaseType::Psql,
        dialect: Dialect::PostgreSQL,
    };

    adapter.run(options.adapter_options)
}
