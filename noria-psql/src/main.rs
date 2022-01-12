use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use async_trait::async_trait;
use clap::Parser;
use nom_sql::Dialect;
use tokio::net;

use noria_client::backend as cl;
use noria_client_adapter::{ConnectionHandler, DatabaseType, NoriaAdapter};
use noria_psql::{Backend, PostgreSqlQueryHandler, PostgreSqlUpstream};
use psql_srv::run_backend;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

#[derive(Clone, Copy)]
struct PsqlHandler;

#[async_trait]
impl ConnectionHandler for PsqlHandler {
    type UpstreamDatabase = PostgreSqlUpstream;
    type Handler = PostgreSqlQueryHandler;

    async fn process_connection(
        &mut self,
        stream: net::TcpStream,
        backend: cl::Backend<PostgreSqlUpstream, PostgreSqlQueryHandler>,
    ) {
        let backend = Backend(backend);
        run_backend(backend, stream).await;
    }
}

#[derive(Parser)]
#[clap(name = "noria-psql", version)]
struct Options {
    #[clap(flatten)]
    adapter_options: noria_client_adapter::Options,
}

fn main() -> anyhow::Result<()> {
    let options = Options::parse();

    let mut adapter = NoriaAdapter {
        description: "PostgreSQL adapter for Noria.",
        default_address: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 3306),
        connection_handler: PsqlHandler,
        database_type: DatabaseType::Psql,
        dialect: Dialect::PostgreSQL,
        // PostgreSQL has no replication of DDL, so we have to mirror any DDL changes between the
        // upstream db and noria
        mirror_ddl: true,
    };

    adapter.run(options.adapter_options)
}
