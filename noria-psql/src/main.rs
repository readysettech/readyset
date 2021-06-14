#![warn(clippy::dbg_macro)]
use async_trait::async_trait;
use noria_client::backend as cl;
use noria_client_adapter::{ConnectionHandler, NoriaAdapter};
use noria_psql::backend::Backend;
use psql_srv::run_backend;
use tokio::net;

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

fn main() {
    let mut adapter = NoriaAdapter {
        name: "noria-psql",
        version: "0.1.0",
        description: "PostgreSQL adapter for Noria.",
        default_address: "127.0.0.1:5432",
        connection_handler: PsqlHandler,
    };
    adapter.run()
}
