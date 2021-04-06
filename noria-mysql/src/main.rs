#![warn(clippy::dbg_macro)]
#[macro_use]
extern crate tracing;

use async_trait::async_trait;
use msql_srv::MysqlIntermediary;
use noria_client::backend::Backend;
use noria_client_adapter::{ConnectionHandler, NoriaAdapter};
use std::io;
use tokio::net;

#[derive(Clone, Copy)]
struct MysqlHandler;

#[async_trait]
impl ConnectionHandler for MysqlHandler {
    async fn process_connection(&mut self, stream: net::TcpStream, backend: Backend) {
        if let Err(noria_client::backend::error::Error::Io(e)) =
            MysqlIntermediary::run_on_tcp(backend, stream).await
        {
            match e.kind() {
                io::ErrorKind::ConnectionReset | io::ErrorKind::BrokenPipe => {}
                _ => {
                    error!(err = ?e, "connection lost");
                }
            }
        }
    }
}

fn main() {
    let mut adapter = NoriaAdapter {
        name: "noria-mysql",
        version: "0.0.1",
        description: "MySQL adapter for Noria.",
        default_address: "127.0.0.1:3306",
        connection_handler: MysqlHandler,
    };
    adapter.run()
}
