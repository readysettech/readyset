#![feature(generic_associated_types)]

mod backend;
mod constants;
mod error;
mod query_handler;
mod schema;
mod upstream;
mod value;

use async_trait::async_trait;
pub use backend::Backend;
pub use error::Error;
use mysql_srv::MySqlIntermediary;
pub use query_handler::MySqlQueryHandler;
use readyset_client_adapter::ConnectionHandler;
use tokio::net;
use tracing::{error, instrument};
pub use upstream::{MySqlUpstream, QueryResult};

#[derive(Clone, Copy)]
pub struct MySqlHandler;

#[async_trait]
impl ConnectionHandler for MySqlHandler {
    type UpstreamDatabase = MySqlUpstream;
    type Handler = MySqlQueryHandler;

    #[instrument(level = "debug", "connection", skip_all, fields(addr = ?stream.peer_addr().unwrap()))]
    async fn process_connection(
        &mut self,
        stream: net::TcpStream,
        backend: readyset_client::Backend<MySqlUpstream, MySqlQueryHandler>,
    ) {
        if let Err(e) = MySqlIntermediary::run_on_tcp(Backend::new(backend), stream).await {
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
