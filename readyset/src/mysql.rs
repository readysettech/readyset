use async_trait::async_trait;
use mysql_srv::MySqlIntermediary;
use readyset_mysql::{MySqlQueryHandler, MySqlUpstream};
use readyset_tracing::error;
use tokio::net::TcpStream;
use tracing::instrument;

use crate::ConnectionHandler;

#[derive(Clone, Copy)]
pub struct MySqlHandler;

#[async_trait]
impl ConnectionHandler for MySqlHandler {
    type UpstreamDatabase = MySqlUpstream;
    type Handler = MySqlQueryHandler;

    #[instrument(level = "debug", "connection", skip_all, fields(addr = ?stream.peer_addr().unwrap()))]
    async fn process_connection(
        &mut self,
        stream: TcpStream,
        backend: readyset_adapter::Backend<MySqlUpstream, MySqlQueryHandler>,
    ) {
        if let Err(e) =
            MySqlIntermediary::run_on_tcp(readyset_mysql::Backend::new(backend), stream).await
        {
            error!(err = %e, "connection lost");
        }
    }

    async fn immediate_error(self, stream: TcpStream, error_message: String) {
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
