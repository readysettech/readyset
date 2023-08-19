use async_trait::async_trait;
use mysql_srv::MySqlIntermediary;
use readyset_mysql::{MySqlQueryHandler, MySqlUpstream};
use tokio::net::TcpStream;
use tracing::{error, instrument};

use crate::ConnectionHandler;

#[derive(Clone, Copy)]
pub struct VitessHandler {
    /// Whether to log statements received by the client
    pub enable_statement_logging: bool,
}

#[async_trait]
impl ConnectionHandler for VitessHandler {
    type UpstreamDatabase = MySqlUpstream;
    type Handler = MySqlQueryHandler;

    #[instrument(level = "debug", "connection", skip_all, fields(addr = ?stream.peer_addr().unwrap()))]
    async fn process_connection(
        &mut self,
        stream: TcpStream,
        backend: readyset_adapter::Backend<MySqlUpstream, MySqlQueryHandler>,
    ) {
        if let Err(e) = MySqlIntermediary::run_on_tcp(
            readyset_mysql::Backend {
                noria: backend,
                enable_statement_logging: self.enable_statement_logging,
            },
            stream,
            self.enable_statement_logging,
        )
        .await
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
