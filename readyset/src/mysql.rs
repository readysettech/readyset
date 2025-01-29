use std::io;
use std::sync::Arc;

use database_utils::TlsMode;
use mysql_srv::MySqlIntermediary;
use readyset_adapter::upstream_database::LazyUpstream;
use readyset_mysql::{MySqlQueryHandler, MySqlUpstream};
use tokio::net::TcpStream;
use tokio_native_tls::TlsAcceptor;
use tracing::{debug, error};

use crate::ConnectionHandler;

#[derive(Clone)]
pub struct MySqlHandler {
    /// Whether to log statements received by the client
    pub enable_statement_logging: bool,
    /// Optional struct to accept a TLS handshake and return a `TlsConnection`.
    pub tls_acceptor: Option<Arc<TlsAcceptor>>,
    /// Indicates which type of client connections are allowed
    pub tls_mode: TlsMode,
}

impl ConnectionHandler for MySqlHandler {
    type UpstreamDatabase = LazyUpstream<MySqlUpstream>;
    type Handler = MySqlQueryHandler;

    async fn process_connection(
        &mut self,
        stream: TcpStream,
        backend: readyset_adapter::Backend<LazyUpstream<MySqlUpstream>, MySqlQueryHandler>,
    ) {
        if let Err(e) = MySqlIntermediary::run_on_tcp(
            readyset_mysql::Backend {
                noria: backend,
                enable_statement_logging: self.enable_statement_logging,
            },
            stream,
            self.enable_statement_logging,
            self.tls_acceptor.clone(),
            self.tls_mode,
        )
        .await
        {
            if e.kind() == io::ErrorKind::Other {
                debug!(err = %e, "connection lost, error ignored")
            } else {
                error!(err = %e, "connection lost");
            }
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
