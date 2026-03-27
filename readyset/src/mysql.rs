use std::io;
use std::sync::Arc;

use clap::Parser;
use database_utils::TlsMode;
use mysql_srv::{AuthCache, AuthPlugin, MySqlIntermediary};
use readyset_adapter::upstream_database::LazyUpstream;
use readyset_mysql::{MySqlQueryHandler, MySqlUpstream};
use tokio::net::TcpStream;
use tokio_native_tls::TlsAcceptor;
use tracing::{debug, error};

use crate::ConnectionHandler;

/// MySQL-specific CLI options.
#[derive(Clone, Debug, Parser)]
pub struct MySqlOptions {
    /// Authentication method for MySQL client connections
    ///
    /// [possible values: mysql_native_password, caching_sha2_password]
    #[arg(
        long,
        env = "MYSQL_AUTHENTICATION_METHOD",
        default_value_t = AuthPlugin::default(),
    )]
    pub mysql_authentication_method: AuthPlugin,
}

#[derive(Clone)]
pub struct MySqlHandler {
    /// Whether to log statements received by the client
    pub enable_statement_logging: bool,
    /// Optional struct to accept a TLS handshake and return a `TlsConnection`.
    pub tls_acceptor: Option<Arc<TlsAcceptor>>,
    /// Indicates which type of client connections are allowed
    pub tls_mode: TlsMode,
    /// Shared cache for caching_sha2_password fast-auth lookups.
    pub auth_cache: Arc<AuthCache>,
    /// The authentication plugin to advertise to clients.
    pub mysql_authentication_method: AuthPlugin,
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
            self.auth_cache.clone(),
            self.mysql_authentication_method,
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
