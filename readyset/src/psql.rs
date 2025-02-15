use std::sync::Arc;

use clap::Parser;
use database_utils::TlsMode;
use readyset_adapter::upstream_database::LazyUpstream;
use readyset_errors::ReadySetResult;
use readyset_psql::{AuthenticationMethod, PostgreSqlQueryHandler, PostgreSqlUpstream};
use tokio::net;
use tokio_native_tls::TlsAcceptor;
use tracing::error;

use crate::ConnectionHandler;

// readyset-psql specific options
#[derive(Clone, Debug, Parser)]
pub struct PsqlOptions {
    /// Authentication method to use for PostgreSQL clients
    #[arg(
        long,
        env = "POSTGRES_AUTHENTICATION_METHOD",
        default_value = "scram-sha-256"
    )]
    postgres_authentication_method: AuthenticationMethod,
}

pub struct Config {
    /// Contains psql-srv specific `Options`
    pub options: PsqlOptions,
    /// Whether to log statements received by the client
    pub enable_statement_logging: bool,
    /// Optional struct to accept a TLS handshake and return a `TlsConnection`.
    pub tls_acceptor: Option<Arc<TlsAcceptor>>,
    /// Indicates which type of client connections are allowed
    pub tls_mode: TlsMode,
}

#[derive(Clone)]
pub struct PsqlHandler {
    /// Whether to log statements received from the client
    pub enable_statement_logging: bool,
    /// Authentication method to use for clients
    pub authentication_method: AuthenticationMethod,
    /// Optional struct to accept a TLS handshake and return a `TlsConnection`.
    pub tls_acceptor: Option<Arc<TlsAcceptor>>,
    /// Indicates which type of client connections are allowed
    pub tls_mode: TlsMode,
}

impl PsqlHandler {
    pub fn new(config: Config) -> ReadySetResult<PsqlHandler> {
        Ok(PsqlHandler {
            enable_statement_logging: config.enable_statement_logging,
            authentication_method: config.options.postgres_authentication_method,
            tls_acceptor: config.tls_acceptor,
            tls_mode: config.tls_mode,
        })
    }
}

impl ConnectionHandler for PsqlHandler {
    type UpstreamDatabase = LazyUpstream<PostgreSqlUpstream>;
    type Handler = PostgreSqlQueryHandler;

    async fn process_connection(
        &mut self,
        stream: net::TcpStream,
        backend: readyset_adapter::Backend<
            LazyUpstream<PostgreSqlUpstream>,
            PostgreSqlQueryHandler,
        >,
    ) {
        psql_srv::run_backend(
            readyset_psql::Backend::new(backend)
                .with_authentication_method(self.authentication_method),
            stream,
            self.enable_statement_logging,
            self.tls_acceptor.clone(),
            self.tls_mode,
        )
        .await;
    }

    async fn immediate_error(self, stream: net::TcpStream, error_message: String) {
        if let Err(error) = psql_srv::send_immediate_err::<readyset_psql::Backend, _>(
            stream,
            psql_srv::Error::InternalError(error_message),
        )
        .await
        {
            error!(%error, "Could not send immediate error packet")
        }
    }
}
