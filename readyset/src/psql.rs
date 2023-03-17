use std::io::Read;
use std::sync::Arc;

use async_trait::async_trait;
use clap::Parser;
use readyset_errors::ReadySetResult;
use readyset_psql::{PostgreSqlQueryHandler, PostgreSqlUpstream};
use tokio::net;
use tokio_native_tls::{native_tls, TlsAcceptor};
use tracing::{error, instrument};

use crate::ConnectionHandler;

/// readyset-psql specific options
#[derive(Clone, Debug, Parser)]
pub struct Options {
    /// The pkcs12 identity file (certificate and key) used by ReadySet for establishing TLS
    /// connections as the server.
    ///
    /// ReadySet will not accept TLS connections if there is no identity file specified.
    #[clap(long, env = "READYSET_IDENTITY_FILE")]
    readyset_identity_file: Option<String>,
    /// Password for the pkcs12 identity file used by ReadySet for establishing TLS connections as
    /// the server.
    ///
    /// If password is not provided, ReadySet will try using an empty string to unlock the identity
    /// file.
    #[clap(long, requires = "readyset-identity-file")]
    readyset_identity_file_password: Option<String>,
}

/// Contains psql-srv specific `Options` and whether to enable statement logging.
pub struct Config {
    pub options: Options,
    pub enable_statement_logging: bool,
}

#[derive(Clone)]
pub struct PsqlHandler {
    /// Whether to log statements received from the client
    pub enable_statement_logging: bool,
    /// Optional struct to accept a TLS handshake and return a `TlsConnection`.
    pub tls_acceptor: Option<Arc<TlsAcceptor>>,
}

/// Load the `native_tls::Identity` from user provided `Config`.
fn load_pkcs12_identity(options: &Options) -> ReadySetResult<Option<native_tls::Identity>> {
    let Some(ref path) = options.readyset_identity_file else {
        return Ok(None);
    };

    let mut identity_file = std::fs::File::open(path)?;
    let mut identity = vec![];
    identity_file.read_to_end(&mut identity)?;

    let password = options
        .readyset_identity_file_password
        .clone()
        .unwrap_or_default();

    Ok(Some(native_tls::Identity::from_pkcs12(
        &identity, &password,
    )?))
}

impl PsqlHandler {
    pub fn new(config: Config) -> ReadySetResult<PsqlHandler> {
        let tls_acceptor = match load_pkcs12_identity(&config.options)? {
            Some(identity) => Some(Arc::new(TlsAcceptor::from(native_tls::TlsAcceptor::new(
                identity,
            )?))),
            None => None,
        };

        Ok(PsqlHandler {
            enable_statement_logging: config.enable_statement_logging,
            tls_acceptor,
        })
    }
}

#[async_trait]
impl ConnectionHandler for PsqlHandler {
    type UpstreamDatabase = PostgreSqlUpstream;
    type Handler = PostgreSqlQueryHandler;

    #[instrument(level = "debug", "connection", skip_all, fields(addr = ?stream.peer_addr().unwrap()))]
    async fn process_connection(
        &mut self,
        stream: net::TcpStream,
        backend: readyset_adapter::Backend<PostgreSqlUpstream, PostgreSqlQueryHandler>,
    ) {
        psql_srv::run_backend(
            readyset_psql::Backend(backend),
            stream,
            self.enable_statement_logging,
            self.tls_acceptor.clone(),
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
