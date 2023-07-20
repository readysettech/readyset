use std::sync::Arc;

use tokio::io::{AsyncRead, AsyncWrite};
use tokio_native_tls::TlsAcceptor;
use tracing::{error, info};

use crate::channel::Channel;
use crate::error::Error;
use crate::message::FrontendMessage;
use crate::protocol::Protocol;
use crate::{codec, PsqlBackend};

/// A helper struct that can be used to run a `Protocol` on a `Backend` and `Channel`.
pub struct Runner<B: PsqlBackend, C> {
    /// ReadySet `Backend` to handle routing queries to the upstream or Readyset
    backend: B,
    /// Read and write stream. Handles io, TLS and protocol decoding/encoding
    channel: Channel<C>,
    /// Handles Postgres protocol messages and maintains protocol state
    protocol: Protocol,
    /// Whether to log statements received from the client
    enable_statement_logging: bool,
}

/// Indicates whether the client is initiating a TLS connection, or the client has closed the
/// stream.
enum MainLoopStatus {
    // The stream has closed
    Terminate,
    // Restart the main loop after intiating a TLS connection
    RestartWithTls,
}

impl<B: PsqlBackend> Runner<B, tokio::net::TcpStream> {
    /// A simple run loop. For each `FrontendMessage` received on `channel`, use `protocol` to
    /// generate a response. Then send the response. If an error occurs, use `protocol` to generate
    /// an error response, then send the error response.
    pub async fn run(
        backend: B,
        byte_channel: tokio::net::TcpStream,
        enable_statement_logging: bool,
        tls_acceptor: Option<Arc<TlsAcceptor>>,
    ) {
        let mut protocol = Protocol::new();
        if tls_acceptor.is_some() {
            protocol.allow_tls_connections()
        };
        let mut runner = Runner {
            backend,
            channel: Channel::new(byte_channel),
            protocol,
            enable_statement_logging,
        };

        // Connection has closed or is waiting for tls handshake
        let loop_status = runner.main_loop().await;

        if matches!(loop_status, MainLoopStatus::RestartWithTls) && let Some(acceptor) = tls_acceptor {
            let backend = runner.backend;
            let stream = runner.channel.into_inner();
            let mut protocol = runner.protocol;

            let stream = acceptor.accept(stream).await;

            match stream {
                Ok(stream) => {
                    info!("Established TLS connection");
                    protocol.completed_ssl_handshake(
                        stream
                            .get_ref()
                            .tls_server_end_point()
                            .expect("Nothing we can do if getting the TLS server endpoint fails")
                    );
                    let mut runner = Runner {
                        backend,
                        channel: Channel::new(stream),
                        protocol,
                        enable_statement_logging,
                    };
                    // Run loop again. Warn client if we get an unexpected RestartWithTls status.
                    if matches!(runner.main_loop().await, MainLoopStatus::RestartWithTls) {
                        let _ = runner.handle_error(Error::UnexpectedMessage("Received second SSLRequest".to_string())).await;
                    }
                }
                Err(error) => {
                    // The acceptor consumes the stream, so we can't respond with an error
                    error!(%error);
                }
            }
        } else if matches!(loop_status, MainLoopStatus::RestartWithTls) && tls_acceptor.is_none() {
            // Nothing to do, but warn client that ReadySet experienced an internal error.
            let _ = runner.handle_error(Error::InternalError("Attempted to complete TLS handshake with no TlsAcceptor".to_string())).await;
        }
    }
}

impl<B: PsqlBackend, C: AsyncRead + AsyncWrite + Unpin> Runner<B, C> {
    async fn handle_request(
        &mut self,
        request: Result<FrontendMessage, codec::DecodeError>,
    ) -> Result<(), Error> {
        let request = request?;
        if self.enable_statement_logging {
            info!(target: "client_statement", "{:?}", request);
        }
        if request == FrontendMessage::Flush {
            self.channel.flush().await?;
        }
        let response = self
            .protocol
            .on_request(request, &mut self.backend, &mut self.channel)
            .await?;
        self.channel.send(response).await?;
        Ok(())
    }

    async fn handle_error(&mut self, error: Error) -> Result<(), Error> {
        let response = self.protocol.on_error::<B>(error).await?;
        self.channel.send(response).await?;
        Ok(())
    }

    /// Main loop for Protocol handling. When the client requests a TLS connection, we exit this
    /// loop so that we can construct a TLS capable `Channel` and restart.
    async fn main_loop(&mut self) -> MainLoopStatus {
        while let Some(message) = self.channel.next().await {
            match self.handle_request(message).await {
                Ok(()) => {
                    // Client requests a TLS channel. We exit so that we can reconstruct a TLS
                    // capable `Channel`
                    if self.protocol.is_initiating_ssl_handshake() {
                        return MainLoopStatus::RestartWithTls;
                    }
                }
                // Return an error message but do not exit the loop
                Err(e) => {
                    self.handle_error(e)
                        .await
                        .unwrap_or_else(|e| eprintln!("{}", e));
                }
            }
        }
        MainLoopStatus::Terminate
    }
}
