use crate::channel::Channel;
use crate::codec;
use crate::error::Error;
use crate::message::FrontendMessage;
use crate::protocol::Protocol;
use crate::Backend;
use tokio::io::{AsyncRead, AsyncWrite};

/// A helper struct that can be used to run a `Protocol` on a `Backend` and `Channel`.
pub struct Runner<B: Backend, C> {
    backend: B,
    channel: Channel<C, B::Row>,
    protocol: Protocol,
}

impl<B: Backend, C: AsyncRead + AsyncWrite + Unpin> Runner<B, C> {
    /// A simple run loop. For each `FrontendMessage` received on `channel`, use `protocol` to
    /// generate a response. Then send the response. If an error occurs, use `protocol` to generate
    /// an error response, then send the error response.
    pub async fn run(backend: B, byte_channel: C) {
        let mut runner = Runner {
            backend,
            channel: Channel::new(byte_channel),
            protocol: Protocol::new(),
        };

        while let Some(message) = runner.channel.next().await {
            match runner.handle_request(message).await {
                Ok(_) => {}
                Err(e) => {
                    runner
                        .handle_error(e)
                        .await
                        .unwrap_or_else(|e| eprintln!("{}", e));
                }
            };
        }
    }

    async fn handle_request(
        &mut self,
        request: Result<FrontendMessage, codec::DecodeError>,
    ) -> Result<(), Error> {
        let response = self
            .protocol
            .on_request(request?, &mut self.backend, &mut self.channel)
            .await?;
        self.channel.send(response).await?;
        Ok(())
    }

    async fn handle_error(&mut self, error: Error) -> Result<(), Error> {
        let response = self.protocol.on_error::<B>(error).await?;
        self.channel.send(response).await?;
        Ok(())
    }
}
