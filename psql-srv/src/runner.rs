use crate::channel::Channel;
use crate::codec;
use crate::error::Error;
use crate::message::FrontendMessage;
use crate::protocol::Protocol;
use crate::Backend;
use tokio::io::{AsyncRead, AsyncWrite};

pub struct Runner<B: Backend, C> {
    backend: B,
    channel: Channel<C, B::Row>,
    protocol: Protocol,
}

impl<B: Backend, C: AsyncRead + AsyncWrite + Unpin> Runner<B, C> {
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
        self.protocol
            .handle_request(request?, &mut self.backend, &mut self.channel)
            .await?;
        Ok(())
    }

    async fn handle_error(&mut self, error: Error) -> Result<(), Error> {
        self.protocol
            .handle_error::<B, C>(error, &mut self.channel)
            .await?;
        Ok(())
    }
}
