use std::io;
use std::sync::Arc;

use tokio::io::{AsyncRead, AsyncWrite};
use tokio_native_tls::TlsAcceptor;
use tokio_native_tls::TlsStream;

use tracing::debug;

pub(crate) struct SwitchableStream<S: AsyncRead + AsyncWrite + Unpin>(Option<Stream<S>>);

enum Stream<S: AsyncRead + AsyncWrite + Unpin> {
    Plain(S),
    Tls(TlsStream<S>),
}

impl<S: AsyncRead + AsyncWrite + Unpin> SwitchableStream<S> {
    pub fn new(s: S) -> SwitchableStream<S> {
        SwitchableStream(Some(Stream::Plain(s)))
    }

    pub async fn switch_to_tls(&mut self, tls_acceptor: Arc<TlsAcceptor>) -> io::Result<()> {
        match self.0.take() {
            Some(Stream::Plain(stream)) => {
                let tls_stream = tls_acceptor.accept(stream).await;
                match tls_stream {
                    Ok(tls_stream) => {
                        debug!("Established TLS connection");
                        self.0 = Some(Stream::Tls(tls_stream));
                    }
                    Err(_) => {
                        // The acceptor consumes the stream, so we can't respond with an error
                        return Err(io::Error::other("TLS handshake failed"));
                    }
                };
            }
            Some(Stream::Tls(_)) => {
                return Err(io::Error::other(
                    "tls variant found when plain was expected",
                ))
            }
            None => unreachable!(),
        };
        Ok(())
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncRead for SwitchableStream<S> {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match &mut self.0.as_mut().unwrap() {
            Stream::Plain(stream) => std::pin::Pin::new(stream).poll_read(cx, buf),
            Stream::Tls(stream) => std::pin::Pin::new(stream).poll_read(cx, buf),
        }
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncWrite for SwitchableStream<S> {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        match &mut self.0.as_mut().unwrap() {
            Stream::Plain(stream) => std::pin::Pin::new(stream).poll_write(cx, buf),
            Stream::Tls(stream) => std::pin::Pin::new(stream).poll_write(cx, buf),
        }
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match &mut self.0.as_mut().unwrap() {
            Stream::Plain(stream) => std::pin::Pin::new(stream).poll_flush(cx),
            Stream::Tls(stream) => std::pin::Pin::new(stream).poll_flush(cx),
        }
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match &mut self.0.as_mut().unwrap() {
            Stream::Plain(stream) => std::pin::Pin::new(stream).poll_shutdown(cx),
            Stream::Tls(stream) => std::pin::Pin::new(stream).poll_shutdown(cx),
        }
    }
}
