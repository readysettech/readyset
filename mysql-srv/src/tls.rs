use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::Bytes;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_native_tls::TlsAcceptor;
use tokio_native_tls::TlsStream;

use tracing::debug;

pub(crate) struct SwitchableStream<S: AsyncRead + AsyncWrite + Unpin>(Option<Stream<S>>);

enum Stream<S: AsyncRead + AsyncWrite + Unpin> {
    Plain(S),
    Tls(TlsStream<Prefixed<S>>),
}

/// A stream that yields `prefix` before reading from `inner`.
struct Prefixed<S> {
    prefix: Bytes,
    inner: S,
}

impl<S: AsyncRead + Unpin> AsyncRead for Prefixed<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if this.prefix.is_empty() {
            return Pin::new(&mut this.inner).poll_read(cx, buf);
        }
        let n = this.prefix.len().min(buf.remaining());
        buf.put_slice(&this.prefix.split_to(n));
        Poll::Ready(Ok(()))
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for Prefixed<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write(cx, buf)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> SwitchableStream<S> {
    pub fn new(s: S) -> SwitchableStream<S> {
        SwitchableStream(Some(Stream::Plain(s)))
    }

    /// Returns `true` if the connection has been upgraded to TLS.
    #[allow(dead_code)] // only reachable via the disabled full-auth path.
    pub fn is_secure(&self) -> bool {
        matches!(self.0.as_ref(), Some(Stream::Tls(_)))
    }

    /// Upgrades the stream to TLS. `buffered` holds bytes already read from the plain stream,
    /// which the client may have sent as the start of its TLS handshake.
    pub async fn switch_to_tls(
        &mut self,
        tls_acceptor: Arc<TlsAcceptor>,
        buffered: Bytes,
    ) -> io::Result<()> {
        match self.0.take() {
            Some(Stream::Plain(inner)) => {
                let stream = Prefixed {
                    prefix: buffered,
                    inner,
                };
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
                ));
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

    fn poll_write_vectored(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> std::task::Poll<std::io::Result<usize>> {
        match &mut self.0.as_mut().unwrap() {
            Stream::Plain(stream) => std::pin::Pin::new(stream).poll_write_vectored(cx, bufs),
            Stream::Tls(stream) => std::pin::Pin::new(stream).poll_write_vectored(cx, bufs),
        }
    }

    fn is_write_vectored(&self) -> bool {
        match self.0.as_ref().unwrap() {
            Stream::Plain(stream) => stream.is_write_vectored(),
            Stream::Tls(stream) => stream.is_write_vectored(),
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
