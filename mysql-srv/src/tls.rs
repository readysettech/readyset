use tokio::net::TcpStream;
use tokio_native_tls::TlsStream;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use std::io::IoSlice;
use tokio::io;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use tokio::io::ReadBuf;

pub enum StreamType<S> {
    Plain(S),
    Tls(TlsStream<S>),
}

impl<S: AsyncRead + AsyncWrite + Unpin> StreamType<S> {
    pub fn new(s: S) -> Self {
        StreamType::Plain(s)
    }

}

impl<S: AsyncWrite + Unpin> AsyncWrite for StreamType<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        match self.get_mut() {
            StreamType::Plain(s) => Pin::new(s).poll_write(cx, buf),
            StreamType::Tls(s) => Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        unimplemented!()
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        unimplemented!()
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for StreamType<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        unimplemented!()
    }

}
