use tokio_native_tls::TlsStream;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::io;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use bytes::BufMut;
use tokio::io::ReadBuf;
use std::io::IoSlice;

pub enum StreamType<S> {
    Plain(S),
    Tls(TlsStream<S>),
}

impl<S: AsyncRead + AsyncWrite + Unpin> StreamType<S> {
    pub fn new(s: S) -> Self {
        StreamType::Plain(s)
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncWrite for StreamType<S> {
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
        match self.get_mut() {
            StreamType::Plain(s) => Pin::new(s).poll_flush(cx),
            StreamType::Tls(s) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        match self.get_mut() {
            StreamType::Plain(s) => Pin::new(s).poll_shutdown(cx),
            StreamType::Tls(s) => Pin::new(s).poll_shutdown(cx),
        }
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncRead for StreamType<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            StreamType::Plain(s) => Pin::new(s).poll_read(cx,buf),
            StreamType::Tls(s) => Pin::new(s).poll_read(cx,buf),
        }
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> StreamType<S> {
    fn flush(&mut self) -> io::Result<()> {
        unimplemented!()
        /*match self {
            StreamType::Plain(s) => Pin::new(s).read_buf(buf),
            StreamType::Tls(s) => Pin::new(s).read_buf(buf),
        }*/
    }

    fn read_buf<B: BufMut>(&mut self, buf: &mut B) -> io::Result<usize> {
        unimplemented!()
        /*match self {
            StreamType::Plain(s) => Pin::new(s).read_buf(buf),
            StreamType::Tls(s) => Pin::new(s).read_buf(buf),
        }*/
    }

    fn write_vectored(&mut self, bufs: &[IoSlice<'_>]) -> io::Result<usize> {
        unimplemented!();
    }
}
