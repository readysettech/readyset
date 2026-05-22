use std::pin::Pin;
use std::task::{Context, Poll};

use async_bincode::tokio::{AsyncBincodeStream, AsyncDestination};
use futures_util::ready;
use futures_util::sink::Sink;
use futures_util::stream::Stream;
use pin_project::pin_project;
use readyset_client::{PacketData, Tagged};
use tokio::io::BufStream;
use tokio::net::TcpStream;

use crate::Packet;

/// TCP stream for base-table writes coming from the replicator (or any
/// external base-table writer). Wraps an `AsyncBincodeStream` carrying
/// `Tagged<PacketData>` on receive and `Tagged<()>` on send, with an
/// upgrade closure that maps each received `Tagged<PacketData>` into the
/// internal `Packet` representation the replica's domain expects.
#[pin_project]
pub struct BaseWriteStream {
    #[pin]
    stream:
        AsyncBincodeStream<BufStream<TcpStream>, Tagged<PacketData>, Tagged<()>, AsyncDestination>,
    upgrade: Box<dyn FnMut(Tagged<PacketData>) -> Packet + Send + Sync>,
}

impl BaseWriteStream {
    pub fn upgrade<F: 'static + FnMut(Tagged<PacketData>) -> Packet + Send + Sync>(
        stream: BufStream<TcpStream>,
        f: F,
    ) -> Self {
        BaseWriteStream {
            stream: AsyncBincodeStream::from(stream).for_async(),
            upgrade: Box::new(f),
        }
    }

    pub fn get_ref(&self) -> &BufStream<TcpStream> {
        self.stream.get_ref()
    }
}

impl Sink<Tagged<()>> for BaseWriteStream {
    type Error = bincode::Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().stream.poll_ready(cx)
    }

    fn start_send(self: Pin<&mut Self>, item: Tagged<()>) -> Result<(), Self::Error> {
        self.project().stream.start_send(item)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().stream.poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().stream.poll_close(cx)
    }
}

impl Stream for BaseWriteStream {
    type Item = Result<Packet, bincode::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        Poll::Ready(
            ready!(this.stream.poll_next(cx))
                .transpose()?
                .map(this.upgrade)
                .map(Ok),
        )
    }
}
