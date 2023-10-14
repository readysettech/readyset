use std::convert::TryFrom;
use std::io::{self, Write};
use std::net::{Ipv4Addr, SocketAddr};
use std::pin::Pin;
use std::task::{Context, Poll};

use async_bincode::{AsyncBincodeStream, AsyncDestination};
use bincode::{ErrorKind, Options};
use byteorder::{NetworkEndian, WriteBytesExt};
use futures_util::ready;
use futures_util::sink::Sink;
use futures_util::stream::Stream;
use pin_project::pin_project;
use readyset_client::{PacketData, Tagged};
use readyset_errors::ReadySetError;
use socket2::{Domain, Protocol, SockAddr, Socket, Type};
use thiserror::Error;
use tokio::io::BufStream;
use tokio::net::TcpStream;

use crate::Packet;

#[derive(Debug, Error)]
pub enum SendError {
    #[error("{0}")]
    BincodeError(#[source] bincode::Error),
    #[error("{0}")]
    IoError(#[source] io::Error),
    #[error("channel has previously encountered an error")]
    Poisoned,
}

impl From<bincode::Error> for SendError {
    fn from(e: bincode::Error) -> Self {
        SendError::BincodeError(e)
    }
}

impl From<io::Error> for SendError {
    fn from(e: io::Error) -> Self {
        SendError::IoError(e)
    }
}

/// HACK(eta): this From impl just stringifies the error, so that `ReadySetError` can be serialized
/// and deserialized.
impl From<SendError> for ReadySetError {
    fn from(e: SendError) -> ReadySetError {
        ReadySetError::TcpSendError(e.to_string())
    }
}

macro_rules! poisoning_try {
    ($self_:ident, $e:expr) => {
        match $e {
            Ok(v) => v,
            Err(r) => {
                $self_.poisoned = true;
                return Err(r.into());
            }
        }
    };
}

pub struct TcpSender {
    stream: bufstream::BufStream<std::net::TcpStream>,
    poisoned: bool,
}

impl TcpSender {
    pub fn new(stream: std::net::TcpStream) -> Result<Self, io::Error> {
        stream.set_nodelay(true).map(|_| Self {
            stream: bufstream::BufStream::new(stream),
            poisoned: false,
        })
    }

    pub fn connect_from(sport: Option<u16>, addr: &SocketAddr) -> Result<Self, io::Error> {
        let bind_addr = std::net::SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, sport.unwrap_or(0));
        let s = Socket::new(Domain::IPV4, Type::STREAM, Some(Protocol::TCP))?;
        s.set_reuse_address(true)?;
        s.bind(&SockAddr::from(bind_addr))?;
        s.set_nodelay(true)?;
        s.connect(&SockAddr::from(*addr))?;
        Self::new(std::net::TcpStream::from(s))
    }

    pub fn connect(addr: &SocketAddr) -> Result<Self, io::Error> {
        Self::connect_from(None, addr)
    }

    pub fn get_mut(&mut self) -> &mut bufstream::BufStream<std::net::TcpStream> {
        &mut self.stream
    }

    pub fn into_inner(self) -> bufstream::BufStream<std::net::TcpStream> {
        self.stream
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.stream.get_ref().local_addr()
    }

    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.stream.get_ref().peer_addr()
    }

    /// Send a packet on this channel. Ownership isn't actually required, but is taken anyway to
    /// conform to the same api as mpsc::Sender.
    pub fn send(&mut self, packet: Packet) -> Result<(), SendError> {
        self.send_ref(&packet)
    }

    pub fn send_ref(&mut self, packet: &Packet) -> Result<(), SendError> {
        if self.poisoned {
            return Err(SendError::Poisoned);
        }

        // NOTE: this *has* to match exactly what the AsyncBincodeReader expects on the other
        // end, or else we'll silently get the wrong data (but no deserialization errors!)
        // https://app.clubhouse.io/readysettech/story/437 to fix that
        let c = bincode::options()
            .with_limit(u32::max_value() as u64)
            .allow_trailing_bytes();
        let size = c
            .serialized_size(packet)
            .and_then(|s| u32::try_from(s).map_err(|_| Box::new(ErrorKind::SizeLimit)))?;
        poisoning_try!(self, self.stream.write_u32::<NetworkEndian>(size));
        poisoning_try!(self, c.serialize_into(&mut self.stream, packet));
        poisoning_try!(self, self.stream.flush());
        Ok(())
    }

    pub fn reader(&mut self) -> impl io::Read + '_ {
        &mut self.stream
    }
}

#[pin_project(project = DualTcpStreamProj)]
pub enum DualTcpStream {
    Passthrough(
        #[pin] AsyncBincodeStream<BufStream<TcpStream>, Packet, Tagged<()>, AsyncDestination>,
    ),
    Upgrade(
        #[pin]
        AsyncBincodeStream<BufStream<TcpStream>, Tagged<PacketData>, Tagged<()>, AsyncDestination>,
        Box<dyn FnMut(Tagged<PacketData>) -> Packet + Send + Sync>,
    ),
}

impl From<BufStream<TcpStream>> for DualTcpStream {
    fn from(stream: BufStream<TcpStream>) -> Self {
        DualTcpStream::Passthrough(AsyncBincodeStream::from(stream).for_async())
    }
}

impl DualTcpStream {
    pub fn upgrade<F: 'static + FnMut(Tagged<PacketData>) -> Packet + Send + Sync>(
        stream: BufStream<TcpStream>,
        f: F,
    ) -> Self {
        let s: AsyncBincodeStream<
            BufStream<TcpStream>,
            Tagged<PacketData>,
            Tagged<()>,
            AsyncDestination,
        > = AsyncBincodeStream::from(stream).for_async();
        DualTcpStream::Upgrade(s, Box::new(f))
    }

    pub fn get_ref(&self) -> &BufStream<TcpStream> {
        match *self {
            DualTcpStream::Passthrough(ref abs) => abs.get_ref(),
            DualTcpStream::Upgrade(ref abs, _) => abs.get_ref(),
        }
    }
}

impl Sink<Tagged<()>> for DualTcpStream {
    type Error = bincode::Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        match self.project() {
            DualTcpStreamProj::Passthrough(abs) => abs.poll_ready(cx),
            DualTcpStreamProj::Upgrade(abs, _) => abs.poll_ready(cx),
        }
    }

    fn start_send(self: Pin<&mut Self>, item: Tagged<()>) -> Result<(), Self::Error> {
        match self.project() {
            DualTcpStreamProj::Passthrough(abs) => abs.start_send(item),
            DualTcpStreamProj::Upgrade(abs, _) => abs.start_send(item),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        match self.project() {
            DualTcpStreamProj::Passthrough(abs) => abs.poll_flush(cx),
            DualTcpStreamProj::Upgrade(abs, _) => abs.poll_flush(cx),
        }
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        match self.project() {
            DualTcpStreamProj::Passthrough(abs) => abs.poll_close(cx),
            DualTcpStreamProj::Upgrade(abs, _) => abs.poll_close(cx),
        }
    }
}

impl Stream for DualTcpStream {
    type Item = Result<Packet, bincode::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.project() {
            DualTcpStreamProj::Passthrough(abr) => abr.poll_next(cx),
            DualTcpStreamProj::Upgrade(abr, upgrade) => {
                Poll::Ready(ready!(abr.poll_next(cx)).transpose()?.map(upgrade).map(Ok))
            }
        }
    }
}
