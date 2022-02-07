use std::convert::TryFrom;
use std::io::{self, Write};
use std::marker::PhantomData;
use std::net::{Ipv4Addr, SocketAddr};
use std::pin::Pin;
use std::task::{Context, Poll};

use async_bincode::{AsyncBincodeStream, AsyncDestination};
use bincode::{ErrorKind, Options};
use bufstream::BufStream;
use byteorder::{NetworkEndian, WriteBytesExt};
use futures_util::ready;
use futures_util::sink::Sink;
use futures_util::stream::Stream;
use noria_errors::ReadySetError;
use pin_project::pin_project;
use serde::{Deserialize, Serialize};
use socket2::{Domain, Protocol, SockAddr, Socket, Type};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::Tagged;

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

pub struct TcpSender<T> {
    stream: BufStream<std::net::TcpStream>,
    poisoned: bool,

    phantom: PhantomData<T>,
}

impl<T: Serialize> TcpSender<T> {
    pub fn new(stream: std::net::TcpStream) -> Result<Self, io::Error> {
        stream.set_nodelay(true).map(|_| Self {
            stream: BufStream::new(stream),
            poisoned: false,
            phantom: PhantomData,
        })
    }

    pub(crate) fn connect_from(sport: Option<u16>, addr: &SocketAddr) -> Result<Self, io::Error> {
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

    pub fn get_mut(&mut self) -> &mut BufStream<std::net::TcpStream> {
        &mut self.stream
    }

    pub(crate) fn into_inner(self) -> BufStream<std::net::TcpStream> {
        self.stream
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.stream.get_ref().local_addr()
    }

    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.stream.get_ref().peer_addr()
    }

    /// Send a message on this channel. Ownership isn't actually required, but is taken anyway to
    /// conform to the same api as mpsc::Sender.
    pub fn send(&mut self, t: T) -> Result<(), SendError> {
        self.send_ref(&t)
    }

    pub fn send_ref(&mut self, t: &T) -> Result<(), SendError> {
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
            .serialized_size(t)
            .and_then(|s| u32::try_from(s).map_err(|_| Box::new(ErrorKind::SizeLimit)))?;
        poisoning_try!(self, self.stream.write_u32::<NetworkEndian>(size));
        poisoning_try!(self, c.serialize_into(&mut self.stream, t));
        poisoning_try!(self, self.stream.flush());
        Ok(())
    }

    pub fn reader(&mut self) -> impl io::Read + '_ {
        &mut self.stream
    }
}

impl<T: Serialize> super::Sender for TcpSender<T> {
    type Item = T;
    fn send(&mut self, t: T) -> Result<(), SendError> {
        self.send_ref(&t)
    }
}

#[derive(Debug)]
pub enum TryRecvError {
    Empty,
    Disconnected,
    DeserializationError(bincode::Error),
}

#[derive(Debug)]
pub enum RecvError {
    Disconnected,
    DeserializationError(bincode::Error),
}

#[pin_project(project = DualTcpStreamProj)]
pub enum DualTcpStream<S, T, T2, D> {
    Passthrough(#[pin] AsyncBincodeStream<S, T, Tagged<()>, D>),
    Upgrade(
        #[pin] AsyncBincodeStream<S, T2, Tagged<()>, D>,
        Box<dyn FnMut(T2) -> T + Send + Sync>,
    ),
}

impl<S, T, T2> From<S> for DualTcpStream<S, T, T2, AsyncDestination> {
    fn from(stream: S) -> Self {
        DualTcpStream::Passthrough(AsyncBincodeStream::from(stream).for_async())
    }
}

impl<S, T, T2> DualTcpStream<S, T, T2, AsyncDestination> {
    pub fn upgrade<F: 'static + FnMut(T2) -> T + Send + Sync>(stream: S, f: F) -> Self {
        let s: AsyncBincodeStream<S, T2, Tagged<()>, AsyncDestination> =
            AsyncBincodeStream::from(stream).for_async();
        DualTcpStream::Upgrade(s, Box::new(f))
    }

    pub fn get_ref(&self) -> &S {
        match *self {
            DualTcpStream::Passthrough(ref abs) => abs.get_ref(),
            DualTcpStream::Upgrade(ref abs, _) => abs.get_ref(),
        }
    }
}

impl<S, T, T2, D> Sink<Tagged<()>> for DualTcpStream<S, T, T2, D>
where
    S: AsyncWrite,
    AsyncBincodeStream<S, T, Tagged<()>, D>: Sink<Tagged<()>, Error = bincode::Error>,
    AsyncBincodeStream<S, T2, Tagged<()>, D>: Sink<Tagged<()>, Error = bincode::Error>,
{
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

impl<S, T, T2, D> Stream for DualTcpStream<S, T, T2, D>
where
    for<'a> T: Deserialize<'a>,
    for<'a> T2: Deserialize<'a>,
    S: AsyncRead,
    AsyncBincodeStream<S, T, Tagged<()>, D>: Stream<Item = Result<T, bincode::Error>>,
    AsyncBincodeStream<S, T2, Tagged<()>, D>: Stream<Item = Result<T2, bincode::Error>>,
{
    type Item = Result<T, bincode::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.project() {
            DualTcpStreamProj::Passthrough(abr) => abr.poll_next(cx),
            DualTcpStreamProj::Upgrade(abr, upgrade) => {
                Poll::Ready(ready!(abr.poll_next(cx)).transpose()?.map(upgrade).map(Ok))
            }
        }
    }
}
