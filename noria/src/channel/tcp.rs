use std::convert::TryFrom;
use std::io::{self, Write};
use std::marker::PhantomData;
use std::net::{Ipv4Addr, SocketAddr};

use crate::Tagged;
use async_bincode::{AsyncBincodeStream, AsyncDestination};
use bufstream::BufStream;
use byteorder::{NetworkEndian, WriteBytesExt};
use futures_util::ready;
use futures_util::{sink::Sink, stream::Stream};
use pin_project::{pin_project, project};
use serde::{Deserialize, Serialize};
use std::{
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite};

#[derive(Debug, Fail)]
pub enum SendError {
    #[fail(display = "{}", _0)]
    BincodeError(#[cause] bincode::Error),
    #[fail(display = "{}", _0)]
    IoError(#[cause] io::Error),
    #[fail(display = "channel has previously encountered an error")]
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
        stream.set_nodelay(true).unwrap();
        Ok(Self {
            stream: BufStream::new(stream),
            poisoned: false,
            phantom: PhantomData,
        })
    }

    pub(crate) fn connect_from(sport: Option<u16>, addr: &SocketAddr) -> Result<Self, io::Error> {
        let s = net2::TcpBuilder::new_v4()?
            .reuse_address(true)?
            .bind((Ipv4Addr::UNSPECIFIED, sport.unwrap_or(0)))?
            .connect(addr)?;
        s.set_nodelay(true)?;
        Self::new(s)
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

        let size = u32::try_from(bincode::serialized_size(t).unwrap()).unwrap();
        poisoning_try!(self, self.stream.write_u32::<NetworkEndian>(size));
        poisoning_try!(self, bincode::serialize_into(&mut self.stream, t));
        poisoning_try!(self, self.stream.flush());
        Ok(())
    }

    pub fn reader<'a>(&'a mut self) -> impl io::Read + 'a {
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

#[pin_project]
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

    #[project]
    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        #[project]
        match self.project() {
            DualTcpStream::Passthrough(abs) => abs.poll_ready(cx),
            DualTcpStream::Upgrade(abs, _) => abs.poll_ready(cx),
        }
    }

    #[project]
    fn start_send(self: Pin<&mut Self>, item: Tagged<()>) -> Result<(), Self::Error> {
        #[project]
        match self.project() {
            DualTcpStream::Passthrough(abs) => abs.start_send(item),
            DualTcpStream::Upgrade(abs, _) => abs.start_send(item),
        }
    }

    #[project]
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        #[project]
        match self.project() {
            DualTcpStream::Passthrough(abs) => abs.poll_flush(cx),
            DualTcpStream::Upgrade(abs, _) => abs.poll_flush(cx),
        }
    }

    #[project]
    fn poll_close(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        #[project]
        match self.project() {
            DualTcpStream::Passthrough(abs) => abs.poll_close(cx),
            DualTcpStream::Upgrade(abs, _) => abs.poll_close(cx),
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

    #[project]
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // https://github.com/rust-lang/rust-clippy/issues/3071
        #[project]
        #[allow(clippy::redundant_closure)]
        match self.project() {
            DualTcpStream::Passthrough(abr) => abr.poll_next(cx),
            DualTcpStream::Upgrade(abr, upgrade) => {
                Poll::Ready(ready!(abr.poll_next(cx)).transpose()?.map(upgrade).map(Ok))
            }
        }
    }
}
