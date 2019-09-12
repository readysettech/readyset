use std;
use std::convert::TryFrom;
use std::io::{self, BufReader, Write};
use std::marker::PhantomData;
use std::net::{Ipv4Addr, SocketAddr};

use crate::Tagged;
use async_bincode::{AsyncBincodeStream, AsyncDestination};
use bincode;
use bufstream::BufStream;
use byteorder::{NetworkEndian, WriteBytesExt};
use futures_util::ready;
use mio::{self, Evented, PollOpt, Ready, Token};
use net2;
use pin_project::{pin_project, project};
use serde::{Deserialize, Serialize};
use std::{
    pin::Pin,
    task::{Context, Poll},
};
use tokio::prelude::*;

use super::{DeserializeReceiver, NonBlockingWriter, ReceiveError};

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
    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        #[project]
        match self.project() {
            DualTcpStream::Passthrough(abs) => abs.poll_ready(cx),
            DualTcpStream::Upgrade(abs, _) => abs.poll_ready(cx),
        }
    }

    #[project]
    fn start_send(mut self: Pin<&mut Self>, item: Tagged<()>) -> Result<(), Self::Error> {
        #[project]
        match self.project() {
            DualTcpStream::Passthrough(abs) => abs.start_send(item),
            DualTcpStream::Upgrade(abs, _) => abs.start_send(item),
        }
    }

    #[project]
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        #[project]
        match self.project() {
            DualTcpStream::Passthrough(abs) => abs.poll_flush(cx),
            DualTcpStream::Upgrade(abs, _) => abs.poll_flush(cx),
        }
    }

    #[project]
    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
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
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
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

pub struct TcpReceiver<T> {
    pub(crate) stream: BufReader<NonBlockingWriter<mio::net::TcpStream>>,
    poisoned: bool,
    deserialize_receiver: DeserializeReceiver<T>,

    phantom: PhantomData<T>,
}

impl<T> TcpReceiver<T>
where
    for<'de> T: Deserialize<'de>,
{
    pub fn new(stream: mio::net::TcpStream) -> Self {
        Self::new_inner(None, stream)
    }

    pub fn with_capacity(cap: usize, stream: mio::net::TcpStream) -> Self {
        Self::new_inner(Some(cap), stream)
    }

    fn new_inner(cap: Option<usize>, stream: mio::net::TcpStream) -> Self {
        stream.set_nodelay(true).unwrap(); // for acks
        let stream = NonBlockingWriter::new(stream);
        let stream = if let Some(cap) = cap {
            BufReader::with_capacity(cap, stream)
        } else {
            BufReader::new(stream)
        };

        Self {
            stream,
            poisoned: false,
            deserialize_receiver: DeserializeReceiver::new(),
            phantom: PhantomData,
        }
    }

    pub fn get_ref(&self) -> &mio::net::TcpStream {
        &*self.stream.get_ref().get_ref()
    }

    pub fn listen(addr: &SocketAddr) -> Result<Self, io::Error> {
        let listener = mio::net::TcpListener::bind(addr)?;
        Ok(Self::new(listener.accept()?.0))
    }

    pub fn local_addr(&self) -> Result<SocketAddr, io::Error> {
        self.stream.get_ref().get_ref().local_addr()
    }

    pub fn is_empty(&self) -> bool {
        self.stream.buffer().is_empty()
    }

    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        if self.poisoned {
            return Err(TryRecvError::Disconnected);
        }

        match self.deserialize_receiver.try_recv(&mut self.stream) {
            Ok(msg) => Ok(msg),
            Err(ReceiveError::WouldBlock) => Err(TryRecvError::Empty),
            Err(ReceiveError::IoError(_)) => {
                self.poisoned = true;
                Err(TryRecvError::Disconnected)
            }
            Err(ReceiveError::DeserializationError(e)) => {
                self.poisoned = true;
                Err(TryRecvError::DeserializationError(e))
            }
        }
    }

    pub fn recv(&mut self) -> Result<T, RecvError> {
        loop {
            return match self.try_recv() {
                Err(TryRecvError::Empty) => continue,
                Err(TryRecvError::Disconnected) => Err(RecvError::Disconnected),
                Err(TryRecvError::DeserializationError(e)) => {
                    Err(RecvError::DeserializationError(e))
                }
                Ok(t) => Ok(t),
            };
        }
    }
}

impl<T> Evented for TcpReceiver<T> {
    fn register(
        &self,
        poll: &mio::Poll,
        token: Token,
        interest: Ready,
        opts: PollOpt,
    ) -> io::Result<()> {
        self.stream
            .get_ref()
            .get_ref()
            .register(poll, token, interest, opts)
    }

    fn reregister(
        &self,
        poll: &mio::Poll,
        token: Token,
        interest: Ready,
        opts: PollOpt,
    ) -> io::Result<()> {
        self.stream
            .get_ref()
            .get_ref()
            .reregister(poll, token, interest, opts)
    }

    fn deregister(&self, poll: &mio::Poll) -> io::Result<()> {
        self.stream.get_ref().get_ref().deregister(poll)
    }
}

fn connect(listen_addr: SocketAddr) -> (std::net::TcpStream, mio::net::TcpStream) {
    let listener = std::net::TcpListener::bind(&listen_addr).unwrap();
    let rx = mio::net::TcpStream::connect(&listener.local_addr().unwrap()).unwrap();
    let tx = listener.accept().unwrap().0;

    (tx, rx)
}

pub fn channel<T: Serialize>(listen_addr: SocketAddr) -> (TcpSender<T>, TcpReceiver<T>)
where
    for<'de> T: Deserialize<'de>,
{
    let (tx, rx) = connect(listen_addr);
    let tx = TcpSender::new(tx).unwrap();
    let rx = TcpReceiver::new(rx);
    (tx, rx)
}

#[cfg(test)]
mod tests {
    use super::*;
    use mio::Events;
    use std::thread;

    #[test]
    fn it_works() {
        let (mut sender, mut receiver) = channel::<u32>("127.0.0.1:0".parse().unwrap());

        sender.send(12).unwrap();
        assert_eq!(receiver.recv().unwrap(), 12);

        sender.send(65).unwrap();
        sender.send(13).unwrap();
        assert_eq!(receiver.recv().unwrap(), 65);
        assert_eq!(receiver.recv().unwrap(), 13);
    }

    #[test]
    fn multithread() {
        let (mut sender, mut receiver) = channel::<u32>("127.0.0.1:0".parse().unwrap());

        let t1 = thread::spawn(move || {
            sender.send(12).unwrap();
            sender.send(65).unwrap();
            sender.send(13).unwrap();
        });

        let t2 = thread::spawn(move || {
            assert_eq!(receiver.recv().unwrap(), 12);
            assert_eq!(receiver.recv().unwrap(), 65);
            assert_eq!(receiver.recv().unwrap(), 13);
        });

        t1.join().unwrap();
        t2.join().unwrap();
    }

    #[test]
    fn poll() {
        let (mut sender, mut receiver) = channel::<u32>("127.0.0.1:0".parse().unwrap());

        let t1 = thread::spawn(move || {
            sender.send(12).unwrap();
            sender.send(65).unwrap();
            sender.send(13).unwrap();
        });

        let t2 = thread::spawn(move || {
            let poll = mio::Poll::new().unwrap();
            let mut events = Events::with_capacity(128);
            poll.register(&receiver, Token(0), Ready::readable(), PollOpt::level())
                .unwrap();
            poll.poll(&mut events, None).unwrap();
            let mut events = events.into_iter();
            assert_eq!(events.next().unwrap().token(), Token(0));
            assert_eq!(receiver.recv().unwrap(), 12);
            assert_eq!(receiver.recv().unwrap(), 65);
            assert_eq!(receiver.recv().unwrap(), 13);
        });

        t1.join().unwrap();
        t2.join().unwrap();
    }

    #[test]
    fn ping_pong() {
        let (mut sender, mut receiver) = channel::<u32>("127.0.0.1:0".parse().unwrap());
        let (mut sender2, mut receiver2) = channel::<u32>("127.0.0.1:0".parse().unwrap());

        let t1 = thread::spawn(move || {
            let poll = mio::Poll::new().unwrap();
            let mut events = Events::with_capacity(128);
            poll.register(&receiver, Token(0), Ready::readable(), PollOpt::level())
                .unwrap();

            poll.poll(&mut events, None).unwrap();
            assert_eq!(events.iter().next().unwrap().token(), Token(0));
            assert_eq!(receiver.recv().unwrap(), 15);

            sender2.send(12).unwrap();

            poll.poll(&mut events, None).unwrap();
            assert_eq!(events.iter().next().unwrap().token(), Token(0));
            assert_eq!(receiver.recv().unwrap(), 54);

            sender2.send(65).unwrap();
        });

        let t2 = thread::spawn(move || {
            let poll = mio::Poll::new().unwrap();
            let mut events = Events::with_capacity(128);
            poll.register(&receiver2, Token(0), Ready::readable(), PollOpt::level())
                .unwrap();

            sender.send(15).unwrap();

            poll.poll(&mut events, None).unwrap();
            assert_eq!(events.iter().next().unwrap().token(), Token(0));
            assert_eq!(receiver2.recv().unwrap(), 12);

            sender.send(54).unwrap();

            poll.poll(&mut events, None).unwrap();
            assert_eq!(events.iter().next().unwrap().token(), Token(0));
            assert_eq!(receiver2.recv().unwrap(), 65);
        });

        t1.join().unwrap();
        t2.join().unwrap();
    }
}
