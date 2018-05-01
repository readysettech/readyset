use std;
use std::io::{self, Write};
use std::marker::PhantomData;
use std::net::{Ipv4Addr, SocketAddr};

use bincode;
use bufstream::BufStream;
use byteorder::{NetworkEndian, WriteBytesExt};
use mio::{self, Evented, Poll, PollOpt, Ready, Token};
use net2;
use serde::{Deserialize, Serialize};

use super::{DeserializeReceiver, NonBlockingWriter, ReceiveError};
use tcp::{SendError, TryRecvError};

pub struct RpcClient<Q, R> {
    stream: BufStream<std::net::TcpStream>,
    poisoned: bool,
    phantom: PhantomData<Q>,
    phantom2: PhantomData<R>,
    is_local: bool,
}

pub struct Eventually<'a, Q: 'a, R: 'a>(&'a mut RpcClient<Q, R>);

impl<'a, Q: 'a, R: 'a> Eventually<'a, Q, R>
where
    for<'de> R: Deserialize<'de>,
{
    pub fn wait(self) -> Result<R, SendError> {
        match bincode::deserialize_from(&mut self.0.stream) {
            Ok(r) => Ok(r),
            Err(e) => {
                self.0.poisoned = true;
                Err(e.into())
            }
        }
    }
}

impl<Q: Serialize, R> RpcClient<Q, R>
where
    for<'de> R: Deserialize<'de>,
{
    pub fn new(stream: std::net::TcpStream, is_local: bool) -> Result<Self, io::Error> {
        stream.set_nodelay(true)?;
        Ok(Self {
            stream: BufStream::new(stream),
            poisoned: false,
            phantom: PhantomData,
            phantom2: PhantomData,
            is_local,
        })
    }

    pub fn is_local(&self) -> bool {
        self.is_local
    }

    pub fn connect_from(
        sport: Option<u16>,
        addr: &SocketAddr,
        is_local: bool,
    ) -> Result<Self, io::Error> {
        let s = net2::TcpBuilder::new_v4()?
            .reuse_address(true)?
            .bind((Ipv4Addr::unspecified(), sport.unwrap_or(0)))?
            .connect(addr)?;
        Self::new(s, is_local)
    }

    pub fn connect(addr: &SocketAddr, is_local: bool) -> Result<Self, io::Error> {
        Self::connect_from(None, addr, is_local)
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.stream.get_ref().local_addr()
    }

    fn send_internal(&mut self, query: &Q) -> Result<Eventually<Q, R>, SendError> {
        if self.poisoned {
            return Err(SendError::Poisoned);
        }

        let size: u32 = bincode::serialized_size(query).unwrap() as u32;
        self.stream.write_u32::<NetworkEndian>(size)?;
        bincode::serialize_into(&mut self.stream, query)?;
        self.stream.flush()?;
        Ok(Eventually(self))
    }

    pub fn send_async(&mut self, query: &Q) -> Result<Eventually<Q, R>, SendError> {
        self.send_internal(query)
    }

    pub fn send(&mut self, query: &Q) -> Result<R, SendError> {
        self.send_internal(query)?.wait()
    }
}

#[derive(Debug)]
pub enum RpcSendError {
    SerializationError(bincode::Error),
    Disconnected,
    StillNeedsFlush,
}

pub struct RpcServiceEndpoint<Q, R> {
    pub(crate) stream: NonBlockingWriter<BufStream<mio::net::TcpStream>>,
    deserialize_receiver: DeserializeReceiver<Q>,
    poisoned: bool,
    phantom: PhantomData<Q>,
    phantom2: PhantomData<R>,
}

impl<Q, R: Serialize> RpcServiceEndpoint<Q, R>
where
    for<'de> Q: Deserialize<'de>,
{
    pub fn new(stream: mio::net::TcpStream) -> Self {
        stream.set_nodelay(true).unwrap();
        Self {
            stream: NonBlockingWriter::new(BufStream::new(stream)),
            deserialize_receiver: DeserializeReceiver::new(),
            poisoned: false,
            phantom: PhantomData,
            phantom2: PhantomData,
        }
    }

    pub fn listen(addr: &SocketAddr) -> Result<Self, io::Error> {
        let listener = mio::net::TcpListener::bind(addr)?;
        Ok(Self::new(listener.accept()?.0))
    }

    pub fn local_addr(&self) -> Result<SocketAddr, io::Error> {
        self.stream.get_ref().get_ref().local_addr()
    }

    pub fn try_recv(&mut self) -> Result<Q, TryRecvError> {
        if self.poisoned {
            return Err(TryRecvError::Disconnected);
        }

        match self.deserialize_receiver.try_recv(&mut self.stream) {
            Ok(msg) => Ok(msg),
            Err(ReceiveError::WouldBlock) => Err(TryRecvError::Empty),
            Err(ReceiveError::IoError(e)) => match e.kind() {
                io::ErrorKind::BrokenPipe
                | io::ErrorKind::NotConnected
                | io::ErrorKind::UnexpectedEof
                | io::ErrorKind::ConnectionAborted
                | io::ErrorKind::ConnectionReset => {
                    self.poisoned = true;
                    Err(TryRecvError::Disconnected)
                }
                _ => {
                    panic!("{:?}", e);
                }
            },
            Err(ReceiveError::DeserializationError(e)) => {
                self.poisoned = true;
                Err(TryRecvError::DeserializationError(e))
            }
        }
    }

    pub fn send(&mut self, reply: &R) -> Result<(), RpcSendError> {
        if self.poisoned {
            return Err(RpcSendError::Disconnected);
        }

        if let Err(e) = bincode::serialize_into(&mut self.stream, reply) {
            if let bincode::ErrorKind::Io(e) = *e {
                match e.kind() {
                    io::ErrorKind::BrokenPipe
                    | io::ErrorKind::NotConnected
                    | io::ErrorKind::UnexpectedEof
                    | io::ErrorKind::ConnectionAborted
                    | io::ErrorKind::ConnectionReset => {
                        self.poisoned = true;
                        return Err(RpcSendError::Disconnected);
                    }
                    _ => {
                        panic!("{:?}", e);
                    }
                }
            }
            self.poisoned = true;
            return Err(RpcSendError::SerializationError(e));
        }

        if self.stream.needs_flush_to_inner() {
            return Err(RpcSendError::StillNeedsFlush);
        }

        match self.stream.flush() {
            Ok(()) => Ok(()),
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                Err(RpcSendError::StillNeedsFlush)
            }
            Err(e) => match e.kind() {
                io::ErrorKind::BrokenPipe
                | io::ErrorKind::NotConnected
                | io::ErrorKind::UnexpectedEof
                | io::ErrorKind::ConnectionAborted
                | io::ErrorKind::ConnectionReset => {
                    self.poisoned = true;
                    Err(RpcSendError::Disconnected)
                }
                _ => {
                    panic!("{:?}", e);
                }
            },
        }
    }

    pub fn flush(&mut self) -> Result<(), RpcSendError> {
        if self.poisoned {
            return Err(RpcSendError::Disconnected);
        }

        match self.stream.flush() {
            Ok(()) => Ok(()),
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                Err(RpcSendError::StillNeedsFlush)
            }
            Err(e) => match e.kind() {
                io::ErrorKind::BrokenPipe
                | io::ErrorKind::NotConnected
                | io::ErrorKind::UnexpectedEof
                | io::ErrorKind::ConnectionAborted
                | io::ErrorKind::ConnectionReset => {
                    self.poisoned = true;
                    Err(RpcSendError::Disconnected)
                }
                _ => {
                    panic!("{:?}", e);
                }
            },
        }
    }

    pub fn get_ref(&self) -> &mio::net::TcpStream {
        self.stream.get_ref().get_ref()
    }
}

#[cfg(unix)]
mod unix_ext {
    use super::RpcServiceEndpoint;
    use std::os::unix::io::{AsRawFd, RawFd};
    impl<Q, R> AsRawFd for RpcServiceEndpoint<Q, R> {
        fn as_raw_fd(&self) -> RawFd {
            self.stream.get_ref().get_ref().as_raw_fd()
        }
    }
}

impl<Q, R> Evented for RpcServiceEndpoint<Q, R> {
    fn register(
        &self,
        poll: &Poll,
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
        poll: &Poll,
        token: Token,
        interest: Ready,
        opts: PollOpt,
    ) -> io::Result<()> {
        self.stream
            .get_ref()
            .get_ref()
            .reregister(poll, token, interest, opts)
    }

    fn deregister(&self, poll: &Poll) -> io::Result<()> {
        self.stream.get_ref().get_ref().deregister(poll)
    }
}
