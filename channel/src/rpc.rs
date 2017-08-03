use std;
use std::io::{self, Write};
use std::marker::PhantomData;
use std::net::SocketAddr;

use bincode::{self, Infinite};
use bufstream::BufStream;
use byteorder::{NetworkEndian, WriteBytesExt};
use mio::{self, Evented, Poll, PollOpt, Ready, Token};
use serde::{Serialize, Deserialize};

use tcp::{SendError, TryRecvError};
use super::{ReceiveError, DeserializeReceiver, NonBlockingWriter};

pub struct RpcClient<Q, R> {
    stream: BufStream<std::net::TcpStream>,
    poisoned: bool,
    phantom: PhantomData<Q>,
    phantom2: PhantomData<R>,
}


impl<Q: Serialize, R> RpcClient<Q, R>
where
    for<'de> R: Deserialize<'de>,
{
    pub fn new(stream: std::net::TcpStream) -> Result<Self, io::Error> {
        Ok(Self {
            stream: BufStream::new(stream),
            poisoned: false,
            phantom: PhantomData,
            phantom2: PhantomData,
        })
    }

    pub fn connect(addr: &SocketAddr) -> Result<Self, io::Error> {
        Self::new(std::net::TcpStream::connect(addr)?)
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.stream.get_ref().local_addr()
    }

    fn send_internal(&mut self, query: &Q) -> Result<R, SendError> {
        let size: u32 = bincode::serialized_size(query) as u32;
        self.stream.write_u32::<NetworkEndian>(size)?;
        bincode::serialize_into(&mut self.stream, query, Infinite)?;
        self.stream.flush()?;
        Ok(bincode::deserialize_from(&mut self.stream, Infinite)?)
    }

    pub fn send(&mut self, query: &Q) -> Result<R, SendError> {
        if self.poisoned {
            return Err(SendError::Poisoned);
        }

        let reply = self.send_internal(query);
        if reply.is_err() {
            self.poisoned = true;
        }
        reply
    }
}

#[derive(Debug)]
pub enum RpcSendError {
    SerializationError,
    Disconnected,
    StillNeedsFlush,
}

pub struct RpcServiceEndpoint<Q, R> {
    pub(crate) stream: NonBlockingWriter<mio::net::TcpStream>,
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
        Self {
            stream: NonBlockingWriter::new(stream),
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
        self.stream.get_ref().local_addr()
    }

    pub fn try_recv(&mut self) -> Result<Q, TryRecvError> {
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
            Err(ReceiveError::DeserializationError(_)) => {
                self.poisoned = true;
                Err(TryRecvError::DeserializationError)
            }

        }
    }

    pub fn send(&mut self, reply: &R) -> Result<(), RpcSendError> {
        if self.poisoned {
            return Err(RpcSendError::Disconnected);
        }

        if bincode::serialize_into(&mut self.stream, reply, Infinite).is_err() {
            self.poisoned = true;
            return Err(RpcSendError::SerializationError);
        }

        if self.stream.needs_flush() {
            return Err(RpcSendError::StillNeedsFlush);
        }

        Ok(())
    }

    pub fn flush(&mut self) -> Result<(), RpcSendError> {
        if self.poisoned || self.stream.flush().is_err() {
            self.poisoned = true;
            return Err(RpcSendError::Disconnected);
        }

        Ok(())
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
        self.stream.get_ref().register(poll, token, interest, opts)
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
            .reregister(poll, token, interest, opts)
    }

    fn deregister(&self, poll: &Poll) -> io::Result<()> {
        self.stream.get_ref().deregister(poll)
    }
}
