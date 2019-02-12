//! A wrapper around TCP channels that Noria uses to communicate between clients and servers, and
//! inside the data-flow graph. At this point, this is mostly a thin wrapper around
//! [`async-bincode`](https://docs.rs/async-bincode/), and it might go away in the long run.

use std::borrow::Borrow;
use std::collections::HashMap;
use std::hash::Hash;
use std::io::{self, BufWriter, Read, Write};
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::ops::{Deref, DerefMut};
use std::sync::mpsc::{self, SendError};
use std::sync::RwLock;

use async_bincode::{AsyncBincodeWriter, AsyncDestination};
use byteorder::{ByteOrder, NetworkEndian};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use tokio::prelude::*;

pub mod rpc;
pub mod tcp;

pub use self::tcp::{channel, DualTcpStream, TcpReceiver, TcpSender};

pub const CONNECTION_FROM_BASE: u8 = 1;
pub const CONNECTION_FROM_DOMAIN: u8 = 2;

pub struct Remote;
pub struct MaybeLocal;

pub struct DomainConnectionBuilder<D, T> {
    sport: Option<u16>,
    addr: SocketAddr,
    chan: Option<futures::sync::mpsc::UnboundedSender<T>>,
    is_for_base: bool,
    _marker: D,
}

impl<T> DomainConnectionBuilder<Remote, T> {
    pub fn for_base(addr: SocketAddr) -> Self {
        DomainConnectionBuilder {
            sport: None,
            chan: None,
            addr,
            is_for_base: true,
            _marker: Remote,
        }
    }
}

impl<D, T> DomainConnectionBuilder<D, T> {
    pub fn maybe_on_port(mut self, sport: Option<u16>) -> Self {
        self.sport = sport;
        self
    }

    pub fn on_port(mut self, sport: u16) -> Self {
        self.sport = Some(sport);
        self
    }
}

impl<T> DomainConnectionBuilder<Remote, T>
where
    T: serde::Serialize,
{
    pub fn build_async(
        self,
    ) -> io::Result<AsyncBincodeWriter<BufWriter<tokio::net::TcpStream>, T, AsyncDestination>> {
        // TODO: async
        // we must currently write and call flush, because the remote end (currently) does a
        // synchronous read upon accepting a connection.
        let s = self.build_sync()?.into_inner().into_inner()?;

        tokio::net::TcpStream::from_std(s, &tokio::reactor::Handle::default())
            .map(BufWriter::new)
            .map(AsyncBincodeWriter::from)
            .map(AsyncBincodeWriter::for_async)
    }

    pub fn build_sync(self) -> io::Result<TcpSender<T>> {
        let mut s = TcpSender::connect_from(self.sport, &self.addr)?;
        {
            let s = s.get_mut();
            s.write_all(&[if self.is_for_base {
                CONNECTION_FROM_BASE
            } else {
                CONNECTION_FROM_DOMAIN
            }])?;
            s.flush()?;
        }

        Ok(s)
    }
}

pub trait Sender {
    type Item;

    fn send(&mut self, t: Self::Item) -> Result<(), tcp::SendError>;
}

impl<T> Sender for futures::sync::mpsc::UnboundedSender<T> {
    type Item = T;

    fn send(&mut self, t: Self::Item) -> Result<(), tcp::SendError> {
        self.unbounded_send(t).map_err(|_| {
            tcp::SendError::IoError(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "local peer went away",
            ))
        })
    }
}

impl<T> DomainConnectionBuilder<MaybeLocal, T>
where
    T: serde::Serialize + 'static + Send,
{
    pub fn build_async(
        self,
    ) -> io::Result<Box<dyn Sink<SinkItem = T, SinkError = bincode::Error> + Send>> {
        if let Some(chan) = self.chan {
            Ok(
                Box::new(chan.sink_map_err(|_| serde::de::Error::custom("failed to do local send")))
                    as Box<_>,
            )
        } else {
            DomainConnectionBuilder {
                sport: self.sport,
                chan: None,
                addr: self.addr,
                is_for_base: false,
                _marker: Remote,
            }
            .build_async()
            .map(|c| Box::new(c) as Box<_>)
        }
    }

    pub fn build_sync(self) -> io::Result<Box<dyn Sender<Item = T> + Send>> {
        if let Some(chan) = self.chan {
            Ok(Box::new(chan))
        } else {
            DomainConnectionBuilder {
                sport: self.sport,
                chan: None,
                addr: self.addr,
                is_for_base: false,
                _marker: Remote,
            }
            .build_sync()
            .map(|c| Box::new(c) as Box<_>)
        }
    }
}

#[derive(Debug)]
pub enum ChannelSender<T> {
    Local(mpsc::Sender<T>),
    LocalSync(mpsc::SyncSender<T>),
}

impl<T> Clone for ChannelSender<T> {
    fn clone(&self) -> Self {
        // derive(Clone) uses incorrect bound, so we implement it ourselves. See issue #26925.
        match *self {
            ChannelSender::Local(ref s) => ChannelSender::Local(s.clone()),
            ChannelSender::LocalSync(ref s) => ChannelSender::LocalSync(s.clone()),
        }
    }
}

impl<T> Serialize for ChannelSender<T> {
    fn serialize<S: Serializer>(&self, _serializer: S) -> Result<S::Ok, S::Error> {
        unreachable!()
    }
}

impl<'de, T> Deserialize<'de> for ChannelSender<T> {
    fn deserialize<D: Deserializer<'de>>(_deserializer: D) -> Result<Self, D::Error> {
        unreachable!()
    }
}

impl<T> ChannelSender<T> {
    pub fn send(&self, t: T) -> Result<(), SendError<T>> {
        match *self {
            ChannelSender::Local(ref s) => s.send(t),
            ChannelSender::LocalSync(ref s) => s.send(t),
        }
    }

    pub fn from_local(local: mpsc::Sender<T>) -> Self {
        ChannelSender::Local(local)
    }
}

mod panic_serialize {
    use serde::{Deserializer, Serializer};
    pub fn serialize<S, T>(_t: &T, _serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        unreachable!()
    }
    pub fn deserialize<'de, D, T>(_deserializer: D) -> Result<T, D::Error>
    where
        D: Deserializer<'de>,
    {
        unreachable!()
    }
}

/// A wrapper around TcpSender that appears to be Serializable, but panics if it is ever serialized.
#[derive(Serialize, Deserialize)]
pub struct STcpSender<T>(#[serde(with = "panic_serialize")] pub TcpSender<T>);

impl<T> Deref for STcpSender<T> {
    type Target = TcpSender<T>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl<T> DerefMut for STcpSender<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

pub type TraceSender<T> = ChannelSender<T>;
pub type TransactionReplySender<T> = ChannelSender<T>;
pub type StreamSender<T> = ChannelSender<T>;

struct ChannelCoordinatorInner<K: Eq + Hash + Clone, T> {
    /// Map from key to remote address.
    addrs: HashMap<K, SocketAddr>,
    /// Map from key to channel sender for local connections.
    locals: HashMap<K, futures::sync::mpsc::UnboundedSender<T>>,
}

pub struct ChannelCoordinator<K: Eq + Hash + Clone, T> {
    inner: RwLock<ChannelCoordinatorInner<K, T>>,
}

impl<K: Eq + Hash + Clone, T> Default for ChannelCoordinator<K, T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K: Eq + Hash + Clone, T> ChannelCoordinator<K, T> {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(ChannelCoordinatorInner {
                addrs: Default::default(),
                locals: Default::default(),
            }),
        }
    }

    pub fn insert_remote(&self, key: K, addr: SocketAddr) {
        let mut inner = self.inner.write().unwrap();
        inner.addrs.insert(key, addr);
    }

    pub fn insert_local(&self, key: K, chan: futures::sync::mpsc::UnboundedSender<T>) {
        let mut inner = self.inner.write().unwrap();
        inner.locals.insert(key, chan);
    }

    pub fn has<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.inner.read().unwrap().addrs.contains_key(key)
    }

    pub fn get_addr<Q>(&self, key: &Q) -> Option<SocketAddr>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.inner.read().unwrap().addrs.get(key).cloned()
    }

    pub fn is_local<Q>(&self, key: &Q) -> Option<bool>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.inner.read().unwrap().locals.get(key).map(|_| true)
    }

    pub fn builder_for<Q>(&self, key: &Q) -> Option<DomainConnectionBuilder<MaybeLocal, T>>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let inner = self.inner.read().unwrap();
        Some(DomainConnectionBuilder {
            sport: None,
            addr: *inner.addrs.get(key)?,
            chan: inner.locals.get(key).cloned(),
            is_for_base: false,
            _marker: MaybeLocal,
        })
    }
}

/// A wrapper around a writer that handles `Error::WouldBlock` when attempting to write.
///
/// Instead of return that error, it places the bytes into a buffer so that subsequent calls to
/// `write()` can retry writing them.
pub struct NonBlockingWriter<T> {
    writer: T,
    buffer: Vec<u8>,
    cursor: usize,
}

impl<T: Write> NonBlockingWriter<T> {
    pub fn new(writer: T) -> Self {
        Self {
            writer,
            buffer: Vec::new(),
            cursor: 0,
        }
    }

    pub fn needs_flush_to_inner(&self) -> bool {
        self.buffer.len() != self.cursor
    }

    pub fn flush_to_inner(&mut self) -> io::Result<()> {
        if !self.buffer.is_empty() {
            while self.cursor < self.buffer.len() {
                match self.writer.write(&self.buffer[self.cursor..])? {
                    0 => return Err(io::Error::from(io::ErrorKind::BrokenPipe)),
                    n => self.cursor += n,
                }
            }
            self.buffer.clear();
            self.cursor = 0;
        }
        Ok(())
    }

    pub fn get_ref(&self) -> &T {
        &self.writer
    }

    pub fn get_mut(&mut self) -> &mut T {
        &mut self.writer
    }
}

impl<T: Write> Write for NonBlockingWriter<T> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.buffer.extend_from_slice(buf);
        match self.flush_to_inner() {
            Ok(_) => Ok(buf.len()),
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => Ok(buf.len()),
            Err(e) => {
                let old_len = self.buffer.len() - buf.len();
                self.buffer.truncate(old_len);
                Err(e)
            }
        }
    }
    fn flush(&mut self) -> io::Result<()> {
        self.flush_to_inner()?;
        self.writer.flush()
    }
}
impl<T: Read> Read for NonBlockingWriter<T> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.writer.read(buf)
    }
}

#[derive(Debug)]
pub enum ReceiveError {
    WouldBlock,
    IoError(io::Error),
    DeserializationError(bincode::Error),
}

impl From<io::Error> for ReceiveError {
    fn from(error: io::Error) -> Self {
        if error.kind() == io::ErrorKind::WouldBlock {
            ReceiveError::WouldBlock
        } else {
            ReceiveError::IoError(error)
        }
    }
}
impl From<bincode::Error> for ReceiveError {
    fn from(error: bincode::Error) -> Self {
        ReceiveError::DeserializationError(error)
    }
}

#[derive(Default)]
pub struct DeserializeReceiver<T> {
    buffer: Vec<u8>,
    size: usize,
    phantom: PhantomData<T>,
}

impl<T> DeserializeReceiver<T>
where
    for<'a> T: Deserialize<'a>,
{
    pub fn new() -> Self {
        Self {
            buffer: Vec::new(),
            size: 0,
            phantom: PhantomData,
        }
    }

    fn fill_from<R: Read>(
        &mut self,
        stream: &mut R,
        target_size: usize,
    ) -> Result<(), ReceiveError> {
        if self.buffer.len() < target_size {
            self.buffer.resize(target_size, 0u8);
        }

        while self.size < target_size {
            let n = stream.read(&mut self.buffer[self.size..target_size])?;
            if n == 0 {
                return Err(io::Error::from(io::ErrorKind::BrokenPipe).into());
            }
            self.size += n;
        }
        Ok(())
    }

    pub fn try_recv<R: Read>(&mut self, reader: &mut R) -> Result<T, ReceiveError> {
        if self.size < 4 {
            self.fill_from(reader, 5)?;
        }

        let message_size: u32 = NetworkEndian::read_u32(&self.buffer[0..4]);
        let target_buffer_size = message_size as usize + 4;
        self.fill_from(reader, target_buffer_size)?;

        let message = bincode::deserialize(&self.buffer[4..target_buffer_size])?;
        self.size = 0;
        Ok(message)
    }
}
