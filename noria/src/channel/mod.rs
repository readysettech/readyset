//! A wrapper around TCP channels that Noria uses to communicate between clients and servers, and
//! inside the data-flow graph. At this point, this is mostly a thin wrapper around
//! [`async-bincode`](https://docs.rs/async-bincode/), and it might go away in the long run.

use std::borrow::Borrow;
use std::collections::HashMap;
use std::hash::Hash;
use std::io::{self, Write};
use std::net::SocketAddr;
use std::ops::{Deref, DerefMut};
use std::sync::mpsc::{self, SendError};
use std::sync::RwLock;
use std::{
    pin::Pin,
    task::{Context, Poll},
};

use async_bincode::{AsyncBincodeWriter, AsyncDestination};
use futures_util::sink::{Sink, SinkExt};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use tokio::io::BufWriter;

pub mod tcp;

pub use self::tcp::{DualTcpStream, TcpSender};

pub const CONNECTION_FROM_BASE: u8 = 1;
pub const CONNECTION_FROM_DOMAIN: u8 = 2;

pub struct Remote;
pub struct MaybeLocal;

pub struct DomainConnectionBuilder<D, T> {
    sport: Option<u16>,
    addr: SocketAddr,
    chan: Option<tokio::sync::mpsc::UnboundedSender<T>>,
    is_for_base: bool,
    _marker: D,
}

struct ImplSinkForSender<T>(tokio::sync::mpsc::UnboundedSender<T>);

impl<T> Sink<T> for ImplSinkForSender<T> {
    type Error = tokio::sync::mpsc::error::SendError<T>;

    fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, item: T) -> Result<(), Self::Error> {
        self.0.send(item)
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
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

        tokio::net::TcpStream::from_std(s)
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

impl<T> Sender for tokio::sync::mpsc::UnboundedSender<T> {
    type Item = T;

    fn send(&mut self, t: Self::Item) -> Result<(), tcp::SendError> {
        tokio::sync::mpsc::UnboundedSender::send(self, t).map_err(|_| {
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
    ) -> io::Result<Box<dyn Sink<T, Error = bincode::Error> + Send + Unpin>> {
        if let Some(chan) = self.chan {
            Ok(Box::new(
                ImplSinkForSender(chan)
                    .sink_map_err(|_| serde::de::Error::custom("failed to do local send")),
            ) as Box<_>)
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
    locals: HashMap<K, tokio::sync::mpsc::UnboundedSender<T>>,
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

    pub fn insert_local(&self, key: K, chan: tokio::sync::mpsc::UnboundedSender<T>) {
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
