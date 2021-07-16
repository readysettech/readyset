//! A wrapper around TCP channels that Noria uses to communicate between clients and servers, and
//! inside the data-flow graph. At this point, this is mostly a thin wrapper around
//! [`async-bincode`](https://docs.rs/async-bincode/), and it might go away in the long run.

use std::borrow::Borrow;
use std::collections::HashMap;
use std::hash::Hash;
use std::io::{self, Write};
use std::net::SocketAddr;
use std::sync::RwLock;
use std::{
    pin::Pin,
    task::{Context, Poll},
};

use async_bincode::{AsyncBincodeWriter, AsyncDestination};
use futures_util::sink::{Sink, SinkExt};
use tokio::io::BufWriter;

pub mod tcp;

pub use self::tcp::{DualTcpStream, TcpSender};
use crate::internal::DomainIndex;
use crate::ReadySetError;
use crate::ReadySetResult;

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

    pub fn insert_remote(&self, key: K, addr: SocketAddr) -> ReadySetResult<()> {
        #[allow(clippy::expect_used)]
        // This can only fail if the mutex is poisoned, in which case we can't recover,
        // so we allow to panic if that happens.
        let mut guard = self.inner.write().expect("poisoned mutex");
        guard.addrs.insert(key, addr);
        Ok(())
    }

    pub fn insert_local(
        &self,
        key: K,
        chan: tokio::sync::mpsc::UnboundedSender<T>,
    ) -> ReadySetResult<()> {
        #[allow(clippy::expect_used)]
        // This can only fail if the mutex is poisoned, in which case we can't recover,
        // so we allow to panic if that happens.
        let mut guard = self.inner.write().expect("poisoned mutex");
        guard.locals.insert(key, chan);
        Ok(())
    }

    pub fn has<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        #[allow(clippy::expect_used)]
        // This can only fail if the mutex is poisoned, in which case we can't recover,
        // so we allow to panic if that happens.
        let guard = self.inner.read().expect("poisoned mutex");
        guard.addrs.contains_key(key)
    }

    pub fn get_addr<Q>(&self, key: &Q) -> Option<SocketAddr>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        #[allow(clippy::expect_used)]
        // This can only fail if the mutex is poisoned, in which case we can't recover,
        // so we allow to panic if that happens.
        let guard = self.inner.read().expect("poisoned mutex");
        guard.addrs.get(key).cloned()
    }

    pub fn is_local<Q>(&self, key: &Q) -> Option<bool>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        #[allow(clippy::expect_used)]
        // This can only fail if the mutex is poisoned, in which case we can't recover,
        // so we allow to panic if that happens.
        let guard = self.inner.read().expect("poisoned mutex");
        guard.locals.get(key).map(|_| true)
    }

    pub fn builder_for(
        &self,
        key: &(DomainIndex, usize),
    ) -> ReadySetResult<DomainConnectionBuilder<MaybeLocal, T>>
    where
        K: Borrow<(DomainIndex, usize)>,
    {
        #[allow(clippy::expect_used)]
        // This can only fail if the mutex is poisoned, in which case we can't recover,
        // so we allow to panic if that happens.
        let guard = self.inner.read().expect("poisoned mutex");
        match guard.addrs.get(key) {
            None => Err(ReadySetError::NoSuchDomain {
                domain_index: key.0.index(),
                shard: key.1,
            }),
            Some(addrs) => Ok(DomainConnectionBuilder {
                sport: None,
                addr: *addrs,
                chan: guard.locals.get(key).cloned(),
                is_for_base: false,
                _marker: MaybeLocal,
            }),
        }
    }
}
