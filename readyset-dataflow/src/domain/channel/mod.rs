//! A wrapper around TCP channels that ReadySet uses to communicate between clients and servers, and
//! inside the data-flow graph. At this point, this is mostly a thin wrapper around
//! [`async-bincode`](https://docs.rs/async-bincode/), and it might go away in the long run.

use std::collections::HashMap;
use std::io::{self, Write};
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::RwLock;
use std::task::{Context, Poll};

use async_bincode::{AsyncBincodeWriter, AsyncDestination};
use futures_util::sink::{Sink, SinkExt};
use readyset_client::internal::ReplicaAddress;
use readyset_client::{CONNECTION_FROM_BASE, CONNECTION_FROM_DOMAIN};
use readyset_errors::{ReadySetError, ReadySetResult};
use tokio::io::BufWriter;
use tokio::sync::broadcast;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};

use crate::prelude::Packet;

pub mod tcp;

pub use self::tcp::{DualTcpStream, TcpSender};

/// Buffer size to use for the broadcast channel to notify replicas about changes to the addresses
/// of other replicas
///
/// If more than this number of changes to replica addresses are enqueued without all replicas
/// reading them, the replicas that lag behind will reconnect to all other replicas
const COORDINATOR_CHANGE_CHANNEL_BUFFER_SIZE: usize = 64;

/// Constructs a [`DomainSender`]/[`DomainReceiver`] channel that can be used to send [`Packet`]s to
/// a domain who lives in the same process as the sender.
pub fn channel() -> (DomainSender, DomainReceiver) {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

    (DomainSender { tx }, DomainReceiver { rx })
}

/// A wrapper around a [`tokio::sync::mpsc::UnboundedSender`] to be used for sending messages to
/// domains who live in the same process as the sender.
#[derive(Clone)]
pub struct DomainSender {
    tx: UnboundedSender<Packet>,
}

impl DomainSender {
    pub fn send(&self, packet: Packet) -> Result<(), mpsc::error::SendError<Packet>> {
        self.tx.send(packet)
    }
}

/// A wrapper around a [`tokio::sync::mpsc::UnboundedReceiver`] to be used for sending messages to
/// domains who live in the same process as the sender.
pub struct DomainReceiver {
    rx: UnboundedReceiver<Packet>,
}

impl DomainReceiver {
    pub async fn recv(&mut self) -> Option<Packet> {
        self.rx.recv().await
    }
}

pub struct Remote;
pub struct MaybeLocal;

#[must_use]
pub struct DomainConnectionBuilder<D> {
    sport: Option<u16>,
    addr: SocketAddr,
    chan: Option<DomainSender>,
    is_for_base: bool,
    _marker: D,
}

impl Sink<Packet> for DomainSender {
    type Error = mpsc::error::SendError<Packet>;

    fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, item: Packet) -> Result<(), Self::Error> {
        DomainSender::send(&self, item)
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}

impl DomainConnectionBuilder<Remote> {
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

impl DomainConnectionBuilder<Remote> {
    /// Establishes a TCP sink for an asynchronous context. The function may block for a long
    /// time while the connection is being established, be careful not to call it on our main Tokio
    /// executer, but only from inside a Domain thread.
    pub fn build_async(
        self,
    ) -> io::Result<AsyncBincodeWriter<BufWriter<tokio::net::TcpStream>, Packet, AsyncDestination>>
    {
        // TODO: async
        // we must currently write and call flush, because the remote end (currently) does a
        // synchronous read upon accepting a connection.
        let s = self.build_sync()?.into_inner().into_inner()?;

        tokio::net::TcpStream::from_std(s)
            .map(BufWriter::new)
            .map(AsyncBincodeWriter::from)
            .map(AsyncBincodeWriter::for_async)
    }

    /// Establishes a TCP sink for a synchronous context. The function may block for a long
    /// time while the connection is being established, be careful not to call it on our main Tokio
    /// executer, but only from inside a Domain thread.
    pub fn build_sync(self) -> io::Result<TcpSender> {
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
    fn send(&mut self, packet: Packet) -> Result<(), tcp::SendError>;
}

impl Sender for DomainSender {
    fn send(&mut self, packet: Packet) -> Result<(), tcp::SendError> {
        DomainSender::send(self, packet).map_err(|_| {
            tcp::SendError::IoError(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "local peer went away",
            ))
        })
    }
}

impl Sender for TcpSender {
    fn send(&mut self, packet: Packet) -> Result<(), tcp::SendError> {
        self.send_ref(&packet)
    }
}

impl DomainConnectionBuilder<MaybeLocal> {
    pub fn build_async(
        self,
    ) -> io::Result<Box<dyn Sink<Packet, Error = bincode::Error> + Send + Unpin>> {
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

    pub fn build_sync(self) -> io::Result<Box<dyn Sender + Send>> {
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

struct ChannelCoordinatorInner {
    /// Map from key to remote address.
    addrs: HashMap<ReplicaAddress, SocketAddr>,
    /// Map from key to channel sender for local connections.
    locals: HashMap<ReplicaAddress, DomainSender>,
}

pub struct ChannelCoordinator {
    inner: RwLock<ChannelCoordinatorInner>,
    /// Broadcast channel that can be used to be notified when the address for a key changes
    changes_tx: broadcast::Sender<ReplicaAddress>,
}

impl Default for ChannelCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

impl ChannelCoordinator {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(ChannelCoordinatorInner {
                addrs: Default::default(),
                locals: Default::default(),
            }),
            changes_tx: broadcast::channel(COORDINATOR_CHANGE_CHANNEL_BUFFER_SIZE).0,
        }
    }

    /// Create a new [`broadcast::Receiver`] which will be notified whenver the *remote* address for
    /// a key is changed (but *not* when a new key is added, or when the local address changes!)
    pub fn subscribe(&self) -> broadcast::Receiver<ReplicaAddress> {
        self.changes_tx.subscribe()
    }

    pub fn insert_remote(&self, key: ReplicaAddress, addr: SocketAddr) {
        #[allow(clippy::expect_used)]
        // This can only fail if the mutex is poisoned, in which case we can't recover,
        // so we allow to panic if that happens.
        let changed = {
            let mut guard = self.inner.write().expect("poisoned mutex");
            let old = guard.addrs.insert(key, addr);

            old.is_some() // Don't publish if we're inserting a *new* address
                && old != Some(addr)
        };

        if changed {
            let _ = self.changes_tx.send(key);
        }
    }

    pub fn remove(&self, key: ReplicaAddress) {
        #[allow(clippy::expect_used)]
        // This can only fail if the mutex is poisoned, in which case we can't recover,
        // so we allow to panic if that happens.
        let removed = {
            let mut guard = self.inner.write().expect("poisoned mutex");
            guard.locals.remove(&key);
            guard.addrs.remove(&key).is_some()
        };

        if removed {
            let _ = self.changes_tx.send(key);
        }
    }

    pub fn insert_local(&self, key: ReplicaAddress, chan: DomainSender) {
        #[allow(clippy::expect_used)]
        // This can only fail if the mutex is poisoned, in which case we can't recover,
        // so we allow to panic if that happens.
        {
            let mut guard = self.inner.write().expect("poisoned mutex");
            guard.locals.insert(key, chan);
        }
    }

    pub fn has(&self, key: &ReplicaAddress) -> bool {
        #[allow(clippy::expect_used)]
        // This can only fail if the mutex is poisoned, in which case we can't recover,
        // so we allow to panic if that happens.
        let guard = self.inner.read().expect("poisoned mutex");
        guard.addrs.contains_key(key)
    }

    pub fn get_addr(&self, key: &ReplicaAddress) -> Option<SocketAddr> {
        #[allow(clippy::expect_used)]
        // This can only fail if the mutex is poisoned, in which case we can't recover,
        // so we allow to panic if that happens.
        let guard = self.inner.read().expect("poisoned mutex");
        guard.addrs.get(key).cloned()
    }

    pub fn is_local(&self, key: &ReplicaAddress) -> Option<bool> {
        #[allow(clippy::expect_used)]
        // This can only fail if the mutex is poisoned, in which case we can't recover,
        // so we allow to panic if that happens.
        let guard = self.inner.read().expect("poisoned mutex");
        guard.locals.get(key).map(|_| true)
    }

    pub fn builder_for(
        &self,
        key: &ReplicaAddress,
    ) -> ReadySetResult<DomainConnectionBuilder<MaybeLocal>> {
        #[allow(clippy::expect_used)]
        // This can only fail if the mutex is poisoned, in which case we can't recover,
        // so we allow to panic if that happens.
        let guard = self.inner.read().expect("poisoned mutex");
        #[allow(clippy::significant_drop_in_scrutinee)]
        match guard.addrs.get(key) {
            None => Err(ReadySetError::NoSuchReplica {
                domain_index: key.domain_index.index(),
                shard: key.shard,
                replica: key.replica,
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

    pub fn clear(&self) {
        let mut guard = self.inner.write().expect("poisoned mutex");
        guard.addrs.clear();
        guard.locals.clear();
    }
}
