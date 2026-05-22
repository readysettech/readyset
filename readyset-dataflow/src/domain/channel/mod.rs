//! Per-process channel coordination and TCP framing for a single ReadySet server.
//!
//! Inter-domain traffic stays in-process via the [`ChannelCoordinator`]; the only
//! TCP path is for base-table writes from the replicator (or any external base-table
//! writer), handled by the [`tcp`] submodule.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::RwLock;
use std::task::{Context, Poll};

use futures_util::sink::{Sink, SinkExt};
use metrics::{Gauge, gauge};
use readyset_client::internal::DomainIndex;
use readyset_client::metrics::recorded;
use readyset_errors::{ReadySetError, ReadySetResult};
use strum::{EnumCount, IntoEnumIterator};
use tokio::sync::broadcast;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};

use crate::{Packet, PacketDiscriminants};

pub mod tcp;

pub use self::tcp::BaseWriteStream;

/// Default capacity for the bounded replay channel used for backpressure during full
/// materialization replays.
const DEFAULT_REPLAY_CHANNEL_CAPACITY: usize = 256;

/// Sender half of the bounded replay channel. Used by the replay thread to send chunked
/// replay packets back to the domain with backpressure.
#[derive(Clone)]
pub struct ReplaySender {
    tx: mpsc::Sender<Packet>,
}

impl ReplaySender {
    /// Send a replay packet, blocking the current thread until capacity is available.
    /// This provides backpressure so the replay thread doesn't outrun the domain.
    #[allow(clippy::result_large_err)]
    pub fn blocking_send(&self, packet: Packet) -> Result<(), mpsc::error::SendError<Packet>> {
        self.tx.blocking_send(packet)
    }
}

/// Receiver half of the bounded replay channel. Polled by the Replica event loop to
/// receive chunked replay packets.
pub struct ReplayReceiver {
    rx: mpsc::Receiver<Packet>,
}

impl ReplayReceiver {
    /// Receive the next replay packet, or `None` if all senders have been dropped.
    pub async fn recv(&mut self) -> Option<Packet> {
        self.rx.recv().await
    }
}

/// Create a bounded replay channel with the default capacity.
pub fn replay_channel() -> (ReplaySender, ReplayReceiver) {
    replay_channel_with_capacity(DEFAULT_REPLAY_CHANNEL_CAPACITY)
}

/// Create a bounded replay channel with the given capacity.
pub fn replay_channel_with_capacity(capacity: usize) -> (ReplaySender, ReplayReceiver) {
    let (tx, rx) = mpsc::channel(capacity);
    (ReplaySender { tx }, ReplayReceiver { rx })
}

/// Buffer size to use for the broadcast channel to notify replicas about changes to the addresses
/// of other replicas
///
/// If more than this number of changes to replica addresses are enqueued without all replicas
/// reading them, the replicas that lag behind will reconnect to all other replicas
const COORDINATOR_CHANGE_CHANNEL_BUFFER_SIZE: usize = 64;

/// Constructs a [`DomainSender`]/[`DomainReceiver`] channel that can be used to send [`Packet`]s to
/// a domain who lives in the same process as the sender.
pub(crate) fn domain_channel() -> (DomainSender, DomainReceiver) {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let packets_queued: [Gauge; PacketDiscriminants::COUNT] = PacketDiscriminants::iter()
        .map(|d| {
            let name: &'static str = d.into();
            gauge!(recorded::DOMAIN_PACKETS_QUEUED, "packet_type" => name)
        })
        .collect::<Vec<Gauge>>()
        .try_into()
        .ok()
        .unwrap();

    (
        DomainSender {
            tx,
            packets_queued: packets_queued.clone(),
        },
        DomainReceiver { rx, packets_queued },
    )
}

/// A wrapper around a [`tokio::sync::mpsc::UnboundedSender`] to be used for sending messages to
/// domains who live in the same process as the sender.
#[derive(Clone)]
pub struct DomainSender {
    tx: UnboundedSender<Packet>,
    packets_queued: [Gauge; PacketDiscriminants::COUNT],
}

impl DomainSender {
    #[allow(clippy::result_large_err)]
    pub fn send(&self, packet: Packet) -> Result<(), mpsc::error::SendError<Packet>> {
        let discriminant: PacketDiscriminants = (&packet).into();

        self.tx.send(packet).map(|()| {
            self.packets_queued[discriminant as usize].increment(1.0);
        })
    }
}

/// A wrapper around a [`tokio::sync::mpsc::UnboundedReceiver`] to be used for sending messages to
/// domains who live in the same process as the sender.
pub struct DomainReceiver {
    rx: UnboundedReceiver<Packet>,
    packets_queued: [Gauge; PacketDiscriminants::COUNT],
}

impl DomainReceiver {
    pub async fn recv(&mut self) -> Option<Packet> {
        let packet = self.rx.recv().await;
        packet.as_ref().inspect(|packet| {
            let discriminant: PacketDiscriminants = (*packet).into();
            self.packets_queued[discriminant as usize].decrement(1.0);
        });
        packet
    }
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

struct ChannelCoordinatorInner {
    /// Map from key to remote address.
    addrs: HashMap<DomainIndex, SocketAddr>,
    /// Map from key to channel sender for local connections.
    locals: HashMap<DomainIndex, DomainSender>,
}

pub struct ChannelCoordinator {
    inner: RwLock<ChannelCoordinatorInner>,
    /// Broadcast channel that can be used to be notified when the address for a key changes
    changes_tx: broadcast::Sender<DomainIndex>,
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
    pub fn subscribe(&self) -> broadcast::Receiver<DomainIndex> {
        self.changes_tx.subscribe()
    }

    pub fn insert_remote(&self, key: DomainIndex, addr: SocketAddr) {
        #[allow(clippy::expect_used)]
        // This can only fail if the mutex is poisoned, in which case we can't recover,
        // so we allow to panic if that happens.
        let changed = {
            let mut guard = self.inner.write().expect("poisoned mutex");
            let old = guard.addrs.insert(key, addr);

            // Don't publish if we're inserting a *new* address
            old.is_some() && old != Some(addr)
        };

        if changed {
            let _ = self.changes_tx.send(key);
        }
    }

    pub fn remove(&self, key: DomainIndex) {
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

    pub fn insert_local(&self, key: DomainIndex, chan: DomainSender) {
        #[allow(clippy::expect_used)]
        // This can only fail if the mutex is poisoned, in which case we can't recover,
        // so we allow to panic if that happens.
        {
            let mut guard = self.inner.write().expect("poisoned mutex");
            guard.locals.insert(key, chan);
        }
    }

    pub fn has(&self, key: &DomainIndex) -> bool {
        #[allow(clippy::expect_used)]
        // This can only fail if the mutex is poisoned, in which case we can't recover,
        // so we allow to panic if that happens.
        let guard = self.inner.read().expect("poisoned mutex");
        guard.addrs.contains_key(key)
    }

    pub fn get_addr(&self, key: &DomainIndex) -> Option<SocketAddr> {
        #[allow(clippy::expect_used)]
        // This can only fail if the mutex is poisoned, in which case we can't recover,
        // so we allow to panic if that happens.
        let guard = self.inner.read().expect("poisoned mutex");
        guard.addrs.get(key).cloned()
    }

    pub fn is_local(&self, key: &DomainIndex) -> Option<bool> {
        #[allow(clippy::expect_used)]
        // This can only fail if the mutex is poisoned, in which case we can't recover,
        // so we allow to panic if that happens.
        let guard = self.inner.read().expect("poisoned mutex");
        guard.locals.get(key).map(|_| true)
    }

    /// Open a `Sink<Packet>` for sending packets to the given domain via its
    /// in-process channel. Returns an error if the domain has not yet
    /// registered itself with `insert_local`.
    pub fn connect_to(
        &self,
        key: &DomainIndex,
    ) -> ReadySetResult<Box<dyn Sink<Packet, Error = bincode::Error> + Send + Unpin>> {
        #[allow(clippy::expect_used)]
        // This can only fail if the mutex is poisoned, in which case we can't recover,
        // so we allow to panic if that happens.
        let guard = self.inner.read().expect("poisoned mutex");
        let chan = guard
            .locals
            .get(key)
            .cloned()
            .ok_or_else(|| ReadySetError::DomainNotFound {
                domain_index: key.index(),
            })?;
        Ok(Box::new(chan.sink_map_err(|_| {
            serde::de::Error::custom("failed to do local send")
        })))
    }

    pub fn clear(&self) {
        let mut guard = self.inner.write().expect("poisoned mutex");
        guard.addrs.clear();
        guard.locals.clear();
    }
}
