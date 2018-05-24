use channel::rpc::RpcServiceEndpoint;
use channel::tcp::{TcpSender, TryRecvError};
use channel::{self, DomainConnectionBuilder};
use consensus::Epoch;
use dataflow::payload;
use dataflow::prelude::{ChannelCoordinator, DomainIndex};
use dataflow::{DomainBuilder, Readers};

use controller::{readers, ControllerState};
use coordination::{CoordinationMessage, CoordinationPayload};
use worker;

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use slog;

pub(super) struct WorkerInner {
    epoch: Epoch,
    worker_pool: worker::WorkerPool,
    channel_coordinator: Arc<ChannelCoordinator>,
    readers: Readers,
    read_threads: PoolHandle<()>,

    sender: TcpSender<CoordinationMessage>,
    sender_addr: SocketAddr,

    heartbeat_every: Duration,
    last_heartbeat: Instant,

    listen_addr: IpAddr,

    memory_limit: Option<usize>,
    evict_every: Option<Duration>,

    state_sizes: HashMap<(DomainIndex, usize), Arc<AtomicUsize>>,
    domain_senders: HashMap<(DomainIndex, usize), TcpSender<Box<payload::Packet>>>,

    log: slog::Logger,
}
