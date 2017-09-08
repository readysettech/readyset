use petgraph::graph::NodeIndex;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time;
use std::collections::hash_map::Entry;
use std::rc::Rc;

use std::net::SocketAddr;

use channel::TcpSender;
use channel::poll::{KeepPolling, PollEvent, PollingLoop, StopPolling};
use flow::prelude::*;
use flow::payload::{ControlReplyPacket, ReplayPieceContext, ReplayTransactionState,
                    TransactionState};
use flow::statistics;
use flow::transactions;
use flow::persistence;
use flow::debug;
use flow;
use checktable;
use slog::Logger;
use timekeeper::{RealTime, SimpleTracker, ThreadTime, Timer, TimerSet};
use tarpc::sync::client::{self, ClientExt};

const BATCH_SIZE: usize = 256;

const NANOS_PER_SEC: u64 = 1_000_000_000;
macro_rules! dur_to_ns {
    ($d:expr) => {{
        let d = $d;
        d.as_secs() * NANOS_PER_SEC + d.subsec_nanos() as u64
    }}
}

#[allow(missing_docs)]
#[derive(Eq, PartialEq, Ord, PartialOrd, Hash, Clone, Copy, Debug, Serialize, Deserialize)]
pub struct Index(usize);

impl From<usize> for Index {
    fn from(i: usize) -> Self {
        Index(i)
    }
}

impl Into<usize> for Index {
    fn into(self) -> usize {
        self.0
    }
}

#[allow(missing_docs)]
impl Index {
    pub fn index(&self) -> usize {
        self.0
    }
}

pub mod local;
mod handle;
pub use self::handle::{DomainHandle, DomainInputHandle};

enum DomainMode {
    Forwarding,
    Replaying {
        to: LocalNodeIndex,
        buffered: VecDeque<Box<Packet>>,
        passes: usize,
    },
}

enum TriggerEndpoint {
    None,
    Start(Vec<usize>),
    End(Vec<TcpSender<Box<Packet>>>),
    Local(Vec<usize>),
}

struct ReplayPath {
    source: Option<LocalNodeIndex>,
    path: Vec<(LocalNodeIndex, Option<usize>)>,
    notify_done: bool,
    trigger: TriggerEndpoint,
}

/// When one node misses in another during a replay, a HoleSubscription will be registered with the
/// target node, and a replay to the target node will be triggered for the key in question. When
/// that replay eventually finishes, the subscription will cause the target node to notify this
/// subscription, causing the replay to progress.
#[derive(Debug)]
struct HoleSubscription {
    key: Vec<DataType>,
    tag: Tag,
}

/// A waiting node is one that is waiting for at least one incoming replay.
///
/// Upon receiving a replay message, the node should attempt to re-process replays for any
/// downstream nodes that missed in on the key that was just filled.
#[derive(Debug)]
struct Waiting {
    subscribed: Vec<HoleSubscription>,
}

/// Struct sent to a worker to start a domain.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DomainBuilder {
    /// The domain's index.
    pub index: Index,
    /// The shard ID represented by this `DomainBuilder`.
    pub shard: usize,
    /// The number of shards in the domain.
    pub nshards: usize,
    /// The nodes in the domain.
    pub nodes: DomainNodes,
    /// The domain's persistence setting.
    pub persistence_parameters: persistence::Parameters,
    /// The starting timestamp.
    pub ts: i64,
    /// The socket address at which this domain receives control messages.
    pub control_addr: SocketAddr,
    /// The socket address over which this domain communicates with the checktable service.
    pub checktable_addr: SocketAddr,
    /// The socket address for debug interactions with this domain.
    pub debug_addr: Option<SocketAddr>,
}

impl DomainBuilder {
    /// Starts up the domain represented by this `DomainBuilder`.
    pub fn boot(
        self,
        log: Logger,
        readers: flow::Readers,
        channel_coordinator: Arc<ChannelCoordinator>,
    ) -> (thread::JoinHandle<()>, SocketAddr) {
        // initially, all nodes are not ready
        let not_ready = self.nodes
            .values()
            .map(|n| *n.borrow().local_addr())
            .collect();

        let shard = if self.nshards == 1 {
            None
        } else {
            Some(self.shard)
        };

        let debug_tx = self.debug_addr
            .as_ref()
            .map(|addr| TcpSender::connect(addr, None).unwrap());
        let mut control_reply_tx = TcpSender::connect(&self.control_addr, None).unwrap();

        // Create polling loop and tell the controller what port we are listening on.
        let polling_loop = PollingLoop::<Box<Packet>>::new();
        let addr = polling_loop.get_listener_addr().unwrap();
        control_reply_tx
            .send(ControlReplyPacket::Booted(shard.unwrap_or(0), addr.clone()))
            .unwrap();

        info!(log, "booting domain"; "nodes" => self.nodes.iter().count());
        let name = match shard {
            Some(shard) => format!("domain{}.{}", self.index.0, shard),
            None => format!("domain{}", self.index.0),
        };

        let jh = thread::Builder::new()
            .name(name)
            .spawn(move || {
                let checktable = Rc::new(
                    checktable::CheckTableClient::connect(
                        self.checktable_addr,
                        client::Options::default(),
                    ).unwrap(),
                );

                Domain {
                    index: self.index,
                    shard,
                    nshards: self.nshards,
                    transaction_state: transactions::DomainState::new(
                        self.index,
                        checktable,
                        self.ts,
                    ),
                    persistence_parameters: self.persistence_parameters,
                    nodes: self.nodes,
                    state: StateMap::default(),
                    log,
                    not_ready,
                    mode: DomainMode::Forwarding,
                    waiting: local::Map::new(),
                    reader_triggered: local::Map::new(),
                    replay_paths: HashMap::new(),

                    addr,
                    readers,
                    inject: None,
                    _debug_tx: debug_tx,
                    control_reply_tx,
                    channel_coordinator,

                    concurrent_replays: 0,
                    replay_request_queue: Default::default(),

                    total_time: Timer::new(),
                    total_ptime: Timer::new(),
                    wait_time: Timer::new(),
                    process_times: TimerSet::new(),
                    process_ptimes: TimerSet::new(),
                }.run(polling_loop)
            })
            .unwrap();
        (jh, addr)
    }
}

pub struct Domain {
    index: Index,
    shard: Option<usize>,
    nshards: usize,

    nodes: DomainNodes,
    state: StateMap,
    log: Logger,

    not_ready: HashSet<LocalNodeIndex>,

    transaction_state: transactions::DomainState,
    persistence_parameters: persistence::Parameters,

    mode: DomainMode,
    waiting: local::Map<Waiting>,
    replay_paths: HashMap<Tag, ReplayPath>,
    reader_triggered: local::Map<HashSet<DataType>>,

    concurrent_replays: usize,
    replay_request_queue: VecDeque<(Tag, Vec<DataType>)>,

    addr: SocketAddr,
    readers: flow::Readers,
    inject: Option<Box<Packet>>,
    _debug_tx: Option<TcpSender<debug::DebugEvent>>,
    control_reply_tx: TcpSender<ControlReplyPacket>,
    channel_coordinator: Arc<ChannelCoordinator>,

    total_time: Timer<SimpleTracker, RealTime>,
    total_ptime: Timer<SimpleTracker, ThreadTime>,
    wait_time: Timer<SimpleTracker, RealTime>,
    process_times: TimerSet<LocalNodeIndex, SimpleTracker, RealTime>,
    process_ptimes: TimerSet<LocalNodeIndex, SimpleTracker, ThreadTime>,
}

impl Domain {
    fn on_replay_miss(&mut self, miss: LocalNodeIndex, key: Vec<DataType>, needed_for: Tag) {
        // when the replay eventually succeeds, we want to re-do the replay.
        let mut already_filling = false;
        let mut subscribed = match self.waiting.remove(&miss) {
            Some(Waiting { subscribed }) => {
                already_filling = subscribed.iter().any(|s| s.key == key);
                subscribed
            }
            None => Vec::new(),
        };
        subscribed.push(HoleSubscription {
            key: key.clone(),
            tag: needed_for,
        });
        self.waiting.insert(miss, Waiting { subscribed });

        if already_filling {
            // no need to trigger again
            return;
        }

        let tags: Vec<Tag> = self.replay_paths.keys().cloned().collect();
        for tag in tags {
            if let TriggerEndpoint::Start(..) = self.replay_paths[&tag].trigger {
                continue;
            }
            if self.replay_paths[&tag].path.last().unwrap().0 != miss {
                continue;
            }

            // send a message to the source domain(s) responsible
            // for the chosen tag so they'll start replay.
            let key = key.clone(); // :(
            if let TriggerEndpoint::Local(..) = self.replay_paths[&tag].trigger {
                if self.already_requested(&tag, &key[..]) {
                    return;
                }

                trace!(self.log,
                       "got replay request";
                       "tag" => tag.id(),
                       "key" => format!("{:?}", key)
                );
                self.seed_replay(tag, &key[..], None);
                continue;
            }

            // NOTE: due to MAX_CONCURRENT_REPLAYS, it may be that we only replay from *some* of
            // these ancestors now, and some later. this will cause more of the replay to be
            // buffered up at the union above us, but that's probably fine.
            self.request_partial_replay(tag, key);
        }
    }

    fn send_partial_replay_request(&mut self, tag: Tag, key: Vec<DataType>) {
        debug_assert!(self.concurrent_replays < ::MAX_CONCURRENT_REPLAYS);
        if let TriggerEndpoint::End(ref mut triggers) =
            self.replay_paths.get_mut(&tag).unwrap().trigger
        {
            // find right shard. it's important that we only request a replay from the right
            // shard, because otherwise all the other shard domains will miss, and then request
            // replays themselves for the miss key. however, the response to that request will
            // never be routed to them, leading to infinite loops and whatnot.
            let shard = if triggers.len() == 1 {
                0
            } else {
                assert!(key.len() == 1);
                ::shard_by(&key[0], triggers.len())
            };
            self.concurrent_replays += 1;
            trace!(self.log, "sending replay request";
                   "tag" => ?tag,
                   "key" => ?key,
                   "buffered" => self.replay_request_queue.len(),
                   "concurrent" => self.concurrent_replays,
                   );
            if triggers[shard]
                .send(box Packet::RequestPartialReplay { tag, key })
                .is_err()
            {
                // we're shutting down -- it's fine.
            }
        } else {
            unreachable!("asked to replay along non-existing path")
        }
    }

    fn request_partial_replay(&mut self, tag: Tag, key: Vec<DataType>) {
        if self.concurrent_replays < ::MAX_CONCURRENT_REPLAYS {
            assert_eq!(self.replay_request_queue.len(), 0);
            self.send_partial_replay_request(tag, key);
        } else {
            trace!(self.log, "buffering replay request";
                   "tag" => ?tag,
                   "key" => ?key,
                   "buffered" => self.replay_request_queue.len(),
                   );
            self.replay_request_queue.push_back((tag, key));
        }
    }

    fn finished_partial_replay(&mut self, tag: &Tag) {
        match self.replay_paths[tag].trigger {
            TriggerEndpoint::End(..) => {
                // A backfill request we made to another domain was just satisfied!
                // We can now issue another request from the concurrent replay queue.
                // However, since unions require multiple backfill requests, but produce only one
                // backfill reply, we need to check how many requests we're now free to issue. If
                // we just naively release one slot here, a union with two parents would mean that
                // `self.concurrent_replays` constantly grows by +1 (+2 for the backfill requests,
                // -1 when satisfied), which would lead to a deadlock!
                let end = self.replay_paths[tag].path.last().unwrap().0;
                let requests_satisfied = self.replay_paths
                    .iter()
                    .filter(|&(_, p)| if let TriggerEndpoint::End(..) = p.trigger {
                        p.path.last().unwrap().0 == end
                    } else {
                        false
                    })
                    .count();

                self.concurrent_replays -= requests_satisfied;
                trace!(self.log, "notified of finished replay";
                       "#done" => requests_satisfied,
                       "ongoing" => self.concurrent_replays,
                       );
                debug_assert!(self.concurrent_replays < ::MAX_CONCURRENT_REPLAYS);
                while self.concurrent_replays < ::MAX_CONCURRENT_REPLAYS {
                    if let Some((tag, key)) = self.replay_request_queue.pop_front() {
                        trace!(self.log, "releasing replay request";
                               "tag" => ?tag,
                               "key" => ?key,
                               "left" => self.replay_request_queue.len(),
                               "ongoing" => self.concurrent_replays,
                               );
                        self.send_partial_replay_request(tag, key);
                    } else {
                        return;
                    }
                }
            }
            TriggerEndpoint::Local(..) => {
                // didn't count against our quote, so we're also not decementing
            }
            TriggerEndpoint::Start(..) | TriggerEndpoint::None => {
                unreachable!();
            }
        }
    }

    fn dispatch(
        m: Box<Packet>,
        not_ready: &HashSet<LocalNodeIndex>,
        mode: &mut DomainMode,
        waiting: &mut local::Map<Waiting>,
        states: &mut StateMap,
        nodes: &DomainNodes,
        shard: Option<usize>,
        paths: &mut HashMap<Tag, ReplayPath>,
        process_times: &mut TimerSet<LocalNodeIndex, SimpleTracker, RealTime>,
        process_ptimes: &mut TimerSet<LocalNodeIndex, SimpleTracker, ThreadTime>,
        enable_output: bool,
    ) -> HashMap<LocalNodeIndex, Vec<Record>> {
        let me = m.link().dst;
        let mut output_messages = HashMap::new();

        match *mode {
            DomainMode::Forwarding => (),
            DomainMode::Replaying {
                ref to,
                ref mut buffered,
                ..
            } if to == &me =>
            {
                buffered.push_back(m);
                return output_messages;
            }
            DomainMode::Replaying { .. } => (),
        }

        if !not_ready.is_empty() && not_ready.contains(&me) {
            return output_messages;
        }

        let mut n = nodes[&me].borrow_mut();
        process_times.start(me);
        process_ptimes.start(me);
        let mut m = Some(m);
        n.process(&mut m, None, states, nodes, shard, true);
        process_ptimes.stop();
        process_times.stop();
        drop(n);

        if m.is_none() {
            // no need to deal with our children if we're not sending them anything
            return output_messages;
        }

        // ignore misses during regular forwarding
        match m.as_ref().unwrap() {
            m @ &box Packet::Message { .. } if m.is_empty() => {
                // no need to deal with our children if we're not sending them anything
                return output_messages;
            }
            &box Packet::Message { .. } => {}
            &box Packet::Transaction { .. } => {
                // Any message with a timestamp (ie part of a transaction) must flow through the
                // entire graph, even if there are no updates associated with it.
            }
            &box Packet::ReplayPiece { .. } | &box Packet::FullReplay { .. } => {
                unreachable!("replay should never go through dispatch");
            }
            ref m => unreachable!("dispatch process got {:?}", m),
        }

        let n = nodes[&me].borrow();
        for i in 0..n.nchildren() {
            // avoid cloning if we can
            let mut m = if i == n.nchildren() - 1 {
                m.take().unwrap()
            } else {
                m.as_ref().map(|m| box m.clone_data()).unwrap()
            };

            if enable_output || !nodes[n.child(i)].borrow().is_output() {
                if n.is_shard_merger() {
                    // we need to preserve the egress src (which includes shard identifier)
                } else {
                    m.link_mut().src = me;
                }
                m.link_mut().dst = *n.child(i);

                for (k, mut v) in Self::dispatch(
                    m,
                    not_ready,
                    mode,
                    waiting,
                    states,
                    nodes,
                    shard,
                    paths,
                    process_times,
                    process_ptimes,
                    enable_output,
                ) {
                    use std::collections::hash_map::Entry;
                    match output_messages.entry(k) {
                        Entry::Occupied(mut rs) => rs.get_mut().append(&mut v),
                        Entry::Vacant(slot) => {
                            slot.insert(v);
                        }
                    }
                }
            } else {
                let mut data = m.take_data();
                match output_messages.entry(*n.child(i)) {
                    Entry::Occupied(entry) => {
                        entry.into_mut().append(&mut data);
                    }
                    Entry::Vacant(entry) => {
                        entry.insert(data.into());
                    }
                };
            }
        }

        output_messages
    }

    fn dispatch_(
        &mut self,
        m: Box<Packet>,
        enable_output: bool,
    ) -> HashMap<LocalNodeIndex, Vec<Record>> {
        Self::dispatch(
            m,
            &self.not_ready,
            &mut self.mode,
            &mut self.waiting,
            &mut self.state,
            &self.nodes,
            self.shard,
            &mut self.replay_paths,
            &mut self.process_times,
            &mut self.process_ptimes,
            enable_output,
        )
    }

    pub fn transactional_dispatch(&mut self, messages: Vec<Box<Packet>>) {
        assert!(!messages.is_empty());

        let mut egress_messages = HashMap::new();
        let (ts, tracer) = if let Packet::Transaction {
            state: ref ts @ TransactionState::Committed(..),
            ref tracer,
            ..
        } = *messages[0]
        {
            (ts.clone(), tracer.clone())
        } else {
            unreachable!();
        };

        for m in messages {
            let new_messages = self.dispatch_(m, false);

            for (key, mut value) in new_messages {
                egress_messages
                    .entry(key)
                    .or_insert_with(Vec::new)
                    .append(&mut value);
            }
        }

        let base = if let TransactionState::Committed(_, base, _) = ts {
            base
        } else {
            unreachable!()
        };

        for n in self.transaction_state.egress_for(base) {
            let n = &self.nodes[n];
            let data = match egress_messages.entry(*n.borrow().local_addr()) {
                Entry::Occupied(entry) => entry.remove().into(),
                _ => Records::default(),
            };

            let addr = *n.borrow().local_addr();
            // TODO: message should be from actual parent, not self.
            let m = if n.borrow().is_transactional() {
                box Packet::Transaction {
                    link: Link::new(addr, addr),
                    data: data,
                    state: ts.clone(),
                    tracer: tracer.clone(),
                }
            } else {
                // The packet is about to hit a non-transactional output node (which could be an
                // egress node), so it must be converted to a normal normal message.
                box Packet::Message {
                    link: Link::new(addr, addr),
                    data: data,
                    tracer: tracer.clone(),
                }
            };

            if !self.not_ready.is_empty() && self.not_ready.contains(&addr) {
                continue;
            }

            self.process_times.start(addr);
            self.process_ptimes.start(addr);
            let mut m = Some(m);
            self.nodes[&addr].borrow_mut().process(
                &mut m,
                None,
                &mut self.state,
                &self.nodes,
                self.shard,
                true,
            );
            self.process_ptimes.stop();
            self.process_times.stop();
            assert_eq!(n.borrow().nchildren(), 0);
        }
    }

    fn process_transactions(&mut self) {
        loop {
            match self.transaction_state.get_next_event() {
                transactions::Event::Transaction(m) => self.transactional_dispatch(m),
                transactions::Event::StartMigration => {
                    self.control_reply_tx
                        .send(ControlReplyPacket::ack())
                        .unwrap();
                }
                transactions::Event::CompleteMigration => {}
                transactions::Event::SeedReplay(tag, key, rts) => {
                    self.seed_replay(tag, &key[..], Some(rts))
                }
                transactions::Event::Replay(m) => self.handle_replay(m),
                transactions::Event::None => break,
            }
        }
    }

    fn already_requested(&mut self, tag: &Tag, key: &[DataType]) -> bool {
        match self.replay_paths.get(tag).unwrap() {
            &ReplayPath {
                trigger: TriggerEndpoint::End(..),
                ref path,
                ..
            } |
            &ReplayPath {
                trigger: TriggerEndpoint::Local(..),
                ref path,
                ..
            } => {
                // a miss in a reader! make sure we don't re-do work
                let addr = path.last().unwrap().0;
                let n = self.nodes[&addr].borrow();
                let mut already_replayed = false;
                n.with_reader(|r| {
                    if let Some(wh) = r.writer() {
                        if wh.try_find_and(&key[0], |_| ()).unwrap().0.is_some() {
                            // key has already been replayed!
                            already_replayed = true;
                        }
                    }
                });
                if already_replayed {
                    return true;
                }

                let mut had = false;
                if let Some(ref mut prev) = self.reader_triggered.get_mut(&addr) {
                    if prev.contains(&key[0]) {
                        // we've already requested a replay of this key
                        return true;
                    }
                    prev.insert(key[0].clone());
                    had = true;
                }

                if !had {
                    self.reader_triggered.insert(addr, HashSet::new());
                }
                false
            }
            _ => false,
        }
    }

    fn handle(&mut self, m: Box<Packet>) {
        m.trace(PacketEvent::Handle);

        match *m {
            Packet::Message { .. } => {
                self.dispatch_(m, true);
            }
            Packet::Transaction { .. } |
            Packet::StartMigration { .. } |
            Packet::CompleteMigration { .. } |
            Packet::ReplayPiece {
                transaction_state: Some(_),
                ..
            } => {
                self.transaction_state.handle(m);
                self.process_transactions();
            }
            Packet::ReplayPiece { .. } | Packet::FullReplay { .. } => {
                self.handle_replay(m);
            }
            consumed => {
                match consumed {
                    // workaround #16223
                    Packet::AddNode { node, parents } => {
                        use std::cell;
                        let addr = *node.local_addr();
                        self.not_ready.insert(addr);

                        for p in parents {
                            self.nodes
                                .get_mut(&p)
                                .unwrap()
                                .borrow_mut()
                                .add_child(*node.local_addr());
                        }
                        self.nodes.insert(addr, cell::RefCell::new(node));
                        trace!(self.log, "new node incorporated"; "local" => addr.id());
                    }
                    Packet::AddBaseColumn {
                        node,
                        field,
                        default,
                    } => {
                        let mut n = self.nodes[&node].borrow_mut();
                        n.add_column(&field);
                        n.get_base_mut()
                            .expect("told to add base column to non-base node")
                            .add_column(default);
                        self.control_reply_tx
                            .send(ControlReplyPacket::ack())
                            .unwrap();
                    }
                    Packet::DropBaseColumn { node, column } => {
                        let mut n = self.nodes[&node].borrow_mut();
                        n.get_base_mut()
                            .expect("told to drop base column from non-base node")
                            .drop_column(column);
                        self.control_reply_tx
                            .send(ControlReplyPacket::ack())
                            .unwrap();
                    }
                    Packet::UpdateEgress {
                        node,
                        new_tx,
                        new_tag,
                    } => {
                        let channel = new_tx.as_ref().map(|&(_, _, ref k)| {
                            let mut tx = None;
                            // The `UpdateEgress` message can race with the channel
                            // coordinator finding out about a parent domain. Thus, we need to
                            // spin here to ensure that the parent is indeed connected.
                            while tx.is_none() {
                                tx = self.channel_coordinator.get_tx(k);
                            }
                            tx.unwrap()
                        });
                        let mut n = self.nodes[&node].borrow_mut();
                        n.with_egress_mut(move |e| {
                            if let (Some(new_tx), Some(channel)) = (new_tx, channel) {
                                e.add_tx(new_tx.0, new_tx.1, channel);
                            }
                            if let Some(new_tag) = new_tag {
                                e.add_tag(new_tag.0, new_tag.1);
                            }
                        });
                    }
                    Packet::UpdateSharder { node, new_txs } => {
                        let new_channels: Vec<_> = new_txs
                            .1
                            .iter()
                            .filter_map(|ntx| self.channel_coordinator.get_tx(ntx))
                            .collect();
                        let mut n = self.nodes[&node].borrow_mut();
                        n.with_sharder_mut(move |s| {
                            s.add_sharded_child(new_txs.0, new_channels);
                        });
                    }
                    Packet::AddStreamer { node, new_streamer } => {
                        let mut n = self.nodes[&node].borrow_mut();
                        n.with_reader_mut(|r| r.add_streamer(new_streamer).unwrap());
                    }
                    Packet::StateSizeProbe { node } => {
                        let size = self.state.get(&node).map(|state| state.len()).unwrap_or(0);
                        self.control_reply_tx
                            .send(ControlReplyPacket::StateSize(size))
                            .unwrap();
                    }
                    Packet::PrepareState { node, state } => {
                        use flow::payload::InitialState;
                        match state {
                            InitialState::PartialLocal(key) => {
                                let mut state = State::default();
                                state.add_key(&[key], true);
                                self.state.insert(node, state);
                            }
                            InitialState::IndexedLocal(index) => {
                                let mut state = State::default();
                                for idx in index {
                                    state.add_key(&idx[..], false);
                                }
                                self.state.insert(node, state);
                            }
                            InitialState::PartialGlobal {
                                gid,
                                cols,
                                key,
                                tag,
                                trigger_domain: (trigger_domain, shards),
                            } => {
                                use backlog;
                                let txs = Mutex::new(
                                    (0..shards)
                                        .map(|shard| {
                                            self.channel_coordinator
                                                .get_unbounded_tx(&(trigger_domain, shard))
                                                .unwrap()
                                        })
                                        .collect::<Vec<_>>(),
                                );
                                let (r_part, w_part) =
                                    backlog::new_partial(cols, key, move |key| {
                                        let mut txs = txs.lock().unwrap();
                                        let tx = if txs.len() == 1 {
                                            &mut txs[0]
                                        } else {
                                            let n = txs.len();
                                            &mut txs[::shard_by(key, n)]
                                        };
                                        tx.send(box Packet::RequestPartialReplay {
                                            key: vec![key.clone()],
                                            tag: tag,
                                        }).unwrap();
                                    });
                                assert!(
                                    self.readers
                                        .lock()
                                        .unwrap()
                                        .insert((gid, *self.shard.as_ref().unwrap_or(&0)), r_part)
                                        .is_none()
                                );

                                let mut n = self.nodes[&node].borrow_mut();
                                n.with_reader_mut(|r| {
                                    // make sure Reader is actually prepared to receive state
                                    r.set_write_handle(w_part)
                                });
                            }
                            InitialState::Global { gid, cols, key } => {
                                use backlog;
                                let (r_part, w_part) = backlog::new(cols, key);
                                assert!(
                                    self.readers
                                        .lock()
                                        .unwrap()
                                        .insert((gid, *self.shard.as_ref().unwrap_or(&0)), r_part)
                                        .is_none()
                                );

                                let mut n = self.nodes[&node].borrow_mut();
                                n.with_reader_mut(|r| {
                                    // make sure Reader is actually prepared to receive state
                                    r.set_write_handle(w_part)
                                });
                            }
                        }
                    }
                    Packet::SetupReplayPath {
                        tag,
                        source,
                        path,
                        notify_done,
                        trigger,
                    } => {
                        // let coordinator know that we've registered the tagged path
                        self.control_reply_tx
                            .send(ControlReplyPacket::ack())
                            .unwrap();

                        if notify_done {
                            info!(self.log,
                                  "told about terminating replay path {:?}",
                                  path;
                                  "tag" => tag.id()
                            );
                        // NOTE: we set self.replaying_to when we first receive a replay with
                        // this tag
                        } else {
                            info!(self.log, "told about replay path {:?}", path; "tag" => tag.id());
                        }

                        use flow::payload;
                        let trigger = match trigger {
                            payload::TriggerEndpoint::None => TriggerEndpoint::None,
                            payload::TriggerEndpoint::Start(v) => TriggerEndpoint::Start(v),
                            payload::TriggerEndpoint::Local(v) => TriggerEndpoint::Local(v),
                            payload::TriggerEndpoint::End(domain, shards) => TriggerEndpoint::End(
                                (0..shards)
                                    .map(|shard| {
                                        self.channel_coordinator
                                            .get_unbounded_tx(&(domain, shard))
                                            .unwrap()
                                    })
                                    .collect(),
                            ),
                        };

                        self.replay_paths.insert(
                            tag,
                            ReplayPath {
                                source,
                                path,
                                notify_done,
                                trigger,
                            },
                        );
                    }
                    Packet::RequestPartialReplay { tag, key } => {
                        if self.already_requested(&tag, &key) {
                            return;
                        }

                        if let ReplayPath {
                            trigger: TriggerEndpoint::End(..),
                            ..
                        } = self.replay_paths[&tag]
                        {
                            // request came in from reader -- forward
                            self.request_partial_replay(tag, key);
                            return;
                        }

                        trace!(self.log,
                               "got replay request";
                               "tag" => tag.id(),
                               "key" => format!("{:?}", key)
                        );
                        self.seed_replay(tag, &key[..], None);
                    }
                    Packet::StartReplay { tag, from } => {
                        assert_eq!(self.replay_paths[&tag].source, Some(from));

                        let start = time::Instant::now();
                        info!(self.log, "starting replay");

                        // we know that the node is materialized, as the migration coordinator
                        // picks path that originate with materialized nodes. if this weren't the
                        // case, we wouldn't be able to do the replay, and the entire migration
                        // would fail.
                        //
                        // we clone the entire state so that we can continue to occasionally
                        // process incoming updates to the domain without disturbing the state that
                        // is being replayed.
                        let state: State = self.state
                            .get(&from)
                            .expect("migration replay path started with non-materialized node")
                            .clone();

                        debug!(self.log,
                               "current state cloned for replay";
                               "μs" => dur_to_ns!(start.elapsed()) / 1000
                        );

                        let m = box Packet::FullReplay {
                            link: Link::new(from, self.replay_paths[&tag].path[0].0),
                            tag: tag,
                            state: state,
                        };

                        self.handle_replay(m);
                    }
                    Packet::Finish(tag, ni) => {
                        self.finish_replay(tag, ni);
                    }
                    Packet::Ready { node, index } => {
                        if let DomainMode::Forwarding = self.mode {
                        } else {
                            unreachable!();
                        }

                        if !index.is_empty() {
                            let mut s = {
                                let n = self.nodes[&node].borrow();
                                if n.is_internal() && n.get_base().is_some() {
                                    State::base()
                                } else {
                                    State::default()
                                }
                            };
                            for idx in index {
                                s.add_key(&idx[..], false);
                            }
                            assert!(self.state.insert(node, s).is_none());
                        } else {
                            // NOTE: just because index_on is None does *not* mean we're not
                            // materialized
                        }

                        if self.not_ready.remove(&node) {
                            trace!(self.log, "readying empty node"; "local" => node.id());
                        }

                        // swap replayed reader nodes to expose new state
                        {
                            let mut n = self.nodes[&node].borrow_mut();
                            if n.is_reader() {
                                n.with_reader_mut(
                                    |r| if let Some(ref mut state) = r.writer_mut() {
                                        trace!(self.log, "swapping state"; "local" => node.id());
                                        state.swap();
                                        trace!(self.log, "state swapped"; "local" => node.id());
                                    },
                                );
                            }
                        }

                        self.control_reply_tx
                            .send(ControlReplyPacket::ack())
                            .unwrap();
                    }
                    Packet::GetStatistics => {
                        let domain_stats = statistics::DomainStats {
                            total_time: self.total_time.num_nanoseconds(),
                            total_ptime: self.total_ptime.num_nanoseconds(),
                            wait_time: self.wait_time.num_nanoseconds(),
                        };

                        let node_stats = self.nodes
                            .values()
                            .filter_map(|nd| {
                                let ref n = *nd.borrow();
                                let local_index: LocalNodeIndex = *n.local_addr();
                                let node_index: NodeIndex = n.global_addr();

                                let time = self.process_times.num_nanoseconds(local_index);
                                let ptime = self.process_ptimes.num_nanoseconds(local_index);
                                if time.is_some() && ptime.is_some() {
                                    Some((
                                        node_index,
                                        statistics::NodeStats {
                                            process_time: time.unwrap(),
                                            process_ptime: ptime.unwrap(),
                                        },
                                    ))
                                } else {
                                    None
                                }
                            })
                            .collect();

                        self.control_reply_tx
                            .send(ControlReplyPacket::Statistics(domain_stats, node_stats))
                            .unwrap();
                    }
                    Packet::Captured => {
                        unreachable!("captured packets should never be sent around")
                    }
                    Packet::Quit => unreachable!("Quit messages are handled by event loop"),
                    _ => unreachable!(),
                }
            }
        }
    }

    fn seed_replay(
        &mut self,
        tag: Tag,
        key: &[DataType],
        transaction_state: Option<ReplayTransactionState>,
    ) {
        if transaction_state.is_none() {
            if let ReplayPath {
                source: Some(source),
                trigger: TriggerEndpoint::Start(..),
                ..
            } = self.replay_paths[&tag]
            {
                if self.nodes[&source].borrow().is_transactional() {
                    self.transaction_state.schedule_replay(tag, key.into());
                    self.process_transactions();
                    return;
                }
            }
        }

        let (m, source, is_miss) = match self.replay_paths[&tag] {
            ReplayPath {
                source: Some(source),
                trigger: TriggerEndpoint::Start(ref cols),
                ref path,
                ..
            } |
            ReplayPath {
                source: Some(source),
                trigger: TriggerEndpoint::Local(ref cols),
                ref path,
                ..
            } => {
                let rs = self.state
                    .get(&source)
                    .expect("migration replay path started with non-materialized node")
                    .lookup(&cols[..], &KeyType::Single(&key[0]));

                if let LookupResult::Some(rs) = rs {
                    use std::iter::FromIterator;
                    let m = Some(box Packet::ReplayPiece {
                        link: Link::new(source, path[0].0),
                        tag: tag,
                        nshards: 1,
                        context: ReplayPieceContext::Partial {
                            for_key: Vec::from(key),
                            ignore: false,
                        },
                        data: Records::from_iter(rs.into_iter().map(|r| (**r).clone())),
                        transaction_state: transaction_state,
                    });
                    (m, source, false)
                } else if transaction_state.is_some() {
                    // we need to forward a ReplayPiece for the timestamp we claimed
                    let m = Some(box Packet::ReplayPiece {
                        link: Link::new(source, path[0].0),
                        tag: tag,
                        nshards: 1,
                        context: ReplayPieceContext::Partial {
                            for_key: Vec::from(key),
                            ignore: true,
                        },
                        data: Records::default(),
                        transaction_state: transaction_state,
                    });
                    (m, source, true)
                } else {
                    (None, source, true)
                }
            }
            _ => unreachable!(),
        };

        if is_miss {
            // we have missed in our lookup, so we have a partial replay through a partial replay
            // trigger a replay to source node, and enqueue this request.
            self.on_replay_miss(source, Vec::from(key), tag);
            trace!(self.log,
                   "missed during replay request";
                   "tag" => tag.id(),
                   "key" => ?key);
        } else {
            trace!(self.log,
                   "satisfied replay request";
                   "tag" => tag.id(),
                   //"data" => ?m.as_ref().unwrap().data(),
                   "key" => ?key,
            );
        }

        if let Some(m) = m {
            self.handle_replay(m);
        }
    }

    fn handle_replay(&mut self, m: Box<Packet>) {
        let tag = m.tag().unwrap();
        let mut finished = None;
        let mut playback = None;
        let mut need_replay = None;
        let mut finished_partial = false;
        'outer: loop {
            // this loop is just here so we have a way of giving up the borrow of self.replay_paths

            let &mut ReplayPath {
                ref path,
                notify_done,
                ref trigger,
                ..
            } = self.replay_paths.get_mut(&tag).unwrap();

            match self.mode {
                DomainMode::Forwarding if notify_done => {
                    // this is the first message we receive for this tagged replay path. only at
                    // this point should we start buffering messages for the target node. since the
                    // node is not yet marked ready, all previous messages for this node will
                    // automatically be discarded by dispatch(). the reason we should ignore all
                    // messages preceeding the first replay message is that those have already been
                    // accounted for in the state we are being replayed. if we buffered them and
                    // applied them after all the state has been replayed, we would double-apply
                    // those changes, which is bad.
                    self.mode = DomainMode::Replaying {
                        to: path.last().unwrap().0,
                        buffered: VecDeque::new(),
                        passes: 0,
                    };
                }
                DomainMode::Forwarding => {
                    // we're replaying to forward to another domain
                }
                DomainMode::Replaying { .. } => {
                    // another packet the local state we are constructing
                }
            }

            // we may be able to just absorb all the state in one go if we're lucky!
            let mut can_handle_directly = path.len() == 1;
            if can_handle_directly {
                // unfortunately, if this is a reader node, we can't just copy in the state
                // since State and Reader use different internal data structures
                // TODO: can we do better?
                let n = self.nodes[&path[0].0].borrow();
                if n.is_reader() {
                    can_handle_directly = false;
                }
            }

            if can_handle_directly {
                let n = self.nodes[&path[0].0].borrow();
                if n.is_internal() && n.get_base().map(|b| b.is_unmodified()) == Some(false) {
                    // also need to include defaults for new columns
                    can_handle_directly = false;
                }
            }

            // if the key columns of the state and the target state differ, we cannot use the
            // state directly, even if it is otherwise suitable. Note that we need to check
            // `can_handle_directly` again here because it will have been changed for reader
            // nodes above, and this check only applies to non-reader nodes.
            if can_handle_directly && notify_done {
                if let box Packet::FullReplay { ref state, .. } = m {
                    let local_pkey = self.state[&path[0].0].keys();
                    if local_pkey != state.keys() {
                        debug!(self.log,
                           "cannot use state directly, so falling back to regular replay";
                           "node" => %path[0].0,
                           "src keys" => ?state.keys(),
                           "dst keys" => ?local_pkey);
                        can_handle_directly = false;
                    }
                }
            }

            // TODO: if StateCopy debug_assert!(last);
            // TODO
            // we've been given a state dump, and only have a single node in this domain that needs
            // to deal with that dump. chances are, we'll be able to re-use that state wholesale.

            if let box Packet::ReplayPiece {
                context: ReplayPieceContext::Partial { ignore: true, .. },
                ..
            } = m
            {
                let mut n = self.nodes[&path.last().unwrap().0].borrow_mut();
                if n.is_egress() && n.is_transactional() {
                    // We need to propagate this replay even though it contains no data, so that
                    // downstream domains don't wait for its timestamp.  There is no need to set
                    // link src/dst since the egress node will not use them.
                    let mut m = Some(m);
                    n.process(
                        &mut m,
                        None,
                        &mut self.state,
                        &self.nodes,
                        self.shard,
                        false,
                    );
                }
                break;
            }

            // will look somewhat nicer with https://github.com/rust-lang/rust/issues/15287
            let m = *m; // workaround for #16223
            match m {
                Packet::FullReplay { tag, link, state } => {
                    if can_handle_directly && notify_done {
                        // oh boy, we're in luck! we're replaying into one of our nodes, and were
                        // just given the entire state. no need to process or anything, just move
                        // in the state and we're done.
                        let node = path[0].0;
                        debug!(self.log, "absorbing state clone"; "node" => %node);
                        assert_eq!(self.state[&node].keys(), state.keys());
                        self.state.insert(node, state);
                        debug!(self.log, "direct state clone absorbed");
                        finished = Some((tag, node, None));
                    } else if can_handle_directly {
                        // if we're not terminal, and the domain only has a single node, that node
                        // *has* to be an egress node (since we're relaying to another domain).
                        let node = path[0].0;
                        let mut n = self.nodes[&node].borrow_mut();
                        if n.is_egress() {
                            // forward the state to the next domain without doing anything with it.
                            let mut p = Some(box Packet::FullReplay {
                                tag: tag,
                                link: link, // the egress node will fix this up
                                state: state,
                            });
                            debug!(self.log, "doing bulk egress forward");
                            n.process(
                                &mut p,
                                None,
                                &mut self.state,
                                &self.nodes,
                                self.shard,
                                false,
                            );
                            debug!(self.log, "bulk egress forward completed");
                            drop(n);
                        } else {
                            unreachable!();
                        }
                    } else if state.is_empty() {
                        // TODO: eliminate this branch by detecting at the source
                        //
                        // we're been given an entire state snapshot, which needs to be replayed
                        // row by row, *but* it's empty. fun fact: creating a chunked iterator over
                        // an empty hashmap yields *no* chunks, which *also* means that an update
                        // with last=true is never sent, which means that the replay never
                        // finishes. so, we deal with this case separately (and also avoid spawning
                        // a thread to walk empty state).
                        let p = box Packet::ReplayPiece {
                            tag: tag,
                            link: link,
                            nshards: self.nshards,
                            context: ReplayPieceContext::Regular { last: true },
                            data: Records::default(),
                            transaction_state: None,
                        };

                        debug!(self.log, "empty full state replay conveyed");
                        playback = Some(p);
                    } else {
                        use std::thread;

                        // we're been given an entire state snapshot, but we need to digest it
                        // piece by piece spawn off a thread to do that chunking. however, before
                        // we spin off that thread, we need to send a single Replay message to tell
                        // the target domain to start buffering everything that follows. we can't
                        // do that inside the thread, because by the time that thread is scheduled,
                        // we may already have processed some other messages that are not yet a
                        // part of state.
                        let p = box Packet::ReplayPiece {
                            tag: tag,
                            link: link.clone(),
                            nshards: self.nshards,
                            context: ReplayPieceContext::Regular { last: false },
                            data: Vec::<Record>::new().into(),
                            transaction_state: None,
                        };
                        playback = Some(p);

                        let log = self.log.new(o!());
                        let nshards = self.nshards;
                        let domain_addr = self.addr;
                        thread::Builder::new()
                            .name(format!(
                                "replay{}.{}",
                                self.nodes
                                    .values()
                                    .next()
                                    .unwrap()
                                    .borrow()
                                    .domain()
                                    .index(),
                                link.src
                            ))
                            .spawn(move || {
                                use itertools::Itertools;

                                let mut chunked_replay_tx =
                                    TcpSender::connect(&domain_addr, Some(1)).unwrap();

                                let start = time::Instant::now();
                                debug!(log, "starting state chunker"; "node" => %link.dst);

                                let iter = state
                                    .into_iter()
                                    .flat_map(|rs| rs)
                                    .map(|rs| (*rs).clone())
                                    .chunks(BATCH_SIZE);
                                let mut iter = iter.into_iter().enumerate().peekable();

                                // process all records in state to completion within domain
                                // and then forward on tx (if there is one)
                                while let Some((i, chunk)) = iter.next() {
                                    use std::iter::FromIterator;
                                    let chunk = Records::from_iter(chunk.into_iter());
                                    let len = chunk.len();
                                    let last = iter.peek().is_none();
                                    let p = box Packet::ReplayPiece {
                                        tag: tag,
                                        link: link.clone(), // to will be overwritten by receiver
                                        nshards: nshards,
                                        context: ReplayPieceContext::Regular { last },
                                        data: chunk,
                                        transaction_state: None,
                                    };

                                    trace!(log, "sending batch"; "#" => i, "[]" => len);
                                    if chunked_replay_tx.send(p).is_err() {
                                        warn!(log, "replayer noticed domain shutdown");
                                        break;
                                    }
                                }

                                debug!(log,
                                   "state chunker finished";
                                   "node" => %link.dst,
                                   "μs" => dur_to_ns!(start.elapsed()) / 1000
                                );
                            })
                            .unwrap();
                    }
                }
                Packet::ReplayPiece {
                    tag,
                    link,
                    data,
                    nshards,
                    context,
                    transaction_state,
                } => {
                    if let ReplayPieceContext::Partial { .. } = context {
                        trace!(self.log, "replaying batch"; "#" => data.len(), "tag" => tag.id());
                    } else {
                        debug!(self.log, "replaying batch"; "#" => data.len());
                    }

                    let mut is_transactional = transaction_state.is_some();

                    // forward the current message through all local nodes.
                    let m = box Packet::ReplayPiece {
                        link: link.clone(),
                        tag,
                        data,
                        nshards,
                        context: context.clone(),
                        transaction_state: transaction_state.clone(),
                    };
                    let mut m = Some(m);

                    // keep track of whether we're filling any partial holes
                    let partial_key =
                        if let ReplayPieceContext::Partial { ref for_key, .. } = context {
                            Some(for_key)
                        } else {
                            None
                        };

                    for (i, &(ref ni, keyed_by)) in path.iter().enumerate() {
                        let mut n = self.nodes[&ni].borrow_mut();
                        let is_reader = n.with_reader(|r| r.is_materialized()).unwrap_or(false);

                        if !n.is_transactional() {
                            if let Some(box Packet::ReplayPiece {
                                ref mut transaction_state,
                                ..
                            }) = m
                            {
                                // Transactional replays that cross into non-transactional subgraphs
                                // should stop being transactional. This is necessary to ensure that
                                // they don't end up being buffered, and thus re-ordered relative to
                                // subsequent writes to the same key.
                                transaction_state.take();
                                is_transactional = false;
                            } else {
                                unreachable!();
                            }
                        }

                        // figure out if we're the target of a partial replay.
                        // this is the case either if the current node is waiting for a replay,
                        // *or* if the target is a reader. the last case is special in that when a
                        // client requests a replay, the Reader isn't marked as "waiting".
                        let target =
                            partial_key.is_some() && (is_reader || self.waiting.contains_key(&ni));

                        // targets better be last
                        assert!(!target || i == path.len() - 1);

                        // are we about to fill a hole?
                        if target {
                            let partial_key = partial_key.unwrap();
                            // mark the state for the key being replayed as *not* a hole otherwise
                            // we'll just end up with the same "need replay" response that
                            // triggered this replay initially.
                            if let Some(state) = self.state.get_mut(&ni) {
                                state.mark_filled(partial_key.clone());
                            } else {
                                n.with_reader_mut(|r| {
                                    // we must be filling a hole in a Reader. we need to ensure
                                    // that the hole for the key we're replaying ends up being
                                    // filled, even if that hole is empty!
                                    r.writer_mut().map(|wh| wh.mark_filled(&partial_key[0]));
                                });
                            }
                        }

                        // process the current message in this node
                        let mut misses = n.process(
                            &mut m,
                            keyed_by,
                            &mut self.state,
                            &self.nodes,
                            self.shard,
                            false,
                        );

                        if target {
                            let hole_filled = if let Some(box Packet::Captured) = m {
                                // the node captured our replay. in the latter case, there is
                                // nothing more for us to do. it will eventually release, and then
                                // all the other things will happen. for now though, we need to
                                // reset the hole we opened up. crucially though, the hole was
                                // *not* filled.
                                false
                            } else {
                                // we produced some output, but did we also miss?
                                // if we did, we don't want to consider the hole filled.
                                misses.is_empty()
                            };

                            let partial_key = partial_key.unwrap();
                            if !hole_filled {
                                if let Some(state) = self.state.get_mut(&ni) {
                                    state.mark_hole(&partial_key[..]);
                                } else {
                                    n.with_reader_mut(|r| {
                                        r.writer_mut().map(|wh| wh.mark_hole(&partial_key[0]));
                                    });
                                }
                            } else if is_reader {
                                // we filled a hole! swap the reader.
                                n.with_reader_mut(|r| {
                                    r.writer_mut().map(|wh| wh.swap());
                                });
                                // and also unmark the replay request
                                if let Some(ref mut prev) = self.reader_triggered.get_mut(&ni) {
                                    prev.remove(&partial_key[0]);
                                }
                            }
                        }

                        // we're done with the node
                        let is_shard_merger = n.is_shard_merger();
                        drop(n);

                        if let Some(box Packet::Captured) = m {
                            if partial_key.is_some() && is_transactional {
                                let last_ni = path.last().unwrap().0;
                                if last_ni != *ni {
                                    let mut n = self.nodes[&last_ni].borrow_mut();
                                    if n.is_egress() && n.is_transactional() {
                                        // The partial replay was captured, but we still need to
                                        // propagate an (ignored) ReplayPiece so that downstream
                                        // domains don't end up waiting forever for the timestamp we
                                        // claimed.
                                        let m = box Packet::ReplayPiece {
                                            link: link, // TODO: use dummy link instead
                                            tag,
                                            data: Vec::<Record>::new().into(),
                                            nshards: self.nshards,
                                            context: ReplayPieceContext::Partial {
                                                for_key: partial_key.unwrap().clone(),
                                                ignore: true,
                                            },
                                            transaction_state,
                                        };
                                        // No need to set link src/dst since the egress node will
                                        // not use them.
                                        let mut m = Some(m);
                                        n.process(
                                            &mut m,
                                            None,
                                            &mut self.state,
                                            &self.nodes,
                                            self.shard,
                                            false,
                                        );
                                    }
                                }
                            }

                            // it's been captured, so we need to *not* consider the replay finished
                            // (which the logic below matching on context would do)
                            break 'outer;
                        }

                        // if we missed during replay, we need to do a replay
                        if partial_key.is_some() && !misses.is_empty() {
                            // replays are always for just one key
                            assert!(misses.iter().all(|miss| {
                                miss.node == misses[0].node && miss.key == misses[0].key
                            }));
                            let miss = misses.swap_remove(0);
                            need_replay = Some((miss.node, miss.key, tag));
                            if let TriggerEndpoint::End(..) = *trigger {
                                finished_partial = true;
                            }
                            break 'outer;
                        }

                        // we're all good -- continue propagating
                        if m.as_ref().map(|m| m.is_empty()).unwrap_or(true) {
                            if let ReplayPieceContext::Regular { last: false } = context {
                                // don't continue processing empty updates, *except* if this is the
                                // last replay batch. in that case we need to send it so that the
                                // next domain knows that we're done
                                // TODO: we *could* skip ahead to path.last() here
                                break;
                            }
                        }

                        if i != path.len() - 1 {
                            // update link for next iteration
                            if is_shard_merger {
                                // we need to preserve the egress src
                                // (which includes shard identifier)
                            } else {
                                m.as_mut().unwrap().link_mut().src = *ni;
                            }
                            m.as_mut().unwrap().link_mut().dst = path[i + 1].0;
                        }
                    }

                    let dst = path.last().unwrap().0;
                    match context {
                        ReplayPieceContext::Regular { last } if last => {
                            debug!(self.log,
                                   "last batch processed";
                                   "terminal" => notify_done
                            );
                            if notify_done {
                                debug!(self.log, "last batch received"; "local" => dst.id());
                                finished = Some((tag, dst, None));
                            }
                        }
                        ReplayPieceContext::Regular { .. } => {
                            debug!(self.log, "batch processed");
                        }
                        ReplayPieceContext::Partial { for_key, ignore } => {
                            assert!(!ignore);
                            if self.waiting.contains_key(&dst) {
                                trace!(self.log, "partial replay completed"; "local" => dst.id());
                                finished = Some((tag, dst, Some(for_key)));
                                finished_partial = true;
                            } else if self.nodes[&dst].borrow().is_reader() {
                                finished_partial = true;
                            } else {
                                // we're just on the replay path
                            }
                        }
                    }
                }
                _ => unreachable!(),
            }
            break;
        }

        if finished_partial {
            self.finished_partial_replay(&tag);
        }

        if let Some((node, key, tag)) = need_replay {
            self.on_replay_miss(node, key, tag);
            return;
        }

        if let Some(p) = playback {
            self.handle(p);
        }

        if let Some((tag, ni, for_key)) = finished {
            if let Some(Waiting { mut subscribed }) = self.waiting.remove(&ni) {
                // we got a partial replay result that we were waiting for. it's time we let any
                // downstream nodes that missed in us on that key know that they can (probably)
                // continue with their replays.
                let for_key = for_key.unwrap();
                subscribed.retain(|subscription| {
                    if for_key != subscription.key {
                        // we didn't fulfill this subscription
                        return true;
                    }

                    // we've filled the hole that prevented the replay previously!
                    self.seed_replay(subscription.tag, &subscription.key[..], None);
                    false
                });

                if !subscribed.is_empty() {
                    // we still have more things waiting on us
                    if let Some(_) = self.waiting.insert(ni, Waiting { subscribed }) {
                        // seed_replay *could* cause us to start waiting again
                        unimplemented!();
                    }
                }
                return;
            }

            assert!(for_key.is_none());

            // NOTE: node is now ready, in the sense that it shouldn't ignore all updates since
            // replaying_to is still set, "normal" dispatch calls will continue to be buffered, but
            // this allows finish_replay to dispatch into the node by overriding replaying_to.
            self.not_ready.remove(&ni);
            // NOTE: inject must be None because it is only set to Some in the main domain loop, it
            // hasn't yet been set in the processing of the current packet, and it can't still be
            // set from a previous iteration because then it would have been dealt with instead of
            // the current packet.
            assert!(self.inject.is_none());
            self.inject = Some(box Packet::Finish(tag, ni));
        }
    }

    fn finish_replay(&mut self, tag: Tag, node: LocalNodeIndex) {
        let finished = if let DomainMode::Replaying {
            ref to,
            ref mut buffered,
            ref mut passes,
        } = self.mode
        {
            if *to != node {
                // we're told to continue replay for node a, but not b is being replayed
                unreachable!();
            }
            // log that we did another pass
            *passes += 1;

            let mut handle = buffered.len();
            if handle > 100 {
                handle /= 2;
            }

            let mut handled = 0;
            while let Some(m) = buffered.pop_front() {
                // some updates were propagated to this node during the migration. we need to
                // replay them before we take even newer updates. however, we don't want to
                // completely block the domain data channel, so we only process a few backlogged
                // updates before yielding to the main loop (which might buffer more things).

                if let m @ box Packet::Message { .. } = m {
                    // NOTE: we cannot use self.dispatch_ here, because we specifically need to
                    // override the buffering behavior that our self.replaying_to = Some above would
                    // initiate.
                    Self::dispatch(
                        m,
                        &self.not_ready,
                        &mut DomainMode::Forwarding,
                        &mut self.waiting,
                        &mut self.state,
                        &self.nodes,
                        self.shard,
                        &mut self.replay_paths,
                        &mut self.process_times,
                        &mut self.process_ptimes,
                        true,
                    );
                } else {
                    // no transactions allowed here since we're still in a migration
                    unreachable!();
                }

                handled += 1;
                if handled == handle {
                    // we want to make sure we actually drain the backlog we've accumulated
                    // but at the same time we don't want to completely stall the system
                    // therefore we only handle half the backlog at a time
                    break;
                }
            }

            buffered.is_empty()
        } else {
            // we're told to continue replay, but nothing is being replayed
            unreachable!();
        };

        if finished {
            use std::mem;
            // node is now ready, and should start accepting "real" updates
            if let DomainMode::Replaying { passes, .. } =
                mem::replace(&mut self.mode, DomainMode::Forwarding)
            {
                debug!(self.log,
                       "node is fully up-to-date";
                       "local" => node.id(),
                       "passes" => passes
                );
            } else {
                unreachable!();
            }

            if self.replay_paths[&tag].notify_done {
                // NOTE: this will only be Some for non-partial replays
                info!(self.log, "acknowledging replay completed"; "node" => node.id());
                self.control_reply_tx
                    .send(ControlReplyPacket::ack())
                    .unwrap();
            } else {
                unreachable!()
            }
        } else {
            // we're not done -- inject a request to continue handling buffered things
            // NOTE: similarly to in handle_replay, inject can never be Some before this function
            // because it is called directly from handle, which is always called with inject being
            // None.
            assert!(self.inject.is_none());
            self.inject = Some(box Packet::Finish(tag, node));
        }
    }

    pub fn run(mut self, mut polling_loop: PollingLoop<Box<Packet>>) {
        let mut group_commit_queues = persistence::GroupCommitQueueSet::new(
            self.index,
            self.shard.unwrap_or(0),
            &self.persistence_parameters,
            self.transaction_state.get_checktable().clone(),
        );

        // Run polling loop.
        self.total_time.start();
        self.total_ptime.start();
        polling_loop.run_polling_loop(|event| match event {
            PollEvent::ResumePolling(timeout) => {
                *timeout = group_commit_queues.duration_until_flush();
                KeepPolling
            }
            PollEvent::Process(packet) => {
                if let Packet::Quit = *packet {
                    return StopPolling;
                }

                // TODO: Initialize tracer here, and when flushing group commit
                // queue.
                if group_commit_queues.should_append(&packet, &self.nodes) {
                    debug_assert!(packet.is_regular());
                    packet.trace(PacketEvent::ExitInputChannel);
                    let merged_packet = group_commit_queues.append(packet, &self.nodes);
                    if let Some(packet) = merged_packet {
                        self.handle(packet);
                    }
                } else {
                    self.handle(packet);
                }

                while let Some(p) = self.inject.take() {
                    self.handle(p);
                }

                KeepPolling
            }
            PollEvent::Timeout => {
                if let Some(m) = group_commit_queues.flush_if_necessary(&self.nodes) {
                    self.handle(m);
                    while let Some(p) = self.inject.take() {
                        self.handle(p);
                    }
                }
                KeepPolling
            }
        });
    }
}
