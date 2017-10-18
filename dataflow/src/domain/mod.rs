use petgraph::graph::NodeIndex;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time;
use std::io::{BufRead, BufReader};
use std::fs::File;
use std::path::PathBuf;
use std::collections::hash_map::Entry;
use std::rc::Rc;

use std::net::SocketAddr;

use Readers;
use channel::TcpSender;
use channel::poll::{KeepPolling, PollEvent, PollingLoop, StopPolling};
use prelude::*;
use payload::{ControlReplyPacket, ReplayPieceContext, ReplayTransactionState, TransactionState};
use statistics;
use transactions;
use persistence;
use debug;
use checktable;
use serde_json;
use slog::Logger;
use timekeeper::{RealTime, SimpleTracker, ThreadTime, Timer, TimerSet};
use tarpc::sync::client::{self, ClientExt};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Config {
    pub concurrent_replays: usize,
    pub replay_batch_timeout: time::Duration,
    pub replay_batch_size: usize,
}

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

#[derive(Debug)]
enum DomainMode {
    Forwarding,
    Replaying {
        to: LocalNodeIndex,
        buffered: VecDeque<Box<Packet>>,
        passes: usize,
    },
}

impl PartialEq for DomainMode {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (&DomainMode::Forwarding, &DomainMode::Forwarding) => true,
            _ => false,
        }
    }
}

enum TriggerEndpoint {
    None,
    Start(Vec<usize>),
    End(Vec<TcpSender<Box<Packet>>>),
    Local(Vec<usize>),
}

struct ReplayPath {
    source: Option<LocalNodeIndex>,
    path: Vec<ReplayPathSegment>,
    notify_done: bool,
    trigger: TriggerEndpoint,
}

type Hole = (usize, DataType);
type Redo = (Tag, DataType);
/// When a replay misses while being processed, it triggers a replay to backfill the hole that it
/// missed in. We need to ensure that when this happens, we re-run the original replay to fill the
/// hole we *originally* were trying to fill.
///
/// This comes with some complexity:
///
///  - If two replays both hit the *same* hole, we should only request a backfill of it once, but
///    need to re-run *both* replays when the hole is filled.
///  - If one replay hits two *different* holes, we should backfill both holes, but we must ensure
///    that we only re-run the replay once when both holes have been filled.
///
/// To keep track of this, we use the `Waiting` structure below. One is created for every node with
/// at least one outstanding backfill, and contains the necessary bookkeeping to ensure the two
/// behaviors outlined above.
///
/// Note that in the type aliases above, we have chosen to use Vec<usize> instead of Tag to
/// identify a hole. This is because there may be more than one Tag used to fill a given hole, and
/// the set of columns uniquely identifies the set of tags.
#[derive(Debug, Default)]
struct Waiting {
    /// For each eventual redo, how many holes are we waiting for?
    holes: HashMap<Redo, usize>,
    /// For each hole, which redos do we expect we'll have to do?
    redos: HashMap<Hole, HashSet<Redo>>,
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
    /// Configuration parameters for the domain.
    pub config: Config,
}

impl DomainBuilder {
    /// Starts up the domain represented by this `DomainBuilder`.
    pub fn boot(
        self,
        log: Logger,
        readers: Readers,
        channel_coordinator: Arc<ChannelCoordinator>,
        listen_addr: SocketAddr,
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
        let polling_loop = PollingLoop::<Packet>::new(listen_addr);
        // We extract this here because `listen_addr` may not specify a port and rely on
        // auto-assignment
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
                    waiting: Default::default(),
                    reader_triggered: Default::default(),
                    replay_paths: Default::default(),

                    ingress_inject: Default::default(),

                    addr,
                    readers,
                    inject: None,
                    _debug_tx: debug_tx,
                    control_reply_tx,
                    channel_coordinator,

                    buffered_replay_requests: Default::default(),
                    has_buffered_replay_requests: false,
                    replay_batch_size: self.config.replay_batch_size,
                    replay_batch_timeout: self.config.replay_batch_timeout,

                    concurrent_replays: 0,
                    max_concurrent_replays: self.config.concurrent_replays,
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

    ingress_inject: local::Map<(usize, Vec<DataType>)>,

    transaction_state: transactions::DomainState,
    persistence_parameters: persistence::Parameters,

    mode: DomainMode,
    waiting: local::Map<Waiting>,
    replay_paths: HashMap<Tag, ReplayPath>,
    reader_triggered: local::Map<HashSet<DataType>>,

    concurrent_replays: usize,
    max_concurrent_replays: usize,
    replay_request_queue: VecDeque<(Tag, Vec<DataType>)>,

    addr: SocketAddr,
    readers: Readers,
    inject: Option<Box<Packet>>,
    _debug_tx: Option<TcpSender<debug::DebugEvent>>,
    control_reply_tx: TcpSender<ControlReplyPacket>,
    channel_coordinator: Arc<ChannelCoordinator>,

    buffered_replay_requests: HashMap<Tag, (time::Instant, HashSet<Vec<DataType>>)>,
    has_buffered_replay_requests: bool,
    replay_batch_timeout: time::Duration,
    replay_batch_size: usize,

    total_time: Timer<SimpleTracker, RealTime>,
    total_ptime: Timer<SimpleTracker, ThreadTime>,
    wait_time: Timer<SimpleTracker, RealTime>,
    process_times: TimerSet<LocalNodeIndex, SimpleTracker, RealTime>,
    process_ptimes: TimerSet<LocalNodeIndex, SimpleTracker, ThreadTime>,
}

impl Domain {
    fn on_replay_miss(
        &mut self,
        miss_in: LocalNodeIndex,
        miss_columns: &[usize],
        replay_key: Vec<DataType>,
        miss_key: Vec<DataType>,
        needed_for: Tag,
    ) {
        use std::ops::AddAssign;
        use std::collections::hash_map::Entry;

        // when the replay eventually succeeds, we want to re-do the replay.
        let mut w = self.waiting.remove(&miss_in).unwrap_or_default();

        assert_eq!(miss_columns.len(), 1);
        assert_eq!(replay_key.len(), 1);
        assert_eq!(miss_key.len(), 1);

        let mut redundant = false;
        let redo = (needed_for, replay_key[0].clone());
        match w.redos.entry((miss_columns[0], miss_key[0].clone())) {
            Entry::Occupied(e) => {
                // we have already requested backfill of this key
                // remember to notify this Redo when backfill completes
                if e.into_mut().insert(redo.clone()) {
                    // this Redo should wait for this backfill to complete before redoing
                    w.holes.entry(redo).or_default().add_assign(1);
                }
                redundant = true;
            }
            Entry::Vacant(e) => {
                // we haven't already requested backfill of this key
                let mut redos = HashSet::new();
                // remember to notify this Redo when backfill completes
                redos.insert(redo.clone());
                e.insert(redos);
                // this Redo should wait for this backfill to complete before redoing
                w.holes.entry(redo).or_default().add_assign(1);
            }
        }

        self.waiting.insert(miss_in, w);
        if redundant {
            return;
        }

        let mut found = false;
        let tags: Vec<Tag> = self.replay_paths.keys().cloned().collect();
        for tag in tags {
            if let TriggerEndpoint::Start(..) = self.replay_paths[&tag].trigger {
                continue;
            }
            {
                let p = self.replay_paths[&tag].path.last().unwrap();
                if p.node != miss_in {
                    continue;
                }
                assert!(p.partial_key.is_some());
                assert_eq!(miss_columns.len(), 1);
                if p.partial_key.unwrap() != miss_columns[0] {
                    continue;
                }
            }

            // send a message to the source domain(s) responsible
            // for the chosen tag so they'll start replay.
            let key = miss_key.clone(); // :(
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
                found = true;
                continue;
            }

            // NOTE: due to max_concurrent_replays, it may be that we only replay from *some* of
            // these ancestors now, and some later. this will cause more of the replay to be
            // buffered up at the union above us, but that's probably fine.
            self.request_partial_replay(tag, key);
            found = true;
            continue;
        }

        if !found {
            unreachable!(format!(
                "no tag found to fill missing value {:?} in {}.{:?}",
                miss_key,
                miss_in,
                miss_columns
            ));
        }
    }

    fn send_partial_replay_request(&mut self, tag: Tag, key: Vec<DataType>) {
        debug_assert!(self.concurrent_replays < self.max_concurrent_replays);
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
        if self.concurrent_replays < self.max_concurrent_replays {
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

    fn finished_partial_replay(&mut self, tag: &Tag, num: usize) {
        match self.replay_paths[tag].trigger {
            TriggerEndpoint::End(..) => {
                // A backfill request we made to another domain was just satisfied!
                // We can now issue another request from the concurrent replay queue.
                // However, since unions require multiple backfill requests, but produce only one
                // backfill reply, we need to check how many requests we're now free to issue. If
                // we just naively release one slot here, a union with two parents would mean that
                // `self.concurrent_replays` constantly grows by +1 (+2 for the backfill requests,
                // -1 when satisfied), which would lead to a deadlock!
                let mut requests_satisfied = {
                    let last = self.replay_paths[tag].path.last().unwrap();
                    self.replay_paths
                        .iter()
                        .filter(|&(_, p)| {
                            if let TriggerEndpoint::End(..) = p.trigger {
                                let p = p.path.last().unwrap();
                                p.node == last.node && p.partial_key == last.partial_key
                            } else {
                                false
                            }
                        })
                        .count()
                };

                // we also sent that many requests *per key*.
                requests_satisfied *= num;

                // TODO: figure out why this can underflow
                self.concurrent_replays =
                    self.concurrent_replays.saturating_sub(requests_satisfied);
                trace!(self.log, "notified of finished replay";
                       "#done" => requests_satisfied,
                       "ongoing" => self.concurrent_replays,
                       );
                debug_assert!(self.concurrent_replays < self.max_concurrent_replays);
                while self.concurrent_replays < self.max_concurrent_replays {
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
            &box Packet::ReplayPiece { .. } => {
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
                let addr = path.last().unwrap().node;
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
            Packet::ReplayPiece { .. } => {
                self.handle_replay(m);
            }
            Packet::StartRecovery { .. } => {
                self.handle_recovery();
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
                        if n.is_internal() && n.get_base().is_some() {
                            n.get_base_mut().unwrap().add_column(default);
                        } else if n.is_ingress() {
                            self.ingress_inject
                                .entry(node)
                                .or_insert_with(|| (n.fields().len(), Vec::new()))
                                .1
                                .push(default);
                        } else {
                            unreachable!("node unrelated to base got AddBaseColumn");
                        }
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
                            if let (Some(new_tx), Some((channel, is_local))) = (new_tx, channel) {
                                e.add_tx(new_tx.0, new_tx.1, channel, is_local);
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
                        use payload::InitialState;
                        match state {
                            InitialState::PartialLocal(index) => {
                                if !self.state.contains_key(&node) {
                                    self.state.insert(node, State::default());
                                }
                                let state = self.state.get_mut(&node).unwrap();
                                for (key, tags) in index {
                                    info!(self.log, "told to prepare partial state";
                                           "key" => ?key,
                                           "tags" => ?tags);
                                    state.add_key(&key[..], Some(tags));
                                }
                            }
                            InitialState::IndexedLocal(index) => {
                                if !self.state.contains_key(&node) {
                                    self.state.insert(node, State::default());
                                }
                                let state = self.state.get_mut(&node).unwrap();
                                for idx in index {
                                    info!(self.log, "told to prepare full state";
                                           "key" => ?idx);
                                    state.add_key(&idx[..], None);
                                }
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
                                        let mut m = box Packet::RequestPartialReplay {
                                            key: vec![key.clone()],
                                            tag: tag,
                                        };
                                        if tx.1 {
                                            m = m.make_local();
                                        }
                                        tx.0.send(m).unwrap();
                                    });

                                let mut n = self.nodes[&node].borrow_mut();
                                n.with_reader_mut(|r| {
                                    let token_generator = r.token_generator().cloned();
                                    assert!(
                                        self.readers
                                            .lock()
                                            .unwrap()
                                            .insert(
                                                (gid, *self.shard.as_ref().unwrap_or(&0)),
                                                (r_part, token_generator)
                                            )
                                            .is_none()
                                    );

                                    // make sure Reader is actually prepared to receive state
                                    r.set_write_handle(w_part)
                                });
                            }
                            InitialState::Global { gid, cols, key } => {
                                use backlog;
                                let (r_part, w_part) = backlog::new(cols, key);

                                let mut n = self.nodes[&node].borrow_mut();
                                n.with_reader_mut(|r| {
                                    let token_generator = r.token_generator().cloned();
                                    assert!(
                                        self.readers
                                            .lock()
                                            .unwrap()
                                            .insert(
                                                (gid, *self.shard.as_ref().unwrap_or(&0)),
                                                (r_part, token_generator)
                                            )
                                            .is_none()
                                    );

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

                        use payload;
                        let trigger = match trigger {
                            payload::TriggerEndpoint::None => TriggerEndpoint::None,
                            payload::TriggerEndpoint::Start(v) => TriggerEndpoint::Start(v),
                            payload::TriggerEndpoint::Local(v) => TriggerEndpoint::Local(v),
                            payload::TriggerEndpoint::End(domain, shards) => TriggerEndpoint::End(
                                (0..shards)
                                    .map(|shard| {
                                        // TODO: take advantage of local channels for replay paths.
                                        self.channel_coordinator
                                            .get_unbounded_tx(&(domain, shard))
                                            .unwrap()
                                            .0
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
                        if !self.already_requested(&tag, &key) {
                            if let ReplayPath {
                                trigger: TriggerEndpoint::End(..),
                                ..
                            } = self.replay_paths[&tag]
                            {
                                // request came in from reader -- forward
                                self.request_partial_replay(tag, key);
                                return;
                            } else {
                                trace!(self.log,
                               "got replay request";
                               "tag" => tag.id(),
                               "key" => format!("{:?}", key)
                        );
                                self.seed_replay(tag, &key[..], None);
                            }
                        }
                    }
                    Packet::StartReplay { tag, from } => {
                        use std::thread;
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
                        let state = self.state
                            .get(&from)
                            .expect("migration replay path started with non-materialized node")
                            .cloned_records();

                        debug!(self.log,
                               "current state cloned for replay";
                               "μs" => dur_to_ns!(start.elapsed()) / 1000
                        );

                        let link = Link::new(from, self.replay_paths[&tag].path[0].node);

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
                            context: ReplayPieceContext::Regular {
                                last: state.is_empty(),
                            },
                            data: Vec::<Record>::new().into(),
                            transaction_state: None,
                        };

                        if !state.is_empty() {
                            let log = self.log.new(o!());
                            let nshards = self.nshards;
                            let domain_addr = self.addr;

                            let added_cols = self.ingress_inject.get(&from).cloned();
                            let default = {
                                let n = self.nodes[&from].borrow();
                                let mut default = None;
                                if n.is_internal() {
                                    if let Some(b) = n.get_base() {
                                        let mut row = (Vec::new(), true).into();
                                        b.fix(&mut row);
                                        default = Some(row);
                                    }
                                }
                                default
                            };
                            let fix = move |mut r: Vec<DataType>| -> Vec<DataType> {
                                if let Some((start, ref added)) = added_cols {
                                    let rlen = r.len();
                                    r.extend(added.iter().skip(rlen - start).cloned());
                                } else if let Some(ref defaults) = default {
                                    let rlen = r.len();
                                    r.extend(defaults.iter().skip(rlen).cloned());
                                }
                                r
                            };

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

                                    let iter = state.into_iter().chunks(BATCH_SIZE);
                                    let mut iter = iter.into_iter().enumerate().peekable();

                                    // process all records in state to completion within domain
                                    // and then forward on tx (if there is one)
                                    while let Some((i, chunk)) = iter.next() {
                                        use std::iter::FromIterator;
                                        let chunk = Records::from_iter(chunk.into_iter().map(&fix));
                                        let len = chunk.len();
                                        let last = iter.peek().is_none();
                                        let p = box Packet::ReplayPiece {
                                            tag: tag,
                                            link: link.clone(), // to is overwritten by receiver
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

                        self.handle_replay(p);
                    }
                    Packet::Finish(tag, ni) => {
                        self.finish_replay(tag, ni);
                    }
                    Packet::Ready { node, index } => {
                        assert_eq!(self.mode, DomainMode::Forwarding);

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
                                s.add_key(&idx[..], None);
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
                                n.with_reader_mut(|r| {
                                    if let Some(ref mut state) = r.writer_mut() {
                                        trace!(self.log, "swapping state"; "local" => node.id());
                                        state.swap();
                                        trace!(self.log, "state swapped"; "local" => node.id());
                                    }
                                });
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
                    Packet::Spin => {
                        // spinning as instructed
                    }
                    _ => unreachable!(),
                }
            }
        }

        if self.has_buffered_replay_requests {
            let now = time::Instant::now();
            let to = self.replay_batch_timeout;
            self.has_buffered_replay_requests = false;
            let elapsed_replays: Vec<_> = {
                let has = &mut self.has_buffered_replay_requests;
                self.buffered_replay_requests
                    .iter_mut()
                    .filter_map(|(&tag, &mut (first, ref mut keys))| {
                        if !keys.is_empty() && now.duration_since(first) > to {
                            use std::mem;
                            let l = keys.len();
                            Some((tag, mem::replace(keys, HashSet::with_capacity(l))))
                        } else {
                            if !keys.is_empty() {
                                *has = true;
                            }
                            None
                        }
                    })
                    .collect()
            };
            for (tag, keys) in elapsed_replays {
                self.seed_all(tag, keys);
            }
        }
    }

    fn seed_row(&self, source: LocalNodeIndex, row: &Row<Vec<DataType>>) -> Record {
        if let Some(&(start, ref defaults)) = self.ingress_inject.get(&source) {
            let mut v = Vec::with_capacity(start + defaults.len());
            v.extend(row.iter().cloned());
            v.extend(defaults.iter().cloned());
            return (v, true).into();
        }

        let n = self.nodes[&source].borrow();
        if n.is_internal() {
            if let Some(b) = n.get_base() {
                let mut row = ((*row).clone(), true).into();
                b.fix(&mut row);
                return row;
            }
        }

        return ((*row).clone(), true).into();
    }

    fn seed_all(&mut self, tag: Tag, keys: HashSet<Vec<DataType>>) {
        let (m, source, is_miss) = match self.replay_paths[&tag] {
            ReplayPath {
                source: Some(source),
                trigger: TriggerEndpoint::Start(ref cols),
                ref path,
                ..
            } => {
                let state = self.state
                    .get(&source)
                    .expect("migration replay path started with non-materialized node");

                let mut rs = Vec::new();
                let (keys, misses): (HashSet<_>, _) = keys.into_iter().partition(|key| {
                    match state.lookup(&cols[..], &KeyType::Single(&key[0])) {
                        LookupResult::Some(res) => {
                            rs.extend(res.into_iter().map(|r| self.seed_row(source, r)));
                            true
                        }
                        LookupResult::Missing => false,
                    }
                });

                let m = if !keys.is_empty() {
                    Some(box Packet::ReplayPiece {
                        link: Link::new(source, path[0].node),
                        tag: tag,
                        nshards: 1,
                        context: ReplayPieceContext::Partial {
                            for_keys: keys,
                            ignore: false,
                        },
                        data: rs.into(),
                        transaction_state: None,
                    })
                } else {
                    None
                };

                let miss = if !misses.is_empty() {
                    Some((cols.clone(), misses))
                } else {
                    None
                };

                (m, source, miss)
            }
            _ => unreachable!(),
        };

        if let Some((cols, misses)) = is_miss {
            // we have missed in our lookup, so we have a partial replay through a partial replay
            // trigger a replay to source node, and enqueue this request.
            for key in misses {
                trace!(self.log,
                       "missed during replay request";
                       "tag" => tag.id(),
                       "key" => ?key);
                self.on_replay_miss(source, &cols[..], key.clone(), key, tag);
            }
        }

        if let Some(m) = m {
            if let box Packet::ReplayPiece {
                context: ReplayPieceContext::Partial { ref for_keys, .. },
                ..
            } = m
            {
                trace!(self.log,
                       "satisfied replay request";
                       "tag" => tag.id(),
                       //"data" => ?m.as_ref().unwrap().data(),
                       "keys" => ?for_keys,
                );
            } else {
                unreachable!();
            }

            self.handle_replay(m);
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

                // maybe delay this seed request so that we can batch respond later?
                // TODO
                use std::collections::hash_map::Entry;
                let key = Vec::from(key);
                let full = match self.buffered_replay_requests.entry(tag) {
                    Entry::Occupied(mut o) => {
                        let l = o.get().1.len();
                        if l == self.replay_batch_size - 1 {
                            use std::mem;
                            let mut o =
                                mem::replace(&mut o.get_mut().1, HashSet::with_capacity(l + 1));
                            o.insert(key);
                            Some(o)
                        } else {
                            if l == 0 {
                                o.get_mut().0 = time::Instant::now();
                            }
                            o.into_mut().1.insert(key);
                            self.has_buffered_replay_requests = true;
                            None
                        }
                    }
                    Entry::Vacant(v) => {
                        let mut ks = HashSet::new();
                        ks.insert(key);
                        if self.replay_batch_size == 1 {
                            Some(ks)
                        } else {
                            v.insert((time::Instant::now(), ks));
                            self.has_buffered_replay_requests = true;
                            None
                        }
                    }
                };

                if let Some(all) = full {
                    self.seed_all(tag, all);
                }
                return;
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

                let mut k = HashSet::new();
                k.insert(Vec::from(key));
                if let LookupResult::Some(rs) = rs {
                    use std::iter::FromIterator;
                    let m = Some(box Packet::ReplayPiece {
                        link: Link::new(source, path[0].node),
                        tag: tag,
                        nshards: 1,
                        context: ReplayPieceContext::Partial {
                            for_keys: k,
                            ignore: false,
                        },
                        data: Records::from_iter(rs.into_iter().map(|r| self.seed_row(source, r))),
                        transaction_state: transaction_state,
                    });
                    (m, source, None)
                } else if transaction_state.is_some() {
                    // we need to forward a ReplayPiece for the timestamp we claimed
                    let m = Some(box Packet::ReplayPiece {
                        link: Link::new(source, path[0].node),
                        tag: tag,
                        nshards: 1,
                        context: ReplayPieceContext::Partial {
                            for_keys: k,
                            ignore: true,
                        },
                        data: Records::default(),
                        transaction_state: transaction_state,
                    });
                    (m, source, Some(cols.clone()))
                } else {
                    (None, source, Some(cols.clone()))
                }
            }
            _ => unreachable!(),
        };

        if let Some(cols) = is_miss {
            // we have missed in our lookup, so we have a partial replay through a partial replay
            // trigger a replay to source node, and enqueue this request.
            self.on_replay_miss(source, &cols[..], Vec::from(key), Vec::from(key), tag);
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

    fn handle_recovery(&mut self) {
        let indices = self.nodes
            .iter()
            .map(|(index, _node)| index)
            .collect::<Vec<_>>();

        for index in indices {
            let path = PathBuf::from(&format!(
                "soup-log-{}_{}-{}.json",
                self.index.index(),
                self.shard.unwrap_or(0),
                index.id(),
            ));

            if !path.exists() {
                debug!(
                    self.log,
                    "Could not find file while recovering";
                    "path" => format!("{:?}", path)
                );

                continue;
            }

            debug!(self.log, "Recovering from file"; "path" => format!("{:?}", path));
            let file = File::open(path).unwrap();
            BufReader::new(file)
                .lines()
                .flat_map(|line| {
                    let line = line.unwrap();
                    let records: Vec<Records> = serde_json::from_str(&line).unwrap();
                    records
                })
                .for_each(|records| {
                    let packet = box Packet::Message {
                        data: records,
                        link: Link::new(index, index),
                        tracer: None,
                    };

                    self.handle(packet);
                });
        }

        self.control_reply_tx
            .as_ref()
            .unwrap()
            .send(ControlReplyPacket::ack())
            .unwrap();
    }

    fn handle_replay(&mut self, m: Box<Packet>) {
        let tag = m.tag().unwrap();
        let mut finished = None;
        let mut need_replay = Vec::new();
        let mut finished_partial = 0;
        'outer: loop {
            // this loop is just here so we have a way of giving up the borrow of self.replay_paths

            let &mut ReplayPath {
                ref path,
                notify_done,
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
                        to: path.last().unwrap().node,
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

            if let box Packet::ReplayPiece {
                context: ReplayPieceContext::Partial { ignore: true, .. },
                ..
            } = m
            {
                let mut n = self.nodes[&path.last().unwrap().node].borrow_mut();
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
                Packet::ReplayPiece {
                    tag,
                    link,
                    data,
                    nshards,
                    mut context,
                    transaction_state,
                } => {
                    if let ReplayPieceContext::Partial { ref for_keys, .. } = context {
                        trace!(
                            self.log,
                            "replaying batch";
                            "#" => data.len(),
                            "tag" => tag.id(),
                            "keys" => ?for_keys,
                        );
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

                    // let's collect some informationn about the destination of this replay
                    let dst = path.last().unwrap().node;
                    let dst_is_reader = self.nodes[&dst]
                        .borrow()
                        .with_reader(|r| r.is_materialized())
                        .unwrap_or(false);
                    let dst_is_target = self.waiting.contains_key(&dst);

                    for (i, segment) in path.iter().enumerate() {
                        let mut n = self.nodes[&segment.node].borrow_mut();
                        let is_reader = n.with_reader(|r| r.is_materialized()).unwrap_or(false);

                        // keep track of whether we're filling any partial holes
                        let partial_key_cols = segment.partial_key.as_ref();
                        let backfill_keys = if let ReplayPieceContext::Partial {
                            ref mut for_keys,
                            ..
                        } = context
                        {
                            debug_assert!(partial_key_cols.is_some());
                            Some(for_keys)
                        } else {
                            None
                        };

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
                        let target = backfill_keys.is_some() && i == path.len() - 1
                            && (is_reader || self.waiting.contains_key(&segment.node));

                        // targets better be last
                        assert!(!target || i == path.len() - 1);

                        // are we about to fill a hole?
                        if target {
                            let backfill_keys = backfill_keys.as_ref().unwrap();
                            // mark the state for the key being replayed as *not* a hole otherwise
                            // we'll just end up with the same "need replay" response that
                            // triggered this replay initially.
                            if let Some(state) = self.state.get_mut(&segment.node) {
                                for key in backfill_keys.iter() {
                                    state.mark_filled(key.clone(), &tag);
                                }
                            } else {
                                n.with_reader_mut(|r| {
                                    // we must be filling a hole in a Reader. we need to ensure
                                    // that the hole for the key we're replaying ends up being
                                    // filled, even if that hole is empty!
                                    r.writer_mut().map(|wh| {
                                        for key in backfill_keys.iter() {
                                            wh.mark_filled(&key[0]);
                                        }
                                    });
                                });
                            }
                        }

                        // process the current message in this node
                        let mut misses = n.process(
                            &mut m,
                            segment.partial_key,
                            &mut self.state,
                            &self.nodes,
                            self.shard,
                            false,
                        );

                        // ignore duplicate misses
                        misses.sort_unstable_by(|a, b| {
                            use std::cmp::Ordering;
                            let mut x = a.replay_key.cmp(&b.replay_key);
                            if x != Ordering::Equal {
                                return x;
                            }
                            x = a.columns.cmp(&b.columns);
                            if x != Ordering::Equal {
                                return x;
                            }
                            x = a.key.cmp(&b.key);
                            if x != Ordering::Equal {
                                return x;
                            }
                            a.node.cmp(&b.node)
                        });
                        misses.dedup();

                        let missed_on = if backfill_keys.is_some() {
                            let mut prev = None;
                            let mut missed_on = Vec::with_capacity(misses.len());
                            for miss in &misses {
                                let k = miss.replay_key.as_ref().unwrap();
                                if prev.is_none() || k != prev.unwrap() {
                                    missed_on.push(k.clone());
                                    prev = Some(k);
                                }
                            }
                            missed_on
                        } else {
                            Vec::new()
                        };

                        if target {
                            if !misses.is_empty() {
                                // we missed while processing
                                // it's important that we clear out any partially-filled holes.
                                if let Some(state) = self.state.get_mut(&segment.node) {
                                    for miss in &missed_on {
                                        state.mark_hole(&miss[..], &tag);
                                    }
                                } else {
                                    n.with_reader_mut(|r| {
                                        r.writer_mut().map(|wh| {
                                            for miss in &missed_on {
                                                wh.mark_hole(&miss[0]);
                                            }
                                        });
                                    });
                                }
                            } else if is_reader {
                                // we filled a hole! swap the reader.
                                n.with_reader_mut(|r| {
                                    r.writer_mut().map(|wh| wh.swap());
                                });
                                // and also unmark the replay request
                                if let Some(ref mut prev) =
                                    self.reader_triggered.get_mut(&segment.node)
                                {
                                    for key in backfill_keys.as_ref().unwrap().iter() {
                                        prev.remove(&key[0]);
                                    }
                                }
                            }
                        }

                        // we're done with the node
                        let is_shard_merger = n.is_shard_merger();
                        drop(n);

                        if let Some(box Packet::Captured) = m {
                            assert_eq!(misses.len(), 0);
                            if backfill_keys.is_some() && is_transactional {
                                let last_ni = path.last().unwrap().node;
                                if last_ni != segment.node {
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
                                                for_keys: backfill_keys.unwrap().clone(),
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

                        // we need to track how many replays we completed, and we need to do so
                        // *before* we prune keys that missed. these conditions are all important,
                        // so let's walk through them
                        //
                        //  1. this applies only to partial backfills
                        //  2. we should only set finished_partial if it hasn't already been set.
                        //     this is important, as misses will cause backfill_keys to be pruned
                        //     over time, which would cause finished_partial to hold the wrong
                        //     value!
                        //  3. if the last node on this path is a reader, or is a ::End (so we
                        //     triggered the replay) then we need to decrement the concurrent
                        //     replay count! note that it's *not* sufficient to check if the
                        //     *current* node is a target/reader, because we could miss during a
                        //     join along the path.
                        if backfill_keys.is_some() && finished_partial == 0
                            && (dst_is_reader || dst_is_target)
                        {
                            finished_partial = backfill_keys.as_ref().unwrap().len();
                        }

                        // if we missed during replay, we need to do another replay
                        if backfill_keys.is_some() && !misses.is_empty() {
                            let misses = misses;
                            for miss in misses {
                                need_replay.push((
                                    miss.node,
                                    miss.replay_key.unwrap(),
                                    miss.key,
                                    miss.columns,
                                    tag,
                                ));
                            }

                            // we still need to finish the replays for any keys that *didn't* miss
                            let backfill_keys = backfill_keys.map(|backfill_keys| {
                                backfill_keys.retain(|k| !missed_on.contains(k));
                                backfill_keys
                            });
                            if backfill_keys.as_ref().unwrap().is_empty() {
                                break 'outer;
                            }

                            let partial_col = *partial_key_cols.unwrap();
                            m.as_mut().unwrap().map_data(|rs| {
                                rs.retain(|r| {
                                    // XXX: don't we technically need to translate the columns a
                                    // bunch here? what if two key columns are reordered?
                                    let r = r.rec();
                                    !missed_on.contains(&vec![r[partial_col].clone()])
                                })
                            });
                        }

                        // we're all good -- continue propagating
                        if m.as_ref().map(|m| m.is_empty()).unwrap_or(true) {
                            if let ReplayPieceContext::Regular { last: false } = context {
                                trace!(self.log, "dropping empty non-terminal full replay packet");
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
                                m.as_mut().unwrap().link_mut().src = segment.node;
                            }
                            m.as_mut().unwrap().link_mut().dst = path[i + 1].node;
                        }

                        if let Some(box Packet::ReplayPiece {
                            context: ReplayPieceContext::Regular { last },
                            ..
                        }) = m
                        {
                            if let ReplayPieceContext::Regular {
                                last: ref mut old_last,
                            } = context
                            {
                                *old_last = last;
                            }
                        }
                    }

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
                        ReplayPieceContext::Partial { for_keys, ignore } => {
                            assert!(!ignore);
                            if dst_is_target {
                                trace!(self.log, "partial replay completed"; "local" => dst.id());
                                if finished_partial == 0 {
                                    assert_eq!(for_keys, HashSet::<Vec<DataType>>::new());
                                }
                                finished = Some((tag, dst, Some(for_keys)));
                            } else if dst_is_reader {
                                assert_ne!(finished_partial, 0);
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

        if finished_partial != 0 {
            self.finished_partial_replay(&tag, finished_partial);
        }

        for (node, while_replaying_key, miss_key, miss_cols, tag) in need_replay {
            trace!(self.log,
                   "missed during replay processing";
                   "tag" => tag.id(),
                   "during" => ?while_replaying_key,
                   "missed" => ?miss_key,
                   "on" => %node,
            );
            self.on_replay_miss(node, &miss_cols[..], while_replaying_key, miss_key, tag);
        }

        if let Some((tag, ni, for_keys)) = finished {
            trace!(self.log, "partial replay finished";
                   "node" => ?ni,
                   "keys" => ?for_keys);
            if let Some(mut waiting) = self.waiting.remove(&ni) {
                trace!(
                    self.log,
                    "partial replay finished to node with waiting backfills"
                );

                let key_col = *self.replay_paths[&tag]
                    .path
                    .last()
                    .unwrap()
                    .partial_key
                    .as_ref()
                    .unwrap();

                // we got a partial replay result that we were waiting for. it's time we let any
                // downstream nodes that missed in us on that key know that they can (probably)
                // continue with their replays.
                for mut key in for_keys.unwrap() {
                    assert_eq!(key.len(), 1);
                    let hole = (key_col, key.swap_remove(0));
                    let replay = waiting
                        .redos
                        .remove(&hole)
                        .expect("got backfill for unnecessary key");

                    // we may need more holes to fill before some replays should be re-attempted
                    let replay: Vec<_> = replay
                        .into_iter()
                        .filter_map(|tagged_replay_key| {
                            let left = {
                                let left = waiting.holes.get_mut(&tagged_replay_key).unwrap();
                                *left -= 1;
                                *left
                            };

                            if left == 0 {
                                trace!(self.log, "filled last hole for key, triggering replay";
                                   "k" => ?tagged_replay_key);

                                // we've filled all holes that prevented the replay previously!
                                waiting.holes.remove(&tagged_replay_key);
                                Some(tagged_replay_key)
                            } else {
                                trace!(self.log, "filled hole for key, not triggering replay";
                                   "k" => ?tagged_replay_key,
                                   "left" => left);
                                None
                            }
                        })
                        .collect();

                    if !waiting.holes.is_empty() {
                        // there are still holes, so there must still be pending redos
                        assert!(!waiting.redos.is_empty());

                        // restore Waiting in case seeding triggers more replays
                        self.waiting.insert(ni, waiting);
                    } else {
                        // there are no more holes that are filling, so there can't be more redos
                        assert!(waiting.redos.is_empty());
                    }

                    for (tag, replay_key) in replay {
                        self.seed_replay(tag, &[replay_key], None);
                    }

                    waiting = self.waiting.remove(&ni).unwrap_or_default();
                }

                if !waiting.holes.is_empty() {
                    assert!(!waiting.redos.is_empty());
                    self.waiting.insert(ni, waiting);
                } else {
                    assert!(waiting.redos.is_empty());
                }
                return;
            }

            assert!(for_keys.is_none());

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

    pub fn run(mut self, mut polling_loop: PollingLoop<Packet>) {
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
                *timeout = group_commit_queues.duration_until_flush().or_else(|| {
                    let now = time::Instant::now();
                    self.buffered_replay_requests
                        .iter()
                        .map(|(_, &(first, _))| {
                            self.replay_batch_timeout
                                .checked_sub(now.duration_since(first))
                                .unwrap_or(time::Duration::from_millis(0))
                        })
                        .min()
                });
                KeepPolling
            }
            PollEvent::Process(packet) => {
                let packet = packet.make_boxed_normal();
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
                } else if self.has_buffered_replay_requests {
                    self.handle(box Packet::Spin);
                }
                KeepPolling
            }
        });
    }
}
