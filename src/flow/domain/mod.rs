use petgraph::graph::NodeIndex;

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{mpsc, Arc, Mutex};
use std::time;

use std::collections::hash_map::Entry;

use timekeeper::{Timer, TimerSet, SimpleTracker, RealTime, ThreadTime};

use flow::prelude::*;
use flow::payload::{TransactionState, ReplayData};
pub use flow::domain::single::NodeDescriptor;
use flow::statistics;

use slog::Logger;

use ops;
use checktable;

const BATCH_SIZE: usize = 128;

const NANOS_PER_SEC: u64 = 1_000_000_000;
macro_rules! dur_to_ns {
    ($d:expr) => {{
        let d = $d;
        d.as_secs() * NANOS_PER_SEC + d.subsec_nanos() as u64
    }}
}

#[derive(Eq, PartialEq, Ord, PartialOrd, Hash, Clone, Copy, Debug)]
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

impl Index {
    pub fn index(&self) -> usize {
        self.0
    }
}

pub mod single;
pub mod local;

enum BufferedTransaction {
    RemoteTransaction,
    Transaction(NodeIndex, Vec<Packet>),
    MigrationStart(mpsc::SyncSender<()>),
    MigrationEnd(HashMap<NodeIndex, usize>),
}

type InjectCh = mpsc::SyncSender<Packet>;

pub struct Domain {
    index: Index,

    nodes: DomainNodes,
    state: StateMap,

    log: Logger,

    /// Map from timestamp to data buffered for that timestamp.
    buffered_transactions: HashMap<i64, BufferedTransaction>,
    /// Number of ingress nodes in the domain that receive updates from each base node. Base nodes
    /// that are only connected by timestamp ingress nodes are not included.
    ingress_from_base: HashMap<NodeIndex, usize>,
    /// Timestamp that the domain has seen all transactions up to.
    ts: i64,

    not_ready: HashSet<LocalNodeIndex>,

    checktable: Arc<Mutex<checktable::CheckTable>>,

    replaying_to: Option<(LocalNodeIndex, VecDeque<Packet>, usize)>,
    replay_paths: HashMap<Tag, (Vec<NodeAddress>, Option<mpsc::SyncSender<()>>)>,

    total_time: Timer<SimpleTracker, RealTime>,
    total_ptime: Timer<SimpleTracker, ThreadTime>,
    wait_time: Timer<SimpleTracker, RealTime>,
    process_times: TimerSet<LocalNodeIndex, SimpleTracker, RealTime>,
    process_ptimes: TimerSet<LocalNodeIndex, SimpleTracker, ThreadTime>,
}

impl Domain {
    pub fn new(log: Logger,
               index: Index,
               nodes: DomainNodes,
               checktable: Arc<Mutex<checktable::CheckTable>>,
               ts: i64)
               -> Self {
        // initially, all nodes are not ready (except for timestamp egress nodes)!
        let not_ready = nodes.iter()
            .map(|n| *n.borrow().addr().as_local())
            .collect();

        Domain {
            index: index,
            nodes: nodes,
            state: StateMap::default(),
            log: log,
            buffered_transactions: HashMap::new(),
            ingress_from_base: HashMap::new(),
            not_ready: not_ready,
            ts: ts,
            checktable: checktable,
            replaying_to: None,
            replay_paths: HashMap::new(),
            total_time: Timer::new(),
            total_ptime: Timer::new(),
            wait_time: Timer::new(),
            process_times: TimerSet::new(),
            process_ptimes: TimerSet::new(),
        }
    }

    pub fn dispatch(m: Packet,
                    not_ready: &HashSet<LocalNodeIndex>,
                    replaying_to: &mut Option<(LocalNodeIndex, VecDeque<Packet>, usize)>,
                    states: &mut StateMap,
                    nodes: &DomainNodes,
                    process_times: &mut TimerSet<LocalNodeIndex, SimpleTracker, RealTime>,
                    process_ptimes: &mut TimerSet<LocalNodeIndex, SimpleTracker, ThreadTime>,
                    enable_output: bool)
                    -> HashMap<NodeAddress, Vec<ops::Record>> {

        let me = m.link().dst;
        let mut output_messages = HashMap::new();

        if let Some((ref bufnode, ref mut buffered, _)) = *replaying_to {
            if bufnode == me.as_local() {
                buffered.push_back(m);
                return output_messages;
            }
        }
        if !not_ready.is_empty() && not_ready.contains(me.as_local()) {
            return output_messages;
        }

        let mut n = nodes[me.as_local()].borrow_mut();
        process_times.start(*me.as_local());
        process_ptimes.start(*me.as_local());
        let m = n.process(m, states, nodes, true);
        process_ptimes.stop();
        process_times.stop();
        drop(n);

        match m {
            Packet::Message { .. } if m.is_empty() => {
                // no need to deal with our children if we're not sending them anything
                return output_messages;
            }
            Packet::None => {
                // no need to deal with our children if we're not sending them anything
                return output_messages;
            }
            Packet::Message { .. } => {}
            Packet::Transaction { .. } => {
                // Any message with a timestamp (ie part of a transaction) must flow through the
                // entire graph, even if there are no updates associated with it.
            }
            Packet::Replay { .. } => {
                unreachable!("replay should never go through dispatch");
            }
            m => unreachable!("dispatch process got {:?}", m),
        }

        let mut m = Some(m); // so we can choose to take() the last one
        let n = nodes[me.as_local()].borrow();
        for i in 0..n.children.len() {
            // avoid cloning if we can
            let mut m = if i == n.children.len() - 1 {
                m.take().unwrap()
            } else {
                m.as_ref().map(|m| m.clone_data()).unwrap()
            };

            if enable_output || !nodes[n.children[i].as_local()].borrow().is_output() {
                m.link_mut().src = me;
                m.link_mut().dst = n.children[i];

                for (k, mut v) in Self::dispatch(m,
                                                 not_ready,
                                                 replaying_to,
                                                 states,
                                                 nodes,
                                                 process_times,
                                                 process_ptimes,
                                                 enable_output) {
                    output_messages.entry(k).or_insert_with(Vec::new).append(&mut v);
                }
            } else {
                let mut data = m.take_data();
                match output_messages.entry(n.children[i]) {
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

    fn dispatch_(&mut self,
                 m: Packet,
                 enable_output: bool)
                 -> HashMap<NodeAddress, Vec<ops::Record>> {
        Self::dispatch(m,
                       &self.not_ready,
                       &mut self.replaying_to,
                       &mut self.state,
                       &self.nodes,
                       &mut self.process_times,
                       &mut self.process_ptimes,
                       enable_output)
    }

    pub fn transactional_dispatch(&mut self, messages: Vec<Packet>) {
        assert!(!messages.is_empty());

        let mut egress_messages = HashMap::new();
        let ts =
            if let Some(&Packet::Transaction { state: ref ts@TransactionState::Committed(..), .. }) =
                messages.iter().next() {
                ts.clone()
            } else {
                unreachable!();
            };

        for m in messages {
            let new_messages = self.dispatch_(m, false);

            for (key, mut value) in new_messages {
                egress_messages.entry(key).or_insert_with(Vec::new).append(&mut value);
            }
        }

        for n in self.nodes.iter().filter(|n| n.borrow().is_output()) {
            let data = match egress_messages.entry(n.borrow().addr()) {
                Entry::Occupied(entry) => entry.remove().into(),
                _ => Records::default(),
            };

            let addr = n.borrow().addr();
            let m = Packet::Transaction {
                link: Link::new(addr, addr), // TODO: message should be from actual parent, not self.
                data: data,
                state: ts.clone(),
            };

            if !self.not_ready.is_empty() && self.not_ready.contains(addr.as_local()) {
                continue;
            }

            self.process_times.start(*addr.as_local());
            self.process_ptimes.start(*addr.as_local());
            self.nodes[addr.as_local()]
                .borrow_mut()
                .process(m, &mut self.state, &self.nodes, true);
            self.process_ptimes.stop();
            self.process_times.stop();
            assert_eq!(n.borrow().children.len(), 0);
        }
    }

    fn apply_transactions(&mut self) {
        while !self.buffered_transactions.is_empty() {
            let e = {
                // If we don't have anything for this timestamp yet, then stop.
                let entry = match self.buffered_transactions.entry(self.ts + 1) {
                    Entry::Occupied(e) => e,
                    _ => break,
                };

                // If this is a normal transaction and we don't have all the messages for this
                // timestamp, then stop.
                if let BufferedTransaction::Transaction(base, ref messages) = *entry.get() {
                    if messages.len() < self.ingress_from_base[&base] {
                        break;
                    }
                }
                entry.remove()
            };

            match e {
                BufferedTransaction::RemoteTransaction => {}
                BufferedTransaction::Transaction(_, messages) => {
                    self.transactional_dispatch(messages);
                }
                BufferedTransaction::MigrationStart(channel) => {
                    let _ = channel.send(());
                }
                BufferedTransaction::MigrationEnd(ingress_from_base) => {
                    self.ingress_from_base = ingress_from_base;
                }
            }
            self.ts += 1;
        }
    }

    // Skip the range of timestamps from start (inclusive) to end (non-inclusive).
    fn skip_timestamp_range(&mut self, start: i64, end: i64) {
        use std::cmp::max;
        if self.ts + 1 >= start {
            self.ts = end - 1;
            // ok to return early here because self.ts + 1 == end, so below loop won't execute
            return;
        }
        for t in max(start, self.ts + 1)..end {
            let o = BufferedTransaction::RemoteTransaction;
            self.buffered_transactions.insert(t, o);
        }
    }

    fn buffer_transaction(&mut self, m: Packet) {
        // Skip timestamps if possible. Unfortunately, this can't be combined with the if statement
        // below because that one inserts m into messages, while this one needs to borrow prevs from
        // within it.
        if let Packet::Transaction { state: TransactionState::Committed(ts, _, ref prevs), .. } = m {
            if let &Some(ref prevs) = prevs {
                if let Some(prev) = prevs.get(&self.index) {
                    self.skip_timestamp_range(*prev+1, ts);
                }
            }
        }

        if let Packet::Transaction { state: TransactionState::Committed(ts, base, _), .. } = m {
            if self.ts + 1 == ts && self.ingress_from_base[&base] == 1 {
                self.transactional_dispatch(vec![m]);
                self.ts += 1;
            } else {
                // Insert message into buffer.
                match *self.buffered_transactions
                    .entry(ts)
                    .or_insert_with(|| BufferedTransaction::Transaction(base, vec![])) {
                        BufferedTransaction::Transaction(_, ref mut messages) => messages.push(m),
                        _ => unreachable!(),
                    }
            }
            self.apply_transactions();
        } else {
            unreachable!();
        }
    }

    fn assign_ts(&mut self, packet: &mut Packet) -> bool {
        match *packet {
            Packet::Transaction { state: TransactionState::Committed(..), .. } => true,
            Packet::Transaction { ref mut state, ref link, ref data } => {
                let empty = TransactionState::Committed(0, 0.into(), None);
                let pending = ::std::mem::replace(state, empty);
                if let TransactionState::Pending(token, send) = pending {
                    let ingress = self.nodes[link.dst.as_local()].borrow();
                    // TODO: is this the correct node?
                    let base_node = self.nodes[ingress.children[0].as_local()].borrow().index;
                    let result = self.checktable
                        .lock()
                        .unwrap()
                        .claim_timestamp(&token, base_node, data);
                    match result {
                        checktable::TransactionResult::Committed(ts, prevs) => {
                            ::std::mem::replace(state,
                                                TransactionState::Committed(ts, base_node, prevs));
                            let _ = send.send(Ok(ts));
                            true
                        }
                        checktable::TransactionResult::Aborted => {
                            let _ = send.send(Err(()));
                            false
                        }
                    }
                } else {
                    unreachable!();
                }
            }
            _ => true,
        }
    }

    fn handle(&mut self, mut m: Packet, inject_tx: &mut InjectCh) {

        // assign ts to pending transactions
        if !self.assign_ts(&mut m) {
            // transaction aborted
            return;
        }

        match m {
            m @ Packet::Message { .. } => {
                self.dispatch_(m, true);
            }
            m @ Packet::Transaction { .. } => {
                self.buffer_transaction(m);
            }
            m @ Packet::Replay { .. } => {
                self.handle_replay(m, inject_tx);
            }
            Packet::Timestamp(ts) => {
                let o = BufferedTransaction::RemoteTransaction;
                let o = self.buffered_transactions.insert(ts, o);
                assert!(o.is_none());

                self.apply_transactions();
            }
            Packet::AddNode { node, parents } => {
                use std::cell;
                let addr = *node.addr().as_local();
                self.not_ready.insert(addr);

                for p in parents {
                    self.nodes.get_mut(&p).unwrap().borrow_mut().children.push(node.addr());
                }
                self.nodes.insert(addr, cell::RefCell::new(node));
                trace!(self.log, "new node incorporated"; "local" => addr.id());
            }
            Packet::PrepareState { node, index } => {
                let mut state = State::default();
                for idx in index {
                    state.add_key(&idx[..]);
                }
                self.state.insert(node, state);
            }
            Packet::SetupReplayPath { tag, path, done_tx, ack } => {
                // let coordinator know that we've registered the tagged path
                ack.send(()).unwrap();

                if done_tx.is_some() {
                    info!(self.log, "tag" => tag.id(); "told about terminating replay path {:?}", path);
                    // NOTE: we set self.replaying_to when we first receive a replay with this tag
                } else {
                    info!(self.log, "tag" => tag.id(); "told about replay path {:?}", path);
                }
                self.replay_paths.insert(tag, (path, done_tx));
            }
            Packet::StartReplay { tag, from, ack } => {
                // let coordinator know that we've entered replay loop
                ack.send(()).unwrap();

                let start = time::Instant::now();
                info!(self.log, "starting replay");

                // we know that the node is materialized, as the migration coordinator picks path
                // that originate with materialized nodes. if this weren't the case, we wouldn't be
                // able to do the replay, and the entire migration would fail.
                //
                // we clone the entire state so that we can continue to occasionally process
                // incoming updates to the domain without disturbing the state that is being
                // replayed.
                let state: State = self.state
                    .get(from.as_local())
                    .expect("migration replay path started with non-materialized node")
                    .clone();

                debug!(self.log, "current state cloned for replay"; "μs" => dur_to_ns!(start.elapsed()) / 1000);

                let m = Packet::Replay {
                    link: Link::new(from, from),
                    tag: tag,
                    last: true,
                    data: ReplayData::StateCopy(state),
                };

                self.handle_replay(m, inject_tx);
            }
            Packet::Finish(tag, ni) => {
                self.finish_replay(tag, ni, inject_tx);
            }
            Packet::Ready { node, index, ack } => {

                assert!(self.replaying_to.is_none());

                if !index.is_empty() {
                    let mut s = {
                        let n = self.nodes[&node].borrow();
                        if n.is_internal() && n.is_base() {
                            State::base()
                        } else {
                            State::default()
                        }
                    };
                    for idx in index {
                        s.add_key(&idx[..]);
                    }
                    assert!(self.state.insert(node, s).is_none());
                } else {
                    // NOTE: just because index_on is None does *not* mean we're not materialized
                }

                if self.not_ready.remove(&node) {
                    trace!(self.log, "readying empty node"; "local" => node.id());
                }

                // swap replayed reader nodes to expose new state
                {
                    use flow::node::Type;
                    let mut n = self.nodes[&node].borrow_mut();
                    if let Type::Reader(ref mut w, _) = *n.inner {
                        if let Some(ref mut state) = *w {
                            trace!(self.log, "swapping state"; "local" => node.id());
                            state.swap();
                            trace!(self.log, "state swapped"; "local" => node.id());
                        }
                    }
                }

                drop(ack);
            }
            Packet::StartMigration { at, prev_ts, ack } => {
                self.skip_timestamp_range(prev_ts, at);
                let o = self.buffered_transactions
                    .insert(at, BufferedTransaction::MigrationStart(ack));
                assert!(o.is_none());

                self.apply_transactions();
            }
            Packet::CompleteMigration { at, ingress_from_base } => {
                let o = self.buffered_transactions
                    .insert(at, BufferedTransaction::MigrationEnd(ingress_from_base));
                assert!(o.is_none());
                assert_eq!(at, self.ts + 1);
                self.apply_transactions();
            }
            Packet::GetStatistics(sender) => {
                let domain_stats = statistics::DomainStats {
                    total_time: self.total_time.num_nanoseconds(),
                    total_ptime: self.total_ptime.num_nanoseconds(),
                    wait_time: self.wait_time.num_nanoseconds(),
                };

                let node_stats = self.nodes
                    .iter()
                    .filter_map(|nd| {
                        let ref n: NodeDescriptor = *nd.borrow();
                        let local_index: LocalNodeIndex = *n.addr().as_local();
                        let node_index: NodeIndex = n.index;

                        let time = self.process_times.num_nanoseconds(local_index);
                        let ptime = self.process_ptimes.num_nanoseconds(local_index);
                        if time.is_some() && ptime.is_some() {
                            Some((node_index,
                                  statistics::NodeStats {
                                      process_time: time.unwrap(),
                                      process_ptime: ptime.unwrap(),
                                  }))
                        } else {
                            None
                        }
                    })
                    .collect();

                sender.send((domain_stats, node_stats)).unwrap();
            }
            Packet::None => unreachable!("None packets should never be sent around"),
            Packet::Quit => unreachable!("Quit messages are handled by event loop"),
        }
    }

    fn handle_replay(&mut self, m: Packet, inject_tx: &mut InjectCh) {
        let mut finished = None;
        let mut playback = None;
        if let Packet::Replay { mut link, tag, last, data } = m {
            let &mut (ref path, ref mut done_tx) = self.replay_paths.get_mut(&tag).unwrap();

            if done_tx.is_some() && self.replaying_to.is_none() {
                // this is the first message we receive for this tagged replay path. only at this
                // point should we start buffering messages for the target node. since the node is
                // not yet marked ready, all previous messages for this node will automatically be
                // discarded by dispatch(). the reason we should ignore all messages preceeding the
                // first replay message is that those have already been accounted for in the state
                // we are being replayed. if we buffered them and applied them after all the state
                // has been replayed, we would double-apply those changes, which is bad.
                self.replaying_to = Some((*path.last().unwrap().as_local(), VecDeque::new(), 0))
            }

            // we may be able to just absorb all the state in one go if we're lucky!
            let mut can_handle_directly = path.len() == 1;
            if can_handle_directly {
                // unfortunately, if this is a reader node, we can't just copy in the state
                // since State and Reader use different internal data structures
                // TODO: can we do better?
                use flow::node::Type;
                let n = self.nodes[path[0].as_local()].borrow();
                if let Type::Reader(..) = *n.inner {
                    can_handle_directly = false;
                }
            }
            // if the key columns of the state and the target state differ, we cannot use the
            // state directly, even if it is otherwise suitable. Note that we need to check
            // `can_handle_directly` again here because it will have been changed for reader
            // nodes above, and this check only applies to non-reader nodes.
            if can_handle_directly && done_tx.is_some() {
                if let ReplayData::StateCopy(ref state) = data {
                    let local_pkey = self.state[path[0].as_local()].keys();
                    if local_pkey != state.keys() {
                        debug!(self.log, "cannot use state directly, so falling back to regular replay";
                               "node" => path[0].as_local().id(),
                               "src keys" => format!("{:?}", state.keys()),
                               "dst keys" => format!("{:?}", local_pkey));
                        can_handle_directly = false;
                    }
                }
            }

            // TODO: if StateCopy debug_assert!(last);
            // TODO
            // we've been given a state dump, and only have a single node in this domain that needs
            // to deal with that dump. chances are, we'll be able to re-use that state wholesale.

            match data {
                ReplayData::StateCopy(state) => {
                    if can_handle_directly && done_tx.is_some() {
                        // oh boy, we're in luck! we're replaying into one of our nodes, and were just
                        // given the entire state. no need to process or anything, just move in the
                        // state and we're done.
                        let node = path[0];
                        debug!(self.log, "absorbing state clone"; "node" => node.as_local().id());
                        assert_eq!(self.state[node.as_local()].keys(), state.keys());
                        self.state.insert(*node.as_local(), state);
                        debug!(self.log, "direct state clone absorbed");
                        finished = Some((tag, *node.as_local()));
                    } else if can_handle_directly {
                        use flow::node::Type;
                        // if we're not terminal, and the domain only has a single node, that node
                        // *has* to be an egress node (since we're relaying to another domain).
                        let node = path[0];
                        let mut n = self.nodes[node.as_local()].borrow_mut();
                        if let Type::Egress { .. } = *n.inner {
                            // forward the state to the next domain without doing anything with it.
                            let p = Packet::Replay {
                                tag: tag,
                                link: Link::new(node, node),
                                last: true,
                                data: ReplayData::StateCopy(state),
                            };
                            debug!(self.log, "doing bulk egress forward");
                            n.process(p, &mut self.state, &self.nodes, false);
                            debug!(self.log, "bulk egress forward completed");
                            drop(n);
                        } else {
                            unreachable!();
                        }
                    } else if state.is_empty() {
                        // we're been given an entire state snapshot, which needs to be replayed
                        // row by row, *but* it's empty. fun fact: creating a chunked iterator over
                        // an empty hashmap yields *no* chunks, which *also* means that an update
                        // with last=true is never sent, which means that the replay never
                        // finishes. so, we deal with this case separately (and also avoid spawning
                        // a thread to walk empty state).
                        let p = Packet::Replay {
                            tag: tag,
                            link: Link::new(path[0], path[0]), // to will be overwritten by receiver
                            last: true,
                            data: ReplayData::Records(Vec::<Record>::new().into()),
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
                        let p = Packet::Replay {
                            tag: tag,
                            link: Link::new(path[0], path[0]), // to will be overwritten by receiver
                            last: false,
                            data: ReplayData::Records(Vec::<Record>::new().into()),
                        };
                        playback = Some(p);

                        // the sender doesn't know about us, it only knows about local nodes
                        // so we need to set the path correctly for process() to later work right
                        link.dst = path[0];

                        let log = self.log.new(None);
                        let inject_tx = inject_tx.clone();
                        thread::Builder::new()
                        .name(format!("replay{}.{}",
                                      self.nodes.iter().next().unwrap().borrow().domain().index(),
                                      link.src))
                        .spawn(move || {
                            use itertools::Itertools;

                            let from = link.src;
                            let to = link.dst;

                            let start = time::Instant::now();
                            debug!(log, "starting state chunker"; "node" => to.as_local().id());

                            let iter = state.into_iter()
                                .flat_map(|(_, rs)| rs)
                                .chunks(BATCH_SIZE);
                            let mut iter = iter
                                .into_iter()
                                .enumerate()
                                .peekable();

                            let link = Link::new(from, to);

                            // process all records in state to completion within domain
                            // and then forward on tx (if there is one)
                            while let Some((i, chunk)) = iter.next() {
                                use std::iter::FromIterator;
                                let chunk = Records::from_iter(chunk.into_iter());
                                let len = chunk.len();
                                let p = Packet::Replay {
                                    tag: tag,
                                    link: link.clone(), // to will be overwritten by receiver
                                    last: iter.peek().is_none(),
                                    data: ReplayData::Records(chunk),
                                };

                                trace!(log, "sending batch"; "#" => i, "[]" => len);
                                inject_tx.send(p).unwrap();
                            }

                            debug!(log, "state chunker finished"; "node" => to.as_local().id(), "μs" => dur_to_ns!(start.elapsed()) / 1000);
                        }).unwrap();
                    }
                }
                ReplayData::Records(data) => {
                    debug!(self.log, "replaying batch"; "#" => data.len());

                    // forward the current message through all local nodes
                    let mut m = Packet::Replay {
                        link: link,
                        tag: tag,
                        last: last,
                        data: ReplayData::Records(data),
                    };
                    for (i, ni) in path.iter().enumerate() {
                        // process the current message in this node
                        let mut n = self.nodes[ni.as_local()].borrow_mut();
                        m = n.process(m, &mut self.state, &self.nodes, false);
                        drop(n);

                        if i == path.len() - 1 {
                            // don't unnecessarily construct the last Message which is then
                            // immediately dropped.
                            break;
                        }

                        if m.is_empty() && !last {
                            // don't continue processing empty updates, *except* if this is the
                            // last replay batch. in that case we need to send it so that the next
                            // domain knows that we're done
                            // TODO: we *could* skip ahead to path.last() here
                            break;
                        }

                        // NOTE: the if above guarantees that nodes[i+1] will never go out of bounds
                        m = Packet::Replay {
                            tag: tag,
                            link: Link::new(*ni, path[i + 1]),
                            last: last,
                            data: ReplayData::Records(m.take_data()),
                        };
                    }

                    if last {
                        debug!(self.log, "last batch processed"; "terminal" => done_tx.is_some());
                    } else {
                        debug!(self.log, "batch processed");
                    }

                    if last && done_tx.is_some() {
                        let ni = *path.last().unwrap().as_local();
                        debug!(self.log, "last batch received"; "local" => ni.id());
                        finished = Some((tag, ni));
                    }
                }
            }
        } else {
            unreachable!();
        }

        if let Some(p) = playback {
            self.handle(p, inject_tx);
        }
        if let Some((tag, ni)) = finished {
            // NOTE: node is now ready, in the sense that it shouldn't ignore all updates since
            // replaying_to is still set, "normal" dispatch calls will continue to be buffered, but
            // this allows finish_replay to dispatch into the node by overriding replaying_to.
            self.not_ready.remove(&ni);
            inject_tx.send(Packet::Finish(tag, ni)).unwrap();
        }
    }

    fn finish_replay(&mut self, tag: Tag, node: LocalNodeIndex, inject: &mut InjectCh) {
        if self.replaying_to.is_none() {
            // we're told to continue replay, but nothing is being replayed
            unreachable!();
        }

        let finished = {
            let replaying_to = self.replaying_to.as_mut().unwrap();
            if replaying_to.0 != node {
                // we're told to continue replay for node a, but not b is being replayed
                unreachable!();
            }
            // log that we did another pass
            replaying_to.2 += 1;

            let mut first = true;
            while let Some(m) = replaying_to.1.pop_front() {
                // some updates were propagated to this node during the migration. we need to
                // replay them before we take even newer updates. however, we don't want to
                // completely block the domain data channel, so we only process a few backlogged
                // updates before yielding to the main loop (which might buffer more things).

                if let m @ Packet::Message { .. } = m {
                    // NOTE: we cannot use self.dispatch_ here, because we specifically need to
                    // override the buffering behavior that our self.replaying_to = Some above would
                    // initiate.
                    Self::dispatch(m,
                                   &self.not_ready,
                                   &mut None,
                                   &mut self.state,
                                   &self.nodes,
                                   &mut self.process_times,
                                   &mut self.process_ptimes,
                                   true);
                } else {
                    // no transactions allowed here since we're still in a migration
                    unreachable!();
                }

                if !first {
                    // handle two buffered for every one "real" update
                    break;
                }
                first = false;
            }

            replaying_to.1.is_empty()
        };

        if finished {
            // node is now ready, and should start accepting "real" updates
            let rt = self.replaying_to.take().unwrap();
            debug!(self.log, "node is fully up-to-date"; "local" => node.id(), "passes" => rt.2);

            if let Some(done_tx) = self.replay_paths.get_mut(&tag).and_then(|p| p.1.as_mut()) {
                info!(self.log, "acknowledging replay completed"; "node" => node.id());
                done_tx.send(()).unwrap();
            } else {
                unreachable!()
            }
        } else {
            // we're not done -- inject a request to continue handling buffered things
            inject.send(Packet::Finish(tag, node)).unwrap();
        }
    }

    pub fn boot(mut self, rx: mpsc::Receiver<Packet>) {
        use std::thread;

        info!(self.log, "booting domain"; "nodes" => self.nodes.iter().count());
        let name: usize = self.nodes.iter().next().unwrap().borrow().domain().into();
        thread::Builder::new()
            .name(format!("domain{}", name))
            .spawn(move || {
                let (mut inject_tx, inject_rx) = mpsc::sync_channel(1);

                // construct select so we can receive on all channels at the same time
                let sel = mpsc::Select::new();
                let mut rx_handle = sel.handle(&rx);
                let mut inject_rx_handle = sel.handle(&inject_rx);

                unsafe {
                    rx_handle.add();
                    inject_rx_handle.add();
                }

                self.total_time.start();
                self.total_ptime.start();
                loop {
                    self.wait_time.start();
                    let id = sel.wait();
                    self.wait_time.stop();

                    let m = if id == rx_handle.id() {
                        rx_handle.recv()
                    } else if id == inject_rx_handle.id() {
                        inject_rx_handle.recv()
                    } else {
                        unreachable!()
                    };
                    if m.is_err() {
                        break;
                    }
                    let m = m.unwrap();
                    if let Packet::Quit = m {
                        break;
                    }
                    self.handle(m, &mut inject_tx);
                }
            })
            .unwrap();
    }
}
