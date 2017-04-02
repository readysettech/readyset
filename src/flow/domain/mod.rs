use petgraph::graph::NodeIndex;

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time;

use std::collections::hash_map::Entry;

use timekeeper::{Timer, TimerSet, SimpleTracker, RealTime, ThreadTime};

use flow::prelude::*;
use flow::payload::{TransactionState, ReplayPieceContext};
pub use flow::domain::single::NodeDescriptor;
use flow::statistics;

use slog::Logger;

use flow::transactions;

use checktable;

const BATCH_SIZE: usize = 256;

const NANOS_PER_SEC: u64 = 1_000_000_000;
macro_rules! dur_to_ns {
    ($d:expr) => {{
        let d = $d;
        d.as_secs() * NANOS_PER_SEC + d.subsec_nanos() as u64
    }}
}

#[allow(missing_docs)]
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

#[allow(missing_docs)]
impl Index {
    pub fn index(&self) -> usize {
        self.0
    }
}

pub mod single;
pub mod local;

enum DomainMode {
    Forwarding,
    Replaying {
        to: LocalNodeIndex,
        buffered: VecDeque<Packet>,
        passes: usize,
    },
}

struct ReplayPath {
    source: Option<NodeAddress>,
    path: Vec<NodeAddress>,
    done_tx: Option<mpsc::SyncSender<()>>,
    trigger: TriggerEndpoint,
}

enum Waiting {
    /// A paused node is waiting for the state of *another* node to be filled.
    Paused {
        buffer: VecDeque<Packet>,
        queued: VecDeque<LocalNodeIndex>,
    },

    /// A target node is waiting for an incoming replay.
    ///
    /// Note that the Tag isn't needed, since we can just look for any Replay whose destination is
    /// this node. We know that other replays that go *through* this node won't have this node as
    /// the last node in the path, because the last node in the path must then be an Egress node.
    Target {
        waiting_for: Tag,
        buffer: VecDeque<Packet>,
        queued: VecDeque<LocalNodeIndex>,
    },
}

pub struct Domain {
    _index: Index,

    nodes: DomainNodes,
    state: StateMap,

    log: Logger,

    not_ready: HashSet<LocalNodeIndex>,

    transaction_state: transactions::DomainState,

    mode: DomainMode,
    waiting: local::Map<Waiting>,
    replay_paths: HashMap<Tag, ReplayPath>,

    inject_tx: Option<mpsc::SyncSender<Packet>>,

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
        // initially, all nodes are not ready
        let not_ready = nodes.iter().map(|n| *n.borrow().addr().as_local()).collect();

        Domain {
            _index: index,
            transaction_state: transactions::DomainState::new(index, &nodes, checktable, ts),
            nodes: nodes,
            state: StateMap::default(),
            log: log,
            not_ready: not_ready,
            mode: DomainMode::Forwarding,
            waiting: local::Map::new(),
            replay_paths: HashMap::new(),

            inject_tx: None,

            total_time: Timer::new(),
            total_ptime: Timer::new(),
            wait_time: Timer::new(),
            process_times: TimerSet::new(),
            process_ptimes: TimerSet::new(),
        }
    }

    fn handle_partial_miss(waiting: &mut local::Map<Waiting>,
                           paths: &mut HashMap<Tag, ReplayPath>,
                           me: LocalNodeIndex,
                           node: LocalNodeIndex,
                           key: Vec<DataType>,
                           was: Packet) {
        // find tag we should use for replay
        let tag = {
            let mut tags = paths.iter()
                .filter(|&(_, ref info)| match info.trigger {
                            TriggerEndpoint::End(..) |
                            TriggerEndpoint::Local(..) => {
                                info.path
                                    .last()
                                    .unwrap()
                                    .as_local() == &node
                            }
                            _ => false,
                        })
                .map(|(tag, _)| *tag);

            let tag = tags.next();
            if tags.next().is_some() {
                // union for example
                unimplemented!();
            }

            tag.expect("partial replay attempted on node with no partial path")
        };

        // if the lookup failure occured on a *different* node than the one we were
        // processing the update for, we need to be a bit trickier. specifically, we need
        // to buffer both at the node being replayed into (so that we can prune), *and* the
        // node at which we were processing (it is still conceptually blocking on the
        // update the caused the lookup miss). we also have to make sure that when the
        // replay finishes, the processing of this node continues.
        let mut release = None;
        let mut was = Some(was);
        if node != me {
            release = Some(me);

            let wait = match waiting.remove(&me) {
                None => {
                    let mut buffered = VecDeque::new();
                    buffered.push_front(was.take().unwrap());
                    Waiting::Paused {
                        buffer: buffered,
                        queued: VecDeque::new(),
                    }
                }
                Some(Waiting::Paused { .. }) => {
                    // we were sent a message while paused... that shouldn't happen.
                    unreachable!();
                }
                Some(Waiting::Target { .. }) => {
                    // a join is the target of a replay, but is also processing a packet?
                    unreachable!();
                }
            };
            waiting.insert(me, wait);
        }

        // make sure we buffer until the replay comes in
        let wait = match waiting.get_mut(&node) {
            None => {
                // the easy case -- we're not currently up to anything funky, just buffer
                // until the replay finishes.
                let mut buffered = VecDeque::new();
                if let Some(was) = was {
                    // also process the message that just missed again
                    buffered.push_front(was);
                }
                let mut queued = VecDeque::new();
                if let Some(release) = release {
                    queued.push_back(release);
                }
                Waiting::Target {
                    waiting_for: tag,
                    buffer: buffered,
                    queued: queued,
                }
            }
            Some(&mut Waiting::Paused { ref mut queued, .. }) |
            Some(&mut Waiting::Target { ref mut queued, .. }) => {
                // we did a lookup into a node that is already awaiting a replay from one
                // of its children, and missed.
                //
                // OR
                //
                // we did a lookup into a node that is already being replayed into by some
                // *other* node (note that it cannot be us, because then we'd be paused).
                //
                // let's make sure that other node gets back to us when it eventually
                // finishes replaying.
                queued.push_back(me);
                return;
            }
        };
        waiting.insert(node, wait);

        // send a message to the source domain(s) responsible for the chosen tag so they'll
        // start replay.
        let replay = paths.get_mut(&tag).unwrap();
        match replay.trigger {
            TriggerEndpoint::Local(ref key) => {
                unimplemented!();
            }
            TriggerEndpoint::End(ref mut trigger) => {
                trigger.send(Packet::RequestPartialReplay { tag, key }).unwrap();
            }
            TriggerEndpoint::Start(..) => unreachable!(),
            TriggerEndpoint::None => unreachable!("asked to replay along non-existing path"),
        }
    }

    fn dispatch(m: Packet,
                not_ready: &HashSet<LocalNodeIndex>,
                mode: &mut DomainMode,
                waiting: &mut local::Map<Waiting>,
                states: &mut StateMap,
                nodes: &DomainNodes,
                paths: &mut HashMap<Tag, ReplayPath>,
                process_times: &mut TimerSet<LocalNodeIndex, SimpleTracker, RealTime>,
                process_ptimes: &mut TimerSet<LocalNodeIndex, SimpleTracker, ThreadTime>,
                enable_output: bool)
                -> HashMap<NodeAddress, Vec<Record>> {

        let me = m.link().dst;
        let mut output_messages = HashMap::new();

        match *mode {
            DomainMode::Forwarding => (),
            DomainMode::Replaying {
                ref to,
                ref mut buffered,
                ..
            } if to == me.as_local() => {
                buffered.push_back(m);
                return output_messages;
            }
            DomainMode::Replaying { .. } => (),
        }

        match waiting.get_mut(me.as_local()) {
            Some(&mut Waiting::Paused { ref mut buffer, .. }) => {
                buffer.push_back(m);
                return output_messages;
            }
            Some(&mut Waiting::Target { ref mut buffer, .. }) => {
                buffer.push_back(m);
                return output_messages;
            }
            None => {}
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

        let m = match m {
            single::FinalProcessingResult::Done(m) => m,
            single::FinalProcessingResult::NeedReplay { node, key, was, .. } => {
                Self::handle_partial_miss(waiting,
                                          paths,
                                          *me.as_local(),
                                          *node.as_local(),
                                          key,
                                          was);
                return output_messages;
            }
        };

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
            Packet::ReplayPiece { .. } |
            Packet::FullReplay { .. } => {
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
                                                 mode,
                                                 waiting,
                                                 states,
                                                 nodes,
                                                 paths,
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

    fn dispatch_(&mut self, m: Packet, enable_output: bool) -> HashMap<NodeAddress, Vec<Record>> {
        Self::dispatch(m,
                       &self.not_ready,
                       &mut self.mode,
                       &mut self.waiting,
                       &mut self.state,
                       &self.nodes,
                       &mut self.replay_paths,
                       &mut self.process_times,
                       &mut self.process_ptimes,
                       enable_output)
    }

    pub fn transactional_dispatch(&mut self, messages: Vec<Packet>) {
        assert!(!messages.is_empty());

        let mut egress_messages = HashMap::new();
        let ts = if let Some(&Packet::Transaction {
                                  state: ref ts @ TransactionState::Committed(..), ..
                              }) = messages.iter().next() {
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
            self.nodes[addr.as_local()].borrow_mut().process(m, &mut self.state, &self.nodes, true);
            self.process_ptimes.stop();
            self.process_times.stop();
            assert_eq!(n.borrow().children.len(), 0);
        }
    }

    fn handle(&mut self, m: Packet) {
        match m {
            m @ Packet::Message { .. } => {
                self.dispatch_(m, true);
            }
            m @ Packet::Transaction { .. } |
            m @ Packet::StartMigration { .. } |
            m @ Packet::CompleteMigration { .. } => {
                self.transaction_state.handle(m);
                loop {
                    match self.transaction_state.get_next_event() {
                        transactions::Event::Transaction(m) => self.transactional_dispatch(m),
                        transactions::Event::StartMigration => {}
                        transactions::Event::CompleteMigration => {}
                        transactions::Event::None => break,
                    }
                }
            }
            m @ Packet::ReplayPiece { .. } |
            m @ Packet::FullReplay { .. } => {
                self.handle_replay(m);
            }
            Packet::AddNode { node, parents } => {
                use std::cell;
                let addr = *node.addr().as_local();
                self.not_ready.insert(addr);

                for p in parents {
                    self.nodes
                        .get_mut(&p)
                        .unwrap()
                        .borrow_mut()
                        .children
                        .push(node.addr());
                }
                self.nodes.insert(addr, cell::RefCell::new(node));
                trace!(self.log, "new node incorporated"; "local" => addr.id());
            }
            Packet::StateSizeProbe { node, ack } => {
                if let Some(state) = self.state.get(&node) {
                    ack.send(state.len()).unwrap();
                } else {
                    drop(ack);
                }
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
                    InitialState::PartialGlobal(new_wh, new_rh) => {
                        use flow::node::{Type, Reader};
                        let mut n = self.nodes[&node].borrow_mut();
                        if let Type::Reader(ref mut wh, Reader { ref mut state, .. }) = *n.inner {
                            // make sure Reader is actually prepared to receive state
                            assert!(wh.is_some());
                            let wh = wh.as_mut().unwrap();
                            assert!(state.is_some());
                            let rh = state.as_mut().unwrap();
                            assert_eq!(rh.len(), 0);
                            *wh = new_wh;
                            *rh = new_rh;
                        } else {
                            unreachable!();
                        }
                    }
                    InitialState::Global => {}
                }
            }
            Packet::SetupReplayPath {
                tag,
                source,
                path,
                done_tx,
                trigger,
                ack,
            } => {
                // let coordinator know that we've registered the tagged path
                ack.send(()).unwrap();

                if done_tx.is_some() {
                    info!(self.log, "tag" => tag.id(); "told about terminating replay path {:?}", path);
                    // NOTE: we set self.replaying_to when we first receive a replay with this tag
                } else {
                    info!(self.log, "tag" => tag.id(); "told about replay path {:?}", path);
                }
                self.replay_paths.insert(tag,
                                         ReplayPath {
                                             source,
                                             path,
                                             done_tx,
                                             trigger,
                                         });
            }
            Packet::RequestPartialReplay { tag, key } => {
                let (m, source) = match self.replay_paths[&tag] {
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
                        match self.waiting.get_mut(source.as_local()) {
                            Some(&mut Waiting::Paused { ref mut buffer, .. }) |
                            Some(&mut Waiting::Target { ref mut buffer, .. }) => {
                                // node is already doing a replay, so we just make sure it
                                // eventually triggers this replay when it's ready
                                buffer.push_back(Packet::RequestPartialReplay { tag, key });
                                return;
                            }
                            None => {}
                        }

                        let rs = self.state
                            .get(source.as_local())
                            .expect("migration replay path started with non-materialized node")
                            .lookup(&cols[..], &KeyType::Single(&key[0]));

                        let m = if let LookupResult::Some(rs) = rs {
                            use std::iter::FromIterator;
                            Some(Packet::ReplayPiece {
                                     link: Link::new(source, path[0]),
                                     tag: tag,
                                     context: ReplayPieceContext::Partial { for_key: key.clone() },
                                     data: Records::from_iter(rs.into_iter().cloned()),
                                 })
                        } else {
                            None
                        };

                        (m, source)
                    }
                    _ => unreachable!(),
                };

                if let Some(m) = m {
                    self.handle_replay(m);
                    return;
                }

                // we must have missed in our lookup,
                // so we have a partial replay through a partial replay
                // trigger a replay to source node, and enqueue this request.
                Self::handle_partial_miss(&mut self.waiting,
                                          &mut self.replay_paths,
                                          *source.as_local(),
                                          *source.as_local(),
                                          key.clone(),
                                          Packet::RequestPartialReplay { tag, key });
            }
            Packet::StartReplay { tag, from, ack } => {
                // let coordinator know that we've entered replay loop
                ack.send(()).unwrap();

                assert_eq!(self.replay_paths[&tag].source, Some(from));

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

                let m = Packet::FullReplay {
                    link: Link::new(from, self.replay_paths[&tag].path[0]),
                    tag: tag,
                    state: state,
                };

                self.handle_replay(m);
            }
            Packet::Finish(tag, ni) => {
                self.finish_replay(tag, ni);
            }
            Packet::Ready { node, index, ack } => {

                if let DomainMode::Forwarding = self.mode {
                } else {
                    unreachable!();
                }

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
                        s.add_key(&idx[..], false);
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
            Packet::RequestUnboundedTx(_) => unreachable!("Requests for unbounded tx channel are handled by event loop"),
            Packet::Quit => unreachable!("Quit messages are handled by event loop"),
        }
    }

    fn handle_replay(&mut self, m: Packet) {
        let tag = m.tag().unwrap();
        let mut finished = None;
        let mut playback = None;
        let mut need_replay = None;
        'outer: loop {
            // this loop is just here so we have a way of giving up the borrow of self.replay_paths

            let &mut ReplayPath {
                         ref path,
                         ref mut done_tx,
                         ..
                     } = self.replay_paths.get_mut(&tag).unwrap();

            match self.mode {
                DomainMode::Forwarding if done_tx.is_some() => {
                    // this is the first message we receive for this tagged replay path. only at this
                    // point should we start buffering messages for the target node. since the node is
                    // not yet marked ready, all previous messages for this node will automatically be
                    // discarded by dispatch(). the reason we should ignore all messages preceeding the
                    // first replay message is that those have already been accounted for in the state
                    // we are being replayed. if we buffered them and applied them after all the state
                    // has been replayed, we would double-apply those changes, which is bad.
                    self.mode = DomainMode::Replaying {
                        to: *path.last().unwrap().as_local(),
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

            // should this really be buffered?
            match self.waiting.get_mut(path[0].as_local()) {
                Some(&mut Waiting::Paused { ref mut buffer, .. }) => {
                    buffer.push_back(m);
                    return;
                }
                Some(&mut Waiting::Target {
                              ref mut buffer,
                              ref waiting_for,
                              ..
                          }) if *waiting_for != tag => {
                    buffer.push_back(m);
                    return;
                }
                _ => {}
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
                if let Packet::FullReplay { ref state, .. } = m {
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

            // will look somewhat nicer with https://github.com/rust-lang/rust/issues/15287
            match m {
                Packet::FullReplay { tag, link, state } => {
                    if can_handle_directly && done_tx.is_some() {
                        // oh boy, we're in luck! we're replaying into one of our nodes, and were just
                        // given the entire state. no need to process or anything, just move in the
                        // state and we're done.
                        let node = path[0];
                        debug!(self.log, "absorbing state clone"; "node" => node.as_local().id());
                        assert_eq!(self.state[node.as_local()].keys(), state.keys());
                        self.state.insert(*node.as_local(), state);
                        debug!(self.log, "direct state clone absorbed");
                        finished = Some((tag, *node.as_local(), None));
                    } else if can_handle_directly {
                        use flow::node::Type;
                        // if we're not terminal, and the domain only has a single node, that node
                        // *has* to be an egress node (since we're relaying to another domain).
                        let node = path[0];
                        let mut n = self.nodes[node.as_local()].borrow_mut();
                        if let Type::Egress { .. } = *n.inner {
                            // forward the state to the next domain without doing anything with it.
                            let p = Packet::FullReplay {
                                tag: tag,
                                link: link, // the egress node will fix this up
                                state: state,
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
                        let p = Packet::ReplayPiece {
                            tag: tag,
                            link: link,
                            context: ReplayPieceContext::Regular { last: true },
                            data: Vec::<Record>::new().into(),
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
                        let p = Packet::ReplayPiece {
                            tag: tag,
                            link: link.clone(),
                            context: ReplayPieceContext::Regular { last: false },
                            data: Vec::<Record>::new().into(),
                        };
                        playback = Some(p);

                        let log = self.log.new(None);
                        let inject_tx = self.inject_tx.clone().unwrap();
                        thread::Builder::new()
                        .name(format!("replay{}.{}",
                                      self.nodes.iter().next().unwrap().borrow().domain().index(),
                                      link.src))
                        .spawn(move || {
                            use itertools::Itertools;

                            let start = time::Instant::now();
                            debug!(log, "starting state chunker"; "node" => link.dst.as_local().id());

                            let iter = state.into_iter()
                                .flat_map(|(_, rs)| rs)
                                .chunks(BATCH_SIZE);
                            let mut iter = iter
                                .into_iter()
                                .enumerate()
                                .peekable();

                            // process all records in state to completion within domain
                            // and then forward on tx (if there is one)
                            while let Some((i, chunk)) = iter.next() {
                                use std::iter::FromIterator;
                                let chunk = Records::from_iter(chunk.into_iter());
                                let len = chunk.len();
                                let p = Packet::ReplayPiece {
                                    tag: tag,
                                    link: link.clone(), // to will be overwritten by receiver
                                    context: ReplayPieceContext::Regular{last: iter.peek().is_none()},
                                    data: chunk,
                                };

                                trace!(log, "sending batch"; "#" => i, "[]" => len);
                                if inject_tx.send(p).is_err() {
                                    warn!(log, "replayer noticed domain shutdown");
                                    break;
                                }
                            }

                            debug!(log, "state chunker finished"; "node" => link.dst.as_local().id(), "μs" => dur_to_ns!(start.elapsed()) / 1000);
                        }).unwrap();
                    }
                }
                Packet::ReplayPiece {
                    tag,
                    link,
                    data,
                    context,
                } => {
                    if let ReplayPieceContext::Partial { .. } = context {
                        trace!(self.log, "replaying batch"; "#" => data.len());
                    } else {
                        debug!(self.log, "replaying batch"; "#" => data.len());
                    }

                    // forward the current message through all local nodes.
                    let mut m = Packet::ReplayPiece {
                        link,
                        tag,
                        data,
                        context: context.clone(),
                    };

                    // keep track of whether we're filling any partial holes
                    let mut release = false;
                    let partial_key =
                        if let ReplayPieceContext::Partial { ref for_key } = context {
                            Some(for_key)
                        } else {
                            None
                        };

                    // we may be replaying a partially processed state replay, in which case we
                    // need to make sure to only process along the remaining suffix of the path.
                    let skip = path.iter().position(|ni| ni == &m.link().dst).unwrap();

                    for (i, ni) in path.iter().enumerate().skip(skip) {
                        // do we have to buffer at this node?
                        match self.waiting.get_mut(ni.as_local()) {
                            Some(&mut Waiting::Paused { ref mut buffer, .. }) => {
                                buffer.push_back(m);
                                return;
                            }
                            Some(&mut Waiting::Target { ref waiting_for, .. }) if *waiting_for ==
                                                                                  tag => {
                                // it's waiting, but for us!
                                release = true;
                            }
                            Some(&mut Waiting::Target { ref mut buffer, .. }) => {
                                buffer.push_back(m);
                                return;
                            }
                            _ => {}
                        }

                        // are we about to fill a hole?
                        if i == path.len() - 1 && partial_key.is_some() {
                            let partial_key = partial_key.unwrap();
                            if release {
                                // we're waiting for this particular replay packet to release a
                                // buffer. mark the state for the key being replayed as *not* a
                                // hole otherwise we'll just end up with the same NeedReply
                                // response that triggered this replay initially.
                                self.state
                                    .get_mut(ni.as_local())
                                    .unwrap()
                                    .mark_filled(partial_key.clone());
                            } else {
                                // we could be filling a hole in a Reader. we need to ensure
                                // that the hole for the key we're replaying ends up being
                                // filled, even if that hole is empty!
                                use flow::node::Type;
                                let mut n = self.nodes[ni.as_local()].borrow_mut();
                                if let Type::Reader(Some(ref mut wh), _) = *n.inner {
                                    wh.mark_filled(&partial_key[0]);
                                }
                            }
                        }

                        // process the current message in this node
                        let mut n = self.nodes[ni.as_local()].borrow_mut();
                        m = match n.process(m, &mut self.state, &self.nodes, false) {
                            single::FinalProcessingResult::Done(m) => m,
                            single::FinalProcessingResult::NeedReplay {
                                node, key, was, ..
                            } => {
                                // oh no, we missed during replay!

                                // let's make sure holes aren't considered filled
                                if i == path.len() - 1 && partial_key.is_some() {
                                    let partial_key = partial_key.unwrap();
                                    if release {
                                        self.state
                                            .get_mut(ni.as_local())
                                            .unwrap()
                                            .mark_hole(&partial_key[..]);
                                    } else {
                                        use flow::node::Type;
                                        let mut n = self.nodes[ni.as_local()].borrow_mut();
                                        if let Type::Reader(Some(ref mut wh), _) = *n.inner {
                                            wh.mark_hole(&partial_key[0]);
                                        }
                                    }
                                }

                                // and then request a replay
                                need_replay = Some((node, *ni.as_local(), key, was));
                                break 'outer;
                            }
                        };
                        drop(n);

                        if i == path.len() - 1 {
                            // don't unnecessarily construct the last Message which is then
                            // immediately dropped.
                            break;
                        }

                        if m.is_empty() {
                            if let ReplayPieceContext::Regular { last: false } = context {
                                // don't continue processing empty updates, *except* if this is the
                                // last replay batch. in that case we need to send it so that the next
                                // domain knows that we're done
                                // TODO: we *could* skip ahead to path.last() here
                                break;
                            }
                        }

                        // NOTE: the if above guarantees that nodes[i+1] will never go out of bounds
                        m.link_mut().src = *ni;
                        m.link_mut().dst = path[i + 1];
                    }

                    let dst = *path.last().unwrap().as_local();
                    match context {
                        ReplayPieceContext::Regular { last } if last => {
                            debug!(self.log, "last batch processed"; "terminal" => done_tx.is_some());
                            if done_tx.is_some() {
                                debug!(self.log, "last batch received"; "local" => dst.id());
                                finished = Some((tag, dst, None));
                            }
                        }
                        ReplayPieceContext::Regular { .. } => {
                            debug!(self.log, "batch processed");
                        }
                        ReplayPieceContext::Partial { for_key } => {
                            if let Some(&Waiting::Target { .. }) = self.waiting.get(&dst) {
                                trace!(self.log, "partial replay completed"; "local" => dst.id());
                                finished = Some((tag, dst, Some(for_key)));
                            } else {
                                // replay to a node that's not waiting for it?
                                // TODO: could be a Reader
                            }
                        }
                    }
                }
                _ => unreachable!(),
            }
            break;
        }

        if let Some((node, ni, key, was)) = need_replay {
            Self::handle_partial_miss(&mut self.waiting,
                                      &mut self.replay_paths,
                                      ni,
                                      *node.as_local(),
                                      key,
                                      was);
            return;
        }

        if let Some(p) = playback {
            self.handle(p);
        }

        if let Some((tag, ni, for_key)) = finished {
            if let Some(Waiting::Target {
                            mut buffer,
                            mut queued,
                            ..
                        }) = self.waiting.remove(&ni) {

                // we got a partial replay result that we were waiting for.
                // we now need to process any messages that got held up waiting for it.

                let for_key = for_key.unwrap();
                let columns = self.state[&ni].keys().swap_remove(0);

                // NOTE: some of the data in the buffered messages may already be contained within
                // the data we just replayed. we need to prune those out so we don't end up
                // double-applying them!
                for m in &mut buffer {
                    if !m.is_regular() {
                        continue;
                    }

                    m.map_data(|rs| {
                        rs.retain(|r| {
                                      let _ = (); // force block format
                                      !columns.iter().enumerate().all(|(i, &c)| r[c] == for_key[i])
                                  });
                    });
                }

                // did someone else try to do lookups into us while we were being filled?
                while let Some(node) = queued.pop_front() {
                    // yes, so we need to replay their queues too.
                    // if any of the triggered nodes do another lookup that misses into us, they'll
                    // either trigger a replay, or queue themselves again. in that case, we should
                    // stop processing this node, and wait for that replay.
                    if self.release_paused(node, Some((ni, &mut buffer, &mut queued))) {
                        return;
                    }
                }

                // NOTE
                // it is important that we only release our buffer only after any downstream nodes
                // have had their turn. consider what could otherwise happen in the following
                // graph:
                //
                //   L     R
                //   |     |
                //   +--+--+
                //      |
                //      J
                //
                //  say R1 arrives at J with key K1, and K1 misses in L.
                //  L now has to do a replay for K1.
                //  meanwhile, it buffers some updates (say L2 and L3, with keys K2 and K3).
                //  J also buffers some more updates from R (say R4, with key K4).
                //  the replay for K1 at L then finishes.
                //  consider what would happen if we replay L's buffer first.
                //  assume K2 is not present in R.
                //  L forwards K2 to J, which misses in R.
                //  R now has to replay, so enters Target state.
                //  J is waiting for R, so enters Paused state.
                //  what state should L be in?
                //  it isn't Paused, because it's not waiting on another node really.
                //  it isn't Target, because it's not waiting on a state replay.
                //  but it also isn't not waiting, because it still has L3 to replay.
                //  we *could* make it Paused, and add it to the queued list of J, but that seems
                //  really weird, and could (maybe) lead to cycles.
                //  if we instead replay J first, and then L, this problem does not arise.
                //  any buffered messages at J must be from R, and so must miss in L, so L can
                //  re-enter the Target state.
                //  unfortunately, this argument *only* works as long as we have two-way joins.
                //  with three-way joins, J could miss in some other node N...

                // replay our (pruned) buffer
                while let Some(m) = buffer.pop_front() {
                    use std::mem;
                    self.handle(m);
                    match self.waiting.get_mut(&ni) {
                        None => {}
                        Some(&mut Waiting::Paused {
                                      buffer: ref mut next_buffer,
                                      queued: ref mut next_queued,
                                  }) |
                        Some(&mut Waiting::Target {
                                      buffer: ref mut next_buffer,
                                      queued: ref mut next_queued,
                                      ..
                                  }) => {
                            // while processing this update, *we* missed in some other node.
                            // we need to make sure we keep working through our backlog when the
                            // hole is eventually filled.
                            //
                            // OR
                            //
                            // a downstream node missed on us while processing this update.
                            // make sure we keep our backlog so it eventually gets processed.

                            // semantically, we now want to do
                            // next_buffer.append(&mut buffer);
                            // next_queued.append(&mut queued);
                            //
                            // however, we know that since we only replayed *one* packet, and that
                            // can trigger at most *one* miss, we *must* have at most one new
                            // buffered message (if we missed while processing m), and at most one
                            // new queued node (if we were missed on). thus, it is likely to be
                            // much more efficient to start from our *current* buffer/queued, and
                            // push that one element to the front, than to copy over all the
                            // current elements into the new one.
                            //
                            // so, swap the buffers
                            mem::swap(next_buffer, &mut buffer);
                            // take the one buffered element if there is one, and stick it first
                            if let Some(buffered) = buffer.pop_front() {
                                next_buffer.push_front(buffered);
                            }
                            // make sure there aren't more
                            assert_eq!(buffer.len(), 0);
                            // then swap the queued queue
                            mem::swap(next_queued, &mut queued);
                            // same, stick the new one first if there is one
                            if let Some(queued) = queued.pop_front() {
                                next_queued.push_front(queued);
                            }
                            // and make sure there was indeed at most one
                            assert_eq!(queued.len(), 0);
                            return;
                        }
                    }
                }

                return;
            }

            // NOTE: node is now ready, in the sense that it shouldn't ignore all updates since
            // replaying_to is still set, "normal" dispatch calls will continue to be buffered, but
            // this allows finish_replay to dispatch into the node by overriding replaying_to.
            self.not_ready.remove(&ni);
            // NOTE: if this call ever blocks, we're in big trouble: handle_replay is called
            // directly from the main loop of a domain, so if we block here, we're also blocking
            // the loop that is supposed to drain the channel we're blocking on. luckily, in this
            // particular case, we know that sending will not block, because:
            //
            //  - inject_tx has a buffer size of 1, so we will block if either inject_tx is
            //    already full, or if there are other concurrent senders.
            //  - there are no concurrent senders because:
            //    - there are only two other places that send on inject_tx: in finish_replay, and in
            //      state replay.
            //    - finish_replay is not running, because it is only run from the main domain loop,
            //      and that's currently executing us.
            //    - no state replay can be sending, because:
            //      - there is only one replay: this one
            //      - that replay sent an entry with last: true (so we got finished.is_some)
            //      - last: true is the *last* thing the replay thread sends
            //  - inject_tx must be empty, because
            //    - if the previous send was from the replay thread, it had last: true (otherwise we
            //      wouldn't have finished.is_some), and we just recv'd that.
            //    - if the last send was from finish_replay, it must have been recv'd by the time
            //      this code runs. the reason for this is a bit more involved:
            //      - we just received a Packet::Replay with last: true.
            //      - at some point prior to this, finish_replay sent a Packet::Finish
            //      - it turns out that there *must* have been a recv on the inject channel between
            //        these two. by contradiction:
            //        - assume no packet was received on inject between the two times
            //        - if we are using local replay, we know the replay thread has finished,
            //          since handle_replay must have seen last: true from it in order to trigger
            //          finish_replay
            //        - if we were being replayed to from another domain, the *previous* Replay we
            //          received from it must have had last: true (again, to trigger finish_replay)
            //        - thus, the Replay we are receiving *now* must be a part of the *next* replay
            //        - we know finish_replay has not acknowledged the previous replay to the
            //          parent domain:
            //          - it does so only after receiving a Finish (from the inject channel), and
            //            *not* emitting another Finish
            //          - since it *did* emit a Finish, we know it did *not* ack last time
            //          - by assumption, the Finish it emitted has not been received, so we also
            //            know it hasn't run again
            //        - since no replay message is sent after a last: true until the migration sees
            //          the ack from finish_replay, we know the most recent replay must be the
            //          last: true that triggered finish_replay.
            //        - but this is a contradiction, since we just received a Packet::Replay
            //
            // phew.
            // hopefully that made sense.
            // this (informal) argument relies on there only being one active replay in the system
            // at any given point in time, so we may need to revisit it for partial materialization
            // (TODO)
            match self.inject_tx
                      .as_mut()
                      .unwrap()
                      .try_send(Packet::Finish(tag, ni)) {
                Ok(_) => {}
                Err(mpsc::TrySendError::Disconnected(_)) => {
                    // can't happen, since we know the reader thread (us) is still running
                    unreachable!();
                }
                Err(mpsc::TrySendError::Full(_)) => {
                    unreachable!();
                }
            }
        }
    }

    /// Replay any buffered updates at the given node, since the hole that blocked it has been
    /// filled.
    ///
    /// Returns true if another hole was encountered while processing the buffer.
    fn release_paused(&mut self,
                      paused: LocalNodeIndex,
                      waited_for: Option<(LocalNodeIndex,
                                          &mut VecDeque<Packet>,
                                          &mut VecDeque<LocalNodeIndex>)>)
                      -> bool {
        if let Some(Waiting::Paused {
                        buffer: mut pbuffer,
                        queued: mut pqueued,
                    }) = self.waiting.remove(&paused) {
            // keep processing the buffered entries for this paused node until we've either gone
            // through all of them, or until we trigger another replay.
            while let Some(m) = pbuffer.pop_front() {
                self.handle(m);
                // did we start waiting again?
                // NOTE: we need to .remove() since the block beneath must have &mut self
                match self.waiting.remove(&paused) {
                    None => {}
                    Some(Waiting::Paused {
                             mut buffer,
                             mut queued,
                         }) => {
                        // yup, seems we hit another missing entry while replaying.

                        // transfer over the remaining buffered things and leave it up to the next
                        // arrival of a replay for that to continue.
                        buffer.append(&mut pbuffer);

                        // make sure that any nodes that were queued on our state are eventually
                        // unblocked.
                        queued.append(&mut pqueued);

                        if let Some((waited_for, mut target_buffered, target_queued)) = waited_for {
                            // since we process the paused node's buffer first, there might
                            // messages waiting to be processed at the ancestor that we were waiting
                            // for a replay into. we need to make sure that those buffered messages are
                            // eventually delivered too.
                            match self.waiting.get_mut(&waited_for) {
                                None => {
                                    // okay, this is pretty bad...
                                    // while processing a buffered update, we hit a hole in a
                                    // *different* ancestor, who is now replaying. this means we
                                    // don't have anywhere to put the buffered elements of the
                                    // ancestor that we *previously* missed in. If we only ever
                                    // have two-way joins, this shouldn't happen (if we miss in R,
                                    // all buffered updates should be L, so all misses during
                                    // buffer replay should remain in R), but with more elaborate
                                    // joins (and possibly unions) this could happen.
                                    //
                                    // NOTE: we *may* be able to just replay the rest of buffer in
                                    // this case, and essentially transfer our buffer to J. we'd
                                    // need to think carefully about that though.
                                    unimplemented!();
                                }
                                Some(&mut Waiting::Target {
                                              ref mut buffer,
                                              ref mut queued,
                                              ..
                                          }) => {
                                    use std::mem;
                                    // we must have caused this to happen.
                                    assert!(queued.iter().any(|q| q == &paused));
                                    // make sure the node wakes up any other nodes that were
                                    // waiting for it when it finishes the replay.
                                    queued.append(target_queued);
                                    // since we must have failed a lookup into waited_for, and it
                                    // was previously idle, we know that there can be no bufferd
                                    // updates.
                                    assert_eq!(buffer.len(), 0);
                                    // we now *want* to do
                                    //
                                    //   buffer.append(target_buffered);
                                    //
                                    // but since buffer is empty, we can instead just swap
                                    mem::swap(buffer, &mut target_buffered);
                                }
                                Some(&mut Waiting::Paused { .. }) => unreachable!(),
                            }
                        }

                        // put back what we stole
                        self.waiting.insert(paused, Waiting::Paused { buffer, queued });
                        return true;
                    }
                    Some(..) => {
                        // processing a buffered update somehow caused a replay to *our* state.
                        // this can only happen with an operator that looks at its ancestors'
                        // state, and *then* at its own state, which we don't currently have.
                        unreachable!()
                    }
                }
            }

            // did someone else try to do lookups into us while we were waiting?
            for node in pqueued {
                // yes, so we need to replay their queues too. note that here, like above, we don't
                // care about triggering further replays, because the node will then automatically
                // trigger a replay of queue itself again.
                self.release_paused(node, None);
            }
        } else {
            unreachable!();
        }

        false
    }

    fn finish_replay(&mut self, tag: Tag, node: LocalNodeIndex) {
        let finished = if let DomainMode::Replaying {
                   ref to,
                   ref mut buffered,
                   ref mut passes,
               } = self.mode {
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

                if let m @ Packet::Message { .. } = m {
                    // NOTE: we cannot use self.dispatch_ here, because we specifically need to
                    // override the buffering behavior that our self.replaying_to = Some above would
                    // initiate.
                    Self::dispatch(m,
                                   &self.not_ready,
                                   &mut DomainMode::Forwarding,
                                   &mut self.waiting,
                                   &mut self.state,
                                   &self.nodes,
                                   &mut self.replay_paths,
                                   &mut self.process_times,
                                   &mut self.process_ptimes,
                                   true);
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
                mem::replace(&mut self.mode, DomainMode::Forwarding) {
                debug!(self.log, "node is fully up-to-date"; "local" => node.id(), "passes" => passes);
            } else {
                unreachable!();
            }

            if let Some(done_tx) = self.replay_paths.get_mut(&tag).and_then(|p| {
                                                                                p.done_tx.as_mut()
                                                                            }) {
                info!(self.log, "acknowledging replay completed"; "node" => node.id());
                done_tx.send(()).unwrap();
            } else {
                unreachable!()
            }
        } else {
            // we're not done -- inject a request to continue handling buffered things
            // NOTE: similarly to in handle_replay, if this call ever blocks, we're in big trouble:
            // finish_replay is also called directly from the main loop of a domain, so if we block
            // here, we're also blocking the loop that is supposed to drain the channel we're
            // blocking on. the argument for why this won't block is very similar to for
            // handle_replay. briefly:
            //
            //  - we know there's only one replay going on
            //  - we know there are no more Replay packets for this replay, since one with last:
            //    true must have been received for finish_replay to be triggered
            //  - therefore we know that no replay thread is running
            //  - since handle_replay will only send Packet::Finish once (when it receives last:
            //    true), we also know that it will not send again until the replay is over
            //  - the replay is over when we acknowedge the replay, which we haven't done yet
            //    (otherwise we'd be hitting the if branch above).
            match self.inject_tx
                      .as_mut()
                      .unwrap()
                      .try_send(Packet::Finish(tag, node)) {
                Ok(_) => {}
                Err(mpsc::TrySendError::Disconnected(_)) => {
                    // can't happen, since we know the reader thread (us) is still running
                    unreachable!();
                }
                Err(mpsc::TrySendError::Full(_)) => {
                    unreachable!();
                }
            }
        }
    }

    pub fn boot(mut self, rx: mpsc::Receiver<Packet>) -> thread::JoinHandle<()> {
        info!(self.log, "booting domain"; "nodes" => self.nodes.iter().count());
        let name: usize = self.nodes
            .iter()
            .next()
            .unwrap()
            .borrow()
            .domain()
            .into();
        thread::Builder::new()
            .name(format!("domain{}", name))
            .spawn(move || {
                let (inject_tx, inject_rx) = mpsc::sync_channel(1);
                let (back_tx, back_rx) = mpsc::channel();

                // construct select so we can receive on all channels at the same time
                let sel = mpsc::Select::new();
                let mut rx_handle = sel.handle(&rx);
                let mut inject_rx_handle = sel.handle(&inject_rx);
                let mut back_rx_handle = sel.handle(&back_rx);

                unsafe {
                    rx_handle.add();
                    inject_rx_handle.add();
                    back_rx_handle.add();
                }

                self.inject_tx = Some(inject_tx);

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
                    } else if id == back_rx_handle.id() {
                        back_rx_handle.recv()
                    } else {
                        unreachable!()
                    };

                    match m {
                        Err(_) => break,
                        Ok(Packet::Quit) => break,
                        Ok(Packet::RequestUnboundedTx(ack)) => {
                            ack.send(back_tx.clone()).unwrap();
                        }
                        Ok(m) => self.handle(m),
                    }
                }
            })
            .unwrap()
    }
}

#[cfg(test)]
impl Drop for Domain {
    fn drop(&mut self) {
        //println!("Dropping Domain!")
    }
}
