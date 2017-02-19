use petgraph::graph::NodeIndex;

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::sync::mpsc;
use std::time;

use std::collections::hash_map::Entry;

use flow::prelude::*;
use flow::domain::single::NodeDescriptor;

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

pub enum Control {
    AddNode(NodeDescriptor, Vec<LocalNodeIndex>),
    Ready(LocalNodeIndex, Option<usize>, mpsc::SyncSender<()>),
    SetupReplayPath(Tag, Vec<NodeAddress>, Option<mpsc::SyncSender<()>>, mpsc::SyncSender<()>),
    Replay(Tag, NodeAddress, mpsc::SyncSender<()>),
    PrepareState(LocalNodeIndex, usize),

    /// At the start of a migration, flush pending transactions then notify blender.
    StartMigration(i64, mpsc::SyncSender<()>),
    /// At the end of a migration, send the new timestamp and ingress_from_base counts.
    CompleteMigration(i64, HashMap<NodeIndex, usize>),
}

pub mod single;
pub mod local;

enum BufferedTransaction {
    RemoteTransaction,
    Transaction(NodeIndex, Vec<Message>),
    MigrationStart(mpsc::SyncSender<()>),
    MigrationEnd(HashMap<NodeIndex, usize>),
}

type InjectCh = Arc<Mutex<mpsc::SyncSender<Message>>>;

pub struct Domain {
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

    replaying_to: Option<(LocalNodeIndex, Vec<Message>)>,
    replay_paths: HashMap<Tag, (Vec<NodeAddress>, Option<mpsc::SyncSender<()>>)>,
}

impl Domain {
    pub fn new(log: Logger,
               nodes: DomainNodes,
               checktable: Arc<Mutex<checktable::CheckTable>>,
               ts: i64)
               -> Self {
        // initially, all nodes are not ready (except for timestamp egress nodes)!
        let not_ready = nodes.iter()
            .filter_map(|n| {
                use flow::node::Type;
                if let Type::TimestampEgress(..) = *n.borrow().inner {
                    return None;
                }

                Some(*n.borrow().addr().as_local())
            })
            .collect();

        Domain {
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
        }
    }

    pub fn dispatch(m: Message,
                    not_ready: &HashSet<LocalNodeIndex>,
                    replaying_to: &mut Option<(LocalNodeIndex, Vec<Message>)>,
                    states: &mut StateMap,
                    nodes: &DomainNodes,
                    enable_output: bool)
                    -> HashMap<NodeAddress, Vec<ops::Record>> {
        let me = m.to;
        let ts = m.ts;
        let mut output_messages = HashMap::new();

        if let Some((ref bufnode, ref mut buffered)) = *replaying_to {
            if bufnode == me.as_local() {
                buffered.push(m);
                return output_messages;
            }
        }
        if !not_ready.is_empty() && not_ready.contains(me.as_local()) {
            return output_messages;
        }

        let mut n = nodes[me.as_local()].borrow_mut();
        let mut u = n.process(m, states, nodes, true);
        drop(n);

        if ts.is_some() {
            // Any message with a timestamp (ie part of a transaction) must flow through the entire
            // graph, even if there are no updates associated with it.
            u = u.or_else(|| Some((Records::default(), ts, None)));
        }

        if u.is_none() {
            // no need to deal with our children if we're not sending them anything
            return output_messages;
        }

        let n = nodes[me.as_local()].borrow();
        for i in 0..n.children.len() {
            // avoid cloning if we can
            let (data, ts, token) = if i == n.children.len() - 1 {
                u.take().unwrap()
            } else {
                u.clone().unwrap()
            };

            if enable_output || !nodes[n.children[i].as_local()].borrow().is_output() {
                let m = Message {
                    from: me,
                    to: n.children[i],
                    replay: None,
                    data: data,
                    ts: ts,
                    token: token,
                };

                for (k, mut v) in Self::dispatch(m,
                                                 not_ready,
                                                 replaying_to,
                                                 states,
                                                 nodes,
                                                 enable_output) {
                    output_messages.entry(k).or_insert_with(Vec::new).append(&mut v);
                }
            } else {
                let mut data = data;
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
                 m: Message,
                 enable_output: bool)
                 -> HashMap<NodeAddress, Vec<ops::Record>> {
        Self::dispatch(m,
                       &self.not_ready,
                       &mut self.replaying_to,
                       &mut self.state,
                       &self.nodes,
                       enable_output)
    }

    pub fn transactional_dispatch(&mut self, messages: Vec<Message>) {
        assert!(!messages.is_empty());

        let mut egress_messages = HashMap::new();
        let ts = messages.iter().next().unwrap().ts;

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

            let m = Message {
                from: n.borrow().addr(), // TODO: message should be from actual parent, not self.
                to: n.borrow().addr(),
                replay: None,
                data: data,
                ts: ts,
                token: None,
            };

            if !self.not_ready.is_empty() && self.not_ready.contains(m.to.as_local()) {
                continue;
            }

            self.nodes[m.to.as_local()]
                .borrow_mut()
                .process(m, &mut self.state, &self.nodes, true);
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

    fn buffer_transaction(&mut self, m: Message) {
        let (ts, base) = m.ts.unwrap();

        // Insert message into buffer.
        match *self.buffered_transactions
            .entry(ts)
            .or_insert_with(|| BufferedTransaction::Transaction(base, vec![])) {
            BufferedTransaction::Transaction(_, ref mut messages) => messages.push(m),
            _ => unreachable!(),
        }

        if ts == self.ts + 1 {
            self.apply_transactions();
        }
    }

    pub fn boot(mut self,
                mut rx: mpsc::Receiver<Message>,
                timestamp_rx: mpsc::Receiver<i64>)
                -> mpsc::SyncSender<Control> {
        use std::thread;

        let (ctx, crx) = mpsc::sync_channel(16);

        info!(self.log, "booting domain"; "nodes" => self.nodes.iter().count());
        let name: usize = self.nodes.iter().next().unwrap().borrow().domain().into();
        thread::Builder::new()
            .name(format!("domain{}", name))
            .spawn(move || {
                // we want to keep around a second handle to the data channel so that we can access
                // it during replay. we know that that's safe, because while handle_control is
                // executing, we know we're not also using the Select or its handles.
                let secondary_rx = &mut rx as *mut _;
                let secondary_rx = unsafe { &mut *secondary_rx };

                let (inject_tx, inject_rx) = mpsc::sync_channel(1);
                let inject_tx = Arc::new(Mutex::new(inject_tx));

                // construct select so we can receive on all channels at the same time
                let sel = mpsc::Select::new();
                let mut rx_handle = sel.handle(&rx);
                let mut timestamp_rx_handle = sel.handle(&timestamp_rx);
                let mut control_rx_handle = sel.handle(&crx);
                let mut inject_rx_handle = sel.handle(&inject_rx);

                unsafe {
                    rx_handle.add();
                    timestamp_rx_handle.add();
                    control_rx_handle.add();
                    inject_rx_handle.add();
                }

                loop {
                    let id = sel.wait();
                    if id == control_rx_handle.id() {
                        let control = control_rx_handle.recv();
                        if control.is_err() {
                            return;
                        }
                        self.handle_control(control.unwrap(), secondary_rx, inject_tx.clone());
                    } else if id == timestamp_rx_handle.id() {
                        let ts = timestamp_rx_handle.recv();
                        if ts.is_err() {
                            return;
                        }
                        let ts = ts.unwrap();

                        let o = BufferedTransaction::RemoteTransaction;
                        let o = self.buffered_transactions.insert(ts, o);
                        assert!(o.is_none());

                        self.apply_transactions();
                    } else if id == inject_rx_handle.id() {
                        let m = inject_rx_handle.recv();
                        if m.is_err() {
                            return;
                        }
                        self.handle_message(m.unwrap(), secondary_rx, inject_tx.clone());
                    } else if id == rx_handle.id() {
                        let m = rx_handle.recv();
                        if m.is_err() {
                            return;
                        }
                        self.handle_message(m.unwrap(), secondary_rx, inject_tx.clone());
                    }
                }
            })
            .unwrap();

        ctx
    }

    fn handle_message(&mut self,
                      mut m: Message,
                      domain_rx: &mut mpsc::Receiver<Message>,
                      inject: InjectCh) {
        if m.replay.is_some() {
            if let Some((tag, ni)) = self.handle_replay(m, inject) {
                self.replay_done(tag, ni, domain_rx);
                trace!(self.log, "node is fully up-to-date"; "local" => ni.id());
            }
            return;
        }

        if let Some((token, send)) = m.token.take() {
            let ingress = self.nodes[m.to.as_local()].borrow();
            // TODO: is this the correct node?
            let base_node = self.nodes[ingress.children[0].as_local()].borrow().index;
            let result = self.checktable
                .lock()
                .unwrap()
                .claim_timestamp(&token, base_node, &m.data);
            match result {
                checktable::TransactionResult::Committed(i) => {
                    m.ts = Some((i, base_node));
                    m.token = None;
                    let _ = send.send(result);
                }
                checktable::TransactionResult::Aborted => {
                    let _ = send.send(result);
                    return;
                }
            }
        }

        match m.ts {
            None => {
                self.dispatch_(m, true);
            }
            Some(_) => {
                self.buffer_transaction(m);
            }
        }
    }

    fn handle_replay(&mut self, mut m: Message, inject: InjectCh) -> Option<(Tag, LocalNodeIndex)> {
        debug_assert!(m.replay.is_some());
        let replay = m.replay.take().unwrap();
        let tag = replay.0;
        let last = replay.1;
        m.replay = Some((tag, last, None));

        assert!(replay.2.is_none() || m.data.is_empty());
        assert!(replay.2.is_none() || last);
        let &mut (ref nodes, ref mut done) = self.replay_paths.get_mut(&tag).unwrap();

        // the replay source doesn't know who it is replaying to, so we need to set to correctly
        m.to = nodes[0];

        if replay.2.is_some() && nodes.len() == 1 {
            let state = replay.2.unwrap();

            // we've been given a state dump, and only have a single node in this domain that needs
            // to deal with that dump. chances are, we'll be able to re-use that state wholesale.
            let node = nodes[0];
            if done.is_some() {
                // oh boy, we're in luck! we're replaying into one of our nodes, and were just
                // given the entire state. no need to process or anything, just move in the state
                // and we're done.
                // TODO: fall back to regular replay here
                assert_eq!(self.state[node.as_local()].get_pkey(), state.get_pkey());
                self.state.insert(*node.as_local(), state);
                debug!(self.log, "direct state clone absorbed");
                return Some((tag, *node.as_local()));
            } else {
                use flow::node::Type;
                // if we're not terminal, and the domain only has a single node, that node *has* to
                // be an egress node (since we're relaying to another domain).
                let mut n = self.nodes[node.as_local()].borrow_mut();
                if let Type::Egress { .. } = *n.inner {
                    // we can just forward the state to the next domain without doing anything with
                    // it.
                    // TODO: egress node needs to know to only forward to *one* domain!
                    n.process(m, &mut self.state, &self.nodes, false);
                    drop(n);
                } else {
                    unreachable!();
                }
            }
            return None;
        }

        if let Some(state) = replay.2 {
            use std::thread;

            // we're been given an entire state snapshot, but we need to digest it piece by piece
            // spawn off a thread to do that chunking.
            let log = self.log.new(None);
            thread::Builder::new()
                .name(format!("replay{}.{}",
                              self.nodes.iter().next().unwrap().borrow().domain().index(),
                              m.from))
                .spawn(move || {
                    use itertools::Itertools;

                    let from = m.from;
                    let to = m.to;

                    let start = time::Instant::now();
                    debug!(log, "starting state chunker"; "node" => from.as_local().id());

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
                        let m = Message {
                            from: from,
                            to: to, // to will be overwritten by receiver
                            replay: Some((tag, iter.peek().is_none(), None)),
                            data: chunk,
                            ts: None,
                            token: None,
                        };

                        trace!(log, "sending batch"; "#" => i, "[]" => m.data.len());
                        inject.lock().unwrap().send(m).unwrap();
                    }

                    debug!(log, "state chunker finished"; "node" => from.as_local().id(), "μs" => dur_to_ns!(start.elapsed()) / 1000);
                }).unwrap();
            return None;
        }

        debug!(self.log, "replaying batch"; "#" => m.data.len());

        // forward the current message through all local nodes
        for (i, ni) in nodes.iter().enumerate() {
            // process the current message in this node
            let mut n = self.nodes[ni.as_local()].borrow_mut();
            let u = n.process(m, &mut self.state, &self.nodes, false);
            drop(n);

            if u.is_none() || i == nodes.len() - 1 {
                // the last condition is just to early-terminate so we don't unnecessarily
                // construct the last Message that is then immediately dropped.
                break;
            }

            // NOTE: the if above guarantees that nodes[i+1] will never go out of bounds
            m = Message {
                from: *ni,
                to: nodes[i + 1],
                replay: Some((replay.0, last, None)),
                data: u.unwrap().0,
                ts: None,
                token: None,
            };
        }

        if last && done.is_some() {
            let ni = *nodes.last().unwrap().as_local();
            trace!(self.log, "last batch received"; "local" => ni.id());
            return Some((tag, ni));
        }

        None
    }

    fn replay_done(&mut self, tag: Tag, node: LocalNodeIndex, rx: &mut mpsc::Receiver<Message>) {
        use std::time;

        // node is now ready, and should start accepting "real" updates
        trace!(self.log, "readying node"; "local" => node.id());
        self.not_ready.remove(&node);

        let start = time::Instant::now();
        let mut iterations = 0;
        while let Some((target, buffered)) = self.replaying_to.take() {
            assert_eq!(target, node);
            if buffered.is_empty() {
                break;
            }
            if iterations == 0 {
                info!(self.log, "starting backlog drain");
            }

            // some updates were propagated to this node during the migration. we need to replay
            // them before we take even newer updates. however, we don't want to completely block
            // the domain data channel, so we keep processing updates and backlogging them if
            // necessary.

            // we drain the buffered messages, and for every other message we process. we also
            // process a domain message. this has the effect of letting us catch up, but also not
            // stopping the domain entirely. we don't do this if there are fewer than 10 things
            // left, just to avoid the overhead of the switching.
            let switching = buffered.len() > 10;
            let mut even = true;

            debug!(self.log, "draining backlog"; "length" => buffered.len());

            // make sure any updates from rx that we handle, and that hit this node, are buffered
            // so we can get back to them later.
            if switching {
                self.replaying_to = Some((target, Vec::with_capacity(buffered.len() / 2)));
            }

            for m in buffered {
                // no transactions allowed here since we're still in a migration
                assert!(m.token.is_none());
                assert!(m.ts.is_none());
                if switching && !even {
                    // also process from rx
                    if let Ok(m) = rx.try_recv() {
                        // still no transactions allowed
                        assert!(m.token.is_none());
                        assert!(m.ts.is_none());

                        self.dispatch_(m, true);
                    }
                }
                even = !even;

                // NOTE: we cannot use self.dispatch_ here, because we specifically need to
                // override the buffering behavior that our self.replaying_to = Some above would
                // initiate.
                Self::dispatch(m,
                               &self.not_ready,
                               &mut None,
                               &mut self.state,
                               &self.nodes,
                               true);
            }

            iterations += 1;
        }

        if iterations != 0 {
            info!(self.log, "backlog drained"; "iterations" => iterations, "μs" => dur_to_ns!(start.elapsed()) / 1000);
        }

        if let Some(done_tx) = self.replay_paths.get_mut(&tag).and_then(|p| p.1.as_mut()) {
            info!(self.log, "acknowledging replay completed");
            done_tx.send(()).unwrap();
        } else {
            unreachable!()
        }
    }

    fn handle_control(&mut self,
                      c: Control,
                      domain_rx: &mut mpsc::Receiver<Message>,
                      inject: InjectCh) {
        match c {
            Control::AddNode(n, parents) => {
                use std::cell;
                let addr = *n.addr().as_local();
                self.not_ready.insert(addr);

                for p in parents {
                    self.nodes.get_mut(&p).unwrap().borrow_mut().children.push(n.addr());
                }
                self.nodes.insert(addr, cell::RefCell::new(n));
                trace!(self.log, "new node incorporated"; "local" => addr.id());
            }
            Control::Ready(ni, index_on, ack) => {
                if let Some(index_on) = index_on {
                    let mut s = {
                        let n = self.nodes[&ni].borrow();
                        if n.is_internal() && n.is_base() {
                            State::base()
                        } else {
                            State::default()
                        }
                    };
                    s.set_pkey(index_on);
                    assert!(self.state.insert(ni, s).is_none());
                } else {
                    // NOTE: just because index_on is None does *not* mean we're not materialized
                }

                if self.not_ready.remove(&ni) {
                    trace!(self.log, "readying empty node"; "local" => ni.id());
                }

                // swap replayed reader nodes to expose new state
                {
                    use flow::node::Type;
                    let mut n = self.nodes[&ni].borrow_mut();
                    if let Type::Reader(ref mut w, _) = *n.inner {
                        if let Some(ref mut state) = *w {
                            trace!(self.log, "swapping state"; "local" => ni.id());
                            state.swap();
                            trace!(self.log, "state swapped"; "local" => ni.id());
                        }
                    }
                }

                drop(ack);
            }
            Control::PrepareState(ni, on) => {
                let mut state = State::default();
                state.set_pkey(on);
                self.state.insert(ni, state);
            }
            Control::SetupReplayPath(tag, nodes, done, ack) => {
                // let coordinator know that we've registered the tagged path
                ack.send(()).unwrap();

                if done.is_some() {
                    info!(self.log, "tag" => tag.id(); "told about terminating replay path {:?}", nodes);
                    self.replaying_to = Some((*nodes.last().unwrap().as_local(), vec![]));
                } else {
                    info!(self.log, "tag" => tag.id(); "told about replay path {:?}", nodes);
                }
                self.replay_paths.insert(tag, (nodes, done));
            }
            Control::Replay(tag, node, ack) => {
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
                    .get(node.as_local())
                    .expect("migration replay path started with non-materialized node")
                    .clone();

                debug!(self.log, "current state cloned for replay"; "μs" => dur_to_ns!(start.elapsed()) / 1000);

                let m = Message {
                    from: node,
                    to: node,
                    replay: Some((tag, true, Some(state))),
                    data: Vec::<Vec<DataType>>::new().into(),
                    ts: None,
                    token: None,
                };

                if let Some((tag, ni)) = self.handle_replay(m, inject) {
                    self.replay_done(tag, ni, domain_rx);
                    trace!(self.log, "node is fully up-to-date"; "local" => ni.id());
                }
            }
            Control::StartMigration(ts, channel) => {
                let o = self.buffered_transactions
                    .insert(ts, BufferedTransaction::MigrationStart(channel));
                assert!(o.is_none());

                if ts == self.ts + 1 {
                    self.apply_transactions();
                }
            }
            Control::CompleteMigration(ts, ingress_from_base) => {
                let o = self.buffered_transactions
                    .insert(ts, BufferedTransaction::MigrationEnd(ingress_from_base));
                assert!(o.is_none());
                assert_eq!(ts, self.ts + 1);
                self.apply_transactions();
            }
        }
    }
}
