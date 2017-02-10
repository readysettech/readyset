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
const INTERBATCH_LIMIT: usize = 16;

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

pub enum ReplayBatch {
    Full(NodeAddress, State),
    Partial(Message),
}

pub enum Control {
    AddNode(NodeDescriptor, Vec<LocalNodeIndex>),
    Ready(LocalNodeIndex, Option<usize>, mpsc::SyncSender<()>),
    ReplayThrough(Vec<NodeAddress>,
                  mpsc::Receiver<ReplayBatch>,
                  Option<mpsc::SyncSender<ReplayBatch>>,
                  mpsc::SyncSender<()>),
    Replay(Vec<NodeAddress>, Option<mpsc::SyncSender<ReplayBatch>>, mpsc::SyncSender<()>),
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

                // construct select so we can receive on all channels at the same time
                let sel = mpsc::Select::new();
                let mut rx_handle = sel.handle(&rx);
                let mut timestamp_rx_handle = sel.handle(&timestamp_rx);
                let mut control_rx_handle = sel.handle(&crx);

                unsafe {
                    rx_handle.add();
                    timestamp_rx_handle.add();
                    control_rx_handle.add();
                }

                loop {
                    let id = sel.wait();
                    if id == control_rx_handle.id() {
                        let control = control_rx_handle.recv();
                        if control.is_err() {
                            return;
                        }
                        self.handle_control(control.unwrap(), secondary_rx);
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
                    } else if id == rx_handle.id() {
                        let m = rx_handle.recv();
                        if m.is_err() {
                            return;
                        }
                        let mut m = m.unwrap();

                        if let Some((token, send)) = m.token.take() {
                            let ingress = self.nodes[m.to.as_local()].borrow();
                            // TODO: is this the correct node?
                            let base_node =
                                self.nodes[ingress.children[0].as_local()].borrow().index;
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
                                    continue;
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
                }
            })
            .unwrap();

        ctx
    }

    fn replay_done(&mut self, node: LocalNodeIndex, rx: &mut mpsc::Receiver<Message>) {
        use std::time;

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
    }

    fn mid_replay_process(&mut self, rx: &mut mpsc::Receiver<Message>) {
        let mut left = INTERBATCH_LIMIT;
        while let Ok(m) = rx.try_recv() {
            // we know no transactions happen during migrations
            assert!(m.token.is_none());
            assert!(m.ts.is_none());

            self.dispatch_(m, true);

            // don't process too many things
            left -= 1;
            if left == 0 {
                break;
            }
        }
        trace!(self.log, "processed updates during replay"; "[]" => INTERBATCH_LIMIT - left);
    }

    fn handle_control(&mut self, c: Control, domain_rx: &mut mpsc::Receiver<Message>) {
        use itertools::Itertools;
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
                    self.state.insert(ni, s);
                } else {
                    // NOTE: just because index_on is None does *not* mean we're not materialized
                }

                trace!(self.log, "readying node"; "local" => ni.id());
                self.replay_done(ni, domain_rx);
                trace!(self.log, "replay finished"; "local" => ni.id());

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
            Control::Replay(nodes, mut tx, ack) => {
                // let coordinator know that we've entered replay loop
                ack.send(()).unwrap();

                // check for stupidity
                assert!(!nodes.is_empty());

                let start = time::Instant::now();
                info!(self.log, "starting replay");

                // we know that nodes[0] is materialized, as the migration coordinator picks path
                // that originate with materialized nodes. if this weren't the case, we wouldn't be
                // able to do the replay, and the entire migration would fail.
                //
                // we clone the entire state so that we can continue to occasionally process
                // incoming updates to the domain without disturbing the state that is being
                // replayed.
                let state: State = self.state
                    .get(nodes[0].as_local())
                    .expect("migration replay path started with non-materialized node")
                    .clone();

                debug!(self.log, "current state cloned for replay"; "μs" => dur_to_ns!(start.elapsed()) / 1000);

                if nodes.len() == 1 {
                    // now, we can just send our entire state in one go, rather than chunk it. this
                    // will be much faster than iterating over the map one-by-one and cloning each
                    // record. furthermore, it allows the receiver to simply replace their current
                    // empty state with this state if it is not passing thorugh other nodes.
                    if let Some(tx) = tx {
                        trace!(self.log, "sending full state");
                        tx.send(ReplayBatch::Full(nodes[0], state)).unwrap();
                    } else {
                        // replaying a single node has no purpose if there isn't someone we're
                        // sending to.
                        unreachable!()
                    }
                    info!(self.log, "replay done using shortcut"; "μs" => dur_to_ns!(start.elapsed()) / 1000);
                    return;
                }

                // TODO: in the special case where nodes.len() == 2 and tx.is_none(), and we
                // literally just need a copy of the state, we could early-terminate here.

                // since we must have more than one node, this is safe
                let init_to = nodes[1];

                if tx.is_none() {
                    // the sink node is in this domain. make sure we buffer any updates that get
                    // propagated to it during the migration (as they logically follow the state
                    // snapshot that is being replayed to it).
                    trace!(self.log, "domain is also replay target");
                    self.replaying_to = Some((*nodes.last().as_ref().unwrap().as_local(),
                                              Vec::new()));
                }

                // process all records in state to completion within domain
                // and then forward on tx (if there is one)
                'chunks: for (i, chunk) in state.into_iter()
                    .flat_map(|(_, rs)| rs)
                    .chunks(BATCH_SIZE)
                    .into_iter()
                    .enumerate() {
                    use std::iter::FromIterator;
                    let chunk = Records::from_iter(chunk.into_iter());
                    let mut m = Message {
                        from: nodes[0],
                        to: init_to,
                        data: chunk,
                        ts: None,
                        token: None,
                    };

                    // forward the current chunk through all local nodes
                    for (i, ni) in nodes.iter().enumerate().skip(1) {
                        // process the current chunk in this node
                        let mut n = self.nodes[ni.as_local()].borrow_mut();
                        assert!(ni != &nodes[0]);
                        let u = n.process(m, &mut self.state, &self.nodes, false);
                        drop(n);

                        if u.is_none() {
                            continue 'chunks;
                        }

                        m = Message {
                            from: *ni,
                            to: *ni,
                            data: u.unwrap().0,
                            ts: None,
                            token: None,
                        };

                        if i != nodes.len() - 1 {
                            m.to = nodes[i + 1];
                        } else {
                            // to is overwritten by receiving domain. from doesn't need to be set
                            // to the egress, because the ingress ignores it. setting it to this
                            // node is basically just as correct.
                        }
                    }

                    if let Some(tx) = tx.as_mut() {
                        trace!(self.log, "sending batch"; "#" => i, "[]" => m.data.len());
                        tx.send(ReplayBatch::Partial(m)).unwrap();
                    }

                    // NOTE: at this point, the downstream domain is probably busy handling our
                    // replayed message. We take this opportunity to process some updates from our
                    // upstream.
                    // TODO: don't do this for the last batch?
                    self.mid_replay_process(domain_rx);
                }

                if tx.is_none() {
                    // we must mark the node as ready immediately, otherwise it might miss updates
                    // that follow the replay, but precede the ready.
                    self.replay_done(*nodes.last().unwrap().as_local(), domain_rx);
                }

                info!(self.log, "replay done"; "μs" => dur_to_ns!(start.elapsed()) / 1000);
            }
            Control::ReplayThrough(nodes, rx, mut tx, ack) => {
                // let coordinator know that we've entered replay loop
                ack.send(()).unwrap();

                let start = time::Instant::now();
                info!(self.log, "ready for replay");

                // a couple of shortcuts first...
                // if nodes.len() == 1, we know we're an ingress node, and we can just stuff the
                // state directly into it. we *also* know that that ingress is the node whose state
                // is being rebuilt.
                if nodes.len() == 1 {
                    assert!(self.nodes[nodes[0].as_local()].borrow().is_ingress());
                    assert!(tx.is_none());
                    for (i, batch) in rx.into_iter().enumerate() {
                        match batch {
                            ReplayBatch::Full(_, state) => {
                                // oh boy, we're in luck! we just sent the full state we need for
                                // this node. no need to process or anything, just move in the
                                // state and we're done.
                                // TODO: fall back to regular replay here
                                assert_eq!(self.state[nodes[0].as_local()].get_pkey(),
                                           state.get_pkey());
                                self.state.insert(*nodes[0].as_local(), state);
                                debug!(self.log, "direct state clone absorbed");
                                break;
                            }
                            ReplayBatch::Partial(m) => {
                                let state = self.state.get_mut(nodes[0].as_local()).unwrap();
                                for r in m.data.into_iter() {
                                    match r {
                                        ops::Record::Positive(r) => state.insert(r),
                                        ops::Record::Negative(ref r) => state.remove(r),
                                    }
                                }
                                debug!(self.log, "direct state absorption of batch"; "#" => i);
                                // TODO self.mid_replay_process(domain_rx);
                            }
                        }
                    }
                    self.replay_done(*nodes[0].as_local(), domain_rx);
                    info!(self.log, "replay completed using shortcut"; "μs" => dur_to_ns!(start.elapsed()) / 1000);
                    return;
                }

                let rx = BatchedIterator::new(rx, nodes[0]);

                if tx.is_none() {
                    // the sink node is in this domain. make sure we buffer any updates that get
                    // propagated to it during the migration (as they logically follow the state
                    // snapshot that is being replayed to it).
                    trace!(self.log, "domain is replay target");
                    self.replaying_to = Some((*nodes.last().as_ref().unwrap().as_local(),
                                              Vec::new()));
                }

                // process all records in state to completion within domain
                // and then forward on tx (if there is one)
                let mut i = 0;
                'replay: for m in rx {
                    if let ReplayMessage::Batch(mut m) = m {
                        debug!(self.log, "forwarding batch"; "#" => i);

                        // forward the current message through all local nodes
                        for (i, ni) in nodes.iter().enumerate() {
                            // process the current message in this node
                            let mut n = self.nodes[ni.as_local()].borrow_mut();
                            let u = n.process(m, &mut self.state, &self.nodes, false);
                            drop(n);

                            if u.is_none() {
                                continue 'replay;
                            }

                            m = Message {
                                from: *ni,
                                to: *ni,
                                data: u.unwrap().0,
                                ts: None,
                                token: None,
                            };

                            if i != nodes.len() - 1 {
                                m.to = nodes[i + 1];
                            } else {
                                // to is overwritten by receiving domain. from doesn't need to be set
                                // to the egress, because the ingress ignores it. setting it to this
                                // node is basically just as correct.
                            }
                        }

                        if let Some(tx) = tx.as_mut() {
                            trace!(self.log, "sending batch"; "#" => m.data.len());
                            tx.send(ReplayBatch::Partial(m)).unwrap();
                        }
                        i += 1;
                    }

                    // NOTE: at this point, the downstream domain is probably busy handling our
                    // replayed message. We take this opportunity to process some updates from our
                    // upstream.
                    // TODO: don't do this for the last batch?
                    self.mid_replay_process(domain_rx);
                }

                if tx.is_none() {
                    // we must mark the node as ready immediately, otherwise it might miss updates
                    // that follow the replay, but precede the ready.
                    self.replay_done(*nodes.last().unwrap().as_local(), domain_rx);
                }

                info!(self.log, "replay completed"; "μs" => dur_to_ns!(start.elapsed()) / 1000);
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

use std::collections::hash_map;
struct BatchedIterator {
    rx: mpsc::Receiver<ReplayBatch>,
    state_iter: Option<hash_map::IntoIter<DataType, Vec<Arc<Vec<DataType>>>>>,
    to: NodeAddress,
    from: Option<NodeAddress>,
}

impl BatchedIterator {
    fn new(rx: mpsc::Receiver<ReplayBatch>, to: NodeAddress) -> Self {
        BatchedIterator {
            rx: rx,
            state_iter: None,
            to: to,
            from: None,
        }
    }
}

enum ReplayMessage {
    Batch(Message),
    Continue,
}

impl Iterator for BatchedIterator {
    type Item = ReplayMessage;
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(ref mut state_iter) = self.state_iter {
            let from = self.from.unwrap();
            let to = self.to;
            let mut rs = Vec::with_capacity(BATCH_SIZE);
            while let Some((_, next)) = state_iter.next() {
                rs.extend(next);
                if rs.len() >= BATCH_SIZE {
                    break;
                }
            }
            if rs.is_empty() {
                None
            } else {
                use std::iter::FromIterator;
                Some(ReplayMessage::Batch(Message {
                    from: from,
                    to: to,
                    data: FromIterator::from_iter(rs.into_iter()),
                    ts: None,
                    token: None,
                }))
            }
        } else {
            match self.rx.try_recv() {
                Err(mpsc::TryRecvError::Disconnected) => None,
                Err(mpsc::TryRecvError::Empty) => Some(ReplayMessage::Continue),
                Ok(ReplayBatch::Partial(m)) => Some(ReplayMessage::Batch(m)),
                Ok(ReplayBatch::Full(from, state)) => {
                    self.from = Some(from);
                    self.state_iter = Some(state.into_iter());
                    self.next()
                }
            }
        }
    }
}
