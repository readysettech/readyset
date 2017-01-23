use petgraph::graph::NodeIndex;

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::sync::mpsc;

use std::collections::hash_map::Entry;

use flow::prelude::*;
use flow::domain::single::NodeDescriptor;

use ops;
use checktable;

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

pub enum Control {
    AddNode(NodeDescriptor, Vec<LocalNodeIndex>),
    Ready(LocalNodeIndex, Option<State>, mpsc::SyncSender<()>),
    ReplayThrough(Vec<NodeAddress>,
                  mpsc::Receiver<Message>,
                  Option<mpsc::SyncSender<Message>>,
                  mpsc::SyncSender<()>),
    Replay(Vec<NodeAddress>, Option<mpsc::SyncSender<Message>>, mpsc::SyncSender<()>),
    PrepareState(LocalNodeIndex, usize),
}

pub mod single;
pub mod local;

pub struct Domain {
    nodes: DomainNodes,
    state: StateMap,

    /// Map from timestamp to vector of messages buffered for that timestamp. None in place of
    /// NodeIndex means that the timestamp will be skipped (because it is from a base node that
    /// never sends updates to this domain).
    buffered_transactions: HashMap<i64, (Option<NodeIndex>, Vec<Message>)>,
    /// Number of ingress nodes in the domain that receive updates from each base node. Base nodes
    /// that are only connected by timestamp ingress nodes are not included.
    ingress_from_base: HashMap<NodeIndex, usize>,
    /// Timestamp that the domain has seen all transactions up to.
    ts: i64,

    not_ready: HashSet<LocalNodeIndex>,

    checktable: Arc<Mutex<checktable::CheckTable>>,
}

impl Domain {
    pub fn new(nodes: DomainNodes,
               in_from_base: HashMap<NodeIndex, usize>,
               checktable: Arc<Mutex<checktable::CheckTable>>)
               -> Self {
        // initially, all nodes are not ready!
        let not_ready = nodes.iter().map(|n| *n.borrow().addr().as_local()).collect();

        Domain {
            nodes: nodes,
            state: StateMap::default(),
            buffered_transactions: HashMap::new(),
            ingress_from_base: in_from_base,
            not_ready: not_ready,
            ts: -1,
            checktable: checktable,
        }
    }

    pub fn dispatch(m: Message,
                    not_ready: &HashSet<LocalNodeIndex>,
                    states: &mut StateMap,
                    nodes: &DomainNodes,
                    enable_output: bool)
                    -> HashMap<NodeAddress, Vec<ops::Record>> {
        let me = m.to;
        let ts = m.ts;
        let mut output_messages = HashMap::new();

        if !not_ready.is_empty() && not_ready.contains(me.as_local()) {
            return output_messages;
        }

        let mut n = nodes[me.as_local()].borrow_mut();
        let mut u = n.process(m, states, nodes, true);
        drop(n);

        if ts.is_some() {
            // Any message with a timestamp (ie part of a transaction) must flow through the entire
            // graph, even if there are no updates associated with it.
            u = u.or(Some((Records::default(), ts, None)));
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

                for (k, mut v) in Self::dispatch(m, not_ready, states, nodes, enable_output)
                    .into_iter() {
                    output_messages.entry(k).or_insert(vec![]).append(&mut v);
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

    pub fn transactional_dispatch(&mut self, messages: Vec<Message>) {
        if messages.len() == 0 {
            return;
        }

        let mut egress_messages = HashMap::new();
        let ts = messages.iter().next().unwrap().ts;

        for m in messages {
            let new_messages =
                Self::dispatch(m, &self.not_ready, &mut self.state, &self.nodes, false);

            for (key, mut value) in new_messages.into_iter() {
                egress_messages.entry(key).or_insert(vec![]).append(&mut value);
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
            // Extract a complete set of messages for timestep (self.ts+1) if one exists.
            let messages = match self.buffered_transactions.entry(self.ts + 1) {
                Entry::Occupied(entry) => {
                    match entry.get().0 {
                        Some(base) => {
                            let messages_needed = self.ingress_from_base.get(&base).unwrap();
                            // If we don't have all the messages for this timestamp, then stop.
                            if entry.get().1.len() < *messages_needed {
                                break;
                            }

                            // Otherwise, extract this set of messages.
                            entry.remove().1
                        }
                        None => {
                            // If we learned about this timestamp from our timestamp ingress node,
                            // then skip it and move on to the next.
                            entry.remove();
                            self.ts += 1;
                            continue;
                        }
                    }
                }
                Entry::Vacant(_) => break,
            };

            // Process messages and advance to the next timestep.
            self.transactional_dispatch(messages);
            self.ts += 1;
        }
    }

    fn buffer_transaction(&mut self, m: Message) {
        let (ts, base) = m.ts.unwrap();

        // Insert message into buffer.
        self.buffered_transactions.entry(ts).or_insert((Some(base), vec![])).1.push(m);

        if ts == self.ts + 1 {
            self.apply_transactions();
        }
    }

    pub fn boot(mut self,
                rx: mpsc::Receiver<Message>,
                timestamp_rx: mpsc::Receiver<i64>)
                -> mpsc::SyncSender<Control> {
        use std::thread;

        let (ctx, crx) = mpsc::sync_channel(16);

        thread::spawn(move || {
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
                    self.handle_control(control.unwrap());
                } else if id == timestamp_rx_handle.id() {
                    let ts = timestamp_rx_handle.recv();
                    if ts.is_err() {
                        return;
                    }
                    let ts = ts.unwrap();

                    self.buffered_transactions.insert(ts, (None, vec![]));
                    self.apply_transactions();
                } else if id == rx_handle.id() {
                    let m = rx_handle.recv();
                    if m.is_err() {
                        return;
                    }
                    let mut m = m.unwrap();

                    if let Some((token, send)) = m.token.take() {
                        let ingress = self.nodes[m.to.as_local()].borrow();
                        let base_node = self.nodes[ingress.children[0].as_local()].borrow().index; // TODO: is this the correct node?
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
                            Self::dispatch(m, &self.not_ready, &mut self.state, &self.nodes, true);
                        }
                        Some(_) => {
                            self.buffer_transaction(m);
                        }
                    }
                }
            }
        });

        ctx
    }

    fn handle_control(&mut self, c: Control) {
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
            }
            Control::Ready(ni, state, ack) => {
                if let Some(state) = state {
                    self.state.insert(ni, state);
                } else {
                    // NOTE: just because state is None does *not* mean we're not materialized
                }

                // swap replayed reader nodes to expose new state
                use flow::node::Type;
                let mut n = self.nodes[&ni].borrow_mut();
                if let Type::Reader(ref mut w, _) = *n.inner {
                    if let Some(ref mut state) = *w {
                        state.swap();
                    }
                }

                self.not_ready.remove(&ni);
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

                // okay, I'm sorry in advance for this.
                // we have to have read-only reference to the state of the node we are replaying.
                // however, we *also* need to have a mutable reference to the states such that we
                // can call .process for each node we're operating on. and *that* is again
                // necessary because process() may need to update materialized state. and *that*
                // can happen even in this flow (where the whole reason we're replaying through
                // nodes is because they *aren't* materialized) because the target node of the
                // replay may *also* be one of ours.
                // so, to facilitate this, we stash away an &mut self.state here.
                let extra_mut_state: *mut _ = &mut self.state;

                // we know that nodes[0] is materialized, as the migration coordinator picks path
                // that originate with materialized nodes. if this weren't the case, we wouldn't be
                // able to do the replay, and the entire migration would fail.
                let state = self.state
                    .get(nodes[0].as_local())
                    .expect("migration replay path started with non-materialized node");

                let init_to = if nodes.len() == 1 { nodes[0] } else { nodes[1] };

                // process all records in state to completion within domain
                // and then forward on tx (if there is one)
                'chunks: for chunk in &state.iter().flat_map(|rs| rs).chunks(1000) {
                    let chunk: Records = chunk.into_iter().map(|r| r.clone().into()).collect();
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
                        //
                        // NOTE: the unsafe below is safe because
                        //
                        //   a) .process never modifies state, only its individual states
                        //   b) the state we have borrow immutably is for nodes[0]
                        //   c) we here iterate over all nodes that are *not* nodes[0]
                        //      (due to .skip(1)). the assertion should be unnecssary.
                        //
                        let mut n = self.nodes[ni.as_local()].borrow_mut();
                        assert!(ni != &nodes[0]);
                        let state: &mut _ = unsafe { &mut *extra_mut_state };
                        let u = n.process(m, state, &self.nodes, false);
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
                        tx.send(m).unwrap();
                    }
                }
            }
            Control::ReplayThrough(nodes, rx, mut tx, ack) => {
                // let coordinator know that we've entered replay loop
                ack.send(()).unwrap();

                // process all records in state to completion within domain
                // and then forward on tx (if there is one)
                'replay: for mut m in rx {
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
                        tx.send(m).unwrap();
                    }
                }
            }
        }
    }
}
