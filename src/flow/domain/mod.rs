use petgraph::graph::NodeIndex;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::sync::mpsc;

use std::collections::hash_map::Entry;

use flow::prelude::*;

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

    checktable: Arc<Mutex<checktable::CheckTable>>,
}

impl Domain {
    pub fn new(nodes: DomainNodes,
               in_from_base: HashMap<NodeIndex, usize>,
               checktable: Arc<Mutex<checktable::CheckTable>>)
               -> Self {
        Domain {
            nodes: nodes,
            state: StateMap::default(),
            buffered_transactions: HashMap::new(),
            ingress_from_base: in_from_base,
            ts: -1,
            checktable: checktable,
        }
    }

    pub fn dispatch(m: Message,
                    states: &mut StateMap,
                    nodes: &DomainNodes,
                    enable_output: bool)
                    -> HashMap<NodeAddress, Vec<ops::Record>> {
        let me = m.to;
        let ts = m.ts;
        let mut output_messages = HashMap::new();

        let mut n = nodes[me.as_local()].borrow_mut();
        let mut u = n.process(m, states, nodes);
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

                for (k, mut v) in Self::dispatch(m, states, nodes, enable_output).into_iter() {
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
            let new_messages = Self::dispatch(m, &mut self.state, &self.nodes, false);

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

            self.nodes[m.to.as_local()]
                .borrow_mut()
                .process(m, &mut self.state, &self.nodes);
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
                            Self::dispatch(m, &mut self.state, &self.nodes, true);
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
        match c {
        }
    }
}
