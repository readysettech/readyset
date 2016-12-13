use flow;
use petgraph::graph::NodeIndex;
use flow::prelude::*;

use ops;
use checktable;

use std::sync;

macro_rules! broadcast {
    ($from:expr, $handoffs:ident, $m:expr, $children:expr) => {{
        let c = $children;
        let mut m = $m;
        m.from = $from;
        let mut m = Some(m); // so we can .take() below
        for (i, to) in c.iter().enumerate() {
            let u = if i == c.len() - 1 {
                m.take()
            } else {
                m.clone()
            };

            $handoffs.get_mut(to).unwrap().push_back(u.unwrap());
        }
    }}
}

pub struct NodeDescriptor {
    pub index: NodeIndex,
    pub addr: NodeAddress,
    pub inner: Node,
    pub children: Vec<NodeAddress>,
}

impl NodeDescriptor {
    pub fn process(&mut self,
                   m: Message,
                   state: &mut StateMap,
                   nodes: &DomainNodes)
                   -> Option<(Update,
                              Option<(i64, NodeIndex)>,
                              Option<(checktable::Token,
                                      sync::mpsc::Sender<checktable::TransactionResult>)>)> {

        match *self.inner {
            flow::node::Type::Ingress(..) => {
                materialize(&m.data, state.get_mut(self.addr.as_local()));
                Some((m.data, m.ts, m.token))
            }
            flow::node::Type::Reader(_, ref mut w, ref r) => {
                if let Some(ref mut state) = *w {
                    match m.data {
                        ops::Update::Records(ref rs) => state.add(rs.iter().cloned()),
                    }
                    if m.ts.is_some() {
                        state.update_ts(m.ts.unwrap().0);
                    }
                    state.swap();
                }

                let mut data = Some(m.data); // so we can .take() for last tx
                let mut txs = r.streamers.lock().unwrap();
                let mut left = txs.len();

                // remove any channels where the receiver has hung up
                txs.retain(|tx| {
                    left -= 1;
                    if left == 0 {
                            tx.send(data.take().unwrap())
                        } else {
                            tx.send(data.clone().unwrap())
                        }
                        .is_ok()
                });

                // readers never have children
                None
            }
            flow::node::Type::Egress(_, ref txs) => {
                // send any queued updates to all external children
                let mut txs = txs.lock().unwrap();
                let txn = txs.len() - 1;

                let ts = m.ts;
                let mut u = Some(m.data); // so we can use .take()
                for (txi, &mut (dst, ref mut tx)) in txs.iter_mut().enumerate() {
                    if txi == txn && self.children.is_empty() {
                        tx.send(Message {
                            from: NodeAddress::make_global(self.index), // the ingress node knows where it should go
                            to: dst,
                            data: u.take().unwrap(),
                            ts: m.ts.clone(),
                            token: None,
                        })
                    } else {
                        tx.send(Message {
                            from: NodeAddress::make_global(self.index),
                            to: dst,
                            data: u.clone().unwrap(),
                            ts: m.ts.clone(),
                            token: None,
                        })
                    }
                    .unwrap();
                }

                debug_assert!(u.is_some() || self.children.is_empty());
                u.map(|update| (update, ts, None))
            }
            flow::node::Type::Internal(_, ref mut i) => {
                let ts = m.ts;
                let u = i.on_input(m, nodes, state);
                if let Some(ref u) = u {
                    materialize(u, state.get_mut(self.addr.as_local()));
                }
                u.map(|update| (update, ts, None))
            }
            flow::node::Type::TimestampEgress(_, ref txs) => {
                if let Some((ts, _)) = m.ts {
                    let txs = txs.lock().unwrap();
                    for tx in txs.iter() {
                        tx.send(ts).unwrap();
                    }
                }
                None
            }
            flow::node::Type::TimestampIngress(..) |
            flow::node::Type::Unassigned(..) |
            flow::node::Type::Taken(..) |
            flow::node::Type::Source => unreachable!(),
        }
    }

    pub fn is_ingress(&self) -> bool {
        if let flow::node::Type::Ingress(..) = *self.inner {
            true
        } else {
            false
        }
    }

    pub fn is_internal(&self) -> bool {
        if let flow::node::Type::Internal(..) = *self.inner {
            true
        } else {
            false
        }
    }

    /// A node is considered to be an output node if changes to its state are visible outside of its domain.
    pub fn is_output(&self) -> bool {
        match *self.inner {
            flow::node::Type::Egress(_, _) => true,
            flow::node::Type::Reader(..) => true,
            _ => false,
        }
    }
}

pub fn materialize(u: &Update, state: Option<&mut State>) {
    // our output changed -- do we need to modify materialized state?
    if state.is_none() {
        // nope
        return;
    }

    // yes!
    let mut state = state.unwrap();
    match *u {
        ops::Update::Records(ref rs) => {
            for r in rs {
                match *r {
                    ops::Record::Positive(ref r) => state.insert(r.clone()),
                    ops::Record::Negative(ref r) => state.remove(r),
                }
            }
        }
    }
}

use std::ops::Deref;
impl Deref for NodeDescriptor {
    type Target = Node;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
