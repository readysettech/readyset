use flow;
use petgraph::graph::NodeIndex;
use flow::prelude::*;

use ops;

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
                   -> Option<Update> {

        // i wish we could use match here, but unfortunately the borrow checker isn't quite
        // sophisticated enough to deal with match and DerefMut in a way that lets us do it
        //
        //   https://github.com/rust-lang/rust/issues/37949
        //
        // so, instead, we have to do it in two parts
        if let flow::node::Type::Ingress(..) = *self.inner {
            self.materialize(&m.data, state);
            return Some(m.data);
        }

        if let flow::node::Type::Reader(_, ref mut w, ref r) = *self.inner {
            {
                if let Some(ref mut state) = *w {
                    match m.data {
                        ops::Update::Records(ref rs) => state.add(rs.iter().cloned()),
                    }
                    state.swap();
                }

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
            return None;
        }

        if let flow::node::Type::Egress(_, ref txs) = *self.inner {
            // send any queued updates to all external children
            let mut txs = txs.lock().unwrap();
            let txn = txs.len() - 1;

            let mut u = Some(m.data); // so we can use .take()
            for (txi, &mut (dst, ref mut tx)) in txs.iter_mut().enumerate() {
                if txi == txn && self.children.is_empty() {
                        tx.send(Message {
                            from: NodeAddress::make_global(self.index), // the ingress node knows where it should go
                            to: dst,
                            data: u.take().unwrap(),
                        })
                    } else {
                        tx.send(Message {
                            from: NodeAddress::make_global(self.index),
                            to: dst,
                            data: u.clone().unwrap(),
                        })
                    }
                    .unwrap();
            }

            debug_assert!(u.is_some() || self.children.is_empty());
            return u;
        }

        let u = if let flow::node::Type::Internal(_, ref mut i) = *self.inner {
            i.on_input(m, nodes, state)
        } else {
            unreachable!();
        };

        if let Some(ref u) = u {
            self.materialize(u, state);
        }
        u
    }

    fn materialize(&mut self, u: &Update, state: &mut StateMap) {
        // our output changed -- do we need to modify materialized state?
        materialize(u, state.get_mut(self.addr.as_local()));
    }

    pub fn is_internal(&self) -> bool {
        if let flow::node::Type::Internal(..) = *self.inner {
            true
        } else {
            false
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
