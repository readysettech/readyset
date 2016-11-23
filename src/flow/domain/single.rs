use std::collections::HashMap;
use std::collections::VecDeque;

use flow;
use flow::prelude::*;

use ops;
use shortcut;

pub struct NodeDescriptor {
    pub index: NodeIndex,
    pub inner: Node,
    pub children: Vec<NodeIndex>,
}

impl NodeDescriptor {
    pub fn iterate(&mut self,
                   handoffs: &mut HashMap<NodeIndex, VecDeque<Message>>,
                   state: &mut StateMap,
                   nodes: &NodeList) {
        match *self.inner {
            flow::node::Type::Ingress(_, ref mut rx) => {
                // receive an update
                debug_assert!(handoffs[&self.index].is_empty());
                broadcast!(handoffs, rx.recv().unwrap(), &self.children[..]);
                // TODO: may also need to materialize its output
            }
            flow::node::Type::Egress(_, ref txs) => {
                // send any queued updates to all external children
                let mut txs = txs.lock().unwrap();
                let txn = txs.len() - 1;
                while let Some(m) = handoffs.get_mut(&self.index).unwrap().pop_front() {
                    let mut m = Some(m); // so we can use .take()
                    for (txi, tx) in txs.iter_mut().enumerate() {
                        if txi == txn && self.children.is_empty() {
                            tx.send(m.take().unwrap()).unwrap();
                        } else {
                            tx.send(m.clone().unwrap()).unwrap();
                        }
                    }

                    if let Some(m) = m {
                        broadcast!(handoffs, m, &self.children[..]);
                    } else {
                        debug_assert!(self.children.is_empty());
                    }
                }
            }
            flow::node::Type::Internal(..) => {
                // we need &mut self
                // workaround for https://github.com/rust-lang/rust/issues/37949
            }
            _ => unreachable!(),
        }

        if let flow::node::Type::Internal(..) = *self.inner {
            while let Some(m) = handoffs.get_mut(&self.index).unwrap().pop_front() {
                if let Some(u) = self.process_one(m, state, nodes) {
                    broadcast!(handoffs,
                               Message {
                                   from: self.index,
                                   data: u,
                               },
                               &self.children[..]);
                }
            }
        }
    }

    fn process_one(&mut self,
                   m: Message,
                   state: &mut StateMap,
                   nodes: &NodeList)
                   -> Option<Update> {

        // first, process the incoming message
        let u = match *self.inner {
            flow::node::Type::Internal(_, ref mut i) => i.on_input(m, nodes, &state),
            _ => unreachable!(),
        };

        // if our output didn't change there's nothing more to do
        if u.is_none() {
            return u;
        }

        // our output changed -- do we need to modify materialized state?
        let state = state.get_mut(&self.index);
        if state.is_none() {
            // nope
            return u;
        }

        // yes!
        let mut state = state.unwrap();
        if let Some(ops::Update::Records(ref rs)) = u {
            for r in rs.iter().cloned() {
                match r {
                    ops::Record::Positive(r) => state.insert(r),
                    ops::Record::Negative(r) => {
                        // we need a cond that will match this row.
                        let conds = r.into_iter()
                            .enumerate()
                            .map(|(coli, v)| {
                                shortcut::Condition {
                                    column: coli,
                                    cmp: shortcut::Comparison::Equal(shortcut::Value::Const(v)),
                                }
                            })
                            .collect::<Vec<_>>();

                        // however, multiple rows may have the same values as this row for every
                        // column. afaict, it is safe to delete any one of these rows. we do this
                        // by returning true for the first invocation of the filter function, and
                        // false for all subsequent invocations.
                        let mut first = true;
                        state.delete_filter(&conds[..], |_| if first {
                            first = false;
                            true
                        } else {
                            false
                        });
                    }
                }
            }
        }

        u
    }
}

use std::ops::Deref;
impl Deref for NodeDescriptor {
    type Target = Node;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
