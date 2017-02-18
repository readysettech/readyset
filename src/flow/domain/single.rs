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
    pub inner: Node,
    pub children: Vec<NodeAddress>,
}

impl NodeDescriptor {
    pub fn new(graph: &mut Graph, node: NodeIndex) -> Self {
        use petgraph;

        let inner = graph.node_weight_mut(node).unwrap().take();
        let children: Vec<_> = graph.neighbors_directed(node, petgraph::EdgeDirection::Outgoing)
            .filter(|&c| graph[c].domain() == inner.domain())
            .map(|ni| graph[ni].addr())
            .collect();

        NodeDescriptor {
            index: node,
            inner: inner,
            children: children,
        }
    }

    pub fn process(&mut self,
                   m: Message,
                   state: &mut StateMap,
                   nodes: &DomainNodes,
                   swap: bool)
                   -> Option<(Records,
                              Option<(i64, NodeIndex)>,
                              Option<(checktable::Token,
                                      sync::mpsc::Sender<checktable::TransactionResult>)>)> {

        let addr = *self.addr().as_local();
        match *self.inner {
            flow::node::Type::Ingress => {
                materialize(&m.data, state.get_mut(&addr));
                Some((m.data, m.ts, m.token))
            }
            flow::node::Type::Reader(ref mut w, ref r) => {
                if let Some(ref mut state) = *w {
                    state.add(m.data.iter().cloned());
                    if m.ts.is_some() {
                        state.update_ts(m.ts.unwrap().0);
                    }

                    if swap {
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
                            tx.send(data.take().unwrap().into_iter().map(|r| r.into()).collect())
                        } else {
                            tx.send(data.clone().unwrap().into_iter().map(|r| r.into()).collect())
                        }
                        .is_ok()
                });

                // readers never have children
                None
            }
            flow::node::Type::Egress(ref txs) => {
                // send any queued updates to all external children
                let mut txs = txs.lock().unwrap();
                let txn = txs.len() - 1;

                if let Some(ref replay) = m.replay {
                    // TODO: we need to find the ingress node following this egress according to
                    // the path with replay.tag, and then forward this message only on the channel
                    // corresponding to that ingress node. unfortunately, we don't currently have
                    // that information here (we only have the tag)...
                    // TODO TODO TODO TODO TODO TODO
                    unimplemented!();
                }

                let ts = m.ts;
                let mut u = Some(m.data); // so we can use .take()
                for (txi, &mut (dst, ref mut tx)) in txs.iter_mut().enumerate() {
                    if txi == txn && self.children.is_empty() {
                            tx.send(Message {
                                // the ingress node knows where it should go
                                from: NodeAddress::make_global(self.index),
                                to: dst,
                                replay: None,
                                data: u.take().unwrap(),
                                ts: m.ts,
                                token: None,
                            })
                        } else {
                            tx.send(Message {
                                from: NodeAddress::make_global(self.index),
                                to: dst,
                                replay: None,
                                data: u.clone().unwrap(),
                                ts: m.ts,
                                token: None,
                            })
                        }
                        .unwrap();
                }

                debug_assert!(u.is_some() || self.children.is_empty());
                u.map(|update| (update, ts, None))
            }
            flow::node::Type::Internal(ref mut i) => {
                let ts = m.ts;
                let u = i.on_input(m.from, m.data, nodes, state);
                materialize(&u, state.get_mut(&addr));
                Some((u, ts, None))
            }
            flow::node::Type::TimestampEgress(ref txs) => {
                if let Some((ts, _)) = m.ts {
                    let txs = txs.lock().unwrap();
                    for tx in txs.iter() {
                        tx.send(ts).unwrap();
                    }
                }
                None
            }
            flow::node::Type::TimestampIngress(..) |
            flow::node::Type::Source => unreachable!(),
        }
    }
}

pub fn materialize(rs: &Records, state: Option<&mut State>) {
    // our output changed -- do we need to modify materialized state?
    if state.is_none() {
        // nope
        return;
    }

    // yes!
    let mut state = state.unwrap();
    for r in rs.iter() {
        match *r {
            ops::Record::Positive(ref r) => state.insert(r.clone()),
            ops::Record::Negative(ref r) => state.remove(r),
            ops::Record::DeleteRequest(..) => unreachable!(),
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
