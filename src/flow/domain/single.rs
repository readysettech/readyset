use flow;
use petgraph::graph::NodeIndex;
use flow::prelude::*;

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
                   mut m: Packet,
                   state: &mut StateMap,
                   nodes: &DomainNodes,
                   swap: bool)
                   -> Packet {

        use flow::payload::TransactionState;
        let addr = *self.addr().as_local();
        match *self.inner {
            flow::node::Type::Ingress => {
                materialize(m.data(), state.get_mut(&addr));
                m
            }
            flow::node::Type::Reader(ref mut w, ref r) => {
                if let Some(ref mut state) = *w {
                    state.add(m.data().iter().cloned());
                    if let Packet::Transaction { state: TransactionState::Committed(ts, ..), .. } =
                        m {
                        state.update_ts(ts);
                    }

                    if swap {
                        state.swap();
                    }
                }

                let mut data = Some(m.take_data()); // so we can .take() for last tx
                let mut txs = r.streamers.lock().unwrap();
                let mut left = txs.len();

                // remove any channels where the receiver has hung up
                txs.retain(|tx| {
                    left -= 1;
                    if left == 0 {
                            tx.send(data.take()
                                        .unwrap()
                                        .into_iter()
                                        .map(|r| r.into())
                                        .collect())
                        } else {
                            tx.send(data.clone()
                                        .unwrap()
                                        .into_iter()
                                        .map(|r| r.into())
                                        .collect())
                        }
                        .is_ok()
                });

                // readers never have children
                Packet::None
            }
            flow::node::Type::Hook(ref mut h) => {
                if let &mut Some(ref mut h) = h {
                    h.on_input(m.take_data());
                } else {
                    unreachable!();
                }
                Packet::None
            }
            flow::node::Type::Egress { ref txs, ref tags } => {
                // send any queued updates to all external children
                let mut txs = txs.lock().unwrap();
                let txn = txs.len() - 1;

                debug_assert!(self.children.is_empty());

                // we need to find the ingress node following this egress according to the path
                // with replay.tag, and then forward this message only on the channel corresponding
                // to that ingress node.
                let replay_to = if let Packet::Replay { tag, .. } = m {
                    Some(tags.lock()
                        .unwrap()
                        .get(&tag)
                        .map(|n| *n)
                        .expect("egress node told about replay message, but not on replay path"))
                } else {
                    None
                };

                let mut m = Some(m); // so we can use .take()
                for (txi, &mut (ref globaddr, dst, ref mut tx)) in txs.iter_mut().enumerate() {
                    let mut take = txi == txn;
                    if let Some(replay_to) = replay_to.as_ref() {
                        if replay_to == globaddr {
                            take = true;
                        } else {
                            continue;
                        }
                    }

                    // avoid cloning if this is last send
                    let mut m = if take {
                        m.take().unwrap()
                    } else {
                        // we know this is a data (not a replay)
                        // because, a replay will force a take
                        m.as_ref().map(|m| m.clone_data()).unwrap()
                    };

                    m.link_mut().src = self.index.into();
                    m.link_mut().dst = dst;

                    if tx.send(m).is_err() {
                        // we must be shutting down...
                        break;
                    }

                    if take {
                        break;
                    }
                }
                debug_assert!(m.is_none());
                Packet::None
            }
            flow::node::Type::Internal(ref mut i) => {
                let from = m.link().src;
                m.map_data(|data| i.on_input(from, data, nodes, state).unwrap());
                materialize(m.data(), state.get_mut(&addr));
                m
            }
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
            Record::Positive(ref r) => state.insert(r.clone()),
            Record::Negative(ref r) => state.remove(r),
            Record::DeleteRequest(..) => unreachable!(),
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
