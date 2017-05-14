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
        let children: Vec<_> = graph
            .neighbors_directed(node, petgraph::EdgeDirection::Outgoing)
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
                   m: &mut Option<Box<Packet>>,
                   keyed_by: Option<usize>,
                   state: &mut StateMap,
                   nodes: &DomainNodes,
                   swap: bool)
                   -> Vec<Miss> {
        m.as_mut().unwrap().trace(PacketEvent::Process);

        let addr = self.addr();
        match *self.inner {
            flow::node::Type::Ingress => {
                let m = m.as_mut().unwrap();
                let mut misses = Vec::new();
                m.map_data(|rs| {
                               misses = materialize(rs,
                                                    *addr.as_local(),
                                                    state.get_mut(addr.as_local()));
                           });
                misses
            }
            flow::node::Type::Reader(ref mut r) => {
                r.process(m, swap);
                vec![]
            }
            flow::node::Type::Hook(ref mut h) => {
                if let &mut Some(ref mut h) = h {
                    h.on_input(m.take().unwrap().take_data());
                } else {
                    unreachable!();
                }
                vec![]
            }
            flow::node::Type::Egress(None) => unreachable!(),
            flow::node::Type::Egress(Some(ref mut e)) => {
                debug_assert!(self.children.is_empty());
                e.process(m, self.index);
                vec![]
            }
            flow::node::Type::Internal(ref mut i) => {
                let mut captured = false;
                let mut misses = Vec::new();
                let mut tracer;

                {
                    let m = m.as_mut().unwrap();
                    let from = m.link().src;

                    let replay = if let Packet::ReplayPiece {
                               context: flow::payload::ReplayPieceContext::Partial {
                                   ref for_key,
                                   ignore,
                               },
                               ..
                           } = **m {
                        assert!(!ignore);
                        assert!(keyed_by.is_some());
                        assert_eq!(for_key.len(), 1);
                        Some((keyed_by.unwrap(), for_key[0].clone()))
                    } else {
                        None
                    };

                    tracer = m.tracer().and_then(|t| t.take());
                    m.map_data(|data| {
                        use std::mem;

                        // we need to own the data
                        let old_data = mem::replace(data, Records::default());

                        match i.on_input_raw(from, old_data, &mut tracer, replay, nodes, state) {
                            RawProcessingResult::Regular(m) => {
                                mem::replace(data, m.results);
                                misses = m.misses;
                            }
                            RawProcessingResult::ReplayPiece(rs) => {
                                // we already know that m must be a ReplayPiece since only a
                                // ReplayPiece can release a ReplayPiece.
                                mem::replace(data, rs);
                            }
                            RawProcessingResult::Captured => {
                                captured = true;
                            }
                        }
                    });
                }

                if captured {
                    use std::mem;
                    mem::replace(m, Some(box Packet::Captured));
                    return misses;
                }

                let m = m.as_mut().unwrap();
                if let Some(t) = m.tracer() {
                    *t = tracer.take();
                }

                m.map_data(|rs| {
                               misses.extend(materialize(rs,
                                                         *addr.as_local(),
                                                         state.get_mut(addr.as_local())));
                           });

                misses
            }
            flow::node::Type::Source => unreachable!(),
        }
    }
}

pub fn materialize(rs: &mut Records, node: LocalNodeIndex, state: Option<&mut State>) -> Vec<Miss> {
    // our output changed -- do we need to modify materialized state?
    if state.is_none() {
        // nope
        return Vec::new();
    }

    // yes!
    let mut holes = Vec::new();
    let state = state.unwrap();
    rs.retain(|r| {
        if state.is_partial() {
            // we need to check that we're not hitting any holes
            if let Some(columns) = state.hits_hole(r) {
                // we would need a replay of this update.
                holes.push(Miss {
                               node: node,
                               key: columns.into_iter().map(|&c| r[c].clone()).collect(),
                           });

                // we don't want to propagate records that miss
                return false;
            }
        }
        match *r {
            Record::Positive(ref r) => state.insert(r.clone()),
            Record::Negative(ref r) => state.remove(r),
            Record::DeleteRequest(..) => unreachable!(),
        }

        true
    });

    holes
}

use std::ops::Deref;
impl Deref for NodeDescriptor {
    type Target = Node;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
