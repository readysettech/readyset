use flow::prelude::*;
use flow::node::NodeType;
use flow::payload;

impl Node {
    pub fn process(&mut self,
                   m: &mut Option<Box<Packet>>,
                   keyed_by: Option<usize>,
                   state: &mut StateMap,
                   nodes: &DomainNodes,
                   on_shard: Option<usize>,
                   swap: bool)
                   -> Vec<Miss> {
        m.as_mut().unwrap().trace(PacketEvent::Process);

        let addr = *self.local_addr();
        match self.inner {
            NodeType::Ingress => {
                let m = m.as_mut().unwrap();
                let mut misses = Vec::new();
                m.map_data(|rs| {
                               misses = materialize(rs,
                                                    *addr.as_local(),
                                                    state.get_mut(addr.as_local()));
                           });
                misses
            }
            NodeType::Reader(ref mut r) => {
                r.process(m, swap);
                vec![]
            }
            NodeType::Hook(ref mut h) => {
                if let &mut Some(ref mut h) = h {
                    h.on_input(m.take().unwrap().take_data());
                } else {
                    unreachable!();
                }
                vec![]
            }
            NodeType::Egress(None) => unreachable!(),
            NodeType::Egress(Some(ref mut e)) => {
                e.process(m, on_shard.unwrap_or(0));
                vec![]
            }
            NodeType::Sharder(ref mut s) => {
                s.process(m, *self.index.unwrap().as_global(), on_shard.is_some());
                vec![]
            }
            NodeType::Internal(ref mut i) => {
                let mut captured = false;
                let mut misses = Vec::new();
                let mut tracer;

                {
                    let m = m.as_mut().unwrap();
                    let from = m.link().src;

                    let nshards = match **m {
                        Packet::ReplayPiece { ref nshards, .. } => *nshards,
                        _ => 1,
                    };

                    let replay = if let Packet::ReplayPiece {
                               context: payload::ReplayPieceContext::Partial {
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

                        match i.on_input_raw(from,
                                             old_data,
                                             &mut tracer,
                                             replay,
                                             nshards,
                                             nodes,
                                             state) {
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

                // When a replay originates at a base node, we replay the data *through* that same
                // base node because its column set may have changed. However, this replay through
                // the base node itself should *NOT* update the materialization, because otherwise
                // it would duplicate each record in the base table every time a replay happens!
                //
                // So: only materialize if either (1) the message we're processing is not a replay,
                // or (2) if the node we're at is not a base.
                if m.is_regular() || i.get_base().is_none() {
                    m.map_data(|rs| {
                                   misses.extend(materialize(rs,
                                                             *addr.as_local(),
                                                             state.get_mut(addr.as_local())));
                               });
                }

                misses
            }
            NodeType::Source | NodeType::Dropped => unreachable!(),
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
