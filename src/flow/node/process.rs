use flow::prelude::*;
use flow::node::NodeType;
use flow::payload;

impl Node {
    pub fn process(
        &mut self,
        m: &mut Option<Box<Packet>>,
        keyed_by: Option<usize>,
        state: &mut StateMap,
        nodes: &DomainNodes,
        on_shard: Option<usize>,
        swap: bool,
    ) -> Vec<Miss> {
        m.as_mut().unwrap().trace(PacketEvent::Process);

        let addr = *self.local_addr();
        match self.inner {
            NodeType::Ingress => {
                let m = m.as_mut().unwrap();
                let tag = m.tag();
                m.map_data(|rs| {
                    materialize(rs, tag, state.get_mut(&addr));
                });
                vec![]
            }
            NodeType::Reader(ref mut r) => {
                r.process(m, swap);
                vec![]
            }
            NodeType::Egress(None) => unreachable!(),
            NodeType::Egress(Some(ref mut e)) => {
                e.process(m, on_shard.unwrap_or(0));
                vec![]
            }
            NodeType::Sharder(ref mut s) => {
                s.process(m, addr, on_shard.is_some());
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
                    } = **m
                    {
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

                        match i.on_input_raw(
                            from,
                            old_data,
                            &mut tracer,
                            replay,
                            nshards,
                            nodes,
                            state,
                        ) {
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
                    let tag = m.tag();
                    m.map_data(|rs| {
                        materialize(rs, tag, state.get_mut(&addr));
                    });
                }

                misses
            }
            NodeType::Source | NodeType::Dropped => unreachable!(),
        }
    }
}

pub fn materialize(rs: &mut Records, partial: Option<Tag>, state: Option<&mut State>) {
    // our output changed -- do we need to modify materialized state?
    if state.is_none() {
        // nope
        return;
    }

    // yes!
    let state = state.unwrap();
    if state.is_partial() {
        rs.retain(|r| {
            // we need to check that we're not erroneously filling any holes
            // there are two cases here:
            //
            //  - if the incoming record is a partial replay (i.e., partial.is_some()), then we
            //    *know* that we are the target of the replay, and therefore we *know* that the
            //    materialization must already have marked the given key as "not a hole".
            //  - if the incoming record is a normal message (i.e., partial.is_none()), then we
            //    need to be careful. since this materialization is partial, it may be that we
            //    haven't yet replayed this `r`'s key, in which case we shouldn't forward that
            //    record! if all of our indices have holes for this record, there's no need for us
            //    to forward it. it would just be wasted work.
            //
            //    XXX: we could potentially save come computation here in joins by not forcing
            //    `right` to backfill the lookup key only to then throw the record away
            match *r {
                Record::Positive(ref r) => state.insert(r.clone(), partial),
                Record::Negative(ref r) => state.remove(r),
                Record::DeleteRequest(..) => unreachable!(),
            }
        });
    } else {
        for r in rs.iter() {
            match *r {
                Record::Positive(ref r) => {
                    let hit = state.insert(r.clone(), partial);
                    debug_assert!(hit);
                }
                Record::Negative(ref r) => {
                    let hit = state.remove(r);
                    debug_assert!(hit);
                }
                Record::DeleteRequest(..) => unreachable!(),
            }
        }
    }
}
