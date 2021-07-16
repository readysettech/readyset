use crate::node::special::base::BaseWrite;
use crate::node::NodeType;
use crate::payload;
use crate::prelude::*;
use core::convert::TryInto;
use launchpad::hash::hash;
use noria::consistency::Timestamp;
use noria::errors::ReadySetResult;
use noria::{internal, KeyComparison, PacketData, ReadySetError, ReplicationOffset};
use slog::Logger;
use std::collections::HashSet;
use std::mem;

/// The results of running a forward pass on a node
#[derive(Debug, PartialEq, Eq, Default)]
pub(crate) struct NodeProcessingResult {
    /// The values that we failed to lookup
    pub(crate) misses: Vec<Miss>,

    /// The lookups that were performed during processing
    pub(crate) lookups: Vec<Lookup>,

    /// Keys for replays captured during processing
    pub(crate) captured: HashSet<KeyComparison>,
}

impl NodeProcessingResult {
    /// Returns a vector of the unique contents of `misses`, by only the columns that were missed on
    ///
    /// This isn't as simple as just returning a `HashSet` or similar structure of misses, since we
    /// don't care about the columns in the record that aren't part of the key
    pub(crate) fn unique_misses(&self) -> Vec<&Miss> {
        let mut res: Vec<&Miss> = self.misses.iter().collect();
        res.sort_unstable_by(|a, b| {
            a.on.cmp(&b.on)
                .then_with(|| a.replay_cols.cmp(&b.replay_cols))
                .then_with(|| a.lookup_idx.cmp(&b.lookup_idx))
                .then_with(|| a.lookup_cols.cmp(&b.lookup_cols))
                // we only need *some* sort of stable ordering on KeyComparisons - we don't require
                // any semantics here. Keys aren't Ord (since they might contain ranges, which don't
                // have good ordering semantics) so we just compare the hashes
                .then_with(|| hash(&a.lookup_key()).cmp(&hash(&b.lookup_key())))
                .then_with(|| hash(&a.replay_key()).cmp(&hash(&b.replay_key())))
        });
        res.dedup();
        res
    }
}

impl Node {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn process(
        &mut self,
        m: &mut Option<Box<Packet>>,
        keyed_by: Option<&Vec<usize>>,
        state: &mut StateMap,
        nodes: &DomainNodes,
        on_shard: Option<usize>,
        swap: bool,
        replay_path: Option<&crate::domain::ReplayPath>,
        ex: &mut dyn Executor,
        // if true, forces a materialization into all indices of the node, not just those for the
        // current replay path tag
        materialize_into_all: bool,
        log: &Logger,
    ) -> Result<NodeProcessingResult, ReadySetError> {
        let addr = self.local_addr();
        let gaddr = self.global_addr();
        match self.inner {
            NodeType::Ingress => {
                let m = m.as_mut().unwrap();
                let tag = m.tag();
                m.map_data(|rs| {
                    materialize(rs, None, tag, state.get_mut(addr));
                });
            }
            NodeType::Base(ref mut b) => {
                // NOTE: bases only accept BaseOperations
                match m.take().map(|p| *p) {
                    Some(Packet::Input {
                        inner, mut senders, ..
                    }) => {
                        let PacketData { dst, data } = unsafe { inner.take() };
                        let data = data
                            .try_into()
                            .expect("Payload of Input packet was not of Input type");
                        let BaseWrite {
                            records: mut rs,
                            replication_offset,
                        } = b.process(addr, data, &*state)?;

                        // When a replay originates at a base node, we replay the data *through* that
                        // same base node because its column set may have changed. However, this replay
                        // through the base node itself should *NOT* update the materialization,
                        // because otherwise it would duplicate each record in the base table every
                        // time a replay happens!
                        //
                        // So: only materialize if the message we're processing is not a replay!
                        if keyed_by.is_none() {
                            materialize(&mut rs, replication_offset, None, state.get_mut(addr));
                        }

                        // Send write-ACKs to all the clients with updates that made
                        // it into this merged packet:
                        senders.drain(..).for_each(|src| ex.ack(src));

                        *m = Some(Box::new(Packet::Message {
                            link: Link::new(dst, dst),
                            data: rs,
                        }));
                    }
                    Some(ref p) => {
                        // TODO: replays?
                        unreachable!("base received non-input packet {:?}", p);
                    }
                    None => unreachable!(),
                }
            }
            NodeType::Reader(ref mut r) => {
                r.process(m, swap);
            }
            NodeType::Egress(None) => unreachable!(),
            NodeType::Egress(Some(ref mut e)) => {
                e.process(m, keyed_by.map(Vec::as_slice), on_shard.unwrap_or(0), ex)?;
            }
            NodeType::Sharder(ref mut s) => {
                s.process(
                    m,
                    addr,
                    on_shard.is_some(),
                    replay_path.and_then(|rp| rp.partial_unicast_sharder.map(|ni| ni == gaddr)),
                    ex,
                );
            }
            NodeType::Internal(ref mut i) => {
                let mut captured_full = false;
                let mut captured = HashSet::new();
                let mut misses = Vec::new();
                let mut lookups = Vec::new();

                {
                    let m = m.as_mut().unwrap();
                    let from = m.src();

                    let (data, replay) = match **m {
                        Packet::ReplayPiece {
                            tag,
                            ref mut data,
                            context:
                                payload::ReplayPieceContext::Partial {
                                    ref for_keys,
                                    requesting_shard,
                                    unishard,
                                    ignore,
                                },
                            ..
                        } => {
                            assert!(!ignore);
                            assert!(keyed_by.is_some());
                            (
                                data,
                                ReplayContext::Partial {
                                    key_cols: keyed_by.unwrap(),
                                    keys: for_keys,
                                    requesting_shard,
                                    unishard,
                                    tag,
                                },
                            )
                        }
                        Packet::ReplayPiece {
                            ref mut data,
                            context: payload::ReplayPieceContext::Regular { last },
                            ..
                        } => (data, ReplayContext::Full { last }),
                        Packet::Message { ref mut data, .. } => (data, ReplayContext::None),
                        _ => unreachable!(),
                    };

                    let mut set_replay_last = None;
                    // we need to own the data
                    let old_data = mem::take(data);

                    match i.on_input_raw(ex, from, old_data, replay, nodes, state, log)? {
                        RawProcessingResult::Regular(m) => {
                            *data = m.results;
                            lookups = m.lookups;
                            misses = m.misses;
                        }
                        RawProcessingResult::CapturedFull => {
                            captured_full = true;
                        }
                        RawProcessingResult::ReplayPiece {
                            rows,
                            keys: emitted_keys,
                            captured: were_captured,
                        } => {
                            // we already know that m must be a ReplayPiece since only a
                            // ReplayPiece can release a ReplayPiece.
                            // NOTE: no misses or lookups here since this is a union
                            *data = rows;
                            captured = were_captured;
                            if let Packet::ReplayPiece {
                                context:
                                    payload::ReplayPieceContext::Partial {
                                        ref mut for_keys, ..
                                    },
                                ..
                            } = **m
                            {
                                *for_keys = emitted_keys;
                            } else {
                                unreachable!();
                            }
                        }
                        RawProcessingResult::FullReplay(rs, last) => {
                            // we already know that m must be a (full) ReplayPiece since only a
                            // (full) ReplayPiece can release a FullReplay
                            *data = rs;
                            set_replay_last = Some(last);
                        }
                    }

                    if let Some(new_last) = set_replay_last {
                        if let Packet::ReplayPiece {
                            context: payload::ReplayPieceContext::Regular { ref mut last },
                            ..
                        } = **m
                        {
                            *last = new_last;
                        } else {
                            unreachable!();
                        }
                    }

                    if let Packet::ReplayPiece {
                        context:
                            payload::ReplayPieceContext::Partial {
                                ref mut unishard, ..
                            },
                        ..
                    } = **m
                    {
                        // hello, it's me again.
                        //
                        // on every replay path, there are some number of shard mergers, and
                        // some number of sharders.
                        //
                        // if the source of a replay is sharded, and the upquery key matches
                        // the sharding key, then only the matching shard of the source will be
                        // queried. in that case, the next shard merger (if there is one)
                        // shouldn't wait for replays from other shards, since none will
                        // arrive. the same is not true for any _subsequent_ shard mergers
                        // though, since sharders along a replay path send to _all_ shards
                        // (modulo the last one if the destination is sharded, but then there
                        // is no shard merger after it).
                        //
                        // to ensure that this is in fact what happens, we need to _unset_
                        // unishard once we've passed the first shard merger, so that it is not
                        // propagated to subsequent unions.
                        if let NodeOperator::Union(ref u) = i {
                            if u.is_shard_merger() {
                                *unishard = false;
                            }
                        }
                    }
                }

                if captured_full {
                    *m = None;
                    return Ok(Default::default());
                }

                let m = m.as_mut().unwrap();
                let tag = match **m {
                    Packet::ReplayPiece {
                        tag,
                        context: payload::ReplayPieceContext::Partial { .. },
                        ..
                    } if !materialize_into_all => {
                        // NOTE: non-partial replays shouldn't be materialized only for a
                        // particular index, and so the tag shouldn't be forwarded to the
                        // materialization code. this allows us to keep some asserts deeper in
                        // the code to check that we don't do partial replays to non-partial
                        // indices, or for unknown tags.
                        Some(tag)
                    }
                    _ => None,
                };
                m.map_data(|rs| {
                    materialize(rs, None, tag, state.get_mut(addr));
                });

                for miss in misses.iter_mut() {
                    if miss.on != addr {
                        self.reroute_miss(nodes, miss)?;
                    }
                }

                return Ok(NodeProcessingResult {
                    misses,
                    lookups,
                    captured,
                });
            }
            NodeType::Dropped => {
                *m = None;
            }
            NodeType::Source => unreachable!(),
        }
        Ok(Default::default())
    }

    pub(crate) fn process_eviction(
        &mut self,
        from: LocalNodeIndex,
        key_columns: &[usize],
        keys: &[KeyComparison],
        tag: Tag,
        on_shard: Option<usize>,
        ex: &mut dyn Executor,
    ) -> ReadySetResult<()> {
        let addr = self.local_addr();
        match self.inner {
            NodeType::Base(..) => {}
            NodeType::Egress(Some(ref mut e)) => {
                e.process(
                    &mut Some(Box::new(Packet::EvictKeys {
                        link: Link {
                            src: addr,
                            dst: addr,
                        },
                        tag,
                        keys: keys.to_vec(),
                    })),
                    None,
                    on_shard.unwrap_or(0),
                    ex,
                )?;
            }
            NodeType::Sharder(ref mut s) => {
                s.process_eviction(key_columns, tag, keys, addr, on_shard.is_some(), ex);
            }
            NodeType::Internal(ref mut i) => {
                i.on_eviction(from, tag, keys);
            }
            NodeType::Reader(ref mut r) => {
                r.on_eviction(keys);
            }
            NodeType::Ingress => {}
            NodeType::Dropped => {}
            NodeType::Egress(None) | NodeType::Source => internal!(),
        }
        Ok(())
    }

    // When we miss in can_query_through, that miss is *really* in the can_query_through node's
    // ancestor. We need to ensure that a replay is done to there, not the query_through node
    // itself, by translating the Miss into the right parent.
    fn reroute_miss(&self, nodes: &DomainNodes, miss: &mut Miss) -> Result<(), ReadySetError> {
        let node = nodes[miss.on].borrow();
        if node.is_internal() && node.can_query_through() {
            let mut new_parent: Option<IndexPair> = None;
            for col in miss.lookup_idx.iter_mut() {
                let parents = node.resolve(*col)?.unwrap();
                assert_eq!(parents.len(), 1, "query_through with more than one parent");

                let (parent_global, parent_col) = parents[0];
                if let Some(p) = new_parent {
                    assert_eq!(
                        p.as_global(),
                        parent_global,
                        "query_through from different parents"
                    );
                } else {
                    let parent_node = nodes
                        .iter()
                        .filter_map(|(i, n)| {
                            match n.try_borrow() {
                                Ok(n) => Some(n),
                                Err(_) => {
                                    // 'self' can be skipped. It cannot be its own ancestor and
                                    // is expected to already be mutably borrowed.
                                    assert_eq!(i, self.local_addr(), "unexpected borrow failure");
                                    assert_ne!(
                                        self.global_addr(),
                                        parent_global,
                                        "rerouting back to self requested"
                                    );
                                    None
                                }
                            }
                        })
                        .find(|n| n.global_addr() == parent_global)
                        .unwrap();
                    let mut pair: IndexPair = parent_global.into();
                    pair.set_local(parent_node.local_addr());
                    new_parent = Some(pair);
                }

                *col = parent_col;
            }

            miss.on = *new_parent.unwrap();
            // Recurse in case the parent we landed at also is a query_through node:
            self.reroute_miss(nodes, miss)?;
        }
        Ok(())
    }

    pub(crate) fn process_timestamp(
        &mut self,
        m: Packet,
        executor: &mut dyn Executor,
    ) -> Option<Box<Packet>> {
        // TODO: not error handling compliant!
        let src_node = m.src();
        match m {
            Packet::Timestamp {
                link,
                src,
                timestamp,
            } => {
                let PacketData { dst, data } = unsafe { timestamp.take() };

                let timestamp: Timestamp =
                    data.try_into().expect("Packet data not of timestamp type");

                // Set the incoming timestamp in the current nodes map of
                // upstream timestamps.
                self.timestamps
                    .entry(src_node)
                    .and_modify(|e| {
                        *e = Timestamp::join(e, &timestamp);
                    })
                    .or_insert_with(|| timestamp.clone());

                // Calculate the minimum timestamp over all timestamps for each parent
                // of the node. If the node does not have a timestamp for any parent,
                // then the minimum timestamp is returned.
                let mut parent_timestamps: Vec<&Timestamp> = Vec::with_capacity(self.parents.len());
                let mut parent_without_timestamp = false;
                for parent in self.parents() {
                    match self.timestamps.get(parent) {
                        Some(t) => {
                            parent_timestamps.push(&t);
                        }
                        None => {
                            parent_without_timestamp = true;
                            break;
                        }
                    }
                }

                // If a node has no parents it is a base table node, we pass the
                // timestamp in the packet along.
                let timestamp = if self.parents().is_empty() {
                    self.timestamps.get(&src_node).unwrap().clone()
                } else if parent_without_timestamp {
                    // The empty timestamp is a placeholder for the minimum timestamp.
                    Timestamp::default()
                } else {
                    Timestamp::min(&parent_timestamps[..])
                };

                if let NodeType::Reader(ref mut r) = self.inner {
                    r.process_timestamp(timestamp);
                    return None;
                }

                // Create a link if one does not already exist. This only happens
                // at the base table. The domain is responsible for setting the
                // new dst for the link.
                let link = Some(link.unwrap_or_else(|| Link::new(src_node, src_node)));

                // We leave the link untouched, the domain will be responsible
                // for updating it.
                let p = Box::new(Packet::Timestamp {
                    link,
                    src: None,
                    timestamp: LocalOrNot::new(PacketData {
                        dst,
                        data: PacketPayload::Timestamp(timestamp),
                    }),
                });

                // Some node types require additional packet handling after aggregating
                // all parent node timestamps.
                match self.inner {
                    NodeType::Egress(Some(ref mut e)) => {
                        // TODO(justin): Should this use on_shard like process.
                        let p = &mut Some(p);
                        e.process(p, None, 0, executor).unwrap();
                        None
                    }
                    NodeType::Base(_) => {
                        executor.ack(src.unwrap());
                        Some(p)
                    }
                    _ => Some(p),
                }
            }
            _ => unreachable!("process_timestamp passed non timestamp packet."),
        }
    }
}

#[allow(clippy::borrowed_box)]
// crate visibility due to use by tests
pub(crate) fn materialize(
    rs: &mut Records,
    replication_offset: Option<ReplicationOffset>,
    partial: Option<Tag>,
    state: Option<&mut Box<dyn State>>,
) {
    // our output changed -- do we need to modify materialized state?
    if state.is_none() {
        // nope
        return;
    }

    // yes!
    state
        .unwrap()
        .process_records(rs, partial, replication_offset);
}
