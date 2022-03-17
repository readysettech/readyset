use std::collections::{HashMap, HashSet};
use std::convert::TryInto;
use std::mem;

use dataflow_state::{MaterializedNodeState, SnapshotMode};
use noria::consistency::Timestamp;
use noria::replication::ReplicationOffset;
use noria::{KeyComparison, PacketData, ReadySetError};
use noria_errors::ReadySetResult;
use tracing::{debug_span, trace};

use crate::node::special::base::{BaseWrite, SetSnapshotMode};
use crate::node::NodeType;
use crate::payload;
use crate::prelude::*;
use crate::processing::{MissLookupKey, MissReplayKey};

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

/// A helper struct that combines unique misses for the same columns in the same node
struct MissSet<'a> {
    /// The node we missed when looking up into.
    on: LocalNodeIndex,
    /// The columns of `on` we were looking up on.
    lookup_idx: Vec<usize>,
    /// The key that we useed to do the lookup that resulted in the miss
    lookup_key: MissLookupKey,
    /// The replay key that was being processed during the lookup (if any)
    replay_key: Option<MissReplayKey>,
    set: HashMap<&'a [DataType], &'a Miss>,
}

impl<'a> MissSet<'a> {
    /// Create a new [`MissSet`] from a [`Miss`]
    fn from_miss(miss: &'a Miss) -> Self {
        let mut set = HashMap::new();
        set.insert(miss.record.as_slice(), miss);
        MissSet {
            on: miss.on,
            lookup_idx: miss.lookup_idx.clone(),
            lookup_key: miss.lookup_key.clone(),
            replay_key: miss.replay_key.clone(),
            set,
        }
    }

    /// Adds a [`Miss`] to the set, returns `true` iff it belongs to the set
    fn add(&mut self, miss: &'a Miss) -> bool {
        if miss.on == self.on
            && miss.lookup_idx == self.lookup_idx
            && miss.lookup_key == self.lookup_key
            && miss.replay_key == self.replay_key
        {
            self.set.insert(&miss.record, miss);
            true
        } else {
            false
        }
    }
}

impl NodeProcessingResult {
    /// Returns a vector of the unique contents of `misses`, by only the columns that were missed on
    pub(crate) fn unique_misses(&self) -> Vec<&Miss> {
        // Since a list of misses can be rather long, performing a sort on all the misses can be
        // rather expensive. On the other hand the misses will likely belong to only a
        // handful of queries (or even just one) and thus share the same parameters in most
        // cases. Therefore we simply perform multiple passes over the vector removing all
        // the entries that share the same characteristics each time. In most cases a single pass
        // will suffice.
        let mut misses = self.misses.iter();

        let first = match misses.next() {
            Some(miss) => miss,
            None => return Vec::new(),
        };

        let mut set = MissSet::from_miss(first);
        let mut filtered: Vec<_> = misses.filter(|miss| !set.add(miss)).collect();
        let mut sets = vec![set];

        while !filtered.is_empty() {
            let mut misses = filtered.iter().copied();
            let mut set = MissSet::from_miss(misses.next().unwrap());
            filtered = misses.filter(|miss| !set.add(miss)).collect();
            sets.push(set);
        }

        sets.into_iter().flat_map(|s| s.set.into_values()).collect()
    }
}

/// Information about the domain required by [`Node::process`].
pub(crate) struct ProcessEnv<'domain> {
    pub(crate) state: &'domain mut StateMap,
    pub(crate) nodes: &'domain DomainNodes,
    pub(crate) executor: &'domain mut dyn Executor,
    pub(crate) shard: Option<usize>,
}

impl Node {
    #[allow(clippy::unreachable)]
    pub(crate) fn process(
        &mut self,
        m: &mut Option<Box<Packet>>,
        keyed_by: Option<&Vec<usize>>,
        replay_path: Option<&crate::domain::ReplayPath>,
        swap_reader: bool,
        env: ProcessEnv,
    ) -> ReadySetResult<NodeProcessingResult> {
        let addr = self.local_addr();
        let gaddr = self.global_addr();

        let span = debug_span!("node:process", local = %addr, global = %gaddr.index());
        let _guard = span.enter();

        match self.inner {
            NodeType::Ingress => {
                let m = m.as_mut().unwrap();
                let tag = m.tag();
                m.map_data(|rs| {
                    materialize(rs, None, tag, env.state.get_mut(addr));
                });
            }
            NodeType::Base(ref mut b) => {
                // NOTE: bases only accept BaseOperations
                match m.take().map(|p| *p) {
                    Some(Packet::Input { inner, .. }) => {
                        let PacketData { dst, data, trace } = unsafe { inner.take() };
                        let data = data
                            .try_into()
                            .expect("Payload of Input packet was not of Input type");

                        let snapshot_mode = env
                            .state
                            .get(addr)
                            .and_then(|s| s.as_persistent())
                            .map(|p| p.snapshot_mode())
                            .unwrap_or(SnapshotMode::SnapshotModeDisabled);

                        let BaseWrite {
                            records: mut rs,
                            replication_offset,
                            set_snapshot_mode,
                        } = b.process(addr, data, &*env.state, snapshot_mode)?;

                        if let (Some(SetSnapshotMode::EnterSnapshotMode), Some(s)) = (
                            set_snapshot_mode,
                            env.state.get_mut(addr).and_then(|s| s.as_persistent_mut()),
                        ) {
                            s.set_snapshot_mode(SnapshotMode::SnapshotModeEnabled);
                        }

                        // When a replay originates at a base node, we replay the data *through*
                        // that same base node because its column set may
                        // have changed. However, this replay through the
                        // base node itself should *NOT* update the materialization,
                        // because otherwise it would duplicate each record in the base table every
                        // time a replay happens!
                        //
                        // So: only materialize if the message we're processing is not a replay!
                        if keyed_by.is_none() {
                            materialize(&mut rs, replication_offset, None, env.state.get_mut(addr));
                        }

                        if let (Some(SetSnapshotMode::FinishSnapshotMode), Some(s)) = (
                            set_snapshot_mode,
                            env.state.get_mut(addr).and_then(|s| s.as_persistent_mut()),
                        ) {
                            s.set_snapshot_mode(SnapshotMode::SnapshotModeDisabled);
                        }

                        *m = Some(Box::new(Packet::Message {
                            link: Link::new(dst, dst),
                            data: rs,
                            trace,
                        }));
                    }
                    Some(ref p) => {
                        // TODO: replays?
                        // TODO: Scoped for a future refactor:
                        // https://readysettech.atlassian.net/browse/ENG-455
                        unreachable!("base received non-input packet {:?}", p);
                    }
                    None => {
                        // The domain should never pass a None.
                        unreachable!("the domain should never pass a None packet to process")
                    }
                }
            }
            NodeType::Reader(ref mut r) => {
                r.process(m, swap_reader);
            }
            NodeType::Egress(None) => internal!("tried to process through taken egress"),
            NodeType::Egress(Some(ref mut e)) => {
                e.process(
                    m,
                    keyed_by.map(Vec::as_slice),
                    env.shard.unwrap_or(0),
                    env.executor,
                )?;
            }
            NodeType::Sharder(ref mut s) => {
                s.process(
                    m,
                    addr,
                    env.shard.is_some(),
                    replay_path.and_then(|rp| rp.partial_unicast_sharder.map(|ni| ni == gaddr)),
                    env.executor,
                )?;
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
                                },
                            ..
                        } => {
                            invariant!(keyed_by.is_some());
                            trace!(
                                ?data,
                                ?for_keys,
                                requesting_shard,
                                unishard,
                                %tag,
                                "received partial replay"
                            );
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
                        } => {
                            trace!(?data, last, "received full replay");
                            (data, ReplayContext::Full { last })
                        }
                        Packet::Message { ref mut data, .. } => {
                            trace!(?data, "received regular message");
                            (data, ReplayContext::None)
                        }
                        _ => {
                            // TODO: Scoped for a future refactor:
                            // https://readysettech.atlassian.net/browse/ENG-455
                            unreachable!("internal dataflow node received an invalid packet type")
                        }
                    };

                    let mut set_replay_last = None;
                    // we need to own the data
                    let old_data = mem::take(data);

                    match i.on_input_raw(from, old_data, replay, env.nodes, env.state)? {
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
                                // TODO: Scope for future refactor:
                                // https://readysettech.atlassian.net/browse/ENG-455
                                unreachable!("only a ReplayPiece can release a ReplayPiece")
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
                            // TODO: Scope for future refactor:
                            // https://readysettech.atlassian.net/browse/ENG-455
                            unreachable!("only a ReplayPiece can release a ReplayPiece")
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
                    } => {
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
                    materialize(rs, None, tag, env.state.get_mut(addr));
                });

                for miss in misses.iter_mut() {
                    if miss.on != addr {
                        self.reroute_miss(env.nodes, miss)?;
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
            NodeType::Source => {
                internal!("you can't process through a source node")
            }
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
                s.process_eviction(key_columns, tag, keys, addr, on_shard.is_some(), ex)?;
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
    fn reroute_miss(&self, nodes: &DomainNodes, miss: &mut Miss) -> ReadySetResult<()> {
        let node = nodes[miss.on].borrow();
        if node.is_internal() && node.can_query_through() {
            let mut new_parent: Option<IndexPair> = None;
            for col in miss.lookup_idx.iter_mut() {
                let parents = node.resolve(*col).unwrap();
                invariant_eq!(parents.len(), 1, "query_through with more than one parent");

                let (parent_global, parent_col) = parents[0];
                if let Some(p) = new_parent {
                    invariant_eq!(
                        p.as_global(),
                        parent_global,
                        "query_through from different parents"
                    );
                } else {
                    let parent_node = nodes
                        .iter()
                        .filter_map(|(i, n)| {
                            match n.try_borrow() {
                                Ok(n) => Some(Ok(n)),
                                Err(_) => {
                                    // 'self' can be skipped. It cannot be its own ancestor and
                                    // is expected to already be mutably borrowed.
                                    if self.local_addr() != i {
                                        Some(Err(ReadySetError::Internal(
                                            "unexpected borrow failure".to_string(),
                                        )))
                                    } else if self.global_addr() == parent_global {
                                        Some(Err(ReadySetError::Internal(
                                            "rerouting back to self requested".to_string(),
                                        )))
                                    } else {
                                        None
                                    }
                                }
                            }
                        })
                        // TODO(peter): Do we want to incur this perf cost just so we can return
                        // errors here?
                        .collect::<ReadySetResult<Vec<_>>>()?
                        .into_iter()
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
    ) -> ReadySetResult<Option<Box<Packet>>> {
        // TODO: not error handling compliant!
        let src_node = m.src();
        match m {
            Packet::Timestamp {
                link,
                src,
                timestamp,
            } => {
                let PacketData { dst, data, .. } = unsafe { timestamp.take() };

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
                            parent_timestamps.push(t);
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
                    return Ok(None);
                }

                // Create a link if one does not already exist. This only happens
                // at the base table. The domain is responsible for setting the
                // new dst for the link.
                let link = Some(link.unwrap_or_else(|| Link::new(src_node, src_node)));

                // We leave the link untouched, the domain will be responsible
                // for updating it.
                let p = Box::new(Packet::Timestamp {
                    link,
                    src,
                    timestamp: LocalOrNot::new(PacketData {
                        dst,
                        data: PacketPayload::Timestamp(timestamp),
                        trace: None,
                    }),
                });

                // Some node types require additional packet handling after aggregating
                // all parent node timestamps.
                Ok(match self.inner {
                    NodeType::Egress(Some(ref mut e)) => {
                        // TODO(justin): Should this use on_shard like process.
                        let p = &mut Some(p);
                        e.process(p, None, 0, executor)?;
                        None
                    }
                    NodeType::Base(_) => Some(p),
                    _ => Some(p),
                })
            }
            _ => internal!("process_timestamp passed non timestamp packet."),
        }
    }
}

#[allow(clippy::borrowed_box)]
// crate visibility due to use by tests
pub(crate) fn materialize(
    rs: &mut Records,
    replication_offset: Option<ReplicationOffset>,
    partial: Option<Tag>,
    state: Option<&mut MaterializedNodeState>,
) {
    // our output changed -- do we need to modify materialized state?
    if state.is_none() {
        // nope
        return;
    }

    // yes!
    trace!(?rs, "materializing");
    state
        .unwrap()
        .process_records(rs, partial, replication_offset);
}

#[cfg(feature = "bench")]
pub mod bench {
    use noria_data::DataType::Int;

    use super::*;

    pub fn unique_misses(c: &mut criterion::Criterion) {
        let state = unsafe {
            NodeProcessingResult {
                misses: vec![
                    Miss {
                        on: LocalNodeIndex::make(4),
                        lookup_idx: [0].to_vec(),
                        lookup_key: MissLookupKey::RecordColumns(vec![1]),
                        replay_key: Some(MissReplayKey::RecordColumns(vec![0])),
                        record: vec![Int(30995), Int(2652263)],
                    },
                    Miss {
                        on: LocalNodeIndex::make(4),
                        lookup_idx: [0].to_vec(),
                        lookup_key: MissLookupKey::RecordColumns(vec![1]),
                        replay_key: Some(MissReplayKey::RecordColumns(vec![0])),
                        record: vec![Int(30995), Int(5851294)],
                    },
                    Miss {
                        on: LocalNodeIndex::make(4),
                        lookup_idx: [0].to_vec(),
                        lookup_key: MissLookupKey::RecordColumns(vec![1]),
                        replay_key: Some(MissReplayKey::RecordColumns(vec![0])),
                        record: vec![Int(30995), Int(522983)],
                    },
                    Miss {
                        on: LocalNodeIndex::make(4),
                        lookup_idx: [0].to_vec(),
                        lookup_key: MissLookupKey::RecordColumns(vec![1]),
                        replay_key: Some(MissReplayKey::RecordColumns(vec![0])),
                        record: vec![Int(30995), Int(9807676)],
                    },
                    Miss {
                        on: LocalNodeIndex::make(4),
                        lookup_idx: [0].to_vec(),
                        lookup_key: MissLookupKey::RecordColumns(vec![1]),
                        replay_key: Some(MissReplayKey::RecordColumns(vec![0])),
                        record: vec![Int(30995), Int(5210968)],
                    },
                    Miss {
                        on: LocalNodeIndex::make(4),
                        lookup_idx: [0].to_vec(),
                        lookup_key: MissLookupKey::RecordColumns(vec![1]),
                        replay_key: Some(MissReplayKey::RecordColumns(vec![0])),
                        record: vec![Int(30995), Int(835640)],
                    },
                    Miss {
                        on: LocalNodeIndex::make(4),
                        lookup_idx: [0].to_vec(),
                        lookup_key: MissLookupKey::RecordColumns(vec![1]),
                        replay_key: Some(MissReplayKey::RecordColumns(vec![0])),
                        record: vec![Int(30995), Int(4751888)],
                    },
                    Miss {
                        on: LocalNodeIndex::make(4),
                        lookup_idx: [0].to_vec(),
                        lookup_key: MissLookupKey::RecordColumns(vec![1]),
                        replay_key: Some(MissReplayKey::RecordColumns(vec![0])),
                        record: vec![Int(30995), Int(6806915)],
                    },
                    Miss {
                        on: LocalNodeIndex::make(4),
                        lookup_idx: [0].to_vec(),
                        lookup_key: MissLookupKey::RecordColumns(vec![1]),
                        replay_key: Some(MissReplayKey::RecordColumns(vec![0])),
                        record: vec![Int(30995), Int(7730521)],
                    },
                    Miss {
                        on: LocalNodeIndex::make(4),
                        lookup_idx: [0].to_vec(),
                        lookup_key: MissLookupKey::RecordColumns(vec![1]),
                        replay_key: Some(MissReplayKey::RecordColumns(vec![0])),
                        record: vec![Int(30995), Int(2474859)],
                    },
                    Miss {
                        on: LocalNodeIndex::make(4),
                        lookup_idx: [0].to_vec(),
                        lookup_key: MissLookupKey::RecordColumns(vec![1]),
                        replay_key: Some(MissReplayKey::RecordColumns(vec![0])),
                        record: vec![Int(30995), Int(4017737)],
                    },
                    Miss {
                        on: LocalNodeIndex::make(4),
                        lookup_idx: [0].to_vec(),
                        lookup_key: MissLookupKey::RecordColumns(vec![1]),
                        replay_key: Some(MissReplayKey::RecordColumns(vec![0])),
                        record: vec![Int(30995), Int(3197849)],
                    },
                    Miss {
                        on: LocalNodeIndex::make(4),
                        lookup_idx: [0].to_vec(),
                        lookup_key: MissLookupKey::RecordColumns(vec![1]),
                        replay_key: Some(MissReplayKey::RecordColumns(vec![0])),
                        record: vec![Int(30995), Int(4035289)],
                    },
                    Miss {
                        on: LocalNodeIndex::make(4),
                        lookup_idx: [0].to_vec(),
                        lookup_key: MissLookupKey::RecordColumns(vec![1]),
                        replay_key: Some(MissReplayKey::RecordColumns(vec![0])),
                        record: vec![Int(30995), Int(6857801)],
                    },
                    Miss {
                        on: LocalNodeIndex::make(4),
                        lookup_idx: [0].to_vec(),
                        lookup_key: MissLookupKey::RecordColumns(vec![1]),
                        replay_key: Some(MissReplayKey::RecordColumns(vec![0])),
                        record: vec![Int(30995), Int(3937104)],
                    },
                    Miss {
                        on: LocalNodeIndex::make(4),
                        lookup_idx: [0].to_vec(),
                        lookup_key: MissLookupKey::RecordColumns(vec![1]),
                        replay_key: Some(MissReplayKey::RecordColumns(vec![0])),
                        record: vec![Int(30995), Int(7490175)],
                    },
                    Miss {
                        on: LocalNodeIndex::make(4),
                        lookup_idx: [0].to_vec(),
                        lookup_key: MissLookupKey::RecordColumns(vec![1]),
                        replay_key: Some(MissReplayKey::RecordColumns(vec![0])),
                        record: vec![Int(30995), Int(8095737)],
                    },
                    Miss {
                        on: LocalNodeIndex::make(4),
                        lookup_idx: [0].to_vec(),
                        lookup_key: MissLookupKey::RecordColumns(vec![1]),
                        replay_key: Some(MissReplayKey::RecordColumns(vec![0])),
                        record: vec![Int(30995), Int(4484071)],
                    },
                    Miss {
                        on: LocalNodeIndex::make(4),
                        lookup_idx: [0].to_vec(),
                        lookup_key: MissLookupKey::RecordColumns(vec![1]),
                        replay_key: Some(MissReplayKey::RecordColumns(vec![0])),
                        record: vec![Int(30995), Int(427605)],
                    },
                    Miss {
                        on: LocalNodeIndex::make(4),
                        lookup_idx: [0].to_vec(),
                        lookup_key: MissLookupKey::RecordColumns(vec![1]),
                        replay_key: Some(MissReplayKey::RecordColumns(vec![0])),
                        record: vec![Int(30995), Int(8613827)],
                    },
                    Miss {
                        on: LocalNodeIndex::make(4),
                        lookup_idx: [0].to_vec(),
                        lookup_key: MissLookupKey::RecordColumns(vec![1]),
                        replay_key: Some(MissReplayKey::RecordColumns(vec![0])),
                        record: vec![Int(30995), Int(401022)],
                    },
                    Miss {
                        on: LocalNodeIndex::make(4),
                        lookup_idx: [0].to_vec(),
                        lookup_key: MissLookupKey::RecordColumns(vec![1]),
                        replay_key: Some(MissReplayKey::RecordColumns(vec![0])),
                        record: vec![Int(30995), Int(9310624)],
                    },
                    Miss {
                        on: LocalNodeIndex::make(4),
                        lookup_idx: [0].to_vec(),
                        lookup_key: MissLookupKey::RecordColumns(vec![1]),
                        replay_key: Some(MissReplayKey::RecordColumns(vec![0])),
                        record: vec![Int(30995), Int(8671521)],
                    },
                    Miss {
                        on: LocalNodeIndex::make(4),
                        lookup_idx: [0].to_vec(),
                        lookup_key: MissLookupKey::RecordColumns(vec![1]),
                        replay_key: Some(MissReplayKey::RecordColumns(vec![0])),
                        record: vec![Int(30995), Int(9955358)],
                    },
                ],
                lookups: vec![],
                captured: HashSet::new(),
            }
        };

        c.bench_function("unique_misses", |b| {
            b.iter(|| {
                criterion::black_box(state.unique_misses());
            })
        });
    }
}
