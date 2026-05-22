use std::convert::TryInto;
use std::mem;

use dataflow_state::{MaterializedNodeState, SnapshotMode};
use readyset_client::{KeyComparison, PacketData, ReplayKeys};
use readyset_errors::ReadySetResult;
use replication_offset::ReplicationOffset;
use tracing::{debug_span, trace};

use crate::node::NodeType;
use crate::node::special::base::{BaseWrite, SetSnapshotMode};
use crate::payload::Eviction;
use crate::prelude::*;
use crate::{backlog, payload};

/// The results of running a forward pass on a node
#[derive(Debug, PartialEq, Eq, Default)]
pub(crate) struct NodeProcessingResult {
    /// The values that we failed to lookup
    pub(crate) misses: Vec<Miss>,

    /// The lookups that were performed during processing
    pub(crate) lookups: Vec<Lookup>,

    /// Keys for replays captured during processing
    pub(crate) captured: ReplayKeys,
}

/// Information about the domain required by [`Node::process`].
pub(crate) struct ProcessEnv<'domain> {
    pub(crate) state: &'domain mut StateMap,
    pub(crate) reader_write_handles: &'domain mut NodeMap<backlog::WriteHandle>,
    pub(crate) nodes: &'domain DomainNodes,
    pub(crate) executor: &'domain mut dyn Executor,
    pub(crate) shard: Option<usize>,
    /// Per-Node mutable state for nodes that require it
    pub(crate) auxiliary_node_states: &'domain mut AuxiliaryNodeStateMap,
}

impl Node {
    pub(crate) fn process(
        &mut self,
        m: &mut Option<Packet>,
        keyed_by: Option<&Vec<usize>>,
        publish_reader: bool,
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
                materialize(m.data_mut(), None, tag, env.state.get_mut(addr))?;
            }
            NodeType::Constant(_) => {
                // Constant nodes are materialized during replay via on_input
                // During normal processing, they just pass through their materialized state
                let m = m.as_mut().unwrap();
                let tag = m.tag();
                materialize(m.data_mut(), None, tag, env.state.get_mut(addr))?;
            }
            NodeType::Base(ref mut b) => {
                // NOTE: bases only accept BaseOperations
                match m.take() {
                    Some(Packet::Input(Input { inner, .. })) => {
                        let PacketData { dst, data, trace } = inner;
                        let ops = data
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
                        } = b.process_ops(
                            addr,
                            &self.columns,
                            ops,
                            &*env.state,
                            snapshot_mode,
                            self.name.clone(),
                        )?;

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
                            materialize(
                                &mut rs,
                                replication_offset,
                                None,
                                env.state.get_mut(addr),
                            )?;
                        }

                        if let (Some(SetSnapshotMode::FinishSnapshotMode), Some(s)) = (
                            set_snapshot_mode,
                            env.state.get_mut(addr).and_then(|s| s.as_persistent_mut()),
                        ) {
                            s.set_snapshot_mode(SnapshotMode::SnapshotModeDisabled);
                        }

                        *m = Some(Packet::Update(Update {
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
                        unreachable!("the domain should never pass a None packet to process")
                    }
                }
            }
            NodeType::Reader(ref mut r) => {
                if let Some(state) = env.reader_write_handles.get_mut(addr) {
                    r.process(m, publish_reader, state);
                }
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
            NodeType::Internal(ref mut i) => {
                let mut captured_full = false;
                let mut captured = ReplayKeys::new();
                let mut misses = Vec::new();
                let mut lookups = Vec::new();

                {
                    let m = m.as_mut().unwrap();
                    let from = m.src();

                    let (data, replay) = match *m {
                        Packet::ReplayPiece(ReplayPiece {
                            tag,
                            ref mut data,
                            context: payload::ReplayPieceContext::Partial { ref for_keys },
                            ..
                        }) => {
                            invariant!(keyed_by.is_some());
                            trace!(?data, ?for_keys, %tag, "received partial replay");
                            (
                                data,
                                ReplayContext::Partial {
                                    key_cols: keyed_by.unwrap(),
                                    keys: for_keys,
                                    tag,
                                },
                            )
                        }
                        Packet::ReplayPiece(ReplayPiece {
                            ref mut data,
                            context: payload::ReplayPieceContext::Full { last },
                            tag,
                            ..
                        }) => {
                            trace!(?data, %tag, last, "received full replay");
                            (data, ReplayContext::Full { last, tag })
                        }
                        Packet::Update(ref mut x) => {
                            let data = x.data_mut();
                            trace!(?data, "received regular message");
                            (data, ReplayContext::None)
                        }
                        _ => {
                            unreachable!("internal dataflow node received an invalid packet type")
                        }
                    };

                    let mut set_replay_last = None;
                    // we need to own the data
                    let old_data = mem::take(data);

                    match i.on_input_raw(
                        from,
                        old_data,
                        replay,
                        env.nodes,
                        env.state,
                        env.auxiliary_node_states,
                    )? {
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
                            if let Packet::ReplayPiece(ReplayPiece {
                                context:
                                    payload::ReplayPieceContext::Partial {
                                        ref mut for_keys, ..
                                    },
                                ..
                            }) = *m
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
                        if let Packet::ReplayPiece(ReplayPiece {
                            context: payload::ReplayPieceContext::Full { ref mut last, .. },
                            ..
                        }) = *m
                        {
                            *last = new_last;
                        } else {
                            // TODO: Scope for future refactor:
                            // https://readysettech.atlassian.net/browse/ENG-455
                            unreachable!("only a ReplayPiece can release a ReplayPiece")
                        }
                    }
                }

                if captured_full {
                    *m = None;
                    return Ok(Default::default());
                }

                let m = m.as_mut().unwrap();
                let tag = match *m {
                    Packet::ReplayPiece(ReplayPiece {
                        tag,
                        context: payload::ReplayPieceContext::Partial { .. },
                        ..
                    }) => {
                        // NOTE: non-partial replays shouldn't be materialized only for a
                        // particular index, and so the tag shouldn't be forwarded to the
                        // materialization code. this allows us to keep some asserts deeper in
                        // the code to check that we don't do partial replays to non-partial
                        // indices, or for unknown tags.
                        Some(tag)
                    }
                    _ => None,
                };
                materialize(m.data_mut(), None, tag, env.state.get_mut(addr))?;

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

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn process_eviction(
        &mut self,
        from: LocalNodeIndex,
        keys: &[KeyComparison],
        tag: Tag,
        on_shard: Option<usize>,
        reader_write_handles: &mut NodeMap<backlog::WriteHandle>,
        ex: &mut dyn Executor,
        auxiliary_node_states: &mut AuxiliaryNodeStateMap,
    ) -> ReadySetResult<()> {
        let addr = self.local_addr();
        match self.inner {
            NodeType::Base(..) => {}
            NodeType::Constant(_) => {
                // Constants are fully materialized and never evict
            }
            NodeType::Egress(Some(ref mut e)) => {
                e.process(
                    &mut Some(Packet::Evict(Evict {
                        req: Eviction::Keys {
                            link: Link {
                                src: addr,
                                dst: addr,
                            },
                            tag,
                            keys: keys.to_vec(),
                        },
                        barrier: None,
                    })),
                    None,
                    on_shard.unwrap_or(0),
                    ex,
                )?;
            }
            NodeType::Internal(ref mut i) => {
                i.on_eviction(from, tag, keys, auxiliary_node_states);
            }
            NodeType::Reader(_) => {
                if let Some(state) = reader_write_handles.get_mut(addr) {
                    trace!(
                        local = %self.local_addr(),
                        ?keys,
                        ?tag,
                        "Evicting keys from reader"
                    );
                    for k in keys {
                        state.mark_hole(k)?;
                    }
                    state.publish();
                    state.notify_readers_of_eviction()?;
                }
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
}

pub(crate) fn materialize(
    rs: &mut Records,
    replication_offset: Option<ReplicationOffset>,
    partial: Option<Tag>,
    state: Option<&mut MaterializedNodeState>,
) -> ReadySetResult<()> {
    if let Some(state) = state {
        trace!(?rs, ?replication_offset, "materializing");
        state.process_records(rs, partial, replication_offset)?;
    }

    Ok(())
}
