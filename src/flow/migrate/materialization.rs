//! Functions for identifying which nodes should be materialized, and what indices should be used
//! for those materializations.
//!
//! This module also holds the logic for *identifying* state that must be transfered from other
//! domains, but does not perform that copying itself (that is the role of the `augmentation`
//! module).

use flow;
use flow::keys;
use flow::domain;
use flow::prelude::*;
use flow::payload::TriggerEndpoint;
use backlog::ReadHandle;

use petgraph;
use petgraph::graph::NodeIndex;

use std::collections::{HashMap, HashSet};

use slog::Logger;

const FILTER_SPECIFICITY: usize = 10;

const NANOS_PER_SEC: u64 = 1_000_000_000;
macro_rules! dur_to_ns {
    ($d:expr) => {{
        let d = $d;
        d.as_secs() * NANOS_PER_SEC + d.subsec_nanos() as u64
    }}
}

use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize, Deserialize)]
pub struct Tag(u32);

impl Tag {
    pub fn id(&self) -> u32 {
        self.0
    }
}

struct Indices(HashSet<Vec<usize>>);

struct Materializations {
    log: Logger,

    have: HashMap<NodeIndex, Indices>,
    added: HashMap<NodeIndex, Indices>,

    partial: HashSet<NodeIndex>,
    partial_enabled: bool,

    // TODO: this doesn't belong here
    domains_on_path: HashMap<Tag, Vec<domain::Index>>,

    tag_generator: AtomicUsize,
}

impl Materializations {
    /// Create a new set of materializations.
    pub fn new(logger: &Logger) -> Self {
        Materializations {
            log: logger.new(o!()),

            have: HashMap::default(),
            added: HashMap::default(),

            partial: HashSet::default(),
            partial_enabled: true,
        }
    }

    /// Disable partial materialization for all new materializations.
    pub fn disable_partial(&mut self) {
        self.partial_enabled = false;
    }
}

struct PendingReplay {
    source: LocalNodeIndex,
    source_domain: domain::Index,
    target_domain: domain::Index,
}

impl Materializations {
    /// Extend the current set of materializations with any additional materializations needed to
    /// satisfy indexing obligations in the given set of (new) nodes.
    pub fn extend(&mut self, graph: &Graph, nodes: &[(NodeIndex, bool)]) {
        // this code used to be a mess, and will likely be a mess this time around too.
        // but, let's try to start out in a principled way...
        //
        // we have a bunch of known existing materializations (self.have), and potentially a set of
        // newly added, but not yet constructed, materializations (self.added). Everything in
        // self.added is also in self.have. We're now being asked to compute any indexing
        // obligations created by the nodes in `nodes`, some of which may be new (iff the boolean
        // is true). `extend` will be called once per new domain, so it will be called several
        // times before `commit` is ultimately called to create the new materializations.
        //
        // There are two ways in which an indexing obligation can be created: a node can ask for
        // its own state to be materialized, or a node can indicate that it will perform lookups on
        // its ancestors. In the former case, the materialization decision is easy: we materialize
        // the node in question. In the latter case, it is a bit more complex, since the parent may
        // be in a different domain, or may be a "query through" node that we want to avoid
        // materializing.
        //
        // Computing indexing obligations is therefore a two-stage process. First, we compute what
        // indexes each *new* operator requires. Then, we push those indexing requirements up to
        // the nearest non-"query through" operator.
        let mut obligations = HashMap::new();
        for &(ni, new) in nodes {
            if !new {
                // we only construct obligations from new nodes, since existing nodes cannot
                // suddenly require additional indices.
                continue;
            }

            let n = &graph[ni];
            if !n.is_internal() {
                // non-internal nodes cannot generate indexing obligations
                continue;
            }

            let mut indices = n.suggest_indexes();
            if indices.is_empty() && n.get_base().is_some() {
                // we must *always* materialize base nodes
                // so, just make up some column to index on
                indices.insert(ni, vec![0]);
            }

            for (ni, cols) in n.suggest_indexes() {
                trace!(self.log, "new indexing obligation";
                       "node" => ni.index(),
                       "columns" => ?cols);

                obligations
                    .entry(ni)
                    .or_insert_with(HashSet::new)
                    .insert(cols);
            }
        }

        // now, we do a little song-and-dance where we eliminate any obligations that have been
        // met, push down any indices on "query through" operators, and decide to materialize those
        // that aren't. And then we continue until there are no obligations left.
        while !obligations.is_empty() {
            let ni = obligations.keys().next().map(|&ni| ni).unwrap(); // unwrap ok because !empty
            let (ni, indices) = obligations.remove(&ni).unwrap();

            if let Some(have) = self.have.get_mut(&ni) {
                // no need to push these indices down further, just add them to this already
                // materialized view (which must also be the closest).
                for columns in indices {
                    if have.insert(columns.clone()) {
                        // not already indexed on this particular column set
                        info!(self.log,
                              "adding index to existing view";
                              "node" => ni.index(),
                              "columns" => ?columns,
                          );
                        self.added
                            .entry(ni)
                            .or_insert_with(HashSet::new)
                            .insert(columns);
                    }
                }
                continue;
            }

            let n = &graph[ni];
            if n.is_internal() && n.can_query_through() {
                // push these obligations to the ancestor of n.
                let mut ancestors = graph.neighbors_directed(petgraph::EdgeDirection::Incoming);
                let ancestor = ancestors
                    .next()
                    .expect("query through node has no ancestor");
                assert!(
                    ancestors.next().is_none(),
                    "query through node has >1 ancestors"
                );

                // optimization: ingress -> query through = materialize only query through
                if !graph[ancestor].is_ingress() {
                    // TODO: what about ingress -> query through -> query through?

                    // we need to figure out how the columns remap through the ancestor
                    for index in &mut indices {
                        for col in &mut index {
                            let really = n.parent_columns(col);
                            assert_eq!(
                                really.len(),
                                1,
                                "query through node does not resolve column"
                            );
                            assert_eq!(
                                really[0].0,
                                ancestor,
                                "query through node doesn't resolve to ancestor"
                            );
                            assert!(
                                really[0].1.is_some(),
                                "query through node does not resolve column"
                            );
                            *col = really.into_iter().next().unwrap().1.unwrap();
                        }
                    }

                    trace!(self.log, "hoisting indexing obligations";
                           "for" => ni.index(),
                           "to" => ancestor.index());

                    obligations.insert(ancestor, indices);
                    continue;
                } else {
                    trace!(self.log, "not hoisting indexing obligations to ingress";
                           "for" => ni.index());
                }
            }

            // no pushing through, we need to materialize this node!
            info!(self.log,
                  "adding indices";
                  "node" => ni.index(),
                  "cols" => ?indices,
              );
            self.have.insert(ni, indices.clone());
            self.added.insert(ni, indices);
        }
    }

    /// Commit to all materialization decisions since the last time `commit` was called.
    ///
    /// This includes setting up replay paths, adding new indices to existing materializations, and
    /// populating new materializations.
    pub fn commit(&mut self, graph: &Graph, new: &HashSet<NodeIndex>) {
        let mut reindex = Vec::with_capacity(new.len());
        let mut make = Vec::with_capacity(new.len());
        let mut topo = petgraph::visit::Topo::new(graph);
        while let Some(node) = topo.next(graph) {
            if graph[node].is_source() {
                continue;
            }
            if graph[node].is_dropped() {
                continue;
            }

            if new.contains_key(&node) {
                make.push(node);
            } else if self.added.contains_key(&node) {
                reindex.push(node);
            }
        }

        // first, we add any new indices to existing nodes
        // TODO FIXME

        // then, we start prepping new nodes
        let mut empty = HashSet::new();
        for ni in make {
            let n = &graph[ni];
            let index_on = self.added
                .remove(ni)
                .map(|idxs| {
                    assert!(!idxs.is_empty());
                    idxs
                })
                .unwrap_or_else(Vec::new);

            let start = ::std::time::Instant::now();
            self.ready_one(ni, &mut index_on, graph, &mut empty);
            let reconstructed = index_on.is_empty();

            // communicate to the domain in charge of a particular node that it should start
            // delivering updates to a given new node. note that we wait for the domain to
            // acknowledge the change. this is important so that we don't ready a child in a
            // different domain before the parent has been readied. it's also important to avoid us
            // returning before the graph is actually fully operational.
            trace!(self.log, "readying node"; "node" => node.index());
            let domain = unimplemented!().handles.get_mut(&n.domain()).unwrap();
            domain
                .send(box Packet::Ready {
                    node: *n.local_addr(),
                    index: index_on,
                })
                .unwrap();
            domain.wait_for_ack().unwrap();
            trace!(self.log, "node ready"; "node" => node.index());

            if reconstructed {
                info!(self.log, "reconstruction completed";
                      "ms" => dur_to_ns!(start.elapsed()) / 1_000_000);
            }
        }

        self.added.clear();
    }

    /// Perform all operations necessary to bring any materializations for the given node up, and
    /// then mark that node as ready to receive updates.
    fn ready_one(
        &mut self,
        ni: NodeIndex,
        index_on: &mut Vec<Vec<usize>>,
        graph: &Graph,
        empty: &mut HashSet<NodeIndex>,
    ) {
        let n = &graph[ni];
        if graph
            .neighbors_directed(ni, petgraph::EdgeDirection::Incoming)
            .filter(|&ni| !graph[ni].is_source())
            .all(|n| empty.contains(&n))
        {
            // all parents are empty, so we can materialize it immediately
            info!(self.log, "no need to replay empty view"; "node" => ni.index());
            empty.insert(ni);

            // we need to make sure the domain constructs reader backlog handles!
            let prep = n.with_reader(|r| {
                r.key().map(|key| {
                    use flow::payload::InitialState;

                    match n.sharded_by() {
                        Sharding::None => {
                            unimplemented!()
                                .readers
                                .lock()
                                .unwrap()
                                .insert(ni, ReadHandle::Singleton(None));
                        }
                        _ => {
                            use arrayvec::ArrayVec;
                            let mut shards = ArrayVec::new();
                            for _ in 0..::SHARDS {
                                shards.push(None);
                            }
                            unimplemented!()
                                .readers
                                .lock()
                                .unwrap()
                                .insert(ni, ReadHandle::Sharded(shards));
                        }
                    }

                    box Packet::PrepareState {
                        node: *n.local_addr(),
                        state: InitialState::Global {
                            cols: n.fields().len(),
                            key: key,
                            gid: ni,
                        },
                    }
                })
            });
            if let Some(Some(prep)) = prep {
                unimplemented!()
                    .domains
                    .get_mut(&n.domain())
                    .unwrap()
                    .send(prep)
                    .unwrap();
            }
            return;
        }

        // if this node doesn't need to be materialized, then we're done. note that this check
        // needs to happen *after* the empty parents check so that we keep tracking whether or not
        // nodes are empty.
        let mut has_state = !index_on.is_empty();
        n.with_reader(|r| if r.is_materialized() {
            has_state = true;
        });

        if !has_state {
            debug!(self.log, "no need to replay non-materialized view"; "node" => node.index());
            return;
        }

        // we have a parent that has data, so we need to replay and reconstruct
        let log = log.new(o!("node" => node.index()));
        info!(self.log, "beginning reconstruction of {:?}", n);
        self.reconstruct(graph, ni, index_on, graph, &empty);

        // NOTE: the state has already been marked ready by the replay completing, but we want to
        // wait for the domain to finish replay, which the ready executed by the outer commit()
        // loop does.
        index_on.clear();
        return;
    }

    /// Reconstruct the materialized state required by the given (new) node through replay.
    fn reconstruct(
        &mut self,
        ni: NodeIndex,
        index_on: &mut Vec<Vec<usize>>,
        graph: &Graph,
        empty: &HashSet<NodeIndex>,
    ) {
        if index_on.is_empty() {
            // we must be reconstructing a Reader.
            // figure out what key that Reader is using
            graph[node]
                .with_reader(|r| {
                    assert!(r.is_materialized());
                    if let Some(rh) = r.key() {
                        index_on.push(vec![rh]);
                    }
                })
                .unwrap();
        }

        // okay, so here's the situation: `ni` is a node that
        //
        //   a) was not previously materialized, and
        //   b) now needs to be materialized, and
        //   c) at least one of node's parents has existing data
        //
        // because of the topological traversal done by `initialize`, we know that all our
        // ancestors that should be materialized have been. from here, we first need to decide
        // whether to partially materialize the new node, or fully reconstruct its state.
        //
        // to decide that, we need to find all our paths up the tree.
        let paths = {
            let mut on_join = self.cost_fn(graph, &mut unimplemented!().domains, empty);
            // TODO: what if we're constructing multiple indices?
            // TODO: what if we have a compound index?
            let trace_col = index_on[0][0];
            keys::provenance_of(graph, ni, trace_col, &mut *on_join)
        };
        // and cut paths so they only reach to the the closest materialized node
        let paths: Vec<_> = paths
            .into_iter()
            .map(|path| -> Vec<_> {
                let mut found = false;
                path.into_iter()
                    .enumerate()
                    .take_while(|&(i, (node, _))| {
                        // remember, the paths are "backwards", so the first node is target node
                        if i == 0 {
                            return true;
                        }

                        // keep taking until we get our first materialized node
                        // (`found` helps us emulate `take_while_inclusive`)
                        if found {
                            // we've already found a materialized node
                            return false;
                        }

                        if self.have.get(&node).map(|m| !m.is_empty()).unwrap_or(false) {
                            // we want to take this node, but not any later ones
                            found = true;
                        }
                        true
                    })
                    .map(|(_, segment)| segment)
                    .collect()
            })
            .collect();

        // can we do partial materialization?
        //
        // for now, we require index_on.len() == 1 && index_on[0].len() == 1.
        // this is because we don't yet support multiple partial indices on the same node (it would
        // would require per-index replay paths) and we don't support partial materialization on
        // compound keys (it would require more complex key provenance computation).
        let partial_ok = self.partial_enabled && index_on.len() == 1 && index_on[0].len() == 1;
        // we also require that `index_on[0][0]` of `ni` trace back to some `key` in the
        // materialized state we're replaying? if it does not, partial replay isn't possible.
        let partial_ok = partial_ok && paths.iter().all(|path| {
            let &(node, col) = path.last().unwrap();
            if col.is_none() {
                // doesn't trace back to a column
                return false;
            }
            let col = col.unwrap();

            // node must also have an *index* on col
            self.have
                .get(&node)
                .map(|indices| {
                    indices.iter().any(|idx| idx.len() == 1 && idx[0] == col)
                })
                .unwrap_or(false)
        });
        // FIXME: if a reader has no materialized views between it and a union, we will end
        // up in this case. we *can* solve that case by requesting replays across all
        // the tagged paths through the union, but since we at this point in the code don't
        // yet know about those paths, that's a bit inconvenient. we might be able to move
        // this entire block below the main loop somehow (?), but for now:
        let partial_ok = partial_ok && (!graph[ni].is_reader() || paths.len() == 1);

        // keep track of the fact that this view is partially materialized
        if partial_ok {
            warn!(self.log, "using partial materialization");
            self.partial.insert(node);
        }

        // FIXME: we need to detect materialized nodes downstream of partially materialized nodes.
        // FIXME: what if we have two paths with the same source because of a fork-join? we'd need
        // to buffer somewhere to avoid splitting pieces...

        // inform domains about replay paths
        let mut pending = Vec::with_capacity(paths.len());
        for path in &mut paths {
            // there should always be a replay path
            assert!(
                !path.is_empty(),
                "needed to replay non-empty node, but no materializations found"
            );

            // we want path to have the ancestor closest to the root *first*
            path.reverse();

            if let Some(p) = self.expose_path(path, partial_ok, graph) {
                // this path requires doing a replay and then waiting for the replay to finish
                pending.push(p);
            }
        }

        trace!(self.log, "all domains ready for replay");
        if !partial_ok {
            assert_eq!(pending.len(), paths.len());
        } else {
            assert!(pending.is_empty());
        }

        // prepare for, start, and wait for replays
        self.prepare_state(ni, graph, &paths[..]);
        for pending in pending {
            // tell the first domain to start playing
            trace!(self.log, "telling root domain to start replay";
                   "domain" => pending.source_domain.index());

            unimplemented!()
                .domains
                .get_mut(&pending.source_domain)
                .unwrap()
                .send(box Packet::StartReplay {
                    tag: tag,
                    from: pending.source,
                })
                .unwrap();

            // and then wait for the last domain to receive all the records
            trace!(self.log,
               "waiting for done message from target";
               "domain" => pending.target_domain
            );

            unimplemented!()
                .domains
                .get_mut(&pending.target_domain)
                .unwrap()
                .wait_for_ack()
                .unwrap();
        }
    }

    /// Tell all domains along the given replay path about that path
    fn expose_path(
        &mut self,
        path: &[(NodeIndex, Vec<usize>)],
        graph: &Graph,
    ) -> Option<PendingReplay> {
        let tag = Tag(self.tag_generator.fetch_add(1, Ordering::SeqCst) as u32);
        trace!(self.log, "setting up replay path {:?}", path; "tag" => tag.id());

        // what key are we using for partial materialization (if any)?
        let mut partial = None;
        if partial_ok {
            if let Some(&(_, Some(ref key))) = path.first() {
                partial = Some(key.clone());
            }
        }

        // first, find out which domains we are crossing
        let mut segments = Vec::new();
        let mut last_domain = None;
        for &mut (node, ref mut key) in &mut path {
            let domain = graph[node].domain();
            if last_domain.is_none() || domain != last_domain.unwrap() {
                segments.push((domain, Vec::new()));
                last_domain = Some(domain);

                if partial_ok && graph[node].is_transactional() {
                    self.domains_on_path
                        .entry(tag.clone())
                        .or_insert_with(Vec::new)
                        .push(domain);
                }
            }

            segments.last_mut().unwrap().1.push((node, key.take()));
        }

        debug!(self.log, "domain replay path is {:?}", segments; "tag" => tag.id());

        // tell all the domains about their segment of this replay path
        let mut pending = None;
        let mut seen = HashSet::new();
        for (i, &(ref domain, ref nodes)) in segments.iter().enumerate() {
            // TODO:
            //  a domain may appear multiple times in this list if a path crosses into the same
            //  domain more than once. currently, that will cause a deadlock.
            assert!(
                !seen.contains(domain),
                "a-b-a domain replays are not yet supported"
            );
            seen.insert(*domain);

            // we're not replaying through the starter node
            // *unless* it's a Base (because it might need to add defaults)
            let mut skip_first = 0;
            if i == 0 {
                let n = &graph[segments[0].1[0].0];
                if !n.is_internal() || n.get_base().is_none() {
                    skip_first = 1;
                }
            }

            // use the local index for each node
            let locals = nodes
                .iter()
                .skip(skip_first)
                .map(|&(ni, key)| (*graph[ni].local_addr(), key))
                .collect();

            // the first domain in the chain may *only* have the source node
            // in which case it doesn't need to know about the path
            if locals.is_empty() {
                assert_eq!(i, 0);
                continue;
            }

            // build the message we send to this domain to tell it about this replay path.
            let mut setup = box Packet::SetupReplayPath {
                tag: tag,
                source: None,
                path: locals,
                notify_done: false,
                trigger: TriggerEndpoint::None,
            };

            // the first domain also gets to know source node
            if i == 0 {
                if let box Packet::SetupReplayPath { ref mut source, .. } = setup {
                    *source = Some(*graph[nodes[0].0].local_addr());
                }
            }


            if let Some(ref key) = partial {
                // for partial materializations, nodes need to know how to trigger replays
                if let box Packet::SetupReplayPath {
                    ref mut trigger, ..
                } = setup
                {
                    if segments.len() == 1 {
                        // replay is entirely contained within one domain
                        *trigger = TriggerEndpoint::Local(vec![*key]);
                    } else if i == 0 {
                        // first domain needs to be told about partial replay trigger
                        *trigger = TriggerEndpoint::Start(vec![*key]);
                    } else if i == segments.len() - 1 {
                        // otherwise, should know how to trigger partial replay
                        let shards = blender.domains.get_mut(&segments[0].0).unwrap().shards();
                        *trigger = TriggerEndpoint::End(segments[0].0.clone(), shards);
                    }
                } else {
                    unreachable!();
                }
            } else {
                // for full materializations, the last domain should report when it's done
                if i == segments.len() - 1 {
                    if let box Packet::SetupReplayPath {
                        ref mut notify_done,
                        ..
                    } = setup
                    {
                        *notify_done = true;
                        assert!(pending.is_none());
                        pending = Some(PendingReplay{
                            source: graph[segments[0].1[0].0].local_addr(),
                            source_domain: segments[0].0,
                            target_domain: domain,
                        });
                }
            }

            if i != segments.len() - 1 {
                // since there is a later domain, the last node of any non-final domain must either
                // be an egress or a Sharder. If it's an egress, we need to tell it about this
                // replay path so that it knows what path to forward replay packets on.
                let n = &graph[nodes.last().unwrap().0];
                if n.is_egress() {
                    unimplemented!()
                        .domains
                        .get_mut(domain)
                        .unwrap()
                        .send(box Packet::UpdateEgress {
                            node: *n.local_addr(),
                            new_tx: None,
                            new_tag: Some((tag, segments[i + 1].1[0].0.into())),
                        })
                        .unwrap();
                } else {
                    assert!(n.is_sharder());
                }
            }

            trace!(self.log, "telling domain about replay path"; "domain" => domain.index());
            let ctx = unimplemented!().domains.get_mut(domain).unwrap();
            ctx.send(setup).unwrap();
            ctx.wait_for_ack().unwrap();
        }

        pending
    }

    /// Tell the node's domain to create an empty state for the node in question
    fn prepare_state(&self, ni: NodeIndex, graph: &Graph, paths: &[(NodeIndex, Vec<usize>)]) {
        use flow::payload::InitialState;

        // we need to play a little bit of trickery here.
        // if we are partially materializing a reader, the reader needs to know how to request
        // backfills when a read misses. in order to do that, it needs to know the replay path tag.
        // but since we haven't constructed the replay paths yet, we don't yet know the tag. and we
        // can't construct the tag
        let last_domain = paths.get(0).map(|p| graph[p[0].0].domain());
        let next_tag = Tag(self.tag_generator.load(Ordering::SeqCst) as u32);

        // NOTE: we cannot use the impl of DerefMut here, since it (reasonably) disallows getting
        // mutable references to taken state.
        let s = graph[ni]
            .with_reader(|r| {
                // we need to make sure there's an entry in readers for this reader!
                match graph[ni].sharded_by() {
                    Sharding::None => {
                        blender
                            .readers
                            .lock()
                            .unwrap()
                            .insert(node, ReadHandle::Singleton(None));
                    }
                    _ => {
                        use arrayvec::ArrayVec;
                        let mut shards = ArrayVec::new();
                        for _ in 0..::SHARDS {
                            shards.push(None);
                        }
                        blender
                            .readers
                            .lock()
                            .unwrap()
                            .insert(node, ReadHandle::Sharded(shards));
                    }
                }

                if partial_ok {
                    // make sure Reader is actually prepared to receive state
                    assert!(r.is_materialized());

                    if paths.len() != 1 {
                        unreachable!(); // due to FIXME above
                    }

                    let num_shards = blender.domains[&last_domain.unwrap()].shards();

                    // since we're partially materializing a reader node,
                    // we need to give it a way to trigger replays.
                    InitialState::PartialGlobal {
                        gid: node,
                        cols,
                        key: r.key().unwrap(),
                        tag: first_tag.unwrap(),
                        trigger_domain: (last_domain.unwrap(), num_shards),
                    }
                } else {
                    InitialState::Global {
                        cols,
                        key: r.key().unwrap(),
                        gid: node,
                    }
                }
            })
            .unwrap_or_else(|| if partial_ok {
                assert_eq!(index_on.len(), 1);
                assert_eq!(index_on[0].len(), 1);
                InitialState::PartialLocal(index_on[0][0])
            } else {
                InitialState::IndexedLocal(index_on)
            });

        unimplemented!()
            .domains
            .get_mut(&domain)
            .unwrap()
            .send(box Packet::PrepareState {
                node: addr,
                state: s,
            })
            .unwrap();
    }

    /// `cost_fn` provides a cost function that can be used to determine which ancestor (if any)
    /// should be preferred when replaying.
    fn cost_fn(
        &self,
        graph: &Graph,
        txs: &mut HashMap<domain::Index, domain::DomainHandle>,
        empty: &HashSet<NodeIndex>,
    ) -> Box<FnMut(NodeIndex, &[NodeIndex]) -> Option<NodeIndex> + 'a> {
        Box::new(move |node, parents| {
            // this function should only be called when there's a choice
            assert!(parents.len() > 1);

            // and only internal nodes have multiple parents
            let n = &graph[node];
            assert!(n.is_internal());

            // keep track of remaining parents
            let mut parents = Vec::from(parents);

            // the node dictates that we *must* replay the state of some ancestor(s)
            let options = n.must_replay_among()
                .expect("join did not have must replay preference");
            parents.retain(|&parent| options.contains(&parent));
            assert!(!parents.is_empty());

            // if there is only one left, we don't have a choice
            if parents.len() == 1 {
                // no need to pick
                return parents.pop();
            }

            // if *all* the options are empty, we can safely pick any of them
            if parents.iter().all(|&p| empty.contains(p)) {
                return parents.pop();
            }

            // if any required parent is empty, it is tempting to conclude that the join must be empty
            // (which it must indeed be; outer join targets aren't required), and therefore that we
            // can just pick that parent and get a free full materialization. *however*, this would
            // cause the node to be marked as fully materialized, which is *not* okay if it has
            // partially a materialized ancestor!
            if let Some(&parent) = parents.iter().find(|&p| empty.contains(p)) {
                if !parents.iter().any(|p| self.partial.contains(p)) {
                    // no partial ancestors, so let's replay the empty view!
                    return Some(parent);
                }
            }

            let is_materialized =
                |ni: NodeIndex| self.have.get(&ni).map(|m| !m.is_empty()).unwrap_or(false);

            // we want to pick the ancestor that causes us to do the least amount of work.
            // this is really a balancing act between
            //
            //   a) how many records we are going to join; and
            //   b) how much state we need to replay
            //
            // consider the case where we are replaying a node downstream of a join, and the
            // join has two ancestors: a base node (A) and a filter (F) over a base node (B).
            // when should we choose to replay one or the other?
            //
            //  - the cost of replaying A is
            //        |A| joins
            //      + |A| lookups in B through F
            //  - the cost of replaying B through F is
            //        |B| filter operations
            //      + |F(B)| join operations
            //      + |F(B)| lookups in A
            //
            // which of these is more costly? even assuming we know |A| and |B|, it is not
            // clear, because we don't know F's specificity. let's assume some things:
            //
            //  - filters are cheaper than joins (~10x)
            //  - replaying is cheaper than filtering (~10x)
            //  - a filter emits one record for every FILTER_SPECIFICITY input records
            //
            // given those rough estimates, what's the best choice? well, we should pick a node
            // N with filters F1..Fn to replay which minimizes
            //
            //    1   * |N|                                 # replay cost
            let replay_cost = 1;
            //  + 10  * ( ∑i |N| / FILTER_SPECIFICITY ^ i ) # filter cost
            let filter_cost = 10;
            //  + 100 * |N| / FILTER_SPECIFICITY ^ n        # join cost
            let join_cost = 100;
            //
            // it is worth pointing out that this heuristic does *not* capture the fact that
            // replaying A above will encounter more expensive lookups on the join path (since
            // the lookups are in F(B), as opposed to directly in A).
            //
            // to compute this, we need to find |N| and n for each candidate node.
            // let's do that now
            parents
                .into_iter()
                .map(|p| {
                    let mut intermediates = vec![];
                    let mut stateful = p;

                    // find the nearest materialized ancestor, and keep track of filters we pass
                    while !is_materialized(stateful) {
                        let n = &graph[stateful];
                        // joins require their inputs to be materialized. therefore, we know that
                        // any non-materialized ancestors *must* be query_through.
                        assert!(n.is_internal());
                        assert!(n.can_query_through());
                        // if this node is selective (e.g., a filter), increase the filter factor.
                        // we need to keep track of non-selective project/permute nodes too though,
                        // as they increase the cost
                        intermediates.push(n.is_selective());
                        // now walk to the parent.
                        let mut ps =
                            graph.neighbors_directed(stateful, petgraph::EdgeDirection::Incoming);
                        // of which there must be at least one
                        stateful = ps.next().expect("recursed all the way to source");
                        // there shouldn't ever be multiple, because neither join nor union
                        // are query_through.
                        assert_eq!(ps.count(), 0);
                    }

                    // find the size of the state we would end up replaying
                    let stateful = &graph[stateful];
                    let domain = txs.get_mut(&stateful.domain()).unwrap();
                    domain
                        .send(box Packet::StateSizeProbe {
                            node: *stateful.local_addr(),
                        })
                        .unwrap();
                    let mut size = domain.wait_for_state_size().unwrap();

                    // compute the total cost
                    // replay cost
                    let mut cost = replay_cost * size;
                    // filter cost
                    for does_filter in intermediates {
                        cost += filter_cost * size;
                        if does_filter {
                            size /= FILTER_SPECIFICITY;
                        }
                    }
                    // join cost
                    cost += join_cost * size;

                    debug!(self.log, "cost of replaying from {:?}: {}", p, cost);
                    (p, cost)
                })
                .min_by_key(|&(_, cost)| cost)
                .map(|(node, cost)| {
                    debug!(self.log, "picked replay source {:?}", node; "cost" => cost);
                    node
                })
        })
    }
}
