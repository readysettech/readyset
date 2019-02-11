//! Functions for identifying which nodes should be materialized, and what indices should be used
//! for those materializations.
//!
//! This module also holds the logic for *identifying* state that must be transfered from other
//! domains, but does not perform that copying itself (that is the role of the `augmentation`
//! module).

use crate::controller::{
    inner::{graphviz, DomainReplies},
    keys,
};
use crate::worker::domain_handle::DomainHandle;
use crate::worker::{Worker, WorkerIdentifier};
use dataflow::prelude::*;
use petgraph;
use petgraph::graph::NodeIndex;
use slog::Logger;
use std::collections::{HashMap, HashSet};
use std::mem;
use std::sync::atomic::{AtomicUsize, Ordering};

mod plan;

type Indices = HashSet<Vec<usize>>;

pub struct Materializations {
    log: Logger,

    have: HashMap<NodeIndex, Indices>,
    added: HashMap<NodeIndex, Indices>,

    partial: HashSet<NodeIndex>,
    partial_enabled: bool,

    // TODO: this doesn't belong here
    pub domains_on_path: HashMap<Tag, Vec<DomainIndex>>,

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

            domains_on_path: Default::default(),

            tag_generator: AtomicUsize::default(),
        }
    }

    #[allow(unused)]
    pub fn set_logger(&mut self, logger: &Logger) {
        self.log = logger.new(o!());
    }

    /// Disable partial materialization for all new materializations.
    pub fn disable_partial(&mut self) {
        self.partial_enabled = false;
    }
}

impl Materializations {
    fn next_tag(&self) -> Tag {
        Tag(self.tag_generator.fetch_add(1, Ordering::SeqCst) as u32)
    }

    /// Extend the current set of materializations with any additional materializations needed to
    /// satisfy indexing obligations in the given set of (new) nodes.
    fn extend(&mut self, graph: &Graph, new: &HashSet<NodeIndex>) {
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
        // There are multiple ways in which an indexing obligation can be created:
        //
        //  - a node can ask for its own state to be materialized
        //  - a node can indicate that it will perform lookups on its ancestors
        //  - a node can declare that it would benefit from an ancestor index for replays
        //
        // The last point is special, in that those indexes can be hoisted past *all* nodes,
        // including across domain boundaries. We call these "replay obligations". They are also
        // special in that they also need to be carried along all the way to the nearest *full*
        // materialization.
        //
        // In the first case, the materialization decision is easy: we materialize the node in
        // question. In the latter case, it is a bit more complex, since the parent may be in a
        // different domain, or may be a "query through" node that we want to avoid materializing.
        //
        // Computing indexing obligations is therefore a multi-stage process.
        //
        //  1. Compute what indexes each *new* operator requires.
        //  2. Add materializations for any lookup obligations, considering query-through.
        //  3. Recursively add indexes for replay obligations.
        //

        // Holds all lookup obligations. Keyed by the node that should be materialized.
        let mut lookup_obligations = HashMap::new();

        // Holds all replay obligations. Keyed by the node whose *parent* should be materialized.
        let mut replay_obligations = HashMap::new();

        // Find indices we need to add.
        for &ni in new {
            let n = &graph[ni];
            let mut indices = if n.is_reader() {
                let key = n.with_reader(|r| r.key()).unwrap();
                if key.is_none() {
                    // only streaming, no indexing needed
                    continue;
                }

                // for a reader that will get lookups, we'd like to have an index above us
                // somewhere on our key so that we can make the reader partial
                let mut i = HashMap::new();
                i.insert(ni, (Vec::from(key.unwrap()), false));
                i
            } else {
                n.suggest_indexes(ni)
            };

            if indices.is_empty() && n.is_base() {
                // we must *always* materialize base nodes
                // so, just make up some column to index on
                indices.insert(ni, (vec![0], true));
            }

            for (ni, (cols, lookup)) in indices {
                trace!(self.log, "new indexing obligation";
                       "node" => ni.index(),
                       "columns" => ?cols,
                       "lookup" => lookup);

                if lookup {
                    lookup_obligations
                        .entry(ni)
                        .or_insert_with(HashSet::new)
                        .insert(cols);
                } else {
                    replay_obligations
                        .entry(ni)
                        .or_insert_with(HashSet::new)
                        .insert(cols);
                }
            }
        }

        // map all the indices to the corresponding columns in the parent
        fn map_indices(
            n: &Node,
            parent: NodeIndex,
            indices: &HashSet<Vec<usize>>,
        ) -> Result<HashSet<Vec<usize>>, String> {
            indices
                .iter()
                .map(|index| {
                    index
                        .iter()
                        .map(|&col| {
                            if !n.is_internal() {
                                if n.is_base() {
                                    unreachable!();
                                }
                                return Ok(col);
                            }

                            let really = n.parent_columns(col);
                            let really = really
                                .into_iter()
                                .find(|&(anc, _)| anc == parent)
                                .and_then(|(_, col)| col);

                            really.ok_or_else(|| {
                                format!(
                                    "could not resolve obligation past operator;\
                                     node => {}, ancestor => {}, column => {}",
                                    n.global_addr().index(),
                                    parent.index(),
                                    col
                                )
                            })
                        })
                        .collect()
                })
                .collect()
        }

        // lookup obligations are fairly rigid, in that they require a materialization, and can
        // only be pushed through query-through nodes, and never across domains. so, we deal with
        // those first.
        //
        // it's also *important* that we do these first, because these are the only ones that can
        // force non-materialized nodes to become materialized. if we didn't do this first, a
        // partial node may add indices to only a subset of the intermediate partial views between
        // it and the nearest full materialization (because the intermediate ones haven't been
        // marked as materialized yet).
        for (ni, mut indices) in lookup_obligations {
            // we want to find the closest materialization that allows lookups (i.e., counting
            // query-through operators).
            let mut mi = ni;
            let mut m = &graph[mi];
            loop {
                if self.have.contains_key(&mi) {
                    break;
                }
                if !m.is_internal() || !m.can_query_through() {
                    break;
                }

                let mut parents = graph.neighbors_directed(mi, petgraph::EdgeDirection::Incoming);
                let parent = parents.next().unwrap();
                assert_eq!(
                    parents.count(),
                    0,
                    "query_through had more than one ancestor"
                );

                // hoist index to parent
                trace!(self.log, "hoisting indexing obligations";
                       "for" => mi.index(),
                       "to" => parent.index());
                mi = parent;
                indices = map_indices(m, mi, &indices).unwrap();
                m = &graph[mi];
            }

            for columns in indices {
                info!(self.log,
                    "adding lookup index to view";
                    "node" => ni.index(),
                    "columns" => ?columns,
                );

                if self.have.entry(mi).or_default().insert(columns.clone()) {
                    // also add a replay obligation to enable partial
                    replay_obligations
                        .entry(mi)
                        .or_default()
                        .insert(columns.clone());

                    self.added.entry(mi).or_default().insert(columns);
                }
            }
        }

        // we need to compute which views can be partial, and which can not.
        // in addition, we need to figure out what indexes each view should have.
        // this is surprisingly difficult to get right.
        //
        // the approach we are going to take is to require walking the graph bottom-up:
        let mut ordered = Vec::with_capacity(graph.node_count());
        let mut topo = petgraph::visit::Topo::new(graph);
        while let Some(node) = topo.next(graph) {
            if graph[node].is_source() {
                continue;
            }
            if graph[node].is_dropped() {
                continue;
            }

            // unfortunately, we may end up adding indexes to existing views, and we need to walk
            // them *all* in reverse topological order.
            ordered.push(node);
        }
        ordered.reverse();
        // for each node, we will check if it has any *new* indexes (i.e., in self.added).
        // if it does, see if the indexed columns resolve into its nearest ancestor
        // materializations. if they do, we mark this view as partial. if not, we, well, don't.
        // if the view was marked as partial, we add the necessary indexes to self.added for the
        // parent views, and keep walking. this is the reason we need the reverse topological
        // order: if we didn't, a node could receive additional indexes after we've checked it!
        for ni in ordered {
            let indexes = match replay_obligations.remove(&ni) {
                Some(idxs) => idxs,
                None => continue,
            };

            // we want to find out if it's possible to partially materialize this node. for that to
            // be the case, we need to keep moving up the ancestor tree of `ni`, and check at each
            // stage that we can trace the key column back into each of our nearest
            // materializations.
            let mut able = self.partial_enabled;
            let mut add = HashMap::new();

            // bases can't be partial
            if graph[ni].is_base() {
                able = false;
            }

            if graph[ni].is_internal() && graph[ni].requires_full_materialization() {
                warn!(self.log, "full because required"; "node" => ni.index());
                able = false;
            }

            // we are already fully materialized, so can't be made partial
            if !new.contains(&ni)
                && self.added.get(&ni).map(|i| i.len()).unwrap_or(0)
                    != self.have.get(&ni).map(|i| i.len()).unwrap_or(0)
                && !self.partial.contains(&ni)
            {
                warn!(self.log, "cannot turn full into partial"; "node" => ni.index());
                able = false;
            }

            // do we have a full materialization below us?
            let mut stack: Vec<_> = graph
                .neighbors_directed(ni, petgraph::EdgeDirection::Outgoing)
                .collect();
            while let Some(child) = stack.pop() {
                // allow views to force full (XXX)
                if graph[child].name().starts_with("FULL_") {
                    stack.clear();
                    able = false;
                }

                if self.have.contains_key(&child) {
                    // materialized child -- don't need to keep walking along this path
                    if !self.partial.contains(&child) {
                        // child is full, so we can't be partial
                        warn!(self.log, "full because descendant is full"; "node" => ni.index(), "child" => child.index());
                        stack.clear();
                        able = false
                    }
                } else if let Ok(Some(_)) = graph[child].with_reader(|r| r.key()) {
                    // reader child (which is effectively materialized)
                    if !self.partial.contains(&child) {
                        // reader is full, so we can't be partial
                        warn!(self.log, "full because reader below is full"; "node" => ni.index(), "reader" => child.index());
                        stack.clear();
                        able = false
                    }
                } else {
                    // non-materialized child -- keep walking
                    stack
                        .extend(graph.neighbors_directed(child, petgraph::EdgeDirection::Outgoing));
                }
            }

            'attempt: for index in &indexes {
                if !able {
                    break;
                }

                let paths = keys::provenance_of(graph, ni, &index[..], plan::Plan::on_join(graph));

                for path in paths {
                    for (pni, cols) in path.into_iter().skip(1) {
                        if let Some(p) = cols.iter().position(|c| c.is_none()) {
                            warn!(self.log, "full because column {} does not resolve", index[p];
                                  "node" => ni.index(), "broken at" => pni.index());
                            able = false;
                            break 'attempt;
                        }
                        let index: Vec<_> = cols.into_iter().map(|c| c.unwrap()).collect();
                        if let Some(m) = self.have.get(&pni) {
                            if !m.contains(&index) {
                                // we'd need to add an index to this view,
                                add.entry(pni)
                                    .or_insert_with(HashSet::new)
                                    .insert(index.clone());
                            }
                            break;
                        }
                    }
                }
            }

            if able {
                // we can do partial if we add all those indices!
                self.partial.insert(ni);
                warn!(self.log, "using partial materialization for {}", ni.index());
                for (mi, indices) in add {
                    let m = replay_obligations.entry(mi).or_default();
                    for index in indices {
                        m.insert(index);
                    }
                }
            }

            // no matter what happens, we're going to have to fulfill our replay obligations.
            if let Some(m) = self.have.get_mut(&ni) {
                for index in indexes {
                    let new_index = m.insert(index.clone());

                    if new_index {
                        info!(self.log,
                          "adding index to view to enable partial";
                          "on" => ni.index(),
                          "columns" => ?index,
                        );
                    }

                    if new_index || self.partial.contains(&ni) {
                        // we need to add to self.added even if we didn't explicitly add any new
                        // indices if we're partial, because existing domains will need to be told
                        // about new partial replay paths sourced from this node.
                        self.added.entry(ni).or_default().insert(index);
                    }
                }
            } else {
                assert!(graph[ni].is_reader());
            }
        }
        assert!(replay_obligations.is_empty());
    }

    /// Retrieves the materialization status of a given node, or None
    /// if the node isn't materialized.
    pub fn get_status(&self, index: &NodeIndex, node: &Node) -> MaterializationStatus {
        let is_materialized = self.have.contains_key(index)
            || node.with_reader(|r| r.is_materialized()).unwrap_or(false);

        if !is_materialized {
            MaterializationStatus::Not
        } else if self.partial.contains(index) {
            MaterializationStatus::Partial
        } else {
            MaterializationStatus::Full
        }
    }

    /// Commit to all materialization decisions since the last time `commit` was called.
    ///
    /// This includes setting up replay paths, adding new indices to existing materializations, and
    /// populating new materializations.
    pub(super) fn commit(
        &mut self,
        graph: &Graph,
        new: &HashSet<NodeIndex>,
        domains: &mut HashMap<DomainIndex, DomainHandle>,
        workers: &HashMap<WorkerIdentifier, Worker>,
        replies: &mut DomainReplies,
    ) {
        self.extend(graph, new);

        // check that we don't have fully materialized nodes downstream of partially materialized
        // nodes.
        {
            fn any_partial(
                this: &Materializations,
                graph: &Graph,
                ni: NodeIndex,
            ) -> Option<NodeIndex> {
                if this.partial.contains(&ni) {
                    return Some(ni);
                }
                for ni in graph.neighbors_directed(ni, petgraph::EdgeDirection::Incoming) {
                    if let Some(ni) = any_partial(this, graph, ni) {
                        return Some(ni);
                    }
                }
                None
            }

            for (&ni, _) in &self.added {
                if self.partial.contains(&ni) {
                    continue;
                }

                if let Some(pi) = any_partial(self, graph, ni) {
                    println!("{}", graphviz(graph, true, &self));
                    crit!(self.log, "partial materializations above full materialization";
                              "full" => ni.index(),
                              "partial" => pi.index());
                    unimplemented!();
                }
            }
        }

        // check that no node is partial over a subset of the indices in its parent
        {
            for (&ni, added) in &self.added {
                if !self.partial.contains(&ni) {
                    continue;
                }

                for index in added {
                    let paths =
                        keys::provenance_of(graph, ni, &index[..], plan::Plan::on_join(graph));

                    for path in paths {
                        for (pni, columns) in path {
                            if columns.iter().any(|c| c.is_none()) {
                                break;
                            } else if self.partial.contains(&pni) {
                                for index in &self.have[&pni] {
                                    // is this node partial over some of the child's partial
                                    // columns, but not others? if so, we run into really sad
                                    // situations where the parent could miss in its state despite
                                    // the child having state present for that key.

                                    // do we share a column?
                                    if index.iter().all(|&c| !columns.contains(&Some(c))) {
                                        continue;
                                    }

                                    // is there a column we *don't* share?
                                    let unshared = index
                                        .iter()
                                        .map(|&c| c)
                                        .find(|&c| !columns.contains(&Some(c)))
                                        .or_else(|| {
                                            columns
                                                .iter()
                                                .map(|c| c.unwrap())
                                                .find(|c| !index.contains(&c))
                                        });
                                    if let Some(not_shared) = unshared {
                                        println!("{}", graphviz(graph, true, &self));
                                        crit!(self.log, "partially overlapping partial indices";
                                                  "parent" => pni.index(),
                                                  "pcols" => ?index,
                                                  "child" => ni.index(),
                                                  "cols" => ?columns,
                                                  "conflict" => not_shared,
                                        );
                                        unimplemented!();
                                    }
                                }
                            } else if self.have.contains_key(&ni) {
                                break;
                            }
                        }
                    }
                }
            }
        }

        // check that we don't have any cases where a subgraph is sharded by one column, and then
        // has a replay path on a duplicated copy of that column. for example, a join with
        // [B(0, 0), R(0)] where the join's subgraph is sharded by .0, but a downstream replay path
        // looks up by .1. this causes terrible confusion where the target (correctly) queries only
        // one shard, but the shard merger expects to have to wait for all shards (since the replay
        // key and the sharding key do not match at the shard merger).
        {
            for &node in new {
                let n = &graph[node];
                if !n.is_shard_merger() {
                    continue;
                }

                // we don't actually store replay paths anywhere in Materializations (perhaps we
                // should). however, we can check a proxy for the necessary property by making sure
                // that our parent's sharding key is never aliased. this will lead to some false
                // positives (all replay paths may use the same alias as we shard by), but we'll
                // deal with that.
                let parent = graph
                    .neighbors_directed(node, petgraph::EdgeDirection::Incoming)
                    .next()
                    .expect("shard mergers must have a parent");
                let psharding = graph[parent].sharded_by();

                if let Sharding::ByColumn(col, _) = psharding {
                    // we want to resolve col all the way to its nearest materialized ancestor.
                    // and then check whether any other cols of the parent alias that source column
                    let columns: Vec<_> = (0..n.fields().len()).collect();
                    for path in keys::provenance_of(graph, parent, &columns[..], |_, _, _| None) {
                        let (mat_anc, cols) = path
                            .into_iter()
                            .skip_while(|&(n, _)| !self.have.contains_key(&n))
                            .next()
                            .expect(
                                "since bases are materialized, \
                                 every path must eventually have a materialized node",
                            );
                        let src = cols[col];
                        if src.is_none() {
                            continue;
                        }

                        if let Some((c, res)) = cols
                            .iter()
                            .enumerate()
                            .find(|&(c, res)| c != col && res == &src)
                        {
                            // another column in the merger's parent resolved to the source column!
                            //println!("{}", graphviz(graph, &self));
                            crit!(self.log, "attempting to merge sharding by aliased column";
                                      "parent" => mat_anc.index(),
                                      "aliased" => res,
                                      "sharded" => parent.index(),
                                      "alias" => c,
                                      "shard" => col,
                            );
                            unimplemented!();
                        }
                    }
                }
            }
        }

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

            if new.contains(&node) {
                make.push(node);
            } else if self.added.contains_key(&node) {
                reindex.push(node);
            }
        }

        // first, we add any new indices to existing nodes
        for node in reindex {
            let mut index_on = self.added.remove(&node).unwrap();

            // are they trying to make a non-materialized node materialized?
            if self.have[&node] == index_on {
                if self.partial.contains(&node) {
                    // we can't make this node partial if any of its children are materialized, as
                    // we might stop forwarding updates to them, which would make them very sad.
                    //
                    // the exception to this is for new children, or old children that are now
                    // becoming materialized; those are necessarily empty, and so we won't be
                    // violating key monotonicity.
                    let mut stack: Vec<_> = graph
                        .neighbors_directed(node, petgraph::EdgeDirection::Outgoing)
                        .collect();
                    while let Some(child) = stack.pop() {
                        if new.contains(&child) {
                            // NOTE: no need to check its children either
                            continue;
                        }

                        if self.added.get(&child).map(|i| i.len()).unwrap_or(0)
                            != self.have.get(&child).map(|i| i.len()).unwrap_or(0)
                        {
                            // node was previously materialized!
                            println!("{}", graphviz(graph, true, &self));
                            crit!(
                                self.log,
                                "attempting to make old non-materialized node with children partial";
                                "node" => node.index(),
                                "child" => child.index(),
                            );
                            unimplemented!();
                        }

                        stack.extend(
                            graph.neighbors_directed(child, petgraph::EdgeDirection::Outgoing),
                        );
                    }
                }

                warn!(self.log, "materializing existing non-materialized node";
                      "node" => node.index(),
                      "cols" => ?index_on);
            }

            let n = &graph[node];
            if self.partial.contains(&node) {
                info!(self.log, "adding partial index to existing {:?}", n);
                let log = self.log.new(o!("node" => node.index()));
                let log = mem::replace(&mut self.log, log);
                self.setup(node, &mut index_on, graph, domains, workers, replies);
                mem::replace(&mut self.log, log);
                index_on.clear();
            } else if !n.sharded_by().is_none() {
                // what do we even do here?!
                println!("{}", graphviz(graph, true, &self));
                crit!(self.log, "asked to add index to sharded node";
                           "node" => node.index(),
                           "cols" => ?index_on);
            // unimplemented!();
            } else {
                use dataflow::payload::InitialState;
                domains
                    .get_mut(&n.domain())
                    .unwrap()
                    .send_to_healthy(
                        box Packet::PrepareState {
                            node: n.local_addr(),
                            state: InitialState::IndexedLocal(index_on),
                        },
                        workers,
                    )
                    .unwrap();
            }
        }

        // then, we start prepping new nodes
        for ni in make {
            let n = &graph[ni];
            let mut index_on = self
                .added
                .remove(&ni)
                .map(|idxs| {
                    assert!(!idxs.is_empty());
                    idxs
                })
                .unwrap_or_else(HashSet::new);

            let start = ::std::time::Instant::now();
            self.ready_one(ni, &mut index_on, graph, domains, workers, replies);
            let reconstructed = index_on.is_empty();

            // communicate to the domain in charge of a particular node that it should start
            // delivering updates to a given new node. note that we wait for the domain to
            // acknowledge the change. this is important so that we don't ready a child in a
            // different domain before the parent has been readied. it's also important to avoid us
            // returning before the graph is actually fully operational.
            trace!(self.log, "readying node"; "node" => ni.index());
            let domain = domains.get_mut(&n.domain()).unwrap();
            domain
                .send_to_healthy(
                    box Packet::Ready {
                        node: n.local_addr(),
                        index: index_on,
                    },
                    workers,
                )
                .unwrap();
            replies.wait_for_acks(&domain);
            trace!(self.log, "node ready"; "node" => ni.index());

            if reconstructed {
                info!(self.log, "reconstruction completed";
                "ms" => start.elapsed().as_millis(),
                "node" => ni.index(),
                );
            }
        }

        self.added.clear();
    }

    /// Perform all operations necessary to bring any materializations for the given node up, and
    /// then mark that node as ready to receive updates.
    fn ready_one(
        &mut self,
        ni: NodeIndex,
        index_on: &mut HashSet<Vec<usize>>,
        graph: &Graph,
        domains: &mut HashMap<DomainIndex, DomainHandle>,
        workers: &HashMap<WorkerIdentifier, Worker>,
        replies: &mut DomainReplies,
    ) {
        let n = &graph[ni];
        let mut has_state = !index_on.is_empty();

        if has_state {
            if self.partial.contains(&ni) {
                debug!(self.log, "new partially-materialized node: {:?}", n);
            } else {
                debug!(self.log, "new fully-materalized node: {:?}", n);
            }
        } else {
            debug!(self.log, "new stateless node: {:?}", n);
        }

        if n.is_base() {
            // a new base must be empty, so we can materialize it immediately
            info!(self.log, "no need to replay empty new base"; "node" => ni.index());
            assert!(!self.partial.contains(&ni));
            return;
        }

        // if this node doesn't need to be materialized, then we're done.
        has_state = !index_on.is_empty();
        n.with_reader(|r| {
            if r.is_materialized() {
                has_state = true;
            }
        })
        .unwrap_or(());

        if !has_state {
            debug!(self.log, "no need to replay non-materialized view"; "node" => ni.index());
            return;
        }

        // we have a parent that has data, so we need to replay and reconstruct
        info!(self.log, "beginning reconstruction of {:?}", n);
        let log = self.log.new(o!("node" => ni.index()));
        let log = mem::replace(&mut self.log, log);
        self.setup(ni, index_on, graph, domains, workers, replies);
        mem::replace(&mut self.log, log);

        // NOTE: the state has already been marked ready by the replay completing, but we want to
        // wait for the domain to finish replay, which the ready executed by the outer commit()
        // loop does.
        index_on.clear();
        return;
    }

    /// Reconstruct the materialized state required by the given (new) node through replay.
    fn setup(
        &mut self,
        ni: NodeIndex,
        index_on: &mut HashSet<Vec<usize>>,
        graph: &Graph,
        domains: &mut HashMap<DomainIndex, DomainHandle>,
        workers: &HashMap<WorkerIdentifier, Worker>,
        replies: &mut DomainReplies,
    ) {
        if index_on.is_empty() {
            // we must be reconstructing a Reader.
            // figure out what key that Reader is using
            graph[ni]
                .with_reader(|r| {
                    assert!(r.is_materialized());
                    if let Some(rh) = r.key() {
                        index_on.insert(Vec::from(rh));
                    }
                })
                .unwrap();
        }

        // construct and disseminate a plan for each index
        let pending = {
            let mut plan = plan::Plan::new(self, graph, ni, domains, workers);
            for index in index_on.drain() {
                plan.add(index, replies);
            }
            plan.finalize()
        };

        if !pending.is_empty() {
            trace!(self.log, "all domains ready for replay");

            // prepare for, start, and wait for replays
            for pending in pending {
                // tell the first domain to start playing
                trace!(self.log, "telling root domain to start replay";
                   "domain" => pending.source_domain.index());

                domains
                    .get_mut(&pending.source_domain)
                    .unwrap()
                    .send_to_healthy(
                        box Packet::StartReplay {
                            tag: pending.tag,
                            from: pending.source,
                        },
                        workers,
                    )
                    .unwrap();
            }

            // and then wait for the last domain to receive all the records
            let target = graph[ni].domain();
            trace!(self.log,
               "waiting for done message from target";
               "domain" => target.index(),
            );

            replies.wait_for_acks(&domains[&target]);
        }
    }
}
