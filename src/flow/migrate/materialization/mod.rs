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
use backlog::ReadHandle;
use petgraph;
use petgraph::graph::NodeIndex;
use std::collections::{HashMap, HashSet};
use slog::Logger;
use std::mem;
use std::sync::atomic::{AtomicUsize, Ordering};

mod plan;

const NANOS_PER_SEC: u64 = 1_000_000_000;
macro_rules! dur_to_ns {
    ($d:expr) => {{
        let d = $d;
        d.as_secs() * NANOS_PER_SEC + d.subsec_nanos() as u64
    }}
}


#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize, Deserialize)]
pub struct Tag(u32);

impl Tag {
    pub fn id(&self) -> u32 {
        self.0
    }
}

type Indices = HashSet<Vec<usize>>;

pub struct Materializations {
    log: Logger,

    have: HashMap<NodeIndex, Indices>,
    added: HashMap<NodeIndex, Indices>,

    partial: HashSet<NodeIndex>,
    partial_enabled: bool,

    // TODO: this doesn't belong here
    pub domains_on_path: HashMap<Tag, Vec<domain::Index>>,

    tag_generator: AtomicUsize,
    readers: flow::Readers,
}

impl Materializations {
    /// Create a new set of materializations.
    pub fn new(logger: &Logger, readers: &flow::Readers) -> Self {
        Materializations {
            log: logger.new(o!()),

            have: HashMap::default(),
            added: HashMap::default(),

            partial: HashSet::default(),
            partial_enabled: true,

            domains_on_path: Default::default(),

            tag_generator: AtomicUsize::default(),
            readers: readers.clone(),
        }
    }

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
        for &(ni, new) in nodes {
            if !new {
                // we only construct obligations from new nodes, since existing nodes cannot
                // suddenly require additional indices.
                continue;
            }

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
                i.insert(ni, (vec![key.unwrap()], false));
                i
            } else if !n.is_internal() {
                // non-internal nodes cannot generate indexing obligations
                continue;
            } else {
                n.suggest_indexes(ni)
            };

            if indices.is_empty() && n.get_base().is_some() {
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

                if self.have
                    .entry(mi)
                    .or_insert_with(HashSet::new)
                    .insert(columns.clone())
                {
                    // also add a replay obligation to enable partial
                    replay_obligations
                        .entry(mi)
                        .or_insert_with(HashSet::new)
                        .insert(columns.clone());

                    self.added
                        .entry(mi)
                        .or_insert_with(HashSet::new)
                        .insert(columns);
                }
            }
        }

        // we're now going to walk the replay obligations, and try to apply them to enable more
        // partial materialization opportunities. this is a little tricky, because there are fairly
        // strict restrictions on when we can use partial replay. in particular, it is required
        // that the key traces all the way back to some existing materialization, and that that
        // materialization has a key for the same columns. this means that a single replay
        // obligation is *really* a set of replay obligations going all the way up to the nearest
        // full materialization.
        for (ni, indices) in replay_obligations {
            // we first want to find out if it's even *possible* to partially materialize this
            // node. for that to be the case, we need to keep moving up the ancestor tree of `ni`,
            // and check at each stage that we can continue tracing the key column back until we
            // reach a full materialization (or one that's already partially materialized on the
            // same key, which inductively must mean that the path above works out).
            let mut able = self.partial_enabled;
            let mut add = HashMap::new();

            if graph[ni].is_internal() && graph[ni].get_base().is_some() {
                able = false;
            }

            'try: for index in &indices {
                if !able {
                    break;
                }

                if index.len() != 1 {
                    // FIXME
                    able = false;
                    break;
                }
                let index = index[0];
                let paths = {
                    let mut on_join = plan::Plan::partial_on_join(graph);
                    keys::provenance_of(graph, ni, index, &mut *on_join)
                };

                // TODO: if a reader has no materialized views between it and a union, we will end
                // up in this case. we *can* solve that case by requesting replays across all the
                // tagged paths through the union, but since we at this point in the code don't yet
                // know about those paths, that's a bit inconvenient. we might be able to move this
                // entire block below the main loop somehow (?), but for now:
                if graph[ni].is_reader() && paths.len() != 1 {
                    able = false;
                    break;
                }

                for path in paths {
                    // keep walking until:
                    //
                    //  - col is None; key doesn't trace, partial not ok
                    //  - node is fully materialized; partial is ok
                    //  - node is partial on same key; partial is ok
                    //
                    for (ni, col) in path.into_iter().skip(1) {
                        if col.is_none() {
                            able = false;
                            break 'try;
                        }
                        let index = vec![col.unwrap()];
                        if let Some(m) = self.have.get(&ni) {
                            if self.partial.contains(&ni) {
                                // already keyed on the right thing?
                                if m.contains(&index) {
                                    // no index needed, and no need to walk further!
                                    // we're all good as far as this path is concerned.
                                    break;
                                } else {
                                    // we'd need to add an index to this view
                                    // we also need to keep walking, since we may need more
                                    if !add.entry(ni).or_insert_with(HashSet::new).insert(index) {
                                        // we've already added that obligation, so the rest of this
                                        // path must be ok
                                        break;
                                    }
                                }
                            } else {
                                // full materialization! as long as we add an index, we're all good
                                add.entry(ni).or_insert_with(HashSet::new).insert(index);
                                break;
                            }
                        }
                    }
                }
            }

            if !able {
                // no reason to add all these indices; partial replay is impossible anyway.
                //
                // FIXME: we must take this path if this is an existing node that is not already
                // partial!
                //
                // we can't have fully materialized nodes downstream of partially materialized nodes.
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

                if let Some(pi) = any_partial(self, graph, ni) {
                    crit!(self.log, "partial materializations above full materialization";
                          "full" => ni.index(),
                          "partial" => pi.index());
                    unimplemented!();
                }
                continue;
            }

            // we can do partial if we add all these indices!
            self.partial.insert(ni);
            warn!(self.log, "using partial materialization for {}", ni.index());
            for (mi, indices) in add {
                let m = self.have.entry(mi).or_insert_with(HashSet::new);
                for index in indices {
                    if m.insert(index.clone()) {
                        info!(self.log,
                          "adding index to view to enable partial";
                          "on" => mi.index(),
                          "for" => ni.index(),
                          "columns" => ?index,
                        );
                    }

                    // we actually need to communicate this to the domain even though it
                    // already has the index, because it needs to be told about the new replay
                    // tags associated with this index!
                    self.added
                        .entry(mi)
                        .or_insert_with(HashSet::new)
                        .insert(index);
                }
            }
        }
    }

    /// Commit to all materialization decisions since the last time `commit` was called.
    ///
    /// This includes setting up replay paths, adding new indices to existing materializations, and
    /// populating new materializations.
    pub fn commit(
        &mut self,
        graph: &Graph,
        new: &HashSet<NodeIndex>,
        domains: &mut HashMap<domain::Index, domain::DomainHandle>,
    ) {
        let mut reindex = Vec::with_capacity(new.len());
        let mut make = Vec::with_capacity(new.len());
        let mut topo = petgraph::visit::Topo::new(graph);
        let mut empty = HashSet::new();
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
            let n = &graph[node];
            if self.partial.contains(&node) {
                info!(self.log, "adding partial index to existing {:?}", n);
                let log = self.log.new(o!("node" => node.index()));
                let log = mem::replace(&mut self.log, log);
                self.setup(node, &mut index_on, graph, &empty, domains);
                mem::replace(&mut self.log, log);
                index_on.clear();
            } else if n.sharded_by() != Sharding::None {
                // what do we even do here?!
                crit!(self.log, "asked to add index to sharded node";
                           "node" => node.index(),
                           "cols" => ?index_on);
            // unimplemented!();
            } else {
                use flow::payload::InitialState;
                domains
                    .get_mut(&n.domain())
                    .unwrap()
                    .send(box Packet::PrepareState {
                        node: *n.local_addr(),
                        state: InitialState::IndexedLocal(index_on),
                    })
                    .unwrap();
            }
        }

        // then, we start prepping new nodes
        for ni in make {
            let n = &graph[ni];
            let mut index_on = self.added
                .remove(&ni)
                .map(|idxs| {
                    assert!(!idxs.is_empty());
                    idxs
                })
                .unwrap_or_else(HashSet::new);

            let start = ::std::time::Instant::now();
            self.ready_one(ni, &mut index_on, graph, &mut empty, domains);
            let reconstructed = index_on.is_empty();

            // communicate to the domain in charge of a particular node that it should start
            // delivering updates to a given new node. note that we wait for the domain to
            // acknowledge the change. this is important so that we don't ready a child in a
            // different domain before the parent has been readied. it's also important to avoid us
            // returning before the graph is actually fully operational.
            trace!(self.log, "readying node"; "node" => ni.index());
            let domain = domains.get_mut(&n.domain()).unwrap();
            domain
                .send(box Packet::Ready {
                    node: *n.local_addr(),
                    index: index_on,
                })
                .unwrap();
            domain.wait_for_ack().unwrap();
            trace!(self.log, "node ready"; "node" => ni.index());

            if reconstructed {
                info!(self.log, "reconstruction completed";
                      "ms" => dur_to_ns!(start.elapsed()) / 1_000_000,
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
        empty: &mut HashSet<NodeIndex>,
        domains: &mut HashMap<domain::Index, domain::DomainHandle>,
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
                            self.readers
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
                            self.readers
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
                domains.get_mut(&n.domain()).unwrap().send(prep).unwrap();
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
            debug!(self.log, "no need to replay non-materialized view"; "node" => ni.index());
            return;
        }

        // we have a parent that has data, so we need to replay and reconstruct
        info!(self.log, "beginning reconstruction of {:?}", n);
        let log = self.log.new(o!("node" => ni.index()));
        let log = mem::replace(&mut self.log, log);
        self.setup(ni, index_on, graph, &empty, domains);
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
        empty: &HashSet<NodeIndex>,
        domains: &mut HashMap<domain::Index, domain::DomainHandle>,
    ) {
        if index_on.is_empty() {
            // we must be reconstructing a Reader.
            // figure out what key that Reader is using
            graph[ni]
                .with_reader(|r| {
                    assert!(r.is_materialized());
                    if let Some(rh) = r.key() {
                        index_on.insert(vec![rh]);
                    }
                })
                .unwrap();
        }

        // construct and disseminate a plan for each index
        let pending = {
            let mut plan = plan::Plan::new(self, graph, ni, empty, domains);
            for index in index_on.drain() {
                plan.add(index);
            }
            plan.finalize()
        };

        trace!(self.log, "all domains ready for replay");

        // prepare for, start, and wait for replays
        for pending in pending {
            // tell the first domain to start playing
            trace!(self.log, "telling root domain to start replay";
                   "domain" => pending.source_domain.index());

            domains
                .get_mut(&pending.source_domain)
                .unwrap()
                .send(box Packet::StartReplay {
                    tag: pending.tag,
                    from: pending.source,
                })
                .unwrap();

            // and then wait for the last domain to receive all the records
            trace!(self.log,
               "waiting for done message from target";
               "domain" => pending.target_domain.index()
            );

            domains
                .get_mut(&pending.target_domain)
                .unwrap()
                .wait_for_ack()
                .unwrap();
        }
    }
}
