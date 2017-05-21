//! Functions for identifying which nodes should be materialized, and what indices should be used
//! for those materializations.
//!
//! This module also holds the logic for *identifying* state that must be transfered from other
//! domains, but does not perform that copying itself (that is the role of the `augmentation`
//! module).

use flow::keys;
use flow::domain;
use flow::prelude::*;

use petgraph;
use petgraph::graph::NodeIndex;

use std::collections::{HashSet, HashMap};
use std::sync::mpsc;

use slog::Logger;

const FILTER_SPECIFICITY: usize = 10;

const NANOS_PER_SEC: u64 = 1_000_000_000;
macro_rules! dur_to_ns {
    ($d:expr) => {{
        let d = $d;
        d.as_secs() * NANOS_PER_SEC + d.subsec_nanos() as u64
    }}
}

use std::sync::atomic::{AtomicUsize, Ordering, ATOMIC_USIZE_INIT};
static TAG_GENERATOR: AtomicUsize = ATOMIC_USIZE_INIT;

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct Tag(u32);

impl Tag {
    pub fn id(&self) -> u32 {
        self.0
    }
}

pub fn pick(log: &Logger, graph: &Graph, nodes: &[(NodeIndex, bool)]) -> HashSet<LocalNodeIndex> {
    let nodes: Vec<_> = nodes
        .iter()
        .map(|&(ni, new)| (ni, &graph[ni], new))
        .collect();

    let mut materialize: HashSet<_> = nodes
        .iter()
        .filter_map(|&(ni, n, _)| {
            // materialized state for any nodes that need it
            // in particular, we keep state for
            //
            //  - any internal node that requires its own state to be materialized
            //  - any internal node that has an outgoing edge marked as materialized (we know
            //    that that edge has to be internal, since ingress/egress nodes have already
            //    been added, and they make sure that there are no cross-domain materialized
            //    edges).
            //  - any ingress node with children that say that they may query their ancestors
            //
            // that last point needs to be checked *after* we have determined if all internal
            // nodes should be materialized
            if n.is_internal() {
                if n.should_materialize() ||
                   graph
                       .edges_directed(ni, petgraph::EdgeDirection::Outgoing)
                       .any(|e| *e.weight()) {
                    trace!(log, "should materialize"; "node" => format!("{}", ni.index()));
                    Some(*n.local_addr().as_local())
                } else {
                    trace!(log, "not materializing"; "node" => format!("{}", ni.index()));
                    None
                }
            } else {
                None
            }
        })
        .collect();

    let mut inquisitive_children = HashSet::new();
    {
        let mark_parent_inquisitive_or_materialize =
            |ni: NodeIndex,
             materialize: &mut HashSet<LocalNodeIndex>,
             inquisitive_children: &mut HashSet<NodeIndex>|
             -> Option<NodeIndex> {
                let n = &graph[ni];
                if n.is_internal() {
                    if !materialize.contains(n.local_addr().as_local()) {
                        if n.can_query_through() {
                            trace!(log, "parent can be queried through, mark it as querying";
                                   "node" => format!("{}", ni.index()));
                            inquisitive_children.insert(ni);
                            // continue backtracking
                            return Some(ni);
                        } else {
                            // we can't query through this internal node, so materialize it
                            trace!(log, "parent can't be queried through, so materialize it";
                                   "node" => format!("{}", ni.index()));
                            materialize.insert(*n.local_addr().as_local());
                        }
                    }
                }
                None
            };
        for &(ni, n, _) in nodes.iter() {
            if n.is_internal() {
                if n.will_query(materialize.contains(n.local_addr().as_local())) {
                    trace!(log, "found querying child"; "node" => format!("{}", ni.index()));
                    inquisitive_children.insert(ni);
                    // track child back to an ingress, marking any unmaterialized nodes on the path
                    // as inquisitive as long as we can query through them
                    let mut q = vec![ni];
                    while !q.is_empty() {
                        let ni = q.pop().unwrap();
                        for ni in graph.neighbors_directed(ni, petgraph::EdgeDirection::Incoming) {
                            let next =
                                mark_parent_inquisitive_or_materialize(ni,
                                                                       &mut materialize,
                                                                       &mut inquisitive_children);
                            match next {
                                Some(next_ni) => q.push(next_ni),
                                None => continue,
                            }
                        }
                    }
                }
            }
        }
    }

    for &(ni, n, _) in &nodes {
        if n.is_ingress() {
            if graph
                   .neighbors_directed(ni, petgraph::EdgeDirection::Outgoing)
                   .any(|child| inquisitive_children.contains(&child)) {
                // we have children that may query us, so our output should be materialized
                trace!(log,
                       "querying children force materialization of node {}",
                       ni.index());
                materialize.insert(*n.local_addr().as_local());
            }
        }
    }

    // find all nodes that can be queried through, and where any of its outgoing edges are
    // materialized. for those nodes, we should instead materialize the input to that node.
    for &(ni, n, _) in &nodes {
        if n.is_internal() {
            if !n.can_query_through() {
                continue;
            }

            if !materialize.contains(n.local_addr().as_local()) {
                // we're not materialized, so no materialization shifting necessary
                continue;
            }

            if graph
                   .edges_directed(ni, petgraph::EdgeDirection::Outgoing)
                   .any(|e| *e.weight()) {
                // our output is materialized! what a waste. instead, materialize our input.
                materialize.remove(n.local_addr().as_local());
                trace!(log, "hoisting materialization"; "past" => ni.index());

                // TODO: unclear if we need *all* our parents to be materialized. it's
                // certainly the case for filter, which is our only use-case for now...
                for p in graph.neighbors_directed(ni, petgraph::EdgeDirection::Incoming) {
                    materialize.insert(*graph[p].local_addr().as_local());
                }
            }
        }
    }

    materialize
}

pub fn index(log: &Logger,
             graph: &Graph,
             nodes: &[(NodeIndex, bool)],
             materialize: HashSet<LocalNodeIndex>)
             -> HashMap<LocalNodeIndex, Vec<Vec<usize>>> {

    let map: HashMap<_, _> = nodes
        .iter()
        .map(|&(ni, _)| (*graph[ni].local_addr().as_local(), ni))
        .collect();
    let nodes: Vec<_> = nodes.iter().map(|&(ni, new)| (&graph[ni], new)).collect();

    let mut state: HashMap<_, Option<Vec<Vec<usize>>>> =
        materialize.into_iter().map(|n| (n, None)).collect();

    // Now let's talk indices.
    //
    // We need to query all our nodes for what indices they believe should be maintained, and
    // apply those to the stores in state. However, this is somewhat complicated by the fact
    // that we need to push indices through non-materialized views so that they end up on the
    // columns of the views that will actually query into a table of some sort.
    {
        let nodes: HashMap<_, _> = nodes.iter().map(|&(n, _)| (n.local_addr(), n)).collect();
        let mut indices = nodes.iter()
                .filter(|&(_, node)| node.is_internal()) // only internal nodes can suggest indices
                .filter(|&(_, node)| {
                    // under what circumstances might a node need indices to be placed?
                    // there are two cases:
                    //
                    //  - if makes queries into its ancestors regardless of whether it's
                    //    materialized or not
                    //  - if it queries its ancestors when it is *not* materialized (implying that
                    //    it queries into its own output)
                    //
                    //  unless we come up with a weird operator that *doesn't* need indices when
                    //  it is *not* materialized, but *does* when is, we can therefore just use
                    //  will_query(false) as an indicator of whether indices are necessary.
                    node.will_query(false)
                })
                .flat_map(|(ni, node)| node.suggest_indexes(**ni).into_iter())
                .filter(|&(ref node, _)| nodes.contains_key(node))
                .fold(HashMap::new(), |mut hm, (v, idx)| {
                    hm.entry(v).or_insert_with(HashSet::new).insert(idx);
                    hm
                });

        // push up indices
        let mut leftover_indices: HashMap<_, _> = indices.drain().collect();
        let mut tmp = HashMap::new();
        while !leftover_indices.is_empty() {
            for (v, idxs) in leftover_indices.drain() {
                if let Some(mut state) = state.get_mut(v.as_local()) {
                    // this node is materialized! add the indices!
                    info!(log,
                          "adding indices";
                          "node" => map[v.as_local()].index(),
                          "cols" => format!("{:?}", idxs)
                      );
                    *state = Some(idxs.into_iter().collect());
                } else if let Some(node) = nodes.get(&v) {
                    // this node is not materialized
                    // we need to push the index up to its ancestor(s)
                    if node.is_ingress() {
                        // we can't push further up!
                        crit!(log, "suggested index is at domain edge, but ingress isn't \
                                      materialized";
                                      "node" => ?node.global_addr(),
                                      "idxs" => ?idxs.into_iter().collect::<Vec<_>>());
                        unreachable!();
                    }

                    assert!(node.is_internal());
                    // push indices up through views. This is needed because a query-through
                    // operators must inform its parents to set up appropriate indices; if it
                    // doesn't, the previously established materialization on the parent(s) will be
                    // considered unnnecessary and removed in the next step
                    for idx in idxs {
                        // idx could be compound, so we resolve each contained column separately.
                        // Note that a single column can resolve into *multiple* parent columns
                        // that need to be indexed.
                        let real_cols: Vec<_> = idx.iter().map(|col| node.resolve(*col)).collect();
                        // here's the deal:
                        // real_cols holds a vec of parent colums for each column in a compound
                        // key. Each element of this vec is (parent_node, parent_col). We need to
                        // collect these inner tuples and install corresponding indexing
                        // requirements on the nodes/columns in them.
                        let cols_to_index_per_node = real_cols.into_iter().fold(HashMap::new(),
                                                                                |mut acc, nc| {
                            if let Some(p_cols) = nc {
                                for (pn, pc) in p_cols {
                                    acc.entry(pn).or_insert_with(Vec::new).push(pc);
                                }
                            }
                            acc
                        });
                        // cols_to_index_per_node is now a map of node -> Vec<usize>, and we add an
                        // index on each individual column in the Vec.
                        // Note that this, and the semantics of node.resolve(), imply that each
                        // column must resolve to one ore more *single* parent node columns. In
                        // other words, we never install compound keys by pushing indices upwards;
                        // hence the two nested loops are required here.
                        for (n, cols) in cols_to_index_per_node {
                            for col in cols {
                                trace!(log,
                                       "pushing up index {:?} on {} into columns {:?} of {}",
                                       idx,
                                       v,
                                       col,
                                       n);
                                tmp.entry(n).or_insert_with(HashSet::new).insert(vec![col]);
                            }
                        }
                    }
                } else {
                    unreachable!("node suggested index outside domain");
                }
            }
            leftover_indices.extend(tmp.drain());
        }
    }

    state
        .into_iter()
        .filter_map(|(n, col)| {
            if let Some(col) = col {
                Some((n, col))
            } else {
                // this materialization doesn't have any primary key,
                // so we assume it's not in use.

                let ref node = graph[map[&n]];
                if node.is_internal() && node.get_base().is_some() {
                    // but it's a base nodes!
                    // we must *always* materialize base nodes
                    // so, just make up some column to index on
                    return Some((n, vec![vec![0]]));
                }

                info!(log, "removing unnecessary materialization"; "node" => map[&n].index());
                None
            }
        })
        .collect()
}

pub fn initialize(log: &Logger,
                  graph: &mut Graph,
                  source: NodeIndex,
                  new: &HashSet<NodeIndex>,
                  partial: &mut HashSet<NodeIndex>,
                  partial_ok: bool,
                  mut materialize: HashMap<domain::Index,
                                           HashMap<LocalNodeIndex, Vec<Vec<usize>>>>,
                  txs: &mut HashMap<domain::Index, mpsc::SyncSender<Box<Packet>>>)
                  -> HashMap<Tag, Vec<domain::Index>> {
    let mut topo_list = Vec::with_capacity(new.len());
    let mut topo = petgraph::visit::Topo::new(&*graph);
    while let Some(node) = topo.next(&*graph) {
        if node == source {
            continue;
        }
        if !new.contains(&node) {
            continue;
        }
        topo_list.push(node);
    }

    // TODO: what about adding materialization to *existing* views?
    let mut domains_on_path = HashMap::new();
    let mut empty = HashSet::new();
    for node in topo_list {
        let addr = *graph[node].local_addr();
        let d = graph[node].domain();

        let index_on = materialize
            .get_mut(&d)
            .and_then(|ss| ss.get(addr.as_local()))
            .cloned()
            .map(|idxs| {
                     // we've been told to materialize a node using 0 indices
                     assert!(!idxs.is_empty());
                     idxs
                 })
            .unwrap_or_else(Vec::new);
        let mut has_state = !index_on.is_empty();

        graph[node].with_reader(|r| if r.is_materialized() {
                                    has_state = true;
                                });

        // ready communicates to the domain in charge of a particular node that it should start
        // delivering updates to a given new node. note that we wait for the domain to acknowledge
        // the change. this is important so that we don't ready a child in a different domain
        // before the parent has been readied. it's also important to avoid us returning before the
        // graph is actually fully operational.
        let ready = |txs: &mut HashMap<_, mpsc::SyncSender<_>>, index_on: Vec<Vec<usize>>| {
            let (ack_tx, ack_rx) = mpsc::sync_channel(0);
            trace!(log, "readying node"; "node" => node.index());
            txs[&d]
                .send(box Packet::Ready {
                          node: *addr.as_local(),
                          index: index_on,
                          ack: ack_tx,
                      })
                .unwrap();
            match ack_rx.recv() {
                Err(mpsc::RecvError) => (),
                _ => unreachable!(),
            }
            trace!(log, "node ready"; "node" => node.index());
        };

        if graph
               .neighbors_directed(node, petgraph::EdgeDirection::Incoming)
               .filter(|&ni| ni != source)
               .all(|n| empty.contains(&n)) {
            // all parents are empty, so we can materialize it immediately
            info!(log, "no need to replay empty view"; "node" => node.index());
            empty.insert(node);

            // we need to make sure the domain constructs reader backlog handles!
            graph[node].with_reader(|r| if let Some(key) = r.key() {
                                        use flow::payload::InitialState;

                                        txs[&d]
                                            .send(box Packet::PrepareState {
                                                      node: *addr.as_local(),
                                                      state: InitialState::Global {
                                                          cols: graph[node].fields().len(),
                                                          key: key,
                                                          gid: node,
                                                      },
                                                  })
                                            .unwrap();
                                    });

            ready(txs, index_on);
        } else {
            // if this node doesn't need to be materialized, then we're done. note that this check
            // needs to happen *after* the empty parents check so that we keep tracking whether or
            // not nodes are empty.
            if !has_state {
                debug!(log, "no need to replay non-materialized view"; "node" => node.index());
                ready(txs, index_on);
                continue;
            }

            // we have a parent that has data, so we need to replay and reconstruct
            let start = ::std::time::Instant::now();
            let log = log.new(o!("node" => node.index()));
            info!(log, "beginning reconstruction of {:?}", graph[node]);
            let new_paths = reconstruct(&log,
                                        graph,
                                        &empty,
                                        partial,
                                        partial_ok,
                                        &materialize,
                                        txs,
                                        node,
                                        index_on);
            domains_on_path.extend(new_paths.into_iter());

            // NOTE: the state has already been marked ready by the replay completing,
            // but we want to wait for the domain to finish replay, which a Ready does.
            ready(txs, vec![]);
            info!(log, "reconstruction completed"; "ms" => dur_to_ns!(start.elapsed()) / 1_000_000);
        }
    }
    domains_on_path
}

pub fn reconstruct(log: &Logger,
                   graph: &mut Graph,
                   empty: &HashSet<NodeIndex>,
                   partial: &mut HashSet<NodeIndex>,
                   mut partial_ok: bool,
                   materialized: &HashMap<domain::Index,
                                          HashMap<LocalNodeIndex, Vec<Vec<usize>>>>,
                   txs: &mut HashMap<domain::Index, mpsc::SyncSender<Box<Packet>>>,
                   node: NodeIndex,
                   mut index_on: Vec<Vec<usize>>)
                   -> HashMap<Tag, Vec<domain::Index>> {

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

    // okay, so here's the situation: `node` is a node that
    //
    //   a) was not previously materialized, and
    //   b) now needs to be materialized, and
    //   c) at least one of node's parents has existing data
    //
    // because of the topological traversal done by `initialize`, we know that all our ancestors
    // that should be materialized have been.
    //
    // our plan is as follows:
    //
    //   1. search our ancestors for the closest materialization points along each path
    //   2. for each such path, identify the domains along that path and pause them
    //   3. construct a daisy-chain of channels, and pass them to each domain along the path
    //   4. tell the domain nearest to the root to start replaying
    //
    // so, first things first, let's find all our paths up the tree
    let paths = {
        let mut on_join = cost_fn(log, graph, empty, partial, materialized, txs);
        // TODO: what if we're constructing multiple indices?
        // TODO: what if we have a compound index?
        let trace_col = index_on[0][0];
        keys::provenance_of(graph, node, trace_col, &mut *on_join)
    };

    // cut paths so they only reach to the the closest materialized node
    let paths: Vec<_> = paths
        .into_iter()
        .map(|path| -> Vec<_> {
            let mut found = false;
            path.into_iter()
                .enumerate()
                .take_while(|&(i, (node, _))| {
                    if i == 0 {
                        // first node is target node
                        return true;
                    }

                    // keep taking until we get our first materialized node
                    // (`found` helps us emulate `take_while_inclusive`)
                    let n = &graph[node];
                    if found {
                        // we've already found a materialized node
                        return false;
                    }

                    let is_materialized = materialized
                        .get(&n.domain())
                        .map(|dm| dm.contains_key(n.local_addr().as_local()))
                        .unwrap_or(false);
                    if is_materialized {
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
    // probably only makes sense if index_on.len() == 1, because otherwise we still have to fully
    // replay the ancestor to construct the other index, at which point we might as well fill both
    // indices.
    //
    // futhermore, we need index_on[0].len() == 1, because partial replay with compound indices
    // would require us to do more key provenance resolution, and make sure they all source from
    // the same view. parent state!
    //
    // if there are multiple paths (for example through a union), we'd need more mechanism for
    // partial replay that we don't yet have (like multiple triggers, waiting for *multiple*
    // messages from multiple sources with multiple tags).
    //
    // and perhaps most importantly, does column `index_on[0][0]` of `node` trace back to some
    // `key` in the materialized state we're replaying?
    if partial_ok {
        partial_ok = index_on.len() == 1 && index_on[0].len() == 1 &&
                     paths.iter().all(|path| {
            let &(node, col) = path.last().unwrap();
            if col.is_none() {
                // doesn't trace back to a column
                return false;
            }

            let n = &graph[node];
            let col = col.unwrap();
            // node must also have an *index* on col
            materialized
                .get(&n.domain())
                .and_then(|d| d.get(n.local_addr().as_local()))
                .map(|indices| indices.iter().any(|idx| idx.len() == 1 && idx[0] == col))
                .unwrap_or(false)
        });
    }

    // FIXME: if a reader has no materialized views between it and a union, we will end
    // up in this case. we *can* solve that case by requesting replays across all
    // the tagged paths through the union, but since we at this point in the code don't
    // yet know about those paths, that's a bit inconvenient. we might be able to mvoe
    // this entire block below the main loop somehow (?), but for now:
    if partial_ok {
        if graph[node].is_reader() {
            partial_ok = paths.len() == 1;
        }
    }

    if partial_ok {
        warn!(log, "using partial materialization");
        partial.insert(node);
    }

    let domain = graph[node].domain();
    let addr = *graph[node].local_addr();
    let cols = graph[node].fields().len();
    assert!(!index_on.is_empty(),
            "all materialized nodes must have a state key");

    // tell the domain in question to create an empty state for the node in question
    use flow::payload::InitialState;

    // if there's only one path
    let last_domain = paths.get(0).map(|p| graph[p[0].0].domain());
    let mut first_tag = Some(Tag(TAG_GENERATOR.fetch_add(1, Ordering::SeqCst) as u32));

    // NOTE: we cannot use the impl of DerefMut here, since it (reasonably) disallows getting
    // mutable references to taken state.
    let s = graph[node]
        .with_reader(|r| {
            if partial_ok {
                // make sure Reader is actually prepared to receive state
                assert!(r.is_materialized());

                if paths.len() != 1 {
                    unreachable!(); // due to FIXME above
                }

                // since we're partially materializing a reader node,
                // we need to give it a way to trigger replays.
                InitialState::PartialGlobal {
                    gid: node,
                    cols,
                    key: r.key().unwrap(),
                    tag: first_tag.unwrap(),
                    trigger_tx: txs[&last_domain.unwrap()].clone(),
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

    txs[&domain]
        .send(box Packet::PrepareState {
                  node: *addr.as_local(),
                  state: s,
              })
        .unwrap();

    // NOTE:
    // there could be no paths left here. for example, if a symmetric join is joining an existing
    // view with a newm, empty view, the empty view will be chosen for replay, and will be
    // eliminated in the parents.retain() above.

    // TODO:
    // technically, we can be a bit smarter here. for example, a union with a 1-1 projection does
    // not need to be replayed through if it is not materialized. neither does an ingress node.
    // unfortunately, skipping things this way would make `Message::to` and `Message::from` contain
    // weird values, and cause breakage.

    // TODO FIXME:
    // we need to detect materialized nodes downstream of partially materialized nodes.

    let mut domains_on_path = HashMap::new();

    // set up channels for replay along each path
    for mut path in paths {
        // there should always be a replay path
        assert!(!path.is_empty(),
                "needed to replay non-empty node, but no materializations found");

        // we want path to have the ancestor closest to the root *first*
        path.reverse();

        let tag = first_tag
            .take()
            .unwrap_or_else(|| Tag(TAG_GENERATOR.fetch_add(1, Ordering::SeqCst) as u32));
        trace!(log, "replaying along path {:?}", path; "tag" => tag.id());

        // partial materialization possible?
        let mut partial = None;
        if partial_ok {
            if let Some(&(_, Some(ref key))) = path.first() {
                partial = Some(key.clone());
            }
        }

        // first, find out which domains we are crossing
        let mut segments = Vec::new();
        let mut last_domain = None;
        for (node, key) in path {
            let domain = graph[node].domain();
            if last_domain.is_none() || domain != last_domain.unwrap() {
                segments.push((domain, Vec::new()));
                last_domain = Some(domain);

                if partial_ok && graph[node].is_transactional() {
                    domains_on_path
                        .entry(tag.clone())
                        .or_insert_with(Vec::new)
                        .push(domain.clone());
                }
            }

            segments.last_mut().unwrap().1.push((node, key));
        }

        debug!(log, "domain replay path is {:?}", segments; "tag" => tag.id());

        let locals = |i: usize| -> Vec<(NodeAddress, Option<usize>)> {
            let mut skip = 0;
            if i == 0 {
                // we're not replaying through the starter node
                // *unless* it's a Base (because it might need to add defaults)
                let n = &graph[segments[0].1[0].0];
                if !n.is_internal() || n.get_base().is_none() {
                    skip = 1
                }
            }

            segments[i]
                .1
                .iter()
                .skip(skip)
                .map(|&(ni, key)| (*graph[ni].local_addr(), key))
                .collect()
        };

        let (wait_tx, wait_rx) = mpsc::sync_channel(segments.len());
        let (done_tx, done_rx) = mpsc::sync_channel(1);
        let mut main_done_tx = Some(done_tx);

        // first, tell all the domains about the replay path
        let mut seen = HashSet::new();
        for (i, &(ref domain, ref nodes)) in segments.iter().enumerate() {
            // TODO:
            //  a domain may appear multiple times in this list if a path crosses into the same
            //  domain more than once. currently, that will cause a deadlock.
            assert!(!seen.contains(domain),
                    "a-b-a domain replays are not yet supported");
            seen.insert(*domain);

            let locals = locals(i);
            if locals.is_empty() {
                // first domain may *only* have the starter state
                assert_eq!(i, 0);
                continue;
            }

            let mut setup = box Packet::SetupReplayPath {
                tag: tag,
                source: None,
                path: locals,
                done_tx: None,
                trigger: TriggerEndpoint::None,
                ack: wait_tx.clone(),
            };
            if i == 0 {
                // first domain also gets to know source node
                if let box Packet::SetupReplayPath { ref mut source, .. } = setup {
                    *source = Some(*graph[nodes[0].0].local_addr());
                }
            }


            if let Some(ref key) = partial {
                if let box Packet::SetupReplayPath { ref mut trigger, .. } = setup {
                    if segments.len() == 1 {
                        // replay is entirely contained within one domain
                        *trigger = TriggerEndpoint::Local(vec![*key]);
                    } else if i == 0 {
                        // first domain needs to be told about partial replay trigger
                        *trigger = TriggerEndpoint::Start(vec![*key]);
                    } else if i == segments.len() - 1 {
                        // otherwise, should know what how to trigger partial replay
                        let (tx, rx) = mpsc::channel();
                        txs[&segments[0].0]
                            .send(box Packet::RequestUnboundedTx(tx))
                            .unwrap();
                        let root_unbounded_tx = rx.recv().unwrap();
                        *trigger = TriggerEndpoint::End(root_unbounded_tx);
                    }
                } else {
                    unreachable!();
                }
            } else {
                if i == segments.len() - 1 {
                    // last domain should report when it's done if it is to be fully replayed
                    if let box Packet::SetupReplayPath { ref mut done_tx, .. } = setup {
                        assert!(main_done_tx.is_some());
                        *done_tx = main_done_tx.take();
                    } else {
                        unreachable!();
                    }
                }
            }

            if i != segments.len() - 1 {
                // the last node *must* be an egress node since there's a later domain
                txs[domain]
                    .send(box Packet::UpdateEgress {
                              node: graph[nodes.last().unwrap().0]
                                  .local_addr()
                                  .as_local()
                                  .clone(),
                              new_tx: None,
                              new_tag: Some((tag, segments[i + 1].1[0].0.into())),
                          })
                    .unwrap();
            }

            trace!(log, "telling domain about replay path"; "domain" => domain.index());
            txs[domain].send(setup).unwrap();
        }

        // wait for them all to have seen that message
        for _ in &segments {
            wait_rx.recv().unwrap();
        }
        trace!(log, "all domains ready for replay");

        if !partial_ok {
            // tell the first domain to start playing
            trace!(log, "telling root domain to start replay"; "domain" => segments[0].0.index());
            txs[&segments[0].0]
                .send(box Packet::StartReplay {
                          tag: tag,
                          from: *graph[segments[0].1[0].0].local_addr(),
                          ack: wait_tx.clone(),
                      })
                .unwrap();

            // and finally, wait for the last domain to finish the replay
            trace!(log,
                   "waiting for done message from target";
                   "domain" => segments.last().unwrap().0.index()
            );
            done_rx.recv().unwrap();
        }
    }
    domains_on_path
}

fn cost_fn<'a, T>(log: &'a Logger,
                  graph: &'a Graph,
                  empty: &'a HashSet<NodeIndex>,
                  partial: &'a HashSet<NodeIndex>,
                  materialized: &'a HashMap<domain::Index, HashMap<LocalNodeIndex, T>>,
                  txs: &'a mut HashMap<domain::Index, mpsc::SyncSender<Box<Packet>>>)
                  -> Box<FnMut(NodeIndex, &[NodeIndex]) -> Option<NodeIndex> + 'a> {

    Box::new(move |node, parents| {
        assert!(parents.len() > 1);

        let in_materialized = |ni: NodeIndex| {
            let n = &graph[ni];
            materialized
                .get(&n.domain())
                .map(|dm| dm.contains_key(n.local_addr().as_local()))
                .unwrap_or(false)
        };

        // keep track of remaining parents
        let mut parents = Vec::from(parents);

        let n = &graph[node];
        assert!(n.is_internal());

        // find empty parents
        let empty: HashSet<_> = parents
            .iter()
            .filter(|ni| empty.contains(ni))
            .map(|ni| *graph[*ni].local_addr())
            .collect();

        let options = n.must_replay_among()
            .expect("join did not have must replay preference");

        // we *must* replay the state of one of the nodes in options
        parents.retain(|&parent| options.contains(graph[parent].local_addr()));
        assert!(!parents.is_empty());

        // if there is only one left, we don't have a choice
        if parents.len() == 1 {
            // no need to pick
            return parents.pop();
        }

        // if *all* the options are empty, we can safely pick any of them
        if parents
               .iter()
               .all(|&p| empty.contains(graph[p].local_addr())) {
            return parents.pop();
        }

        // if any parent is empty, it is tempting to conclude that the join must be empty (which it
        // must indeed be), and therefore that we can just pick that parent and get a free full
        // materialization. *however*, this would cause the node to be marked as fully
        // materialized, which is *not* okay if it has partially a materialized ancestor!
        if let Some(&parent) = parents
               .iter()
               .find(|&&p| empty.contains(graph[p].local_addr())) {
            if !parents.iter().any(|p| partial.contains(p)) {
                // no partial ancestors!
                return Some(parent);
            }
        }

        // we want to pick the ancestor that causes us to do the least amount of work.
        // this is really a balancing act between
        //
        //   a) how many records we are going to push through the filter; and
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
                while !in_materialized(stateful) {
                    let n = &graph[stateful];
                    // joins require their inputs to be materialized.
                    // therefore, we know that this node must be query_through.
                    assert!(n.is_internal());
                    assert!(n.can_query_through());
                    // if this node is selective (e.g., a filter), increase the filter factor
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
                let (tx, rx) = mpsc::sync_channel(1);
                let stateful = &graph[stateful];
                txs[&stateful.domain()]
                    .send(box Packet::StateSizeProbe {
                              node: *stateful.local_addr().as_local(),
                              ack: tx,
                          })
                    .unwrap();
                let mut size = rx.recv().expect("stateful parent should have state");

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

                debug!(log, "cost of replaying from {:?}: {}", p, cost);
                (p, cost)
            })
            .min_by_key(|&(_, cost)| cost)
            .map(|(node, cost)| {
                debug!(log, "picked replay source {:?}", node; "cost" => cost);
                node
            })
    })
}
