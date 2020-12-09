use dataflow::node;
use dataflow::ops;
use dataflow::prelude::*;
use petgraph;
use petgraph::graph::NodeIndex;
use slog::Logger;
use std::collections::{HashMap, HashSet};

#[allow(clippy::cognitive_complexity)]
pub fn shard(
    log: &Logger,
    graph: &mut Graph,
    new: &mut HashSet<NodeIndex>,
    topo_list: &[NodeIndex],
    sharding_factor: usize,
) -> (Vec<NodeIndex>, HashMap<(NodeIndex, NodeIndex), NodeIndex>) {
    // we must keep track of changes we make to the parent of a node, since this remapping must be
    // communicated to the nodes so they know the true identifier of their parent in the graph.
    let mut swaps = HashMap::new();

    // we want to shard every node by its "input" index. if the index required from a parent
    // doesn't match the current sharding key, we need to do a shuffle (i.e., a Union + Sharder).
    'nodes: for &node in topo_list {
        let mut input_shardings: HashMap<_, _> = graph
            .neighbors_directed(node, petgraph::EdgeDirection::Incoming)
            .map(|ni| (ni, graph[ni].sharded_by()))
            .collect();

        let mut need_sharding = if graph[node].is_internal() || graph[node].is_base() {
            // suggest_indexes is okay because `node` *must* be new, and therefore will return
            // global node indices.
            graph[node].suggest_indexes(node)
        } else if graph[node].is_reader() {
            assert_eq!(input_shardings.len(), 1);
            let ni = input_shardings.keys().next().cloned().unwrap();
            if input_shardings[&ni].is_none() {
                continue;
            }

            let s = graph[node]
                .with_reader(|r| r.key())
                .unwrap()
                .and_then(|c| {
                    if c.len() == 1 {
                        if graph[node].fields()[c[0]] == "bogokey" {
                            Some(Sharding::ForcedNone)
                        } else {
                            Some(Sharding::ByColumn(c[0], sharding_factor))
                        }
                    } else {
                        None
                    }
                })
                .unwrap_or(Sharding::ForcedNone);
            if s.is_none() {
                info!(log, "de-sharding prior to poorly keyed reader"; "node" => ?node);
            } else {
                info!(log, "sharding reader"; "node" => ?node);
                graph[node]
                    .with_reader_mut(|r| r.shard(sharding_factor))
                    .unwrap();
            }

            if s != input_shardings[&ni] {
                // input is sharded by different key -- need shuffle
                reshard(log, new, &mut swaps, graph, ni, node, s);
            }
            graph.node_weight_mut(node).unwrap().shard_by(s);
            continue;
        } else if graph[node].is_source() {
            continue;
        } else {
            // non-internal nodes are always pass-through
            HashMap::new()
        };
        if need_sharding.is_empty()
            && (input_shardings.len() == 1 || input_shardings.iter().all(|(_, &s)| s.is_none()))
        {
            let mut s = if input_shardings
                .iter()
                .any(|(_, &s)| s == Sharding::ForcedNone)
            {
                Sharding::ForcedNone
            } else {
                input_shardings.iter().map(|(_, &s)| s).next().unwrap()
            };
            info!(log, "preserving sharding of pass-through node";
                  "node" => ?node,
                  "sharding" => ?s);

            if graph[node].is_internal() || graph[node].is_base() {
                if let Sharding::ByColumn(c, shards) = s {
                    // remap c according to node's semantics
                    let n = &graph[node];
                    let src = (0..n.fields().len()).find(|&col| {
                        if let Some(src) = n.parent_columns(col)[0].1 {
                            src == c
                        } else {
                            false
                        }
                    });

                    if let Some(src) = src {
                        s = Sharding::ByColumn(src, shards);
                    } else {
                        // sharding column is not emitted by this node!
                        // at this point, sharding is effectively random.
                        s = Sharding::Random(shards);
                    }
                }
            }
            graph.node_weight_mut(node).unwrap().shard_by(s);
            continue;
        }

        let mut complex = false;
        for lookup_col in need_sharding.values() {
            if lookup_col.len() != 1 {
                complex = true;
            }
        }
        if complex {
            if !graph[node].is_base() {
                // not supported yet -- force no sharding
                // TODO: if we're sharding by a two-part key and need sharding by the *first* part
                // of that key, we can probably re-use the existing sharding?
                error!(log, "de-sharding for lack of multi-key sharding support"; "node" => ?node);
                for &ni in input_shardings.keys() {
                    reshard(log, new, &mut swaps, graph, ni, node, Sharding::ForcedNone);
                }
            }
            continue;
        }

        // if a node does a lookup into itself by a given key, it must be sharded by that key (or
        // not at all). this *also* means that its inputs must be sharded by the column(s) that the
        // output column resolves to.
        if let Some(want_sharding) = need_sharding.remove(&node) {
            assert_eq!(want_sharding.len(), 1);
            let want_sharding = want_sharding[0];

            if graph[node].fields()[want_sharding] == "bogokey" {
                info!(log, "de-sharding node that operates on bogokey"; "node" => ?node);
                for (ni, s) in input_shardings.iter_mut() {
                    reshard(log, new, &mut swaps, graph, *ni, node, Sharding::ForcedNone);
                    *s = Sharding::ForcedNone;
                }
                continue;
            }

            let resolved = if graph[node].is_internal() {
                graph[node].resolve(want_sharding)
            } else if graph[node].is_base() {
                // nothing resolves through a base
                None
            } else {
                // non-internal nodes just pass through columns
                assert_eq!(input_shardings.len(), 1);
                Some(
                    graph
                        .neighbors_directed(node, petgraph::EdgeDirection::Incoming)
                        .map(|ni| (ni, want_sharding))
                        .collect(),
                )
            };
            match resolved {
                None if !graph[node].is_base() => {
                    // weird operator -- needs an index in its output, which it generates.
                    // we need to have *no* sharding on our inputs!
                    info!(log, "de-sharding node that partitions by output key";
                          "node" => ?node);
                    for (ni, s) in input_shardings.iter_mut() {
                        reshard(log, new, &mut swaps, graph, *ni, node, Sharding::ForcedNone);
                        *s = Sharding::ForcedNone;
                    }
                    // ok to continue since standard shard_by is None
                    continue;
                }
                None => {
                    // base nodes -- what do we shard them by?
                    warn!(log, "sharding base node"; "node" => ?node, "column" => want_sharding);
                    graph
                        .node_weight_mut(node)
                        .unwrap()
                        .shard_by(Sharding::ByColumn(want_sharding, sharding_factor));
                    continue;
                }
                Some(want_sharding_input) => {
                    let want_sharding_input: HashMap<_, _> =
                        want_sharding_input.into_iter().collect();

                    // we can shard by the ouput column `want_sharding` *only* if we don't do
                    // lookups based on any *other* columns in any ancestor. if we do, we must
                    // force no sharding :(
                    let mut ok = true;
                    for (ni, lookup_col) in &need_sharding {
                        assert_eq!(lookup_col.len(), 1);
                        let lookup_col = lookup_col[0];

                        if let Some(&in_shard_col) = want_sharding_input.get(ni) {
                            if in_shard_col != lookup_col {
                                // we do lookups on this input on a different column than the one
                                // that produces the output shard column.
                                warn!(log, "not sharding self-lookup node; lookup conflict";
                                      "node" => ?node,
                                      "wants" => want_sharding,
                                      "lookup" => ?(ni, lookup_col));
                                ok = false;
                            }
                        } else {
                            // we do lookups on this input column, but it's not the one we're
                            // sharding output on -- no unambigous sharding.
                            warn!(log, "not sharding self-lookup node; also looks up by other";
                                  "node" => ?node,
                                  "wants" => want_sharding,
                                  "lookup" => ?(ni, lookup_col));
                            ok = false;
                        }
                    }

                    if ok {
                        // we can shard ourselves and our inputs by a single column!
                        let s = Sharding::ByColumn(want_sharding, sharding_factor);
                        info!(log, "sharding node doing self-lookup";
                              "node" => ?node,
                              "sharding" => ?s);

                        for (ni, col) in want_sharding_input {
                            let need_sharding = Sharding::ByColumn(col, sharding_factor);
                            if input_shardings[&ni] != need_sharding {
                                // input is sharded by different key -- need shuffle
                                reshard(log, new, &mut swaps, graph, ni, node, need_sharding);
                                input_shardings.insert(ni, need_sharding);
                            }
                        }

                        graph.node_weight_mut(node).unwrap().shard_by(s);
                        continue;
                    }
                }
            }

        // if we get here, there is no way to reconcile the sharding the node needs to do
        // lookups on its own state with the lookup key it uses for its ancestors, so we must
        // force no sharding.
        } else {
            // if we get here, the node does no lookups into itself, but we still need to figure
            // out a "safe" sharding for it given that its inputs may be sharded. the safe thing to
            // do here is to simply force all our ancestors to be unsharded, but that would lead to
            // a very suboptimal graph. instead, we try to choose a sharding that is "harmonious"
            // with that of our inputs.
            debug!(log, "testing for harmonious sharding"; "node" => ?node);

            // you can think of this loop as happening inside each of the ifs below, just hoisted
            // up to share some code.
            'outer: for col in 0..graph[node].fields().len() {
                let srcs = if graph[node].is_base() {
                    vec![(node, None)]
                } else {
                    graph[node].parent_columns(col)
                };
                let srcs: Vec<_> = srcs
                    .into_iter()
                    .filter_map(|(ni, src)| src.map(|src| (ni, src)))
                    .collect();

                if srcs.len() != input_shardings.len() {
                    // column does not resolve to all inputs
                    continue;
                }

                if need_sharding.is_empty() {
                    // if we don't ever do lookups into our ancestors, we just need to find _some_
                    // good sharding for this node. a column that resolves to all ancestors makes
                    // for a good candidate! if this single output column (which resolves to a
                    // column in all our inputs) matches what each ancestor is individually sharded
                    // by, then we know that the output of the node is also sharded by that key.
                    // this is sufficiently common that we want to make sure we don't accidentally
                    // shuffle in those cases.

                    let mut all_same = true;
                    for &(ni, src) in &srcs {
                        if input_shardings[&ni] != Sharding::ByColumn(src, sharding_factor) {
                            all_same = false;
                            break;
                        }
                    }

                    if all_same {
                        // col is consistent with all input shardings!
                        let s = Sharding::ByColumn(col, sharding_factor);
                        info!(log, "continuing consistent sharding through node";
                              "node" => ?node,
                              "sharding" => ?s);
                        graph.node_weight_mut(node).unwrap().shard_by(s);
                        continue 'nodes;
                    }
                } else {
                    // if a single output column resolves to the lookup column we use for *every*
                    // ancestor, we know that sharding by that column is safe, so we shard the node
                    // by that key (and shuffle any inputs that are not already shareded by the
                    // chosen column).

                    for &(ni, src) in &srcs {
                        match need_sharding.get(&ni) {
                            Some(col) if col.len() != 1 => {
                                // we're looking up by a compound key -- that's hard to shard
                                trace!(log, "column traces to node looked up in by compound key";
                                   "node" => ?node,
                                   "ancestor" => ?ni,
                                   "column" => src);
                                // give up and just force no sharding
                                break 'outer;
                            }
                            Some(col) if col[0] != src => {
                                // we're looking up by a different key. it's kind of weird that this
                                // output column still resolved to a column in all our inputs...
                                trace!(log, "column traces to node that is not looked up by";
                                   "node" => ?node,
                                   "ancestor" => ?ni,
                                   "column" => src,
                                   "lookup" => col[0]);
                                // let's hope another column works instead
                                continue 'outer;
                            }
                            Some(_) => {
                                // looking up by the same column -- that's fine
                            }
                            None => {
                                // we're never looking up in this view. must mean that a given
                                // column resolved to *two* columns in the *same* view?
                                unreachable!()
                            }
                        }
                    }

                    // `col` resolves to the same column we use to lookup in each ancestor
                    // so it's safe for us to shard by `col`!
                    let s = Sharding::ByColumn(col, sharding_factor);
                    info!(log, "sharding node with consistent lookup column";
                          "node" => ?node,
                          "sharding" => ?s);

                    // we have to ensure that each input is also sharded by that key
                    // specifically, some inputs may _not_ be sharded previously
                    for &(ni, src) in &srcs {
                        let need_sharding = Sharding::ByColumn(src, sharding_factor);
                        if input_shardings[&ni] != need_sharding {
                            debug!(log, "resharding input with sharding {:?} to match desired sharding {:?}",
                               input_shardings[&ni], need_sharding; "node" => ?node, "input" => ?ni);
                            reshard(log, new, &mut swaps, graph, ni, node, need_sharding);
                            input_shardings.insert(ni, need_sharding);
                        }
                    }
                    graph.node_weight_mut(node).unwrap().shard_by(s);
                    continue 'nodes;
                }
            }

            if need_sharding.is_empty() {
                // if we get here, that means no one column resolves to matching shardings across
                // all ancestors. we have two options here, either force no sharding or force
                // sharding to the "most common" sharding of our ancestors. the latter is left as
                // TODO for now.
            } else {
                // if we get here, there is no way the node can be sharded such that all of its
                // lookups are satisfiable on one shard. this effectively means that the operator
                // is unshardeable (or would need to _always_ do remote lookups for some
                // ancestors).
            }
        }

        // force everything to be unsharded...
        let sharding = Sharding::ForcedNone;
        warn!(log, "forcing de-sharding"; "node" => ?node);
        for (&ni, in_sharding) in &mut input_shardings {
            if !in_sharding.is_none() {
                // ancestor must be forced to right sharding
                reshard(log, new, &mut swaps, graph, ni, node, sharding);
                *in_sharding = sharding;
            }
        }
    }

    // the code above can do some stupid things, such as adding a sharder after a new, unsharded
    // node. we want to "flatten" such cases so that we shard as early as we can.
    let mut new_sharders: Vec<_> = new
        .iter()
        .filter(|&&n| graph[n].is_sharder())
        .cloned()
        .collect();
    let mut gone = HashSet::new();
    while !new_sharders.is_empty() {
        'sharders: for n in new_sharders.split_off(0) {
            trace!(log, "can we eliminate sharder {:?}?", n);

            if gone.contains(&n) {
                trace!(log, "no, parent is weird (already eliminated)");
                continue;
            }

            // we know that a sharder only has one parent.
            let p = {
                let mut ps = graph.neighbors_directed(n, petgraph::EdgeDirection::Incoming);
                let p = ps.next().unwrap();
                assert_eq!(ps.count(), 0);
                p
            };

            // a sharder should never be placed right under the source node
            assert!(!graph[p].is_source());

            // and that its children must be sharded somehow (otherwise what is the sharder doing?)
            let col = graph[n].with_sharder(|s| s.sharded_by()).unwrap();
            let by = Sharding::ByColumn(col, sharding_factor);

            // we can only push sharding above newly created nodes that are not already sharded.
            if !new.contains(&p) || graph[p].sharded_by() != Sharding::None {
                trace!(log, "no, parent is weird (not new or already sharded)");
                continue;
            }

            // if the parent is a base, the only option we have is to shard the base.
            if graph[p].is_base() {
                trace!(log, "well, its parent is a base");

                // we can't shard compound bases (yet)
                if let Some(k) = graph[p].get_base().unwrap().key() {
                    if k.len() != 1 {
                        trace!(log, "no, parent is weird (has compound key)");
                        continue;
                    }
                }

                // if the base has other children, sharding it may have other effects
                if graph
                    .neighbors_directed(p, petgraph::EdgeDirection::Outgoing)
                    .count()
                    != 1
                {
                    // TODO: technically we could still do this if the other children were
                    // sharded by the same column.
                    trace!(log, "no, parent is weird (has other children)");
                    continue;
                }

                // shard the base
                warn!(log, "eagerly sharding unsharded base"; "by" => col, "base" => ?p);
                graph[p].shard_by(by);
                // remove the sharder at n by rewiring its outgoing edges directly to the base.
                let mut cs = graph
                    .neighbors_directed(n, petgraph::EdgeDirection::Outgoing)
                    .detach();
                while let Some((_, c)) = cs.next(&graph) {
                    // undo the swap that inserting the sharder in the first place generated
                    swaps.remove(&(c, p)).unwrap();
                    // unwire the child from the sharder and wire to the base directly
                    let e = graph.find_edge(n, c).unwrap();
                    graph.remove_edge(e).unwrap();
                    graph.add_edge(p, c, ());
                }
                // also unwire the sharder from the base
                let e = graph.find_edge(p, n).unwrap();
                graph.remove_edge(e).unwrap();
                // NOTE: we can't remove nodes from the graph, because petgraph doesn't
                // guarantee that NodeIndexes are stable when nodes are removed from the
                // graph.
                graph[n].remove();
                gone.insert(n);
                continue;
            }

            let src_cols = graph[p].parent_columns(col);
            if src_cols.len() != 1 {
                // TODO: technically we could push the sharder to all parents here
                continue;
            }
            let (grandp, src_col) = src_cols[0];
            if src_col.is_none() {
                // we can't shard a node by a column it generates
                continue;
            }
            let src_col = src_col.unwrap();

            // we now know that we have the following
            //
            //    grandp[src_col] -> p[col] -> n[col] ---> nchildren[][]
            //                       :
            //                       +----> pchildren[col][]
            //
            // we want to move the sharder to "before" p.
            // this requires us to:
            //
            //  - rewire all nchildren to refer to p instead of n
            //  - rewire p so that it refers to n instead of grandp
            //  - remove any pchildren that also shard p by the same key
            //  - mark p as sharded
            //
            // there are some cases we need to look out for though. in particular, if any of n's
            // siblings (i.e., pchildren) do *not* have a sharder, we can't lift n!

            let mut remove = Vec::new();
            for c in graph.neighbors_directed(p, petgraph::EdgeDirection::Outgoing) {
                // what does c shard by?
                let col = graph[c].with_sharder(|s| s.sharded_by());
                if col.is_none() {
                    // lifting n would shard a node that isn't expecting to be sharded
                    // TODO: we *could* insert a de-shard here
                    continue 'sharders;
                }
                let csharding = Sharding::ByColumn(col.unwrap(), sharding_factor);

                if csharding == by {
                    // sharding by the same key, which is now unnecessary.
                    remove.push(c);
                } else {
                    // sharding by a different key, which is okay
                    //
                    // TODO:
                    // we have two sharders for different keys below p
                    // which should we shard p by?
                }
            }

            // it is now safe to hoist the sharder

            // first, remove any sharders that are now unnecessary. unfortunately, we can't fully
            // remove nodes from the graph, because petgraph doesn't guarantee that NodeIndexes are
            // stable when nodes are removed from the graph.
            for c in remove {
                // disconnect the sharder from p
                let e = graph.find_edge(p, c).unwrap();
                graph.remove_edge(e);
                // connect its children to p directly
                let mut grandc = graph
                    .neighbors_directed(c, petgraph::EdgeDirection::Outgoing)
                    .detach();
                while let Some((_, gc)) = grandc.next(&graph) {
                    let e = graph.find_edge(c, gc).unwrap();
                    graph.remove_edge(e).unwrap();
                    // undo any swaps as well
                    swaps.remove(&(gc, p));
                    // add back the original edge
                    graph.add_edge(p, gc, ());
                }
                // c is now entirely disconnected from the graph
                // if petgraph indices were stable, we could now remove c (if != n) from the graph
                if c != n {
                    graph[c].remove();
                    gone.insert(c);
                }
            }

            let mut grandp = grandp;
            let real_grandp = grandp;
            if let Some(current_grandp) = swaps.get(&(p, grandp)) {
                // so, this is interesting... the parent of p has *already* been swapped, most
                // likely by another (hoisted) sharder. it doesn't really matter to us here, but we
                // will want to remove the duplication of sharders (whcih we'll do below).
                grandp = *current_grandp;
            }

            // then wire us (n) above the parent instead
            warn!(log, "hoisting sharder above new unsharded node"; "sharder" => ?n, "node" => ?p);
            let new = graph[grandp].mirror(node::special::Sharder::new(src_col));
            *graph.node_weight_mut(n).unwrap() = new;
            let e = graph.find_edge(grandp, p).unwrap();
            graph.remove_edge(e).unwrap();
            graph.add_edge(grandp, n, ());
            graph.add_edge(n, p, ());
            swaps.remove(&(p, grandp)); // may be None
            swaps.insert((p, real_grandp), n);

            // mark p as now being sharded
            graph[p].shard_by(by);

            // and then recurse up to checking us again
            new_sharders.push(n);
        }
    }

    // and finally, because we don't *currently* support sharded shuffles (i.e., going directly
    // from one sharding to another), we replace such patterns with a merge + a shuffle. the merge
    // will ensure that replays from the first sharding are turned into a single update before
    // arriving at the second sharding, and the merged sharder will ensure that nshards is set
    // correctly.
    let sharded_sharders: Vec<_> = new
        .iter()
        .filter(|&&n| graph[n].is_sharder() && !graph[n].sharded_by().is_none())
        .cloned()
        .collect();
    for n in sharded_sharders {
        // sharding what?
        let p = {
            let mut ps = graph.neighbors_directed(n, petgraph::EdgeDirection::Incoming);
            let p = ps.next().unwrap();
            assert_eq!(ps.count(), 0);
            p
        };
        error!(log, "preventing unsupported sharded shuffle"; "sharder" => ?n);
        reshard(log, new, &mut swaps, graph, p, n, Sharding::ForcedNone);
        graph
            .node_weight_mut(n)
            .unwrap()
            .shard_by(Sharding::ForcedNone);
    }

    // check that we didn't mess anything up
    // topo list changed though, so re-compute it
    let mut topo_list = Vec::with_capacity(new.len());
    let mut topo = petgraph::visit::Topo::new(&*graph);
    while let Some(node) = topo.next(&*graph) {
        if graph[node].is_source() || graph[node].is_dropped() {
            continue;
        }
        if !new.contains(&node) {
            continue;
        }
        topo_list.push(node);
    }
    validate(log, graph, &topo_list, sharding_factor);

    (topo_list, swaps)
}

/// Modify the graph such that the path between `src` and `dst` shuffles the input such that the
/// records received by `dst` are sharded by sharding `to`.
fn reshard(
    log: &Logger,
    new: &mut HashSet<NodeIndex>,
    swaps: &mut HashMap<(NodeIndex, NodeIndex), NodeIndex>,
    graph: &mut Graph,
    src: NodeIndex,
    dst: NodeIndex,
    to: Sharding,
) {
    assert!(!graph[src].is_source());

    if graph[src].sharded_by().is_none() && to.is_none() {
        debug!(log, "no need to shuffle";
               "src" => ?src,
               "dst" => ?dst,
               "sharding" => ?to);
        return;
    }

    let node = match to {
        Sharding::None | Sharding::ForcedNone => {
            // NOTE: this *must* be a union so that we correctly buffer partial replays
            let n: NodeOperator =
                ops::union::Union::new_deshard(src, graph[src].sharded_by()).into();
            let mut n = graph[src].mirror(n);
            n.shard_by(to);
            n
        }
        Sharding::ByColumn(c, _) => {
            let mut n = graph[src].mirror(node::special::Sharder::new(c));
            n.shard_by(graph[src].sharded_by());
            n
        }
        Sharding::Random(_) => unreachable!(),
    };
    let node = graph.add_node(node);
    error!(log, "told to shuffle";
           "src" => ?src,
           "dst" => ?dst,
           "using" => ?node,
           "sharding" => ?to);

    new.insert(node);

    // TODO: if there is already sharder child of src with the right sharding target,
    // just add us as a child of that node!

    // hook in node that does appropriate shuffle
    let old = graph.find_edge(src, dst).unwrap();
    graph.remove_edge(old).unwrap();
    graph.add_edge(src, node, ());
    graph.add_edge(node, dst, ());

    // if `dst` refers to `src`, it now needs to refer to `node` instead
    let old = swaps.insert((dst, src), node);
    assert_eq!(
        old, None,
        "re-sharding already sharded node introduces swap collision"
    );
}

pub fn validate(log: &Logger, graph: &Graph, topo_list: &[NodeIndex], sharding_factor: usize) {
    // ensure that each node matches the sharding of each of its ancestors, unless the ancestor is
    // a sharder or a shard merger
    for &node in topo_list {
        let n = &graph[node];
        if n.is_internal() && n.is_shard_merger() {
            // shard mergers legitimately have a different sharding than their ancestors
            continue;
        }

        let inputs: Vec<_> = graph
            .neighbors_directed(node, petgraph::EdgeDirection::Incoming)
            .filter(|ni| !graph[*ni].is_source())
            .collect();

        let remap = |nd: &Node, pni: NodeIndex, ps: Sharding| -> Sharding {
            if nd.is_internal() || nd.is_base() {
                if let Sharding::ByColumn(c, shards) = ps {
                    // remap c according to node's semantics
                    let src = (0..nd.fields().len()).find(|&col| {
                        for pc in nd.parent_columns(col) {
                            if let (p, Some(src)) = pc {
                                // found column c in parent pni
                                if p == pni && src == c {
                                    // extract *child* column ID that we found a match for
                                    return true;
                                } else if !graph[pni].is_internal() {
                                    // need to look transitively for an indirect parent, since
                                    // `parent_columns`'s return values does not take sharder
                                    // and desharder nodes previously added into account (as
                                    // the `src` in the operator is only rewritten to the
                                    // sharder later, in `on_connected`).
                                    // NOTE(malte): just checking connectivity here is perhaps a
                                    // bit too lax (i.e., may miss some incorrect shardings)
                                    if petgraph::algo::has_path_connecting(graph, p, pni, None)
                                        && src == c
                                    {
                                        return true;
                                    }
                                }
                            }
                        }
                        false
                    });

                    if let Some(src) = src {
                        return Sharding::ByColumn(src, shards);
                    } else {
                        return Sharding::Random(shards);
                    }
                }
            }
            // in all other cases, the sharding matches the parent's
            ps
        };

        for in_ni in inputs {
            let in_node = &graph[in_ni];
            if in_node.is_sharder() {
                // ancestor is a sharder, so its output sharding must match ours
                in_node.with_sharder(|s| {
                    let in_sharding = remap(
                        n,
                        in_ni,
                        Sharding::ByColumn(s.sharded_by(), sharding_factor),
                    );
                    if in_sharding != n.sharded_by() {
                        crit!(
                            log,
                            "invalid sharding: {} shards to {:?} != {}'s {:?}",
                            in_ni.index(),
                            in_sharding,
                            node.index(),
                            n.sharded_by(),
                        );
                    }
                    assert_eq!(in_sharding, n.sharded_by());
                });
            } else {
                // ancestor is an ordinary node, so it must have the same sharding
                let in_sharding = remap(n, in_ni, in_node.sharded_by());
                let out_sharding = n.sharded_by();
                let equal = match in_sharding {
                    // ForcedNone and None are different enum variants, but correspond to the same
                    // sharding (namely, none)
                    Sharding::ForcedNone | Sharding::None => match out_sharding {
                        Sharding::ForcedNone | Sharding::None => true,
                        _ => in_sharding == out_sharding,
                    },
                    _ => in_sharding == out_sharding,
                };

                if !equal {
                    crit!(
                        log,
                        "invalid sharding: {} sharded by {:?} != {}'s {:?}",
                        in_ni.index(),
                        in_sharding,
                        node.index(),
                        graph[node].sharded_by(),
                    );
                }
                assert!(equal);
            }
        }
    }
}
