use flow::prelude::*;
use flow::node;
use petgraph::graph::NodeIndex;
use std::collections::{HashMap, HashSet};
use slog::Logger;
use petgraph;
use ops;

#[derive(Copy, Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum Sharding {
    None,
    Random,
    ByColumn(usize),
}

pub fn shard(
    log: &Logger,
    graph: &mut Graph,
    source: NodeIndex,
    new: &mut HashSet<NodeIndex>,
) -> HashMap<(NodeIndex, NodeIndex), NodeIndex> {
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

    // we must keep track of changes we make to the parent of a node, since this remapping must be
    // communicated to the nodes so they know the true identifier of their parent in the graph.
    let mut swaps = HashMap::new();

    // we want to shard every node by its "input" index. if the index required from a parent
    // doesn't match the current sharding key, we need to do a shuffle (i.e., a Union + Sharder).
    'nodes: for node in topo_list {
        let mut input_shardings: HashMap<_, _> = graph
            .neighbors_directed(node, petgraph::EdgeDirection::Incoming)
            .map(|ni| (ni, graph[ni].sharded_by()))
            .collect();

        let mut need_sharding = if graph[node].is_internal() {
            // suggest_indexes is okay because `node` *must* be new, and therefore will return
            // global node indices.
            graph[node].suggest_indexes(node.into())
        } else if graph[node].is_reader() {
            assert_eq!(input_shardings.len(), 1);
            let ni = input_shardings.keys().next().cloned().unwrap();
            if let Sharding::None = input_shardings[&ni] {
                continue;
            }

            let s = graph[node]
                .with_reader(|r| r.key())
                .unwrap()
                .map(Sharding::ByColumn)
                .unwrap_or(Sharding::None);
            if s == Sharding::None {
                info!(log, "de-sharding prior to stream-only reader"; "node" => ?node);
            } else {
                info!(log, "sharding reader"; "node" => ?node);
                graph[node].with_reader_mut(|r| r.shard(::SHARDS));
            }

            if s != input_shardings[&ni] {
                // input is sharded by different key -- need shuffle
                reshard(log, new, &mut swaps, graph, ni, node, s);
            }
            graph.node_weight_mut(node).unwrap().shard_by(s);
            continue;
        } else {
            // non-internal nodes are always pass-through
            HashMap::new()
        };
        if need_sharding.is_empty() &&
            (input_shardings.len() == 1 ||
                input_shardings.iter().all(|(_, &s)| s == Sharding::None))
        {
            let s = input_shardings.into_iter().map(|(_, s)| s).next().unwrap();
            info!(log, "preserving sharding of pass-through node";
                  "node" => ?node,
                  "sharding" => ?s);
            graph.node_weight_mut(node).unwrap().shard_by(s);
            continue;
        }

        let mut complex = false;
        for (_, &(ref lookup_col, _)) in &need_sharding {
            if lookup_col.len() != 1 {
                complex = true;
            }
        }
        if complex {
            if graph[node].get_base().is_none() {
                // not supported yet -- force no sharding
                // TODO: if we're sharding by a two-part key and need sharding by the *first* part
                // of that key, we can probably re-use the existing sharding?
                error!(log, "de-sharding for lack of multi-key sharding support"; "node" => ?node);
                for (&ni, _) in &input_shardings {
                    reshard(log, new, &mut swaps, graph, ni, node, Sharding::None);
                }
            }
            continue;
        }

        if graph[node].get_base().is_some() && graph[node].is_transactional() {
            error!(log, "not sharding transactional base node"; "node" => ?node);
            continue;
        }

        // if a node does a lookup into itself by a given key, it must be sharded by that key (or
        // not at all). this *also* means that its inputs must be sharded by the column(s) that the
        // output column resolves to.
        if let Some((want_sharding, _)) = need_sharding.remove(&node.into()) {
            assert_eq!(want_sharding.len(), 1);
            let want_sharding = want_sharding[0];

            let resolved = if graph[node].is_internal() {
                graph[node].resolve(want_sharding)
            } else {
                // non-internal nodes just pass through columns
                assert_eq!(input_shardings.len(), 1);
                Some(
                    graph
                        .neighbors_directed(node, petgraph::EdgeDirection::Incoming)
                        .map(|ni| (ni.into(), want_sharding))
                        .collect(),
                )
            };
            match resolved {
                None if !graph[node].is_internal() || graph[node].get_base().is_none() => {
                    // weird operator -- needs an index in its output, which it generates.
                    // we need to have *no* sharding on our inputs!
                    info!(log, "de-sharding node that partitions by output key";
                          "node" => ?node);
                    for (ni, s) in input_shardings.iter_mut() {
                        reshard(log, new, &mut swaps, graph, *ni, node, Sharding::None);
                        *s = Sharding::None;
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
                        .shard_by(Sharding::ByColumn(want_sharding));
                    continue;
                }
                Some(want_sharding_input) => {
                    let want_sharding_input: HashMap<_, _> =
                        want_sharding_input.into_iter().collect();

                    // we can shard by the ouput column `want_sharding` *only* if we don't do
                    // lookups based on any *other* columns in any ancestor. if we do, we must
                    // force no sharding :(
                    let mut ok = true;
                    for (ni, &(ref lookup_col, _)) in &need_sharding {
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
                        let s = Sharding::ByColumn(want_sharding);
                        info!(log, "sharding node doing self-lookup";
                              "node" => ?node,
                              "sharding" => ?s);

                        for (ni, col) in want_sharding_input {
                            let need_sharding = Sharding::ByColumn(col);
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
        }

        if false && input_shardings.values().all(|s| s == &Sharding::None) {
            // none of our inputs are sharded, so we are also not sharded
            // disabled because we want to eagerly reshard our inputs
            //
            // TODO: we kind of want to know if any of our children (transitively) *want* us to
            // sharded by a particular key. if they do, we could shard more of the computation,
            // which is probably good for us.
            info!(log, "preserving non-sharding of node"; "node" => ?node);
            continue;
        }

        // the safe thing to do here is to simply force all our ancestors to be unsharded. however,
        // if a single output column resolves to the lookup column of *every* ancestor, we know
        // that sharding by that column *should* be safe, so we mark the output as sharded by that
        // key. we then make sure all our inputs are sharded by that key too.
        debug!(log, "testing for sharding opportunities"; "node" => ?node);
        'outer: for col in 0..graph[node].fields().len() {
            let srcs = if graph[node].get_base().is_some() {
                vec![(node.into(), None)]
            } else {
                graph[node].parent_columns(col)
            };
            let srcs: Vec<_> = srcs.into_iter()
                .filter_map(|(ni, src)| src.map(|src| (ni, src)))
                .collect();

            if srcs.len() != input_shardings.len() {
                trace!(log, "column does not resolve to all inputs";
                           "node" => ?node,
                           "col" => col,
                           "all" => input_shardings.len(),
                           "got" => srcs.len());
                continue;
            }


            if need_sharding.is_empty() {
                // a node that doesn't need any sharding, yet has multiple parents. must be a
                // union. if this single output column (which resolves to a column in all our
                // inputs) matches what each ancestor is individually sharded by, then we know that
                // the output of the union is also sharded by that key. this is sufficiently common
                // that we want to make sure we don't accidentally shuffle in those cases.
                for &(ni, src) in &srcs {
                    if input_shardings[&ni] != Sharding::ByColumn(src) {
                        // TODO: technically we could revert to Sharding::Random here, which is a
                        // little better than forcing a de-shard, but meh.
                        continue 'outer;
                    }
                }
            } else {
                // `col` resolved to all ancestors!
                // does it match the key we're doing lookups based on?
                for &(ni, src) in &srcs {
                    match need_sharding.get(&ni) {
                        Some(&(ref col, _)) if col.len() != 1 => {
                            // we're looking up by a compound key -- that's hard to shard
                            trace!(log, "column traces to node looked up in by compound key";
                                   "node" => ?node,
                                   "ancestor" => ?ni,
                                   "column" => src);
                            break 'outer;
                        }
                        Some(&(ref col, _)) if col[0] != src => {
                            // we're looking up by a different key. it's kind of weird that this
                            // output column still resolved to a column in all our inputs...
                            trace!(log, "column traces to node that is not looked up by";
                                   "node" => ?node,
                                   "ancestor" => ?ni,
                                   "column" => src,
                                   "lookup" => col[0]);
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
            }

            // `col` resolves to the same column we use to lookup in each ancestor,
            // so it's safe for us to shard by `col`!
            let s = Sharding::ByColumn(col);
            info!(log, "sharding node with consistent lookup column";
                      "node" => ?node,
                      "sharding" => ?s);

            // we have to ensure that each input is also sharded by that key
            for &(ni, src) in &srcs {
                let need_sharding = Sharding::ByColumn(src);
                if input_shardings[&ni] != need_sharding {
                    reshard(log, new, &mut swaps, graph, ni, node, need_sharding);
                    input_shardings.insert(ni, need_sharding);
                }
            }
            graph.node_weight_mut(node).unwrap().shard_by(s);
            continue 'nodes;
        }

        // we couldn't use our heuristic :(
        // force everything to be unsharded...
        let sharding = Sharding::None;
        warn!(log, "forcing de-sharding"; "node" => ?node);
        for &ni in need_sharding.keys() {
            if input_shardings[&ni] != sharding {
                // ancestor must be forced to right sharding
                reshard(log, new, &mut swaps, graph, ni, node, sharding);
                input_shardings.insert(ni, sharding);
            }
        }
    }

    // the code above can do some stupid things, such as adding a sharder after a new, unsharded
    // node. we want to "flatten" such cases so that we shard as early as we can.
    let mut new_sharders: Vec<_> = new.iter()
        .filter(|&&n| graph[n].is_sharder())
        .cloned()
        .collect();
    let mut gone = HashSet::new();
    while !new_sharders.is_empty() {
        'sharders: for n in new_sharders.split_off(0) {
            if gone.contains(&n) {
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
            let by = Sharding::ByColumn(col);

            // we can only push sharding above newly created nodes that are not already sharded.
            if !new.contains(&p) || graph[p].sharded_by() != Sharding::None {
                continue;
            }

            // if the parent is a base, the only option we have is to shard the base.
            if graph[p].get_base().is_some() {
                // sharded transactional bases are hard
                if graph[p].is_transactional() {
                    continue;
                }

                // we can't shard compound bases (yet)
                if let Some(k) = graph[p].get_base().unwrap().key() {
                    if k.len() != 1 {
                        continue;
                    }
                }

                // if the base has other children, sharding it may have other effects
                if graph
                    .neighbors_directed(p, petgraph::EdgeDirection::Outgoing)
                    .count() != 1
                {
                    // TODO: technically we could still do this if the other children were
                    // sharded by the same column.
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
                    let w = graph.remove_edge(e).unwrap();
                    graph.add_edge(p, c, w);
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
                let csharding = Sharding::ByColumn(col.unwrap());

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
                    let w = graph.remove_edge(e).unwrap();
                    // undo the swap as well
                    swaps.remove(&(gc, p)).unwrap();
                    // add back the original edge
                    graph.add_edge(p, gc, w);
                }
                // c is now entirely disconnected from the graph
                // if petgraph indices were stable, we could now remove c (if != n) from the graph
                if c != n {
                    graph[c].remove();
                    gone.insert(c);
                }
            }

            // then wire us (n) above the parent instead
            warn!(log, "hoisting sharder above new unsharded node"; "sharder" => ?n, "node" => ?p);
            let new = graph[grandp].mirror(node::special::Sharder::new(src_col));
            *graph.node_weight_mut(n).unwrap() = new;
            let e = graph.find_edge(grandp, p).unwrap();
            let w = graph.remove_edge(e).unwrap();
            graph.add_edge(grandp, n, ());
            graph.add_edge(n, p, w);
            swaps.insert((p, grandp), n);

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
    let sharded_sharders: Vec<_> = new.iter()
        .filter(|&&n| {
            graph[n].is_sharder() && graph[n].sharded_by() != Sharding::None
        })
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
        reshard(log, new, &mut swaps, graph, p, n, Sharding::None);
        graph.node_weight_mut(n).unwrap().shard_by(Sharding::None);
    }

    swaps
}

/// Modify the graph such that the path between `src` and `dst` shuffles the input such that the
/// records received by `dst` are sharded by column `col`.
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

    if graph[src].sharded_by() == Sharding::None && to == Sharding::None {
        trace!(log, "no need to shuffle";
               "src" => ?src,
               "dst" => ?dst,
               "sharding" => ?to);
        return;
    }

    let node = match to {
        Sharding::None => {
            // NOTE: this *must* be a union so that we correctly buffer partial replays
            let n: NodeOperator = ops::union::Union::new_deshard(src.into(), ::SHARDS).into();
            let mut n = graph[src].mirror(n);
            n.shard_by(Sharding::None);
            n.mark_as_shard_merger(true);
            n
        }
        Sharding::ByColumn(c) => {
            use flow::node;
            let mut n = graph[src].mirror(node::special::Sharder::new(c));
            n.shard_by(graph[src].sharded_by());
            n
        }
        Sharding::Random => unreachable!(),
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
    let was_materialized = graph.remove_edge(old).unwrap();
    graph.add_edge(src, node, ());
    graph.add_edge(node, dst, was_materialized);

    // if `dst` refers to `src`, it now needs to refer to `node` instead
    let old = swaps.insert((dst, src), node);
    assert_eq!(
        old,
        None,
        "re-sharding already sharded node introduces swap collision"
    );
}
