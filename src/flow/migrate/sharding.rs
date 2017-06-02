use flow::prelude::*;
use flow::node;
use petgraph::graph::NodeIndex;
use std::collections::{HashSet, HashMap};
use slog::Logger;
use petgraph;
use ops;

#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub enum Sharding {
    None,
    Random,
    ByColumn(usize),
}

pub fn shard(log: &Logger,
             graph: &mut Graph,
             source: NodeIndex,
             new: &mut HashSet<NodeIndex>)
             -> HashMap<(NodeIndex, NodeIndex), NodeIndex> {

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
            // TODO: we may want to allow sharded Readers eventually...
            info!(log, "forcing de-sharding prior to Reader"; "node" => ?node);
            assert_eq!(input_shardings.len(), 1);
            reshard(log,
                    new,
                    &mut swaps,
                    graph,
                    input_shardings.keys().next().cloned().unwrap(),
                    node,
                    Sharding::None);
            continue;
        } else {
            // non-internal nodes are always pass-through
            HashMap::new()
        };
        if need_sharding.is_empty() {
            // no shuffle necessary -- can re-use any existing sharding
            let s = if input_shardings.len() == 1 ||
                       input_shardings.iter().all(|(_, &s)| s == Sharding::None) {
                input_shardings.into_iter().map(|(_, s)| s).next().unwrap()
            } else {
                // FIXME: if every shard unions with a Sharding::None, we'll get repeated rows :(
                Sharding::Random
            };
            info!(log, "preserving sharding of pass-through node";
                  "node" => ?node,
                  "sharding" => ?s);
            graph.node_weight_mut(node).unwrap().shard_by(s);
            continue;
        }

        let mut complex = false;
        for (_, lookup_col) in &need_sharding {
            if lookup_col.len() != 1 {
                complex = true;
            }
        }
        if complex {
            // not supported yet -- force no sharding
            // TODO: if we're sharding by a two-part key and need sharding by the *first* part
            // of that key, we can probably re-use the existing sharding?
            error!(log, "de-sharding for lack of multi-key sharding support"; "node" => ?node);
            for (&ni, _) in &input_shardings {
                reshard(log, new, &mut swaps, graph, ni, node, Sharding::None);
            }
            continue;
        }

        // if a node does a lookup into itself by a given key, it must be sharded by that key (or
        // not at all). this *also* means that its inputs must be sharded by the column(s) that the
        // output column resolves to.
        if let Some(want_sharding) = need_sharding.remove(&node.into()) {
            assert_eq!(want_sharding.len(), 1);
            let want_sharding = want_sharding[0];

            let resolved = if graph[node].is_internal() {
                graph[node].resolve(want_sharding)
            } else {
                // non-internal nodes just pass through columns
                assert_eq!(input_shardings.len(), 1);
                Some(graph
                         .neighbors_directed(node, petgraph::EdgeDirection::Incoming)
                         .map(|ni| (ni.into(), want_sharding))
                         .collect())
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
                        let s = Sharding::ByColumn(want_sharding);
                        info!(log, "sharding node doing self-lookup";
                              "node" => ?node,
                              "sharding" => ?s);

                        for (ni, col) in want_sharding_input {
                            let need_sharding = Sharding::ByColumn(col);
                            if input_shardings[ni.as_global()] != need_sharding {
                                // input is sharded by different key -- need shuffle
                                reshard(log,
                                        new,
                                        &mut swaps,
                                        graph,
                                        *ni.as_global(),
                                        node,
                                        need_sharding);
                                input_shardings.insert(*ni.as_global(), need_sharding);
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
            let srcs: Vec<_> = graph[node]
                .parent_columns(col)
                .into_iter()
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

            // `col` resolved to all ancestors!
            // does it match the key we're doing lookups based on?
            for &(ni, src) in &srcs {
                match need_sharding.get(&ni) {
                    Some(col) if col.len() != 1 => {
                        // we're looking up by a compound key -- that's hard to shard
                        trace!(log, "column traces to node looked up in by compound key";
                                   "node" => ?node,
                                   "ancestor" => ?ni,
                                   "column" => src);
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

            // `col` resolves to the same column we use to lookup in each ancestor,
            // so it's safe for us to shard by `col`!
            let s = Sharding::ByColumn(col);
            info!(log, "sharding node with consistent lookup column";
                      "node" => ?node,
                      "sharding" => ?s);

            // we have to ensure that each input is also sharded by that key
            for &(ni, src) in &srcs {
                let need_sharding = Sharding::ByColumn(src);
                if input_shardings[ni.as_global()] != need_sharding {
                    reshard(log,
                            new,
                            &mut swaps,
                            graph,
                            *ni.as_global(),
                            node,
                            need_sharding);
                    input_shardings.insert(*ni.as_global(), need_sharding);
                }
            }
            graph.node_weight_mut(node).unwrap().shard_by(s);
            continue 'nodes;
        }

        // we couldn't use our heuristic :(
        // force everything to be unsharded...
        let sharding = Sharding::None;
        warn!(log, "forcing de-sharding"; "node" => ?node);
        for ni in need_sharding.keys() {
            if input_shardings[ni.as_global()] != sharding {
                // ancestor must be forced to right sharding
                reshard(log, new, &mut swaps, graph, *ni.as_global(), node, sharding);
                input_shardings.insert(*ni.as_global(), sharding);
            }
        }
    }

    // the code above can do some stupid things, such as adding a sharder after a new, unsharded
    // node. we want to "flatten" such cases so that we shard as early as we can.
    let mut new_sharders: Vec<_> = new.iter()
        .filter(|&&n| graph[n].is_sharder())
        .cloned()
        .collect();
    let mut stable = false;
    while !stable {
        stable = true;
        for n in new_sharders.split_off(0) {
            let ps: Vec<_> = graph
                .neighbors_directed(n, petgraph::EdgeDirection::Incoming)
                .collect();
            let by = graph[graph
                               .neighbors_directed(n, petgraph::EdgeDirection::Outgoing)
                               .next()
                               .unwrap()]
                .sharded_by();
            // *technically* we could also push sharding up even if some parents are already
            // sharded, but let's leave that for another day.
            //
            // TODO: p could have changed in previous iterations, so we should re-check here
            // whether p.sharded_by() == n.sharded_by() now.
            if ps.iter()
                   .all(|&p| new.contains(&p) && graph[p].sharded_by() == Sharding::None) {
                'parents: for p in ps {
                    if p == source {
                        continue;
                    }

                    let col = match by {
                        Sharding::ByColumn(c) => c,
                        Sharding::Random => continue,
                        Sharding::None => continue,
                    };

                    let src_cols = graph[p].parent_columns(col);
                    if src_cols.len() != 1 {
                        continue;
                    }
                    let (grandp, src_col) = src_cols[0];
                    if src_col.is_none() {
                        continue;
                    }
                    let src_col = src_col.unwrap();
                    let grandp = *grandp.as_global();

                    let mut cs = 0;
                    for c in graph.neighbors_directed(p, petgraph::EdgeDirection::Outgoing) {
                        // if p has other children, it is only safe to push up the Sharder if
                        // those other children are also sharded by the same key.
                        if !graph[c].is_sharder() {
                            continue 'parents;
                        }
                        let sibling_sharder_child = graph
                            .neighbors_directed(c, petgraph::EdgeDirection::Outgoing)
                            .next()
                            .unwrap();
                        if graph[sibling_sharder_child].sharded_by() != by {
                            continue 'parents;
                        }
                        cs += 1;
                    }

                    // it's safe to push up the Sharder.
                    // to do so, we need to undo all the changes we made to `swaps` when
                    // introducing the Sharders below this parent in the first place
                    let mut remove = Vec::with_capacity(cs);
                    let mut children = Vec::new();
                    for c in graph.neighbors_directed(p, petgraph::EdgeDirection::Outgoing) {
                        for grandchild in
                            graph.neighbors_directed(p, petgraph::EdgeDirection::Outgoing) {
                            // undo the swap that inserting the Sharder generated
                            swaps.remove(&(p, grandchild));
                            children.push(grandchild);
                        }
                        if c != n {
                            remove.push(c);
                        }
                    }
                    // we then need to wire in the Sharder above the parent instead
                    let new = graph[p].mirror(node::special::Sharder::new(src_col));
                    *graph.node_weight_mut(n).unwrap() = new;
                    let e = graph.find_edge(p, n).unwrap();
                    graph.remove_edge(e);
                    let e = graph.find_edge(grandp, p).unwrap();
                    let w = graph.remove_edge(e).unwrap();
                    graph.add_edge(grandp, n, w);
                    for &child in &children {
                        if let Some(e) = graph.find_edge(n, child) {
                            graph.remove_edge(e);
                        }
                        // TODO: materialized edges?
                        graph.add_edge(n, child, false);
                    }
                    // and finally, we need to remove any Sharders that were added for other
                    // children. unfortunately, we can't remove nodes from the graph, because
                    // petgraph doesn't guarantee that NodeIndexes are stable when nodes are
                    // removed from the graph.
                    for r in remove {
                        let e = graph.find_edge(p, r).unwrap();
                        graph.remove_edge(e);
                        // ugh:
                        for &child in &children {
                            if let Some(e) = graph.find_edge(r, child) {
                                graph.remove_edge(e);
                            }
                        }
                        // r is now entirely disconnected from the graph
                    }
                }
            }
        }
    }

    swaps
}

/// Modify the graph such that the path between `src` and `dst` shuffles the input such that the
/// records received by `dst` are sharded by column `col`.
fn reshard(log: &Logger,
           new: &mut HashSet<NodeIndex>,
           swaps: &mut HashMap<(NodeIndex, NodeIndex), NodeIndex>,
           graph: &mut Graph,
           src: NodeIndex,
           dst: NodeIndex,
           to: Sharding) {
    if graph[src].sharded_by() == Sharding::None {
        // src isn't sharded, so conforms to all shardings
        return;
    }

    let node = match to {
        Sharding::None => {
            // an identity node that is *not* marked as sharded will end up acting like a union!
            let n: NodeOperator = ops::identity::Identity::new(src.into()).into();
            let mut n = graph[src].mirror(n);
            n.shard_by(Sharding::None);
            n
        }
        Sharding::ByColumn(c) => {
            use flow::node;
            let mut n = graph[src].mirror(node::special::Sharder::new(c));

            // the sharder itself isn't sharded
            n.shard_by(Sharding::None);

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

    // hook in node that does appropriate shuffle
    // FIXME: what if we already added a sharder in previous migration?
    let old = graph.find_edge(src, dst).unwrap();
    let was_materialized = graph.remove_edge(old).unwrap();
    graph.add_edge(src, node, false);
    graph.add_edge(node, dst, was_materialized);

    // if `dst` refers to `src`, it now needs to refer to `node` instead
    let old = swaps.insert((dst, src), node);
    assert_eq!(old,
               None,
               "re-sharding already sharded node introduces swap collision");
}
