use flow::prelude::*;
use flow::domain;
use petgraph::graph::NodeIndex;
use std::collections::{HashSet, HashMap};
use slog::Logger;
use petgraph;

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
             -> HashMap<domain::Index, HashMap<NodeIndex, NodeIndex>> {

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

        // FIXME: suggest_indexes will start returning local indices after the first migration :(
        let mut need_sharding = if graph[node].is_internal() {
            graph[node].suggest_indexes(node.into())
        } else if graph[node].is_reader() {
            // TODO: we may want to allow sharded Readers eventually...
            info!(log, "forcing de-sharding prior to Reader"; "node" => ?node);
            assert_eq!(input_shardings.len(), 1);
            reshard(log,
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
            let s = if input_shardings.len() == 1 {
                input_shardings.into_iter().map(|(_, s)| s).next().unwrap()
            } else {
                Sharding::Random
            };
            info!(log, "preserving sharding of pass-through node";
                  "node" => ?node,
                  "sharding" => ?s);
            graph.node_weight_mut(node).unwrap().shard_by(s);
            continue;
        }

        // if a node does a lookup into itself by a given key, it must be sharded by that key (or
        // not at all). this *also* means that its inputs must be sharded by the column(s) that the
        // output column resolves to.
        if let Some(want_sharding) = need_sharding.remove(&node.into()) {
            if want_sharding.len() != 1 {
                // no supported yet -- force no sharding
                error!(log, "de-sharding for lack of multi-key sharding support"; "node" => ?node);
                for (&ni, _) in &input_shardings {
                    reshard(log, graph, ni, node, Sharding::None);
                }
                continue;
            }
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
                        reshard(log, graph, *ni, node, Sharding::None);
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
                        if lookup_col.len() != 1 {
                            unimplemented!();
                        }
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
                                reshard(log, graph, *ni.as_global(), node, need_sharding);
                                input_shardings.insert(*ni.as_global(), need_sharding);
                            }
                        }

                        graph.node_weight_mut(node).unwrap().shard_by(s);
                        continue;
                    }
                }
            }
        }

        if input_shardings.values().all(|s| s == &Sharding::None) {
            // none of our inputs are sharded, so we are also not sharded
            //
            // TODO: we kind of want to know if any of our children (transitively) *want* us to
            // sharded by a particular key. if they do, we could shard more of the computation,
            // which is probably good for us.
            info!(log, "preserving non-sharding of node"; "node" => ?node);
            continue;
        } else {
            // we do lookups into at least one view that is sharded. the safe thing to do here is
            // to simply force all our ancestors to be unsharded. however, if a single output
            // column resolves to the lookup column of *every* ancestor, we know that sharding by
            // that column *should* be safe, so we mark the output as sharded by that key.
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
                        reshard(log, graph, *ni.as_global(), node, need_sharding);
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
                    reshard(log, graph, *ni.as_global(), node, sharding);
                    input_shardings.insert(*ni.as_global(), sharding);
                }
            }
        }
    }

    // TODO
    // how do we actually split a sharded node?
    // ideally we want to keep the graph representation unchanged, and instead just *instantiate*
    // multiple copies of the given node. but that probably has very deep-reaching consequences in
    // the code...

    // TODO
    // maybe we can "flatten" shuffles here? if we detect a node that is unsharded that is
    // immediately followed by a sharder, we can push the sharder up?

    swaps
}

/// Modify the graph such that the path between `src` and `dst` shuffles the input such that the
/// records received by `dst` are sharded by column `col`.
fn reshard(log: &Logger, graph: &mut Graph, src: NodeIndex, dst: NodeIndex, to: Sharding) {
    if graph[src].sharded_by() == Sharding::None {
        // src isn't sharded, so conforms to all shardings
        return;
    }

    error!(log, "told to shuffle";
           "src" => ?src,
           "dst" => ?dst,
           "sharding" => ?to);
    //unimplemented!();
}
