use flow::prelude::*;
use flow::domain;
use petgraph::graph::NodeIndex;
use std::collections::{HashSet, HashMap};
use slog::Logger;
use petgraph;

#[derive(Copy, Clone, PartialEq, Eq)]
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
    for node in topo_list {
        let mut input_shardings: HashMap<_, _> = graph
            .neighbors_directed(node, petgraph::EdgeDirection::Incoming)
            .map(|ni| (ni, graph[ni].sharded_by()))
            .collect();

        let mut need_sharding = graph[node].suggest_indexes(node.into());
        if need_sharding.is_empty() {
            // no shuffle necessary -- can re-use any existing sharding
            let s = if input_shardings.len() == 1 {
                input_shardings.into_iter().map(|(_, s)| s).next().unwrap()
            } else {
                Sharding::Random
            };
            graph.node_weight_mut(node).unwrap().shard_by(s);
            continue;
        }

        let mut sharding = Sharding::Random;

        // if a node does a lookup into itself by a given key, it must be sharded by that key (or
        // not at all). this *also* means that its inputs must be sharded by the column(s) that the
        // output column resolves to.
        if let Some(need_sharding) = need_sharding.remove(&node.into()) {
            if need_sharding.len() != 1 {
                unimplemented!();
            }
            let need_sharding = need_sharding[0];

            match graph[node].resolve(need_sharding) {
                None => {
                    // weird operator -- needs an index in its output, which it generates.
                    // we need to have *no* sharding on our inputs!
                    for (ni, s) in input_shardings.iter_mut() {
                        reshard(graph, *ni, node, Sharding::None);
                        *s = Sharding::None;
                    }
                }
                Some(need_sharding_in) => {
                    for (ni, need_sharding) in need_sharding_in {
                        let need_sharding = Sharding::ByColumn(need_sharding);
                        if input_shardings[ni.as_global()] != need_sharding {
                            // input is sharded by different key -- need shuffle
                            reshard(graph, *ni.as_global(), node, need_sharding);
                            input_shardings.insert(*ni.as_global(), need_sharding);
                        }
                    }
                    sharding = Sharding::ByColumn(need_sharding);
                }
            }
        }

        if input_shardings.values().all(|s| s == &Sharding::None) {
            // none of our inputs are sharded, so we are also not sharded
            // TODO: we kind of want to know if any of our children (transitively) *want* us to
            // sharded by a particular key. if they do, we could shard more of the computation,
            // which is probably good for us.
            sharding = Sharding::None

            // if sharding == Sharding::Random, the if let Some above must have kicked in, and we
            // might want to start sharding here and keep that sharding.
        } else {
            // we do lookups into at least one view that is sharded. the safe thing to do here is
            // to simply force all our ancestors to be unsharded. however, if a single output
            // column resolves to the lookup column of *every* ancestor, we know that sharding by
            // that column *should* be safe, so we mark the output as sharded by that key.
            let mut done = false;
            'outer: for col in 0..graph[node].fields().len() {
                let srcs = match graph[node].resolve(col) {
                    Some(srcs) => srcs,
                    None => continue,
                };

                if srcs.len() != input_shardings.len() {
                    continue;
                }

                // resolved to all ancestors!
                // does it match the key we're doing lookups based on?
                for &(ni, src) in &srcs {
                    match need_sharding.get(&ni) {
                        Some(col) if col.len() != 1 => {
                            // we're looking up by a compound key -- that's hard to shard
                            continue 'outer;
                        }
                        Some(col) if col[0] != src => {
                            // we're looking up by a different key. it's kind of weird that this
                            // output column still resolved to a column in all our inputs...
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
                // so it's safe for us to shard by `col`! we do have to ensure that each
                // input is also sharded by that key though.
                if let Sharding::ByColumn(oldcol) = sharding {
                    // we've already chosen to shard by a key?
                    assert_eq!(oldcol, col);
                }
                sharding = Sharding::ByColumn(col);
                for &(ni, src) in &srcs {
                    let need_sharding = Sharding::ByColumn(src);
                    if input_shardings[ni.as_global()] != need_sharding {
                        reshard(graph, *ni.as_global(), node, need_sharding);
                        input_shardings.insert(*ni.as_global(), need_sharding);
                    }
                }
                done = true;
                break;
            }

            if !done {
                // we couldn't use our heuristic :(
                // force everything to be unsharded...
                sharding = Sharding::None;
                for ni in need_sharding.keys() {
                    if input_shardings[ni.as_global()] != sharding {
                        // ancestor must be forced to right sharding
                        reshard(graph, *ni.as_global(), node, sharding);
                        input_shardings.insert(*ni.as_global(), sharding);
                    }
                }
            }
        }

        graph.node_weight_mut(node).unwrap().shard_by(sharding);
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
fn reshard(graph: &mut Graph, src: NodeIndex, dst: NodeIndex, to: Sharding) {
    if graph[src].sharded_by() == Sharding::None {
        // src isn't sharded, so conforms to all shardings
        return;
    }

    unimplemented!();
}
