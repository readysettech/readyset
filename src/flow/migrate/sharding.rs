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
        let input_shardings: HashMap<_, _> = graph
            .neighbors_directed(node, petgraph::EdgeDirection::Incoming)
            .map(|ni| (ni, graph[ni].sharded_by()))
            .collect();

        let need_sharding = graph[node].suggest_indexes(node.into());
        if need_sharding.is_empty() {
            // no shuffle necessary -- can re-use any existing sharding
            let s = if input_shardings.len() == 1 {
                input_shardings.into_iter().next().unwrap()
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
            match graph[node].resolve(need_sharding) {
                None => {
                    // weird operator -- needs an index in its output, which it generates.
                    // we need to have *no* sharding on our inputs!
                    for ni in input_shardings.keys() {
                        reshard(graph, ni, node, Sharding::None);
                        input_shardings.insert(ni, Sharding::None);
                    }
                }
                Some(need_sharding_in) => {
                    for (ni, need_sharding) in need_sharding_in {
                        if have_sharding != Sharding::ByColum(need_sharding) {
                            // input is sharded by different key -- need shuffle
                            reshard(graph, ni, node, need_sharding);
                            input_shardings.insert(ni, Sharding::ByColumn(need_sharding));
                        }
                    }
                    sharding = Sharding::ByColumn(need_sharding);
                }
            }
        }

        // any lookups we do into ancestor views need to be sharded by the lookup key (or not at
        // all) to give correct results. if they're not, we need to shuffle them.
        if input_shardings.values().all(|s| s == Sharding::None) {
            // none of our inputs are sharded, so we are also not sharded
            if sharding == Sharding::Random {
                // TODO: we kind of want to know if any of our children (transitively) *want* us to
                // sharded by a particular key. if they do, we could shard more of the computation,
                // which is probably good for us.
                sharding = Sharding::None
            } else {
                // the if let Some above must have kicked in, and we should keep that sharding
            }
        } else {
            for (ni, cols) in need_sharding {
                if cols.len() != 1 {
                    unimplemented!();
                }
                let need_sharding = cols[0];

                // given input must be unsharded, or sharded by given key
                if let Some(have_sharding) = input_shardings.get(ni) {
                    if have_sharding != Sharding::ByColumn(need_sharding) {
                        // ancestor must be forced to right sharding
                        reshard(graph, ni, node, need_sharding);
                        input_shardings.insert(ni, Sharding::ByColumn(need_sharding));
                    }
                }
            }

            // at least one of our inputs is sharded. who knows what we're sharded by now!
            //
            // TODO:
            // we should do better here. a join should be marked as sharded by the join column.
            // the way to detect this is probably to check if there is *some* column in the
            // output of this node that resolves to the need_sharding column of *all*
            // ancestors? could also add `Node::produces_sharding`?
            sharding = Sharding::Random;
        }

        graph.node_weight_mut(node).unwrap().shard_by(sharding);
    }

    // TODO
    // how do we actually split a sharded node?
    // ideally we want to keep the graph representation unchanged, and instead just *instantiate*
    // multiple copies of the given node. but that probably has very deep-reaching consequences in
    // the code...

    swaps
}

/// Modify the graph such that the path between `src` and `dst` shuffles the input such that the
/// records received by `dst` are sharded by column `col`.
fn reshard(graph: &mut Graph, src: NodeIndex, dst: NodeIndex, col: usize) {
    if graph[src].shard_by() == Sharding::None {
        // src isn't sharded, so conforms to all shardings
        return;
    }

    unimplemented!();
}
