use flow::prelude::*;

use petgraph;
use petgraph::graph::NodeIndex;

use std::collections::HashMap;

pub fn provenance_of<F>(graph: &Graph,
                        node: NodeIndex,
                        column: usize,
                        mut on_join: F)
                        -> Vec<Vec<(NodeIndex, Option<usize>)>>
    where F: FnMut(NodeIndex, &[NodeIndex]) -> Option<NodeIndex>
{
    let path = vec![(node, Some(column))];
    trace(graph, &mut on_join, path)
}

fn trace<F>(graph: &Graph,
            on_join: &mut F,
            mut path: Vec<(NodeIndex, Option<usize>)>)
            -> Vec<Vec<(NodeIndex, Option<usize>)>>
    where F: FnMut(NodeIndex, &[NodeIndex]) -> Option<NodeIndex>
{
    // figure out what node/column we're looking up
    let (node, column) = path.last().cloned().unwrap();

    let parents: Vec<_> = graph.neighbors_directed(node, petgraph::EdgeDirection::Incoming)
        .collect();

    if parents.is_empty() {
        // this path reached the source node.
        // but we should have stopped at base nodes above...
        unreachable!();
    }

    let n = &graph[node];
    let domain = n.domain();
    let local_to_global: HashMap<_, _> = parents.iter()
        .filter_map(|&ni| {
            let n = &graph[ni];
            if n.domain() == domain {
                Some((n.addr(), ni))
            } else {
                None
            }
        })
        .collect();

    // have we reached a base node?
    if n.is_internal() && n.is_base() {
        return vec![path];
    }

    // if the column isn't known, our job is trivial -- just map to all ancestors
    if column.is_none() {
        // except if we're a join and on_join says to only walk through one...
        if n.is_internal() && n.is_join() {
            if let Some(parent) = on_join(node, &parents[..]) {
                path.push((parent, None));
                return trace(graph, on_join, path);
            }
        }

        let mut paths = Vec::with_capacity(parents.len());
        for p in parents {
            let mut path = path.clone();
            path.push((p, None));
            paths.extend(trace(graph, on_join, path));
        }
        return paths;
    }
    let column = column.unwrap();

    // we know all non-internal nodes use an identity mapping
    if !n.is_internal() {
        let parent = parents.into_iter().next().unwrap();
        path.push((parent, Some(column)));
        return trace(graph, on_join, path);
    }

    // try to resolve the currently selected column
    let resolved = n.parent_columns(column);
    assert!(!resolved.is_empty());

    // is it a generated column?
    let local = n.addr();
    if resolved.len() == 1 && resolved[0].0 == local {
        assert!(resolved[0].1.is_none()); // how could this be Some?
        // path terminates here, and has no connection to ancestors
        // so, we depend on *all* our *full* parents
        let mut paths = Vec::with_capacity(parents.len());
        for p in parents {
            let mut path = path.clone();
            path.push((p, None));
            paths.extend(trace(graph, on_join, path));
        }
        return paths;
    }

    // no, it resolves to at least one parent column
    // if there is only one parent, we can step right to that
    if parents.len() == 1 {
        let parent = parents.into_iter().next().unwrap();
        let resolved = resolved.into_iter().next().unwrap();
        assert_eq!(resolved.0, graph[parent].addr());
        // since resolved.0 != local, we know the column isn't generated
        assert!(resolved.1.is_some());
        let col = resolved.1.unwrap();
        path.push((parent, Some(col)));
        return trace(graph, on_join, path);
    }

    // there are multiple parents.
    // this means we are either a union or a join.
    // let's deal with the union case first.
    // in unions, all keys resolve to more than one parent.
    if !n.is_join() {
        // all columns come from all parents
        assert_eq!(parents.len(), resolved.len());
        // no columns are generated
        assert!(resolved.iter().all(|r| r.1.is_some()));
        // traverse up all the paths
        let mut paths = Vec::with_capacity(parents.len());
        for (parent, column) in resolved {
            let column = column.unwrap();
            let mut path = path.clone();
            // we know that the parent is in the same domain for unions, so [] is ok
            path.push((local_to_global[&parent], Some(column)));
            paths.extend(trace(graph, on_join, path));
        }
        return paths;
    }

    // TODO: JOIN should emit multi-parent resolve for join key that is emitted
    let (specific_parent, column) = resolved.into_iter().next().unwrap();
    // we know join parents are all in the same domain as the join.
    let specific_parent = local_to_global[&specific_parent];
    // we know joins don't generate values.
    let column = column.unwrap();

    // okay, so this is a join. it's up to the on_join function to tell us whether to walk up *all*
    // the parents of the join, or just one of them. let's ask.
    // TODO: provide an early-termination mechanism?
    match on_join(node, &parents[..]) {
        None => {
            // our caller wants information about all our parents.
            // since the column we're chasing only follows a single path through a join (unless it
            // is a join key, which we don't yet handle), we need to produce (_, None) for all the
            // others.
            let mut paths = Vec::with_capacity(parents.len());
            for parent in parents {
                let mut path = path.clone();
                if parent == specific_parent {
                    path.push((parent, Some(column)));
                    paths.extend(trace(graph, on_join, path));
                } else {
                    path.push((parent, None));
                    paths.extend(trace(graph, on_join, path));
                }
            }
            paths
        }
        Some(parent) => {
            // our caller only cares about *one* parent.
            // hopefully we can give key information about that parent
            if parent == specific_parent {
                // \o/
                path.push((parent, Some(column)));
                trace(graph, on_join, path)
            } else {
                // nope. we can't resolve any more for this column.
                path.push((parent, None));
                trace(graph, on_join, path)
            }
        }
    }
}
