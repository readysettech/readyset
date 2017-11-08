use flow::domain;
use flow::prelude::*;
use flow::payload::{EgressForBase, IngressFromBase};
use petgraph;
use petgraph::graph::NodeIndex;
use std::borrow::Borrow;
use std::collections::{HashMap, HashSet};
use slog::Logger;

fn count_base_ingress(
    graph: &Graph,
    source: NodeIndex,
    nodes: &[NodeIndex],
    has_path: &HashSet<(NodeIndex, NodeIndex)>,
) -> IngressFromBase {
    let ingress_nodes: Vec<_> = nodes
        .into_iter()
        .filter(|&&ni| graph[ni].borrow().is_ingress())
        .filter(|&&ni| graph[ni].borrow().is_transactional())
        .collect();

    graph
        .neighbors_directed(source, petgraph::EdgeDirection::Outgoing)
        .map(|base| {
            let num_paths = ingress_nodes
                .iter()
                .filter(|&&ingress| {
                    has_path.contains(&(base, *ingress))
                })
                .map(|&&ingress| if graph[ingress].is_shard_merger() {
                    ::SHARDS
                } else {
                    1
                })
                .sum();

            (base, num_paths)
        })
        .collect()
}

fn base_egress_map(
    graph: &Graph,
    source: NodeIndex,
    nodes: &[NodeIndex],
    has_path: &HashSet<(NodeIndex, NodeIndex)>
) -> EgressForBase {
    let output_nodes: Vec<_> = nodes
        .into_iter()
        .filter(|&&ni| graph[ni].is_output())
        //.filter(|&ni| graph[ni].is_transactional())
        .collect();

    graph
        .neighbors_directed(source, petgraph::EdgeDirection::Outgoing)
        .map(|base| {
            let outs = output_nodes
                .iter()
                .filter(|&&out| {
                    has_path.contains(&(base, *out))
                })
                .map(|&&out| *graph[out].local_addr())
                .collect();
            (base, outs)
        })
        .collect()
}

fn has_path(graph: &Graph, source: NodeIndex) -> HashSet<(NodeIndex, NodeIndex)> {
    use petgraph::visit::Bfs;
    let mut has_path = HashSet::new();
    for b in graph.neighbors_directed(source, petgraph::EdgeDirection::Outgoing) {
        let mut bfs = Bfs::new(&graph, b);
        while let Some(nx) = bfs.next(&graph) {
            has_path.insert((b, nx));
        }
    }

    has_path
}

pub fn analyze_graph(
    graph: &Graph,
    source: NodeIndex,
    domain_nodes: HashMap<domain::Index, Vec<NodeIndex>>,
    old: &mut HashMap<domain::Index, (IngressFromBase, EgressForBase)>,
) {
    let has_path = has_path(graph, source);
    let new = domain_nodes
        .into_iter()
        .map(|(domain, nodes): (_, Vec<NodeIndex>)| {
            (
                domain,
                (
                    count_base_ingress(graph, source, &nodes[..], &has_path),
                    base_egress_map(graph, source, &nodes[..], &has_path),
                ),
            )
        })
        .collect();

    merge_deps(graph, old, new);
}

fn merge_deps(
    graph: &Graph,
    old: &mut HashMap<domain::Index, (IngressFromBase, EgressForBase)>,
    new: HashMap<domain::Index, (IngressFromBase, EgressForBase)>
) {
    for (di, (new_ingress, new_egress)) in new {
        let entry = old.entry(di).or_insert((HashMap::new(), HashMap::new()));
        let old_ingress = &mut entry.0;
        let old_egress = &mut entry.1;

        for (base, v) in new_egress {
            let e = old_egress.entry(base).or_insert(vec![]);
            (*e).extend(v);
        }

        for (base, v) in new_ingress {
            let e = old_ingress.entry(base).or_insert(0);
            *e += v;

            // Domains containing a base node will get a single copy of each packet sent to it.
            if graph[base].domain() == di {
                *e = 1;
            }
        }
    }
}

pub fn finalize(
    deps: HashMap<domain::Index, (IngressFromBase, EgressForBase)>,
    log: &Logger,
    txs: &mut HashMap<domain::Index, domain::DomainHandle>,
    at: i64,
) {
    for (domain, (ingress_from_base, egress_for_base)) in deps {
        trace!(log, "notifying domain of migration completion"; "domain" => domain.index());
        let ctx = txs.get_mut(&domain).unwrap();
        let _ = ctx.send(box Packet::CompleteMigration {
            at,
            ingress_from_base,
            egress_for_base,
        });
    }
}
