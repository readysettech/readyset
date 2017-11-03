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
    nodes: &[(NodeIndex, bool)],
    domain: &domain::Index,
    has_path: &HashSet<(NodeIndex, NodeIndex)>,
) -> IngressFromBase {
    let ingress_nodes: Vec<_> = nodes
        .into_iter()
        .map(|&(ni, _)| ni)
        .filter(|&ni| graph[ni].borrow().is_ingress())
        .filter(|&ni| graph[ni].borrow().is_transactional())
        .collect();

    graph
        .neighbors_directed(source, petgraph::EdgeDirection::Outgoing)
        .map(|base| {
            let mut num_paths = ingress_nodes
                .iter()
                .filter(|&&ingress| {
                    has_path.contains(&(base, ingress))
                })
                .map(|&ingress| if graph[ingress].is_shard_merger() {
                    ::SHARDS
                } else {
                    1
                })
                .sum();

            // Domains containing a base node will get a single copy of each packet sent to it.
            if graph[base].domain() == *domain {
                assert_eq!(num_paths, 0);
                num_paths = 1;
            }

            (base, num_paths)
        })
        .collect()
}

fn base_egress_map(
    graph: &Graph,
    source: NodeIndex,
    nodes: &[(NodeIndex, bool)],
    has_path: &HashSet<(NodeIndex, NodeIndex)>
) -> EgressForBase {
    let output_nodes: Vec<_> = nodes
        .into_iter()
        .map(|&(ni, _)| ni)
        .filter(|&ni| graph[ni].is_output())
        //.filter(|&ni| graph[ni].is_transactional())
        .collect();

    graph
        .neighbors_directed(source, petgraph::EdgeDirection::Outgoing)
        .map(|base| {
            let outs = output_nodes
                .iter()
                .filter(|&&out| {
                    has_path.contains(&(base, out))
                })
                .map(|&out| *graph[out].local_addr())
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
            // we can access `graph` mutably here still
            has_path.insert((b, nx));
        }
    }

    has_path
}

pub fn analyze_graph(
    graph: &Graph,
    source: NodeIndex,
    domain_nodes: HashMap<domain::Index, Vec<(NodeIndex, bool)>>,
) -> HashMap<domain::Index, (IngressFromBase, EgressForBase)> {
    let has_path = has_path(graph, source);
    domain_nodes
        .into_iter()
        .map(|(domain, nodes): (_, Vec<(NodeIndex, bool)>)| {
            (
                domain,
                (
                    count_base_ingress(graph, source, &nodes[..], &domain, &has_path),
                    base_egress_map(graph, source, &nodes[..], &has_path),
                ),
            )
        })
        .collect()
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
