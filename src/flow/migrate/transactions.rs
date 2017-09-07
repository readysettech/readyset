use flow::domain;
use flow::prelude::*;
use flow::payload::{EgressForBase, IngressFromBase};
use petgraph;
use petgraph::graph::NodeIndex;
use std::borrow::Borrow;
use std::collections::HashMap;
use slog::Logger;

fn count_base_ingress(
    graph: &Graph,
    source: NodeIndex,
    nodes: &[(NodeIndex, bool)],
    domain: &domain::Index,
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
                    petgraph::algo::has_path_connecting(graph, base, ingress, None)
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

fn base_egress_map(graph: &Graph, source: NodeIndex, nodes: &[(NodeIndex, bool)]) -> EgressForBase {
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
                    petgraph::algo::has_path_connecting(graph, base, out, None)
                })
                .map(|&out| *graph[out].local_addr())
                .collect();
            (base, outs)
        })
        .collect()
}

pub fn analyze_graph(
    graph: &Graph,
    source: NodeIndex,
    domain_nodes: HashMap<domain::Index, Vec<(NodeIndex, bool)>>,
) -> HashMap<domain::Index, (IngressFromBase, EgressForBase)> {
    domain_nodes
        .into_iter()
        .map(|(domain, nodes): (_, Vec<(NodeIndex, bool)>)| {
            (
                domain,
                (
                    count_base_ingress(graph, source, &nodes[..], &domain),
                    base_egress_map(graph, source, &nodes[..]),
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
