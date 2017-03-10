use flow::domain;
use flow::prelude::*;

use petgraph;
use petgraph::graph::NodeIndex;

use std::borrow::Borrow;
use std::collections::HashMap;
use std::sync::mpsc;

use slog::Logger;

fn count_base_ingress(graph: &Graph,
                      source: NodeIndex,
                      nodes: &[(NodeIndex, bool)])
                      -> HashMap<NodeIndex, usize> {

    let ingress_nodes: Vec<_> = nodes.into_iter()
        .map(|&(ni, _)| ni)
        .filter(|&ni| graph[ni].borrow().is_ingress())
        .collect();

    graph.neighbors_directed(source, petgraph::EdgeDirection::Outgoing)
        .map(|ingress| {
            graph.neighbors_directed(ingress, petgraph::EdgeDirection::Outgoing)
                .next()
                .expect("source ingress must have a base child")
        })
        .map(|base| {
            let num_paths = ingress_nodes.iter()
                .filter(|&&ingress| petgraph::algo::has_path_connecting(graph, base, ingress, None))
                .count();
            (base, num_paths)
        })
        .collect()
}

pub fn analyze_graph(graph: &Graph,
                     source: NodeIndex,
                     domain_nodes: HashMap<domain::Index, Vec<(NodeIndex, bool)>>)
                     -> (HashMap<domain::Index, HashMap<petgraph::graph::NodeIndex, usize>>,
                         HashMap<domain::Index, Vec<petgraph::graph::NodeIndex>>) {
    let ingresses_from_base: HashMap<_, _> = domain_nodes.into_iter()
        .map(|(domain, nodes): (_, Vec<(NodeIndex, bool)>)| {
            (domain, count_base_ingress(graph, source, &nodes[..]))
        })
        .collect();

    let domain_dependencies = ingresses_from_base.iter()
        .map(|(domain, ingress_from_base)| {
            (*domain,
             ingress_from_base.iter()
                .filter_map(|(k, n)| { if *n > 0 { Some(*k) } else { None } })
                .collect())
        })
        .collect();

    (ingresses_from_base, domain_dependencies)
}

pub fn finalize(ingresses_from_base: HashMap<domain::Index,
                                             HashMap<petgraph::graph::NodeIndex, usize>>,
                log: &Logger,
                txs: &mut HashMap<domain::Index, mpsc::SyncSender<Packet>>,
                at: i64) {
    for (domain, ingress_from_base) in ingresses_from_base {
        trace!(log, "notifying domain of migration completion"; "domain" => domain.index());
        let ctx = txs.get_mut(&domain).unwrap();
        let _ = ctx.send(Packet::CompleteMigration {
            at: at,
            ingress_from_base: ingress_from_base,
        });
    }
}
