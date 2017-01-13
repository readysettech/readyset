//! Functions for starting up a *new* domain.
//!
//! This includes constructing local identifiers for nodes, construcing domain-local structures
//! such as `DomainNodes`, and initializing transaction handling.

use flow::prelude::*;
use flow::domain::single;
use flow::domain;
use flow::checktable;

use petgraph;
use petgraph::graph::NodeIndex;

use std::collections::HashMap;
use std::borrow::Borrow;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::cell;

fn build_descriptors(domain: domain::Index,
                     graph: &mut Graph,
                     nodes: Vec<(NodeIndex, bool)>)
                     -> DomainNodes {
    nodes.into_iter()
            .map(|(ni, _)| {
                // also include all *internal* descendants
                let children: Vec<_> = graph
                    .neighbors_directed(ni, petgraph::EdgeDirection::Outgoing)
                    .filter(|&c| {
                        graph[c].domain() == domain
                    })
                    .map(|ni| graph[ni].addr())
                    .collect();
                    (ni, children)
            })
            .collect::<Vec<_>>() // because above closure mutably borrows self.mainline
            .into_iter()
            .map(|(ni, children)| {
                single::NodeDescriptor {
                    index: ni,
                    inner: graph.node_weight_mut(ni).unwrap().take(),
                    children: children,
                }
            })
            .map(|nd| {
                (*nd.addr().as_local(), cell::RefCell::new(nd))
            })
            .collect()
}

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
            let num_paths =
                ingress_nodes.iter()
                    .filter(|&&ingress| {
                        petgraph::algo::has_path_connecting(graph, base, ingress, None)
                    })
                    .count();
            (base, num_paths)
        })
        .collect()
}

pub fn boot_new(domain: domain::Index,
                graph: &mut Graph,
                source: NodeIndex,
                nodes: Vec<(NodeIndex, bool)>,
                checktable: Arc<Mutex<checktable::CheckTable>>,
                rx: mpsc::Receiver<Message>,
                timestamp_rx: mpsc::Receiver<i64>)
                -> mpsc::SyncSender<domain::Control> {

    let ingress_from_base = count_base_ingress(graph, source, &nodes[..]);
    let nodes = build_descriptors(domain, graph, nodes);

    let domain = domain::Domain::new(nodes, ingress_from_base, checktable);
    domain.boot(rx, timestamp_rx)
}
