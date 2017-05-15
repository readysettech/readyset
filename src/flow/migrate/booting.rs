//! Functions for starting up a *new* domain.
//!
//! This includes constructing local identifiers for nodes, construcing domain-local structures
//! such as `DomainNodes`, and initializing transaction handling.

use flow::prelude::*;
use flow::domain;
use flow::checktable;

use petgraph::graph::NodeIndex;

use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::cell;
use std::thread;

use slog::Logger;

fn build_descriptors(graph: &mut Graph, nodes: Vec<(NodeIndex, bool)>) -> DomainNodes {
    nodes
        .into_iter()
        .map(|(ni, _)| graph.node_weight_mut(ni).unwrap().take())
        .map(|nd| (*nd.local_addr().as_local(), cell::RefCell::new(nd)))
        .collect()
}

pub fn boot_new(log: Logger,
                index: domain::Index,
                graph: &mut Graph,
                nodes: Vec<(NodeIndex, bool)>,
                checktable: Arc<Mutex<checktable::CheckTable>>,
                rx: mpsc::Receiver<Box<Packet>>,
                input_rx: mpsc::Receiver<Box<Packet>>,
                ts: i64)
                -> thread::JoinHandle<()> {
    let nodes = build_descriptors(graph, nodes);
    let domain = domain::Domain::new(log, index, nodes, checktable, ts);
    domain.boot(rx, input_rx)
}
