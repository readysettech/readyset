//! Functions for assigning new nodes to thread domains.
//!
//! This includes tracking all new nodes assigned to each domain, and assigning them domain-local
//! identifiers.

use flow::prelude::*;

pub fn divvy_up_graph(&mut graph: Graph, source: NodeIndex, new: Vec<NodeIndex>) {
    for node in new {
        let domain = self.added[&node].unwrap_or_else(|| {
            // new node that doesn't belong to a domain
            // create a new domain just for that node
            self.add_domain()
        });

    }
}

pub fn assign_local(&mut domain_nodes: HashMap<domain::Index, Vec<(NodeIndex, NodeAddress)>>,
                    domain: domain::Index,
                    node: NodeIndex)
                    -> NodeAddress {
    let no = NodeAddress::make_local(domain_nodes.entry(domain)
        .or_insert_with(Vec::new)
        .len());
    domain_nodes.get_mut(&domain).unwrap().push((index, no));
    no
}
