use std::cell;
use std::slice;
use std::collections::HashMap;

use petgraph::graph::NodeIndex;
use flow::alt::domain::single::NodeDescriptor;

pub struct NodeList {
    nodes: Vec<cell::RefCell<NodeDescriptor>>,
    map: HashMap<NodeIndex, usize>,
}

impl From<Vec<NodeDescriptor>> for NodeList {
    fn from(nodes: Vec<NodeDescriptor>) -> Self {
        // okay, this deserves some explanation...
        //
        // we're going to be iterating over nodes one at a time, processing updates, each time
        // having a mutable borrow of the *current* node. however, in order to support queries
        // on non-materialized nodes, we also want that node to be able to access *other* nodes
        // using read-only borrows. since they are all in the same vector, this is tricky to do
        // simply using the borrow checker.
        //
        // instead, we make a RefCell of each node to track the borrows at runtime. when we hit
        // a given node, we can lend it the RefCell of all nodes in this domain, and it can
        // freely take out borrows on all nodes (except itself, which will be checked at
        // runtime).
        //
        // as if that wasn't enough, we also want an efficient way for a node to look up its
        // ancestors by node index if it needs to do so. unfortunately, nodes are stored in a
        // Vec (and need to be, since we want to maintain the topological order). we therefore
        // construct a map from node index to each node's index in the Vec, which nodes can use
        // to quickly find ancestor RefCells. this mapping can even be inspected at set-up
        // time, and all NodeIndexes translated to Vec indices instead, to remove the
        // performance penalty of the map.
        //
        // it's unfortunate that we have to resort to refcounting to solve this, as it means
        // every query is a bit more expensive. luckily, since we construct the cells inside
        let map = nodes.iter().enumerate().map(|(i, n)| (n.index, i)).collect();
        let nodes = nodes.into_iter().map(|n| cell::RefCell::new(n)).collect();

        NodeList {
            map: map,
            nodes: nodes,
        }
    }
}

impl NodeList {
    pub fn lookup(&self, node: NodeIndex) -> cell::Ref<NodeDescriptor> {
        self.nodes[self.translate(node)].borrow()
    }

    pub fn index(&self, index: usize) -> cell::Ref<NodeDescriptor> {
        self.nodes[index].borrow()
    }

    pub fn translate(&self, node: NodeIndex) -> usize {
        self.map[&node]
    }
}

impl<'a> IntoIterator for &'a NodeList {
    type Item = &'a cell::RefCell<NodeDescriptor>;
    type IntoIter = slice::Iter<'a, cell::RefCell<NodeDescriptor>>;

    fn into_iter(self) -> Self::IntoIter {
        self.nodes.iter()
    }
}
