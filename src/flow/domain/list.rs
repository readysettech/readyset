use std::cell;
use std::collections::HashMap;

use petgraph::graph::NodeIndex;
use flow::domain::single::NodeDescriptor;
use flow::node;

// TODO: get rid of and move HashMap into domain directly
pub struct NodeList {
    nodes: HashMap<NodeIndex, cell::RefCell<NodeDescriptor>>,
}

impl From<Vec<NodeDescriptor>> for NodeList {
    fn from(nodes: Vec<NodeDescriptor>) -> Self {
        NodeList { nodes: nodes.into_iter().map(|n| (n.index, cell::RefCell::new(n))).collect() }
    }
}

impl NodeList {
    pub fn lookup(&self, node: NodeIndex) -> cell::Ref<NodeDescriptor> {
        let nd = self.nodes[&node].borrow();
        match *nd.inner {
            node::Type::Internal(..) => (),
            node::Type::Ingress(..) => {
                println!("attempt at looking up foreign node {:?}", node);
                unimplemented!();
            }
            _ => {
                unreachable!();
            }
        }
        nd
    }

    pub fn lookup_mut(&self, node: NodeIndex) -> cell::RefMut<NodeDescriptor> {
        self.nodes[&node].borrow_mut()
    }
}
