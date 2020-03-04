#![deny(unused_extern_crates)]

#[macro_use]
extern crate slog;

use petgraph::graph::NodeIndex;
use std::cell::RefCell;
use std::rc::Rc;

mod column;
pub mod node;
mod optimize;
pub mod query;
pub mod reuse;
mod rewrite;
pub mod visualize;

pub type MirNodeRef = Rc<RefCell<node::MirNode>>;

pub use column::Column;

#[derive(Clone, Debug)]
pub enum FlowNode {
    New(NodeIndex),
    Existing(NodeIndex),
}
impl FlowNode {
    pub fn address(&self) -> NodeIndex {
        match *self {
            FlowNode::New(na) | FlowNode::Existing(na) => na,
        }
    }
}
