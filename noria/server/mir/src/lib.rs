#![warn(clippy::dbg_macro)]
#![deny(unused_extern_crates)]

#[macro_use]
extern crate slog;

use std::cell::RefCell;
use std::rc::Rc;

use petgraph::graph::NodeIndex;

pub use column::Column;

mod column;
pub mod node;
pub mod query;
pub mod reuse;
mod rewrite;
pub mod visualize;

pub type MirNodeRef = Rc<RefCell<node::MirNode>>;

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
