#![warn(clippy::dbg_macro)]
#![warn(clippy::panic)]
#![deny(unused_extern_crates, macro_use_extern_crate)]
#![feature(stmt_expr_attributes)]

use std::cell::RefCell;
use std::rc::{Rc, Weak};

use petgraph::graph::NodeIndex;

pub use column::Column;

mod column;
pub mod node;
pub mod query;
pub mod reuse;
mod rewrite;
pub mod visualize;

pub type MirNodeRef = Rc<RefCell<node::MirNode>>;
pub type MirNodeWeakRef = Weak<RefCell<node::MirNode>>;

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
