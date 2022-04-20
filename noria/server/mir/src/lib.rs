#![warn(clippy::panic)]
#![deny(unused_extern_crates, macro_use_extern_crate)]
#![feature(stmt_expr_attributes, box_patterns)]

use std::cell::RefCell;
use std::rc::{Rc, Weak};

pub use column::Column;
use lazy_static::lazy_static;
use nom_sql::SqlIdentifier;
use petgraph::graph::NodeIndex;
use serde::{Deserialize, Serialize};

mod column;
pub mod node;
pub mod query;
pub mod reuse;
mod rewrite;
pub mod visualize;

pub type MirNodeRef = Rc<RefCell<node::MirNode>>;
pub type MirNodeWeakRef = Weak<RefCell<node::MirNode>>;

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
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

lazy_static! {
    /// The column used by the [`Paginate`] node for its page number
    ///
    /// [`Paginate`]: node::node_inner::MirNodeInner::Paginate
    pub static ref PAGE_NUMBER_COL: SqlIdentifier = "__page_number".into();
}
