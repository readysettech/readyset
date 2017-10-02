use std::rc::Rc;
use std::cell::RefCell;

pub mod reuse;
pub(crate) mod node;
pub(crate) mod query;
mod rewrite;
pub(crate) mod to_flow;
mod optimize;
pub mod visualize;

pub type MirNodeRef = Rc<RefCell<node::MirNode>>;
