#![deny(unused_extern_crates)]

extern crate basics;
extern crate dataflow;
extern crate nom_sql;
extern crate regex;
#[macro_use]
extern crate slog;

use std::rc::Rc;
use std::cell::RefCell;

use basics::*;

pub mod reuse;
pub mod node;
pub mod query;
mod rewrite;
mod optimize;
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
