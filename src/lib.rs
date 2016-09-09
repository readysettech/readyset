#![feature(optin_builtin_traits)]

extern crate clocked_dispatch;
extern crate parking_lot;
extern crate petgraph;
extern crate shortcut;
extern crate rustc_serialize;

#[macro_use]
extern crate rustful;

mod flow;
mod query;
mod ops;
mod backlog;

pub use flow::FlowGraph;
pub use flow::NodeIndex;
pub use ops::new;
pub use ops::NodeType;
pub use ops::Update;
pub use ops::Record;
pub use ops::base::Base;
pub use ops::aggregate::*;
pub use ops::join::Joiner;
pub use ops::union::Union;
pub use ops::latest::Latest;
pub use query::Query;
pub use query::DataType;

pub mod web;
