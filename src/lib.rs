#![feature(optin_builtin_traits)]

extern crate clocked_dispatch;
extern crate petgraph;
extern crate shortcut;
extern crate bus;

// NOTE: these shouldn't actually be pub -- just useful to silence some warnings for now
mod flow;
mod query;
mod ops;
mod backlog;

pub use flow::FlowGraph;
pub use ops::new;
pub use ops::Update;
pub use ops::Record;
pub use ops::base::Base;
pub use ops::aggregate::*;
pub use ops::join::Joiner;
pub use ops::union::Union;
pub use query::Query;
pub use query::DataType;
