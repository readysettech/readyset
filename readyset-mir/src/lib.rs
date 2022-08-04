//! The **M**id-level **I**ntermediate **R**epresentation
//!
//! This crate contains the data structure definitions for `MIR`, the Mid-level Intermediate
//! Representation for the compiler from SQL to Noria. MIR is structured as a directed acyclic
//! graph, where (similarly to dataflow) nodes in the graph each represent some abstract computation
//! on rows produced by their parents. MIR sits in between the SQL [AST] and the final dataflow
//! graph in the compilation process. When converting from MIR to dataflow, each [node] in the MIR
//! graph is converted to zero or more nodes in the dataflow graph.
//!
//! # Why MIR?
//!
//! As a directed acyclic graph, MIR is structured in a way that is similar to the dataflow graph
//! (the final representation we use for a query), but has several differences in its design that
//! make it better suited for use as an intermediate representation. At a high level, while the
//! structure of the dataflow graph is primarily optimized for the most efficient *execution* of a
//! query, the structure of the MIR graph is primarily optmized for making it easiest for
//! *developers* to deal with the graph. Most notably, while dataflow uses numeric column *indices*
//! throughout to refer to columns in its parents, MIR uses *named* [column references]. This allows
//! us to much more easily perform [rewrite passes] which may add new columns, remove columns, or
//! reorder columns, without ever having to worry about keeping numeric references up-to-date. In
//! addition, where convenient, node types in MIR do not have to correspond one-to-one with node
//! types in dataflow - for example:
//!
//! * The [`TableAlias`] node is skipped entirely when converting to dataflow since its only purpose
//!   is to change table names in column references, something which doesn't exist in dataflow
//! * The [`Distinct`] node, which corresponds in the most obvious way to the SQL `DISTINCT`
//!   keyword, is actually converted to a `Count` node when lowering to dataflow, since due to the
//!   intricacies of partial stateful dataflow the best way to implement distinct is to calculate a
//!   count grouped by all columns followed by omitting the actual count value in the result set
//! * The [`DependentJoin`] node will throw an *error* if encountered when converting to dataflow -
//!   this node type is created as part of compiling correlated subqueries, and the expectation is
//!   that it will be removed entirely as part of a [rewrite pass][decorrelate]
//!
//! [AST]: nom_sql
//! [node]: crate::node::MirNode
//! [column references]: crate::Column
//! [rewrite passes]: crate::rewrite
//! [`TableAlias`]: crate::node::MirNodeInner::TableAlias
//! [`Distinct`]: crate::node::MirNodeInner::Distinct
//! [decorrelate]: crate::rewrite::decorrelate::eliminate_dependent_joins

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
