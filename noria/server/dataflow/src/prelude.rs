//! This module defines crate-local *and* public type exports.
//!
//! It is expected that files within the dataflow crate have use prelude::* at the top, and the
//! same applies to external users of the dataflow crate. Therefore, pay attention to whether `pub`
//! or `crate` is used.

use std::cell;

// core types
pub(crate) use crate::processing::Ingredient;
pub(crate) use crate::processing::{
    Lookup, Miss, ProcessingResult, RawProcessingResult, ReplayContext,
};
pub(crate) type Edge = ();

// dataflow types
pub(crate) use crate::payload::ReplayPathSegment;
pub(crate) use noria::PacketPayload;

// domain local state
pub use crate::node_map::NodeMap;
pub(crate) use crate::state::{
    LookupResult, MemoryState, PersistentState, RecordResult, Row, Rows, State,
};
pub(crate) type StateMap = NodeMap<Box<dyn State>>;
pub type DomainNodes = NodeMap<cell::RefCell<Node>>;
pub(crate) type ReplicaAddr = (DomainIndex, usize);

// public exports
pub use crate::node::Node;
pub use crate::ops::NodeOperator;
pub use crate::payload::Packet;
pub use crate::Sharding;
pub use common::*;
pub use noria::internal::*;
pub use petgraph::graph::NodeIndex;
pub type Graph = petgraph::Graph<Node, Edge>;
pub use crate::processing::{ColumnRef, ColumnSource};
pub use crate::DurabilityMode;
pub use crate::PersistenceParameters;
pub use noria_errors::*;
pub use vec1::vec1;

/// Channel coordinator type specialized for domains
pub type ChannelCoordinator = noria::channel::ChannelCoordinator<(DomainIndex, usize), Box<Packet>>;
pub trait Executor {
    fn send(&mut self, dest: ReplicaAddr, m: Box<Packet>);
}
