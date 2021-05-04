//! This module defines crate-local *and* public type exports.
//!
//! It is expected that files within the dataflow crate have use prelude::* at the top, and the
//! same applies to external users of the dataflow crate. Therefore, pay attention to whether `pub`
//! or `crate` is used.
use std::cell;
use std::collections::HashMap;

// core types
pub(crate) use crate::processing::Ingredient;
pub(crate) use crate::processing::{
    Lookup, Miss, ProcessingResult, RawProcessingResult, ReplayContext,
};
pub(crate) type Edge = ();

// dataflow types
pub(crate) use crate::payload::{ReplayPathSegment, SourceChannelIdentifier};
pub(crate) use noria::PacketPayload;

// domain local state
pub(crate) use crate::state::{
    LookupResult, MemoryState, PersistentState, RecordResult, Row, Rows, State,
};
pub(crate) type StateMap = Map<Box<dyn State>>;
pub(crate) type DomainNodes = Map<cell::RefCell<Node>>;
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
pub use crate::DurabilityMode;
pub use crate::PersistenceParameters;
pub use noria::errors::*;
pub use noria::{internal, invariant, invariant_eq, unsupported};

/// Channel coordinator type specialized for domains
pub type ChannelCoordinator = noria::channel::ChannelCoordinator<(DomainIndex, usize), Box<Packet>>;
pub trait Executor {
    fn ack(&mut self, tag: SourceChannelIdentifier);
    fn create_universe(&mut self, req: HashMap<String, DataType>);
    fn send(&mut self, dest: ReplicaAddr, m: Box<Packet>);
}
