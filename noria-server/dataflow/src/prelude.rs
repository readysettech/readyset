/// This module defines crate-local *and* public type exports.
///
/// It is expected that files within the dataflow crate have use prelude::* at the top, and the
/// same applies to external users of the dataflow crate. Therefore, pay attention to whether `pub`
/// or `crate` is used.
use petgraph;
use std::cell;
use std::collections::HashMap;

// core types
crate use processing::Ingredient;
crate use processing::{Miss, ProcessingResult, RawProcessingResult, ReplayContext};
crate type Edge = ();

// dataflow types
crate use noria::debug::trace::{PacketEvent, Tracer};
crate use noria::Input;
crate use payload::{ReplayPathSegment, SourceChannelIdentifier};

// domain local state
crate use state::{LookupResult, MemoryState, PersistentState, RecordResult, Row, State};
crate type StateMap = Map<Box<State>>;
crate type DomainNodes = Map<cell::RefCell<Node>>;
crate type ReplicaAddr = (DomainIndex, usize);

use fnv::FnvHashMap;
use std::collections::VecDeque;
crate type EnqueuedSends = FnvHashMap<ReplicaAddr, VecDeque<Box<Packet>>>;

// public exports
pub use common::*;
pub use node::Node;
pub use noria::internal::*;
pub use ops::NodeOperator;
pub use payload::Packet;
pub use petgraph::graph::NodeIndex;
pub use Sharding;
pub type Graph = petgraph::Graph<Node, Edge>;
pub use DurabilityMode;
pub use PersistenceParameters;

/// Channel coordinator type specialized for domains
pub type ChannelCoordinator = noria::channel::ChannelCoordinator<(DomainIndex, usize), Box<Packet>>;
pub trait Executor {
    fn ack(&mut self, tag: SourceChannelIdentifier);
    fn create_universe(&mut self, req: HashMap<String, DataType>);
}
