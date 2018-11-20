use petgraph;
use std::cell;
use std::collections::HashMap;

// core types
pub use common::*;
pub use noria::internal::*;
pub use ops::NodeOperator;
pub use petgraph::graph::NodeIndex;
pub use processing::Ingredient;
pub(crate) use processing::{Miss, ProcessingResult, RawProcessingResult, ReplayContext};

// graph types
pub use node::Node;
pub type Edge = ();
pub type Graph = petgraph::Graph<Node, Edge>;

// dataflow types
pub use noria::debug::trace::{Event, PacketEvent, Tracer};
pub use noria::Input;
pub use payload::{Packet, ReplayPathSegment, SourceChannelIdentifier};
pub use Sharding;

// domain local state
pub use state::{LookupResult, MemoryState, PersistentState, RecordResult, Row, State};
pub type StateMap = Map<Box<State>>;
pub type DomainNodes = Map<cell::RefCell<Node>>;
pub type ReplicaAddr = (DomainIndex, usize);

// persistence configuration
pub use DurabilityMode;
pub use PersistenceParameters;

/// Channel coordinator type specialized for domains
pub type ChannelCoordinator = noria::channel::ChannelCoordinator<(DomainIndex, usize), Box<Packet>>;
pub trait Executor {
    fn send_back(&mut self, client: SourceChannelIdentifier, ack: ());
    fn create_universe(&mut self, req: HashMap<String, DataType>);
}
