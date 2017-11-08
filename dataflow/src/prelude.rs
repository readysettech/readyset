use petgraph;
use std::cell;

// core types
pub use core::*;
pub use processing::{Ingredient, Miss, ProcessingResult, RawProcessingResult, ReplayContext};
pub use ops::NodeOperator;

// graph types
pub use node::Node;
pub type Edge = ();
pub type Graph = petgraph::Graph<Node, Edge>;

// dataflow types
pub use payload::{Link, Packet, PacketEvent, ReplayPathSegment, Tracer, TransactionState};
pub use Sharding;

// domain local state
pub type DomainNodes = ::core::local::Map<cell::RefCell<Node>>;
pub type DomainIndex = domain::Index;

// channel related types
use channel;
use domain;
/// Channel coordinator type specialized for domains
pub type ChannelCoordinator = channel::ChannelCoordinator<(DomainIndex, usize)>;

// debug types
pub use debug::DebugEvent;
