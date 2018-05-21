use petgraph;
use std::cell;

// core types
pub use basics::*;
pub use ops::NodeOperator;
pub use processing::Ingredient;
pub(crate) use processing::{Miss, ProcessingResult, RawProcessingResult, ReplayContext};

// graph types
pub use node::Node;
pub type Edge = ();
pub type Graph = petgraph::Graph<Node, Edge>;

// dataflow types
pub use payload::{Packet, PacketEvent, ReplayPathSegment, SourceChannelIdentifier, Tracer};
pub use Sharding;

// domain local state
pub type DomainNodes = Map<cell::RefCell<Node>>;
pub type ReplicaAddr = (DomainIndex, usize);

// channel related types
use channel;
/// Channel coordinator type specialized for domains
pub type ChannelCoordinator = channel::ChannelCoordinator<(DomainIndex, usize)>;
pub trait Executor {
    fn send_back(&self, SourceChannelIdentifier, Result<i64, ()>);
}

// debug types
pub use debug::DebugEvent;
