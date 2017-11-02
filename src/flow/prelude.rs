use petgraph;
use std::cell;

// core types
pub use core::*;
pub use flow::processing::{Ingredient, Miss, ProcessingResult, RawProcessingResult, ReplayContext};
pub use ops::NodeOperator;

// graph types
pub use flow::node::Node;
pub use flow::Edge;
pub type Graph = petgraph::Graph<Node, Edge>;

// dataflow types
pub use flow::payload::{Link, Packet, PacketEvent, ReplayPathSegment, Tracer, TransactionState};
pub use flow::migrate::sharding::Sharding;

// domain local state
pub type DomainNodes = ::core::local::Map<cell::RefCell<Node>>;

// channel related types
use channel;
use flow::domain;
/// Channel coordinator type specialized for domains
pub type ChannelCoordinator = channel::ChannelCoordinator<(domain::Index, usize)>;

// distributed operation types
use std::net;
use std::sync::{Arc, Mutex};
use flow::coordination::CoordinationMessage;
pub type WorkerIdentifier = net::SocketAddr;
pub type WorkerEndpoint = Arc<Mutex<channel::TcpSender<CoordinationMessage>>>;

// debug types
pub use flow::debug::DebugEvent;
