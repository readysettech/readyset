use petgraph;
use std::cell;

// core types
use flow::core;
pub use flow::core::{LocalNodeIndex, IndexPair};
pub use petgraph::graph::NodeIndex;
pub use flow::core::{DataType, Record, Records, Datas};
pub use flow::core::{Ingredient, ProcessingResult, Miss, RawProcessingResult};
pub use ops::NodeOperator;

// graph types
pub use flow::node::Node;
pub use flow::Edge;
pub type Graph = petgraph::Graph<Node, Edge>;

// dataflow types
pub use flow::payload::{Link, Packet, PacketEvent, Tracer, TransactionState};
pub use flow::migrate::materialization::Tag;
pub use flow::migrate::sharding::Sharding;

// domain local state
use flow::domain::local;
pub use flow::domain::local::Map;
pub type DomainNodes = local::Map<cell::RefCell<Node>>;
pub type State = local::State<core::DataType>;
pub type StateMap = local::Map<State>;
pub use flow::domain::local::KeyType;
pub use flow::domain::local::LookupResult;

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
