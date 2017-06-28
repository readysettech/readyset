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
pub use flow::payload::{Link, Packet, PacketEvent, Tracer, TriggerEndpoint, TransactionState};
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

use channel;
use flow::domain;
pub type ChannelCoordinator = channel::ChannelCoordinator<(domain::Index, usize), Box<Packet>>;
