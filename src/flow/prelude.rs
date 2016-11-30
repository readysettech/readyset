pub use flow::Ingredient;

use petgraph;
pub use flow::NodeAddress;
pub use flow::LocalNodeIndex;
pub use flow::node::Node;
use flow::Edge;
pub type Graph = petgraph::Graph<Node, Edge>;
pub use flow::Message;
use flow::domain::single;
use std::cell;
use flow::domain::local;
pub type DomainNodes = local::Map<cell::RefCell<single::NodeDescriptor>>;
use query;
pub type StateMap = local::Map<State>;
pub use ops::{Update, Record};
pub type State = local::State<query::DataType>;
