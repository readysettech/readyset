use std::collections::HashMap;

pub use flow::Ingredient;

use petgraph;
pub use flow::NodeAddress;
pub use flow::node::Node;
use flow::Edge;
pub type Graph = petgraph::Graph<Node, Edge>;
pub use flow::Message;
use flow::domain::single;
use std::cell;
pub type DomainNodes = HashMap<NodeAddress, cell::RefCell<single::NodeDescriptor>>;
use shortcut;
use query;
pub type State = shortcut::Store<query::DataType>;
pub type StateMap = HashMap<NodeAddress, State>;
pub use ops::{Update, Record};
