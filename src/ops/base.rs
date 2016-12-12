use std::collections::HashMap;

/// Base is used to represent the root nodes of the distributary data flow graph.
///
/// These nodes perform no computation, and their job is merely to persist all received updates and
/// forward them to interested downstream operators. A base node should only be sent updates of the
/// type corresponding to the node's type.
#[derive(Debug)]
pub struct Base { }

use flow::prelude::*;

impl Ingredient for Base {
    fn ancestors(&self) -> Vec<NodeAddress> {
        vec![]
    }

    fn should_materialize(&self) -> bool {
        true
    }

    fn will_query(&self, _: bool) -> bool {
        false
    }

    fn on_connected(&mut self, _: &Graph) {}
    fn on_commit(&mut self, _: NodeAddress, _: &HashMap<NodeAddress, NodeAddress>) {}
    fn on_input(&mut self, input: Message, _: &DomainNodes, _: &StateMap) -> Option<Update> {
        Some(input.data)
    }

    fn suggest_indexes(&self, _: NodeAddress) -> HashMap<NodeAddress, usize> {
        HashMap::new()
    }

    fn resolve(&self, _: usize) -> Option<Vec<(NodeAddress, usize)>> {
        None
    }

    fn is_base(&self) -> bool {
        true
    }

    fn description(&self) -> String {
        "B".into()
    }
}
