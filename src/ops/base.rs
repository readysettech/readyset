use ops;
use query;

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
    fn ancestors(&self) -> Vec<NodeIndex> {
        vec![]
    }

    fn should_materialize(&self) -> bool {
        true
    }

    fn will_query(&self, _: bool) -> bool {
        false
    }

    fn on_connected(&mut self, _: &Graph) {}
    fn on_commit(&mut self, _: NodeIndex, _: &HashMap<NodeIndex, NodeIndex>) {}
    fn on_input(&mut self, input: Message, _: &DomainNodes, _: &StateMap) -> Option<Update> {
        Some(input.data)
    }

    fn query(&self, _: Option<&query::Query>, _: &DomainNodes, _: &StateMap) -> ops::Datas {
        unreachable!("base nodes should never be queried directly");
    }

    fn suggest_indexes(&self, _: NodeIndex) -> HashMap<NodeIndex, Vec<usize>> {
        HashMap::new()
    }

    fn resolve(&self, _: usize) -> Option<Vec<(NodeIndex, usize)>> {
        None
    }

    fn description(&self) -> String {
        "B".into()
    }
}
