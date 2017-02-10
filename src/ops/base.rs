use std::collections::HashMap;

/// Base is used to represent the root nodes of the distributary data flow graph.
///
/// These nodes perform no computation, and their job is merely to persist all received updates and
/// forward them to interested downstream operators. A base node should only be sent updates of the
/// type corresponding to the node's type.
#[derive(Debug, Clone)]
pub struct Base {
    key_column: Option<usize>,
    us: Option<NodeAddress>,
}

impl Base {
    /// Create a base node operator.
    pub fn new(key_column: usize) -> Self {
        Base {
            key_column: Some(key_column),
            us: None,
        }
    }
}

impl Default for Base {
    fn default() -> Self {
        Base {
            key_column: None,
            us: None,
        }
    }
}

use flow::prelude::*;

impl Ingredient for Base {
    fn take(&mut self) -> Box<Ingredient> {
        Box::new(Clone::clone(self))
    }

    fn ancestors(&self) -> Vec<NodeAddress> {
        vec![]
    }

    fn should_materialize(&self) -> bool {
        true
    }

    fn will_query(&self, materialized: bool) -> bool {
        !materialized && self.key_column.is_some()
    }

    fn on_connected(&mut self, _: &Graph) {}
    fn on_commit(&mut self, us: NodeAddress, _: &HashMap<NodeAddress, NodeAddress>) {
        self.us = Some(us);
    }
    fn on_input(&mut self, _: NodeAddress, rs: Records, _: &DomainNodes, state: &StateMap) -> Records {
        rs.into_iter().map(|r| match r {
            Record::Positive(u) => Record::Positive(u),
            Record::Negative(u) => Record::Negative(u),
            Record::DeleteRequest(key) => {
                let db = state.get(self.us.as_ref().unwrap().as_local())
                    .expect("base must have its own state materialized to support deletions");
                let rows = db.lookup(self.key_column.unwrap(), &key);
                assert_eq!(rows.len(), 1);

                Record::Negative(rows[0].clone())
            }
        }).collect()
    }

    fn suggest_indexes(&self, n: NodeAddress) -> HashMap<NodeAddress, usize> {
        if self.key_column.is_some() {
            Some((n, self.key_column.unwrap())).into_iter().collect()
        } else {
            HashMap::new()
        }
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

    fn parent_columns(&self, _: usize) -> Vec<(NodeAddress, Option<usize>)> {
        unreachable!();
    }
}
