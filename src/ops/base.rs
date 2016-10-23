use ops;
use flow;
use query;
use backlog;
use ops::NodeOp;
use ops::NodeType;

use std::collections::HashMap;

/// Base is used to represent the root nodes of the distributary data flow graph.
///
/// These nodes perform no computation, and their job is merely to persist all received updates and
/// forward them to interested downstream operators. A base node should only be sent updates of the
/// type corresponding to the node's type.
#[derive(Debug)]
pub struct Base {}

impl From<Base> for NodeType {
    fn from(b: Base) -> NodeType {
        NodeType::Base(b)
    }
}

impl NodeOp for Base {
    fn prime(&mut self, _: &ops::Graph) -> Vec<flow::NodeIndex> {
        vec![]
    }

    fn forward(&self,
               mut u: ops::Update,
               _: flow::NodeIndex,
               ts: i64,
               _: Option<&backlog::BufferedStore>)
               -> Option<ops::Update> {

        // basically our only job is to record timestamps
        match u {
            ops::Update::Records(ref mut rs) => {
                for r in rs.iter_mut() {
                    match *r {
                        ops::Record::Positive(_, ref mut rts) |
                        ops::Record::Negative(_, ref mut rts) => *rts = ts,
                    }
                }
            }
        }
        Some(u)
    }

    fn query(&self, _: Option<&query::Query>, _: i64) -> ops::Datas {
        unreachable!("base nodes are always materialized");
    }

    fn suggest_indexes(&self, _: flow::NodeIndex) -> HashMap<flow::NodeIndex, Vec<usize>> {
        HashMap::new()
    }

    fn resolve(&self, _: usize) -> Option<Vec<(flow::NodeIndex, usize)>> {
        // base tables are always materialized
        unreachable!();
    }

    fn is_base(&self) -> bool {
        true
    }
}
