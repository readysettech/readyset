use ops;
use flow;
use query;
use backlog;
use ops::NodeOp;
use ops::NodeType;

use std::collections::HashMap;

#[derive(Debug)]
pub struct Base {}

impl From<Base> for NodeType {
    fn from(b: Base) -> NodeType {
        NodeType::BaseNode(b)
    }
}

impl NodeOp for Base {
    fn forward(&self,
               mut u: ops::Update,
               _: flow::NodeIndex,
               ts: i64,
               _: Option<&backlog::BufferedStore>,
               _: &ops::AQ)
               -> Option<ops::Update> {

        // basically our only job is to record timestamps
        match u {
            ops::Update::Records(ref mut rs) => {
                for r in rs.iter_mut() {
                    match *r {
                        ops::Record::Positive(_, ref mut rts) => *rts = ts,
                        ops::Record::Negative(_, ref mut rts) => *rts = ts,
                    }
                }
            }
        }
        Some(u)
    }

    fn query(&self, _: Option<&query::Query>, _: i64, _: &ops::AQ) -> ops::Datas {
        unreachable!("base nodes are always materialized");
    }

    fn suggest_indexes(&self, _: flow::NodeIndex) -> HashMap<flow::NodeIndex, Vec<usize>> {
        HashMap::new()
    }

    fn resolve(&self, _: usize) -> Vec<(flow::NodeIndex, usize)> {
        // base tables are always materialized
        unreachable!();
    }
}
