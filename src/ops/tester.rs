use ops;
use flow;
use query;
use backlog;
use ops::NodeOp;
use ops::NodeType;

use std::collections::HashMap;

#[derive(Debug)]
pub struct Tester(pub i64);

impl From<Tester> for NodeType {
    fn from(b: Tester) -> NodeType {
        NodeType::TestNode(b)
    }
}

impl NodeOp for Tester {
    fn forward(&self,
               u: ops::Update,
               _: flow::NodeIndex,
               _: i64,
               _: Option<&backlog::BufferedStore>,
               _: &ops::AQ)
               -> Option<ops::Update> {
        // forward
        match u {
            ops::Update::Records(mut rs) => {
                if let Some(ops::Record::Positive(r, ts)) = rs.pop() {
                    if let query::DataType::Number(r) = r[0] {
                        Some(ops::Update::Records(vec![ops::Record::Positive(vec![(r + self.0).into()],
                                                                             ts)]))
                    } else {
                        unreachable!();
                    }
                } else {
                    unreachable!();
                }
            }
        }
    }

    fn query<'a>(&'a self, _: Option<&query::Query>, ts: i64, aqs: &ops::AQ) -> ops::Datas {
        // query all ancestors, emit r + c for each
        let rs = aqs.iter().flat_map(|(_, aq)| aq(vec![], ts));
        let c = self.0;
        rs.map(move |(r, ts)| {
                if let query::DataType::Number(r) = r[0] {
                    (vec![(r + c).into()], ts)
                } else {
                    unreachable!();
                }
            })
            .collect()
    }

    fn suggest_indexes(&self, _: flow::NodeIndex) -> HashMap<flow::NodeIndex, Vec<usize>> {
        HashMap::new()
    }

    fn resolve(&self, _: usize) -> Vec<(flow::NodeIndex, usize)> {
        vec![]
    }
}
