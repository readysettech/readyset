use ops;
use flow;
use query;
use backlog;
use ops::NodeOp;

use std::collections::HashMap;

pub struct Base {}

impl NodeOp for Base {
    fn forward(&self,
               u: ops::Update,
               _: flow::NodeIndex,
               _: i64,
               _: Option<&backlog::BufferedStore>,
               _: &ops::AQ)
               -> Option<ops::Update> {
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
