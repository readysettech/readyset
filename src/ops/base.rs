use ops;
use flow;
use query;
use backlog;
use ops::NodeOp;

use std::sync;

pub struct Base {}

impl NodeOp for Base {
    fn forward(&self,
               u: ops::Update,
               _: flow::NodeIndex,
               _: Option<&backlog::BufferedStore>,
               _: &ops::AQ)
               -> Option<ops::Update> {
        Some(u)
    }

    fn query<'a>(&'a self,
                 _: Option<&query::Query>,
                 _: i64,
                 _: sync::Arc<ops::AQ>)
                 -> ops::Datas<'a> {
        unreachable!("base nodes are always materialized");
    }
}
