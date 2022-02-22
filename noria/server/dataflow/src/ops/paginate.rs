use std::collections::HashMap;

use launchpad::Indices;
use nom_sql::OrderType;
use serde::{Deserialize, Serialize};

use crate::ops::utils::Order;
use crate::prelude::*;
use crate::processing::SuggestedIndex;

#[derive(Clone, Serialize, Deserialize)]
pub struct Paginate {
    /// The direct Ingredient or Base ancestor of this node
    src: IndexPair,
    /// The index of this node. Used to look up into our own state
    our_index: Option<IndexPair>,
    /// The List of column indices that we're grouping by
    group_by: Vec<usize>,
    /// The ordering for our records
    order: Order,
    /// The page length (LIMIT) field of our Paginator. We only handle fixed LIMITs
    limit: usize,
}

impl Paginate {
    pub fn new(
        src: NodeIndex,
        order: Vec<(usize, OrderType)>,
        group_by: Vec<usize>,
        limit: usize,
    ) -> Self {
        Paginate {
            src: src.into(),
            our_index: None,
            group_by,
            order: order.into(),
            limit,
        }
    }

    /// Project the columns we are grouping by out of the given record
    #[allow(dead_code)]
    fn project_group<'rec, R>(&self, rec: &'rec R) -> ReadySetResult<Vec<&'rec DataType>>
    where
        R: Indices<'static, usize, Output = DataType> + ?Sized,
    {
        rec.indices(self.group_by.clone())
            .map_err(|_| ReadySetError::InvalidRecordLength)
    }
}

impl Ingredient for Paginate {
    fn take(&mut self) -> NodeOperator {
        self.clone().into()
    }

    fn ancestors(&self) -> Vec<NodeIndex> {
        vec![self.src.as_global()]
    }

    #[allow(clippy::todo)]
    fn on_connected(&mut self, _g: &Graph) {
        todo!()
    }

    #[allow(clippy::todo)]
    fn on_commit(&mut self, _us: NodeIndex, _remap: &HashMap<NodeIndex, IndexPair>) {
        todo!()
    }

    #[allow(clippy::todo)]
    fn on_input<'a>(
        &mut self,
        _from: LocalNodeIndex,
        _rs: Records,
        _replay: &ReplayContext,
        _nodes: &DomainNodes,
        _state: &'a StateMap,
    ) -> ReadySetResult<ProcessingResult> {
        todo!()
    }

    #[allow(clippy::todo)]
    fn suggest_indexes(&self, _this: NodeIndex) -> HashMap<NodeIndex, SuggestedIndex> {
        todo!()
    }

    #[allow(clippy::todo)]
    fn column_source(&self, _cols: &[usize]) -> ColumnSource {
        todo!()
    }

    #[allow(clippy::todo)]
    fn description(&self, _detailed: bool) -> String {
        todo!()
    }

    #[allow(clippy::todo)]
    fn is_selective(&self) -> bool {
        todo!()
    }
}
