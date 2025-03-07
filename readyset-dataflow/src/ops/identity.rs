use std::collections::HashMap;

use readyset_errors::ReadySetResult;
use serde::{Deserialize, Serialize};

use crate::prelude::*;
use crate::processing::{ColumnSource, LookupIndex};

/// Applies the identity operation to the view. Since the identity does nothing,
/// it is the simplest possible operation. Primary intended as a reference
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Identity {
    src: IndexPair,
}

impl Identity {
    /// Construct a new identity operator.
    pub fn new(src: NodeIndex) -> Identity {
        Identity { src: src.into() }
    }
}

impl Ingredient for Identity {
    fn ancestors(&self) -> Vec<NodeIndex> {
        vec![self.src.as_global()]
    }

    fn on_connected(&mut self, _: &Graph) {}

    impl_replace_sibling!(src);

    fn on_commit(&mut self, _: NodeIndex, remap: &HashMap<NodeIndex, IndexPair>) {
        self.src.remap(remap);
    }

    fn on_input(
        &mut self,
        _: LocalNodeIndex,
        rs: Records,
        _: &ReplayContext<'_>,
        _: &DomainNodes,
        _: &StateMap,
        _: &mut AuxiliaryNodeStateMap,
    ) -> ReadySetResult<ProcessingResult> {
        Ok(ProcessingResult {
            results: rs,
            ..Default::default()
        })
    }

    fn suggest_indexes(&self, _: NodeIndex) -> HashMap<NodeIndex, LookupIndex> {
        HashMap::new()
    }

    fn column_source(&self, cols: &[usize]) -> ColumnSource {
        ColumnSource::exact_copy(self.src.as_global(), cols.into())
    }

    fn description(&self) -> String {
        "≡".into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ops;

    fn setup(materialized: bool) -> ops::test::MockGraph {
        let mut g = ops::test::MockGraph::new();
        let s = g.add_base("source", &["x", "y", "z"]);
        g.set_op(
            "identity",
            &["x", "y", "z"],
            Identity::new(s.as_global()),
            materialized,
        );
        g
    }

    #[test]
    fn it_forwards() {
        let mut g = setup(false);

        let left: Vec<DfValue> = vec![1.into(), "a".into()];
        assert_eq!(g.narrow_one_row(left.clone(), false), vec![left].into());
    }

    #[test]
    fn it_suggests_indices() {
        let g = setup(false);
        let me = 1.into();
        let idx = g.node().suggest_indexes(me);
        assert_eq!(idx.len(), 0);
    }

    #[test]
    fn it_resolves() {
        let g = setup(false);
        assert_eq!(
            g.node().resolve(0),
            Some(vec![(g.narrow_base_id().as_global(), 0)])
        );
        assert_eq!(
            g.node().resolve(1),
            Some(vec![(g.narrow_base_id().as_global(), 1)])
        );
        assert_eq!(
            g.node().resolve(2),
            Some(vec![(g.narrow_base_id().as_global(), 2)])
        );
    }
}
