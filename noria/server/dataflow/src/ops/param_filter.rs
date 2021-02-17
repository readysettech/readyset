use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt::Display;

use crate::prelude::*;

/// The operator we're comparing on for a [`ParamFilter`]
///
/// This is obviously quite simple right now - at some point in the future this should probably be
/// extended to be a full predicate AST, with ANDs and ORs and the like - at that point
/// `ParamFilter::col` and `ParamFilter::emit_key` would probably also grow to be slices
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum Operator {
    Like,
    ILike,
}

impl Display for Operator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Like => write!(f, "LIKE"),
            Self::ILike => write!(f, "ILIKE"),
        }
    }
}

/// Stores all records where a column matches a key, and emits that key as the column at `emit_key`.
///
/// For example, if we have [`Operator::Like`] and `col` is 0, our state might be:
///
/// ```ignore
/// "%a%": [["bar", 0], ["baz", 1]]
/// "b%": [["bar", 0], ["baz", 1]]
/// "ba%": [["bar", 0], ["baz", 1]]
/// "bar%": [["bar", 0]]
/// ```
///
/// See https://www.notion.so/KeyedFilter-Operator-aka-Slow-ILIKE-1b9b5693a12943e083b296a031652d3f
/// for documentation on the design of this operator
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParamFilter {
    src: IndexPair,
    col: usize,
    emit_key: usize,
    operator: Operator,
}

impl ParamFilter {
    pub fn new(src: NodeIndex, col: usize, emit_key: usize, operator: Operator) -> Self {
        ParamFilter {
            src: src.into(),
            col,
            emit_key,
            operator,
        }
    }
}

impl Ingredient for ParamFilter {
    fn take(&mut self) -> NodeOperator {
        self.clone().into()
    }

    fn ancestors(&self) -> Vec<NodeIndex> {
        vec![self.src.as_global()]
    }

    fn suggest_indexes(&self, _: NodeIndex) -> HashMap<NodeIndex, Index> {
        HashMap::new()
    }

    fn resolve(&self, i: usize) -> Option<Vec<(NodeIndex, usize)>> {
        if i == self.emit_key {
            None
        } else {
            Some(vec![(self.src.as_global(), i)])
        }
    }

    fn description(&self, detailed: bool) -> String {
        if detailed {
            format!("σφ[{} {} → {}]", self.operator, self.col, self.emit_key)
        } else {
            "σφ".to_string()
        }
    }

    fn on_commit(&mut self, _: NodeIndex, remap: &HashMap<NodeIndex, IndexPair>) {
        self.src.remap(remap);
    }

    fn on_input(
        &mut self,
        _executor: &mut dyn Executor,
        _from: LocalNodeIndex,
        _data: Records,
        _replay_key_cols: Option<&[usize]>,
        _domain: &DomainNodes,
        _states: &StateMap,
    ) -> ProcessingResult {
        ProcessingResult::default()
    }

    fn parent_columns(&self, column: usize) -> Vec<(NodeIndex, Option<usize>)> {
        vec![(
            self.src.as_global(),
            if column == self.emit_key {
                None
            } else {
                Some(column)
            },
        )]
    }

    fn can_query_through(&self) -> bool {
        true
    }

    #[allow(clippy::type_complexity)]
    fn query_through<'a>(
        &self,
        _columns: &[usize],
        _key: &KeyType,
        _nodes: &DomainNodes,
        _states: &'a StateMap,
    ) -> Option<Option<Box<dyn Iterator<Item = Cow<'a, [DataType]>> + 'a>>> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use Operator::*;

    use crate::ops;

    fn setup(operator: Operator) -> ops::test::MockGraph {
        let mut g = ops::test::MockGraph::new();
        let a = g.add_base("a", &["x", "y"]);
        g.set_op(
            "key",
            &["x", "y", "x_q"],
            ParamFilter::new(a.as_global(), 0, 2, operator),
            false,
        );
        g
    }

    #[test]
    fn resolve() {
        let g = setup(Like);
        assert_eq!(
            g.node().resolve(0),
            Some(vec![(g.narrow_base_id().as_global(), 0)])
        );
        assert_eq!(
            g.node().resolve(1),
            Some(vec![(g.narrow_base_id().as_global(), 1)])
        );
        assert_eq!(g.node().resolve(2), None);
    }

    #[test]
    fn process_when_nothing_matches_returns_nothing() {
        let mut g = setup(Like);
        let res = g.narrow_one_row(vec!["abc".into(), 2.into()], false);
        assert!(res.is_empty());
    }
}
