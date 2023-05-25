use std::borrow::Cow;
use std::collections::HashMap;

use dataflow_expression::Expr;
use dataflow_state::PointKey;
use itertools::Itertools;
use readyset_errors::ReadySetResult;
use serde::{Deserialize, Serialize};
use tracing::error;

use crate::prelude::*;
use crate::processing::{ColumnSource, IngredientLookupResult, LookupIndex, LookupMode};

/// Permutes or omits columns from its source node, or adds additional columns whose values are
/// given by expressions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Project {
    us: Option<IndexPair>,
    emit: Vec<Expr>,
    src: IndexPair,
    cols: usize,
}

impl Project {
    /// Construct a new project operator.
    pub fn new(src: NodeIndex, emit: Vec<Expr>) -> Project {
        Project {
            emit,
            src: src.into(),
            cols: 0,
            us: None,
        }
    }
}

impl Ingredient for Project {
    fn take(&mut self) -> NodeOperator {
        Clone::clone(self).into()
    }

    fn ancestors(&self) -> Vec<NodeIndex> {
        vec![self.src.as_global()]
    }

    fn can_query_through(&self) -> bool {
        // TODO(grfn): Make query_through just use column provenance (?) or make it pass column
        // indices at least
        self.emit
            .iter()
            .all(|expr| matches!(expr, Expr::Column { .. }))
    }

    #[allow(clippy::type_complexity)]
    fn query_through<'a>(
        &self,
        columns: &[usize],
        key: &PointKey,
        nodes: &DomainNodes,
        states: &'a StateMap,
        mode: LookupMode,
    ) -> ReadySetResult<IngredientLookupResult<'a>> {
        let in_cols = match columns
            .iter()
            .map(|&outi| match self.emit.get(outi) {
                Some(Expr::Column { index, .. }) => Ok(*index),
                _ => Err(internal_err!(
                    "query_through should never be queried for generated columns; \
                         columns: {columns:?}",
                )),
            })
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(cols) => cols,
            Err(e) => return Ok(IngredientLookupResult::err(e)),
        };

        let res = self.lookup(*self.src, &in_cols, key, nodes, states, mode)?;
        let emit = self.emit.clone();
        Ok(res.map(move |r| {
            let r = r?;
            emit.iter()
                .map(|expr| expr.eval(&r))
                .collect::<ReadySetResult<Vec<DfValue>>>()
                .map(Cow::Owned)
        }))
    }

    fn on_connected(&mut self, g: &Graph) {
        self.cols = g[self.src.as_global()].columns().len();
    }

    impl_replace_sibling!(src);

    fn on_commit(&mut self, us: NodeIndex, remap: &HashMap<NodeIndex, IndexPair>) {
        self.src.remap(remap);
        self.us = Some(remap[&us]);
    }

    fn on_input(
        &mut self,
        from: LocalNodeIndex,
        mut rs: Records,
        _: &ReplayContext,
        _: &DomainNodes,
        _: &StateMap,
        _: &mut AuxiliaryNodeStateMap,
    ) -> ReadySetResult<ProcessingResult> {
        debug_assert_eq!(from, *self.src);
        for r in &mut *rs {
            **r = self
                .emit
                .iter()
                .map(|expr| match expr.eval(r) {
                    Ok(val) => val,
                    Err(error) => {
                        error!(%error, "Error evaluating project expression");
                        DfValue::None
                    }
                })
                .collect();
        }

        Ok(ProcessingResult {
            results: rs,
            ..Default::default()
        })
    }

    fn suggest_indexes(&self, _: NodeIndex) -> HashMap<NodeIndex, LookupIndex> {
        HashMap::new()
    }

    fn column_source(&self, cols: &[usize]) -> ColumnSource {
        let mapped_cols = cols
            .iter()
            .filter_map(|&col| match self.emit.get(col) {
                Some(Expr::Column { index, .. }) => Some(*index),
                _ => None,
            })
            .collect::<Vec<_>>();

        if mapped_cols.len() != cols.len() {
            ColumnSource::RequiresFullReplay(vec1![self.src.as_global()])
        } else {
            ColumnSource::exact_copy(self.src.as_global(), mapped_cols)
        }
    }

    fn description(&self, detailed: bool) -> String {
        if !detailed {
            return String::from("π");
        }

        format!("π[{}]", self.emit.iter().join(", "))
    }
}

#[cfg(test)]
mod tests {
    use dataflow_expression::utils::{make_int_column, make_literal};
    use dataflow_expression::BinaryOperator;
    use dataflow_state::MaterializedNodeState;
    use readyset_data::DfType;
    use Expr::Op;

    use super::*;
    use crate::ops;

    fn setup(materialized: bool, all: bool, add: bool) -> ops::test::MockGraph {
        let mut g = ops::test::MockGraph::new();
        let s = g.add_base("source", &["x", "y", "z"]);

        let mut exprs = (if all { vec![0, 1, 2] } else { vec![2, 0] })
            .into_iter()
            .map(|index| Expr::Column {
                index,
                ty: DfType::DEFAULT_TEXT,
            })
            .collect::<Vec<_>>();
        if add {
            exprs.extend([
                Expr::Literal {
                    val: DfValue::from("hello"),
                    ty: DfType::DEFAULT_TEXT,
                },
                Expr::Literal {
                    val: DfValue::Int(42),
                    ty: DfType::Int,
                },
            ]);
        }
        g.set_op(
            "permute",
            &["x", "y", "z"],
            Project::new(s.as_global(), exprs),
            materialized,
        );
        g
    }

    fn setup_arithmetic(expression: Expr) -> ops::test::MockGraph {
        let mut g = ops::test::MockGraph::new();
        let s = g.add_base("source", &["x", "y", "z"]);

        let permutation = vec![
            Expr::Column {
                index: 0,
                ty: DfType::DEFAULT_TEXT,
            },
            Expr::Column {
                index: 1,
                ty: DfType::DEFAULT_TEXT,
            },
            expression,
        ];
        g.set_op(
            "permute",
            &["x", "y", "z"],
            Project::new(s.as_global(), permutation),
            false,
        );
        g
    }

    fn setup_column_arithmetic(op: BinaryOperator) -> ops::test::MockGraph {
        let expression = Expr::Op {
            left: Box::new(make_int_column(0)),
            right: Box::new(make_int_column(1)),
            op,
            ty: DfType::Int,
        };

        setup_arithmetic(expression)
    }

    #[test]
    fn it_describes() {
        let p = setup(false, false, true);
        assert_eq!(
            p.node().description(true),
            "π[2, 0, (lit: hello), (lit: 42)]"
        );
    }

    #[test]
    fn it_describes_arithmetic() {
        let p = setup_column_arithmetic(BinaryOperator::Add);
        assert_eq!(p.node().description(true), "π[0, 1, (0 + 1)]");
    }

    #[test]
    fn it_describes_all_w_literals() {
        let p = setup(false, true, true);
        assert_eq!(
            p.node().description(true),
            "π[0, 1, 2, (lit: hello), (lit: 42)]"
        );
    }

    #[test]
    fn it_forwards_some() {
        let mut p = setup(false, false, true);

        let rec = vec![
            "a".try_into().unwrap(),
            "b".try_into().unwrap(),
            "c".try_into().unwrap(),
        ];
        assert_eq!(
            p.narrow_one_row(rec, false),
            vec![vec![
                "c".try_into().unwrap(),
                "a".try_into().unwrap(),
                "hello".try_into().unwrap(),
                42.into()
            ]]
            .into()
        );
    }

    #[test]
    fn it_forwards_all() {
        let mut p = setup(false, true, false);

        let rec = vec![
            "a".try_into().unwrap(),
            "b".try_into().unwrap(),
            "c".try_into().unwrap(),
        ];
        assert_eq!(
            p.narrow_one_row(rec, false),
            vec![vec![
                "a".try_into().unwrap(),
                "b".try_into().unwrap(),
                "c".try_into().unwrap()
            ]]
            .into()
        );
    }

    #[test]
    fn it_forwards_all_w_literals() {
        let mut p = setup(false, true, true);

        let rec = vec![
            "a".try_into().unwrap(),
            "b".try_into().unwrap(),
            "c".try_into().unwrap(),
        ];
        assert_eq!(
            p.narrow_one_row(rec, false),
            vec![vec![
                "a".try_into().unwrap(),
                "b".try_into().unwrap(),
                "c".try_into().unwrap(),
                "hello".try_into().unwrap(),
                42.into(),
            ]]
            .into()
        );
    }

    #[test]
    fn it_forwards_addition_arithmetic() {
        let mut p = setup_column_arithmetic(BinaryOperator::Add);
        let rec = vec![10.into(), 20.into()];
        assert_eq!(
            p.narrow_one_row(rec, false),
            vec![vec![10.into(), 20.into(), 30.into()]].into()
        );
    }

    #[test]
    fn it_forwards_subtraction_arithmetic() {
        let mut p = setup_column_arithmetic(BinaryOperator::Subtract);
        let rec = vec![10.into(), 20.into()];
        assert_eq!(
            p.narrow_one_row(rec, false),
            vec![vec![10.into(), 20.into(), (-10).into()]].into()
        );
    }

    #[test]
    fn it_forwards_multiplication_arithmetic() {
        let mut p = setup_column_arithmetic(BinaryOperator::Multiply);
        let rec = vec![10.into(), 20.into()];
        assert_eq!(
            p.narrow_one_row(rec, false),
            vec![vec![10.into(), 20.into(), 200.into()]].into()
        );
    }

    #[test]
    fn it_forwards_division_arithmetic() {
        let mut p = setup_column_arithmetic(BinaryOperator::Divide);
        let rec = vec![10.into(), 2.into()];
        assert_eq!(
            p.narrow_one_row(rec, false),
            vec![vec![10.into(), 2.into(), 5.into()]].into()
        );
    }

    #[test]
    fn it_forwards_arithmetic_w_literals() {
        let number: DfValue = 40.into();
        let expression = Expr::Op {
            left: Box::new(make_int_column(0)),
            right: Box::new(make_literal(number)),
            op: BinaryOperator::Multiply,
            ty: DfType::Int,
        };

        let mut p = setup_arithmetic(expression);
        let rec = vec![10.into(), 0.into()];
        assert_eq!(
            p.narrow_one_row(rec, false),
            vec![vec![10.into(), 0.into(), 400.into()]].into()
        );
    }

    #[test]
    fn it_forwards_arithmetic_w_only_literals() {
        let a: DfValue = 80.into();
        let b: DfValue = 40.into();
        let expression = Expr::Op {
            left: Box::new(make_literal(a)),
            right: Box::new(make_literal(b)),
            op: BinaryOperator::Divide,
            ty: DfType::Int,
        };

        let mut p = setup_arithmetic(expression);
        let rec = vec![0.into(), 0.into()];
        assert_eq!(
            p.narrow_one_row(rec, false),
            vec![vec![0.into(), 0.into(), 2.into()]].into()
        );
    }

    fn setup_query_through(
        mut state: MaterializedNodeState,
        emit: Vec<Expr>,
    ) -> (Project, StateMap) {
        let global = NodeIndex::new(0);
        let mut index: IndexPair = global.into();
        let local = LocalNodeIndex::make(0);
        index.set_local(local);

        let mut states = StateMap::default();
        let row: Record = vec![1.into(), 2.into(), 3.into()].into();
        state.add_key(Index::hash_map(vec![0]), None);
        state.add_key(Index::hash_map(vec![1]), None);
        state.process_records(&mut row.into(), None, None).unwrap();
        states.insert(local, state);

        let mut project = Project::new(global, emit);
        let mut remap = HashMap::new();
        remap.insert(global, index);
        project.on_commit(global, &remap);
        (project, states)
    }

    fn assert_query_through(
        project: Project,
        by_column: usize,
        key: DfValue,
        states: StateMap,
        expected: Vec<DfValue>,
    ) {
        let mut iter = project
            .query_through(
                &[by_column],
                &PointKey::Single(key),
                &DomainNodes::default(),
                &states,
                LookupMode::Strict,
            )
            .unwrap()
            .unwrap();
        assert_eq!(expected, iter.next().unwrap().unwrap().into_owned());
    }

    #[test]
    fn it_queries_through_all() {
        let state = MaterializedNodeState::Memory(MemoryState::default());
        let (p, states) = setup_query_through(
            state,
            vec![
                Expr::Column {
                    index: 0,
                    ty: DfType::Int,
                },
                Expr::Column {
                    index: 1,
                    ty: DfType::Int,
                },
                Expr::Column {
                    index: 2,
                    ty: DfType::Int,
                },
            ],
        );
        let expected: Vec<DfValue> = vec![1.into(), 2.into(), 3.into()];
        assert_query_through(p, 0, 1.into(), states, expected);
    }

    #[test]
    fn it_queries_through_all_persistent() {
        let state = MaterializedNodeState::Persistent(
            PersistentState::new(
                String::from("it_queries_through_all_persistent"),
                Vec::<Box<[usize]>>::new(),
                &PersistenceParameters::default(),
            )
            .unwrap(),
        );

        let (p, states) = setup_query_through(
            state,
            vec![
                Expr::Column {
                    index: 0,
                    ty: DfType::Int,
                },
                Expr::Column {
                    index: 1,
                    ty: DfType::Int,
                },
                Expr::Column {
                    index: 2,
                    ty: DfType::Int,
                },
            ],
        );
        let expected: Vec<DfValue> = vec![1.into(), 2.into(), 3.into()];
        assert_query_through(p, 0, 1.into(), states, expected);
    }

    #[test]
    fn it_queries_through_some() {
        let state = MaterializedNodeState::Memory(MemoryState::default());
        let (p, states) = setup_query_through(
            state,
            vec![Expr::Column {
                index: 1,
                ty: DfType::Int,
            }],
        );
        let expected: Vec<DfValue> = vec![2.into()];
        assert_query_through(p, 0, 2.into(), states, expected);
    }

    #[test]
    fn it_queries_through_some_persistent() {
        let state = MaterializedNodeState::Persistent(
            PersistentState::new(
                String::from("it_queries_through_some_persistent"),
                Vec::<Box<[usize]>>::new(),
                &PersistenceParameters::default(),
            )
            .unwrap(),
        );

        let (p, states) = setup_query_through(
            state,
            vec![Expr::Column {
                index: 1,
                ty: DfType::Int,
            }],
        );
        let expected: Vec<DfValue> = vec![2.into()];
        assert_query_through(p, 0, 2.into(), states, expected);
    }

    #[test]
    fn it_queries_through_w_literals() {
        let state = MaterializedNodeState::Memory(MemoryState::default());
        let (p, states) = setup_query_through(
            state,
            vec![
                Expr::Column {
                    index: 1,
                    ty: DfType::Int,
                },
                Expr::Literal {
                    val: DfValue::Int(42),
                    ty: DfType::Int,
                },
            ],
        );
        let expected: Vec<DfValue> = vec![2.into(), 42.into()];
        assert_query_through(p, 0, 2.into(), states, expected);
    }

    #[test]
    fn it_queries_through_w_literals_persistent() {
        let state = MaterializedNodeState::Persistent(
            PersistentState::new(
                String::from("it_queries_through_w_literals"),
                Vec::<Box<[usize]>>::new(),
                &PersistenceParameters::default(),
            )
            .unwrap(),
        );

        let (p, states) = setup_query_through(
            state,
            vec![
                Expr::Column {
                    index: 1,
                    ty: DfType::Int,
                },
                Expr::Literal {
                    val: DfValue::Int(42),
                    ty: DfType::Int,
                },
            ],
        );
        let expected: Vec<DfValue> = vec![2.into(), 42.into()];
        assert_query_through(p, 0, 2.into(), states, expected);
    }

    #[test]
    fn it_queries_through_w_arithmetic_and_literals() {
        let state = MaterializedNodeState::Memory(MemoryState::default());
        let (p, states) = setup_query_through(
            state,
            vec![
                Expr::Column {
                    index: 1,
                    ty: DfType::Int,
                },
                Expr::Op {
                    left: Box::new(make_int_column(0)),
                    right: Box::new(make_int_column(1)),
                    op: BinaryOperator::Add,
                    ty: DfType::Int,
                },
                Expr::Literal {
                    val: DfValue::Int(42),
                    ty: DfType::Int,
                },
            ],
        );
        let expected: Vec<DfValue> = vec![2.into(), (1 + 2).into(), 42.into()];
        assert_query_through(p, 0, 2.into(), states, expected);
    }

    #[test]
    fn it_queries_through_w_arithmetic_and_literals_persistent() {
        let state = MaterializedNodeState::Persistent(
            PersistentState::new(
                String::from("it_queries_through_w_arithmetic_and_literals_persistent"),
                Vec::<Box<[usize]>>::new(),
                &PersistenceParameters::default(),
            )
            .unwrap(),
        );

        let (p, states) = setup_query_through(
            state,
            vec![
                Expr::Column {
                    index: 1,
                    ty: DfType::Int,
                },
                Expr::Op {
                    left: Box::new(make_int_column(0)),
                    right: Box::new(make_int_column(1)),
                    op: BinaryOperator::Add,
                    ty: DfType::Int,
                },
                Expr::Literal {
                    val: DfValue::Int(42),
                    ty: DfType::Int,
                },
            ],
        );
        let expected: Vec<DfValue> = vec![2.into(), (1 + 2).into(), 42.into()];
        assert_query_through(p, 0, 2.into(), states, expected);
    }

    #[test]
    fn it_queries_nested_expressions() {
        let expression = Op {
            op: BinaryOperator::Multiply,
            left: Box::new(Op {
                left: Box::new(make_int_column(0)),
                right: Box::new(make_int_column(1)),
                op: BinaryOperator::Add,
                ty: DfType::Int,
            }),
            right: Box::new(make_literal(DfValue::Int(2))),
            ty: DfType::Int,
        };

        let state = MaterializedNodeState::Persistent(
            PersistentState::new(
                String::from("it_queries_nested_expressions"),
                Vec::<Box<[usize]>>::new(),
                &PersistenceParameters::default(),
            )
            .unwrap(),
        );

        let (p, states) = setup_query_through(
            state,
            vec![
                Expr::Column {
                    index: 1,
                    ty: DfType::Int,
                },
                expression,
            ],
        );
        let expected: Vec<DfValue> = vec![2.into(), ((1 + 2) * 2).into()];
        assert_query_through(p, 0, 2.into(), states, expected);
    }

    #[test]
    fn it_suggests_indices() {
        let me = 1.into();
        let p = setup(false, false, true);
        let idx = p.node().suggest_indexes(me);
        assert_eq!(idx.len(), 0);
    }

    #[test]
    fn it_resolves() {
        let p = setup(false, false, true);
        assert_eq!(
            p.node().resolve(0),
            Some(vec![(p.narrow_base_id().as_global(), 2)])
        );
        assert_eq!(
            p.node().resolve(1),
            Some(vec![(p.narrow_base_id().as_global(), 0)])
        );
    }

    #[test]
    fn it_resolves_all() {
        let p = setup(false, true, true);
        assert_eq!(
            p.node().resolve(0),
            Some(vec![(p.narrow_base_id().as_global(), 0)])
        );
        assert_eq!(
            p.node().resolve(1),
            Some(vec![(p.narrow_base_id().as_global(), 1)])
        );
        assert_eq!(
            p.node().resolve(2),
            Some(vec![(p.narrow_base_id().as_global(), 2)])
        );
    }

    #[test]
    fn it_fails_to_resolve_literal() {
        let p = setup(false, false, true);
        assert!(p.node().resolve(2).is_none());
    }
}
