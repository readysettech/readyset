use std::collections::HashMap;

use dataflow_expression::Expr;
use dataflow_state::PointKey;
use readyset_errors::ReadySetResult;
use serde::{Deserialize, Serialize};
use tracing::error;

use crate::prelude::*;
use crate::processing::{ColumnSource, IngredientLookupResult, LookupIndex, LookupMode};

/// The filter operator
///
/// The filter operator evaluates an [`Expr`] on incoming records, and only emits records for
/// which that expression is truthy (is not 0, 0.0, '', or NULL).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Filter {
    src: IndexPair,
    expression: Expr,
}

impl Filter {
    /// Construct a new filter operator with an expression
    pub fn new(src: NodeIndex, expression: Expr) -> Filter {
        Filter {
            src: src.into(),
            expression,
        }
    }
}

impl Ingredient for Filter {
    fn ancestors(&self) -> Vec<NodeIndex> {
        vec![self.src.as_global()]
    }

    impl_replace_sibling!(src);

    fn on_commit(&mut self, _: NodeIndex, remap: &HashMap<NodeIndex, IndexPair>) {
        self.src.remap(remap);
    }

    fn on_input(
        &mut self,
        _: LocalNodeIndex,
        rs: Records,
        _: &ReplayContext,
        _: &DomainNodes,
        _: &StateMap,
        _: &mut AuxiliaryNodeStateMap,
    ) -> ReadySetResult<ProcessingResult> {
        let mut results = Vec::new();
        let mut log_error_once_flag = false;
        for r in rs {
            // If expression evaluation fails, we will filter out the row.
            let condition_res = match self.expression.eval(r.rec()) {
                Ok(v) => v.is_truthy(),
                // TODO (REA-2964): Handle expression eval errors
                Err(e) => {
                    // only log this error once per on_input call
                    if !log_error_once_flag {
                        error!(%e, "Error evaluating filter expression");
                        log_error_once_flag = true;
                    }
                    false
                }
            };

            if condition_res {
                results.push(r);
            }
        }

        Ok(ProcessingResult {
            results: results.into(),
            ..Default::default()
        })
    }

    fn suggest_indexes(&self, _: NodeIndex) -> HashMap<NodeIndex, LookupIndex> {
        HashMap::new()
    }

    fn column_source(&self, cols: &[usize]) -> ColumnSource {
        ColumnSource::exact_copy(self.src.as_global(), cols.to_vec())
    }

    fn description(&self) -> String {
        format!("σ[{}]", self.expression)
    }

    fn can_query_through(&self) -> bool {
        true
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
        match self.lookup(*self.src, columns, key, nodes, states, mode)? {
            IngredientLookupResult::Records(rs) => {
                let f = self.expression.clone();
                let filter = move |r: &[DfValue]| Ok(f.eval(r)?.is_truthy());
                Ok(IngredientLookupResult::Records(
                    Box::new(rs.filter_map(move |r| {
                        match r {
                            Ok(data) => {
                                match filter(&data) {
                                    Ok(true) => Some(Ok(data)),
                                    Err(e) => {
                                        // If we got an error, we need to combine this result as
                                        // part of the
                                        // iterator, so a caller can deal with the error.
                                        Some(Err(e))
                                    }
                                    _ => None,
                                }
                            }
                            Err(e) => {
                                // If we got an error, we need to combine this result as part of the
                                // iterator, so a caller can deal with the error.
                                Some(Err(e))
                            }
                        }
                    })) as _,
                ))
            }
            IngredientLookupResult::Miss => Ok(IngredientLookupResult::Miss),
        }
    }
}

#[cfg(test)]
mod tests {
    use dataflow_expression::utils::{column_with_type, make_literal};
    use dataflow_expression::BinaryOperator;
    use readyset_data::DfType;
    use Expr::Op;

    use super::*;
    use crate::ops;

    fn setup(materialized: bool, filters: Option<Expr>) -> ops::test::MockGraph {
        let mut g = ops::test::MockGraph::new();
        let s = g.add_base("source", &["x", "y"]);
        g.set_op(
            "filter",
            &["x", "y"],
            Filter::new(
                s.as_global(),
                filters.unwrap_or_else(|| Op {
                    left: Box::new(column_with_type(1, DfType::DEFAULT_TEXT)),
                    op: BinaryOperator::Equal,
                    right: Box::new(make_literal(DfValue::from("a"))),
                    ty: DfType::Bool,
                }),
            ),
            materialized,
        );
        g
    }

    #[test]
    fn it_forwards_constant_expr() {
        let mut g = setup(false, Some(make_literal(DfValue::from(1))));

        let mut left: Vec<DfValue> = vec![1.into(), "a".into()];
        assert_eq!(g.narrow_one_row(left.clone(), false), vec![left].into());

        left = vec![1.into(), "b".into()];
        assert_eq!(g.narrow_one_row(left.clone(), false), vec![left].into());

        left = vec![2.into(), "a".into()];
        assert_eq!(g.narrow_one_row(left.clone(), false), vec![left].into());
    }

    #[test]
    fn it_forwards() {
        let mut g = setup(false, None);

        let mut left: Vec<DfValue> = vec![1.into(), "a".into()];
        assert_eq!(g.narrow_one_row(left.clone(), false), vec![left].into());

        left = vec![1.into(), "b".into()];
        assert!(g.narrow_one_row(left.clone(), false).is_empty());

        left = vec![2.into(), "a".into()];
        assert_eq!(g.narrow_one_row(left.clone(), false), vec![left].into());
    }

    #[test]
    fn it_forwards_mfilter() {
        let mut g = setup(
            false,
            Some(Op {
                left: Box::new(Op {
                    left: Box::new(column_with_type(0, DfType::Int)),
                    op: BinaryOperator::Equal,
                    right: Box::new(make_literal(DfValue::from(1))),
                    ty: DfType::Bool,
                }),
                op: BinaryOperator::And,
                right: Box::new(Op {
                    left: Box::new(column_with_type(1, DfType::DEFAULT_TEXT)),
                    op: BinaryOperator::Equal,
                    right: Box::new(make_literal(DfValue::from("a"))),
                    ty: DfType::Bool,
                }),
                ty: DfType::Bool,
            }),
        );

        let mut left: Vec<DfValue> = vec![1.into(), "a".into()];
        assert_eq!(g.narrow_one_row(left.clone(), false), vec![left].into());

        left = vec![1.into(), "b".into()];
        assert!(g.narrow_one_row(left.clone(), false).is_empty());

        left = vec![2.into(), "a".into()];
        assert!(g.narrow_one_row(left.clone(), false).is_empty());

        left = vec![2.into(), "b".into()];
        assert!(g.narrow_one_row(left, false).is_empty());
    }

    #[test]
    fn it_suggests_indices() {
        let g = setup(false, None);
        let me = 1.into();
        let idx = g.node().suggest_indexes(me);
        assert_eq!(idx.len(), 0);
    }

    #[test]
    fn it_resolves() {
        let g = setup(false, None);
        assert_eq!(
            g.node().resolve(0),
            Some(vec![(g.narrow_base_id().as_global(), 0)])
        );
        assert_eq!(
            g.node().resolve(1),
            Some(vec![(g.narrow_base_id().as_global(), 1)])
        );
    }

    #[test]
    fn it_works_with_many() {
        let mut g = setup(false, None);

        let mut many = Vec::new();

        for i in 0..10 {
            many.push(vec![i.into(), "a".into()]);
        }

        assert_eq!(g.narrow_one(many.clone(), false), many.into());
    }

    #[test]
    fn it_works_with_inequalities() {
        let mut g = setup(
            false,
            Some(Op {
                left: Box::new(Op {
                    left: Box::new(column_with_type(0, DfType::Int)),
                    op: BinaryOperator::LessOrEqual,
                    right: Box::new(make_literal(DfValue::from(2))),
                    ty: DfType::Bool,
                }),
                op: BinaryOperator::And,
                right: Box::new(Expr::Not {
                    expr: Box::new(Op {
                        left: Box::new(column_with_type(1, DfType::DEFAULT_TEXT)),
                        op: BinaryOperator::Equal,
                        right: Box::new(make_literal(DfValue::from("a"))),
                        ty: DfType::Bool,
                    }),
                    ty: DfType::Bool,
                }),
                ty: DfType::Bool,
            }),
        );

        // both conditions match (2 <= 2, "b" != "a")
        let mut left: Vec<DfValue> = vec![2.into(), "b".into()];
        assert_eq!(g.narrow_one_row(left.clone(), false), vec![left].into());

        // second condition fails ("a" != "a")
        left = vec![2.into(), "a".into()];
        assert!(g.narrow_one_row(left.clone(), false).is_empty());

        // first condition fails (3 <= 2)
        left = vec![3.into(), "b".into()];
        assert!(g.narrow_one_row(left.clone(), false).is_empty());

        // both conditions match (1 <= 2, "b" != "a")
        left = vec![1.into(), "b".into()];
        assert_eq!(g.narrow_one_row(left.clone(), false), vec![left].into());
    }

    #[test]
    fn it_works_with_columns() {
        let mut g = setup(
            false,
            Some(Op {
                left: Box::new(column_with_type(0, DfType::Int)),
                op: BinaryOperator::Equal,
                right: Box::new(column_with_type(1, DfType::Int)),
                ty: DfType::Bool,
            }),
        );

        let mut left: Vec<DfValue> = vec![2.into(), 2.into()];
        assert_eq!(g.narrow_one_row(left.clone(), false), vec![left].into());
        left = vec![2.into(), 3.into()];
        assert_eq!(g.narrow_one_row(left, false), Records::default());
    }
}
