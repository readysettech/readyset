use std::collections::HashMap;
use std::convert::TryInto;

use dataflow_expression::Expression;
pub use nom_sql::BinaryOperator;
use noria_errors::ReadySetResult;
use serde::{Deserialize, Serialize};

use crate::prelude::*;
use crate::processing::{ColumnSource, IngredientLookupResult, LookupIndex, LookupMode};

/// The filter operator
///
/// The filter operator evaluates an [`Expression`] on incoming records, and only emits records for
/// which that expression is truthy (is not 0, 0.0, '', or NULL).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Filter {
    src: IndexPair,
    expression: Expression,
}

impl Filter {
    /// Construct a new filter operator with an expression
    pub fn new(src: NodeIndex, expression: Expression) -> Filter {
        Filter {
            src: src.into(),
            expression,
        }
    }
}

impl Ingredient for Filter {
    fn take(&mut self) -> NodeOperator {
        Clone::clone(self).into()
    }

    fn ancestors(&self) -> Vec<NodeIndex> {
        vec![self.src.as_global()]
    }

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
    ) -> ReadySetResult<ProcessingResult> {
        let mut results = Vec::new();
        for r in rs {
            if self.expression.eval(r.rec())?.is_truthy() {
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
        ColumnSource::exact_copy(self.src.as_global(), cols.try_into().unwrap())
    }

    fn description(&self, detailed: bool) -> String {
        if !detailed {
            String::from("σ")
        } else {
            format!("σ[{}]", self.expression)
        }
    }

    fn can_query_through(&self) -> bool {
        true
    }

    #[allow(clippy::type_complexity)]
    fn query_through<'a>(
        &self,
        columns: &[usize],
        key: &KeyType,
        nodes: &DomainNodes,
        states: &'a StateMap,
        mode: LookupMode,
    ) -> ReadySetResult<IngredientLookupResult<'a>> {
        match self.lookup(*self.src, columns, key, nodes, states, mode)? {
            IngredientLookupResult::Records(rs) => {
                let f = self.expression.clone();
                let filter = move |r: &[DataType]| Ok(f.eval(r)?.is_truthy());
                Ok(IngredientLookupResult::Records(
                    Box::new(rs.filter_map(move |r| {
                        match r {
                            Ok(data) => {
                                match filter(&(*data)) {
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

    fn is_selective(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use dataflow_expression::utils::{column_with_type, make_literal};
    use nom_sql::SqlType;
    use noria_data::noria_type::Type;
    use Expression::Op;

    use super::*;
    use crate::ops;

    fn setup(materialized: bool, filters: Option<Expression>) -> ops::test::MockGraph {
        let mut g = ops::test::MockGraph::new();
        let s = g.add_base("source", &["x", "y"]);
        g.set_op(
            "filter",
            &["x", "y"],
            Filter::new(
                s.as_global(),
                filters.unwrap_or_else(|| Op {
                    left: Box::new(column_with_type(1, Type::Sql(SqlType::Text))),
                    op: BinaryOperator::Equal,
                    right: Box::new(make_literal(DataType::from("a"))),
                    ty: Type::Sql(SqlType::Bool),
                }),
            ),
            materialized,
        );
        g
    }

    #[test]
    fn it_forwards_constant_expr() {
        let mut g = setup(false, Some(make_literal(DataType::from(1))));

        let mut left: Vec<DataType> = vec![1.into(), "a".try_into().unwrap()];
        assert_eq!(g.narrow_one_row(left.clone(), false), vec![left].into());

        left = vec![1.into(), "b".try_into().unwrap()];
        assert_eq!(g.narrow_one_row(left.clone(), false), vec![left].into());

        left = vec![2.into(), "a".try_into().unwrap()];
        assert_eq!(g.narrow_one_row(left.clone(), false), vec![left].into());
    }

    #[test]
    fn it_forwards() {
        let mut g = setup(false, None);

        let mut left: Vec<DataType> = vec![1.into(), "a".try_into().unwrap()];
        assert_eq!(g.narrow_one_row(left.clone(), false), vec![left].into());

        left = vec![1.into(), "b".try_into().unwrap()];
        assert!(g.narrow_one_row(left.clone(), false).is_empty());

        left = vec![2.into(), "a".try_into().unwrap()];
        assert_eq!(g.narrow_one_row(left.clone(), false), vec![left].into());
    }

    #[test]
    fn it_forwards_mfilter() {
        let mut g = setup(
            false,
            Some(Op {
                left: Box::new(Op {
                    left: Box::new(column_with_type(0, Type::Sql(SqlType::Int(None)))),
                    op: BinaryOperator::Equal,
                    right: Box::new(make_literal(DataType::from(1))),
                    ty: Type::Sql(SqlType::Bool),
                }),
                op: BinaryOperator::And,
                right: Box::new(Op {
                    left: Box::new(column_with_type(1, Type::Sql(SqlType::Text))),
                    op: BinaryOperator::Equal,
                    right: Box::new(make_literal(DataType::from("a"))),
                    ty: Type::Sql(SqlType::Bool),
                }),
                ty: Type::Sql(SqlType::Bool),
            }),
        );

        let mut left: Vec<DataType> = vec![1.into(), "a".try_into().unwrap()];
        assert_eq!(g.narrow_one_row(left.clone(), false), vec![left].into());

        left = vec![1.into(), "b".try_into().unwrap()];
        assert!(g.narrow_one_row(left.clone(), false).is_empty());

        left = vec![2.into(), "a".try_into().unwrap()];
        assert!(g.narrow_one_row(left.clone(), false).is_empty());

        left = vec![2.into(), "b".try_into().unwrap()];
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
            many.push(vec![i.into(), "a".try_into().unwrap()]);
        }

        assert_eq!(g.narrow_one(many.clone(), false), many.into());
    }

    #[test]
    fn it_works_with_inequalities() {
        let mut g = setup(
            false,
            Some(Op {
                left: Box::new(Op {
                    left: Box::new(column_with_type(0, Type::Sql(SqlType::Int(None)))),
                    op: BinaryOperator::LessOrEqual,
                    right: Box::new(make_literal(DataType::from(2))),
                    ty: Type::Sql(SqlType::Bool),
                }),
                op: BinaryOperator::And,
                right: Box::new(Op {
                    left: Box::new(column_with_type(1, Type::Sql(SqlType::Text))),
                    op: BinaryOperator::NotEqual,
                    right: Box::new(make_literal(DataType::from("a"))),
                    ty: Type::Sql(SqlType::Bool),
                }),
                ty: Type::Sql(SqlType::Bool),
            }),
        );

        // both conditions match (2 <= 2, "b" != "a")
        let mut left: Vec<DataType> = vec![2.into(), "b".try_into().unwrap()];
        assert_eq!(g.narrow_one_row(left.clone(), false), vec![left].into());

        // second condition fails ("a" != "a")
        left = vec![2.into(), "a".try_into().unwrap()];
        assert!(g.narrow_one_row(left.clone(), false).is_empty());

        // first condition fails (3 <= 2)
        left = vec![3.into(), "b".try_into().unwrap()];
        assert!(g.narrow_one_row(left.clone(), false).is_empty());

        // both conditions match (1 <= 2, "b" != "a")
        left = vec![1.into(), "b".try_into().unwrap()];
        assert_eq!(g.narrow_one_row(left.clone(), false), vec![left].into());
    }

    #[test]
    fn it_works_with_columns() {
        let mut g = setup(
            false,
            Some(Op {
                left: Box::new(column_with_type(0, Type::Sql(SqlType::Text))),
                op: BinaryOperator::Equal,
                right: Box::new(column_with_type(1, Type::Sql(SqlType::Text))),
                ty: Type::Sql(SqlType::Bool),
            }),
        );

        let mut left: Vec<DataType> = vec![2.into(), 2.into()];
        assert_eq!(g.narrow_one_row(left.clone(), false), vec![left].into());
        left = vec![2.into(), "b".try_into().unwrap()];
        assert_eq!(g.narrow_one_row(left, false), Records::default());
    }
}
