use std::borrow::Cow;
use std::collections::HashMap;

use crate::prelude::*;
use crate::processing::ColumnSource;
use dataflow_expression::Expression;
pub use nom_sql::BinaryOperator;
use noria::errors::ReadySetResult;
use std::convert::TryInto;

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
        _: &mut dyn Executor,
        _: LocalNodeIndex,
        rs: Records,
        _: Option<&[usize]>,
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

    fn suggest_indexes(&self, _: NodeIndex) -> HashMap<NodeIndex, Index> {
        HashMap::new()
    }

    fn column_source(&self, cols: &[usize]) -> ReadySetResult<ColumnSource> {
        Ok(ColumnSource::exact_copy(
            self.src.as_global(),
            cols.try_into().unwrap(),
        ))
    }

    fn description(&self, detailed: bool) -> String {
        if !detailed {
            String::from("σ")
        } else {
            format!("σ[{}]", self.expression.to_string())
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
    ) -> Option<Option<Box<dyn Iterator<Item = Cow<'a, [DataType]>> + 'a>>> {
        self.lookup(*self.src, columns, key, nodes, states)
            .map(|result| {
                let f = self.expression.clone();
                let filter = move |r: &[DataType]| {
                    f.eval(r)
                        .expect("query_through can't currently return an error")
                        .is_truthy()
                };

                result.map(|rs| Box::new(rs.filter(move |r| filter(r))) as Box<_>)
            })
    }

    fn is_selective(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
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
                filters.unwrap_or_else(|| Expression::Op {
                    left: Box::new(Expression::Column(1)),
                    op: BinaryOperator::Equal,
                    right: Box::new(Expression::Literal("a".into())),
                }),
            ),
            materialized,
        );
        g
    }

    #[test]
    fn it_forwards_constant_expr() {
        let mut g = setup(false, Some(Expression::Literal(1.into())));

        let mut left: Vec<DataType>;

        left = vec![1.into(), "a".into()];
        assert_eq!(g.narrow_one_row(left.clone(), false), vec![left].into());

        left = vec![1.into(), "b".into()];
        assert_eq!(g.narrow_one_row(left.clone(), false), vec![left].into());

        left = vec![2.into(), "a".into()];
        assert_eq!(g.narrow_one_row(left.clone(), false), vec![left].into());
    }

    #[test]
    fn it_forwards() {
        let mut g = setup(false, None);

        let mut left: Vec<DataType>;

        left = vec![1.into(), "a".into()];
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
            Some(Expression::Op {
                left: Box::new(Expression::Op {
                    left: Box::new(Expression::Column(0)),
                    op: BinaryOperator::Equal,
                    right: Box::new(Expression::Literal(1.into())),
                }),
                op: BinaryOperator::And,
                right: Box::new(Expression::Op {
                    left: Box::new(Expression::Column(1)),
                    op: BinaryOperator::Equal,
                    right: Box::new(Expression::Literal("a".into())),
                }),
            }),
        );

        let mut left: Vec<DataType>;

        left = vec![1.into(), "a".into()];
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
            g.node().resolve(0).unwrap(),
            Some(vec![(g.narrow_base_id().as_global(), 0)])
        );
        assert_eq!(
            g.node().resolve(1).unwrap(),
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
            Some(Expression::Op {
                left: Box::new(Expression::Op {
                    left: Box::new(Expression::Column(0)),
                    op: BinaryOperator::LessOrEqual,
                    right: Box::new(Expression::Literal(2.into())),
                }),
                op: BinaryOperator::And,
                right: Box::new(Expression::Op {
                    left: Box::new(Expression::Column(1)),
                    op: BinaryOperator::NotEqual,
                    right: Box::new(Expression::Literal("a".into())),
                }),
            }),
        );

        let mut left: Vec<DataType>;

        // both conditions match (2 <= 2, "b" != "a")
        left = vec![2.into(), "b".into()];
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
            Some(Expression::Op {
                left: Box::new(Expression::Column(0)),
                op: BinaryOperator::Equal,
                right: Box::new(Expression::Column(1)),
            }),
        );

        let mut left: Vec<DataType>;
        left = vec![2.into(), 2.into()];
        assert_eq!(g.narrow_one_row(left.clone(), false), vec![left].into());
        left = vec![2.into(), "b".into()];
        assert_eq!(g.narrow_one_row(left, false), Records::default());
    }
}
