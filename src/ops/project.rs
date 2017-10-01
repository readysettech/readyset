use nom_sql::ArithmeticOperator;
use std::fmt;
use std::collections::HashMap;

use flow::prelude::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ProjectExpressionBase {
    Column(usize),
    Literal(DataType),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectExpression {
    op: ArithmeticOperator,
    left: ProjectExpressionBase,
    right: ProjectExpressionBase,
}

impl ProjectExpression {
    pub fn new(
        op: ArithmeticOperator,
        left: ProjectExpressionBase,
        right: ProjectExpressionBase,
    ) -> ProjectExpression {
        ProjectExpression {
            op: op,
            left: left,
            right: right,
        }
    }
}

impl fmt::Display for ProjectExpressionBase {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            ProjectExpressionBase::Column(u) => write!(f, "{}", u),
            ProjectExpressionBase::Literal(ref l) => write!(f, "(lit: {})", l),
        }
    }
}

impl fmt::Display for ProjectExpression {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let op = match self.op {
            ArithmeticOperator::Add => "+",
            ArithmeticOperator::Subtract => "-",
            ArithmeticOperator::Divide => "/",
            ArithmeticOperator::Multiply => "*",
        };

        write!(f, "{} {} {}", self.left, op, self.right)
    }
}


/// Permutes or omits columns from its source node, or adds additional literal value columns.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Project {
    us: Option<IndexPair>,
    emit: Option<Vec<usize>>,
    additional: Option<Vec<DataType>>,
    expressions: Option<Vec<ProjectExpression>>,
    src: IndexPair,
    cols: usize,
}

impl Project {
    /// Construct a new permuter operator.
    pub fn new(
        src: NodeIndex,
        emit: &[usize],
        additional: Option<Vec<DataType>>,
        expressions: Option<Vec<ProjectExpression>>,
    ) -> Project {
        Project {
            emit: Some(emit.into()),
            additional: additional,
            expressions: expressions,
            src: src.into(),
            cols: 0,
            us: None,
        }
    }

    fn resolve_col(&self, col: usize) -> usize {
        if self.emit.is_some() && col >= self.emit.as_ref().unwrap().len() {
            panic!(
                "can't resolve literal column {} that doesn't come from parent node!",
                col
            );
        } else {
            self.emit.as_ref().map_or(col, |emit| emit[col])
        }
    }

    fn eval_expression(&self, expression: &ProjectExpression, record: &Record) -> DataType {
        let left = match expression.left {
            ProjectExpressionBase::Column(i) => &record[i],
            ProjectExpressionBase::Literal(ref data) => data,
        }.clone();

        let right = match expression.right {
            ProjectExpressionBase::Column(i) => &record[i],
            ProjectExpressionBase::Literal(ref data) => data,
        }.clone();

        match expression.op {
            ArithmeticOperator::Add => left + right,
            ArithmeticOperator::Subtract => left - right,
            ArithmeticOperator::Multiply => left * right,
            ArithmeticOperator::Divide => left / right,
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

    fn on_connected(&mut self, g: &Graph) {
        self.cols = g[self.src.as_global()].fields().len();
    }

    fn on_commit(&mut self, us: NodeIndex, remap: &HashMap<NodeIndex, IndexPair>) {
        self.src.remap(remap);
        self.us = Some(remap[&us]);

        // Eliminate emit specifications which require no permutation of
        // the inputs, so we don't needlessly perform extra work on each
        // update.
        self.emit = self.emit.take().and_then(|emit| {
            let complete = emit.len() == self.cols && self.additional.is_none();
            let sequential = emit.iter().enumerate().all(|(i, &j)| i == j);
            if complete && sequential {
                None
            } else {
                Some(emit)
            }
        });
    }

    fn on_input(
        &mut self,
        from: LocalNodeIndex,
        mut rs: Records,
        _: &mut Tracer,
        _: Option<usize>,
        _: &DomainNodes,
        _: &StateMap,
    ) -> ProcessingResult {
        debug_assert_eq!(from, *self.src);

        if self.emit.is_some() {
            for r in &mut *rs {
                if self.emit.is_none() {
                    continue;
                }

                let mut new_r = Vec::with_capacity(r.len());
                let e = self.emit.as_ref().unwrap();
                for i in e {
                    new_r.push(r[*i].clone());
                }
                match self.expressions {
                    Some(ref e) => {
                        for i in e {
                            new_r.push(self.eval_expression(i, r));
                        }
                    }
                    None => (),
                }
                match self.additional {
                    Some(ref a) => {
                        for i in a {
                            new_r.push(i.clone());
                        }
                    }
                    None => (),
                }
                **r = new_r;
            }
        }
        ProcessingResult {
            results: rs,
            misses: Vec::new(),
        }
    }

    fn suggest_indexes(&self, _: NodeIndex) -> HashMap<NodeIndex, (Vec<usize>, bool)> {
        HashMap::new()
    }

    fn resolve(&self, col: usize) -> Option<Vec<(NodeIndex, usize)>> {
        Some(vec![(self.src.as_global(), self.resolve_col(col))])
    }

    fn description(&self) -> String {
        let mut emit_cols = vec![];
        match self.emit.as_ref() {
            None => emit_cols.push("*".to_string()),
            Some(emit) => {
                emit_cols.extend(emit.iter().map(|e| e.to_string()).collect::<Vec<_>>());

                if let Some(ref arithmetic) = self.expressions {
                    emit_cols.extend(
                        arithmetic
                            .iter()
                            .map(|e| format!("{}", e))
                            .collect::<Vec<_>>(),
                    );
                }

                if let Some(ref add) = self.additional {
                    emit_cols.extend(
                        add.iter()
                            .map(|e| format!("lit: {}", e.to_string()))
                            .collect::<Vec<_>>(),
                    );
                }
            }
        };
        format!("π[{}]", emit_cols.join(", "))
    }

    fn parent_columns(&self, column: usize) -> Vec<(NodeIndex, Option<usize>)> {
        let result = if self.emit.is_some() && column >= self.emit.as_ref().unwrap().len() {
            None
        } else {
            Some(self.resolve_col(column))
        };
        vec![(self.src.as_global(), result)]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use ops;

    fn setup(materialized: bool, all: bool, add: bool) -> ops::test::MockGraph {
        let mut g = ops::test::MockGraph::new();
        let s = g.add_base("source", &["x", "y", "z"]);

        let permutation = if all { vec![0, 1, 2] } else { vec![2, 0] };
        let additional = if add {
            Some(vec![DataType::from("hello"), DataType::Int(42)])
        } else {
            None
        };
        g.set_op(
            "permute",
            &["x", "y", "z"],
            Project::new(s.as_global(), &permutation[..], additional, None),
            materialized,
        );
        g
    }

    fn setup_arithmetic(expression: ProjectExpression) -> ops::test::MockGraph {
        let mut g = ops::test::MockGraph::new();
        let s = g.add_base("source", &["x", "y", "z"]);

        let permutation = vec![0, 1];
        g.set_op(
            "permute",
            &["x", "y", "z"],
            Project::new(
                s.as_global(),
                &permutation[..],
                None,
                Some(vec![expression]),
            ),
            false,
        );
        g
    }

    fn setup_column_arithmetic(op: ArithmeticOperator) -> ops::test::MockGraph {
        let expression = ProjectExpression {
            left: ProjectExpressionBase::Column(0),
            right: ProjectExpressionBase::Column(1),
            op: op,
        };

        setup_arithmetic(expression)
    }

    #[test]
    fn it_describes() {
        let p = setup(false, false, true);
        assert_eq!(p.node().description(), "π[2, 0, lit: \"hello\", lit: 42]");
    }

    #[test]
    fn it_describes_arithmetic() {
        let mut p = setup_column_arithmetic(ArithmeticOperator::Add);
        assert_eq!(p.node().description(), "π[0, 1, 0 + 1]");
    }

    #[test]
    fn it_describes_all() {
        let p = setup(false, true, false);
        assert_eq!(p.node().description(), "π[*]");
    }

    #[test]
    fn it_describes_all_w_literals() {
        let p = setup(false, true, true);
        assert_eq!(
            p.node().description(),
            "π[0, 1, 2, lit: \"hello\", lit: 42]"
        );
    }

    #[test]
    fn it_forwards_some() {
        let mut p = setup(false, false, true);

        let rec = vec!["a".into(), "b".into(), "c".into()];
        assert_eq!(
            p.narrow_one_row(rec, false),
            vec![vec!["c".into(), "a".into(), "hello".into(), 42.into()]].into()
        );
    }

    #[test]
    fn it_forwards_all() {
        let mut p = setup(false, true, false);

        let rec = vec!["a".into(), "b".into(), "c".into()];
        assert_eq!(
            p.narrow_one_row(rec, false),
            vec![vec!["a".into(), "b".into(), "c".into()]].into()
        );
    }

    #[test]
    fn it_forwards_all_w_literals() {
        let mut p = setup(false, true, true);

        let rec = vec!["a".into(), "b".into(), "c".into()];
        assert_eq!(
            p.narrow_one_row(rec, false),
            vec![
                vec![
                    "a".into(),
                    "b".into(),
                    "c".into(),
                    "hello".into(),
                    42.into(),
                ],
            ].into()
        );
    }

    #[test]
    fn it_forwards_addition_arithmetic() {
        let mut p = setup_column_arithmetic(ArithmeticOperator::Add);
        let rec = vec![10.into(), 20.into()];
        assert_eq!(
            p.narrow_one_row(rec, false),
            vec![vec![10.into(), 20.into(), 30.into()]].into()
        );
    }

    #[test]
    fn it_forwards_subtraction_arithmetic() {
        let mut p = setup_column_arithmetic(ArithmeticOperator::Subtract);
        let rec = vec![10.into(), 20.into()];
        assert_eq!(
            p.narrow_one_row(rec, false),
            vec![vec![10.into(), 20.into(), (-10).into()]].into()
        );
    }

    #[test]
    fn it_forwards_multiplication_arithmetic() {
        let mut p = setup_column_arithmetic(ArithmeticOperator::Multiply);
        let rec = vec![10.into(), 20.into()];
        assert_eq!(
            p.narrow_one_row(rec, false),
            vec![vec![10.into(), 20.into(), 200.into()]].into()
        );
    }

    #[test]
    fn it_forwards_division_arithmetic() {
        let mut p = setup_column_arithmetic(ArithmeticOperator::Divide);
        let rec = vec![10.into(), 2.into()];
        assert_eq!(
            p.narrow_one_row(rec, false),
            vec![vec![10.into(), 2.into(), 5.into()]].into()
        );
    }

    #[test]
    fn it_forwards_arithmetic_w_literals() {
        let number: DataType = 40.into();
        let expression = ProjectExpression {
            left: ProjectExpressionBase::Column(0),
            right: ProjectExpressionBase::Literal(number),
            op: ArithmeticOperator::Multiply,
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
        let a: DataType = 80.into();
        let b: DataType = 40.into();
        let expression = ProjectExpression {
            left: ProjectExpressionBase::Literal(a),
            right: ProjectExpressionBase::Literal(b),
            op: ArithmeticOperator::Divide,
        };

        let mut p = setup_arithmetic(expression);
        let rec = vec![0.into(), 0.into()];
        assert_eq!(
            p.narrow_one_row(rec, false),
            vec![vec![0.into(), 0.into(), 2.into()]].into()
        );
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
    #[should_panic(expected = "can't resolve literal column")]
    fn it_fails_to_resolve_literal() {
        let p = setup(false, false, true);
        p.node().resolve(2);
    }
}
