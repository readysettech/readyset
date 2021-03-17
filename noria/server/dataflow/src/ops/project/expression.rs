use std::borrow::Cow;
use std::fmt;
use thiserror::Error;

use nom_sql::ArithmeticOperator;
use noria::DataType;

#[derive(Debug, Error)]
pub enum EvalError {
    #[error("Column index out-of-bounds: {0}")]
    InvalidColumnIndex(usize),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ProjectExpression {
    Column(usize),
    Literal(DataType),
    Op {
        op: ArithmeticOperator,
        left: Box<ProjectExpression>,
        right: Box<ProjectExpression>,
    },
}

impl fmt::Display for ProjectExpression {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use ProjectExpression::*;

        match self {
            Column(u) => write!(f, "{}", u),
            Literal(l) => write!(f, "(lit: {})", l),
            Op { op, left, right } => write!(f, "({} {} {})", left, op, right),
        }
    }
}

impl ProjectExpression {
    /// Evaluate a [`ProjectExpression`] given a source record
    pub fn eval<'a>(&self, record: &'a [DataType]) -> Result<Cow<'a, DataType>, EvalError> {
        use ProjectExpression::*;

        match self {
            Column(c) => record
                .get(*c)
                .map(Cow::Borrowed)
                .ok_or(EvalError::InvalidColumnIndex(*c)),
            Literal(dt) => Ok(Cow::Owned(dt.clone())),
            Op { op, left, right } => {
                let left = left.eval(record)?;
                let right = right.eval(record)?;
                match op {
                    ArithmeticOperator::Add => Ok(Cow::Owned(left.as_ref() + right.as_ref())),
                    ArithmeticOperator::Subtract => Ok(Cow::Owned(left.as_ref() - right.as_ref())),
                    ArithmeticOperator::Multiply => Ok(Cow::Owned(left.as_ref() * right.as_ref())),
                    ArithmeticOperator::Divide => Ok(Cow::Owned(left.as_ref() / right.as_ref())),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ProjectExpression::*;

    #[test]
    fn eval_column() {
        let expr = Column(1);
        assert_eq!(
            expr.eval(&[1.into(), "two".into()]).unwrap(),
            Cow::Owned("two".into())
        )
    }

    #[test]
    fn eval_literal() {
        let expr = Literal(1.into());
        assert_eq!(
            expr.eval(&[1.into(), "two".into()]).unwrap(),
            Cow::Owned(1.into())
        )
    }

    #[test]
    fn eval_add() {
        let expr = Op {
            left: Box::new(Column(0)),
            right: Box::new(Op {
                left: Box::new(Column(1)),
                right: Box::new(Literal(3.into())),
                op: ArithmeticOperator::Add,
            }),
            op: ArithmeticOperator::Add,
        };
        assert_eq!(
            expr.eval(&[1.into(), 2.into()]).unwrap(),
            Cow::Owned(6.into())
        );
    }
}
