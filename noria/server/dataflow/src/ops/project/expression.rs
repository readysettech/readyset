use std::borrow::Cow;
use std::fmt;
use thiserror::Error;

use nom_sql::{ArithmeticOperator, SqlType};
use noria::{DataType, ValueCoerceError};

#[derive(Debug, Error)]
pub enum EvalError {
    /// An index in a [`Column`](ProjectExpression::Column) was out-of-bounds for the source record
    #[error("Column index out-of-bounds: {0}")]
    InvalidColumnIndex(usize),

    /// Error coercing a value, whether implicitly or as part of a [`Cast`](ProjectExpression::Cast)
    #[error(transparent)]
    CoerceError(#[from] ValueCoerceError),
}

/// Expression AST for projection
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ProjectExpression {
    /// A reference to a column, by index, in the parent node
    Column(usize),

    /// A literal DataType value
    Literal(DataType),

    /// A binary operation
    Op {
        op: ArithmeticOperator,
        left: Box<ProjectExpression>,
        right: Box<ProjectExpression>,
    },

    /// CAST(expr AS type)
    Cast(Box<ProjectExpression>, SqlType),
}

impl fmt::Display for ProjectExpression {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use ProjectExpression::*;

        match self {
            Column(u) => write!(f, "{}", u),
            Literal(l) => write!(f, "(lit: {})", l),
            Op { op, left, right } => write!(f, "({} {} {})", left, op, right),
            Cast(expr, ty) => write!(f, "cast({} as {})", expr, ty),
        }
    }
}

impl ProjectExpression {
    /// Evaluate a [`ProjectExpression`] given a source record to pull columns from
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
            Cast(expr, ty) => match expr.eval(record)? {
                Cow::Borrowed(val) => Ok(val.coerce_to(ty)?),
                Cow::Owned(val) => Ok(Cow::Owned(val.coerce_to(ty)?.into_owned())),
            },
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

    #[test]
    fn eval_cast() {
        let expr = Cast(Box::new(Column(0)), SqlType::Int(32));
        assert_eq!(
            expr.eval(&["1".into(), "2".into()]).unwrap(),
            Cow::Owned(1i32.into())
        );
    }
}
