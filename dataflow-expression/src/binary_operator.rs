use std::fmt;

use nom_sql::BinaryOperator as SqlBinaryOperator;
use readyset_data::{DfType, Dialect};
use readyset_errors::{invalid_err, ReadySetResult};
use serde::{Deserialize, Serialize};

/// Binary infix operators with [`Expr`](crate::Expr) on both the left- and right-hand sides
///
/// This type is used as the operator in [`Expr::BinaryOp`](crate::Expr::BinaryOp).
///
/// Note that because all binary operators have expressions on both sides, SQL `IN` is not a binary
/// operator - since it must have either a subquery or a list of expressions on its right-hand side
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum BinaryOperator {
    /// `AND`
    And,

    /// `OR`
    Or,

    /// `LIKE`
    Like,

    /// `NOT LIKE`
    NotLike,

    /// `ILIKE`
    ILike,

    /// `NOT ILIKE`
    NotILike,

    /// `=`
    Equal,

    /// `!=` or `<>`
    NotEqual,

    /// `>`
    Greater,

    /// `>=`
    GreaterOrEqual,

    /// `<`
    Less,

    /// `<=`
    LessOrEqual,

    /// `IS`
    Is,

    /// `IS NOT`
    IsNot,

    /// `+`
    Add,

    /// `-`
    Subtract,

    /// `*`
    Multiply,

    /// `/`
    Divide,

    /// `?`
    JsonExists,

    /// `?|`
    JsonAnyExists,

    /// `?&`
    JsonAllExists,

    /// `||`
    JsonConcat,
}

impl BinaryOperator {
    /// Converts from a [`nom_sql::BinaryOperator`] within the context of a SQL [`Dialect`].
    pub fn from_sql_op(op: SqlBinaryOperator, _dialect: Dialect) -> Self {
        // TODO: Use dialect for future operators, such as JSON extraction arrows.
        use SqlBinaryOperator::*;
        match op {
            And => Self::And,
            Or => Self::Or,
            Greater => Self::Greater,
            GreaterOrEqual => Self::GreaterOrEqual,
            Less => Self::Less,
            LessOrEqual => Self::LessOrEqual,
            Add => Self::Add,
            Subtract => Self::Subtract,
            Multiply => Self::Multiply,
            Divide => Self::Divide,
            Like => Self::Like,
            NotLike => Self::NotLike,
            ILike => Self::ILike,
            NotILike => Self::NotILike,
            Equal => Self::Equal,
            NotEqual => Self::NotEqual,
            Is => Self::Is,
            IsNot => Self::IsNot,
            QuestionMark => Self::JsonExists,
            QuestionMarkPipe => Self::JsonAnyExists,
            QuestionMarkAnd => Self::JsonAllExists,
            DoublePipe => Self::JsonConcat, // TODO handle other || operators
        }
    }

    /// Checks this operator's input types, returning
    /// [`ReadySetError::InvalidQuery`](readyset_errors::ReadySetError::InvalidQuery) if either is
    /// unexpected.
    fn check_arg_types(&self, left_type: &DfType, right_type: &DfType) -> ReadySetResult<()> {
        enum Side {
            Left,
            Right,
        }
        use Side::*;

        let error = |side: Side, expected_type: &str| -> ReadySetResult<()> {
            let (side_name, offending_type) = match side {
                Left => ("left", left_type),
                Right => ("right", right_type),
            };

            Err(invalid_err!(
                "cannot invoke '{self}' on {side_name}-side operand type {offending_type}; \
                expected {expected_type}"
            ))
        };

        // TODO: Type-check more operators.
        // TODO: Proper type unification instead of blindly allowing `Unknown`.
        match self {
            // Left type checks:

            // jsonb, unknown
            Self::JsonExists | Self::JsonAnyExists | Self::JsonAllExists
                if !(left_type.is_jsonb() || left_type.is_unknown()) =>
            {
                error(Left, "JSONB")
            }

            // Right type checks:

            // text, char, varchar, unknown
            Self::JsonExists if !(right_type.is_any_text() || right_type.is_unknown()) => {
                error(Right, "TEXT")
            }

            // text[], char[], varchar[], unknown, unknown[]
            Self::JsonAnyExists | Self::JsonAllExists
                if !((right_type.is_array()
                    && right_type.innermost_array_type().is_any_text())
                    || right_type.innermost_array_type().is_unknown()) =>
            {
                error(Right, "TEXT[]")
            }

            _ => Ok(()),
        }
    }

    /// Returns this operator's output type given its input types, or
    /// [`ReadySetError::InvalidQuery`](readyset_errors::ReadySetError::InvalidQuery) if it could
    /// not be inferred.
    pub(crate) fn output_type(
        &self,
        left_type: &DfType,
        right_type: &DfType,
    ) -> ReadySetResult<DfType> {
        self.check_arg_types(left_type, right_type)?;

        // TODO: Maybe consider `right_type` in some cases too.
        // TODO: What is the correct return type for `And` and `Or`?
        match self {
            Self::Like
            | Self::NotLike
            | Self::ILike
            | Self::NotILike
            | Self::Equal
            | Self::NotEqual
            | Self::Greater
            | Self::GreaterOrEqual
            | Self::Less
            | Self::LessOrEqual
            | Self::Is
            | Self::IsNot
            | Self::JsonExists
            | Self::JsonAnyExists
            | Self::JsonAllExists => Ok(DfType::Bool),

            _ => Ok(left_type.clone()),
        }
    }
}

impl fmt::Display for BinaryOperator {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let op = match *self {
            Self::And => "AND",
            Self::Or => "OR",
            Self::Like => "LIKE",
            Self::NotLike => "NOT LIKE",
            Self::ILike => "ILIKE",
            Self::NotILike => "NOT ILIKE",
            Self::Equal => "=",
            Self::NotEqual => "!=",
            Self::Greater => ">",
            Self::GreaterOrEqual => ">=",
            Self::Less => "<",
            Self::LessOrEqual => "<=",
            Self::Is => "IS",
            Self::IsNot => "IS NOT",
            Self::Add => "+",
            Self::Subtract => "-",
            Self::Multiply => "*",
            Self::Divide => "/",
            Self::JsonExists => "?",
            Self::JsonAnyExists => "?|",
            Self::JsonAllExists => "?&",
            Self::JsonConcat => "||",
        };
        f.write_str(op)
    }
}
