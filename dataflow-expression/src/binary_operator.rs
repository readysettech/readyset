use std::fmt;

use nom_sql::BinaryOperator as SqlBinaryOperator;
use readyset_data::dialect::SqlEngine;
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

    /// (Unimplemented) [MySQL `->`](https://dev.mysql.com/doc/refman/5.7/en/json-search-functions.html#operator_json-column-path)
    /// operator to extract JSON values via a path: `json -> jsonpath` to `json`.
    // TODO(ENG-1517)
    JsonPathExtract,

    /// (Unimplemented) [MySQL `->>`](https://dev.mysql.com/doc/refman/5.7/en/json-search-functions.html#operator_json-inline-path)
    /// operator to extract JSON values and apply [`json_unquote`](https://dev.mysql.com/doc/refman/5.7/en/json-modification-functions.html#function_json-unquote):
    /// `json ->> jsonpath` to unquoted `text`.
    // TODO(ENG-1518)
    JsonPathExtractUnquote,

    /// PostgreSQL `->` operator to extract JSON values as JSON via a key:
    /// `json[b] -> {text,integer}` to `json[b]`.
    JsonKeyExtract,

    /// PostgreSQL `->>` operator to extract JSON values as text via a key:
    /// `json[b] ->> {text,integer}` to `text`.
    JsonKeyExtractText,

    /// `@>`
    JsonContains,

    /// `<@`
    JsonContainedIn,

    /// `-` applied to the PostreSQL JSONB type
    JsonSubtract,
}

impl BinaryOperator {
    /// Converts from a [`nom_sql::BinaryOperator`] within the context of a SQL [`Dialect`].
    pub fn from_sql_op(
        op: SqlBinaryOperator,
        dialect: Dialect,
        left_type: &DfType,
        _right_type: &DfType,
    ) -> ReadySetResult<Self> {
        use SqlBinaryOperator::*;
        let res = match op {
            And => Self::And,
            Or => Self::Or,
            Greater => Self::Greater,
            GreaterOrEqual => Self::GreaterOrEqual,
            Less => Self::Less,
            LessOrEqual => Self::LessOrEqual,
            Add => Self::Add,
            Subtract => {
                if left_type.is_jsonb() {
                    Self::JsonSubtract
                } else {
                    Self::Subtract
                }
            }
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
            // TODO When we want to implement the double pipe string concat operator, we'll need to
            // look at the types of the arguments to this operator to infer which `BinaryOperator`
            // variant to return. For now we just support the JSON `||` concat though:
            DoublePipe => {
                if dialect.double_pipe_is_concat() {
                    Self::JsonConcat
                } else {
                    Self::Or
                }
            }
            Arrow1 => match dialect.engine() {
                SqlEngine::MySQL => Self::JsonPathExtract,
                SqlEngine::PostgreSQL => Self::JsonKeyExtract,
            },
            Arrow2 => match dialect.engine() {
                SqlEngine::MySQL => Self::JsonPathExtractUnquote,
                SqlEngine::PostgreSQL => Self::JsonKeyExtractText,
            },
            AtArrowRight => Self::JsonContains,
            AtArrowLeft => Self::JsonContainedIn,
        };
        Ok(res)
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
                if left_type.is_known() && !left_type.is_jsonb() =>
            {
                error(Left, "JSONB")
            }

            // json, jsonb, unknown
            Self::JsonPathExtract
            | Self::JsonKeyExtract
            | Self::JsonPathExtractUnquote
            | Self::JsonKeyExtractText
                if left_type.is_known() && !left_type.is_any_json() =>
            {
                error(Left, "JSON")
            }

            // Right type checks:

            // text, char, varchar, unknown
            Self::JsonExists if right_type.is_known() && !right_type.is_any_text() => {
                error(Right, "TEXT")
            }

            // text[], char[], varchar[], unknown, unknown[]
            Self::JsonAnyExists | Self::JsonAllExists
                if right_type.innermost_array_type().is_known()
                    && !(right_type.is_array()
                        && right_type.innermost_array_type().is_any_text()) =>
            {
                error(Right, "TEXT[]")
            }

            // text, char, varchar, ints, unknown
            Self::JsonKeyExtract | Self::JsonKeyExtractText
                if right_type.is_known()
                    && !(right_type.is_any_text() || right_type.is_any_int()) =>
            {
                error(Right, "TEXT or INT")
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
            | Self::JsonAllExists
            | Self::JsonContains
            | Self::JsonContainedIn => Ok(DfType::Bool),

            Self::JsonPathExtractUnquote | Self::JsonKeyExtractText => Ok(DfType::DEFAULT_TEXT),

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
            Self::Subtract | Self::JsonSubtract => "-",
            Self::Multiply => "*",
            Self::Divide => "/",
            Self::JsonExists => "?",
            Self::JsonAnyExists => "?|",
            Self::JsonAllExists => "?&",
            Self::JsonConcat => "||",
            Self::JsonPathExtract | Self::JsonKeyExtract => "->",
            Self::JsonPathExtractUnquote | Self::JsonKeyExtractText => "->>",
            Self::JsonContains => "@>",
            Self::JsonContainedIn => "<@",
        };
        f.write_str(op)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_subtract_lowering() {
        assert_eq!(
            BinaryOperator::from_sql_op(
                SqlBinaryOperator::Subtract,
                Dialect::DEFAULT_POSTGRESQL,
                &DfType::Jsonb,
                &DfType::Int
            )
            .unwrap(),
            BinaryOperator::JsonSubtract
        );
    }

    mod output_type {
        use super::*;

        #[track_caller]
        fn test_json_extract(op: BinaryOperator, left_type: DfType, output_type: DfType) {
            assert_eq!(
                op.output_type(&left_type, &DfType::DEFAULT_TEXT).unwrap(),
                output_type
            );
        }

        #[test]
        fn json_path_extract() {
            test_json_extract(BinaryOperator::JsonPathExtract, DfType::Json, DfType::Json);
        }

        #[test]
        fn json_path_extract_unquote() {
            test_json_extract(
                BinaryOperator::JsonPathExtractUnquote,
                DfType::Json,
                DfType::DEFAULT_TEXT,
            );
        }

        #[test]
        fn json_key_extract() {
            test_json_extract(BinaryOperator::JsonKeyExtract, DfType::Json, DfType::Json);
            test_json_extract(BinaryOperator::JsonKeyExtract, DfType::Jsonb, DfType::Jsonb);
        }

        #[test]
        fn json_key_extract_text() {
            test_json_extract(
                BinaryOperator::JsonKeyExtractText,
                DfType::Json,
                DfType::DEFAULT_TEXT,
            );
            test_json_extract(
                BinaryOperator::JsonKeyExtractText,
                DfType::Jsonb,
                DfType::DEFAULT_TEXT,
            );
        }
    }
}
