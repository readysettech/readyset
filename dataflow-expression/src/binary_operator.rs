use std::fmt;

use nom_sql::BinaryOperator as SqlBinaryOperator;
use readyset_data::dialect::SqlEngine;
use readyset_data::{DfType, Dialect};
use readyset_errors::{invalid_err, unsupported, ReadySetResult};
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

    /// `ILIKE`
    ILike,

    /// `=`
    Equal,

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

    /// PostgreSQL `#>` to extract JSON values as JSON via a key path:
    /// `json[b] #> text[]` to `json[b]`.
    ///
    /// Keys in the path are treated as indices when operating over JSON arrays.
    JsonKeyPathExtract,

    /// PostgreSQL `#>>` to extract JSON values as TEXT via a key path:
    /// `json[b] #>> text[]` to `text`.
    ///
    /// Keys in the path are treated as indices when operating over JSON arrays.
    JsonKeyPathExtractText,

    /// `@>`
    JsonContains,

    /// `<@`
    JsonContainedIn,

    /// `-` applied to the PostreSQL JSONB type
    JsonSubtract,

    /// PostgreSQL `#-` operator to remove from JSONB values via a key/index.
    JsonSubtractPath,
}

impl BinaryOperator {
    /// Convert a [`nom_sql::BinaryOperator`] to a pair of `BinaryOperator` and a boolean indicating
    /// whether the result should be negated, within the context of a SQL [`Dialect`].
    pub fn from_sql_op(
        op: SqlBinaryOperator,
        dialect: Dialect,
        left_type: &DfType,
        _right_type: &DfType,
    ) -> ReadySetResult<(Self, bool)> {
        use SqlBinaryOperator::*;
        match op {
            And => Ok((Self::And, false)),
            Or => Ok((Self::Or, false)),
            Greater => Ok((Self::Greater, false)),
            GreaterOrEqual => Ok((Self::GreaterOrEqual, false)),
            Less => Ok((Self::Less, false)),
            LessOrEqual => Ok((Self::LessOrEqual, false)),
            Add => Ok((Self::Add, false)),
            Subtract => {
                if left_type.is_jsonb() {
                    Ok((Self::JsonSubtract, false))
                } else {
                    Ok((Self::Subtract, false))
                }
            }
            HashSubtract => Ok((Self::JsonSubtractPath, false)),
            Multiply => Ok((Self::Multiply, false)),
            Divide => Ok((Self::Divide, false)),
            Like => Ok((Self::Like, false)),
            NotLike => Ok((Self::Like, true)),
            ILike => Ok((Self::ILike, false)),
            NotILike => Ok((Self::ILike, true)),
            Equal => Ok((Self::Equal, false)),
            NotEqual => Ok((Self::Equal, true)),
            Is => Ok((Self::Is, false)),
            IsNot => Ok((Self::Is, true)),
            QuestionMark => Ok((Self::JsonExists, false)),
            QuestionMarkPipe => Ok((Self::JsonAnyExists, false)),
            QuestionMarkAnd => Ok((Self::JsonAllExists, false)),
            // TODO When we want to implement the double pipe string concat operator, we'll need to
            // look at the types of the arguments to this operator to infer which `BinaryOperator`
            // variant to return. For now we just support the JSON `||` concat though:
            DoublePipe => {
                if dialect.double_pipe_is_concat() {
                    Ok((Self::JsonConcat, false))
                } else {
                    Ok((Self::Or, false))
                }
            }
            Arrow1 => match dialect.engine() {
                SqlEngine::MySQL => Ok((Self::JsonPathExtract, false)),
                SqlEngine::PostgreSQL => Ok((Self::JsonKeyExtract, false)),
            },
            Arrow2 => match dialect.engine() {
                SqlEngine::MySQL => Ok((Self::JsonPathExtractUnquote, false)),
                SqlEngine::PostgreSQL => Ok((Self::JsonKeyExtractText, false)),
            },
            HashArrow1 | HashArrow2 if dialect.engine() != SqlEngine::PostgreSQL => {
                unsupported!("''{op}' not available in {}'", dialect.engine())
            }
            HashArrow1 => Ok((Self::JsonKeyPathExtract, false)),
            HashArrow2 => Ok((Self::JsonKeyPathExtractText, false)),
            AtArrowRight => Ok((Self::JsonContains, false)),
            AtArrowLeft => Ok((Self::JsonContainedIn, false)),
        }
    }

    /// Given the types of the lhs and rhs expressions for this binary operator, if either side
    /// needs to be coerced before evaluation, returns the type that it should be coerced to
    pub(crate) fn argument_type_coercions(
        &self,
        left_type: &DfType,
        right_type: &DfType,
    ) -> ReadySetResult<(Option<DfType>, Option<DfType>)> {
        enum Side {
            Left,
            Right,
        }
        use Side::*;

        let error = |side: Side, expected_type: &str| {
            let (side_name, offending_type) = match side {
                Left => ("left", left_type),
                Right => ("right", right_type),
            };

            Err(invalid_err!(
                "cannot invoke '{self}' on {side_name}-side operand type {offending_type}; \
                expected {expected_type}"
            ))
        };

        let coerce_to_text_type = |ty: &DfType| {
            if ty.is_any_text() {
                None
            } else {
                Some(DfType::DEFAULT_TEXT)
            }
        };

        use BinaryOperator::*;
        match self {
            Add | Subtract | Multiply | Divide | And | Or | Greater | GreaterOrEqual | Less
            | LessOrEqual | Is => Ok((None, None)),

            Like | ILike => Ok((
                coerce_to_text_type(left_type),
                coerce_to_text_type(right_type),
            )),

            Equal => Ok((None, Some(left_type.clone()))),

            JsonExists => {
                if left_type.is_known() && !left_type.is_jsonb() {
                    return error(Left, "JSONB");
                }
                Ok((None, Some(DfType::DEFAULT_TEXT)))
            }
            JsonAnyExists
            | JsonAllExists
            | JsonKeyPathExtract
            | JsonKeyPathExtractText
            | JsonSubtractPath => {
                if left_type.is_known() && !left_type.is_any_json() {
                    return error(Left, "JSONB");
                }

                if right_type.innermost_array_type().is_known()
                    && !(right_type.is_array() && right_type.innermost_array_type().is_any_text())
                {
                    return error(Right, "TEXT[]");
                }
                Ok((
                    Some(DfType::DEFAULT_TEXT),
                    Some(DfType::Array(Box::new(DfType::DEFAULT_TEXT))),
                ))
            }
            JsonConcat | JsonContains | JsonContainedIn => {
                if left_type.is_known() && !left_type.is_jsonb() {
                    return error(Left, "JSONB");
                }

                if right_type.is_known() && !right_type.is_jsonb() {
                    return error(Right, "JSONB");
                }

                Ok((Some(DfType::DEFAULT_TEXT), Some(DfType::DEFAULT_TEXT)))
            }
            JsonKeyExtract | JsonKeyExtractText => Ok((Some(DfType::DEFAULT_TEXT), None)),

            JsonSubtract => {
                if left_type.is_known() && !left_type.is_jsonb() {
                    return error(Left, "JSONB");
                }

                Ok((None, None))
            }

            JsonPathExtract | JsonPathExtractUnquote => {
                unsupported!("'{self}' operator not implemented yet for MySQL")
            }
        }
    }

    /// Returns this operator's output type given its input types, or
    /// [`ReadySetError::InvalidQuery`](readyset_errors::ReadySetError::InvalidQuery) if it could
    /// not be inferred.
    pub(crate) fn output_type(
        &self,
        left_type: &DfType,
        _right_type: &DfType,
    ) -> ReadySetResult<DfType> {
        // TODO: Maybe consider `right_type` in some cases too.
        // TODO: What is the correct return type for `And` and `Or`?
        match self {
            Self::Like
            | Self::ILike
            | Self::Equal
            | Self::Greater
            | Self::GreaterOrEqual
            | Self::Less
            | Self::LessOrEqual
            | Self::Is
            | Self::JsonExists
            | Self::JsonAnyExists
            | Self::JsonAllExists
            | Self::JsonContains
            | Self::JsonContainedIn => Ok(DfType::Bool),

            Self::JsonPathExtractUnquote
            | Self::JsonKeyExtractText
            | Self::JsonKeyPathExtractText => Ok(DfType::DEFAULT_TEXT),

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
            Self::ILike => "ILIKE",
            Self::Equal => "=",
            Self::Greater => ">",
            Self::GreaterOrEqual => ">=",
            Self::Less => "<",
            Self::LessOrEqual => "<=",
            Self::Is => "IS",
            Self::Add => "+",
            Self::Subtract | Self::JsonSubtract => "-",
            Self::JsonSubtractPath => "#-",
            Self::Multiply => "*",
            Self::Divide => "/",
            Self::JsonExists => "?",
            Self::JsonAnyExists => "?|",
            Self::JsonAllExists => "?&",
            Self::JsonConcat => "||",
            Self::JsonPathExtract | Self::JsonKeyExtract => "->",
            Self::JsonPathExtractUnquote | Self::JsonKeyExtractText => "->>",
            Self::JsonKeyPathExtract => "#>",
            Self::JsonKeyPathExtractText => "#>>",
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
            (BinaryOperator::JsonSubtract, false)
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

        #[track_caller]
        fn test_json_key_path_extract(op: BinaryOperator, left_type: DfType, output_type: DfType) {
            assert_eq!(
                op.output_type(&left_type, &DfType::Array(Box::new(DfType::DEFAULT_TEXT)))
                    .unwrap(),
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

        #[test]
        fn json_key_path_extract() {
            test_json_key_path_extract(
                BinaryOperator::JsonKeyPathExtract,
                DfType::Json,
                DfType::Json,
            );
            test_json_key_path_extract(
                BinaryOperator::JsonKeyPathExtract,
                DfType::Jsonb,
                DfType::Jsonb,
            );
        }

        #[test]
        fn json_key_path_extract_text() {
            test_json_key_path_extract(
                BinaryOperator::JsonKeyPathExtractText,
                DfType::Json,
                DfType::DEFAULT_TEXT,
            );
            test_json_key_path_extract(
                BinaryOperator::JsonKeyPathExtractText,
                DfType::Jsonb,
                DfType::DEFAULT_TEXT,
            );
        }
    }
}
