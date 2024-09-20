use std::fmt;

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

    /// `%`
    Modulo,

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
            Self::Modulo => "%",
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
