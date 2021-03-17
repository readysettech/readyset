use std::fmt::{self, Display};

use crate::{ArithmeticExpression, FunctionExpression, Literal};

/// SQL Expression AST
///
/// NOTE: This type is here as the first step of a gradual refactor of the AST for this crate -
/// nothing (in this crate) currently *uses* this type as part of its AST, but in the future I'd
/// like to gradually refactor things like column defaults, select fields, conditions, etc. to use
/// this data type instead of defining their own ad-hoc enums.
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub enum Expression {
    /// Arithmetic expressions
    ///
    /// TODO(grfn): Eventually, the members of ArithmeticExpression should be inlined here
    Arithmetic(ArithmeticExpression),

    /// Function call expressions
    ///
    /// TODO(grfn): Eventually, the members of FunctionExpression should be inlined here
    Call(FunctionExpression),

    /// Literal values
    Literal(Literal),

    /// A reference to a column, optionally qualified by a table
    Column { name: String, table: Option<String> },
}

impl Display for Expression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Expression::Arithmetic(ae) => ae.fmt(f),
            Expression::Call(fe) => fe.fmt(f),
            Expression::Literal(l) => write!(f, "{}", l.to_string()),
            Expression::Column { name, table } => {
                if let Some(table) = table {
                    write!(f, "{}.", table)?;
                }
                name.fmt(f)
            }
        }
    }
}
