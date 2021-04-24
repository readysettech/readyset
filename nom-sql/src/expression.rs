use derive_more::From;
use itertools::{Either, Itertools};
use nom::{alt, named};
use std::fmt::{self, Display};
use std::iter;

use crate::arithmetic::arithmetic;
use crate::case::case_when;
use crate::common::{column_function, column_identifier_no_alias, literal};
use crate::{Arithmetic, Column, ConditionExpression, Literal, SqlType};

/// Function call expressions
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum FunctionExpression {
    /// `AVG` aggregation. The boolean argument is `true` if `DISTINCT`
    Avg {
        expr: Box<Expression>,
        distinct: bool,
    },

    /// `COUNT` aggregation
    Count {
        expr: Box<Expression>,
        distinct: bool,
    },

    /// `COUNT(*)` aggregation
    CountStar,

    /// `SUM` aggregation
    Sum {
        expr: Box<Expression>,
        distinct: bool,
    },

    /// `MAX` aggregation
    Max(Box<Expression>),

    /// `MIN` aggregation
    Min(Box<Expression>),

    /// `GROUP_CONCAT` aggregation. The second argument is the separator
    GroupConcat {
        expr: Box<Expression>,
        separator: String,
    },

    /// `CAST(expression AS type)`.
    ///
    /// TODO(grfn): This isn't really a function call, so should really just be a member of
    /// Expression
    Cast(Box<Expression>, SqlType),

    /// Generic function call expression
    Call {
        name: String,
        arguments: Vec<Expression>,
    },
}

impl FunctionExpression {
    /// Returns an iterator over all the direct arguments passed to the given function call expression
    pub fn arguments(&self) -> impl Iterator<Item = &Expression> {
        match self {
            FunctionExpression::Avg { expr: arg, .. }
            | FunctionExpression::Count { expr: arg, .. }
            | FunctionExpression::Sum { expr: arg, .. }
            | FunctionExpression::Max(arg)
            | FunctionExpression::Min(arg)
            | FunctionExpression::GroupConcat { expr: arg, .. }
            | FunctionExpression::Cast(arg, _) => Either::Left(iter::once(arg.as_ref())),
            FunctionExpression::CountStar => Either::Right(Either::Left(iter::empty())),
            FunctionExpression::Call { arguments, .. } => {
                Either::Right(Either::Right(arguments.iter()))
            }
        }
    }
}

impl Display for FunctionExpression {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            FunctionExpression::Avg {
                expr,
                distinct: true,
            } => write!(f, "avg(distinct {})", expr),
            FunctionExpression::Count {
                expr,
                distinct: true,
            } => write!(f, "count(distinct {})", expr),
            FunctionExpression::Sum {
                expr,
                distinct: true,
            } => write!(f, "sum(distinct {})", expr),
            FunctionExpression::Avg { expr, .. } => write!(f, "avg({})", expr),
            FunctionExpression::Count { expr, .. } => write!(f, "count({})", expr),
            FunctionExpression::CountStar => write!(f, "count(*)"),
            FunctionExpression::Sum { expr, .. } => write!(f, "sum({})", expr),
            FunctionExpression::Max(col) => write!(f, "max({})", col),
            FunctionExpression::Min(col) => write!(f, "min({})", col),
            FunctionExpression::GroupConcat { expr, separator } => {
                write!(f, "group_concat({} separator '{}')", expr, separator)
            }
            FunctionExpression::Cast(arg, typ) => write!(f, "CAST({} AS {})", arg, typ),
            FunctionExpression::Call { name, arguments } => {
                write!(f, "{}({})", name, arguments.iter().join(", "))
            }
        }
    }
}

/// SQL Expression AST
///
/// NOTE: This type is here as the first step of a gradual refactor of the AST for this crate - not
/// as much (in this crate) currently *uses* this type as part of its AST as it should, but in the
/// future I'd like to gradually refactor things like column defaults, select fields, conditions,
/// etc. to use this data type instead of defining their own ad-hoc enums.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize, From)]
pub enum Expression {
    /// Arithmetic expressions
    ///
    /// TODO(grfn): Eventually, the members of ArithmeticExpression should be inlined here
    Arithmetic(Arithmetic),

    /// Function call expressions
    ///
    /// TODO(grfn): Eventually, the members of FunctionExpression should be inlined here
    Call(FunctionExpression),

    /// Literal values
    Literal(Literal),

    /// CASE WHEN condition THEN then_expr ELSE else_expr
    CaseWhen {
        condition: ConditionExpression,
        then_expr: Box<Expression>,
        else_expr: Option<Box<Expression>>,
    },

    /// A reference to a column
    ///
    /// TODO(grfn): Inline Column here once we get a chance to get rid of the `alias` attribute on
    /// Column. Until then, an invariant is that `function = None` for all columns in this enum
    Column(Column),
}

impl Display for Expression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Expression::Arithmetic(ae) => ae.fmt(f),
            Expression::Call(fe) => fe.fmt(f),
            Expression::Literal(l) => write!(f, "{}", l.to_string()),
            Expression::Column(col) => col.fmt(f),
            Expression::CaseWhen {
                condition,
                then_expr,
                else_expr,
            } => {
                write!(f, "CASE WHEN {} THEN {}", condition, then_expr)?;
                if let Some(else_expr) = else_expr {
                    write!(f, " ELSE {}", else_expr)?;
                }
                write!(f, " END")
            }
        }
    }
}

named!(pub(crate) expression(&[u8]) -> Expression, alt!(
    arithmetic => { |a| Expression::Arithmetic(a) } |
    column_function => { |f| Expression::Call(f) } |
    literal => { |l| Expression::Literal(l) } |
    case_when |
    column_identifier_no_alias => { |c| Expression::Column(c) }
));
