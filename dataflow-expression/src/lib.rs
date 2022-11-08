#![feature(box_patterns, let_else)]

mod eval;
pub mod like;
mod lower;
mod post_lookup;
pub mod utils;

use std::fmt;
use std::fmt::Formatter;

use itertools::Itertools;
use nom_sql::{BinaryOperator as SqlBinaryOperator, SqlType};
pub use readyset_data::Dialect;
use readyset_data::{DfType, DfValue};
use readyset_errors::{invalid_err, ReadySetResult};
use serde::{Deserialize, Serialize};
use vec1::Vec1;

pub use crate::lower::LowerContext;
pub use crate::post_lookup::{
    PostLookup, PostLookupAggregate, PostLookupAggregateFunction, PostLookupAggregates,
    PreInsertion, ReaderProcessing,
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum BuiltinFunction {
    /// [`convert_tz`](https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_convert-tz)
    ConvertTZ {
        args: [Expr; 3],

        /// Precision for coercing input to [`DfType::Timestamp`].
        subsecond_digits: u16,
    },
    /// [`dayofweek`](https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_dayofweek)
    DayOfWeek(Expr),
    /// [`ifnull`](https://dev.mysql.com/doc/refman/8.0/en/flow-control-functions.html#function_ifnull)
    IfNull(Expr, Expr),
    /// [`month`](https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_month)
    Month(Expr),
    /// [`timediff`](https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_timediff)
    Timediff(Expr, Expr),
    /// [`addtime`](https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_addtime)
    Addtime(Expr, Expr),
    /// [`round`](https://dev.mysql.com/doc/refman/8.0/en/mathematical-functions.html#function_round)
    Round(Expr, Expr),
    /// [`json_typeof`](https://www.postgresql.org/docs/current/functions-json.html)
    JsonTypeof(Expr),
    /// [`jsonb_typeof`](https://www.postgresql.org/docs/current/functions-json.html)
    JsonbTypeof(Expr),
    /// [`coalesce`](https://www.postgresql.org/docs/current/functions-conditional.html#FUNCTIONS-COALESCE-NVL-IFNULL)
    Coalesce(Expr, Vec<Expr>),
    /// [`concat`](https://dev.mysql.com/doc/refman/8.0/en/string-functions.html#function_concat)
    Concat(Expr, Vec<Expr>),

    /// `substring`:
    ///
    /// * [MySQL](https://dev.mysql.com/doc/refman/8.0/en/string-functions.html#function_substring)
    /// * [Postgres](https://www.postgresql.org/docs/9.1/functions-string.html)
    Substring(Expr, Option<Expr>, Option<Expr>),

    /// [`split_part`](https://www.postgresql.org/docs/current/functions-string.html)
    SplitPart(Expr, Expr, Expr),

    /// `greatest`:
    ///
    /// * [MySQL](https://dev.mysql.com/doc/refman/8.0/en/comparison-operators.html#function_greatest)
    /// * [PostgreSQL](https://www.postgresql.org/docs/current/functions-conditional.html#FUNCTIONS-GREATEST-LEAST)
    Greatest {
        args: Vec1<Expr>,
        /// Which type to coerce the arguments to *for comparison*. This might be distinct from the
        /// actual return type of the function call.
        compare_as: DfType,
    },

    /// `least`:
    ///
    /// * [MySQL](https://dev.mysql.com/doc/refman/8.0/en/comparison-operators.html#function_least)
    /// * [PostgreSQL](https://www.postgresql.org/docs/current/functions-conditional.html#FUNCTIONS-GREATEST-LEAST)
    Least {
        args: Vec1<Expr>,
        /// Which type to coerce the arguments to *for comparison*. This might be distinct from the
        /// actual return type of the function call.
        compare_as: DfType,
    },
}

impl BuiltinFunction {
    fn name(&self) -> &'static str {
        use BuiltinFunction::*;
        match self {
            ConvertTZ { .. } => "convert_tz",
            DayOfWeek { .. } => "dayofweek",
            IfNull { .. } => "ifnull",
            Month { .. } => "month",
            Timediff { .. } => "timediff",
            Addtime { .. } => "addtime",
            Round { .. } => "round",
            JsonTypeof { .. } => "json_typeof",
            JsonbTypeof { .. } => "jsonb_typeof",
            Coalesce { .. } => "coalesce",
            Concat { .. } => "concat",
            Substring { .. } => "substring",
            SplitPart { .. } => "split_part",
            Greatest { .. } => "greatest",
            Least { .. } => "least",
        }
    }
}

impl fmt::Display for BuiltinFunction {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        use BuiltinFunction::*;

        write!(f, "{}", self.name())?;

        match self {
            ConvertTZ {
                args: [arg1, arg2, arg3],
                ..
            } => {
                write!(f, "({}, {}, {})", arg1, arg2, arg3)
            }
            DayOfWeek(arg) => {
                write!(f, "({})", arg)
            }
            IfNull(arg1, arg2) => {
                write!(f, "({}, {})", arg1, arg2)
            }
            Month(arg) => {
                write!(f, "({})", arg)
            }
            Timediff(arg1, arg2) => {
                write!(f, "({}, {})", arg1, arg2)
            }
            Addtime(arg1, arg2) => {
                write!(f, "({}, {})", arg1, arg2)
            }
            Round(arg1, precision) => {
                write!(f, "({}, {})", arg1, precision)
            }
            JsonTypeof(arg) => {
                write!(f, "({})", arg)
            }
            JsonbTypeof(arg) => {
                write!(f, "({})", arg)
            }
            Coalesce(arg1, args) => {
                write!(f, "({}, {})", arg1, args.iter().join(", "))
            }
            Concat(arg1, args) => {
                write!(f, "({}, {})", arg1, args.iter().join(", "))
            }
            Substring(string, from, len) => {
                write!(f, "({string}")?;
                if let Some(from) = from {
                    write!(f, " from {from}")?;
                }
                if let Some(len) = len {
                    write!(f, " for {len}")?;
                }
                write!(f, ")")
            }
            SplitPart(string, delimiter, field) => write!(f, "({string}, {delimiter}, {field})"),
            Greatest { args, .. } | Least { args, .. } => {
                write!(f, "({})", args.iter().join(", "))
            }
        }
    }
}

/// Binary infix operators with [`Expr`] on both the left- and right-hand sides
///
/// This type is used as the operator in [`Expr::BinaryOp`].
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
    fn output_type(&self, left_type: &DfType, right_type: &DfType) -> ReadySetResult<DfType> {
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

/// Expressions that can be evaluated during execution of a query
///
/// This type, which is the final lowered version of the original Expression AST, essentially
/// represents a desugared version of [`nom_sql::Expr`], with the following transformations
/// applied during lowering:
///
/// - Literals replaced with their corresponding [`DfValue`]
/// - [Column references](nom_sql::Column) resolved into column indices in the parent node.
/// - Function calls resolved, and arities checked
/// - Desugaring x IN (y, z, ...) to `x = y OR x = z OR ...` and x NOT IN (y, z, ...) to `x != y AND
///   x = z AND ...`
///
/// During forward processing of dataflow, instances of these expressions are
/// [evaluated](Expr::eval) by both projection nodes and filter nodes.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Expr {
    /// A reference to a column, by index, in the parent node
    Column { index: usize, ty: DfType },

    /// A literal DfValue value
    Literal { val: DfValue, ty: DfType },

    /// A binary operation
    Op {
        op: BinaryOperator,
        left: Box<Expr>,
        right: Box<Expr>,
        ty: DfType,
    },

    /// CAST(expr AS type)
    Cast {
        /// The `Expr` to cast
        expr: Box<Expr>,
        /// The `SqlType` that we're attempting to cast to. This is provided
        /// when `Expr::Cast` is created.
        to_type: SqlType,
        /// The `DfType` of the resulting cast. For now, this should be
        /// `Sql(to_type)`.
        /// TODO: This field may not be necessary
        ty: DfType,
    },

    Call {
        func: Box<BuiltinFunction>,
        ty: DfType,
    },

    CaseWhen {
        condition: Box<Expr>,
        then_expr: Box<Expr>,
        else_expr: Box<Expr>,
        ty: DfType,
    },

    Array {
        elements: Vec<Expr>,
        shape: Vec<usize>,
        ty: DfType,
    },
}

impl fmt::Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use Expr::*;

        match self {
            Column { index, .. } => write!(f, "{}", index),
            Literal { val, .. } => write!(f, "(lit: {})", val),
            Op {
                op, left, right, ..
            } => write!(f, "({} {} {})", left, op, right),
            Cast { expr, to_type, .. } => write!(f, "cast({} as {})", expr, to_type),
            Call { func, .. } => write!(f, "{}", func),
            CaseWhen {
                condition,
                then_expr,
                else_expr,
                ..
            } => write!(
                f,
                "case when {} then {} else {}",
                condition, then_expr, else_expr
            ),
            Array { elements, .. } => {
                write!(f, "ARRAY[{}]", elements.iter().join(","))
            }
        }
    }
}

impl Expr {
    pub fn ty(&self) -> &DfType {
        match self {
            Expr::Column { ty, .. }
            | Expr::Literal { ty, .. }
            | Expr::Op { ty, .. }
            | Expr::Call { ty, .. }
            | Expr::CaseWhen { ty, .. }
            | Expr::Cast { ty, .. }
            | Expr::Array { ty, .. } => ty,
        }
    }
}
