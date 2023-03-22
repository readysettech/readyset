#![feature(box_patterns, let_else)]

mod binary_operator;
mod eval;
pub mod like;
mod lower;
mod post_lookup;
pub mod utils;

use std::fmt::{self, Display, Formatter};

use itertools::Itertools;
pub use readyset_data::Dialect;
use readyset_data::{DfType, DfValue};
use serde::{Deserialize, Serialize};
use vec1::Vec1;

pub use crate::binary_operator::*;
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
    /// [`date_format`](https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_date-format)
    DateFormat(Expr, Expr),
    /// [`round`](https://dev.mysql.com/doc/refman/8.0/en/mathematical-functions.html#function_round)
    Round(Expr, Expr),
    /// [`json_depth`](https://dev.mysql.com/doc/refman/8.0/en/json-attribute-functions.html#function_json-depth)
    JsonDepth(Expr),
    /// [`json_valid`](https://dev.mysql.com/doc/refman/8.0/en/json-attribute-functions.html#function_json-valid)
    JsonValid(Expr),
    /// [`json_quote`](https://dev.mysql.com/doc/refman/8.0/en/json-creation-functions.html#function_json-quote)
    JsonQuote(Expr),
    /// [`json_overlaps`](https://dev.mysql.com/doc/refman/8.0/en/json-search-functions.html#function_json-overlaps)
    JsonOverlaps(Expr, Expr),
    /// [`json[b]_typeof`](https://www.postgresql.org/docs/current/functions-json.html)
    JsonTypeof(Expr),
    /// [`json[b]_object`](https://www.postgresql.org/docs/current/functions-json.html)
    JsonObject {
        arg1: Expr,
        arg2: Option<Expr>,

        /// `json_object` allows for duplicate keys whereas `jsonb_object` does not.
        allow_duplicate_keys: bool,
    },
    /// [`json[b]_array_length`](https://www.postgresql.org/docs/current/functions-json.html)
    JsonArrayLength(Expr),
    /// [`json[b]_strip_nulls`](https://www.postgresql.org/docs/current/functions-json.html)
    JsonStripNulls(Expr),
    /// [`json[b]_extract_path[_text]`](https://www.postgresql.org/docs/current/functions-json.html)
    JsonExtractPath { json: Expr, keys: Vec1<Expr> },
    /// [`jsonb_insert`](https://www.postgresql.org/docs/current/functions-json.html)
    JsonbInsert(Expr, Expr, Expr, Option<Expr>),
    /// [`jsonb_set[_lax]`](https://www.postgresql.org/docs/current/functions-json.html)
    JsonbSet(Expr, Expr, Expr, Option<Expr>, NullValueTreatmentArg),
    /// [`jsonb_pretty`](https://www.postgresql.org/docs/current/functions-json.html)
    JsonbPretty(Expr),
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

    /// [`array_to_string`](https://www.postgresql.org/docs/current/functions-array.html)
    ArrayToString(Expr, Expr, Option<Expr>),
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
            DateFormat { .. } => "date_format",
            Round { .. } => "round",
            JsonDepth { .. } => "json_depth",
            JsonValid { .. } => "json_valid",
            JsonQuote { .. } => "json_quote",
            JsonOverlaps { .. } => "json_overlaps",
            JsonTypeof { .. } => "json_typeof",
            JsonObject { .. } => "json_object",
            JsonArrayLength { .. } => "json_array_length",
            JsonStripNulls { .. } => "json_strip_nulls",
            JsonExtractPath { .. } => "json_extract_path",
            JsonbInsert { .. } => "jsonb_insert",
            JsonbSet { .. } => "jsonb_set",
            JsonbPretty { .. } => "jsonb_pretty",
            Coalesce { .. } => "coalesce",
            Concat { .. } => "concat",
            Substring { .. } => "substring",
            SplitPart { .. } => "split_part",
            Greatest { .. } => "greatest",
            Least { .. } => "least",
            ArrayToString { .. } => "array_to_string",
        }
    }
}

impl Display for BuiltinFunction {
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
            DateFormat(arg1, arg2) => {
                write!(f, "({}, {})", arg1, arg2)
            }
            Round(arg1, precision) => {
                write!(f, "({}, {})", arg1, precision)
            }
            JsonDepth(arg) | JsonValid(arg) | JsonQuote(arg) | JsonTypeof(arg)
            | JsonArrayLength(arg) | JsonStripNulls(arg) | JsonbPretty(arg) => {
                write!(f, "({})", arg)
            }
            JsonOverlaps(arg1, arg2) => {
                write!(f, "({}, {})", arg1, arg2)
            }
            JsonObject { arg1, arg2, .. } => {
                write!(f, "({arg1}")?;
                if let Some(arg2) = arg2 {
                    write!(f, ", {arg2}")?;
                }
                write!(f, ")")
            }
            JsonExtractPath { json, keys } => {
                write!(f, "({}, {})", json, keys.iter().join(", "))
            }
            JsonbInsert(arg1, arg2, arg3, arg4) => {
                write!(f, "({arg1}, {arg2}, {arg3}")?;
                if let Some(arg4) = arg4 {
                    write!(f, ", {arg4}")?;
                }
                write!(f, ")")
            }
            JsonbSet(arg1, arg2, arg3, arg4, arg5) => {
                write!(f, "({arg1}, {arg2}, {arg3}")?;

                for arg in [arg4.as_ref(), arg5.expr()].into_iter().flatten() {
                    write!(f, ", {arg}")?;
                }

                write!(f, ")")
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
            ArrayToString(array, delimiter, null_string) => {
                write!(f, "({array}, {delimiter}")?;
                if let Some(null_string) = null_string {
                    write!(f, ", {null_string}")?;
                }
                write!(f, ")")
            }
        }
    }
}

/// Argument for [`BuiltinFunction::JsonbSet`] that differentiates between `jsonb_set` and
/// `jsonb_set_lax` behavior.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum NullValueTreatmentArg {
    /// `jsonb_set` behavior.
    ReturnNull,
    /// `jsonb_set_lax` behavior.
    Expr(Option<Expr>),
}

impl NullValueTreatmentArg {
    pub fn expr(&self) -> Option<&Expr> {
        match self {
            Self::ReturnNull => None,
            Self::Expr(expr) => expr.as_ref(),
        }
    }
}

/// A single `WHEN expr THEN expr` branch of a `CASE WHEN` expr
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CaseWhenBranch {
    condition: Expr,
    body: Expr,
}

impl Display for CaseWhenBranch {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "WHEN {} THEN {} ", self.condition, self.body)
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

    /// Test if the LHS satisfies OP for any element in the RHS, which must evaluate to some kind
    /// of array.
    ///
    /// The OP must return a boolean
    OpAny {
        op: BinaryOperator,
        left: Box<Expr>,
        right: Box<Expr>,
        ty: DfType,
    },

    /// Test if the LHS satisfies OP for every element in the RHS, which must evaluate to some kind
    /// of array
    ///
    /// The OP must return a boolean
    OpAll {
        op: BinaryOperator,
        left: Box<Expr>,
        right: Box<Expr>,
        ty: DfType,
    },

    /// CAST(expr AS type)
    Cast {
        /// The `Expr` to cast
        expr: Box<Expr>,
        /// The `DfType` to cast to
        ty: DfType,
    },

    Call {
        func: Box<BuiltinFunction>,
        ty: DfType,
    },

    CaseWhen {
        branches: Vec<CaseWhenBranch>,
        else_expr: Box<Expr>,
        ty: DfType,
    },

    Array {
        elements: Vec<Expr>,
        shape: Vec<usize>,
        ty: DfType,
    },
}

impl Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use Expr::*;

        match self {
            Column { index, .. } => write!(f, "{}", index),
            Literal { val, .. } => write!(f, "(lit: {})", val),
            Op {
                op, left, right, ..
            } => write!(f, "({} {} {})", left, op, right),
            OpAny {
                op, left, right, ..
            } => {
                write!(f, "({left} {op} ANY ({right}))")
            }
            OpAll {
                op, left, right, ..
            } => {
                write!(f, "({left} {op} ALL ({right}))")
            }
            Cast { expr, ty, .. } => write!(f, "cast({} as {})", expr, ty,),
            Call { func, .. } => write!(f, "{}", func),
            CaseWhen {
                branches,
                else_expr,
                ..
            } => {
                write!(f, "CASE ")?;
                for branch in branches {
                    write!(f, "{branch} ")?;
                }
                write!(f, "ELSE {else_expr} END")
            }
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
            | Expr::OpAny { ty, .. }
            | Expr::OpAll { ty, .. }
            | Expr::Call { ty, .. }
            | Expr::CaseWhen { ty, .. }
            | Expr::Cast { ty, .. }
            | Expr::Array { ty, .. } => ty,
        }
    }
}
