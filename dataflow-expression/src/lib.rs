#![feature(box_patterns)]

mod eval;
mod like;
mod post_lookup;
pub mod utils;

use std::fmt;
use std::fmt::Formatter;

use nom_sql::{BinaryOperator, SqlType};
use readyset_data::noria_type::Type;
use readyset_data::DataType;
use readyset_errors::ReadySetError;
use serde::{Deserialize, Serialize};

pub use crate::post_lookup::{
    PostLookup, PostLookupAggregate, PostLookupAggregateFunction, PostLookupAggregates,
    PreInsertion, ReaderProcessing,
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum BuiltinFunction {
    /// convert_tz(expr, expr, expr)
    ConvertTZ(Expr, Expr, Expr),
    /// dayofweek(expr)
    DayOfWeek(Expr),
    /// ifnull(expr, expr)
    IfNull(Expr, Expr),
    /// month(expr)
    Month(Expr),
    /// timediff(expr, expr)
    Timediff(Expr, Expr),
    /// addtime(expr, expr)
    Addtime(Expr, Expr),
    /// round(expr, prec)
    Round(Expr, Expr),
    /// json_typeof(expr)
    JsonTypeof(Expr),
    /// jsonb_typeof(expr)
    JsonbTypeof(Expr),
}

impl BuiltinFunction {
    pub fn from_name_and_args<A>(name: &str, args: A) -> Result<(Self, Type), ReadySetError>
    where
        A: IntoIterator<Item = Expr>,
    {
        fn type_for_round(expr: &Expr, _precision: &Expr) -> Type {
            match expr.ty() {
                Type::Sql(ty) => match ty {
                    // When the first argument is of any integer type, the return type is always
                    // BIGINT.
                    SqlType::Tinyint(_)
                    | SqlType::UnsignedTinyint(_)
                    | SqlType::Smallint(_)
                    | SqlType::UnsignedSmallint(_)
                    | SqlType::Int(_)
                    | SqlType::UnsignedInt(_)
                    | SqlType::Bigint(_)
                    | SqlType::UnsignedBigint(_) => Type::Sql(SqlType::Bigint(None)),
                    // When the first argument is a DECIMAL value, the return type is also DECIMAL.
                    SqlType::Decimal(_, _) => expr.ty().clone(),
                    // When the first argument is of any floating-point type or of any non-numeric
                    // type, the return type is always DOUBLE.
                    _ => Type::Sql(SqlType::Double),
                },
                Type::Unknown => Type::Unknown,
            }
        }

        let arity_error = || ReadySetError::ArityError(name.to_owned());

        let mut args = args.into_iter();
        let mut next_arg = || args.next().ok_or_else(arity_error);

        let result = match name {
            "convert_tz" => {
                // Type is inferred from input argument
                let input = next_arg()?;
                let ty = input.ty().clone();
                Ok((Self::ConvertTZ(input, next_arg()?, next_arg()?), ty))
            }
            "dayofweek" => {
                Ok((
                    Self::DayOfWeek(next_arg()?),
                    Type::Sql(SqlType::Int(None)), // Day of week is always an int
                ))
            }
            "ifnull" => {
                let expr = next_arg()?;
                let val = next_arg()?;
                // Type is inferred from the value provided
                let ty = val.ty().clone();
                Ok((Self::IfNull(expr, val), ty))
            }
            "month" => {
                Ok((
                    Self::Month(next_arg()?),
                    Type::Sql(SqlType::Int(None)), // Month is always an int
                ))
            }
            "timediff" => {
                Ok((
                    Self::Timediff(next_arg()?, next_arg()?),
                    Type::Sql(SqlType::Time), // type is always time
                ))
            }
            "addtime" => {
                let base_time = next_arg()?;
                let ty = base_time.ty().clone();
                Ok((Self::Addtime(base_time, next_arg()?), ty))
            }
            "round" => {
                let expr = next_arg()?;
                let prec = args.next().unwrap_or(Expr::Literal {
                    val: DataType::Int(0),
                    ty: Type::Sql(SqlType::Int(None)),
                });
                let ty = type_for_round(&expr, &prec);
                Ok((Self::Round(expr, prec), ty))
            }
            "json_typeof" => {
                Ok((
                    Self::JsonTypeof(next_arg()?),
                    Type::Sql(SqlType::Text), // Always returns text containing the JSON type
                ))
            }
            "jsonb_typeof" => {
                Ok((
                    Self::JsonbTypeof(next_arg()?),
                    Type::Sql(SqlType::Text), // Always returns text containing the JSON type
                ))
            }
            _ => Err(ReadySetError::NoSuchFunction(name.to_owned())),
        };

        if args.next().is_some() {
            return Err(arity_error());
        }

        result
    }

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
        }
    }
}

impl fmt::Display for BuiltinFunction {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        use BuiltinFunction::*;

        write!(f, "{}", self.name())?;

        match self {
            ConvertTZ(arg1, arg2, arg3) => {
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
        }
    }
}

/// Expressions that can be evaluated during execution of a query
///
/// This type, which is the final lowered version of the original Expression AST, essentially
/// represents a desugared version of [`nom_sql::Expr`], with the following transformations
/// applied during lowering:
///
/// - Literals replaced with their corresponding [`DataType`]
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
    Column { index: usize, ty: Type },

    /// A literal DataType value
    Literal { val: DataType, ty: Type },

    /// A binary operation
    Op {
        op: BinaryOperator,
        left: Box<Expr>,
        right: Box<Expr>,
        ty: Type,
    },

    /// CAST(expr AS type)
    Cast {
        /// The `Expr` to cast
        expr: Box<Expr>,
        /// The `SqlType` that we're attempting to cast to. This is provided
        /// when `Expr::Cast` is created.
        to_type: SqlType,
        /// The `Type` of the resulting cast. For now, this should be `Type::Sql(to_type)`
        /// TODO: This field may not be necessary
        ty: Type,
    },

    Call {
        func: Box<BuiltinFunction>,
        ty: Type,
    },

    CaseWhen {
        condition: Box<Expr>,
        then_expr: Box<Expr>,
        else_expr: Box<Expr>,
        ty: Type,
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
        }
    }
}

impl Expr {
    pub fn ty(&self) -> &Type {
        match self {
            Expr::Column { ty, .. }
            | Expr::Literal { ty, .. }
            | Expr::Op { ty, .. }
            | Expr::Call { ty, .. }
            | Expr::CaseWhen { ty, .. }
            | Expr::Cast { ty, .. } => ty,
        }
    }
}
