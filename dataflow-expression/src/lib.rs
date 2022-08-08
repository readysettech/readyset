#![feature(box_patterns)]

mod eval;
mod like;
mod post_lookup;
pub mod utils;

use std::fmt;
use std::fmt::Formatter;

use launchpad::redacted::Sensitive;
use nom_sql::{
    BinaryOperator, Column, Expr as AstExpr, FunctionExpr, InValue, SqlType, UnaryOperator,
};
use readyset_data::{DfType, DfValue};
use readyset_errors::{internal, unsupported, ReadySetError, ReadySetResult};
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
    pub fn from_name_and_args<A>(name: &str, args: A) -> Result<(Self, DfType), ReadySetError>
    where
        A: IntoIterator<Item = Expr>,
    {
        fn type_for_round(expr: &Expr, _precision: &Expr) -> DfType {
            match expr.ty() {
                DfType::Sql(ty) => match ty {
                    // When the first argument is of any integer type, the return type is always
                    // BIGINT.
                    SqlType::TinyInt(_)
                    | SqlType::UnsignedTinyInt(_)
                    | SqlType::SmallInt(_)
                    | SqlType::UnsignedSmallInt(_)
                    | SqlType::Int(_)
                    | SqlType::UnsignedInt(_)
                    | SqlType::BigInt(_)
                    | SqlType::UnsignedBigInt(_) => DfType::Sql(SqlType::BigInt(None)),
                    // When the first argument is a DECIMAL value, the return type is also DECIMAL.
                    SqlType::Decimal(_, _) => expr.ty().clone(),
                    // When the first argument is of any floating-point type or of any non-numeric
                    // type, the return type is always DOUBLE.
                    _ => DfType::Sql(SqlType::Double),
                },
                DfType::Unknown => DfType::Unknown,
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
                    DfType::Sql(SqlType::Int(None)), // Day of week is always an int
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
                    DfType::Sql(SqlType::Int(None)), // Month is always an int
                ))
            }
            "timediff" => {
                Ok((
                    Self::Timediff(next_arg()?, next_arg()?),
                    DfType::Sql(SqlType::Time), // type is always time
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
                    val: DfValue::Int(0),
                    ty: DfType::Sql(SqlType::Int(None)),
                });
                let ty = type_for_round(&expr, &prec);
                Ok((Self::Round(expr, prec), ty))
            }
            "json_typeof" => {
                Ok((
                    Self::JsonTypeof(next_arg()?),
                    // Always returns text containing the JSON type.
                    DfType::Sql(SqlType::Text),
                ))
            }
            "jsonb_typeof" => {
                Ok((
                    Self::JsonbTypeof(next_arg()?),
                    // Always returns text containing the JSON type.
                    DfType::Sql(SqlType::Text),
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
    /// Lower the given nom_sql AST expression to a dataflow expression, given a function to lookup
    /// the index and type of a column by name.
    ///
    /// Currently, this involves:
    ///
    /// - Literals being replaced with their corresponding [`DfValue`]
    /// - [Column references](nom_sql::Column) being resolved into column indices in the parent
    ///   node.
    /// - Function calls being resolved to built-in functions, and arities checked
    /// - Desugaring x IN (y, z, ...) to `x = y OR x = z OR ...` and x NOT IN (y, z, ...) to `x != y
    ///   AND x != z AND ...`
    /// - Replacing unary negation with `(expr * -1)`
    /// - Replacing unary NOT with `(expr != 1)`
    /// - Inferring the type of each node in the expression AST.
    pub fn lower<F>(expr: AstExpr, mut resolve_column: F) -> ReadySetResult<Self>
    where
        F: FnMut(Column) -> ReadySetResult<(usize, DfType)> + Copy,
    {
        match expr {
            AstExpr::Call(FunctionExpr::Call {
                name: fname,
                arguments,
            }) => {
                let args = arguments
                    .into_iter()
                    .map(|arg| Self::lower(arg, resolve_column))
                    .collect::<Result<Vec<_>, _>>()?;
                let (func, ty) = BuiltinFunction::from_name_and_args(&fname, args)?;
                Ok(Self::Call {
                    func: Box::new(func),
                    ty,
                })
            }
            AstExpr::Call(call) => internal!(
                "Unexpected (aggregate?) call node in project expression: {:?}",
                Sensitive(&call)
            ),
            AstExpr::Literal(lit) => {
                let val: DfValue = lit.try_into()?;
                // TODO: Infer type from SQL
                let ty = val.sql_type().into();

                Ok(Self::Literal { val, ty })
            }
            AstExpr::Column(col) => {
                let (index, ty) = resolve_column(col)?;
                Ok(Self::Column { index, ty })
            }
            AstExpr::BinaryOp { lhs, op, rhs } => {
                // TODO: Consider rhs and op when inferring type
                let left = Box::new(Self::lower(*lhs, resolve_column)?);
                let ty = left.ty().clone();
                Ok(Self::Op {
                    op,
                    left,
                    right: Box::new(Self::lower(*rhs, resolve_column)?),
                    ty,
                })
            }
            AstExpr::UnaryOp {
                op: UnaryOperator::Neg,
                rhs,
            } => {
                let left = Box::new(Self::lower(*rhs, resolve_column)?);
                // TODO: Negation may change type to signed
                let ty = left.ty().clone();
                Ok(Self::Op {
                    op: BinaryOperator::Multiply,
                    left,
                    right: Box::new(Self::Literal {
                        val: DfValue::Int(-1),
                        ty: DfType::Sql(SqlType::Int(None)),
                    }),
                    ty,
                })
            }
            AstExpr::UnaryOp {
                op: UnaryOperator::Not,
                rhs,
            } => Ok(Self::Op {
                op: BinaryOperator::NotEqual,
                left: Box::new(Self::lower(*rhs, resolve_column)?),
                right: Box::new(Self::Literal {
                    val: DfValue::Int(1),
                    ty: DfType::Sql(SqlType::Int(None)),
                }),
                ty: DfType::Sql(SqlType::Bool), // type of NE is always bool
            }),
            AstExpr::Cast { expr, ty, .. } => Ok(Self::Cast {
                expr: Box::new(Self::lower(*expr, resolve_column)?),
                to_type: ty.clone(),
                ty: ty.into(),
            }),
            AstExpr::CaseWhen {
                condition,
                then_expr,
                else_expr,
            } => {
                let then_expr = Box::new(Self::lower(*then_expr, resolve_column)?);
                let ty = then_expr.ty().clone();
                Ok(Self::CaseWhen {
                    // TODO: Do case arm types have to match? See if type is inferred at runtime
                    condition: Box::new(Self::lower(*condition, resolve_column)?),
                    then_expr,
                    else_expr: match else_expr {
                        Some(else_expr) => Box::new(Self::lower(*else_expr, resolve_column)?),
                        None => Box::new(Self::Literal {
                            val: DfValue::None,
                            ty: DfType::Unknown,
                        }),
                    },
                    ty,
                })
            }
            AstExpr::In {
                lhs,
                rhs: InValue::List(exprs),
                negated,
            } => {
                let mut exprs = exprs.into_iter();
                if let Some(fst) = exprs.next() {
                    let (comparison_op, logical_op) = if negated {
                        (BinaryOperator::NotEqual, BinaryOperator::And)
                    } else {
                        (BinaryOperator::Equal, BinaryOperator::Or)
                    };

                    let lhs = Self::lower(*lhs, resolve_column)?;
                    let make_comparison = |rhs| -> ReadySetResult<_> {
                        Ok(Self::Op {
                            left: Box::new(lhs.clone()),
                            op: comparison_op,
                            right: Box::new(Self::lower(rhs, resolve_column)?),
                            ty: DfType::Sql(SqlType::Bool), // type of =/!= is always bool
                        })
                    };

                    exprs.try_fold(make_comparison(fst)?, |acc, rhs| {
                        Ok(Self::Op {
                            left: Box::new(acc),
                            op: logical_op,
                            right: Box::new(make_comparison(rhs)?),
                            ty: DfType::Sql(SqlType::Bool), // type of =/!= is always bool
                        })
                    })
                } else if negated {
                    // x IN () is always false
                    Ok(Self::Literal {
                        val: DfValue::None,
                        ty: DfType::Sql(SqlType::Bool),
                    })
                } else {
                    // x NOT IN () is always false
                    Ok(Self::Literal {
                        val: DfValue::from(1),
                        ty: DfType::Sql(SqlType::Bool),
                    })
                }
            }
            AstExpr::Exists(_) => unsupported!("EXISTS not currently supported"),
            AstExpr::Variable(_) => unsupported!("Variables not currently supported"),
            AstExpr::Between { .. } | AstExpr::NestedSelect(_) | AstExpr::In { .. } => {
                internal!("Expression should have been desugared earlier: {expr}")
            }
        }
    }

    pub fn ty(&self) -> &DfType {
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

#[cfg(test)]
mod tests {
    use super::*;

    mod lower {
        use super::*;
        #[test]
        fn simple_column_reference() {
            let input = AstExpr::Column("t.x".into());
            let result = Expr::lower(input, |c| {
                if c == "t.x".into() {
                    Ok((0, DfType::Sql(SqlType::Int(None))))
                } else {
                    internal!("what's this column!?")
                }
            })
            .unwrap();
            assert_eq!(
                result,
                Expr::Column {
                    index: 0,
                    ty: DfType::Sql(SqlType::Int(None))
                }
            );
        }
    }
}
