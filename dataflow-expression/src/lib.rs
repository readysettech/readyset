#![feature(box_patterns)]

mod eval;
mod like;
mod post_lookup;
pub mod utils;

use std::fmt::Formatter;
use std::{fmt, iter};

use itertools::Itertools;
use launchpad::redacted::Sensitive;
use nom_sql::{
    BinaryOperator, Column, Dialect, Expr as AstExpr, FunctionExpr, InValue, SqlType, UnaryOperator,
};
use readyset_data::{Collation, DfType, DfValue};
use readyset_errors::{internal, unsupported, ReadySetError, ReadySetResult};
use serde::{Deserialize, Serialize};

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
}

impl BuiltinFunction {
    pub fn from_name_and_args<A>(name: &str, args: A) -> Result<(Self, DfType), ReadySetError>
    where
        A: IntoIterator<Item = Expr>,
    {
        fn type_for_round(expr: &Expr, _precision: &Expr) -> DfType {
            use DfType::*;
            match *expr.ty() {
                Unknown => Unknown,

                // When the first argument is a DECIMAL value, the return type is also DECIMAL.
                Numeric { prec, scale } => Numeric { prec, scale },

                // When the first argument is of any integer type, the return type is always BIGINT.
                ref ty if ty.is_any_int() => BigInt,

                // When the first argument is of any floating-point or non-numeric type, the return
                // type is always DOUBLE.
                _ => Double,
            }
        }

        // TODO(ENG-1418): Propagate dialect info.
        let dialect = Dialect::MySQL;

        let arity_error = || ReadySetError::ArityError(name.to_owned());

        let mut args = args.into_iter();
        let mut next_arg = || args.next().ok_or_else(arity_error);

        let result = match name {
            "convert_tz" => {
                // Type is inferred from input argument
                let input = next_arg()?;
                let ty = input.ty().clone();
                (
                    Self::ConvertTZ {
                        args: [input, next_arg()?, next_arg()?],
                        subsecond_digits: ty
                            .subsecond_digits()
                            .unwrap_or_else(|| dialect.default_subsecond_digits()),
                    },
                    ty,
                )
            }
            "dayofweek" => {
                (
                    Self::DayOfWeek(next_arg()?),
                    DfType::Int, // Day of week is always an int
                )
            }
            "ifnull" => {
                let expr = next_arg()?;
                let val = next_arg()?;
                // Type is inferred from the value provided
                let ty = val.ty().clone();
                (Self::IfNull(expr, val), ty)
            }
            "month" => {
                (
                    Self::Month(next_arg()?),
                    DfType::Int, // Month is always an int
                )
            }
            "timediff" => {
                (
                    Self::Timediff(next_arg()?, next_arg()?),
                    // type is always time
                    DfType::Time {
                        subsecond_digits: dialect.default_subsecond_digits(),
                    },
                )
            }
            "addtime" => {
                let base_time = next_arg()?;
                let ty = base_time.ty().clone();
                (Self::Addtime(base_time, next_arg()?), ty)
            }
            "round" => {
                let expr = next_arg()?;
                let prec = args.next().unwrap_or(Expr::Literal {
                    val: DfValue::Int(0),
                    ty: DfType::Int,
                });
                let ty = type_for_round(&expr, &prec);
                (Self::Round(expr, prec), ty)
            }
            "json_typeof" => {
                (
                    Self::JsonTypeof(next_arg()?),
                    // Always returns text containing the JSON type.
                    DfType::Text(Collation::default()),
                )
            }
            "jsonb_typeof" => {
                (
                    Self::JsonbTypeof(next_arg()?),
                    // Always returns text containing the JSON type.
                    DfType::Text(Collation::default()),
                )
            }
            "coalesce" => {
                let arg1 = next_arg()?;
                let ty = arg1.ty().clone();
                (Self::Coalesce(arg1, args.by_ref().collect()), ty)
            }
            "concat" => {
                let arg1 = next_arg()?;
                let rest_args = args.by_ref().collect::<Vec<_>>();
                let collation = iter::once(&arg1)
                    .chain(&rest_args)
                    .find_map(|expr| match expr.ty() {
                        DfType::Text(c) => Some(*c),
                        _ => None,
                    })
                    .unwrap_or_default();
                (Self::Concat(arg1, rest_args), DfType::Text(collation))
            }
            "substring" | "substr" => {
                let string = next_arg()?;
                let ty = if string.ty().is_any_text() {
                    string.ty().clone()
                } else {
                    DfType::Text(Collation::default())
                };

                (
                    Self::Substring(string, next_arg().ok(), next_arg().ok()),
                    ty,
                )
            }
            _ => return Err(ReadySetError::NoSuchFunction(name.to_owned())),
        };

        if args.next().is_some() {
            return Err(arity_error());
        }

        Ok(result)
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
            Coalesce { .. } => "coalesce",
            Concat { .. } => "concat",
            Substring { .. } => "substring",
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
                write!(f, "substring({string}")?;
                if let Some(from) = from {
                    write!(f, " from {from}")?;
                }
                if let Some(len) = len {
                    write!(f, " for {len}")?;
                }
                write!(f, ")")
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
    /// Lower the given [`nom_sql`] AST expression to a dataflow expression, given a function to
    /// look up the index and type of a column by name.
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
    pub fn lower<F>(expr: AstExpr, dialect: Dialect, mut resolve_column: F) -> ReadySetResult<Self>
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
                    .map(|arg| Self::lower(arg, dialect, resolve_column))
                    .collect::<Result<Vec<_>, _>>()?;
                let (func, ty) = BuiltinFunction::from_name_and_args(&fname, args)?;
                Ok(Self::Call {
                    func: Box::new(func),
                    ty,
                })
            }
            AstExpr::Call(FunctionExpr::Substring { string, pos, len }) => {
                let args = iter::once(string)
                    .chain(pos)
                    .chain(len)
                    .map(|arg| Self::lower(*arg, dialect, resolve_column))
                    .collect::<Result<Vec<_>, _>>()?;
                let (func, ty) = BuiltinFunction::from_name_and_args("substring", args)?;

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
                let is_string_literal = lit.is_string();
                let val: DfValue = lit.try_into()?;
                // TODO: Infer type from SQL
                let ty = if is_string_literal && dialect == Dialect::PostgreSQL {
                    DfType::Unknown
                } else {
                    val.infer_dataflow_type()
                };

                Ok(Self::Literal { val, ty })
            }
            AstExpr::Column(col) => {
                let (index, ty) = resolve_column(col)?;
                Ok(Self::Column { index, ty })
            }
            AstExpr::BinaryOp { lhs, op, rhs } => {
                // TODO: Consider rhs and op when inferring type
                let left = Box::new(Self::lower(*lhs, dialect, resolve_column)?);
                let ty = left.ty().clone();
                Ok(Self::Op {
                    op,
                    left,
                    right: Box::new(Self::lower(*rhs, dialect, resolve_column)?),
                    ty,
                })
            }
            AstExpr::UnaryOp {
                op: UnaryOperator::Neg,
                rhs,
            } => {
                let left = Box::new(Self::lower(*rhs, dialect, resolve_column)?);
                // TODO: Negation may change type to signed
                let ty = left.ty().clone();
                Ok(Self::Op {
                    op: BinaryOperator::Multiply,
                    left,
                    right: Box::new(Self::Literal {
                        val: DfValue::Int(-1),
                        ty: DfType::Int,
                    }),
                    ty,
                })
            }
            AstExpr::UnaryOp {
                op: UnaryOperator::Not,
                rhs,
            } => Ok(Self::Op {
                op: BinaryOperator::NotEqual,
                left: Box::new(Self::lower(*rhs, dialect, resolve_column)?),
                right: Box::new(Self::Literal {
                    val: DfValue::Int(1),
                    ty: DfType::Int,
                }),
                ty: DfType::Bool, // type of NE is always bool
            }),
            AstExpr::Cast { expr, ty, .. } => Ok(Self::Cast {
                expr: Box::new(Self::lower(*expr, dialect, resolve_column)?),
                ty: DfType::from_sql_type(&ty, dialect),
                to_type: ty,
            }),
            AstExpr::CaseWhen {
                condition,
                then_expr,
                else_expr,
            } => {
                let then_expr = Box::new(Self::lower(*then_expr, dialect, resolve_column)?);
                let ty = then_expr.ty().clone();
                Ok(Self::CaseWhen {
                    // TODO: Do case arm types have to match? See if type is inferred at runtime
                    condition: Box::new(Self::lower(*condition, dialect, resolve_column)?),
                    then_expr,
                    else_expr: match else_expr {
                        Some(else_expr) => {
                            Box::new(Self::lower(*else_expr, dialect, resolve_column)?)
                        }
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

                    let lhs = Self::lower(*lhs, dialect, resolve_column)?;
                    let make_comparison = |rhs| -> ReadySetResult<_> {
                        Ok(Self::Op {
                            left: Box::new(lhs.clone()),
                            op: comparison_op,
                            right: Box::new(Self::lower(rhs, dialect, resolve_column)?),
                            ty: DfType::Bool, // type of =/!= is always bool
                        })
                    };

                    exprs.try_fold(make_comparison(fst)?, |acc, rhs| {
                        Ok(Self::Op {
                            left: Box::new(acc),
                            op: logical_op,
                            right: Box::new(make_comparison(rhs)?),
                            ty: DfType::Bool, // type of =/!= is always bool
                        })
                    })
                } else if negated {
                    // x IN () is always false
                    Ok(Self::Literal {
                        val: DfValue::None,
                        ty: DfType::Bool,
                    })
                } else {
                    // x NOT IN () is always false
                    Ok(Self::Literal {
                        val: DfValue::from(1),
                        ty: DfType::Bool,
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
        use nom_sql::parse_expr;

        use super::*;

        #[test]
        fn postgresql_text_literaal() {
            // localhost/postgres=# select pg_typeof('abc');
            //  pg_typeof
            // -----------
            //  unknown

            let input = AstExpr::Literal("abc".into());
            let result = Expr::lower(input, Dialect::PostgreSQL, |_| internal!()).unwrap();
            assert_eq!(result.ty(), &DfType::Unknown);
        }

        #[test]
        fn simple_column_reference() {
            let input = AstExpr::Column("t.x".into());
            let result = Expr::lower(input, Dialect::MySQL, |c| {
                if c == "t.x".into() {
                    Ok((0, DfType::Int))
                } else {
                    internal!("what's this column!?")
                }
            })
            .unwrap();
            assert_eq!(
                result,
                Expr::Column {
                    index: 0,
                    ty: DfType::Int
                }
            );
        }

        #[test]
        fn call_coalesce() {
            let input = AstExpr::Call(FunctionExpr::Call {
                name: "coalesce".into(),
                arguments: vec![AstExpr::Column("t.x".into()), AstExpr::Literal(2.into())],
            });

            let result = Expr::lower(input, Dialect::MySQL, |c| {
                if c == "t.x".into() {
                    Ok((0, DfType::Int))
                } else {
                    internal!("what's this column!?")
                }
            })
            .unwrap();

            assert_eq!(
                result,
                Expr::Call {
                    func: Box::new(BuiltinFunction::Coalesce(
                        Expr::Column {
                            index: 0,
                            ty: DfType::Int
                        },
                        vec![Expr::Literal {
                            val: 2.into(),
                            ty: DfType::BigInt
                        }]
                    )),
                    ty: DfType::Int
                }
            );
        }

        #[test]
        fn call_concat_with_texts() {
            let input = parse_expr(Dialect::MySQL, "concat('My', 'SQ', 'L')").unwrap();
            let res = Expr::lower(input, Dialect::MySQL, |_| internal!()).unwrap();
            assert_eq!(
                res,
                Expr::Call {
                    func: Box::new(BuiltinFunction::Concat(
                        Expr::Literal {
                            val: "My".into(),
                            ty: DfType::Text(Collation::default()),
                        },
                        vec![
                            Expr::Literal {
                                val: "SQ".into(),
                                ty: DfType::Text(Collation::default()),
                            },
                            Expr::Literal {
                                val: "L".into(),
                                ty: DfType::Text(Collation::default()),
                            },
                        ],
                    )),
                    ty: DfType::Text(Collation::default()),
                }
            );
        }

        #[test]
        fn substring_from_for() {
            let input = parse_expr(Dialect::MySQL, "substr(col from 1 for 7)").unwrap();
            let res = Expr::lower(input, Dialect::MySQL, |c| {
                if c == "col".into() {
                    Ok((0, DfType::Text(Collation::Citext)))
                } else {
                    internal!()
                }
            })
            .unwrap();
            assert_eq!(
                res,
                Expr::Call {
                    func: Box::new(BuiltinFunction::Substring(
                        Expr::Column {
                            index: 0,
                            ty: DfType::Text(Collation::Citext)
                        },
                        Some(Expr::Literal {
                            val: 1u32.into(),
                            ty: DfType::UnsignedBigInt
                        }),
                        Some(Expr::Literal {
                            val: 7u32.into(),
                            ty: DfType::UnsignedBigInt
                        })
                    )),
                    ty: DfType::Text(Collation::Citext)
                }
            )
        }

        #[test]
        fn substr_regular() {
            let input = parse_expr(Dialect::MySQL, "substr('abcdefghi', 1, 7)").unwrap();
            let res = Expr::lower(input, Dialect::MySQL, |_| internal!()).unwrap();
            assert_eq!(
                res,
                Expr::Call {
                    func: Box::new(BuiltinFunction::Substring(
                        Expr::Literal {
                            val: "abcdefghi".into(),
                            ty: DfType::Text(Collation::default())
                        },
                        Some(Expr::Literal {
                            val: 1.into(),
                            ty: DfType::UnsignedBigInt
                        }),
                        Some(Expr::Literal {
                            val: 7.into(),
                            ty: DfType::UnsignedBigInt
                        })
                    )),
                    ty: DfType::Text(Collation::default())
                }
            )
        }

        #[test]
        fn substring_regular() {
            let input = parse_expr(Dialect::MySQL, "substring('abcdefghi', 1, 7)").unwrap();
            let res = Expr::lower(input, Dialect::MySQL, |_| internal!()).unwrap();
            assert_eq!(
                res,
                Expr::Call {
                    func: Box::new(BuiltinFunction::Substring(
                        Expr::Literal {
                            val: "abcdefghi".into(),
                            ty: DfType::Text(Collation::default())
                        },
                        Some(Expr::Literal {
                            val: 1.into(),
                            ty: DfType::UnsignedBigInt
                        }),
                        Some(Expr::Literal {
                            val: 7.into(),
                            ty: DfType::UnsignedBigInt
                        })
                    )),
                    ty: DfType::Text(Collation::default())
                }
            )
        }

        #[test]
        fn substring_without_string_arg() {
            let input = parse_expr(Dialect::MySQL, "substring(123 from 2)").unwrap();
            let res = Expr::lower(input, Dialect::MySQL, |_| internal!()).unwrap();
            assert_eq!(res.ty(), &DfType::Text(Collation::default()));
        }
    }
}
