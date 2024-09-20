use std::{cmp, iter};

use vec1::Vec1;

use nom_sql::{
    BinaryOperator as SqlBinaryOperator, Column, DialectDisplay, Expr as AstExpr, FunctionExpr,
    InValue, Relation, UnaryOperator,
};
use readyset_data::dialect::SqlEngine;
use readyset_data::{Collation, DfType, DfValue};
use readyset_errors::{
    internal, invalid_query, invalid_query_err, unsupported, unsupported_err, ReadySetError,
    ReadySetResult,
};
use readyset_util::redacted::Sensitive;

use crate::{
    BinaryOperator, BuiltinFunction, CaseWhenBranch, Dialect, Expr, NullValueTreatmentArg,
};

/// Context supplied to expression lowering to allow resolving references to objects within the
/// schema
pub trait LowerContext: Clone {
    /// Look up a column in the parent node of the node containing this expression, returning its
    /// column index and type
    fn resolve_column(&self, col: Column) -> ReadySetResult<(usize, DfType)>;

    /// Look up a named custom type in the schema.
    fn resolve_type(&self, ty: Relation) -> Option<DfType>;
}

/// Unify the given list of types according to PostgreSQL's [type unification rules][pg-docs]
///
/// [pg-docs]: https://www.postgresql.org/docs/current/typeconv-union-case.html
fn unify_postgres_types(types: Vec<&DfType>) -> ReadySetResult<DfType> {
    let Some(first_ty) = types.first() else {
        return Ok(DfType::DEFAULT_TEXT);
    };

    // > 1. If all inputs are of the same type, and it is not unknown, resolve as that type.
    if types.iter().skip(1).all(|t| t == first_ty) && first_ty.is_known() {
        return Ok((*first_ty).clone());
    }

    // > 2. If any input is of a domain type, treat it as being of the domain's base type for all
    // > subsequent steps.
    //
    // We don't support domain types, so we can skip this step

    // > 3. If all inputs are of type unknown, resolve as type text (the preferred type of the
    // > string category). Otherwise, unknown inputs are ignored for the purposes of the remaining
    // > rules.
    let Some(first_known_type) = types.iter().find(|t| t.is_known()) else {
        return Ok(DfType::DEFAULT_TEXT);
    };

    // > 4. If the non-unknown inputs are not all of the same type category, fail.
    if types
        .iter()
        .skip(1)
        .filter(|t| t.is_known())
        .any(|t| t.pg_category() != first_known_type.pg_category())
    {
        invalid_query_err!(
            "Cannot coerce type {} to type {}",
            first_ty,
            types
                .get(1)
                .expect("can't get here unless we have at least 2 types")
        );
    }

    // > 5. Select the first non-unknown input type as the candidate type, then consider each other
    // > non-unknown input type, left to right. [13] If the candidate type can be implicitly
    // > converted to the other type, but not vice-versa, select the other type as the new candidate
    // > type. Then continue considering the remaining inputs. If, at any stage of this process, a
    // > preferred type is selected, stop considering additional inputs.
    //
    // TODO(ENG-1909): This bit is not completely correct, because we haven't yet implemented the
    // notion of "preferred" types, or whether or not types can be implicitly converted. For now, we
    // just take the first non-unknown type
    Ok((*first_known_type).clone())
}

/// Returns a tuple of the inferred type to convert arguments to for comparison within a call to
/// `GREATEST` or `LEAST` using MySQL's [inference rules for those functions][mysql-docs]
///
/// [mysql-docs]: https://dev.mysql.com/doc/refman/8.0/en/comparison-operators.html#function_least
fn mysql_least_greatest_compare_as(arg_types: Vec<&DfType>) -> DfType {
    // > * If any argument is NULL, the result is NULL. No comparison is needed.
    if arg_types.is_empty() || arg_types.iter().any(|t| t.is_unknown()) {
        return DfType::Unknown;
    }

    // > * If all arguments are integer-valued, they are compared as integers.
    if arg_types.iter().all(|t| t.is_any_int()) {
        return DfType::BigInt;
    }

    // > * If at least one argument is double precision, they are compared as double-precision
    // > values
    if arg_types.iter().any(|t| t.is_any_float()) {
        return DfType::Double;
    }

    // > * If the arguments comprise a mix of numbers and strings, they are compared as strings.
    // > * If any argument is a nonbinary (character) string, the arguments are compared as
    // > nonbinary
    // > strings.
    //
    // As far as I can tell, these two bullet points mean the same thing...
    let mut has_strings = false;
    let mut has_numbers = false;
    for arg_ty in &arg_types {
        if arg_ty.is_any_text() {
            has_strings = true;
        } else if arg_ty.is_any_int() {
            has_numbers = true;
        }

        if has_strings && has_numbers {
            return DfType::DEFAULT_TEXT;
        }
    }

    // > * In all other cases, the arguments are compared as binary strings.
    DfType::VarBinary(u16::MAX)
}

impl BuiltinFunction {
    pub(crate) fn from_name_and_args<A>(
        name: &str,
        args: A,
        dialect: Dialect,
    ) -> Result<(Self, DfType), ReadySetError>
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

        let arity_error = || ReadySetError::ArityError(name.to_owned());

        // TODO: Type-check arguments.
        let mut args = args.into_iter();
        let mut next_arg = || args.next().ok_or_else(arity_error);
        let cast = |expr: Expr, ty| {
            if *expr.ty() == ty {
                // optimization: don't cast if the expr already has the target type
                expr
            } else {
                Expr::Cast {
                    expr: Box::new(expr),
                    ty,
                    null_on_failure: false,
                }
            }
        };
        let try_cast = |expr, ty| Expr::Cast {
            expr: Box::new(expr),
            ty,
            null_on_failure: true,
        };

        let result = match name.to_lowercase().as_str() {
            "convert_tz" => {
                // Type is inferred from input argument
                let input = next_arg()?;
                let ty = input.ty().clone();
                (
                    Self::ConvertTZ([
                        try_cast(
                            input,
                            DfType::Timestamp {
                                subsecond_digits: ty
                                    .subsecond_digits()
                                    .unwrap_or_else(|| dialect.default_subsecond_digits()),
                            },
                        ),
                        cast(next_arg()?, DfType::DEFAULT_TEXT),
                        cast(next_arg()?, DfType::DEFAULT_TEXT),
                    ]),
                    ty,
                )
            }
            "dayofweek" => {
                (
                    Self::DayOfWeek(try_cast(next_arg()?, DfType::Date)),
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
                    Self::Month(try_cast(next_arg()?, DfType::Date)),
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
            "date_format" => (
                Self::DateFormat(next_arg()?, next_arg()?),
                DfType::DEFAULT_TEXT,
            ),
            "round" => {
                let expr = next_arg()?;
                let prec = args.next().unwrap_or(Expr::Literal {
                    val: DfValue::Int(0),
                    ty: DfType::Int,
                });
                let ty = type_for_round(&expr, &prec);
                (Self::Round(expr, prec), ty)
            }
            "json_depth" => (Self::JsonDepth(next_arg()?), DfType::Int),
            "json_valid" => (Self::JsonValid(next_arg()?), DfType::BigInt),
            "json_overlaps" => (Self::JsonOverlaps(next_arg()?, next_arg()?), DfType::BigInt),
            "json_quote" => (Self::JsonQuote(next_arg()?), DfType::DEFAULT_TEXT),
            "json_typeof" | "jsonb_typeof" => (
                Self::JsonTypeof(next_arg()?),
                // Always returns text containing the JSON type.
                DfType::DEFAULT_TEXT,
            ),
            "json_object" => match dialect.engine() {
                // TODO(ENG-1536): https://dev.mysql.com/doc/refman/8.0/en/json-creation-functions.html#function_json-object
                SqlEngine::MySQL => unsupported!("MySQL 'json_object' not yet supported"),
                SqlEngine::PostgreSQL => (
                    Self::JsonObject {
                        arg1: next_arg()?,
                        arg2: args.next(),
                        allow_duplicate_keys: true,
                    },
                    DfType::Json,
                ),
            },
            "jsonb_object" => (
                Self::JsonObject {
                    arg1: next_arg()?,
                    arg2: args.next(),
                    allow_duplicate_keys: false,
                },
                DfType::Jsonb,
            ),
            "json_array_length" | "jsonb_array_length" => {
                (Self::JsonArrayLength(next_arg()?), DfType::Int)
            }
            "json_strip_nulls" => (Self::JsonStripNulls(next_arg()?), DfType::Json),
            "jsonb_strip_nulls" => (Self::JsonStripNulls(next_arg()?), DfType::Jsonb),
            "json_extract_path" => (
                Self::JsonExtractPath {
                    json: next_arg()?,
                    keys: Vec1::try_from_vec(args.by_ref().collect()).map_err(|_| arity_error())?,
                },
                DfType::Json,
            ),
            "jsonb_extract_path" => (
                Self::JsonExtractPath {
                    json: next_arg()?,
                    keys: Vec1::try_from_vec(args.by_ref().collect()).map_err(|_| arity_error())?,
                },
                DfType::Jsonb,
            ),
            "json_extract_path_text" | "jsonb_extract_path_text" => (
                Self::JsonExtractPath {
                    json: next_arg()?,
                    keys: Vec1::try_from_vec(args.by_ref().collect()).map_err(|_| arity_error())?,
                },
                DfType::DEFAULT_TEXT,
            ),
            "jsonb_insert" => (
                Self::JsonbInsert(next_arg()?, next_arg()?, next_arg()?, args.next()),
                DfType::Jsonb,
            ),
            "jsonb_set" => (
                Self::JsonbSet(
                    next_arg()?,
                    next_arg()?,
                    next_arg()?,
                    args.next(),
                    NullValueTreatmentArg::ReturnNull,
                ),
                DfType::Jsonb,
            ),
            "jsonb_set_lax" => (
                Self::JsonbSet(
                    next_arg()?,
                    next_arg()?,
                    next_arg()?,
                    args.next(),
                    NullValueTreatmentArg::Expr(args.next()),
                ),
                DfType::Jsonb,
            ),
            "jsonb_pretty" => (Self::JsonbPretty(next_arg()?), DfType::DEFAULT_TEXT),
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
                let ty = DfType::Text(collation);
                (
                    Self::Concat(
                        cast(arg1, ty.clone()),
                        rest_args
                            .into_iter()
                            .map(|arg| cast(arg, ty.clone()))
                            .collect(),
                    ),
                    ty,
                )
            }
            "substring" | "substr" => {
                let string = next_arg()?;
                let ty = if string.ty().is_any_text() {
                    string.ty().clone()
                } else {
                    DfType::DEFAULT_TEXT
                };

                (
                    Self::Substring(
                        cast(string, ty.clone()),
                        next_arg().ok().map(|arg| cast(arg, DfType::BigInt)),
                        next_arg().ok().map(|arg| cast(arg, DfType::BigInt)),
                    ),
                    ty,
                )
            }
            "split_part" => (
                Self::SplitPart(
                    cast(next_arg()?, DfType::DEFAULT_TEXT),
                    cast(next_arg()?, DfType::DEFAULT_TEXT),
                    cast(next_arg()?, DfType::Int),
                ),
                DfType::DEFAULT_TEXT,
            ),
            "greatest" | "least" => {
                // The type inference rules for GREATEST and LEAST are the same, so this block
                // covers both then dispatches for the actual function construction at the end
                let arg1 = next_arg()?;
                let rest_args = args.by_ref().collect::<Vec<_>>();
                let arg_tys = iter::once(arg1.ty())
                    .chain(rest_args.iter().map(|arg| arg.ty()))
                    .collect::<Vec<_>>();
                let (compare_as, ty) = match dialect.engine() {
                    SqlEngine::PostgreSQL => {
                        let ty = unify_postgres_types(arg_tys)?;
                        (ty.clone(), ty)
                    }
                    SqlEngine::MySQL => {
                        // TODO(ENG-1911): What are the rules for MySQL's return type inference?
                        // The documentation just says "The return type of LEAST() is the aggregated
                        // type of the comparison argument types."
                        let return_ty = arg_tys
                            .iter()
                            .find(|t| t.is_known())
                            .copied()
                            .cloned()
                            .unwrap_or(DfType::Binary(0));
                        let compare_as = mysql_least_greatest_compare_as(arg_tys);
                        (compare_as, return_ty)
                    }
                };

                let mut args = Vec1::with_capacity(arg1, rest_args.len() + 1);
                args.extend(rest_args);

                (
                    if name == "greatest" {
                        Self::Greatest { args, compare_as }
                    } else {
                        Self::Least { args, compare_as }
                    },
                    ty,
                )
            }
            "array_to_string" => {
                let array_arg = next_arg()?;
                let elem_ty = match array_arg.ty() {
                    DfType::Array(t) => (**t).clone(),
                    _ => DfType::Unknown,
                };
                (
                    Self::ArrayToString(
                        cast(array_arg, DfType::Array(Box::new(elem_ty))),
                        next_arg()?,
                        next_arg().ok(),
                    ),
                    DfType::DEFAULT_TEXT,
                )
            }
            "date_trunc" => {
                // this is the time unit (precision) to truncate by ('hour', 'minute', and so on).
                // called 'field' in the postgres docs.
                let precision = next_arg()?;

                // next is an Expr that evaluates to either timestamp or timestamptz.
                // the postgres date_trunc() function also accepts INTERVAL, but that's
                // not a supported type in DfType as of March-2024.
                let source = next_arg()?;
                let ret_type = source.ty().clone();

                // last is an optional time zone argument to the function.
                // We do not yet support the optional time zone.
                if args.next().is_some() {
                    unsupported!("The time zone parameter is not yet supported.");
                }

                (Self::DateTrunc(precision, source), ret_type)
            }
            "length" | "octet_length" | "char_length" | "character_length" => {
                // MySQL - `LENGTH()`, `OCTET_LENGTH()` = in bytes | `CHAR_LENGTH()`, `CHARACTER_LENGTH()` = in characters
                // PostgreSQL - `OCTET_LENGTH()` = in bytes | `LENGTH()`, `CHAR_LENGTH()`, `CHARACTER_LENGTH()` = in characters
                let expr = next_arg()?;
                let ty = if expr.ty().is_any_text() {
                    DfType::BigInt
                } else {
                    DfType::Int
                };
                let in_bytes = (matches!(dialect.engine(), SqlEngine::MySQL) && name == "length")
                    || name == "octet_length";
                (
                    Self::Length {
                        expr,
                        in_bytes,
                        dialect,
                    },
                    ty,
                )
            }
            "ascii" => {
                let expr = next_arg()?;
                (Self::Ascii { expr, dialect }, DfType::UnsignedInt)
            }
            _ => unsupported!("Function {name} does not exist"),
        };

        if args.next().is_some() {
            return Err(arity_error());
        }

        Ok(result)
    }
}

fn max_subsecond_digits(left: &DfType, right: &DfType) -> Option<u16> {
    match (left, right) {
        (
            DfType::Time {
                subsecond_digits: left,
            },
            DfType::Time {
                subsecond_digits: right,
            },
        ) => Some(cmp::max(*left, *right)),
        (_, _) => None,
    }
}

fn mysql_temporal_types_cvt(left: &DfType, right: &DfType) -> Option<DfType> {
    let left_is_date_and_time = left.is_date_and_time();
    let right_is_date_and_time = right.is_date_and_time();

    let left_is_temporal = left_is_date_and_time || left.is_any_temporal();
    let right_is_temporal = right_is_date_and_time || right.is_any_temporal();

    if left_is_temporal && right_is_temporal {
        if left_is_date_and_time && right_is_date_and_time {
            match (left, right) {
                (DfType::DateTime { subsecond_digits }, _) => Some(DfType::DateTime {
                    subsecond_digits: cmp::max(*subsecond_digits, right.subsecond_digits()?),
                }),
                (_, DfType::DateTime { subsecond_digits }) => Some(DfType::DateTime {
                    subsecond_digits: cmp::max(*subsecond_digits, left.subsecond_digits()?),
                }),
                (DfType::TimestampTz { .. }, DfType::TimestampTz { .. }) => {
                    Some(DfType::TimestampTz {
                        subsecond_digits: cmp::max(
                            left.subsecond_digits()?,
                            right.subsecond_digits()?,
                        ),
                    })
                }
                (left, right) => Some(DfType::Timestamp {
                    subsecond_digits: cmp::max(left.subsecond_digits()?, right.subsecond_digits()?),
                }),
            }
        } else if let Some(max) = max_subsecond_digits(left, right) {
            Some(DfType::Time {
                subsecond_digits: max,
            })
        } else if (matches!(left, DfType::Time { .. }) && right.is_any_int())
            || (matches!(right, DfType::Time { .. }) && left.is_any_int())
        {
            Some(DfType::BigInt)
        } else if left_is_date_and_time {
            Some(left.clone())
        } else if right_is_date_and_time {
            Some(right.clone())
        } else {
            Some(DfType::Date)
        }
    } else if (left_is_date_and_time && right.is_any_int())
        || (right_is_date_and_time && left.is_any_int())
    {
        Some(DfType::BigInt)
    } else if left_is_temporal && right.is_any_text() {
        Some(left.clone())
    } else if right_is_temporal && left.is_any_text() {
        Some(right.clone())
    } else {
        None
    }
}

fn get_text_type_max_length(ty: &DfType) -> Option<u16> {
    match ty {
        DfType::Text(..) => Some(65535),
        DfType::VarChar(ln, _) => Some(*ln),
        DfType::Char(ln, _) => Some(*ln),
        _ => None,
    }
}

fn mysql_text_type_cvt(left: &DfType, right: &DfType) -> Option<DfType> {
    if left.is_any_text() && right.is_any_text() {
        if matches!(left, DfType::Text(..)) || matches!(right, DfType::Text(..)) {
            Some(DfType::DEFAULT_TEXT)
        } else {
            let left_len = get_text_type_max_length(left)?;
            let right_len = get_text_type_max_length(right)?;
            match (left, right) {
                (DfType::Char(..), DfType::Char(..)) => Some(DfType::Char(
                    cmp::max(left_len, right_len),
                    Collation::default(),
                )),
                (_, _) => Some(DfType::VarChar(
                    cmp::max(left_len, right_len),
                    Collation::default(),
                )),
            }
        }
    } else {
        None
    }
}

fn mysql_numerical_type_cvt(left: &DfType, right: &DfType) -> Option<DfType> {
    if left.is_any_float() || right.is_any_float() {
        Some(DfType::Double)
    } else if left.is_any_int() && right.is_any_int() {
        if left.is_any_unsigned_int() || right.is_any_unsigned_int() {
            Some(DfType::UnsignedBigInt)
        } else {
            Some(DfType::BigInt)
        }
    } else {
        let left_is_decimal = left.is_numeric();
        let right_is_decimal = right.is_numeric();
        if left_is_decimal && right_is_decimal {
            // TODO: should return decimal, capable of storing max pres and scale
            Some(left.clone())
        } else if left_is_decimal && right.is_any_exact_number() {
            Some(left.clone())
        } else if left.is_any_exact_number() && right_is_decimal {
            Some(right.clone())
        } else {
            None
        }
    }
}

/// Handle the case where one or the other type is unknown, which occurs when one is a null literal.
/// In those cases, assuming no other type conversion rules apply, we should use the type of the
/// non-null operand.
fn mysql_null_type_cvt(left: &DfType, right: &DfType) -> Option<DfType> {
    if left.is_unknown() && !right.is_unknown() {
        Some(right.clone())
    } else if !left.is_unknown() && right.is_unknown() {
        Some(left.clone())
    } else {
        None
    }
}

/// <https://dev.mysql.com/doc/refman/8.0/en/type-conversion.html>
fn mysql_type_conversion_body(left_ty: &DfType, right_ty: &DfType) -> DfType {
    if left_ty.is_bool() && right_ty.is_bool() {
        DfType::Bool
    } else if let Some(ty) = mysql_text_type_cvt(left_ty, right_ty) {
        ty
    } else if let Some(ty) = mysql_temporal_types_cvt(left_ty, right_ty) {
        ty
    } else if let Some(ty) = mysql_numerical_type_cvt(left_ty, right_ty) {
        ty
    } else if let Some(ty) = mysql_null_type_cvt(left_ty, right_ty) {
        ty
    } else {
        DfType::Unknown
    }
}

fn mysql_type_conversion(left_ty: &DfType, right_ty: &DfType) -> DfType {
    let ty = mysql_type_conversion_body(left_ty, right_ty);
    if let DfType::Unknown = ty {
        DfType::Double
    } else {
        ty
    }
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
            Modulo => Ok((Self::Modulo, false)),
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
        dialect: Dialect,
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

            Err(invalid_query_err!(
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
            Add | Subtract | Multiply | Divide | Modulo | And | Or | Greater | GreaterOrEqual
            | Less | LessOrEqual | Is => match dialect.engine() {
                SqlEngine::PostgreSQL => Ok((None, None)),
                SqlEngine::MySQL => {
                    let ty = mysql_type_conversion(left_type, right_type);
                    Ok((Some(ty.clone()), Some(ty)))
                }
            },

            Like | ILike => Ok((
                coerce_to_text_type(left_type),
                coerce_to_text_type(right_type),
            )),

            Equal => match dialect.engine() {
                SqlEngine::PostgreSQL => Ok((None, Some(left_type.clone()))),
                SqlEngine::MySQL => {
                    let ty = mysql_type_conversion(left_type, right_type);
                    Ok((Some(ty.clone()), Some(ty)))
                }
            },

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
    fn output_type(
        &self,
        dialect: Dialect,
        left_type: &DfType,
        right_type: &DfType,
    ) -> ReadySetResult<DfType> {
        if let Some(ty) = match dialect.engine() {
            SqlEngine::MySQL => crate::promotion::mysql::output_type(left_type, self, right_type),
            SqlEngine::PostgreSQL => {
                crate::promotion::psql::output_type(left_type, self, right_type)
            }
        } {
            return if let DfType::Unknown = ty {
                Err(invalid_query_err!(
                    "operator does not exist: {} {} {}",
                    left_type,
                    self,
                    right_type
                ))
            } else {
                Ok(ty)
            };
        }

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

impl Expr {
    fn infer_case_result_type<'a>(
        then_types_it: impl Iterator<Item = &'a DfType>,
        else_type: &DfType,
    ) -> DfType {
        //  Create a new iterator for THEN expressions, which ignores NULL ones
        let mut then_types_it = then_types_it.filter(|ty| !matches!(ty, DfType::Unknown));

        //  Set result_type to either ELSE or THEN expression which is not null, or
        //  return from here if we don't have it.
        let mut result_type = if let DfType::Unknown = else_type {
            if let Some(ty) = then_types_it.next() {
                ty
            } else {
                //  At this point we don't have any not NULL expressions neither in THEN(s) nor in
                // ELSE.  We can't handle this case.
                return DfType::Unknown;
            }
        } else {
            else_type
        }
        .clone();

        //  At this point result_type can only be not null, so iterate over not null THEN
        //  expressions, and figure out common type between result_type and each THEN one.
        for ty in then_types_it {
            result_type = mysql_type_conversion_body(&result_type, ty);
            if let DfType::Unknown = result_type {
                //  At this point we found incompatible data types.
                //  Let's be compliant with MySQL, and coerce all to TEXT
                return DfType::DEFAULT_TEXT;
            }
        }

        result_type
    }

    /// Lower the given [`nom_sql`] AST expression to a dataflow expression
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
    pub fn lower<C>(expr: AstExpr, dialect: Dialect, context: &C) -> ReadySetResult<Self>
    where
        C: LowerContext,
    {
        match expr {
            AstExpr::Call(FunctionExpr::Call {
                name: fname,
                arguments,
            }) => {
                let args = arguments
                    .into_iter()
                    .map(|arg| Self::lower(arg, dialect, context))
                    .collect::<Result<Vec<_>, _>>()?;
                let (func, ty) = BuiltinFunction::from_name_and_args(&fname, args, dialect)?;
                Ok(Self::Call {
                    func: Box::new(func),
                    ty,
                })
            }
            AstExpr::Call(FunctionExpr::Substring { string, pos, len }) => {
                let string = Self::lower(*string, dialect, context)?;
                let ty = if string.ty().is_any_text() {
                    string.ty().clone()
                } else {
                    DfType::DEFAULT_TEXT
                };
                let func = Box::new(BuiltinFunction::Substring(
                    string,
                    pos.map(|expr| Self::lower(*expr, dialect, context))
                        .transpose()?,
                    len.map(|expr| Self::lower(*expr, dialect, context))
                        .transpose()?,
                ));

                Ok(Self::Call { func, ty })
            }
            AstExpr::Call(FunctionExpr::Extract { field, expr }) => {
                let expr = Self::lower(*expr, dialect, context)?;
                let ty = DfType::Numeric { prec: 20, scale: 6 };
                let func = Box::new(BuiltinFunction::Extract(field, expr));

                Ok(Self::Call { func, ty })
            }
            AstExpr::Call(call) => internal!(
                "Unexpected (aggregate?) call node in project expression: {:?}",
                Sensitive(&call)
            ),
            AstExpr::Literal(lit) => {
                let is_string_literal = lit.is_string();
                let val: DfValue = lit.try_into()?;
                // TODO: Infer type from SQL
                let ty = if is_string_literal && dialect.engine() == SqlEngine::PostgreSQL {
                    DfType::Unknown
                } else {
                    val.infer_dataflow_type()
                };

                Ok(Self::Literal { val, ty })
            }
            AstExpr::Column(col) => {
                let (index, ty) = context.resolve_column(col)?;
                Ok(Self::Column { index, ty })
            }
            AstExpr::BinaryOp { lhs, op, rhs } => {
                let mut left = Box::new(Self::lower(*lhs, dialect, context)?);
                let mut right = Box::new(Self::lower(*rhs, dialect, context)?);
                let (op, negated) =
                    BinaryOperator::from_sql_op(op, dialect, left.ty(), right.ty())?;

                if matches!(
                    op,
                    BinaryOperator::JsonPathExtract | BinaryOperator::JsonPathExtractUnquote
                ) {
                    unsupported!("'{op}' operator not implemented yet for MySQL");
                }

                let out = op.output_type(dialect, left.ty(), right.ty())?;
                let (left_coerce_target, right_coerce_target) =
                    op.argument_type_coercions(left.ty(), right.ty(), dialect)?;

                if let Some(ty) = left_coerce_target {
                    if ty != out {
                        left = Box::new(Self::Cast {
                            expr: left,
                            ty,
                            null_on_failure: false,
                        });
                    }
                }
                if let Some(ty) = right_coerce_target {
                    if ty != out {
                        right = Box::new(Self::Cast {
                            expr: right,
                            ty,
                            null_on_failure: false,
                        });
                    }
                }

                let op_node = Self::Op {
                    op,
                    left,
                    right,
                    ty: out,
                };
                if negated {
                    Ok(Self::Not {
                        expr: Box::new(op_node),
                        ty: DfType::Bool,
                    })
                } else {
                    Ok(op_node)
                }
            }
            AstExpr::OpAny { lhs, op, rhs } | AstExpr::OpSome { lhs, op, rhs } => {
                Self::lower_op_any_or_all(*lhs, op, *rhs, dialect, context, false)
            }
            AstExpr::OpAll { lhs, op, rhs } => {
                Self::lower_op_any_or_all(*lhs, op, *rhs, dialect, context, true)
            }
            AstExpr::UnaryOp {
                op: UnaryOperator::Neg,
                rhs,
            } => {
                let left = Box::new(Self::lower(*rhs, dialect, context)?);
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
            } => Ok(Self::Not {
                expr: Box::new(Self::lower(*rhs, dialect, context)?),
                ty: DfType::Bool, // type of NOT is always bool
            }),
            AstExpr::Cast {
                expr, ty: to_type, ..
            } => {
                let ty = DfType::from_sql_type(&to_type, dialect, |t| context.resolve_type(t))?;
                Ok(Self::Cast {
                    expr: Box::new(Self::lower(*expr, dialect, context)?),
                    ty,
                    null_on_failure: false,
                })
            }
            AstExpr::CaseWhen {
                branches,
                else_expr,
            } => {
                let branches = branches
                    .into_iter()
                    .map(|branch| {
                        let condition = Self::lower(branch.condition, dialect, context)?;
                        let body = Self::lower(branch.body, dialect, context)?;
                        Ok(CaseWhenBranch { condition, body })
                    })
                    .collect::<ReadySetResult<Vec<_>>>()?;
                let else_expr = match else_expr {
                    Some(else_expr) => Self::lower(*else_expr, dialect, context)?,
                    None => Self::Literal {
                        val: DfValue::None,
                        ty: DfType::Unknown,
                    },
                };
                let ty = Self::infer_case_result_type(
                    branches.iter().map(|branch| branch.body.ty()),
                    else_expr.ty(),
                );
                if let DfType::Unknown = ty {
                    Err(unsupported_err!(
                        "Can not infer result type for CASE expression"
                    ))
                } else {
                    Ok(Self::CaseWhen {
                        branches,
                        else_expr: Box::new(else_expr),
                        ty,
                    })
                }
            }
            AstExpr::In {
                lhs,
                rhs: InValue::List(exprs),
                negated,
            } => {
                let mut exprs = exprs.into_iter();
                if let Some(fst) = exprs.next() {
                    let logical_op = if negated {
                        BinaryOperator::And
                    } else {
                        BinaryOperator::Or
                    };

                    let lhs = Self::lower(*lhs, dialect, context)?;
                    let make_comparison = |rhs| -> ReadySetResult<_> {
                        let equal = Self::Op {
                            left: Box::new(lhs.clone()),
                            op: BinaryOperator::Equal,
                            right: Box::new(Self::lower(rhs, dialect, context)?),
                            ty: DfType::Bool, // type of = is always bool
                        };
                        if negated {
                            Ok(Self::Not {
                                expr: Box::new(equal),
                                ty: DfType::Bool,
                            })
                        } else {
                            Ok(equal)
                        }
                    };

                    exprs.try_fold(make_comparison(fst)?, |acc, rhs| {
                        Ok(Self::Op {
                            left: Box::new(acc),
                            op: logical_op,
                            right: Box::new(make_comparison(rhs)?),
                            ty: DfType::Bool, // type of and/or is always bool
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
            expr @ AstExpr::Array(_) => {
                fn find_shape(expr: &AstExpr, out: &mut Vec<usize>) {
                    if let AstExpr::Array(elems) = expr {
                        out.push(elems.len());
                        if let Some(elem) = elems.first() {
                            find_shape(elem, out)
                        }
                    }
                }

                fn flatten<C>(
                    expr: AstExpr,
                    out: &mut Vec<Expr>,
                    dialect: Dialect,
                    context: &C,
                ) -> ReadySetResult<()>
                where
                    C: LowerContext,
                {
                    match expr {
                        AstExpr::Array(exprs) => {
                            for expr in exprs {
                                flatten(expr, out, dialect, context)?;
                            }
                        }
                        _ => out.push(Expr::lower(expr, dialect, context)?),
                    }
                    Ok(())
                }

                let mut shape = vec![];
                let mut elements = vec![];
                find_shape(&expr, &mut shape);
                flatten(expr, &mut elements, dialect, context)?;

                let mut ty =
                    // Array exprs are only supported for postgresql
                    unify_postgres_types(elements.iter().map(|expr| expr.ty()).collect())?;

                for _ in &shape {
                    ty = DfType::Array(Box::new(ty));
                }

                Ok(Self::Array {
                    elements,
                    shape,
                    ty,
                })
            }
            AstExpr::Row { .. } => unsupported!("Row expressions not currently supported"),
            AstExpr::Exists(_) => unsupported!("EXISTS not currently supported"),
            AstExpr::Variable(_) => unsupported!("Variables not currently supported"),
            AstExpr::Between { .. } | AstExpr::NestedSelect(_) | AstExpr::In { .. } => {
                internal!(
                    "Expression should have been desugared earlier: {}",
                    expr.display(nom_sql::Dialect::MySQL)
                )
            }
        }
    }

    fn lower_op_any_or_all<C>(
        lhs: AstExpr,
        op: SqlBinaryOperator,
        rhs: AstExpr,
        dialect: Dialect,
        context: &C,
        mut is_all: bool,
    ) -> ReadySetResult<Expr>
    where
        C: LowerContext,
    {
        let mut left = Box::new(Self::lower(lhs, dialect, context)?);
        let mut right = Box::new(Self::lower(rhs, dialect, context)?);
        let (op, negated) = BinaryOperator::from_sql_op(op, dialect, left.ty(), right.ty())?;
        if negated {
            is_all = !is_all
        }

        let right_member_ty = if right.ty().is_array() {
            right.ty().innermost_array_type()
        } else if right.ty().is_unknown() {
            &DfType::Unknown
        } else {
            // localhost/postgres=# select 1 = any(1);
            // ERROR:  42809: op ANY/ALL (array) requires array on right side
            // LINE 1: select 1 = any(1);
            //                  ^
            // LOCATION:  make_scalar_array_op, parse_oper.c:814
            // Time: 0.396 ms
            invalid_query!("op ANY/ALL (array) requires an array on the right-hand side")
        };

        let ty = op.output_type(dialect, left.ty(), right_member_ty)?;
        if !ty.is_bool() {
            // localhost/noria=# select 1 + any('{1,2}');
            // ERROR:  42809: op ANY/ALL (array) requires operator to yield boolean
            // LINE 1: select 1 + any('{1,2}');
            //                  ^
            // LOCATION:  make_scalar_array_op, parse_oper.c:856
            // Time: 0.330 ms
            invalid_query!("op ANY/ALL (array) requires the operator to yield a boolean")
        }

        let (left_coerce_target, right_coerce_target) =
            op.argument_type_coercions(left.ty(), right_member_ty, dialect)?;

        if let Some(ty) = left_coerce_target {
            left = Box::new(Self::Cast {
                expr: left,
                ty,
                null_on_failure: false,
            })
        }
        if let Some(ty) = right_coerce_target {
            right = Box::new(Self::Cast {
                expr: right,
                ty: DfType::Array(Box::new(ty)),

                null_on_failure: false,
            })
        } else if !right.ty().is_array() {
            // Even if we don't need to cast the right member type to a target type, we still need
            // to cast the rhs to an array (but we can make it an array of UNKNOWN,
            // since we can leave the values alone)
            right = Box::new(Self::Cast {
                expr: right,
                ty: DfType::Array(Box::new(right_coerce_target.unwrap_or(DfType::Unknown))),
                null_on_failure: false,
            });
        }

        let op_node = if is_all {
            Self::OpAll {
                op,
                left,
                right,
                ty,
            }
        } else {
            Self::OpAny {
                op,
                left,
                right,
                ty,
            }
        };

        if negated {
            Ok(Self::Not {
                expr: Box::new(op_node),
                ty: DfType::Bool,
            })
        } else {
            Ok(op_node)
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use nom_sql::{
        parse_expr, BinaryOperator as AstBinaryOperator, Dialect as ParserDialect, Float, Literal,
    };
    use readyset_data::{Collation, PgEnumMetadata};

    use super::*;

    #[derive(Clone)]
    pub(crate) struct TestLowerContext<RC, RT> {
        resolve_column: RC,
        resolve_type: RT,
    }

    impl<RC, RT> LowerContext for TestLowerContext<RC, RT>
    where
        RC: Fn(Column) -> ReadySetResult<(usize, DfType)> + Clone,
        RT: Fn(Relation) -> Option<DfType> + Clone,
    {
        fn resolve_column(&self, col: Column) -> ReadySetResult<(usize, DfType)> {
            (self.resolve_column)(col)
        }

        fn resolve_type(&self, ty: Relation) -> Option<DfType> {
            (self.resolve_type)(ty)
        }
    }

    pub(crate) fn no_op_lower_context() -> impl LowerContext {
        TestLowerContext {
            resolve_column: |_| internal!(),
            resolve_type: |_| None,
        }
    }

    pub(crate) fn resolve_columns<F>(resolve_column: F) -> impl LowerContext
    where
        F: Fn(Column) -> ReadySetResult<(usize, DfType)> + Clone,
    {
        TestLowerContext {
            resolve_column,
            resolve_type: |_| None,
        }
    }

    pub(crate) fn resolve_types<F>(resolve_type: F) -> impl LowerContext
    where
        F: Fn(Relation) -> Option<DfType> + Clone,
    {
        TestLowerContext {
            resolve_column: |_| internal!(),
            resolve_type,
        }
    }

    #[test]
    fn postgresql_text_literaal() {
        // localhost/postgres=# select pg_typeof('abc');
        //  pg_typeof
        // -----------
        //  unknown

        let input = AstExpr::Literal("abc".into());
        let result =
            Expr::lower(input, Dialect::DEFAULT_POSTGRESQL, &no_op_lower_context()).unwrap();
        assert_eq!(result.ty(), &DfType::Unknown);
    }

    #[test]
    fn simple_column_reference() {
        let input = AstExpr::Column("t.x".into());
        let result = Expr::lower(
            input,
            Dialect::DEFAULT_MYSQL,
            &resolve_columns(|c| {
                if c == "t.x".into() {
                    Ok((0, DfType::Int))
                } else {
                    internal!("what's this column!?")
                }
            }),
        )
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
    fn cast_to_custom_type() {
        let input =
            parse_expr(ParserDialect::PostgreSQL, "cast('foo' as something.custom)").unwrap();
        let enum_ty = DfType::from_enum_variants(
            ["foo".into(), "bar".into()],
            Some(PgEnumMetadata {
                name: "custom".into(),
                schema: "something".into(),
                oid: 12345,
                array_oid: 12344,
            }),
        );
        let result = Expr::lower(
            input,
            Dialect::DEFAULT_POSTGRESQL,
            &resolve_types(|ty| {
                if ty.schema == Some("something".into()) && ty.name == "custom" {
                    Some(enum_ty.clone())
                } else {
                    None
                }
            }),
        )
        .unwrap();
        assert_eq!(
            result,
            Expr::Cast {
                expr: Box::new(Expr::Literal {
                    val: "foo".into(),
                    ty: DfType::Unknown
                }),
                ty: enum_ty,
                null_on_failure: false
            }
        );
    }

    #[test]
    fn call_coalesce() {
        let input = AstExpr::Call(FunctionExpr::Call {
            name: "coalesce".into(),
            arguments: vec![AstExpr::Column("t.x".into()), AstExpr::Literal(2.into())],
        });

        let result = Expr::lower(
            input,
            Dialect::DEFAULT_MYSQL,
            &resolve_columns(|c| {
                if c == "t.x".into() {
                    Ok((0, DfType::Int))
                } else {
                    internal!("what's this column!?")
                }
            }),
        )
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
    fn call_coalesce_uppercase() {
        let input = AstExpr::Call(FunctionExpr::Call {
            name: "COALESCE".into(),
            arguments: vec![AstExpr::Column("t.x".into()), AstExpr::Literal(2.into())],
        });

        let result = Expr::lower(
            input,
            Dialect::DEFAULT_MYSQL,
            &resolve_columns(|c| {
                if c == "t.x".into() {
                    Ok((0, DfType::Int))
                } else {
                    internal!("what's this column!?")
                }
            }),
        )
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
        let input = parse_expr(ParserDialect::MySQL, "concat('My', 'SQ', 'L')").unwrap();
        let res = Expr::lower(input, Dialect::DEFAULT_MYSQL, &no_op_lower_context()).unwrap();
        assert_eq!(
            res,
            Expr::Call {
                func: Box::new(BuiltinFunction::Concat(
                    Expr::Literal {
                        val: "My".into(),
                        ty: DfType::DEFAULT_TEXT,
                    },
                    vec![
                        Expr::Literal {
                            val: "SQ".into(),
                            ty: DfType::DEFAULT_TEXT,
                        },
                        Expr::Literal {
                            val: "L".into(),
                            ty: DfType::DEFAULT_TEXT,
                        },
                    ],
                )),
                ty: DfType::DEFAULT_TEXT,
            }
        );
    }

    #[test]
    fn substring_from_for() {
        let input = parse_expr(ParserDialect::MySQL, "substr(col from 1 for 7)").unwrap();
        let res = Expr::lower(
            input,
            Dialect::DEFAULT_MYSQL,
            &resolve_columns(|c| {
                if c == "col".into() {
                    Ok((0, DfType::Text(Collation::Citext)))
                } else {
                    internal!()
                }
            }),
        )
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
                        val: 1.into(),
                        ty: DfType::BigInt
                    }),
                    Some(Expr::Literal {
                        val: 7.into(),
                        ty: DfType::BigInt
                    })
                )),
                ty: DfType::Text(Collation::Citext)
            }
        )
    }

    #[test]
    fn substr_regular() {
        let input = parse_expr(ParserDialect::MySQL, "substr('abcdefghi', 1, 7)").unwrap();
        let res = Expr::lower(input, Dialect::DEFAULT_MYSQL, &no_op_lower_context()).unwrap();
        assert_eq!(
            res,
            Expr::Call {
                func: Box::new(BuiltinFunction::Substring(
                    Expr::Literal {
                        val: "abcdefghi".into(),
                        ty: DfType::DEFAULT_TEXT
                    },
                    Some(Expr::Literal {
                        val: 1.into(),
                        ty: DfType::BigInt
                    }),
                    Some(Expr::Literal {
                        val: 7.into(),
                        ty: DfType::BigInt
                    }),
                )),
                ty: DfType::DEFAULT_TEXT
            }
        )
    }

    #[test]
    fn substring_regular() {
        let input = parse_expr(ParserDialect::MySQL, "substring('abcdefghi', 1, 7)").unwrap();
        let res = Expr::lower(input, Dialect::DEFAULT_MYSQL, &no_op_lower_context()).unwrap();
        assert_eq!(
            res,
            Expr::Call {
                func: Box::new(BuiltinFunction::Substring(
                    Expr::Literal {
                        val: "abcdefghi".into(),
                        ty: DfType::DEFAULT_TEXT
                    },
                    Some(Expr::Literal {
                        val: 1.into(),
                        ty: DfType::BigInt
                    }),
                    Some(Expr::Literal {
                        val: 7.into(),
                        ty: DfType::BigInt
                    }),
                )),
                ty: DfType::DEFAULT_TEXT
            }
        )
    }

    #[test]
    fn substring_without_string_arg() {
        let input = parse_expr(ParserDialect::MySQL, "substring(123 from 2)").unwrap();
        let res = Expr::lower(input, Dialect::DEFAULT_MYSQL, &no_op_lower_context()).unwrap();
        assert_eq!(res.ty(), &DfType::DEFAULT_TEXT);
    }

    #[test]
    fn case_when_then() {
        fn get_case_result_type(stmt: &str) -> ReadySetResult<Expr> {
            let input = parse_expr(ParserDialect::MySQL, stmt).unwrap();
            Expr::lower(input, Dialect::DEFAULT_MYSQL, &no_op_lower_context())
        }

        const ERROR_EXPR: Expr = Expr::Literal {
            val: DfValue::None,
            ty: DfType::Unknown,
        };

        // 1st THEN is NULL and ELSE is missing
        assert_eq!(
            get_case_result_type("case when 1=1 then NULL when 2=2 then 'BCD' end")
                .unwrap()
                .ty(),
            &DfType::DEFAULT_TEXT
        );
        // 1st THEN is NULL and ELSE is NULL
        assert_eq!(
            get_case_result_type("case when 1=1 then NULL when 2=2 then 'BCD' else NULL end")
                .unwrap()
                .ty(),
            &DfType::DEFAULT_TEXT
        );
        // The THEN(s) are not NULL and ELSE is missing
        assert_eq!(
            get_case_result_type("case when 1=1 then 2 else 2.5 end")
                .unwrap()
                .ty(),
            &DfType::Double
        );
        // The THEN(s) are not NULL and ELSE is explicit NULL
        assert_eq!(
            get_case_result_type(
                "case when 1=1 then '01:01:01'::time  when 2=2 then '2024-01-01 12:30:30'::timestamp else NULL end"
            )
                .unwrap()
                .ty(),
            &DfType::Timestamp {
                subsecond_digits: 0
            }
        );
        // A middle THEN is NULL and ELSE is not NULL
        assert_eq!(
            get_case_result_type("case when 1=1 then 2 when 2=2 then NULL else 5 end")
                .unwrap()
                .ty(),
            &DfType::BigInt
        );
        // Negative test: The single THEN is NULL and ELSE is missing
        assert_eq!(
            get_case_result_type("case when 1=1 then NULL end")
                .unwrap_or(ERROR_EXPR)
                .ty(),
            &DfType::Unknown
        );
        // Incompatible THEN expressions
        assert_eq!(
            get_case_result_type("case when 1=1 then 2 when 2=2 then 'ABC' end")
                .unwrap()
                .ty(),
            &DfType::DEFAULT_TEXT
        );
        // Incompatible THEN and ELSE expressions
        assert_eq!(
            get_case_result_type("case when 1=1 then 2 else 'ABC' end")
                .unwrap()
                .ty(),
            &DfType::DEFAULT_TEXT
        );
    }

    #[test]
    fn greatest_inferred_type() {
        use Literal::Null;

        #[track_caller]
        fn infers_type(args: Vec<Literal>, dialect: Dialect, expected_ty: DfType) {
            let input = AstExpr::Call(FunctionExpr::Call {
                name: "greatest".into(),
                arguments: args.into_iter().map(AstExpr::Literal).collect(),
            });
            let result = Expr::lower(input, dialect, &no_op_lower_context()).unwrap();
            assert_eq!(result.ty(), &expected_ty);
        }

        infers_type(
            vec![Null, Null],
            Dialect::DEFAULT_POSTGRESQL,
            DfType::DEFAULT_TEXT,
        );

        infers_type(
            vec!["123".into(), 23.into()],
            Dialect::DEFAULT_POSTGRESQL,
            DfType::BigInt,
        );

        infers_type(
            vec!["123".into(), "456".into()],
            Dialect::DEFAULT_POSTGRESQL,
            DfType::DEFAULT_TEXT,
        );

        infers_type(
            vec!["123".into(), "456".into()],
            Dialect::DEFAULT_POSTGRESQL,
            DfType::DEFAULT_TEXT,
        );

        infers_type(
            vec![
                "123".into(),
                Literal::Float(Float {
                    value: 1.23,
                    precision: 2,
                }),
            ],
            Dialect::DEFAULT_POSTGRESQL,
            // TODO: should actually be DfType::Numeric { prec: 10, scale: 0 }, but our type
            // inference for float literals in pg is (currently) wrong
            DfType::Float,
        );

        infers_type(
            vec![Null, 23.into()],
            Dialect::DEFAULT_MYSQL,
            DfType::BigInt,
        );
        infers_type(vec![Null, Null], Dialect::DEFAULT_MYSQL, DfType::Binary(0));

        infers_type(
            vec![
                "123".into(),
                Literal::Float(Float {
                    value: 1.23,
                    precision: 2,
                }),
            ],
            Dialect::DEFAULT_MYSQL,
            DfType::DEFAULT_TEXT,
        );

        // TODO(ENG-1911)
        // infers_type(
        //     vec![
        //         123.into(),
        //         Literal::Float(Float {
        //             value: 1.23,
        //             precision: 2,
        //         }),
        //     ],
        //     MySQL,
        //     DfType::Numeric { prec: 5, scale: 2 },
        // );
    }

    #[test]
    fn greatest_compare_as() {
        use Literal::Null;

        #[track_caller]
        fn compares_as(args: Vec<Literal>, dialect: Dialect, expected_ty: DfType) {
            let input = AstExpr::Call(FunctionExpr::Call {
                name: "greatest".into(),
                arguments: args.into_iter().map(AstExpr::Literal).collect(),
            });
            let result = Expr::lower(input, dialect, &no_op_lower_context()).unwrap();
            let compare_as = match result {
                Expr::Call { func, .. } => match *func {
                    BuiltinFunction::Greatest { compare_as, .. } => compare_as,
                    _ => panic!("Wrong function"),
                },
                _ => panic!("Not a function call"),
            };
            assert_eq!(compare_as, expected_ty);
        }

        compares_as(
            vec![Null, Null],
            Dialect::DEFAULT_POSTGRESQL,
            DfType::DEFAULT_TEXT,
        );
        compares_as(vec![Null, Null], Dialect::DEFAULT_MYSQL, DfType::Unknown);
        compares_as(
            vec![Null, "a".into(), "b".into()],
            Dialect::DEFAULT_MYSQL,
            DfType::Unknown,
        );
        compares_as(
            vec![Null, "a".into(), "b".into()],
            Dialect::DEFAULT_POSTGRESQL,
            DfType::DEFAULT_TEXT,
        );
        compares_as(
            vec![12u64.into(), (-123).into()],
            Dialect::DEFAULT_MYSQL,
            DfType::BigInt,
        );
        // TODO(ENG-1909)
        // compares_as(vec![12u64.into(), (-123).into()], Dialect::DEFAULT_POSTGRESQL,
        // DfType::Int);
        compares_as(
            vec![12u64.into(), "123".into()],
            Dialect::DEFAULT_MYSQL,
            DfType::DEFAULT_TEXT,
        );
        compares_as(
            vec!["A".into(), "b".into(), 1.into()],
            Dialect::DEFAULT_MYSQL,
            DfType::DEFAULT_TEXT,
        );
    }

    #[test]
    fn eq_returns_bool() {
        let input = parse_expr(ParserDialect::MySQL, "x = 1").unwrap();
        let result = Expr::lower(
            input,
            Dialect::DEFAULT_MYSQL,
            &resolve_columns(|c| {
                if c.name == "x" {
                    Ok((0, DfType::DEFAULT_TEXT))
                } else {
                    internal!("what's this column?")
                }
            }),
        )
        .unwrap();
        assert_eq!(*result.ty(), DfType::Bool);
    }

    #[test]
    fn lowered_json_op_expr_types() {
        for op in [
            AstBinaryOperator::QuestionMark,
            AstBinaryOperator::QuestionMarkPipe,
            AstBinaryOperator::QuestionMarkAnd,
        ] {
            let input = AstExpr::BinaryOp {
                lhs: Box::new(AstExpr::Literal("{\"abc\": 42}".into())),
                op,
                rhs: Box::new(AstExpr::Literal("abc".into())),
            };
            let result =
                Expr::lower(input, Dialect::DEFAULT_POSTGRESQL, &no_op_lower_context()).unwrap();
            assert_eq!(*result.ty(), DfType::Bool);
        }
    }

    #[test]
    fn array_expr() {
        let expr = parse_expr(
            ParserDialect::PostgreSQL,
            "ARRAY[[1, '2'::int], array[3, 4]]",
        )
        .unwrap();
        let result =
            Expr::lower(expr, Dialect::DEFAULT_POSTGRESQL, &no_op_lower_context()).unwrap();
        assert_eq!(
            result,
            Expr::Array {
                elements: vec![
                    Expr::Literal {
                        val: 1u32.into(),
                        ty: DfType::BigInt
                    },
                    Expr::Cast {
                        expr: Box::new(Expr::Literal {
                            val: "2".into(),
                            ty: DfType::Unknown
                        }),
                        ty: DfType::Int,
                        null_on_failure: false
                    },
                    Expr::Literal {
                        val: 3u32.into(),
                        ty: DfType::BigInt
                    },
                    Expr::Literal {
                        val: 4u32.into(),
                        ty: DfType::BigInt
                    }
                ],
                shape: vec![2, 2],
                ty: DfType::Array(Box::new(DfType::Array(Box::new(DfType::BigInt))))
            }
        )
    }

    #[test]
    fn op_some_to_any() {
        let expr = parse_expr(ParserDialect::PostgreSQL, "1 = ANY ('{1,2}')").unwrap();
        let result =
            Expr::lower(expr, Dialect::DEFAULT_POSTGRESQL, &no_op_lower_context()).unwrap();
        assert_eq!(
            result,
            Expr::OpAny {
                op: BinaryOperator::Equal,
                left: Box::new(Expr::Literal {
                    val: 1u64.into(),
                    ty: DfType::BigInt
                }),
                right: Box::new(Expr::Cast {
                    expr: Box::new(Expr::Literal {
                        val: "{1,2}".into(),
                        ty: DfType::Unknown
                    }),
                    ty: DfType::Array(Box::new(DfType::BigInt)),
                    null_on_failure: false
                }),
                ty: DfType::Bool
            }
        );
    }

    #[test]
    fn op_all() {
        let expr = parse_expr(ParserDialect::PostgreSQL, "1 = ALL ('{1,1}')").unwrap();
        let result =
            Expr::lower(expr, Dialect::DEFAULT_POSTGRESQL, &no_op_lower_context()).unwrap();
        assert_eq!(
            result,
            Expr::OpAll {
                op: BinaryOperator::Equal,
                left: Box::new(Expr::Literal {
                    val: 1u64.into(),
                    ty: DfType::BigInt
                }),
                right: Box::new(Expr::Cast {
                    expr: Box::new(Expr::Literal {
                        val: "{1,1}".into(),
                        ty: DfType::Unknown
                    }),
                    ty: DfType::Array(Box::new(DfType::BigInt)),
                    null_on_failure: false
                }),
                ty: DfType::Bool
            }
        );
    }

    #[test]
    fn array_to_string() {
        let expr = parse_expr(
            ParserDialect::PostgreSQL,
            "array_to_string(ARRAY[1], ',', '*')",
        )
        .unwrap();
        let result =
            Expr::lower(expr, Dialect::DEFAULT_POSTGRESQL, &no_op_lower_context()).unwrap();
        assert_eq!(
            result,
            Expr::Call {
                func: Box::new(BuiltinFunction::ArrayToString(
                    Expr::Array {
                        elements: vec![Expr::Literal {
                            val: 1u64.into(),
                            ty: DfType::BigInt,
                        }],
                        shape: vec![1],
                        ty: DfType::Array(Box::new(DfType::BigInt))
                    },
                    Expr::Literal {
                        val: ",".into(),
                        ty: DfType::Unknown
                    },
                    Some(Expr::Literal {
                        val: "*".into(),
                        ty: DfType::Unknown
                    })
                )),
                ty: DfType::DEFAULT_TEXT
            }
        );
    }

    mod binary_operator {
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
                    op.output_type(
                        Dialect::DEFAULT_POSTGRESQL,
                        &left_type,
                        &DfType::DEFAULT_TEXT
                    )
                    .unwrap(),
                    output_type
                );
            }

            #[track_caller]
            fn test_json_key_path_extract(
                op: BinaryOperator,
                left_type: DfType,
                output_type: DfType,
            ) {
                assert_eq!(
                    op.output_type(
                        Dialect::DEFAULT_POSTGRESQL,
                        &left_type,
                        &DfType::Array(Box::new(DfType::DEFAULT_TEXT))
                    )
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
}
