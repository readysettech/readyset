mod like;

use std::borrow::Borrow;
use std::cmp::min;
use std::convert::TryFrom;
use std::fmt;
use std::fmt::Formatter;
use std::ops::{Add, Sub};

use chrono::{Datelike, LocalResult, NaiveDate, NaiveDateTime, TimeZone};
use chrono_tz::Tz;
use maths::int::integer_rnd;
use mysql_time::MysqlTime;
use nom_sql::{BinaryOperator, SqlType};
use noria_data::DataType;
use noria_errors::{ReadySetError, ReadySetResult};
use rust_decimal::prelude::ToPrimitive;
use serde::{Deserialize, Serialize};

use crate::like::{CaseInsensitive, CaseSensitive, LikePattern};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum BuiltinFunction {
    /// convert_tz(expr, expr, expr)
    ConvertTZ(Box<Expression>, Box<Expression>, Box<Expression>),
    /// dayofweek(expr)
    DayOfWeek(Box<Expression>),
    /// ifnull(expr, expr)
    IfNull(Box<Expression>, Box<Expression>),
    /// month(expr)
    Month(Box<Expression>),
    /// timediff(expr, expr)
    Timediff(Box<Expression>, Box<Expression>),
    /// addtime(expr, expr)
    Addtime(Box<Expression>, Box<Expression>),
    /// round(expr, prec)
    Round(Box<Expression>, Box<Expression>),
}

impl BuiltinFunction {
    pub fn from_name_and_args<A>(name: &str, args: A) -> Result<Self, ReadySetError>
    where
        A: IntoIterator<Item = Expression>,
    {
        let mut args = args.into_iter();
        match name {
            "convert_tz" => {
                let arity_error = || ReadySetError::ArityError("convert_tz".to_owned());
                Ok(Self::ConvertTZ(
                    Box::new(args.next().ok_or_else(arity_error)?),
                    Box::new(args.next().ok_or_else(arity_error)?),
                    Box::new(args.next().ok_or_else(arity_error)?),
                ))
            }
            "dayofweek" => {
                let arity_error = || ReadySetError::ArityError("dayofweek".to_owned());
                Ok(Self::DayOfWeek(Box::new(
                    args.next().ok_or_else(arity_error)?,
                )))
            }
            "ifnull" => {
                let arity_error = || ReadySetError::ArityError("ifnull".to_owned());
                Ok(Self::IfNull(
                    Box::new(args.next().ok_or_else(arity_error)?),
                    Box::new(args.next().ok_or_else(arity_error)?),
                ))
            }
            "month" => {
                let arity_error = || ReadySetError::ArityError("month".to_owned());
                Ok(Self::Month(Box::new(args.next().ok_or_else(arity_error)?)))
            }
            "timediff" => {
                let arity_error = || ReadySetError::ArityError("timediff".to_owned());
                Ok(Self::Timediff(
                    Box::new(args.next().ok_or_else(arity_error)?),
                    Box::new(args.next().ok_or_else(arity_error)?),
                ))
            }
            "addtime" => {
                let arity_error = || ReadySetError::ArityError("addtime".to_owned());
                Ok(Self::Addtime(
                    Box::new(args.next().ok_or_else(arity_error)?),
                    Box::new(args.next().ok_or_else(arity_error)?),
                ))
            }
            "round" => {
                let arity_error = || ReadySetError::ArityError("round".to_owned());
                Ok(Self::Round(
                    Box::new(args.next().ok_or_else(arity_error)?),
                    Box::new(args.next().unwrap_or(Expression::Literal(DataType::Int(0)))),
                ))
            }
            _ => Err(ReadySetError::NoSuchFunction(name.to_owned())),
        }
    }
}

impl fmt::Display for BuiltinFunction {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        use BuiltinFunction::*;

        match self {
            ConvertTZ(arg1, arg2, arg3) => {
                write!(f, "convert_tz({},{},{})", arg1, arg2, arg3)
            }
            DayOfWeek(arg) => {
                write!(f, "dayofweek({})", arg)
            }
            IfNull(arg1, arg2) => {
                write!(f, "ifnull({}, {})", arg1, arg2)
            }
            Month(arg) => {
                write!(f, "month({})", arg)
            }
            Timediff(arg1, arg2) => {
                write!(f, "timediff({}, {})", arg1, arg2)
            }
            Addtime(arg1, arg2) => {
                write!(f, "addtime({}, {})", arg1, arg2)
            }
            Round(arg1, precision) => {
                write!(f, "round({}, {})", arg1, precision)
            }
        }
    }
}

/// Expressions that can be evaluated during execution of a query
///
/// This type, which is the final lowered version of the original Expression AST, essentially
/// represents a desugared version of [`nom_sql::Expression`], with the following transformations
/// applied during lowering:
///
/// - Literals replaced with their corresponding [`DataType`]
/// - [Column references](nom_sql::Column) resolved into column indices in the parent node.
/// - Function calls resolved, and arities checked
/// - Desugaring x IN (y, z, ...) to `x = y OR x = z OR ...` and x NOT IN (y, z, ...) to `x != y AND
///   x = z AND ...`
///
/// During forward processing of dataflow, instances of these expressions are
/// [evaluated](Expression::eval) by both projection nodes and filter nodes.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Expression {
    /// A reference to a column, by index, in the parent node
    Column(usize),

    /// A literal DataType value
    Literal(DataType),

    /// A binary operation
    Op {
        op: BinaryOperator,
        left: Box<Expression>,
        right: Box<Expression>,
    },

    /// CAST(expr AS type)
    Cast(Box<Expression>, SqlType),

    Call(BuiltinFunction),

    CaseWhen {
        condition: Box<Expression>,
        then_expr: Box<Expression>,
        else_expr: Box<Expression>,
    },
}

impl fmt::Display for Expression {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use Expression::*;

        match self {
            Column(u) => write!(f, "{}", u),
            Literal(l) => write!(f, "(lit: {})", l),
            Op { op, left, right } => write!(f, "({} {} {})", left, op, right),
            Cast(expr, ty) => write!(f, "cast({} as {})", expr, ty),
            Call(func) => write!(f, "{}", func),
            CaseWhen {
                condition,
                then_expr,
                else_expr,
            } => write!(
                f,
                "case when {} then {} else {}",
                condition, then_expr, else_expr
            ),
        }
    }
}

macro_rules! try_cast_or_none {
    ($datatype:expr, $sqltype:expr) => {{
        match $datatype.coerce_to($sqltype) {
            Ok(v) => v,
            Err(_) => return Ok(DataType::None),
        }
    }};
}

macro_rules! get_time_or_default {
    ($datatype:expr) => {
        $datatype
            .coerce_to(&SqlType::Timestamp)
            .or($datatype.coerce_to(&SqlType::Time))
            .unwrap_or(DataType::None)
    };
}

macro_rules! non_null {
    ($datatype:expr) => {
        if let Some(dt) = $datatype.non_null() {
            dt
        } else {
            return Ok(DataType::None);
        }
    };
}

impl Expression {
    /// Evaluate this expression, given a source record to pull columns from
    pub fn eval<D>(&self, record: &[D]) -> ReadySetResult<DataType>
    where
        D: Borrow<DataType>,
    {
        use Expression::*;

        match self {
            Column(c) => record
                .get(*c)
                .map(|dt| dt.borrow().clone())
                .ok_or(ReadySetError::ProjectExpressionInvalidColumnIndex(*c)),
            Literal(dt) => Ok(dt.clone()),
            Op { op, left, right } => {
                use BinaryOperator::*;

                let left = left.eval(record)?;
                let right = right.eval(record)?;

                macro_rules! like {
                    ($case_sensitivity: expr, $negated: expr) => {{
                        match (
                            left.coerce_to(&SqlType::Text),
                            right.coerce_to(&SqlType::Text),
                        ) {
                            (Ok(left), Ok(right)) => {
                                // NOTE(grfn): At some point, we may want to optimize this to
                                // pre-cache the LikePattern if the value is constant, since
                                // constructing a new LikePattern can be kinda slow
                                let pat = LikePattern::new(
                                    // unwrap: we just coerced it to Text, so it's definitely a string
                                    String::try_from(right).unwrap().as_str(),
                                    $case_sensitivity,
                                );
                                let matches =
                                    // unwrap: we just coerced it to Text, so it's definitely a string
                                    pat.matches(String::try_from(left).unwrap().as_str());
                                Ok(if $negated { !matches } else { matches }.into())
                            }
                            // Anything that isn't Text or text-coercible can never be LIKE
                            // anything, so we return true if not negated, false otherwise
                            _ => Ok(DataType::from(!$negated)),
                        }
                    }};
                }

                match op {
                    Add => Ok((non_null!(left) + non_null!(right))?),
                    Subtract => Ok((non_null!(left) - non_null!(right))?),
                    Multiply => Ok((non_null!(left) * non_null!(right))?),
                    Divide => Ok((non_null!(left) / non_null!(right))?),
                    And => Ok((non_null!(left).is_truthy() && non_null!(right).is_truthy()).into()),
                    Or => Ok((non_null!(left).is_truthy() || non_null!(right).is_truthy()).into()),
                    Equal => Ok((non_null!(left) == non_null!(right)).into()),
                    NotEqual => Ok((non_null!(left) != non_null!(right)).into()),
                    Greater => Ok((non_null!(left) > non_null!(right)).into()),
                    GreaterOrEqual => Ok((non_null!(left) >= non_null!(right)).into()),
                    Less => Ok((non_null!(left) < non_null!(right)).into()),
                    LessOrEqual => Ok((non_null!(left) <= non_null!(right)).into()),
                    Is => Ok((left == right).into()),
                    IsNot => Ok((left != right).into()),
                    Like => like!(CaseSensitive, false),
                    NotLike => like!(CaseSensitive, true),
                    ILike => like!(CaseInsensitive, false),
                    NotILike => like!(CaseInsensitive, true),
                }
            }
            Cast(expr, ty) => Ok(expr.eval(record)?.coerce_to(ty)?),
            Call(func) => match func {
                BuiltinFunction::ConvertTZ(arg1, arg2, arg3) => {
                    let param1 = arg1.eval(record)?;
                    let param2 = arg2.eval(record)?;
                    let param3 = arg3.eval(record)?;
                    let param1_cast = try_cast_or_none!(param1, &SqlType::Timestamp);
                    let param2_cast = try_cast_or_none!(param2, &SqlType::Text);
                    let param3_cast = try_cast_or_none!(param3, &SqlType::Text);
                    match convert_tz(
                        &(NaiveDateTime::try_from(&param1_cast))?,
                        <&str>::try_from(&param2_cast)?,
                        <&str>::try_from(&param3_cast)?,
                    ) {
                        Ok(v) => Ok(DataType::TimestampTz(v.into())),
                        Err(_) => Ok(DataType::None),
                    }
                }
                BuiltinFunction::DayOfWeek(arg) => {
                    let param = arg.eval(record)?;
                    let param_cast = try_cast_or_none!(param, &SqlType::Date);
                    Ok(DataType::Int(
                        day_of_week(&(NaiveDate::try_from(&param_cast)?)) as i64,
                    ))
                }
                BuiltinFunction::IfNull(arg1, arg2) => {
                    let param1 = arg1.eval(record)?;
                    let param2 = arg2.eval(record)?;
                    if param1.is_none() {
                        Ok(param2)
                    } else {
                        Ok(param1)
                    }
                }
                BuiltinFunction::Month(arg) => {
                    let param = arg.eval(record)?;
                    let param_cast = try_cast_or_none!(param, &SqlType::Date);
                    Ok(DataType::UnsignedInt(
                        month(&(NaiveDate::try_from(non_null!(param_cast))?)) as u64,
                    ))
                }
                BuiltinFunction::Timediff(arg1, arg2) => {
                    let param1 = arg1.eval(record)?;
                    let param2 = arg2.eval(record)?;
                    let null_result = Ok(DataType::None);
                    let time_param1 = get_time_or_default!(param1);
                    let time_param2 = get_time_or_default!(param2);
                    if time_param1.is_none()
                        || time_param1
                            .sql_type()
                            .and_then(|st| time_param2.sql_type().map(|st2| (st, st2)))
                            .filter(|(st1, st2)| st1.eq(st2))
                            .is_none()
                    {
                        return null_result;
                    }
                    let time = if time_param1.is_datetime() {
                        timediff_datetimes(
                            &(NaiveDateTime::try_from(&time_param1)?),
                            &(NaiveDateTime::try_from(&time_param2)?),
                        )
                    } else {
                        timediff_times(
                            &(MysqlTime::try_from(&time_param1)?),
                            &(MysqlTime::try_from(&time_param2)?),
                        )
                    };
                    Ok(DataType::Time(time))
                }
                BuiltinFunction::Addtime(arg1, arg2) => {
                    let param1 = arg1.eval(record)?;
                    let param2 = arg2.eval(record)?;
                    let time_param2 = get_time_or_default!(param2);
                    if time_param2.is_datetime() {
                        return Ok(DataType::None);
                    }
                    let time_param1 = get_time_or_default!(param1);
                    if time_param1.is_datetime() {
                        Ok(DataType::TimestampTz(
                            addtime_datetime(
                                &(NaiveDateTime::try_from(&time_param1)?),
                                &(MysqlTime::try_from(&time_param2)?),
                            )
                            .into(),
                        ))
                    } else {
                        Ok(DataType::Time(addtime_times(
                            &(MysqlTime::try_from(&time_param1)?),
                            &(MysqlTime::try_from(&time_param2)?),
                        )))
                    }
                }
                BuiltinFunction::Round(arg1, arg2) => {
                    let expr = arg1.eval(record)?;
                    let param2 = arg2.eval(record)?;
                    let rnd_prec = match non_null!(param2) {
                        DataType::Int(inner) => *inner as i32,
                        DataType::UnsignedInt(inner) => *inner as i32,
                        DataType::Float(f, _) => f.round() as i32,
                        DataType::Double(f, _) => f.round() as i32,
                        DataType::Numeric(ref d) => {
                            // TODO(fran): I don't know if this is the right thing to do.
                            d.round().to_i32().ok_or_else(|| {
                                ReadySetError::BadRequest(format!(
                                    "NUMERIC value {} exceeds 32-byte integer size",
                                    d
                                ))
                            })?
                        }
                        _ => 0,
                    };

                    macro_rules! round {
                        ($real:expr, $prec:expr, $real_type:ty) => {{
                            let base: $real_type = 10.0;
                            if rnd_prec > 0 {
                                // If rounding precision is positive, than we keep the returned
                                // type as a float. We never return greater precision than was
                                // stored so we choose the minimum of stored precision or rounded
                                // precision.
                                let out_prec = min(*$prec, rnd_prec as u8);
                                let rounded_float = ($real * base.powf(out_prec as $real_type))
                                    .round()
                                    / base.powf(out_prec as $real_type);
                                let real = DataType::try_from(rounded_float).unwrap();
                                Ok(real)
                            } else {
                                // Rounding precision is negative, so we need to convert to a
                                // rounded int.
                                let rounded = (($real / base.powf(-rnd_prec as $real_type)).round()
                                    * base.powf(-rnd_prec as $real_type))
                                    as i64;
                                Ok(DataType::Int(rounded))
                            }
                        }};
                    }

                    match non_null!(expr) {
                        DataType::Float(float, prec) => round!(float, prec, f32),
                        DataType::Double(double, prec) => round!(double, prec, f64),
                        DataType::Int(val) => {
                            let rounded = integer_rnd(*val as i128, rnd_prec) as i64;
                            Ok(DataType::Int(rounded))
                        }
                        DataType::UnsignedInt(val) => {
                            let rounded = integer_rnd(*val as i128, rnd_prec) as u64;
                            Ok(DataType::UnsignedInt(rounded))
                        }
                        _ => Err(ReadySetError::ProjectExpressionBuiltInFunctionError {
                            function: "round".to_string(),
                            message: "expression does not result in a type that can be rounded."
                                .to_string(),
                        }),
                    }
                }
            },
            CaseWhen {
                condition,
                then_expr,
                else_expr,
            } => {
                if condition.eval(record)?.is_truthy() {
                    then_expr.eval(record)
                } else {
                    else_expr.eval(record)
                }
            }
        }
    }

    pub fn sql_type(
        &self,
        parent_column_type: impl Fn(usize) -> ReadySetResult<Option<SqlType>>,
    ) -> ReadySetResult<Option<SqlType>> {
        macro_rules! round {
            ($e1:expr, $prec:expr, $sql_type:expr) => {
                match $prec {
                    // Precision should always be coercable to a DataType::Int.
                    Expression::Literal(DataType::Int(p)) => {
                        if p < 0 {
                            // Precision is negative, which means that we will be returning a
                            // rounded Int.
                            Ok(Some(SqlType::Int(None)))
                        } else {
                            // Precision is positive so we will continue to return a Real.
                            Ok(Some($sql_type))
                        }
                    }
                    Expression::Literal(DataType::UnsignedInt(_)) => {
                        // Precision is positive so we will continue to return a Real.
                        Ok(Some($sql_type))
                    }
                    Expression::Literal(DataType::Double(f, _)) => {
                        if f.is_sign_negative() {
                            // Precision is negative, which means that we will be returning a
                            // rounded Int.
                            Ok(Some(SqlType::Int(None)))
                        } else {
                            // Precision is positive so we will continue to return a Real.
                            Ok(Some($sql_type))
                        }
                    }
                    Expression::Literal(DataType::Float(f, _)) => {
                        if f.is_sign_negative() {
                            // Precision is negative, which means that we will be returning a
                            // rounded Int.
                            Ok(Some(SqlType::Int(None)))
                        } else {
                            // Precision is positive so we will continue to return a Real.
                            Ok(Some($sql_type))
                        }
                    }
                    _ => $e1.sql_type(parent_column_type),
                }
            };
        }
        // TODO(grfn): Throughout this whole function we basically just assume everything
        // typechecks, which isn't great - but when we actually have a typechecker it'll be
        // attaching types to expressions ahead of time so this is just a stop-gap for now
        match self {
            Expression::Column(c) => parent_column_type(*c),
            Expression::Literal(l) => Ok(l.sql_type()),
            Expression::Op { left, .. } => left.sql_type(parent_column_type),
            Expression::Cast(_, typ) => Ok(Some(typ.clone())),
            Expression::Call(f) => match f {
                BuiltinFunction::ConvertTZ(input, _, _) => input.sql_type(parent_column_type),
                BuiltinFunction::DayOfWeek(_) => Ok(Some(SqlType::Int(None))),
                BuiltinFunction::IfNull(_, y) => y.sql_type(parent_column_type),
                BuiltinFunction::Month(_) => Ok(Some(SqlType::Int(None))),
                BuiltinFunction::Timediff(_, _) => Ok(Some(SqlType::Time)),
                BuiltinFunction::Addtime(e1, _) => e1.sql_type(parent_column_type),
                BuiltinFunction::Round(e1, prec) => match **e1 {
                    Expression::Literal(DataType::Float(_, _)) => {
                        round!(e1, **prec, SqlType::Float)
                    }
                    Expression::Literal(DataType::Double(_, _)) => {
                        round!(e1, **prec, SqlType::Real)
                    }
                    // For all other numeric types we always return the same type as they are.
                    Expression::Literal(DataType::UnsignedInt(_)) => {
                        Ok(Some(SqlType::UnsignedInt(None)))
                    }
                    Expression::Literal(DataType::Int(_)) => Ok(Some(SqlType::Int(None))),
                    _ => e1.sql_type(parent_column_type),
                },
            },
            Expression::CaseWhen { then_expr, .. } => then_expr.sql_type(parent_column_type),
        }
    }
}

/// Transforms a `[NaiveDateTime]` into a new one with a different timezone.
/// The `[NaiveDateTime]` is interpreted as having the timezone specified by the
/// `src` parameter, and then it's transformed to timezone specified by the `target` parameter.
pub fn convert_tz(
    datetime: &NaiveDateTime,
    src: &str,
    target: &str,
) -> ReadySetResult<NaiveDateTime> {
    let mk_err = |message: &str| ReadySetError::ProjectExpressionBuiltInFunctionError {
        function: "convert_tz".to_owned(),
        message: message.to_owned(),
    };

    let src_tz: Tz = src
        .parse()
        .map_err(|_| mk_err("Failed to parse the source timezone"))?;
    let target_tz: Tz = target
        .parse()
        .map_err(|_| mk_err("Failed to parse the target timezone"))?;

    let datetime_tz = match src_tz.from_local_datetime(datetime) {
        LocalResult::Single(dt) => dt,
        LocalResult::None => {
            return Err(mk_err(
                "Failed to transform the datetime to a different timezone",
            ))
        }
        LocalResult::Ambiguous(_, _) => {
            return Err(mk_err(
                "Failed to transform the datetime to a different timezone",
            ))
        }
    };

    Ok(datetime_tz.with_timezone(&target_tz).naive_local())
}

fn day_of_week(date: &NaiveDate) -> u8 {
    date.weekday().number_from_sunday() as u8
}

fn month(date: &NaiveDate) -> u8 {
    date.month() as u8
}

fn timediff_datetimes(time1: &NaiveDateTime, time2: &NaiveDateTime) -> MysqlTime {
    let duration = time1.sub(*time2);
    MysqlTime::new(duration)
}

fn timediff_times(time1: &MysqlTime, time2: &MysqlTime) -> MysqlTime {
    time1.sub(*time2)
}

fn addtime_datetime(time1: &NaiveDateTime, time2: &MysqlTime) -> NaiveDateTime {
    time2.add(*time1)
}

fn addtime_times(time1: &MysqlTime, time2: &MysqlTime) -> MysqlTime {
    time1.add(*time2)
}

#[cfg(test)]
mod tests {
    use std::convert::TryInto;

    use chrono::{Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike};
    use test_strategy::proptest;
    use Expression::*;

    use super::*;

    #[test]
    fn eval_column() {
        let expr = Column(1);
        assert_eq!(
            expr.eval(&[DataType::from(1), "two".try_into().unwrap()])
                .unwrap(),
            "two".try_into().unwrap()
        )
    }

    #[test]
    fn eval_literal() {
        let expr = Literal(1.into());
        assert_eq!(
            expr.eval(&[DataType::from(1), "two".try_into().unwrap()])
                .unwrap(),
            1.into()
        )
    }

    #[test]
    fn eval_add() {
        let expr = Op {
            left: Box::new(Column(0)),
            right: Box::new(Op {
                left: Box::new(Column(1)),
                right: Box::new(Literal(3.into())),
                op: BinaryOperator::Add,
            }),
            op: BinaryOperator::Add,
        };
        assert_eq!(
            expr.eval(&[DataType::from(1), DataType::from(2)]).unwrap(),
            6.into()
        );
    }

    #[test]
    fn eval_comparisons() {
        let dt = NaiveDateTime::new(
            NaiveDate::from_ymd(2009, 10, 17),
            NaiveTime::from_hms(12, 0, 0),
        );
        let text_dt: DataType = "2009-10-17 12:00:00".try_into().unwrap();
        let text_less_dt: DataType = "2009-10-16 12:00:00".try_into().unwrap();

        macro_rules! assert_op {
            ($binary_op:expr, $value:expr, $expected:expr) => {
                let expr = Op {
                    left: Box::new(Column(0)),
                    right: Box::new(Literal($value)),
                    op: $binary_op,
                };
                assert_eq!(
                    expr.eval::<DataType>(&[dt.into()]).unwrap(),
                    $expected.into()
                );
            };
        }
        assert_op!(BinaryOperator::Less, text_less_dt.clone(), 0u8);
        assert_op!(BinaryOperator::Less, text_dt.clone(), 0u8);
        assert_op!(BinaryOperator::LessOrEqual, text_less_dt.clone(), 0u8);
        assert_op!(BinaryOperator::LessOrEqual, text_dt.clone(), 1u8);
        assert_op!(BinaryOperator::Greater, text_less_dt.clone(), 1u8);
        assert_op!(BinaryOperator::Greater, text_dt.clone(), 0u8);
        assert_op!(BinaryOperator::GreaterOrEqual, text_less_dt.clone(), 1u8);
        assert_op!(BinaryOperator::GreaterOrEqual, text_dt.clone(), 1u8);
        assert_op!(BinaryOperator::Equal, text_less_dt, 0u8);
        assert_op!(BinaryOperator::Equal, text_dt, 1u8);
    }

    #[test]
    fn eval_cast() {
        let expr = Cast(Box::new(Column(0)), SqlType::Int(None));
        assert_eq!(
            expr.eval::<DataType>(&["1".try_into().unwrap(), "2".try_into().unwrap()])
                .unwrap(),
            1i32.into()
        );
    }

    #[test]
    fn eval_call_convert_tz() {
        let expr = Call(BuiltinFunction::ConvertTZ(
            Box::new(Column(0)),
            Box::new(Column(1)),
            Box::new(Column(2)),
        ));
        let datetime = NaiveDateTime::new(
            NaiveDate::from_ymd(2003, 10, 12),
            NaiveTime::from_hms(5, 13, 33),
        );
        let expected = NaiveDateTime::new(
            NaiveDate::from_ymd(2003, 10, 12),
            NaiveTime::from_hms(11, 58, 33),
        );
        let src = "Atlantic/Cape_Verde";
        let target = "Asia/Kathmandu";
        assert_eq!(
            expr.eval::<DataType>(&[
                datetime.into(),
                src.try_into().unwrap(),
                target.try_into().unwrap()
            ])
            .unwrap(),
            expected.into()
        );
        assert_eq!(
            expr.eval::<DataType>(&[
                datetime.into(),
                "invalid timezone".try_into().unwrap(),
                target.try_into().unwrap()
            ])
            .unwrap(),
            DataType::None
        );
        assert_eq!(
            expr.eval::<DataType>(&[
                datetime.into(),
                src.try_into().unwrap(),
                "invalid timezone".try_into().unwrap()
            ])
            .unwrap(),
            DataType::None
        );

        let string_datetime = datetime.to_string();
        assert_eq!(
            expr.eval::<DataType>(&[
                string_datetime.clone().try_into().unwrap(),
                src.try_into().unwrap(),
                target.try_into().unwrap()
            ])
            .unwrap(),
            expected.into()
        );

        assert_eq!(
            expr.eval::<DataType>(&[
                string_datetime.clone().try_into().unwrap(),
                "invalid timezone".try_into().unwrap(),
                target.try_into().unwrap()
            ])
            .unwrap(),
            DataType::None
        );
        assert_eq!(
            expr.eval::<DataType>(&[
                string_datetime.try_into().unwrap(),
                src.try_into().unwrap(),
                "invalid timezone".try_into().unwrap()
            ])
            .unwrap(),
            DataType::None
        );
    }

    #[test]
    fn eval_call_day_of_week() {
        let expr = Call(BuiltinFunction::DayOfWeek(Box::new(Column(0))));
        let expected = DataType::Int(2);

        let date = NaiveDate::from_ymd(2021, 3, 22); // Monday

        assert_eq!(expr.eval::<DataType>(&[date.into()]).unwrap(), expected);
        assert_eq!(
            expr.eval::<DataType>(&[date.to_string().try_into().unwrap()])
                .unwrap(),
            expected
        );

        let datetime = NaiveDateTime::new(
            date, // Monday
            NaiveTime::from_hms(18, 8, 00),
        );
        assert_eq!(expr.eval::<DataType>(&[datetime.into()]).unwrap(), expected);
        assert_eq!(
            expr.eval::<DataType>(&[datetime.to_string().try_into().unwrap()])
                .unwrap(),
            expected
        );
    }

    #[test]
    fn eval_call_if_null() {
        let expr = Call(BuiltinFunction::IfNull(
            Box::new(Column(0)),
            Box::new(Column(1)),
        ));
        let value = DataType::Int(2);

        assert_eq!(
            expr.eval(&[DataType::None, DataType::from(2)]).unwrap(),
            value
        );
        assert_eq!(
            expr.eval(&[DataType::from(2), DataType::from(3)]).unwrap(),
            value
        );

        let expr2 = Call(BuiltinFunction::IfNull(
            Box::new(Literal(DataType::None)),
            Box::new(Column(0)),
        ));
        assert_eq!(expr2.eval::<DataType>(&[2.into()]).unwrap(), value);

        let expr3 = Call(BuiltinFunction::IfNull(
            Box::new(Column(0)),
            Box::new(Literal(DataType::Int(2))),
        ));
        assert_eq!(expr3.eval(&[DataType::None]).unwrap(), value);
    }

    #[test]
    fn eval_call_month() {
        let expr = Call(BuiltinFunction::Month(Box::new(Column(0))));
        let datetime = NaiveDateTime::new(
            NaiveDate::from_ymd(2003, 10, 12),
            NaiveTime::from_hms(5, 13, 33),
        );
        let expected = 10_u32;
        assert_eq!(
            expr.eval(&[DataType::from(datetime)]).unwrap(),
            expected.into()
        );
        assert_eq!(
            expr.eval::<DataType>(&[datetime.to_string().try_into().unwrap()])
                .unwrap(),
            expected.into()
        );
        assert_eq!(
            expr.eval::<DataType>(&[datetime.date().into()]).unwrap(),
            expected.into()
        );
        assert_eq!(
            expr.eval::<DataType>(&[datetime.date().to_string().try_into().unwrap()])
                .unwrap(),
            expected.into()
        );
        assert_eq!(
            expr.eval::<DataType>(&["invalid date".try_into().unwrap()])
                .unwrap(),
            DataType::None
        );
    }

    #[test]
    fn eval_call_timediff() {
        let expr = Call(BuiltinFunction::Timediff(
            Box::new(Column(0)),
            Box::new(Column(1)),
        ));
        let param1 = NaiveDateTime::new(
            NaiveDate::from_ymd(2003, 10, 12),
            NaiveTime::from_hms(5, 13, 33),
        );
        let param2 = NaiveDateTime::new(
            NaiveDate::from_ymd(2003, 10, 14),
            NaiveTime::from_hms(4, 13, 33),
        );
        assert_eq!(
            expr.eval::<DataType>(&[param1.into(), param2.into()])
                .unwrap(),
            DataType::Time(MysqlTime::from_hmsus(false, 47, 0, 0, 0))
        );
        assert_eq!(
            expr.eval::<DataType>(&[
                param1.to_string().try_into().unwrap(),
                param2.to_string().try_into().unwrap()
            ])
            .unwrap(),
            DataType::Time(MysqlTime::from_hmsus(false, 47, 0, 0, 0))
        );
        let param1 = NaiveDateTime::new(
            NaiveDate::from_ymd(2003, 10, 12),
            NaiveTime::from_hms(5, 13, 33),
        );
        let param2 = NaiveDateTime::new(
            NaiveDate::from_ymd(2003, 10, 10),
            NaiveTime::from_hms(4, 13, 33),
        );
        assert_eq!(
            expr.eval::<DataType>(&[param1.into(), param2.into()])
                .unwrap(),
            DataType::Time(MysqlTime::from_hmsus(true, 49, 0, 0, 0))
        );
        assert_eq!(
            expr.eval::<DataType>(&[
                param1.to_string().try_into().unwrap(),
                param2.to_string().try_into().unwrap()
            ])
            .unwrap(),
            DataType::Time(MysqlTime::from_hmsus(true, 49, 0, 0, 0))
        );
        let param2 = NaiveTime::from_hms(4, 13, 33);
        assert_eq!(
            expr.eval::<DataType>(&[param1.into(), param2.into()])
                .unwrap(),
            DataType::None
        );
        assert_eq!(
            expr.eval::<DataType>(&[
                param1.to_string().try_into().unwrap(),
                param2.to_string().try_into().unwrap()
            ])
            .unwrap(),
            DataType::None
        );
        let param1 = NaiveTime::from_hms(5, 13, 33);
        assert_eq!(
            expr.eval::<DataType>(&[param1.into(), param2.into()])
                .unwrap(),
            DataType::Time(MysqlTime::from_hmsus(true, 1, 0, 0, 0))
        );
        assert_eq!(
            expr.eval::<DataType>(&[
                param1.to_string().try_into().unwrap(),
                param2.to_string().try_into().unwrap()
            ])
            .unwrap(),
            DataType::Time(MysqlTime::from_hmsus(true, 1, 0, 0, 0))
        );
        let param1 = "not a date nor time";
        let param2 = "01:00:00.4";
        assert_eq!(
            expr.eval::<DataType>(&[param1.try_into().unwrap(), param2.try_into().unwrap()])
                .unwrap(),
            DataType::Time(MysqlTime::from_hmsus(false, 1, 0, 0, 400_000))
        );
        assert_eq!(
            expr.eval::<DataType>(&[param2.try_into().unwrap(), param1.try_into().unwrap()])
                .unwrap(),
            DataType::Time(MysqlTime::from_hmsus(true, 1, 0, 0, 400_000))
        );

        let param2 = "10000.4";
        assert_eq!(
            expr.eval::<DataType>(&[param1.try_into().unwrap(), param2.try_into().unwrap()])
                .unwrap(),
            DataType::Time(MysqlTime::from_hmsus(false, 1, 0, 0, 400_000))
        );
        assert_eq!(
            expr.eval::<DataType>(&[param2.try_into().unwrap(), param1.try_into().unwrap()])
                .unwrap(),
            DataType::Time(MysqlTime::from_hmsus(true, 1, 0, 0, 400_000))
        );

        let param2: f32 = 3.57;
        assert_eq!(
            expr.eval::<DataType>(&[
                DataType::try_from(param1).unwrap(),
                DataType::try_from(param2).unwrap()
            ])
            .unwrap(),
            DataType::Time(MysqlTime::from_microseconds(
                (-param2 * 1_000_000_f32) as i64
            ))
        );

        let param2: f64 = 3.57;
        assert_eq!(
            expr.eval::<DataType>(&[
                DataType::try_from(param1).unwrap(),
                DataType::try_from(param2).unwrap()
            ])
            .unwrap(),
            DataType::Time(MysqlTime::from_microseconds(
                (-param2 * 1_000_000_f64) as i64
            ))
        );
    }

    #[test]
    fn eval_call_addtime() {
        let expr = Call(BuiltinFunction::Addtime(
            Box::new(Column(0)),
            Box::new(Column(1)),
        ));
        let param1 = NaiveDateTime::new(
            NaiveDate::from_ymd(2003, 10, 12),
            NaiveTime::from_hms(5, 13, 33),
        );
        let param2 = NaiveDateTime::new(
            NaiveDate::from_ymd(2003, 10, 14),
            NaiveTime::from_hms(4, 13, 33),
        );
        assert_eq!(
            expr.eval::<DataType>(&[param1.into(), param2.into()])
                .unwrap(),
            DataType::None
        );
        assert_eq!(
            expr.eval::<DataType>(&[
                param1.to_string().try_into().unwrap(),
                param2.to_string().try_into().unwrap()
            ])
            .unwrap(),
            DataType::None
        );
        let param2 = NaiveTime::from_hms(4, 13, 33);
        assert_eq!(
            expr.eval::<DataType>(&[param1.into(), param2.into()])
                .unwrap(),
            DataType::TimestampTz(
                NaiveDateTime::new(
                    NaiveDate::from_ymd(2003, 10, 12),
                    NaiveTime::from_hms(9, 27, 6),
                )
                .into()
            )
        );
        assert_eq!(
            expr.eval::<DataType>(&[
                param1.to_string().try_into().unwrap(),
                param2.to_string().try_into().unwrap()
            ])
            .unwrap(),
            DataType::TimestampTz(
                NaiveDateTime::new(
                    NaiveDate::from_ymd(2003, 10, 12),
                    NaiveTime::from_hms(9, 27, 6),
                )
                .into()
            )
        );
        let param2 = MysqlTime::from_hmsus(false, 3, 11, 35, 0);
        assert_eq!(
            expr.eval::<DataType>(&[param1.into(), param2.into()])
                .unwrap(),
            DataType::TimestampTz(
                NaiveDateTime::new(
                    NaiveDate::from_ymd(2003, 10, 12),
                    NaiveTime::from_hms(2, 1, 58),
                )
                .into()
            )
        );
        assert_eq!(
            expr.eval::<DataType>(&[
                param1.to_string().try_into().unwrap(),
                param2.to_string().try_into().unwrap()
            ])
            .unwrap(),
            DataType::TimestampTz(
                NaiveDateTime::new(
                    NaiveDate::from_ymd(2003, 10, 12),
                    NaiveTime::from_hms(2, 1, 58),
                )
                .into()
            )
        );
        let param1 = MysqlTime::from_hmsus(true, 10, 12, 44, 123_000);
        assert_eq!(
            expr.eval::<DataType>(&[param2.into(), param1.into()])
                .unwrap(),
            DataType::Time(MysqlTime::from_hmsus(true, 7, 1, 9, 123_000))
        );
        assert_eq!(
            expr.eval::<DataType>(&[
                param2.to_string().try_into().unwrap(),
                param1.to_string().try_into().unwrap()
            ])
            .unwrap(),
            DataType::Time(MysqlTime::from_hmsus(true, 7, 1, 9, 123_000))
        );
        let param1 = "not a date nor time";
        let param2 = "01:00:00.4";
        assert_eq!(
            expr.eval::<DataType>(&[param1.try_into().unwrap(), param2.try_into().unwrap()])
                .unwrap(),
            DataType::Time(MysqlTime::from_hmsus(true, 1, 0, 0, 400_000))
        );
        assert_eq!(
            expr.eval::<DataType>(&[param2.try_into().unwrap(), param1.try_into().unwrap()])
                .unwrap(),
            DataType::Time(MysqlTime::from_hmsus(true, 1, 0, 0, 400_000))
        );

        let param2 = "10000.4";
        assert_eq!(
            expr.eval::<DataType>(&[param1.try_into().unwrap(), param2.try_into().unwrap()])
                .unwrap(),
            DataType::Time(MysqlTime::from_hmsus(true, 1, 0, 0, 400_000))
        );
        assert_eq!(
            expr.eval::<DataType>(&[param2.try_into().unwrap(), param1.try_into().unwrap()])
                .unwrap(),
            DataType::Time(MysqlTime::from_hmsus(true, 1, 0, 0, 400_000))
        );

        let param2: f32 = 3.57;
        assert_eq!(
            expr.eval::<DataType>(&[
                param1.try_into().unwrap(),
                DataType::try_from(param2).unwrap()
            ])
            .unwrap(),
            DataType::Time(MysqlTime::from_microseconds(
                (param2 * 1_000_000_f32) as i64
            ))
        );

        let param2: f64 = 3.57;
        assert_eq!(
            expr.eval::<DataType>(&[
                param1.try_into().unwrap(),
                DataType::try_from(param2).unwrap()
            ])
            .unwrap(),
            DataType::Time(MysqlTime::from_microseconds(
                (param2 * 1_000_000_f64) as i64
            ))
        );
    }

    #[test]
    fn eval_call_round() {
        let expr = Call(BuiltinFunction::Round(
            Box::new(Column(0)),
            Box::new(Column(1)),
        ));
        let number: f64 = 4.12345;
        let precision = 3;
        let param1 = DataType::try_from(number).unwrap();
        let param2 = DataType::Int(precision);
        let want = DataType::try_from(4.123_f64).unwrap();
        assert_eq!(
            expr.eval::<DataType>(&[param1, param2.clone()]).unwrap(),
            want
        );

        let number: f32 = 4.12345;
        let param1 = DataType::try_from(number).unwrap();
        let want = DataType::try_from(4.123_f32).unwrap();
        assert_eq!(expr.eval::<DataType>(&[param1, param2]).unwrap(), want);
    }

    #[test]
    fn eval_call_round_with_negative_precision() {
        let expr = Call(BuiltinFunction::Round(
            Box::new(Column(0)),
            Box::new(Column(1)),
        ));
        let number: f64 = 52.12345;
        let precision = -1;
        let param1 = DataType::try_from(number).unwrap();
        let param2 = DataType::Int(precision);
        let want = DataType::try_from(50).unwrap();
        assert_eq!(
            expr.eval::<DataType>(&[param1, param2.clone()]).unwrap(),
            want
        );

        let number: f32 = 52.12345;
        let param1 = DataType::try_from(number).unwrap();
        assert_eq!(expr.eval::<DataType>(&[param1, param2]).unwrap(), want);
    }

    #[test]
    fn eval_call_round_with_float_precision() {
        let expr = Call(BuiltinFunction::Round(
            Box::new(Column(0)),
            Box::new(Column(1)),
        ));
        let number: f32 = 52.12345;
        let precision = -1.0_f64;
        let param1 = DataType::try_from(number).unwrap();
        let param2 = DataType::try_from(precision).unwrap();
        let want = DataType::try_from(50).unwrap();
        assert_eq!(
            expr.eval::<DataType>(&[param1, param2.clone()]).unwrap(),
            want,
        );

        let number: f64 = 52.12345;
        let param1 = DataType::try_from(number).unwrap();
        assert_eq!(expr.eval::<DataType>(&[param1, param2]).unwrap(), want);
    }

    // This is actually straight from MySQL:
    // mysql> SELECT ROUND(123.3, "banana");
    // +------------------------+
    // | ROUND(123.3, "banana") |
    // +------------------------+
    // |                    123 |
    // +------------------------+
    // 1 row in set, 2 warnings (0.00 sec)
    #[test]
    fn eval_call_round_with_banana() {
        let expr = Call(BuiltinFunction::Round(
            Box::new(Column(0)),
            Box::new(Column(1)),
        ));
        let number: f32 = 52.12345;
        let precision = "banana";
        let param1 = DataType::try_from(number).unwrap();
        let param2 = DataType::try_from(precision).unwrap();
        let want = DataType::try_from(52).unwrap();
        assert_eq!(
            expr.eval::<DataType>(&[param1, param2.clone()]).unwrap(),
            want,
        );

        let number: f64 = 52.12345;
        let param1 = DataType::try_from(number).unwrap();
        assert_eq!(expr.eval::<DataType>(&[param1, param2]).unwrap(), want,);
    }

    #[test]
    fn month_null() {
        let expr = Call(BuiltinFunction::Month(Box::new(Column(0))));
        assert_eq!(
            expr.eval::<DataType>(&[DataType::None]).unwrap(),
            DataType::None
        );
    }

    #[test]
    fn value_truthiness() {
        assert_eq!(
            Expression::Op {
                left: Box::new(Expression::Literal(1.into())),
                op: BinaryOperator::And,
                right: Box::new(Expression::Literal(3.into())),
            }
            .eval::<DataType>(&[])
            .unwrap(),
            1.into()
        );

        assert_eq!(
            Expression::Op {
                left: Box::new(Expression::Literal(1.into())),
                op: BinaryOperator::And,
                right: Box::new(Expression::Literal(0.into())),
            }
            .eval::<DataType>(&[])
            .unwrap(),
            0.into()
        );
    }

    #[test]
    fn eval_case_when() {
        let expr = Expression::CaseWhen {
            condition: Box::new(Op {
                left: Box::new(Expression::Column(0)),
                op: BinaryOperator::Equal,
                right: Box::new(Expression::Literal(1.into())),
            }),
            then_expr: Box::new(Expression::Literal("yes".try_into().unwrap())),
            else_expr: Box::new(Expression::Literal("no".try_into().unwrap())),
        };

        assert_eq!(
            expr.eval::<DataType>(&[1.into()]).unwrap(),
            DataType::try_from("yes").unwrap()
        );

        assert_eq!(
            expr.eval::<DataType>(&[DataType::from(8)]).unwrap(),
            DataType::try_from("no").unwrap()
        );
    }

    #[test]
    fn like_expr() {
        let expr = Expression::Op {
            left: Box::new(Expression::Literal("foo".into())),
            op: BinaryOperator::Like,
            right: Box::new(Expression::Literal("f%".into())),
        };
        let res = expr.eval::<DataType>(&[]).unwrap();
        assert!(res.is_truthy());
    }

    mod builtin_funcs {
        use launchpad::arbitrary::arbitrary_timestamp_naive_date_time;

        use super::*;

        // NOTE(Fran): We have to be careful when testing timezones, as the time difference
        //   between two timezones might differ depending on the date (due to daylight savings
        //   or by historical changes).
        #[proptest]
        fn convert_tz(#[strategy(arbitrary_timestamp_naive_date_time())] datetime: NaiveDateTime) {
            let src = "Atlantic/Cape_Verde";
            let target = "Asia/Kathmandu";
            let src_tz: Tz = src.parse().unwrap();
            let target_tz: Tz = target.parse().unwrap();
            let expected = src_tz
                .yo_opt(datetime.year(), datetime.ordinal())
                .and_hms_opt(datetime.hour(), datetime.minute(), datetime.second())
                .unwrap()
                .with_timezone(&target_tz)
                .naive_local();
            assert_eq!(super::convert_tz(&datetime, src, target).unwrap(), expected);
            assert!(super::convert_tz(&datetime, "invalid timezone", target).is_err());
            assert!(super::convert_tz(&datetime, src, "invalid timezone").is_err());
        }

        #[proptest]
        fn day_of_week(#[strategy(arbitrary_timestamp_naive_date_time())] datetime: NaiveDateTime) {
            let expected = datetime.weekday().number_from_sunday() as u8;
            assert_eq!(super::day_of_week(&datetime.date()), expected);
        }

        #[proptest]
        fn month(#[strategy(arbitrary_timestamp_naive_date_time())] datetime: NaiveDateTime) {
            let expected = datetime.month() as u8;
            assert_eq!(super::month(&datetime.date()), expected);
        }
    }
}
