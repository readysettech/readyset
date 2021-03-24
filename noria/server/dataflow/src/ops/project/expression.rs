use std::borrow::Cow;
use std::fmt;
use thiserror::Error;

use chrono::{Datelike, LocalResult, NaiveDate, NaiveDateTime, TimeZone};
use chrono_tz::Tz;
use nom_sql::{ArithmeticOperator, SqlType};
use noria::{DataType, ValueCoerceError};
use std::fmt::Formatter;

#[derive(Debug, Error)]
pub enum EvalError {
    /// An index in a [`Column`](ProjectExpression::Column) was out-of-bounds for the source record
    #[error("Column index out-of-bounds: {0}")]
    InvalidColumnIndex(usize),

    /// Error coercing a value, whether implicitly or as part of a [`Cast`](ProjectExpression::Cast)
    #[error(transparent)]
    CoerceError(#[from] ValueCoerceError),

    /// Error calling a built-in function.
    #[error(transparent)]
    CallError(#[from] BuiltinFunctionError),
}

/// Errors that can occur when calling a builtin function.
#[derive(Debug, Error)]
#[error("error running function {function:?}: {message}")]
pub struct BuiltinFunctionError {
    /// The function that failed
    pub function: String,
    /// A human-readable message for the error
    pub message: String,
    source: Option<anyhow::Error>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BuiltinFunction {
    // Timediff,
    /// convert_tz(expr, expr, expr)
    ConvertTZ(
        Box<ProjectExpression>,
        Box<ProjectExpression>,
        Box<ProjectExpression>,
    ),
    /// dayofweek(expr)
    DayOfWeek(Box<ProjectExpression>),
    /// ifnull(expr, expr)
    IfNull(Box<ProjectExpression>, Box<ProjectExpression>),
    /// month(expr)
    Month(Box<ProjectExpression>),
}

#[derive(Debug, Error)]
pub enum BuiltinFunctionConvertError {
    #[error("Function {0} does not exist")]
    NoSuchFunction(String),

    #[error("Incorrect parameter count in the call to native function '{0}'")]
    ArityError(String),
}

impl BuiltinFunction {
    pub fn from_name_and_args<A>(name: &str, args: A) -> Result<Self, BuiltinFunctionConvertError>
    where
        A: IntoIterator<Item = ProjectExpression>,
    {
        let mut args = args.into_iter();
        match name {
            "convert_tz" => {
                let arity_error =
                    || BuiltinFunctionConvertError::ArityError("convert_tz".to_owned());
                Ok(Self::ConvertTZ(
                    Box::new(args.next().ok_or_else(arity_error)?),
                    Box::new(args.next().ok_or_else(arity_error)?),
                    Box::new(args.next().ok_or_else(arity_error)?),
                ))
            }
            "dayofweek" => {
                let arity_error =
                    || BuiltinFunctionConvertError::ArityError("dayofweek".to_owned());
                Ok(Self::DayOfWeek(Box::new(
                    args.next().ok_or_else(arity_error)?,
                )))
            }
            "ifnull" => {
                let arity_error = || BuiltinFunctionConvertError::ArityError("ifnull".to_owned());
                Ok(Self::IfNull(
                    Box::new(args.next().ok_or_else(arity_error)?),
                    Box::new(args.next().ok_or_else(arity_error)?),
                ))
            }
            _ => Err(BuiltinFunctionConvertError::NoSuchFunction(name.to_owned())),
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
        }
    }
}

/// Expression AST for projection
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ProjectExpression {
    /// A reference to a column, by index, in the parent node
    Column(usize),

    /// A literal DataType value
    Literal(DataType),

    /// A binary operation
    Op {
        op: ArithmeticOperator,
        left: Box<ProjectExpression>,
        right: Box<ProjectExpression>,
    },

    /// CAST(expr AS type)
    Cast(Box<ProjectExpression>, SqlType),

    Call(BuiltinFunction),
}

impl fmt::Display for ProjectExpression {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use ProjectExpression::*;

        match self {
            Column(u) => write!(f, "{}", u),
            Literal(l) => write!(f, "(lit: {})", l),
            Op { op, left, right } => write!(f, "({} {} {})", left, op, right),
            Cast(expr, ty) => write!(f, "cast({} as {})", expr, ty),
            Call(func) => write!(f, "{}", func),
        }
    }
}

macro_rules! try_cast_or_none {
    ($datatype:expr, $sqltype:expr) => {
        match $datatype.coerce_to($sqltype) {
            Ok(v) => v,
            Err(_) => return Ok(Cow::Owned(DataType::None)),
        };
    };
}

impl ProjectExpression {
    /// Evaluate a [`ProjectExpression`] given a source record to pull columns from
    pub fn eval<'a>(&self, record: &'a [DataType]) -> Result<Cow<'a, DataType>, EvalError> {
        use ProjectExpression::*;

        match self {
            Column(c) => record
                .get(*c)
                .map(Cow::Borrowed)
                .ok_or(EvalError::InvalidColumnIndex(*c)),
            Literal(dt) => Ok(Cow::Owned(dt.clone())),
            Op { op, left, right } => {
                let left = left.eval(record)?;
                let right = right.eval(record)?;
                match op {
                    ArithmeticOperator::Add => Ok(Cow::Owned(left.as_ref() + right.as_ref())),
                    ArithmeticOperator::Subtract => Ok(Cow::Owned(left.as_ref() - right.as_ref())),
                    ArithmeticOperator::Multiply => Ok(Cow::Owned(left.as_ref() * right.as_ref())),
                    ArithmeticOperator::Divide => Ok(Cow::Owned(left.as_ref() / right.as_ref())),
                }
            }
            Cast(expr, ty) => match expr.eval(record)? {
                Cow::Borrowed(val) => Ok(val.coerce_to(ty)?),
                Cow::Owned(val) => Ok(Cow::Owned(val.coerce_to(ty)?.into_owned())),
            },
            Call(func) => match func {
                BuiltinFunction::ConvertTZ(arg1, arg2, arg3) => {
                    let param1 = arg1.eval(record)?;
                    let param2 = arg2.eval(record)?;
                    let param3 = arg3.eval(record)?;
                    let param1_cast = try_cast_or_none!(param1, &SqlType::Timestamp);
                    let param2_cast = try_cast_or_none!(param2, &SqlType::Text);
                    let param3_cast = try_cast_or_none!(param3, &SqlType::Text);
                    match convert_tz(
                        &(param1_cast.as_ref().into()),
                        param2_cast.as_ref().into(),
                        param3_cast.as_ref().into(),
                    ) {
                        Ok(v) => Ok(Cow::Owned(DataType::Timestamp(v))),
                        Err(_) => Ok(Cow::Owned(DataType::None)),
                    }
                }
                BuiltinFunction::DayOfWeek(arg) => {
                    let param = arg.eval(record)?;
                    let param_cast = try_cast_or_none!(param, &SqlType::Date);
                    Ok(Cow::Owned(DataType::Int(
                        day_of_week(&(param_cast.as_ref().into())) as i32,
                    )))
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
                    Ok(Cow::Owned(DataType::UnsignedInt(
                        month(&(param_cast.as_ref().into())) as u32,
                    )))
                }
            },
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
) -> Result<NaiveDateTime, BuiltinFunctionError> {
    let mk_err = |message: &str, source: Option<anyhow::Error>| BuiltinFunctionError {
        function: "convert_tz".to_owned(),
        message: message.to_owned(),
        source,
    };
    let src_tz: Tz = src
        .parse()
        .map_err(|_err: String| mk_err("Failed to parse the source timezone", None))?;
    let target_tz: Tz = target
        .parse()
        .map_err(|_err: String| mk_err("Failed to parse the target timezone", None))?;

    let datetime_tz = match src_tz.from_local_datetime(datetime) {
        LocalResult::Single(dt) => dt,
        LocalResult::None => {
            return Err(mk_err(
                "Failed to transform the datetime to a different timezone",
                None,
            ))
        }
        LocalResult::Ambiguous(_, _) => {
            return Err(mk_err(
                "Failed to transform the datetime to a different timezone",
                None,
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

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike};
    use test_strategy::proptest;
    use ProjectExpression::*;

    #[test]
    fn eval_column() {
        let expr = Column(1);
        assert_eq!(
            expr.eval(&[1.into(), "two".into()]).unwrap(),
            Cow::Owned("two".into())
        )
    }

    #[test]
    fn eval_literal() {
        let expr = Literal(1.into());
        assert_eq!(
            expr.eval(&[1.into(), "two".into()]).unwrap(),
            Cow::Owned(1.into())
        )
    }

    #[test]
    fn eval_add() {
        let expr = Op {
            left: Box::new(Column(0)),
            right: Box::new(Op {
                left: Box::new(Column(1)),
                right: Box::new(Literal(3.into())),
                op: ArithmeticOperator::Add,
            }),
            op: ArithmeticOperator::Add,
        };
        assert_eq!(
            expr.eval(&[1.into(), 2.into()]).unwrap(),
            Cow::Owned(6.into())
        );
    }

    #[test]
    fn eval_cast() {
        let expr = Cast(Box::new(Column(0)), SqlType::Int(32));
        assert_eq!(
            expr.eval(&["1".into(), "2".into()]).unwrap(),
            Cow::Owned(1i32.into())
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
            expr.eval(&[datetime.into(), src.into(), target.into()])
                .unwrap(),
            Cow::Owned(expected.into())
        );
        assert_eq!(
            expr.eval(&[datetime.into(), "invalid timezone".into(), target.into()])
                .unwrap(),
            Cow::Owned(DataType::None)
        );
        assert_eq!(
            expr.eval(&[datetime.into(), src.into(), "invalid timezone".into()])
                .unwrap(),
            Cow::Owned(DataType::None)
        );

        let string_datetime = datetime.to_string();
        assert_eq!(
            expr.eval(&[string_datetime.clone().into(), src.into(), target.into()])
                .unwrap(),
            Cow::Owned(expected.into())
        );

        assert_eq!(
            expr.eval(&[
                string_datetime.clone().into(),
                "invalid timezone".into(),
                target.into()
            ])
            .unwrap(),
            Cow::Owned(DataType::None)
        );
        assert_eq!(
            expr.eval(&[
                string_datetime.into(),
                src.into(),
                "invalid timezone".into()
            ])
            .unwrap(),
            Cow::Owned(DataType::None)
        );
    }

    #[test]
    fn eval_call_day_of_week() {
        let expr = Call(BuiltinFunction::DayOfWeek(Box::new(Column(0))));
        let expected = Cow::Owned(DataType::Int(2).into());

        let date = NaiveDate::from_ymd(2021, 3, 22); // Monday

        assert_eq!(expr.eval(&[date.into()]).unwrap(), expected);
        assert_eq!(expr.eval(&[date.to_string().into()]).unwrap(), expected);

        let datetime = NaiveDateTime::new(
            date, // Monday
            NaiveTime::from_hms(18, 08, 00),
        );
        assert_eq!(expr.eval(&[datetime.into()]).unwrap(), expected);
        assert_eq!(expr.eval(&[datetime.to_string().into()]).unwrap(), expected);
    }

    #[test]
    fn eval_call_if_null() {
        let expr = Call(BuiltinFunction::IfNull(
            Box::new(Column(0)),
            Box::new(Column(1)),
        ));
        let value = Cow::Owned(DataType::Int(2).into());

        assert_eq!(expr.eval(&[DataType::None, 2.into()]).unwrap(), value);
        assert_eq!(expr.eval(&[2.into(), 3.into()]).unwrap(), value);

        let expr2 = Call(BuiltinFunction::IfNull(
            Box::new(Literal(DataType::None)),
            Box::new(Column(0)),
        ));
        assert_eq!(expr2.eval(&[2.into()]).unwrap(), value);

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
        let expected = 10 as u32;
        assert_eq!(
            expr.eval(&[datetime.into()]).unwrap(),
            Cow::Owned(expected.into())
        );
        assert_eq!(
            expr.eval(&[datetime.to_string().into()]).unwrap(),
            Cow::Owned(expected.into())
        );
        assert_eq!(
            expr.eval(&[datetime.date().into()]).unwrap(),
            Cow::Owned(expected.into())
        );
        assert_eq!(
            expr.eval(&[datetime.date().to_string().into()]).unwrap(),
            Cow::Owned(expected.into())
        );
        assert_eq!(
            expr.eval(&["invalid date".into()]).unwrap(),
            Cow::Owned(DataType::None)
        );
    }

    mod builtin_funcs {
        use super::*;
        use launchpad::arbitrary::arbitrary_timestamp_naive_date_time;

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
