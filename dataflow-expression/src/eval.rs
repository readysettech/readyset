use std::borrow::Borrow;
use std::convert::TryFrom;
use std::ops::{Add, Div, Mul, Sub};
use std::str::FromStr;

use chrono::{Datelike, LocalResult, NaiveDate, NaiveDateTime, TimeZone};
use chrono_tz::Tz;
use launchpad::redacted::Sensitive;
use maths::int::integer_rnd;
use mysql_time::MysqlTime;
use nom_sql::{BinaryOperator, SqlType};
use readyset_data::DfValue;
use readyset_errors::{ReadySetError, ReadySetResult};
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rust_decimal::Decimal;

use crate::like::{CaseInsensitive, CaseSensitive, LikePattern};
use crate::{utils, BuiltinFunction, Expr};

macro_rules! try_cast_or_none {
    ($df_value:expr, $sqltype:expr, $from_sqltype:expr) => {{
        match $df_value.coerce_to($sqltype, $from_sqltype) {
            Ok(v) => v,
            Err(_) => return Ok(DfValue::None),
        }
    }};
}

macro_rules! get_time_or_default {
    ($df_value:expr, $from_sqltype:expr) => {
        $df_value
            .coerce_to(&SqlType::Timestamp, $from_sqltype)
            .or($df_value.coerce_to(&SqlType::Time, $from_sqltype))
            .unwrap_or(DfValue::None)
    };
}

macro_rules! non_null {
    ($df_value:expr) => {
        if let Some(val) = $df_value.non_null() {
            val
        } else {
            return Ok(DfValue::None);
        }
    };
}

/// Transforms a `[NaiveDateTime]` into a new one with a different timezone.
/// The `[NaiveDateTime]` is interpreted as having the timezone specified by the
/// `src` parameter, and then it's transformed to timezone specified by the `target` parameter.
pub fn convert_tz(
    datetime: &NaiveDateTime,
    src: &str,
    target: &str,
) -> ReadySetResult<NaiveDateTime> {
    let mk_err = |message: &str| ReadySetError::ProjectExprBuiltInFunctionError {
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

impl Expr {
    /// Evaluate this expression, given a source record to pull columns from
    pub fn eval<D>(&self, record: &[D]) -> ReadySetResult<DfValue>
    where
        D: Borrow<DfValue>,
    {
        use Expr::*;

        // TODO: Enforce type coercion
        match self {
            Column { index, .. } => record
                .get(*index)
                .map(|dt| dt.borrow().clone())
                .ok_or(ReadySetError::ProjectExprInvalidColumnIndex(*index)),
            Literal { val, .. } => Ok(val.clone()),
            Op {
                op, left, right, ..
            } => {
                use BinaryOperator::*;

                let left_ty = left.ty();
                let right_ty = right.ty();

                let left = left.eval(record)?;
                let right = right.eval(record)?;

                macro_rules! like {
                    ($case_sensitivity: expr, $negated: expr) => {{
                        match (
                            left.coerce_to(&SqlType::Text, left_ty),
                            right.coerce_to(&SqlType::Text, right_ty),
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
                            _ => Ok(DfValue::from(!$negated)),
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
            Cast { expr, to_type, .. } => {
                let res = expr.eval(record)?;
                Ok(res.coerce_to(to_type, expr.ty())?)
            }
            Call { func, .. } => match &**func {
                BuiltinFunction::ConvertTZ(arg1, arg2, arg3) => {
                    let param1 = arg1.eval(record)?;
                    let param2 = arg2.eval(record)?;
                    let param3 = arg3.eval(record)?;
                    let param1_cast = try_cast_or_none!(param1, &SqlType::Timestamp, arg1.ty());
                    let param2_cast = try_cast_or_none!(param2, &SqlType::Text, arg2.ty());
                    let param3_cast = try_cast_or_none!(param3, &SqlType::Text, arg3.ty());
                    match convert_tz(
                        &(NaiveDateTime::try_from(&param1_cast))?,
                        <&str>::try_from(&param2_cast)?,
                        <&str>::try_from(&param3_cast)?,
                    ) {
                        Ok(v) => Ok(DfValue::TimestampTz(v.into())),
                        Err(_) => Ok(DfValue::None),
                    }
                }
                BuiltinFunction::DayOfWeek(arg) => {
                    let param = arg.eval(record)?;
                    let param_cast = try_cast_or_none!(param, &SqlType::Date, arg.ty());
                    Ok(DfValue::Int(
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
                    let param_cast = try_cast_or_none!(param, &SqlType::Date, arg.ty());
                    Ok(DfValue::UnsignedInt(
                        month(&(NaiveDate::try_from(non_null!(param_cast))?)) as u64,
                    ))
                }
                BuiltinFunction::Timediff(arg1, arg2) => {
                    let param1 = arg1.eval(record)?;
                    let param2 = arg2.eval(record)?;
                    let null_result = Ok(DfValue::None);
                    let time_param1 = get_time_or_default!(param1, arg1.ty());
                    let time_param2 = get_time_or_default!(param2, arg2.ty());
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
                    Ok(DfValue::Time(time))
                }
                BuiltinFunction::Addtime(arg1, arg2) => {
                    let param1 = arg1.eval(record)?;
                    let param2 = arg2.eval(record)?;
                    let time_param2 = get_time_or_default!(param2, arg2.ty());
                    if time_param2.is_datetime() {
                        return Ok(DfValue::None);
                    }
                    let time_param1 = get_time_or_default!(param1, arg1.ty());
                    if time_param1.is_datetime() {
                        Ok(DfValue::TimestampTz(
                            addtime_datetime(
                                &(NaiveDateTime::try_from(&time_param1)?),
                                &(MysqlTime::try_from(&time_param2)?),
                            )
                            .into(),
                        ))
                    } else {
                        Ok(DfValue::Time(addtime_times(
                            &(MysqlTime::try_from(&time_param1)?),
                            &(MysqlTime::try_from(&time_param2)?),
                        )))
                    }
                }
                BuiltinFunction::Round(arg1, arg2) => {
                    let expr = arg1.eval(record)?;
                    let param2 = arg2.eval(record)?;
                    let rnd_prec = match non_null!(param2) {
                        DfValue::Int(inner) => *inner as i32,
                        DfValue::UnsignedInt(inner) => *inner as i32,
                        DfValue::Float(f) => f.round() as i32,
                        DfValue::Double(f) => f.round() as i32,
                        DfValue::Numeric(ref d) => {
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
                        ($real:expr, $real_type:ty) => {{
                            let base: $real_type = 10.0;
                            if rnd_prec > 0 {
                                // If rounding precision is positive, than we keep the returned
                                // type as a float. We never return greater precision than was
                                // stored so we choose the minimum of stored precision or rounded
                                // precision.
                                let rounded_float = ($real * base.powf(rnd_prec as $real_type))
                                    .round()
                                    / base.powf(rnd_prec as $real_type);
                                let real = DfValue::try_from(rounded_float).unwrap();
                                Ok(real)
                            } else {
                                // Rounding precision is negative, so we need to zero out some
                                // digits.
                                let rounded_float = (($real / base.powf(-rnd_prec as $real_type))
                                    .round()
                                    * base.powf(-rnd_prec as $real_type));
                                let real = DfValue::try_from(rounded_float).unwrap();
                                Ok(real)
                            }
                        }};
                    }

                    match non_null!(expr) {
                        DfValue::Float(float) => round!(float, f32),
                        DfValue::Double(double) => round!(double, f64),
                        DfValue::Int(val) => {
                            let rounded = integer_rnd(*val as i128, rnd_prec);
                            Ok(DfValue::Int(rounded as _))
                        }
                        DfValue::UnsignedInt(val) => {
                            let rounded = integer_rnd(*val as i128, rnd_prec);
                            Ok(DfValue::Int(rounded as _))
                        }
                        DfValue::Numeric(d) => {
                            let rounded_dec = if rnd_prec >= 0 {
                                d.round_dp_with_strategy(
                                    rnd_prec as _,
                                    rust_decimal::RoundingStrategy::MidpointAwayFromZero,
                                )
                            } else {
                                let factor =
                                    Decimal::from_f64(10.0f64.powf(-rnd_prec as _)).unwrap();

                                d.div(factor)
                                    .round_dp_with_strategy(
                                        0,
                                        rust_decimal::RoundingStrategy::MidpointAwayFromZero,
                                    )
                                    .mul(factor)
                            };

                            Ok(DfValue::Numeric(rounded_dec.into()))
                        }
                        dt => {
                            let dt_str = dt.to_string();
                            // MySQL will parse as many characters as it possibly can from a string
                            // as double
                            let mut double = 0f64;
                            let mut chars = 1;
                            if dt_str.starts_with('-') {
                                chars += 1;
                            }
                            while chars < dt_str.len() {
                                // This is very sad that Rust doesn't tell us how many characters of
                                // a string it was able to parse, but for now we just try to parse
                                // incrementally more characters until we fail
                                match dt_str[..chars].parse() {
                                    Ok(v) => {
                                        double = v;
                                        chars += 1;
                                    }
                                    Err(_) => break,
                                }
                            }
                            round!(double, f64)
                        }
                    }
                }
                BuiltinFunction::JsonTypeof(expr) | BuiltinFunction::JsonbTypeof(expr) => {
                    // TODO: Change this to coerce to `SqlType::Jsonb` and have it return a
                    // `DfValue` actually representing JSON.
                    let val = try_cast_or_none!(
                        non_null!(&expr.eval(record)?),
                        &SqlType::Text,
                        expr.ty()
                    );
                    let json_str = <&str>::try_from(&val)?;

                    let json = serde_json::Value::from_str(json_str).map_err(|e| {
                        ReadySetError::ProjectExprBuiltInFunctionError {
                            function: func.name().into(),
                            message: format!("parsing JSON expression failed: {}", Sensitive(&e)),
                        }
                    })?;

                    Ok(utils::get_json_value_type(&json).into())
                }
            },
            CaseWhen {
                condition,
                then_expr,
                else_expr,
                ..
            } => {
                if condition.eval(record)?.is_truthy() {
                    then_expr.eval(record)
                } else {
                    else_expr.eval(record)
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::convert::TryInto;

    use chrono::{Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike};
    use readyset_data::DfType;
    use rust_decimal::prelude::FromPrimitive;
    use rust_decimal::Decimal;
    use test_strategy::proptest;
    use Expr::*;

    use super::*;
    use crate::utils::{make_call, make_column, make_literal};

    #[test]
    fn eval_column() {
        let expr = make_column(1);
        assert_eq!(
            expr.eval(&[DfValue::from(1), "two".try_into().unwrap()])
                .unwrap(),
            "two".try_into().unwrap()
        )
    }

    #[test]
    fn eval_literal() {
        let expr = make_literal(1.into());
        assert_eq!(
            expr.eval(&[DfValue::from(1), "two".try_into().unwrap()])
                .unwrap(),
            1.into()
        )
    }

    #[test]
    fn eval_add() {
        let expr = Op {
            left: Box::new(make_column(0)),
            right: Box::new(Op {
                left: Box::new(make_column(1)),
                right: Box::new(make_literal(3.into())),
                op: BinaryOperator::Add,
                ty: DfType::Unknown,
            }),
            op: BinaryOperator::Add,
            ty: DfType::Unknown,
        };
        assert_eq!(
            expr.eval(&[DfValue::from(1), DfValue::from(2)]).unwrap(),
            6.into()
        );
    }

    #[test]
    fn eval_comparisons() {
        let dt = NaiveDateTime::new(
            NaiveDate::from_ymd(2009, 10, 17),
            NaiveTime::from_hms(12, 0, 0),
        );
        let text_dt: DfValue = "2009-10-17 12:00:00".try_into().unwrap();
        let text_less_dt: DfValue = "2009-10-16 12:00:00".try_into().unwrap();

        macro_rules! assert_op {
            ($binary_op:expr, $value:expr, $expected:expr) => {
                let expr = Op {
                    left: Box::new(make_column(0)),
                    right: Box::new(make_literal($value)),
                    op: $binary_op,
                    ty: DfType::Unknown,
                };
                assert_eq!(
                    expr.eval::<DfValue>(&[dt.into()]).unwrap(),
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
        let expr = Cast {
            expr: Box::new(make_column(0)),
            to_type: SqlType::Int(None),
            ty: DfType::Sql(SqlType::Int(None)),
        };
        assert_eq!(
            expr.eval::<DfValue>(&["1".try_into().unwrap(), "2".try_into().unwrap()])
                .unwrap(),
            1i32.into()
        );
    }

    #[test]
    fn eval_call_convert_tz() {
        let expr = make_call(BuiltinFunction::ConvertTZ(
            make_column(0),
            make_column(1),
            make_column(2),
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
            expr.eval::<DfValue>(&[
                datetime.into(),
                src.try_into().unwrap(),
                target.try_into().unwrap()
            ])
            .unwrap(),
            expected.into()
        );
        assert_eq!(
            expr.eval::<DfValue>(&[
                datetime.into(),
                "invalid timezone".try_into().unwrap(),
                target.try_into().unwrap()
            ])
            .unwrap(),
            DfValue::None
        );
        assert_eq!(
            expr.eval::<DfValue>(&[
                datetime.into(),
                src.try_into().unwrap(),
                "invalid timezone".try_into().unwrap()
            ])
            .unwrap(),
            DfValue::None
        );

        let string_datetime = datetime.to_string();
        assert_eq!(
            expr.eval::<DfValue>(&[
                string_datetime.clone().try_into().unwrap(),
                src.try_into().unwrap(),
                target.try_into().unwrap()
            ])
            .unwrap(),
            expected.into()
        );

        assert_eq!(
            expr.eval::<DfValue>(&[
                string_datetime.clone().try_into().unwrap(),
                "invalid timezone".try_into().unwrap(),
                target.try_into().unwrap()
            ])
            .unwrap(),
            DfValue::None
        );
        assert_eq!(
            expr.eval::<DfValue>(&[
                string_datetime.try_into().unwrap(),
                src.try_into().unwrap(),
                "invalid timezone".try_into().unwrap()
            ])
            .unwrap(),
            DfValue::None
        );
    }

    #[test]
    fn eval_call_day_of_week() {
        let expr = make_call(BuiltinFunction::DayOfWeek(make_column(0)));
        let expected = DfValue::Int(2);

        let date = NaiveDate::from_ymd(2021, 3, 22); // Monday

        assert_eq!(expr.eval::<DfValue>(&[date.into()]).unwrap(), expected);
        assert_eq!(
            expr.eval::<DfValue>(&[date.to_string().try_into().unwrap()])
                .unwrap(),
            expected
        );

        let datetime = NaiveDateTime::new(
            date, // Monday
            NaiveTime::from_hms(18, 8, 00),
        );
        assert_eq!(expr.eval::<DfValue>(&[datetime.into()]).unwrap(), expected);
        assert_eq!(
            expr.eval::<DfValue>(&[datetime.to_string().try_into().unwrap()])
                .unwrap(),
            expected
        );
    }

    #[test]
    fn eval_call_if_null() {
        let expr = make_call(BuiltinFunction::IfNull(make_column(0), make_column(1)));
        let value = DfValue::Int(2);

        assert_eq!(
            expr.eval(&[DfValue::None, DfValue::from(2)]).unwrap(),
            value
        );
        assert_eq!(
            expr.eval(&[DfValue::from(2), DfValue::from(3)]).unwrap(),
            value
        );

        let expr2 = make_call(BuiltinFunction::IfNull(
            make_literal(DfValue::None),
            make_column(0),
        ));
        assert_eq!(expr2.eval::<DfValue>(&[2.into()]).unwrap(), value);

        let expr3 = make_call(BuiltinFunction::IfNull(
            make_column(0),
            make_literal(DfValue::Int(2)),
        ));
        assert_eq!(expr3.eval(&[DfValue::None]).unwrap(), value);
    }

    #[test]
    fn eval_call_month() {
        let expr = make_call(BuiltinFunction::Month(make_column(0)));
        let datetime = NaiveDateTime::new(
            NaiveDate::from_ymd(2003, 10, 12),
            NaiveTime::from_hms(5, 13, 33),
        );
        let expected = 10_u32;
        assert_eq!(
            expr.eval(&[DfValue::from(datetime)]).unwrap(),
            expected.into()
        );
        assert_eq!(
            expr.eval::<DfValue>(&[datetime.to_string().try_into().unwrap()])
                .unwrap(),
            expected.into()
        );
        assert_eq!(
            expr.eval::<DfValue>(&[datetime.date().into()]).unwrap(),
            expected.into()
        );
        assert_eq!(
            expr.eval::<DfValue>(&[datetime.date().to_string().try_into().unwrap()])
                .unwrap(),
            expected.into()
        );
        assert_eq!(
            expr.eval::<DfValue>(&["invalid date".try_into().unwrap()])
                .unwrap(),
            DfValue::None
        );
    }

    #[test]
    fn eval_call_timediff() {
        let expr = make_call(BuiltinFunction::Timediff(make_column(0), make_column(1)));
        let param1 = NaiveDateTime::new(
            NaiveDate::from_ymd(2003, 10, 12),
            NaiveTime::from_hms(5, 13, 33),
        );
        let param2 = NaiveDateTime::new(
            NaiveDate::from_ymd(2003, 10, 14),
            NaiveTime::from_hms(4, 13, 33),
        );
        assert_eq!(
            expr.eval::<DfValue>(&[param1.into(), param2.into()])
                .unwrap(),
            DfValue::Time(MysqlTime::from_hmsus(false, 47, 0, 0, 0))
        );
        assert_eq!(
            expr.eval::<DfValue>(&[
                param1.to_string().try_into().unwrap(),
                param2.to_string().try_into().unwrap()
            ])
            .unwrap(),
            DfValue::Time(MysqlTime::from_hmsus(false, 47, 0, 0, 0))
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
            expr.eval::<DfValue>(&[param1.into(), param2.into()])
                .unwrap(),
            DfValue::Time(MysqlTime::from_hmsus(true, 49, 0, 0, 0))
        );
        assert_eq!(
            expr.eval::<DfValue>(&[
                param1.to_string().try_into().unwrap(),
                param2.to_string().try_into().unwrap()
            ])
            .unwrap(),
            DfValue::Time(MysqlTime::from_hmsus(true, 49, 0, 0, 0))
        );
        let param2 = NaiveTime::from_hms(4, 13, 33);
        assert_eq!(
            expr.eval::<DfValue>(&[param1.into(), param2.into()])
                .unwrap(),
            DfValue::None
        );
        assert_eq!(
            expr.eval::<DfValue>(&[
                param1.to_string().try_into().unwrap(),
                param2.to_string().try_into().unwrap()
            ])
            .unwrap(),
            DfValue::None
        );
        let param1 = NaiveTime::from_hms(5, 13, 33);
        assert_eq!(
            expr.eval::<DfValue>(&[param1.into(), param2.into()])
                .unwrap(),
            DfValue::Time(MysqlTime::from_hmsus(true, 1, 0, 0, 0))
        );
        assert_eq!(
            expr.eval::<DfValue>(&[
                param1.to_string().try_into().unwrap(),
                param2.to_string().try_into().unwrap()
            ])
            .unwrap(),
            DfValue::Time(MysqlTime::from_hmsus(true, 1, 0, 0, 0))
        );
        let param1 = "not a date nor time";
        let param2 = "01:00:00.4";
        assert_eq!(
            expr.eval::<DfValue>(&[param1.try_into().unwrap(), param2.try_into().unwrap()])
                .unwrap(),
            DfValue::Time(MysqlTime::from_hmsus(false, 1, 0, 0, 400_000))
        );
        assert_eq!(
            expr.eval::<DfValue>(&[param2.try_into().unwrap(), param1.try_into().unwrap()])
                .unwrap(),
            DfValue::Time(MysqlTime::from_hmsus(true, 1, 0, 0, 400_000))
        );

        let param2 = "10000.4";
        assert_eq!(
            expr.eval::<DfValue>(&[param1.try_into().unwrap(), param2.try_into().unwrap()])
                .unwrap(),
            DfValue::Time(MysqlTime::from_hmsus(false, 1, 0, 0, 400_000))
        );
        assert_eq!(
            expr.eval::<DfValue>(&[param2.try_into().unwrap(), param1.try_into().unwrap()])
                .unwrap(),
            DfValue::Time(MysqlTime::from_hmsus(true, 1, 0, 0, 400_000))
        );

        let param2: f32 = 3.57;
        assert_eq!(
            expr.eval::<DfValue>(&[
                DfValue::try_from(param1).unwrap(),
                DfValue::try_from(param2).unwrap()
            ])
            .unwrap(),
            DfValue::Time(MysqlTime::from_microseconds(
                (-param2 * 1_000_000_f32) as i64
            ))
        );

        let param2: f64 = 3.57;
        assert_eq!(
            expr.eval::<DfValue>(&[
                DfValue::try_from(param1).unwrap(),
                DfValue::try_from(param2).unwrap()
            ])
            .unwrap(),
            DfValue::Time(MysqlTime::from_microseconds(
                (-param2 * 1_000_000_f64) as i64
            ))
        );
    }

    #[test]
    fn eval_call_addtime() {
        let expr = make_call(BuiltinFunction::Addtime(make_column(0), make_column(1)));
        let param1 = NaiveDateTime::new(
            NaiveDate::from_ymd(2003, 10, 12),
            NaiveTime::from_hms(5, 13, 33),
        );
        let param2 = NaiveDateTime::new(
            NaiveDate::from_ymd(2003, 10, 14),
            NaiveTime::from_hms(4, 13, 33),
        );
        assert_eq!(
            expr.eval::<DfValue>(&[param1.into(), param2.into()])
                .unwrap(),
            DfValue::None
        );
        assert_eq!(
            expr.eval::<DfValue>(&[
                param1.to_string().try_into().unwrap(),
                param2.to_string().try_into().unwrap()
            ])
            .unwrap(),
            DfValue::None
        );
        let param2 = NaiveTime::from_hms(4, 13, 33);
        assert_eq!(
            expr.eval::<DfValue>(&[param1.into(), param2.into()])
                .unwrap(),
            DfValue::TimestampTz(
                NaiveDateTime::new(
                    NaiveDate::from_ymd(2003, 10, 12),
                    NaiveTime::from_hms(9, 27, 6),
                )
                .into()
            )
        );
        assert_eq!(
            expr.eval::<DfValue>(&[
                param1.to_string().try_into().unwrap(),
                param2.to_string().try_into().unwrap()
            ])
            .unwrap(),
            DfValue::TimestampTz(
                NaiveDateTime::new(
                    NaiveDate::from_ymd(2003, 10, 12),
                    NaiveTime::from_hms(9, 27, 6),
                )
                .into()
            )
        );
        let param2 = MysqlTime::from_hmsus(false, 3, 11, 35, 0);
        assert_eq!(
            expr.eval::<DfValue>(&[param1.into(), param2.into()])
                .unwrap(),
            DfValue::TimestampTz(
                NaiveDateTime::new(
                    NaiveDate::from_ymd(2003, 10, 12),
                    NaiveTime::from_hms(2, 1, 58),
                )
                .into()
            )
        );
        assert_eq!(
            expr.eval::<DfValue>(&[
                param1.to_string().try_into().unwrap(),
                param2.to_string().try_into().unwrap()
            ])
            .unwrap(),
            DfValue::TimestampTz(
                NaiveDateTime::new(
                    NaiveDate::from_ymd(2003, 10, 12),
                    NaiveTime::from_hms(2, 1, 58),
                )
                .into()
            )
        );
        let param1 = MysqlTime::from_hmsus(true, 10, 12, 44, 123_000);
        assert_eq!(
            expr.eval::<DfValue>(&[param2.into(), param1.into()])
                .unwrap(),
            DfValue::Time(MysqlTime::from_hmsus(true, 7, 1, 9, 123_000))
        );
        assert_eq!(
            expr.eval::<DfValue>(&[
                param2.to_string().try_into().unwrap(),
                param1.to_string().try_into().unwrap()
            ])
            .unwrap(),
            DfValue::Time(MysqlTime::from_hmsus(true, 7, 1, 9, 123_000))
        );
        let param1 = "not a date nor time";
        let param2 = "01:00:00.4";
        assert_eq!(
            expr.eval::<DfValue>(&[param1.try_into().unwrap(), param2.try_into().unwrap()])
                .unwrap(),
            DfValue::Time(MysqlTime::from_hmsus(true, 1, 0, 0, 400_000))
        );
        assert_eq!(
            expr.eval::<DfValue>(&[param2.try_into().unwrap(), param1.try_into().unwrap()])
                .unwrap(),
            DfValue::Time(MysqlTime::from_hmsus(true, 1, 0, 0, 400_000))
        );

        let param2 = "10000.4";
        assert_eq!(
            expr.eval::<DfValue>(&[param1.try_into().unwrap(), param2.try_into().unwrap()])
                .unwrap(),
            DfValue::Time(MysqlTime::from_hmsus(true, 1, 0, 0, 400_000))
        );
        assert_eq!(
            expr.eval::<DfValue>(&[param2.try_into().unwrap(), param1.try_into().unwrap()])
                .unwrap(),
            DfValue::Time(MysqlTime::from_hmsus(true, 1, 0, 0, 400_000))
        );

        let param2: f32 = 3.57;
        assert_eq!(
            expr.eval::<DfValue>(&[
                param1.try_into().unwrap(),
                DfValue::try_from(param2).unwrap()
            ])
            .unwrap(),
            DfValue::Time(MysqlTime::from_microseconds(
                (param2 * 1_000_000_f32) as i64
            ))
        );

        let param2: f64 = 3.57;
        assert_eq!(
            expr.eval::<DfValue>(&[
                param1.try_into().unwrap(),
                DfValue::try_from(param2).unwrap()
            ])
            .unwrap(),
            DfValue::Time(MysqlTime::from_microseconds(
                (param2 * 1_000_000_f64) as i64
            ))
        );
    }

    #[test]
    fn eval_call_round() {
        let expr = make_call(BuiltinFunction::Round(make_column(0), make_column(1)));
        let number: f64 = 4.12345;
        let precision = 3;
        let param1 = DfValue::try_from(number).unwrap();
        let param2 = DfValue::Int(precision);
        let want = DfValue::try_from(4.123_f64).unwrap();
        assert_eq!(
            expr.eval::<DfValue>(&[param1, param2.clone()]).unwrap(),
            want
        );

        let number: f32 = 4.12345;
        let param1 = DfValue::try_from(number).unwrap();
        let want = DfValue::try_from(4.123_f32).unwrap();
        assert_eq!(expr.eval::<DfValue>(&[param1, param2]).unwrap(), want);
    }

    #[test]
    fn eval_call_round_with_negative_precision() {
        let expr = make_call(BuiltinFunction::Round(make_column(0), make_column(1)));
        let number: f64 = 52.12345;
        let precision = -1;
        let param1 = DfValue::try_from(number).unwrap();
        let param2 = DfValue::Int(precision);
        let want = DfValue::try_from(50.0).unwrap();
        assert_eq!(
            expr.eval::<DfValue>(&[param1, param2.clone()]).unwrap(),
            want
        );

        let number: f32 = 52.12345;
        let param1 = DfValue::try_from(number).unwrap();
        assert_eq!(expr.eval::<DfValue>(&[param1, param2]).unwrap(), want);
    }

    #[test]
    fn eval_call_round_with_float_precision() {
        let expr = make_call(BuiltinFunction::Round(make_column(0), make_column(1)));
        let number: f32 = 52.12345;
        let precision = -1.0_f64;
        let param1 = DfValue::try_from(number).unwrap();
        let param2 = DfValue::try_from(precision).unwrap();
        let want = DfValue::try_from(50.0).unwrap();
        assert_eq!(
            expr.eval::<DfValue>(&[param1, param2.clone()]).unwrap(),
            want,
        );

        let number: f64 = 52.12345;
        let param1 = DfValue::try_from(number).unwrap();
        assert_eq!(expr.eval::<DfValue>(&[param1, param2]).unwrap(), want);
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
        let expr = make_call(BuiltinFunction::Round(make_column(0), make_column(1)));
        let number: f32 = 52.12345;
        let precision = "banana";
        let param1 = DfValue::try_from(number).unwrap();
        let param2 = DfValue::try_from(precision).unwrap();
        let want = DfValue::try_from(52.).unwrap();
        assert_eq!(
            expr.eval::<DfValue>(&[param1, param2.clone()]).unwrap(),
            want,
        );

        let number: f64 = 52.12345;
        let param1 = DfValue::try_from(number).unwrap();
        assert_eq!(expr.eval::<DfValue>(&[param1, param2]).unwrap(), want,);
    }

    #[test]
    fn eval_call_round_with_decimal() {
        let expr = make_call(BuiltinFunction::Round(make_column(0), make_column(1)));
        assert_eq!(
            expr.eval::<DfValue>(&[
                DfValue::from(Decimal::from_f64(52.123).unwrap()),
                DfValue::from(1)
            ])
            .unwrap(),
            DfValue::from(Decimal::from_f64(52.1)),
        );

        assert_eq!(
            expr.eval::<DfValue>(&[
                DfValue::from(Decimal::from_f64(-52.666).unwrap()),
                DfValue::from(2)
            ])
            .unwrap(),
            DfValue::from(Decimal::from_f64(-52.67)),
        );

        assert_eq!(
            expr.eval::<DfValue>(&[
                DfValue::from(Decimal::from_f64(-52.666).unwrap()),
                DfValue::from(-1)
            ])
            .unwrap(),
            DfValue::from(Decimal::from_f64(-50.)),
        );
    }

    #[test]
    fn eval_call_round_with_strings() {
        let expr = make_call(BuiltinFunction::Round(make_column(0), make_column(1)));
        assert_eq!(
            expr.eval::<DfValue>(&[DfValue::from("52.123"), DfValue::from(1)])
                .unwrap(),
            DfValue::try_from(52.1).unwrap(),
        );

        assert_eq!(
            expr.eval::<DfValue>(&[DfValue::from("-52.666banana"), DfValue::from(2)])
                .unwrap(),
            DfValue::try_from(-52.67).unwrap(),
        );

        assert_eq!(
            expr.eval::<DfValue>(&[DfValue::from("-52.666banana"), DfValue::from(-1)])
                .unwrap(),
            DfValue::try_from(-50.).unwrap(),
        );
    }

    #[test]
    fn eval_call_json_typeof() {
        let examples = [
            ("null", "null"),
            ("true", "boolean"),
            ("false", "boolean"),
            ("123", "number"),
            (r#""hello""#, "string"),
            (r#"["hello", 123]"#, "array"),
            (r#"{ "hello": "world", "abc": 123 }"#, "object"),
        ];

        let expr = make_call(BuiltinFunction::JsonTypeof(make_column(0)));

        for (json, expected_type) in examples {
            let json_type = expr.eval::<DfValue>(&[json.into()]).unwrap();
            assert_eq!(json_type, DfValue::from(expected_type));
        }
    }

    #[test]
    fn month_null() {
        let expr = make_call(BuiltinFunction::Month(make_column(0)));
        assert_eq!(
            expr.eval::<DfValue>(&[DfValue::None]).unwrap(),
            DfValue::None
        );
    }

    #[test]
    fn value_truthiness() {
        assert_eq!(
            Expr::Op {
                left: Box::new(make_literal(1.into())),
                op: BinaryOperator::And,
                right: Box::new(make_literal(3.into())),
                ty: DfType::Unknown,
            }
            .eval::<DfValue>(&[])
            .unwrap(),
            1.into()
        );

        assert_eq!(
            Expr::Op {
                left: Box::new(make_literal(1.into())),
                op: BinaryOperator::And,
                right: Box::new(make_literal(0.into())),
                ty: DfType::Unknown,
            }
            .eval::<DfValue>(&[])
            .unwrap(),
            0.into()
        );
    }

    #[test]
    fn eval_case_when() {
        let expr = Expr::CaseWhen {
            condition: Box::new(Op {
                left: Box::new(make_column(0)),
                op: BinaryOperator::Equal,
                right: Box::new(make_literal(1.into())),
                ty: DfType::Unknown,
            }),
            then_expr: Box::new(make_literal("yes".try_into().unwrap())),
            else_expr: Box::new(make_literal("no".try_into().unwrap())),
            ty: DfType::Unknown,
        };

        assert_eq!(expr.eval::<DfValue>(&[1.into()]), Ok(DfValue::from("yes")));

        assert_eq!(
            expr.eval::<DfValue>(&[DfValue::from(8)]),
            Ok(DfValue::from("no"))
        );
    }

    #[test]
    fn like_expr() {
        let expr = Expr::Op {
            left: Box::new(make_literal("foo".into())),
            op: BinaryOperator::Like,
            right: Box::new(make_literal("f%".into())),
            ty: DfType::Unknown,
        };
        let res = expr.eval::<DfValue>(&[]).unwrap();
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
