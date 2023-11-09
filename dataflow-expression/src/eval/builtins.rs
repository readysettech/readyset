use std::borrow::Borrow;
use std::cmp::Ordering;
use std::fmt::Write;
use std::ops::{Add, Div, Mul, Sub};

use chrono::{Datelike, LocalResult, Month, NaiveDate, NaiveDateTime, TimeZone, Timelike, Weekday};
use chrono_tz::Tz;
use itertools::Either;
use mysql_time::MySqlTime;
use readyset_data::{DfType, DfValue};
use readyset_errors::{invalid_query_err, ReadySetError, ReadySetResult};
use readyset_util::math::integer_rnd;
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rust_decimal::Decimal;
use serde_json::Value as JsonValue;
use vec1::Vec1;

use crate::{BuiltinFunction, Expr};

macro_rules! try_cast_or_none {
    ($df_value:expr, $to_ty:expr, $from_ty:expr) => {{
        match $df_value.coerce_to($to_ty, $from_ty) {
            Ok(v) => v,
            Err(_) => return Ok(DfValue::None),
        }
    }};
}

/// Returns the type of data stored in a JSON value as a string.
fn get_json_value_type(json: &serde_json::Value) -> &'static str {
    match json {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "boolean",
        serde_json::Value::Number(_) => "number",
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) => "array",
        serde_json::Value::Object(_) => "object",
    }
}

/// Attempts to coerce the value to `Timestamp` or `Time`, otherwise defaults to null on failure.
fn get_time_or_default(value: &DfValue, from_ty: &DfType) -> DfValue {
    // Default to 0 for consistency rather than rely on type dialect.
    // TODO: Use the database's real default.
    let subsecond_digits = from_ty.subsecond_digits().unwrap_or_default();

    value
        .coerce_to(&DfType::Timestamp { subsecond_digits }, from_ty)
        .or_else(|_| value.coerce_to(&DfType::Time { subsecond_digits }, from_ty))
        .unwrap_or(DfValue::None)
}

/// Transforms a `[NaiveDateTime]` into a new one with a different timezone.
/// The `[NaiveDateTime]` is interpreted as having the timezone specified by the
/// `src` parameter, and then it's transformed to timezone specified by the `target` parameter.
fn convert_tz(datetime: &NaiveDateTime, src: &str, target: &str) -> ReadySetResult<NaiveDateTime> {
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

fn timediff_datetimes(time1: &NaiveDateTime, time2: &NaiveDateTime) -> MySqlTime {
    let duration = time1.sub(*time2);
    MySqlTime::new(duration)
}

fn timediff_times(time1: &MySqlTime, time2: &MySqlTime) -> MySqlTime {
    time1.sub(*time2)
}

fn addtime_datetime(time1: &NaiveDateTime, time2: &MySqlTime) -> NaiveDateTime {
    time2.add(*time1)
}

fn addtime_times(time1: &MySqlTime, time2: &MySqlTime) -> MySqlTime {
    time1.add(*time2)
}

/// Format the given time value according to the given `format_string`, using the [MySQL date
/// formatting rules][mysql-docs]. Since these rules don't match up well with anything available in
/// the Rust crate ecosystem, this is done manually.
///
/// [mysql-docs]: https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_date-format
fn mysql_date_format<T>(time: T, format_string: &str) -> ReadySetResult<String>
where
    T: Timelike + Datelike,
{
    /// Calculate the week (and year!) number of a date-like value according to the ...algorithm...
    /// that MySQL uses. Returns a tuple of (week number, year), since in some operating modes a day
    /// may be part of the first week of the next year, or last week of the previous year.
    ///
    /// The actual algorithm here, and the arguments passed, are pretty close to a line-for-line
    /// translation of what MySQL uses, hence being quite impressively unidiomatic Rust: basically,
    /// I'd recommend thinking of this function and all of its callers as a black-box, at least for
    /// the time being (until we can dedicate the energy to actually understand what's going on
    /// here)
    fn week_and_year<T>(
        time: &T,
        monday_first: bool,
        mut week_year: bool,
        first_weekday: bool,
    ) -> (u32, i32)
    where
        T: Datelike,
    {
        fn days_in_year(year: i32) -> i32 {
            if (year & 3) == 0 && ((year % 100 != 0) || (year % 400 == 0 && year != 0)) {
                366
            } else {
                365
            }
        }

        let mut days;
        let daynr = time.num_days_from_ce();
        let mut first_daynr = time
            .with_day(1)
            .unwrap()
            .with_month(1)
            .unwrap()
            .num_days_from_ce();
        let mut weekday = if monday_first {
            time.weekday().num_days_from_monday()
        } else {
            time.weekday().num_days_from_sunday()
        } as i32;
        let mut year = time.year();

        if time.month() == 1 && time.day() <= (7 - weekday) as u32 {
            if !week_year && ((first_weekday && weekday != 0) || (!first_weekday && weekday >= 4)) {
                return (0, year);
            }
            week_year = true;
            year -= 1;
            days = days_in_year(year);
            first_daynr -= days;
            weekday = (weekday + 53 * 7 - days) % 7;
        }

        if (first_weekday && weekday != 0) || (!first_weekday && weekday >= 4) {
            days = daynr - (first_daynr + (7 - weekday))
        } else {
            days = daynr - (first_daynr - weekday)
        }

        if week_year && days >= 52 * 7 {
            weekday = (weekday + days_in_year(year)) % 7;
            if (!first_weekday && weekday < 4) || (first_weekday && weekday == 0) {
                year += 1;
                return (1, year);
            }
        }

        ((days / 7 + 1) as u32, year)
    }

    // | %a   | Abbreviated weekday name (Sun..Sat)
    // | %b   | Abbreviated month name (Jan..Dec)
    // | %c   | Month, numeric (0..12)
    // | %D   | Day of the month with English suffix (0th, 1st, 2nd, 3rd, …)
    // | %d   | Day of the month, numeric (00..31)
    // | %e   | Day of the month, numeric (0..31)
    // | %f   | Microseconds (000000..999999)
    // | %H   | Hour (00..23)
    // | %h   | Hour (01..12)
    // | %I   | Hour (01..12)
    // | %i   | Minutes, numeric (00..59)
    // | %j   | Day of year (001..366)
    // | %k   | Hour (0..23)
    // | %l   | Hour (1..12)
    // | %M   | Month name (January..December)
    // | %m   | Month, numeric (00..12)
    // | %p   | AM or PM
    // | %r   | Time, 12-hour (hh:mm:ss followed by AM or PM)
    // | %S   | Seconds (00..59)
    // | %s   | Seconds (00..59)
    // | %T   | Time, 24-hour (hh:mm:ss)
    // | %U   | Week (00..53), where Sunday is the first day of the week; WEEK() mode 0
    // | %u   | Week (00..53), where Monday is the first day of the week; WEEK() mode 1
    // | %V   | Week (01..53), where Sunday is the first day of the week; WEEK() mode 2;used with %X
    // | %v   | Week (01..53), where Monday is the first day of the week; WEEK() mode 3;used with %x
    // | %W   | Weekday name (Sunday..Saturday)
    // | %w   | Day of the week (0=Sunday..6=Saturday)
    // | %X   | Year for the week where Sunday is the first day of the week, numeric, four digits
    // | %x   | Year for the week, where Monday is the first day of the week, numeric, four digits
    // | %Y   | Year, numeric, four digits
    // | %y   | Year, numeric (two digits)
    // | %%   | A literal % character
    // | %x   | x, for any “x” not listed above
    let mut res = String::with_capacity(format_string.len().next_power_of_two());
    let mut chars = format_string.chars();
    while let Some(c) = chars.next() {
        if c == '%' {
            let Some(format_spec) = chars.next() else {
                res.push(c);
                break;
            };

            match format_spec {
                'a' => write!(res, "{}", time.weekday()).unwrap(),
                'b' => write!(
                    res,
                    "{}",
                    &Month::from_u32(time.month()).unwrap().name()[..3]
                )
                .unwrap(),
                'c' => write!(res, "{}", time.month()).unwrap(),
                'D' => {
                    let dom = time.day();
                    write!(res, "{dom}").unwrap();
                    write!(
                        res,
                        "{}",
                        match res.as_str() {
                            "11" | "12" | "13" => "th",
                            _ => match res.chars().last().unwrap() {
                                '1' => "st",
                                '2' => "nd",
                                '3' => "rd",
                                _ => "th",
                            },
                        }
                    )
                    .unwrap()
                }
                'd' => write!(res, "{:02}", time.day()).unwrap(),
                'e' => write!(res, "{}", time.day()).unwrap(),
                'f' => {
                    write!(res, "{:06.0}", time.nanosecond() / 1000).unwrap();
                }
                'H' => write!(res, "{:02}", time.hour()).unwrap(),
                'h' => write!(res, "{:02}", time.hour12().1).unwrap(),
                'I' => write!(res, "{:02}", time.hour12().1).unwrap(),
                'i' => write!(res, "{:02}", time.minute()).unwrap(),
                'j' => write!(res, "{:03}", time.ordinal()).unwrap(),
                'k' => write!(res, "{}", time.hour()).unwrap(),
                'l' => write!(res, "{}", time.hour12().1).unwrap(),
                'M' => write!(res, "{}", Month::from_u32(time.month()).unwrap().name()).unwrap(),
                'm' => write!(res, "{:02}", time.month()).unwrap(),
                'p' => {
                    if time.hour() >= 12 {
                        res.push_str("PM");
                    } else {
                        res.push_str("AM");
                    }
                }
                'r' => write!(
                    res,
                    "{:02}:{:02}:{:02} {}",
                    time.hour12().1,
                    time.minute(),
                    time.second(),
                    if time.hour12().0 { "PM" } else { "AM" }
                )
                .unwrap(),
                'S' | 's' => write!(res, "{:02}", time.second()).unwrap(),
                'T' => write!(
                    res,
                    "{:02}:{:02}:{:02}",
                    time.hour(),
                    time.minute(),
                    time.second()
                )
                .unwrap(),
                'U' => write!(res, "{:02}", week_and_year(&time, false, false, false).0).unwrap(),
                'u' => write!(res, "{:02}", week_and_year(&time, true, false, false).0).unwrap(),
                'V' => write!(res, "{:02}", week_and_year(&time, false, true, true).0).unwrap(),
                'v' => write!(res, "{:02}", week_and_year(&time, true, true, false).0).unwrap(),
                'W' => res.push_str(match time.weekday() {
                    Weekday::Mon => "Monday",
                    Weekday::Tue => "Tuesday",
                    Weekday::Wed => "Wednesday",
                    Weekday::Thu => "Thursday",
                    Weekday::Fri => "Friday",
                    Weekday::Sat => "Saturday",
                    Weekday::Sun => "Sunday",
                }),
                'w' => write!(res, "{}", time.weekday().num_days_from_sunday()).unwrap(),
                'X' => write!(res, "{:02}", week_and_year(&time, false, true, true).1).unwrap(),
                'x' => write!(res, "{:02}", week_and_year(&time, true, true, false).1).unwrap(),
                'Y' => write!(res, "{:04}", time.year()).unwrap(),
                'y' => write!(res, "{:02}", time.year() % 100).unwrap(),
                '%' => res.push('%'),
                c => res.push(c),
            }
        } else {
            res.push(c);
        }
    }

    Ok(res)
}

fn greatest_or_least<F, D>(
    args: &Vec1<Expr>,
    record: &[D],
    compare_as: &DfType,
    ty: &DfType,
    mut compare: F,
) -> ReadySetResult<DfValue>
where
    F: FnMut(&DfValue, &DfValue) -> bool,
    D: Borrow<DfValue>,
{
    let arg1 = args.first();
    let mut res = non_null!(arg1.eval(record)?);
    let mut res_ty = arg1.ty();
    let mut res_compare = try_cast_or_none!(res, compare_as, arg1.ty());
    for arg in args.iter().skip(1) {
        let val = non_null!(arg.eval(record)?);
        let val_compare = try_cast_or_none!(val, compare_as, arg.ty());
        if compare(&val_compare, &res_compare) {
            res = val;
            res_ty = arg.ty();
            res_compare = val_compare;
        }
    }
    Ok(try_cast_or_none!(res, ty, res_ty))
}

impl BuiltinFunction {
    pub(crate) fn eval<D>(&self, ty: &DfType, record: &[D]) -> ReadySetResult<DfValue>
    where
        D: Borrow<DfValue>,
    {
        match self {
            BuiltinFunction::ConvertTZ([arg1, arg2, arg3]) => {
                match convert_tz(
                    &(&non_null!(arg1.eval(record)?)).try_into()?,
                    (&non_null!(arg2.eval(record)?)).try_into()?,
                    (&non_null!(arg3.eval(record)?)).try_into()?,
                ) {
                    Ok(v) => Ok(DfValue::TimestampTz(v.into())),
                    Err(_) => Ok(DfValue::None),
                }
            }
            BuiltinFunction::DayOfWeek(arg) => Ok(DfValue::Int(day_of_week(
                &(NaiveDate::try_from(&non_null!(arg.eval(record)?))?),
            ) as i64)),
            BuiltinFunction::IfNull(arg1, arg2) => {
                let param1 = arg1.eval(record)?;
                let param2 = arg2.eval(record)?;
                if param1.is_none() {
                    Ok(param2)
                } else {
                    Ok(param1)
                }
            }
            BuiltinFunction::Month(arg) => Ok(DfValue::UnsignedInt(month(
                &(NaiveDate::try_from(non_null!(&arg.eval(record)?))?),
            ) as u64)),
            BuiltinFunction::Timediff(arg1, arg2) => {
                let param1 = arg1.eval(record)?;
                let param2 = arg2.eval(record)?;
                let null_result = Ok(DfValue::None);
                let time_param1 = get_time_or_default(&param1, arg1.ty());
                let time_param2 = get_time_or_default(&param2, arg2.ty());
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
                        &(MySqlTime::try_from(&time_param1)?),
                        &(MySqlTime::try_from(&time_param2)?),
                    )
                };
                Ok(DfValue::Time(time))
            }
            BuiltinFunction::Addtime(arg1, arg2) => {
                let param1 = arg1.eval(record)?;
                let param2 = arg2.eval(record)?;
                let time_param2 = get_time_or_default(&param2, arg2.ty());
                if time_param2.is_datetime() {
                    return Ok(DfValue::None);
                }
                let time_param1 = get_time_or_default(&param1, arg1.ty());
                if time_param1.is_datetime() {
                    Ok(DfValue::TimestampTz(
                        addtime_datetime(
                            &(NaiveDateTime::try_from(&time_param1)?),
                            &(MySqlTime::try_from(&time_param2)?),
                        )
                        .into(),
                    ))
                } else {
                    Ok(DfValue::Time(addtime_times(
                        &(MySqlTime::try_from(&time_param1)?),
                        &(MySqlTime::try_from(&time_param2)?),
                    )))
                }
            }
            BuiltinFunction::DateFormat(arg1, arg2) => {
                let date = get_time_or_default(&arg1.eval(record)?, arg1.ty());
                let format_string_v =
                    try_cast_or_none!(arg2.eval(record)?, &DfType::DEFAULT_TEXT, arg2.ty());
                let format_str: &str = (&format_string_v).try_into()?;
                if let Ok(t) = NaiveDateTime::try_from(&date) {
                    Ok(mysql_date_format(t, format_str)?.into())
                } else {
                    Ok(DfValue::None)
                }
            }
            BuiltinFunction::Round(arg1, arg2) => {
                let expr = arg1.eval(record)?;
                let param2 = arg2.eval(record)?;
                let rnd_prec = match non_null!(param2) {
                    DfValue::Int(inner) => inner as i32,
                    DfValue::UnsignedInt(inner) => inner as i32,
                    DfValue::Float(f) => f.round() as i32,
                    DfValue::Double(f) => f.round() as i32,
                    DfValue::Numeric(d) => {
                        // TODO(fran): I don't know if this is the right thing to do.
                        d.round().to_i32().ok_or_else(|| {
                            ReadySetError::BadRequest(format!(
                                "NUMERIC value {} exceeds 32-bit integer size",
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
                            let rounded_float = ($real * base.powf(rnd_prec as $real_type)).round()
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
                        let rounded = integer_rnd(val as i128, rnd_prec);
                        Ok(DfValue::Int(rounded as _))
                    }
                    DfValue::UnsignedInt(val) => {
                        let rounded = integer_rnd(val as i128, rnd_prec);
                        Ok(DfValue::Int(rounded as _))
                    }
                    DfValue::Numeric(d) => {
                        let rounded_dec = if rnd_prec >= 0 {
                            d.round_dp_with_strategy(
                                rnd_prec as _,
                                rust_decimal::RoundingStrategy::MidpointAwayFromZero,
                            )
                        } else {
                            let factor = Decimal::from_f64(10.0f64.powf(-rnd_prec as _)).unwrap();

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
            BuiltinFunction::JsonValid(expr) => {
                let value = expr.eval(record)?;

                let valid = if expr.ty().is_known() && !expr.ty().is_any_json_like() {
                    // Known non-json-like types return `false` and don't null-propagate.
                    false
                } else {
                    // Non-text unknown-type values return `false`.
                    <&str>::try_from(non_null!(&value))
                        .map(|json| serde_json::from_str::<serde::de::IgnoredAny>(json).is_ok())
                        .unwrap_or_default()
                };

                Ok(valid.into())
            }
            BuiltinFunction::JsonQuote(expr) => {
                // MySQL does not validate the JSON text.
                let json = non_null!(expr.eval(record)?);
                Ok(crate::eval::json::json_quote(<&str>::try_from(&json)?).into())
            }
            BuiltinFunction::JsonOverlaps(expr1, expr2) => Ok(crate::eval::json::json_overlaps(
                &non_null!(expr1.eval(record)?).to_json()?,
                &non_null!(expr2.eval(record)?).to_json()?,
            )
            .into()),
            BuiltinFunction::JsonObject {
                arg1: kv_pairs,
                arg2: None,
                allow_duplicate_keys,
            } => crate::eval::json::json_object_from_pairs(
                non_null!(kv_pairs.eval(record)?).as_array()?,
                *allow_duplicate_keys,
            ),
            BuiltinFunction::JsonObject {
                arg1: keys,
                arg2: Some(values),
                allow_duplicate_keys,
            } => crate::eval::json::json_object_from_keys_and_values(
                non_null!(keys.eval(record)?).as_array()?,
                non_null!(values.eval(record)?).as_array()?,
                *allow_duplicate_keys,
            ),
            BuiltinFunction::JsonTypeof(expr) => {
                let json = non_null!(expr.eval(record)?).to_json()?;
                Ok(get_json_value_type(&json).into())
            }
            BuiltinFunction::JsonStripNulls(expr) => {
                let mut json = non_null!(expr.eval(record)?).to_json()?;
                crate::eval::json::json_strip_nulls(&mut json);
                Ok(json.into())
            }
            BuiltinFunction::JsonArrayLength(expr) => non_null!(expr.eval(record)?)
                .to_json()?
                .as_array()
                .map(|array| DfValue::from(array.len()))
                .ok_or_else(|| invalid_query_err!("cannot get array length of a non-array")),
            BuiltinFunction::JsonDepth(expr) => non_null!(expr.eval(record)?)
                .to_json()
                .map(|json| crate::eval::json::json_depth(&json).into()),
            BuiltinFunction::JsonExtractPath { json, keys } => {
                let json = json.eval(record)?.to_json()?;

                let keys = keys
                    .iter()
                    .map(|key| key.eval(record))
                    .collect::<ReadySetResult<Vec<_>>>()?;

                crate::eval::json::json_extract_key_path(&json, &keys)
            }
            BuiltinFunction::JsonbInsert(target_json, key_path, inserted_json, insert_after) => {
                let mut target_json = non_null!(target_json.eval(record)?).to_json()?;

                let key_path = non_null!(key_path.eval(record)?);
                let key_path = key_path.as_array()?.values();

                let inserted_json = non_null!(inserted_json.eval(record)?).to_json()?;

                let insert_after = match insert_after {
                    Some(insert_after) => bool::try_from(non_null!(insert_after.eval(record)?))?,
                    None => false,
                };

                crate::eval::json::json_insert(
                    &mut target_json,
                    key_path,
                    inserted_json,
                    insert_after,
                )?;

                Ok(target_json.into())
            }
            BuiltinFunction::JsonbSet(
                target_json,
                key_path,
                new_json,
                create_if_missing,
                null_value_treatment,
            ) => {
                use crate::eval::json::NullValueTreatment;
                use crate::NullValueTreatmentArg;

                let mut target_json = non_null!(target_json.eval(record)?).to_json()?;

                let key_path = non_null!(key_path.eval(record)?);
                let key_path = key_path.as_array()?.values();

                let new_json = new_json.eval(record)?;

                let create_if_missing = match create_if_missing {
                    Some(create_if_missing) => {
                        bool::try_from(non_null!(create_if_missing.eval(record)?))?
                    }
                    None => true,
                };

                // PostgreSQL's behavior is to always evaluate `null_value_treatment` regardless if
                // `new_json` is null, so we replicate the behavior.
                let null_value_treatment = match null_value_treatment {
                    NullValueTreatmentArg::ReturnNull => None,
                    NullValueTreatmentArg::Expr(arg) => {
                        let nvt = arg
                            .as_ref()
                            .map(|expr| {
                                <&str>::try_from(&expr.eval(record)?)?.parse::<NullValueTreatment>()
                            })
                            .transpose()?
                            .unwrap_or_default();

                        Some(nvt)
                    }
                };

                let new_json = if new_json.is_none() {
                    match null_value_treatment {
                        None => return Ok(DfValue::None),
                        Some(NullValueTreatment::UseJsonNull) => JsonValue::Null,
                        Some(NullValueTreatment::DeleteKey) => {
                            crate::eval::json::json_remove_path(&mut target_json, key_path)?;
                            return Ok(target_json.into());
                        }
                        Some(NullValueTreatment::ReturnTarget) => return Ok(target_json.into()),
                        Some(NullValueTreatment::RaiseException) => {
                            return Err(invalid_query_err!("JSON value must not be null"))
                        }
                    }
                } else {
                    new_json.to_json()?
                };

                crate::eval::json::json_set(
                    &mut target_json,
                    key_path,
                    new_json,
                    create_if_missing,
                )?;

                Ok(target_json.into())
            }
            BuiltinFunction::JsonbPretty(json) => {
                let json = json.eval(record)?.to_json()?;
                Ok(crate::eval::json::json_to_pretty(&json).into())
            }
            BuiltinFunction::Coalesce(arg1, rest_args) => {
                let val1 = arg1.eval(record)?;
                let rest_vals = rest_args
                    .iter()
                    .map(|expr| expr.eval(record))
                    .collect::<Result<Vec<_>, _>>()?;
                if !val1.is_none() {
                    Ok(val1)
                } else {
                    Ok(rest_vals
                        .into_iter()
                        .find(|v| !v.is_none())
                        .unwrap_or(DfValue::None))
                }
            }
            BuiltinFunction::Concat(arg1, rest_args) => {
                let mut s = <&str>::try_from(&non_null!(arg1.eval(record)?))?.to_owned();

                for arg in rest_args {
                    let val = non_null!(arg.eval(record)?);
                    s.push_str((&val).try_into()?)
                }

                Ok(s.into())
            }
            BuiltinFunction::Substring(string, from, len) => {
                let string = non_null!(string.eval(record)?);
                let s = <&str>::try_from(&string)?;

                let from = match from {
                    Some(from) => non_null!(from.eval(record)?).try_into()?,
                    None => 1i64,
                };

                let len = match len {
                    Some(len) => non_null!(len.eval(record)?).try_into()?,
                    None => s.len() as i64 + 1,
                };

                if len <= 0 {
                    return Ok("".into());
                }

                let start = match from.cmp(&0) {
                    Ordering::Equal => return Ok("".into()),
                    Ordering::Less => {
                        let reverse_from = -from as usize;
                        if reverse_from > s.len() {
                            return Ok("".into());
                        }
                        s.len() - reverse_from
                    }
                    Ordering::Greater => (from - 1) as usize,
                };

                Ok(s.chars()
                    .skip(start)
                    .take(len as _)
                    .collect::<String>()
                    .into())
            }
            BuiltinFunction::SplitPart(string, delimiter, field) => {
                let string = non_null!(string.eval(record)?);
                let delimiter = non_null!(delimiter.eval(record)?);
                let field = <i64>::try_from(non_null!(field.eval(record)?))?;

                let mut parts = <&str>::try_from(&string)?.split(<&str>::try_from(&delimiter)?);
                match field.cmp(&0) {
                    Ordering::Less => {
                        let parts = parts.collect::<Vec<_>>();
                        let rfield = -field as usize;
                        if rfield >= parts.len() {
                            return Ok("".into());
                        }
                        Ok(parts
                            .get(parts.len() - rfield)
                            .copied()
                            .unwrap_or("")
                            .into())
                    }
                    Ordering::Equal => Err(invalid_query_err!("field position must not be zero")),
                    Ordering::Greater => {
                        Ok(parts
                            .nth((field - 1/* 1-indexed */).try_into().unwrap())
                            .unwrap_or("")
                            .into())
                    }
                }
            }
            BuiltinFunction::Greatest { args, compare_as } => {
                greatest_or_least(args, record, compare_as, ty, |v1, v2| v1 > v2)
            }
            BuiltinFunction::Least { args, compare_as } => {
                greatest_or_least(args, record, compare_as, ty, |v1, v2| v1 < v2)
            }
            BuiltinFunction::ArrayToString(array, delimiter, null_string) => {
                let elem_type = match array.ty() {
                    DfType::Array(t) => t.as_ref(),
                    _ => &DfType::Unknown,
                };
                let array = non_null!(array.eval(record)?);
                let array = array.as_array()?;
                let delimiter: String = delimiter.eval(record)?.try_into()?;
                let null_string = null_string
                    .as_ref()
                    .map(|ns| ns.eval(record))
                    .transpose()?
                    .filter(|ns| !ns.is_none())
                    .map(String::try_from)
                    .transpose()?;
                let mut res = String::new();
                let values = array.values();
                let filtered_values = if null_string.is_none() {
                    Either::Left(values.filter(|v| !v.is_none()))
                } else {
                    Either::Right(values)
                };
                for (i, val) in filtered_values.enumerate() {
                    if i != 0 {
                        res.push_str(&delimiter);
                    }

                    if val.is_none() {
                        if let Some(null_string) = &null_string {
                            res.push_str(null_string);
                        }
                    } else {
                        res.push_str(
                            (&val.coerce_to(&DfType::DEFAULT_TEXT, elem_type)?).try_into()?,
                        );
                    }
                }

                Ok(res.into())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::{NaiveTime, Timelike};
    use lazy_static::lazy_static;
    use nom_sql::parse_expr;
    use nom_sql::Dialect::*;
    use readyset_errors::{internal, internal_err};
    use readyset_util::arbitrary::arbitrary_timestamp_naive_date_time;
    use test_strategy::proptest;

    use super::*;
    use crate::eval::tests::{eval_expr, try_eval_expr};
    use crate::lower::tests::resolve_columns;
    use crate::utils::strings_to_array_expr;
    use crate::{Dialect, LowerContext};

    /// Resolves columns with names like `c0`, `c1`, `c2` to the corresponding number, with type
    /// Unknown
    fn numbered_columns() -> impl LowerContext {
        resolve_columns(|c| {
            if c.table.is_some() {
                internal!("Qualified columns not allowed")
            }
            let Some(number_s) = c.name.strip_prefix('c') else {
                internal!("Column name must start with 'c'")
            };

            let column_idx = number_s
                .parse()
                .map_err(|e| internal_err!("Could not parse column index: {e}"))?;

            Ok((column_idx, DfType::Unknown))
        })
    }

    fn parse_and_lower(expr: &str, dialect: nom_sql::Dialect) -> Expr {
        let ast = parse_expr(dialect, expr).unwrap();
        let expr_dialect = match dialect {
            PostgreSQL => crate::Dialect::DEFAULT_POSTGRESQL,
            MySQL => crate::Dialect::DEFAULT_MYSQL,
        };
        Expr::lower(ast, expr_dialect, numbered_columns()).unwrap()
    }

    #[test]
    fn eval_call_convert_tz() {
        let expr = parse_and_lower("convert_tz(c0, c1, c2)", MySQL);
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
            expr.eval::<DfValue>(&[datetime.into(), src.into(), target.into()])
                .unwrap(),
            expected.into()
        );
        assert_eq!(
            expr.eval::<DfValue>(&[datetime.into(), "invalid timezone".into(), target.into()])
                .unwrap(),
            DfValue::None
        );
        assert_eq!(
            expr.eval::<DfValue>(&[datetime.into(), src.into(), "invalid timezone".into()])
                .unwrap(),
            DfValue::None
        );

        let string_datetime = datetime.to_string();
        assert_eq!(
            expr.eval::<DfValue>(&[string_datetime.clone().into(), src.into(), target.into()])
                .unwrap(),
            expected.into()
        );

        assert_eq!(
            expr.eval::<DfValue>(&[
                string_datetime.clone().into(),
                "invalid timezone".into(),
                target.into()
            ])
            .unwrap(),
            DfValue::None
        );
        assert_eq!(
            expr.eval::<DfValue>(&[
                string_datetime.into(),
                src.into(),
                "invalid timezone".into()
            ])
            .unwrap(),
            DfValue::None
        );
    }

    #[test]
    fn eval_call_day_of_week() {
        let expr = parse_and_lower("dayofweek(c0)", MySQL);
        let expected = DfValue::Int(2);

        let date = NaiveDate::from_ymd(2021, 3, 22); // Monday

        assert_eq!(expr.eval::<DfValue>(&[date.into()]).unwrap(), expected);
        assert_eq!(
            expr.eval::<DfValue>(&[date.to_string().into()]).unwrap(),
            expected
        );

        let datetime = NaiveDateTime::new(
            date, // Monday
            NaiveTime::from_hms(18, 8, 00),
        );
        assert_eq!(expr.eval::<DfValue>(&[datetime.into()]).unwrap(), expected);
        assert_eq!(
            expr.eval::<DfValue>(&[datetime.to_string().into()])
                .unwrap(),
            expected
        );

        assert_eq!(expr.eval(&[DfValue::None]), Ok(DfValue::None));
    }

    #[test]
    fn eval_call_if_null() {
        let expr = parse_and_lower("ifnull(c0, c1)", MySQL);
        let value = DfValue::Int(2);

        assert_eq!(
            expr.eval(&[DfValue::None, DfValue::from(2)]).unwrap(),
            value
        );
        assert_eq!(
            expr.eval(&[DfValue::from(2), DfValue::from(3)]).unwrap(),
            value
        );

        let expr2 = parse_and_lower("ifnull(null, c0)", MySQL);
        assert_eq!(expr2.eval::<DfValue>(&[2.into()]).unwrap(), value);

        let expr3 = parse_and_lower("ifnull(c0, 2)", MySQL);
        assert_eq!(expr3.eval(&[DfValue::None]).unwrap(), value);
    }

    #[test]
    fn eval_call_month() {
        let expr = parse_and_lower("month(c0)", MySQL);
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
            expr.eval::<DfValue>(&[datetime.to_string().into()])
                .unwrap(),
            expected.into()
        );
        assert_eq!(
            expr.eval::<DfValue>(&[datetime.date().into()]).unwrap(),
            expected.into()
        );
        assert_eq!(
            expr.eval::<DfValue>(&[datetime.date().to_string().into()])
                .unwrap(),
            expected.into()
        );
        assert_eq!(
            expr.eval::<DfValue>(&["invalid date".into()]).unwrap(),
            DfValue::None
        );
    }

    #[test]
    fn eval_call_timediff() {
        let expr = parse_and_lower("timediff(c0, c1)", MySQL);
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
            DfValue::Time(MySqlTime::from_hmsus(false, 47, 0, 0, 0))
        );
        assert_eq!(
            expr.eval::<DfValue>(&[param1.to_string().into(), param2.to_string().into()])
                .unwrap(),
            DfValue::Time(MySqlTime::from_hmsus(false, 47, 0, 0, 0))
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
            DfValue::Time(MySqlTime::from_hmsus(true, 49, 0, 0, 0))
        );
        assert_eq!(
            expr.eval::<DfValue>(&[param1.to_string().into(), param2.to_string().into()])
                .unwrap(),
            DfValue::Time(MySqlTime::from_hmsus(true, 49, 0, 0, 0))
        );
        let param2 = NaiveTime::from_hms(4, 13, 33);
        assert_eq!(
            expr.eval::<DfValue>(&[param1.into(), param2.into()])
                .unwrap(),
            DfValue::None
        );
        assert_eq!(
            expr.eval::<DfValue>(&[param1.to_string().into(), param2.to_string().into()])
                .unwrap(),
            DfValue::None
        );
        let param1 = NaiveTime::from_hms(5, 13, 33);
        assert_eq!(
            expr.eval::<DfValue>(&[param1.into(), param2.into()])
                .unwrap(),
            DfValue::Time(MySqlTime::from_hmsus(true, 1, 0, 0, 0))
        );
        assert_eq!(
            expr.eval::<DfValue>(&[param1.to_string().into(), param2.to_string().into()])
                .unwrap(),
            DfValue::Time(MySqlTime::from_hmsus(true, 1, 0, 0, 0))
        );
        let param1 = "not a date nor time";
        let param2 = "01:00:00.4";
        assert_eq!(
            expr.eval::<DfValue>(&[param1.into(), param2.into()])
                .unwrap(),
            DfValue::Time(MySqlTime::from_hmsus(false, 1, 0, 0, 400_000))
        );
        assert_eq!(
            expr.eval::<DfValue>(&[param2.into(), param1.into()])
                .unwrap(),
            DfValue::Time(MySqlTime::from_hmsus(true, 1, 0, 0, 400_000))
        );

        let param2 = "10000.4";
        assert_eq!(
            expr.eval::<DfValue>(&[param1.into(), param2.into()])
                .unwrap(),
            DfValue::Time(MySqlTime::from_hmsus(false, 1, 0, 0, 400_000))
        );
        assert_eq!(
            expr.eval::<DfValue>(&[param2.into(), param1.into()])
                .unwrap(),
            DfValue::Time(MySqlTime::from_hmsus(true, 1, 0, 0, 400_000))
        );

        let param2: f32 = 3.57;
        assert_eq!(
            expr.eval::<DfValue>(&[
                DfValue::try_from(param1).unwrap(),
                DfValue::try_from(param2).unwrap()
            ])
            .unwrap(),
            DfValue::Time(MySqlTime::from_microseconds(
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
            DfValue::Time(MySqlTime::from_microseconds(
                (-param2 * 1_000_000_f64) as i64
            ))
        );
    }

    #[test]
    fn eval_call_addtime() {
        let expr = parse_and_lower("addtime(c0, c1)", MySQL);
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
            expr.eval::<DfValue>(&[param1.to_string().into(), param2.to_string().into()])
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
            expr.eval::<DfValue>(&[param1.to_string().into(), param2.to_string().into()])
                .unwrap(),
            DfValue::TimestampTz(
                NaiveDateTime::new(
                    NaiveDate::from_ymd(2003, 10, 12),
                    NaiveTime::from_hms(9, 27, 6),
                )
                .into()
            )
        );
        let param2 = MySqlTime::from_hmsus(false, 3, 11, 35, 0);
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
            expr.eval::<DfValue>(&[param1.to_string().into(), param2.to_string().into()])
                .unwrap(),
            DfValue::TimestampTz(
                NaiveDateTime::new(
                    NaiveDate::from_ymd(2003, 10, 12),
                    NaiveTime::from_hms(2, 1, 58),
                )
                .into()
            )
        );
        let param1 = MySqlTime::from_hmsus(true, 10, 12, 44, 123_000);
        assert_eq!(
            expr.eval::<DfValue>(&[param2.into(), param1.into()])
                .unwrap(),
            DfValue::Time(MySqlTime::from_hmsus(true, 7, 1, 9, 123_000))
        );
        assert_eq!(
            expr.eval::<DfValue>(&[param2.to_string().into(), param1.to_string().into()])
                .unwrap(),
            DfValue::Time(MySqlTime::from_hmsus(true, 7, 1, 9, 123_000))
        );
        let param1 = "not a date nor time";
        let param2 = "01:00:00.4";
        assert_eq!(
            expr.eval::<DfValue>(&[param1.into(), param2.into()])
                .unwrap(),
            DfValue::Time(MySqlTime::from_hmsus(true, 1, 0, 0, 400_000))
        );
        assert_eq!(
            expr.eval::<DfValue>(&[param2.into(), param1.into()])
                .unwrap(),
            DfValue::Time(MySqlTime::from_hmsus(true, 1, 0, 0, 400_000))
        );

        let param2 = "10000.4";
        assert_eq!(
            expr.eval::<DfValue>(&[param1.into(), param2.into()])
                .unwrap(),
            DfValue::Time(MySqlTime::from_hmsus(true, 1, 0, 0, 400_000))
        );
        assert_eq!(
            expr.eval::<DfValue>(&[param2.into(), param1.into()])
                .unwrap(),
            DfValue::Time(MySqlTime::from_hmsus(true, 1, 0, 0, 400_000))
        );

        let param2: f32 = 3.57;
        assert_eq!(
            expr.eval::<DfValue>(&[param1.into(), DfValue::try_from(param2).unwrap()])
                .unwrap(),
            DfValue::Time(MySqlTime::from_microseconds(
                (param2 * 1_000_000_f32) as i64
            ))
        );

        let param2: f64 = 3.57;
        assert_eq!(
            expr.eval::<DfValue>(&[param1.into(), DfValue::try_from(param2).unwrap()])
                .unwrap(),
            DfValue::Time(MySqlTime::from_microseconds(
                (param2 * 1_000_000_f64) as i64
            ))
        );
    }

    #[test]
    fn eval_call_round() {
        let expr = parse_and_lower("round(c0, c1)", MySQL);
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
        let expr = parse_and_lower("round(c0, c1)", MySQL);
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
        let expr = parse_and_lower("round(c0, c1)", MySQL);
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
        let expr = parse_and_lower("round(c0, c1)", MySQL);
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
        let expr = parse_and_lower("round(c0, c1)", MySQL);
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
        let expr = parse_and_lower("round(c0, c1)", MySQL);
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

        let expr = parse_and_lower("json_typeof(c0)", PostgreSQL);

        for (json, expected_type) in examples {
            let json_type = expr.eval::<DfValue>(&[json.into()]).unwrap();
            assert_eq!(json_type, DfValue::from(expected_type));
        }

        assert_eq!(expr.eval(&[DfValue::None]), Ok(DfValue::None));
    }

    #[test]
    fn month_null() {
        let expr = parse_and_lower("month(c0)", MySQL);
        assert_eq!(
            expr.eval::<DfValue>(&[DfValue::None]).unwrap(),
            DfValue::None
        );
    }

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
        super::convert_tz(&datetime, "invalid timezone", target).unwrap_err();
        super::convert_tz(&datetime, src, "invalid timezone").unwrap_err();
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

    #[test]
    fn coalesce() {
        let expr = parse_and_lower("coalesce(c0, 1)", PostgreSQL);
        let call_with = |val: DfValue| expr.eval(&[val]);

        assert_eq!(call_with(DfValue::None).unwrap(), 1.into());
        assert_eq!(call_with(123.into()).unwrap(), 123.into());
    }

    #[test]
    fn coalesce_more_args() {
        let expr = parse_and_lower("coalesce(c0, c1, 1)", PostgreSQL);
        let call_with = |val1: DfValue, val2: DfValue| expr.eval(&[val1, val2]);

        assert_eq!(call_with(DfValue::None, DfValue::None).unwrap(), 1.into());
        assert_eq!(
            call_with(DfValue::None, "abc".into()).unwrap(),
            "abc".into()
        );
        assert_eq!(call_with(123.into(), DfValue::None).unwrap(), 123.into());
        assert_eq!(call_with(123.into(), 456.into()).unwrap(), 123.into());
    }

    #[test]
    fn concat() {
        let expr = parse_and_lower("concat('My', 'S', 'QL')", MySQL);

        let res = expr.eval::<DfValue>(&[]).unwrap();
        assert_eq!(res, "MySQL".into());
    }

    #[test]
    fn concat_with_nulls() {
        let expr = parse_and_lower("concat('My', null, 'QL')", MySQL);

        let res = expr.eval::<DfValue>(&[]).unwrap();
        assert_eq!(res, DfValue::None);
    }

    #[test]
    fn substring_with_from_and_for() {
        let expr = parse_and_lower("substring('abcdef', c0, c1)", MySQL);
        let call_with =
            |from: i64, len: i64| expr.eval::<DfValue>(&[from.into(), len.into()]).unwrap();

        assert_eq!(call_with(2, 3), "bcd".into());
        assert_eq!(call_with(3, 3), "cde".into());
        assert_eq!(call_with(6, 3), "f".into());
        assert_eq!(call_with(7, 12), "".into());
        assert_eq!(call_with(-3, 3), "def".into());
        assert_eq!(call_with(-3, -3), "".into());
        assert_eq!(call_with(0, 3), "".into());
        assert_eq!(call_with(0, 0), "".into());
        assert_eq!(call_with(-7, 2), "".into());
    }

    #[test]
    fn substring_multibyte() {
        let expr = parse_and_lower("substring('é' from 1 for 1)", PostgreSQL);
        let res = expr.eval::<DfValue>(&[]).unwrap();
        assert_eq!(res, "é".into());
    }

    #[test]
    fn substring_with_from() {
        let expr = parse_and_lower("substring('abcdef' from c0)", PostgreSQL);
        let res = expr.eval::<DfValue>(&[2.into()]).unwrap();
        assert_eq!(res, "bcdef".into());
    }

    #[test]
    fn substring_with_for() {
        let expr = parse_and_lower("substring('abcdef' for c0)", PostgreSQL);
        let res = expr.eval::<DfValue>(&[3.into()]).unwrap();
        assert_eq!(res, "abc".into());
    }

    #[test]
    fn greatest_mysql() {
        assert_eq!(eval_expr("greatest(1, 2, 3)", MySQL), 3.into());
        assert_eq!(
            eval_expr("greatest(123, '23')", MySQL),
            23.into() // TODO(ENG-1911) this should be a string!
        );
        assert_eq!(
            eval_expr("greatest(1.23, '23')", MySQL),
            (23.0).try_into().unwrap()
        );
    }

    #[test]
    fn least_mysql() {
        assert_eq!(eval_expr("least(1, 2, 3)", MySQL), 1u64.into());
        assert_eq!(
            eval_expr("least(123, '23')", MySQL),
            123.into() // TODO(ENG-1911) this should be a string!
        );
        assert_eq!(
            eval_expr("least(1.23, '23')", MySQL),
            (1.23_f64).try_into().unwrap() // TODO(ENG-1911) this should be a string!
        );
    }

    #[test]
    #[ignore = "ENG-1909"]
    fn greatest_mysql_ints_and_floats() {
        assert_eq!(
            eval_expr("greatest(1, 2.5, 3)", MySQL),
            (3.0f64).try_into().unwrap()
        );
    }

    #[test]
    fn greatest_postgresql() {
        assert_eq!(eval_expr("greatest(1,2,3)", PostgreSQL), 3.into());
        assert_eq!(eval_expr("greatest(123, '23')", PostgreSQL), 123.into());
        assert_eq!(eval_expr("greatest(23, '123')", PostgreSQL), 123.into());
    }

    #[test]
    fn least_postgresql() {
        assert_eq!(eval_expr("least(1,2,3)", PostgreSQL), 1.into());
        assert_eq!(eval_expr("least(123, '23')", PostgreSQL), 23.into());
    }

    #[test]
    fn split_part() {
        assert_eq!(
            eval_expr("split_part('abc~@~def~@~ghi', '~@~', 2)", PostgreSQL),
            "def".into()
        );
        assert_eq!(
            eval_expr("split_part('a.b.c', '.', 4)", PostgreSQL),
            "".into()
        );
        assert_eq!(
            eval_expr("split_part('a.b.c', '.', -1)", PostgreSQL),
            "c".into()
        );
        assert_eq!(
            eval_expr("split_part('a.b.c', '.', -4)", PostgreSQL),
            "".into()
        );
    }

    #[track_caller]
    fn date_format(time: &str, fmt: &str) -> DfValue {
        lazy_static! {
            static ref EXPR: Expr = {
                let ast = parse_expr(MySQL, "date_format(t, f)").unwrap();
                Expr::lower(
                    ast,
                    Dialect::DEFAULT_MYSQL,
                    resolve_columns(|c| match c.name.as_str() {
                        "t" => Ok((0, DfType::DEFAULT_TEXT)),
                        "f" => Ok((1, DfType::DEFAULT_TEXT)),
                        _ => internal!(),
                    }),
                )
                .unwrap()
            };
        }
        EXPR.eval(&[DfValue::from(time), DfValue::from(fmt)])
            .unwrap()
    }

    #[test]
    fn date_format_with_datetimes() {
        assert_eq!(
            date_format("2003-01-02 10:11:12.000000", "%y-%m-%d %h:%i:%s"),
            "03-01-02 10:11:12".into()
        );
        assert_eq!(
            date_format("2003-01-02 08:11:02.123456", "%y-%m-%d %h:%i:%s.%f"),
            "03-01-02 08:11:02.123456".into()
        );
        assert_eq!(
            date_format("2003-01-02 08:11:02.000006", "%y-%m-%d %h:%i:%s.%f"),
            "03-01-02 08:11:02.000006".into()
        );
        assert_eq!(
            date_format("0003-01-02 08:11:02.000000", "%Y-%m-%d %h:%i:%s.%f"),
            "0003-01-02 08:11:02.000000".into()
        );
        assert_eq!(
            date_format("2003-01-02 08:11:02.000000", "%y-%m-%d %h:%i:%s.%f"),
            "03-01-02 08:11:02.000000".into()
        );
        assert_eq!(
            date_format("2003-01-02 01:11:12.123450", "%Y-%m-%d %h:%i:%s.%f%p"),
            "2003-01-02 01:11:12.123450AM".into()
        );
        assert_eq!(
            date_format("2003-01-02 00:11:12.123450", "%Y-%m-%d %h:%i:%s.%f%p"),
            "2003-01-02 12:11:12.123450AM".into()
        );
        assert_eq!(
            date_format("2003-01-02 10:11:12.000000", "%f"),
            "000000".into()
        );
        assert_eq!(
            date_format("2003-01-02 23:11:12.000000", "%Y-%m-%d %h:%i:%s%p"),
            "2003-01-02 11:11:12PM".into()
        );
        assert_eq!(
            date_format("2001-01-15 12:59:58.000000", "%d-%m-%Y %h:%i:%s"),
            "15-01-2001 12:59:58".into()
        );
        assert_eq!(
            date_format("2001-09-15 00:00:00.000000", "%d %M %Y"),
            "15 September 2001".into()
        );
        assert_eq!(
            date_format("2001-09-15 00:00:00.000000", "%d %b %Y"),
            "15 Sep 2001".into()
        );
        assert_eq!(
            date_format("2001-05-15 00:00:00.000000", "%d %b %Y"),
            "15 May 2001".into()
        );
        assert_eq!(
            date_format("2001-05-15 00:00:00.000000", "%w %d %b %y"),
            "2 15 May 01".into()
        );
        assert_eq!(
            date_format("2002-01-01 00:00:00.000000", "%W %u %Y"),
            "Tuesday 01 2002".into()
        );
        assert_eq!(
            date_format("1998-12-31 00:00:00.000000", "%W %u %y"),
            "Thursday 53 98".into()
        );
        assert_eq!(
            date_format("2001-01-07 00:00:00.000000", "%w %v %x"),
            "0 01 2001".into()
        );
        assert_eq!(
            date_format("2002-01-01 00:00:00.000000", "%w %v %x"),
            "2 01 2002".into()
        );
        assert_eq!(
            date_format("2004-02-29 00:00:00.000000", "%j %y"),
            "060 04".into()
        );
        assert_eq!(
            date_format("1998-12-31 00:00:00.000000", "%w %u %y"),
            "4 53 98".into()
        );
        assert_eq!(
            date_format("2001-01-15 00:00:00.000000", "%d-%m-%y %h:%i:%s"),
            "15-01-01 12:00:00".into()
        );
        assert_eq!(
            date_format("2020-01-15 00:00:00.000000", "%d-%m-%y"),
            "15-01-20".into()
        );
        assert_eq!(
            date_format("2001-01-15 00:00:00.000000", "%d-%y-%c"),
            "15-01-1".into()
        );
        assert_eq!(date_format("2002-01-01", "%D"), "1st".into());
        assert_eq!(date_format("2002-01-31", "%D"), "31st".into());
        assert_eq!(date_format("2002-01-22", "%D"), "22nd".into());
        assert_eq!(date_format("2002-01-11", "%D"), "11th".into());
        assert_eq!(date_format("2002-01-12", "%D"), "12th".into());
        assert_eq!(date_format("2002-01-13", "%D"), "13th".into());
        assert_eq!(date_format("2002-01-13", "%e"), "13".into());
        assert_eq!(date_format("2002-01-01", "%e"), "1".into());
        assert_eq!(date_format("2002-01-01 01:15:45.123456", "%I"), "01".into());
        assert_eq!(
            date_format("2002-01-01 12:15:45.123456", "%D %H %I %k %l %r %S %T %X"),
            "1st 12 12 12 12 12:15:45 PM 45 12:15:45 2001".into()
        );
    }

    mod json {
        use super::*;
        use crate::utils::normalize_json;

        #[test]
        fn json_valid() {
            #[track_caller]
            fn test(json_expr: &str, expected: Option<bool>) {
                let expr = format!("json_valid({json_expr})");

                assert_eq!(
                    eval_expr(&expr, MySQL),
                    expected.into(),
                    "incorrect result for for `{expr}`"
                );
            }

            test("null", None);

            test("'null'", Some(true));
            test("'1'", Some(true));
            test("'1.5'", Some(true));
            test(r#"'"hi"'"#, Some(true));
            test("'[]'", Some(true));
            test("'[42]'", Some(true));
            test("'{}'", Some(true));
            test(r#"'{ "a": 42 }'"#, Some(true));

            test("''", Some(false));
            test("'hello'", Some(false));
            test("'['", Some(false));
            test("'{'", Some(false));
            test("'+'", Some(false));
            test("'-'", Some(false));

            // Non-text types are allowed and return false.
            test("1", Some(false));
            test("1.5", Some(false));
            test("dayofweek(null)", Some(false));
        }

        // This is more thoroughly tested in `eval::json::tests::json_quote`.
        #[test]
        fn json_quote() {
            #[track_caller]
            fn test(json: &str, expected: &str) {
                let expr = format!("json_quote('{json}')");

                assert_eq!(
                    eval_expr(&expr, MySQL),
                    expected.into(),
                    "incorrect result for `{expr}`"
                );
            }

            test("hello", "\"hello\"");
            test("straße", "\"straße\"");
            test("\0\u{1f}", r#""\u0000\u001f""#);

            // Parsed expr backslash must be escaped.
            test(r#"wo"r\\ld"#, r#""wo\"r\\ld""#);
        }

        #[test]
        fn json_array_length() {
            #[track_caller]
            fn test(json_expr: &str, expected: Option<usize>) {
                for expr in [
                    format!("json_array_length({json_expr})"),
                    format!("jsonb_array_length({json_expr})"),
                ] {
                    assert_eq!(
                        eval_expr(&expr, PostgreSQL),
                        expected.into(),
                        "incorrect result for for `{expr}`"
                    );
                }
            }

            test("null", None);
            test("'[]'", Some(0));
            test("'[1]'", Some(1));
            test("'[1, 2, 3]'", Some(3));
        }

        mod json_depth {
            use super::*;

            #[track_caller]
            fn test(json: &str, expected: usize) {
                let expr = format!("json_depth('{json}')");

                assert_eq!(
                    eval_expr(&expr, MySQL),
                    expected.into(),
                    "incorrect result for for `{expr}`"
                )
            }

            #[test]
            fn scalar() {
                test("1", 1);
                test("1.5", 1);
                test("true", 1);
                test("\"hi\"", 1);
                test("null", 1);
            }

            #[test]
            fn empty_array() {
                test("[]", 1);
            }

            #[test]
            fn empty_object() {
                test("{}", 1);
            }

            #[test]
            fn simple_array() {
                test("[42]", 2);
            }

            #[test]
            fn simple_object() {
                test("{ \"a\": 42 }", 2);
            }

            #[test]
            fn nested_array() {
                test("[[42]]", 3);
                test("[1, [42]]", 3);
                test("[[42], 1]", 3);

                test(r#"{ "a": [42] }"#, 3);
            }

            #[test]
            fn nested_object() {
                test(r#"{ "a": { "b": 42 } }"#, 3);
                test(r#"{ "a": { "b": 42 }, "z": 42 }"#, 3);
                test(r#"{ "z": 42, "a": { "b": 42 } }"#, 3);

                test(r#"[{ "a": 42 }]"#, 3);
                test(r#"[{ "a": 42 }, 42]"#, 3);
                test(r#"[42, { "a": 42 }]"#, 3);
            }

            #[test]
            fn deeply_nested_array() {
                use std::iter::repeat;

                // Recursion limit for parsing.
                let depth = 127;

                let json: String = (repeat('[').take(depth))
                    .chain(repeat(']').take(depth))
                    .collect();

                test(&json, depth);
            }

            #[test]
            fn deeply_nested_object() {
                use std::iter::repeat;

                // Recursion limit for parsing.
                let depth = 127;

                let json: String = (repeat("{\"a\": ").take(depth - 1))
                    .chain(Some("{")) // innermost
                    .chain(repeat("}").take(depth))
                    .collect();

                test(&json, depth);
            }
        }

        #[test]
        fn json_strip_nulls() {
            #[track_caller]
            fn test(json_expr: &str, expected: Option<&str>) {
                // Normalize formatting and convert.
                let expected = expected
                    .map(normalize_json)
                    .map(DfValue::from)
                    .unwrap_or_default();

                for expr in [
                    format!("json_strip_nulls({json_expr})"),
                    format!("jsonb_strip_nulls({json_expr})"),
                ] {
                    assert_eq!(
                        eval_expr(&expr, PostgreSQL),
                        expected,
                        "incorrect result for for `{expr}`"
                    );
                }
            }

            test(r#"'{ "abc" : 1, "xyz": null }'"#, Some(r#"{ "abc": 1 }"#));
            test(
                r#"'[{ "abc" : 1, "xyz": null }]'"#,
                Some(r#"[{ "abc": 1 }]"#),
            );
            test(
                r#"'[{ "abc" : 1, "xyz": [{ "123": null }] }]'"#,
                Some(r#"[{ "abc" : 1, "xyz": [{}] }]"#),
            );
            test("null", None);

            #[track_caller]
            fn test_identity(json: &str) {
                test(&format!("'{json}'"), Some(json));
            }

            test_identity("{}");
            test_identity("[]");
            test_identity("1");
            test_identity("2.0");
            test_identity("true");
            test_identity("false");
            test_identity("null");
            test_identity("[null]");
        }

        #[test]
        fn json_extract_path() {
            #[track_caller]
            fn test(object: &str, keys: &str, expected: Option<&str>) {
                for f in [
                    "json_extract_path",
                    "jsonb_extract_path",
                    "json_extract_path_text",
                    "jsonb_extract_path_text",
                ] {
                    let expr = format!("{f}('{object}', {keys})");
                    assert_eq!(
                        eval_expr(&expr, PostgreSQL),
                        expected.into(),
                        "incorrect result for for `{expr}`"
                    );
                }
            }

            let array = "[[\"world\", 123]]";

            test(array, "'1'", None);
            test(array, "null::text", None);

            test(array, "'0', '0'", Some("\"world\""));
            test(array, "'0', '1'", Some("123"));
            test(array, "'0', '2'", None);
            test(array, "'0', null::text", None);

            let object = r#"{ "hello": ["world"], "abc": [123] }"#;

            test(object, "null::text", None);
            test(object, "'world'", None);

            test(object, "'hello', '0'", Some("\"world\""));
            test(object, "'hello', '1'", None);
            test(object, "'hello', null::text", None);

            test(object, "'abc'::char(3), '0'", Some("123"));
            test(object, "'abc'::char(3), null::text", None);
        }

        mod json_overlaps {
            use super::*;

            #[track_caller]
            fn test_nullable(json1_expr: &str, json2_expr: &str, expected: Option<bool>) {
                let expr = format!("json_overlaps({json1_expr}, {json2_expr})");
                assert_eq!(
                    eval_expr(&expr, PostgreSQL),
                    expected.into(),
                    "incorrect result for `{expr}`"
                );
            }

            #[track_caller]
            fn test_non_null(json1: &str, json2: &str, expected: bool) {
                test_nullable(&format!("'{json1}'"), &format!("'{json2}'"), Some(expected));
            }

            #[test]
            fn null_propagation() {
                test_nullable("null", "null", None);
                test_nullable("null", "'[]'", None);
                test_nullable("'[]'", "null", None);
            }

            #[test]
            fn scalar() {
                test_non_null("true", "true", true);
                test_non_null("true", "false", false);

                test_non_null("1", "1", true);
                test_non_null("1", "0", false);

                test_non_null("1.5", "1.5", true);
                test_non_null("1.5", "0.5", false);

                test_non_null("\"hello\"", "\"hello\"", true);
                test_non_null("\"hello\"", "\"world\"", false);
            }

            #[test]
            fn array() {
                test_non_null("[]", "[]", false);
                test_non_null("[42]", "[]", false);
                test_non_null("[]", "[42]", false);

                test_non_null("[42]", "[42]", true);
                test_non_null("[42]", "[0, 42]", true);
                test_non_null("[42]", "[0, 0, 42]", true);
                test_non_null("[42]", "[0, 42, 0]", true);
                test_non_null("[0]", "[0, 42, 0]", true);

                test_non_null("[[]]", "[[]]", true);
                test_non_null("[[1]]", "[[1]]", true);
                test_non_null("[[1]]", "[[1, 2]]", false);

                test_non_null("[{}]", "[{}]", true);
                test_non_null("[{ \"hello\": 42 }]", "[{ \"hello\": 42 }]", true);
                test_non_null(
                    "[{ \"hello\": 42 }]",
                    "[{ \"hello\": 42, \"world\": 123 }]",
                    false,
                );
            }

            #[test]
            fn object() {
                let obj1 = r#"{ "hello": 42 }"#;
                let obj2 = r#"{ "hello": 42, "world": 123 }"#;
                let obj3 = r#"{ "abc": { "hello": 42 } }"#;
                let obj4 = r#"{ "abc": { "hello": 42, "world": 123 } }"#;

                test_non_null("{}", "{}", false);
                test_non_null(obj1, "{}", false);
                test_non_null("{}", obj1, false);

                test_non_null(obj1, obj1, true);
                test_non_null(obj1, obj2, true);
                test_non_null(obj2, obj1, true);
                test_non_null(obj2, obj2, true);

                test_non_null(obj3, obj3, true);
                test_non_null(obj4, obj4, true);
                test_non_null(obj3, obj4, false);
                test_non_null(obj4, obj3, false);
            }
        }

        mod json_object {
            use super::*;
            use crate::utils::{empty_array_expr, iter_to_array_expr};

            #[track_caller]
            fn test_nullable(
                arg1_expr: &str,
                arg2_expr: Option<&str>,
                expected_json: Option<&str>,
            ) {
                // Normalize formatting and convert.
                let expected_json = expected_json
                    .map(normalize_json)
                    .map(DfValue::from)
                    .unwrap_or_default();

                for f in ["json_object", "jsonb_object"] {
                    let expr = match arg2_expr {
                        None => format!("{f}({arg1_expr})"),
                        Some(arg2_expr) => format!("{f}({arg1_expr}, {arg2_expr})"),
                    };

                    assert_eq!(
                        eval_expr(&expr, PostgreSQL),
                        expected_json,
                        "incorrect result for for `{expr}`"
                    );
                }
            }

            #[track_caller]
            fn test_non_null(arg1_expr: &str, arg2_expr: Option<&str>, expected_json: &str) {
                test_nullable(arg1_expr, arg2_expr, Some(expected_json))
            }

            #[track_caller]
            fn test_error(arg1_expr: &str, arg2_expr: Option<&str>) {
                for f in ["json_object", "jsonb_object"] {
                    let expr = match arg2_expr {
                        None => format!("{f}({arg1_expr})"),
                        Some(arg2_expr) => format!("{f}({arg1_expr}, {arg2_expr})"),
                    };

                    if let Ok(value) = try_eval_expr(&expr, PostgreSQL) {
                        panic!("Expected error for `{expr}`, got {value:?}");
                    };
                }
            }

            #[test]
            fn null_propagation() {
                test_nullable("null", None, None);
                test_nullable("null", Some("array[]"), None);
                test_nullable("array[]", Some("null"), None);
            }

            #[test]
            fn pairs_1d() {
                #[track_caller]
                fn test(pairs: &[&str], expected_json: &str) {
                    let pairs = strings_to_array_expr(pairs);
                    test_non_null(&pairs, None, expected_json);
                }

                test(&[], "{}");
                test(&["abc", "123"], r#"{ "abc": "123" }"#);
                test(
                    &["abc", "123", "hello", "world"],
                    r#"{ "abc": "123", "hello": "world" }"#,
                );
            }

            #[test]
            fn pairs_1d_error() {
                #[track_caller]
                fn test(pairs: &[&str]) {
                    let pairs = strings_to_array_expr(pairs);
                    test_error(&pairs, None);
                }

                // Missing pair value:
                test(&["abc"]);

                // Missing value in second pair:
                test(&["abc", "123", "hello"]);
            }

            #[test]
            fn pairs_2d() {
                #[track_caller]
                fn test(pairs: &[[&str; 2]], expected_json: &str) {
                    let pairs = iter_to_array_expr(pairs.iter().map(strings_to_array_expr));
                    test_non_null(&pairs, None, expected_json);
                }

                test(&[], "{}");
                test(&[["abc", "123"]], r#"{ "abc": "123" }"#);
                test(
                    &[["abc", "123"], ["hello", "world"]],
                    r#"{ "abc": "123", "hello": "world" }"#,
                );
            }

            #[test]
            fn pairs_2d_error() {
                #[track_caller]
                fn test(pairs: &[&[&str]]) {
                    let pairs =
                        iter_to_array_expr(pairs.iter().map(|&pair| strings_to_array_expr(pair)));

                    test_error(&pairs, None);
                }

                // Missing pair value:
                test(&[&["abc"]]);

                // Extra pair elements:
                test(&[&["abc", "123", "hello"]]);
                test(&[&["abc", "123", "hello", "world"]]);

                // Missing value in second pair:
                test(&[&["abc", "123"], &["hello"]]);
            }

            #[test]
            fn keys_and_values() {
                #[track_caller]
                fn test(keys: &[&str], values: &[&str], expected_json: &str) {
                    let keys = strings_to_array_expr(keys);
                    let values = strings_to_array_expr(values);
                    test_non_null(&keys, Some(&values), expected_json);
                }

                test(&[], &[], "{}");
                test(&["abc"], &["123"], r#"{ "abc": "123" }"#);
                test(
                    &["abc", "hello"],
                    &["123", "world"],
                    r#"{ "abc": "123", "hello": "world" }"#,
                );
            }

            #[test]
            fn keys_and_values_error() {
                #[track_caller]
                fn test(keys: &[&str], values: &[&str]) {
                    let keys = strings_to_array_expr(keys);
                    let values = strings_to_array_expr(values);
                    test_error(&keys, Some(&values));
                }

                // Missing pair key:
                test(&[], &["123"]);
                test(&["abc"], &["123", "world"]);

                // Missing pair value:
                test(&["abc"], &[]);
                test(&["abc", "hello"], &["123"]);
            }

            #[test]
            fn empty() {
                // PostgreSQL allows empty arrays of any number of dimensions.
                for arg1_dimensions in 1..=5 {
                    let arg1 = empty_array_expr(arg1_dimensions);
                    test_non_null(&arg1, None, "{}");

                    for arg2_dimensions in 1..=5 {
                        let arg2 = empty_array_expr(arg2_dimensions);
                        test_non_null(&arg1, Some(&arg2), "{}");
                    }
                }
            }
        }

        mod jsonb_insert {
            use super::*;

            #[track_caller]
            fn test_nullable(
                json: &str,
                keys: &str,
                inserted_json: &str,
                insert_after: Option<bool>,
                expected_json: Option<&str>,
            ) {
                // Normalize formatting and convert.
                let expected_json = expected_json
                    .map(normalize_json)
                    .map(DfValue::from)
                    .unwrap_or_default();

                let insert_after_args: &[&str] = match insert_after {
                    // Test calling with explicit and implicit false `insert_after` argument.
                    Some(false) => &[", false", ""],
                    Some(true) => &[", true"],

                    // Test null result.
                    None => &[", null"],
                };

                for insert_after_arg in insert_after_args {
                    let expr =
                        format!("jsonb_insert({json}, {keys}, {inserted_json}{insert_after_arg})");
                    assert_eq!(
                        eval_expr(&expr, PostgreSQL),
                        expected_json,
                        "incorrect result for `{expr}`"
                    );
                }
            }

            #[track_caller]
            fn test_non_null(
                json: &str,
                keys: &[&str],
                inserted_json: &str,
                insert_after: bool,
                expected_json: &str,
            ) {
                test_nullable(
                    &format!("'{json}'"),
                    &strings_to_array_expr(keys),
                    &format!("'{inserted_json}'"),
                    Some(insert_after),
                    Some(expected_json),
                )
            }

            #[track_caller]
            fn test_error(json: &str, keys: &[&str], inserted_json: &str, insert_after: bool) {
                let keys = strings_to_array_expr(keys);
                let expr =
                    format!("jsonb_insert('{json}', {keys}, '{inserted_json}', {insert_after})");

                if let Ok(value) = try_eval_expr(&expr, PostgreSQL) {
                    panic!("Expected error for `{expr}`, got {value:?}");
                }
            }

            #[test]
            fn null_propagation() {
                test_nullable("null", "array[]", "'42'", Some(false), None);
                test_nullable("'{}'", "null", "'42'", Some(false), None);
                test_nullable("'{}'", "array[]", "null", Some(false), None);
                test_nullable("'{}'", "array[]", "'42'", None, None);
            }

            #[test]
            fn scalar() {
                test_error("1", &["0"], "42", false);
                test_error("1", &["0"], "42", true);

                test_error("1.5", &["0"], "42", false);
                test_error("1.5", &["0"], "42", true);

                test_error("true", &["0"], "42", false);
                test_error("true", &["0"], "42", true);

                test_error("\"hi\"", &["0"], "42", false);
                test_error("\"hi\"", &["0"], "42", true);

                test_error("null", &["0"], "42", false);
                test_error("null", &["0"], "42", true);
            }

            // NOTE: Tests for one-dimension array cases are done directly on the
            // `insert_bidirectional` utility function.

            #[test]
            fn object() {
                // Insertable:
                test_non_null("{}", &["a"], "42", false, r#"{"a": 42}"#);
                test_non_null("{}", &["a"], "42", true, r#"{"a": 42}"#);

                // Not insertable:
                test_non_null("{\"a\": 0}", &["a"], "42", false, "{\"a\": 0}");
                test_non_null("{\"a\": 0}", &["a"], "42", true, "{\"a\": 0}");

                // Not found:
                test_non_null("{}", &["a", "b"], "42", false, "{}");
                test_non_null("{}", &["a", "b"], "42", true, "{}");
            }

            #[test]
            fn array_nested() {
                let array = "[[[0, 1, 2]]]";
                let keys = ["0", "0", "0"];

                test_non_null(array, &keys, "42", false, "[[[42, 0, 1, 2]]]");
                test_non_null(array, &keys, "42", true, "[[[0, 42, 1, 2]]]");
            }

            #[test]
            fn object_nested() {
                let object = r#"{ "a": { "b": {} } }"#;
                let expected = r#"{ "a": { "b": { "c": 42 } } }"#;
                let keys = ["a", "b", "c"];

                test_non_null(object, &keys, "42", false, expected);
                test_non_null(object, &keys, "42", true, expected);
            }
        }

        mod jsonb_set {
            use super::*;

            #[track_caller]
            fn test_nullable(
                json: &str,
                keys: &str,
                new_json: &str,
                create_if_missing: Option<bool>,
                expected_json: Option<&str>,
            ) {
                // Normalize formatting and convert.
                let expected_json = expected_json
                    .map(normalize_json)
                    .map(DfValue::from)
                    .unwrap_or_default();

                let create_if_missing_args: &[&str] = match create_if_missing {
                    // Test calling with explicit and implicit true `create_if_missing` argument.
                    Some(true) => &[", true", ""],
                    Some(false) => &[", false"],

                    // Test null result.
                    None => &[", null"],
                };

                for create_if_missing_arg in create_if_missing_args {
                    let expr =
                        format!("jsonb_set({json}, {keys}, {new_json}{create_if_missing_arg})");
                    assert_eq!(
                        eval_expr(&expr, PostgreSQL),
                        expected_json,
                        "incorrect result for for `{expr}`"
                    );
                }
            }

            #[track_caller]
            fn test_non_null(
                json: &str,
                keys: &[&str],
                new_json: &str,
                create_if_missing: bool,
                expected_json: &str,
            ) {
                test_nullable(
                    &format!("'{json}'"),
                    &strings_to_array_expr(keys),
                    &format!("'{new_json}'"),
                    Some(create_if_missing),
                    Some(expected_json),
                )
            }

            #[track_caller]
            fn test_error(json: &str, keys: &[&str], new_json: &str, create_if_missing: bool) {
                let keys = strings_to_array_expr(keys);
                let expr =
                    format!("jsonb_set('{json}', {keys}, '{new_json}', {create_if_missing})");

                if let Ok(value) = try_eval_expr(&expr, PostgreSQL) {
                    panic!("Expected error for `{expr}`, got {value:?}");
                }
            }

            #[test]
            fn null_propagation() {
                test_nullable("null", "array[]", "'42'", Some(false), None);
                test_nullable("'{}'", "null", "'42'", Some(false), None);
                test_nullable("'{}'", "array[]", "null", Some(false), None);
                test_nullable("'{}'", "array[]", "'42'", None, None);
            }

            #[test]
            fn scalar() {
                test_error("1", &["0"], "42", false);
                test_error("1", &["0"], "42", true);

                test_error("1.5", &["0"], "42", false);
                test_error("1.5", &["0"], "42", true);

                test_error("true", &["0"], "42", false);
                test_error("true", &["0"], "42", true);

                test_error("\"hi\"", &["0"], "42", false);
                test_error("\"hi\"", &["0"], "42", true);

                test_error("null", &["0"], "42", false);
                test_error("null", &["0"], "42", true);
            }

            #[test]
            fn array_empty() {
                test_non_null("[]", &["0"], "42", false, "[]");
                test_non_null("[]", &["0"], "42", true, "[42]");

                test_non_null("[]", &["1"], "42", false, "[]");
                test_non_null("[]", &["1"], "42", true, "[42]");

                test_non_null("[]", &["-1"], "42", false, "[]");
                test_non_null("[]", &["-1"], "42", true, "[42]");
            }

            #[test]
            fn array_positive() {
                let array = "[0, 1, 2]";

                // Positive start:
                test_non_null(array, &["0"], "42", false, "[42, 1, 2]");
                test_non_null(array, &["0"], "42", true, "[42, 1, 2]");

                // Positive middle:
                test_non_null(array, &["1"], "42", false, "[0, 42, 2]");
                test_non_null(array, &["1"], "42", true, "[0, 42, 2]");

                // Positive end:
                test_non_null(array, &["2"], "42", false, "[0, 1, 42]");
                test_non_null(array, &["2"], "42", true, "[0, 1, 42]");

                // Positive end, out-of-bounds:
                test_non_null(array, &["3"], "42", false, "[0, 1, 2]");
                test_non_null(array, &["3"], "42", true, "[0, 1, 2, 42]");
                test_non_null(array, &["4"], "42", false, "[0, 1, 2]");
                test_non_null(array, &["4"], "42", true, "[0, 1, 2, 42]");
            }

            #[test]
            fn array_negative() {
                let array = "[0, 1, 2]";

                // Negative start, out-of-bounds:
                test_non_null(array, &["-4"], "42", false, "[0, 1, 2]");
                test_non_null(array, &["-4"], "42", true, "[42, 0, 1, 2]");
                test_non_null(array, &["-5"], "42", false, "[0, 1, 2]");
                test_non_null(array, &["-5"], "42", true, "[42, 0, 1, 2]");

                // Negative start:
                test_non_null(array, &["-3"], "42", false, "[42, 1, 2]");
                test_non_null(array, &["-3"], "42", true, "[42, 1, 2]");

                // Negative middle:
                test_non_null(array, &["-2"], "42", false, "[0, 42, 2]");
                test_non_null(array, &["-2"], "42", true, "[0, 42, 2]");

                // Negative end:
                test_non_null(array, &["-1"], "42", false, "[0, 1, 42]");
                test_non_null(array, &["-1"], "42", true, "[0, 1, 42]");
            }

            #[test]
            fn object() {
                // Empty:
                test_non_null("{}", &["a"], "42", false, "{}");
                test_non_null("{}", &["a"], "42", true, r#"{"a": 42}"#);

                // Override:
                test_non_null("{\"a\": 0}", &["a"], "42", false, "{\"a\": 42}");
                test_non_null("{\"a\": 0}", &["a"], "42", true, "{\"a\": 42}");

                // Not found:
                test_non_null("{}", &["a", "b"], "42", false, "{}");
                test_non_null("{}", &["a", "b"], "42", true, "{}");
            }

            #[test]
            fn array_nested() {
                let array = "[[[0, 1, 2]]]";
                let keys = ["0", "0", "0"];

                test_non_null(array, &keys, "42", false, "[[[42, 1, 2]]]");
                test_non_null(array, &keys, "42", true, "[[[42, 1, 2]]]");
            }

            #[test]
            fn object_nested() {
                let object = r#"{ "a": { "b": { "c": 0 } } }"#;
                let expected = r#"{ "a": { "b": { "c": 42 } } }"#;
                let keys = ["a", "b", "c"];

                test_non_null(object, &keys, "42", false, expected);
                test_non_null(object, &keys, "42", true, expected);
            }
        }

        mod jsonb_set_lax {
            use super::*;
            use crate::eval::json::NullValueTreatment;

            #[track_caller]
            fn test_nullable(
                json: &str,
                keys: &str,
                new_json: &str,
                create_if_missing: Option<bool>,
                null_value_treatment: Option<&str>,
                expected_json: Option<&str>,
            ) {
                // Normalize formatting and convert.
                let expected_json = expected_json
                    .map(normalize_json)
                    .map(DfValue::from)
                    .unwrap_or_default();

                let create_if_missing_args: &[&str] = match create_if_missing {
                    // Test calling with explicit and implicit default argument.
                    Some(true) => &[", true", ""],
                    Some(false) => &[", false"],

                    // Test null result.
                    None => &[", null"],
                };

                for create_if_missing_arg in create_if_missing_args {
                    let null_value_treatment_args: Vec<String> = match null_value_treatment {
                        // Test calling with explicit and implicit default argument.
                        Some(arg @ "use_json_null") => {
                            vec![format!(", '{arg}'"), "".to_owned()]
                        }
                        Some(arg) => vec![format!(", '{arg}'")],

                        // Test null result.
                        None => vec![", null".to_owned()],
                    };

                    for null_value_treatment_arg in null_value_treatment_args {
                        // Skip explicit last arg if previous is implicit.
                        if create_if_missing_arg.is_empty() && !null_value_treatment_arg.is_empty()
                        {
                            continue;
                        }

                        let expr = format!("jsonb_set_lax({json}, {keys}, {new_json}{create_if_missing_arg}{null_value_treatment_arg})");
                        assert_eq!(
                            eval_expr(&expr, PostgreSQL),
                            expected_json,
                            "incorrect result for for `{expr}`"
                        );
                    }
                }
            }

            #[track_caller]
            fn test_non_null(
                json: &str,
                keys: &[&str],
                new_json: Option<&str>,
                create_if_missing: bool,
                null_value_treatment: &str,
                expected_json: &str,
            ) {
                let new_json = new_json
                    .map(|j| format!("'{j}'"))
                    .unwrap_or_else(|| "null".to_owned());

                test_nullable(
                    &format!("'{json}'"),
                    &strings_to_array_expr(keys),
                    &new_json,
                    Some(create_if_missing),
                    Some(null_value_treatment),
                    Some(expected_json),
                );
            }

            #[track_caller]
            fn test_error(
                json: &str,
                keys: &[&str],
                new_json: Option<&str>,
                create_if_missing: bool,
                null_value_treatment: &str,
            ) {
                let keys = strings_to_array_expr(keys);

                let new_json = new_json
                    .map(|j| format!("'{j}'"))
                    .unwrap_or_else(|| "null".to_owned());

                let expr =
                    format!("jsonb_set_lax('{json}', {keys}, {new_json}, {create_if_missing}, '{null_value_treatment}')");

                if let Ok(value) = try_eval_expr(&expr, PostgreSQL) {
                    panic!("Expected error for `{expr}`, got {value:?}");
                }
            }

            #[test]
            fn null_propagation() {
                for null_value_treatment in NullValueTreatment::all() {
                    let null_value_treatment = null_value_treatment.to_string();
                    test_nullable(
                        "null",
                        "array[]",
                        "'42'",
                        Some(false),
                        Some(&null_value_treatment),
                        None,
                    );
                    test_nullable(
                        "'{}'",
                        "null",
                        "'42'",
                        Some(false),
                        Some(&null_value_treatment),
                        None,
                    );
                    test_nullable(
                        "'{}'",
                        "array[]",
                        "'42'",
                        None,
                        Some(&null_value_treatment),
                        None,
                    );
                }
                test_nullable("'{}'", "null", "'42'", Some(false), None, None);
            }

            // NOTE: All subsequent tests are for behavior specific to how `jsonb_set_lax` uses
            // `null_value_treatment`. Because `jsonb_set_lax` reuses the same logic as `jsonb_set`,
            // much of the behavior is already covered by `jsonb_set` tests.

            #[test]
            fn unknown_null_value_treatment() {
                test_error("[42]", &["0"], None, false, "unknown");
                test_error("[42]", &["0"], None, true, "unknown");
            }

            #[test]
            fn array_delete_key() {
                let array = "[[42]]";

                // Empty:
                test_non_null("[]", &["0"], None, false, "delete_key", "[]");
                test_non_null("[]", &["0"], None, true, "delete_key", "[]");

                // Positive found:
                test_non_null(array, &["0"], None, false, "delete_key", "[]");
                test_non_null(array, &["0"], None, true, "delete_key", "[]");
                test_non_null(array, &["0", "0"], None, false, "delete_key", "[[]]");
                test_non_null(array, &["0", "0"], None, true, "delete_key", "[[]]");

                // Negative found:
                test_non_null(array, &["-1"], None, false, "delete_key", "[]");
                test_non_null(array, &["-1"], None, true, "delete_key", "[]");
                test_non_null(array, &["0", "-1"], None, false, "delete_key", "[[]]");
                test_non_null(array, &["0", "-1"], None, true, "delete_key", "[[]]");
                test_non_null(array, &["-1", "-1"], None, false, "delete_key", "[[]]");
                test_non_null(array, &["-1", "-1"], None, true, "delete_key", "[[]]");

                // Positive out-of-bounds:
                test_non_null(array, &["0", "1"], None, false, "delete_key", array);
                test_non_null(array, &["0", "1"], None, true, "delete_key", array);
                test_non_null(array, &["1"], None, false, "delete_key", array);
                test_non_null(array, &["1"], None, true, "delete_key", array);
                test_non_null(array, &["1", "0"], None, false, "delete_key", array);
                test_non_null(array, &["1", "0"], None, true, "delete_key", array);

                // Negative out-of-bounds:
                test_non_null(array, &["0", "-2"], None, false, "delete_key", array);
                test_non_null(array, &["0", "-2"], None, true, "delete_key", array);
                test_non_null(array, &["-2"], None, false, "delete_key", array);
                test_non_null(array, &["-2"], None, true, "delete_key", array);
                test_non_null(array, &["-2", "0"], None, false, "delete_key", array);
                test_non_null(array, &["-2", "0"], None, true, "delete_key", array);
            }

            #[test]
            fn object_delete_key() {
                let object = r#"{ "a": { "b": 42 } }"#;
                let del_object = r#"{ "a": {} }"#;

                // Empty:
                test_non_null("{}", &["x"], None, false, "delete_key", "{}");
                test_non_null("{}", &["x"], None, true, "delete_key", "{}");

                // Found:
                test_non_null(object, &["a"], None, false, "delete_key", "{}");
                test_non_null(object, &["a"], None, true, "delete_key", "{}");
                test_non_null(object, &["a", "b"], None, false, "delete_key", del_object);
                test_non_null(object, &["a", "b"], None, true, "delete_key", del_object);

                // Missing:
                test_non_null(object, &["x"], None, false, "delete_key", object);
                test_non_null(object, &["x"], None, true, "delete_key", object);
                test_non_null(object, &["a", "x"], None, false, "delete_key", object);
                test_non_null(object, &["a", "x"], None, true, "delete_key", object);
            }

            #[test]
            fn array_return_target() {
                test_non_null("[]", &["0"], None, false, "return_target", "[]");
                test_non_null("[]", &["0"], None, true, "return_target", "[]");

                test_non_null("[42]", &["0"], None, false, "return_target", "[42]");
                test_non_null("[42]", &["0"], None, true, "return_target", "[42]");
            }

            #[test]
            fn object_return_target() {
                let object = r#"{ "a": { "b": 42 } }"#;

                test_non_null("{}", &["a"], None, false, "return_target", "{}");
                test_non_null("{}", &["a"], None, true, "return_target", "{}");

                test_non_null(object, &["a"], None, false, "return_target", object);
                test_non_null(object, &["a"], None, true, "return_target", object);
            }

            #[test]
            fn array_use_json_null() {
                test_non_null("[]", &["0"], None, false, "use_json_null", "[]");
                test_non_null("[]", &["0"], None, true, "use_json_null", "[null]");

                test_non_null("[42]", &["0"], None, false, "use_json_null", "[null]");
                test_non_null("[42]", &["0"], None, true, "use_json_null", "[null]");
            }

            #[test]
            fn object_use_json_null() {
                let object = r#"{ "a": 42 }"#;
                let object_with_null = r#"{ "a": null }"#;

                test_non_null("{}", &["a"], None, false, "use_json_null", "{}");
                test_non_null("{}", &["a"], None, true, "use_json_null", object_with_null);

                test_non_null(
                    object,
                    &["a"],
                    None,
                    false,
                    "use_json_null",
                    object_with_null,
                );
                test_non_null(
                    object,
                    &["a"],
                    None,
                    true,
                    "use_json_null",
                    object_with_null,
                );
            }

            #[test]
            fn array_raise_exception() {
                test_error("[42]", &["0"], None, false, "raise_exception");
                test_error("[42]", &["0"], None, true, "raise_exception");
            }

            #[test]
            fn object_raise_exception() {
                let object = r#"{ "a": 42 }"#;

                test_error(object, &["a"], None, false, "raise_exception");
                test_error(object, &["a"], None, true, "raise_exception");
            }
        }
    }

    #[test]
    fn array_to_string() {
        #[track_caller]
        fn test(array: &str, expected: &str) {
            let expr = format!("array_to_string('{array}', ',')");

            assert_eq!(
                eval_expr(&expr, PostgreSQL),
                expected.into(),
                "incorrect result for for `{expr}`"
            );

            let null_expr = format!("array_to_string('{array}', ',', null)");

            assert_eq!(
                eval_expr(&null_expr, PostgreSQL),
                expected.into(),
                "incorrect result for for `{null_expr}`"
            );
        }

        test("{}", "");
        test("{1,2,3,null,5}", "1,2,3,5");
        test("{null,1,2,3,null,5}", "1,2,3,5");
        test("{{1,2},{3,4}}", "1,2,3,4");

        #[track_caller]
        fn test_with_null_string(array: &str, expected: &str) {
            let expr = format!("array_to_string('{array}', ',', '*')");

            assert_eq!(
                eval_expr(&expr, PostgreSQL),
                expected.into(),
                "incorrect result for for `{expr}`"
            );
        }

        test_with_null_string("{}", "");
        test_with_null_string("{1,2,3,null,5}", "1,2,3,*,5");
        test_with_null_string("{null,1,2,3,null,5}", "*,1,2,3,*,5");
        test_with_null_string("{{1,2},{3,4},{null,5}}", "1,2,3,4,*,5");
    }
}
