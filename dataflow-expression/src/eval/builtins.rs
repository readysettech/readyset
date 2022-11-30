use std::borrow::Borrow;
use std::cmp::Ordering;
use std::fmt::Write;
use std::ops::{Add, Div, Mul, Sub};

use chrono::{Datelike, LocalResult, Month, NaiveDate, NaiveDateTime, TimeZone, Timelike, Weekday};
use chrono_tz::Tz;
use maths::int::integer_rnd;
use mysql_time::MySqlTime;
use readyset_data::{DfType, DfValue};
use readyset_errors::{invalid_err, ReadySetError, ReadySetResult};
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rust_decimal::Decimal;
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
    /// Calcluate the week (and year!) number of a date-like value according to the ...algorithm...
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
            .num_days_from_ce() as i32;
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
            days = days_in_year(year) as i32;
            first_daynr -= days as i32;
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
            BuiltinFunction::ConvertTZ {
                args: [arg1, arg2, arg3],
                subsecond_digits,
            } => {
                let param1 = arg1.eval(record)?;
                let param2 = arg2.eval(record)?;
                let param3 = arg3.eval(record)?;

                let param1_cast = try_cast_or_none!(
                    param1,
                    &DfType::Timestamp {
                        subsecond_digits: *subsecond_digits
                    },
                    arg1.ty()
                );
                let param2_cast = try_cast_or_none!(param2, &DfType::DEFAULT_TEXT, arg2.ty());
                let param3_cast = try_cast_or_none!(param3, &DfType::DEFAULT_TEXT, arg3.ty());

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
                let param_cast = try_cast_or_none!(param, &DfType::Date, arg.ty());
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
                let param_cast = try_cast_or_none!(param, &DfType::Date, arg.ty());
                Ok(DfValue::UnsignedInt(
                    month(&(NaiveDate::try_from(non_null!(&param_cast))?)) as u64,
                ))
            }
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
                .ok_or_else(|| invalid_err!("cannot get array length of a non-array")),
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
                let mut s =
                    <&str>::try_from(&non_null!(arg1.eval(record)?).coerce_to(ty, arg1.ty())?)?
                        .to_owned();

                for arg in rest_args {
                    let val = non_null!(arg.eval(record)?).coerce_to(ty, arg.ty())?;
                    s.push_str((&val).try_into()?)
                }

                Ok(s.into())
            }
            BuiltinFunction::Substring(string, from, len) => {
                let string = non_null!(string.eval(record)?).coerce_to(ty, string.ty())?;
                let s = <&str>::try_from(&string)?;

                let from = match from {
                    Some(from) => non_null!(from.eval(record)?)
                        .coerce_to(&DfType::BigInt, from.ty())?
                        .try_into()?,
                    None => 1i64,
                };

                let len = match len {
                    Some(len) => non_null!(len.eval(record)?)
                        .coerce_to(&DfType::BigInt, len.ty())?
                        .try_into()?,
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
                let string = non_null!(string.eval(record)?)
                    .coerce_to(&DfType::DEFAULT_TEXT, string.ty())?;
                let delimiter = non_null!(delimiter.eval(record)?)
                    .coerce_to(&DfType::DEFAULT_TEXT, delimiter.ty())?;
                let field = <i64>::try_from(
                    non_null!(field.eval(record)?).coerce_to(&DfType::Int, field.ty())?,
                )?;

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
                    Ordering::Equal => Err(invalid_err!("field position must not be zero")),
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
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::{NaiveTime, Timelike};
    use launchpad::arbitrary::arbitrary_timestamp_naive_date_time;
    use lazy_static::lazy_static;
    use nom_sql::parse_expr;
    use nom_sql::Dialect::*;
    use readyset_errors::internal;
    use test_strategy::proptest;

    use super::*;
    use crate::eval::tests::eval_expr;
    use crate::lower::tests::resolve_columns;
    use crate::utils::{make_call, make_column, make_literal, strings_to_array_expr};
    use crate::Dialect;

    #[test]
    fn eval_call_convert_tz() {
        let expr = make_call(BuiltinFunction::ConvertTZ {
            args: [make_column(0), make_column(1), make_column(2)],
            subsecond_digits: Dialect::DEFAULT_MYSQL.default_subsecond_digits(),
        });
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
            DfValue::Time(MySqlTime::from_hmsus(false, 47, 0, 0, 0))
        );
        assert_eq!(
            expr.eval::<DfValue>(&[
                param1.to_string().try_into().unwrap(),
                param2.to_string().try_into().unwrap()
            ])
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
            expr.eval::<DfValue>(&[
                param1.to_string().try_into().unwrap(),
                param2.to_string().try_into().unwrap()
            ])
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
            DfValue::Time(MySqlTime::from_hmsus(true, 1, 0, 0, 0))
        );
        assert_eq!(
            expr.eval::<DfValue>(&[
                param1.to_string().try_into().unwrap(),
                param2.to_string().try_into().unwrap()
            ])
            .unwrap(),
            DfValue::Time(MySqlTime::from_hmsus(true, 1, 0, 0, 0))
        );
        let param1 = "not a date nor time";
        let param2 = "01:00:00.4";
        assert_eq!(
            expr.eval::<DfValue>(&[param1.try_into().unwrap(), param2.try_into().unwrap()])
                .unwrap(),
            DfValue::Time(MySqlTime::from_hmsus(false, 1, 0, 0, 400_000))
        );
        assert_eq!(
            expr.eval::<DfValue>(&[param2.try_into().unwrap(), param1.try_into().unwrap()])
                .unwrap(),
            DfValue::Time(MySqlTime::from_hmsus(true, 1, 0, 0, 400_000))
        );

        let param2 = "10000.4";
        assert_eq!(
            expr.eval::<DfValue>(&[param1.try_into().unwrap(), param2.try_into().unwrap()])
                .unwrap(),
            DfValue::Time(MySqlTime::from_hmsus(false, 1, 0, 0, 400_000))
        );
        assert_eq!(
            expr.eval::<DfValue>(&[param2.try_into().unwrap(), param1.try_into().unwrap()])
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
        let param1 = MySqlTime::from_hmsus(true, 10, 12, 44, 123_000);
        assert_eq!(
            expr.eval::<DfValue>(&[param2.into(), param1.into()])
                .unwrap(),
            DfValue::Time(MySqlTime::from_hmsus(true, 7, 1, 9, 123_000))
        );
        assert_eq!(
            expr.eval::<DfValue>(&[
                param2.to_string().try_into().unwrap(),
                param1.to_string().try_into().unwrap()
            ])
            .unwrap(),
            DfValue::Time(MySqlTime::from_hmsus(true, 7, 1, 9, 123_000))
        );
        let param1 = "not a date nor time";
        let param2 = "01:00:00.4";
        assert_eq!(
            expr.eval::<DfValue>(&[param1.try_into().unwrap(), param2.try_into().unwrap()])
                .unwrap(),
            DfValue::Time(MySqlTime::from_hmsus(true, 1, 0, 0, 400_000))
        );
        assert_eq!(
            expr.eval::<DfValue>(&[param2.try_into().unwrap(), param1.try_into().unwrap()])
                .unwrap(),
            DfValue::Time(MySqlTime::from_hmsus(true, 1, 0, 0, 400_000))
        );

        let param2 = "10000.4";
        assert_eq!(
            expr.eval::<DfValue>(&[param1.try_into().unwrap(), param2.try_into().unwrap()])
                .unwrap(),
            DfValue::Time(MySqlTime::from_hmsus(true, 1, 0, 0, 400_000))
        );
        assert_eq!(
            expr.eval::<DfValue>(&[param2.try_into().unwrap(), param1.try_into().unwrap()])
                .unwrap(),
            DfValue::Time(MySqlTime::from_hmsus(true, 1, 0, 0, 400_000))
        );

        let param2: f32 = 3.57;
        assert_eq!(
            expr.eval::<DfValue>(&[
                param1.try_into().unwrap(),
                DfValue::try_from(param2).unwrap()
            ])
            .unwrap(),
            DfValue::Time(MySqlTime::from_microseconds(
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
            DfValue::Time(MySqlTime::from_microseconds(
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

        assert_eq!(expr.eval(&[DfValue::None]), Ok(DfValue::None));
    }

    #[test]
    fn month_null() {
        let expr = make_call(BuiltinFunction::Month(make_column(0)));
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

    #[test]
    fn coalesce() {
        let expr = Expr::Call {
            func: Box::new(BuiltinFunction::Coalesce(
                Expr::Column {
                    index: 0,
                    ty: DfType::Unknown,
                },
                vec![Expr::Literal {
                    val: 1.into(),
                    ty: DfType::Int,
                }],
            )),
            ty: DfType::Unknown,
        };
        let call_with = |val: DfValue| expr.eval(&[val]);

        assert_eq!(call_with(DfValue::None).unwrap(), 1.into());
        assert_eq!(call_with(123.into()).unwrap(), 123.into());
    }

    #[test]
    fn coalesce_more_args() {
        let expr = Expr::Call {
            func: Box::new(BuiltinFunction::Coalesce(
                Expr::Column {
                    index: 0,
                    ty: DfType::Unknown,
                },
                vec![
                    Expr::Column {
                        index: 1,
                        ty: DfType::Unknown,
                    },
                    Expr::Literal {
                        val: 1.into(),
                        ty: DfType::Int,
                    },
                ],
            )),
            ty: DfType::Unknown,
        };
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
        let expr = Expr::Call {
            func: Box::new(BuiltinFunction::Concat(
                Expr::Literal {
                    val: "My".into(),
                    ty: DfType::DEFAULT_TEXT,
                },
                vec![
                    Expr::Literal {
                        val: "S".into(),
                        ty: DfType::DEFAULT_TEXT,
                    },
                    Expr::Literal {
                        val: "QL".into(),
                        ty: DfType::DEFAULT_TEXT,
                    },
                ],
            )),
            ty: DfType::DEFAULT_TEXT,
        };

        let res = expr.eval::<DfValue>(&[]).unwrap();
        assert_eq!(res, "MySQL".into());
    }

    #[test]
    fn concat_with_nulls() {
        let expr = Expr::Call {
            func: Box::new(BuiltinFunction::Concat(
                Expr::Literal {
                    val: "My".into(),
                    ty: DfType::DEFAULT_TEXT,
                },
                vec![
                    Expr::Literal {
                        val: DfValue::None,
                        ty: DfType::DEFAULT_TEXT,
                    },
                    Expr::Literal {
                        val: "QL".into(),
                        ty: DfType::DEFAULT_TEXT,
                    },
                ],
            )),
            ty: DfType::DEFAULT_TEXT,
        };

        let res = expr.eval::<DfValue>(&[]).unwrap();
        assert_eq!(res, DfValue::None);
    }

    #[test]
    fn substring_with_from_and_for() {
        let expr = Expr::Call {
            func: Box::new(BuiltinFunction::Substring(
                Expr::Literal {
                    val: "abcdef".into(),
                    ty: DfType::DEFAULT_TEXT,
                },
                Some(Expr::Column {
                    index: 0,
                    ty: DfType::Int,
                }),
                Some(Expr::Column {
                    index: 1,
                    ty: DfType::Int,
                }),
            )),
            ty: DfType::DEFAULT_TEXT,
        };
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
        let expr = Expr::Call {
            func: Box::new(BuiltinFunction::Substring(
                Expr::Literal {
                    val: "é".into(),
                    ty: DfType::DEFAULT_TEXT,
                },
                Some(Expr::Literal {
                    val: 1.into(),
                    ty: DfType::Int,
                }),
                Some(Expr::Literal {
                    val: 1.into(),
                    ty: DfType::Int,
                }),
            )),
            ty: DfType::DEFAULT_TEXT,
        };
        let res = expr.eval::<DfValue>(&[]).unwrap();
        assert_eq!(res, "é".into());
    }

    #[test]
    fn substring_with_from() {
        let expr = Expr::Call {
            func: Box::new(BuiltinFunction::Substring(
                Expr::Literal {
                    val: "abcdef".into(),
                    ty: DfType::DEFAULT_TEXT,
                },
                Some(Expr::Column {
                    index: 0,
                    ty: DfType::Int,
                }),
                None,
            )),
            ty: DfType::DEFAULT_TEXT,
        };
        let res = expr.eval::<DfValue>(&[2.into()]).unwrap();
        assert_eq!(res, "bcdef".into());
    }

    #[test]
    fn substring_with_for() {
        let expr = Expr::Call {
            func: Box::new(BuiltinFunction::Substring(
                Expr::Literal {
                    val: "abcdef".into(),
                    ty: DfType::DEFAULT_TEXT,
                },
                None,
                Some(Expr::Column {
                    index: 0,
                    ty: DfType::Int,
                }),
            )),
            ty: DfType::DEFAULT_TEXT,
        };
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

        #[track_caller]
        fn normalize_json(json: &str) -> String {
            json.parse::<serde_json::Value>().unwrap().to_string()
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
                        "incorrect result for for `{expr}`"
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

            #[test]
            fn null_propagation() {
                test_nullable("null", "array[]", "'42'", Some(false), None);
                test_nullable("'{}'", "null", "'42'", Some(false), None);
                test_nullable("'{}'", "array[]", "null", Some(false), None);
                test_nullable("'{}'", "array[]", "'42'", None, None);
            }

            #[test]
            fn scalar() {
                test_non_null("1", &["0"], "42", false, "1");
                test_non_null("1", &["0"], "42", true, "1");

                test_non_null("1.5", &["0"], "42", false, "1.5");
                test_non_null("1.5", &["0"], "42", true, "1.5");

                test_non_null("true", &["0"], "42", false, "true");
                test_non_null("true", &["0"], "42", true, "true");

                test_non_null("\"hi\"", &["0"], "42", false, "\"hi\"");
                test_non_null("\"hi\"", &["0"], "42", true, "\"hi\"");

                test_non_null("null", &["0"], "42", false, "null");
                test_non_null("null", &["0"], "42", true, "null");
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
    }
}
