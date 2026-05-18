//! AST for `sqllogictest` files. See the [SQLite documentation][1] for more information.
//!
//! [1]: https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki

use std::borrow::Cow;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::error::Error;
use std::fmt::{self, Display};
use std::num::TryFromIntError;
use std::ops::RangeInclusive;
use std::str::FromStr;
use std::{cmp, vec};

use anyhow::{anyhow, bail};
use bit_vec::BitVec;
use chrono::{DateTime, FixedOffset, NaiveDate, NaiveTime, TimeZone};
use derive_more::{From, TryInto};
use itertools::Itertools;
use mysql_common::chrono::NaiveDateTime;
use mysql_time::MySqlTime;
use pgsql::types::to_sql_checked;
use readyset_data::{DfValue, TIMESTAMP_FORMAT};
use readyset_decimal::Decimal;
use readyset_sql::ast::{Literal, SqlQuery};
use serde_json::Value as JsonValue;
use thiserror::Error;
use tokio_postgres as pgsql;

/// The expected result of a statement
#[derive(Debug, Eq, PartialEq, Clone)]
pub enum StatementResult {
    /// The statement should succeed
    Ok,
    /// The statement should fail
    Error { pattern: Option<String> },
}

impl Display for StatementResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StatementResult::Ok => f.write_str("ok"),
            StatementResult::Error { pattern } => {
                f.write_str("error")?;
                if let Some(pattern) = pattern {
                    write!(f, " \"{pattern}\"")?;
                }
                Ok(())
            }
        }
    }
}

/// A conditional for either a [`Statement`] or a [`Query`]. Can be used to omit or include tests on
/// specific database engines
#[derive(Debug, Eq, PartialEq, Clone)]
pub enum Conditional {
    /// Skip this [`Statement`] or [`Query`] on the database engine with the given name.
    SkipIf(String),
    /// Only run this [`Statement`] or [`Query`] on the database engine with the given name.
    OnlyIf(String),
    /// Invert the ['Query'] result if no upstream connector is present. Pass becomes fail, fail
    /// becomes pass. Ignored for ['Statement'].
    InvertNoUpstream,
}

impl Display for Conditional {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Conditional::SkipIf(engine) => write!(f, "skipif {engine}"),
            Conditional::OnlyIf(engine) => write!(f, "onlyif {engine}"),
            Conditional::InvertNoUpstream => write!(f, "invertupstream"),
        }
    }
}

/// Run a statement against the database engine
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct Statement {
    /// The expected result of the statement
    pub result: StatementResult,
    /// The SQL string to run
    pub command: String,
    /// Optional list of [`Conditional`]s for the statement
    pub conditionals: Vec<Conditional>,
}

impl Statement {
    pub fn ok(command: String) -> Self {
        Self {
            result: StatementResult::Ok,
            command,
            conditionals: vec![],
        }
    }
}

impl Display for Statement {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}\nstatement {}\n{}\n",
            self.conditionals.iter().map(|c| c.to_string()).join("\n"),
            self.result,
            self.command
        )
    }
}

/// The type of a column in the result set of a [`Query`]
#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub enum Type {
    Text,
    Integer,
    UnsignedInteger,
    Real,
    Numeric,
    // note: `Date` currently behaves more like a `DateTime`/`Timestamp`
    Date,
    Time,
    TimestampTz,
    ByteArray,
    BitVec,
    Json,
}

impl Type {
    pub fn of_mysql_value(val: &mysql_async::Value) -> Option<Self> {
        use mysql_async::Value::*;
        match val {
            Bytes(_) => Some(Self::Text),
            Int(_) => Some(Self::Integer),
            UInt(_) => Some(Self::UnsignedInteger),
            Float(_) => Some(Self::Real),
            Double(_) => Some(Self::Real),
            Date(_, _, _, _, _, _, _) => Some(Self::Date),
            Time(_, _, _, _, _, _) => Some(Self::Time),
            NULL => None,
        }
    }

    pub fn of_mysql_value_with_json(val: &mysql_async::Value) -> Option<Self> {
        use mysql_async::Value::*;
        match val {
            Bytes(b) => {
                let s = String::from_utf8_lossy(b);
                if serde_json::from_str::<JsonValue>(&s).is_ok() {
                    Some(Self::Json)
                } else {
                    Some(Self::Text)
                }
            }
            _ => Self::of_mysql_value(val),
        }
    }
}

impl Display for Type {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Text => write!(f, "T"),
            Self::Integer => write!(f, "I"),
            Self::UnsignedInteger => write!(f, "UI"),
            Self::Real => write!(f, "R"),
            Self::Numeric => write!(f, "F"), // F, as in fixed-point number
            Self::Date => write!(f, "D"),
            Self::Time => write!(f, "M"),
            Self::ByteArray => write!(f, "B"),
            Self::BitVec => write!(f, "BV"),
            Self::TimestampTz => write!(f, "Z"),
            Self::Json => write!(f, "J"),
        }
    }
}

/// Result set sorting mode of a query
#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub enum SortMode {
    /// nosort - the default value. In nosort mode, the results appear in exactly the order in
    /// which they were received from the database engine. The nosort mode should only be used
    /// on queries that have an ORDER BY clause or which only have a single row of result,
    /// since otherwise the order of results is undefined and might vary from one database
    /// engine to another.
    NoSort,

    /// The "rowsort" mode gathers all output from the database engine then sorts it by rows on the
    /// client side. Sort comparisons use strcmp() on the rendered ASCII text representation of the
    /// values. Hence, "9" sorts after "10", not before.
    RowSort,

    /// The "valuesort" mode works like rowsort except that it does not honor row groupings. Each
    /// individual result value is sorted on its own.
    ValueSort,
}

impl Default for SortMode {
    /// Returns [`Self::NoSort`]
    fn default() -> Self {
        Self::NoSort
    }
}

impl Display for SortMode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SortMode::NoSort => f.write_str("nosort"),
            SortMode::RowSort => f.write_str("rowsort"),
            SortMode::ValueSort => f.write_str("valuesort"),
        }
    }
}

/// A SQL literal value, used for expected result values and values for parameters
///
/// TODO: Add an unsigned integer literal to support values between i64::max and u64::max
#[derive(Debug, Eq, PartialEq, Clone, TryInto, From)]
pub enum Value {
    Text(String),
    Integer(i64),
    UnsignedInteger(u64),
    Real(i64, u64),
    DateTime(NaiveDateTime),
    Time(MySqlTime),
    TimestampTz(DateTime<FixedOffset>),
    ByteArray(Vec<u8>),
    Numeric(Decimal),
    Null,
    BitVector(BitVec),
    Json(JsonValue),
}

#[derive(Error, Debug)]
#[error("Failed to convert mysql_async::Value: {0}")]
pub struct ValueConversionError(String);

impl TryFrom<mysql_async::Value> for Value {
    type Error = ValueConversionError;

    fn try_from(value: mysql_async::Value) -> Result<Self, Self::Error> {
        use mysql_async::Value::*;
        match value {
            NULL => Ok(Self::Null),
            // return UTF-8 string for binary data, else return a hex string
            Bytes(bs) => Ok(Self::Text(String::from_utf8(bs.clone()).or_else(|_| {
                Ok(format!(
                    "0x{}",
                    bs.iter().map(|b| format!("{b:02X}")).join("")
                ))
            })?)),
            Int(i) => Ok(Self::Integer(i)),
            UInt(i) => Ok(Self::UnsignedInteger(i)),
            Float(f) => Ok(Self::from(f)),
            Double(f) => Ok(Self::from(f)),
            Date(y, mo, d, h, min, s, us) => Ok(Self::DateTime(
                NaiveDate::from_ymd_opt(y.into(), mo.into(), d.into())
                    .unwrap()
                    .and_hms_micro_opt(h.into(), min.into(), s.into(), us)
                    .unwrap(),
            )),
            Time(neg, d, h, m, s, us) => Ok(Self::Time(MySqlTime::from_hmsus(
                !neg,
                (d * 24 + (h as u32))
                    .try_into()
                    .map_err(|e: TryFromIntError| ValueConversionError(e.to_string()))?,
                m,
                s,
                us.into(),
            ))),
        }
    }
}

impl TryFrom<Literal> for Value {
    type Error = ValueConversionError;
    fn try_from(value: Literal) -> Result<Self, Self::Error> {
        Ok(match value {
            Literal::Null => Value::Null,
            Literal::Boolean(b) => Value::Integer(i64::from(b)),
            Literal::Integer(v) => Value::Integer(v),
            Literal::UnsignedInteger(v) => Value::UnsignedInteger(v),
            Literal::Number(s) => {
                Value::Numeric(Decimal::from_str(&s).map_err(|e| {
                    ValueConversionError(format!("Invalid numeric value '{s}': {e}"))
                })?)
            }
            Literal::String(v) => Value::Text(v),
            Literal::Blob(v) => {
                Value::Text(String::from_utf8(v).map_err(|e| ValueConversionError(e.to_string()))?)
            }
            Literal::ByteArray(b) => Value::ByteArray(b),
            Literal::BitVector(b) => Value::BitVector(b),
            Literal::Placeholder(_) => {
                return Err(ValueConversionError(
                    "Placeholders are not valid values".to_string(),
                ))
            }
            Literal::Preserved(_) => {
                return Err(ValueConversionError(
                    "Preserved literal markers are not valid values".to_string(),
                ))
            }
        })
    }
}

impl From<Value> for mysql_async::Value {
    fn from(val: Value) -> Self {
        match val {
            Value::Text(x) => x.into(),
            Value::Integer(x) => x.into(),
            Value::UnsignedInteger(x) => x.into(),
            Value::Real(i, f) => (i as f64 + ((f as f64) / 1_000_000_000.0)).into(),
            Value::Numeric(d) => d.to_string().into(),
            Value::Null => mysql_async::Value::NULL,
            Value::DateTime(dt) => mysql_async::Value::from(dt),
            Value::Time(t) => mysql_async::Value::Time(
                !t.is_positive(),
                (t.hour() / 24).into(),
                (t.hour() % 24) as _,
                t.minutes(),
                t.seconds(),
                t.microseconds(),
            ),
            // Though `BitVec` is really PostgreSQL-specific, it's useful to compare bitstrings for
            // non-utf8 bytes.
            Value::BitVector(bv) => mysql_async::Value::Bytes(bv.to_bytes()),
            Value::ByteArray(bytes) => mysql_async::Value::Bytes(bytes),
            Value::Json(json) => mysql_async::Value::from(json.to_string()),
            Value::TimestampTz(_) => unimplemented!("PostgreSQL-specific"),
        }
    }
}

impl pgsql::types::ToSql for Value {
    fn to_sql(
        &self,
        ty: &pgsql::types::Type,
        out: &mut bytes::BytesMut,
    ) -> Result<pgsql::types::IsNull, Box<dyn Error + Sync + Send>> {
        use pgsql::types::Type;

        match self {
            Value::Text(x) => match *ty {
                Type::TEXT | Type::VARCHAR => x.to_sql(ty, out),
                ref ty if ty.name() == "citext" => x.to_sql(ty, out),
                _ => panic!("unexpected type {ty:?} for Text"),
            },
            Value::Integer(x) => match *ty {
                Type::BOOL => (*x != 0).to_sql(ty, out),
                Type::CHAR => (*x as i8).to_sql(ty, out),
                Type::INT2 => (*x as i16).to_sql(ty, out),
                Type::INT4 => (*x as i32).to_sql(ty, out),
                Type::INT8 => x.to_sql(ty, out),
                _ => panic!("unexpected type {ty:?} for Integer"),
            },
            Value::UnsignedInteger(x) => match *ty {
                Type::BOOL => (*x != 0).to_sql(ty, out),
                Type::CHAR => (*x as i8).to_sql(ty, out),
                Type::INT2 => (*x as i16).to_sql(ty, out),
                Type::INT4 => (*x as i32).to_sql(ty, out),
                Type::INT8 => (*x as i64).to_sql(ty, out),
                _ => panic!("unexpected type {ty:?} for UnsignedInteger"),
            },
            Value::Real(i, f) => {
                let val = *i as f64 + ((*f as f64) / 1_000_000_000.0);
                match *ty {
                    Type::FLOAT4 => (val as f32).to_sql(ty, out),
                    Type::FLOAT8 => val.to_sql(ty, out),
                    _ => panic!("unexpected type {ty:?} for Real"),
                }
            }
            Value::Numeric(d) => match *ty {
                Type::NUMERIC => d.to_sql(ty, out),
                // Parsed numeric literals and `From<f64>`/`From<f32>` produce `Numeric`, so
                // a `Value::Numeric` bound to a FLOAT4/FLOAT8 parameter is the common path.
                Type::FLOAT4 => f32::try_from(d)?.to_sql(ty, out),
                Type::FLOAT8 => f64::try_from(d)?.to_sql(ty, out),
                _ => panic!("unexpected type {ty:?} for Numeric"),
            },
            Value::DateTime(x) => match *ty {
                Type::TIMESTAMP => x.to_sql(ty, out),
                _ => panic!("unexpected type {ty:?} for Date"),
            },
            Value::Time(x) => match *ty {
                Type::TIME => NaiveTime::from(*x).to_sql(ty, out),
                _ => panic!("unexpected type {ty:?} for Time"),
            },
            Value::ByteArray(array) => match *ty {
                Type::BYTEA => array.to_sql(ty, out),
                _ => panic!("unexpected type {ty:?} for ByteArray"),
            },
            Value::Null => None::<i8>.to_sql(ty, out),
            Value::BitVector(b) => match *ty {
                Type::BIT | Type::VARBIT => b.to_sql(ty, out),
                _ => panic!("unexpected type {ty:?} for BitVector"),
            },
            Value::TimestampTz(ts) => match *ty {
                Type::TIMESTAMPTZ => ts.to_sql(ty, out),
                _ => panic!("unexpected type {ty:?} for TimestampTz"),
            },
            Value::Json(json) => match *ty {
                Type::JSON | Type::JSONB => json.to_sql(ty, out),
                _ => panic!("unexpected type {ty:?} for Json"),
            },
        }
    }

    fn accepts(ty: &pgsql::types::Type) -> bool {
        use pgsql::types::Type;

        match *ty {
            Type::BOOL
            | Type::CHAR
            | Type::INT2
            | Type::INT4
            | Type::INT8
            | Type::FLOAT4
            | Type::FLOAT8
            | Type::NUMERIC
            | Type::TEXT
            | Type::VARCHAR
            | Type::TIMESTAMP
            | Type::TIMESTAMPTZ
            | Type::TIME
            | Type::BYTEA
            | Type::BIT
            | Type::VARBIT
            | Type::JSON
            | Type::JSONB => true,
            ref ty if ty.name() == "citext" => true,
            _ => false,
        }
    }

    to_sql_checked!();
}

impl<'a> pgsql::types::FromSql<'a> for Value {
    fn from_sql(
        ty: &pgsql::types::Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn Error + Sync + Send>> {
        use pgsql::types::Type;

        // Macro to handle integer array conversion for different integer types.
        // Falls back to readyset_data::Array for multidimensional arrays, since
        // tokio-postgres's Vec<T>::from_sql rejects dimensions > 1.
        macro_rules! handle_int_array {
            ($int_type:ty) => {{
                match Vec::<Option<$int_type>>::from_sql(ty, raw) {
                    Ok(int_array) => {
                        let joined = int_array
                            .iter()
                            .map(|opt| match opt {
                                Some(v) => v.to_string(),
                                None => "NULL".to_string(),
                            })
                            .collect::<Vec<_>>()
                            .join(",");
                        Ok(Self::Text(format!("{{{}}}", joined)))
                    }
                    Err(e) => {
                        // tokio-postgres Vec<T>::from_sql rejects dimensions > 1;
                        // fall back to readyset_data::Array for multidimensional arrays.
                        tracing::debug!(%e, "Vec::from_sql failed, falling back to Array::from_sql");
                        let arr = readyset_data::Array::from_sql(ty, raw)?;
                        Ok(Self::Text(arr.to_string()))
                    }
                }
            }};
        }

        match *ty {
            Type::BOOL => Ok(Self::Integer(bool::from_sql(ty, raw)? as _)),
            Type::CHAR => Ok(Self::Integer(i8::from_sql(ty, raw)? as _)),
            Type::INT2 => Ok(Self::Integer(i16::from_sql(ty, raw)? as _)),
            Type::INT4 => Ok(Self::Integer(i32::from_sql(ty, raw)? as _)),
            Type::INT8 => Ok(Self::Integer(i64::from_sql(ty, raw)?)),
            Type::FLOAT4 => Ok(Self::from(f32::from_sql(ty, raw)?)),
            Type::FLOAT8 => Ok(Self::from(f64::from_sql(ty, raw)?)),
            Type::NUMERIC => Ok(Self::Numeric(Decimal::from_sql(ty, raw)?)),
            Type::TEXT | Type::VARCHAR => Ok(Self::Text(String::from_sql(ty, raw)?)),
            Type::TEXT_ARRAY | Type::VARCHAR_ARRAY => {
                // convert a text array into something like "{string1,string2,NULL,string3}"
                // Falls back to readyset_data::Array for multidimensional arrays.
                match Vec::<Option<String>>::from_sql(ty, raw) {
                    Ok(string_array) => {
                        let joined = string_array
                            .iter()
                            .map(|opt| opt.as_deref().unwrap_or("NULL"))
                            .collect::<Vec<_>>()
                            .join(",");
                        Ok(Self::Text(format!("{{{}}}", joined)))
                    }
                    Err(e) => {
                        // tokio-postgres Vec<T>::from_sql rejects dimensions > 1;
                        // fall back to readyset_data::Array for multidimensional arrays.
                        tracing::debug!(%e, "Vec::from_sql failed, falling back to Array::from_sql");
                        let arr = readyset_data::Array::from_sql(ty, raw)?;
                        Ok(Self::Text(arr.to_string()))
                    }
                }
            }
            Type::INT2_ARRAY => handle_int_array!(i16),
            Type::INT4_ARRAY => handle_int_array!(i32),
            Type::INT8_ARRAY => handle_int_array!(i64),
            Type::DATE => {
                // This is a hack to work around the fact that we don't have
                // a distinct 'Date' type, and that the existing 'Date' is
                // actually a 'DateTime' (a/k/a Timestamp)
                let val = match NaiveDateTime::from_sql(ty, raw) {
                    Ok(datetime) => datetime,
                    Err(_) => NaiveDate::from_sql(ty, raw)?.and_hms_opt(0, 0, 0).unwrap(),
                };
                Ok(Self::DateTime(val))
            }
            Type::TIME => Ok(Self::Time(NaiveTime::from_sql(ty, raw)?.into())),
            Type::TIMESTAMP => Ok(Self::TimestampTz(DateTime::<FixedOffset>::from_sql(
                ty, raw,
            )?)),
            Type::TIMESTAMPTZ => Ok(Self::TimestampTz(DateTime::<FixedOffset>::from_sql(
                ty, raw,
            )?)),
            Type::BIT | Type::VARBIT => Ok(Self::BitVector(BitVec::from_sql(ty, raw)?)),
            Type::JSON | Type::JSONB | Type::JSONB_ARRAY | Type::JSON_ARRAY => {
                Ok(Self::Json(Self::parse_json_value(str::from_utf8(raw)?)?))
            }
            ref ty if ty.name() == "geometry" => Ok(Self::Text(format!(
                "0x{}",
                raw.iter().map(|b| format!("{b:02X}")).join("")
            ))),
            _ => Err("Invalid type".into()),
        }
    }

    fn from_sql_null(_: &pgsql::types::Type) -> Result<Self, Box<dyn Error + Sync + Send>> {
        Ok(Value::Null)
    }

    fn accepts(ty: &pgsql::types::Type) -> bool {
        use pgsql::types::Type;

        match *ty {
            Type::BOOL
            | Type::CHAR
            | Type::INT2
            | Type::INT4
            | Type::INT8
            | Type::INT4_ARRAY
            | Type::INT8_ARRAY
            | Type::FLOAT4
            | Type::FLOAT8
            | Type::NUMERIC
            | Type::TEXT
            | Type::TEXT_ARRAY
            | Type::VARCHAR
            | Type::VARCHAR_ARRAY
            | Type::DATE
            | Type::TIMESTAMP
            | Type::TIMESTAMPTZ
            | Type::TIME
            | Type::TS_VECTOR
            | Type::BIT
            | Type::VARBIT
            | Type::JSON
            | Type::JSONB
            | Type::JSONB_ARRAY
            | Type::JSON_ARRAY => true,
            ref ty if ty.name() == "citext" => true,
            ref ty if ty.name() == "geometry" => true,
            _ => false,
        }
    }
}

impl TryFrom<DfValue> for Value {
    type Error = anyhow::Error;

    fn try_from(value: DfValue) -> Result<Self, Self::Error> {
        match value {
            DfValue::None | DfValue::Default | DfValue::Max => Ok(Value::Null),
            DfValue::Int(i) => Ok(Value::Integer(i)),
            DfValue::UnsignedInt(u) => Ok(Value::UnsignedInteger(u)),
            DfValue::Float(f) => Ok(f.into()),
            DfValue::Double(f) => Ok(f.into()),
            DfValue::Text(_) | DfValue::TinyText(_) => Ok(Value::Text(value.try_into()?)),
            DfValue::TimestampTz(ref ts) => Ok(Value::DateTime(ts.to_chrono().naive_utc())),
            DfValue::Time(t) => Ok(Value::Time(t)),
            DfValue::ByteArray(t) => Ok(Value::ByteArray(t.as_ref().clone())),
            DfValue::Numeric(ref d) => Ok(Value::Numeric(d.as_ref().clone())),
            DfValue::BitVector(ref b) => Ok(Value::BitVector(b.as_ref().clone())),
            DfValue::Array(_) => bail!("Arrays not supported"),
            DfValue::PassThrough(_) => unimplemented!(),
        }
    }
}

impl Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Text(s) => {
                if s.is_empty() {
                    write!(f, "(empty)")
                } else {
                    for chr in s.chars() {
                        let code = chr as u8;
                        if (0x20..0x7f).contains(&code) {
                            write!(f, "{chr}")?;
                        } else {
                            write!(f, "@")?;
                        }
                    }
                    Ok(())
                }
            }
            Self::Integer(i) => write!(f, "{i}"),
            Self::UnsignedInteger(i) => write!(f, "{i}"),
            Self::Real(whole, frac) => {
                write!(f, "{whole}.")?;
                let frac = frac.to_string();
                write!(f, "{}", &frac[..(cmp::min(frac.len(), 3))])
            }
            Self::Numeric(d) => {
                // TODO(fran): We will probably need to extend our NUMERIC
                //  implementation to correctly support the precision and scale,
                //  so we can display it correctly.
                write!(f, "{d}")
            }
            Self::DateTime(dt) => write!(f, "{}", dt.format(TIMESTAMP_FORMAT)),
            Self::Null => write!(f, "NULL"),
            Self::Time(t) => write!(f, "{t}"),
            Self::ByteArray(a) => {
                // TODO(fran): This is gonna be more complicated than this, probably.
                write!(f, "{a:?}")
            }
            Self::BitVector(b) => {
                write!(
                    f,
                    "{}",
                    b.iter().map(|bit| if bit { "1" } else { "0" }).join("")
                )
            }
            Self::TimestampTz(ts) => write!(f, "{ts}"),
            Self::Json(json) => write!(f, "{json}"),
        }
    }
}

impl From<i32> for Value {
    fn from(i: i32) -> Self {
        Self::Integer(i.into())
    }
}

/// Convert a float to a [`Decimal`] via its shortest round-tripping decimal text.
///
/// Recorded result literals are parsed with `Decimal::from_str`, so a float must reach the same
/// decimal as its textual form (`f64::to_string`). `Decimal::try_from(f64)` instead yields the
/// exact binary expansion (e.g. `-34.84` becomes `-3484...e-45`), which never compares equal to
/// the recorded `-34.84`. Non-finite floats map to the decimal sentinels.
///
/// `is_sign_positive` is only consulted for infinities; finite values carry their sign in `repr`.
fn numeric_from_float_repr(
    is_nan: bool,
    is_infinite: bool,
    is_sign_positive: bool,
    repr: String,
) -> Decimal {
    if is_nan {
        Decimal::NaN
    } else if is_infinite {
        if is_sign_positive {
            Decimal::Infinity
        } else {
            Decimal::NegativeInfinity
        }
    } else {
        Decimal::from_str(&repr).expect("finite float Display always parses as a Decimal")
    }
}

impl From<f32> for Value {
    fn from(f: f32) -> Self {
        // Use the f32's own shortest representation rather than widening to f64, which would
        // expand to the f32's exact binary value.
        Self::Numeric(numeric_from_float_repr(
            f.is_nan(),
            f.is_infinite(),
            f.is_sign_positive(),
            f.to_string(),
        ))
    }
}

impl From<f64> for Value {
    fn from(f: f64) -> Self {
        Self::Numeric(numeric_from_float_repr(
            f.is_nan(),
            f.is_infinite(),
            f.is_sign_positive(),
            f.to_string(),
        ))
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Value {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        // Sort by display string so the order matches what the type-erased generator path wrote
        // into `.test` files: recorded values lost their original wire type and came back through
        // `Display`. Comparing the same way at verify keeps sorted results aligned with the
        // recorded form even when the runner now decodes typed wire values (e.g. NEWDECIMAL into
        // `Numeric`).
        match (self, other) {
            // `Decimal` equality is scale-insensitive, so two Numerics that differ only in scale
            // must compare `Equal` to keep `Ord` consistent with `Eq`. Normalizing collapses the
            // scale (matching the hash path) while preserving display-string ordering for distinct
            // magnitudes, so sorted results still line up with the recorded form.
            (Self::Numeric(a), Self::Numeric(b)) => {
                a.normalize().to_string().cmp(&b.normalize().to_string())
            }
            _ => self.to_string().cmp(&other.to_string()),
        }
    }
}

impl Value {
    fn normalize_json_value(value: JsonValue) -> anyhow::Result<JsonValue> {
        Ok(match value {
            JsonValue::Number(n) => {
                if n.is_i64() || n.is_u64() {
                    JsonValue::Number(n)
                } else if let Some(f) = n.as_f64() {
                    JsonValue::Number(
                        serde_json::Number::from_f64(f)
                            .ok_or_else(|| anyhow!("Invalid float value"))?,
                    )
                } else {
                    JsonValue::Number(n)
                }
            }
            JsonValue::Array(vals) => JsonValue::Array(
                vals.into_iter()
                    .map(Self::normalize_json_value)
                    .collect::<Result<_, _>>()?,
            ),
            JsonValue::Object(map) => JsonValue::Object(
                map.into_iter()
                    .map(|(k, v)| Self::normalize_json_value(v).map(|v| (k, v)))
                    .collect::<Result<_, _>>()?,
            ),
            other => other,
        })
    }

    fn parse_json_value(s: &str) -> anyhow::Result<JsonValue> {
        Self::normalize_json_value(serde_json::from_str(s).map_err(|e| anyhow!("{e}"))?)
    }

    pub fn typ(&self) -> Option<Type> {
        match self {
            Self::Text(_) => Some(Type::Text),
            Self::Integer(_) => Some(Type::Integer),
            Self::UnsignedInteger(_) => Some(Type::UnsignedInteger),
            Self::Real(_, _) => Some(Type::Real),
            Self::Numeric(_) => Some(Type::Numeric),
            Self::DateTime(_) => Some(Type::Date),
            Self::Time(_) => Some(Type::Time),
            Self::ByteArray(_) => Some(Type::ByteArray),
            Self::Null => None,
            Self::BitVector(_) => Some(Type::BitVec),
            Self::TimestampTz(_) => Some(Type::TimestampTz),
            Self::Json(_) => Some(Type::Json),
        }
    }

    pub fn from_mysql_value_with_type(
        val: mysql_async::Value,
        typ: &Type,
    ) -> anyhow::Result<Value> {
        if val == mysql_async::Value::NULL {
            return Ok(Self::Null);
        }
        match typ {
            Type::Text => Ok(Self::Text(mysql_async::from_value_opt(val)?)),
            Type::Integer => Ok(Self::Integer(
                mysql_async::from_value_opt(val.clone()).or_else(|_| -> anyhow::Result<i64> {
                    Ok(mysql_async::from_value_opt::<f64>(val)?.trunc() as i64)
                })?,
            )),
            Type::UnsignedInteger => Ok(Self::UnsignedInteger(
                mysql_async::from_value_opt(val.clone()).or_else(|_| -> anyhow::Result<u64> {
                    Ok(mysql_async::from_value_opt::<f64>(val)?.trunc() as u64)
                })?,
            )),
            Type::Real => {
                let f: f64 = mysql_async::from_value_opt(val)?;
                Ok(Self::from(f))
            }
            Type::Numeric => Ok(Self::Numeric(match val {
                mysql_async::Value::Bytes(ref b) => Decimal::from_str(std::str::from_utf8(b)?)?,
                mysql_async::Value::Int(i) => Decimal::from(i),
                mysql_async::Value::UInt(u) => Decimal::from(u),
                mysql_async::Value::Float(f) => numeric_from_float_repr(
                    f.is_nan(),
                    f.is_infinite(),
                    f.is_sign_positive(),
                    f.to_string(),
                ),
                mysql_async::Value::Double(f) => numeric_from_float_repr(
                    f.is_nan(),
                    f.is_infinite(),
                    f.is_sign_positive(),
                    f.to_string(),
                ),
                _ => bail!("Could not convert {:?} to Numeric", val),
            })),
            Type::Date => Ok(Self::DateTime(mysql_async::from_value_opt(val)?)),
            Type::Time => Ok(Self::Time(match val {
                mysql_async::Value::Bytes(s) => {
                    MySqlTime::from_str(std::str::from_utf8(&s)?).map_err(|e| anyhow!("{}", e))?
                }
                mysql_async::Value::Time(neg, d, h, m, s, us) => {
                    MySqlTime::from_hmsus(!neg, ((d * 24) + h as u32).try_into()?, m, s, us.into())
                }
                _ => bail!("Could not convert {:?} to Time", val),
            })),
            // Though `BitVec` is really PostgreSQL-specific, it's useful to compare bitstrings for
            // non-utf8 bytes.
            Type::BitVec => Ok(Self::BitVector(match val {
                mysql_async::Value::Bytes(b) => BitVec::from_bytes(&b),
                _ => unimplemented!(),
            })),
            Type::Json => Ok(Self::try_from(val)?.convert_type(typ)?.into_owned()),
            // These types are PostgreSQL specific.
            Type::ByteArray => unimplemented!(),
            Type::TimestampTz => unimplemented!(),
        }
    }

    /// Decode a single `mysql_async::Value` using its column's declared type.
    ///
    /// MySQL's wire layer returns NEWDECIMAL columns as opaque bytes; the type-erased
    /// `TryFrom<mysql_async::Value>` path would decode those as `Value::Text` and break
    /// numeric comparison against Postgres/Readyset results. Dispatching on column type
    /// preserves the declared semantics.
    pub fn from_mysql_value_with_column(
        val: mysql_async::Value,
        column: &mysql_async::Column,
    ) -> anyhow::Result<Value> {
        use mysql_async::consts::ColumnType::*;

        match column.column_type() {
            MYSQL_TYPE_NEWDECIMAL | MYSQL_TYPE_DECIMAL => {
                Value::from_mysql_value_with_type(val, &Type::Numeric)
            }
            MYSQL_TYPE_FLOAT | MYSQL_TYPE_DOUBLE => {
                Value::from_mysql_value_with_type(val, &Type::Real)
            }
            _ => Ok(Value::try_from(val)?),
        }
    }

    pub fn convert_type<'a>(&'a self, typ: &Type) -> anyhow::Result<Cow<'a, Self>> {
        match (self, typ) {
            (Self::Text(_), Type::Text)
            | (Self::Integer(_), Type::Integer)
            | (Self::UnsignedInteger(_), Type::UnsignedInteger)
            | (Self::Real(_, _), Type::Real)
            | (Self::Numeric(_), Type::Numeric)
            | (Self::DateTime(_), Type::Date)
            | (Self::Time(_), Type::Time)
            | (Self::TimestampTz(_), Type::TimestampTz)
            | (Self::BitVector(_), Type::BitVec)
            | (Self::Json(_), Type::Json)
            | (Self::Null, _) => Ok(Cow::Borrowed(self)),
            (Self::Integer(i), Type::UnsignedInteger) => {
                Ok(Cow::Owned(Self::UnsignedInteger((*i).try_into()?)))
            }
            (Self::UnsignedInteger(u), Type::Integer) => {
                Ok(Cow::Owned(Self::Integer((*u).try_into()?)))
            }
            (Self::TimestampTz(ts), Type::Date) => Ok(Cow::Owned(Self::DateTime(ts.naive_local()))),
            (Self::Text(txt), Type::Integer) => Ok(Cow::Owned(Self::Integer(txt.parse()?))),
            (Self::Text(txt), Type::UnsignedInteger) => {
                Ok(Cow::Owned(Self::UnsignedInteger(txt.parse()?)))
            }
            (Self::Text(txt), Type::Real) => Ok(Cow::Owned(Self::from(txt.parse::<f64>()?))),
            (Self::Text(txt), Type::Date) => Ok(Cow::Owned(Self::DateTime(
                NaiveDateTime::parse_from_str(txt, "%Y-%m-%d %H:%M:%S").or_else(|_| {
                    NaiveDate::parse_from_str(txt, "%Y-%m-%d")
                        .map(|nd| nd.and_hms_opt(0, 0, 0).unwrap())
                })?,
            ))),
            (Self::Text(txt), Type::TimestampTz) => Ok(Cow::Owned(Self::TimestampTz(
                FixedOffset::east_opt(0).unwrap().from_utc_datetime(
                    &NaiveDateTime::parse_from_str(txt, "%Y-%m-%d %H:%M:%S%.f")?,
                ),
            ))),
            (Self::Text(txt), Type::Time) => Ok(Cow::Owned(Self::Time(txt.parse()?))),
            (Self::Text(txt), Type::BitVec) => Ok(Cow::Owned(Self::BitVector(BitVec::from_bytes(
                txt.as_bytes(),
            )))),
            (Self::Text(txt), Type::Json) => {
                Ok(Cow::Owned(Self::Json(Self::parse_json_value(txt)?)))
            }
            (Self::Numeric(dec), Type::Integer) => Ok(Cow::Owned(Self::Integer(
                dec.try_into()
                    .map_err(|e| anyhow!("Numeric to Integer: {e}"))?,
            ))),
            (Self::Integer(i), Type::Numeric) => Ok(Cow::Owned(Self::Numeric(Decimal::from(*i)))),
            (Self::Integer(i), Type::Real) => Ok(Cow::Owned(Self::Real(*i, 0))),
            (Self::Numeric(dec), Type::Real) => {
                let whole: i64 = dec
                    .try_into()
                    .map_err(|e| anyhow!("Numeric to Real whole: {e}"))?;
                let frac_signed: i64 = (dec.fract() * Decimal::from(1_000_000_000))
                    .try_into()
                    .map_err(|e| anyhow!("Numeric to Real frac: {e}"))?;
                Ok(Cow::Owned(Self::Real(whole, frac_signed.unsigned_abs())))
            }
            (Self::Integer(i), Type::Json) => Ok(Cow::Owned(Self::Json(JsonValue::from(*i)))),
            (Self::UnsignedInteger(u), Type::Json) => {
                Ok(Cow::Owned(Self::Json(JsonValue::from(*u))))
            }
            (Self::Real(whole, frac), Type::Json) => {
                let sign = if *whole < 0 { -1.0 } else { 1.0 };
                let magnitude = whole.abs() as f64 + (*frac as f64) / 1_000_000_000.0;
                let num = serde_json::Number::from_f64(sign * magnitude)
                    .ok_or_else(|| anyhow!("Invalid float value"))?;
                Ok(Cow::Owned(Self::Json(JsonValue::Number(num))))
            }
            (Self::Numeric(dec), Type::Json) => Ok(Cow::Owned(Self::Json(Self::parse_json_value(
                &dec.to_string(),
            )?))),
            (Self::DateTime(ndt), Type::Json) => {
                Ok(Cow::Owned(Self::Json(JsonValue::String(ndt.to_string()))))
            }
            (Self::Time(time), Type::Json) => {
                Ok(Cow::Owned(Self::Json(JsonValue::String(time.to_string()))))
            }
            (Self::TimestampTz(ts), Type::Json) => {
                Ok(Cow::Owned(Self::Json(JsonValue::String(ts.to_string()))))
            }
            (Self::Integer(i), Type::Text) => Ok(Cow::Owned(Self::Text(i.to_string()))),
            (Self::DateTime(ndt), Type::Text) => Ok(Cow::Owned(Self::Text(ndt.to_string()))),
            (Self::DateTime(ndt), Type::TimestampTz) => Ok(Cow::Owned(Self::TimestampTz(
                FixedOffset::east_opt(0).unwrap().from_utc_datetime(ndt),
            ))),
            (Self::Numeric(dec), Type::Text) => Ok(Cow::Owned(Self::Text(dec.to_string()))),
            (Self::Real(whole, frac), Type::Text) => {
                let frac_str = frac.to_string();
                let padded_frac = if frac_str.len() < 9 {
                    format!("{:0>9}", frac_str)
                } else {
                    frac_str
                };
                let trimmed = padded_frac.trim_end_matches('0');
                if trimmed.is_empty() {
                    Ok(Cow::Owned(Self::Text(format!("{}.", whole))))
                } else {
                    Ok(Cow::Owned(Self::Text(format!("{}.{trimmed}", whole))))
                }
            }
            (Self::Json(json), Type::Text) => Ok(Cow::Owned(Self::Text(json.to_string()))),
            (Self::TimestampTz(ts), Type::Text) => {
                Ok(Cow::Owned(Self::Text(ts.naive_local().to_string())))
            }
            (v, t) => {
                bail!("Conversion from {v:?} to {t:?} is not supported")
            }
        }
    }

    pub fn hash_results(results: &[Self]) -> md5::Digest {
        let mut context = md5::Context::new();
        for result in results {
            // Normalize Numeric so `1.0` and `1.00` hash the same, matching
            // `Decimal`'s scale-insensitive `PartialEq`.
            match result {
                Self::Numeric(d) => context.consume(d.normalize().to_string()),
                other => context.consume(other.to_string()),
            }
            context.consume("\n");
        }
        context.finalize()
    }

    pub fn compare_type_insensitive(&self, other: &Self) -> bool {
        match other.typ() {
            None => *self == Value::Null,
            // PostgreSQL-specific variants have no `mysql_async::Value` round-trip
            // (the conversions in either direction panic). Direct equality is what
            // the round-trip is approximating anyway, so short-circuit here.
            Some(Type::TimestampTz | Type::ByteArray) => self == other,
            Some(typ) => Self::from_mysql_value_with_type(mysql_async::Value::from(self), &typ)
                .is_ok_and(|v| v == *other),
        }
    }
}

/// The expected results of a query. Past a [`HashThreshold`][Record::HashThreshold], an [`md5`] sum
/// of the results will be computed and compared.
#[derive(Debug, Eq, PartialEq, Clone, TryInto, From)]
pub enum QueryResults {
    Hash { count: usize, digest: md5::Digest },
    Results(Vec<Value>),
}

impl QueryResults {
    pub fn hash(vals: &[Value]) -> Self {
        Self::Hash {
            count: vals.len(),
            digest: Value::hash_results(vals),
        }
    }
}

impl Display for QueryResults {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            QueryResults::Hash { count, digest } => {
                write!(f, "{count} values hashing to {digest:x}")
            }
            QueryResults::Results(results) => write!(f, "{}", results.iter().join("\n")),
        }
    }
}

impl Default for QueryResults {
    fn default() -> Self {
        QueryResults::Results(vec![])
    }
}

/// The parameters passed to a prepared query, either positional or named
#[derive(Debug, Eq, PartialEq, Clone, TryInto, From)]
pub enum QueryParams {
    PositionalParams(Vec<Value>),
    NumberedParams(HashMap<u32, Value>),
}

impl QueryParams {
    pub fn is_empty(&self) -> bool {
        match self {
            Self::PositionalParams(p) => p.is_empty(),
            Self::NumberedParams(p) => p.is_empty(),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Self::PositionalParams(p) => p.len(),
            Self::NumberedParams(p) => p.len(),
        }
    }
}

impl Default for QueryParams {
    fn default() -> Self {
        QueryParams::PositionalParams(vec![])
    }
}

impl Display for QueryParams {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            QueryParams::PositionalParams(ps) => {
                write!(f, "{}", ps.iter().map(|p| format!("? = {p}")).join("\n"))?;
            }
            QueryParams::NumberedParams(ps) => {
                write!(
                    f,
                    "{}",
                    ps.iter().map(|(n, p)| format!("${n} = {p}")).join("\n")
                )?;
            }
        }

        if !self.is_empty() {
            writeln!(f)?;
        }

        Ok(())
    }
}

pub enum QueryParamsIntoIter {
    Empty,
    Positional(vec::IntoIter<Value>),
    Numbered(RangeInclusive<u32>, HashMap<u32, Value>),
}

impl Iterator for QueryParamsIntoIter {
    type Item = Value;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Empty => None,
            Self::Positional(vs) => vs.next(),
            Self::Numbered(is, vals) => is.next().map(|i| vals.remove(&i).unwrap()),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = match self {
            Self::Empty => 0,
            Self::Positional(vs) => vs.len(),
            Self::Numbered(_, vs) => vs.len(),
        };
        (len, Some(len))
    }
}

impl ExactSizeIterator for QueryParamsIntoIter {}

impl IntoIterator for QueryParams {
    type Item = Value;

    type IntoIter = QueryParamsIntoIter;

    fn into_iter(self) -> Self::IntoIter {
        use QueryParamsIntoIter::*;

        match self {
            QueryParams::NumberedParams(np) if np.is_empty() => Empty,
            QueryParams::PositionalParams(ps) => Positional(ps.into_iter()),
            QueryParams::NumberedParams(np) => {
                let min_val = np.keys().min().unwrap();
                let max_val = np.keys().max().unwrap();
                Numbered(*min_val..=*max_val, np)
            }
        }
    }
}

impl From<QueryParams> for mysql_async::Params {
    fn from(qp: QueryParams) -> Self {
        match qp {
            qp if qp.is_empty() => mysql_async::Params::Empty,
            QueryParams::PositionalParams(vs) => mysql_async::Params::Positional(
                vs.into_iter().map(mysql_async::Value::from).collect(),
            ),
            QueryParams::NumberedParams(nps) => mysql_async::Params::Named(
                nps.into_iter()
                    .map(|(n, v)| (n.to_string().into_bytes(), mysql_async::Value::from(v)))
                    .collect(),
            ),
        }
    }
}

/// Run a query against the database engine and check the results against an expected result set
#[derive(Debug, Eq, PartialEq, Clone, Default)]
pub struct Query {
    pub label: Option<String>,
    pub column_types: Option<Vec<Type>>,
    pub sort_mode: Option<SortMode>,
    pub conditionals: Vec<Conditional>,
    pub query: String,
    pub results: QueryResults,
    pub params: QueryParams,
    /// When set, the test expects the query to fail (wrong results, prepare/execute error, or
    /// proxied to upstream). The optional pattern is matched as a regex against the error message.
    /// The test passes if the query fails (optionally matching the pattern) and fails if it
    /// succeeds (indicating the bug is fixed — remove the `error` tag).
    pub expected_error: Option<String>,
}

impl Display for Query {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let error_tag = match &self.expected_error {
            Some(pattern) if !pattern.is_empty() => format!(" error: {pattern}"),
            Some(_) => " error".to_string(),
            None => String::new(),
        };
        write!(
            f,
            "{}\nquery {} {}{}\n{}\n{}----\n{}",
            self.conditionals.iter().join("\n"),
            self.column_types
                .as_ref()
                .map_or("".to_owned(), |cts| cts.iter().join("")),
            self.sort_mode.map_or("".to_owned(), |sm| sm.to_string()),
            error_tag,
            self.query,
            self.params,
            self.results,
        )
    }
}

/// Top level expression in a sqllogictest test script
#[derive(Debug, Eq, PartialEq, Clone)]
pub enum Record {
    Statement(Statement),
    Query(Query),

    /// The "hash-threshold" record sets a limit on the number of values that can appear in a
    /// result set. If the number of values exceeds this, then instead of recording each
    /// individual value in the full test script, an MD5 hash of all values is computed in
    /// stored. This makes the full test scripts much shorter, but at the cost of obscuring the
    /// results. If the hash-threshold is 0, then results are never hashed. A hash-threshold of
    /// 10 or 20 is recommended. During debugging, it is advantage to set the hash-threshold to
    /// zero so that all results can be seen.
    HashThreshold(usize),

    /// Stop testing and halt immediately. Useful when debugging.
    Halt {
        conditionals: Vec<Conditional>,
    },

    /// Sleep for the given number of milliseconds
    Sleep(u64),

    /// Print a graphviz representation of the current query graph.
    Graphviz,
}

impl Display for Record {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Record::Statement(s) => write!(f, "{s}"),
            Record::Query(q) => write!(f, "{q}"),
            Record::HashThreshold(ht) => writeln!(f, "hash-threshold {ht}"),
            Record::Halt { conditionals } => {
                writeln!(f, "{}\nhalt\n", conditionals.iter().join("\n"))
            }
            Record::Graphviz => f.write_str("graphviz\n"),
            Record::Sleep(msecs) => writeln!(f, "sleep {msecs}"),
        }
    }
}

impl Record {
    /// Constructs a Record::Query with the given query string, optional parsed SqlQuery, list of
    /// parameters, and list of result rows
    pub fn query(
        query: String,
        parsed: Option<&SqlQuery>,
        params: Vec<Value>,
        mut results: Vec<Vec<Value>>,
    ) -> Self {
        Self::Query(Query {
            label: None,
            column_types: None,
            sort_mode: Some(match parsed {
                Some(SqlQuery::Select(select)) if select.order.is_some() => SortMode::NoSort,
                _ => {
                    results.sort();
                    SortMode::RowSort
                }
            }),
            conditionals: vec![],
            query,
            results: QueryResults::hash(&results.into_iter().flatten().collect::<Vec<_>>()),
            params: QueryParams::PositionalParams(params),
            expected_error: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_result_value() {
        assert_eq!(Value::Text("\0".to_string()).to_string(), "@");
    }

    #[test]
    fn compare_result_value() {
        assert!(Value::Integer(9) > Value::Integer(10));
    }

    #[test]
    fn convert_whole_integer_to_numeric() {
        // A `.test` records a whole-number result (e.g. `2`) under an `F` (Numeric)
        // column as a bare integer with no decimal point, so the parser yields
        // `Integer`. Converting it to the column's `Numeric` type must succeed
        // rather than bail (which makes the parser panic).
        let v = Value::Integer(2);
        let converted = v.convert_type(&Type::Numeric).unwrap();
        assert_eq!(*converted, Value::Numeric(Decimal::from(2)));
    }

    // REA-6023: `compare_type_insensitive` previously panicked on PostgreSQL-only
    // variants (`TimestampTz`, `ByteArray`) because their `mysql_async::Value`
    // round-trip is `unimplemented!()`. Direct equality is the right semantics.
    #[test]
    fn compare_type_insensitive_timestamptz() {
        let ts = DateTime::parse_from_rfc3339("2020-01-19T17:00:00+00:00").unwrap();
        let a = Value::TimestampTz(ts);
        let b = Value::TimestampTz(ts);
        assert!(a.compare_type_insensitive(&b));

        let other = DateTime::parse_from_rfc3339("2020-01-19T18:00:00+00:00").unwrap();
        assert!(!a.compare_type_insensitive(&Value::TimestampTz(other)));
    }

    #[test]
    fn compare_type_insensitive_bytearray() {
        let a = Value::ByteArray(vec![0xde, 0xad, 0xbe, 0xef]);
        let b = Value::ByteArray(vec![0xde, 0xad, 0xbe, 0xef]);
        assert!(a.compare_type_insensitive(&b));

        let c = Value::ByteArray(vec![0xca, 0xfe]);
        assert!(!a.compare_type_insensitive(&c));
    }

    #[test]
    fn json_values_compare_equally_ignoring_formatting() {
        let expected = Value::Text(r#"{"id": 1, "price": 10.5, "category": "books"}"#.to_string())
            .convert_type(&Type::Json)
            .unwrap()
            .into_owned();
        let actual = Value::Text(r#"{"category":"books","id":1,"price":10.50}"#.to_string())
            .convert_type(&Type::Json)
            .unwrap()
            .into_owned();

        assert_eq!(expected, actual);
        assert!(actual.compare_type_insensitive(&expected));
    }

    #[test]
    fn numeric_arm_decodes_decimal_bytes() {
        for s in [
            "-691179223.7500",
            "0.0000",
            "-0.0001",
            "-0.7500",
            "0.00",
            "0",
        ] {
            let bytes = mysql_async::Value::Bytes(s.as_bytes().to_vec());
            let v = Value::from_mysql_value_with_type(bytes, &Type::Numeric).unwrap();
            let expected = Value::Numeric(Decimal::from_str(s).unwrap());
            assert_eq!(v, expected, "decoding {s:?}");
        }
    }

    #[test]
    fn numeric_arm_decodes_int_uint_and_float() {
        let cases = [
            (mysql_async::Value::Int(5), Decimal::from(5i64)),
            (mysql_async::Value::Int(-7), Decimal::from(-7i64)),
            (mysql_async::Value::UInt(42), Decimal::from(42u64)),
            (
                mysql_async::Value::Double(1.5),
                Decimal::from_str("1.5").unwrap(),
            ),
        ];
        for (val, expected) in cases {
            let v = Value::from_mysql_value_with_type(val.clone(), &Type::Numeric)
                .unwrap_or_else(|e| panic!("decoding {val:?}: {e}"));
            assert_eq!(v, Value::Numeric(expected), "decoding {val:?}");
        }
    }

    #[test]
    fn hash_results_ignores_numeric_scale() {
        // `Decimal::PartialEq` is scale-insensitive (`"1.0" == "1.00"`), so the
        // result hash must be too; otherwise a wire scale that differs from a
        // recorded scale produces a spurious hash-mode failure.
        let lhs = vec![Value::Numeric(Decimal::from_str("1.0").unwrap())];
        let rhs = vec![Value::Numeric(Decimal::from_str("1.00").unwrap())];
        assert_eq!(Value::hash_results(&lhs), Value::hash_results(&rhs));
    }

    #[test]
    fn cmp_numerics_is_consistent_with_equality() {
        // `Decimal` equality is scale-insensitive (`"1.0" == "1.00"`), so `Value::cmp` must report
        // equal-magnitude Numerics as `Equal` to honor the `Ord`/`Eq` contract. Otherwise the
        // rowsort/valuesort verify path (which sorts actual results by `cmp` then compares them
        // positionally against recorded results with a scale-insensitive `==`) can pair values up
        // by the wrong neighbor and spuriously fail once a wire scale differs from the recorded
        // scale.
        let a = Value::Numeric(Decimal::from_str("1.0").unwrap());
        let b = Value::Numeric(Decimal::from_str("1.00").unwrap());
        assert_eq!(a, b);
        assert_eq!(a.cmp(&b), cmp::Ordering::Equal);

        // Distinct magnitudes still order by normalized display string, so the order stays aligned
        // with what was recorded into `.test` files (larger integer part first, as the type-erased
        // generator wrote it).
        let ten = Value::Numeric(Decimal::from_str("10.0").unwrap());
        let two = Value::Numeric(Decimal::from_str("2.0").unwrap());
        assert_eq!(ten.cmp(&two), cmp::Ordering::Less);
    }

    fn mock_column(name: &str, ty: mysql_async::consts::ColumnType) -> mysql_async::Column {
        use mysql_async::consts::ColumnFlags;
        mysql_async::Column::new(ty)
            .with_name(name.as_bytes())
            .with_flags(ColumnFlags::empty())
    }

    #[test]
    fn column_dispatch_routes_newdecimal_to_numeric() {
        use mysql_async::consts::ColumnType::MYSQL_TYPE_NEWDECIMAL;
        let col = mock_column("avg_col", MYSQL_TYPE_NEWDECIMAL);
        let v = Value::from_mysql_value_with_column(
            mysql_async::Value::Bytes(b"-691179223.7500".to_vec()),
            &col,
        )
        .unwrap();
        assert_eq!(
            v,
            Value::Numeric(readyset_decimal::Decimal::from_str("-691179223.7500").unwrap()),
        );
    }

    #[test]
    fn column_dispatch_routes_varchar_to_text() {
        use mysql_async::consts::ColumnType::MYSQL_TYPE_VAR_STRING;
        let col = mock_column("name", MYSQL_TYPE_VAR_STRING);
        let v =
            Value::from_mysql_value_with_column(mysql_async::Value::Bytes(b"hello".to_vec()), &col)
                .unwrap();
        assert_eq!(v, Value::Text("hello".to_string()));
    }

    #[test]
    fn column_dispatch_routes_null_uniformly() {
        use mysql_async::consts::ColumnType::MYSQL_TYPE_NEWDECIMAL;
        let col = mock_column("x", MYSQL_TYPE_NEWDECIMAL);
        assert_eq!(
            Value::from_mysql_value_with_column(mysql_async::Value::NULL, &col).unwrap(),
            Value::Null,
        );
    }

    #[test]
    fn postgres_float4_routes_to_numeric() {
        use pgsql::types::{FromSql, Type};
        let bytes = (-12345.6f32).to_be_bytes();
        let v = Value::from_sql(&Type::FLOAT4, &bytes).unwrap();
        let expected = Value::Numeric(Decimal::from_str("-12345.6").unwrap());
        assert_eq!(v, expected);
    }

    #[test]
    fn postgres_float8_routes_to_numeric() {
        use pgsql::types::{FromSql, Type};
        let bytes = (-12345.6f64).to_be_bytes();
        let v = Value::from_sql(&Type::FLOAT8, &bytes).unwrap();
        let expected = Value::Numeric(Decimal::from_str("-12345.6").unwrap());
        assert_eq!(v, expected);
    }

    #[test]
    fn from_f64_negative_produces_numeric() {
        let v: Value = (-123.4500_f64).into();
        let expected = Value::Numeric(Decimal::from_str("-123.45").unwrap());
        assert_eq!(v, expected);
    }

    #[test]
    fn from_f64_nonfinite_routes_to_numeric() {
        assert_eq!(Value::from(f64::NAN), Value::Numeric(Decimal::NaN));
        assert_eq!(
            Value::from(f64::INFINITY),
            Value::Numeric(Decimal::Infinity)
        );
        assert_eq!(
            Value::from(f64::NEG_INFINITY),
            Value::Numeric(Decimal::NegativeInfinity),
        );
    }

    #[test]
    fn from_f32_negative_produces_numeric() {
        let v: Value = (-12.5_f32).into();
        let expected = Value::Numeric(Decimal::from_str("-12.5").unwrap());
        assert_eq!(v, expected);
    }

    #[test]
    fn dfvalue_double_routes_to_numeric() {
        let v = Value::try_from(readyset_data::DfValue::Double(-123.45)).unwrap();
        let expected = Value::Numeric(Decimal::from_str("-123.45").unwrap());
        assert_eq!(v, expected);
    }

    #[test]
    fn dfvalue_float_routes_to_numeric() {
        let v = Value::try_from(readyset_data::DfValue::Float(-12.5)).unwrap();
        let expected = Value::Numeric(Decimal::from_str("-12.5").unwrap());
        assert_eq!(v, expected);
    }

    #[test]
    fn decimal_equality_ignores_scale() {
        let a = Decimal::from_str("1.0").unwrap();
        let b = Decimal::from_str("1.00").unwrap();
        assert_eq!(a, b, "Decimal::eq must be scale-insensitive");
    }

    #[test]
    fn convert_integer_to_numeric() {
        let v = Value::Integer(2);
        let result = v.convert_type(&Type::Numeric).unwrap().into_owned();
        assert_eq!(result, Value::Numeric(Decimal::from(2i64)));
    }

    #[test]
    fn convert_numeric_to_real_rounds_through_f64() {
        // Recorded `query R` results were written at f64 precision; an upstream
        // Numeric with wider scale (e.g. EXTRACT(JULIAN ...) returning a repeating
        // fraction at scale 100) must compare equal to the recorded scale-20 value
        // once both sides land in `Type::Real`.
        let recorded = Value::Numeric(Decimal::from_str("2451895.51473379629629630").unwrap());
        let upstream = Value::Numeric(
            Decimal::from_str(
                "2451895.5147337962962962962962962962962962962962962962962962962962962962962962962962962962962962962962962963",
            )
            .unwrap(),
        );
        let normalized_recorded = recorded.convert_type(&Type::Real).unwrap().into_owned();
        let normalized_upstream = upstream.convert_type(&Type::Real).unwrap().into_owned();
        assert_eq!(normalized_recorded, normalized_upstream);
    }

    #[test]
    fn parsed_decimal_equals_wire_decimal() {
        let input = b"query F nosort\nSELECT avg(price) FROM t\n----\n-691179223.7500\n";
        let records = nom::combinator::complete(crate::parser::records)(input)
            .unwrap()
            .1;
        let Record::Query(q) = records.into_iter().next().unwrap() else {
            panic!("expected Query record");
        };
        let QueryResults::Results(vs) = q.results else {
            panic!("expected Results");
        };
        let parsed = vs.into_iter().next().unwrap();
        let wired = Value::from_mysql_value_with_type(
            mysql_async::Value::Bytes(b"-691179223.7500".to_vec()),
            &Type::Numeric,
        )
        .unwrap();
        assert_eq!(parsed, wired);
    }

    #[test]
    fn numeric_to_sql_handles_float4_and_float8() {
        use pgsql::types::{ToSql, Type as PgType};
        let v = Value::Numeric(Decimal::try_from(-12.5_f64).unwrap());

        // A `Numeric` bound to a float parameter must encode the same wire bytes as the
        // native float would, not just avoid panicking.
        let mut actual = bytes::BytesMut::new();
        let mut expected = bytes::BytesMut::new();

        v.to_sql(&PgType::FLOAT4, &mut actual).unwrap();
        (-12.5_f32).to_sql(&PgType::FLOAT4, &mut expected).unwrap();
        assert_eq!(actual, expected, "FLOAT4 encoding");

        actual.clear();
        expected.clear();

        v.to_sql(&PgType::FLOAT8, &mut actual).unwrap();
        (-12.5_f64).to_sql(&PgType::FLOAT8, &mut expected).unwrap();
        assert_eq!(actual, expected, "FLOAT8 encoding");
    }
}
