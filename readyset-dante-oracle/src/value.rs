//! Lightweight value type for comparing database query results.
//!
//! This is a focused subset of `readyset-logictest`'s `ast::Value`, containing
//! only the variants and trait impls needed by the constraint-fuzz driver.

use std::collections::HashMap;
use std::convert::TryFrom;
use std::error::Error;
use std::fmt::{self, Display};
use std::num::TryFromIntError;
use std::{cmp, vec};

use anyhow::bail;
use bit_vec::BitVec;
use chrono::{DateTime, FixedOffset, NaiveDate, NaiveTime};
use itertools::Itertools;
use mysql_common::chrono::NaiveDateTime;
use mysql_time::MySqlTime;
use readyset_data::{DfValue, TIMESTAMP_FORMAT};
use readyset_decimal::Decimal;
use serde_json::Value as JsonValue;
use thiserror::Error as ThisError;
use tokio_postgres as pgsql;

/// A SQL literal value used for comparing query results between upstream and
/// Readyset.
#[derive(Debug, Eq, PartialEq, Clone)]
pub enum Value {
    Text(String),
    Integer(i64),
    UnsignedInteger(u64),
    /// IEEE 754 binary64 stored as raw bits. NaN values are canonicalized to
    /// [`CANONICAL_NAN_BITS`] so all NaNs compare equal under derived
    /// `PartialEq`. `Ord` uses [`f64::total_cmp`] so it agrees with bit
    /// equality across `±0`, `±inf`, and the canonical NaN.
    Real(u64),
    DateTime(NaiveDateTime),
    Time(MySqlTime),
    TimestampTz(DateTime<FixedOffset>),
    ByteArray(Vec<u8>),
    Numeric(Decimal),
    Null,
    BitVector(BitVec),
    Json(JsonValue),
}

/// Quiet, positive, payload-zero NaN — the canonical NaN bit pattern produced
/// by `f64::NAN` on every supported platform.
const CANONICAL_NAN_BITS: u64 = 0x7ff8_0000_0000_0000;

#[derive(ThisError, Debug)]
#[error("Failed to convert mysql_async::Value: {0}")]
pub struct ValueConversionError(String);

impl TryFrom<mysql_async::Value> for Value {
    type Error = ValueConversionError;

    fn try_from(value: mysql_async::Value) -> Result<Self, Self::Error> {
        use mysql_async::Value::*;
        match value {
            NULL => Ok(Self::Null),
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
            Date(y, mo, d, h, min, s, us) => {
                // MySQL allows the zero-date `0000-00-00 00:00:00` under
                // permissive sql_modes; chrono rejects it. Surface as an
                // error rather than panicking — callers map this back to a
                // distinct diagnostic.
                let date =
                    NaiveDate::from_ymd_opt(y.into(), mo.into(), d.into()).ok_or_else(|| {
                        ValueConversionError(format!(
                            "invalid date {y:04}-{mo:02}-{d:02} (MySQL zero-date or out-of-range)"
                        ))
                    })?;
                let datetime = date
                    .and_hms_micro_opt(h.into(), min.into(), s.into(), us)
                    .ok_or_else(|| {
                        ValueConversionError(format!("invalid time {h:02}:{min:02}:{s:02}.{us:06}"))
                    })?;
                Ok(Self::DateTime(datetime))
            }
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

impl From<Value> for mysql_async::Value {
    fn from(val: Value) -> Self {
        match val {
            Value::Text(x) => x.into(),
            Value::Integer(x) => x.into(),
            Value::UnsignedInteger(x) => x.into(),
            Value::Real(bits) => f64::from_bits(bits).into(),
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
            Value::BitVector(bv) => mysql_async::Value::Bytes(bv.to_bytes()),
            Value::ByteArray(bytes) => mysql_async::Value::Bytes(bytes),
            Value::Json(json) => mysql_async::Value::from(json.to_string()),
            // MySQL has no TIMESTAMPTZ type; project to UTC-naive so we can
            // round-trip the absolute instant through the MySQL wire format.
            // Lossy on offset, but never panics the run.
            Value::TimestampTz(ts) => mysql_async::Value::from(ts.naive_utc()),
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

        // ToSql is invoked from inside the postgres driver; a panic here
        // would abort the whole fuzz run and lose all stats. Return Err
        // instead so the harness can record a per-query failure and keep
        // going.
        let unsupported = |variant: &str| -> Box<dyn Error + Sync + Send> {
            format!("unsupported type coercion: {variant} → {ty:?}").into()
        };

        match self {
            Value::Text(x) => match *ty {
                Type::TEXT | Type::VARCHAR => x.to_sql(ty, out),
                ref ty if ty.name() == "citext" => x.to_sql(ty, out),
                _ => Err(unsupported("Text")),
            },
            Value::Integer(x) => match *ty {
                Type::BOOL => (*x != 0).to_sql(ty, out),
                Type::CHAR => (*x as i8).to_sql(ty, out),
                Type::INT2 => (*x as i16).to_sql(ty, out),
                Type::INT4 => (*x as i32).to_sql(ty, out),
                Type::INT8 => x.to_sql(ty, out),
                _ => Err(unsupported("Integer")),
            },
            Value::UnsignedInteger(x) => match *ty {
                Type::BOOL => (*x != 0).to_sql(ty, out),
                Type::CHAR => (*x as i8).to_sql(ty, out),
                Type::INT2 => (*x as i16).to_sql(ty, out),
                Type::INT4 => (*x as i32).to_sql(ty, out),
                Type::INT8 => (*x as i64).to_sql(ty, out),
                _ => Err(unsupported("UnsignedInteger")),
            },
            Value::Real(bits) => {
                let val = f64::from_bits(*bits);
                match *ty {
                    Type::FLOAT4 => (val as f32).to_sql(ty, out),
                    Type::FLOAT8 => val.to_sql(ty, out),
                    _ => Err(unsupported("Real")),
                }
            }
            Value::Numeric(d) => match *ty {
                Type::NUMERIC => d.to_sql(ty, out),
                _ => Err(unsupported("Numeric")),
            },
            Value::DateTime(x) => match *ty {
                Type::TIMESTAMP => x.to_sql(ty, out),
                _ => Err(unsupported("DateTime")),
            },
            Value::Time(x) => match *ty {
                Type::TIME => NaiveTime::from(*x).to_sql(ty, out),
                _ => Err(unsupported("Time")),
            },
            Value::ByteArray(array) => match *ty {
                Type::BYTEA => array.to_sql(ty, out),
                _ => Err(unsupported("ByteArray")),
            },
            Value::Null => None::<i8>.to_sql(ty, out),
            Value::BitVector(b) => match *ty {
                Type::BIT | Type::VARBIT => b.to_sql(ty, out),
                _ => Err(unsupported("BitVector")),
            },
            Value::TimestampTz(ts) => match *ty {
                Type::TIMESTAMPTZ => ts.to_sql(ty, out),
                _ => Err(unsupported("TimestampTz")),
            },
            Value::Json(json) => match *ty {
                Type::JSON | Type::JSONB => json.to_sql(ty, out),
                _ => Err(unsupported("Json")),
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

    pgsql::types::to_sql_checked!();
}

impl<'a> pgsql::types::FromSql<'a> for Value {
    fn from_sql(
        ty: &pgsql::types::Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn Error + Sync + Send>> {
        use pgsql::types::Type;

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
            Type::FLOAT4 => Ok(Self::from(f32::from_sql(ty, raw)? as f64)),
            Type::FLOAT8 => Ok(Self::from(f64::from_sql(ty, raw)?)),
            Type::NUMERIC => Ok(Self::Numeric(Decimal::from_sql(ty, raw)?)),
            Type::TEXT | Type::VARCHAR => Ok(Self::Text(String::from_sql(ty, raw)?)),
            Type::TEXT_ARRAY | Type::VARCHAR_ARRAY => {
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
                let val = match NaiveDateTime::from_sql(ty, raw) {
                    Ok(datetime) => datetime,
                    Err(_) => {
                        let date = NaiveDate::from_sql(ty, raw)?;
                        date.and_hms_opt(0, 0, 0)
                            .ok_or("midnight does not exist for parsed date")?
                    }
                };
                Ok(Self::DateTime(val))
            }
            Type::TIME => Ok(Self::Time(NaiveTime::from_sql(ty, raw)?.into())),
            Type::TIMESTAMP => Ok(Self::DateTime(NaiveDateTime::from_sql(ty, raw)?)),
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
            DfValue::TimestampTz(ref ts) => {
                if ts.has_timezone() {
                    Ok(Value::TimestampTz(ts.to_chrono()))
                } else {
                    Ok(Value::DateTime(ts.to_chrono().naive_utc()))
                }
            }
            DfValue::Time(t) => Ok(Value::Time(t)),
            DfValue::ByteArray(t) => Ok(Value::ByteArray(t.as_ref().clone())),
            DfValue::Numeric(ref d) => Ok(Value::Numeric(d.as_ref().clone())),
            DfValue::BitVector(ref b) => Ok(Value::BitVector(b.as_ref().clone())),
            DfValue::Array(_) => bail!("Arrays not supported"),
            DfValue::PassThrough(_) => bail!("PassThrough DfValue not supported"),
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
            Self::Real(bits) => write!(f, "{}", f64::from_bits(*bits)),
            Self::Numeric(d) => write!(f, "{d}"),
            Self::DateTime(dt) => write!(f, "{}", dt.format(TIMESTAMP_FORMAT)),
            Self::Null => write!(f, "NULL"),
            Self::Time(t) => write!(f, "{t}"),
            Self::ByteArray(a) => write!(f, "{a:?}"),
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

impl From<f32> for Value {
    fn from(f: f32) -> Self {
        Self::from(f as f64)
    }
}

impl From<f64> for Value {
    fn from(f: f64) -> Self {
        let bits = if f.is_nan() {
            CANONICAL_NAN_BITS
        } else {
            f.to_bits()
        };
        Self::Real(bits)
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Value {
    /// Total order on `Value` that agrees with the derived `PartialEq`/`Eq`:
    /// values of different variants compare via a fixed variant-tag tie-break,
    /// and within a variant we delegate to the inner type's `Ord`. This avoids
    /// `to_string`-based ordering, which would otherwise break invariants the
    /// harness depends on (lexicographic integer order, `Display`-collapsed
    /// `Real` and non-printable `Text` bytes sorting equal, etc.).
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        use Value::*;
        match (self, other) {
            (Null, Null) => cmp::Ordering::Equal,
            (Integer(a), Integer(b)) => a.cmp(b),
            (UnsignedInteger(a), UnsignedInteger(b)) => a.cmp(b),
            (Real(a), Real(b)) => f64::from_bits(*a).total_cmp(&f64::from_bits(*b)),
            (Numeric(a), Numeric(b)) => a.cmp(b),
            (Text(a), Text(b)) => a.cmp(b),
            (ByteArray(a), ByteArray(b)) => a.cmp(b),
            (DateTime(a), DateTime(b)) => a.cmp(b),
            (Time(a), Time(b)) => a.cmp(b),
            (TimestampTz(a), TimestampTz(b)) => a.cmp(b),
            (BitVector(a), BitVector(b)) => a.cmp(b),
            (Json(a), Json(b)) => cmp_json(a, b),
            _ => variant_tag(self).cmp(&variant_tag(other)),
        }
    }
}

/// Structural total order on [`JsonValue`] that agrees with derived
/// `PartialEq`. `serde_json::Value` does not implement `Ord` (numbers can be
/// arbitrary-precision and float NaN has no total order), so we walk the tree
/// recursively and use [`f64::total_cmp`] for numeric leaves.
fn cmp_json(a: &JsonValue, b: &JsonValue) -> cmp::Ordering {
    fn variant_tag(v: &JsonValue) -> u8 {
        match v {
            JsonValue::Null => 0,
            JsonValue::Bool(_) => 1,
            JsonValue::Number(_) => 2,
            JsonValue::String(_) => 3,
            JsonValue::Array(_) => 4,
            JsonValue::Object(_) => 5,
        }
    }
    match (a, b) {
        (JsonValue::Null, JsonValue::Null) => cmp::Ordering::Equal,
        (JsonValue::Bool(x), JsonValue::Bool(y)) => x.cmp(y),
        (JsonValue::Number(x), JsonValue::Number(y)) => {
            // serde_json::Number has no Ord; coerce to f64 for total ordering.
            // `as_f64` returns None only for arbitrary-precision integers we
            // can't represent — fall back to lexicographic to keep the order
            // total.
            match (x.as_f64(), y.as_f64()) {
                (Some(xf), Some(yf)) => xf.total_cmp(&yf),
                _ => x.to_string().cmp(&y.to_string()),
            }
        }
        (JsonValue::String(x), JsonValue::String(y)) => x.cmp(y),
        (JsonValue::Array(xs), JsonValue::Array(ys)) => xs
            .iter()
            .zip(ys.iter())
            .map(|(a, b)| cmp_json(a, b))
            .find(|o| !o.is_eq())
            .unwrap_or_else(|| xs.len().cmp(&ys.len())),
        (JsonValue::Object(xs), JsonValue::Object(ys)) => {
            // serde_json::Map without `preserve_order` is BTreeMap-backed, so
            // both iterators yield entries in sorted-key order.
            xs.iter()
                .zip(ys.iter())
                .map(|((xk, xv), (yk, yv))| xk.cmp(yk).then_with(|| cmp_json(xv, yv)))
                .find(|o| !o.is_eq())
                .unwrap_or_else(|| xs.len().cmp(&ys.len()))
        }
        _ => variant_tag(a).cmp(&variant_tag(b)),
    }
}

/// Stable per-variant tag used as the inter-variant tie-break for `Ord`.
/// Order has no semantic meaning — it just needs to be total and consistent.
fn variant_tag(v: &Value) -> u8 {
    match v {
        Value::Null => 0,
        Value::Integer(_) => 1,
        Value::UnsignedInteger(_) => 2,
        Value::Real(_) => 3,
        Value::Numeric(_) => 4,
        Value::Text(_) => 5,
        Value::ByteArray(_) => 6,
        Value::DateTime(_) => 7,
        Value::Time(_) => 8,
        Value::TimestampTz(_) => 9,
        Value::BitVector(_) => 10,
        Value::Json(_) => 11,
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
                            .ok_or_else(|| anyhow::anyhow!("Invalid float value"))?,
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
        Self::normalize_json_value(serde_json::from_str(s).map_err(|e| anyhow::anyhow!("{e}"))?)
    }

    /// Comparison oracle for query-result equality.
    ///
    /// Coerces across:
    /// - numeric variants (`Integer` ↔ `UnsignedInteger` ↔ `Numeric`) so that
    ///   values upstream and Readyset choose to surface in different runtime
    ///   types but with the same wall value still compare equal;
    /// - timestamp variants (`DateTime` ↔ `TimestampTz`) by absolute UTC
    ///   instant — `DateTime` is treated as already in UTC, matching the
    ///   convention MySQL uses on the wire.
    ///
    /// All other variants fall back to the derived structural `PartialEq`.
    /// Use this — not `==` — when comparing results between the two
    /// databases.
    pub fn value_eq(&self, other: &Self) -> bool {
        use Value::*;
        match (self, other) {
            (Integer(a), Integer(b)) => a == b,
            (UnsignedInteger(a), UnsignedInteger(b)) => a == b,
            (Numeric(a), Numeric(b)) => a == b,
            (Integer(i), UnsignedInteger(u)) | (UnsignedInteger(u), Integer(i)) => {
                *i >= 0 && (*i as u64) == *u
            }
            (Integer(i), Numeric(d)) | (Numeric(d), Integer(i)) => &Decimal::from(*i) == d,
            (UnsignedInteger(u), Numeric(d)) | (Numeric(d), UnsignedInteger(u)) => {
                &Decimal::from(*u) == d
            }
            (DateTime(naive), TimestampTz(ts)) | (TimestampTz(ts), DateTime(naive)) => {
                *naive == ts.naive_utc()
            }
            _ => self == other,
        }
    }

    /// Total order that agrees with [`Value::value_eq`].
    ///
    /// `Ord` on `Value` tie-breaks unequal variants by a fixed variant tag,
    /// which sorts e.g. `Integer(5)` before `Numeric(3)`. That ordering
    /// disagrees with `value_eq`'s cross-variant coercion: when one side of
    /// a result set returns a column with mixed numeric variants and the
    /// other returns it normalized, sorting by `Ord` produces row orderings
    /// that don't align, so cell-wise `value_eq` checks see mispaired rows
    /// and flag a spurious mismatch. Use this comparator when sorting
    /// results for comparison; use `Ord` elsewhere.
    pub fn cmp_compat(&self, other: &Self) -> cmp::Ordering {
        use Value::*;
        match (self, other) {
            (Integer(i), UnsignedInteger(u)) => Decimal::from(*i).cmp(&Decimal::from(*u)),
            (UnsignedInteger(u), Integer(i)) => Decimal::from(*u).cmp(&Decimal::from(*i)),
            (Integer(i), Numeric(d)) => Decimal::from(*i).cmp(d),
            (Numeric(d), Integer(i)) => d.cmp(&Decimal::from(*i)),
            (UnsignedInteger(u), Numeric(d)) => Decimal::from(*u).cmp(d),
            (Numeric(d), UnsignedInteger(u)) => d.cmp(&Decimal::from(*u)),
            (DateTime(naive), TimestampTz(ts)) => naive.cmp(&ts.naive_utc()),
            (TimestampTz(ts), DateTime(naive)) => ts.naive_utc().cmp(naive),
            _ => self.cmp(other),
        }
    }
}

// ---------------------------------------------------------------------------
// QueryParams — wraps parameter values for prepared statement execution
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
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
}

pub enum QueryParamsIntoIter {
    Empty,
    Positional(vec::IntoIter<Value>),
    /// Sorted parameter indices paired with the lookup table. Iterating
    /// over the actual keys (rather than `min..=max`) avoids panicking on
    /// sparse numbering and keeps `size_hint` honest.
    Numbered(vec::IntoIter<u32>, HashMap<u32, Value>),
}

impl Iterator for QueryParamsIntoIter {
    type Item = Value;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Empty => None,
            Self::Positional(vs) => vs.next(),
            Self::Numbered(keys, vals) => keys.next().and_then(|i| vals.remove(&i)),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = match self {
            Self::Empty => 0,
            Self::Positional(vs) => vs.len(),
            Self::Numbered(keys, _) => keys.len(),
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
                let mut keys: Vec<u32> = np.keys().copied().collect();
                keys.sort_unstable();
                Numbered(keys.into_iter(), np)
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

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;

    use readyset_decimal::Decimal;

    use super::*;

    #[test]
    fn integer_ord_is_numeric_not_lexicographic() {
        assert_eq!(Value::Integer(2).cmp(&Value::Integer(10)), Ordering::Less);
    }

    #[test]
    fn mysql_zero_date_returns_error_not_panic() {
        // MySQL admits `0000-00-00 00:00:00` under permissive sql_modes; the
        // mysql_async driver surfaces it as Date(0,0,0,...) which chrono
        // rejects. Surface that as a structured error rather than panicking
        // through `.unwrap()`.
        let zero = mysql_async::Value::Date(0, 0, 0, 0, 0, 0, 0);
        let err = Value::try_from(zero).expect_err("zero-date must error");
        assert!(format!("{err}").contains("0000-00-00"));
    }

    #[test]
    fn numbered_params_with_sparse_keys_iterate_without_panic() {
        // An iterator over numbered params used to panic when keys were
        // non-contiguous (`vals.remove(&i).unwrap()` for i ∉ keys). The fix
        // walks sorted keys directly so sparse maps round-trip cleanly.
        let mut np: HashMap<u32, Value> = HashMap::new();
        np.insert(1, Value::Integer(10));
        np.insert(3, Value::Integer(30));
        np.insert(5, Value::Integer(50));
        let qp = QueryParams::NumberedParams(np);
        let collected: Vec<Value> = qp.into_iter().collect();
        assert_eq!(
            collected,
            vec![Value::Integer(10), Value::Integer(30), Value::Integer(50)]
        );
    }

    #[test]
    fn text_ord_distinguishes_byte_distinct_nonprintables() {
        let a = Value::Text("\x01".to_string());
        let b = Value::Text("\x02".to_string());
        assert_eq!(a.to_string(), b.to_string());
        assert_ne!(a.cmp(&b), Ordering::Equal);
    }

    #[test]
    fn real_ord_uses_total_cmp_not_display() {
        // Order under total_cmp: -inf < -finite < -0 < +0 < +finite < +inf < NaN.
        let neg_inf = Value::from(f64::NEG_INFINITY);
        let neg_one = Value::from(-1.0_f64);
        let neg_zero = Value::from(-0.0_f64);
        let pos_zero = Value::from(0.0_f64);
        let one = Value::from(1.0_f64);
        let pos_inf = Value::from(f64::INFINITY);
        let nan = Value::from(f64::NAN);
        let expected = vec![
            neg_inf.clone(),
            neg_one.clone(),
            neg_zero.clone(),
            pos_zero.clone(),
            one.clone(),
            pos_inf.clone(),
            nan.clone(),
        ];
        let mut shuffled = expected.clone();
        shuffled.reverse();
        shuffled.sort();
        assert_eq!(shuffled, expected);
    }

    #[test]
    fn real_round_trip_finite() {
        // Including a fraction that the old (whole, frac*1e9) encoding would
        // have truncated past 9 digits.
        let cases = [
            0.0_f64,
            -0.0,
            1.0,
            -1.0,
            std::f64::consts::PI,
            1.234_567_890_123,
            f64::MIN_POSITIVE,
            f64::MIN_POSITIVE / 2.0, // denormal
            f64::MAX,
            f64::MIN,
        ];
        for x in cases {
            let v = Value::from(x);
            match v {
                Value::Real(bits) => {
                    assert_eq!(
                        f64::from_bits(bits).to_bits(),
                        x.to_bits(),
                        "round-trip failed for {x:?}"
                    );
                }
                other => panic!("expected Real, got {other:?}"),
            }
        }
    }

    #[test]
    fn real_round_trip_special() {
        for x in [f64::INFINITY, f64::NEG_INFINITY] {
            let v = Value::from(x);
            match v {
                Value::Real(bits) => assert_eq!(f64::from_bits(bits), x),
                other => panic!("expected Real for {x:?}, got {other:?}"),
            }
        }
        match Value::from(f64::NAN) {
            Value::Real(bits) => assert!(f64::from_bits(bits).is_nan()),
            other => panic!("expected Real for NaN, got {other:?}"),
        }
    }

    #[test]
    fn real_canonicalizes_nan() {
        // Two distinct NaN bit patterns must collapse to the same Value so
        // derived `PartialEq` reports them as equal.
        let nan1 = f64::NAN;
        let nan2 = f64::from_bits(0x7ff8_0000_0000_0001);
        assert!(nan1.is_nan() && nan2.is_nan());
        assert_eq!(Value::from(nan1), Value::from(nan2));
    }

    #[test]
    fn real_signed_zeros_distinct_under_total_cmp() {
        // total_cmp orders -0 before +0; the canonical NaN must not collapse them.
        let pos = Value::from(0.0_f64);
        let neg = Value::from(-0.0_f64);
        assert_ne!(pos, neg);
        assert_eq!(neg.cmp(&pos), Ordering::Less);
    }

    #[test]
    fn real_precision_past_nine_fractional_digits() {
        // The old encoding (i64, u64*1e9) truncated past 9 fractional digits
        // and would falsely report these distinct values as equal.
        let a = Value::from(1.234_567_890_123_f64);
        let b = Value::from(1.234_567_891_999_f64);
        assert_ne!(a, b);
    }

    #[test]
    fn real_negative_paths_agree_across_f32_and_f64() {
        // Old code applied `.fract().abs()` only on the f64 path, producing
        // diverging Values for the same wall value. f32 now widens to f64
        // through a single conversion path, so they must match.
        assert_eq!(Value::from(-1.5_f32), Value::from(-1.5_f64));
        assert_eq!(Value::from(-0.25_f32), Value::from(-0.25_f64));
    }

    #[test]
    fn ord_inter_variant_is_stable() {
        let null = Value::Null;
        let int = Value::Integer(0);
        let text = Value::Text(String::new());
        assert_ne!(null.cmp(&int), Ordering::Equal);
        assert_ne!(int.cmp(&text), Ordering::Equal);
        assert_eq!(null.cmp(&int), null.cmp(&int));
        assert_eq!(int.cmp(&text), int.cmp(&text));
    }

    #[test]
    fn ord_agrees_with_eq_within_variant() {
        assert_eq!(Value::Integer(5).cmp(&Value::Integer(5)), Ordering::Equal);
        assert_eq!(
            Value::Text("abc".to_string()).cmp(&Value::Text("abc".to_string())),
            Ordering::Equal
        );
    }

    #[test]
    fn value_eq_coerces_integer_unsigned_integer() {
        assert!(Value::Integer(5).value_eq(&Value::UnsignedInteger(5)));
        assert!(Value::UnsignedInteger(5).value_eq(&Value::Integer(5)));
        assert!(!Value::Integer(-1).value_eq(&Value::UnsignedInteger(u64::MAX)));
    }

    #[test]
    fn value_eq_coerces_integer_numeric() {
        assert!(Value::Integer(5).value_eq(&Value::Numeric(Decimal::from(5_i64))));
        assert!(Value::Numeric(Decimal::from(5_i64)).value_eq(&Value::Integer(5)));
        assert!(Value::UnsignedInteger(5).value_eq(&Value::Numeric(Decimal::from(5_u64))));
    }

    #[test]
    fn value_eq_falls_back_to_structural_equality() {
        assert!(Value::Text("abc".to_string()).value_eq(&Value::Text("abc".to_string())));
        assert!(!Value::Text("abc".to_string()).value_eq(&Value::Text("abd".to_string())));
        assert!(Value::Null.value_eq(&Value::Null));
        assert!(!Value::Integer(5).value_eq(&Value::Text("5".to_string())));
    }

    #[test]
    fn cross_variant_partial_eq_remains_structural() {
        // Derived PartialEq must remain strict so that Ord can agree with Eq.
        assert_ne!(Value::Integer(5), Value::UnsignedInteger(5));
        assert_ne!(Value::Integer(5), Value::Numeric(Decimal::from(5_i64)));
    }

    #[test]
    fn value_eq_treats_datetime_as_utc_naive_against_timestamptz() {
        // 17:00 UTC naive == 12:00 UTC-05:00 (same instant)
        let utc_naive = Value::DateTime(
            NaiveDate::from_ymd_opt(2024, 1, 1)
                .unwrap()
                .and_hms_opt(17, 0, 0)
                .unwrap(),
        );
        let est =
            Value::TimestampTz(DateTime::parse_from_rfc3339("2024-01-01T12:00:00-05:00").unwrap());
        assert!(utc_naive.value_eq(&est));
        assert!(est.value_eq(&utc_naive));
    }

    #[test]
    fn value_eq_rejects_equal_wall_clock_different_zones() {
        // Same wall-clock 12:00:00, different zones → different absolute
        // instants → must compare unequal under the cross-dialect oracle.
        let utc_naive = Value::DateTime(
            NaiveDate::from_ymd_opt(2024, 1, 1)
                .unwrap()
                .and_hms_opt(12, 0, 0)
                .unwrap(),
        );
        let est =
            Value::TimestampTz(DateTime::parse_from_rfc3339("2024-01-01T12:00:00-05:00").unwrap());
        assert!(!utc_naive.value_eq(&est));
    }

    #[test]
    fn cmp_compat_agrees_with_value_eq_across_coerced_variants() {
        // For every pair where `value_eq` returns true, `cmp_compat` must
        // return `Equal`; otherwise sort-then-zip comparison can drift
        // mispaired rows out from under `value_eq`.
        let pairs: &[(Value, Value)] = &[
            (Value::Integer(5), Value::UnsignedInteger(5)),
            (Value::Integer(7), Value::Numeric(Decimal::from(7))),
            (Value::UnsignedInteger(9), Value::Numeric(Decimal::from(9))),
            (
                Value::DateTime(
                    NaiveDate::from_ymd_opt(2024, 1, 1)
                        .unwrap()
                        .and_hms_opt(12, 0, 0)
                        .unwrap(),
                ),
                Value::TimestampTz(
                    DateTime::parse_from_rfc3339("2024-01-01T12:00:00+00:00").unwrap(),
                ),
            ),
        ];
        for (a, b) in pairs {
            assert!(a.value_eq(b), "precondition: value_eq({a:?}, {b:?})");
            assert_eq!(a.cmp_compat(b), cmp::Ordering::Equal, "{a:?} vs {b:?}");
            assert_eq!(b.cmp_compat(a), cmp::Ordering::Equal, "{b:?} vs {a:?}");
        }
    }

    #[test]
    fn cmp_compat_orders_across_numeric_variants_by_value() {
        // Variant-tag tie-break in `Ord` would sort `Integer(5)` before
        // `Numeric(3)`; `cmp_compat` must order by numeric value instead.
        assert_eq!(
            Value::Integer(5).cmp_compat(&Value::Numeric(Decimal::from(3))),
            cmp::Ordering::Greater
        );
        assert_eq!(
            Value::Numeric(Decimal::from(3)).cmp_compat(&Value::Integer(5)),
            cmp::Ordering::Less
        );
        assert_eq!(
            Value::UnsignedInteger(2).cmp_compat(&Value::Integer(10)),
            cmp::Ordering::Less
        );
    }

    #[test]
    fn datetime_preserves_subsecond_precision() {
        // The naive timestamp path must preserve at least microsecond
        // precision for round-tripping Postgres TIMESTAMP through Value.
        let dt = NaiveDate::from_ymd_opt(2024, 1, 1)
            .unwrap()
            .and_hms_micro_opt(0, 0, 0, 123_456)
            .unwrap();
        let v = Value::DateTime(dt);
        let mysql: mysql_async::Value = v.clone().into();
        match mysql {
            mysql_async::Value::Date(_, _, _, _, _, _, us) => assert_eq!(us, 123_456),
            other => panic!("expected mysql Date, got {other:?}"),
        }
    }

    #[test]
    fn to_sql_returns_err_on_type_mismatch_instead_of_panic() {
        use bytes::BytesMut;
        use tokio_postgres::types::{ToSql, Type};

        // Pair each variant with a Type it does NOT accept; the impl must
        // surface an Err rather than panic. INT4 is "wrong" for everything
        // except Integer/UnsignedInteger.
        let cases: Vec<(Value, Type)> = vec![
            (Value::Text("x".into()), Type::INT4),
            (Value::Real(0u64), Type::TEXT),
            (Value::Numeric(Decimal::from(1_i64)), Type::TEXT),
            (
                Value::DateTime(
                    NaiveDate::from_ymd_opt(2024, 1, 1)
                        .unwrap()
                        .and_hms_opt(0, 0, 0)
                        .unwrap(),
                ),
                Type::TEXT,
            ),
            (Value::Time(MySqlTime::from_microseconds(0)), Type::TEXT),
            (Value::ByteArray(vec![1, 2]), Type::TEXT),
            (Value::BitVector(BitVec::from_bytes(&[0])), Type::TEXT),
            (
                Value::TimestampTz(DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z").unwrap()),
                Type::TEXT,
            ),
            (Value::Json(serde_json::json!({})), Type::TEXT),
        ];

        for (v, ty) in cases {
            let mut buf = BytesMut::new();
            let res = v.to_sql(&ty, &mut buf);
            assert!(res.is_err(), "expected Err for {v:?} → {ty:?}");
        }
    }

    #[test]
    fn to_sql_succeeds_for_each_variant_with_compatible_type() {
        use bytes::BytesMut;
        use tokio_postgres::types::{ToSql, Type};

        let cases: Vec<(Value, Type)> = vec![
            (Value::Text("x".into()), Type::TEXT),
            (Value::Integer(1), Type::INT8),
            (Value::UnsignedInteger(1), Type::INT8),
            (Value::Real(0u64), Type::FLOAT8),
            (Value::Numeric(Decimal::from(1_i64)), Type::NUMERIC),
            (
                Value::DateTime(
                    NaiveDate::from_ymd_opt(2024, 1, 1)
                        .unwrap()
                        .and_hms_opt(0, 0, 0)
                        .unwrap(),
                ),
                Type::TIMESTAMP,
            ),
            (Value::Time(MySqlTime::from_microseconds(0)), Type::TIME),
            (Value::ByteArray(vec![1, 2]), Type::BYTEA),
            (Value::Null, Type::INT4),
            (Value::BitVector(BitVec::from_bytes(&[0])), Type::BIT),
            (
                Value::TimestampTz(DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z").unwrap()),
                Type::TIMESTAMPTZ,
            ),
            (Value::Json(serde_json::json!({})), Type::JSON),
        ];

        for (v, ty) in cases {
            let mut buf = BytesMut::new();
            let res = v.to_sql(&ty, &mut buf);
            assert!(
                res.is_ok(),
                "expected Ok for {v:?} → {ty:?}, got Err: {:?}",
                res.err()
            );
        }
    }

    #[test]
    fn timestamptz_serializes_to_mysql_without_panic() {
        // Previously `From<Value> for mysql_async::Value` had
        // `unimplemented!()` for TimestampTz; the run would panic on the
        // first such param. We project to UTC-naive instead.
        let ts =
            Value::TimestampTz(DateTime::parse_from_rfc3339("2024-01-01T12:00:00-05:00").unwrap());
        let mysql: mysql_async::Value = ts.into();
        match mysql {
            mysql_async::Value::Date(year, mo, d, h, _, _, _) => {
                assert_eq!((year, mo, d, h), (2024, 1, 1, 17));
            }
            other => panic!("expected mysql Date, got {other:?}"),
        }
    }
}
