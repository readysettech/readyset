//! AST for `sqllogictest` files. See the [SQLite documentation][1] for more information.
//!
//! [1]: https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki

use std::borrow::Cow;
use std::collections::HashMap;
use std::convert::{TryFrom, TryInto};
use std::error::Error;
use std::fmt::{self, Display};
use std::ops::RangeInclusive;
use std::str::FromStr;
use std::{cmp, vec};

use anyhow::{anyhow, bail};
use ascii_utils::Check;
use bit_vec::BitVec;
use chrono::{NaiveDate, NaiveTime, Utc};
use derive_more::{From, TryInto};
use itertools::Itertools;
use mysql_common::chrono::NaiveDateTime;
use mysql_time::MysqlTime;
use nom_sql::{Literal, SqlQuery};
use noria_data::{DataType, TIMESTAMP_FORMAT};
use pgsql::types::{accepts, to_sql_checked};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use tokio_postgres as pgsql;

/// The expected result of a statement
#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub enum StatementResult {
    /// The statement should succeed
    Ok,
    /// The statement should fail
    Error,
}

impl Display for StatementResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StatementResult::Ok => f.write_str("ok"),
            StatementResult::Error => f.write_str("error"),
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
            Conditional::SkipIf(engine) => write!(f, "skipif {}", engine),
            Conditional::OnlyIf(engine) => write!(f, "onlyif {}", engine),
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
    Real,
    Numeric,
    Date,
    Time,
    ByteArray,
    BitVec,
}

impl Type {
    pub fn of_mysql_value(val: &mysql_async::Value) -> Option<Self> {
        use mysql_async::Value::*;
        match val {
            Bytes(_) => Some(Self::Text),
            Int(_) => Some(Self::Real),
            UInt(_) => Some(Self::Real),
            Float(_) => Some(Self::Real),
            Double(_) => Some(Self::Real),
            Date(_, _, _, _, _, _, _) => Some(Self::Date),
            Time(_, _, _, _, _, _) => Some(Self::Time),
            NULL => None,
        }
    }
}

impl Display for Type {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Text => write!(f, "T"),
            Self::Integer => write!(f, "I"),
            Self::Real => write!(f, "R"),
            Self::Numeric => write!(f, "F"), // F, as in fixed-point number
            Self::Date => write!(f, "D"),
            Self::Time => write!(f, "M"),
            Self::ByteArray => write!(f, "B"),
            Self::BitVec => write!(f, "BV"),
        }
    }
}

/// Result set sorting mode of a query
#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub enum SortMode {
    /// nosort - the default value. In nosort mode, the results appear in exactly the order in which
    /// they were received from the database engine. The nosort mode should only be used on queries
    /// that have an ORDER BY clause or which only have a single row of result, since otherwise the
    /// order of results is undefined and might vary from one database engine to another.
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
#[derive(Debug, Eq, PartialEq, Clone, TryInto, From)]
pub enum Value {
    Text(String),
    Integer(i64),
    Real(i64, u64),
    Date(NaiveDateTime),
    Time(MysqlTime),
    ByteArray(Vec<u8>),
    Numeric(Decimal),
    Null,
    BitVector(BitVec),
}

impl TryFrom<mysql_async::Value> for Value {
    type Error = anyhow::Error;

    fn try_from(value: mysql_async::Value) -> Result<Self, Self::Error> {
        use mysql_async::Value::*;
        match value {
            NULL => Ok(Self::Null),
            Bytes(bs) => Ok(Self::Text(String::from_utf8(bs)?)),
            Int(i) => Ok(Self::Integer(i)),
            UInt(i) => Ok(Self::Integer(i.try_into()?)),
            Float(f) => Self::try_from(Double(f as f64)),
            Double(f) => {
                if !f.is_finite() {
                    bail!("Invalid infinite float value");
                }
                Ok(Self::Real(
                    f.trunc() as i64,
                    (f.fract() * 1_000_000_000.0).round() as _,
                ))
            }
            Date(y, mo, d, h, min, s, us) => Ok(Self::Date(
                NaiveDate::from_ymd(y.into(), mo.into(), d.into()).and_hms_micro(
                    h.into(),
                    min.into(),
                    s.into(),
                    us,
                ),
            )),
            Time(neg, d, h, m, s, us) => Ok(Self::Time(MysqlTime::from_hmsus(
                !neg,
                (d * 24 + (h as u32)).try_into()?,
                m,
                s,
                us.into(),
            ))),
        }
    }
}

impl TryFrom<Literal> for Value {
    type Error = anyhow::Error;
    fn try_from(value: Literal) -> Result<Self, Self::Error> {
        macro_rules! real_value {
            ($real:expr, $prec:expr) => {{
                let integral = $real as i64;
                Value::Real(
                    integral,
                    ((($real as f64) - (integral as f64)) * (10_u64.pow($prec as u32) as f64))
                        as u64,
                )
            }};
        }
        Ok(match value {
            Literal::Null => Value::Null,
            Literal::Boolean(b) => Value::Integer(if b { 1 } else { 0 }),
            Literal::Integer(v) => Value::Integer(v),
            Literal::Float(float) => real_value!(float.value, float.precision),
            Literal::Double(double) => real_value!(double.value, double.precision),
            Literal::Numeric(mantissa, scale) => Decimal::try_from_i128_with_scale(mantissa, scale)
                .map_err(|e| {
                    anyhow::Error::msg(format!(
                        "Could not convert literal value to NUMERIC type: {}",
                        e
                    ))
                })
                .map(Value::Numeric)?,
            Literal::String(v) => Value::Text(v),
            Literal::Blob(v) => Value::Text(String::from_utf8(v)?),
            Literal::CurrentTime => Value::Time(Utc::now().naive_utc().time().into()),
            Literal::CurrentDate => Value::Date(Utc::now().naive_utc()),
            Literal::CurrentTimestamp => Value::Date(Utc::now().naive_utc()),
            Literal::ByteArray(b) => Value::ByteArray(b),
            Literal::BitVector(b) => Value::BitVector(BitVec::from_bytes(b.as_slice())),
            Literal::Placeholder(_) => bail!("Placeholders are not valid values"),
        })
    }
}

impl From<Value> for mysql_async::Value {
    fn from(val: Value) -> Self {
        match val {
            Value::Text(x) => x.into(),
            Value::Integer(x) => x.into(),
            Value::Real(i, f) => (i as f64 + ((f as f64) / 1_000_000_000.0)).into(),
            Value::Numeric(d) => {
                // FIXME(fran): This shouldn't be implemented for mysql_async::Value, since
                // MySQL has it's own type `DECIMAL`, which is not supported by the library.
                // However, it seems like the AST relies on this even when the database being used
                // is Postgres, so we return a float to bypass that for now.
                d.to_f64().unwrap_or(f64::MAX).into()
            }
            Value::Null => mysql_async::Value::NULL,
            Value::Date(dt) => mysql_async::Value::from(dt),
            Value::Time(t) => mysql_async::Value::Time(
                !t.is_positive(),
                (t.hour() / 24).into(),
                (t.hour() % 24) as _,
                t.minutes(),
                t.seconds(),
                t.microseconds(),
            ),
            // These types are PostgreSQL-specific
            Value::ByteArray(_) => unimplemented!(),
            Value::BitVector(_) => unimplemented!(),
        }
    }
}

impl pgsql::types::ToSql for Value {
    fn to_sql(
        &self,
        ty: &pgsql::types::Type,
        out: &mut bytes::BytesMut,
    ) -> Result<pgsql::types::IsNull, Box<dyn Error + Sync + Send>> {
        match self {
            Value::Text(x) => x.to_sql(ty, out),
            Value::Integer(x) => x.to_sql(ty, out),
            Value::Real(i, f) => (*i as f64 + ((*f as f64) / 1_000_000_000.0)).to_sql(ty, out),
            Value::Numeric(d) => d.to_sql(ty, out),
            Value::Date(x) => x.to_sql(ty, out),
            Value::Time(x) => NaiveTime::from(*x).to_sql(ty, out),
            Value::ByteArray(array) => array.to_sql(ty, out),
            Value::Null => None::<i8>.to_sql(ty, out),
            Value::BitVector(b) => b.to_sql(ty, out),
        }
    }

    accepts!(
        BOOL, BYTEA, CHAR, NAME, INT2, INT4, INT8, TEXT, VARCHAR, DATE, TIME, TIMESTAMP, BIT,
        VARBIT
    );

    to_sql_checked!();
}

impl<'a> pgsql::types::FromSql<'a> for Value {
    fn from_sql(
        ty: &pgsql::types::Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn Error + Sync + Send>> {
        use pgsql::types::Type;

        match *ty {
            Type::BOOL => Ok(Self::Integer(bool::from_sql(ty, raw)? as _)),
            Type::CHAR => Ok(Self::Integer(i8::from_sql(ty, raw)? as _)),
            Type::INT2 => Ok(Self::Integer(i16::from_sql(ty, raw)? as _)),
            Type::INT4 => Ok(Self::Integer(i32::from_sql(ty, raw)? as _)),
            Type::INT8 => Ok(Self::Integer(i64::from_sql(ty, raw)?)),
            Type::FLOAT4 => Ok(Self::from(f32::from_sql(ty, raw)? as f64)),
            Type::FLOAT8 => Ok(Self::from(f64::from_sql(ty, raw)?)),
            Type::NUMERIC => Ok(Self::Numeric(Decimal::from_sql(ty, raw)?)),
            Type::TEXT => Ok(Self::Text(String::from_sql(ty, raw)?)),
            Type::DATE => Ok(Self::Date(NaiveDateTime::from_sql(ty, raw)?)),
            Type::TIME => Ok(Self::Time(NaiveTime::from_sql(ty, raw)?.into())),
            Type::BIT | Type::VARBIT => Ok(Self::BitVector(BitVec::from_sql(ty, raw)?)),
            _ => Err("Invalid type".into()),
        }
    }

    accepts!(BOOL, CHAR, INT2, INT4, INT8, FLOAT4, FLOAT8, NUMERIC, TEXT, DATE, TIME);
}

impl TryFrom<DataType> for Value {
    type Error = anyhow::Error;

    fn try_from(value: DataType) -> Result<Self, Self::Error> {
        match value {
            DataType::None => Ok(Value::Null),
            DataType::Int(i) => Ok(Value::Integer(i.into())),
            DataType::UnsignedInt(u) => Ok(Value::Integer(u.into())),
            DataType::BigInt(bi) => Ok(Value::Integer(bi)),
            DataType::UnsignedBigInt(bu) => Ok(Value::Integer(bu.try_into()?)),
            DataType::Float(f, _) => Ok(f.into()),
            DataType::Double(f, _) => Ok(f.into()),
            DataType::Text(_) | DataType::TinyText(_) => Ok(Value::Text(value.try_into()?)),
            DataType::Timestamp(ts) => Ok(Value::Date(ts)),
            DataType::TimestampTz(ref ts) => Ok(Value::Date(ts.as_ref().naive_utc())),
            DataType::Time(t) => Ok(Value::Time(*t)),
            DataType::ByteArray(t) => Ok(Value::ByteArray(t.as_ref().clone())),
            DataType::Numeric(ref d) => Ok(Value::Numeric(*d.as_ref())),
            DataType::BitVector(ref b) => Ok(Value::BitVector(b.as_ref().clone())),
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
                        if chr.is_printable() {
                            write!(f, "{}", chr)?;
                        } else {
                            write!(f, "@")?;
                        }
                    }
                    Ok(())
                }
            }
            Self::Integer(i) => write!(f, "{}", i),
            Self::Real(whole, frac) => {
                write!(f, "{}.", whole)?;
                let frac = frac.to_string();
                write!(f, "{}", &frac[..(cmp::min(frac.len(), 3))])
            }
            Self::Numeric(d) => {
                // TODO(fran): We will probably need to extend our NUMERIC
                //  implementation to correctly support the precision and scale,
                //  so we can display it correctly.
                write!(f, "{}", d)
            }
            Self::Date(dt) => write!(f, "{}", dt.format(TIMESTAMP_FORMAT)),
            Self::Null => write!(f, "NULL"),
            Self::Time(t) => write!(f, "{}", t),
            Self::ByteArray(a) => {
                // TODO(fran): This is gonna be more complicated than this, probably.
                write!(f, "{:?}", a)
            }
            Self::BitVector(b) => {
                write!(
                    f,
                    "{}",
                    b.iter().map(|bit| if bit { "1" } else { "0" }).join("")
                )
            }
        }
    }
}

impl From<f32> for Value {
    fn from(f: f32) -> Self {
        Self::Real(f.trunc() as i64, (f.fract() * 1_000_000_000.0).round() as _)
    }
}

impl From<f64> for Value {
    fn from(f: f64) -> Self {
        Self::Real(
            f.trunc() as i64,
            (f.fract().abs() * 1_000_000_000.0).round() as _,
        )
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Value {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        format!("{}", self).cmp(&format!("{}", other))
    }
}

impl Value {
    pub fn typ(&self) -> Option<Type> {
        match self {
            Self::Text(_) => Some(Type::Text),
            Self::Integer(_) => Some(Type::Integer),
            Self::Real(_, _) => Some(Type::Real),
            Self::Numeric(_) => Some(Type::Numeric),
            Self::Date(_) => Some(Type::Date),
            Self::Time(_) => Some(Type::Time),
            Self::ByteArray(_) => Some(Type::ByteArray),
            Self::Null => None,
            Self::BitVector(_) => Some(Type::BitVec),
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
            Type::Real => {
                let f: f64 = mysql_async::from_value_opt(val)?;
                Ok(Self::Real(
                    f.trunc() as i64,
                    (f.fract() * 1_000_000_000.0).round() as _,
                ))
            }
            Type::Numeric => {
                // TODO(fran): Add support for MySQL's DECIMAL.
                bail!("Conversion of {:?} to DECIMAL is not implemented", val)
            }
            Type::Date => Ok(Self::Date(mysql_async::from_value_opt(val)?)),
            Type::Time => Ok(Self::Time(match val {
                mysql_async::Value::Bytes(s) => {
                    MysqlTime::from_str(std::str::from_utf8(&s)?).map_err(|e| anyhow!("{}", e))?
                }
                mysql_async::Value::Time(neg, d, h, m, s, us) => {
                    MysqlTime::from_hmsus(!neg, ((d * 24) + h as u32).try_into()?, m, s, us.into())
                }
                _ => bail!("Could not convert {:?} to Time", val),
            })),
            // These types are PostgreSQL specific.
            Type::ByteArray => unimplemented!(),
            Type::BitVec => unimplemented!(),
        }
    }

    pub fn convert_type<'a>(&'a self, typ: &Type) -> anyhow::Result<Cow<'a, Self>> {
        match (self, typ) {
            (Self::Text(_), Type::Text)
            | (Self::Integer(_), Type::Integer)
            | (Self::Real(_, _), Type::Real)
            | (Self::Date(_), Type::Date)
            | (Self::Time(_), Type::Time)
            | (Self::Null, _) => Ok(Cow::Borrowed(self)),
            (Self::Text(txt), Type::Integer) => Ok(Cow::Owned(Self::Integer(txt.parse()?))),
            (Self::Text(txt), Type::Real) => Ok(Cow::Owned(Self::from(txt.parse::<f64>()?))),
            (Self::Text(txt), Type::Date) => Ok(Cow::Owned(Self::Date(
                NaiveDateTime::parse_from_str(txt, "%Y-%m-%d %H:%M:%S")?,
            ))),
            (Self::Text(txt), Type::Time) => Ok(Cow::Owned(Self::Time(txt.parse()?))),
            _ => {
                todo!()
            }
        }
    }

    pub fn hash_results(results: &[Self]) -> md5::Digest {
        let mut context = md5::Context::new();
        for result in results {
            context.consume(&format!("{}", result));
            context.consume("\n");
        }
        context.compute()
    }

    pub fn compare_type_insensitive(&self, other: &Self) -> bool {
        match other.typ() {
            None => *self == Value::Null,
            Some(typ) => Self::from_mysql_value_with_type(mysql_async::Value::from(self), &typ)
                .map_or(false, |v| v == *other),
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
                write!(f, "{} values hashing to {:x}", count, digest)
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
                write!(f, "{}", ps.iter().map(|p| format!("? = {}", p)).join("\n"))?;
            }
            QueryParams::NumberedParams(ps) => {
                write!(
                    f,
                    "{}",
                    ps.iter().map(|(n, p)| format!("${} = {}", n, p)).join("\n")
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
                let max_val = np.keys().max().unwrap();
                Numbered(0..=*max_val, np)
            }
        }
    }
}

impl From<QueryParams> for mysql_async::Params {
    fn from(qp: QueryParams) -> Self {
        match qp {
            qp if qp.is_empty() => mysql_async::Params::Empty,
            QueryParams::PositionalParams(vs) => {
                mysql_async::Params::Positional(vs.into_iter().map(mysql::Value::from).collect())
            }
            QueryParams::NumberedParams(nps) => mysql_async::Params::Named(
                nps.into_iter()
                    .map(|(n, v)| (n.to_string(), mysql_async::Value::from(v)))
                    .collect(),
            ),
        }
    }
}

/// Run a query against the database engine and check the results against an expected result set
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct Query {
    pub label: Option<String>,
    pub column_types: Option<Vec<Type>>,
    pub sort_mode: Option<SortMode>,
    pub conditionals: Vec<Conditional>,
    pub query: String,
    pub results: QueryResults,
    pub params: QueryParams,
}

impl Display for Query {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}\nquery {} {}\n{}\n{}----\n{}",
            self.conditionals.iter().join("\n"),
            self.column_types
                .as_ref()
                .map_or("".to_owned(), |cts| cts.iter().join("")),
            self.sort_mode.map_or("".to_owned(), |sm| sm.to_string()),
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

    /// The "hash-threshold" record sets a limit on the number of values that can appear in a result
    /// set. If the number of values exceeds this, then instead of recording each individual value
    /// in the full test script, an MD5 hash of all values is computed in stored. This makes the
    /// full test scripts much shorter, but at the cost of obscuring the results. If the
    /// hash-threshold is 0, then results are never hashed. A hash-threshold of 10 or 20 is
    /// recommended. During debugging, it is advantage to set the hash-threshold to zero so that all
    /// results can be seen.
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
            Record::Statement(s) => write!(f, "{}", s),
            Record::Query(q) => write!(f, "{}", q),
            Record::HashThreshold(ht) => writeln!(f, "hash-threshold {}", ht),
            Record::Halt { conditionals } => {
                writeln!(f, "{}\nhalt\n", conditionals.iter().join("\n"))
            }
            Record::Graphviz => f.write_str("graphviz\n"),
            Record::Sleep(msecs) => writeln!(f, "sleep {}", msecs),
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
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_result_value() {
        assert_eq!(format!("{}", Value::Text("\0".to_string())), "@");
    }

    #[test]
    fn compare_result_value() {
        assert!(Value::Integer(9) > Value::Integer(10));
    }
}
